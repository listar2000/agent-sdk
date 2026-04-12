"""Data models for the AFE server: agents, sandboxes, and runtime session state."""

from __future__ import annotations

import asyncio
import time
from collections import deque
from dataclasses import asdict, dataclass, field
from typing import Any


@dataclass
class AgentConfig:
    agent_type: str = "claude"
    model: str | None = None
    prompt: str | None = None
    cwd: str | None = None
    tools: list[str] | None = None
    mcp_servers: dict | None = None
    skills: dict | None = None
    dockerfile: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {k: v for k, v in asdict(self).items() if v is not None}

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> AgentConfig:
        return cls(**{k: v for k, v in data.items() if k in _AGENT_CONFIG_FIELDS})


_AGENT_CONFIG_FIELDS = frozenset(AgentConfig.__dataclass_fields__)

# ── Session event type constants ──
EVT_USER_MESSAGE = "user_message"
EVT_ASSISTANT_MESSAGE = "assistant_message"
EVT_REASONING = "reasoning"
EVT_TOOL_CALL = "tool_call"
EVT_TOOL_RESULT = "tool_result"
EVT_USAGE = "usage"
EVT_ERROR = "error"

# ── Sandbox status constants ──
STATUS_RUNNING = "running"
STATUS_STOPPED = "stopped"
STATUS_ERROR = "error"
STATUS_CREATING = "creating"


@dataclass
class AgentRecord:
    id: str
    name: str | None = None
    config: AgentConfig = field(default_factory=AgentConfig)


@dataclass
class SandboxRecord:
    id: str
    provider: str
    sandbox_ref: str
    status: str = "stopped"

    def derive_url(self) -> str:
        from .providers import PORT_BASED_PROVIDERS
        if self.provider in PORT_BASED_PROVIDERS:
            return f"http://localhost:{self.sandbox_ref}"
        raise NotImplementedError(
            f"URL derivation for provider '{self.provider}' requires external resolver"
        )


@dataclass
class SessionState:
    """In-memory runtime state binding an agent to a sandbox."""
    session_id: str
    agent_id: str
    sandbox_id: str
    acp_session_id: str | None = None
    inner_session_id: str | None = None
    client: object | None = None  # SandboxAgentClient, typed loosely to avoid circular import
    shutdown: asyncio.Event = field(default_factory=asyncio.Event)
    last_event_id: str | None = None  # SSE cursor — skip events before this ID
    last_activity: float = field(default_factory=time.time)
    agent_busy: bool = field(default=False)  # True while agent is processing (between prompt and stopReason)
    turn_completed_at: float | None = field(default=None)  # when the last stopReason arrived
    current_rpc_id: str | None = field(default=None)  # rpc_id of the in-flight prompt; tags log events
    # Persistent SSE reader — connects once at session creation, fans out to subscribers
    _reader_task: object | None = field(default=None, repr=False)  # asyncio.Task
    _subscribers: list = field(default_factory=list, repr=False)  # list[asyncio.Queue]
    _reader_alive: bool = field(default=False, repr=False)
    _replay_buffer: deque = field(default_factory=lambda: deque(maxlen=5000), repr=False)
    _turn_gen: int = field(default=0, repr=False)  # incremented by new_turn(); tags buffered events
    _buffering_paused: bool = field(default=False, repr=False)  # True between new_turn() and resume_buffering()
    _log_chain: object | None = field(default=None, repr=False)  # asyncio.Task — serializes log writes
    errors: deque = field(default_factory=lambda: deque(maxlen=100), repr=False)

    def new_turn(self) -> None:
        """Mark the start of a new agent turn.

        Increments the turn generation counter, clears the replay buffer,
        and pauses buffering.  While paused, broadcast() forwards events
        to live subscribers but does NOT add them to the replay buffer —
        this prevents trailing chunks from the prior turn (delivered by
        the SSE reader after new_turn() returns) from being tagged with
        the new generation and replayed to future subscribers.

        Call resume_buffering() once the prompt has been accepted by the
        agent so that events from the new turn are captured correctly.
        """
        self._turn_gen += 1
        self._replay_buffer.clear()
        self._buffering_paused = True

    def resume_buffering(self) -> None:
        """Allow broadcast() to start filling the replay buffer again.

        Must be called after new_turn() once it is safe to buffer events
        for the current turn (i.e. the prompt call has been accepted and
        any in-flight chunks from the prior turn have been flushed).
        """
        self._buffering_paused = False

    def subscribe(self) -> "asyncio.Queue":
        """Register a new subscriber queue. Replays buffered events from an in-progress turn only.

        If no turn is in progress (agent not busy), no replay happens —
        prevents old events from bleeding into the next turn.
        """
        q: asyncio.Queue = asyncio.Queue(maxsize=10000)
        if self.agent_busy:
            current_gen = self._turn_gen
            for gen, item in self._replay_buffer:
                if gen != current_gen:
                    continue
                try:
                    q.put_nowait(item)
                except asyncio.QueueFull:
                    break
        self._subscribers.append(q)
        return q

    def unsubscribe(self, q: "asyncio.Queue") -> None:
        """Remove a subscriber queue."""
        try:
            self._subscribers.remove(q)
        except ValueError:
            pass

    def broadcast(self, item) -> None:
        """Push item to all subscriber queues and replay buffer.

        Events are always forwarded to live subscribers.  They are added
        to the replay buffer only when buffering is active (i.e. after
        resume_buffering() has been called following new_turn()).  This
        prevents trailing chunks from a completed turn — which the SSE
        reader may deliver after new_turn() bumps the generation — from
        being stored under the new generation and replayed to future
        subscribers.
        """
        # Only buffer when not paused; paused = between new_turn() and resume_buffering()
        if isinstance(item, str) and not self._buffering_paused:
            self._replay_buffer.append((self._turn_gen, item))
        for q in self._subscribers:
            try:
                q.put_nowait(item)
            except asyncio.QueueFull:
                pass  # drop if subscriber is too slow


@dataclass
class LogEntry:
    id: int
    session_id: str
    agent_id: str
    sandbox_id: str
    event_type: str      # "user_message" | "assistant_message" | "tool_call" | "tool_result" | "usage" | "error"
    payload: dict        # JSON, structure varies by event_type
    created_at: float
