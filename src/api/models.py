"""Data models for the AFE server: agents, sandboxes, and runtime session state."""

from __future__ import annotations

import asyncio
import time
from collections import deque
from dataclasses import asdict, dataclass, field
from typing import Any


@dataclass
class PendingPrompt:
    """Queued prompt waiting for the active prompt to finish."""
    rpc_id: str
    message: str
    submitted_at: float = field(default_factory=time.time)



@dataclass
class AgentConfig:
    agent_type: str = "claude"
    model: str | None = None
    prompt: str | None = None
    cwd: str | None = None
    tools: list[str] | None = None
    mcp_servers: dict | None = None
    skills: list | dict | None = None  # npx skills sources
    dockerfile: str | None = None
    env: dict[str, str] = field(default_factory=dict)
    # Named shared folders this agent mounts at /mnt/<name>. Each entry refers
    # to a subdir under <volume>/shared/ on the agent's volume. Opt-in per
    # agent — an empty list means no shared mounts (default). Unknown names
    # are silently ignored at mount time; the mount just doesn't appear.
    shared_mounts: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        d = {k: v for k, v in asdict(self).items() if v is not None}
        if not d.get("env"):
            d.pop("env", None)
        if not d.get("shared_mounts"):
            d.pop("shared_mounts", None)
        return d

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> AgentConfig:
        return cls(**{k: v for k, v in data.items() if k in _AGENT_CONFIG_FIELDS})


_AGENT_CONFIG_FIELDS = frozenset(AgentConfig.__dataclass_fields__)

# Sentinel pushed to slow subscribers when they are kicked off for being full
_KICK_SENTINEL = object()

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
    root: str = "/tmp"
    volume_id: str | None = None
    subpath: str | None = None
    # Host-side port the supervisor listens on. Populated for docker/local;
    # always NULL for Daytona (URL comes from the SDK-signed preview API).
    listen_port: int | None = None

    def derive_url(self) -> str:
        """Reconstruct the supervisor URL from the DB row.

        Used on cold-start when the in-memory ``_INSTANCES`` cache is empty.
        Requires ``listen_port`` to be set (docker/local). Daytona has no
        port-based URL; callers must consult the provider's SDK instead.
        """
        if self.listen_port is not None:
            return f"http://localhost:{self.listen_port}"
        raise NotImplementedError(
            f"URL derivation for provider '{self.provider}' requires external resolver"
        )


@dataclass
class VolumeRecord:
    id: str
    name: str
    provider: str
    provider_ref: str
    status: str = "ready"
    supervisor_agent_types: list[str] = field(default_factory=list)


@dataclass
class SessionState:
    """In-memory runtime state binding an agent to a sandbox."""
    session_id: str
    agent_id: str
    sandbox_id: str
    acp_session_id: str | None = None
    inner_session_id: str | None = None
    agent_type: str = "claude"  # claude | codex (selects ProviderAdapter)
    client: object | None = None  # AcpClient, typed loosely to avoid circular import
    shutdown: asyncio.Event = field(default_factory=asyncio.Event)
    last_event_id: str | None = None  # SSE cursor — skip events before this ID
    last_activity: float = field(default_factory=time.time)
    turn_completed_at: float | None = field(default=None)  # when the last stopReason arrived
    # Persistent SSE reader — connects once at session creation, fans out to subscribers
    _reader_task: object | None = field(default=None, repr=False)  # asyncio.Task
    _reader_alive: bool = field(default=False, repr=False)
    # True only while the upstream SSE stream is actively connected (between
    # a successful raise_for_status and the stream's exit). Unlike
    # _reader_alive (task running, possibly in retry backoff), this is the
    # "supervisor is definitely responsive" signal the hot-path fast-check
    # uses to skip a redundant health probe on every POST /message.
    _reader_connected: bool = field(default=False, repr=False)
    _log_chain: object | None = field(default=None, repr=False)
    errors: deque = field(default_factory=lambda: deque(maxlen=100), repr=False)

    # ── Explicit scheduler ──
    active_rpc_id: str | None = field(default=None, repr=False)
    pending_prompts: deque = field(default_factory=deque, repr=False)  # deque[PendingPrompt]
    _prompt_ready: asyncio.Event = field(default_factory=asyncio.Event, repr=False)
    _prompt_done: asyncio.Event = field(default_factory=asyncio.Event, repr=False)
    _scheduler_task: object | None = field(default=None, repr=False)  # asyncio.Task
    # ── Per-session supervisor ──
    supervisor_url: str | None = field(default=None, repr=False)
    supervisor_port: int | None = field(default=None, repr=False)
    # ── Cached ACP state ──
    available_commands: list | None = field(default=None, repr=False)
    # ── Subscribers ──
    _session_subscribers: list = field(default_factory=list, repr=False)  # list[asyncio.Queue]
    _rpc_subscribers: dict = field(default_factory=dict, repr=False)  # dict[str, list[asyncio.Queue]]

    @property
    def agent_busy(self) -> bool:
        return self.active_rpc_id is not None

    # ── Session-scoped subscribers (receive all events) ──

    def subscribe_session(self) -> "asyncio.Queue":
        """Register a session-scoped subscriber queue."""
        q: asyncio.Queue = asyncio.Queue(maxsize=10000)
        self._session_subscribers.append(q)
        return q

    def unsubscribe_session(self, q: "asyncio.Queue") -> None:
        """Remove a session-scoped subscriber queue."""
        try:
            self._session_subscribers.remove(q)
        except ValueError:
            pass

    # ── RPC-scoped subscribers (receive events for one rpc_id only) ──

    def subscribe_rpc(self, rpc_id: str) -> "asyncio.Queue":
        """Register an RPC-scoped subscriber queue for a specific prompt."""
        q: asyncio.Queue = asyncio.Queue(maxsize=10000)
        self._rpc_subscribers.setdefault(rpc_id, []).append(q)
        return q

    def unsubscribe_rpc(self, rpc_id: str, q: "asyncio.Queue") -> None:
        """Remove an RPC-scoped subscriber queue."""
        qs = self._rpc_subscribers.get(rpc_id)
        if qs is not None:
            try:
                qs.remove(q)
            except ValueError:
                pass
            if not qs:
                self._rpc_subscribers.pop(rpc_id, None)

    # ── Dispatch ──

    def _kick_subscriber(self, q: "asyncio.Queue") -> None:
        """Kick a subscriber by putting _KICK_SENTINEL. Makes one slot if needed."""
        try:
            q.put_nowait(_KICK_SENTINEL)
        except asyncio.QueueFull:
            try:
                q.get_nowait()
                q.put_nowait(_KICK_SENTINEL)
            except (asyncio.QueueEmpty, asyncio.QueueFull):
                pass

    def _send(self, queues: list, item) -> None:
        """Push item to queues, kicking full ones."""
        dead = [q for q in queues if not _try_put(q, item)]
        for q in dead:
            queues.remove(q)
            self._kick_subscriber(q)

    def dispatch(self, tag: str | None, item) -> None:
        """Route to matching RPC subscribers + all session subscribers."""
        if tag is not None:
            rpc_qs = self._rpc_subscribers.get(tag)
            if rpc_qs:
                self._send(rpc_qs, item)
        self._send(self._session_subscribers, item)

    def broadcast(self, item) -> None:
        """Push to ALL subscribers (session + all RPC). For sentinels/heartbeats."""
        self._send(self._session_subscribers, item)
        for rpc_qs in list(self._rpc_subscribers.values()):
            self._send(rpc_qs, item)



def _try_put(q: asyncio.Queue, item) -> bool:
    try:
        q.put_nowait(item)
        return True
    except asyncio.QueueFull:
        return False


@dataclass
class LogEntry:
    id: int
    session_id: str
    agent_id: str
    sandbox_id: str
    event_type: str      # "user_message" | "assistant_message" | "tool_call" | "tool_result" | "usage" | "error"
    payload: dict        # JSON, structure varies by event_type
    created_at: float
