"""Data models for the API server: agents, volumes, and runtime session state.

Sandbox identity is not modelled here — it lives in ``sessions.sandbox_state``
JSONB and is owned by ``api.sandbox.SessionPool`` (see
``api.sandbox.state.SandboxState`` for the discriminated union, and
 for the model)."""

from __future__ import annotations

import asyncio
import time
from collections import deque
from dataclasses import asdict, dataclass, field
from typing import Any, Literal


# ── Closed enums (Literal aliases) ──
# Provider names and sandbox statuses are closed sets — narrowing them
# lets type-checkers catch the silent-drop bug class (e.g. a dict literal
# that maps "daytona"+"docker" but forgets "unix_local"+"modal").
Provider = Literal["unix_local", "docker", "daytona", "modal"]
SandboxStatus = Literal["running", "stopped", "error", "creating", "missing"]


@dataclass
class PendingPrompt:
    """Queued prompt waiting for the active prompt to finish."""
    rpc_id: str
    message: str
    submitted_at: float = field(default_factory=time.time)



@dataclass
class AgentConfig:
    """Pure agent identity. No per-invocation or provisioning knobs — those
    live on the session (cwd, env, secrets) or sandbox (dockerfile,
    shared_mounts, root) rows.
    """
    agent_type: str = "claude"
    model: str | None = None
    mcp_servers: dict | None = None
    skills: list | dict | None = None  # npx skills sources
    # ACP dynamic config that gets re-applied on every fresh attach so
    # cold-recovery (Type-2) doesn't silently revert a caller's
    # set_mode / set_thought_level. Keep model on its own field above
    # for back-compat (it predates this group).
    mode: str | None = None              # "default" | "plan" | "bypassPermissions" | "acceptEdits" | ...
    thought_level: str | None = None     # "low" | "medium" | "high" — Claude's "thinking" config_id

    def to_dict(self) -> dict[str, Any]:
        return {k: v for k, v in asdict(self).items() if v is not None}

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


# SandboxRecord removed: the sandboxes table is gone. Sandbox identity
# lives in ``sessions.sandbox_state`` JSONB owned by the SessionPool.


@dataclass
class VolumeRecord:
    id: str
    name: str
    provider: Provider
    provider_ref: str
    status: str = "ready"
    # ``supervisor_agent_types`` field deleted in Phase E of
    # the runtime-image-unification refactor. The DB column stays (now unused)
    # until the column-drop migration ships.


SessionLifecycle = Literal["live", "hibernated"]


@dataclass
class SessionState:
    """LEGACY in-memory runtime state binding an agent to a sandbox.

    Vestigial: the runtime is now ``api.sandbox.SessionPool`` +
    ``api.sandbox.session.BaseSandboxSession`` (see
    ). This dataclass survives only for
    response-shape back-compat fields the dashboard reads
    (``agent_busy``, ``active_rpc_id``, ``pending_count``) — which are
    constants in current responses. Do not wire new code to it."""
    session_id: str
    agent_id: str
    sandbox_id: str
    acp_session_id: str | None = None
    inner_session_id: str | None = None
    agent_type: str = "claude"  # claude | codex (selects ProviderAdapter)
    client: object | None = None  # AcpClient, typed loosely to avoid circular import
    # Hibernation is now explicit state. The two writers below keep this
    # in sync with _INSTANCES on the server side: _hibernate_session flips
    # to "hibernated" right after popping _INSTANCES; _rebind_state flips
    # back to "live" right after repopulating it. New sessions are "live"
    # by default since both construction sites are post-supervisor-attach.
    lifecycle: SessionLifecycle = "live"
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
    # Buffer for items dispatched while nobody is listening. When /events
    # reconnects after a stream-closed gap (UI's EventSource retry loop),
    # the POST /message that landed in the gap has already dispatched its
    # reply events; without this buffer they'd be silently dropped and the
    # UI would sit at "Queued for agent" forever. Bounded: an agent turn
    # is ~50-200 blocks so 2000 gives 10-20x headroom without growing
    # unbounded if no client ever returns.
    _pending_broadcasts: deque = field(
        default_factory=lambda: deque(maxlen=2000), repr=False,
    )

    @property
    def agent_busy(self) -> bool:
        return self.active_rpc_id is not None

    @property
    def is_hibernated(self) -> bool:
        return self.lifecycle == "hibernated"

    # ── Session-scoped subscribers (receive all events) ──

    def subscribe_session(self) -> "asyncio.Queue":
        """Register a session-scoped subscriber queue.

        Also drains any items buffered during a no-subscribers gap onto
        the new queue, in order. Without this, the UI's /events reconnect
        after a stream-closed race loses every reply event that arrived
        while the EventSource was retrying.
        """
        q: asyncio.Queue = asyncio.Queue(maxsize=10000)
        while self._pending_broadcasts:
            if not _try_put(q, self._pending_broadcasts.popleft()):
                break  # queue full — stop draining, rest is dropped
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
        """Route to matching RPC subscribers + all session subscribers.

        When BOTH target lists are empty (UI's /events has disconnected
        and no per-rpc caller is waiting), buffer the item onto
        ``_pending_broadcasts`` so the next ``subscribe_session`` call
        can replay. This closes the UI reconnect-gap race where reply
        events flow while the EventSource retry timer is still running.
        """
        rpc_qs = self._rpc_subscribers.get(tag) if tag is not None else None
        rpc_delivered = bool(rpc_qs)
        if rpc_delivered:
            self._send(rpc_qs, item)
        if self._session_subscribers:
            self._send(self._session_subscribers, item)
        elif not rpc_delivered:
            self._pending_broadcasts.append(item)

    def broadcast(self, item) -> None:
        """Push to ALL subscribers (session + all RPC). For sentinels/heartbeats."""
        self._send(self._session_subscribers, item)
        for rpc_qs in list(self._rpc_subscribers.values()):
            self._send(rpc_qs, item)

    def kick_all(self) -> None:
        """Wake every subscriber and empty the subscriber tables.

        Called on fatal reader death and forced shutdown — the /events
        handler's ``q.get()`` needs to unblock so it can see the shutdown
        flag and close the stream. Clearing the tables prevents further
        dispatch into now-orphaned queues.
        """
        subs = list(self._session_subscribers)
        self._session_subscribers.clear()
        for rpc_qs in self._rpc_subscribers.values():
            subs.extend(rpc_qs)
        self._rpc_subscribers.clear()
        for q in subs:
            self._kick_subscriber(q)



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
    event_type: str      # "user_message" | "assistant_message" | "tool_call" | "tool_result" | "usage" | "error"
    payload: dict        # JSON, structure varies by event_type
    created_at: float
