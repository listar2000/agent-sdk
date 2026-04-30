"""Abstract base class for SandboxSession — one per running compute.

Concrete provider classes (DaytonaSandboxSession, DockerSandboxSession,
…) implement the 5 lifecycle methods. Per
``docs/ephemeral-sandbox-design.md`` §5.

Decision: ``stop()`` and ``shutdown()`` are split. ``stop()`` is the
data-preserving operation (snapshot + pause compute). ``shutdown()`` is
the in-memory cleanup (cancel tasks, drop subscribers). The pool calls
both in sequence on graceful release; a truly-dead session gets only
``shutdown()``.
"""
from __future__ import annotations

import abc
import asyncio
import uuid
from collections.abc import AsyncIterator
from typing import Any

from .liveness import Liveness
from .state import SandboxState

# Sentinel placed on a subscriber's queue to signal end-of-stream.
_END = object()


class BaseSandboxSession(abc.ABC):
    """One session's running compute. Lifetime: from ``start()`` to
    ``shutdown()``. Owns provider-side handles, the per-session lock for
    serialised prompts, the subscriber fan-out for ``GET /events``, and
    the liveness oracle.

    Subclass contract: implement ``start()``, ``running()``,
    ``execute_prompt()``, ``stop()``, ``shutdown()``. The base class
    provides subscriber multiplex (``subscribe()``, ``_broadcast()``)
    and the liveness oracle wiring.
    """

    def __init__(self, *, session_id: str, state: SandboxState) -> None:
        self.session_id = session_id
        self.state = state
        self.liveness = Liveness(probe=self._liveness_probe)
        # Subscriber fan-out: persistent across many execute_prompt calls
        # so that GET /events can stay open across N prompts.
        self._subscribers: dict[str, asyncio.Queue[Any]] = {}

    # --- Lifecycle methods (concrete subclasses override) ---

    @abc.abstractmethod
    async def start(self) -> None:
        """Bring compute up; restore from ``state.snapshot_path`` if set;
        attach ACP. Mutates ``state`` in place (e.g. fills ``sandbox_id``
        on cold-create). Idempotent if already started.

        Provider-internal decision tree (not exposed):
          * state has reusable id → reattach if alive; restart if stopped
          * else → fresh create
          * then → mount, supervisor boot, snapshot extract, ACP attach
        """

    @abc.abstractmethod
    async def running(self) -> bool:
        """Single liveness oracle. Cheap fast-path via ``self.liveness``;
        falls through to a bounded supervisor probe when state is
        ``unknown``."""

    @abc.abstractmethod
    async def execute_prompt(
        self, message: str, *, rpc_id: str | None = None,
    ) -> AsyncIterator[Any]:
        """Open an SSE stream from the supervisor for this one prompt;
        drain it; close it; broadcast each event to subscribers AND yield
        to the caller. Errors propagate as exceptions.

        If ``rpc_id`` is supplied, the JSON-RPC envelope sent to the
        supervisor uses it (so callers can correlate events to a tag
        they returned to the user). If None, a fresh uuid is generated.
        """

    @abc.abstractmethod
    async def stop(self) -> None:
        """Write FULL filesystem snapshot to volume (update
        ``state.snapshot_path`` and bump ``state.snapshot_version``);
        then call ``daytona.stop()`` (pause). Never deletes the sandbox
        — explicit deletion is only triggered from
        ``DELETE /sessions/{id}`` or admin paths. Persists state to
        the caller (the pool persists it to DB)."""

    @abc.abstractmethod
    async def shutdown(self) -> None:
        """Final teardown of in-memory tasks. Doesn't touch the daytona
        side. Idempotent."""

    # --- Liveness probe hook (subclass overrides if it has a cheap probe) ---

    async def _liveness_probe(self) -> bool:
        """Default: no probe available. Subclasses override with a
        cheap supervisor /health call or equivalent."""
        return False

    # --- Subscriber fan-out (kept here so multi-subscriber GET /events
    #     works without per-provider plumbing) ---

    async def subscribe(self) -> AsyncIterator[Any]:
        """Yield every event broadcast to this session until either the
        consumer closes the iterator or the session shuts down.

        Late joiners only see events from subscribe-time onward; past
        events come from the ``session_log`` table (separate concern,
        per docs §15.1).
        """
        sid = str(uuid.uuid4())
        # Bounded queue: slow subscribers drop events rather than backpressuring
        # the source supervisor stream. Per docs §15.5 — keep today's behaviour.
        q: asyncio.Queue[Any] = asyncio.Queue(maxsize=1024)
        self._subscribers[sid] = q
        try:
            while True:
                event = await q.get()
                if event is _END:
                    return
                yield event
        finally:
            self._subscribers.pop(sid, None)

    def _broadcast(self, event: Any) -> None:
        """Fan out an event to every active subscriber. Slow subscribers
        whose queue is full silently drop this event."""
        for q in list(self._subscribers.values()):
            try:
                q.put_nowait(event)
            except asyncio.QueueFull:
                # Slow subscriber: drop. They'll catch up on whatever's next.
                pass

    def _close_subscribers(self) -> None:
        """Signal end-of-stream to every subscriber. Called from
        ``shutdown()`` so pending ``subscribe()`` consumers exit."""
        for q in list(self._subscribers.values()):
            try:
                q.put_nowait(_END)
            except asyncio.QueueFull:
                # Drop one event to make room for the sentinel.
                try:
                    q.get_nowait()
                except asyncio.QueueEmpty:
                    pass
                try:
                    q.put_nowait(_END)
                except asyncio.QueueFull:
                    pass
