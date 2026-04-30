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

    # Provider-side discriminator: used by ``_bootstrap_session()`` to
    # validate the session's volume.provider matches what this concrete
    # class expects. Subclass overrides.
    volume_provider: str = ""

    def __init__(self, *, session_id: str, state: SandboxState) -> None:
        self.session_id = session_id
        self.state = state
        self.liveness = Liveness(probe=self._liveness_probe)
        # Subscriber fan-out: persistent across many execute_prompt calls
        # so that GET /events can stay open across N prompts.
        self._subscribers: dict[str, asyncio.Queue[Any]] = {}
        # Set by concrete start(); used by file-proxy endpoints to talk
        # to the supervisor without going through ACP.
        self._supervisor_url: str | None = None
        # Volume + spawn-env + cwd + ACP correlation, populated by
        # _bootstrap_session() on first start().
        self._volume_ref: str | None = None
        self._agent_id: str | None = None
        self._subpath: str | None = None
        self._spawn_env: dict[str, str] = {}
        self._cwd: str = "/tmp"
        self._inner_session_id: str | None = None
        self._acp_session_id: str | None = None
        self._acp_attached: bool = False
        self._supervisor_installed: bool = False

    async def _bootstrap_session(self) -> str:
        """Idempotent: load the session row + volume from DB, install the
        per-agent supervisor on the volume if needed, hydrate
        ``_spawn_env``, ``_cwd``, ``_inner_session_id``, ``_subpath``,
        and ``_volume_ref``.

        Concrete ``start()`` calls this *before* invoking the per-provider
        create/start primitives so the volume is supervisor-ready and the
        spawn environment knows about session env/secrets. Subsequent
        starts on the same in-memory instance return early.

        Returns the volume_ref (provider-native identifier) for the
        caller to pass into create_sandbox.
        """
        if self._volume_ref is not None:
            return self._volume_ref

        from api import db as _db

        sess = await _db.get_session(self.session_id)
        if sess is None:
            raise RuntimeError(f"session {self.session_id} not found in DB")
        volume = await _db.get_volume(sess["volume_id"])
        if volume is None:
            raise RuntimeError(f"session {self.session_id}: volume missing")
        if self.volume_provider and volume.provider != self.volume_provider:
            raise RuntimeError(
                f"session {self.session_id}: volume.provider="
                f"{volume.provider!r} but {type(self).__name__} expects "
                f"{self.volume_provider!r}"
            )

        self._volume_ref = volume.provider_ref
        self._agent_id = sess.get("agent_id")
        # Subpath governs ACP HOME inside the sandbox. Use ``agents/<agent_id>``
        # so multiple sessions of the same agent share Claude's
        # ~/.claude/projects/... JSONL store — that's what makes session/load
        # find the prior conversation. Per session_id would shard the JSONLs
        # and break recovery (`Claude Code executable not found at .../cli.js`
        # is the symptom: SDK ENOENT on the spawn cwd).
        self._subpath = f"agents/{self._agent_id}" if self._agent_id else f"sessions/{self.session_id}"
        self._spawn_env = {
            **(sess.get("env") or {}),
            **(sess.get("secrets") or {}),
        }
        self._cwd = sess.get("cwd") or self.state.recipe.root or "/tmp"
        self._inner_session_id = (
            sess.get("inner_session_id") or self._inner_session_id
        )

        agent_type = self.state.recipe.agent_type
        if agent_type not in volume.supervisor_agent_types:
            from importlib import import_module
            mod_name = {
                "daytona": "api.providers.daytona",
                "docker":  "api.providers.docker",
                "local":   "api.providers.local",
                "modal":   "api.providers.modal",
            }.get(volume.provider)
            if mod_name is None:
                raise RuntimeError(
                    f"no install_supervisor module for provider {volume.provider}"
                )
            provider_mod = import_module(mod_name)
            await provider_mod.install_supervisor(volume.provider_ref, agent_type)
            await _db.add_supervisor_agent_type(volume.id, agent_type)

        return self._volume_ref

    async def _attach_acp(self) -> None:
        """Idempotent: handshake + session/load (or new) over ACP and
        persist any newly-minted ``inner_session_id`` to the session row.

        Concrete ``start()`` calls this after the supervisor is up and
        ``self._supervisor_url`` is set. Re-invocation after a successful
        attach is a no-op."""
        if self._acp_attached:
            return
        if self._supervisor_url is None or self._acp_session_id is None:
            return

        from api import db as _db
        from api.acp_client import AcpClient

        client = AcpClient(self._supervisor_url)
        try:
            await client.attach(
                self._acp_session_id,
                self.state.recipe.agent_type,
                cwd=self._cwd,
                inner_session_id=self._inner_session_id,
            )
            self._inner_session_id = client.get_inner_session_id(
                self._acp_session_id
            )
        finally:
            await client.aclose()

        self._acp_attached = True
        if self._inner_session_id:
            async with _db.get_db() as conn:
                await conn.execute(
                    "UPDATE sessions SET inner_session_id = %s WHERE id = %s",
                    (self._inner_session_id, self.session_id),
                )

    @property
    def supervisor_url(self) -> str | None:
        """Public read of the supervisor URL set by ``start()``. None
        before start or after shutdown. Used by file-browse endpoints."""
        return self._supervisor_url

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
