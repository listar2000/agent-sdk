"""Abstract base class for SandboxSession — one per running compute.

Concrete provider classes (DaytonaSandboxSession, DockerSandboxSession,
UnixLocalSandboxSession, ModalSandboxSession) implement the 5 lifecycle
methods.

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

from collections import deque

from .liveness import Liveness
from .state import SandboxState

# Sentinel placed on a subscriber's queue to signal end-of-stream.
_END = object()

# Sentinel yielded from ``subscribe()`` when no event has arrived for
# ``_HEARTBEAT_INTERVAL_S``. The /events handler renders these as SSE
# comment lines (``: heartbeat\n\n``) so intermediaries (nginx, CF,
# browser EventSource) don't close idle connections during quiet periods
# between prompts.
_HEARTBEAT = object()
_HEARTBEAT_INTERVAL_S = 20.0

# Bound on the per-session event replay buffer. Late subscribers (UI
# reconnects after a "stream closed" hiccup) replay this buffer first,
# then receive live broadcasts. A new POST /message that fires before
# the reconnect lands here, so the user-visible "lost reply" disappears.
_BUFFER_SIZE = 1024


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
        # Serialises ``execute_prompt`` per session so concurrent POST
        # /message calls run sequentially and ``session_log`` row order
        # tracks SSE arrival order. ``interrupt=True`` cancels the active
        # turn (so the lock releases promptly) but doesn't jump the
        # queue — FIFO is preserved (test_queue_plus_interrupt_parity).
        self._prompt_lock = asyncio.Lock()
        # Subscriber fan-out: persistent across many execute_prompt calls
        # so that GET /events can stay open across N prompts.
        self._subscribers: dict[str, asyncio.Queue[Any]] = {}
        # Bounded replay buffer of recent broadcasts so a UI that
        # reconnects /events after a transient close still receives
        # events posted during the gap. Bounded → memory bounded.
        self._buffer: deque = deque(maxlen=_BUFFER_SIZE)
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

        # the runtime-image-unification refactor: the per-volume
        # ``install_supervisor`` step is gone. The supervisor + ACP bins
        # ship in the agent-sdk Docker image at ``/opt/agent-sdk/runtime/``,
        # so each provider's ``create_sandbox`` resolves them from a fixed
        # in-image path. ``volumes.supervisor_agent_types`` (the cache that
        # used to gate this branch) is also removed; the column drop is
        # the last commit of Phase E.
        return self._volume_ref

    async def _attach_acp(self) -> None:
        """Idempotent: handshake + session/load (or new) over ACP and
        persist any newly-minted ``inner_session_id`` to the session row.

        Concrete ``start()`` calls this after the supervisor is up and
        ``self._supervisor_url`` is set. Re-invocation after a successful
        attach is a no-op.

        Re-applies any persisted ``agents.config.model`` after each fresh
        attach. ``set_model`` only affects the current ACP session — every
        cold-create / Type-2 recovery mints a new ACP session that
        defaults to ``"default"`` (sonnet 4.6), so without this replay
        callers who set ``model="haiku"`` once would silently revert to
        sonnet on the first sandbox restart."""
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
            # Re-apply persisted ACP dynamic config. Read fresh from DB
            # rather than caching on the session object — POST /config
            # writes to agents.config so a mid-flight change there also
            # propagates on the next attach. Each set_* is bounded best-
            # effort: a transient failure on one shouldn't block the
            # others (e.g. supervisor accepts model but rejects an
            # unknown thought_level — keep the model change).
            if self._agent_id and self._inner_session_id:
                try:
                    agent = await _db.get_agent(self._agent_id)
                    cfg = agent.config if agent else None
                except Exception:
                    cfg = None
                replay = []
                if cfg:
                    if cfg.model:
                        replay.append(("model", client.set_model, cfg.model))
                    if cfg.mode:
                        replay.append(("mode", client.set_mode, cfg.mode))
                    if cfg.thought_level:
                        replay.append(("thought_level",
                                       client.set_thought_level,
                                       cfg.thought_level))
                for name, fn, val in replay:
                    try:
                        await fn(self._acp_session_id, val)
                    except Exception:
                        import logging
                        logging.getLogger(__name__).exception(
                            "set_%s replay failed for session %s",
                            name, self.session_id,
                        )
        finally:
            await client.aclose()

        self._acp_attached = True
        # The ACP attach above is itself a successful round-trip to the
        # supervisor — record it as a positive liveness signal so the
        # pool's next force_probe doesn't immediately re-probe via HTTP
        # and race the proxy (Daytona's signed-URL proxy returns 502 for
        # ~1-2s after a fresh URL is minted; same race class PR #20
        # fixed in the legacy path). Stale-after-idle still triggers a
        # real probe if the session sits idle past the freshness window.
        self.liveness.observe_chunk()
        if self._inner_session_id:
            await _db.update_session_inner_session_id(
                self.session_id, self._inner_session_id,
            )

    @property
    def supervisor_url(self) -> str | None:
        """Public read of the supervisor URL set by ``start()``. None
        before start or after shutdown. Used by file-browse endpoints."""
        return self._supervisor_url

    @property
    def acp_session_id(self) -> str | None:
        """Public read of the ACP session id minted on first attach."""
        return self._acp_session_id

    @property
    def inner_session_id(self) -> str | None:
        """Public read of the agent-native inner session id (used for
        ``session/load`` on cold-recovery)."""
        return self._inner_session_id

    async def acp_call(
        self, method: str, params: dict | None = None, *, notify: bool = False,
    ) -> Any:
        """Forward a JSON-RPC call to this session's ACP supervisor.

        Encapsulates ``AcpClient`` construction + the inner-session-id
        cache prime that callers used to do by hand. Auto-injects the
        inner ``sessionId`` into ``params`` so callers don't have to
        track it. ``notify=True`` sends as a JSON-RPC notification (no
        response).

        Raises ``RuntimeError`` if the session has no live supervisor or
        no attached ACP session — the route handler maps that to 503.
        """
        if self._supervisor_url is None or self._acp_session_id is None:
            raise RuntimeError("session has no live ACP supervisor")
        from api.acp_client import AcpClient

        client = AcpClient(self._supervisor_url)
        if self._inner_session_id is not None:
            client._inner_session_ids[self._acp_session_id] = self._inner_session_id
        try:
            return await client.call(
                self._acp_session_id, method, params or {}, notify=notify,
            )
        finally:
            await client.aclose()

    # --- Lifecycle methods (concrete subclasses override) ---

    @abc.abstractmethod
    async def start(self) -> None:
        """Bring compute up; restore from ``state.snapshot_path`` if set;
        attach ACP. Mutates ``state`` in place (e.g. fills `sandbox_ref`
        on cold-create). Idempotent if already started.

        Provider-internal decision tree (not exposed):
          * state has reusable id → reattach if alive; restart if stopped
          * else → fresh create
          * then → mount, supervisor boot, snapshot extract, ACP attach
        """

    @abc.abstractmethod
    async def running(self, *, force_probe: bool = False) -> bool:
        """Single liveness oracle. Cheap fast-path via ``self.liveness``;
        falls through to a bounded supervisor probe when state is
        ``unknown``. With ``force_probe=True`` the probe always runs."""

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

    # --- Cancel: best-effort interrupt of an in-flight execute_prompt ---

    async def cancel_active_prompt(self) -> None:
        """Send ``session/cancel`` notification to the supervisor's ACP
        child so the in-flight turn aborts.

        Best-effort — JSON-RPC notification has no response. The actual
        ``done`` event arrives via the existing ``execute_prompt`` SSE
        stream. Caller is responsible for waiting on it (or the
        broadcast queue) if they need synchronous-cancel semantics.

        No-op when the session has no live supervisor URL or no
        attached ACP session — there's nothing to cancel.
        """
        import httpx  # local: avoid pulling httpx into the module-load path

        if (
            self._supervisor_url is None
            or self._inner_session_id is None
            or self._acp_session_id is None
        ):
            return
        cancel_payload = {
            "jsonrpc": "2.0",
            "method": "session/cancel",
            "params": {"sessionId": self._inner_session_id},
        }
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                await client.post(
                    f"{self._supervisor_url}/v1/acp/{self._acp_session_id}",
                    json=cancel_payload,
                )
        except Exception:
            import logging
            logging.getLogger(__name__).exception(
                "cancel_active_prompt failed for session %s", self.session_id,
            )

    # --- ACP config: forward set_mode / set_model / set_thought_level ---

    async def _acp_call(self, method_name: str, *args) -> None:
        """Open an ``AcpClient`` against this session's supervisor and
        invoke ``method_name(self._acp_session_id, *args)``. Used by
        the wrapper methods below so each one stays a one-liner.

        No-op (silently) if the session has no live supervisor URL or
        no attached ACP session — same shape as ``cancel_active_prompt``."""
        if self._supervisor_url is None or self._acp_session_id is None:
            return
        from api.acp_client import AcpClient  # local import: avoid cycles
        client = AcpClient(self._supervisor_url)
        # AcpClient indexes inner_session_ids by acp_session_id internally;
        # mirror what attach() did so set_mode() etc can resolve it.
        if self._inner_session_id is not None:
            client._inner_session_ids[self._acp_session_id] = self._inner_session_id
        try:
            await getattr(client, method_name)(self._acp_session_id, *args)
        finally:
            await client.aclose()

    async def set_mode(self, mode: str) -> None:
        await self._acp_call("set_mode", mode)

    async def set_model(self, model: str) -> None:
        await self._acp_call("set_model", model)

    async def set_thought_level(self, level: str) -> None:
        await self._acp_call("set_thought_level", level)

    # --- Liveness probe hook (subclass overrides if it has a cheap probe) ---

    async def _liveness_probe(self) -> bool:
        """Default: no probe available. Subclasses override with a
        cheap supervisor /health call or equivalent."""
        return False

    # --- Subscriber fan-out (kept here so multi-subscriber GET /events
    #     works without per-provider plumbing) ---

    def register_subscriber(self) -> tuple[str, "asyncio.Queue[Any]"]:
        """Eagerly register a subscriber queue (sync) so callers can
        kick off the producer (e.g. ``execute_prompt``) before iterating.

        Returns ``(sid, queue)``. Caller passes both back to
        ``iterate_subscriber`` to drain. Splitting registration from
        iteration matters because ``async def`` generators don't run
        their body — including queue registration — until the first
        ``__anext__()`` call. Without this split, a producer started
        after ``subscribe()`` returns the generator object but BEFORE
        the first iteration would broadcast events that nothing has
        registered to receive — and the consumer would block up to
        ``_HEARTBEAT_INTERVAL_S`` waiting for the queue to fill.
        """
        sid = str(uuid.uuid4())
        # Bounded queue: slow subscribers drop events rather than backpressuring
        # the source supervisor stream. Per docs §15.5 — keep today's behaviour.
        q: asyncio.Queue[Any] = asyncio.Queue(maxsize=_BUFFER_SIZE * 2)
        # Seed the queue with the buffer BEFORE registering so concurrent
        # broadcasts don't double-deliver: items in the queue stay ordered
        # (replay first, then live).
        for event in list(self._buffer):
            try:
                q.put_nowait(event)
            except asyncio.QueueFull:
                break
        self._subscribers[sid] = q
        return sid, q

    async def iterate_subscriber(
        self, sid: str, q: "asyncio.Queue[Any]",
    ) -> AsyncIterator[Any]:
        """Drain a subscriber queue registered via ``register_subscriber``.

        Yields a ``_HEARTBEAT`` sentinel after each idle window of
        ``_HEARTBEAT_INTERVAL_S`` so the /events handler can emit an
        SSE comment line; otherwise nginx / cloudflare / browser
        EventSource close idle persistent connections between prompts.
        Cleans up the registration on exit.
        """
        try:
            while True:
                try:
                    event = await asyncio.wait_for(
                        q.get(), timeout=_HEARTBEAT_INTERVAL_S,
                    )
                except asyncio.TimeoutError:
                    yield _HEARTBEAT
                    continue
                if event is _END:
                    return
                yield event
        finally:
            self._subscribers.pop(sid, None)

    async def subscribe(self) -> AsyncIterator[Any]:
        """Convenience wrapper: register + iterate. Suits callers that
        don't need to start a producer mid-flight (e.g. ``GET /events``).

        For producer-driven flows, prefer the explicit two-step:
        ``sid, q = session.register_subscriber()``;
        ``task = asyncio.create_task(producer())``;
        ``async for item in session.iterate_subscriber(sid, q): ...``
        """
        sid, q = self.register_subscriber()
        async for item in self.iterate_subscriber(sid, q):
            yield item

    def _broadcast(self, event: Any) -> None:
        """Append to replay buffer + fan out to every active subscriber.
        Slow subscribers whose queue is full silently drop this event."""
        self._buffer.append(event)
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
