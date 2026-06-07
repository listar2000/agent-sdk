"""Abstract base class for SandboxSession — one per running compute.

Concrete provider classes (DaytonaSandboxSession, DockerSandboxSession,
UnixLocalSandboxSession, ModalSandboxSession) implement the 3 abstract
lifecycle methods (``start``/``running``/``stop``); ``execute_prompt``,
``destroy``, and ``shutdown`` are concrete in this base.

Decision: ``stop()`` and ``shutdown()`` are split. ``stop()`` is the
data-preserving operation (snapshot + pause compute). ``shutdown()`` is
the in-memory cleanup (cancel the credential-refresh task, drop
subscribers, close the ACP client). The pool calls both in sequence on
graceful release; a truly-dead session gets only ``shutdown()``.
"""
from __future__ import annotations

import abc
import asyncio
import contextlib
import logging
import uuid
from collections.abc import AsyncIterator
from typing import Any

from .liveness import Liveness
from .state import SandboxState

log = logging.getLogger(__name__)

# Sentinel placed on a subscriber's queue to signal end-of-stream.
_END = object()

# Sentinel yielded from ``subscribe()`` when no event has arrived for
# ``_HEARTBEAT_INTERVAL_S``. The /events handler renders these as SSE
# comment lines (``: heartbeat\n\n``) so intermediaries (nginx, CF,
# browser EventSource) don't close idle connections during quiet periods
# between prompts.
_HEARTBEAT = object()
_HEARTBEAT_INTERVAL_S = 20.0

# Per-subscriber queue capacity. Slow subscribers drop events rather than
# backpressuring the source supervisor stream.
_QUEUE_MAXSIZE = 2048


class _Subscriber:
    """A single fan-out subscriber: its bounded queue plus a pointer to the
    session whose ``_subscribers`` dict currently holds it.

    ``owner`` exists so the consumer's cleanup targets the *right* dict
    after a cold-recovery hand-off. ``iterate_subscriber``'s ``finally``
    pops via ``owner``; ``pool.get_session`` rebinds ``owner`` when it
    splices subscribers onto a replacement session. Without it the
    generator — whose ``self`` is permanently the original (now-dead)
    session — would pop ``sid`` from the original's already-cleared dict
    and leave a zombie entry on the replacement (a never-cleaned-up
    subscriber whose consumer has long since exited)."""

    __slots__ = ("queue", "owner")

    def __init__(
        self, queue: "asyncio.Queue[Any]", owner: "BaseSandboxSession",
    ) -> None:
        self.queue = queue
        self.owner = owner


class BaseSandboxSession(abc.ABC):
    """One session's running compute. Lifetime: from ``start()`` to
    ``shutdown()``. Owns provider-side handles, the per-session lock for
    serialised prompts, the subscriber fan-out for ``GET /events``, and
    the liveness oracle.

    Subclass contract: implement ``start()``, ``running()``, ``stop()``.
    The base class provides ``execute_prompt()``, ``destroy()``,
    ``shutdown()``, subscriber multiplex (``subscribe()``,
    ``_broadcast()``), the ``ProviderInstance`` builder, and the liveness
    oracle wiring.
    """

    # Provider-side discriminator: used by ``_bootstrap_session()`` to
    # validate the session's volume.provider matches what this concrete
    # class expects, AND as the ``provider`` field of every ProviderInstance
    # this session builds. Subclass overrides.
    volume_provider: str = ""
    # Default sandbox root when the recipe doesn't pin one. Subclass overrides
    # (daytona /home/daytona, docker /home/agent, modal /v, local /tmp).
    _default_root: str = "/tmp"

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
        # so that GET /events can stay open across N prompts. Subscribers
        # only receive events broadcast AFTER they register — historical
        # events live in ``session_log`` (GET /sessions/{id}/log).
        self._subscribers: dict[str, _Subscriber] = {}
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
        # Session-scoped ACP vendor-options forwarded as
        # ``_meta.<vendor>.options`` on session/new. Source-of-truth is
        # the sessions row; bootstrap copies it here so ``_attach_acp``
        # doesn't need to re-query the DB on cold-create / Type-2 paths.
        self._extra_options: dict | None = None
        # Cached AcpClient bound to this session's supervisor URL. Constructed
        # lazily on first use, reused across every acp_call / set_mode / etc.
        # so we don't re-handshake TCP+TLS on every notification. Closed in
        # shutdown(). The supervisor URL is set during start() and doesn't
        # change for the session's lifetime, so caching by URL is safe.
        from api.acp_client import AcpClient as _AcpClient  # type-only-ish
        self._acp_client_cls = _AcpClient
        self._acp_client: _AcpClient | None = None
        # Long-lived background task the session owns: the credential-refresh
        # loop (spawned by the pool when the recipe has a credential_refresh_url).
        # Cancelled by ``shutdown()`` so no teardown path can leak it.
        self._credential_refresh_task: asyncio.Task | None = None

    def _provider_instance(
        self,
        *,
        url: str = "",
        sandbox_ref: str | None = None,
        port: int | None = None,
    ) -> "ProviderInstance":
        """Build the ``ProviderInstance`` handle this session passes to its
        provider module (stop/destroy/reattach). Centralises the two fields
        every call needs — ``provider`` (the class discriminator) and ``root``
        (recipe root or the provider default) — which were otherwise rebuilt
        inline at every provider op."""
        from api.providers import ProviderInstance
        return ProviderInstance(
            provider=self.volume_provider,
            url=url,
            root=self.state.recipe.root or self._default_root,
            sandbox_ref=sandbox_ref,
            port=port,
        )

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
        # Subpath governs ACP HOME inside the sandbox. Three branches,
        # mutually exclusive, in priority order:
        #   1. ``workspaces/<name>`` — when the session was created with an
        #      explicit workspace, multiple agents share this dir as HOME.
        #      Server normalizes the name on insert, so we use it as-is.
        #   2. ``agents/<agent_id>`` — the default. Multiple sessions of the
        #      SAME agent share Claude's ~/.claude/projects/... JSONL store
        #      via this dir; that's what makes session/load find prior
        #      conversation. Per session_id would shard the JSONLs and
        #      break recovery.
        #   3. ``sessions/<id>`` — orphan fallback when an unregistered
        #      session somehow has no agent_id.
        ws = sess.get("workspace")
        if ws:
            self._subpath = f"workspaces/{ws}"
        elif self._agent_id:
            self._subpath = f"agents/{self._agent_id}"
        else:
            self._subpath = f"sessions/{self.session_id}"
        self._spawn_env = {
            **(sess.get("env") or {}),
            **(sess.get("secrets") or {}),
        }
        self._cwd = sess.get("cwd") or self.state.recipe.root or "/tmp"
        self._inner_session_id = (
            sess.get("inner_session_id") or self._inner_session_id
        )
        self._extra_options = sess.get("extra_options") or None
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

        # Read agent config BEFORE attach so we can thread
        # ``mcp_servers`` into ``session/new`` / ``session/load`` (ACP
        # rebuilds the MCP set from this param on every attach). Without
        # this read, ``agents.config.mcp_servers`` is dead — set on the
        # row but never wired into the runtime. POST /reload also
        # depends on this path picking up freshly-edited MCP entries.
        cfg = None
        if self._agent_id:
            try:
                agent = await _db.get_agent(self._agent_id)
                cfg = agent.config if agent else None
            except Exception:
                cfg = None
        mcp_servers = cfg.mcp_servers if cfg else None

        client = self._get_acp_client()
        await client.attach(
            self._acp_session_id,
            self.state.recipe.agent_type,
            cwd=self._cwd,
            inner_session_id=self._inner_session_id,
            mcp_servers=mcp_servers,
            extra_options=self._extra_options,
        )
        self._inner_session_id = client.get_inner_session_id(
            self._acp_session_id
        )
        # Re-apply persisted ACP dynamic config. ``cfg`` was already
        # read above for the MCP plumbing — reuse it rather than
        # re-fetching. Each set_* is bounded best-effort: a transient
        # failure on one shouldn't block the others (e.g. supervisor
        # accepts model but rejects an unknown thought_level — keep
        # the model change).
        if self._agent_id and self._inner_session_id:
            agent_type = self.state.recipe.agent_type
            # ``client.set_model`` takes (session_id, model, agent_type) —
            # without the third arg it defaults to ``"claude"`` and normalises
            # full opencode IDs like ``openrouter/anthropic/claude-3.5-haiku``
            # down to the substring ``"haiku"``, which opencode then parses
            # as ``providerID=haiku, modelID=""`` and rejects on every
            # subsequent prompt. Bind agent_type up front so each replay
            # entry below can stay one fn-call wide.
            def _bind_set_model(val: str):
                async def _do(sid: str, _val: str = val) -> None:
                    await client.set_model(sid, _val, agent_type)
                return _do
            replay = []
            if cfg:
                if cfg.model:
                    replay.append(("model", _bind_set_model(cfg.model), cfg.model))
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
    def inner_session_id(self) -> str | None:
        """Public read of the agent-native inner session id (used for
        ``session/load`` on cold-recovery)."""
        return self._inner_session_id

    def _get_acp_client(self) -> "AcpClient":  # noqa: F821
        """Lazily construct and cache the AcpClient bound to this session's
        supervisor URL. Reused across attach + every acp_call / set_*
        invocation so we don't pay TCP+TLS handshake on every notification.
        Closed in ``shutdown()``. Caller must ensure ``_supervisor_url``
        is set before calling — used internally where that's already true.

        If ``_supervisor_url`` has been re-bound (e.g. Daytona signed-URL
        refresh on reattach in ``_resolve_or_create_sandbox``), the stale
        cached client is dropped and a fresh one is built. We don't await
        ``aclose`` here because this is a sync helper — the loose httpx
        connections are closed by their finaliser; the next ``shutdown()``
        is the durable cleanup boundary.
        """
        assert self._supervisor_url is not None, (
            "_get_acp_client called before supervisor URL is set"
        )
        if self._acp_client is not None and self._acp_client.base_url != self._supervisor_url.rstrip("/"):
            self._acp_client = None
        if self._acp_client is None:
            self._acp_client = self._acp_client_cls(self._supervisor_url)
        # Mirror the inner session id mapping so methods that look it up
        # (set_mode, set_model, call) work even if the cached client was
        # constructed before the session was attached.
        if (
            self._acp_session_id is not None
            and self._inner_session_id is not None
        ):
            self._acp_client._inner_session_ids[self._acp_session_id] = (
                self._inner_session_id
            )
        return self._acp_client

    async def _aclose_acp_client(self) -> None:
        """Close the cached AcpClient if any. Safe to call multiple times.
        The base ``shutdown()`` calls this so the underlying httpx pool
        gets cleaned up alongside subscribers."""
        if self._acp_client is not None:
            try:
                await self._acp_client.aclose()
            except Exception:
                import logging
                logging.getLogger(__name__).warning(
                    "AcpClient.aclose failed for session %s",
                    self.session_id,
                    exc_info=True,
                )
            self._acp_client = None

    async def acp_call(
        self, method: str, params: dict | None = None, *, notify: bool = False,
    ) -> Any:
        """Forward a JSON-RPC call to this session's ACP supervisor.

        Reuses the cached ``AcpClient`` so repeated calls share the same
        keep-alive connection to the supervisor. Auto-injects the inner
        ``sessionId`` into ``params`` so callers don't have to track it.
        ``notify=True`` sends as a JSON-RPC notification (no response).

        Raises ``RuntimeError`` if the session has no live supervisor or
        no attached ACP session — the route handler maps that to 503.
        """
        if self._supervisor_url is None or self._acp_session_id is None:
            raise RuntimeError("session has no live ACP supervisor")
        client = self._get_acp_client()
        return await client.call(
            self._acp_session_id, method, params or {}, notify=notify,
        )

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

    async def execute_prompt(
        self, message: str, *, rpc_id: str | None = None,
    ) -> AsyncIterator[Any]:
        """Open an SSE stream from the supervisor for this one prompt;
        drain it; close it; broadcast each event to subscribers AND yield
        to the caller. Errors propagate as exceptions.

        If ``rpc_id`` is supplied, the JSON-RPC envelope sent to the
        supervisor uses it (so callers can correlate events to a tag
        they returned to the user). If None, a fresh uuid is generated.

        Provider-agnostic: every provider's compute runs the same
        ``supervisor.js``, which exposes the identical ``/v1/acp/{id}``
        endpoint over HTTP regardless of where it runs. The SSE drive is
        therefore implemented ONCE here; concrete subclasses only differ
        in how they bring that supervisor up (``start``) and tear it down
        (``stop``/``shutdown``).
        """
        import httpx

        from api.sse import _SSE_READ_TIMEOUT_S, parse_acp_event

        if self._supervisor_url is None or self._acp_session_id is None:
            raise RuntimeError(
                f"{type(self).__name__}.execute_prompt called before start()"
            )

        if rpc_id is None:
            rpc_id = str(uuid.uuid4())
        prompt_payload = {
            "jsonrpc": "2.0",
            "id": rpc_id,
            "method": "session/prompt",
            "params": {
                "sessionId": self._inner_session_id,
                "prompt": [{"type": "text", "text": message}],
            },
        }

        # SEPARATE httpx clients for the SSE GET and the session/prompt POST.
        # Sharing one client serialises both on the same keep-alive
        # connection and prematurely closes the SSE stream (~1.5s after the
        # POST lands) on httpx 0.27+.
        sse_client = httpx.AsyncClient(
            base_url=self._supervisor_url,
            timeout=httpx.Timeout(connect=10, read=None, write=10, pool=10),
        )
        post_client = httpx.AsyncClient(
            base_url=self._supervisor_url,
            timeout=httpx.Timeout(connect=10, read=_SSE_READ_TIMEOUT_S, write=10, pool=10),
        )
        try:
            async with sse_client.stream(
                "GET", f"/v1/acp/{self._acp_session_id}",
                headers={"Accept": "text/event-stream"},
            ) as sse:
                sse.raise_for_status()

                async def _send_prompt() -> None:
                    try:
                        await post_client.post(
                            f"/v1/acp/{self._acp_session_id}", json=prompt_payload,
                        )
                    except Exception:
                        log.exception("prompt POST failed for session %s", self.session_id)

                send_task = asyncio.create_task(_send_prompt())

                buf = ""
                try:
                    async for chunk in sse.aiter_text():
                        self.liveness.observe_chunk()
                        buf += chunk
                        while "\n\n" in buf:
                            block, buf = buf.split("\n\n", 1)
                            event = parse_acp_event(block, rpc_id)
                            if event is None:
                                continue
                            # rpc-tagged tuple so /events emits ``event: rpc:<id>``
                            # and the legacy test/UI ``extract_sse_tag`` can
                            # correlate per-prompt streams.
                            self._broadcast((rpc_id, block))
                            yield event
                            # Both ``done`` (clean stopReason) and ``error``
                            # (top-level JSON-RPC error envelope) signal that
                            # ACP is finished with this rpc_id and will write
                            # nothing else for it — stop iterating so the SSE
                            # stream closes promptly. Tool failures are
                            # ``session/update`` notifications (tool_result /
                            # update events), never ``type=="error"``, so this
                            # check can't end a turn the LLM is still recovering
                            # from.
                            if event.get("type") in ("done", "error"):
                                return
                finally:
                    if not send_task.done():
                        send_task.cancel()
                        try:
                            await send_task
                        except (asyncio.CancelledError, Exception):
                            pass
                    self.liveness.observe_close()
        finally:
            await sse_client.aclose()
            await post_client.aclose()

    @abc.abstractmethod
    async def stop(self) -> None:
        """Write FULL filesystem snapshot to volume (update
        ``state.snapshot_path`` and bump ``state.snapshot_version``);
        then call ``daytona.stop()`` (pause). Never deletes the sandbox
        — explicit deletion is only triggered from
        ``DELETE /sessions/{id}`` or admin paths. Persists state to
        the caller (the pool persists it to DB).

        NOTE the stop-vs-destroy split is provider-specific: daytona ``stop``
        PAUSES (resumable) and docker ``stop`` keeps the container, so for
        those ``stop != destroy``; modal/unix_local have no real pause, so
        their ``stop`` already terminates the compute (``stop == destroy``)."""

    async def destroy(self) -> None:
        """Hard-DELETE the compute — terminal, no resume (vs ``stop()``).
        Uniform across providers via the module's ``destroy_sandbox``: daytona
        deletes the paused VM, docker ``rm -f``s the (merely stopped)
        container, modal/unix_local terminate. Idempotent / no-op without a
        ``sandbox_ref``. Lets exceptions propagate — callers
        (``_safe_destroy_compute`` / the DELETE routes) wrap it."""
        ref = getattr(self.state, "sandbox_ref", None)
        if not ref:
            return
        from api.providers import destroy_instance
        await destroy_instance(self._provider_instance(
            url=self._supervisor_url or "", sandbox_ref=ref,
        ))

    async def shutdown(self) -> None:
        """Final teardown of in-memory tasks/state. Cancels the session's
        background tasks (the credential-refresh loop), drops the supervisor
        URL, and closes the subscribers + ACP client. Doesn't touch provider
        compute. Idempotent.

        Concrete in the base because every provider does the same in-memory
        cleanup; subclasses override ONLY to null provider-specific handles
        and must call ``await super().shutdown()``.
        """
        await self._cancel_background_tasks()
        self._supervisor_url = None
        self._close_subscribers()
        await self._aclose_acp_client()

    async def _cancel_background_tasks(self) -> None:
        """Cancel every long-lived task the session owns (today: the
        credential-refresh loop). Idempotent; never raises. Centralising it
        in ``shutdown()`` means no teardown path can forget it — every path
        ends in ``shutdown()``."""
        task = self._credential_refresh_task
        if task is not None:
            self._credential_refresh_task = None
            task.cancel()
            with contextlib.suppress(BaseException):
                await task

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
        """Invoke ``method_name(self._acp_session_id, *args)`` via the
        cached ``AcpClient``. Used by the wrapper methods below so each
        one stays a one-liner.

        No-op (silently) if the session has no live supervisor URL or
        no attached ACP session — same shape as ``cancel_active_prompt``."""
        if self._supervisor_url is None or self._acp_session_id is None:
            return
        client = self._get_acp_client()
        await getattr(client, method_name)(self._acp_session_id, *args)

    async def set_mode(self, mode: str) -> None:
        await self._acp_call("set_mode", mode)

    async def set_model(self, model: str) -> None:
        await self._acp_call("set_model", model, self.state.recipe.agent_type)

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
        q: asyncio.Queue[Any] = asyncio.Queue(maxsize=_QUEUE_MAXSIZE)
        self._subscribers[sid] = _Subscriber(q, self)
        self.liveness.observe_activity()
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
        # Capture the subscriber record now — synchronously, before the
        # first ``await`` — so cleanup pops from whichever session owns it
        # at exit, not the one this generator was bound to. During mid-
        # prompt cold-recovery the pool hands this queue off to a
        # replacement session and rebinds ``owner``; popping from ``self``
        # (the original, now-dead session) would miss the replacement and
        # leak a never-cleaned-up zombie subscriber entry. See
        # ``_Subscriber``.
        sub = self._subscribers.get(sid)
        try:
            while True:
                try:
                    event = await asyncio.wait_for(
                        q.get(), timeout=_HEARTBEAT_INTERVAL_S,
                    )
                except asyncio.TimeoutError:
                    self.liveness.observe_activity()
                    yield _HEARTBEAT
                    continue
                if event is _END:
                    return
                self.liveness.observe_activity()
                yield event
        finally:
            owner = sub.owner if sub is not None else self
            owner._subscribers.pop(sid, None)

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
        """Fan out to every active subscriber. Slow subscribers whose
        queue is full silently drop this event.

        Live-only: events are not buffered for late subscribers. Callers
        that need historical events read GET /sessions/{id}/log; that
        endpoint is the source of truth for everything broadcast on this
        session. Mixing the two would double-deliver every event a
        cold-loading UI just fetched from /log."""
        for sub in list(self._subscribers.values()):
            try:
                sub.queue.put_nowait(event)
            except asyncio.QueueFull:
                # Slow subscriber: drop. They'll catch up on whatever's next.
                pass

    def _close_subscribers(self) -> None:
        """Signal end-of-stream to every subscriber. Called from
        ``shutdown()`` so pending ``subscribe()`` consumers exit."""
        for sub in list(self._subscribers.values()):
            q = sub.queue
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
