"""SessionPool — the entire recovery surface, in one method.

See ````. Replaced the legacy
recovery chain (``_ensure_sandbox_alive`` / ``_type1_recover`` /
``_type2_recover`` / ``_rebind_state``) plus the in-memory
``_INSTANCES`` and ``SESSIONS`` registries plus the ``_session_locks``
dict plus the ``is_hibernated`` flag — all gone, all replaced by this
one class.

At-most-one active SandboxSession per session_id. Concurrent
``get_session`` calls for the same session_id serialise on
``_locks[session_id]`` so we never end up with two leases for the same
session.
"""
from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import shlex
import time
from collections.abc import Callable

import httpx

from api import db

from .session import BaseSandboxSession, _Subscriber
from .state import Recipe, SandboxState, deserialize, serialize, state_for_provider

log = logging.getLogger(__name__)


class SessionNotFoundError(LookupError):
    """Raised by ``SessionPool.get_session`` when the ``sessions`` row is
    gone (deleted, swept, or never existed). Distinguishes "this session
    has been removed" from "we can't reach the supervisor right now" so
    the HTTP layer can return 404 instead of 500 — without that, every
    UI EventSource pointed at a deleted session_id retries every 2s
    forever and the supervisor's RuntimeError stack ends up in Railway
    logs on each tick.
    """


# Type for the factory that turns a session_id + deserialised state into
# the appropriate concrete SandboxSession subclass. Phase 2 exposes a
# default implementation in factory.py keyed on state.type.
SessionFactory = Callable[[str, SandboxState], BaseSandboxSession]


# Per-worker lease tuning. One heartbeat per process, not per session.
# The worker keeps a single row in the ``workers`` table; sessions point
# at it via ``sessions.owner_id``. A session is "owned" iff its
# owner_id matches a worker whose lease_expires_at is in the future.
#
# Beat at 25s, expire at 60s. 2.4:1 ratio — one missed beat is fine,
# two means the worker is probably wedged and a peer should take over.
# Compared to the previous per-session 8:1 (15s/120s), the worker-level
# heartbeat doesn't need to survive provider cold-creates because the
# worker is alive enough to run cold-create iff it's alive enough to
# heartbeat. Override both at boot via env if the deployment needs it.
_WORKER_HEARTBEAT_S = float(os.environ.get("AGENT_SDK_WORKER_HEARTBEAT_S", "25"))
_WORKER_TTL_S = float(os.environ.get("AGENT_SDK_WORKER_TTL_S", "60"))


class SessionPool:
    """Holds at-most-one active SandboxSession per session_id.

    The single ``get_session`` method handles every recovery scenario
    today's four ``_ensure_*`` functions used to handle:
      * session was hibernated → start fresh (or resume from snapshot)
      * cached session is dead → tear down + start fresh
      * cached session is alive → return immediately (warm path, ~10ms)
      * server just restarted → no cached → load state from DB → start

    No "Type 1 vs Type 2" decision lives here — that's internal to
    ``SandboxSession.start()``.
    """

    def __init__(self, *, factory: SessionFactory) -> None:
        self._factory = factory
        self._active: dict[str, BaseSandboxSession] = {}
        self._locks: dict[str, asyncio.Lock] = {}
        # Lazy import — keeps test fixtures that don't go through the
        # FastAPI lifespan from blowing up on the identity module's
        # import-time env reads.
        from api.identity import owner_addr, owner_id
        self._owner_id = owner_id()
        self._owner_addr = owner_addr()

    def _lock(self, session_id: str) -> asyncio.Lock:
        lock = self._locks.get(session_id)
        if lock is None:
            lock = self._locks.setdefault(session_id, asyncio.Lock())
        return lock

    async def _publish_state(self) -> None:
        """Snapshot our in-memory pool's session_ids to the ``workers``
        row + push lease_expires_at forward. Called on every mutation
        of ``_active`` (so the dashboard JOIN is always within a few
        ms of reality) and periodically from the worker heartbeat task
        in ``runtime.py`` (so a long-idle worker still proves it's
        alive). Best-effort — a transient DB hiccup just delays the
        dashboard view; the next mutation or heartbeat retries.

        Disabled via ``AGENT_SDK_DISABLE_LEASE=1`` for unit-test
        fixtures that don't have the ``workers`` table provisioned.
        """
        if os.environ.get("AGENT_SDK_DISABLE_LEASE") == "1":
            return
        try:
            await db.update_worker_state(
                owner_id=self._owner_id,
                owner_addr=self._owner_addr,
                ttl_seconds=_WORKER_TTL_S,
                session_ids=list(self._active.keys()),
            )
        except Exception:
            log.warning("pool: _publish_state failed", exc_info=True)

    async def get_session(
        self,
        session_id: str,
        *,
        initial_state: SandboxState | None = None,
        peek: bool = False,
    ) -> BaseSandboxSession:
        """Returns a known-alive SandboxSession. The single recovery
        entry point. Per docs §6.

        ``initial_state`` is the explicit input channel for cold-create:
        callers that just inserted a session row pass the recipe directly
        instead of pre-writing the ``sandbox_state`` column. For recovery
        (server restart, hibernated session resume) ``initial_state``
        stays ``None`` and the column is the source of truth.

        ``peek=True`` opts out of cold-recovery: if the session is in
        the live cache, return it as usual; if not, raise ``KeyError``
        instead of provisioning a fresh sandbox. Used by read-only
        endpoints (``GET /sessions/{id}/status``, ``/sandbox``,
        ``/admin/sessions``) so a UI status poll on a hibernated session
        doesn't accidentally unhibernate it. Caller should fall back to
        a DB read of the persisted ``sandbox_state`` JSONB.

        Holds ``_locks[session_id]`` for the entire decide-and-start
        sequence so concurrent callers can't double-provision.
        """
        async with self._lock(session_id):
            cached = self._active.get(session_id)
            handed_off_subscribers: dict[str, _Subscriber] = {}
            if cached is not None:
                # Force-probe so an externally-killed supervisor is detected
                # immediately, even if the previous prompt's last chunk was
                # observed seconds ago (the test 7 race class).
                alive = await cached.running(force_probe=True)
                log.info("[pool.get_session] session=%s cached=True alive=%s peek=%s", session_id, alive, peek)
                if alive:
                    cached.liveness.observe_activity()
                    return cached
                # Not alive. In peek mode, don't tear down or replace —
                # caller wants a snapshot of state, not a side-effect.
                if peek:
                    raise KeyError(f"session {session_id} not in live pool (peek=True)")
                # Stale entry; tear down runtime in background. We don't
                # snapshot here — compute is dead, can't snapshot reliably.
                # The previous successful per-turn snapshot is the fallback.
                #
                # Transfer subscriber queues to the replacement session
                # *before* shutdown so any /events SSE consumers attached
                # to the stale session keep streaming across the recovery
                # — without this hand-off, ``_close_subscribers`` in
                # ``shutdown()`` puts ``_END`` on every queue and the
                # user's chat connection dies mid-recovery (the data-
                # research / Task Builder silent-failure repro). Clearing
                # the dict on the cached session makes ``_close_subscribers``
                # a no-op so subscribers see no spurious _END.
                handed_off_subscribers = dict(cached._subscribers)
                cached._subscribers.clear()
                asyncio.create_task(_safe_shutdown(cached))
                self._active.pop(session_id, None)

            if peek:
                # No cached entry → don't cold-recover; caller will read
                # from DB.
                raise KeyError(f"session {session_id} not in live pool (peek=True)")

            # No per-session ownership claim. We trust the LB's
            # consistent-hash routing: if this request landed on us,
            # we're the right replica. The narrow split-brain window
            # at LB rebalance is the cost of dropping the lease (see
            # README §scaling for the volume-flock follow-up).

            if initial_state is not None:
                state: SandboxState = initial_state
            else:
                # Confirm the session row still exists before cold-recovering.
                # Without this, a stale UI EventSource pointed at a deleted
                # session_id drives ``read_sandbox_state -> None ->
                # deserialize -> state.type=unknown`` and then ``start()``
                # raises ``RuntimeError("session ... not found in DB")``
                # which surfaces as 500. The browser's auto-reconnect loop
                # then hammers /events every 2s. Make the gone state
                # explicit so the HTTP layer can translate to 404.
                if await db.get_session(session_id) is None:
                    raise SessionNotFoundError(session_id)
                state = deserialize(await db.read_sandbox_state(session_id))
            session = self._factory(session_id, state)
            log.info("[pool.get_session] session=%s creating new state.type=%s sandbox_ref=%s",
                     session_id, getattr(state, "type", "?"), getattr(state, "sandbox_ref", None))
            if handed_off_subscribers:
                # Splice the prior session's subscribers onto the new one
                # so its broadcasts (including any error frame from a
                # ``execute_prompt`` failure) reach the existing /events
                # consumers. Done before ``start()`` so the first chunk
                # observed by the supervisor goes to the right queues.
                #
                # Rebind each subscriber's ``owner`` to this replacement
                # session so the consumer's ``iterate_subscriber`` finally
                # pops from HERE, not from the dead session it was created
                # on. Without this the entry leaks onto ``session`` and the
                # idle reaper skips it forever (zombie subscriber).
                for sub in handed_off_subscribers.values():
                    sub.owner = session
                session._subscribers.update(handed_off_subscribers)
            try:
                await session.start()
            except BaseException:
                # Bug A fix — fail-safe compute release. ``start()`` may have
                # already acquired the sandbox (create_sandbox succeeded)
                # before failing at a later stage (health probe / ACP attach /
                # session/new -32603). The session never enters ``_active``,
                # so nothing else will ever release it: without this teardown
                # the SLURM job / daytona VM / container leaks until its own
                # time limit. Tear it down here, then re-raise for the caller
                # to translate (HTTP 5xx).
                #
                # Call ``stop()`` + ``_safe_shutdown`` DIRECTLY, never
                # ``release()`` — release() re-acquires ``self._lock(session_id)``
                # which we still hold here, so it would deadlock.
                with contextlib.suppress(Exception):
                    await session.stop()
                await _safe_shutdown(session)
                raise
            await db.write_sandbox_state(session_id, serialize(session.state))
            self._active[session_id] = session
            # Publish the updated session_ids snapshot so the dashboard
            # sees this session as "active" without waiting for the
            # 25s heartbeat tick. Best-effort; the periodic heartbeat
            # in runtime.py would catch it within one tick anyway.
            await self._publish_state()
            # Spawn the credential-refresh loop if the recipe asks for
            # one. Fires on every wake — cold create AND resume from
            # hibernation — so the agent always has fresh credentials.
            # Cancelled in ``release()`` before shutdown.
            recipe = session.state.recipe
            if recipe.credential_refresh_url:
                session._credential_refresh_task = asyncio.create_task(
                    _credential_refresh_loop(
                        session_id,
                        url=recipe.credential_refresh_url,
                        bearer=recipe.credential_refresh_token or "",
                        get_supervisor_url=lambda s=session: s.supervisor_url,
                    )
                )
            # Fire lifecycle webhook so the orchestrator knows the
            # sandbox is now alive.  URL is derived from the
            # credential_refresh_url already on the recipe.
            lifecycle_url = _lifecycle_url_from_recipe(session.state.recipe)
            if lifecycle_url:
                asyncio.create_task(_fire_lifecycle_webhook(lifecycle_url, session_id, "started"))
            return session

    async def cold_create(
        self,
        session_id: str,
        *,
        provider: str,
        recipe: Recipe,
    ) -> BaseSandboxSession:
        """Cold-start a session for which the row was just inserted.

        Constructs the per-provider initial SandboxState from ``recipe``
        and delegates to ``get_session`` so the same lock + cache logic
        runs. For the recovery path (server restart, hibernated session
        resume) call ``get_session`` directly — that reads state from
        the DB.

        Raises ``ValueError`` for an unknown provider name (caller should
        map to HTTP 400). Any provider-side provisioning failure
        propagates from ``get_session`` for the caller to translate.
        """
        initial_state = state_for_provider(provider, recipe)
        return await self.get_session(session_id, initial_state=initial_state)

    async def release(self, session_id: str) -> None:
        """Hibernate: snapshot + drop compute. Idempotent.

        Triggered by the reaper or explicit ``POST /sessions/{id}/release``.
        """
        async with self._lock(session_id):
            session = self._active.pop(session_id, None)
            if session is None:
                return
            lifecycle_url = _lifecycle_url_from_recipe(session.state.recipe)
            # Cancel the credential-refresh loop (if any) before tearing
            # down compute. Suppress exceptions on await — the task may
            # have already crashed; we just want it gone.
            task = getattr(session, "_credential_refresh_task", None)
            if task is not None:
                task.cancel()
                with contextlib.suppress(BaseException):
                    await task
            try:
                try:
                    await session.stop()
                    await db.write_sandbox_state(session_id, serialize(session.state))
                except Exception:
                    log.exception(
                        "session.stop() failed for %s; proceeding with shutdown",
                        session_id,
                    )
            finally:
                await _safe_shutdown(session)
                # Publish the updated session_ids snapshot so the
                # dashboard drops this session out of the active list
                # right away (vs waiting up to one heartbeat tick).
                await self._publish_state()
            # Fire lifecycle webhook so the orchestrator knows the
            # sandbox is now hibernated.
            if lifecycle_url:
                asyncio.create_task(_fire_lifecycle_webhook(lifecycle_url, session_id, "hibernated"))

    def _should_reap(
        self,
        sess,
        idle_s: float,
        now: float,
        provider_idle_s: dict[str, float] | None = None,
    ) -> tuple[bool, str]:
        """The SINGLE idle-decision for one active session.

        Shared by the background ``reap_idle`` scan and the per-session
        ``reap_session`` (POST /sessions/{id}/reap) so both agree — and so
        the idle policy lives in exactly one place. Returns
        ``(should_hibernate, reason)``.

        De-conflation of the two lifecycles (the fix for the subscriber-pin
        leak): the decision keys off COMPUTE activity only —
        ``Liveness._last_compute_at`` (prompt chunks + successful health
        probes) plus an ``in_flight`` gate for chunk-silent long turns.
        Subscriber presence is NO LONGER consulted: an open /events
        consumer (dashboard, monitor, idle chat UI) marks the session
        ``alive`` for the re-probe path via ``observe_activity`` but does
        not advance the compute clock, so it can no longer pin an idle
        sandbox. A reaped session keeps its conversation (session/load) and
        cold-resumes on the next message; an SSE consumer reconnects.
        """
        # Never hibernate mid-prompt — even a multi-minute, chunk-silent
        # tool call whose compute clock has gone stale.
        if sess.liveness.in_flight:
            return False, "prompt_in_flight"
        last = sess.liveness._last_compute_at
        if last is None:
            # No compute observed yet — likely a session that started
            # but hasn't had a prompt yet. Seed the idle window from now.
            sess.liveness._last_compute_at = now
            return False, "no_activity_yet"
        provider = getattr(sess.state, "type", "")
        limit = (provider_idle_s or {}).get(provider, idle_s)
        if (now - last) > limit:
            return True, "idle"
        return False, "recent_activity"

    async def reap_idle(
        self,
        idle_s: float,
        *,
        provider_idle_s: dict[str, float] | None = None,
    ) -> int:
        """Hibernate every active session that ``_should_reap`` flags as
        idle. Returns the count of sessions released.
        """
        import time as _time
        now = _time.monotonic()
        stale = [
            sid for sid, sess in list(self._active.items())
            if self._should_reap(sess, idle_s, now, provider_idle_s)[0]
        ]
        for sid in stale:
            try:
                await self.release(sid)
            except Exception:
                log.exception("reap_idle: release(%s) failed", sid)
        return len(stale)

    async def reap_session(
        self,
        session_id: str,
        idle_s: float,
        *,
        provider_idle_s: dict[str, float] | None = None,
    ) -> dict:
        """Hibernate ONE session iff it meets the same idle criteria the
        background reaper uses (``_should_reap``).

        Exposed via ``POST /sessions/{id}/reap`` for ops ("reclaim this
        idle sandbox now without waiting for the reaper tick") and as the
        deterministic, per-session, ``-n auto``-safe seam the golden
        reaper test drives. Returns ``{hibernated, reason}``. A session not
        active on THIS replica returns ``reason='not_active'`` (the
        consistent-hash routing should land the call on the owner).
        """
        import time as _time
        now = _time.monotonic()
        sess = self._active.get(session_id)
        if sess is None:
            return {"hibernated": False, "reason": "not_active"}
        should, reason = self._should_reap(sess, idle_s, now, provider_idle_s)
        if not should:
            return {"hibernated": False, "reason": reason}
        await self.release(session_id)
        return {"hibernated": True, "reason": "idle"}

    def find_by_sandbox_ref(self, sandbox_ref: str) -> BaseSandboxSession | None:
        """Reverse lookup: find an active session whose underlying compute
        carries this provider sandbox ref. Used by reverse-lookup callers
        — sandbox identity isn't durable, but if the compute is currently
        running we know who owns it."""
        for sess in self._active.values():
            if getattr(sess.state, "sandbox_ref", None) == sandbox_ref:
                return sess
        return None

    def find_by_agent_id(self, agent_id: str) -> list[BaseSandboxSession]:
        """All currently-active sessions belonging to ``agent_id``.

        Reads ``sess._agent_id`` which is populated during
        ``_bootstrap_session`` (i.e. set for every session in ``_active``
        — entries here have already gone through ``start()``).

        Used by the multi-session create path to enforce the Daytona
        constraint "at most one live sibling per agent on Daytona": the
        S3-FUSE mount of a fresh sandbox doesn't see writes that haven't
        been flushed by an existing sibling's mount. Caller can decide
        whether to 409 or evict the existing sibling first.
        """
        return [
            sess for sess in self._active.values()
            if getattr(sess, "_agent_id", None) == agent_id
        ]

    async def shutdown_all(self, *, per_session_timeout_s: float = 10.0) -> None:
        """Stop the world: snapshot + shutdown every active session.

        Used at server-graceful-shutdown. Releases run in parallel and
        each is bounded by ``per_session_timeout_s`` so one hung provider
        (Daytona signed-URL 502, docker daemon stalled) can't block the
        whole shutdown. A timed-out release is logged and dropped — the
        in-memory session is still removed via ``_active.pop`` inside
        ``release``, so the next start cleanly cold-recovers.
        """
        async def _bounded(sid: str) -> None:
            try:
                await asyncio.wait_for(
                    self.release(sid), timeout=per_session_timeout_s,
                )
            except asyncio.TimeoutError:
                log.warning(
                    "shutdown_all: release(%s) exceeded %.1fs, dropping",
                    sid, per_session_timeout_s,
                )
                # release acquired the lock but didn't finish; pop the
                # active entry so a cold recovery doesn't see the stale
                # session object on next get_session.
                self._active.pop(sid, None)

        await asyncio.gather(
            *(_bounded(sid) for sid in list(self._active.keys())),
            return_exceptions=True,
        )


async def _safe_shutdown(session: BaseSandboxSession) -> None:
    try:
        await session.shutdown()
    except Exception:
        log.exception("shutdown() failed for session %s", session.session_id)


# ────────────────────────── credential refresh ──────────────────────────


async def _credential_refresh_loop(
    session_id: str,
    *,
    url: str,
    bearer: str,
    get_supervisor_url: Callable[[], str | None],
) -> None:
    """Poll ``url`` and write the returned files into the session sandbox
    until cancelled. One task per active session, spawned in
    ``get_session`` and cancelled in ``release``.

    Caller's endpoint must return JSON of the form::

        {"contents": {"<abs-path>": "<base64>", ...},
         "next_refresh_at": <unix-ts>}

    ``contents`` may be empty (no-op tick); ``next_refresh_at`` is
    advisory. Sleep delay is clamped to [60s, 1h] so a buggy response
    can't tight-loop or hang forever.
    """
    log.info("[credential-refresh] session=%s starting", session_id)
    while True:
        try:
            async with httpx.AsyncClient(timeout=15) as client:
                r = await client.post(
                    url, headers={"Authorization": f"Bearer {bearer}"},
                )
                r.raise_for_status()
                payload = r.json()
        except asyncio.CancelledError:
            raise
        except Exception:
            log.warning(
                "[credential-refresh] session=%s fetch failed", session_id,
                exc_info=True,
            )
            await asyncio.sleep(60)
            continue
        contents = payload.get("contents") or {}
        next_at = float(payload.get("next_refresh_at") or 0)
        sup_url = get_supervisor_url()
        if sup_url and contents:
            try:
                await _write_credentials_via_supervisor(sup_url, contents)
            except asyncio.CancelledError:
                raise
            except Exception:
                log.warning(
                    "[credential-refresh] session=%s write failed",
                    session_id, exc_info=True,
                )
                await asyncio.sleep(60)
                continue
        delay = max(60.0, min(3600.0, next_at - time.time()))
        await asyncio.sleep(delay)


async def _write_credentials_via_supervisor(
    supervisor_url: str, contents: dict[str, str],
) -> None:
    """Write each {abs_path: base64_content} into the sandbox atomically
    using the supervisor's /v1/exec channel. base64 carries arbitrary
    bytes safely across the HTTP boundary; tmp + chmod + rename keeps
    in-flight readers from seeing partial files."""
    async with httpx.AsyncClient(timeout=15) as client:
        for path, b64 in contents.items():
            q_path = shlex.quote(path)
            q_b64 = shlex.quote(b64)
            cmd = (
                f"set -e; dir=$(dirname {q_path}); mkdir -p \"$dir\"; "
                f"tmp=$(mktemp \"$dir/.creds.XXXXXX\"); "
                f"printf %s {q_b64} | base64 -d > \"$tmp\"; "
                f"chmod 600 \"$tmp\"; "
                f"mv \"$tmp\" {q_path}"
            )
            r = await client.post(
                f"{supervisor_url}/v1/exec",
                json={"command": cmd, "timeout": 10},
            )
            r.raise_for_status()


def _lifecycle_url_from_recipe(recipe: Recipe) -> str | None:
    """Derive the lifecycle webhook URL from the credential_refresh_url.

    ``credential_refresh_url`` looks like:
        ``https://hivespace/api/internal/agents/{id}/credentials/refresh``

    We strip the per-agent suffix and replace with the lifecycle path:
        ``https://hivespace/api/internal/session-lifecycle``
    """
    url = recipe.credential_refresh_url
    if not url:
        return None
    marker = "/api/internal/agents/"
    idx = url.find(marker)
    if idx < 0:
        return None
    return url[:idx] + "/api/internal/session-lifecycle"


async def _fire_lifecycle_webhook(url: str, session_id: str, event: str) -> None:
    """POST a session lifecycle event to the orchestrator.
    Fire-and-forget — failures are logged but never block the caller."""
    try:
        async with httpx.AsyncClient(timeout=5) as client:
            await client.post(url, json={"session_id": session_id, "event": event})
    except Exception:
        log.debug("lifecycle webhook failed for session=%s event=%s",
                  session_id, event, exc_info=True)
