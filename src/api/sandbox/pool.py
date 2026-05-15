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

from .session import BaseSandboxSession
from .state import Recipe, SandboxState, deserialize, serialize, state_for_provider

log = logging.getLogger(__name__)


# Type for the factory that turns a session_id + deserialised state into
# the appropriate concrete SandboxSession subclass. Phase 2 exposes a
# default implementation in factory.py keyed on state.type.
SessionFactory = Callable[[str, SandboxState], BaseSandboxSession]


# Lease tuning. TTL/heartbeat ratio must give enough headroom for the
# heartbeat to survive a long-running event-loop hop without losing the
# lease. The realistic worst case is a provider cold-create
# (``daytona.create`` / ``modal.Sandbox.create``) which can hold the
# event loop for 30-60s under -n auto bench load even though the call
# itself runs in a thread — the wall time for the surrounding
# ``pool.get_session`` lock + the ACP attach round-trip blocks the
# heartbeat task from being scheduled.
#
# With TTL=30 / HB=10 (3:1), missing 2 consecutive heartbeats expired
# the lease and a peer replica claimed mid-recovery — exactly the
# split-brain we saw under modal cold-create. TTL=120 / HB=15 gives
# 8:1 headroom; the worst observed cold-recovery is ~60s so we're at
# 2x that with a comfortable margin. Crashed-owner takeover still
# completes within the TTL (target: <2 min after dead replica).
_LEASE_TTL_S = float(os.environ.get("AGENT_SDK_LEASE_TTL_S", "120"))
_LEASE_HEARTBEAT_S = float(os.environ.get("AGENT_SDK_LEASE_HEARTBEAT_S", "15"))


class NotOwner(Exception):
    """Raised by ``SessionPool.get_session`` when another replica holds
    an unexpired lease on the session. The 307 handler in ``server.py``
    consumes ``owner_id`` (replica id, used as the routing cookie value)
    and ``owner_addr`` (the host:port, for diagnostics). If
    ``owner_addr`` is empty (lease row gone), the caller should map to
    503."""

    def __init__(self, session_id: str, owner_addr: str = "", owner_id: str = "") -> None:
        super().__init__(f"session {session_id} owned by {owner_id or owner_addr or '<unknown>'}")
        self.session_id = session_id
        self.owner_addr = owner_addr
        self.owner_id = owner_id


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
        # One renewal task per session_id we currently own. Stopped in
        # ``release``/``shutdown_all`` BEFORE the lease itself is dropped
        # so the heartbeat can't reacquire mid-teardown.
        self._heartbeat_tasks: dict[str, asyncio.Task] = {}
        # Lazy import — keeps test fixtures that don't go through the
        # FastAPI lifespan from blowing up on the identity module's
        # import-time env reads (they happen at module load, which is
        # fine, but explicit lazy keeps the dependency obvious).
        from api.identity import owner_addr, owner_id
        self._owner_id = owner_id()
        self._owner_addr = owner_addr()

    def _lock(self, session_id: str) -> asyncio.Lock:
        lock = self._locks.get(session_id)
        if lock is None:
            lock = self._locks.setdefault(session_id, asyncio.Lock())
        return lock

    async def _claim_lease_or_raise(self, session_id: str) -> int:
        """Try to claim/renew the lease. Returns ``lease_generation`` on
        success; raises ``NotOwner`` when another live owner holds it.

        Disabling: setting ``AGENT_SDK_DISABLE_LEASE=1`` bypasses the
        claim entirely. Useful for unit tests that exercise the pool
        without a real DB row backing every session.
        """
        if os.environ.get("AGENT_SDK_DISABLE_LEASE") == "1":
            return 0
        claim = await db.try_claim_lease(
            session_id,
            owner_id=self._owner_id,
            owner_addr=self._owner_addr,
            ttl_seconds=_LEASE_TTL_S,
        )
        if claim is not None and claim["lease_owner_id"] == self._owner_id:
            return int(claim["lease_generation"])
        # We didn't get it. Find out who did so the caller can 307.
        current = claim if claim is not None else await db.read_lease(session_id)
        owner_addr = (current or {}).get("lease_owner_addr") or ""
        owner_id = (current or {}).get("lease_owner_id") or ""
        raise NotOwner(session_id=session_id, owner_addr=owner_addr, owner_id=owner_id)

    async def _heartbeat_loop(self, session_id: str) -> None:
        """Renew the lease every ``_LEASE_HEARTBEAT_S``. If we ever lose
        ownership (another replica reclaimed after we missed a beat
        because of an event-loop stall, etc.), drop the in-memory session
        instead of continuing to drive a sandbox we don't own."""
        while True:
            try:
                await asyncio.sleep(_LEASE_HEARTBEAT_S)
            except asyncio.CancelledError:
                return
            try:
                claim = await db.try_claim_lease(
                    session_id,
                    owner_id=self._owner_id,
                    owner_addr=self._owner_addr,
                    ttl_seconds=_LEASE_TTL_S,
                )
            except Exception:
                log.exception("heartbeat: renewal failed for %s; retrying", session_id)
                continue
            if claim is None or claim["lease_owner_id"] != self._owner_id:
                # We lost the lease — somebody else reclaimed it after
                # a stall longer than the TTL. Surrender the in-memory
                # state so the rightful owner can drive the sandbox.
                log.warning(
                    "heartbeat: lost lease for %s (claim=%r); surrendering",
                    session_id, claim,
                )
                cached = self._active.pop(session_id, None)
                self._heartbeat_tasks.pop(session_id, None)
                if cached is not None:
                    asyncio.create_task(_safe_shutdown(cached))
                return

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
            handed_off_subscribers: dict[str, asyncio.Queue] = {}
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
                # Stop the heartbeat for the dead session; the replacement
                # path below will start a fresh one after the new claim.
                hb = self._heartbeat_tasks.pop(session_id, None)
                if hb is not None and not hb.done():
                    hb.cancel()

            if peek:
                # No cached entry → don't cold-recover; caller will read
                # from DB.
                raise KeyError(f"session {session_id} not in live pool (peek=True)")

            # Claim the cross-replica lease BEFORE we touch any sandbox
            # state. NotOwner propagates up to the FastAPI handler which
            # emits a 307 to ``owner_addr``.
            await self._claim_lease_or_raise(session_id)

            if initial_state is not None:
                state: SandboxState = initial_state
            else:
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
                session._subscribers.update(handed_off_subscribers)
            await session.start()
            await db.write_sandbox_state(session_id, serialize(session.state))
            self._active[session_id] = session
            # Start the lease renewal task. One per active session; cancelled
            # in release() / shutdown_all() before the lease row is cleared.
            if os.environ.get("AGENT_SDK_DISABLE_LEASE") != "1":
                self._heartbeat_tasks[session_id] = asyncio.create_task(
                    self._heartbeat_loop(session_id),
                    name=f"lease-heartbeat-{session_id[:8]}",
                )
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
            # Stop renewal first so an in-flight heartbeat doesn't extend
            # the TTL between our shutdown and our lease release.
            hb = self._heartbeat_tasks.pop(session_id, None)
            if hb is not None and not hb.done():
                hb.cancel()
                try:
                    await hb
                except (asyncio.CancelledError, Exception):
                    pass
            session = self._active.pop(session_id, None)
            if session is None:
                # Even with no in-memory session, attempt to clear any
                # lease we somehow still hold (e.g. partial start that
                # never landed in ``_active``).
                try:
                    await db.release_lease(session_id, owner_id=self._owner_id)
                except Exception:
                    log.warning(
                        "release: db.release_lease(%s) failed", session_id,
                        exc_info=True,
                    )
                return
            # Cancel the credential-refresh loop (if any) before tearing
            # down compute. Suppress exceptions on await — the task may
            # have already crashed; we just want it gone.
            task = getattr(session, "_credential_refresh_task", None)
            if task is not None:
                task.cancel()
                with contextlib.suppress(Exception):
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
                try:
                    await db.release_lease(session_id, owner_id=self._owner_id)
                except Exception:
                    log.warning(
                        "release: db.release_lease(%s) failed", session_id,
                        exc_info=True,
                    )

    def has_active(self, session_id: str) -> bool:
        """For derived UI/admin info ('lifecycle: active|hibernated').
        No I/O — just whether the pool currently holds a session."""
        return session_id in self._active

    async def reap_idle(
        self,
        idle_s: float,
        *,
        provider_idle_s: dict[str, float] | None = None,
    ) -> int:
        """Hibernate every active session whose last observed activity is
        older than ``idle_s``. Returns the count of sessions released.

        Activity = the session's ``Liveness._last_chunk_at``. Prompt
        chunks, successful health probes, file/status traffic, and live
        /events subscribers all count as activity so an open UI does not
        hibernate underneath the user.
        """
        import time as _time
        now = _time.monotonic()
        stale = []
        for sid, sess in list(self._active.items()):
            if sess._subscribers:
                sess.liveness.observe_activity()
                continue
            last = sess.liveness._last_chunk_at
            if last is None:
                # Never observed a chunk — likely a session that started
                # but hasn't had a prompt yet. Use creation as a proxy
                # by giving it the full idle window from now.
                sess.liveness._last_chunk_at = now
                continue
            provider = getattr(sess.state, "type", "")
            limit = (provider_idle_s or {}).get(provider, idle_s)
            if (now - last) > limit:
                stale.append(sid)
        for sid in stale:
            try:
                await self.release(sid)
            except Exception:
                log.exception("reap_idle: release(%s) failed", sid)
        return len(stale)

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
