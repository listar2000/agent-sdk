"""SessionPool — the entire recovery surface, in one method.

Per ``docs/ephemeral-sandbox-design.md`` §6. Replaced the legacy
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
import logging
from collections.abc import Callable
from typing import Any

from .session import BaseSandboxSession
from .state import SandboxState, deserialize, serialize

log = logging.getLogger(__name__)


# Type for the factory that turns a session_id + deserialised state into
# the appropriate concrete SandboxSession subclass. Phase 2 exposes a
# default implementation in factory.py keyed on state.type.
SessionFactory = Callable[[str, SandboxState], BaseSandboxSession]


# Type for the function that loads sandbox_state JSONB for a session_id.
# Real impl reads from sessions.sandbox_state. Single-process
# serialization uses the per-session asyncio lock below; multi-process
# would need a single transaction wrapping the whole load→start→save
# sequence (see db_bindings.py module docstring). Tests can pass a mock.
LoadState = Callable[[str], "asyncio.Future[dict[str, Any] | None]"]
SaveState = Callable[[str, dict[str, Any]], "asyncio.Future[None]"]


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

    def __init__(
        self,
        *,
        factory: SessionFactory,
        load_state: LoadState,
        save_state: SaveState,
    ) -> None:
        self._factory = factory
        self._load_state = load_state
        self._save_state = save_state
        self._active: dict[str, BaseSandboxSession] = {}
        self._locks: dict[str, asyncio.Lock] = {}

    def _lock(self, session_id: str) -> asyncio.Lock:
        lock = self._locks.get(session_id)
        if lock is None:
            lock = self._locks.setdefault(session_id, asyncio.Lock())
        return lock

    async def get_session(self, session_id: str) -> BaseSandboxSession:
        """Returns a known-alive SandboxSession. The single recovery
        entry point. Per docs §6.

        Holds ``_locks[session_id]`` for the entire decide-and-start
        sequence so concurrent callers can't double-provision.
        """
        async with self._lock(session_id):
            cached = self._active.get(session_id)
            if cached is not None:
                # Force-probe so an externally-killed supervisor is detected
                # immediately, even if the previous prompt's last chunk was
                # observed seconds ago (the test 7 race class).
                alive = await cached.running(force_probe=True)
                log.info("[pool.get_session] session=%s cached=True alive=%s", session_id, alive)
                if alive:
                    return cached
                # Stale entry; tear down runtime in background. We don't
                # snapshot here — compute is dead, can't snapshot reliably.
                # The previous successful per-turn snapshot is the fallback.
                asyncio.create_task(_safe_shutdown(cached))
                self._active.pop(session_id, None)

            payload = await self._load_state(session_id)
            state = deserialize(payload)
            session = self._factory(session_id, state)
            log.info("[pool.get_session] session=%s creating new state.type=%s sandbox_id=%s",
                     session_id, getattr(state, "type", "?"), getattr(state, "sandbox_id", None))
            await session.start()
            await self._save_state(session_id, serialize(session.state))
            self._active[session_id] = session
            return session

    async def release(self, session_id: str) -> None:
        """Hibernate: snapshot + drop compute. Idempotent.

        Triggered by the reaper or explicit ``POST /sessions/{id}/release``.
        """
        async with self._lock(session_id):
            session = self._active.pop(session_id, None)
            if session is None:
                return
            try:
                try:
                    await session.stop()
                    await self._save_state(session_id, serialize(session.state))
                except Exception:
                    log.exception(
                        "session.stop() failed for %s; proceeding with shutdown",
                        session_id,
                    )
            finally:
                await _safe_shutdown(session)

    def has_active(self, session_id: str) -> bool:
        """For derived UI/admin info ('lifecycle: active|hibernated').
        No I/O — just whether the pool currently holds a session."""
        return session_id in self._active

    async def reap_idle(self, idle_s: float) -> int:
        """Hibernate every active session whose last observed activity is
        older than ``idle_s``. Returns the count of sessions released.

        Activity = last chunk observed by ``execute_prompt`` (tracked on
        the session's ``Liveness._last_chunk_at``). Heartbeats from the
        supervisor count as activity, so a session stays warm for the
        duration of any in-flight prompt regardless of how long it runs.
        """
        import time as _time
        now = _time.monotonic()
        stale = []
        for sid, sess in list(self._active.items()):
            last = sess.liveness._last_chunk_at
            if last is None:
                # Never observed a chunk — likely a session that started
                # but hasn't had a prompt yet. Use creation as a proxy
                # by giving it the full idle window from now.
                sess.liveness._last_chunk_at = now
                continue
            if (now - last) > idle_s:
                stale.append(sid)
        for sid in stale:
            try:
                await self.release(sid)
            except Exception:
                log.exception("reap_idle: release(%s) failed", sid)
        return len(stale)

    def find_by_sandbox_id(self, sandbox_id: str) -> BaseSandboxSession | None:
        """Reverse lookup: find an active session whose underlying compute
        carries this provider sandbox id. Used by ``/sandboxes/{id}/files/*``
        endpoints — sandbox identity isn't durable, but if the compute is
        currently running we know who owns it."""
        for sess in self._active.values():
            if getattr(sess.state, "sandbox_id", None) == sandbox_id:
                return sess
        return None

    async def shutdown_all(self) -> None:
        """Stop the world: snapshot + shutdown every active session.
        Used at server-graceful-shutdown."""
        for sid in list(self._active.keys()):
            await self.release(sid)


async def _safe_shutdown(session: BaseSandboxSession) -> None:
    try:
        await session.shutdown()
    except Exception:
        log.exception("shutdown() failed for session %s", session.session_id)
