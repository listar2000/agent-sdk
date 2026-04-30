"""Process-singleton SessionPool wired to DB bindings + factory.

Exposed via ``get_pool()``. Phase 2 sub-task 3 lands this; sub-task 4
redirects the existing recovery functions in server.py to use it.
"""
from __future__ import annotations

import asyncio
import logging
import os

from .db_bindings import load_sandbox_state, save_sandbox_state
from .factory import make_session
from .pool import SessionPool

log = logging.getLogger(__name__)

# Module-level pool. Lazily instantiated on first ``get_pool()`` call so
# tests that don't need it pay no construction cost.
_pool: SessionPool | None = None
_reaper_task: asyncio.Task | None = None

# How often the reaper scans + how long an idle session survives before
# hibernation. Tunable via env so ops can dial it without a code change.
_REAPER_INTERVAL_S = float(os.environ.get("AGENT_SDK_REAPER_INTERVAL_S", "60"))
_REAPER_IDLE_S = float(os.environ.get("AGENT_SDK_REAPER_IDLE_S", "180"))  # 3 min


def get_pool() -> SessionPool:
    global _pool
    if _pool is None:
        _pool = SessionPool(
            factory=make_session,
            load_state=load_sandbox_state,
            save_state=save_sandbox_state,
        )
    return _pool


async def start_reaper() -> None:
    """Start the background reaper that hibernates idle sessions.

    Idempotent — calling twice does not start two reapers. Called from
    the server's lifespan startup hook.
    """
    global _reaper_task
    if _reaper_task is not None and not _reaper_task.done():
        return
    pool = get_pool()
    _reaper_task = asyncio.create_task(_reaper_loop(pool))


async def _reaper_loop(pool: SessionPool) -> None:
    while True:
        try:
            await asyncio.sleep(_REAPER_INTERVAL_S)
        except asyncio.CancelledError:
            return
        try:
            n = await pool.reap_idle(_REAPER_IDLE_S)
            if n:
                log.info("reaper: hibernated %d idle session(s)", n)
        except Exception:
            log.exception("reaper tick failed")


async def shutdown_pool() -> None:
    """Snapshot + release every active session, then drop the pool.
    Called from the server's graceful-shutdown path."""
    global _pool, _reaper_task
    if _reaper_task is not None:
        _reaper_task.cancel()
        try:
            await _reaper_task
        except (asyncio.CancelledError, Exception):
            pass
        _reaper_task = None
    if _pool is not None:
        await _pool.shutdown_all()
        _pool = None
