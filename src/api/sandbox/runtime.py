"""Process-singleton SessionPool wired to DB bindings + factory.

Exposed via ``get_pool()``. Phase 2 sub-task 3 lands this; sub-task 4
redirects the existing recovery functions in server.py to use it.
"""
from __future__ import annotations

from .db_bindings import load_sandbox_state, save_sandbox_state
from .factory import make_session
from .pool import SessionPool

# Module-level pool. Lazily instantiated on first ``get_pool()`` call so
# tests that don't need it pay no construction cost.
_pool: SessionPool | None = None


def get_pool() -> SessionPool:
    global _pool
    if _pool is None:
        _pool = SessionPool(
            factory=make_session,
            load_state=load_sandbox_state,
            save_state=save_sandbox_state,
        )
    return _pool


async def shutdown_pool() -> None:
    """Snapshot + release every active session, then drop the pool.
    Called from the server's graceful-shutdown path."""
    global _pool
    if _pool is not None:
        await _pool.shutdown_all()
        _pool = None
