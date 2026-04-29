"""Contracts for the periodic _session_locks sweep.

The sweep must preserve the existing serialization invariant documented
at _shutdown_session_state: a concurrent caller mid-acquire on the same
session_id must never see a fresh Lock minted by _get_session_lock while
the original holder still has work to do.
"""
from __future__ import annotations

import asyncio
import os
import sys

import pytest

_SRC = os.path.join(os.path.dirname(__file__), "..", "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

from api import server as srv  # noqa: E402


@pytest.fixture(autouse=True)
def _clean():
    srv.SESSIONS.clear()
    srv._session_locks.clear()
    yield
    srv.SESSIONS.clear()
    srv._session_locks.clear()


def test_sweep_removes_locks_for_sessions_not_in_SESSIONS():
    srv._session_locks["dead"] = asyncio.Lock()
    srv._session_locks["dead-2"] = asyncio.Lock()
    pruned = srv._sweep_session_locks()
    assert pruned == 2
    assert "dead" not in srv._session_locks
    assert "dead-2" not in srv._session_locks


def test_sweep_keeps_locks_for_active_sessions(monkeypatch):
    """A session_id present in SESSIONS must keep its lock (the lock is the
    one a future caller will acquire to talk to that session)."""
    from api.models import SessionState
    state = SessionState(session_id="alive", agent_id="a", sandbox_id="sb")
    srv.SESSIONS["alive"] = state
    srv._session_locks["alive"] = asyncio.Lock()
    pruned = srv._sweep_session_locks()
    assert pruned == 0
    assert "alive" in srv._session_locks


@pytest.mark.asyncio
async def test_sweep_keeps_held_locks_even_if_session_is_gone():
    """Held locks survive the sweep — a holder is mid-critical-section."""
    lock = asyncio.Lock()
    await lock.acquire()
    try:
        srv._session_locks["held"] = lock
        # not in SESSIONS — would normally be swept, but it's held
        pruned = srv._sweep_session_locks()
        assert pruned == 0
        assert "held" in srv._session_locks
    finally:
        lock.release()


@pytest.mark.asyncio
async def test_sweep_keeps_locks_with_waiters():
    """A lock with suspended acquirers must not be evicted — the sweep would
    detach the waiters from the lookup, and a fresh _get_session_lock call
    would mint a new Lock for the same session_id."""
    lock = asyncio.Lock()
    await lock.acquire()
    srv._session_locks["contended"] = lock

    # Spawn a waiter; give the loop a tick so it suspends inside acquire().
    waiter_started = asyncio.Event()

    async def _waiter():
        waiter_started.set()
        async with lock:
            pass

    task = asyncio.create_task(_waiter())
    await waiter_started.wait()
    await asyncio.sleep(0)  # yield so _waiter reaches the acquire suspend

    try:
        pruned = srv._sweep_session_locks()
        assert pruned == 0, "swept a lock with a suspended waiter"
        assert "contended" in srv._session_locks
    finally:
        lock.release()
        await task
