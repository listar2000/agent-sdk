"""Regression tests for async/await correctness hazards.

Surviving coverage after the SessionPool refactor (which deleted the
``_spawn_bg`` / ``_BG_TASKS`` / ``_get_session_lock`` / ``_shutdown_session_state``
/ ``_maybe_auto_approve_permission`` / ``_cancel_task`` plumbing this file
originally pinned):

1. Local-provider ``create_sandbox`` rollback runs the blocking
   ``_kill_proc`` off the event loop — a hung supervisor health check
   can't stall every other coroutine for 10 seconds. Source-grep test.
2. ``allocate_sandbox_port`` is atomic under concurrent callers (pure-sync
   critical section, asyncio single-threaded guarantee).
"""
from __future__ import annotations

import asyncio
import os
import sys

import pytest

_SRC = os.path.join(os.path.dirname(__file__), "..", "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)


# ---------------------------------------------------------------------------
# 1. Local-provider rollback must not block the event loop
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_local_create_sandbox_rollback_uses_thread():
    """When supervisor health check fails, _kill_proc runs off the event loop.

    We don't exercise the full create_sandbox path — the fix swaps a raw
    ``_kill_proc(proc)`` call for ``await asyncio.to_thread(_kill_proc, proc)``.
    Verify by grepping the source so the regression is detected syntactically
    (integration runs of the local provider cover the behavior).
    """
    import inspect
    from api.providers import unix_local as lp

    src = inspect.getsource(lp.create_sandbox)
    # The rollback paths must NOT call _kill_proc synchronously — that
    # blocks the event loop for up to 10 seconds.
    assert "await asyncio.to_thread(_kill_proc," in src, (
        "local.create_sandbox rollback must offload _kill_proc to a thread"
    )
    # Sanity: at least the two rollback sites are updated.
    assert src.count("await asyncio.to_thread(_kill_proc,") >= 2


# ---------------------------------------------------------------------------
# 2. Port allocator atomicity under concurrent callers
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_allocate_sandbox_port_uniqueness_under_concurrency():
    """Concurrent allocations must return distinct ports.

    ``allocate_sandbox_port`` is a plain sync function that mutates
    module-level dicts. Under asyncio's single-threaded model, each call
    runs atomically between await points — so even 200 concurrent tasks
    should never collide. This test pins that invariant.
    """
    from api.providers import _shared as sh

    sh._sandbox_port_counters.pop("sid-test", None)
    sh._sandbox_freed_ports.pop("sid-test", None)

    async def _alloc():
        return sh.allocate_sandbox_port("sid-test")

    ports = await asyncio.gather(*[_alloc() for _ in range(200)])
    assert len(set(ports)) == len(ports), "duplicate port assignment"


@pytest.mark.asyncio
async def test_allocate_sandbox_port_free_and_reuse():
    """Freed ports must be re-used before bumping the counter."""
    from api.providers import _shared as sh

    sh._sandbox_port_counters.pop("sid-test2", None)
    sh._sandbox_freed_ports.pop("sid-test2", None)

    p1 = sh.allocate_sandbox_port("sid-test2")
    p2 = sh.allocate_sandbox_port("sid-test2")
    assert p1 != p2

    sh.free_sandbox_port("sid-test2", p1)
    p3 = sh.allocate_sandbox_port("sid-test2")
    assert p3 == p1, "freed port must be recycled before the counter advances"
