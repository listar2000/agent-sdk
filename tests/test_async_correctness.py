"""Regression tests for async/await correctness hazards.

Covers:

1. Fire-and-forget background tasks are strongly referenced, so Python's GC
   can't collect them mid-flight ("Task was destroyed but it is pending!").
2. Session shutdown flushes the per-session log chain so in-flight log
   writes aren't silently dropped.
3. Local-provider ``create_sandbox`` rollback runs the blocking
   ``_kill_proc`` off the event loop — a hung supervisor health check
   can't stall every other coroutine for 10 seconds.
4. Sandbox + session lock ordering is consistent across call sites (no
   nested-lock deadlock).
5. ``allocate_sandbox_port`` is atomic under concurrent callers (pure-sync
   critical section, asyncio single-threaded guarantee).
"""
from __future__ import annotations

import asyncio
import gc
import os
import sys
import time
from unittest.mock import AsyncMock, patch

import pytest
import pytest_asyncio

_SRC = os.path.join(os.path.dirname(__file__), "..", "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)


# ---------------------------------------------------------------------------
# 1. Fire-and-forget task reference holder
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_spawn_bg_holds_strong_reference():
    """The _BG_TASKS set must keep the task alive until it completes."""
    from api import server as srv

    started = asyncio.Event()
    finished = asyncio.Event()

    async def _slow():
        started.set()
        await asyncio.sleep(0.05)
        finished.set()

    task = srv._spawn_bg(_slow())

    # While pending, the task must be in the registry.
    await started.wait()
    assert task in srv._BG_TASKS

    # Force GC — task must NOT be collected because _BG_TASKS holds it.
    del task
    gc.collect()

    await asyncio.wait_for(finished.wait(), timeout=2)

    # After completion, the done-callback drains the task from the set.
    # Give the loop one tick for the callback to fire.
    await asyncio.sleep(0)
    assert all(not t.done() for t in srv._BG_TASKS)


@pytest.mark.asyncio
async def test_spawn_bg_exceptions_dont_break_registry():
    """Tasks that raise must still be discarded from _BG_TASKS."""
    from api import server as srv

    async def _boom():
        raise RuntimeError("nope")

    task = srv._spawn_bg(_boom())
    # Wait for task completion — exception is stored on the task, not raised here.
    with pytest.raises(RuntimeError):
        await task
    await asyncio.sleep(0)  # let the done-callback fire
    assert task not in srv._BG_TASKS


@pytest.mark.asyncio
async def test_auto_approve_permission_uses_tracked_task():
    """_maybe_auto_approve_permission must register the grant task."""
    from api import server as srv
    from api.models import SessionState

    state = SessionState(
        session_id="s1", agent_id="a1", sandbox_id="sb1",
        acp_session_id="acp-1",
    )
    # Stub client so we don't need a live httpx.
    fake_client = type("C", (), {})()
    fake_client._client = AsyncMock()
    fake_client._client.post = AsyncMock(
        return_value=type("R", (), {"status_code": 200})()
    )
    state.client = fake_client

    payload = {
        "method": "session/request_permission",
        "id": "rpc-1",
        "params": {
            "options": [
                {"kind": "allow_once", "optionId": "opt-1"},
            ],
        },
    }
    before = set(srv._BG_TASKS)
    srv._maybe_auto_approve_permission(payload, state)
    after = set(srv._BG_TASKS)
    # Exactly one new task was spawned and registered.
    new_tasks = after - before
    assert len(new_tasks) == 1
    # Drain so the test doesn't leak tasks.
    for t in new_tasks:
        await t


# ---------------------------------------------------------------------------
# 2. Log chain flush on shutdown
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_shutdown_flushes_log_chain():
    """Pending log writes must complete before the session is reaped."""
    from api import server as srv
    from api.models import SessionState

    flushed = asyncio.Event()

    async def _slow_log():
        await asyncio.sleep(0.05)
        flushed.set()

    state = SessionState(session_id="s2", agent_id="a2", sandbox_id="sb2")
    state._log_chain = asyncio.create_task(_slow_log())

    # Patch out the daytona-specific supervisor-kill branch so the shutdown
    # path doesn't touch the provider.
    with patch.object(srv, "_close_session_gracefully", AsyncMock()):
        await srv._shutdown_session_state(state, remove=False)

    assert flushed.is_set(), "log chain was not flushed on shutdown"


@pytest.mark.asyncio
async def test_shutdown_log_chain_hang_bounded():
    """A hung log writer can't stall shutdown indefinitely — bounded wait."""
    from api import server as srv
    from api.models import SessionState

    async def _hang():
        await asyncio.sleep(60)  # longer than the 5s shutdown bound

    state = SessionState(session_id="s3", agent_id="a3", sandbox_id="sb3")
    state._log_chain = asyncio.create_task(_hang())

    t0 = time.time()
    with patch.object(srv, "_close_session_gracefully", AsyncMock()):
        await srv._shutdown_session_state(state, remove=False)
    elapsed = time.time() - t0

    assert elapsed < 7.0, f"shutdown took {elapsed:.1f}s, should be <5s+slack"
    state._log_chain.cancel()


# ---------------------------------------------------------------------------
# 3. Local-provider rollback must not block the event loop
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
    from api.providers import local as lp

    src = inspect.getsource(lp.create_sandbox)
    # The rollback paths must NOT call _kill_proc synchronously — that
    # blocks the event loop for up to 10 seconds.
    assert "await asyncio.to_thread(_kill_proc," in src, (
        "local.create_sandbox rollback must offload _kill_proc to a thread"
    )
    # Sanity: at least the two rollback sites are updated.
    assert src.count("await asyncio.to_thread(_kill_proc,") >= 2


# ---------------------------------------------------------------------------
# 4. Lock ordering — no nested session_lock -> sandbox_lock or vice versa
# ---------------------------------------------------------------------------


def test_lock_ordering_is_consistent():
    """No handler should hold one kind of lock while acquiring the other.

    Prevents the classic A→B / B→A deadlock. We enforce "session_lock and
    sandbox_lock are never nested, in either direction" by scanning the
    server module for inner uses.
    """
    import inspect
    from api import server as srv

    # Walk every async function defined in server.py. For each, look for
    # sequences where ``_get_session_lock`` is followed by a call/grep to
    # ``_get_sandbox_lock`` (or vice versa) inside the same function body.
    src = inspect.getsource(srv)

    # Cheap textual check: find every ``async with _get_session_lock`` and
    # verify the enclosing function body never also uses ``_get_sandbox_lock``.
    # (We could parse AST, but substring scan is adequate for the shape.)
    import re

    # Collect line ranges for each async def by tokenizing def boundaries.
    func_bodies: list[tuple[str, str]] = []
    lines = src.splitlines()
    current_name: str | None = None
    current: list[str] = []
    indent = None
    for line in lines:
        m = re.match(r"^(async\s+def|def)\s+(\w+)\s*\(", line)
        if m:
            if current_name is not None:
                func_bodies.append((current_name, "\n".join(current)))
            current_name = m.group(2)
            current = [line]
            indent = len(line) - len(line.lstrip())
        elif current_name is not None:
            # Include everything until we hit a same-or-lower-indent def/top-level.
            stripped = line.lstrip()
            cur_indent = len(line) - len(line.lstrip()) if stripped else None
            if stripped and cur_indent is not None and cur_indent <= (indent or 0) and \
               (stripped.startswith("def ") or stripped.startswith("async def ")
                or stripped.startswith("@") or stripped.startswith("class ")):
                func_bodies.append((current_name, "\n".join(current)))
                current_name = None
                current = []
            else:
                current.append(line)
    if current_name is not None:
        func_bodies.append((current_name, "\n".join(current)))

    violations = []
    for name, body in func_bodies:
        has_session = "_get_session_lock(" in body
        has_sandbox = "_get_sandbox_lock(" in body
        if has_session and has_sandbox:
            violations.append(name)

    assert not violations, (
        f"nested session/sandbox lock detected in: {violations} — "
        "ordering-dependent deadlock risk"
    )


# ---------------------------------------------------------------------------
# 5. Port allocator atomicity under concurrent callers
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


# ---------------------------------------------------------------------------
# 6. _cancel_task must not raise on cancelled / completed tasks
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_cancel_task_tolerates_done_and_exception():
    from api import server as srv

    # None case
    await srv._cancel_task(None)

    # Already-done task
    async def _ok():
        return 1
    t = asyncio.create_task(_ok())
    await t
    await srv._cancel_task(t)  # must not raise

    # Pending task — cancels cleanly
    async def _forever():
        await asyncio.sleep(30)
    t2 = asyncio.create_task(_forever())
    await asyncio.sleep(0)
    await srv._cancel_task(t2)
    assert t2.cancelled() or t2.done()

    # Task that raises — exception swallowed
    async def _raise():
        raise ValueError("x")
    t3 = asyncio.create_task(_raise())
    await srv._cancel_task(t3)  # must not raise
