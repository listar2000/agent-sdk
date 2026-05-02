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
3. ``SessionState.dispatch`` buffers events when no subscribers exist so
   the UI's EventSource-reconnect-gap doesn't drop prompt replies.
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


# ---------------------------------------------------------------------------
# 3. SessionState.dispatch must not drop events when no subscribers exist
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_dispatch_with_no_subscribers_buffers_for_next_subscribe():
    """Prod UI bug: supervisor dies → server's SSE reader kicks subscribers
    → browser's EventSource retry timer fires ~3s later → user types a
    message IN that gap → POST /message queues the prompt → scheduler
    dispatches the reply events via ``SessionState.dispatch`` to zero
    subscribers → events silently dropped → UI sits at "Queued for
    agent" forever.

    Invariant under test: events dispatched while both rpc and session
    subscriber lists are empty must still be delivered to the first
    session subscriber that shows up afterwards (replay buffer).

    Deterministic — no real sandbox, no sleeps. Exercises SessionState
    directly: dispatch with no subscribers, then subscribe_session, then
    drain the queue. On the unfixed baseline the queue is empty (events
    were dropped at dispatch time).
    """
    from api.models import SessionState

    st = SessionState(session_id="s", agent_id="a", sandbox_id="sb")

    # Dispatch three events while nobody is listening. On the real path
    # these would be the assistant_message / tool_call / done SSE blocks
    # from the scheduler's prompt reply.
    st.dispatch("rpc1", ("rpc1", "data: block-a\n\n"))
    st.dispatch("rpc1", ("rpc1", "data: block-b\n\n"))
    st.dispatch("rpc1", ("rpc1", "data: done\n\n"))

    # UI's EventSource finally reconnects — subscribe and drain.
    q = st.subscribe_session()
    drained: list[tuple[str, str]] = []
    while not q.empty():
        drained.append(q.get_nowait())

    assert len(drained) == 3, (
        f"events dispatched while no subscriber existed were dropped: "
        f"got {len(drained)} items, want 3. This is the UI's 'Queued for "
        f"agent' bug — the reconnect gap between server-side kick and "
        f"browser EventSource retry loses every event for prompts the "
        f"user submitted during the gap."
    )
    # Order preserved.
    assert [item[1] for item in drained] == [
        "data: block-a\n\n",
        "data: block-b\n\n",
        "data: done\n\n",
    ]


@pytest.mark.asyncio
async def test_dispatch_with_active_subscriber_skips_buffer():
    """Buffer must not grow while subscribers are actively draining —
    only the no-subscribers path should touch the replay deque.
    """
    from api.models import SessionState

    st = SessionState(session_id="s", agent_id="a", sandbox_id="sb")
    q1 = st.subscribe_session()

    st.dispatch("rpc1", ("rpc1", "data: x\n\n"))
    assert q1.qsize() == 1
    # A late second subscriber should NOT receive the event that already
    # went to q1 — only events dispatched during a zero-subscriber window.
    q2 = st.subscribe_session()
    assert q2.empty(), "buffer replay leaked an event that was already delivered"


@pytest.mark.asyncio
async def test_buffer_bounded_under_flood():
    """The no-subscribers buffer must be bounded so a session with an
    unresponsive client (never reconnects) can't grow memory forever.
    """
    from api.models import SessionState

    st = SessionState(session_id="s", agent_id="a", sandbox_id="sb")

    # Flood with more than any reasonable agent turn's event count.
    for i in range(50_000):
        st.dispatch("rpc", ("rpc", f"data: {i}\n\n"))

    q = st.subscribe_session()
    assert q.qsize() <= 10_000, (
        f"pending-broadcast buffer grew past a sane bound: {q.qsize()} items "
        f"would be drained onto a reconnecting subscriber"
    )
