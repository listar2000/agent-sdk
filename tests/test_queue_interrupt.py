"""Tests for Agent.send() and Agent.events() queue/interrupt primitives.

Focused on:
  - send() bypasses agent_busy gate
  - events() subscribes and receives broadcasts
  - lifecycle: subscriber count goes 0→1→0
  - kick-on-full backpressure
  - SSE reader death cleanup
  - reaper respects active subscribers
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
import time
import uuid
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

# ---------------------------------------------------------------------------
# Path setup
# ---------------------------------------------------------------------------

_SRC = os.path.join(os.path.dirname(__file__), "..", "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

# Mirror TEST_DATABASE_URL -> DATABASE_URL so api.db (captured at its own
# import time) picks up the test URL rather than the dev default.
_TEST_DB = os.environ.get("TEST_DATABASE_URL")
if _TEST_DB and not os.environ.get("DATABASE_URL"):
    os.environ["DATABASE_URL"] = _TEST_DB

import types
from contextlib import asynccontextmanager as _asynccontextmanager


# Generic no-op helpers used by the db stub factory and by individual tests
# that need to disable incidental server calls (e.g. log_event).
async def _noop(*a, **kw): return None
async def _noop_list(*a, **kw): return []
async def _noop_false(*a, **kw): return False
async def _noop_volume(*a, **kw): return None


@_asynccontextmanager
async def _noop_get_db(*a, **kw):
    yield None


def _build_stub_db() -> types.ModuleType:
    """Return a no-op stub module that stands in for api.db during tests."""
    stub = types.ModuleType("api.db")
    stub.init_db = lambda: None
    stub.init_pool = _noop
    stub.close_pool = _noop
    stub.upsert_agent = _noop
    stub.get_agent = _noop
    stub.list_agents = _noop_list
    stub.delete_agent = _noop
    stub.upsert_sandbox = _noop
    stub.get_sandbox = _noop
    stub.list_sandboxes = _noop_list
    stub.get_db = _noop_get_db
    stub.delete_sandbox = _noop
    stub.upsert_session = _noop
    stub.get_session = _noop
    stub.log_event = _noop
    stub.get_session_log = _noop_list
    stub.get_any_session_for_sandbox = _noop
    stub.get_session_env = _noop
    stub.get_session_secrets = _noop
    stub.update_session_env = _noop
    stub.update_session_secrets = _noop
    stub.upsert_volume = _noop
    stub.get_volume = _noop_volume
    stub.get_volume_by_name = _noop_volume
    stub.list_volumes = _noop_list
    stub.delete_volume = _noop
    stub.set_session_current_sandbox = _noop
    stub.add_supervisor_agent_type = _noop
    return stub


_STUBBED_DB_NAMES = (
    "init_db", "init_pool", "close_pool",
    "upsert_agent", "get_agent", "list_agents", "delete_agent",
    "upsert_sandbox", "get_sandbox", "list_sandboxes", "delete_sandbox",
    "upsert_session", "get_session",
    "get_session_env", "get_session_secrets", "get_any_session_for_sandbox",
    "update_session_env", "update_session_secrets",
    "log_event", "get_session_log",
    "get_db", "add_supervisor_agent_type",
    "upsert_volume", "get_volume", "get_volume_by_name",
    "list_volumes", "delete_volume", "set_session_current_sandbox",
)

# Install stub BEFORE importing api.server so that its ``from .db import``
# binds to stubs. Covers the case where pytest runs just this file with no
# preceding module having installed a stub. If test_adversarial already
# installed its own stub, our swap is harmless (both are no-ops) but
# necessary so our fixture can restore a known-good real-db state on exit.
_prior_api_db = sys.modules.get("api.db")
_stub_db_module = _build_stub_db()
sys.modules["api.db"] = _stub_db_module

import api.server as _server_module
from api.server import (
    app,
    SESSIONS,
    _INSTANCES,
    _on_sse_reader_death,
    _broadcast_one_block,
    _process_sse_block,
    _schedule_log,
    _SSE_SENTINEL,
    _shutdown_session_state,
    _session_idle_since,
    _sse_reader_disconnect_is_recoverable,
    _mark_turn_finished,
    _idle_reaper,
    _start_session_tasks,
    _submit_prompt,
    _cancel_and_drain,
    IDLE_TIMEOUT_S,
)
from api.models import (
    SessionState, PendingPrompt, SandboxRecord,
    _KICK_SENTINEL, EVT_TOOL_CALL, EVT_ERROR,
)

# Defensive: ensure every stubbed name on api.server really points at our
# stub, regardless of what any earlier-imported test module did to them.
for _name in _STUBBED_DB_NAMES:
    if hasattr(_stub_db_module, _name):
        setattr(_server_module, _name, getattr(_stub_db_module, _name))


@pytest.fixture(scope="module", autouse=True)
def _stub_api_db_module():
    """Force stub bindings for the duration of this test module.

    At setup we (re-)install our stub into ``sys.modules["api.db"]`` and
    re-bind every ``from .db import ...`` name on api.server to our stub.
    This defends against a sibling test module (e.g. test_adversarial) that
    may have transformed the shared stub into the real api.db during its
    own teardown before we started.

    At teardown we mutate the stub in-place to delegate to the real api.db
    so that any other test module holding ``dbmod = <stub>`` sees real DB
    behaviour on the next attribute access.
    """
    # Re-install stub + rebind server names (covers adversarial's teardown).
    sys.modules["api.db"] = _stub_db_module
    # Fresh no-op functions — adversarial's teardown may have mutated our
    # stub module into the real db; rebuild the stub now.
    fresh_stub = _build_stub_db()
    for attr in dir(fresh_stub):
        if attr.startswith("__"):
            continue
        setattr(_stub_db_module, attr, getattr(fresh_stub, attr))
    for name in _STUBBED_DB_NAMES:
        if hasattr(_stub_db_module, name):
            setattr(_server_module, name, getattr(_stub_db_module, name))
    try:
        yield
    finally:
        sys.modules.pop("api.db", None)
        import importlib
        try:
            real_db = importlib.import_module("api.db")
        except Exception:
            real_db = None
        if real_db is not None:
            for attr in dir(real_db):
                if attr.startswith("__"):
                    continue
                try:
                    setattr(_stub_db_module, attr, getattr(real_db, attr))
                except Exception:
                    pass
            sys.modules["api.db"] = real_db
            for name in _STUBBED_DB_NAMES:
                if hasattr(real_db, name):
                    setattr(_server_module, name, getattr(real_db, name))


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def clear_server_state():
    """Reset server state around each test and short-circuit ensure_session_live.

    Many tests in this module populate SESSIONS directly via _make_state to
    exercise the scheduler / subscriber / reader plumbing without a real DB.
    Under the new API the POST /sessions/{id}/message endpoint goes through
    ensure_session_live -> require_session -> get_session, so we need to
    synthesize a row from the in-memory SESSIONS entry and make
    ensure_sandbox / ensure_runtime return the existing state.
    """
    async def _fake_get_session(session_id: str):
        state = SESSIONS.get(session_id)
        if state is None:
            return None
        return {
            "id": state.session_id,
            "agent_id": state.agent_id,
            "current_sandbox_id": state.sandbox_id,
            "volume_id": "vol-test",
            "inner_session_id": state.inner_session_id,
        }

    async def _fake_ensure_sandbox(session_row):
        return SandboxRecord(
            id=session_row["current_sandbox_id"] or "sbx-test",
            provider="local",
            sandbox_ref="2469",
            status="running",
            volume_id="vol-test",
            subpath="agents/a/home",
            listen_port=2469,
        )

    async def _fake_ensure_runtime(session_row, sandbox):
        return SESSIONS[session_row["id"]]

    with patch(
        "api.server._live_session_looks_healthy",
        AsyncMock(return_value=True),
        create=True,
    ), patch(
        "api.server.get_session",
        side_effect=_fake_get_session,
    ), patch(
        "api.server.ensure_sandbox",
        side_effect=_fake_ensure_sandbox,
    ), patch(
        "api.server.ensure_runtime",
        side_effect=_fake_ensure_runtime,
    ):
        SESSIONS.clear()
        _INSTANCES.clear()
        yield
        # Cancel all background tasks (scheduler + reader) to prevent hangs
        for state in list(SESSIONS.values()):
            state.shutdown.set()
            state._prompt_ready.set()  # unblock scheduler loop
            for task_attr in ('_scheduler_task', '_reader_task'):
                task = getattr(state, task_attr, None)
                if task and not task.done():
                    task.cancel()
        SESSIONS.clear()
        _INSTANCES.clear()
def _make_state(session_id: str | None = None) -> SessionState:
    """Create a minimal connected SessionState."""
    session_id = session_id or str(uuid.uuid4())
    client = MagicMock()
    client.prompt = AsyncMock()
    client.aclose = AsyncMock()
    state = SessionState(
        session_id=session_id,
        agent_id="agent-1",
        sandbox_id="sbx-1",
        acp_session_id="acp-1",
        client=client,
    )
    SESSIONS[session_id] = state
    return state
# ---------------------------------------------------------------------------
# C1: Shared test helpers
# ---------------------------------------------------------------------------

async def _post_message_via_asgi(session_id: str, message: str, *, interrupt: bool = False) -> httpx.Response:
    """POST /sessions/{id}/message via ASGI transport."""
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        return await client.post(
            f"/sessions/{session_id}/message",
            json={"message": message, "interrupt": interrupt},
        )
async def _stop_scheduler(state: SessionState) -> None:
    """Cleanly shut down the scheduler loop so tests don't leak tasks."""
    state.shutdown.set()
    state._prompt_ready.set()
    for attr in ('_scheduler_task', '_reader_task'):
        task = getattr(state, attr, None)
        if task and not task.done():
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass
def _fill_queue(q: asyncio.Queue) -> None:
    """Fill a queue to capacity with 'filler' items."""
    while True:
        try:
            q.put_nowait("filler")
        except asyncio.QueueFull:
            break
# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestSubmitPaths:
    """Submit paths: interrupt=True cancels and drains before submitting;
    default queues via pending_prompts."""

    @pytest.mark.asyncio
    async def test_default_accepts_while_busy(self):
        """Default submission while another turn is in flight returns 200
        and queues in pending_prompts."""
        state = _make_state("sess-busy-1")
        # Simulate busy: set active_rpc_id (scheduler owns this normally)
        state.active_rpc_id = "rpc-busy"

        with patch("api.server.log_event", _noop):
            resp = await _post_message_via_asgi("sess-busy-1", "normal message")

        assert resp.status_code == 200
        assert "rpc_id" in resp.json()
        # New prompt was queued in pending_prompts
        assert len(state.pending_prompts) >= 1
        state.active_rpc_id = None
        state.pending_prompts.clear()

    @pytest.mark.asyncio
    async def test_submit_enqueues_and_scheduler_activates(self):
        """Submitted prompt becomes active via the scheduler loop."""
        state = _make_state("sess-int-3")

        prompt_started = asyncio.Event()
        prompt_hold = asyncio.Event()

        async def slow_prompt(*args, **kwargs):
            prompt_started.set()
            await prompt_hold.wait()

        state.client.prompt = slow_prompt
        state._reader_alive = True  # prevent real SSE connection
        from api.server import _scheduler_loop
        state._scheduler_task = asyncio.create_task(_scheduler_loop(state))

        with patch("api.server.log_event", _noop):
            resp = await _post_message_via_asgi("sess-int-3", "msg")
        rpc_id = resp.json()["rpc_id"]

        await prompt_started.wait()
        assert state.active_rpc_id == rpc_id
        prompt_hold.set()
        await _stop_scheduler(state)

    @pytest.mark.asyncio
    async def test_submit_restarts_reader_when_dead(self):
        """A new message should recreate the upstream reader if it is down."""
        state = _make_state("sess-reader-restart")
        state._reader_alive = False
        state.subscribe_session()

        with patch("api.server._start_sse_reader") as mock_start_reader, \
             patch("api.server.log_event", _noop):
            resp = await _post_message_via_asgi("sess-reader-restart", "wake up")

        assert resp.status_code == 200
        mock_start_reader.assert_called_once_with(state)
class TestInflightTracking:

    @pytest.mark.asyncio
    async def test_clears_agent_busy_on_complete(self):
        """Scheduler loop clears active_rpc_id after prompt completes."""
        state = _make_state("sess-normal-1")

        async def quick_prompt(*args, **kwargs):
            pass  # completes immediately

        state.client.prompt = quick_prompt
        state._reader_alive = True; _start_session_tasks(state)

        with patch("api.server.log_event", _noop):
            await _post_message_via_asgi("sess-normal-1", "hello")

        await asyncio.sleep(0.1)
        assert not state.agent_busy
        await _stop_scheduler(state)

    @pytest.mark.asyncio
    async def test_active_rpc_cleared_on_complete(self):
        """active_rpc_id is None after prompt finishes."""
        state = _make_state("sess-inflight-1")

        async def quick_prompt(*args, **kwargs):
            pass

        state.client.prompt = quick_prompt
        state._reader_alive = True; _start_session_tasks(state)

        with patch("api.server.log_event", _noop):
            await _post_message_via_asgi("sess-inflight-1", "hi")

        await asyncio.sleep(0.1)
        assert state.active_rpc_id is None
        await _stop_scheduler(state)
class TestActiveRpcAttribution:
    """Explicit scheduler: active_rpc_id is the single source of truth
    for which prompt is currently executing."""

    def test_active_is_none_when_never_used(self):
        state = _make_state()
        assert state.active_rpc_id is None
        assert state.agent_busy is False

    def test_agent_busy_tracks_active_rpc_id(self):
        state = _make_state()
        state.active_rpc_id = "rpc-1"
        assert state.agent_busy is True
        state.active_rpc_id = None
        assert state.agent_busy is False

    def test_pending_prompts_queue(self):
        state = _make_state()
        state.pending_prompts.append(PendingPrompt(rpc_id="A", message="hello"))
        state.pending_prompts.append(PendingPrompt(rpc_id="B", message="world"))
        assert len(state.pending_prompts) == 2
        first = state.pending_prompts.popleft()
        assert first.rpc_id == "A"
class TestSubscriberLifecycle:

    def test_subscribe_increments_subscriber_count(self):
        """subscribe() adds a queue to _session_subscribers."""
        state = _make_state()
        assert len(state._session_subscribers) == 0
        q = state.subscribe_session()
        assert len(state._session_subscribers) == 1
        state.unsubscribe_session(q)
        assert len(state._session_subscribers) == 0

    def test_dispatch_delivers_to_session_subscribers(self):
        """dispatch() delivers to all session-scoped subscriber queues."""
        state = _make_state()
        q1 = state.subscribe_session()
        q2 = state.subscribe_session()

        state.dispatch("tag-1", "event-1")
        state.dispatch("tag-2", "event-2")

        assert q1.get_nowait() == "event-1"
        assert q1.get_nowait() == "event-2"
        assert q2.get_nowait() == "event-1"
        assert q2.get_nowait() == "event-2"

    def test_rpc_subscriber_receives_only_matching(self):
        """RPC-scoped subscriber only gets events for its rpc_id."""
        state = _make_state()
        q_rpc = state.subscribe_rpc("rpc-A")
        q_session = state.subscribe_session()

        state.dispatch("rpc-A", "match")
        state.dispatch("rpc-B", "no-match")

        assert q_rpc.get_nowait() == "match"
        assert q_rpc.empty()  # did NOT receive rpc-B
        # Session subscriber got both
        assert q_session.get_nowait() == "match"
        assert q_session.get_nowait() == "no-match"

    def test_unsubscribe_stops_delivery(self):
        """unsubscribe() prevents future dispatches from reaching the queue."""
        state = _make_state()
        q = state.subscribe_session()
        state.unsubscribe_session(q)
        state.dispatch("tag", "missed")
        assert q.empty()

    def test_broadcast_none_reaches_all(self):
        """broadcast(None) sends heartbeat to session + RPC subscribers."""
        state = _make_state()
        q_session = state.subscribe_session()
        q_rpc = state.subscribe_rpc("rpc-X")

        state.broadcast(None)

        assert q_session.get_nowait() is None
        assert q_rpc.get_nowait() is None

    @pytest.mark.asyncio
    async def test_session_status_exposes_counts(self):
        """GET /sessions/{id}/status exposes subscriber counts."""
        state = _make_state("sess-status-1")
        q = state.subscribe_session()
        state.active_rpc_id = "rpc-test-1"

        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://test",
        ) as client:
            resp = await client.get("/sessions/sess-status-1/status")

        data = resp.json()
        assert data["session_subscriber_count"] == 1
        assert data["agent_busy"] is True
        assert data["active_rpc_id"] == "rpc-test-1"

        state.unsubscribe_session(q)
        state.active_rpc_id = None
class TestKickOnFull:

    def test_kick_sentinel_delivered_to_full_queue(self):
        """dispatch() kicks full subscribers with _KICK_SENTINEL.

        O(1) drain contract: _kick_subscriber discards ONE item to make room
        then appends the sentinel.  After a kick, the sentinel is at the END of
        the queue (after all existing items), not at the front.
        """
        state = _make_state()
        q = state.subscribe_session()

        # Fill to capacity
        _fill_queue(q)

        state.dispatch("tag", "overflow")

        # Drain all items; sentinel must be the last one
        items = []
        while not q.empty():
            items.append(q.get_nowait())
        assert items[-1] is _KICK_SENTINEL, "sentinel must be last item after O(1) kick"

    def test_kicked_subscriber_removed_from_list(self):
        """dispatch() removes a full subscriber after kicking it."""
        state = _make_state()
        q = state.subscribe_session()

        _fill_queue(q)

        state.dispatch("tag", "overflow")
        assert q not in state._session_subscribers
class TestSSEReaderDeathCleanup:

    def test_subscribers_kicked_on_reader_death(self):
        """_on_sse_reader_death kicks all subscribers with the sentinel."""
        state = _make_state()
        q1 = state.subscribe_session()
        q2 = state.subscribe_session()

        _on_sse_reader_death(state)

        # Drain each queue; last item must be the kick sentinel
        for q in (q1, q2):
            items = []
            while not q.empty():
                items.append(q.get_nowait())
            assert items and items[-1] is _KICK_SENTINEL

        assert len(state._session_subscribers) == 0

    def test_active_rpc_cleared_on_reader_death(self):
        """_on_sse_reader_death clears active_rpc_id and pending_prompts."""
        state = _make_state()
        state.active_rpc_id = "rpc-dying"
        state.pending_prompts.append(PendingPrompt(rpc_id="rpc-q", message="q"))

        _on_sse_reader_death(state)

        assert state.active_rpc_id is None
        assert len(state.pending_prompts) == 0

    def test_agent_busy_cleared_on_reader_death(self):
        """_on_sse_reader_death clears agent_busy (derived from active_rpc_id)."""
        state = _make_state()
        state.active_rpc_id = "rpc-busy"
        assert state.agent_busy is True

        _on_sse_reader_death(state)

        assert not state.agent_busy

    def test_shutdown_set_on_reader_death(self):
        """_on_sse_reader_death sets the shutdown event."""
        state = _make_state()
        assert not state.shutdown.is_set()

        _on_sse_reader_death(state)

        assert state.shutdown.is_set()

    def test_reader_death_noop_on_intentional_shutdown(self):
        """_on_sse_reader_death is a no-op when shutdown was already set."""
        state = _make_state()
        state.shutdown.set()
        q = state.subscribe_session()

        _on_sse_reader_death(state)

        # Subscriber should NOT be kicked on intentional shutdown
        assert q in state._session_subscribers

    def test_idle_disconnect_is_recoverable(self):
        """Idle reader disconnects should reconnect instead of shutting down."""
        state = _make_state()

        assert _sse_reader_disconnect_is_recoverable(state) is True

    def test_busy_disconnect_is_not_recoverable(self):
        """An in-flight turn cannot safely recover from reader loss."""
        state = _make_state()
        state.active_rpc_id = "rpc-live"

        assert _sse_reader_disconnect_is_recoverable(state) is False

    def test_pending_disconnect_is_not_recoverable(self):
        """Queued work means the reader loss should still be treated as fatal."""
        state = _make_state()
        state.pending_prompts.append(PendingPrompt(rpc_id="rpc-q", message="queued"))

        assert _sse_reader_disconnect_is_recoverable(state) is False

    def test_shutdown_disconnect_is_not_recoverable(self):
        """Intentional shutdown should not enter the reconnect path."""
        state = _make_state()
        state.shutdown.set()

        assert _sse_reader_disconnect_is_recoverable(state) is False
class TestLifecycleBugRegression:

    @pytest.mark.asyncio
    async def test_concurrent_sends_all_accepted_and_queued(self):
        """All sends return 200. Prompts queue in pending_prompts; scheduler
        processes them one at a time."""
        state = _make_state("sess-lifecycle-1")

        with patch("api.server.log_event", _noop):
            r1 = await _post_message_via_asgi("sess-lifecycle-1", "first")
            r2 = await _post_message_via_asgi("sess-lifecycle-1", "second")
            r3 = await _post_message_via_asgi("sess-lifecycle-1", "third")

        assert r1.status_code == 200
        assert r2.status_code == 200
        assert r3.status_code == 200

        ids = [r.json()["rpc_id"] for r in (r1, r2, r3)]
        queued_ids = [p.rpc_id for p in state.pending_prompts]
        # All three should be in pending_prompts (scheduler not started)
        assert queued_ids == ids
class TestConcurrentAccept:

    @pytest.mark.asyncio
    async def test_concurrent_posts_all_accepted(self):
        """N concurrent POSTs all return 200 and each gets a unique rpc_id
        queued in pending_prompts."""
        N = 10
        state = _make_state("sess-race-1")

        with patch("api.server.log_event", _noop):
            responses = await asyncio.gather(
                *[_post_message_via_asgi("sess-race-1", f"msg-{i}") for i in range(N)]
            )

        status_codes = [r.status_code for r in responses]
        assert status_codes == [200] * N, f"expected all 200, got {status_codes}"

        rpc_ids = [r.json()["rpc_id"] for r in responses]
        assert len(set(rpc_ids)) == N, "all rpc_ids should be unique"
        queued_ids = {p.rpc_id for p in state.pending_prompts}
        for rid in rpc_ids:
            assert rid in queued_ids
class TestIdleReaper:
    """Unit tests for the idle reaper's session-picking predicate.

    These don't run the real background task — they exercise the same
    guard conditions the reaper uses so regressions are caught fast.
    """

    def test_reaper_skips_session_with_active_prompt(self):
        """A session with an active prompt must not be reaped."""
        state = _make_state("sess-reap-inflight")
        state.active_rpc_id = "rpc-live"

        # Mirror the reaper guard (server.py _idle_reaper)
        should_skip = (state.active_rpc_id is not None or state.pending_prompts or state._session_subscribers)
        assert should_skip

    def test_reaper_skips_session_with_subscriber(self):
        """A session with an active /events subscriber must not be reaped."""
        state = _make_state("sess-reap-sub")
        state.subscribe_session()

        should_skip = (state.active_rpc_id is not None or state.pending_prompts or state._session_subscribers)
        assert should_skip

    def test_reaper_picks_idle_quiescent_session(self):
        """A session with no active prompt and no subscribers is eligible for reaping."""
        state = _make_state("sess-reap-idle")

        should_skip = (state.active_rpc_id is not None or state.pending_prompts or state._session_subscribers)
        assert not should_skip

    def test_reaper_skips_fresh_session_within_timeout(self):
        """Idle_since check: a fresh session is within the idle window."""
        import time
        state = _make_state("sess-reap-fresh")
        state.last_activity = time.time()

        idle_for = time.time() - _session_idle_since(state)
        assert idle_for < 1.0  # well within any reasonable timeout

    def test_reaper_picks_stale_session_past_timeout(self):
        """A session that has been idle longer than IDLE_TIMEOUT_S is eligible."""
        import time
        state = _make_state("sess-reap-stale")
        # Simulate 1 hour of idleness
        state.last_activity = time.time() - 3600
        state.turn_completed_at = time.time() - 3600

        idle_for = time.time() - _session_idle_since(state)
        assert idle_for > 300  # way past any reasonable timeout

    @pytest.mark.asyncio
    async def test_shutdown_aborts_if_active_prompt(self):
        """_shutdown_session_state aborts and clears shutdown event if an
        active prompt appeared between the caller's idle check and shutdown."""
        state = _make_state("sess-reap-race")
        state.active_rpc_id = "rpc-new"

        await _shutdown_session_state(state, remove=True)

        # Session should NOT have been removed — reaper aborted
        assert "sess-reap-race" in SESSIONS
        assert not state.shutdown.is_set()

    @pytest.mark.asyncio
    async def test_shutdown_aborts_if_subscriber_appears(self):
        """Same race as above, but triggered by a late subscriber."""
        state = _make_state("sess-reap-race-sub")
        state.subscribe_session()

        await _shutdown_session_state(state, remove=True)

        assert "sess-reap-race-sub" in SESSIONS
        assert not state.shutdown.is_set()

    def test_session_idle_since_uses_turn_completed_at(self):
        """_session_idle_since prefers turn_completed_at over last_activity."""
        state = _make_state("sess-idle-since-1")
        state.turn_completed_at = 100.0
        state.last_activity = 200.0
        assert _session_idle_since(state) == 100.0

    def test_session_idle_since_falls_back_to_last_activity(self):
        """_session_idle_since falls back to last_activity when turn_completed_at is None."""
        state = _make_state("sess-idle-since-2")
        state.turn_completed_at = None
        state.last_activity = 999.0
        assert _session_idle_since(state) == 999.0

    @pytest.mark.asyncio
    async def test_shutdown_session_state_removes_from_sessions(self):
        """_shutdown_session_state with remove=True removes the session from SESSIONS."""
        state = _make_state("sess-shutdown-remove")
        await _shutdown_session_state(state, remove=True)
        assert "sess-shutdown-remove" not in SESSIONS

    @pytest.mark.asyncio
    async def test_shutdown_session_state_sets_shutdown_event(self):
        """_shutdown_session_state sets the shutdown event on a clean session."""
        state = _make_state("sess-shutdown-event")
        await _shutdown_session_state(state, remove=True)
        assert state.shutdown.is_set()

    @pytest.mark.asyncio
    async def test_reaper_skips_subscribed_session(self):
        """_shutdown_session_state aborts and does not remove a session that has an active subscriber."""
        state = _make_state("sess-reap-sub-2")
        q = state.subscribe_session()
        state.last_activity = 0.0

        await _shutdown_session_state(state, remove=True)

        assert "sess-reap-sub-2" in SESSIONS
        assert not state.shutdown.is_set()
        state.unsubscribe_session(q)

    def test_mark_turn_finished_sets_timestamps(self):
        """_mark_turn_finished sets turn_completed_at and last_activity."""
        state = _make_state("sess-mark-turn")
        assert state.turn_completed_at is None

        _mark_turn_finished(state)

        assert state.turn_completed_at is not None
        assert state.last_activity >= state.turn_completed_at

    @pytest.mark.asyncio
    async def test_reaper_closes_expired_session(self):
        """_idle_reaper removes a session that has been idle past IDLE_TIMEOUT_S."""
        state = _make_state("sess-reaper-expire")
        _mark_turn_finished(state)
        state.turn_completed_at = 0.0
        state.last_activity = 0.0

        with patch("api.server.IDLE_TIMEOUT_S", 0), patch("api.server.REAPER_TICK_S", 0):
            try:
                await asyncio.wait_for(_idle_reaper(), timeout=0.2)
            except asyncio.TimeoutError:
                pass

        assert "sess-reaper-expire" not in SESSIONS
# ---------------------------------------------------------------------------
# SSE block helper
# ---------------------------------------------------------------------------

import json as _json

def _sse(payload: dict) -> str:
    """Build an SSE block string (no trailing double-newline — callers add that)."""
    return f"data: {_json.dumps(payload)}\n"
# ---------------------------------------------------------------------------
# Part 1: TestProcessSseBlock
# ---------------------------------------------------------------------------

class TestProcessSseBlock:

    def test_heartbeat_comment_is_not_dispatched(self):
        """Upstream SSE comments must not appear as rpc events."""
        state = _make_state()
        state.active_rpc_id = "rpc-heartbeat"
        q = state.subscribe_session()

        _broadcast_one_block(state, ": heartbeat", None, [], [])

        assert q.empty()

    def test_text_delta_accumulates_in_text_parts(self):
        """Text delta content is appended to text_parts."""
        state = _make_state()
        text_parts: list[str] = []
        thinking_parts: list[str] = []
        block = _sse({"method": "session/update", "params": {"update": {
            "sessionUpdate": "agent_message_delta",
            "content": {"text": "hello"},
        }}})
        _process_sse_block(block, state, text_parts, thinking_parts)
        assert "hello" in text_parts

    def test_thinking_delta_accumulates_in_thinking_parts(self):
        """Thinking delta content is appended to thinking_parts."""
        state = _make_state()
        text_parts: list[str] = []
        thinking_parts: list[str] = []
        block = _sse({"method": "session/update", "params": {"update": {
            "sessionUpdate": "agent_message_delta",
            "content": {"thinking": "thinking..."},
        }}})
        _process_sse_block(block, state, text_parts, thinking_parts)
        assert "thinking..." in thinking_parts

    @pytest.mark.asyncio
    async def test_tool_call_flushes_text_parts_first(self):
        """Tool-call block flushes accumulated text before logging the tool call."""
        state = _make_state()
        text_parts: list[str] = ["some text"]
        thinking_parts: list[str] = []
        block = _sse({"method": "session/update", "params": {"update": {
            "sessionUpdate": "tool_call",
            "toolName": "bash",
            "rawInput": {"command": "ls"},
        }}})
        with patch("api.server._schedule_log", MagicMock()) as mock_log:
            _process_sse_block(block, state, text_parts, thinking_parts, log_events=True)
        # text_parts must be cleared (flushed) before the tool_call log
        assert text_parts == [], "text_parts should be cleared after flush"
        # _schedule_log called at least for EVT_TOOL_CALL
        call_event_types = [call.args[1] for call in mock_log.call_args_list]
        assert EVT_TOOL_CALL in call_event_types

    @pytest.mark.asyncio
    async def test_done_result_marks_turn_finished_and_flushes(self):
        """Done-result block with log_events=True clears text_parts and sets turn_completed_at."""
        state = _make_state()
        text_parts: list[str] = ["buffered text"]
        thinking_parts: list[str] = []
        block = _sse({"id": "rpc-123", "result": {"stopReason": "end_turn"}})
        with patch("api.server._schedule_log", MagicMock()):
            _process_sse_block(block, state, text_parts, thinking_parts, log_events=True)
        assert text_parts == [], "text_parts should be empty after done flush"
        assert state.turn_completed_at is not None, "turn_completed_at should be set"

    def test_done_result_log_events_false_clears_buffers_only(self):
        """Done-result with log_events=False clears buffers but does not mutate state."""
        state = _make_state()
        text_parts: list[str] = ["buffered"]
        thinking_parts: list[str] = ["thinking"]
        block = _sse({"id": "rpc-999", "result": {"stopReason": "end_turn"}})
        _process_sse_block(block, state, text_parts, thinking_parts, log_events=False)
        assert text_parts == []
        assert thinking_parts == []
        assert state.turn_completed_at is None

    @pytest.mark.asyncio
    async def test_error_frame_marks_turn_finished(self):
        """Error frame with log_events=True sets turn_completed_at."""
        state = _make_state()
        text_parts: list[str] = []
        thinking_parts: list[str] = []
        block = _sse({"id": "rpc-err", "error": {
            "code": -32000,
            "message": "agent died",
            "data": {"kind": "sandbox_process_died"},
        }})
        with patch("api.server._schedule_log", MagicMock()):
            _process_sse_block(block, state, text_parts, thinking_parts, log_events=True)
        assert state.turn_completed_at is not None
# ---------------------------------------------------------------------------
# Part 2: TestEventsEndpoint
# ---------------------------------------------------------------------------

class TestEventsEndpoint:

    @pytest.mark.asyncio
    async def test_events_subscriber_registered_on_connect(self):
        """Connecting to /events registers exactly one subscriber queue."""
        state = _make_state("sess-events-1")
        state._reader_alive = True  # prevent real SSE reader from firing
        session_id = state.session_id

        async def _stream_task():
            transport = httpx.ASGITransport(app=app)
            async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
                async with client.stream("GET", f"/sessions/{session_id}/events") as resp:
                    async for _ in resp.aiter_text():
                        pass

        task = asyncio.create_task(_stream_task())
        # Wait for subscriber registration
        for _ in range(50):
            if state._session_subscribers:
                break
            await asyncio.sleep(0.01)
        assert len(state._session_subscribers) == 1, "subscriber should be registered"
        # Terminate the stream
        state.broadcast(_SSE_SENTINEL)
        await asyncio.wait_for(task, timeout=3.0)

    @pytest.mark.asyncio
    async def test_events_streams_broadcast_data(self):
        """Broadcast data appears in the SSE stream."""
        state = _make_state("sess-events-2")
        state._reader_alive = True
        session_id = state.session_id
        collected: list[str] = []

        async def _stream_task():
            transport = httpx.ASGITransport(app=app)
            async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
                async with client.stream("GET", f"/sessions/{session_id}/events") as resp:
                    async for chunk in resp.aiter_text():
                        collected.append(chunk)

        task = asyncio.create_task(_stream_task())
        # Wait for subscriber
        for _ in range(50):
            if state._session_subscribers:
                break
            await asyncio.sleep(0.01)
        assert state._session_subscribers

        # Broadcast format is (tag, block) — tag=None means no rpc_id prefix.
        state.broadcast((None, "data: hello\n\n"))
        state.broadcast(_SSE_SENTINEL)

        await asyncio.wait_for(task, timeout=3.0)
        full_text = "".join(collected)
        assert "data: hello" in full_text

    @pytest.mark.asyncio
    async def test_events_subscriber_removed_on_disconnect(self):
        """Subscriber is removed from state._session_subscribers after stream ends."""
        state = _make_state("sess-events-3")
        state._reader_alive = True
        session_id = state.session_id

        async def _stream_task():
            transport = httpx.ASGITransport(app=app)
            async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
                async with client.stream("GET", f"/sessions/{session_id}/events") as resp:
                    async for _ in resp.aiter_text():
                        pass

        task = asyncio.create_task(_stream_task())
        for _ in range(50):
            if state._session_subscribers:
                break
            await asyncio.sleep(0.01)
        assert len(state._session_subscribers) == 1

        state.broadcast(_SSE_SENTINEL)
        await asyncio.wait_for(task, timeout=3.0)
        assert state._session_subscribers == [], "subscriber should be removed after disconnect"

    @pytest.mark.asyncio
    async def test_events_heartbeat_yields_comment(self):
        """None broadcast triggers a heartbeat SSE comment."""
        state = _make_state("sess-events-4")
        state._reader_alive = True
        session_id = state.session_id
        collected: list[str] = []

        async def _stream_task():
            transport = httpx.ASGITransport(app=app)
            async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
                async with client.stream("GET", f"/sessions/{session_id}/events") as resp:
                    async for chunk in resp.aiter_text():
                        collected.append(chunk)

        task = asyncio.create_task(_stream_task())
        for _ in range(50):
            if state._session_subscribers:
                break
            await asyncio.sleep(0.01)
        assert state._session_subscribers

        state.broadcast(None)  # heartbeat
        state.broadcast(_SSE_SENTINEL)

        await asyncio.wait_for(task, timeout=3.0)
        full_text = "".join(collected)
        assert ": heartbeat" in full_text
# ---------------------------------------------------------------------------
# Part 3: TestConcurrentStressMixed
# ---------------------------------------------------------------------------

class TestConcurrentStress:

    @pytest.mark.asyncio
    async def test_high_concurrency_all_accepted(self):
        """N=50 concurrent POSTs all return 200. Each rpc_id ends
        up in pending_prompts."""
        N = 50
        state = _make_state("sess-stress-2")

        with patch("api.server.log_event", _noop):
            responses = await asyncio.gather(
                *[_post_message_via_asgi("sess-stress-2", f"msg-{i}") for i in range(N)]
            )

        assert all(r.status_code == 200 for r in responses)
        rpc_ids = [r.json()["rpc_id"] for r in responses]
        assert len(set(rpc_ids)) == N
        queued_ids = {p.rpc_id for p in state.pending_prompts}
        for rid in rpc_ids:
            assert rid in queued_ids
# ---------------------------------------------------------------------------
# Interrupt (per-call)
# ---------------------------------------------------------------------------
class TestInterrupt:
    """interrupt=True on POST /message cancels the current prompt and waits
    for the agent to confirm cancellation before starting the new one."""

    @pytest.mark.asyncio
    async def test_no_cancel_when_idle(self):
        """When agent is idle, interrupt=True submits immediately without cancel."""
        state = _make_state("sess-int-idle")
        state.client.cancel_prompt = AsyncMock()
        with patch("api.server.log_event", _noop):
            resp = await _post_message_via_asgi("sess-int-idle", "hello", interrupt=True)
        assert resp.status_code == 200
        state.client.cancel_prompt.assert_not_called()

    @pytest.mark.asyncio
    async def test_cancel_fires_when_busy(self):
        """When agent is busy, interrupt=True fires cancel and waits for terminal."""
        state = _make_state("sess-int-busy")

        prompt_cancelled = asyncio.Event()

        async def slow_prompt(*a, **kw):
            await prompt_cancelled.wait()

        state.client.prompt = slow_prompt

        async def cancel_side_effect(*a, **kw):
            prompt_cancelled.set()

        state.client.cancel_prompt = AsyncMock(side_effect=cancel_side_effect)
        state._reader_alive = True; _start_session_tasks(state)

        with patch("api.server.log_event", _noop):
            resp1 = await _post_message_via_asgi("sess-int-busy", "first")
            assert resp1.status_code == 200
            await asyncio.sleep(0.05)
            assert state.agent_busy

            resp2 = await _post_message_via_asgi("sess-int-busy", "second", interrupt=True)
            assert resp2.status_code == 200
            state.client.cancel_prompt.assert_called_once()

        await _stop_scheduler(state)

    @pytest.mark.asyncio
    async def test_drain_waits_for_terminal_state(self):
        """interrupt=True waits for the prompt to reach a terminal state."""
        state = _make_state("sess-int-drain")
        drain_order = []

        prompt_cancelled = asyncio.Event()

        async def slow_prompt(*a, **kw):
            await prompt_cancelled.wait()
            drain_order.append("first_finished")

        state.client.prompt = slow_prompt

        async def cancel_side(*a, **kw):
            await asyncio.sleep(0.1)
            prompt_cancelled.set()

        state.client.cancel_prompt = AsyncMock(side_effect=cancel_side)
        state._reader_alive = True; _start_session_tasks(state)

        with patch("api.server.log_event", _noop):
            resp1 = await _post_message_via_asgi("sess-int-drain", "first")
            assert resp1.status_code == 200
            await asyncio.sleep(0.05)

            resp2 = await _post_message_via_asgi("sess-int-drain", "second", interrupt=True)
            assert resp2.status_code == 200
            drain_order.append("second_submitted")

        assert drain_order == ["first_finished", "second_submitted"]
        await _stop_scheduler(state)

    @pytest.mark.asyncio
    async def test_default_does_not_cancel(self):
        """Without interrupt flag, busy agent is never cancelled."""
        state = _make_state("sess-no-int")
        state.client.cancel_prompt = AsyncMock()
        # Simulate busy by setting active_rpc_id
        state.active_rpc_id = "rpc-first"

        with patch("api.server.log_event", _noop):
            resp2 = await _post_message_via_asgi("sess-no-int", "second")
            assert resp2.status_code == 200
            state.client.cancel_prompt.assert_not_called()

        state.active_rpc_id = None
        state.pending_prompts.clear()


class TestResumeRecovery:
    """Resumption flow for a reaped session.

    History: this used to patch ``api.server._do_resume``. That helper was
    removed in the volume-refactor and replaced with the
    ``ensure_sandbox`` / ``ensure_runtime`` pair (composed by
    ``ensure_session_live``). The test now drives ``ensure_runtime``
    directly with a stub sandbox and verifies that a fresh SessionState is
    returned / stored without issuing ACP ``session/load`` for an empty
    session.
    """

    @pytest.mark.asyncio
    async def test_empty_reaped_session_starts_fresh_inner_session(self):
        """Reaped session -> ensure_runtime rebuilds SESSIONS entry via AcpClient."""
        from api import server as server_mod
        from api.models import AgentConfig, AgentRecord, SandboxRecord, VolumeRecord

        created_clients = []

        class FakeAcpClient:
            def __init__(self, base_url: str):
                self.base_url = base_url
                self.inner_ids = {}
                self.sent_methods = []
                created_clients.append(self)

            def get_inner_session_id(self, session_id: str):
                return self.inner_ids.get(session_id)

            def set_inner_session_id(self, session_id: str, inner_id: str) -> None:
                self.inner_ids[session_id] = inner_id

            async def handshake(self, session_id: str, agent: str):
                self.sent_methods.append(("handshake", session_id, agent))
                return {}

            async def _send_rpc(self, session_id: str, method: str, params: dict, agent=None, rpc_id=None):
                self.sent_methods.append((method, session_id, params))
                return {}

            async def close_session(self, session_id: str) -> None:
                return None

            async def aclose(self) -> None:
                return None

            async def set_mode(self, session_id: str, mode: str) -> None:
                return None

        async def fake_apply(client, config, acp_session_id, cwd):
            client.set_inner_session_id(acp_session_id, "fresh-inner")

        # Simulate an empty reaped session: DB row has no inner_session_id,
        # so ensure_runtime takes the fresh-init branch (not session/load).
        session_row = {
            "id": "sess-1",
            "agent_id": "agent-1",
            "current_sandbox_id": "sbx-1",
            "volume_id": "vol-1",
            "inner_session_id": None,
            "cwd": "/tmp",
        }
        sandbox = SandboxRecord(
            id="sbx-1",
            provider="local",
            sandbox_ref="2469",
            status="running",
            volume_id="vol-1",
            subpath="agents/a/home",
            listen_port=2469,
        )
        volume = VolumeRecord(
            id="vol-1",
            name="vol",
            provider="local",
            provider_ref="vol-1",
            status="ready",
        )

        # Start from a clean SESSIONS/INSTANCES so ensure_runtime walks the
        # fresh-build branch (no healthy existing state to reuse).
        server_mod.SESSIONS.pop("sess-1", None)
        server_mod._INSTANCES.pop("sbx-1", None)

        with patch("api.server.get_agent", AsyncMock(return_value=AgentRecord(
            id="agent-1",
            name="agent",
            config=AgentConfig(agent_type="claude"),
        ))), patch(
            "api.server.get_volume",
            AsyncMock(return_value=volume),
        ), patch(
            "api.server._apply_config_and_initialize",
            side_effect=fake_apply,
        ), patch(
            "api.server.upsert_session",
            AsyncMock(),
        ), patch(
            "api.server._start_session_tasks",
        ), patch(
            "api.server.AcpClient",
            FakeAcpClient,
        ), patch(
            "api.server.allocate_sandbox_port",
            return_value=2469,
        ), patch(
            "api.server.free_sandbox_port",
        ), patch(
            "api.server._build_spawn_env_from_row",
            return_value={},
        ):
            # Call the internal helper directly — server_mod.ensure_runtime is
            # patched in the autouse fixture for the other tests in this module,
            # and ``_ensure_runtime_locked`` contains the real build-fresh logic.
            state = await server_mod._ensure_runtime_locked(session_row, sandbox)

        assert state is not None
        assert state.sandbox_id == "sbx-1"
        assert state.inner_session_id == "fresh-inner"
        assert server_mod.SESSIONS["sess-1"] is state
        # Empty reaped session must not issue session/load on the ACP client.
        assert created_clients, "ensure_runtime should have constructed an AcpClient"
        assert not any(
            method == "session/load" for method, *_ in created_clients[0].sent_methods
        )
