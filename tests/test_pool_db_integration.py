"""Phase 2 sub-task 3: integration test for the SessionPool wired to
real Postgres ``sessions.sandbox_state`` JSONB.

Mocks the SandboxSession (no daytona SDK calls). What this verifies:
  * ``load_sandbox_state`` reads the JSONB correctly
  * ``save_sandbox_state`` writes the JSONB without being clobbered by
    the phase-1 trigger
  * pool.get_session round-trips the state through DB
  * pool.release persists the snapshot fields
  * factory.make_session picks the right concrete class for state.type

This pins the contract that phase 2 sub-task 4 (the wiring of existing
recovery functions to the pool) builds on.
"""
from __future__ import annotations

import os
import sys
import uuid
from collections.abc import AsyncIterator
from typing import Any

import pytest
import pytest_asyncio

_SRC = os.path.join(os.path.dirname(__file__), "..", "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

_DB = os.environ.get("TEST_DATABASE_URL")
pytestmark = pytest.mark.skipif(_DB is None, reason="TEST_DATABASE_URL not set")
if _DB:
    os.environ["DATABASE_URL"] = _DB

from api import db as dbmod  # noqa: E402
from api.sandbox import (  # noqa: E402
    BaseSandboxSession,
    DaytonaSandboxState,
    Recipe,
    SessionPool,
    register,
    serialize,
)
from api.sandbox.db_bindings import load_sandbox_state, save_sandbox_state  # noqa: E402
from api.sandbox.factory import make_session  # noqa: E402


@pytest_asyncio.fixture
async def setup(clean_db):
    yield


# ---------------------------------------------------------------------------
# A test-only SandboxSession that records lifecycle calls without doing
# any actual provisioning. Registered for the duration of each test.
# ---------------------------------------------------------------------------


class _RecordingSession(BaseSandboxSession):
    instances: list["_RecordingSession"] = []

    def __init__(self, *, session_id, state):
        if not isinstance(state, DaytonaSandboxState):
            state = DaytonaSandboxState(recipe=state.recipe)
        super().__init__(session_id=session_id, state=state)
        self.calls: list[str] = []
        self._alive = False
        _RecordingSession.instances.append(self)

    async def start(self):
        self.calls.append("start")
        if self.state.sandbox_id is None:
            self.state.sandbox_id = "fake-sandbox-id"
        self.state.listen_port = 9100
        self._alive = True

    async def running(self):
        return self._alive

    async def execute_prompt(self, message: str) -> AsyncIterator[Any]:
        self.calls.append(f"prompt:{message}")
        yield {"type": "done"}

    async def stop(self):
        self.calls.append("stop")
        self.state.snapshot_path = "/vol/snap.tar"
        self.state.snapshot_version += 1
        self._alive = False

    async def shutdown(self):
        self.calls.append("shutdown")
        self._alive = False
        self._close_subscribers()


@pytest_asyncio.fixture
async def patched_factory():
    """Register _RecordingSession for this test's run; restore after."""
    from api.sandbox import factory as fac
    original = dict(fac._REGISTRY)
    register("daytona", fac._adapt(_RecordingSession))
    register("unknown", fac._adapt(_RecordingSession))
    _RecordingSession.instances = []
    yield
    fac._REGISTRY = original


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


async def _mk_session_row() -> tuple[str, str, str]:
    """Insert agent + volume + session row. Returns (sid, aid, vid)."""
    from api.models import AgentConfig, AgentRecord, VolumeRecord
    aid = f"a-{uuid.uuid4().hex[:8]}"
    vid = f"v-{uuid.uuid4().hex[:8]}"
    sid = f"s-{uuid.uuid4().hex[:8]}"
    await dbmod.upsert_agent(AgentRecord(
        id=aid, name="A", config=AgentConfig(agent_type="claude")))
    await dbmod.upsert_volume(VolumeRecord(
        id=vid, name=vid, provider="daytona", provider_ref="dt-vol"))
    async with dbmod.get_db() as conn:
        await conn.execute(
            "INSERT INTO sessions (id, agent_id, volume_id) VALUES (%s,%s,%s)",
            (sid, aid, vid),
        )
    return sid, aid, vid


# ---------------------------------------------------------------------------
# load / save round-trip
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_load_returns_trigger_populated_state(setup):
    """sandbox_state was populated by the phase-1 trigger; load reads it."""
    sid, _, _ = await _mk_session_row()
    payload = await load_sandbox_state(sid)
    assert payload is not None
    assert payload["type"] == "unknown"
    assert payload["recipe"]["agent_type"] == "claude"


@pytest.mark.asyncio
async def test_load_returns_none_for_missing_session(setup):
    payload = await load_sandbox_state("does-not-exist")
    assert payload is None


@pytest.mark.asyncio
async def test_save_round_trips_via_load(setup):
    """save then load returns the same payload."""
    sid, _, _ = await _mk_session_row()
    state = DaytonaSandboxState(
        sandbox_id="dt-x",
        listen_port=9100,
        snapshot_path="/vol/snap.tar",
        snapshot_version=3,
        recipe=Recipe(dockerfile="/df", shared_mounts=["a", "b"], agent_type="claude"),
    )
    await save_sandbox_state(sid, serialize(state))
    loaded = await load_sandbox_state(sid)
    assert loaded["sandbox_id"] == "dt-x"
    assert loaded["snapshot_path"] == "/vol/snap.tar"
    assert loaded["snapshot_version"] == 3
    assert loaded["recipe"]["shared_mounts"] == ["a", "b"]


# ---------------------------------------------------------------------------
# Pool wired to real DB
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_pool_get_session_starts_and_persists(setup, patched_factory):
    """First get_session: cold path, calls start(), persists state to DB."""
    sid, _, _ = await _mk_session_row()
    pool = SessionPool(
        factory=make_session,
        load_state=load_sandbox_state,
        save_state=save_sandbox_state,
    )
    session = await pool.get_session(sid)
    assert session.calls == ["start"]
    # State.sandbox_id was filled in by start(); pool persisted it.
    persisted = await load_sandbox_state(sid)
    assert persisted["sandbox_id"] == "fake-sandbox-id"


@pytest.mark.asyncio
async def test_pool_warm_reuse_does_not_re_start(setup, patched_factory):
    """Second get_session for same id: cached, no second start()."""
    sid, _, _ = await _mk_session_row()
    pool = SessionPool(
        factory=make_session,
        load_state=load_sandbox_state,
        save_state=save_sandbox_state,
    )
    s1 = await pool.get_session(sid)
    s2 = await pool.get_session(sid)
    assert s1 is s2
    assert s1.calls == ["start"]


@pytest.mark.asyncio
async def test_pool_release_snapshots_and_persists(setup, patched_factory):
    """release() calls stop(), persists the bumped snapshot_version, calls shutdown()."""
    sid, _, _ = await _mk_session_row()
    pool = SessionPool(
        factory=make_session,
        load_state=load_sandbox_state,
        save_state=save_sandbox_state,
    )
    s = await pool.get_session(sid)
    await pool.release(sid)
    assert s.calls == ["start", "stop", "shutdown"]
    persisted = await load_sandbox_state(sid)
    assert persisted["snapshot_path"] == "/vol/snap.tar"
    assert persisted["snapshot_version"] == 1
    assert not pool.has_active(sid)


@pytest.mark.asyncio
async def test_pool_re_get_after_release_starts_new_session(setup, patched_factory):
    """After release, next get_session cold-starts a fresh session.
    The persisted snapshot_path carries forward (cold-restore quality)."""
    sid, _, _ = await _mk_session_row()
    pool = SessionPool(
        factory=make_session,
        load_state=load_sandbox_state,
        save_state=save_sandbox_state,
    )
    s1 = await pool.get_session(sid)
    await pool.release(sid)
    s2 = await pool.get_session(sid)
    assert s2 is not s1
    assert s2.calls == ["start"]
    # The new session sees the persisted snapshot_path.
    assert s2.state.snapshot_path == "/vol/snap.tar"
    assert s2.state.snapshot_version == 1
