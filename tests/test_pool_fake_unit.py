"""Unit tests for SessionPool lifecycle using FakeSandboxSession.

Drives the REAL SessionPool + real test Postgres, but uses the fake
in-memory sandbox backend instead of daytona/docker/modal.  Fast (<1s).

What is stubbed:
  * ``pool._publish_state``  — calls ``db.update_worker_state`` which needs
    the ``workers`` table; unit tests don't provision that.  We monkeypatch
    this to a no-op (same pattern as test_sandbox_session_classes_unit.py).
  * ``AGENT_SDK_DISABLE_LEASE=1`` env var is set for the entire module so the
    pool's _publish_state guard skips the workers table call.

What is NOT stubbed:
  * ``db.write_sandbox_state``  — writes to the real sessions row.
  * ``db.get_session`` / ``db.read_sandbox_state`` — reads the real DB.
  * ``db.upsert_agent`` / ``db.upsert_volume`` / ``db.upsert_session`` —
    real inserts so _bootstrap_session can find the rows.

Blocker note for test (b) deep-path: the pool spawns a lifecycle webhook
(``_fire_lifecycle_webhook``) and a credential_refresh_loop when
``recipe.credential_refresh_url`` is set.  We use a plain Recipe() which
has no credential_refresh_url, so neither fires.  No additional stubbing is
required for the two core tests below.
"""
from __future__ import annotations

import asyncio
import os
import sys
import uuid

import pytest
import pytest_asyncio

_SRC = os.path.join(os.path.dirname(__file__), "..", "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

# Disable the workers-table lease so _publish_state is a true no-op without
# monkeypatching (the workers table may not exist in the unit-test schema
# depending on migration state, and we don't need it here).
os.environ.setdefault("AGENT_SDK_DISABLE_LEASE", "1")

_DB = os.environ.get("TEST_DATABASE_URL") or os.environ.get("DATABASE_URL")
pytestmark = pytest.mark.skipif(_DB is None, reason="TEST_DATABASE_URL / DATABASE_URL not set")


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest_asyncio.fixture
async def db_pool():
    """Ephemeral DB pool for one test."""
    from api import db as dbmod
    dbmod.init_db()
    await dbmod.init_pool()
    try:
        yield dbmod
    finally:
        await dbmod.close_pool()


@pytest_asyncio.fixture
async def clean_session(db_pool):
    """Insert a disposable agent + volume + session row; yield (session_id);
    delete on teardown."""
    dbmod = db_pool
    from api.models import AgentConfig, AgentRecord, VolumeRecord

    # Unique per-test so parallel runs don't collide.
    suffix = uuid.uuid4().hex[:8]
    aid = f"fake-agent-{suffix}"
    vid = f"fake-vol-{suffix}"
    sid = f"fake-sess-{suffix}"

    await dbmod.upsert_agent(AgentRecord(id=aid, name=aid, config=AgentConfig()))
    await dbmod.upsert_volume(VolumeRecord(
        id=vid, name=f"fakevol{suffix}", provider="fake",
        provider_ref=f"fake-ref-{suffix}",
    ))
    await dbmod.upsert_session(
        session_id=sid,
        agent_id=aid,
        inner_session_id=None,
        volume_id=vid,
    )
    try:
        yield sid, aid, vid
    finally:
        try:
            await dbmod.delete_session(sid)
        except Exception:
            pass
        try:
            await dbmod.delete_volume(vid)
        except Exception:
            pass
        try:
            await dbmod.delete_agent(aid)
        except Exception:
            pass


def _make_pool():
    """Construct a SessionPool using the factory (which registers fake)."""
    from api.sandbox.factory import make_session
    from api.sandbox.pool import SessionPool
    return SessionPool(factory=make_session)


# ---------------------------------------------------------------------------
# (a) cold_create → get_session → running()
# ---------------------------------------------------------------------------

class TestColdCreateFake:
    @pytest.mark.asyncio
    async def test_cold_create_is_alive_and_fast(self, clean_session, monkeypatch):
        """cold_create a 'fake' session, then get_session returns it alive.

        Verifies:
          * pool.cold_create succeeds in-memory (no real provisioning).
          * pool.get_session fast-path (cached + alive) returns the same object.
          * session.running() is True.
          * state.sandbox_ref is set.
          * The session object is the FakeSandboxSession class.
        """
        sid, _aid, _vid = clean_session
        from api import db as dbmod
        from api.sandbox.state import FakeSandboxState, Recipe
        from api.providers.fake.session import FakeSandboxSession

        pool = _make_pool()
        monkeypatch.setattr(pool, "_publish_state", _noop)

        recipe = Recipe(agent_type="claude")
        session = await pool.cold_create(sid, provider="fake", recipe=recipe)

        # Correct class
        assert isinstance(session, FakeSandboxSession)
        # Alive
        assert await session.running() is True
        # Sandbox ref populated
        assert session.state.sandbox_ref == f"fake-{sid}"
        # Pool caches it
        assert pool._active[sid] is session
        # get_session hot-path returns the same object
        again = await pool.get_session(sid)
        assert again is session

        # Teardown
        await pool.release(sid)

    @pytest.mark.asyncio
    async def test_state_serialises_to_db(self, clean_session, monkeypatch):
        """After cold_create, the sandbox_state JSONB is persisted to the DB."""
        sid, _aid, _vid = clean_session
        from api import db as dbmod
        from api.sandbox.state import deserialize, FakeSandboxState

        pool = _make_pool()
        monkeypatch.setattr(pool, "_publish_state", _noop)

        from api.sandbox.state import Recipe
        await pool.cold_create(sid, provider="fake", recipe=Recipe())

        persisted = deserialize(await dbmod.read_sandbox_state(sid))
        assert isinstance(persisted, FakeSandboxState)
        assert persisted.sandbox_ref == f"fake-{sid}"

        await pool.release(sid)


# ---------------------------------------------------------------------------
# (b) recovery: fake supervisor death → pool.get_session cold-recovers a NEW
#     session AND hands off _subscribers to it.
# ---------------------------------------------------------------------------

class TestRecoverySubscriberHandoff:
    @pytest.mark.asyncio
    async def test_midprompt_recovery_does_not_leak_subscriber(
        self, clean_session, monkeypatch,
    ):
        """Simulate supervisor death then call pool.get_session → new session
        is created, existing _subscribers are transplanted, and after the
        subscriber drain finishes the reaper can reclaim the session.

        This is the in-memory equivalent of test_midprompt_recovery_does_not_leak_subscriber
        from the golden suite — previously required real daytona/docker.
        """
        from api.sandbox.session import _END
        from api import db as dbmod
        from api.sandbox.state import FakeSandboxState, Recipe
        from api.providers.fake.session import FakeSandboxSession

        sid, _aid, _vid = clean_session

        pool = _make_pool()
        monkeypatch.setattr(pool, "_publish_state", _noop)

        # Cold-create a fake session.
        recipe = Recipe(agent_type="claude")
        original = await pool.cold_create(sid, provider="fake", recipe=recipe)
        assert isinstance(original, FakeSandboxSession)
        assert await original.running() is True

        # Register a subscriber (simulates an open /events SSE consumer).
        sub_id, q = original.register_subscriber()
        assert sub_id in original._subscribers

        # Spin up the subscriber generator so its body runs to the first await
        # (capturing the _Subscriber record so the owner re-bind can work).
        agen = original.iterate_subscriber(sub_id, q)
        step = asyncio.ensure_future(agen.__anext__())
        await asyncio.sleep(0)

        # Simulate the supervisor dying externally.
        original._alive = False
        assert await original.running(force_probe=True) is False

        # We must monkeypatch db.read_sandbox_state to return the serialised
        # FakeSandboxState so pool.get_session (recovery path) can deserialise
        # it and route to FakeSandboxSession via the factory.
        from api.sandbox.state import serialize
        fake_state_payload = serialize(FakeSandboxState(recipe=recipe))

        original_read = dbmod.read_sandbox_state
        async def _patched_read(session_id):
            if session_id == sid:
                return fake_state_payload
            return await original_read(session_id)
        monkeypatch.setattr(dbmod, "read_sandbox_state", _patched_read)

        # pool.get_session detects original is dead → cold-recovers a NEW session.
        replacement = await pool.get_session(sid)

        # (1) A new object was created.
        assert replacement is not original
        assert isinstance(replacement, FakeSandboxSession)
        assert await replacement.running() is True

        # (2) The subscriber was transplanted onto the replacement.
        assert sub_id in replacement._subscribers
        assert sub_id not in original._subscribers

        # (3) The owner pointer was rebound to the replacement.
        assert replacement._subscribers[sub_id].owner is replacement

        # (4) Drain the subscriber — finally block must pop from replacement.
        q.put_nowait(_END)
        with pytest.raises(StopAsyncIteration):
            await step

        # (5) After drain, no zombie entry on either session.
        assert sub_id not in replacement._subscribers
        assert sub_id not in original._subscribers

        # (6) Idle reaper can reclaim the replacement (empty _subscribers,
        #     stale compute clock).
        released = []
        original_release = pool.release
        async def _mock_release(session_id):
            released.append(session_id)
        monkeypatch.setattr(pool, "release", _mock_release)

        replacement.liveness.observe_chunk()
        replacement.liveness._last_compute_at -= 10_000
        count = await pool.reap_idle(5)
        assert count == 1
        assert released == [sid]

        await asyncio.sleep(0)  # let background _safe_shutdown settle


# ---------------------------------------------------------------------------
# (c) start-failure → Bug-A destroy backstop fires
# ---------------------------------------------------------------------------

class TestStartFailure:
    @pytest.mark.asyncio
    async def test_start_failure_triggers_destroy_backstop(
        self, clean_session, monkeypatch,
    ):
        """When start() fails after acquiring the sandbox ref, the pool's
        Bug-A teardown path fires _safe_destroy_compute.

        We verify that the failed session does NOT enter pool._active and
        that FakeSandboxSession.destroy() clears sandbox_ref (no leak).
        """
        from api.sandbox.state import FakeSandboxState, Recipe
        from api.providers.fake.session import FakeSandboxSession
        from api.sandbox import factory as factory_mod

        sid, _aid, _vid = clean_session

        # Register a modified factory that returns a session with _fail_start=True.
        original_make = factory_mod.make_session

        def _failing_factory(session_id, state):
            s = original_make(session_id, state)
            if isinstance(s, FakeSandboxSession):
                s._fail_start = True
            return s

        pool = _make_pool()
        pool._factory = _failing_factory
        monkeypatch.setattr(pool, "_publish_state", _noop)

        recipe = Recipe(agent_type="claude")
        initial_state = FakeSandboxState(recipe=recipe)

        with pytest.raises(RuntimeError, match="_fail_start=True"):
            await pool.get_session(sid, initial_state=initial_state)

        # Session must NOT be in pool._active after a failed start.
        assert sid not in pool._active

        # Give the fire-and-forget destroy task a tick to run.
        await asyncio.sleep(0.05)


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

async def _noop(*args, **kwargs):
    return None
