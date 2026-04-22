"""Unit tests for ensure_sandbox / ensure_runtime helpers."""
from __future__ import annotations
import os, sys
import pytest
import pytest_asyncio
from unittest.mock import AsyncMock, patch
from httpx import ASGITransport, AsyncClient

_SRC = os.path.join(os.path.dirname(__file__), "..", "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

_DB = os.environ.get("TEST_DATABASE_URL")
pytestmark = pytest.mark.skipif(_DB is None, reason="TEST_DATABASE_URL not set")
if _DB:
    os.environ["DATABASE_URL"] = _DB

from api import db as dbmod, server as srv  # noqa: E402


@pytest_asyncio.fixture
async def setup():
    dbmod.init_db()
    await dbmod.init_pool()
    async with dbmod.get_db() as conn:
        await conn.execute("DELETE FROM session_log")
        await conn.execute("DELETE FROM sessions")
        await conn.execute("DELETE FROM sandboxes")
        await conn.execute("DELETE FROM volumes")
        await conn.execute("DELETE FROM agents")
    yield
    await dbmod.close_pool()


async def _mk_fixtures():
    from api.models import AgentConfig, AgentRecord, VolumeRecord
    await dbmod.upsert_agent(AgentRecord(id="a1", name="A",
                                         config=AgentConfig(agent_type="claude")))
    await dbmod.upsert_volume(VolumeRecord(id="v1", name="v", provider="daytona",
                                           provider_ref="dt-v"))
    async with dbmod.get_db() as conn:
        await conn.execute(
            "INSERT INTO sessions (id, agent_id, volume_id) VALUES (%s,%s,%s)",
            ("s1", "a1", "v1"),
        )


@pytest.mark.asyncio
async def test_ensure_sandbox_creates_when_none(setup):
    await _mk_fixtures()
    from api.providers import ProviderInstance
    created = []

    async def fake_provision(**kw):
        created.append(kw)
        return ProviderInstance(provider="daytona", url="http://fake",
                                root="/home/daytona", sandbox_id=f"dt-{len(created)}")

    with patch("api.providers.provision_daytona_sandbox", new=AsyncMock(side_effect=fake_provision)):
        sess = await dbmod.get_session("s1")
        sb = await srv.ensure_sandbox(sess)

    assert sb is not None
    assert len(created) == 1
    assert created[0]["volume_id"] == "dt-v"
    assert created[0]["subpath"] == "agents/a1/home"


@pytest.mark.asyncio
async def test_ensure_sandbox_returns_existing_when_running(setup):
    await _mk_fixtures()
    from api.models import SandboxRecord
    sb = SandboxRecord(id="sb1", provider="daytona", sandbox_ref="dt-live",
                       status="running", root="/home/daytona",
                       volume_id="v1", subpath="agents/a1/home")
    await dbmod.upsert_sandbox(sb)
    await dbmod.set_session_current_sandbox("s1", "sb1")

    with patch("api.providers.get_daytona_sandbox_status",
               new=AsyncMock(return_value="running")), \
         patch("api.providers.provision_daytona_sandbox",
               new=AsyncMock(side_effect=AssertionError("should not provision"))):
        sess = await dbmod.get_session("s1")
        got = await srv.ensure_sandbox(sess)
    assert got.id == "sb1"


@pytest.mark.asyncio
async def test_ensure_sandbox_reprovisions_when_missing_emits_reattach(setup):
    await _mk_fixtures()
    # Create a sandbox row and point the session at it, then patch get_sandbox
    # to return None — simulating the case where the row was deleted externally
    # (e.g. by the reaper) in a way that bypassed the ON DELETE SET NULL trigger.
    from api.models import SandboxRecord
    dead = SandboxRecord(id="sb_dead", provider="daytona", sandbox_ref="dt-dead",
                         status="running", root="/home/daytona",
                         volume_id="v1", subpath="agents/a1/home")
    await dbmod.upsert_sandbox(dead)
    await dbmod.set_session_current_sandbox("s1", "sb_dead")

    from api.providers import ProviderInstance
    async def fake_provision(**kw):
        return ProviderInstance(provider="daytona", url="http://new",
                                root="/home/daytona", sandbox_id="dt-new")

    # Patch get_sandbox to simulate the row being gone after the FK pointer was set.
    original_get_sandbox = dbmod.get_sandbox
    async def fake_get_sandbox(sb_id):
        if sb_id == "sb_dead":
            return None
        return await original_get_sandbox(sb_id)

    with patch("api.providers.provision_daytona_sandbox", new=AsyncMock(side_effect=fake_provision)), \
         patch("api.db.get_sandbox", new=AsyncMock(side_effect=fake_get_sandbox)), \
         patch("api.server.get_sandbox", new=AsyncMock(side_effect=fake_get_sandbox)):
        sess = await dbmod.get_session("s1")
        sb = await srv.ensure_sandbox(sess)

    assert sb.sandbox_ref == "dt-new"
    async with dbmod.get_db() as conn:
        rows = await (await conn.execute(
            "SELECT event_type, payload FROM session_log WHERE session_id='s1'"
        )).fetchall()
    reattach = [r for r in rows if r["event_type"] == "sandbox_reattach"]
    assert len(reattach) == 1
    assert reattach[0]["payload"]["old_sandbox_id"] == "sb_dead"


@pytest.mark.asyncio
async def test_ensure_sandbox_starts_stopped(setup):
    await _mk_fixtures()
    from api.models import SandboxRecord
    sb = SandboxRecord(id="sb1", provider="daytona", sandbox_ref="dt-paused",
                       status="stopped", root="/home/daytona",
                       volume_id="v1", subpath="agents/a1/home")
    await dbmod.upsert_sandbox(sb)
    await dbmod.set_session_current_sandbox("s1", "sb1")

    start_calls = []
    async def fake_start(ref):
        start_calls.append(ref)

    with patch("api.providers.get_daytona_sandbox_status",
               new=AsyncMock(return_value="stopped")), \
         patch("api.providers.start_daytona",
               new=AsyncMock(side_effect=fake_start)), \
         patch("api.providers.provision_daytona_sandbox",
               new=AsyncMock(side_effect=AssertionError("should not provision"))):
        sess = await dbmod.get_session("s1")
        got = await srv.ensure_sandbox(sess)
    assert got.id == "sb1"
    assert start_calls == ["dt-paused"]


@pytest.mark.asyncio
async def test_ensure_runtime_reuses_healthy_state(setup):
    await _mk_fixtures()
    from api.models import SandboxRecord, SessionState
    sb = SandboxRecord(id="sb1", provider="daytona", sandbox_ref="dt-r",
                       status="running", root="/home/daytona",
                       volume_id="v1", subpath="agents/a1/home")
    await dbmod.upsert_sandbox(sb)
    await dbmod.set_session_current_sandbox("s1", "sb1")

    # Pre-populate in-memory state
    fake_client = AsyncMock()
    fake_client.base_url = "http://existing"
    state = SessionState(session_id="s1", agent_id="a1", sandbox_id="sb1",
                         acp_session_id="acp1", inner_session_id="inner1",
                         agent_type="claude", client=fake_client,
                         supervisor_url="http://existing")
    srv.SESSIONS["s1"] = state

    with patch("api.providers._wait_for_health",
               new=AsyncMock(return_value=True)):
        sess = await dbmod.get_session("s1")
        got = await srv.ensure_runtime(sess, sb)

    assert got is state  # identity — no rebuild


@pytest.mark.asyncio
async def test_ensure_runtime_rebuilds_when_missing(setup):
    await _mk_fixtures()
    from api.models import SandboxRecord
    sb = SandboxRecord(id="sb1", provider="daytona", sandbox_ref="dt-r",
                       status="running", root="/home/daytona",
                       volume_id="v1", subpath="agents/a1/home")
    await dbmod.upsert_sandbox(sb)
    await dbmod.set_session_current_sandbox("s1", "sb1")

    srv.SESSIONS.pop("s1", None)  # no in-memory state

    fake_client = AsyncMock()
    with patch("api.server.start_supervisor_in_sandbox",
               new=AsyncMock(return_value=("http://fresh", 9100))), \
         patch("api.server.AcpClient", return_value=fake_client), \
         patch("api.server._start_session_tasks"):
        fake_client.initialize = AsyncMock(return_value={"sessionId": "inner-new"})
        fake_client.get_inner_session_id = lambda *a: "inner-new"

        sess = await dbmod.get_session("s1")
        got = await srv.ensure_runtime(sess, sb)

    assert got.session_id == "s1"
    assert got.sandbox_id == "sb1"
    assert got.supervisor_url == "http://fresh"
