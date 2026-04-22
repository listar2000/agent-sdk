"""Integration tests for session/volume decoupling."""
from __future__ import annotations
import os, sys
import pytest
import pytest_asyncio
from unittest.mock import AsyncMock, MagicMock, patch
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
async def client():
    dbmod.init_db()
    await dbmod.init_pool()
    # Clean any leftovers.
    async with dbmod.get_db() as conn:
        await conn.execute("DELETE FROM session_log")
        await conn.execute("DELETE FROM sessions")
        await conn.execute("DELETE FROM sandboxes")
        await conn.execute("DELETE FROM volumes")
        await conn.execute("DELETE FROM agents")
    transport = ASGITransport(app=srv.app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        yield c
    await dbmod.close_pool()


@pytest.mark.asyncio
async def test_post_session_rejects_without_volume_id(client):
    from api.models import AgentConfig, AgentRecord
    await dbmod.upsert_agent(AgentRecord(id="agent_t", name="T", config=AgentConfig()))

    r = await client.post("/sessions", json={"agent_id": "agent_t"})
    # FastAPI's default validation returns 422 for missing required body fields.
    assert r.status_code in (400, 422), f"got {r.status_code}: {r.text}"


@pytest.mark.asyncio
async def test_post_session_does_not_provision_sandbox(client):
    from api.models import AgentConfig, AgentRecord, VolumeRecord
    await dbmod.upsert_agent(AgentRecord(id="agent_t2", name="T2", config=AgentConfig()))
    await dbmod.upsert_volume(VolumeRecord(id="vol_t", name="vt", provider="daytona",
                                           provider_ref="dt-t"))

    # Any provider provisioning during session create should fail the test.
    with patch("api.providers.create_daytona",
               new=AsyncMock(side_effect=AssertionError("should NOT provision during session create"))), \
         patch("api.providers.provision_daytona_sandbox",
               new=AsyncMock(side_effect=AssertionError("should NOT provision during session create"))):
        r = await client.post("/sessions",
                              json={"agent_id": "agent_t2", "volume_id": "vol_t"})
    assert r.status_code == 200, f"got {r.status_code}: {r.text}"
    body = r.json()
    # Response should include the volume_id and either null current_sandbox_id or no sandbox_id at all.
    assert body.get("volume_id") == "vol_t"
    # current_sandbox_id may be named differently in the response, but we check via the DB:
    sess = await dbmod.get_session(body["id"])
    assert sess is not None
    assert sess.get("current_sandbox_id") is None
    assert sess["volume_id"] == "vol_t"


@pytest.mark.asyncio
async def test_message_lazily_provisions_sandbox(client):
    """First /message on a volume-only session provisions a Daytona sandbox."""
    from api.models import AgentConfig, AgentRecord, VolumeRecord

    await dbmod.upsert_agent(AgentRecord(id="agent_r", name="R", config=AgentConfig()))
    await dbmod.upsert_volume(VolumeRecord(id="vol_r", name="vr", provider="daytona",
                                           provider_ref="dt-r"))
    r = await client.post("/sessions", json={"agent_id": "agent_r", "volume_id": "vol_r"})
    assert r.status_code == 200
    sid = r.json()["id"]

    # Track what create_daytona is called with.
    create_calls = []
    from api.providers import ProviderInstance
    async def fake_create(**kwargs):
        create_calls.append(kwargs)
        return ProviderInstance(
            provider="daytona", url="http://fake-sandbox:7000",
            root="/home/daytona", sandbox_id=f"sb-new-{len(create_calls)}",
        )
    # Also short-circuit anything downstream that tries to hit the fake URL.
    async def fake_wait_for_health(*a, **kw):
        return True

    with patch("api.providers.provision_daytona_sandbox", new=AsyncMock(side_effect=fake_create)), \
         patch("api.providers._wait_for_health", new=AsyncMock(side_effect=fake_wait_for_health)), \
         patch("api.server._start_sse_reader", MagicMock(return_value=None)), \
         patch("api.server._submit_prompt", MagicMock(return_value=None)), \
         patch("api.server._do_resume", new=AsyncMock(return_value=None)), \
         patch("api.server.start_supervisor_in_sandbox", new=AsyncMock(return_value=None)):
        r = await client.post(f"/sessions/{sid}/message", json={"message": "hi"})
    # The /message call may fail at the "not connected" stage (no real ACP client)
    # but by that point the lazy provision must have already happened. Either way,
    # we verify: create_daytona was called with the expected volume_id + subpath.
    assert len(create_calls) == 1, f"expected 1 create, got {len(create_calls)}: {create_calls}"
    assert create_calls[0].get("volume_id") == "dt-r"
    assert create_calls[0].get("subpath") == "agents/agent_r/home"

    # DB was updated: session has a current_sandbox_id.
    sess = await dbmod.get_session(sid)
    assert sess["current_sandbox_id"] is not None


@pytest.mark.asyncio
async def test_start_sandbox_provisions_eagerly(client):
    from api.models import AgentConfig, AgentRecord, VolumeRecord
    from api.providers import ProviderInstance
    await dbmod.upsert_agent(AgentRecord(id="agent_s", name="S", config=AgentConfig()))
    await dbmod.upsert_volume(VolumeRecord(id="vol_s", name="vs", provider="daytona",
                                           provider_ref="dt-s"))
    r = await client.post("/sessions", json={"agent_id": "agent_s", "volume_id": "vol_s"})
    sid = r.json()["id"]

    async def fake_create(**kw):
        return ProviderInstance(provider="daytona", url="http://fake:7000",
                                root="/home/daytona", sandbox_id="sb-eager-1")

    with patch("api.providers.provision_daytona_sandbox", new=AsyncMock(side_effect=fake_create)):
        r = await client.post(f"/sessions/{sid}/start-sandbox")
    assert r.status_code == 200, f"got {r.status_code}: {r.text}"
    body = r.json()
    assert "sandbox_id" in body and body["sandbox_id"] is not None

    sess = await dbmod.get_session(sid)
    assert sess["current_sandbox_id"] is not None


@pytest.mark.asyncio
async def test_stop_sandbox_clears_pointer(client):
    from api.models import AgentConfig, AgentRecord, VolumeRecord, SandboxRecord
    await dbmod.upsert_agent(AgentRecord(id="agent_k", name="K", config=AgentConfig()))
    await dbmod.upsert_volume(VolumeRecord(id="vol_k", name="vk", provider="daytona",
                                           provider_ref="dt-k"))
    r = await client.post("/sessions", json={"agent_id": "agent_k", "volume_id": "vol_k"})
    sid = r.json()["id"]
    sb = SandboxRecord(id="sb_k", provider="daytona", sandbox_ref="dt-sb-k",
                       status="running", root="/home/daytona",
                       volume_id="vol_k", subpath="agents/agent_k/home")
    await dbmod.upsert_sandbox(sb)
    await dbmod.set_session_current_sandbox(sid, "sb_k")

    with patch("api.providers.destroy_daytona", new=AsyncMock(return_value=None)):
        r = await client.post(f"/sessions/{sid}/stop-sandbox")
    assert r.status_code == 204, f"got {r.status_code}: {r.text}"
    sess = await dbmod.get_session(sid)
    assert sess["current_sandbox_id"] is None
    # Sandbox row should also be gone.
    assert await dbmod.get_sandbox("sb_k") is None


@pytest.mark.asyncio
async def test_reset_sandbox_swaps(client):
    from api.models import AgentConfig, AgentRecord, VolumeRecord, SandboxRecord
    from api.providers import ProviderInstance
    await dbmod.upsert_agent(AgentRecord(id="agent_x", name="X", config=AgentConfig()))
    await dbmod.upsert_volume(VolumeRecord(id="vol_x", name="vx", provider="daytona",
                                           provider_ref="dt-x"))
    r = await client.post("/sessions", json={"agent_id": "agent_x", "volume_id": "vol_x"})
    sid = r.json()["id"]
    sb_old = SandboxRecord(id="sb_old", provider="daytona", sandbox_ref="dt-old",
                           status="running", root="/home/daytona",
                           volume_id="vol_x", subpath="agents/agent_x/home")
    await dbmod.upsert_sandbox(sb_old)
    await dbmod.set_session_current_sandbox(sid, "sb_old")

    async def fake_create(**kw):
        return ProviderInstance(provider="daytona", url="http://fake:7000",
                                root="/home/daytona", sandbox_id="dt-new")

    with patch("api.providers.destroy_daytona", new=AsyncMock(return_value=None)), \
         patch("api.providers.provision_daytona_sandbox", new=AsyncMock(side_effect=fake_create)):
        r = await client.post(f"/sessions/{sid}/reset-sandbox")
    assert r.status_code == 200
    body = r.json()
    assert body["sandbox_id"] != "sb_old"
    # Old row gone, new row present
    assert await dbmod.get_sandbox("sb_old") is None
    sess = await dbmod.get_session(sid)
    assert sess["current_sandbox_id"] == body["sandbox_id"]


@pytest.mark.asyncio
async def test_reset_sandbox_emits_reattach_event(client):
    from api.models import AgentConfig, AgentRecord, VolumeRecord, SandboxRecord
    from api.providers import ProviderInstance
    await dbmod.upsert_agent(AgentRecord(id="agent_e", name="E", config=AgentConfig()))
    await dbmod.upsert_volume(VolumeRecord(id="vol_e", name="ve", provider="daytona",
                                           provider_ref="dt-e"))
    r = await client.post("/sessions", json={"agent_id": "agent_e", "volume_id": "vol_e"})
    sid = r.json()["id"]
    sb_old = SandboxRecord(id="sb_old", provider="daytona", sandbox_ref="dt-old",
                           status="running", root="/home/daytona",
                           volume_id="vol_e", subpath="agents/agent_e/home")
    await dbmod.upsert_sandbox(sb_old)
    await dbmod.set_session_current_sandbox(sid, "sb_old")

    async def fake_create(**kw):
        return ProviderInstance(provider="daytona", url="http://fake:7000",
                                root="/home/daytona", sandbox_id="dt-new")

    with patch("api.providers.destroy_daytona", new=AsyncMock(return_value=None)), \
         patch("api.providers.provision_daytona_sandbox", new=AsyncMock(side_effect=fake_create)):
        r = await client.post(f"/sessions/{sid}/reset-sandbox")
    assert r.status_code == 200

    # Look up session_log for a sandbox_reattach event.
    async with dbmod.get_db() as conn:
        rows = await (await conn.execute(
            "SELECT event_type, payload FROM session_log WHERE session_id = %s",
            (sid,),
        )).fetchall()
    reattach = [r for r in rows if r["event_type"] == "sandbox_reattach"]
    assert len(reattach) == 1, f"expected 1 reattach event, got {len(reattach)}: {rows}"
    payload = reattach[0]["payload"]
    assert payload.get("old_sandbox_id") == "sb_old"
    assert payload.get("new_sandbox_id") is not None
    assert payload["new_sandbox_id"] != "sb_old"
