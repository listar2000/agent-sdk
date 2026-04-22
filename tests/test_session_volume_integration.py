"""Integration tests for session/volume decoupling."""
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
