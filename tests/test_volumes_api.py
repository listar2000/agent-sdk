"""REST tests for /volumes endpoints. DB required; provider calls stubbed."""
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
    transport = ASGITransport(app=srv.app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        yield c
    await dbmod.close_pool()


@pytest.mark.asyncio
async def test_post_volume_creates_row(client):
    with patch("api.providers.create_daytona_volume",
               new=AsyncMock(return_value="dt-fake-ref")):
        r = await client.post("/volumes", json={"name": "proj-api-test", "provider": "daytona"})
    assert r.status_code == 200
    body = r.json()
    assert body["name"] == "proj-api-test"
    assert body["provider_ref"] == "dt-fake-ref"
    assert body["status"] == "ready"


@pytest.mark.asyncio
async def test_get_and_list_volumes(client):
    with patch("api.providers.create_daytona_volume",
               new=AsyncMock(return_value="dt-r1")):
        await client.post("/volumes", json={"name": "p1", "provider": "daytona"})

    r = await client.get("/volumes")
    assert r.status_code == 200
    assert any(v["name"] == "p1" for v in r.json())

    r = await client.get("/volumes/p1")
    assert r.status_code == 200
    assert r.json()["name"] == "p1"


@pytest.mark.asyncio
async def test_delete_volume(client):
    with patch("api.providers.create_daytona_volume",
               new=AsyncMock(return_value="dt-del")), \
         patch("api.providers.delete_daytona_volume",
               new=AsyncMock(return_value=None)):
        await client.post("/volumes", json={"name": "to-delete", "provider": "daytona"})
        r = await client.delete("/volumes/to-delete")
    assert r.status_code == 204
    r = await client.get("/volumes/to-delete")
    assert r.status_code == 404


@pytest.mark.asyncio
async def test_delete_volume_conflict_if_session_exists(client):
    """DELETE returns 409 if a session references the volume; force=true cascades."""
    from api.models import AgentConfig, AgentRecord
    await dbmod.upsert_agent(AgentRecord(id="a1", name="A1", config=AgentConfig()))
    with patch("api.providers.create_daytona_volume",
               new=AsyncMock(return_value="dt-c")):
        await client.post("/volumes", json={"name": "conflict", "provider": "daytona"})
    v = await dbmod.get_volume_by_name("conflict")
    # Insert a session referencing this volume directly.
    async with dbmod.get_db() as conn:
        await conn.execute(
            "INSERT INTO sessions (id, agent_id, volume_id) VALUES (%s, %s, %s)",
            ("sess_1", "a1", v.id),
        )
    r = await client.delete("/volumes/conflict")
    assert r.status_code == 409

    # With force=true the referenced session is deleted and the volume too.
    with patch("api.providers.delete_daytona_volume",
               new=AsyncMock(return_value=None)):
        r = await client.delete("/volumes/conflict?force=true")
    assert r.status_code == 204
    # Volume gone.
    r = await client.get("/volumes/conflict")
    assert r.status_code == 404
    # Session gone.
    async with dbmod.get_db() as conn:
        row = await (await conn.execute(
            "SELECT id FROM sessions WHERE id = %s", ("sess_1",)
        )).fetchone()
    assert row is None


@pytest.mark.asyncio
async def test_provision_volume_waits_for_ready(client):
    with patch("api.providers.create_daytona_volume",
               new=AsyncMock(return_value="dt-prov")):
        r = await client.post("/volumes/provision",
                              json={"name": "prov-test", "provider": "daytona"})
    assert r.status_code == 200
    assert r.json()["status"] == "ready"
