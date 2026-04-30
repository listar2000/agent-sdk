"""Unit test: POST /sessions/{id}/release routes through SessionPool
and returns snapshot pointer info.

Mocks ``pool.release`` so the test stays fast and never talks to a
provider. State pre-population uses an UPDATE so the dual-write trigger
(which fires on UPDATE OF current_sandbox_id, not sandbox_state) doesn't
clobber what we want to read back.
"""
from __future__ import annotations

import os
import sys
import uuid
from unittest.mock import AsyncMock, patch

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

_SRC = os.path.join(os.path.dirname(__file__), "..", "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

_DB = os.environ.get("TEST_DATABASE_URL")
pytestmark = pytest.mark.skipif(_DB is None, reason="TEST_DATABASE_URL not set")
if _DB:
    os.environ["DATABASE_URL"] = _DB

from api import db as dbmod  # noqa: E402
from api import server as srv  # noqa: E402


@pytest_asyncio.fixture
async def setup(clean_db):
    yield


async def _mk_session_with_state(
    snapshot_path: str | None = None,
    snapshot_version: int = 0,
) -> str:
    from psycopg.types.json import Json

    from api.models import AgentConfig, AgentRecord, VolumeRecord
    from api.sandbox import DaytonaSandboxState, Recipe, serialize

    aid = f"a-{uuid.uuid4().hex[:8]}"
    vid = f"v-{uuid.uuid4().hex[:8]}"
    sid = f"s-{uuid.uuid4().hex[:8]}"
    await dbmod.upsert_agent(AgentRecord(
        id=aid, name="A", config=AgentConfig(agent_type="claude")))
    await dbmod.upsert_volume(VolumeRecord(
        id=vid, name=vid, provider="daytona", provider_ref="dt-vol"))
    state = DaytonaSandboxState(
        snapshot_path=snapshot_path, snapshot_version=snapshot_version,
        recipe=Recipe(agent_type="claude"),
    )
    async with dbmod.get_db() as conn:
        await conn.execute(
            "INSERT INTO sessions (id, agent_id, volume_id) VALUES (%s,%s,%s)",
            (sid, aid, vid),
        )
        await conn.execute(
            "UPDATE sessions SET sandbox_state = %s WHERE id = %s",
            (Json(serialize(state)), sid),
        )
    return sid


@pytest.mark.asyncio
async def test_release_endpoint_calls_pool_and_returns_snapshot_info(setup):
    sid = await _mk_session_with_state(
        snapshot_path="/vol/snap.tar", snapshot_version=2,
    )

    with patch("api.sandbox.get_pool") as get_pool, \
         patch("api.sandbox.runtime.get_pool", new=get_pool):
        get_pool.return_value.release = AsyncMock(return_value=None)
        async with AsyncClient(
            transport=ASGITransport(app=srv.app), base_url="http://test",
        ) as client:
            resp = await client.post(f"/sessions/{sid}/release")

    assert resp.status_code == 200
    body = resp.json()
    assert body["lifecycle"] == "hibernated"
    assert body["snapshot_path"] == "/vol/snap.tar"
    assert body["snapshot_version"] == 2
    get_pool.return_value.release.assert_awaited_once_with(sid)


@pytest.mark.asyncio
async def test_release_endpoint_idempotent_on_unknown_session(setup):
    """Releasing a session with no DB row: pool.release is a no-op,
    deserialize(None) → UnknownSandboxState with no snapshot."""
    with patch("api.sandbox.get_pool") as get_pool, \
         patch("api.sandbox.runtime.get_pool", new=get_pool):
        get_pool.return_value.release = AsyncMock(return_value=None)
        async with AsyncClient(
            transport=ASGITransport(app=srv.app), base_url="http://test",
        ) as client:
            resp = await client.post("/sessions/does-not-exist/release")

    assert resp.status_code == 200
    body = resp.json()
    assert body["lifecycle"] == "hibernated"
    assert body["snapshot_path"] is None
    assert body["snapshot_version"] == 0
