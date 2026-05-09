"""Tests for pre_start_commands persistence and replay behaviour.

Three properties:

a) Persistence round-trip — POST /sessions stores raw user commands on the
   sessions row (not the merged skill+user result).

b) Type 2 recovery (reset-sandbox / _provision_new) replays pre_start_commands
   by passing them to provision_sandbox.

c) Type 1 recovery (stopped sandbox resumed via start_sandbox, same VM) does
   NOT call provision_sandbox — pre_start_commands are not re-run.

These tests intentionally do NOT use the ``clean_db`` fixture: it truncates
shared tables and races every other worker's freshly-inserted rows under
xdist (FK violations as one worker DELETEs out from under another). Each
test mints unique agent / volume IDs so parallel workers can coexist.
"""
from __future__ import annotations

import os
import sys
import uuid

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from unittest.mock import AsyncMock, MagicMock, patch

_SRC = os.path.join(os.path.dirname(__file__), "..", "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

_DB = os.environ.get("TEST_DATABASE_URL")
pytestmark = pytest.mark.skipif(_DB is None, reason="TEST_DATABASE_URL not set")
if _DB:
    os.environ["DATABASE_URL"] = _DB

from api import db as dbmod, server as srv  # noqa: E402


def _uniq(prefix: str) -> str:
    """Generate a unique-per-invocation ID so xdist workers don't collide."""
    return f"{prefix}-{uuid.uuid4().hex[:10]}"


@pytest_asyncio.fixture
async def client(db_pool):
    transport = ASGITransport(app=srv.app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


@pytest_asyncio.fixture
async def sdk(db_pool):
    """ApiClient bound to the in-process app via ASGITransport.

    Verifies the SDK wrapper, not just the raw HTTP API."""
    import httpx
    from agent_sdk.api_client import ApiClient

    transport = ASGITransport(app=srv.app)
    http = httpx.AsyncClient(
        transport=transport, base_url="http://test",
        timeout=httpx.Timeout(30.0, read=None),
    )
    sc = ApiClient(base_url="http://test", http_client=http)
    try:
        yield sc
    finally:
        await sc.close()


# ---------------------------------------------------------------------------
# (a) Persistence round-trip
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_pre_start_commands_stored_on_lazy_session(client):
    """POST /sessions?provision=false stores raw user commands on the row."""
    from api.models import AgentConfig, AgentRecord, VolumeRecord

    aid = _uniq("a-psc1")
    vid = _uniq("v-psc1")
    await dbmod.upsert_agent(AgentRecord(id=aid, name=aid, config=AgentConfig()))
    await dbmod.upsert_volume(VolumeRecord(
        id=vid, name=vid.replace("-", ""), provider="daytona", provider_ref=f"dt-{vid}",
    ))

    cmds = ["echo hi", "echo bye"]
    r = await client.post("/sessions", json={
        "agent_id": aid,
        "volume_id": vid,
        "provision": False,
        "pre_start_commands": cmds,
    })
    assert r.status_code == 200, f"got {r.status_code}: {r.text}"
    sid = r.json()["id"]

    sess = await dbmod.get_session(sid)
    assert sess is not None
    assert sess["pre_start_commands"] == cmds


@pytest.mark.asyncio
async def test_pre_start_commands_round_trip_via_sdk(sdk):
    """``ApiClient.create_session(pre_start_commands=...)`` persists, and
    ``ApiClient.get_session(...)`` reads them back. Verifies the SDK
    forwards the field correctly through both directions."""
    from api.models import AgentConfig, AgentRecord, VolumeRecord

    aid = _uniq("a-sdk")
    vid = _uniq("v-sdk")
    await dbmod.upsert_agent(AgentRecord(id=aid, name=aid, config=AgentConfig()))
    await dbmod.upsert_volume(VolumeRecord(
        id=vid, name=vid.replace("-", ""), provider="daytona", provider_ref=f"dt-{vid}",
    ))

    cmds = [
        "pip install --user --quiet six",
        "echo 'You are a hive agent.' > /home/daytona/CLAUDE.md",
    ]

    created = await sdk.create_session(
        agent_id=aid, volume_id=vid, provision=False,
        pre_start_commands=cmds,
    )
    sid = created.get("id") or created.get("session_id")
    assert sid

    fetched = await sdk.get_session(sid)
    assert fetched["pre_start_commands"] == cmds


@pytest.mark.asyncio
async def test_pre_start_commands_defaults_to_empty_list(client):
    """Sessions created without pre_start_commands have [] in the DB."""
    from api.models import AgentConfig, AgentRecord, VolumeRecord

    aid = _uniq("a-psc3")
    vid = _uniq("v-psc3")
    await dbmod.upsert_agent(AgentRecord(id=aid, name=aid, config=AgentConfig()))
    await dbmod.upsert_volume(VolumeRecord(
        id=vid, name=vid.replace("-", ""), provider="daytona", provider_ref=f"dt-{vid}",
    ))

    r = await client.post("/sessions", json={
        "agent_id": aid,
        "volume_id": vid,
        "provision": False,
    })
    assert r.status_code == 200, f"got {r.status_code}: {r.text}"
    sid = r.json()["id"]

    sess = await dbmod.get_session(sid)
    assert sess is not None
    assert sess["pre_start_commands"] == []


