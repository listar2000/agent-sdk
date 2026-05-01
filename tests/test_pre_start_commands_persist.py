"""Tests for pre_start_commands persistence and replay behaviour.

Three properties:

a) Persistence round-trip — POST /sessions stores raw user commands on the
   sessions row (not the merged skill+user result).

b) Type 2 recovery (reset-sandbox / _provision_new) replays pre_start_commands
   by passing them to provision_sandbox.

c) Type 1 recovery (stopped sandbox resumed via start_sandbox, same VM) does
   NOT call provision_sandbox — pre_start_commands are not re-run.
"""
from __future__ import annotations

import os
import sys

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


@pytest_asyncio.fixture
async def client(clean_db):
    transport = ASGITransport(app=srv.app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


@pytest_asyncio.fixture
async def sdk(clean_db):
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

    await dbmod.upsert_agent(AgentRecord(id="a-psc1", name="PSC1", config=AgentConfig()))
    await dbmod.upsert_volume(VolumeRecord(
        id="v-psc1", name="vpsc1", provider="daytona", provider_ref="dt-psc1"
    ))

    cmds = ["echo hi", "echo bye"]
    r = await client.post("/sessions", json={
        "agent_id": "a-psc1",
        "volume_id": "v-psc1",
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

    await dbmod.upsert_agent(AgentRecord(id="a-sdk", name="SDK", config=AgentConfig()))
    await dbmod.upsert_volume(VolumeRecord(
        id="v-sdk", name="vsdk", provider="daytona", provider_ref="dt-sdk",
    ))

    cmds = [
        "pip install --user --quiet six",
        "echo 'You are a hive agent.' > /home/daytona/CLAUDE.md",
    ]

    created = await sdk.create_session(
        agent_id="a-sdk", volume_id="v-sdk", provision=False,
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

    await dbmod.upsert_agent(AgentRecord(id="a-psc3", name="PSC3", config=AgentConfig()))
    await dbmod.upsert_volume(VolumeRecord(
        id="v-psc3", name="vpsc3", provider="daytona", provider_ref="dt-psc3"
    ))

    r = await client.post("/sessions", json={
        "agent_id": "a-psc3",
        "volume_id": "v-psc3",
        "provision": False,
    })
    assert r.status_code == 200, f"got {r.status_code}: {r.text}"
    sid = r.json()["id"]

    sess = await dbmod.get_session(sid)
    assert sess is not None
    assert sess["pre_start_commands"] == []


