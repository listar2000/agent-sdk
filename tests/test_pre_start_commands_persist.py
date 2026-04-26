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
    """ServerClient bound to the in-process app via ASGITransport.

    Verifies the SDK wrapper, not just the raw HTTP API."""
    import httpx
    from agent_sdk.server_client import ServerClient

    transport = ASGITransport(app=srv.app)
    http = httpx.AsyncClient(
        transport=transport, base_url="http://test",
        timeout=httpx.Timeout(30.0, read=None),
    )
    sc = ServerClient(base_url="http://test", http_client=http)
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
async def test_pre_start_commands_stored_eager_session(client):
    """POST /sessions (eager) stores raw user commands, not the skill-merged list."""
    from api.models import AgentConfig, AgentRecord, VolumeRecord
    from api.providers import ProviderInstance

    await dbmod.upsert_agent(AgentRecord(id="a-psc2", name="PSC2", config=AgentConfig()))
    await dbmod.upsert_volume(VolumeRecord(
        id="v-psc2", name="vpsc2", provider="daytona", provider_ref="dt-psc2"
    ))

    cmds = ["mkdir -p /tmp/mydir"]

    fake_acp = MagicMock()
    fake_acp.initialize = AsyncMock(return_value=None)
    fake_acp.handshake = AsyncMock(return_value=None)
    fake_acp.get_inner_session_id = MagicMock(return_value=None)

    async def fake_create(**kw):
        return ProviderInstance(
            provider="daytona", url="", root="/home/daytona", sandbox_id="dt-psc2-sb"
        )

    with patch("api.providers.daytona.provision_daytona_sandbox",
               new=AsyncMock(side_effect=fake_create)), \
         patch("api.providers._wait_for_health", new=AsyncMock(return_value=True)), \
         patch("api.server._start_session_tasks", MagicMock(return_value=None)), \
         patch("api.server.ensure_volume_supervisor", new=AsyncMock(return_value=None)), \
         patch("api.providers.daytona.ensure_supervisor_url",
               new=AsyncMock(return_value=("http://fake-sup:7000", 7000))), \
         patch("api.server.AcpClient", return_value=fake_acp):
        r = await client.post("/sessions", json={
            "agent_id": "a-psc2",
            "volume_id": "v-psc2",
            "provider": "daytona",
            "pre_start_commands": cmds,
        })
    assert r.status_code == 200, f"got {r.status_code}: {r.text}"
    sid = r.json()["session_id"]

    sess = await dbmod.get_session(sid)
    assert sess is not None
    # Only raw user commands are stored, not skill install commands.
    assert sess["pre_start_commands"] == cmds


@pytest.mark.asyncio
async def test_pre_start_commands_round_trip_via_sdk(sdk):
    """``ServerClient.create_session(pre_start_commands=...)`` persists, and
    ``ServerClient.get_session(...)`` reads them back. Verifies the SDK
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


# ---------------------------------------------------------------------------
# (b) Type 2 recovery — _provision_new replays pre_start_commands
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_reset_sandbox_replays_pre_start_commands(client):
    """POST /reset-sandbox triggers _provision_new which passes pre_start_commands
    to provision_daytona_sandbox."""
    from api.models import AgentConfig, AgentRecord, VolumeRecord, SandboxRecord
    from api.providers import ProviderInstance

    cmds = ["touch /tmp/marker_recovery"]

    await dbmod.upsert_agent(AgentRecord(id="a-t2", name="T2", config=AgentConfig()))
    await dbmod.upsert_volume(VolumeRecord(
        id="v-t2", name="vt2", provider="daytona", provider_ref="dt-t2"
    ))

    # Create a lazy session WITH pre_start_commands stored.
    r = await client.post("/sessions", json={
        "agent_id": "a-t2",
        "volume_id": "v-t2",
        "provision": False,
        "pre_start_commands": cmds,
    })
    assert r.status_code == 200
    sid = r.json()["id"]

    # Attach an existing sandbox so reset-sandbox has something to tear down.
    sb_old = SandboxRecord(
        id="sb-t2-old", provider="daytona", sandbox_ref="dt-old-t2",
        status="running", root="/home/daytona",
        volume_id="v-t2", subpath="agents/a-t2/home",
    )
    await dbmod.upsert_sandbox(sb_old)
    await dbmod.set_session_current_sandbox(sid, "sb-t2-old")

    provision_calls = []

    async def fake_create(**kw):
        provision_calls.append(kw)
        return ProviderInstance(
            provider="daytona", url="http://fake:7000",
            root="/home/daytona", sandbox_id="dt-new-t2"
        )

    with patch("api.providers.destroy_daytona", new=AsyncMock(return_value=None)), \
         patch("api.server.ensure_volume_supervisor", new=AsyncMock(return_value=None)), \
         patch("api.providers.daytona.provision_daytona_sandbox",
               new=AsyncMock(side_effect=fake_create)):
        r = await client.post(f"/sessions/{sid}/reset-sandbox")
    assert r.status_code == 200, f"got {r.status_code}: {r.text}"

    # provision_daytona_sandbox must have been called exactly once.
    assert len(provision_calls) == 1, (
        f"expected 1 provision call, got {len(provision_calls)}: {provision_calls}"
    )
    # pre_start_commands must have been forwarded.
    passed = provision_calls[0].get("pre_start_commands")
    assert passed is not None, (
        f"pre_start_commands was not passed to provision_sandbox; call: {provision_calls[0]}"
    )
    assert cmds[0] in passed, (
        f"expected {cmds[0]!r} in pre_start_commands, got {passed!r}"
    )


# ---------------------------------------------------------------------------
# (c) Type 1 recovery — start_sandbox does NOT replay pre_start_commands
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_type1_recovery_calls_start_sandbox_not_provision(client):
    """Type 1 recovery: a stopped sandbox is resumed via start_sandbox (same VM).
    provision_sandbox must NOT be called so pre_start_commands are not re-run."""
    from api.models import AgentConfig, AgentRecord, VolumeRecord, SandboxRecord
    from api.providers import ProviderInstance

    cmds = ["touch /tmp/marker_type1"]

    await dbmod.upsert_agent(AgentRecord(id="a-t1", name="T1", config=AgentConfig()))
    await dbmod.upsert_volume(VolumeRecord(
        id="v-t1", name="vt1", provider="daytona", provider_ref="dt-t1"
    ))

    r = await client.post("/sessions", json={
        "agent_id": "a-t1",
        "volume_id": "v-t1",
        "provision": False,
        "pre_start_commands": cmds,
    })
    assert r.status_code == 200
    sid = r.json()["id"]

    # Insert a sandbox row with status=stopped so ensure_sandbox cold-path
    # hits the "stopped → start_sandbox" branch (Type 1).
    sb = SandboxRecord(
        id="sb-t1", provider="daytona", sandbox_ref="dt-t1-ref",
        status="stopped", root="/home/daytona",
        volume_id="v-t1", subpath="agents/a-t1/home",
    )
    await dbmod.upsert_sandbox(sb)
    await dbmod.set_session_current_sandbox(sid, "sb-t1")

    provision_calls = []
    start_calls = []

    async def fake_provision(**kw):
        provision_calls.append(kw)
        return ProviderInstance(
            provider="daytona", url="http://fake:7000",
            root="/home/daytona", sandbox_id="dt-new-t1"
        )

    # daytona.start_sandbox is aliased to start_daytona in daytona.py.
    async def fake_start_daytona(sandbox_ref):
        start_calls.append(sandbox_ref)

    fake_acp = MagicMock()
    fake_acp.initialize = AsyncMock(return_value=None)
    fake_acp.handshake = AsyncMock(return_value=None)
    fake_acp.get_inner_session_id = MagicMock(return_value=None)

    # Mock get_sandbox_status to return "stopped" (triggers Type 1 path in
    # _ensure_sandbox_locked cold path).
    # Mock start_sandbox (the module-level alias for start_daytona).
    with patch("api.providers.daytona.provision_daytona_sandbox",
               new=AsyncMock(side_effect=fake_provision)), \
         patch("api.providers.daytona.start_sandbox",
               new=AsyncMock(side_effect=fake_start_daytona)), \
         patch("api.providers.daytona.get_sandbox_status",
               new=AsyncMock(return_value="stopped")), \
         patch("api.server.ensure_volume_supervisor", new=AsyncMock(return_value=None)), \
         patch("api.server._start_session_tasks", MagicMock(return_value=None)), \
         patch("api.providers.daytona.ensure_supervisor_url",
               new=AsyncMock(return_value=("http://fake-sup:7000", 7000))), \
         patch("api.server.AcpClient", return_value=fake_acp):
        r = await client.post(f"/sessions/{sid}/start-sandbox")

    assert r.status_code == 200, f"got {r.status_code}: {r.text}"

    # Type 1: start_sandbox (start_daytona) was called; provision was NOT called.
    assert len(start_calls) >= 1, (
        f"expected start_daytona to be called for Type 1 recovery, calls: {start_calls}"
    )
    assert len(provision_calls) == 0, (
        f"provision_sandbox should NOT be called for Type 1 recovery "
        f"(same sandbox row, same VM); calls: {provision_calls}"
    )
