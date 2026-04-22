"""Query budget test for POST /sessions/{id}/message on warm sessions.

Asserts an upper bound on DB round-trips per /message call on the happy
path (session warm: sandbox running, SESSIONS cache populated, runtime
healthy). Guards against regressions that silently add get_* calls inside
ensure_session_live.
"""
from __future__ import annotations

import os
import sys
from unittest.mock import AsyncMock, MagicMock, patch

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


# Budget for /message on a warm session. Current implementation:
#   1. require_session -> get_session
#   2. _ensure_sandbox_locked -> get_session (re-read under lock)
#   3. _ensure_sandbox_locked -> get_sandbox
# Fast-path (2026-04-22): skip get_volume + provider probe when _INSTANCES
# and SESSIONS agree that the sandbox is live.
# ensure_runtime hits SESSIONS cache, no DB at all.
WARM_MESSAGE_QUERY_BUDGET = 3


@pytest_asyncio.fixture
async def client():
    dbmod.init_db()
    await dbmod.init_pool()
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
async def test_warm_message_query_budget(client):
    """POST /message on a warm session must hit the DB <= WARM_MESSAGE_QUERY_BUDGET
    times (currently 3)."""
    from api.models import AgentConfig, AgentRecord, VolumeRecord
    from api.providers import ProviderInstance

    await dbmod.upsert_agent(
        AgentRecord(id="agent_qb", name="QB", config=AgentConfig())
    )
    await dbmod.upsert_volume(
        VolumeRecord(
            id="vol_qb", name="vqb", provider="daytona",
            provider_ref="dt-qb",
            supervisor_agent_types=["claude"],  # cache hit in ensure_volume_supervisor
        )
    )
    r = await client.post(
        "/sessions", json={"agent_id": "agent_qb", "volume_id": "vol_qb"}
    )
    sid = r.json()["id"]

    # Mock full startup stack.
    async def fake_create(**kw):
        return ProviderInstance(
            provider="daytona", url="http://fake:7000",
            root="/home/daytona", sandbox_id="sb-qb-1",
        )

    fake_acp = MagicMock()
    fake_acp.initialize = AsyncMock(return_value=None)
    fake_acp.handshake = AsyncMock(return_value=None)
    fake_acp.get_inner_session_id = MagicMock(return_value=None)

    patches = [
        patch("api.providers.daytona.provision_daytona_sandbox",
              new=AsyncMock(side_effect=fake_create)),
        patch("api.providers._wait_for_health",
              new=AsyncMock(return_value=True)),
        patch("api.server._start_session_tasks", MagicMock(return_value=None)),
        patch("api.server._submit_prompt", MagicMock(return_value=None)),
        patch("api.server.ensure_volume_supervisor",
              new=AsyncMock(return_value=None)),
        patch("api.providers.daytona.ensure_supervisor_url",
              new=AsyncMock(return_value="http://fake-supervisor:7000")),
        patch("api.server.AcpClient", return_value=fake_acp),
    ]
    for p in patches:
        p.start()
    try:
        # Warm-up: first /message provisions the sandbox + builds runtime.
        r = await client.post(
            f"/sessions/{sid}/message", json={"message": "warm-up"}
        )
        assert r.status_code == 200, r.text

        # Now measure a second call: sandbox + runtime are both warm.
        # api.server imports the db helpers by name, so each helper is
        # essentially one DB round-trip. We count at the helper-function
        # level by wrapping the symbols in ``api.server``'s namespace
        # (which is where ``ensure_session_live`` resolves them).
        counter = {"n": 0}
        helpers = [
            "get_session", "get_sandbox", "get_volume", "get_agent",
            "get_any_session_for_sandbox",
            "set_session_current_sandbox", "delete_sandbox",
            "upsert_sandbox", "upsert_session", "log_event",
        ]
        orig_fns = {name: getattr(srv, name) for name in helpers
                    if hasattr(srv, name)}

        def _mk_counting(orig):
            async def _wrap(*a, **k):
                counter["n"] += 1
                return await orig(*a, **k)
            return _wrap

        for name, orig in orig_fns.items():
            setattr(srv, name, _mk_counting(orig))
        try:
            r2 = await client.post(
                f"/sessions/{sid}/message", json={"message": "hot"}
            )
        finally:
            for name, orig in orig_fns.items():
                setattr(srv, name, orig)
        assert r2.status_code == 200, r2.text
        assert counter["n"] <= WARM_MESSAGE_QUERY_BUDGET, (
            f"warm /message used {counter['n']} DB round-trips, "
            f"budget is {WARM_MESSAGE_QUERY_BUDGET}"
        )
        # Sanity: the budget is tight — ensure we actually measured something
        # (0 would indicate the patch failed to take effect and the test is vacuous).
        assert counter["n"] > 0, "counter did not observe any DB calls"
    finally:
        for p in patches:
            p.stop()
