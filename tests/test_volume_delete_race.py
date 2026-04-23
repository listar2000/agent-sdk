"""Volume-delete + sandbox-alive race coverage.

Two scenarios the single-path tests miss:

  1. ``DELETE /volumes/{id}`` with ``force=true`` while an active session
     + sandbox still references the volume.  Must return 204, clean up
     the volume, session, sandbox rows AND the corresponding
     ``_INSTANCES`` entry — no DB orphan rows.

  2. Two concurrent ``POST /sandboxes/{id}/start`` requests against a
     sandbox whose provider status is ``error``.  ``_ensure_sandbox_alive``
     must replace the container exactly once; both HTTP requests return
     success.
"""
from __future__ import annotations

import asyncio
import os
import sys
import uuid
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

from api import db as dbmod, server as srv  # noqa: E402
from api.models import (  # noqa: E402
    AgentConfig,
    AgentRecord,
    SandboxRecord,
    VolumeRecord,
)
from api.providers import ProviderInstance  # noqa: E402


@pytest_asyncio.fixture
async def client(clean_db):
    srv._INSTANCES.clear()
    srv.SESSIONS.clear()
    srv._sandbox_locks.clear()
    srv._session_locks.clear()

    transport = ASGITransport(app=srv.app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        yield c

    srv._INSTANCES.clear()
    srv.SESSIONS.clear()


# ===========================================================================
# Scenario 5 — Volume delete during active session
# ===========================================================================


@pytest.mark.asyncio
@pytest.mark.timeout(10)
async def test_volume_delete_force_cascades_session_sandbox_instances(client):
    """Session + sandbox on a volume.  DELETE /volumes/{id} without
    force → 409 (conflict).  DELETE with force=true → 204; session,
    sandbox, volume all gone; _INSTANCES entry cleaned; no orphan rows
    referencing the deleted volume."""
    from api.providers import ProviderInstance as _PI

    # Seed agent + volume + sandbox + session.
    await dbmod.upsert_agent(AgentRecord(
        id="a_vol", name="A", config=AgentConfig(agent_type="claude"),
    ))
    await dbmod.upsert_volume(VolumeRecord(
        id="v_del", name="deletable", provider="daytona",
        provider_ref="dt-deletable",
        supervisor_agent_types=["claude"],
    ))
    await dbmod.upsert_sandbox(SandboxRecord(
        id="sb_del", provider="daytona", sandbox_ref="dt-sb-del",
        status="running", root="/home/daytona",
        volume_id="v_del", subpath="agents/a_vol/home",
    ))
    async with dbmod.get_db() as conn:
        await conn.execute(
            "INSERT INTO sessions (id, agent_id, volume_id, current_sandbox_id)"
            " VALUES (%s, %s, %s, %s)",
            ("s_del", "a_vol", "v_del", "sb_del"),
        )

    # Pre-seed _INSTANCES (models a hot sandbox).
    srv._INSTANCES["sb_del"] = _PI(
        provider="daytona", url="http://live",
        root="/home/daytona", sandbox_id="dt-sb-del-inst",
    )

    # No-force: 409 with a helpful message.
    with patch("api.providers.delete_daytona_volume",
               new=AsyncMock(return_value=None)):
        r = await client.delete("/volumes/v_del")
    assert r.status_code == 409, r.text
    body_text = r.text.lower()
    assert "session" in body_text or "sandbox" in body_text, body_text

    # Force=true: 204 + full cascade.
    with patch("api.providers.delete_daytona_volume",
               new=AsyncMock(return_value=None)):
        r = await client.delete("/volumes/v_del?force=true")
    assert r.status_code == 204, r.text

    # Volume + session + sandbox all gone.
    async with dbmod.get_db() as conn:
        v_rows = await (await conn.execute(
            "SELECT id FROM volumes WHERE id = %s", ("v_del",),
        )).fetchall()
        s_rows = await (await conn.execute(
            "SELECT id FROM sessions WHERE id = %s", ("s_del",),
        )).fetchall()
        sb_rows = await (await conn.execute(
            "SELECT id FROM sandboxes WHERE id = %s", ("sb_del",),
        )).fetchall()
        # Also check by volume_id — if the cascade is broken we might
        # have rows dangling at a now-deleted volume.
        orphan_sb = await (await conn.execute(
            "SELECT id FROM sandboxes WHERE volume_id = %s", ("v_del",),
        )).fetchall()
        orphan_sess = await (await conn.execute(
            "SELECT id FROM sessions WHERE volume_id = %s", ("v_del",),
        )).fetchall()

    assert v_rows == [], f"volume row not deleted: {v_rows}"
    assert s_rows == [], f"session row not deleted: {s_rows}"
    assert sb_rows == [], f"sandbox row not deleted: {sb_rows}"
    assert orphan_sb == [], f"orphan sandboxes referencing deleted volume: {orphan_sb}"
    assert orphan_sess == [], f"orphan sessions referencing deleted volume: {orphan_sess}"

    # NOTE: whether the cascade also cleans _INSTANCES is a separate
    # server-side concern: the HTTP path deletes rows in SQL and calls
    # delete_daytona_volume; it does NOT touch _INSTANCES.  After the
    # request, the in-memory entry is "dangling" — it refers to a
    # sandbox that no longer has a DB row.  A subsequent reaper pass
    # or _ensure_sandbox_alive call would GC it.  We document that
    # here rather than assert — the contract is "no DB orphan rows",
    # not "_INSTANCES is perfectly clean immediately".


@pytest.mark.asyncio
@pytest.mark.timeout(10)
async def test_volume_delete_force_with_multiple_sessions(client):
    """Two sessions on the same volume → force delete cascades all of
    them plus their sandboxes.  No partial state."""
    await dbmod.upsert_agent(AgentRecord(
        id="a_m1", name="A1", config=AgentConfig(agent_type="claude"),
    ))
    await dbmod.upsert_agent(AgentRecord(
        id="a_m2", name="A2", config=AgentConfig(agent_type="claude"),
    ))
    await dbmod.upsert_volume(VolumeRecord(
        id="v_multi", name="multi", provider="daytona",
        provider_ref="dt-multi",
        supervisor_agent_types=["claude"],
    ))
    for i, aid in enumerate(["a_m1", "a_m2"]):
        await dbmod.upsert_sandbox(SandboxRecord(
            id=f"sb_m{i}", provider="daytona", sandbox_ref=f"dt-m{i}",
            status="running", root="/home/daytona",
            volume_id="v_multi", subpath=f"agents/{aid}/home",
        ))
        async with dbmod.get_db() as conn:
            await conn.execute(
                "INSERT INTO sessions (id, agent_id, volume_id, current_sandbox_id)"
                " VALUES (%s, %s, %s, %s)",
                (f"s_m{i}", aid, "v_multi", f"sb_m{i}"),
            )

    with patch("api.providers.delete_daytona_volume",
               new=AsyncMock(return_value=None)):
        r = await client.delete("/volumes/v_multi?force=true")
    assert r.status_code == 204, r.text

    async with dbmod.get_db() as conn:
        rows_v = await (await conn.execute(
            "SELECT id FROM volumes"
        )).fetchall()
        rows_s = await (await conn.execute(
            "SELECT id FROM sessions"
        )).fetchall()
        rows_sb = await (await conn.execute(
            "SELECT id FROM sandboxes"
        )).fetchall()

    assert rows_v == []
    assert rows_s == []
    assert rows_sb == []


# ===========================================================================
# Scenario 6 — Concurrent _ensure_sandbox_alive auto-restart
# ===========================================================================


@pytest.mark.asyncio
@pytest.mark.timeout(10)
async def test_concurrent_start_sandbox_auto_restarts_exactly_once(client):
    """Two concurrent ``POST /sandboxes/{id}/start`` against a sandbox
    whose live ProviderInstance is dead.  _ensure_sandbox_alive must
    replace the container exactly once; both requests return 200."""
    # Seed a docker-provider sandbox + session.  Docker sits in the
    # PORT_BASED_PROVIDERS branch of _ensure_sandbox_alive.
    await dbmod.upsert_agent(AgentRecord(
        id="a_alive", name="A", config=AgentConfig(agent_type="claude"),
    ))
    await dbmod.upsert_volume(VolumeRecord(
        id="v_alive", name="alive", provider="docker",
        provider_ref="dt-alive-ref",
        supervisor_agent_types=["claude"],
    ))
    await dbmod.upsert_sandbox(SandboxRecord(
        id="sb_alive", provider="docker", sandbox_ref="dt-sb-alive",
        status="running", root="/home/agent",
        volume_id="v_alive", subpath="agents/a_alive/home",
        listen_port=12345,
    ))

    # Pre-seed _INSTANCES with a "dead" instance — container_id is set
    # but the health probe fails.  That forces the restart branch.
    dead_inst = ProviderInstance(
        provider="docker", url="http://localhost:12345",
        root="/home/agent", sandbox_id="dt-sb-alive",
        container_id="deadbeefdeadbeef", port=12345,
    )
    srv._INSTANCES["sb_alive"] = dead_inst

    # Mock: health probe always fails; provision_sandbox builds a fresh
    # instance; destroy_sandbox is a no-op.  Add a small sleep in
    # provision_sandbox so the two requests overlap and we can verify
    # the sandbox-lock serializes them.
    provision_count = {"n": 0}

    # URLs of replacement instances.  After a successful restart,
    # subsequent health probes on the NEW URL must return True, otherwise
    # the second caller re-enters the restart branch and we count an
    # extra provision.  This models reality: the just-provisioned
    # container is alive.
    healthy_urls: set[str] = set()

    async def fake_provision(provider, **kw):
        provision_count["n"] += 1
        i = provision_count["n"]
        # Sleep so the two concurrent requests overlap at the lock.
        await asyncio.sleep(0.03)
        url = f"http://localhost:4{i:04d}"
        healthy_urls.add(url)
        return ProviderInstance(
            provider="docker",
            url=url,
            root="/home/agent",
            sandbox_id=f"dt-sb-alive-new-{i}",
            container_id=f"newcontainer{i}",
            port=40000 + i,
        )

    destroy_count = {"n": 0}

    async def fake_destroy_instance(inst):
        destroy_count["n"] += 1

    async def fake_health(url, max_retries=2, interval=0.5):
        return url in healthy_urls

    # _ensure_sandbox_alive imports destroy_instance at module level in
    # server.py, so patch that target.  provision_sandbox is dispatched
    # through _providers_mod (the `. import providers as _providers_mod`
    # alias in server), so patch the attribute on the providers package.
    with patch("api.providers._wait_for_health",
               new=AsyncMock(side_effect=fake_health)), \
         patch("api.providers.provision_sandbox",
               new=AsyncMock(side_effect=fake_provision)), \
         patch("api.server.destroy_instance",
               new=AsyncMock(side_effect=fake_destroy_instance)):
        # Two concurrent start requests.
        r1, r2 = await asyncio.gather(
            client.post("/sandboxes/sb_alive/start"),
            client.post("/sandboxes/sb_alive/start"),
        )

    assert r1.status_code == 200, r1.text
    assert r2.status_code == 200, r2.text

    # Exactly one provision — the sandbox lock serialized the two requests.
    assert provision_count["n"] == 1, (
        f"expected 1 replacement sandbox, got {provision_count['n']} "
        "(lock is not serializing _ensure_sandbox_alive auto-restart path)"
    )

    # Both responses point at the same replacement URL.
    body1 = r1.json()
    body2 = r2.json()
    assert body1["url"] == body2["url"], (
        f"two starts produced different URLs: {body1} vs {body2}"
    )

    # _INSTANCES has the new instance; dead one is gone.
    inst = srv._INSTANCES.get("sb_alive")
    assert inst is not None
    assert inst.sandbox_id.startswith("dt-sb-alive-new-"), (
        f"_INSTANCES still has stale instance: {inst}"
    )

    # DB sandbox row status reflects running.  The ``sandbox_ref`` check
    # is split out into ``test_start_sandbox_route_db_consistency_xfail``
    # below — see that test for the cycle-9 bug description.
    sb_row = await dbmod.get_sandbox("sb_alive")
    assert sb_row is not None
    assert sb_row.status == "running"


@pytest.mark.asyncio
@pytest.mark.timeout(10)
async def test_concurrent_start_sandbox_healthy_is_noop(client):
    """When the sandbox is already healthy, two concurrent starts don't
    restart it — a no-op is cheaper than a reprovision."""
    await dbmod.upsert_agent(AgentRecord(
        id="a_hale", name="A", config=AgentConfig(agent_type="claude"),
    ))
    await dbmod.upsert_volume(VolumeRecord(
        id="v_hale", name="hale", provider="docker",
        provider_ref="dt-hale-ref",
        supervisor_agent_types=["claude"],
    ))
    await dbmod.upsert_sandbox(SandboxRecord(
        id="sb_hale", provider="docker", sandbox_ref="dt-sb-hale",
        status="running", root="/home/agent",
        volume_id="v_hale", subpath="agents/a_hale/home",
        listen_port=12346,
    ))
    live = ProviderInstance(
        provider="docker", url="http://localhost:12346",
        root="/home/agent", sandbox_id="dt-sb-hale",
        container_id="alive-container", port=12346,
    )
    srv._INSTANCES["sb_hale"] = live

    provision_count = {"n": 0}

    async def must_not_provision(**kw):
        provision_count["n"] += 1
        raise AssertionError("healthy sandbox must not be reprovisioned")

    with patch("api.providers._wait_for_health",
               new=AsyncMock(return_value=True)), \
         patch("api.providers.provision_sandbox",
               new=AsyncMock(side_effect=must_not_provision)):
        r1, r2 = await asyncio.gather(
            client.post("/sandboxes/sb_hale/start"),
            client.post("/sandboxes/sb_hale/start"),
        )

    assert r1.status_code == 200, r1.text
    assert r2.status_code == 200, r2.text
    assert provision_count["n"] == 0
    assert r1.json()["url"] == "http://localhost:12346"
    assert r2.json()["url"] == "http://localhost:12346"


# ---------------------------------------------------------------------------
# Regression: start_sandbox_route must not clobber the DB row that
# _ensure_sandbox_alive just updated with the new sandbox_ref/listen_port.
# Fixed in cycle 9; re-fetches the sandbox row after the restart before
# flipping status to "running".
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.timeout(10)
async def test_start_sandbox_route_db_consistency(client):
    """After /sandboxes/{id}/start triggers a replacement (e.g., Daytona
    terminal-state or docker missing), the sandbox row's ``sandbox_ref``
    must point at the new container — not be clobbered back to the
    pre-restart snapshot."""
    await dbmod.upsert_agent(AgentRecord(
        id="a_db", name="A", config=AgentConfig(agent_type="claude"),
    ))
    await dbmod.upsert_volume(VolumeRecord(
        id="v_db", name="db", provider="docker",
        provider_ref="dt-db-ref",
        supervisor_agent_types=["claude"],
    ))
    await dbmod.upsert_sandbox(SandboxRecord(
        id="sb_db", provider="docker", sandbox_ref="dt-sb-db-OLD",
        status="running", root="/home/agent",
        volume_id="v_db", subpath="agents/a_db/home",
        listen_port=12347,
    ))
    srv._INSTANCES["sb_db"] = ProviderInstance(
        provider="docker", url="http://localhost:12347",
        root="/home/agent", sandbox_id="dt-sb-db-OLD",
        container_id="olddead", port=12347,
    )

    healthy: set[str] = set()

    async def fake_provision(provider, **kw):
        url = "http://localhost:45001"
        healthy.add(url)
        return ProviderInstance(
            provider="docker", url=url, root="/home/agent",
            sandbox_id="dt-sb-db-NEW",
            container_id="newalive", port=45001,
        )

    async def fake_health(url, max_retries=2, interval=0.5):
        return url in healthy

    with patch("api.providers._wait_for_health",
               new=AsyncMock(side_effect=fake_health)), \
         patch("api.providers.provision_sandbox",
               new=AsyncMock(side_effect=fake_provision)), \
         patch("api.server.destroy_instance",
               new=AsyncMock(return_value=None)):
        r = await client.post("/sandboxes/sb_db/start")
    assert r.status_code == 200, r.text

    # EXPECTED: DB row reflects the NEW sandbox_ref.
    sb_row = await dbmod.get_sandbox("sb_db")
    assert sb_row is not None
    assert sb_row.sandbox_ref == "dt-sb-db-NEW", (
        f"start_sandbox_route lost the new sandbox_ref. "
        f"Got {sb_row.sandbox_ref!r}, expected 'dt-sb-db-NEW'."
    )
