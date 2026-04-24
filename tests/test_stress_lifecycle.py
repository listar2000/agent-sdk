"""Stress/chaos tests — lifecycle under concurrent load.

These tests push the provision + reaper + ensure_sandbox paths harder than
single-path tests can, looking for residual race conditions and orphaned
state-machine rows.

All provider calls are mocked; the tests hammer the FastAPI/DB layer with
concurrent coroutines and assert on DB invariants and in-memory
``_INSTANCES`` bookkeeping.
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
from api.models import VolumeRecord  # noqa: E402
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


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _mk_fake_instance(provider: str, sandbox_id: str) -> ProviderInstance:
    """Build a fake ProviderInstance with a unique sandbox_id."""
    return ProviderInstance(
        provider=provider,
        url="http://fake:9999",
        root="/home/daytona",
        sandbox_id=f"dt-{sandbox_id}",
        port=9999,
    )


async def _seed_volume(vol_id: str = "v_stress", provider: str = "daytona") -> VolumeRecord:
    v = VolumeRecord(
        id=vol_id, name=f"stress-{vol_id}", provider=provider,
        provider_ref=f"{provider}-ref-{vol_id}",
        supervisor_agent_types=["claude"],  # skip slow install path
    )
    await dbmod.upsert_volume(v)
    return v


# ===========================================================================
# Scenario 1 — High-concurrency provision storm
# ===========================================================================
# N concurrent POST /sessions/quick against the same volume, each with a
# distinct agent_id. Provider is mocked; we verify:
#   - Every call either succeeds (200) with a unique sandbox_id, or errors
#     cleanly with 503 (circuit-breaker) or another well-defined code.
#   - No duplicate sandboxes for (volume_id, subpath). `subpath` is
#     derived from agent_id, so unique agents = unique subpaths.
#   - No dangling DB rows: every successful response's sandbox_id is
#     present in the sandboxes table, and `_INSTANCES` has an entry.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.timeout(15)
async def test_provision_storm_20_concurrent_quick_creates(client):
    """20 concurrent /sessions/quick POSTs → unique sandboxes, no dupes."""
    await _seed_volume()

    # Each provision returns a unique ProviderInstance keyed on a counter.
    # The mock has a tiny sleep to amplify interleaving; exercise the
    # semaphore/lock path under contention.
    counter = {"n": 0}

    async def fake_create_instance(provider, agent_type, **kw):
        counter["n"] += 1
        i = counter["n"]
        # Yield so other coroutines can interleave.
        await asyncio.sleep(0.002)
        return _mk_fake_instance(provider, f"sb-{i}-{uuid.uuid4().hex[:6]}")

    # Stub ACP init + session tasks — we only care about provision accounting.
    fake_acp = MagicMock()
    fake_acp.aclose = AsyncMock(return_value=None)
    fake_acp.get_inner_session_id = MagicMock(return_value="inner-xyz")

    async def noop_apply(*a, **kw):
        return None

    N = 20
    with patch("api.server.create_instance", side_effect=fake_create_instance), \
         patch("api.server.AcpClient", return_value=fake_acp), \
         patch("api.server._apply_config_and_initialize",
               new=AsyncMock(side_effect=noop_apply)), \
         patch("api.server._start_session_tasks", MagicMock()), \
         patch("api.server.ensure_volume_supervisor",
               new=AsyncMock(return_value=None)):
        tasks = [
            client.post("/sessions", json={
                "provider": "daytona",
                "volume_id": "v_stress",
                "agent_type": "claude",
                "name": f"agent-{i}",
            })
            for i in range(N)
        ]
        responses = await asyncio.gather(*tasks, return_exceptions=True)

    successes = []
    retriable = []
    for r in responses:
        if isinstance(r, Exception):
            pytest.fail(f"request raised: {r!r}")
        assert r.status_code in (200, 502, 503), (
            f"unexpected status {r.status_code}: {r.text}"
        )
        if r.status_code == 200:
            successes.append(r.json())
        else:
            retriable.append(r)

    # Expect most to succeed under the mock (no real circuit breaker).
    assert len(successes) == N, (
        f"expected {N} successes, got {len(successes)}; "
        f"{len(retriable)} retriable errors"
    )

    # Uniqueness: every sandbox_id is distinct.
    sandbox_ids = [s["current_sandbox_id"] for s in successes]
    assert len(set(sandbox_ids)) == len(sandbox_ids), (
        f"duplicate sandbox_ids in responses: {sandbox_ids}"
    )

    # Every sandbox is present in the DB and in _INSTANCES.
    async with dbmod.get_db() as conn:
        rows = await (await conn.execute(
            "SELECT id, volume_id, subpath FROM sandboxes"
        )).fetchall()
    db_ids = {r["id"] for r in rows}
    for sid in sandbox_ids:
        assert sid in db_ids, f"sandbox {sid} missing from DB"
        assert sid in srv._INSTANCES, f"sandbox {sid} missing from _INSTANCES"

    # No sandbox row orphaned (each has a session pointer).
    async with dbmod.get_db() as conn:
        orphans = await (await conn.execute(
            "SELECT sb.id FROM sandboxes sb "
            "LEFT JOIN sessions s ON s.current_sandbox_id = sb.id "
            "WHERE s.id IS NULL"
        )).fetchall()
    assert orphans == [], f"orphan sandboxes (no session pointer): {orphans}"

    # Each (volume, subpath) pair is unique (each agent got its own subpath).
    pairs = [(r["volume_id"], r["subpath"]) for r in rows]
    assert len(set(pairs)) == len(pairs), (
        f"duplicate (volume, subpath) pairs — stale agent slot reuse? {pairs}"
    )


@pytest.mark.asyncio
@pytest.mark.timeout(15)
async def test_provision_storm_circuit_breaker_cleans_up_agent(client):
    """When create_instance raises 'circuit breaker', the POST returns 503
    AND the agent row that was speculatively created is deleted. This
    is the explicit contract for the circuit-breaker branch in
    ``sessions_quick_create``; a storm that all trips the breaker must
    not leave N orphan agents in the DB."""
    await _seed_volume()

    async def always_fail(*a, **kw):
        raise RuntimeError("circuit breaker open for daytona")

    with patch("api.server.create_instance", side_effect=always_fail), \
         patch("api.server.ensure_volume_supervisor",
               new=AsyncMock(return_value=None)):
        tasks = [
            client.post("/sessions", json={
                "provider": "daytona",
                "volume_id": "v_stress",
                "agent_type": "claude",
                "name": f"cb-{i}",
            })
            for i in range(10)
        ]
        responses = await asyncio.gather(*tasks)

    for r in responses:
        assert r.status_code == 503, f"expected 503, got {r.status_code}: {r.text}"
        assert r.headers.get("Retry-After") == "30"
        assert "circuit" in r.json()["error"].lower()

    # No agents in DB — cleanup on error path ran for every failed call.
    async with dbmod.get_db() as conn:
        agent_rows = await (await conn.execute(
            "SELECT id FROM agents"
        )).fetchall()
    assert agent_rows == [], (
        f"circuit-breaker failures leaked {len(agent_rows)} agent rows"
    )
    # No sandboxes either.
    async with dbmod.get_db() as conn:
        sb_rows = await (await conn.execute(
            "SELECT id FROM sandboxes"
        )).fetchall()
    assert sb_rows == []
    assert srv._INSTANCES == {}


# ===========================================================================
# Scenario 2 — Reaper + user race
# ===========================================================================
# Simulate a concurrent reaper deleting the sandbox row mid-flight. The
# user's ensure_sandbox call must either:
#   (a) reattach cleanly (see a None row, provision a replacement), or
#   (b) error cleanly
# — NOT leave a half-deleted row / phantom _INSTANCES entry.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.timeout(10)
async def test_reaper_race_during_ensure_sandbox(client):
    """A reaper deletes the sandbox row between ensure_sandbox's DB read
    and the provider status check. ensure_sandbox must (a) get a clean
    reprovision and (b) not leave the old sandbox_id in _INSTANCES."""
    from api.models import AgentConfig, AgentRecord, SandboxRecord
    await _seed_volume()
    await dbmod.upsert_agent(AgentRecord(
        id="a_r", name="A_reaper", config=AgentConfig(agent_type="claude"),
    ))
    sb_old = SandboxRecord(
        id="sb_racy", provider="daytona", sandbox_ref="dt-racy",
        status="running", root="/home/daytona",
        volume_id="v_stress", subpath="agents/a_r/home",
    )
    await dbmod.upsert_sandbox(sb_old)
    async with dbmod.get_db() as conn:
        await conn.execute(
            "INSERT INTO sessions (id, agent_id, volume_id, current_sandbox_id)"
            " VALUES (%s, %s, %s, %s)",
            ("s_racy", "a_r", "v_stress", "sb_racy"),
        )
    srv._INSTANCES["sb_racy"] = _mk_fake_instance("daytona", "racy-inst")

    provision_calls = {"n": 0}

    async def fake_status(ref):
        # Simulate the reaper deleting the sandbox row during the probe:
        # the caller just read (sb_racy) but it's about to be gone.
        await dbmod.delete_sandbox("sb_racy")
        srv._INSTANCES.pop("sb_racy", None)
        return "missing"  # → triggers the missing/error branch

    async def fake_provision(**kw):
        provision_calls["n"] += 1
        return _mk_fake_instance("daytona", f"replaced-{provision_calls['n']}")

    with patch("api.providers.daytona.get_daytona_sandbox_status",
               new=AsyncMock(side_effect=fake_status)), \
         patch("api.providers.daytona.provision_daytona_sandbox",
               new=AsyncMock(side_effect=fake_provision)), \
         patch("api.server.ensure_volume_supervisor",
               new=AsyncMock(return_value=None)):
        sess = await dbmod.get_session("s_racy")
        sb_new = await srv.ensure_sandbox(sess)

    # New sandbox is NOT the old one and IS in _INSTANCES.
    assert sb_new.id != "sb_racy"
    assert sb_new.id in srv._INSTANCES
    # Old sandbox is gone from both DB and _INSTANCES.
    assert await dbmod.get_sandbox("sb_racy") is None
    assert "sb_racy" not in srv._INSTANCES
    # Exactly one provision — not a storm.
    assert provision_calls["n"] == 1

    # Session now points at the new sandbox.
    sess_fresh = await dbmod.get_session("s_racy")
    assert sess_fresh["current_sandbox_id"] == sb_new.id

    # A reattach event was logged.
    async with dbmod.get_db() as conn:
        log_rows = await (await conn.execute(
            "SELECT event_type, payload FROM session_log WHERE session_id='s_racy'"
        )).fetchall()
    reattach = [r for r in log_rows if r["event_type"] == "sandbox_reattach"]
    assert len(reattach) == 1
    assert reattach[0]["payload"]["old_sandbox_id"] == "sb_racy"
    assert reattach[0]["payload"]["new_sandbox_id"] == sb_new.id


@pytest.mark.asyncio
@pytest.mark.timeout(10)
async def test_reaper_race_concurrent_with_ensure_sandbox(client):
    """ensure_sandbox and a concurrent raw row-delete coroutine race.
    The session lock should serialize them: the caller either (a) gets
    a running sandbox, or (b) gets a clean failure. No phantom
    _INSTANCES entries, no orphan rows."""
    from api.models import AgentConfig, AgentRecord, SandboxRecord
    await _seed_volume()
    await dbmod.upsert_agent(AgentRecord(
        id="a_c", name="A", config=AgentConfig(agent_type="claude"),
    ))
    sb = SandboxRecord(
        id="sb_conc", provider="daytona", sandbox_ref="dt-conc",
        status="running", root="/home/daytona",
        volume_id="v_stress", subpath="agents/a_c/home",
    )
    await dbmod.upsert_sandbox(sb)
    async with dbmod.get_db() as conn:
        await conn.execute(
            "INSERT INTO sessions (id, agent_id, volume_id, current_sandbox_id)"
            " VALUES (%s, %s, %s, %s)",
            ("s_conc", "a_c", "v_stress", "sb_conc"),
        )

    async def slow_status(ref):
        await asyncio.sleep(0.05)
        return "running"

    async def reaper_delete():
        # Fire before ensure_sandbox reads — force a race where the reaper
        # sometimes wins, sometimes loses.
        await asyncio.sleep(0.01)
        await dbmod.delete_sandbox("sb_conc")
        srv._INSTANCES.pop("sb_conc", None)
        # Sessions.current_sandbox_id dangles — on the next ensure_sandbox
        # call it will reprovision.  (The schema uses ON DELETE SET NULL
        # for this pointer? check.)

    async def fake_provision(**kw):
        return _mk_fake_instance("daytona", f"new-{uuid.uuid4().hex[:4]}")

    with patch("api.providers.daytona.get_daytona_sandbox_status",
               new=AsyncMock(side_effect=slow_status)), \
         patch("api.providers.daytona.provision_daytona_sandbox",
               new=AsyncMock(side_effect=fake_provision)), \
         patch("api.server.ensure_volume_supervisor",
               new=AsyncMock(return_value=None)):
        sess = await dbmod.get_session("s_conc")
        ensure_task = asyncio.create_task(srv.ensure_sandbox(sess))
        reap_task = asyncio.create_task(reaper_delete())
        results = await asyncio.gather(ensure_task, reap_task,
                                       return_exceptions=True)

    ensure_result = results[0]
    # Two legal outcomes: a SandboxRecord, or a clean error — never a partial crash.
    if isinstance(ensure_result, Exception):
        # Must be a structured error (HTTPException / RuntimeError), not an
        # AttributeError / KeyError suggesting we hit missing state.
        assert not isinstance(ensure_result, (AttributeError, KeyError, TypeError)), (
            f"ensure_sandbox raised a state-machine crash: {ensure_result!r}"
        )
    else:
        sb_got = ensure_result
        assert sb_got.status == "running"
        # Race outcome: the record returned may now be deleted (reaper
        # squeezed in after the probe).  Only assert on _INSTANCES state.

    # Critical invariant: every sandbox row has a session pointer.
    # (A row with no pointer means the reaper-vs-provision race left an
    # orphan — this is the bug class we're hunting.)
    async with dbmod.get_db() as conn:
        orphan_rows = await (await conn.execute(
            "SELECT sb.id FROM sandboxes sb "
            "LEFT JOIN sessions s ON s.current_sandbox_id = sb.id "
            "WHERE s.id IS NULL"
        )).fetchall()
    assert orphan_rows == [], (
        f"reaper race left orphan sandbox rows: {orphan_rows}"
    )

    # No stale _INSTANCES entry for the deleted sandbox.
    assert "sb_conc" not in srv._INSTANCES or \
        await dbmod.get_sandbox("sb_conc") is not None, (
        "phantom _INSTANCES['sb_conc'] entry for a deleted DB row"
    )
