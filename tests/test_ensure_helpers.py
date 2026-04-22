"""Unit tests for ensure_sandbox / ensure_runtime helpers."""
from __future__ import annotations
import asyncio
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
async def setup(clean_db):
    yield


async def _mk_fixtures(provider: str = "daytona"):
    from api.models import AgentConfig, AgentRecord, VolumeRecord
    await dbmod.upsert_agent(AgentRecord(id="a1", name="A",
                                         config=AgentConfig(agent_type="claude")))
    provider_ref = {"daytona": "dt-v", "docker": "agentsdk-testvol",
                    "local": "/tmp/agentsdk-testvol"}[provider]
    await dbmod.upsert_volume(VolumeRecord(id="v1", name="v", provider=provider,
                                           provider_ref=provider_ref))
    async with dbmod.get_db() as conn:
        await conn.execute(
            "INSERT INTO sessions (id, agent_id, volume_id) VALUES (%s,%s,%s)",
            ("s1", "a1", "v1"),
        )


@pytest.mark.asyncio
async def test_ensure_sandbox_creates_when_none(setup):
    await _mk_fixtures()
    from api.providers import ProviderInstance
    created = []

    async def fake_provision(**kw):
        created.append(kw)
        return ProviderInstance(provider="daytona", url="http://fake",
                                root="/home/daytona", sandbox_id=f"dt-{len(created)}")

    # _provision_new now dispatches through providers.provision_sandbox →
    # _PROVIDER_MODS[provider].create_sandbox. Patch at the daytona-module level
    # so the dispatch resolves to the mock.
    with patch("api.providers.daytona.provision_daytona_sandbox",
               new=AsyncMock(side_effect=fake_provision)), \
         patch("api.server.ensure_volume_supervisor", new=AsyncMock(return_value=None)):
        sess = await dbmod.get_session("s1")
        sb = await srv.ensure_sandbox(sess)

    assert sb is not None
    assert len(created) == 1
    assert created[0]["volume_id"] == "dt-v"
    assert created[0]["subpath"] == "agents/a1/home"


@pytest.mark.asyncio
async def test_ensure_sandbox_returns_existing_when_running(setup):
    await _mk_fixtures()
    from api.models import SandboxRecord
    sb = SandboxRecord(id="sb1", provider="daytona", sandbox_ref="dt-live",
                       status="running", root="/home/daytona",
                       volume_id="v1", subpath="agents/a1/home")
    await dbmod.upsert_sandbox(sb)
    await dbmod.set_session_current_sandbox("s1", "sb1")

    with patch("api.providers.daytona.get_daytona_sandbox_status",
               new=AsyncMock(return_value="running")), \
         patch("api.providers.daytona.provision_daytona_sandbox",
               new=AsyncMock(side_effect=AssertionError("should not provision"))):
        sess = await dbmod.get_session("s1")
        got = await srv.ensure_sandbox(sess)
    assert got.id == "sb1"


@pytest.mark.asyncio
async def test_ensure_sandbox_reprovisions_when_missing_emits_reattach(setup):
    await _mk_fixtures()
    # Create a sandbox row and point the session at it, then patch get_sandbox
    # to return None — simulating the case where the row was deleted externally
    # (e.g. by the reaper) in a way that bypassed the ON DELETE SET NULL trigger.
    from api.models import SandboxRecord
    dead = SandboxRecord(id="sb_dead", provider="daytona", sandbox_ref="dt-dead",
                         status="running", root="/home/daytona",
                         volume_id="v1", subpath="agents/a1/home")
    await dbmod.upsert_sandbox(dead)
    await dbmod.set_session_current_sandbox("s1", "sb_dead")

    from api.providers import ProviderInstance
    async def fake_provision(**kw):
        return ProviderInstance(provider="daytona", url="http://new",
                                root="/home/daytona", sandbox_id="dt-new")

    # Patch get_sandbox to simulate the row being gone after the FK pointer was set.
    original_get_sandbox = dbmod.get_sandbox
    async def fake_get_sandbox(sb_id):
        if sb_id == "sb_dead":
            return None
        return await original_get_sandbox(sb_id)

    with patch("api.providers.daytona.provision_daytona_sandbox",
               new=AsyncMock(side_effect=fake_provision)), \
         patch("api.server.ensure_volume_supervisor", new=AsyncMock(return_value=None)), \
         patch("api.db.get_sandbox", new=AsyncMock(side_effect=fake_get_sandbox)), \
         patch("api.server.get_sandbox", new=AsyncMock(side_effect=fake_get_sandbox)):
        sess = await dbmod.get_session("s1")
        sb = await srv.ensure_sandbox(sess)

    assert sb.sandbox_ref == "dt-new"
    async with dbmod.get_db() as conn:
        rows = await (await conn.execute(
            "SELECT event_type, payload FROM session_log WHERE session_id='s1'"
        )).fetchall()
    reattach = [r for r in rows if r["event_type"] == "sandbox_reattach"]
    assert len(reattach) == 1
    assert reattach[0]["payload"]["old_sandbox_id"] == "sb_dead"


@pytest.mark.asyncio
async def test_ensure_sandbox_starts_stopped(setup):
    await _mk_fixtures()
    from api.models import SandboxRecord
    sb = SandboxRecord(id="sb1", provider="daytona", sandbox_ref="dt-paused",
                       status="stopped", root="/home/daytona",
                       volume_id="v1", subpath="agents/a1/home")
    await dbmod.upsert_sandbox(sb)
    await dbmod.set_session_current_sandbox("s1", "sb1")

    start_calls = []
    async def fake_start(ref):
        start_calls.append(ref)

    with patch("api.providers.daytona.get_daytona_sandbox_status",
               new=AsyncMock(return_value="stopped")), \
         patch("api.providers.daytona.start_daytona",
               new=AsyncMock(side_effect=fake_start)), \
         patch("api.providers.daytona.provision_daytona_sandbox",
               new=AsyncMock(side_effect=AssertionError("should not provision"))):
        sess = await dbmod.get_session("s1")
        got = await srv.ensure_sandbox(sess)
    assert got.id == "sb1"
    assert start_calls == ["dt-paused"]


@pytest.mark.asyncio
async def test_ensure_runtime_reuses_healthy_state(setup):
    await _mk_fixtures()
    from api.models import SandboxRecord, SessionState
    sb = SandboxRecord(id="sb1", provider="daytona", sandbox_ref="dt-r",
                       status="running", root="/home/daytona",
                       volume_id="v1", subpath="agents/a1/home")
    await dbmod.upsert_sandbox(sb)
    await dbmod.set_session_current_sandbox("s1", "sb1")

    # Pre-populate in-memory state
    fake_client = AsyncMock()
    fake_client.base_url = "http://existing"
    state = SessionState(session_id="s1", agent_id="a1", sandbox_id="sb1",
                         acp_session_id="acp1", inner_session_id="inner1",
                         agent_type="claude", client=fake_client,
                         supervisor_url="http://existing")
    srv.SESSIONS["s1"] = state

    with patch("api.providers._wait_for_health",
               new=AsyncMock(return_value=True)):
        sess = await dbmod.get_session("s1")
        got = await srv.ensure_runtime(sess, sb)

    assert got is state  # identity — no rebuild


@pytest.mark.asyncio
@pytest.mark.parametrize("provider", ["daytona", "docker", "local"])
async def test_ensure_sandbox_creates_across_providers(setup, provider):
    """_provision_new must dispatch through the provider-agnostic wrapper."""
    await _mk_fixtures(provider)
    from api.providers import ProviderInstance
    created = []

    async def fake_provision(**kw):
        created.append(kw)
        # Local/docker put a URL + port on the instance; daytona has no URL yet.
        url = "http://fake:9999" if provider != "daytona" else ""
        port = 9999 if provider != "daytona" else None
        return ProviderInstance(
            provider=provider, url=url, root="/home/x",
            sandbox_id=f"{provider}-sb-1",
            container_id=f"{provider}-cid" if provider == "docker" else None,
            port=port,
        )

    patches = [
        patch("api.server.ensure_volume_supervisor", new=AsyncMock(return_value=None)),
    ]
    if provider == "daytona":
        patches.append(patch("api.providers.daytona.provision_daytona_sandbox",
                             new=AsyncMock(side_effect=fake_provision)))
    elif provider == "docker":
        patches.append(patch("api.providers.docker.create_sandbox",
                             new=AsyncMock(side_effect=fake_provision)))
    else:  # local
        patches.append(patch("api.providers.local.create_sandbox",
                             new=AsyncMock(side_effect=fake_provision)))

    with patches[0], patches[1]:
        sess = await dbmod.get_session("s1")
        sb = await srv.ensure_sandbox(sess)

    assert sb is not None
    assert sb.provider == provider
    assert len(created) == 1
    # volume_ref (daytona uses `volume_id`, others `volume_ref`)
    assert "volume_ref" in created[0] or "volume_id" in created[0]
    # subpath always present
    assert created[0].get("subpath") == "agents/a1/home"
    # Docker/Local: listen_port should be persisted.
    if provider != "daytona":
        assert sb.listen_port == 9999


@pytest.mark.asyncio
@pytest.mark.parametrize("provider", ["daytona", "docker", "local"])
async def test_ensure_sandbox_reuses_existing_running_across_providers(setup, provider):
    """When a sandbox row exists and provider reports 'running', no reprovision."""
    await _mk_fixtures(provider)
    from api.models import SandboxRecord
    sb = SandboxRecord(id="sb1", provider=provider, sandbox_ref=f"{provider}-live",
                       status="running", root="/home/x",
                       volume_id="v1", subpath="agents/a1/home",
                       listen_port=9999 if provider != "daytona" else None)
    await dbmod.upsert_sandbox(sb)
    await dbmod.set_session_current_sandbox("s1", "sb1")

    status_paths = {
        "daytona": "api.providers.daytona.get_daytona_sandbox_status",
        "docker":  "api.providers.docker.get_sandbox_status",
        "local":   "api.providers.local.get_sandbox_status",
    }
    create_paths = {
        "daytona": "api.providers.daytona.provision_daytona_sandbox",
        "docker":  "api.providers.docker.create_sandbox",
        "local":   "api.providers.local.create_sandbox",
    }
    with patch(status_paths[provider], new=AsyncMock(return_value="running")), \
         patch(create_paths[provider],
               new=AsyncMock(side_effect=AssertionError("should not provision"))):
        sess = await dbmod.get_session("s1")
        got = await srv.ensure_sandbox(sess)
    assert got.id == "sb1"


@pytest.mark.asyncio
async def test_ensure_runtime_rebuilds_when_missing(setup):
    await _mk_fixtures()
    from api.models import SandboxRecord
    sb = SandboxRecord(id="sb1", provider="daytona", sandbox_ref="dt-r",
                       status="running", root="/home/daytona",
                       volume_id="v1", subpath="agents/a1/home")
    await dbmod.upsert_sandbox(sb)
    await dbmod.set_session_current_sandbox("s1", "sb1")

    srv.SESSIONS.pop("s1", None)  # no in-memory state

    import os
    fake_client = AsyncMock()
    with patch("api.providers.daytona.ensure_supervisor_url",
               new=AsyncMock(return_value="http://fresh")), \
         patch("api.server.AcpClient", return_value=fake_client), \
         patch("api.server._start_session_tasks"):
        fake_client.initialize = AsyncMock(return_value={"sessionId": "inner-new"})
        fake_client.get_inner_session_id = lambda *a: "inner-new"

        sess = await dbmod.get_session("s1")
        got = await srv.ensure_runtime(sess, sb)

    assert got.session_id == "s1"
    assert got.sandbox_id == "sb1"
    assert got.supervisor_url == "http://fresh"


# ---------------------------------------------------------------------------
# Scenario 3 — Concurrent ensure_sandbox on fresh session.
# Two coroutines racing into ensure_sandbox() on the same session with
# current_sandbox_id=NULL must only result in ONE provider provision call;
# both callers should see the same sandbox returned.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_concurrent_ensure_sandbox_provisions_once(setup):
    await _mk_fixtures()
    from api.providers import ProviderInstance

    call_count = 0

    async def fake_provision(**kw):
        nonlocal call_count
        call_count += 1
        # Yield so a second caller has a chance to enter _ensure_sandbox_locked
        # and hit the lock — which is exactly the scenario we want to exercise.
        await asyncio.sleep(0.05)
        return ProviderInstance(
            provider="daytona", url="http://fake",
            root="/home/daytona", sandbox_id=f"dt-{call_count}",
        )

    # Once T1 has committed a sandbox row, T2 acquires the lock, re-reads the
    # session, and sees current_sandbox_id pointing at the new sandbox. The
    # probe path then asks the provider for status; we stub that to 'running'
    # so T2 returns the same sandbox instead of reprovisioning.
    with patch("api.providers.daytona.provision_daytona_sandbox",
               new=AsyncMock(side_effect=fake_provision)), \
         patch("api.providers.daytona.get_daytona_sandbox_status",
               new=AsyncMock(return_value="running")), \
         patch("api.server.ensure_volume_supervisor", new=AsyncMock(return_value=None)):
        sess = await dbmod.get_session("s1")
        sb_a, sb_b = await asyncio.gather(
            srv.ensure_sandbox(sess),
            srv.ensure_sandbox(sess),
        )

    # Exactly one provision, and both coroutines see the same sandbox.
    assert call_count == 1, f"expected 1 provision, got {call_count}"
    assert sb_a.id == sb_b.id


# ---------------------------------------------------------------------------
# Scenario 9 — Supervisor health-check timeout.
# When the supervisor never becomes healthy, ensure_runtime must not hang
# indefinitely. It should propagate a clean failure within a reasonable
# time bound.
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Scenario 14 — DB connection loss during ensure_sandbox (_provision_new).
# M4 wraps the sandbox INSERT and the session UPDATE in a single transaction
# (``async with get_db() as conn``). If the UPDATE fails, the INSERT must
# roll back so no orphan ``sandboxes`` row is left with no session pointing
# at it. After the failure, calling ensure_sandbox again should successfully
# provision and leave the DB in a consistent state.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ensure_sandbox_db_failure_mid_provision(setup):
    """Simulate DB failure on the UPDATE sessions ... write inside the
    atomic provision block. The whole transaction must roll back — the
    sandbox INSERT is NOT visible after the failure."""
    import psycopg
    from contextlib import asynccontextmanager
    from api.providers import ProviderInstance

    await _mk_fixtures()

    destroy_calls = []
    provision_calls = 0

    async def fake_provision(**kw):
        nonlocal provision_calls
        provision_calls += 1
        return ProviderInstance(
            provider="daytona", url="http://provisioned",
            root="/home/daytona",
            sandbox_id=f"dt-prov-{provision_calls}",
        )

    async def fake_destroy_daytona(inst, *a, **kw):
        destroy_calls.append(inst)

    # Wrap get_db: on the first ``async with get_db()`` block after the
    # provision (i.e. the atomic INSERT+UPDATE block), intercept conn.execute
    # so the UPDATE sessions ... raises psycopg.OperationalError. Other
    # get_db() calls (the ensure_volume_supervisor advisory-lock read, the
    # log_event writes) are passed through.
    real_get_db = dbmod.get_db
    # We need to make ONLY the specific block that contains both an INSERT
    # sandboxes and an UPDATE sessions fail on the UPDATE. Easiest way:
    # make conn.execute raise whenever the SQL starts with 'UPDATE sessions'.
    sabotage_update_sessions = {"on": True}

    @asynccontextmanager
    async def sabotaged_get_db():
        async with real_get_db() as conn:
            real_execute = conn.execute

            async def fake_execute(sql, params=None, *a, **kw):
                if (
                    sabotage_update_sessions["on"]
                    and isinstance(sql, str)
                    and sql.strip().upper().startswith("UPDATE SESSIONS")
                    and "CURRENT_SANDBOX_ID" in sql.upper()
                ):
                    raise psycopg.OperationalError(
                        "simulated DB failure on UPDATE sessions"
                    )
                if params is None:
                    return await real_execute(sql, *a, **kw)
                return await real_execute(sql, params, *a, **kw)

            conn.execute = fake_execute  # type: ignore[method-assign]
            yield conn

    with patch("api.providers.daytona.provision_daytona_sandbox",
               new=AsyncMock(side_effect=fake_provision)), \
         patch("api.server.ensure_volume_supervisor",
               new=AsyncMock(return_value=None)), \
         patch("api.server.get_db", new=sabotaged_get_db), \
         patch("api.providers.destroy_daytona",
               new=AsyncMock(side_effect=fake_destroy_daytona)):
        sess = await dbmod.get_session("s1")
        with pytest.raises(psycopg.OperationalError):
            await srv.ensure_sandbox(sess)

    # --- Post-failure invariants --------------------------------------------
    # Sandbox INSERT must have been rolled back.
    async with dbmod.get_db() as conn:
        rows = await (await conn.execute(
            "SELECT id FROM sandboxes WHERE volume_id = %s", ("v1",),
        )).fetchall()
    assert rows == [], (
        f"expected 0 sandbox rows after rollback, got {rows}. "
        "This suggests the INSERT + UPDATE are no longer in a single "
        "transaction (M4 regression)."
    )

    # Session's current_sandbox_id is still NULL.
    sess = await dbmod.get_session("s1")
    assert sess["current_sandbox_id"] is None

    # --- Recovery: subsequent ensure_sandbox succeeds -----------------------
    sabotage_update_sessions["on"] = False
    with patch("api.providers.daytona.provision_daytona_sandbox",
               new=AsyncMock(side_effect=fake_provision)), \
         patch("api.server.ensure_volume_supervisor",
               new=AsyncMock(return_value=None)):
        sess = await dbmod.get_session("s1")
        sb = await srv.ensure_sandbox(sess)

    assert sb is not None
    # Second provision call (first attempt bailed after provider returned).
    assert provision_calls >= 2, (
        f"expected retry to reach the provider; provision_calls={provision_calls}"
    )
    # DB now consistent: sandbox row exists AND session points at it.
    sess = await dbmod.get_session("s1")
    assert sess["current_sandbox_id"] == sb.id


@pytest.mark.asyncio
@pytest.mark.timeout(20)
async def test_ensure_runtime_raises_when_supervisor_never_healthy(setup):
    """Cold start where the supervisor URL never answers /v1/health.

    _wait_for_health is called inside the provider's ensure_supervisor_url
    (daytona path). If it always returns False, the daytona implementation
    raises. We simulate that by making ensure_supervisor_url itself raise
    the underlying timeout error, and verify ensure_runtime surfaces it
    cleanly (HTTPException 500 with a descriptive message) rather than
    hanging.
    """
    await _mk_fixtures()
    from api.models import SandboxRecord
    sb = SandboxRecord(id="sb_timeout", provider="daytona", sandbox_ref="dt-r",
                       status="running", root="/home/daytona",
                       volume_id="v1", subpath="agents/a1/home")
    await dbmod.upsert_sandbox(sb)
    await dbmod.set_session_current_sandbox("s1", "sb_timeout")
    srv.SESSIONS.pop("s1", None)
    srv._INSTANCES.pop("sb_timeout", None)

    # Simulate the terminal state: the provider tried to start the supervisor
    # but health never returned True. Daytona wraps this as a RuntimeError.
    async def fake_ensure_supervisor_url(*a, **kw):
        raise RuntimeError("supervisor did not become healthy after N retries")

    with patch("api.providers.daytona.ensure_supervisor_url",
               new=AsyncMock(side_effect=fake_ensure_supervisor_url)), \
         patch("api.providers._wait_for_health",
               new=AsyncMock(return_value=False)):
        sess = await dbmod.get_session("s1")
        with pytest.raises(Exception) as excinfo:
            await srv.ensure_runtime(sess, sb)

    # Error must be clean — not a hang — and must reference health/supervisor.
    msg = str(excinfo.value).lower()
    assert any(k in msg for k in ("supervisor", "health", "timeout", "failed")), (
        f"expected descriptive error, got: {excinfo.value!r}"
    )
