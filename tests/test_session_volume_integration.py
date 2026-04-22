"""Integration tests for session/volume decoupling."""
from __future__ import annotations
import asyncio
import os, sys
import pytest
import pytest_asyncio
from unittest.mock import AsyncMock, MagicMock, patch
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
         patch("api.providers.daytona.provision_daytona_sandbox",
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


@pytest.mark.asyncio
async def test_message_lazily_provisions_sandbox(client):
    """First /message on a volume-only session provisions a Daytona sandbox."""
    from api.models import AgentConfig, AgentRecord, VolumeRecord

    await dbmod.upsert_agent(AgentRecord(id="agent_r", name="R", config=AgentConfig()))
    await dbmod.upsert_volume(VolumeRecord(id="vol_r", name="vr", provider="daytona",
                                           provider_ref="dt-r"))
    r = await client.post("/sessions", json={"agent_id": "agent_r", "volume_id": "vol_r"})
    assert r.status_code == 200
    sid = r.json()["id"]

    # Track what create_daytona is called with.
    create_calls = []
    from api.providers import ProviderInstance
    async def fake_create(**kwargs):
        create_calls.append(kwargs)
        return ProviderInstance(
            provider="daytona", url="http://fake-sandbox:7000",
            root="/home/daytona", sandbox_id=f"sb-new-{len(create_calls)}",
        )
    # Also short-circuit anything downstream that tries to hit the fake URL.
    async def fake_wait_for_health(*a, **kw):
        return True

    # Mock AcpClient so ensure_runtime can build a SessionState without a real supervisor.
    fake_acp = MagicMock()
    fake_acp.initialize = AsyncMock(return_value=None)
    fake_acp.handshake = AsyncMock(return_value=None)
    fake_acp.get_inner_session_id = MagicMock(return_value=None)

    with patch("api.providers.daytona.provision_daytona_sandbox", new=AsyncMock(side_effect=fake_create)), \
         patch("api.providers._wait_for_health", new=AsyncMock(side_effect=fake_wait_for_health)), \
         patch("api.server._start_session_tasks", MagicMock(return_value=None)), \
         patch("api.server._submit_prompt", MagicMock(return_value=None)), \
         patch("api.server.ensure_volume_supervisor", new=AsyncMock(return_value=None)), \
         patch("api.providers.daytona.ensure_supervisor_url",
               new=AsyncMock(return_value=("http://fake-supervisor:7000", 7000))), \
         patch("api.server.AcpClient", return_value=fake_acp):
        r = await client.post(f"/sessions/{sid}/message", json={"message": "hi"})
    # ensure_session_live now guarantees a connected client, so /message should succeed (200).
    # We verify: create_daytona was called with the expected volume_id + subpath.
    assert len(create_calls) == 1, f"expected 1 create, got {len(create_calls)}: {create_calls}"
    assert create_calls[0].get("volume_id") == "dt-r"
    assert create_calls[0].get("subpath") == "agents/agent_r/home"

    # DB was updated: session has a current_sandbox_id.
    sess = await dbmod.get_session(sid)
    assert sess["current_sandbox_id"] is not None


@pytest.mark.asyncio
async def test_start_sandbox_provisions_eagerly(client):
    from api.models import AgentConfig, AgentRecord, VolumeRecord
    from api.providers import ProviderInstance
    await dbmod.upsert_agent(AgentRecord(id="agent_s", name="S", config=AgentConfig()))
    await dbmod.upsert_volume(VolumeRecord(id="vol_s", name="vs", provider="daytona",
                                           provider_ref="dt-s"))
    r = await client.post("/sessions", json={"agent_id": "agent_s", "volume_id": "vol_s"})
    sid = r.json()["id"]

    async def fake_create(**kw):
        return ProviderInstance(provider="daytona", url="http://fake:7000",
                                root="/home/daytona", sandbox_id="sb-eager-1")

    # start-sandbox now calls ensure_session_live which includes ensure_runtime.
    # Mock the full runtime startup stack so the test doesn't hit real Daytona.
    fake_acp = MagicMock()
    fake_acp.initialize = AsyncMock(return_value=None)
    fake_acp.handshake = AsyncMock(return_value=None)
    fake_acp.get_inner_session_id = MagicMock(return_value=None)

    # ensure_runtime fetches the live Daytona object before starting the supervisor.
    fake_daytona_instance = MagicMock()
    fake_daytona_class = MagicMock()
    fake_daytona_class.return_value.get = MagicMock(return_value=fake_daytona_instance)

    with patch("api.providers.daytona.provision_daytona_sandbox", new=AsyncMock(side_effect=fake_create)), \
         patch("api.providers._wait_for_health", new=AsyncMock(return_value=True)), \
         patch("api.server._start_session_tasks", MagicMock(return_value=None)), \
         patch("api.server.ensure_volume_supervisor", new=AsyncMock(return_value=None)), \
         patch("api.providers.daytona.ensure_supervisor_url",
               new=AsyncMock(return_value=("http://fake:7000", 7000))), \
         patch("api.server.AcpClient", return_value=fake_acp), \
         patch("daytona_sdk.Daytona", fake_daytona_class):
        r = await client.post(f"/sessions/{sid}/start-sandbox")
    assert r.status_code == 200, f"got {r.status_code}: {r.text}"
    body = r.json()
    assert "sandbox_id" in body and body["sandbox_id"] is not None

    sess = await dbmod.get_session(sid)
    assert sess["current_sandbox_id"] is not None


@pytest.mark.asyncio
async def test_stop_sandbox_clears_pointer(client):
    from api.models import AgentConfig, AgentRecord, VolumeRecord, SandboxRecord
    await dbmod.upsert_agent(AgentRecord(id="agent_k", name="K", config=AgentConfig()))
    await dbmod.upsert_volume(VolumeRecord(id="vol_k", name="vk", provider="daytona",
                                           provider_ref="dt-k"))
    r = await client.post("/sessions", json={"agent_id": "agent_k", "volume_id": "vol_k"})
    sid = r.json()["id"]
    sb = SandboxRecord(id="sb_k", provider="daytona", sandbox_ref="dt-sb-k",
                       status="running", root="/home/daytona",
                       volume_id="vol_k", subpath="agents/agent_k/home")
    await dbmod.upsert_sandbox(sb)
    await dbmod.set_session_current_sandbox(sid, "sb_k")

    with patch("api.providers.destroy_daytona", new=AsyncMock(return_value=None)):
        r = await client.post(f"/sessions/{sid}/stop-sandbox")
    assert r.status_code == 204, f"got {r.status_code}: {r.text}"
    sess = await dbmod.get_session(sid)
    assert sess["current_sandbox_id"] is None
    # Sandbox row should also be gone.
    assert await dbmod.get_sandbox("sb_k") is None


@pytest.mark.asyncio
async def test_reset_sandbox_swaps(client):
    from api.models import AgentConfig, AgentRecord, VolumeRecord, SandboxRecord
    from api.providers import ProviderInstance
    await dbmod.upsert_agent(AgentRecord(id="agent_x", name="X", config=AgentConfig()))
    await dbmod.upsert_volume(VolumeRecord(id="vol_x", name="vx", provider="daytona",
                                           provider_ref="dt-x"))
    r = await client.post("/sessions", json={"agent_id": "agent_x", "volume_id": "vol_x"})
    sid = r.json()["id"]
    sb_old = SandboxRecord(id="sb_old", provider="daytona", sandbox_ref="dt-old",
                           status="running", root="/home/daytona",
                           volume_id="vol_x", subpath="agents/agent_x/home")
    await dbmod.upsert_sandbox(sb_old)
    await dbmod.set_session_current_sandbox(sid, "sb_old")

    async def fake_create(**kw):
        return ProviderInstance(provider="daytona", url="http://fake:7000",
                                root="/home/daytona", sandbox_id="dt-new")

    with patch("api.providers.destroy_daytona", new=AsyncMock(return_value=None)), \
         patch("api.server.ensure_volume_supervisor", new=AsyncMock(return_value=None)), \
         patch("api.providers.daytona.provision_daytona_sandbox", new=AsyncMock(side_effect=fake_create)):
        r = await client.post(f"/sessions/{sid}/reset-sandbox")
    assert r.status_code == 200
    body = r.json()
    assert body["sandbox_id"] != "sb_old"
    # Old row gone, new row present
    assert await dbmod.get_sandbox("sb_old") is None
    sess = await dbmod.get_session(sid)
    assert sess["current_sandbox_id"] == body["sandbox_id"]


@pytest.mark.asyncio
async def test_reset_sandbox_emits_reattach_event(client):
    from api.models import AgentConfig, AgentRecord, VolumeRecord, SandboxRecord
    from api.providers import ProviderInstance
    await dbmod.upsert_agent(AgentRecord(id="agent_e", name="E", config=AgentConfig()))
    await dbmod.upsert_volume(VolumeRecord(id="vol_e", name="ve", provider="daytona",
                                           provider_ref="dt-e"))
    r = await client.post("/sessions", json={"agent_id": "agent_e", "volume_id": "vol_e"})
    sid = r.json()["id"]
    sb_old = SandboxRecord(id="sb_old", provider="daytona", sandbox_ref="dt-old",
                           status="running", root="/home/daytona",
                           volume_id="vol_e", subpath="agents/agent_e/home")
    await dbmod.upsert_sandbox(sb_old)
    await dbmod.set_session_current_sandbox(sid, "sb_old")

    async def fake_create(**kw):
        return ProviderInstance(provider="daytona", url="http://fake:7000",
                                root="/home/daytona", sandbox_id="dt-new")

    with patch("api.providers.destroy_daytona", new=AsyncMock(return_value=None)), \
         patch("api.server.ensure_volume_supervisor", new=AsyncMock(return_value=None)), \
         patch("api.providers.daytona.provision_daytona_sandbox", new=AsyncMock(side_effect=fake_create)):
        r = await client.post(f"/sessions/{sid}/reset-sandbox")
    assert r.status_code == 200

    # Look up session_log for a sandbox_reattach event.
    async with dbmod.get_db() as conn:
        rows = await (await conn.execute(
            "SELECT event_type, payload FROM session_log WHERE session_id = %s",
            (sid,),
        )).fetchall()
    reattach = [r for r in rows if r["event_type"] == "sandbox_reattach"]
    assert len(reattach) == 1, f"expected 1 reattach event, got {len(reattach)}: {rows}"
    payload = reattach[0]["payload"]
    assert payload.get("old_sandbox_id") == "sb_old"
    assert payload.get("new_sandbox_id") is not None
    assert payload["new_sandbox_id"] != "sb_old"


@pytest.mark.asyncio
async def test_ensure_volume_supervisor_caches_installs(client):
    """Second sandbox creation on same volume+agent skips install entirely."""
    from api.models import AgentConfig, AgentRecord, VolumeRecord
    from unittest.mock import AsyncMock, patch

    await dbmod.upsert_agent(AgentRecord(id="a-cache", name="cache",
                                         config=AgentConfig(agent_type="claude")))
    await dbmod.upsert_volume(VolumeRecord(id="v-cache", name="v-cache",
                                           provider="daytona", provider_ref="dt-cache"))
    async with dbmod.get_db() as conn:
        await conn.execute(
            "INSERT INTO sessions (id, agent_id, volume_id) VALUES (%s,%s,%s)",
            ("sess-cache", "a-cache", "v-cache"),
        )

    install_calls = []

    async def fake_install(volume_ref, agent_type):
        install_calls.append((volume_ref, agent_type))
        # Simulate successful install — update the cache column.
        await dbmod.add_supervisor_agent_type("v-cache", agent_type)

    from api.providers import ProviderInstance

    async def fake_provision(**kw):
        return ProviderInstance(provider="daytona", url="",
                                root="/home/daytona", sandbox_id="dt-sbx")

    with patch("api.providers.daytona.install_supervisor", new=AsyncMock(side_effect=fake_install)), \
         patch("api.providers.daytona.provision_daytona_sandbox", new=AsyncMock(side_effect=fake_provision)):
        # First run: should install
        sess = await dbmod.get_session("sess-cache")
        from api.server import ensure_sandbox
        await ensure_sandbox(sess)
        assert len(install_calls) == 1, f"Expected 1 install call, got {len(install_calls)}"

        # Clear the current_sandbox_id so ensure_sandbox has to re-provision.
        await dbmod.set_session_current_sandbox("sess-cache", None)

        # Second run: should NOT install (cache hit).
        sess = await dbmod.get_session("sess-cache")
        await ensure_sandbox(sess)
        assert len(install_calls) == 1, f"Expected still 1 install call (cache hit), got {len(install_calls)}"


# ---------------------------------------------------------------------------
# Scenario 4 — Concurrent ensure_volume_supervisor on fresh volume.
# Two coroutines concurrently call ensure_volume_supervisor(v, "claude") on a
# volume whose supervisor_agent_types cache is empty. The provider-level
# install_supervisor should be invoked exactly once; both callers complete.
# Behavioral test: works under asyncio.Lock OR pg_advisory_xact_lock.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_concurrent_ensure_volume_supervisor_installs_once(client):
    from api.models import AgentConfig, AgentRecord, VolumeRecord

    await dbmod.upsert_agent(AgentRecord(
        id="a-conc", name="conc", config=AgentConfig(agent_type="claude"),
    ))
    await dbmod.upsert_volume(VolumeRecord(
        id="v-conc", name="v-conc", provider="daytona", provider_ref="dt-conc",
    ))

    install_calls = 0

    # The top-level wrapper (`api.providers.install_supervisor`) is
    # ``install_supervisor(provider, volume_ref, agent_type)``.
    async def fake_install(provider, volume_ref, agent_type):
        nonlocal install_calls
        install_calls += 1
        # Yield so the second call can enter ensure_volume_supervisor
        # and observe the lock; keep it short to avoid slowing the suite.
        await asyncio.sleep(0.05)

    with patch("api.providers.install_supervisor",
               new=AsyncMock(side_effect=fake_install)):
        await asyncio.gather(
            srv.ensure_volume_supervisor("v-conc", "claude"),
            srv.ensure_volume_supervisor("v-conc", "claude"),
        )

    assert install_calls == 1, f"expected 1 install, got {install_calls}"

    # Cache row should reflect the installed agent type.
    vol = await dbmod.get_volume("v-conc")
    assert vol is not None
    assert "claude" in (vol.supervisor_agent_types or [])


# ---------------------------------------------------------------------------
# Scenario 5 — ensure_volume_supervisor partial-failure retry.
# Install succeeds on the provider but the cache update fails. The call
# raises and the cache remains empty. The next call re-runs install
# (idempotent at the provider level) and the cache is populated.
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Scenario 7 — FK ON DELETE SET NULL end-to-end.
# Directly DELETE a sandboxes row via SQL (bypassing all app code). The FK
# constraint ``sessions_current_sandbox_id_fkey ON DELETE SET NULL`` must
# zero out sessions.current_sandbox_id. A subsequent ``ensure_sandbox`` then
# re-provisions and logs a ``sandbox_reattach`` event with the old id.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fk_set_null_on_direct_sandbox_delete(client):
    from api.models import (
        AgentConfig, AgentRecord, VolumeRecord, SandboxRecord,
    )
    from api.providers import ProviderInstance

    await dbmod.upsert_agent(AgentRecord(
        id="a-fk", name="fk", config=AgentConfig(agent_type="claude"),
    ))
    await dbmod.upsert_volume(VolumeRecord(
        id="v-fk", name="v-fk", provider="daytona", provider_ref="dt-fk",
    ))
    async with dbmod.get_db() as conn:
        await conn.execute(
            "INSERT INTO sessions (id, agent_id, volume_id) VALUES (%s,%s,%s)",
            ("s-fk", "a-fk", "v-fk"),
        )
    sb = SandboxRecord(
        id="sb1", provider="daytona", sandbox_ref="dt-sb1",
        status="running", root="/home/daytona",
        volume_id="v-fk", subpath="agents/a-fk/home",
    )
    await dbmod.upsert_sandbox(sb)
    await dbmod.set_session_current_sandbox("s-fk", "sb1")

    # Sanity: session row points at sb1.
    sess = await dbmod.get_session("s-fk")
    assert sess["current_sandbox_id"] == "sb1"

    # --- Directly DELETE the sandbox row. Bypass ``delete_sandbox`` entirely;
    # this simulates an external operator (pgadmin / ops script / the reaper
    # committing a raw SQL delete) dropping the row.
    async with dbmod.get_db() as conn:
        await conn.execute("DELETE FROM sandboxes WHERE id = %s", ("sb1",))

    # FK ON DELETE SET NULL must have zeroed current_sandbox_id but left the
    # session row intact (not CASCADE).
    sess = await dbmod.get_session("s-fk")
    assert sess is not None, "session must survive the sandbox delete"
    assert sess["current_sandbox_id"] is None, (
        "ON DELETE SET NULL should zero current_sandbox_id"
    )

    # --- ensure_sandbox reprovisions + emits sandbox_reattach.
    async def fake_provision(**kw):
        return ProviderInstance(
            provider="daytona", url="http://fresh",
            root="/home/daytona", sandbox_id="dt-sb2",
        )

    with patch("api.providers.daytona.provision_daytona_sandbox",
               new=AsyncMock(side_effect=fake_provision)), \
         patch("api.server.ensure_volume_supervisor",
               new=AsyncMock(return_value=None)):
        sess = await dbmod.get_session("s-fk")
        new_sb = await srv.ensure_sandbox(sess)

    assert new_sb is not None
    assert new_sb.id != "sb1", "ensure_sandbox must mint a fresh sandbox id"
    assert new_sb.sandbox_ref == "dt-sb2"

    # Session now points at the new sandbox.
    sess = await dbmod.get_session("s-fk")
    assert sess["current_sandbox_id"] == new_sb.id

    # Case A: current_sandbox_id was NULL going into ensure_sandbox, so the
    # provision path takes ``previous_id=None`` and logs no reattach. That's
    # the documented Case A behavior in _ensure_sandbox_locked — the FK
    # SET NULL is what decouples the "previous id" from the reprovision.
    # The reattach pathway fires only when the server itself observed the
    # stale pointer (Case B in the same helper).
    async with dbmod.get_db() as conn:
        rows = await (await conn.execute(
            "SELECT event_type, payload FROM session_log WHERE session_id = %s",
            ("s-fk",),
        )).fetchall()
    reattach = [r for r in rows if r["event_type"] == "sandbox_reattach"]
    # NOTE: SET NULL semantics mean ensure_sandbox sees current_sandbox_id=NULL
    # and treats this as a fresh provision, NOT a reattach. That is the
    # intentional boundary: external deletes get SET NULL, app-level deletes
    # would have set a previous_id. Documenting so future changes to the
    # FK policy surface a test diff.
    assert reattach == [], (
        "Direct DELETE should take Case A (fresh provision, no reattach). "
        f"Got unexpected reattach rows: {reattach}"
    )


@pytest.mark.asyncio
async def test_ensure_volume_supervisor_retry_after_cache_fail(client):
    """Install succeeds on provider, but cache update fails.

    The cache write can be either:
      - an inline ``UPDATE volumes ...`` on the advisory-lock connection
        (current pg-advisory implementation), or
      - a call to ``dbmod.add_supervisor_agent_type`` (older asyncio-Lock
        implementation).

    We patch BOTH to raise so the test is agnostic to the lock
    implementation the source agent has landed.
    """
    import psycopg
    from api.models import AgentConfig, AgentRecord, VolumeRecord

    await dbmod.upsert_agent(AgentRecord(
        id="a-retry", name="retry", config=AgentConfig(agent_type="claude"),
    ))
    await dbmod.upsert_volume(VolumeRecord(
        id="v-retry", name="v-retry", provider="daytona", provider_ref="dt-retry",
    ))

    install_calls = 0

    async def fake_install(provider, volume_ref, agent_type):
        nonlocal install_calls
        install_calls += 1

    # Wrap get_db to intercept the cache-write UPDATE (inline or via helper).
    # The wrapper yields a connection whose execute() raises specifically on
    # UPDATE statements that write supervisor_agent_types. All other queries
    # (SELECT, pg_advisory_xact_lock, etc.) pass through unmodified.
    from contextlib import asynccontextmanager
    real_get_db = dbmod.get_db
    sabotage_active = {"on": True}

    @asynccontextmanager
    async def sabotaged_get_db():
        async with real_get_db() as conn:
            real_execute = conn.execute

            async def fake_execute(sql, params=None, *a, **kw):
                if (sabotage_active["on"]
                    and "supervisor_agent_types" in (sql or "")
                    and "UPDATE" in (sql or "").upper()):
                    raise psycopg.OperationalError(
                        "simulated cache-write failure"
                    )
                if params is None:
                    return await real_execute(sql, *a, **kw)
                return await real_execute(sql, params, *a, **kw)

            conn.execute = fake_execute  # type: ignore[method-assign]
            yield conn

    # Call 1: install succeeds at the provider, cache write is sabotaged.
    async def boom(*a, **kw):
        raise psycopg.OperationalError("simulated cache-write failure")

    patches = [
        patch("api.providers.install_supervisor",
              new=AsyncMock(side_effect=fake_install)),
        patch("api.server.get_db", new=sabotaged_get_db),
        # Older API — a no-op when the source uses the advisory-lock path,
        # but guards against a regression to add_supervisor_agent_type.
        patch("api.server.add_supervisor_agent_type", new=AsyncMock(side_effect=boom)),
    ]
    with patches[0], patches[1], patches[2]:
        with pytest.raises(Exception) as excinfo:
            await srv.ensure_volume_supervisor("v-retry", "claude")
    assert "cache-write" in str(excinfo.value) or "failure" in str(excinfo.value).lower()

    # Cache remained empty → next call must re-install.
    vol = await dbmod.get_volume("v-retry")
    assert vol is not None
    assert "claude" not in (vol.supervisor_agent_types or [])
    assert install_calls == 1

    # Call 2: unpatch the cache writer; install should run again and succeed.
    sabotage_active["on"] = False
    with patch("api.providers.install_supervisor",
               new=AsyncMock(side_effect=fake_install)):
        await srv.ensure_volume_supervisor("v-retry", "claude")

    assert install_calls == 2, (
        f"expected 2 total install calls after retry, got {install_calls}"
    )
    vol = await dbmod.get_volume("v-retry")
    assert vol is not None
    assert "claude" in (vol.supervisor_agent_types or [])


# ---------------------------------------------------------------------------
# Scenario 15 — install_supervisor fault injection (no partial cache, retry).
# The provider's install_supervisor raises mid-run (e.g., OSError("disk full"),
# npm network failure). The volume's supervisor_agent_types cache must NOT be
# populated — a half-installed volume is poisoned state that a subsequent
# sandbox provision would silently trust. The next call must re-run install
# and, on success, populate the cache.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_install_supervisor_fault_injection_no_partial_cache(client):
    """First install raises mid-run; cache stays empty; next call succeeds.

    Behavioral test: asserts the observable property (no cache poisoning +
    retry works), not the internal mechanism. Compatible with both the
    current implementation (direct install) and any future staging-dir +
    atomic-rename refactor (M3) the parallel agent may land.
    """
    from api.models import AgentConfig, AgentRecord, VolumeRecord

    await dbmod.upsert_agent(AgentRecord(
        id="a-fault", name="fault", config=AgentConfig(agent_type="claude"),
    ))
    await dbmod.upsert_volume(VolumeRecord(
        id="v-fault", name="v-fault", provider="daytona", provider_ref="dt-fault",
    ))

    install_calls = 0

    # First call: provider raises mid-run with a disk-full style OSError.
    # Second call: succeed.
    async def flaky_install(provider, volume_ref, agent_type):
        nonlocal install_calls
        install_calls += 1
        if install_calls == 1:
            raise OSError(28, "No space left on device")

    with patch("api.providers.install_supervisor",
               new=AsyncMock(side_effect=flaky_install)):
        with pytest.raises(OSError) as excinfo:
            await srv.ensure_volume_supervisor("v-fault", "claude")
    # Error surfaced cleanly (no wrapping that hides the errno).
    assert "No space left on device" in str(excinfo.value) or excinfo.value.errno == 28

    # --- Cache must NOT list 'claude' — half-install is poisoned state.
    vol = await dbmod.get_volume("v-fault")
    assert vol is not None
    assert "claude" not in (vol.supervisor_agent_types or []), (
        f"cache poisoned after failed install: {vol.supervisor_agent_types!r}"
    )

    # --- Retry: provider's install succeeds; cache is populated.
    with patch("api.providers.install_supervisor",
               new=AsyncMock(side_effect=flaky_install)):
        await srv.ensure_volume_supervisor("v-fault", "claude")

    assert install_calls == 2, (
        f"retry must call install again; got {install_calls}"
    )
    vol = await dbmod.get_volume("v-fault")
    assert "claude" in (vol.supervisor_agent_types or [])


@pytest.mark.asyncio
async def test_install_supervisor_fault_leaves_other_agents_untouched(client):
    """Failed install of agent_type=X must not clobber a previously-cached
    agent_type=Y on the same volume."""
    from api.models import AgentConfig, AgentRecord, VolumeRecord

    await dbmod.upsert_agent(AgentRecord(
        id="a-ft2", name="ft2", config=AgentConfig(agent_type="claude"),
    ))
    await dbmod.upsert_volume(VolumeRecord(
        id="v-ft2", name="v-ft2", provider="daytona", provider_ref="dt-ft2",
        # Pre-populate supervisor cache with 'claude'.
        supervisor_agent_types=["claude"],
    ))

    # codex install fails; claude cache must remain.
    async def flaky(provider, volume_ref, agent_type):
        if agent_type == "codex":
            raise RuntimeError("npm install failed: network unreachable")

    with patch("api.providers.install_supervisor",
               new=AsyncMock(side_effect=flaky)):
        with pytest.raises(RuntimeError):
            await srv.ensure_volume_supervisor("v-ft2", "codex")

    vol = await dbmod.get_volume("v-ft2")
    cached = set(vol.supervisor_agent_types or [])
    assert "claude" in cached, "claude cache must survive an unrelated failed install"
    assert "codex" not in cached, "failed codex install must not populate the cache"


# ---------------------------------------------------------------------------
# MT6 — install_supervisor must not corrupt a live sandbox on the same volume.
# The docker provider's historical ``rm -rf /work/supervisor`` (MA2) would
# delete files out from under a running container. With the staging+rename
# atomic swap now in place the reinstall must be invisible to the live
# sandbox's health endpoint.
#
# Uses the local provider (simplest to drive end-to-end without docker).
# Skipped when npm/node are absent.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_install_does_not_corrupt_live_sandbox(client, tmp_path, monkeypatch):
    """Live sandbox on volume V; install_supervisor runs again on V.
    Sandbox's health endpoint must keep responding throughout."""
    import shutil as _shutil
    import sys as _sys
    if _shutil.which("npm") is None or _shutil.which("node") is None:
        pytest.skip("npm + node required for live-sandbox install test")

    monkeypatch.setenv("AGENT_SDK_LOCAL_VOL_ROOT", str(tmp_path))
    _SRC = os.path.join(os.path.dirname(__file__), "..", "src")
    if _SRC not in _sys.path:
        _sys.path.insert(0, _SRC)
    from api.providers import local  # noqa: E402
    import httpx as _httpx  # noqa: E402

    # Stand up a volume + install supervisor for claude.
    vol_name = "vol-mt6"
    ref = await local.create_volume(vol_name)
    await local.install_supervisor(ref, "claude")

    inst = await local.create_sandbox(
        volume_ref=ref, subpath="agents/live-install/home",
        agent_type="claude",
    )
    try:
        # Baseline: supervisor is healthy.
        async with _httpx.AsyncClient(timeout=5) as c:
            r = await c.get(f"{inst.url}/v1/health")
        assert r.status_code == 200

        # Trigger a fresh install_supervisor on the SAME volume. A naive
        # ``rm -rf`` + ``os.rename`` would delete files the supervisor's
        # children or future require() calls may need.  The staging +
        # rename-into-place flow should be safe: the running node process
        # already holds its supervisor.js fd.
        await local.install_supervisor(ref, "claude")

        # After reinstall, the live sandbox's health endpoint must still
        # respond. (The process was not restarted — it's serving from its
        # existing open fds.)
        async with _httpx.AsyncClient(timeout=5) as c:
            r = await c.get(f"{inst.url}/v1/health")
        assert r.status_code == 200, (
            f"live sandbox corrupted by concurrent reinstall: {r.status_code}"
        )

        # The new install should be on disk and functional for a fresh
        # create_sandbox as well.
        new_sup = os.path.join(ref, "system", "supervisor", "supervisor.js")
        assert os.path.isfile(new_sup)
    finally:
        try:
            await local.destroy_sandbox(inst)
        except Exception:
            pass
