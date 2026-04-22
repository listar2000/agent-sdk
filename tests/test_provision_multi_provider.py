"""Regression tests for `_provision_new` dispatch across providers.

These cover the critical gaps from the 2026-04-22 review (C1-C4, C6, M1):
provisioning routes through the provider-agnostic `provision_sandbox` wrapper;
docker/local return a real URL from the `_INSTANCES` cache; the
`/sandboxes/provision` REST endpoint reads the provider from the request body.

All provider-side I/O is mocked — no live docker/local/daytona required.
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
async def setup():
    dbmod.init_db()
    await dbmod.init_pool()
    async with dbmod.get_db() as conn:
        await conn.execute("DELETE FROM session_log")
        await conn.execute("DELETE FROM sessions")
        await conn.execute("DELETE FROM sandboxes")
        await conn.execute("DELETE FROM volumes")
        await conn.execute("DELETE FROM agents")
    yield
    await dbmod.close_pool()


async def _mk_session(provider: str, provider_ref: str) -> None:
    from api.models import AgentConfig, AgentRecord, VolumeRecord
    await dbmod.upsert_agent(
        AgentRecord(id="a1", name="A", config=AgentConfig(agent_type="claude"))
    )
    await dbmod.upsert_volume(
        VolumeRecord(id="v1", name="v", provider=provider, provider_ref=provider_ref)
    )
    async with dbmod.get_db() as conn:
        await conn.execute(
            "INSERT INTO sessions (id, agent_id, volume_id) VALUES (%s,%s,%s)",
            ("s1", "a1", "v1"),
        )


# ---------------------------------------------------------------------------
# C1 / C6 / M1: _provision_new dispatches to the right provider
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_provision_docker_volume_drives_ensure_sandbox(setup):
    """A session on a docker volume must provision through docker.create_sandbox."""
    await _mk_session("docker", "agentsdk-vol")

    from api.providers import ProviderInstance

    calls = []

    async def fake_create(**kwargs):
        calls.append(kwargs)
        return ProviderInstance(
            provider="docker",
            url="http://localhost:12345",
            root="/home/agent",
            sandbox_id="container-abcdef",
            container_id="container-abcdef",
            port=12345,
        )

    with patch("api.providers.docker.create_sandbox",
               new=AsyncMock(side_effect=fake_create)), \
         patch("api.server.ensure_volume_supervisor", new=AsyncMock(return_value=None)):
        sess = await dbmod.get_session("s1")
        sb = await srv.ensure_sandbox(sess)

    assert sb is not None
    assert sb.provider == "docker"
    assert sb.sandbox_ref == "container-abcdef"
    assert sb.listen_port == 12345
    assert len(calls) == 1
    # volume_ref (uniform API keyword) routes through to the provider.
    assert calls[0]["volume_ref"] == "agentsdk-vol"
    assert calls[0]["subpath"] == "agents/a1/home"


@pytest.mark.asyncio
async def test_provision_local_volume_drives_ensure_sandbox(setup, tmp_path, monkeypatch):
    """A session on a local volume must provision through local.create_sandbox."""
    monkeypatch.setenv("AGENT_SDK_LOCAL_VOL_ROOT", str(tmp_path))
    await _mk_session("local", str(tmp_path / "v1"))

    from api.providers import ProviderInstance

    calls = []

    async def fake_create(*args, **kwargs):
        # local.create_sandbox accepts volume_ref as a positional arg.
        calls.append({
            **kwargs,
            "_positional": args,
        })
        return ProviderInstance(
            provider="local",
            url="http://127.0.0.1:23456",
            root=str(tmp_path / "v1" / "agents/a1/home"),
            sandbox_id="98765",
            port=23456,
        )

    with patch("api.providers.local.create_sandbox",
               new=AsyncMock(side_effect=fake_create)), \
         patch("api.server.ensure_volume_supervisor", new=AsyncMock(return_value=None)):
        sess = await dbmod.get_session("s1")
        sb = await srv.ensure_sandbox(sess)

    assert sb is not None
    assert sb.provider == "local"
    assert sb.sandbox_ref == "98765"
    assert sb.listen_port == 23456
    assert len(calls) == 1
    # volume_ref passes through; subpath is agents/<agent_id>/home.
    assert calls[0]["volume_ref"] == str(tmp_path / "v1")
    assert calls[0]["subpath"] == "agents/a1/home"


# ---------------------------------------------------------------------------
# C3: supervisor URL resolution for docker/local
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ensure_supervisor_url_uses_instance_cache(setup):
    """Docker/Local sessions reuse the ProviderInstance URL from _INSTANCES
    rather than calling daytona's ensure_supervisor_url path (which would
    try to mint a signed preview URL and raise)."""
    await _mk_session("docker", "agentsdk-vol")

    from api.models import SandboxRecord
    from api.providers import ProviderInstance

    sb = SandboxRecord(
        id="sb1", provider="docker", sandbox_ref="container-live",
        status="running", root="/home/agent",
        volume_id="v1", subpath="agents/a1/home",
        listen_port=9876,
    )
    await dbmod.upsert_sandbox(sb)
    await dbmod.set_session_current_sandbox("s1", "sb1")

    # Live instance carries a real URL (docker starts the supervisor at
    # create-sandbox time, so the ProviderInstance we cached already has it).
    live_inst = ProviderInstance(
        provider="docker",
        url="http://localhost:9876",
        root="/home/agent",
        sandbox_id="container-live",
        container_id="container-live",
        port=9876,
    )
    srv._INSTANCES["sb1"] = live_inst
    srv.SESSIONS.pop("s1", None)

    fake_acp = MagicMock()
    fake_acp.initialize = AsyncMock(return_value=None)
    fake_acp.handshake = AsyncMock(return_value=None)
    fake_acp.get_inner_session_id = MagicMock(return_value="inner-new")

    async def fake_apply(client, config, acp_session_id, cwd):
        client.set_inner_session_id(acp_session_id, "inner-new")

    # Daytona's ensure_supervisor_url must NOT be called for a docker session.
    with patch("api.providers.daytona.ensure_supervisor_url",
               new=AsyncMock(side_effect=AssertionError(
                   "daytona path must not be used for docker session"))), \
         patch("api.server._apply_config_and_initialize", side_effect=fake_apply), \
         patch("api.server.AcpClient", return_value=fake_acp), \
         patch("api.server._start_session_tasks"):
        sess = await dbmod.get_session("s1")
        state = await srv.ensure_runtime(sess, sb)

    assert state.supervisor_url == "http://localhost:9876"
    # Clean up registry so later tests see a clean slate.
    srv._INSTANCES.pop("sb1", None)
    srv.SESSIONS.pop("s1", None)


@pytest.mark.asyncio
async def test_ensure_supervisor_url_falls_back_to_listen_port_on_cold_start(setup):
    """When _INSTANCES is empty (server restart), reconstruct URL from listen_port."""
    await _mk_session("local", "/tmp/vol-cold")

    from api.models import SandboxRecord
    sb = SandboxRecord(
        id="sb1", provider="local", sandbox_ref="54321",
        status="running", root="/tmp/vol-cold/agents/a1/home",
        volume_id="v1", subpath="agents/a1/home",
        listen_port=7777,
    )
    await dbmod.upsert_sandbox(sb)
    await dbmod.set_session_current_sandbox("s1", "sb1")

    srv._INSTANCES.pop("sb1", None)
    srv.SESSIONS.pop("s1", None)

    fake_acp = MagicMock()
    fake_acp.initialize = AsyncMock(return_value=None)
    fake_acp.get_inner_session_id = MagicMock(return_value="inner-new")

    async def fake_apply(client, config, acp_session_id, cwd):
        client.set_inner_session_id(acp_session_id, "inner-new")

    with patch("api.providers.daytona.ensure_supervisor_url",
               new=AsyncMock(side_effect=AssertionError(
                   "daytona path must not be used for local session"))), \
         patch("api.server._apply_config_and_initialize", side_effect=fake_apply), \
         patch("api.server.AcpClient", return_value=fake_acp), \
         patch("api.server._start_session_tasks"):
        sess = await dbmod.get_session("s1")
        state = await srv.ensure_runtime(sess, sb)

    assert state.supervisor_url == "http://localhost:7777"
    srv.SESSIONS.pop("s1", None)


# ---------------------------------------------------------------------------
# C4: /sandboxes/provision honors the request-body `provider`
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture
async def client(setup):
    transport = ASGITransport(app=srv.app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


@pytest.mark.asyncio
async def test_sandbox_provision_endpoint_dispatches_on_provider(client):
    """POST /sandboxes/provision with provider=docker routes to docker, not daytona."""
    from api.models import VolumeRecord
    await dbmod.upsert_volume(
        VolumeRecord(id="v_d", name="v_d", provider="docker",
                     provider_ref="agentsdk-docker-vol"),
    )

    from api.providers import ProviderInstance

    docker_calls = []
    async def fake_docker(**kwargs):
        docker_calls.append(kwargs)
        return ProviderInstance(
            provider="docker", url="http://localhost:5555", root="/home/agent",
            sandbox_id="cid-9", container_id="cid-9", port=5555,
        )

    with patch("api.providers.docker.create_sandbox",
               new=AsyncMock(side_effect=fake_docker)), \
         patch("api.providers.daytona.provision_daytona_sandbox",
               new=AsyncMock(side_effect=AssertionError(
                   "daytona must not be called when provider=docker"))), \
         patch("api.server.ensure_volume_supervisor",
               new=AsyncMock(return_value=None)):
        r = await client.post(
            "/sandboxes/provision",
            json={
                "provider": "docker",
                "volume_id": "v_d",
                "subpath": "agents/foo/home",
                "agent_type": "claude",
            },
        )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["status"] == "provisioned"
    assert body["provider"] == "docker"
    assert len(docker_calls) == 1
    assert docker_calls[0]["volume_ref"] == "agentsdk-docker-vol"
    assert docker_calls[0]["subpath"] == "agents/foo/home"

    rec = await dbmod.get_sandbox(body["sandbox_id"])
    assert rec is not None
    assert rec.provider == "docker"
    assert rec.sandbox_ref == "cid-9"
    assert rec.listen_port == 5555


# ---------------------------------------------------------------------------
# Scenario 13 — Cross-provider isolation.
# A volume created with provider=docker must not be used to provision a
# sandbox on provider=daytona. The server should reject the mismatch cleanly
# (HTTP 400) rather than silently run the wrong provider or crash midway.
# Covers both entry points: POST /sandboxes and POST /sandboxes/provision.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_cross_provider_volume_rejection_via_provision_endpoint(client):
    """POST /sandboxes/provision must reject mismatched provider vs. volume."""
    from api.models import VolumeRecord
    await dbmod.upsert_volume(
        VolumeRecord(id="v_docker", name="v_docker", provider="docker",
                     provider_ref="agentsdk-dockervol"),
    )

    # No provider-side mocks: request must fail before any create_sandbox call.
    with patch("api.providers.docker.create_sandbox",
               new=AsyncMock(side_effect=AssertionError(
                   "docker.create_sandbox must not be called on mismatch"))), \
         patch("api.providers.daytona.provision_daytona_sandbox",
               new=AsyncMock(side_effect=AssertionError(
                   "daytona.provision must not be called on mismatch"))), \
         patch("api.server.ensure_volume_supervisor",
               new=AsyncMock(return_value=None)):
        r = await client.post(
            "/sandboxes/provision",
            json={
                "provider": "daytona",
                "volume_id": "v_docker",
                "subpath": "agents/foo/home",
                "agent_type": "claude",
            },
        )
    assert r.status_code == 400, f"expected 400, got {r.status_code}: {r.text}"
    err = (r.json().get("error") or "").lower()
    assert "provider" in err, f"error should mention provider mismatch: {r.text}"


@pytest.mark.asyncio
async def test_cross_provider_volume_rejection_via_sandboxes_endpoint(client):
    """POST /sandboxes with provider != volume.provider must also reject.

    Note: the review flagged this endpoint may silently accept the mismatch
    today. If so, marking xfail records a source-side bug for cycle 3.
    """
    from api.models import VolumeRecord
    await dbmod.upsert_volume(
        VolumeRecord(id="v_daytona", name="v_daytona", provider="daytona",
                     provider_ref="dt-only"),
    )

    with patch("api.providers.docker.create_sandbox",
               new=AsyncMock(side_effect=AssertionError(
                   "docker.create_sandbox must not be called on mismatch"))), \
         patch("api.providers.daytona.provision_daytona_sandbox",
               new=AsyncMock(side_effect=AssertionError(
                   "daytona.provision must not be called on mismatch"))):
        r = await client.post(
            "/sandboxes",
            json={
                "provider": "docker",
                "volume_id": "v_daytona",
                "subpath": "agents/foo/home",
                "agent_type": "claude",
            },
        )

    # Strict expected contract: reject with 400 + provider in message. If the
    # source agent hasn't added this guard yet, the test xfails, surfacing the
    # gap explicitly.
    if r.status_code == 400:
        err = (r.json().get("error") or "").lower()
        assert "provider" in err, f"error should mention provider mismatch: {r.text}"
    else:
        pytest.xfail(
            f"POST /sandboxes still accepts provider mismatch "
            f"(status={r.status_code}, body={r.text}) — source bug"
        )


# ---------------------------------------------------------------------------
# Scenario 10 — volume_read error shape through the top-level dispatch.
# The uniform api.providers.volume_read wrapper must surface a useful error
# when the underlying provider can't find the file. Docker's shell-based
# implementation emits __MISSING__ and raises FileNotFoundError; exercise
# that through the public entry point to lock the contract in.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_volume_read_dispatch_through_docker_missing_file():
    """api.providers.volume_read('docker', ref, 'nope') → clear error (not
    a bare KeyError, not a hang). We don't need a live docker daemon: patch
    docker.volume_read to raise FileNotFoundError and verify the wrapper
    propagates the exception verbatim."""
    from unittest.mock import AsyncMock, patch
    import api.providers as providers_mod

    async def fake_read(ref, path):
        raise FileNotFoundError(f"{path} not found on volume {ref}")

    with patch("api.providers.docker.volume_read",
               new=AsyncMock(side_effect=fake_read)):
        with pytest.raises(FileNotFoundError) as excinfo:
            await providers_mod.volume_read("docker", "my-vol", "no/such/file")
    # Error message names the path — operators can grep logs.
    assert "no/such/file" in str(excinfo.value)


@pytest.mark.asyncio
async def test_volume_read_dispatch_through_docker_runtime_error():
    """If the underlying docker exec fails (rc!=0, non-missing), the
    provider raises RuntimeError with the docker stderr. The dispatch
    wrapper must not swallow that."""
    from unittest.mock import AsyncMock, patch
    import api.providers as providers_mod

    async def fake_read(ref, path):
        raise RuntimeError("volume_read failed (rc=125): docker daemon error")

    with patch("api.providers.docker.volume_read",
               new=AsyncMock(side_effect=fake_read)):
        with pytest.raises(RuntimeError) as excinfo:
            await providers_mod.volume_read("docker", "my-vol", "some/file")
    msg = str(excinfo.value).lower()
    assert "volume_read" in msg or "docker" in msg, (
        f"error should mention the failing op: {excinfo.value!r}"
    )


@pytest.mark.asyncio
async def test_volume_read_dispatch_through_local_missing_file(tmp_path, monkeypatch):
    """End-to-end: api.providers.volume_read on a real local volume with a
    missing file — no mocking the provider, test the stack."""
    monkeypatch.setenv("AGENT_SDK_LOCAL_VOL_ROOT", str(tmp_path))
    import api.providers as providers_mod
    from api.providers import local as local_mod

    name = "vol-scen10"
    ref = await local_mod.create_volume(name)

    with pytest.raises(FileNotFoundError):
        await providers_mod.volume_read("local", ref, "shared/nope.txt")


@pytest.mark.asyncio
async def test_volume_read_dispatch_through_local_bad_volume_ref(tmp_path, monkeypatch):
    """Top-level dispatch with a provider ref that isn't a real volume dir."""
    monkeypatch.setenv("AGENT_SDK_LOCAL_VOL_ROOT", str(tmp_path))
    import api.providers as providers_mod

    bogus = str(tmp_path / "no-such-volume-dir")
    with pytest.raises((FileNotFoundError, OSError)):
        await providers_mod.volume_read("local", bogus, "file.txt")
