"""REST tests for /volumes endpoints. DB required; provider calls stubbed."""
from __future__ import annotations
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
async def client():
    dbmod.init_db()
    await dbmod.init_pool()
    transport = ASGITransport(app=srv.app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        yield c
    await dbmod.close_pool()


@pytest.mark.asyncio
async def test_post_volume_creates_row(client):
    with patch("api.providers.daytona.create_daytona_volume",
               new=AsyncMock(return_value="dt-fake-ref")):
        r = await client.post("/volumes", json={"name": "proj-api-test", "provider": "daytona"})
    assert r.status_code == 200
    body = r.json()
    assert body["name"] == "proj-api-test"
    assert body["provider_ref"] == "dt-fake-ref"
    assert body["status"] == "ready"


@pytest.mark.asyncio
async def test_get_and_list_volumes(client):
    with patch("api.providers.daytona.create_daytona_volume",
               new=AsyncMock(return_value="dt-r1")):
        await client.post("/volumes", json={"name": "p1", "provider": "daytona"})

    r = await client.get("/volumes")
    assert r.status_code == 200
    assert any(v["name"] == "p1" for v in r.json())

    r = await client.get("/volumes/p1")
    assert r.status_code == 200
    assert r.json()["name"] == "p1"


@pytest.mark.asyncio
async def test_delete_volume(client):
    with patch("api.providers.daytona.create_daytona_volume",
               new=AsyncMock(return_value="dt-del")), \
         patch("api.providers.delete_daytona_volume",
               new=AsyncMock(return_value=None)):
        await client.post("/volumes", json={"name": "to-delete", "provider": "daytona"})
        r = await client.delete("/volumes/to-delete")
    assert r.status_code == 204
    r = await client.get("/volumes/to-delete")
    assert r.status_code == 404


@pytest.mark.asyncio
async def test_delete_volume_calls_provider_for_docker(client):
    """Mi8: DELETE /volumes/{id} must dispatch to the provider's
    ``delete_volume`` for docker/local too — previously only daytona got
    provider-side cleanup and docker/local volumes leaked.
    """
    # Patch both create and delete on the docker module so no real daemon is
    # touched; assert delete_volume is called with the provider ref we minted.
    with patch("api.providers.docker.create_volume",
               new=AsyncMock(return_value="docker-vol-mi8")), \
         patch("api.providers.docker.delete_volume",
               new=AsyncMock(return_value=None)) as del_mock:
        r = await client.post("/volumes",
                              json={"name": "mi8-docker", "provider": "docker"})
        assert r.status_code == 200, r.text

        r = await client.delete("/volumes/mi8-docker")
        assert r.status_code == 204
    del_mock.assert_awaited_once_with("docker-vol-mi8")


@pytest.mark.asyncio
async def test_delete_volume_provider_not_found_swallowed(client):
    """Mi8 + cycle-8 Ma2: if the provider says the volume is already
    gone ("not found"), we swallow and still remove the DB row — the
    desired end state (volume absent) is already achieved.
    """
    with patch("api.providers.docker.create_volume",
               new=AsyncMock(return_value="docker-vol-gone")), \
         patch("api.providers.docker.delete_volume",
               new=AsyncMock(side_effect=RuntimeError("Error: no such volume"))):
        r = await client.post("/volumes",
                              json={"name": "mi8-gone", "provider": "docker"})
        assert r.status_code == 200, r.text

        r = await client.delete("/volumes/mi8-gone")
        assert r.status_code == 204
    r = await client.get("/volumes/mi8-gone")
    assert r.status_code == 404


@pytest.mark.asyncio
async def test_delete_volume_provider_error_surfaces_as_409(client):
    """Cycle-8 Ma2: transient / non-not-found provider errors (daemon
    unreachable, "volume in use", etc.) must NOT be silently swallowed.
    The DB row stays; client retries.  An out-of-band container mounting
    a docker volume is a real conflict the user should see.
    """
    with patch("api.providers.docker.create_volume",
               new=AsyncMock(return_value="docker-vol-err")), \
         patch("api.providers.docker.delete_volume",
               new=AsyncMock(side_effect=RuntimeError("docker daemon unreachable"))):
        r = await client.post("/volumes",
                              json={"name": "mi8-surface", "provider": "docker"})
        assert r.status_code == 200, r.text

        r = await client.delete("/volumes/mi8-surface")
        assert r.status_code == 409
    # DB row preserved so a retry can succeed later.
    r = await client.get("/volumes/mi8-surface")
    assert r.status_code == 200


@pytest.mark.asyncio
async def test_delete_volume_conflict_if_session_exists(client):
    """DELETE returns 409 if a session references the volume; force=true cascades."""
    from api.models import AgentConfig, AgentRecord
    await dbmod.upsert_agent(AgentRecord(id="a1", name="A1", config=AgentConfig()))
    with patch("api.providers.daytona.create_daytona_volume",
               new=AsyncMock(return_value="dt-c")):
        await client.post("/volumes", json={"name": "conflict", "provider": "daytona"})
    v = await dbmod.get_volume_by_name("conflict")
    # Insert a session referencing this volume directly.
    async with dbmod.get_db() as conn:
        await conn.execute(
            "INSERT INTO sessions (id, agent_id, volume_id) VALUES (%s, %s, %s)",
            ("sess_1", "a1", v.id),
        )
    r = await client.delete("/volumes/conflict")
    assert r.status_code == 409

    # With force=true the referenced session is deleted and the volume too.
    with patch("api.providers.delete_daytona_volume",
               new=AsyncMock(return_value=None)):
        r = await client.delete("/volumes/conflict?force=true")
    assert r.status_code == 204
    # Volume gone.
    r = await client.get("/volumes/conflict")
    assert r.status_code == 404
    # Session gone.
    async with dbmod.get_db() as conn:
        row = await (await conn.execute(
            "SELECT id FROM sessions WHERE id = %s", ("sess_1",)
        )).fetchone()
    assert row is None


@pytest.mark.asyncio
async def test_provision_volume_waits_for_ready(client):
    with patch("api.providers.daytona.create_daytona_volume",
               new=AsyncMock(return_value="dt-prov")):
        r = await client.post("/volumes/provision",
                              json={"name": "prov-test", "provider": "daytona"})
    assert r.status_code == 200
    assert r.json()["status"] == "ready"


@pytest.mark.asyncio
@pytest.mark.parametrize("bad_path", [
    "shared/foo\nbar",   # LF
    "shared/foo\rbar",   # CR
    "shared/foo\x00bar", # NUL
])
async def test_edit_rejects_control_chars(client, bad_path):
    """MT5: ``POST /volumes/{id}/files/edit`` rejects paths containing
    control characters (CR/LF/NUL) with a 400 whose body names the
    violation.  The underlying sanitizer is
    :func:`api.providers._shared._safe_path`; this test asserts the
    behavior is wired through at the HTTP boundary, not just at the
    provider layer."""
    from api.models import VolumeRecord
    v = VolumeRecord(id="vol_ctrl", name="files-ctrl",
                     provider="daytona", provider_ref="dt-ctrl")
    await dbmod.upsert_volume(v)

    # Provider mocks — never reached on a 400, so any call is a bug.
    async def blow_up(*a, **kw):
        raise AssertionError(f"provider must not be called on invalid path {bad_path!r}")

    with patch("api.providers.daytona.volume_write", new=AsyncMock(side_effect=blow_up)):
        r = await client.post(f"/volumes/{v.id}/files/edit",
                              json={"path": bad_path, "content": "data"})
    assert r.status_code == 400, f"got {r.status_code}: {r.text}"
    # Error body names the failure mode — operators grepping logs want to
    # know "control chars" not "500 internal error".
    detail = ""
    try:
        detail = (r.json().get("detail") or r.json().get("error") or "").lower()
    except Exception:
        detail = r.text.lower()
    assert "control" in detail or "invalid" in detail, (
        f"error should name the violation: {r.text}"
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("bad_path", [
    "foo\nbar",
    "foo\rbar",
    "foo\x00bar",
])
async def test_read_rejects_control_chars(client, bad_path):
    """MT5: ``GET /volumes/{id}/files/read`` also rejects control-char
    paths at the HTTP layer."""
    from api.models import VolumeRecord
    v = VolumeRecord(id="vol_ctr2", name="files-ctr2",
                     provider="daytona", provider_ref="dt-ctr2")
    await dbmod.upsert_volume(v)

    async def blow_up(*a, **kw):
        raise AssertionError(f"provider must not be called on invalid path {bad_path!r}")

    with patch("api.providers.daytona.volume_read", new=AsyncMock(side_effect=blow_up)):
        r = await client.get(f"/volumes/{v.id}/files/read",
                             params={"path": bad_path})
    assert r.status_code == 400, f"got {r.status_code}: {r.text}"
    detail = ""
    try:
        detail = (r.json().get("detail") or r.json().get("error") or "").lower()
    except Exception:
        detail = r.text.lower()
    assert "control" in detail or "invalid" in detail, (
        f"error should name the violation: {r.text}"
    )


@pytest.mark.asyncio
async def test_volume_files_edit_and_read(client):
    """File ops dispatch through the volume's provider module."""
    from api.models import VolumeRecord
    v = VolumeRecord(id="vol_f", name="files-test", provider="daytona", provider_ref="dt-f")
    await dbmod.upsert_volume(v)

    # Patch daytona's volume_write directly — the endpoint dispatches via
    # _PROVIDER_MODS[vol.provider].volume_write.
    write_calls = []
    async def fake_write(ref, path, content):
        write_calls.append((ref, path, content))

    async def fake_read(ref, path):
        return b"hi"

    with patch("api.providers.daytona.volume_write",
               new=AsyncMock(side_effect=fake_write)), \
         patch("api.providers.daytona.volume_read",
               new=AsyncMock(side_effect=fake_read)):
        r = await client.post(f"/volumes/{v.id}/files/edit",
                              json={"path": "shared/x.txt", "content": "hi"})
        assert r.status_code == 204
        assert len(write_calls) == 1
        assert write_calls[0][0] == "dt-f"
        assert write_calls[0][1] == "shared/x.txt"
        r = await client.get(f"/volumes/{v.id}/files/read",
                             params={"path": "shared/x.txt"})
        assert r.status_code == 200
        assert r.json()["content"] == "hi"
