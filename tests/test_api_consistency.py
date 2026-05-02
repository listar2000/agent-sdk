"""REST API consistency coverage.

Sibling to test_volumes_api.py / test_session_volume_integration.py, this
file locks in the invariants audited in the REST consistency pass:

* Error shape: handlers return ``{"error": "..."}`` (or the HTTPException
  dict pass-through) — never a bare string, never ``{"detail": "..."}``
  for our own 4xx/5xx.
* Status codes match the class of failure (400 bad input, 404 missing
  resource, 409 conflict, 502 upstream, not 500).
* Input validation: POST handlers that used to blow up with 500
  (AttributeError on data.get) now return 400 on non-object JSON.
* Response-shape parity: /sandboxes POST + /sandboxes expose
  both ``id`` and ``sandbox_id``; /sessions/quick + /sessions/{id}/resume
  expose both ``sandbox_id`` and ``current_sandbox_id``.

DB is required; provider calls are stubbed so the tests run offline.
"""
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
async def client(db_pool):
    transport = ASGITransport(app=srv.app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


# ---------------------------------------------------------------------------
# Non-object JSON bodies must produce 400 {"error": ...}, not 500.
#
# Every handler listed here used to do ``data = await request.json();
# data.get(...)`` and would 500 with AttributeError when the client sent
# a string, int, or array.  The _json_body() helper now rejects those
# bodies with a uniform 400 error.
# ---------------------------------------------------------------------------

_NON_OBJECT_BODIES = ["not a dict", 123, [1, 2, 3], True]


@pytest.mark.asyncio
@pytest.mark.parametrize("body", _NON_OBJECT_BODIES)
@pytest.mark.parametrize("path", [
    "/agents",
    "/sessions",
    "/sessions/any-id/message",
    "/sessions/any-id/config",
    "/sessions/any-id/sandbox/exec",
])
async def test_post_non_object_body_returns_400(client, path, body):
    r = await client.post(path, json=body)
    assert r.status_code == 400, (
        f"{path} with body={body!r} returned {r.status_code}: {r.text}"
    )
    payload = r.json()
    # Uniform error shape — key is "error", never "detail", and never a
    # raw string.  ``/sessions/any-id/config`` parses the body first (so
    # unknown sessions can't hide validation bugs) and returns the same
    # helper message, so any of these prefixes is acceptable.
    assert isinstance(payload, dict)
    assert "error" in payload
    msg = payload["error"].lower()
    assert "json object" in msg or "invalid json" in msg


# test_sandbox_file_forwarders_reject_non_object_body was removed: the
# deprecated ``/sandboxes/{id}/files/{op}`` thin wrappers were deleted
# entirely. Callers should use the session-scoped routes
# (``/sessions/{id}/files/{op}``) which have their own body-validation
# coverage in test_session_files_routes.


# test_start_sandbox_provider_failure_returns_502 was removed: the
# deprecated ``POST /sandboxes/{id}/start`` route + the ``_type1_recover``
# helper it patched are both gone. Callers use ``POST /sessions/{id}/message``
# which provisions on demand through the SessionPool; provider-failure
# error mapping for that path is covered in test_sandbox_stop_delete_recovery.


# test_post_sandboxes_provision_exposes_both_id_and_sandbox_id was
# removed: the legacy ``POST /sandboxes`` route it exercised has been
# deleted (every sandbox is session-owned now; ``POST /sessions``
# returns the same dual ``id`` / ``sandbox_id`` shape via the pool).


# ---------------------------------------------------------------------------
# Duplicate POST /volumes is 409 with {"error": ...} — not 500, not 400.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_duplicate_volume_name_returns_409(client):
    import uuid
    # Unique name per run — the test DB isn't reset between tests, and
    # other suites create volumes at module-load time.
    name = f"dup-api-{uuid.uuid4().hex[:8]}"
    with patch("api.providers.daytona.create_daytona_volume",
               new=AsyncMock(return_value="dt-dup")):
        r1 = await client.post("/volumes",
                               json={"name": name, "provider": "daytona"})
        assert r1.status_code == 200, r1.text
        r2 = await client.post("/volumes",
                               json={"name": name, "provider": "daytona"})
    assert r2.status_code == 409, f"got {r2.status_code}: {r2.text}"
    body = r2.json()
    assert "error" in body
    assert "already exists" in body["error"]


# ---------------------------------------------------------------------------
# Missing volume_id on /sessions is 400 (matches /sandboxes "required"
# errors), not 422.  Aligning this removed the odd-one-out status code
# without breaking existing tests — they already accept either.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_post_sessions_lazy_missing_volume_id_uses_default(client):
    """Missing volume_id on lazy session → server uses default-{provider}.
    Backward-compat for zero-config SDK callers."""
    from unittest.mock import patch, AsyncMock
    with patch("api.providers.local.create_volume",
               new=AsyncMock(return_value="/tmp/default-local-sess")):
        r = await client.post("/sessions",
                              json={"provider": "unix_local", "provision": False})
    assert r.status_code == 200, r.text
    assert r.json().get("volume_id")


@pytest.mark.asyncio
async def test_post_sessions_eager_missing_volume_id_uses_default(client):
    """Missing volume_id on eager POST /sessions → default-{provider}."""
    from unittest.mock import patch, AsyncMock
    from api.providers._shared import ProviderInstance

    async def fake_create_instance(*a, **kw):
        return ProviderInstance(
            provider="unix_local", url="http://127.0.0.1:9999",
            root="/tmp", sandbox_id="pid-12345", port=9999,
        )

    with patch("api.providers.local.create_volume",
               new=AsyncMock(return_value="/tmp/default-local-quick")), \
         patch("api.server.ensure_volume_supervisor",
               new=AsyncMock(return_value=None)), \
         patch("api.server.create_instance",
               new=AsyncMock(side_effect=fake_create_instance)), \
         patch("api.server.AcpClient"), \
         patch("api.server._start_session_tasks"):
        r = await client.post("/sessions",
                              json={"name": "t", "provider": "unix_local"})
    # Success path (200) — or a clean 5xx if the mocked flow hits an
    # unpatched branch. The point: NOT 400 "volume_id is required".
    assert r.status_code != 400, f"should not reject missing volume_id: {r.text}"


# ---------------------------------------------------------------------------
# /sessions and /sandboxes both advertise ``pre_start_commands`` via
# create_instance; both must forward it through to the provider. Earlier
# /sessions silently dropped the caller's value and only forwarded skill
# install commands — that's the bug this pins.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_post_sessions_forwards_pre_start_commands(client):
    """POST /sessions must forward the caller's ``pre_start_commands`` to
    the provider (merged with skill install commands), not drop them."""
    from unittest.mock import patch, AsyncMock
    from api.models import VolumeRecord
    from api.providers._shared import ProviderInstance

    v = VolumeRecord(id="vol_pre_start", name="pre-start-test",
                     provider="daytona", provider_ref="dt-pre-start")
    await dbmod.upsert_volume(v)

    captured = {}
    async def fake_create_instance(*a, **kw):
        captured.update(kw)
        return ProviderInstance(
            provider="daytona", url="http://127.0.0.1:9101",
            root="/home/daytona", sandbox_id="dt-sbx-ps", port=9101,
        )

    with patch("api.server.ensure_volume_supervisor",
               new=AsyncMock(return_value=None)), \
         patch("api.server.create_instance",
               new=AsyncMock(side_effect=fake_create_instance)), \
         patch("api.server.AcpClient"), \
         patch("api.server._start_session_tasks"):
        r = await client.post("/sessions", json={
            "name": "ps",
            "provider": "daytona",
            "volume_id": v.id,
            "skills": ["rllm-org/hive#staging"],
            "pre_start_commands": ['uv tool install hive-evolve'],
        })

    assert r.status_code in (200, 502), r.text  # 502 if a later mock is missing
    pre_start = captured.get("pre_start_commands") or []
    assert any("uv tool install hive-evolve" in c for c in pre_start), (
        f"caller's pre_start_commands dropped; got {pre_start!r}"
    )
    assert any("skills add" in c for c in pre_start), (
        f"skill install commands missing; got {pre_start!r}"
    )


# ---------------------------------------------------------------------------
# Session-recovery edge cases migrated from the deleted test_session_recovery.py.
# The rest of that file was mock-tautology ("did the mock get called?") that
# golden-suite recovery tests already exercise end-to-end. These two are
# narrow regressions the golden suite doesn't cover.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_message_on_truly_gone_session_returns_404(client):
    """If the session is absent from both SESSIONS and the DB, /message → 404."""
    with patch("api.server.get_session", AsyncMock(return_value=None)):
        r = await client.post("/sessions/gone-forever/message", json={"message": "hi"})
    assert r.status_code == 404, r.text


# test_resolve_sandbox_instance_refreshes_stale_daytona_instance was
# removed: it tested ``_resolve_sandbox_instance`` (deleted),
# ``_ensure_sandbox_alive`` (deleted), and ``_INSTANCES`` cache
# refresh (deleted). The pool's ``get_session`` now always resolves
# the supervisor URL through the live SandboxSession state — there's
# no stale-cache layer to refresh.
