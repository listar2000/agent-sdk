"""Unit tests pinning that ``_ensure_sandbox_locked`` re-provisioning paths
preserve the original sandbox's ``dockerfile`` + ``shared_mounts``.

Background — the sandbox row is the only durable home for ``shared_mounts``
and ``dockerfile`` (see db.py:198, ``_AGENT_REJECTED_KEYS`` in server.py).
Three paths in ``_ensure_sandbox_locked`` re-provision after losing the
existing sandbox:

  * Case "missing"  — provider says the sandbox no longer exists
  * Case "error"    — provider says the sandbox is broken (we destroy it)
  * Case B          — DB row was deleted out-of-band

The "missing" / "error" paths still HAVE the prior ``SandboxRecord`` in
scope before they call ``delete_sandbox``, so they CAN forward
``shared_mounts``/``dockerfile`` into ``_provision_new``. Failing to do so
is the "/mnt/<x> is empty after recovery" symptom seen in production.
"""
from __future__ import annotations

import os
import sys

import pytest
import pytest_asyncio
from unittest.mock import AsyncMock, patch

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


async def _mk_fixtures(shared_mounts: list[str] | None,
                       dockerfile: str | None = None,
                       sandbox_id: str = "sb_orig"):
    """Create agent + volume + session + a sandbox row whose
    ``shared_mounts`` and ``dockerfile`` are the values under test."""
    from api.models import AgentConfig, AgentRecord, VolumeRecord, SandboxRecord
    await dbmod.upsert_agent(AgentRecord(
        id="a1", name="A", config=AgentConfig(agent_type="claude")))
    await dbmod.upsert_volume(VolumeRecord(
        id="v1", name="v", provider="daytona", provider_ref="dt-v"))
    async with dbmod.get_db() as conn:
        await conn.execute(
            "INSERT INTO sessions (id, agent_id, volume_id) VALUES (%s,%s,%s)",
            ("s1", "a1", "v1"),
        )
    sb = SandboxRecord(
        id=sandbox_id, provider="daytona", sandbox_ref="dt-orig",
        status="running", root="/home/daytona",
        volume_id="v1", subpath="agents/a1",
        dockerfile=dockerfile,
        shared_mounts=list(shared_mounts) if shared_mounts else [],
    )
    await dbmod.upsert_sandbox(sb)
    await dbmod.set_session_current_sandbox("s1", sandbox_id)


def _capture_provision_kwargs(captured: list[dict]):
    """Build a ``provision_sandbox_core`` patch that records the kwargs the
    caller passed in (so the test can assert on them) and returns a fake
    provisioned sandbox so the rest of ``_provision_new`` doesn't blow up."""
    from api.providers import ProviderInstance
    from api.models import SandboxRecord

    async def fake_core(**kw):
        captured.append(kw)
        from api.server import _ProvisionedSandbox
        return _ProvisionedSandbox(
            instance=ProviderInstance(
                provider="daytona", url="http://new",
                root="/home/daytona", sandbox_id="dt-new"),
            record=SandboxRecord(
                id="sb_new", provider="daytona", sandbox_ref="dt-new",
                status="running", root="/home/daytona",
                volume_id="v1", subpath="agents/a1",
                dockerfile=kw.get("dockerfile"),
                shared_mounts=list(kw.get("shared_mounts") or []),
            ),
        )
    return fake_core


@pytest.mark.asyncio
async def test_missing_status_path_preserves_shared_mounts(setup):
    """Provider returns ``missing`` → re-provisioning MUST forward the
    original sandbox's ``shared_mounts`` to ``_provision_new`` so the new
    sandbox has the same ``/mnt/<name>`` layout. Otherwise the agent
    container that the user is talking to suddenly loses its shared dirs.
    """
    await _mk_fixtures(shared_mounts=["projects", "datasets"])

    captured: list[dict] = []
    with patch("api.providers.daytona.get_daytona_sandbox_status",
               new=AsyncMock(return_value="missing")), \
         patch("api.server._provision_sandbox_core",
               new=AsyncMock(side_effect=_capture_provision_kwargs(captured))), \
         patch("api.server.ensure_volume_supervisor",
               new=AsyncMock(return_value=None)):
        sess = await dbmod.get_session("s1")
        await srv.ensure_sandbox(sess)

    assert captured, "_provision_sandbox_core was never called"
    assert captured[0].get("shared_mounts") == ["projects", "datasets"], (
        f"shared_mounts lost on 'missing' re-provision: "
        f"{captured[0].get('shared_mounts')!r} (expected ['projects', 'datasets'])"
    )


@pytest.mark.asyncio
async def test_error_status_path_preserves_shared_mounts(setup):
    """Provider returns ``error`` → server destroys the sandbox + deletes
    the row + re-provisions. The re-provision MUST forward shared_mounts
    from the (about-to-be-deleted) row.
    """
    await _mk_fixtures(shared_mounts=["projects"])

    captured: list[dict] = []
    with patch("api.providers.daytona.get_daytona_sandbox_status",
               new=AsyncMock(return_value="error")), \
         patch("api.providers.daytona.destroy_daytona",
               new=AsyncMock(return_value=None)), \
         patch("api.server._provision_sandbox_core",
               new=AsyncMock(side_effect=_capture_provision_kwargs(captured))), \
         patch("api.server.ensure_volume_supervisor",
               new=AsyncMock(return_value=None)):
        sess = await dbmod.get_session("s1")
        await srv.ensure_sandbox(sess)

    assert captured, "_provision_sandbox_core was never called"
    assert captured[0].get("shared_mounts") == ["projects"], (
        f"shared_mounts lost on 'error' re-provision: "
        f"{captured[0].get('shared_mounts')!r} (expected ['projects'])"
    )


@pytest.mark.asyncio
async def test_missing_status_path_preserves_dockerfile(setup):
    """Same invariant for ``dockerfile`` — a sandbox built from a custom
    Dockerfile must re-provision with the same image, not the agent's
    snapshot default."""
    await _mk_fixtures(shared_mounts=None, dockerfile="/path/to/Dockerfile")

    captured: list[dict] = []
    with patch("api.providers.daytona.get_daytona_sandbox_status",
               new=AsyncMock(return_value="missing")), \
         patch("api.server._provision_sandbox_core",
               new=AsyncMock(side_effect=_capture_provision_kwargs(captured))), \
         patch("api.server.ensure_volume_supervisor",
               new=AsyncMock(return_value=None)):
        sess = await dbmod.get_session("s1")
        await srv.ensure_sandbox(sess)

    assert captured, "_provision_sandbox_core was never called"
    assert captured[0].get("dockerfile") == "/path/to/Dockerfile", (
        f"dockerfile lost on 'missing' re-provision: "
        f"{captured[0].get('dockerfile')!r}"
    )
