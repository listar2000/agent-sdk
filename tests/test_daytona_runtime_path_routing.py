"""Phase C coverage: daytona provider's image-runtime feature flag.

Mirrors ``tests/test_docker_runtime_path_routing.py`` and
``tests/test_local_volume_integration.py``'s flag-on/flag-off tests, but
for the daytona provider. Doesn't require a daytona account or any
network — the daytona SDK is fully monkeypatched.

What's pinned here:
- Flag on + ``DAYTONA_IMAGE`` set → ``provision_daytona_sandbox`` calls
  ``CreateSandboxFromImageParams(image=<that image>)``.
- Flag on + nothing set → raises with a clear remediation message
  (``DAYTONA_IMAGE or .runtime-image-tag`` required).
- Flag on → ``_build_volume_mounts`` omits the legacy
  ``system/supervisor`` mount; volume becomes data-only.
- Flag on → ``start_supervisor_in_sandbox`` resolves
  ``sup_dir=/opt/agent-sdk/runtime`` directly and skips the
  cache-extract / legacy-fallback path.
- Flag off (default) → preserves the legacy hive-large default and the
  full cache-extract pipeline. Phase E deletes that branch.

When Phase E lands, the ``flag_off`` tests collapse to a single
happy-path check.
"""
from __future__ import annotations

import asyncio
import os
import sys
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

_SRC = os.path.join(os.path.dirname(__file__), "..", "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

# api.providers.daytona imports daytona_sdk lazily inside its functions, so
# importing the module itself is cheap. We patch the SDK at call time.
from api.providers import _shared as shared  # noqa: E402
from api.providers import daytona as dprov  # noqa: E402


# ---------------------------------------------------------------------------
# _build_volume_mounts: flag toggles whether system/supervisor is mounted
# ---------------------------------------------------------------------------


@pytest.fixture
def _stub_volume_mount(monkeypatch):
    """Replace daytona_sdk.VolumeMount with a plain SimpleNamespace so
    _build_volume_mounts doesn't actually need the SDK."""
    fake_module = MagicMock()
    fake_module.VolumeMount = lambda **kw: SimpleNamespace(**kw)
    monkeypatch.setitem(sys.modules, "daytona_sdk", fake_module)
    yield


def test_build_volume_mounts_omits_supervisor_mount(monkeypatch, _stub_volume_mount):
    """Phase E: ``_build_volume_mounts`` is unconditional — the legacy
    ``/opt/supervisor → system/supervisor`` mount no longer exists. Volume
    is always data-only."""
    mounts = shared._build_volume_mounts(
        volume_id="vol-1", subpath="agents/a1", shared_mounts=None,
    )
    targets = [m.mount_path for m in mounts]
    assert "/vol" in targets, f"missing /vol mount: {targets}"
    assert "/opt/supervisor" not in targets, (
        f"legacy /opt/supervisor mount must not be returned: {targets}"
    )


# ``test_build_volume_mounts_flag_off_includes_supervisor_mount`` was deleted
# in the runtime-image-unification refactor — there is no flag-off
# path anymore.


# ---------------------------------------------------------------------------
# provision_daytona_sandbox: flag steers snapshot-vs-image fork
# ---------------------------------------------------------------------------


def _patch_daytona_sdk(monkeypatch, *, calls: list):
    """Stub the AsyncDaytona singleton so we can record what kind of
    CreateSandboxParams ``provision_daytona_sandbox`` constructs.

    Post Phase 1 of the AsyncDaytona migration: ``provision_daytona_sandbox``
    awaits the singleton from ``_get_async_daytona_client``; the only sync
    daytona_sdk imports it does are the ``Create*Params`` factories
    (which we still patch via sys.modules)."""
    create_snapshot_params = MagicMock(side_effect=lambda **kw: ("Snapshot", kw))
    create_image_params = MagicMock(side_effect=lambda **kw: ("Image", kw))

    fake_daytona_instance = MagicMock()

    async def _create(params, **kw):
        calls.append(params)
        return SimpleNamespace(id="sb-fake-id")

    async def _delete(_sb):
        return None

    fake_daytona_instance.create = _create
    fake_daytona_instance.delete = _delete

    fake_module = MagicMock(
        CreateSandboxFromImageParams=create_image_params,
        CreateSandboxFromSnapshotParams=create_snapshot_params,
        VolumeMount=lambda **kw: SimpleNamespace(**kw),
    )
    monkeypatch.setitem(sys.modules, "daytona_sdk", fake_module)
    monkeypatch.setattr(
        dprov,
        "_get_async_daytona_client",
        AsyncMock(return_value=fake_daytona_instance),
    )


@pytest.mark.asyncio
async def test_provision_flag_on_with_daytona_image_uses_image_path(monkeypatch):
    """Flag on + DAYTONA_IMAGE set → CreateSandboxFromImageParams with
    that image, NOT a snapshot. The legacy hive-large default does not
    apply when the flag is on."""
    monkeypatch.setenv("AGENT_SDK_USE_IMAGE_RUNTIME", "1")
    monkeypatch.setenv("DAYTONA_IMAGE", "ghcr.io/agent-sdk:abc1234")
    monkeypatch.setenv("DAYTONA_API_KEY", "fake")
    monkeypatch.delenv("DAYTONA_SNAPSHOT", raising=False)

    # The repo ships ``.runtime-snapshot-tag`` so a fresh checkout
    # provisions from the prebuilt snapshot by default. Force it off
    # here to exercise the image path explicitly.
    monkeypatch.setattr(dprov, "_read_runtime_snapshot_tag", lambda: None)

    calls: list = []
    _patch_daytona_sdk(monkeypatch, calls=calls)

    inst = await dprov.provision_daytona_sandbox(
        agent_type="claude", volume_id="v1", subpath="agents/a1",
    )

    assert len(calls) == 1, f"expected 1 daytona.create call, got {len(calls)}"
    kind, kw = calls[0]
    assert kind == "Image", (
        f"flag-on without DAYTONA_SNAPSHOT must use the image path, got {kind!r}"
    )
    assert kw["image"] == "ghcr.io/agent-sdk:abc1234", (
        f"image arg should be DAYTONA_IMAGE: {kw}"
    )
    assert inst.sandbox_ref == "sb-fake-id"


@pytest.mark.asyncio
async def test_provision_flag_on_without_image_or_snapshot_raises(monkeypatch):
    """Flag on + no DAYTONA_IMAGE + no .runtime-image-tag + no
    DAYTONA_SNAPSHOT → clear error citing the remediation."""
    monkeypatch.setenv("AGENT_SDK_USE_IMAGE_RUNTIME", "1")
    monkeypatch.delenv("DAYTONA_IMAGE", raising=False)
    monkeypatch.delenv("DAYTONA_SNAPSHOT", raising=False)
    monkeypatch.setenv("DAYTONA_API_KEY", "fake")

    # Force _read_runtime_image_tag AND _read_runtime_snapshot_tag to
    # return None (simulates a fresh checkout before scripts/release.sh
    # has run — both files would be absent).
    monkeypatch.setattr(dprov, "_read_runtime_image_tag", lambda: None)
    monkeypatch.setattr(dprov, "_read_runtime_snapshot_tag", lambda: None)

    calls: list = []
    _patch_daytona_sdk(monkeypatch, calls=calls)

    with pytest.raises(RuntimeError) as excinfo:
        await dprov.provision_daytona_sandbox(
            agent_type="claude", volume_id="v1", subpath="agents/a1",
        )

    msg = str(excinfo.value)
    assert "DAYTONA_IMAGE" in msg, msg
    assert ".runtime-image-tag" in msg, msg


# ``test_provision_flag_off_defaults_to_hive_large_snapshot`` was deleted
# in the runtime-image-unification refactor — the legacy hive-large
# default no longer exists, so there's no behavior to pin.


# ---------------------------------------------------------------------------
# start_supervisor_in_sandbox: flag-on skips cache-extract path
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_start_supervisor_uses_image_runtime_paths(monkeypatch):
    """``start_supervisor_in_sandbox`` resolves the supervisor + ACP bin
    from the image-runtime path (``/opt/agent-sdk/runtime/``) — Phase E
    of the runtime-image-unification refactor collapsed the flag-conditional
    branches into this unconditional path."""
    # Capture every shell command the supervisor-start path runs in the
    # sandbox so we can assert which paths got referenced.
    exec_log: list[str] = []

    async def fake_run_sandbox_exec_async(sandbox, cmd, timeout=120):
        exec_log.append(cmd)
        # Idempotency probe: pretend nothing's listening yet.
        if "/v1/health" in cmd:
            return SimpleNamespace(stdout="000", stderr="", exit_code=0)
        # The setsid spawn returns "started" on success.
        return SimpleNamespace(stdout="started", stderr="", exit_code=0)

    monkeypatch.setattr(dprov, "_run_sandbox_exec_async", fake_run_sandbox_exec_async)

    # Skip the 45×1s health-poll — argv is recorded before health checks.
    async def _ok_health(*_a, **_kw):
        return True
    monkeypatch.setattr(dprov, "_wait_for_health", _ok_health)

    async def _fake_signed_url(port, ttl):
        return SimpleNamespace(url="https://fake.daytona.app/")

    fake_sandbox = SimpleNamespace(
        id="sb-fake-1234567890",
        create_signed_preview_url=_fake_signed_url,
    )

    url = await dprov.start_supervisor_in_sandbox(
        fake_sandbox, agent_type="claude", port=9100, root="/home/daytona",
    )

    assert url == "https://fake.daytona.app"

    spawn_cmd = next((c for c in exec_log if "setsid" in c), "")
    assert spawn_cmd, f"no spawn command captured. exec_log: {exec_log}"
    # The supervisor spawn cd's into /opt/agent-sdk/runtime.
    assert "cd /opt/agent-sdk/runtime" in spawn_cmd, (
        f"supervisor spawn should cd into image-runtime path: {spawn_cmd!r}"
    )
    # Legacy /tmp/sup-work-* working dir doesn't appear.
    assert "/tmp/sup-work" not in spawn_cmd, (
        f"image-runtime path must not reference legacy /tmp/sup-work: {spawn_cmd!r}"
    )


# ``test_start_supervisor_flag_off_uses_legacy_resolver`` was deleted in
# Phase E — there is no flag-off path and no ``_resolve_legacy_volume_supervisor``
# helper anymore (also deleted).
