"""Unit tests for the daytona provider's per-session image/snapshot support.

The daytona analog of the modal per-session image (ec3b65d): ``Recipe.image``
→ ``DaytonaSandboxSession._resolve_or_create_sandbox`` → ``create_sandbox`` →
``provision_daytona_sandbox(image=...)``. A value naming a REGISTERED daytona
snapshot boots via ``CreateSandboxFromSnapshotParams``; anything else falls
through to the registry-ref ``CreateSandboxFromImageParams`` path.

Pure-unit: the AsyncDaytona client is mocked; no DAYTONA_API_KEY needed.
"""
from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from daytona_sdk import (
    CreateSandboxFromImageParams,
    CreateSandboxFromSnapshotParams,
)


def _fake_sandbox():
    sb = MagicMock()
    sb.id = "fake-sandbox-abc123"
    return sb


def _fake_client(*, snapshot_exists: bool):
    """AsyncDaytona stand-in: create() returns a sandbox handle; snapshot.get()
    succeeds or raises depending on ``snapshot_exists``."""
    fake = MagicMock()
    fake.create = AsyncMock(return_value=_fake_sandbox())
    fake.delete = AsyncMock(return_value=None)
    if snapshot_exists:
        fake.snapshot.get = AsyncMock(
            return_value=SimpleNamespace(name="whatever", state="active")
        )
    else:
        fake.snapshot.get = AsyncMock(side_effect=Exception("snapshot not found"))
    return fake


@pytest.fixture(autouse=True)
def _clear_snapshot_cache():
    from api.providers import daytona as dt
    dt._snapshot_registered_cache.clear()
    yield
    dt._snapshot_registered_cache.clear()


async def _provision(client, **kwargs):
    from api.providers.daytona import provision_daytona_sandbox
    with patch(
        "api.providers.daytona._get_async_daytona_client",
        AsyncMock(return_value=client),
    ):
        return await provision_daytona_sandbox(agent_type="claude", **kwargs)


class TestPerSessionImage:
    @pytest.mark.asyncio
    async def test_registered_snapshot_boots_snapshot_path(self):
        client = _fake_client(snapshot_exists=True)
        await _provision(client, image="honeycomb-aws-combined-v1")
        params = client.create.call_args.args[0]
        assert isinstance(params, CreateSandboxFromSnapshotParams)
        assert params.snapshot == "honeycomb-aws-combined-v1"

    @pytest.mark.asyncio
    async def test_unregistered_ref_boots_registry_image_path(self):
        client = _fake_client(snapshot_exists=False)
        await _provision(client, image="localstack/localstack:4.4.0")
        params = client.create.call_args.args[0]
        assert isinstance(params, CreateSandboxFromImageParams)
        assert params.image == "localstack/localstack:4.4.0"

    @pytest.mark.asyncio
    async def test_snapshot_wins_over_resources(self):
        # Snapshots bake resources at creation time — the per-session
        # resources are dropped rather than mis-routing the snapshot name
        # down the registry-image path.
        client = _fake_client(snapshot_exists=True)
        await _provision(
            client, image="honeycomb-aws-combined-v1",
            resources=SimpleNamespace(cpu=2, memory=4096),
        )
        params = client.create.call_args.args[0]
        assert isinstance(params, CreateSandboxFromSnapshotParams)
        assert params.snapshot == "honeycomb-aws-combined-v1"

    @pytest.mark.asyncio
    async def test_image_wins_over_dockerfile(self, tmp_path):
        df = tmp_path / "Dockerfile"
        df.write_text("FROM scratch")
        client = _fake_client(snapshot_exists=True)
        await _provision(client, image="my-snap", dockerfile=str(df))
        params = client.create.call_args.args[0]
        assert isinstance(params, CreateSandboxFromSnapshotParams)
        assert params.snapshot == "my-snap"

    @pytest.mark.asyncio
    async def test_positive_snapshot_lookup_is_cached(self):
        client = _fake_client(snapshot_exists=True)
        await _provision(client, image="my-snap")
        await _provision(client, image="my-snap")
        assert client.snapshot.get.await_count == 1

    @pytest.mark.asyncio
    async def test_default_path_untouched(self, monkeypatch):
        # No per-session image → env/pin snapshot resolution, no control-plane
        # snapshot lookup.
        monkeypatch.setenv("DAYTONA_SNAPSHOT", "agent-sdk-pinned")
        client = _fake_client(snapshot_exists=True)
        await _provision(client)
        params = client.create.call_args.args[0]
        assert isinstance(params, CreateSandboxFromSnapshotParams)
        assert params.snapshot == "agent-sdk-pinned"
        client.snapshot.get.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_default_resources_fallback_to_image_path(self, monkeypatch):
        # Pre-existing behavior: default snapshot + per-session resources →
        # image path so the resources are honoured.
        monkeypatch.setenv("DAYTONA_SNAPSHOT", "agent-sdk-pinned")
        monkeypatch.setenv("DAYTONA_IMAGE", "agent-sdk:pinned")
        from api.sandbox.state import Resources
        client = _fake_client(snapshot_exists=True)
        await _provision(client, resources=Resources(cpu=1, memory_mib=1024))
        params = client.create.call_args.args[0]
        assert isinstance(params, CreateSandboxFromImageParams)
        assert params.image == "agent-sdk:pinned"


class TestSessionThreadsRecipeImage:
    @pytest.mark.asyncio
    async def test_cold_create_passes_recipe_image(self):
        from api.providers.daytona.session import DaytonaSandboxSession
        from api.sandbox.state import DaytonaSandboxState, Recipe

        state = DaytonaSandboxState(
            recipe=Recipe(agent_type="claude", image="honeycomb-aws-combined-v1")
        )
        sess = DaytonaSandboxSession(session_id="sess-1", state=state)
        sess._volume_ref = "vol-1"
        sess._subpath = "sessions/sess-1"

        dt_provider = MagicMock()
        dt_provider.create_sandbox = AsyncMock(
            return_value=SimpleNamespace(sandbox_ref="sb-1")
        )
        dt_provider._get_async_daytona_client = AsyncMock(
            return_value=MagicMock(get=AsyncMock(return_value=_fake_sandbox()))
        )

        sandbox, created_fresh = await sess._resolve_or_create_sandbox(dt_provider)
        assert created_fresh is True
        assert dt_provider.create_sandbox.call_args.kwargs["image"] == "honeycomb-aws-combined-v1"
