import os
import sys
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from api.providers import create_daytona


@pytest.mark.asyncio
async def test_create_daytona_uses_default_snapshot_when_env_unset(monkeypatch):
    create_calls = []

    class FakeCreateSandboxFromImageParams:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

    class FakeCreateSandboxFromSnapshotParams:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

    class FakeDaytonaConfig:
        def __init__(self, api_key):
            self.api_key = api_key

    signed = SimpleNamespace(url="https://preview.example.com")
    sandbox = MagicMock()
    sandbox.id = "daytona-sandbox-default"
    sandbox.process.exec = MagicMock(return_value=SimpleNamespace(exit_code=0, result="ok"))
    sandbox.create_signed_preview_url = MagicMock(return_value=signed)

    class FakeDaytona:
        def __init__(self, config):
            self.config = config

        def create(self, params, timeout):
            create_calls.append((params, timeout))
            return sandbox

    fake_daytona_sdk = SimpleNamespace(
        Daytona=FakeDaytona,
        DaytonaConfig=FakeDaytonaConfig,
        CreateSandboxFromImageParams=FakeCreateSandboxFromImageParams,
        CreateSandboxFromSnapshotParams=FakeCreateSandboxFromSnapshotParams,
    )

    monkeypatch.delenv("DAYTONA_SNAPSHOT", raising=False)
    monkeypatch.setenv("DAYTONA_API_KEY", "dtn_test")
    # First health probe fails (forces full bootstrap path); second succeeds.
    monkeypatch.setattr("api.providers._wait_for_health", AsyncMock(side_effect=[False, True]))
    monkeypatch.setitem(sys.modules, "daytona_sdk", fake_daytona_sdk)

    instance = await create_daytona(agent_type="claude", dockerfile=None)

    assert instance.sandbox_id == "daytona-sandbox-default"
    assert len(create_calls) == 1
    params, _ = create_calls[0]
    assert isinstance(params, FakeCreateSandboxFromSnapshotParams)
    assert params.kwargs["snapshot"] == "hive-large"
    sandbox.process.exec.assert_any_call("python3 -m pip install --no-cache-dir hive-evolve")
    # Snapshot bootstrap runs a single guarded install command that short-circuits
    # if sandbox-agent is already present (the expected hive-large case).
    exec_commands = [call.args[0] for call in sandbox.process.exec.call_args_list]
    assert any("command -v sandbox-agent" in cmd for cmd in exec_commands), (
        f"expected guarded sandbox-agent install command, got: {exec_commands}"
    )
    sandbox.process.exec.assert_any_call("sandbox-agent install-agent claude")


@pytest.mark.asyncio
async def test_create_daytona_skips_bootstrap_when_already_serving(monkeypatch):
    """hive-large snapshots that pre-run sandbox-agent should skip bootstrap entirely."""
    class FakeCreateSandboxFromImageParams:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

    class FakeCreateSandboxFromSnapshotParams:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

    class FakeDaytonaConfig:
        def __init__(self, api_key):
            self.api_key = api_key

    signed = SimpleNamespace(url="https://preview.example.com")
    sandbox = MagicMock()
    sandbox.id = "daytona-prebaked"
    sandbox.process.exec = MagicMock(return_value=SimpleNamespace(exit_code=0, result="ok"))
    sandbox.create_signed_preview_url = MagicMock(return_value=signed)

    class FakeDaytona:
        def __init__(self, config):
            pass

        def create(self, params, timeout):
            return sandbox

    fake_daytona_sdk = SimpleNamespace(
        Daytona=FakeDaytona,
        DaytonaConfig=FakeDaytonaConfig,
        CreateSandboxFromImageParams=FakeCreateSandboxFromImageParams,
        CreateSandboxFromSnapshotParams=FakeCreateSandboxFromSnapshotParams,
    )

    monkeypatch.delenv("DAYTONA_SNAPSHOT", raising=False)
    monkeypatch.setenv("DAYTONA_API_KEY", "dtn_test")
    # First health probe succeeds — sandbox-agent is already running.
    monkeypatch.setattr("api.providers._wait_for_health", AsyncMock(return_value=True))
    monkeypatch.setitem(sys.modules, "daytona_sdk", fake_daytona_sdk)

    instance = await create_daytona(agent_type="claude", dockerfile=None)

    assert instance.sandbox_id == "daytona-prebaked"
    # Bootstrap must not run when the server is already healthy.
    sandbox.process.exec.assert_not_called()


@pytest.mark.asyncio
async def test_create_daytona_uses_snapshot_override(monkeypatch):
    create_calls = []

    class FakeCreateSandboxFromImageParams:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

    class FakeCreateSandboxFromSnapshotParams:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

    class FakeDaytonaConfig:
        def __init__(self, api_key):
            self.api_key = api_key

    signed = SimpleNamespace(url="https://preview.example.com")
    sandbox = MagicMock()
    sandbox.id = "daytona-sandbox-123"
    sandbox.process.exec = MagicMock(return_value=SimpleNamespace(exit_code=0, result="ok"))
    sandbox.create_signed_preview_url = MagicMock(return_value=signed)

    class FakeDaytona:
        def __init__(self, config):
            self.config = config

        def create(self, params, timeout):
            create_calls.append((params, timeout))
            return sandbox

    fake_daytona_sdk = SimpleNamespace(
        Daytona=FakeDaytona,
        DaytonaConfig=FakeDaytonaConfig,
        CreateSandboxFromImageParams=FakeCreateSandboxFromImageParams,
        CreateSandboxFromSnapshotParams=FakeCreateSandboxFromSnapshotParams,
    )

    monkeypatch.setenv("DAYTONA_API_KEY", "dtn_test")
    monkeypatch.setenv("DAYTONA_SNAPSHOT", "hive-large")
    # First health probe fails (forces bootstrap path); second succeeds.
    monkeypatch.setattr("api.providers._wait_for_health", AsyncMock(side_effect=[False, True]))
    monkeypatch.setitem(sys.modules, "daytona_sdk", fake_daytona_sdk)

    instance = await create_daytona(agent_type="claude", dockerfile="/tmp/ignored.Dockerfile")

    assert instance.provider == "daytona"
    assert instance.sandbox_id == "daytona-sandbox-123"
    assert instance.url == "https://preview.example.com"
    assert len(create_calls) == 1
    params, timeout = create_calls[0]
    assert isinstance(params, FakeCreateSandboxFromSnapshotParams)
    assert params.kwargs["snapshot"] == "hive-large"
    assert timeout == 60
    sandbox.process.exec.assert_any_call("python3 -m pip install --no-cache-dir hive-evolve")
    # Snapshot bootstrap runs a single guarded install command that short-circuits
    # if sandbox-agent is already present (the expected hive-large case).
    exec_commands = [call.args[0] for call in sandbox.process.exec.call_args_list]
    assert any("command -v sandbox-agent" in cmd for cmd in exec_commands), (
        f"expected guarded sandbox-agent install command, got: {exec_commands}"
    )
    sandbox.process.exec.assert_any_call("sandbox-agent install-agent claude")


@pytest.mark.asyncio
async def test_create_daytona_preserves_sandbox_on_health_failure(monkeypatch):
    """Startup failures must NOT delete the sandbox — we want to debug it.

    History: an earlier version deleted on any startup failure, which masked
    the root cause (logs piped to /dev/null, sandbox gone) and contributed to
    a retry storm that created thousands of orphaned sandboxes. Now we leave
    the sandbox alive so operators can inspect it and Daytona's own lifecycle
    (auto_archive_interval) reaps it.
    """
    class FakeCreateSandboxFromImageParams:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

    class FakeCreateSandboxFromSnapshotParams:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

    class FakeDaytonaConfig:
        def __init__(self, api_key):
            self.api_key = api_key

    sandbox = MagicMock()
    sandbox.id = "daytona-failed-sandbox"
    sandbox.process.exec = MagicMock(return_value=SimpleNamespace(exit_code=0, result="ok"))
    sandbox.create_signed_preview_url = MagicMock(return_value=SimpleNamespace(url="https://preview.example.com"))

    class FakeDaytona:
        last_instance = None

        def __init__(self, config):
            self.config = config
            self.delete = MagicMock()
            FakeDaytona.last_instance = self

        def create(self, params, timeout):
            return sandbox

    fake_daytona_sdk = SimpleNamespace(
        Daytona=FakeDaytona,
        DaytonaConfig=FakeDaytonaConfig,
        CreateSandboxFromImageParams=FakeCreateSandboxFromImageParams,
        CreateSandboxFromSnapshotParams=FakeCreateSandboxFromSnapshotParams,
    )

    monkeypatch.delenv("DAYTONA_SNAPSHOT", raising=False)
    monkeypatch.setenv("DAYTONA_API_KEY", "dtn_test")
    monkeypatch.setattr("api.providers._wait_for_health", AsyncMock(return_value=False))
    monkeypatch.setitem(sys.modules, "daytona_sdk", fake_daytona_sdk)

    with pytest.raises(RuntimeError, match="failed health check"):
        await create_daytona(agent_type="claude", dockerfile=None)

    # Sandbox is left alive for debugging; no delete call expected.
    FakeDaytona.last_instance.delete.assert_not_called()
