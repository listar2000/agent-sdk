"""Unit tests for pre_start_commands failure propagation in the Daytona provider.

These tests are pure-unit (no DAYTONA_API_KEY needed) — sandbox.process.exec
is mocked to return objects with controlled exit_code / result / stderr fields.
"""
from __future__ import annotations

import asyncio
import logging
import sys
import os
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

_SRC = os.path.join(os.path.dirname(__file__), "..", "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_exec_result(exit_code, stdout="", stderr=""):
    """Return a SimpleNamespace that mimics the Daytona SDK exec result object."""
    return SimpleNamespace(exit_code=exit_code, result=stdout, stderr=stderr)


def _make_sandbox(exec_result):
    """Return a mock sandbox whose process.exec always returns exec_result."""
    sb = MagicMock()
    sb.id = "test-sandbox-id"
    sb.process.exec.return_value = exec_result
    return sb


# ---------------------------------------------------------------------------
# Tests for _run_sandbox_exec (the extracted helper)
# ---------------------------------------------------------------------------

class TestRunSandboxExec:
    def test_captures_stdout(self):
        from api.providers.daytona import _run_sandbox_exec
        r = _make_exec_result(exit_code=0, stdout="hello\n", stderr="")
        sb = _make_sandbox(r)
        result = _run_sandbox_exec(sb, "echo hello")
        assert result.stdout == "hello\n"

    def test_captures_stderr(self):
        from api.providers.daytona import _run_sandbox_exec
        r = _make_exec_result(exit_code=1, stdout="", stderr="error message")
        sb = _make_sandbox(r)
        result = _run_sandbox_exec(sb, "badcmd")
        assert result.stderr == "error message"

    def test_captures_exit_code(self):
        from api.providers.daytona import _run_sandbox_exec
        r = _make_exec_result(exit_code=127, stdout="", stderr="command not found")
        sb = _make_sandbox(r)
        result = _run_sandbox_exec(sb, "missingcmd")
        assert result.exit_code == 127

    def test_ok_true_for_zero(self):
        from api.providers.daytona import _run_sandbox_exec
        sb = _make_sandbox(_make_exec_result(exit_code=0))
        assert _run_sandbox_exec(sb, "true").ok is True

    def test_ok_false_for_nonzero(self):
        from api.providers.daytona import _run_sandbox_exec
        sb = _make_sandbox(_make_exec_result(exit_code=1))
        assert _run_sandbox_exec(sb, "false").ok is False

    def test_ok_true_for_none_exit_code(self):
        """exit_code=None means the SDK didn't report it; treat as OK (can't distinguish)."""
        from api.providers.daytona import _run_sandbox_exec
        r = SimpleNamespace(result="some output")  # no exit_code or stderr attribute
        sb = MagicMock()
        sb.process.exec.return_value = r
        result = _run_sandbox_exec(sb, "cmd")
        assert result.exit_code is None
        assert result.ok is True

    def test_defensive_missing_result_field(self):
        """When SDK object has no 'result' attr, stdout should be empty string."""
        from api.providers.daytona import _run_sandbox_exec
        r = SimpleNamespace(exit_code=0)  # no result or stderr
        sb = MagicMock()
        sb.process.exec.return_value = r
        result = _run_sandbox_exec(sb, "cmd")
        assert result.stdout == ""
        assert result.stderr == ""


# ---------------------------------------------------------------------------
# Tests for pre_start_commands behavior in provision_daytona_sandbox
# ---------------------------------------------------------------------------

def _make_fake_sandbox(exec_result):
    """Create a mock sandbox matching what provision_daytona_sandbox works with."""
    sb = MagicMock()
    sb.id = "fake-sandbox-abc123"
    sb.process.exec.return_value = exec_result
    return sb


def _patched_provision(pre_start_commands, exec_result):
    """
    Call provision_daytona_sandbox's pre_start loop in isolation by patching
    daytona.create to return a fake sandbox, and patching out the
    Daytona/DaytonaConfig imports.
    """
    from api.providers.daytona import provision_daytona_sandbox

    fake_sandbox = _make_fake_sandbox(exec_result)

    fake_daytona_instance = MagicMock()
    fake_daytona_instance.create.return_value = fake_sandbox

    FakeDaytona = MagicMock(return_value=fake_daytona_instance)
    FakeDaytonaConfig = MagicMock()
    FakeCreateSandboxFromSnapshotParams = MagicMock()
    FakeCreateSandboxFromImageParams = MagicMock()

    with patch.dict(os.environ, {"DAYTONA_API_KEY": "test-key", "DAYTONA_SNAPSHOT": "0"}), \
         patch("api.providers.daytona.provision_daytona_sandbox.__globals__", {}):
        pass  # just confirming we can import

    return fake_sandbox, fake_daytona_instance, FakeDaytona, FakeDaytonaConfig, \
        FakeCreateSandboxFromSnapshotParams, FakeCreateSandboxFromImageParams


@pytest.mark.asyncio
async def test_pre_start_exit_zero_no_raise(caplog):
    """exit_code=0 → no raise, and INFO log is emitted."""
    from api.providers.daytona import provision_daytona_sandbox

    exec_result = _make_exec_result(exit_code=0, stdout="all good\n", stderr="")
    fake_sandbox = _make_fake_sandbox(exec_result)

    fake_daytona = MagicMock()
    fake_daytona.create.return_value = fake_sandbox

    with caplog.at_level(logging.INFO, logger="api.providers.daytona"), \
         patch.dict(os.environ, {"DAYTONA_API_KEY": "test-key", "DAYTONA_SNAPSHOT": "0"}), \
         patch("api.providers.daytona._get_daytona_client", return_value=fake_daytona), \
         patch("api.providers.daytona._build_volume_mounts", return_value=[]), \
         patch("api.providers.daytona._get_sandbox_env_vars", return_value={}):

        from daytona_sdk import Daytona as _RealDaytona, DaytonaConfig, \
            CreateSandboxFromImageParams, CreateSandboxFromSnapshotParams
        with patch("api.providers.daytona.Daytona" if False else "daytona_sdk.Daytona",
                   _RealDaytona):
            pass

        # Patch the Daytona constructor inside the function's local import
        import daytona_sdk as _dsdk
        orig_daytona_cls = _dsdk.Daytona

        class _FakeDaytonaClass:
            def __init__(self, *a, **kw):
                pass
            def create(self, params, timeout=None):
                return fake_sandbox
            def delete(self, sb):
                pass

        with patch.object(_dsdk, "Daytona", _FakeDaytonaClass), \
             patch.object(_dsdk, "DaytonaConfig", MagicMock()):
            inst = await provision_daytona_sandbox(
                agent_type="claude",
                pre_start_commands=["echo hello"],
            )

    assert inst is not None
    assert inst.sandbox_id == "fake-sandbox-abc123"
    # INFO log for success
    info_records = [r for r in caplog.records if r.levelno == logging.INFO
                    and "pre-start" in r.message]
    assert any("pre-start" in r.message for r in info_records), \
        f"Expected pre-start INFO log, got: {[r.message for r in caplog.records]}"


@pytest.mark.asyncio
async def test_pre_start_exit_127_raises(caplog):
    """exit_code=127 → raises RuntimeError with command + stderr in message."""
    from api.providers.daytona import provision_daytona_sandbox

    exec_result = _make_exec_result(
        exit_code=127,
        stdout="",
        stderr="bash: curl: command not found",
    )
    fake_sandbox = _make_fake_sandbox(exec_result)

    import daytona_sdk as _dsdk

    class _FakeDaytonaClass:
        def __init__(self, *a, **kw):
            pass
        def create(self, params, timeout=None):
            return fake_sandbox
        def delete(self, sb):
            pass

    with caplog.at_level(logging.ERROR, logger="api.providers.daytona"), \
         patch.dict(os.environ, {"DAYTONA_API_KEY": "test-key", "DAYTONA_SNAPSHOT": "0"}), \
         patch.object(_dsdk, "Daytona", _FakeDaytonaClass), \
         patch.object(_dsdk, "DaytonaConfig", MagicMock()), \
         patch("api.providers.daytona._build_volume_mounts", return_value=[]), \
         patch("api.providers.daytona._get_sandbox_env_vars", return_value={}):

        with pytest.raises(RuntimeError) as exc_info:
            await provision_daytona_sandbox(
                agent_type="claude",
                pre_start_commands=["curl https://example.com"],
            )

    msg = str(exc_info.value)
    assert "exit=127" in msg, f"Expected exit=127 in message: {msg!r}"
    assert "curl https://example.com" in msg, f"Expected command in message: {msg!r}"
    assert "command not found" in msg, f"Expected stderr snippet in message: {msg!r}"

    # ERROR log should be present
    error_records = [r for r in caplog.records if r.levelno == logging.ERROR]
    assert error_records, f"Expected ERROR log, got: {[r.message for r in caplog.records]}"


@pytest.mark.asyncio
async def test_pre_start_exit_none_no_raise_warns(caplog):
    """exit_code=None (SDK didn't report it) → no raise, but WARNING logged."""
    from api.providers.daytona import provision_daytona_sandbox

    # Simulate SDK returning an object with no exit_code attribute
    exec_result = SimpleNamespace(result="output", stderr="")  # no exit_code
    fake_sandbox = MagicMock()
    fake_sandbox.id = "fake-sandbox-abc123"
    fake_sandbox.process.exec.return_value = exec_result

    import daytona_sdk as _dsdk

    class _FakeDaytonaClass:
        def __init__(self, *a, **kw):
            pass
        def create(self, params, timeout=None):
            return fake_sandbox
        def delete(self, sb):
            pass

    with caplog.at_level(logging.WARNING, logger="api.providers.daytona"), \
         patch.dict(os.environ, {"DAYTONA_API_KEY": "test-key", "DAYTONA_SNAPSHOT": "0"}), \
         patch.object(_dsdk, "Daytona", _FakeDaytonaClass), \
         patch.object(_dsdk, "DaytonaConfig", MagicMock()), \
         patch("api.providers.daytona._build_volume_mounts", return_value=[]), \
         patch("api.providers.daytona._get_sandbox_env_vars", return_value={}):

        inst = await provision_daytona_sandbox(
            agent_type="claude",
            pre_start_commands=["some-cmd"],
        )

    assert inst is not None
    # WARNING should be present
    warn_records = [r for r in caplog.records if r.levelno == logging.WARNING
                    and "exit_code" in r.message]
    assert warn_records, (
        f"Expected WARNING about missing exit_code, got: {[r.message for r in caplog.records]}"
    )
