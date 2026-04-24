"""Unit tests for pre_start_commands failure propagation in the Daytona provider.

These tests are pure-unit (no DAYTONA_API_KEY needed) — sandbox.process.exec
is mocked to return objects with controlled exit_code / result / stderr fields.
"""
from __future__ import annotations

import logging
import os
import sys
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
    sb.id = "fake-sandbox-abc123"
    sb.process.exec.return_value = exec_result
    return sb


def _fake_daytona_class(sandbox):
    """Return a fake Daytona class that always creates the given sandbox."""

    class _FakeDaytona:
        def __init__(self, *a, **kw):
            pass

        def create(self, params, timeout=None):
            return sandbox

        def delete(self, sb):
            pass

    return _FakeDaytona


# ---------------------------------------------------------------------------
# Tests for _run_sandbox_exec (the extracted helper)
# ---------------------------------------------------------------------------

class TestRunSandboxExec:
    def test_captures_stdout(self):
        from api.providers.daytona import _run_sandbox_exec
        sb = _make_sandbox(_make_exec_result(exit_code=0, stdout="hello\n"))
        assert _run_sandbox_exec(sb, "echo hello").stdout == "hello\n"

    def test_captures_stderr(self):
        from api.providers.daytona import _run_sandbox_exec
        sb = _make_sandbox(_make_exec_result(exit_code=1, stderr="error message"))
        assert _run_sandbox_exec(sb, "badcmd").stderr == "error message"

    def test_captures_exit_code(self):
        from api.providers.daytona import _run_sandbox_exec
        sb = _make_sandbox(_make_exec_result(exit_code=127, stderr="command not found"))
        assert _run_sandbox_exec(sb, "missingcmd").exit_code == 127

    def test_ok_true_for_zero(self):
        from api.providers.daytona import _run_sandbox_exec
        assert _run_sandbox_exec(_make_sandbox(_make_exec_result(0)), "true").ok is True

    def test_ok_false_for_nonzero(self):
        from api.providers.daytona import _run_sandbox_exec
        assert _run_sandbox_exec(_make_sandbox(_make_exec_result(1)), "false").ok is False

    def test_ok_true_for_none_exit_code(self):
        """exit_code=None means the SDK didn't report it; treat as OK."""
        from api.providers.daytona import _run_sandbox_exec
        sb = MagicMock()
        sb.process.exec.return_value = SimpleNamespace(result="output")  # no exit_code
        result = _run_sandbox_exec(sb, "cmd")
        assert result.exit_code is None
        assert result.ok is True

    def test_defensive_missing_result_field(self):
        """When SDK object has no 'result' attr, stdout should be empty string."""
        from api.providers.daytona import _run_sandbox_exec
        sb = MagicMock()
        sb.process.exec.return_value = SimpleNamespace(exit_code=0)  # no result or stderr
        result = _run_sandbox_exec(sb, "cmd")
        assert result.stdout == ""
        assert result.stderr == ""


# ---------------------------------------------------------------------------
# Tests for pre_start_commands behavior in provision_daytona_sandbox
# ---------------------------------------------------------------------------

def _provision_ctx(sandbox):
    """Context manager stack for provision_daytona_sandbox unit tests."""
    import daytona_sdk as _dsdk
    return (
        patch.dict(os.environ, {"DAYTONA_API_KEY": "test-key", "DAYTONA_SNAPSHOT": "0"}),
        patch.object(_dsdk, "Daytona", _fake_daytona_class(sandbox)),
        patch.object(_dsdk, "DaytonaConfig", MagicMock()),
        patch("api.providers.daytona._build_volume_mounts", return_value=[]),
        patch("api.providers.daytona._get_sandbox_env_vars", return_value={}),
    )


@pytest.mark.asyncio
async def test_pre_start_exit_zero_no_raise(caplog):
    """exit_code=0 → no raise, and INFO log is emitted."""
    from api.providers.daytona import provision_daytona_sandbox

    sb = _make_sandbox(_make_exec_result(exit_code=0, stdout="all good\n"))
    env_ctx, daytona_ctx, cfg_ctx, mounts_ctx, env_vars_ctx = _provision_ctx(sb)

    with caplog.at_level(logging.INFO, logger="api.providers.daytona"), \
         env_ctx, daytona_ctx, cfg_ctx, mounts_ctx, env_vars_ctx:
        inst = await provision_daytona_sandbox(
            agent_type="claude",
            pre_start_commands=["echo hello"],
        )

    assert inst is not None
    assert inst.sandbox_id == "fake-sandbox-abc123"
    assert any("pre-start" in r.message for r in caplog.records if r.levelno == logging.INFO), (
        f"Expected pre-start INFO log; records: {[r.message for r in caplog.records]}"
    )


@pytest.mark.asyncio
async def test_pre_start_exit_127_raises(caplog):
    """exit_code=127 → raises RuntimeError with command and stderr in the message."""
    from api.providers.daytona import provision_daytona_sandbox

    sb = _make_sandbox(_make_exec_result(
        exit_code=127, stderr="bash: curl: command not found",
    ))
    env_ctx, daytona_ctx, cfg_ctx, mounts_ctx, env_vars_ctx = _provision_ctx(sb)

    with caplog.at_level(logging.ERROR, logger="api.providers.daytona"), \
         env_ctx, daytona_ctx, cfg_ctx, mounts_ctx, env_vars_ctx:
        with pytest.raises(RuntimeError) as exc_info:
            await provision_daytona_sandbox(
                agent_type="claude",
                pre_start_commands=["curl https://example.com"],
            )

    msg = str(exc_info.value)
    assert "exit=127" in msg, f"Expected exit=127 in message: {msg!r}"
    assert "curl https://example.com" in msg, f"Expected command in message: {msg!r}"
    assert "command not found" in msg, f"Expected stderr snippet in message: {msg!r}"
    assert any(r.levelno == logging.ERROR for r in caplog.records), (
        f"Expected ERROR log; records: {[r.message for r in caplog.records]}"
    )


@pytest.mark.asyncio
async def test_pre_start_exit_none_no_raise_warns(caplog):
    """exit_code=None (SDK didn't report it) → no raise, but WARNING logged."""
    from api.providers.daytona import provision_daytona_sandbox

    sb = MagicMock()
    sb.id = "fake-sandbox-abc123"
    # SDK object with no exit_code attribute
    sb.process.exec.return_value = SimpleNamespace(result="output", stderr="")

    env_ctx, daytona_ctx, cfg_ctx, mounts_ctx, env_vars_ctx = _provision_ctx(sb)

    with caplog.at_level(logging.WARNING, logger="api.providers.daytona"), \
         env_ctx, daytona_ctx, cfg_ctx, mounts_ctx, env_vars_ctx:
        inst = await provision_daytona_sandbox(
            agent_type="claude",
            pre_start_commands=["some-cmd"],
        )

    assert inst is not None
    assert any(
        r.levelno == logging.WARNING and "exit_code" in r.message
        for r in caplog.records
    ), (
        f"Expected WARNING about missing exit_code; records: {[r.message for r in caplog.records]}"
    )
