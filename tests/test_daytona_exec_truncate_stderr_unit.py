"""daytona exec_in_sandbox must cap stderr, not just stdout.

It previously truncated stdout to 1 MiB but returned stderr UNTRUNCATED — a
command with huge stderr (a verbose build, an error dump) returned all of it,
holding it in server RAM and flowing it downstream (ExecResult → agent → logs)
unbounded. modal (#188) and docker cap both streams; this brings daytona to
parity.

Mocked — no cloud.
"""
from __future__ import annotations

from types import SimpleNamespace

import pytest

from api.providers._shared import _MAX_OUTPUT_BYTES

pytestmark = pytest.mark.asyncio


def _fake_exec_result(stdout: str, stderr: str, code: int = 0):
    return SimpleNamespace(result=stdout, stderr=stderr, exit_code=code)


async def test_exec_caps_both_stdout_and_stderr(monkeypatch):
    from api.providers import daytona as dmod
    from api.providers._shared import ProviderInstance

    huge_out = "o" * (_MAX_OUTPUT_BYTES + 50_000)
    huge_err = "e" * (_MAX_OUTPUT_BYTES + 50_000)

    class _FakeProcess:
        async def exec(self, cmd, timeout=None):
            return _fake_exec_result(huge_out, huge_err)

    fake_sandbox = SimpleNamespace(id="sb-1", process=_FakeProcess())

    async def _client():
        async def _get(ref):
            return fake_sandbox
        return SimpleNamespace(get=_get)

    monkeypatch.setattr(dmod, "_get_async_daytona_client", _client)

    inst = ProviderInstance(provider="daytona", url="", sandbox_ref="sb-1")
    res = await dmod.exec_in_sandbox(inst, "noisy-cmd")

    assert len(res.stdout.encode()) == _MAX_OUTPUT_BYTES and res.stdout_truncated is True
    assert len(res.stderr.encode()) == _MAX_OUTPUT_BYTES, (
        "stderr must be capped at 1 MiB — a huge-stderr command must not return "
        "its full stderr (unbounded RAM + downstream)")
    assert res.stderr_truncated is True, "truncation must be flagged for stderr too"
    assert res.exit_code == 0


async def test_exec_small_output_intact(monkeypatch):
    from api.providers import daytona as dmod
    from api.providers._shared import ProviderInstance

    class _FakeProcess:
        async def exec(self, cmd, timeout=None):
            return _fake_exec_result("hi\n", "warn\n", 0)

    fake_sandbox = SimpleNamespace(id="sb-1", process=_FakeProcess())

    async def _client():
        async def _get(ref):
            return fake_sandbox
        return SimpleNamespace(get=_get)

    monkeypatch.setattr(dmod, "_get_async_daytona_client", _client)

    res = await dmod.exec_in_sandbox(
        ProviderInstance(provider="daytona", url="", sandbox_ref="sb-1"), "echo hi")
    assert res.stdout == "hi\n" and res.stdout_truncated is False
    assert res.stderr == "warn\n" and res.stderr_truncated is False
