"""Contracts for POST /sessions/{id}/sandbox/exec.

The route should execute through the resolved live supervisor for the session,
not by reconstructing a partial ProviderInstance in the handler. Provider-level
tests below keep the local/docker direct-exec helper honest for remaining
internal callers.
"""
from __future__ import annotations

import os
import sys
from types import SimpleNamespace

import pytest
from fastapi import Response
from httpx import ASGITransport, AsyncClient

_SRC = os.path.join(os.path.dirname(__file__), "..", "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

from api import providers, server as srv  # noqa: E402


class _FakeProc:
    returncode = 0

    async def communicate(self):
        return b"ok\n", b""

    def kill(self):
        self.returncode = -9


@pytest.fixture(autouse=True)
def _clear_runtime_state():
    # Legacy SESSIONS / _INSTANCES dicts were deleted; the pool keeps
    # its own state and these tests no longer need bookkeeping cleanup.
    yield


async def _post_exec(command: str = "pwd"):
    transport = ASGITransport(app=srv.app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        return await c.post(
            "/sessions/sess-1/sandbox/exec",
            json={"command": command, "timeout": 5},
        )


@pytest.mark.asyncio
async def test_session_sandbox_exec_proxies_to_session_supervisor(monkeypatch):
    """The route delegates to ``_proxy_from_session``, which itself
    resolves the session's supervisor URL via the SessionPool."""
    calls: list[dict] = []

    async def fake_proxy_from_session(session_id, method, path, *,
                                      params=None, json=None, timeout=30):
        calls.append({
            "kind": "proxy", "session_id": session_id,
            "method": method, "path": path,
            "params": params, "json": json, "timeout": timeout,
        })
        return Response(
            content=b'{"stdout":"ok\\n","stderr":"","exit_code":0}',
            status_code=200,
            media_type="application/json",
        )

    monkeypatch.setattr(srv, "_proxy_from_session", fake_proxy_from_session)

    r = await _post_exec("echo ok")

    assert r.status_code == 200, r.text
    assert r.json()["stdout"] == "ok\n"
    assert r.json()["stdout_truncated"] is False
    assert r.json()["stderr_truncated"] is False
    assert r.json()["timed_out"] is False
    assert calls == [{
        "kind": "proxy",
        "session_id": "sess-1",
        "method": "POST",
        "path": "/v1/exec",
        "params": None,
        "json": {"command": "echo ok", "timeout": 5},
        "timeout": 10,
    }]


# test_resolve_session_instance_uses_session_agent_type_and_spawn_env
# was removed: it tested the legacy ``ensure_sandbox`` / agent-config /
# spawn-env wiring inside ``_resolve_session_instance``, all of which is
# gone. The pool now resolves the session's compute directly via
# ``pool.get_session(session_id)`` and the session row owns env/secrets,
# so there's no wiring left at this layer to validate.


@pytest.mark.asyncio
async def test_exec_in_instance_local_runs_with_instance_root(monkeypatch, tmp_path):
    """Local direct exec must run in the sandbox root, not the API process cwd."""
    instance = providers.ProviderInstance(
        provider="unix_local", url="", root=str(tmp_path), sandbox_ref="local-ref",
    )
    captured: dict = {}

    async def fake_create_subprocess_shell(cmd, **kwargs):
        captured["cmd"] = cmd
        captured["kwargs"] = kwargs
        return _FakeProc()

    monkeypatch.setattr(providers.asyncio, "create_subprocess_shell", fake_create_subprocess_shell)

    result = await providers.exec_in_instance(instance, "pwd")

    assert result.stdout == "ok\n"
    assert captured["cmd"] == "pwd"
    assert captured["kwargs"].get("cwd") == str(tmp_path)


@pytest.mark.asyncio
async def test_exec_in_instance_docker_uses_sandbox_id_as_container_fallback(monkeypatch):
    """Docker direct exec can run from a DB-derived instance."""
    instance = providers.ProviderInstance(
        provider="docker", url="", root="/home/agent", sandbox_ref="container-abc123",
    )
    captured: dict = {}

    async def fake_create_subprocess_exec(*args, **kwargs):
        captured["args"] = args
        captured["kwargs"] = kwargs
        return _FakeProc()

    monkeypatch.setattr(providers.shutil, "which", lambda name: "/usr/bin/docker" if name == "docker" else None)
    monkeypatch.setattr(providers.asyncio, "create_subprocess_exec", fake_create_subprocess_exec)

    result = await providers.exec_in_instance(instance, "echo ok")

    assert result.stdout == "ok\n"
    assert captured["args"][:4] == ("/usr/bin/docker", "exec", "container-abc123", "sh")
    assert captured["args"][4:] == ("-c", "echo ok")


@pytest.mark.asyncio
async def test_exec_in_instance_daytona_preserves_exit_code_and_stderr(monkeypatch):
    """Daytona exec must surface real exit_code/stderr for shell-based file ops."""
    from unittest.mock import AsyncMock

    instance = providers.ProviderInstance(
        provider="daytona", url="", root="/home/daytona", sandbox_ref="sb-daytona-1",
    )

    class _FakeProcess:
        @staticmethod
        async def exec(_cmd, timeout=30):
            return SimpleNamespace(result="out", stderr="bad", exit_code=17)

    class _FakeSandbox:
        process = _FakeProcess()

    fake_client = SimpleNamespace(get=AsyncMock(return_value=_FakeSandbox()))

    async def _passthrough(awaitable, timeout=None):
        return await awaitable

    from api.providers import daytona as _dprov
    monkeypatch.setattr(
        _dprov, "_get_async_daytona_client",
        AsyncMock(return_value=fake_client),
    )
    monkeypatch.setattr(providers.asyncio, "wait_for", _passthrough)

    result = await providers.exec_in_instance(instance, "exit 17")

    assert result.stdout == "out"
    assert result.stderr == "bad"
    assert result.exit_code == 17
