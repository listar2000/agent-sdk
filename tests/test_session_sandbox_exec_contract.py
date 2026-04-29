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
from api.models import SandboxRecord  # noqa: E402


class _FakeProc:
    returncode = 0

    async def communicate(self):
        return b"ok\n", b""

    def kill(self):
        self.returncode = -9


@pytest.fixture(autouse=True)
def _clear_runtime_state():
    srv.SESSIONS.clear()
    srv._INSTANCES.clear()
    yield
    srv.SESSIONS.clear()
    srv._INSTANCES.clear()


async def _post_exec(command: str = "pwd"):
    transport = ASGITransport(app=srv.app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        return await c.post(
            "/sessions/sess-1/sandbox/exec",
            json={"command": command, "timeout": 5},
        )


@pytest.mark.asyncio
async def test_session_sandbox_exec_proxies_to_session_supervisor(monkeypatch):
    """The route delegates to the shared session supervisor proxy."""
    instance = providers.ProviderInstance(
        provider="local", url="http://sandbox.fake", root="/tmp/sandbox",
    )
    calls: list[dict] = []

    async def fake_resolve_session_instance(session_id: str):
        calls.append({"kind": "resolve", "session_id": session_id})
        return instance

    async def fake_proxy_instance(inst, method, path, *, params=None, json=None, timeout=30):
        calls.append({
            "kind": "proxy", "instance": inst, "method": method, "path": path,
            "params": params, "json": json, "timeout": timeout,
        })
        return Response(
            content=b'{"stdout":"ok\\n","stderr":"","exit_code":0}',
            status_code=200,
            media_type="application/json",
        )

    monkeypatch.setattr(srv, "_resolve_session_instance", fake_resolve_session_instance)
    monkeypatch.setattr(srv, "_proxy_instance", fake_proxy_instance)

    r = await _post_exec("echo ok")

    assert r.status_code == 200, r.text
    assert r.json()["stdout"] == "ok\n"
    assert r.json()["stdout_truncated"] is False
    assert r.json()["stderr_truncated"] is False
    assert r.json()["timed_out"] is False
    assert calls[0] == {"kind": "resolve", "session_id": "sess-1"}
    assert calls[1] == {
        "kind": "proxy",
        "instance": instance,
        "method": "POST",
        "path": "/v1/exec",
        "params": None,
        "json": {"command": "echo ok", "timeout": 5},
        "timeout": 10,
    }


@pytest.mark.asyncio
async def test_resolve_session_instance_uses_session_agent_type_and_spawn_env(monkeypatch):
    """Session-scoped sandbox recovery must not hard-code claude/default env."""
    session = {
        "id": "sess-1",
        "agent_id": "agent-1",
        "env": {"VISIBLE": "yes"},
        "secrets": {"TOKEN": "secret"},
    }
    sandbox = SandboxRecord(
        id="sb-1",
        provider="docker",
        sandbox_ref="container-abc123",
        status="running",
        root="/home/agent",
        volume_id="vol-1",
        subpath="agents/a1",
    )
    captured: dict = {}

    async def fake_require_session_row(session_id: str):
        assert session_id == "sess-1"
        return session

    async def fake_ensure_sandbox(row):
        assert row is session
        return sandbox

    async def fake_get_agent(agent_id: str):
        assert agent_id == "agent-1"
        return SimpleNamespace(config=SimpleNamespace(agent_type="codex"))

    async def fake_resolve_sandbox_instance(sandbox_id: str, **kwargs):
        captured["sandbox_id"] = sandbox_id
        captured.update(kwargs)
        return providers.ProviderInstance(
            provider="docker", url="http://sandbox.fake", root="/home/agent",
        )

    monkeypatch.setattr(srv, "_require_session_row", fake_require_session_row)
    monkeypatch.setattr(srv, "ensure_sandbox", fake_ensure_sandbox)
    monkeypatch.setattr(srv, "get_agent", fake_get_agent)
    monkeypatch.setattr(srv, "_resolve_sandbox_instance", fake_resolve_sandbox_instance)

    instance = await srv._resolve_session_instance("sess-1")

    assert instance.url == "http://sandbox.fake"
    assert captured == {
        "sandbox_id": "sb-1",
        "agent_type": "codex",
        "spawn_env": {"VISIBLE": "yes", "TOKEN": "secret"},
    }


@pytest.mark.asyncio
async def test_exec_in_instance_local_runs_with_instance_root(monkeypatch, tmp_path):
    """Local direct exec must run in the sandbox root, not the API process cwd."""
    instance = providers.ProviderInstance(
        provider="local", url="", root=str(tmp_path), sandbox_id="local-ref",
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
        provider="docker", url="", root="/home/agent", sandbox_id="container-abc123",
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
    instance = providers.ProviderInstance(
        provider="daytona", url="", root="/home/daytona", sandbox_id="sb-daytona-1",
    )

    class _FakeLoop:
        async def run_in_executor(self, _executor, fn):
            return fn()

    class _FakeSandbox:
        class process:
            @staticmethod
            def exec(_cmd, timeout=30):
                return SimpleNamespace(result="out", stderr="bad", exit_code=17)

    class _FakeClient:
        @staticmethod
        def get(_sandbox_id):
            return _FakeSandbox()

    async def _passthrough(awaitable, timeout=None):
        return await awaitable

    monkeypatch.setattr(providers, "_get_daytona_client", lambda: _FakeClient())
    monkeypatch.setattr(providers.asyncio, "get_running_loop", lambda: _FakeLoop())
    monkeypatch.setattr(providers.asyncio, "wait_for", _passthrough)

    result = await providers.exec_in_instance(instance, "exit 17")

    assert result.stdout == "out"
    assert result.stderr == "bad"
    assert result.exit_code == 17
