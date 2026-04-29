"""Contracts for POST /sessions/{id}/sandbox/exec.

These tests are intentionally provider-aware.  The route promises to execute
inside the session's sandbox, not on the API host and not through an incomplete
ProviderInstance reconstructed from the DB row.
"""
from __future__ import annotations

import os
import sys

import pytest
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
async def test_session_sandbox_exec_local_runs_with_sandbox_root(monkeypatch, tmp_path):
    """Local exec must run in the sandbox root, not the API process cwd."""
    sandbox = SandboxRecord(
        id="sb-local",
        provider="local",
        sandbox_ref="local-ref",
        status="running",
        root=str(tmp_path),
        volume_id="vol-local",
        subpath="agents/a1",
    )

    async def fake_ensure_session_live(session_id: str):
        return {"id": session_id}, sandbox, object()

    captured: dict = {}

    async def fake_create_subprocess_shell(cmd, **kwargs):
        captured["cmd"] = cmd
        captured["kwargs"] = kwargs
        return _FakeProc()

    monkeypatch.setattr(srv, "ensure_session_live", fake_ensure_session_live)
    monkeypatch.setattr(providers.asyncio, "create_subprocess_shell", fake_create_subprocess_shell)

    r = await _post_exec("pwd")

    assert r.status_code == 200, r.text
    assert captured["cmd"] == "pwd"
    assert captured["kwargs"].get("cwd") == str(tmp_path)


@pytest.mark.asyncio
async def test_session_sandbox_exec_docker_uses_container_ref(monkeypatch):
    """Docker exec must receive the session sandbox's container id/ref."""
    sandbox = SandboxRecord(
        id="sb-docker",
        provider="docker",
        sandbox_ref="container-abc123",
        status="running",
        root="/home/agent",
        volume_id="vol-docker",
        subpath="agents/a1",
    )

    async def fake_ensure_session_live(session_id: str):
        return {"id": session_id}, sandbox, object()

    captured: dict = {}

    async def fake_create_subprocess_exec(*args, **kwargs):
        captured["args"] = args
        captured["kwargs"] = kwargs
        return _FakeProc()

    monkeypatch.setattr(srv, "ensure_session_live", fake_ensure_session_live)
    monkeypatch.setattr(providers.shutil, "which", lambda name: "/usr/bin/docker" if name == "docker" else None)
    monkeypatch.setattr(providers.asyncio, "create_subprocess_exec", fake_create_subprocess_exec)

    r = await _post_exec("echo ok")

    assert r.status_code == 200, r.text
    assert captured["args"][:4] == ("/usr/bin/docker", "exec", "container-abc123", "sh")
    assert captured["args"][4:] == ("-c", "echo ok")
