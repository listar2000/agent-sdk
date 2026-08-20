"""Credential-cache bootstrap contract for the ACP supervisor."""
from __future__ import annotations

import json
import os
import shutil
import socket
import stat
import subprocess
import time

import httpx
import pytest


_SUP_JS = os.path.join(
    os.path.dirname(__file__), "..", "src", "supervisor", "supervisor.js"
)


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def _start_supervisor(root, auth_cache: dict) -> tuple[subprocess.Popen, str]:
    port = _free_port()
    env = dict(os.environ)
    env["CODEX_AUTH_JSON"] = json.dumps(auth_cache)
    proc = subprocess.Popen(
        [
            "node", _SUP_JS,
            "--acp", "/bin/cat",
            "--root", str(root),
            "--port", str(port),
        ],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    url = f"http://127.0.0.1:{port}"
    deadline = time.time() + 5
    while time.time() < deadline:
        try:
            if httpx.get(f"{url}/v1/health", timeout=0.5).status_code == 200:
                return proc, url
        except Exception:
            pass
        if proc.poll() is not None:
            stderr = proc.stderr.read().decode(errors="replace")
            raise RuntimeError(f"supervisor exited early: {stderr}")
        time.sleep(0.05)
    proc.terminate()
    raise RuntimeError("supervisor did not become healthy")


def _stop(proc: subprocess.Popen) -> None:
    proc.terminate()
    try:
        proc.wait(timeout=3)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=2)


@pytest.mark.skipif(shutil.which("node") is None, reason="node not on PATH")
def test_codex_auth_cache_is_materialized_and_removed_from_child_env(tmp_path):
    root = tmp_path / "root"
    root.mkdir()
    cache = {"tokens": {"access_token": "test-token", "refresh_token": "refresh"}}

    proc, url = _start_supervisor(root, cache)
    try:
        target = root / ".codex" / "auth.json"
        assert json.loads(target.read_text()) == cache
        assert stat.S_IMODE(target.stat().st_mode) == 0o600
        assert stat.S_IMODE(target.parent.stat().st_mode) == 0o700

        result = httpx.post(
            f"{url}/v1/exec",
            json={"command": "test -z \"${CODEX_AUTH_JSON+x}\""},
            timeout=5,
        )
        result.raise_for_status()
        assert result.json()["exit_code"] == 0
    finally:
        _stop(proc)


@pytest.mark.skipif(shutil.which("node") is None, reason="node not on PATH")
def test_unchanged_bootstrap_does_not_overwrite_runtime_refreshed_cache(tmp_path):
    root = tmp_path / "root"
    root.mkdir()
    initial = {"tokens": {"access_token": "initial"}}

    first, _ = _start_supervisor(root, initial)
    _stop(first)
    target = root / ".codex" / "auth.json"
    refreshed = {"tokens": {"access_token": "refreshed"}}
    target.write_text(json.dumps(refreshed))
    target.chmod(0o600)

    second, _ = _start_supervisor(root, initial)
    try:
        assert json.loads(target.read_text()) == refreshed
    finally:
        _stop(second)


@pytest.mark.skipif(shutil.which("node") is None, reason="node not on PATH")
def test_changed_bootstrap_replaces_existing_cache(tmp_path):
    root = tmp_path / "root"
    root.mkdir()
    initial = {"tokens": {"access_token": "initial"}}
    replacement = {"tokens": {"access_token": "replacement"}}

    first, _ = _start_supervisor(root, initial)
    _stop(first)
    second, _ = _start_supervisor(root, replacement)
    try:
        target = root / ".codex" / "auth.json"
        assert json.loads(target.read_text()) == replacement
    finally:
        _stop(second)
