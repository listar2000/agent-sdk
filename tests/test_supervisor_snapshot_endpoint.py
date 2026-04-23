"""Router-level smoke tests for supervisor.js HTTP endpoints.

Spawns the supervisor with a dummy ACP (`/bin/cat`) so we can exercise the
HTTP router without needing an ANTHROPIC_API_KEY or the real
claude-agent-acp binary. The dummy ACP just has to exist so supervisor.js
doesn't exit at startup; we never send real JSON-RPC through it.

Scope: pure router wiring — does the route match? does the handler respond?
The richer "snapshot actually tars the workspace" path is exercised by the
daytona-backed integration tests.
"""
from __future__ import annotations

import os
import shutil
import socket
import subprocess
import time

import httpx
import pytest


_SUP_JS = os.path.join(
    os.path.dirname(__file__), "..", "src", "supervisor", "supervisor.js"
)


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _wait_for_health(url: str, timeout: float = 5.0) -> None:
    deadline = time.time() + timeout
    last_err: Exception | None = None
    while time.time() < deadline:
        try:
            r = httpx.get(f"{url}/v1/health", timeout=0.5)
            if r.status_code == 200:
                return
        except Exception as e:
            last_err = e
        time.sleep(0.05)
    raise RuntimeError(f"supervisor didn't come up on {url}: {last_err}")


@pytest.fixture
def supervisor_proc(tmp_path):
    """Spawn supervisor.js with a dummy ACP and yield its base URL.

    Cleanup terminates the process regardless of test outcome.
    """
    if shutil.which("node") is None:
        pytest.skip("node not on PATH")

    port = _free_port()
    root = tmp_path / "root"
    root.mkdir()

    proc = subprocess.Popen(
        ["node", _SUP_JS,
         "--acp", "/bin/cat",
         "--root", str(root),
         "--port", str(port)],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    )
    url = f"http://127.0.0.1:{port}"
    try:
        _wait_for_health(url)
        yield url, root
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=3)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=2)


def test_post_v1_snapshot_returns_200_with_no_snapshot_path(supervisor_proc):
    """With no --snapshot-path, runSnapshotOnce is a no-op but the endpoint
    must still respond 200 so server-side snapshot_supervisor calls don't
    spuriously fail on volume-less sandboxes."""
    url, _ = supervisor_proc
    r = httpx.post(f"{url}/v1/snapshot", timeout=5)
    assert r.status_code == 200, f"got {r.status_code}: {r.text}"
    assert r.json().get("ok") is True


def test_post_v1_snapshot_writes_tarball_when_snapshot_path_set(tmp_path):
    """With --snapshot-path, the endpoint must produce a valid tarball on
    the volume containing files from the root."""
    import tarfile

    if shutil.which("node") is None:
        pytest.skip("node not on PATH")

    port = _free_port()
    root = tmp_path / "root"
    root.mkdir()
    (root / "marker.txt").write_text("hello")

    snap_path = tmp_path / "snapshot.tar"

    proc = subprocess.Popen(
        ["node", _SUP_JS,
         "--acp", "/bin/cat",
         "--root", str(root),
         "--port", str(port),
         "--snapshot-path", str(snap_path)],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    )
    url = f"http://127.0.0.1:{port}"
    try:
        _wait_for_health(url)
        r = httpx.post(f"{url}/v1/snapshot", timeout=15)
        assert r.status_code == 200, f"got {r.status_code}: {r.text}"
        assert snap_path.exists(), "tarball should exist at snapshot-path"
        with tarfile.open(snap_path) as tf:
            names = set(tf.getnames())
        assert any(n.endswith("marker.txt") for n in names), (
            f"marker.txt missing from tarball: {sorted(names)}"
        )
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=3)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=2)
