"""Tests for the snapshot-on-stop refactor.

Scope: the server-side helpers ``snapshot_supervisor`` and
``snapshot_and_stop``, and the wired stop paths that now route through them.

Tests use a real supervisor subprocess with a dummy ACP (/bin/cat) so they
don't require ANTHROPIC_API_KEY or the real claude-agent-acp binary. That
matches ``tests/test_supervisor_snapshot_endpoint.py``'s pattern.
"""
from __future__ import annotations

import os
import shutil
import socket
import subprocess
import sys
import tarfile
import time
from pathlib import Path

import httpx
import pytest

_SRC = os.path.join(os.path.dirname(__file__), "..", "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

from api.models import SandboxRecord


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
def live_supervisor(tmp_path):
    """Spawn supervisor.js with a dummy ACP and snapshot-path configured."""
    if shutil.which("node") is None:
        pytest.skip("node not on PATH")

    port = _free_port()
    root = tmp_path / "root"
    root.mkdir()
    (root / "marker.txt").write_text("live-supervisor")
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
        yield type("LiveSup", (), {
            "url": url, "port": port, "root": root,
            "snapshot_path": snap_path,
        })
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=3)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=2)


@pytest.mark.asyncio
async def test_snapshot_supervisor_writes_tarball(live_supervisor):
    """snapshot_supervisor(sandbox) must land the tarball on the volume."""
    from api.server import snapshot_supervisor

    sb = SandboxRecord(
        id="sb_test", provider="local", sandbox_ref="fake",
        status="running", root=str(live_supervisor.root),
        listen_port=live_supervisor.port,
    )
    await snapshot_supervisor(sb)

    assert live_supervisor.snapshot_path.exists(), "tarball should be written"
    with tarfile.open(live_supervisor.snapshot_path) as tf:
        names = set(tf.getnames())
    assert any(n.endswith("marker.txt") for n in names), (
        f"marker.txt missing: {sorted(names)}"
    )


@pytest.mark.asyncio
async def test_snapshot_supervisor_swallows_unreachable(caplog):
    """Pointing at a dead port must not raise — log and return."""
    import logging
    from api.server import snapshot_supervisor

    # Grab a port that's guaranteed to be unused.
    port = _free_port()
    sb = SandboxRecord(
        id="sb_dead", provider="local", sandbox_ref="fake",
        status="running", root="/tmp", listen_port=port,
    )
    with caplog.at_level(logging.WARNING):
        await snapshot_supervisor(sb)  # must not raise
    assert any("snapshot_supervisor" in r.message for r in caplog.records)


@pytest.mark.asyncio
async def test_snapshot_supervisor_no_url_skips(caplog):
    """Port-less port-based sandbox with no _INSTANCES cache → log + return."""
    import logging
    from api.server import snapshot_supervisor

    sb = SandboxRecord(
        id="sb_no_url", provider="daytona", sandbox_ref="fake",
        status="running", root="/tmp", listen_port=None,
    )
    with caplog.at_level(logging.WARNING):
        await snapshot_supervisor(sb)  # must not raise
    assert any("no URL" in r.message for r in caplog.records)
