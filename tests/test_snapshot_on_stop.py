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


# ---------------------------------------------------------------------------
# snapshot_and_stop
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_snapshot_and_stop_orders_snapshot_before_stop(monkeypatch):
    """snapshot_supervisor must be awaited BEFORE stop_instance."""
    from api import server as srv
    from api.providers._shared import ProviderInstance

    order: list[str] = []

    async def fake_snap(sb, *, url=None):
        order.append(f"snapshot:{sb.id}")

    async def fake_stop(inst):
        order.append(f"stop:{inst.sandbox_id}")

    monkeypatch.setattr(srv, "snapshot_supervisor", fake_snap)
    monkeypatch.setattr(srv, "stop_instance", fake_stop)

    sb = SandboxRecord(
        id="sb_test", provider="local", sandbox_ref="fake",
        status="running", root="/tmp", listen_port=12345,
    )
    inst = ProviderInstance(
        provider="local", url="http://localhost:12345",
        root="/tmp", sandbox_id="fake",
    )
    await srv.snapshot_and_stop(sb, inst)
    assert order == ["snapshot:sb_test", "stop:fake"], order


@pytest.mark.asyncio
async def test_snapshot_and_stop_proceeds_when_snapshot_raises(monkeypatch):
    """If snapshot_supervisor raises, stop_instance must still run."""
    from api import server as srv
    from api.providers._shared import ProviderInstance

    called: list[str] = []

    async def boom(_sb, *, url=None):
        raise RuntimeError("S3 is down")

    async def fake_stop(inst):
        called.append("stop")

    monkeypatch.setattr(srv, "snapshot_supervisor", boom)
    monkeypatch.setattr(srv, "stop_instance", fake_stop)

    sb = SandboxRecord(
        id="sb_test", provider="local", sandbox_ref="fake",
        status="running", root="/tmp", listen_port=12345,
    )
    inst = ProviderInstance(
        provider="local", url="http://localhost:12345",
        root="/tmp", sandbox_id="fake",
    )
    await srv.snapshot_and_stop(sb, inst)
    assert called == ["stop"]


# ---------------------------------------------------------------------------
# _reap_one_tick — reap routes idle sandboxes through snapshot_and_stop
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_reap_one_tick_calls_snapshot_and_stop(monkeypatch):
    """When an idle session past IDLE_TIMEOUT_S is reaped and it's the last
    session on the sandbox, snapshot_and_stop must be called."""
    from api import server as srv
    from api.providers._shared import ProviderInstance
    from api.models import SessionState

    calls: list[str] = []

    async def fake_snap_and_stop(sb, inst=None, *, url=None):
        calls.append(f"snap_and_stop:{sb.id}")

    # Avoid DB work — the helper path calls get_sandbox → upsert_sandbox.
    async def fake_get_sandbox(sbid):
        return SandboxRecord(
            id=sbid, provider="local", sandbox_ref="fake",
            status="running", root="/tmp", listen_port=12345,
        )

    async def fake_upsert_sandbox(_rec):
        pass

    async def fake_shutdown_state(state, remove=True, mark_idle_at=None, force=False):
        calls.append(f"shutdown:{state.session_id}")
        if remove:
            srv.SESSIONS.pop(state.session_id, None)

    monkeypatch.setattr(srv, "snapshot_and_stop", fake_snap_and_stop)
    monkeypatch.setattr(srv, "get_sandbox", fake_get_sandbox)
    monkeypatch.setattr(srv, "upsert_sandbox", fake_upsert_sandbox)
    monkeypatch.setattr(srv, "_shutdown_session_state", fake_shutdown_state)

    # Install an idle session.
    sess = SessionState(
        session_id="sess_idle", agent_id="agent_x", sandbox_id="sb_idle",
        last_activity=0.0, turn_completed_at=0.0,
    )
    srv.SESSIONS["sess_idle"] = sess
    srv._INSTANCES["sb_idle"] = ProviderInstance(
        provider="local", url="http://localhost:12345",
        root="/tmp", sandbox_id="fake",
    )
    try:
        # now very far past IDLE_TIMEOUT_S ⇒ reap fires.
        await srv._reap_one_tick(now=srv.IDLE_TIMEOUT_S * 10)
    finally:
        srv.SESSIONS.pop("sess_idle", None)
        srv._INSTANCES.pop("sb_idle", None)

    assert any(c.startswith("snap_and_stop:sb_idle") for c in calls), calls
    # ordering: shutdown session first, then snapshot+stop the sandbox.
    assert calls.index("shutdown:sess_idle") < next(
        i for i, c in enumerate(calls) if c.startswith("snap_and_stop:")
    )
