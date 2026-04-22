"""Integration tests for the Docker provider.

Skip entirely if the ``docker`` CLI is not on PATH or the daemon is
unreachable.  These tests do NOT mock — they spin real containers.  Named
volumes are prefixed ``agentsdk-test-<uuid>`` and cleaned up in try/finally.
"""
from __future__ import annotations

import asyncio
import os
import subprocess
import sys
import uuid
from pathlib import Path

import pytest

_SRC = os.path.join(os.path.dirname(__file__), "..", "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

from api.providers import docker as dprov  # noqa: E402
from api.providers._shared import ProviderInstance  # noqa: E402


def _docker_available() -> bool:
    from shutil import which
    if not which("docker"):
        return False
    try:
        rc = subprocess.run(
            ["docker", "info"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            timeout=5,
        ).returncode
        return rc == 0
    except Exception:
        return False


pytestmark = pytest.mark.skipif(
    not _docker_available(),
    reason="docker CLI not available / daemon unreachable",
)


def _vol_name() -> str:
    return f"agentsdk-test-{uuid.uuid4().hex[:10]}"


# ---------------------------------------------------------------------------
# 3.1 + 3.2: volume CRUD
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_create_volume_creates_layout():
    name = _vol_name()
    try:
        ref = await dprov.create_volume(name)
        assert ref == name
        # Layout dirs should exist via `find`.
        tree = await dprov.volume_tree(ref, "")
        # find -type f returns files only; no files yet, but `find` should succeed.
        assert isinstance(tree, str)
        # The shared + system/supervisor dirs exist — confirm via a write-then-read roundtrip.
        await dprov.volume_write(ref, "shared/ping.txt", b"pong")
        got = await dprov.volume_read(ref, "shared/ping.txt")
        assert got == b"pong"
    finally:
        await dprov.delete_volume(name)


@pytest.mark.asyncio
async def test_delete_volume_tolerates_missing():
    name = _vol_name()
    # Deleting a never-created volume should not raise.
    await dprov.delete_volume(name)


@pytest.mark.asyncio
async def test_delete_volume_raises_when_in_use():
    name = _vol_name()
    container_id = None
    try:
        await dprov.create_volume(name)
        # Attach the volume to a long-running container so delete fails.
        proc = subprocess.run(
            [
                "docker", "run", "-d",
                "--mount", f"type=volume,source={name},target=/v",
                dprov._UTIL_IMAGE, "sh", "-c", "sleep 60",
            ],
            capture_output=True, text=True, timeout=60,
        )
        assert proc.returncode == 0, proc.stderr
        container_id = proc.stdout.strip()
        with pytest.raises(RuntimeError):
            await dprov.delete_volume(name)
    finally:
        if container_id:
            subprocess.run(
                ["docker", "rm", "-f", container_id],
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=30,
            )
        await dprov.delete_volume(name)


# ---------------------------------------------------------------------------
# 3.7: volume file-ops
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_volume_tree_read_write_roundtrip():
    name = _vol_name()
    try:
        await dprov.create_volume(name)
        await dprov.volume_write(name, "a/b/c.txt", b"hello")
        await dprov.volume_write(name, "a/b/d.bin", b"\x00\x01\x02\xff\xfe")
        tree = await dprov.volume_tree(name, "a")
        files = [ln for ln in tree.splitlines() if ln.strip()]
        assert any(f.endswith("/a/b/c.txt") for f in files), files
        assert any(f.endswith("/a/b/d.bin") for f in files), files
        assert await dprov.volume_read(name, "a/b/c.txt") == b"hello"
        assert await dprov.volume_read(name, "a/b/d.bin") == b"\x00\x01\x02\xff\xfe"
    finally:
        await dprov.delete_volume(name)


@pytest.mark.asyncio
async def test_volume_read_rejects_traversal():
    name = _vol_name()
    try:
        await dprov.create_volume(name)
        with pytest.raises(ValueError):
            await dprov.volume_read(name, "../etc/passwd")
        with pytest.raises(ValueError):
            await dprov.volume_write(name, "a/../../b", b"x")
    finally:
        await dprov.delete_volume(name)


@pytest.mark.asyncio
async def test_volume_read_missing_file_raises():
    name = _vol_name()
    try:
        await dprov.create_volume(name)
        with pytest.raises(FileNotFoundError):
            await dprov.volume_read(name, "does/not/exist.txt")
    finally:
        await dprov.delete_volume(name)


# ---------------------------------------------------------------------------
# 3.4-3.6, 3.8, 3.9: sandbox lifecycle with a *fake* supervisor
# ---------------------------------------------------------------------------
#
# A real supervisor install pulls tens of megabytes of npm packages per test
# — too slow and network-dependent for integration coverage.  Instead we
# seed the volume with a tiny Node HTTP server that impersonates the
# supervisor's /v1/health endpoint.  That covers every sandbox-lifecycle
# code path (mounts, port alloc, inspect, stop/start/destroy) without
# requiring npm install.

_FAKE_SUPERVISOR_JS = r"""
const http = require('http');
const fs = require('fs');
const path = require('path');
// Parse --port and --root from argv for parity with the real supervisor.
const args = process.argv.slice(2);
let port = 9100;
let root = '/tmp';
for (let i = 0; i < args.length; i++) {
    if (args[i] === '--port') port = parseInt(args[++i], 10);
    if (args[i] === '--root') root = args[++i];
}
const server = http.createServer((req, res) => {
    if (req.url === '/v1/health') {
        res.writeHead(200, {'Content-Type': 'application/json'});
        res.end(JSON.stringify({ok: true, root}));
        return;
    }
    if (req.url === '/v1/echo') {
        // Record the request in the agent-home so we can prove the mount worked.
        try {
            fs.mkdirSync(root, {recursive: true});
            fs.appendFileSync(path.join(root, 'messages.log'), 'ping\n');
        } catch (e) {}
        res.writeHead(200);
        res.end('pong');
        return;
    }
    res.writeHead(404);
    res.end();
});
server.listen(port, '0.0.0.0', () => {
    console.log('fake-supervisor listening on', port);
});
"""


async def _seed_fake_supervisor(volume_ref: str) -> None:
    """Write a stand-in supervisor.js into <vol>/system/supervisor/ and stub
    node_modules/.bin/claude-agent-acp so the docker.create_sandbox command
    line doesn't need a real npm install.
    """
    await dprov.volume_write(
        volume_ref, "system/supervisor/supervisor.js",
        _FAKE_SUPERVISOR_JS.encode(),
    )
    # Stub ACP binary — the fake supervisor never execs it, but the sandbox
    # command line references the path; create an empty executable so the
    # shell doesn't complain if something resolves it.
    await dprov.volume_write(
        volume_ref, "system/supervisor/node_modules/.bin/claude-agent-acp",
        b"#!/bin/sh\necho fake-acp\n",
    )


@pytest.mark.asyncio
async def test_sandbox_create_inspect_destroy():
    name = _vol_name()
    inst: ProviderInstance | None = None
    try:
        await dprov.create_volume(name)
        await _seed_fake_supervisor(name)
        subpath = "agents/test-agent/home"
        inst = await dprov.create_sandbox(
            volume_ref=name, subpath=subpath, agent_type="claude",
        )
        assert inst.container_id
        assert inst.url.startswith("http://localhost:")
        assert inst.port is not None

        # Status: running
        status = await dprov.get_sandbox_status(inst.container_id)
        assert status == "running", f"expected running, got {status!r}"

        # ensure_supervisor_url is a no-op returning inst.url
        url = await dprov.ensure_supervisor_url(inst, agent_type="claude")
        assert url == inst.url

        # Hit the fake /v1/echo to cause a write into the mounted agent-home.
        import httpx
        async with httpx.AsyncClient(timeout=5) as c:
            r = await c.get(f"{inst.url}/v1/echo")
            assert r.status_code == 200

        # The write should have landed on the volume at <subpath>/messages.log.
        got = await dprov.volume_read(name, f"{subpath}/messages.log")
        assert got == b"ping\n"

        # Destroy removes the container and port should be released.
        await dprov.destroy_sandbox(inst)
        status = await dprov.get_sandbox_status(inst.container_id or "")
        assert status == "missing"
        inst = None
    finally:
        if inst is not None:
            try:
                await dprov.destroy_sandbox(inst)
            except Exception:
                pass
        await dprov.delete_volume(name)


@pytest.mark.asyncio
async def test_sandbox_stop_start_resume():
    name = _vol_name()
    inst: ProviderInstance | None = None
    try:
        await dprov.create_volume(name)
        await _seed_fake_supervisor(name)
        subpath = "agents/resume-agent/home"
        inst = await dprov.create_sandbox(
            volume_ref=name, subpath=subpath, agent_type="claude",
        )
        # Stop (not destroy)
        await dprov.stop_sandbox(inst)
        status = await dprov.get_sandbox_status(inst.container_id)
        assert status == "stopped", f"expected stopped, got {status!r}"

        # Resume
        await dprov.start_sandbox(inst.container_id)
        # Give the fake node process a moment to re-bind.
        for _ in range(20):
            if (await dprov.get_sandbox_status(inst.container_id)) == "running":
                break
            await asyncio.sleep(0.25)
        assert await dprov.get_sandbox_status(inst.container_id) == "running"
    finally:
        if inst is not None:
            try:
                await dprov.destroy_sandbox(inst)
            except Exception:
                pass
        await dprov.delete_volume(name)


@pytest.mark.asyncio
async def test_destroy_recreate_same_subpath_preserves_files():
    """Destroying and recreating a sandbox with the same subpath must
    preserve the agent-home contents (the point of decoupled volumes)."""
    name = _vol_name()
    first: ProviderInstance | None = None
    second: ProviderInstance | None = None
    try:
        await dprov.create_volume(name)
        await _seed_fake_supervisor(name)
        subpath = "agents/persist-agent/home"

        first = await dprov.create_sandbox(
            volume_ref=name, subpath=subpath, agent_type="claude",
        )
        import httpx
        async with httpx.AsyncClient(timeout=5) as c:
            await c.get(f"{first.url}/v1/echo")

        # First write should be visible.
        got = await dprov.volume_read(name, f"{subpath}/messages.log")
        assert got == b"ping\n"

        await dprov.destroy_sandbox(first)
        first = None

        # Recreate on the same subpath.
        second = await dprov.create_sandbox(
            volume_ref=name, subpath=subpath, agent_type="claude",
        )
        async with httpx.AsyncClient(timeout=5) as c:
            await c.get(f"{second.url}/v1/echo")

        # Previous + new writes both present.
        got = await dprov.volume_read(name, f"{subpath}/messages.log")
        assert got == b"ping\nping\n", got
    finally:
        for inst in (first, second):
            if inst is not None:
                try:
                    await dprov.destroy_sandbox(inst)
                except Exception:
                    pass
        await dprov.delete_volume(name)


# ---------------------------------------------------------------------------
# 3.3: install_supervisor (real npm install — slower, but gated by marker)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
@pytest.mark.skipif(
    os.environ.get("AGENT_SDK_SKIP_SLOW_DOCKER_TESTS") == "1",
    reason="slow npm-install test skipped (AGENT_SDK_SKIP_SLOW_DOCKER_TESTS=1)",
)
async def test_install_supervisor_populates_system_supervisor():
    """Runs a real ``npm install`` inside node:20-slim. ~30-90s; skip with env."""
    name = _vol_name()
    try:
        await dprov.create_volume(name)
        await dprov.install_supervisor(name, agent_type="claude")
        tree = await dprov.volume_tree(name, "system/supervisor")
        files = tree.splitlines()
        # supervisor.js copied.
        assert any(f.endswith("system/supervisor/supervisor.js") for f in files), (
            "expected supervisor.js in " + str(files)[:500]
        )
        # package.json created by `npm init`.
        assert any(f.endswith("system/supervisor/package.json") for f in files), (
            "expected package.json in " + str(files)[:500]
        )
        # Claude ACP binary installed.
        assert any("claude-agent-acp" in f for f in files), (
            "expected claude-agent-acp bin in " + str(files)[:500]
        )
    finally:
        await dprov.delete_volume(name)
