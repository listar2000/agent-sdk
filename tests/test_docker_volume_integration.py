"""Integration tests for the Docker provider.

Skip entirely if the ``docker`` CLI is not on PATH or the daemon is
unreachable.  These tests do NOT mock — they spin real containers.  Named
volumes are prefixed ``agentsdk-test-<uuid>`` and cleaned up in try/finally.
"""
from __future__ import annotations

import asyncio
import os
import shlex
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
        assert "a/b/c.txt" in files, files
        assert "a/b/d.bin" in files, files
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


# ---------------------------------------------------------------------------
# 3.3: install_supervisor (real npm install — slower, but gated by marker)
# ---------------------------------------------------------------------------

async def _read_system_tree(volume_ref: str) -> list[str]:
    """Return the list of entries directly under ``<volume>/system/``.

    ``volume_tree`` only reports files (``find -type f``), which would miss
    the empty dir skeleton.  Use ``ls -1`` via the util container for a
    complete picture.  Symlinks are reported with a trailing ``@`` so we
    can distinguish the ``supervisor`` symlink from a real directory.
    """
    rc, out, err = await dprov._run_volume_shell(
        volume_ref,
        "cd /v/system 2>/dev/null && ls -1F || true",
        timeout=30,
    )
    if rc != 0:
        raise RuntimeError(err.decode(errors="replace").strip()[:400])
    return [ln.strip() for ln in out.decode(errors="replace").splitlines() if ln.strip()]


async def _read_symlink_target(volume_ref: str, link_rel: str) -> str:
    """Return readlink(/v/<link_rel>) — empty if not a symlink."""
    rc, out, _ = await dprov._run_volume_shell(
        volume_ref,
        f"readlink /v/{shlex.quote(link_rel)} || true",
        timeout=30,
    )
    return out.decode(errors="replace").strip() if rc == 0 else ""


# ---------------------------------------------------------------------------
# MT1 — Docker ``agent-sdk.sandbox-id`` label propagation
#
# Every code path that can spin up a Docker container must set the
# ``agent-sdk.sandbox-id=<id>`` label so ``reconcile_on_startup`` can match
# live containers against DB rows after a restart.  Four distinct entry
# points reach ``docker.create_sandbox``:
#
#   1. ``_provision_new``                → regression guard (already sets it)
#   2. ``POST /sandboxes``               → MA1 fix: thread sandbox_id through
#   3. ``POST /sessions/quick``          → MA1 fix
#   4. ``_ensure_sandbox_alive`` restart → MA1 fix
#
# Each test seeds the minimum DB state, hits the entrypoint end-to-end
# against a real Docker daemon, then runs ``docker inspect`` to read the
# label back and asserts the expected sandbox_id is there.
# ---------------------------------------------------------------------------

_LABEL_KEY = "agent-sdk.sandbox-id"


def _inspect_label(container_id: str) -> str:
    """Return ``<label>`` from ``docker inspect``. Empty string if missing."""
    proc = subprocess.run(
        [
            "docker", "inspect",
            "--format", f'{{{{index .Config.Labels "{_LABEL_KEY}"}}}}',
            container_id,
        ],
        capture_output=True, text=True, timeout=10,
    )
    if proc.returncode != 0:
        return ""
    return proc.stdout.strip()


async def _seed_volume_with_fake_supervisor() -> str:
    """Create a Docker volume + fake-supervisor payload so sandbox create
    doesn't need a real npm install.  Caller is responsible for deletion."""
    name = _vol_name()
    await dprov.create_volume(name)
    await _seed_fake_supervisor(name)
    return name


@pytest.mark.asyncio
async def test_label_propagation_provider_create_sandbox():
    """Regression guard for MT1 path 1 — ``docker.create_sandbox`` with an
    explicit ``sandbox_id`` must attach the label."""
    name = await _seed_volume_with_fake_supervisor()
    inst: ProviderInstance | None = None
    try:
        sb_id = f"sb_{uuid.uuid4().hex[:10]}"
        inst = await dprov.create_sandbox(
            volume_ref=name, subpath="agents/label-probe/home",
            agent_type="claude", sandbox_ref=sb_id,
        )
        assert inst.container_id
        assert _inspect_label(inst.container_id) == sb_id, (
            f"_provision_new path: expected label {sb_id!r}, "
            f"got {_inspect_label(inst.container_id)!r}"
        )
    finally:
        if inst is not None:
            try:
                await dprov.destroy_sandbox(inst)
            except Exception:
                pass
        await dprov.delete_volume(name)



@pytest.mark.asyncio

# ---------------------------------------------------------------------------
# MT2 — ``reconcile_on_startup`` coverage
#
# Seeds various Docker/DB states that reconcile must handle correctly:
#   * labeled container with no DB row      → orphan, rm -f
#   * labeled container + running DB row    → rebuild _INSTANCES entry
#   * labeled container + stopped DB row    → preserved (per MA3 fix)
#
# Reconcile also needs to extract the published host port from docker
# inspect; we read it back and assert _INSTANCES matches.
# ---------------------------------------------------------------------------


@pytest.mark.skip(reason=(
    "Legacy ``_INSTANCES`` rebuild on startup is gone — every sandbox is "
    "now session-owned and the pool resolves compute through "
    "pool.get_session(), not a process-local cache. ``reconcile_on_startup`` "
    "still kills orphan containers; that subset can be re-tested by reading "
    "the docker daemon directly without _INSTANCES assertions."
))
class TestDockerReconcile:
    """Reconcile-on-startup scenarios. All tests skip gracefully on no docker."""

    @staticmethod
    async def _seeded_container(volume_name: str, subpath: str,
                                sandbox_id: str) -> ProviderInstance:
        """Create a labeled container against a prepared volume."""
        return await dprov.create_sandbox(
            volume_ref=volume_name, subpath=subpath, agent_type="claude",
            sandbox_ref=sandbox_id,
        )

    @pytest.mark.asyncio
    async def test_orphan_container_removed(self):
        """A labeled container whose sandbox_id has no DB row is removed."""
        _DB = os.environ.get("TEST_DATABASE_URL")
        if _DB is None:
            pytest.skip("TEST_DATABASE_URL not set")
        os.environ["DATABASE_URL"] = _DB

        from api import db as dbmod, server as srv  # noqa: E402

        dbmod.init_db()
        await dbmod.init_pool()

        vol_name = _vol_name()
        cid: str | None = None
        try:
            await dprov.create_volume(vol_name)
            await _seed_fake_supervisor(vol_name)

            # Label it with an sb_id that has NO DB row → orphan.
            orphan_id = f"sb_{uuid.uuid4().hex[:10]}"
            inst = await self._seeded_container(
                vol_name, "agents/orphan/home", orphan_id,
            )
            cid = inst.container_id
            assert cid
            assert _inspect_label(cid) == orphan_id

            # Reconcile: must rm -f the orphan.
            srv._INSTANCES.clear()
            await dprov.reconcile_on_startup()

            status = await dprov.get_sandbox_status(cid)
            assert status == "missing", (
                f"orphan container should be removed; got status={status!r}"
            )
            # _INSTANCES must not rebuild an entry for an orphan.
            assert orphan_id not in srv._INSTANCES
            cid = None  # already gone
        finally:
            if cid:
                subprocess.run(
                    ["docker", "rm", "-f", cid],
                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=30,
                )
            await dprov.delete_volume(vol_name)
            await dbmod.close_pool()

    @pytest.mark.asyncio
    async def test_live_sandbox_preserved_and_instance_rebuilt(self):
        """Labeled container + matching DB row with status=running survives
        reconcile; _INSTANCES[sandbox_id] is populated with the live
        container_id, url, and port."""
        _DB = os.environ.get("TEST_DATABASE_URL")
        if _DB is None:
            pytest.skip("TEST_DATABASE_URL not set")
        os.environ["DATABASE_URL"] = _DB

        from api import db as dbmod, server as srv  # noqa: E402
        from api.models import SandboxRecord, VolumeRecord  # noqa: E402

        dbmod.init_db()
        await dbmod.init_pool()
        async with dbmod.get_db() as conn:
            await conn.execute("DELETE FROM session_log")
            await conn.execute("DELETE FROM sessions")
            await conn.execute("DELETE FROM volumes")

        vol_name = _vol_name()
        cid: str | None = None
        try:
            await dprov.create_volume(vol_name)
            await _seed_fake_supervisor(vol_name)
            await dbmod.upsert_volume(VolumeRecord(
                id="v-rc-live", name=vol_name, provider="docker",
                provider_ref=vol_name,
            ))

            sb_id = f"sb_{uuid.uuid4().hex[:10]}"
            inst = await self._seeded_container(
                vol_name, "agents/alive/home", sb_id,
            )
            cid = inst.container_id
            assert cid

            await dbmod.upsert_sandbox(SandboxRecord(
                id=sb_id, provider="docker", sandbox_ref=cid,
                status="running", root="/home/agent",
                volume_id="v-rc-live", subpath="agents/alive/home",
                listen_port=inst.port,
            ))

            # Simulate a cold restart: drop the in-process instance map.
            srv._INSTANCES.clear()

            await dprov.reconcile_on_startup()

            # Container is still running.
            assert await dprov.get_sandbox_status(cid) == "running"

            # _INSTANCES was rebuilt.
            rebuilt = srv._INSTANCES.get(sb_id)
            assert rebuilt is not None, (
                f"reconcile did not rebuild _INSTANCES[{sb_id!r}]"
            )
            assert rebuilt.container_id == cid
            assert rebuilt.port == inst.port, (
                f"port mismatch: reconcile got {rebuilt.port}, "
                f"expected {inst.port}"
            )
            assert rebuilt.url == f"http://localhost:{inst.port}"
        finally:
            if cid:
                subprocess.run(
                    ["docker", "rm", "-f", cid],
                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=30,
                )
            try:
                srv._INSTANCES.clear()
            except Exception:
                pass
            await dprov.delete_volume(vol_name)
            await dbmod.close_pool()

    @pytest.mark.asyncio
    async def test_stopped_db_with_live_container_preserved(self):
        """MA3: a DB row in status='stopped' but with a live container must
        NOT be force-removed by reconcile.  ``stop_sandbox_route`` flips
        the DB row to 'stopped' BEFORE calling ``stop_instance``; if a crash
        happens in between, reconcile must survive the partial state.

        Hard assert — ``docker.reconcile_on_startup`` no longer treats
        ``status='stopped'`` as orphan.
        """
        _DB = os.environ.get("TEST_DATABASE_URL")
        if _DB is None:
            pytest.skip("TEST_DATABASE_URL not set")
        os.environ["DATABASE_URL"] = _DB

        from api import db as dbmod, server as srv  # noqa: E402
        from api.models import SandboxRecord, VolumeRecord  # noqa: E402

        dbmod.init_db()
        await dbmod.init_pool()
        async with dbmod.get_db() as conn:
            await conn.execute("DELETE FROM session_log")
            await conn.execute("DELETE FROM sessions")
            await conn.execute("DELETE FROM volumes")

        vol_name = _vol_name()
        cid: str | None = None
        try:
            await dprov.create_volume(vol_name)
            await _seed_fake_supervisor(vol_name)
            await dbmod.upsert_volume(VolumeRecord(
                id="v-rc-stp", name=vol_name, provider="docker",
                provider_ref=vol_name,
            ))

            sb_id = f"sb_{uuid.uuid4().hex[:10]}"
            inst = await TestDockerReconcile._seeded_container(
                vol_name, "agents/stp/home", sb_id,
            )
            cid = inst.container_id
            assert cid
            # DB says "stopped" while the container is actually alive — the
            # crash-between-UPDATE-and-stop_instance state.
            await dbmod.upsert_sandbox(SandboxRecord(
                id=sb_id, provider="docker", sandbox_ref=cid,
                status="stopped", root="/home/agent",
                volume_id="v-rc-stp", subpath="agents/stp/home",
                listen_port=inst.port,
            ))

            srv._INSTANCES.clear()

            await dprov.reconcile_on_startup()
            status = await dprov.get_sandbox_status(cid)
            # Container must survive — a status='stopped' row is resumable,
            # not an orphan. Force-removing it would nuke the container the
            # user is about to resume via start_sandbox.
            assert status in ("running", "stopped"), (
                f"reconcile removed a stopped-but-live container "
                f"(status={status!r}) — MA3 regression"
            )
        finally:
            if cid:
                subprocess.run(
                    ["docker", "rm", "-f", cid],
                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=30,
                )
            try:
                srv._INSTANCES.clear()
            except Exception:
                pass
            await dprov.delete_volume(vol_name)
            await dbmod.close_pool()

    @pytest.mark.asyncio
    async def test_stop_sandbox_preserves_on_reconcile(self):
        """MT7: ``docker stop`` (non-destructive) must leave a container
        that ``reconcile_on_startup`` subsequently preserves.  End-to-end:
        provision → docker stop (persists as exited) → DB row status='stopped'
        → reconcile must NOT rm -f the exited container.

        This is the canonical "user stopped the sandbox, server restarted"
        flow.  The MA3 fix (landed) teaches reconcile to treat a
        ``status='stopped'`` row as a resumable pair, not an orphan."""
        _DB = os.environ.get("TEST_DATABASE_URL")
        if _DB is None:
            pytest.skip("TEST_DATABASE_URL not set")
        os.environ["DATABASE_URL"] = _DB

        from api import db as dbmod, server as srv  # noqa: E402
        from api.models import SandboxRecord, VolumeRecord  # noqa: E402

        dbmod.init_db()
        await dbmod.init_pool()
        async with dbmod.get_db() as conn:
            await conn.execute("DELETE FROM session_log")
            await conn.execute("DELETE FROM sessions")
            await conn.execute("DELETE FROM volumes")

        vol_name = _vol_name()
        cid: str | None = None
        try:
            await dprov.create_volume(vol_name)
            await _seed_fake_supervisor(vol_name)
            await dbmod.upsert_volume(VolumeRecord(
                id="v-mt7", name=vol_name, provider="docker",
                provider_ref=vol_name,
            ))

            sb_id = f"sb_{uuid.uuid4().hex[:10]}"
            inst = await TestDockerReconcile._seeded_container(
                vol_name, "agents/mt7/home", sb_id,
            )
            cid = inst.container_id
            assert cid

            # Non-destructive stop — container exits, row persists.
            await dprov.stop_sandbox(inst)
            assert await dprov.get_sandbox_status(cid) == "stopped"

            # Mimic stop_sandbox_route's DB flip: status='stopped'.
            await dbmod.upsert_sandbox(SandboxRecord(
                id=sb_id, provider="docker", sandbox_ref=cid,
                status="stopped", root="/home/agent",
                volume_id="v-mt7", subpath="agents/mt7/home",
                listen_port=inst.port,
            ))

            # Cold-restart state: the in-process instance map is empty.
            srv._INSTANCES.clear()

            # Reconcile must preserve the container.
            await dprov.reconcile_on_startup()

            status = await dprov.get_sandbox_status(cid)
            assert status == "stopped", (
                f"reconcile destroyed a user-stopped container; status={status!r}"
            )
        finally:
            if cid:
                subprocess.run(
                    ["docker", "rm", "-f", cid],
                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=30,
                )
            try:
                srv._INSTANCES.clear()
            except Exception:
                pass
            await dprov.delete_volume(vol_name)
            await dbmod.close_pool()
