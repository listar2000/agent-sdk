"""Integration tests for the Local provider (Phase 4).

Exercises volume CRUD, supervisor install, sandbox lifecycle, file ops,
and the realpath-containment check that keeps symlinks from escaping
``<volume>``.

All tests are hermetic: ``AGENT_SDK_LOCAL_VOL_ROOT`` is pointed at a
per-test ``tmp_path`` so nothing leaks into ``~/.agent-sdk/volumes/``.

Skipped when ``npm`` / ``node`` are unavailable — we cannot install a
supervisor or spawn one without them.
"""
from __future__ import annotations

import asyncio
import os
import shutil
import sys
import uuid
from pathlib import Path

import httpx
import pytest

_SRC = os.path.join(os.path.dirname(__file__), "..", "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

pytestmark = pytest.mark.skipif(
    shutil.which("npm") is None or shutil.which("node") is None,
    reason="npm and node required for Local provider integration tests",
)


def _vol_name() -> str:
    return f"vol-{uuid.uuid4().hex[:8]}"


# ---------------------------------------------------------------------------
# Volume CRUD
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_create_volume_makes_dirs_and_returns_path(tmp_path, monkeypatch):
    monkeypatch.setenv("AGENT_SDK_LOCAL_VOL_ROOT", str(tmp_path))
    from api.providers import local

    name = _vol_name()
    ref = await local.create_volume(name)

    assert ref == str((tmp_path / name).resolve())
    assert (tmp_path / name / "shared").is_dir()
    assert (tmp_path / name / "system" / "supervisor").is_dir()


@pytest.mark.asyncio
async def test_create_volume_is_idempotent(tmp_path, monkeypatch):
    monkeypatch.setenv("AGENT_SDK_LOCAL_VOL_ROOT", str(tmp_path))
    from api.providers import local

    name = _vol_name()
    ref1 = await local.create_volume(name)
    ref2 = await local.create_volume(name)
    assert ref1 == ref2
    assert (tmp_path / name / "shared").is_dir()


@pytest.mark.asyncio
async def test_delete_volume_removes_dir(tmp_path, monkeypatch):
    monkeypatch.setenv("AGENT_SDK_LOCAL_VOL_ROOT", str(tmp_path))
    from api.providers import local

    name = _vol_name()
    ref = await local.create_volume(name)
    assert os.path.isdir(ref)

    await local.delete_volume(ref)
    assert not os.path.exists(ref)


@pytest.mark.asyncio
async def test_delete_volume_tolerates_missing(tmp_path, monkeypatch):
    monkeypatch.setenv("AGENT_SDK_LOCAL_VOL_ROOT", str(tmp_path))
    from api.providers import local

    missing = str(tmp_path / "never-existed")
    # Must not raise.
    await local.delete_volume(missing)


# ---------------------------------------------------------------------------
# Supervisor install
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_install_supervisor_populates_volume(tmp_path, monkeypatch):
    monkeypatch.setenv("AGENT_SDK_LOCAL_VOL_ROOT", str(tmp_path))
    from api.providers import local

    name = _vol_name()
    ref = await local.create_volume(name)

    await local.install_supervisor(ref, "claude")

    sup = Path(ref) / "system" / "supervisor"
    assert (sup / "supervisor.js").is_file()
    assert (sup / "package.json").is_file()
    assert (sup / "node_modules").is_dir()
    # ACP binary is the whole point of the install — must be executable.
    acp_bin = sup / "node_modules" / ".bin" / "claude-agent-acp"
    assert acp_bin.exists()


# ---------------------------------------------------------------------------
# Sandbox lifecycle
# ---------------------------------------------------------------------------

async def _fetch_health(url: str) -> httpx.Response:
    async with httpx.AsyncClient(timeout=5) as c:
        return await c.get(f"{url}/v1/health")


@pytest.mark.asyncio
async def test_sandbox_create_health_destroy(tmp_path, monkeypatch):
    monkeypatch.setenv("AGENT_SDK_LOCAL_VOL_ROOT", str(tmp_path))
    from api.providers import local

    name = _vol_name()
    ref = await local.create_volume(name)
    await local.install_supervisor(ref, "claude")

    inst = await local.create_sandbox(
        volume_ref=ref,
        subpath="agents/a1/home",
        agent_type="claude",
    )

    try:
        assert inst.url.startswith("http://127.0.0.1:")
        assert inst.sandbox_id is not None
        pid = int(inst.sandbox_id)
        assert pid in local._PROCESSES

        # Per-sandbox HOME exists on the volume.
        assert (Path(ref) / "agents" / "a1" / "home").is_dir()

        # Health endpoint responds 200.
        r = await _fetch_health(inst.url)
        assert r.status_code == 200, r.text

        # Status reports running.
        status = await local.get_sandbox_status(inst.sandbox_id)
        assert status == "running"
    finally:
        await local.destroy_sandbox(inst)

    # After destroy: no live process, registry cleared.
    assert pid not in local._PROCESSES
    status = await local.get_sandbox_status(str(pid))
    assert status == "missing"

    # Port on URL is no longer accepting connections — give the kernel a
    # moment, then a connection attempt should fail.
    await asyncio.sleep(0.2)
    with pytest.raises((httpx.ConnectError, httpx.ReadError)):
        async with httpx.AsyncClient(timeout=1) as c:
            await c.get(f"{inst.url}/v1/health")


@pytest.mark.asyncio
async def test_ensure_supervisor_url_returns_same_url(tmp_path, monkeypatch):
    monkeypatch.setenv("AGENT_SDK_LOCAL_VOL_ROOT", str(tmp_path))
    from api.providers import local

    name = _vol_name()
    ref = await local.create_volume(name)
    await local.install_supervisor(ref, "claude")

    inst = await local.create_sandbox(
        volume_ref=ref, subpath="agents/e/home", agent_type="claude",
    )
    try:
        got = await local.ensure_supervisor_url(inst, agent_type="claude")
        assert got == inst.url
    finally:
        await local.destroy_sandbox(inst)


# ---------------------------------------------------------------------------
# Volume file ops
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_volume_read_write_roundtrip(tmp_path, monkeypatch):
    monkeypatch.setenv("AGENT_SDK_LOCAL_VOL_ROOT", str(tmp_path))
    from api.providers import local

    name = _vol_name()
    ref = await local.create_volume(name)

    await local.volume_write(ref, "shared/greeting.txt", b"hello\n")
    got = await local.volume_read(ref, "shared/greeting.txt")
    assert got == b"hello\n"


@pytest.mark.asyncio
async def test_volume_tree_lists_entries(tmp_path, monkeypatch):
    monkeypatch.setenv("AGENT_SDK_LOCAL_VOL_ROOT", str(tmp_path))
    from api.providers import local

    name = _vol_name()
    ref = await local.create_volume(name)
    await local.volume_write(ref, "shared/a.txt", b"A")
    await local.volume_write(ref, "shared/sub/b.txt", b"B")

    tree = await local.volume_tree(ref, "shared")
    entries = set(tree.splitlines())
    assert "shared/a.txt" in entries
    assert "shared/sub/" in entries
    assert "shared/sub/b.txt" in entries


@pytest.mark.asyncio
async def test_volume_read_rejects_symlink_escape(tmp_path, monkeypatch):
    """A symlink inside the volume pointing to /etc/passwd must not be
    readable via volume_read — realpath containment check rejects it."""
    monkeypatch.setenv("AGENT_SDK_LOCAL_VOL_ROOT", str(tmp_path))
    from api.providers import local

    name = _vol_name()
    ref = await local.create_volume(name)

    evil = Path(ref) / "shared" / "evil"
    os.symlink("/etc/passwd", evil)

    with pytest.raises(ValueError, match="escapes volume root"):
        await local.volume_read(ref, "shared/evil")


@pytest.mark.asyncio
async def test_volume_write_rejects_symlink_escape(tmp_path, monkeypatch):
    """Writing through a symlink that points outside the volume is rejected."""
    monkeypatch.setenv("AGENT_SDK_LOCAL_VOL_ROOT", str(tmp_path))
    from api.providers import local

    name = _vol_name()
    ref = await local.create_volume(name)

    outside = tmp_path / "outside.txt"
    outside.write_text("original")

    link = Path(ref) / "shared" / "escape"
    os.symlink(str(outside), link)

    with pytest.raises(ValueError, match="escapes volume root"):
        await local.volume_write(ref, "shared/escape", b"pwned")

    # Outside file must be untouched.
    assert outside.read_text() == "original"


@pytest.mark.asyncio
async def test_volume_read_rejects_dotdot(tmp_path, monkeypatch):
    monkeypatch.setenv("AGENT_SDK_LOCAL_VOL_ROOT", str(tmp_path))
    from api.providers import local

    name = _vol_name()
    ref = await local.create_volume(name)

    with pytest.raises(ValueError, match="escapes volume root"):
        await local.volume_read(ref, "../../../../etc/passwd")
