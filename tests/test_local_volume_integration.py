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
    from api.providers import unix_local as local

    name = _vol_name()
    ref = await local.create_volume(name)

    assert ref == str((tmp_path / name).resolve())
    assert (tmp_path / name / "shared").is_dir()
    assert (tmp_path / name / "system" / "supervisor").is_dir()


@pytest.mark.asyncio
async def test_create_volume_is_idempotent(tmp_path, monkeypatch):
    monkeypatch.setenv("AGENT_SDK_LOCAL_VOL_ROOT", str(tmp_path))
    from api.providers import unix_local as local

    name = _vol_name()
    ref1 = await local.create_volume(name)
    ref2 = await local.create_volume(name)
    assert ref1 == ref2
    assert (tmp_path / name / "shared").is_dir()


@pytest.mark.asyncio
async def test_delete_volume_removes_dir(tmp_path, monkeypatch):
    monkeypatch.setenv("AGENT_SDK_LOCAL_VOL_ROOT", str(tmp_path))
    from api.providers import unix_local as local

    name = _vol_name()
    ref = await local.create_volume(name)
    assert os.path.isdir(ref)

    await local.delete_volume(ref)
    assert not os.path.exists(ref)


@pytest.mark.asyncio
async def test_delete_volume_tolerates_missing(tmp_path, monkeypatch):
    monkeypatch.setenv("AGENT_SDK_LOCAL_VOL_ROOT", str(tmp_path))
    from api.providers import unix_local as local

    missing = str(tmp_path / "never-existed")
    # Must not raise.
    await local.delete_volume(missing)


# ``test_install_supervisor_populates_volume`` and
# ``test_install_supervisor_is_cumulative_across_agent_types`` were deleted
# in the runtime-image-unification refactor. The supervisor + ACP bins
# now ship in the image at ``/opt/agent-sdk/runtime/``; no per-volume install
# happens, so there's nothing to populate or to stay cumulative.


# ---------------------------------------------------------------------------
# Phase B: image-runtime feature flag
#
# These tests pin the parity-first behavior of ``AGENT_SDK_USE_IMAGE_RUNTIME``:
# when the flag is set, ``create_sandbox`` resolves supervisor.js + ACP bin
# from the runtime path; when unset, it uses the legacy volume-side install.
# Both paths must remain functional through Phase D; Phase E deletes the
# legacy branch and these tests collapse to a single happy-path check.
#
# The tests intercept ``subprocess.Popen`` so the supervisor never actually
# spawns — we just want to confirm the argv carries the right paths.
# ---------------------------------------------------------------------------


class _CapturingPopen:
    """Stand-in for ``subprocess.Popen`` that records argv and pretends to
    be a live process. ``create_sandbox`` polls /v1/health afterwards; we
    short-circuit by returning a process whose ``poll()`` claims it died,
    so create_sandbox raises during the health check — but argv has already
    been captured by then, which is what we're asserting on."""

    captured: list[list[str]] = []

    def __init__(self, args, **kwargs):
        type(self).captured.append(list(args))
        # Simulate immediate exit so the surrounding wait-for-health loop
        # doesn't hang indefinitely. The test catches the resulting raise.
        self.returncode = 1
        self._stderr_data = b"capturing-popen: simulated exit\n"

    def poll(self):
        return self.returncode

    def kill(self):
        pass

    def wait(self, timeout=None):
        return self.returncode

    @property
    def stderr(self):
        class _F:
            def read(_self):
                return b"capturing-popen: simulated exit\n"
        return _F()


@pytest.mark.asyncio
async def test_create_sandbox_uses_image_runtime_when_flag_set(
    tmp_path, monkeypatch,
):
    """With ``AGENT_SDK_USE_IMAGE_RUNTIME=1`` and ``AGENT_SDK_RUNTIME_PATH``
    pointed at a populated runtime dir, ``create_sandbox`` spawns the
    supervisor with the runtime-path values, NOT the volume-side path."""
    monkeypatch.setenv("AGENT_SDK_LOCAL_VOL_ROOT", str(tmp_path))
    monkeypatch.setenv("AGENT_SDK_USE_IMAGE_RUNTIME", "1")

    # Stub a runtime dir with the sentinels create_sandbox checks for.
    runtime_dir = tmp_path / "runtime"
    bin_dir = runtime_dir / "node_modules" / ".bin"
    bin_dir.mkdir(parents=True)
    (runtime_dir / "supervisor.js").write_text("// stub\n")
    claude_bin = bin_dir / "claude-agent-acp"
    claude_bin.write_text("#!/bin/sh\nexit 0\n")
    claude_bin.chmod(0o755)
    monkeypatch.setenv("AGENT_SDK_RUNTIME_PATH", str(runtime_dir))

    from api.providers import unix_local as local

    name = _vol_name()
    ref = await local.create_volume(name)
    # NOTE: NO install_supervisor call — that's the whole point of this path.

    # Patch Popen ONLY around create_sandbox so install_supervisor (not
    # called here, but kept symmetric with the by_default test) and any
    # other subprocess.run users in the call chain still see real Popen.
    _CapturingPopen.captured.clear()
    monkeypatch.setattr(local.subprocess, "Popen", _CapturingPopen)

    with pytest.raises(Exception):  # _CapturingPopen exits immediately → health check fails
        await local.create_sandbox(
            volume_ref=ref, subpath="agents/a1/home", agent_type="claude",
        )

    assert _CapturingPopen.captured, "supervisor was not spawned"
    argv = _CapturingPopen.captured[0]
    assert str(runtime_dir / "supervisor.js") in argv, (
        f"supervisor argv did not include image-runtime supervisor.js: {argv}"
    )
    assert str(claude_bin) in argv, (
        f"supervisor argv did not include image-runtime claude bin: {argv}"
    )
    # And confirm the legacy volume-side path is NOT in argv.
    legacy_sup = Path(ref) / "system" / "supervisor" / "supervisor.js"
    assert str(legacy_sup) not in argv, (
        f"argv unexpectedly contains the legacy volume path: {argv}"
    )


# test_create_sandbox_uses_volume_runtime_by_default was deleted in Phase E
# of the runtime-image-unification refactor — the legacy volume-install path
# is gone, so there's no flag-off behavior to pin.


# ---------------------------------------------------------------------------
# Sandbox lifecycle
# ---------------------------------------------------------------------------

async def _fetch_health(url: str) -> httpx.Response:
    async with httpx.AsyncClient(timeout=5) as c:
        return await c.get(f"{url}/v1/health")


@pytest.mark.asyncio
async def test_sandbox_create_health_destroy(tmp_path, monkeypatch):
    monkeypatch.setenv("AGENT_SDK_LOCAL_VOL_ROOT", str(tmp_path))
    from api.providers import unix_local as local

    name = _vol_name()
    ref = await local.create_volume(name)
    # Phase E: no install_supervisor — supervisor.js + ACP bins live in the
    # image runtime path. ``_detect_runtime_path()`` falls back to
    # ``<repo>/src/supervisor`` when the image path is absent (CI / dev).

    inst = await local.create_sandbox(
        volume_ref=ref,
        subpath="agents/a1/home",
        agent_type="claude",
    )

    try:
        assert inst.url.startswith("http://127.0.0.1:")
        assert inst.sandbox_ref is not None
        # sandbox_ref is a stable provider ref (``local-<uuid12>``) so the
        # same sandbox can survive an in-place restart with a fresh PID;
        # the _PROCESSES registry is keyed by that ref.
        sandbox_ref = inst.sandbox_ref
        assert sandbox_ref in local._PROCESSES

        # Per-sandbox HOME exists on the volume.
        assert (Path(ref) / "agents" / "a1" / "home").is_dir()

        # Health endpoint responds 200.
        r = await _fetch_health(inst.url)
        assert r.status_code == 200, r.text

        # Status reports running.
        status = await local.get_sandbox_status(inst.sandbox_ref)
        assert status == "running"
    finally:
        await local.destroy_sandbox(inst)

    # After destroy: no live process, registry cleared.
    assert sandbox_ref not in local._PROCESSES
    status = await local.get_sandbox_status(sandbox_ref)
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
    from api.providers import unix_local as local

    name = _vol_name()
    ref = await local.create_volume(name)
    # Phase E: supervisor lives in the image runtime path, no per-volume install.

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
    from api.providers import unix_local as local

    name = _vol_name()
    ref = await local.create_volume(name)

    await local.volume_write(ref, "shared/greeting.txt", b"hello\n")
    got = await local.volume_read(ref, "shared/greeting.txt")
    assert got == b"hello\n"


@pytest.mark.asyncio
async def test_volume_tree_lists_entries(tmp_path, monkeypatch):
    monkeypatch.setenv("AGENT_SDK_LOCAL_VOL_ROOT", str(tmp_path))
    from api.providers import unix_local as local

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
async def test_volume_mkdir_upload_rename_delete(tmp_path, monkeypatch):
    monkeypatch.setenv("AGENT_SDK_LOCAL_VOL_ROOT", str(tmp_path))
    from api.providers import unix_local as local

    name = _vol_name()
    ref = await local.create_volume(name)

    await local.volume_mkdir(ref, "shared/docs")
    assert (Path(ref) / "shared" / "docs").is_dir()

    await local.volume_upload(ref, "shared/docs/a.txt", b"hello")
    assert (Path(ref) / "shared" / "docs" / "a.txt").read_bytes() == b"hello"

    await local.volume_rename(ref, "shared/docs/a.txt", "shared/docs/b.txt")
    assert not (Path(ref) / "shared" / "docs" / "a.txt").exists()
    assert (Path(ref) / "shared" / "docs" / "b.txt").read_bytes() == b"hello"

    await local.volume_upload(ref, "shared/docs/c.txt", b"new")
    await local.volume_rename(ref, "shared/docs/c.txt", "shared/docs/b.txt")
    assert not (Path(ref) / "shared" / "docs" / "c.txt").exists()
    assert (Path(ref) / "shared" / "docs" / "b.txt").read_bytes() == b"new"

    await local.volume_delete(ref, "shared/docs/b.txt")
    assert not (Path(ref) / "shared" / "docs" / "b.txt").exists()

    await local.volume_delete(ref, "shared/docs")
    assert not (Path(ref) / "shared" / "docs").exists()


@pytest.mark.asyncio
async def test_volume_rename_no_overwrite_success_and_collision(tmp_path, monkeypatch):
    monkeypatch.setenv("AGENT_SDK_LOCAL_VOL_ROOT", str(tmp_path))
    from api.providers import VolumeFileExistsError
    from api.providers import unix_local as local

    name = _vol_name()
    ref = await local.create_volume(name)

    await local.volume_upload(ref, "shared/a.txt", b"A")
    await local.volume_rename(ref, "shared/a.txt", "shared/example/a.txt", overwrite=False)
    assert not (Path(ref) / "shared" / "a.txt").exists()
    assert (Path(ref) / "shared" / "example" / "a.txt").read_bytes() == b"A"

    await local.volume_upload(ref, "shared/b.txt", b"B")
    with pytest.raises(VolumeFileExistsError):
        await local.volume_rename(ref, "shared/b.txt", "shared/example/a.txt", overwrite=False)
    assert (Path(ref) / "shared" / "b.txt").read_bytes() == b"B"
    assert (Path(ref) / "shared" / "example" / "a.txt").read_bytes() == b"A"


@pytest.mark.asyncio
async def test_concurrent_volume_rename_no_overwrite_one_wins(tmp_path, monkeypatch):
    monkeypatch.setenv("AGENT_SDK_LOCAL_VOL_ROOT", str(tmp_path))
    from api.providers import VolumeFileExistsError
    from api.providers import unix_local as local

    name = _vol_name()
    ref = await local.create_volume(name)
    await local.volume_upload(ref, "shared/src1.txt", b"one")
    await local.volume_upload(ref, "shared/src2.txt", b"two")

    results = await asyncio.gather(
        local.volume_rename(ref, "shared/src1.txt", "shared/dst.txt", overwrite=False),
        local.volume_rename(ref, "shared/src2.txt", "shared/dst.txt", overwrite=False),
        return_exceptions=True,
    )

    assert sum(result is None for result in results) == 1
    assert sum(isinstance(result, VolumeFileExistsError) for result in results) == 1
    assert (Path(ref) / "shared" / "dst.txt").read_bytes() in {b"one", b"two"}
    remaining_sources = [
        p.read_bytes()
        for p in [Path(ref) / "shared" / "src1.txt", Path(ref) / "shared" / "src2.txt"]
        if p.exists()
    ]
    assert len(remaining_sources) == 1
    assert remaining_sources[0] in {b"one", b"two"}
    assert remaining_sources[0] != (Path(ref) / "shared" / "dst.txt").read_bytes()


@pytest.mark.asyncio
async def test_volume_exists(tmp_path, monkeypatch):
    monkeypatch.setenv("AGENT_SDK_LOCAL_VOL_ROOT", str(tmp_path))
    from api.providers import unix_local as local

    name = _vol_name()
    ref = await local.create_volume(name)
    await local.volume_upload(ref, "shared/exists.txt", b"yes")

    assert await local.volume_exists(ref, "shared/exists.txt") is True
    assert await local.volume_exists(ref, "shared/missing.txt") is False


@pytest.mark.asyncio
async def test_volume_read_rejects_symlink_escape(tmp_path, monkeypatch):
    """A symlink inside the volume pointing to /etc/passwd must not be
    readable via volume_read — realpath containment check rejects it."""
    monkeypatch.setenv("AGENT_SDK_LOCAL_VOL_ROOT", str(tmp_path))
    from api.providers import unix_local as local

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
    from api.providers import unix_local as local

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
    from api.providers import unix_local as local

    name = _vol_name()
    ref = await local.create_volume(name)

    with pytest.raises(ValueError, match="escapes volume root"):
        await local.volume_read(ref, "../../../../etc/passwd")


# ---------------------------------------------------------------------------
# Scenario 10 — volume_read error shapes for invalid path / volume.
# Clear error contract: missing file inside a valid volume raises
# FileNotFoundError (the stdlib type), missing volume root raises
# FileNotFoundError as well (Path doesn't exist → parent_fd open fails),
# and reading a directory path raises IsADirectoryError.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_volume_read_missing_file_raises_filenotfound(tmp_path, monkeypatch):
    monkeypatch.setenv("AGENT_SDK_LOCAL_VOL_ROOT", str(tmp_path))
    from api.providers import unix_local as local

    name = _vol_name()
    ref = await local.create_volume(name)

    with pytest.raises(FileNotFoundError):
        await local.volume_read(ref, "shared/does-not-exist.txt")


@pytest.mark.asyncio
async def test_volume_read_missing_volume_root_raises(tmp_path, monkeypatch):
    """Reading from a volume ref that doesn't exist on disk raises a
    filesystem error — not a silent empty string."""
    monkeypatch.setenv("AGENT_SDK_LOCAL_VOL_ROOT", str(tmp_path))
    from api.providers import unix_local as local

    bogus = str(tmp_path / "no-such-volume-dir")
    # _safe_join resolves the realpath of <bogus>/<path>; since bogus doesn't
    # exist, os.path.realpath returns it unchanged. The subsequent open fails.
    with pytest.raises((FileNotFoundError, OSError)):
        await local.volume_read(bogus, "any/file")


@pytest.mark.asyncio
async def test_volume_read_on_directory_raises(tmp_path, monkeypatch):
    """Reading a path that resolves to a directory is a clear error, not a
    silent empty read. The local provider opens with O_NOFOLLOW|O_RDONLY on
    a regular file descriptor — opening a directory path that way yields
    EISDIR."""
    monkeypatch.setenv("AGENT_SDK_LOCAL_VOL_ROOT", str(tmp_path))
    from api.providers import unix_local as local

    name = _vol_name()
    ref = await local.create_volume(name)

    # "shared" is a directory created by create_volume.
    with pytest.raises((IsADirectoryError, OSError)):
        await local.volume_read(ref, "shared")


@pytest.mark.asyncio
async def test_volume_read_empty_path_raises(tmp_path, monkeypatch):
    """Reading with an empty path targets the volume root (a directory)."""
    monkeypatch.setenv("AGENT_SDK_LOCAL_VOL_ROOT", str(tmp_path))
    from api.providers import unix_local as local

    name = _vol_name()
    ref = await local.create_volume(name)

    # Empty path → _safe_join returns the volume root. _read raises
    # IsADirectoryError because basename is empty OR the open returns EISDIR.
    with pytest.raises((IsADirectoryError, OSError, ValueError)):
        await local.volume_read(ref, "")


@pytest.mark.asyncio
async def test_volume_write_parent_path_through_file_fails(tmp_path, monkeypatch):
    """Writing to a path whose parent is a regular file (not a directory)
    must fail — not silently corrupt the parent."""
    monkeypatch.setenv("AGENT_SDK_LOCAL_VOL_ROOT", str(tmp_path))
    from api.providers import unix_local as local

    name = _vol_name()
    ref = await local.create_volume(name)

    # Create a plain file, then try to write "through" it.
    await local.volume_write(ref, "shared/plain.txt", b"plain")
    with pytest.raises((NotADirectoryError, OSError, FileExistsError)):
        await local.volume_write(ref, "shared/plain.txt/child", b"should fail")
