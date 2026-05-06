"""Cleanup-path correctness for the ``unix_local`` provider.

The on-disk ``<vol>/system/sandboxes/<ref>.json`` marker is the source
of truth for a unix_local supervisor's lifecycle. ``destroy_sandbox``
must derive PID + marker path from it (mirroring how docker derives
container state from the daemon, daytona from the control plane,
modal from Modal's API) — never from the in-memory ``_PROCESSES``
cache, which doesn't survive a server restart.

These tests pin both shapes:

  1. Cross-restart: marker on disk, ``_PROCESSES`` empty.
     Reproduces the original leak (315 stale markers + orphan PIDs
     observed locally) — destroy must read the marker, kill the PID,
     and unlink the marker.

  2. Same-lifetime happy path: marker on disk, ``_PROCESSES`` populated.
     Destroy must still kill, unlink, and reap the zombie. Pins that
     the cross-restart fix doesn't regress current-lifetime behavior.
"""
from __future__ import annotations

import asyncio
import json
import os
import subprocess
import sys
import time
from dataclasses import asdict
from pathlib import Path

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from api.providers import unix_local as lp
from api.providers._shared import ProviderInstance


def _spawn_dummy_supervisor() -> subprocess.Popen:
    """Long-lived stand-in for ``supervisor.js`` — cleanup only cares
    about PID + signal semantics, not supervisor internals.
    """
    return subprocess.Popen(
        [sys.executable, "-c", "import time; time.sleep(120)"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def _wait_for_exit(proc: subprocess.Popen, timeout: float = 10.0) -> bool:
    """Reap the child + return True if it terminated, False on timeout.
    Reaping is a no-op for processes whose parent is init in production
    (server restart reparented the supervisor) — there pytest never sees
    them, so this helper exists for the in-process test scenario.
    """
    try:
        proc.wait(timeout=timeout)
        return True
    except subprocess.TimeoutExpired:
        return False


@pytest.fixture
def isolated_vol_root(tmp_path, monkeypatch):
    monkeypatch.setenv("AGENT_SDK_LOCAL_VOL_ROOT", str(tmp_path))
    saved_procs = dict(lp._PROCESSES)
    lp._PROCESSES.clear()
    # Redirect the global ref→record index away from $HOME so the test
    # doesn't read or scribble on the developer's real
    # ``~/.agent-sdk/sandbox-markers/``.
    monkeypatch.setattr(lp, "_index_dir", lambda: tmp_path / "_index")
    try:
        yield tmp_path
    finally:
        lp._PROCESSES.clear()
        lp._PROCESSES.update(saved_procs)


def _write_marker(vol_root: Path, volume_name: str, ref: str, pid: int) -> Path:
    marker_dir = vol_root / volume_name / "system" / "sandboxes"
    marker_dir.mkdir(parents=True, exist_ok=True)
    marker = marker_dir / f"{ref}.json"
    record = lp._SandboxRecord(
        ref=ref, pid=pid, port=0,
        node="", supervisor_js="", acp_bin="",
    )
    marker.write_text(json.dumps(asdict(record)))
    return marker


@pytest.mark.asyncio
async def test_destroy_kills_orphan_pid_when_in_memory_state_lost(
    isolated_vol_root,
):
    """Cross-restart: dev server restarted between create and DELETE.
    ``_PROCESSES`` is empty; the marker on disk is the only handle to
    the supervisor. ``destroy_sandbox`` must read it, kill the PID,
    unlink the marker.
    """
    proc = _spawn_dummy_supervisor()
    try:
        ref = "local-deadbeef0000"
        marker = _write_marker(isolated_vol_root, "test-vol", ref, proc.pid)

        await lp.destroy_sandbox(ProviderInstance(
            provider="unix_local", url="", sandbox_ref=ref,
        ))

        assert not marker.exists(), (
            f"marker {marker.name} survived destroy — same leak we saw locally"
        )
        assert _wait_for_exit(proc), (
            f"orphan pid {proc.pid} survived destroy — DELETE silently "
            f"dropped the session row but left the supervisor running"
        )
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait(timeout=5)


@pytest.mark.asyncio
async def test_destroy_finds_marker_outside_vol_root_via_global_index(
    isolated_vol_root, tmp_path,
):
    """Volume sits OUTSIDE the active ``AGENT_SDK_LOCAL_VOL_ROOT`` —
    e.g. the DB row was written by a prior server boot whose env var
    pointed elsewhere (a unit test's pytest tmp dir, a different user
    layout). Without the global index, ``_load_record`` globs only
    ``_vol_root()`` and silently returns ``(None, None)`` →
    ``destroy_sandbox`` no-ops the kill (record is None) and the
    supervisor leaks. ``start_sandbox`` recovery falls through to
    ``create_sandbox`` and produces a NEW ``sandbox_ref``, which broke
    ``test_stop_sandbox_same_sandbox_after_restart[unix_local]`` under
    -n auto.
    """
    proc = _spawn_dummy_supervisor()
    try:
        ref = "local-feedface0002"

        # Marker lives at a volume path that ``_vol_root()`` (= isolated_vol_root)
        # cannot reach — outside the glob's search scope on purpose.
        outside_vol = tmp_path.parent / "outside" / "default-unix_local"
        marker_dir = outside_vol / "system" / "sandboxes"
        marker_dir.mkdir(parents=True, exist_ok=True)
        marker = marker_dir / f"{ref}.json"
        record = lp._SandboxRecord(
            ref=ref, pid=proc.pid, port=0,
            node="", supervisor_js="", acp_bin="",
        )
        # ``_write_record`` writes both the per-volume marker (here) and
        # the global index (under tmp_path/_index, see fixture).
        lp._write_record(marker, record)

        # Sanity: the legacy glob path can't see this marker, so the fix
        # is doing real work — recovery only succeeds via the global index.
        import glob
        assert not glob.glob(str(isolated_vol_root / "*" / "system" / "sandboxes" / f"{ref}.json"))

        await lp.destroy_sandbox(ProviderInstance(
            provider="unix_local", url="", sandbox_ref=ref,
        ))

        assert _wait_for_exit(proc), (
            f"orphan pid {proc.pid} survived destroy — global index lookup "
            f"didn't find the marker, so SIGTERM was never sent"
        )
        assert not marker.exists(), (
            f"per-volume marker {marker} survived destroy — _clear_record "
            f"only removed the global index entry, not the on-disk marker"
        )
        assert not (tmp_path / "_index" / f"{ref}.json").exists(), (
            "global index entry survived destroy"
        )
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait(timeout=5)


@pytest.mark.asyncio
async def test_destroy_same_lifetime_path(isolated_vol_root):
    """Happy path: same server lifetime that spawned the supervisor.
    ``_PROCESSES`` has the Popen handle; destroy still kills, unlinks,
    and reaps. Guards against the cross-restart fix regressing this.
    """
    proc = _spawn_dummy_supervisor()
    try:
        ref = "local-cafebabe0001"
        marker = _write_marker(isolated_vol_root, "test-vol", ref, proc.pid)
        async with lp._PROCESSES_LOCK:
            lp._PROCESSES[ref] = proc

        await lp.destroy_sandbox(ProviderInstance(
            provider="unix_local", url="", sandbox_ref=ref,
        ))

        assert not marker.exists()
        assert _wait_for_exit(proc)
        assert ref not in lp._PROCESSES
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait(timeout=5)
