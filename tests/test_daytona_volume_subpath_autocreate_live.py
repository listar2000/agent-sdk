"""LIVE: daytona auto-creates volume mount SUBPATHS, so pre-mkdir is redundant.

create_daytona_volume eagerly spins up a whole 1-shot sandbox
(_init_volume_dirs) just to ``mkdir -p /v/shared /v/system/supervisor`` on
every new volume — a full daytona create (the queue-bound bottleneck) per
session bootstrap. But:

  * the MAIN session mount uses ``subpath=agents/<id>`` which _init_volume_dirs
    never creates — yet sessions work, so daytona must create subpaths on mount;
  * ``system/supervisor/`` has zero consumers in the codebase.

This probe confirms it directly: mount a brand-new volume at a never-created
main subpath AND a never-created ``shared/<name>`` subpath, and verify both are
writable. If so, _init_volume_dirs is dead weight and can be removed (one fewer
daytona create per bootstrap). It also stands as a guard for that assumption.

Live daytona; skips without creds. Slow. Self-cleans.
"""
from __future__ import annotations

import os
import uuid

import pytest


pytestmark = [
    pytest.mark.skipif(not os.environ.get("DAYTONA_API_KEY"),
                       reason="DAYTONA_API_KEY required"),
    pytest.mark.asyncio,
]


async def test_daytona_mount_subpaths_are_autocreated():
    from api.providers.daytona import _get_async_daytona_client, _sandbox_labels
    from daytona_sdk import (
        CreateSandboxFromSnapshotParams, VolumeMount,
    )

    daytona = await _get_async_daytona_client()
    snap = open(".runtime-snapshot-tag").read().strip()
    vname = f"subpath-probe-{uuid.uuid4().hex[:8]}"
    sid = uuid.uuid4().hex[:8]

    # Raw create — NO _init_volume_dirs. Hydrate + wait ready.
    vol = await daytona.volume.get(vname, True)
    from daytona_api_client_async import VolumesApi
    from daytona_api_client.models import VolumeState
    vapi = VolumesApi(daytona._api_client)
    import asyncio
    deadline = asyncio.get_running_loop().time() + 120
    while True:
        dto = await vapi.get_volume(vol.id)
        st = dto.state.value if hasattr(dto.state, "value") else str(dto.state)
        if st == VolumeState.READY:
            break
        if asyncio.get_running_loop().time() > deadline:
            raise TimeoutError(f"volume not ready: {st}")
        await asyncio.sleep(2)

    sandbox = None
    try:
        # Mount at a never-created main subpath AND a never-created shared subpath.
        mounts = [
            VolumeMount(volume_id=vol.id, mount_path="/vol", subpath=f"agents/{sid}"),
            VolumeMount(volume_id=vol.id, mount_path="/mnt/probe", subpath="shared/probe"),
        ]
        sandbox = await daytona.create(
            CreateSandboxFromSnapshotParams(
                snapshot=snap, auto_stop_interval=0,
                labels=_sandbox_labels(), volumes=mounts),
            timeout=300)

        async def _ex(cmd):
            r = await sandbox.process.exec(cmd, timeout=30)
            return r.exit_code, (r.result or r.stdout or "")

        # Both mountpoints must exist and be writable WITHOUT any pre-mkdir.
        rc_main, _ = await _ex("sh -c 'echo hi > /vol/probe.txt && cat /vol/probe.txt'")
        rc_shared, _ = await _ex("sh -c 'echo hi > /mnt/probe/probe.txt && cat /mnt/probe/probe.txt'")

        assert rc_main == 0, (
            "main subpath mount (agents/<id>, never pre-created) was not writable "
            "— daytona does NOT auto-create subpaths; _init_volume_dirs is load-bearing")
        assert rc_shared == 0, (
            "shared subpath mount (shared/probe, never pre-created) was not writable "
            "— _init_volume_dirs's shared/ pre-mkdir IS load-bearing, keep it")
    finally:
        if sandbox is not None:
            try:
                await daytona.delete(sandbox)
            except Exception:
                pass
        try:
            await vapi.delete_volume(vol.id)
        except Exception:
            pass
