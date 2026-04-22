"""Integration tests for Daytona volume adapter. Requires DAYTONA_API_KEY."""
from __future__ import annotations
import os, sys
import uuid
import pytest

_SRC = os.path.join(os.path.dirname(__file__), "..", "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

pytestmark = pytest.mark.skipif(
    not os.environ.get("DAYTONA_API_KEY"),
    reason="DAYTONA_API_KEY not set",
)


@pytest.mark.asyncio
async def test_daytona_create_and_delete_volume():
    from api.providers import create_daytona_volume, delete_daytona_volume
    from daytona_api_client.exceptions import ForbiddenException

    name = f"test-vol-agent-sdk-{uuid.uuid4().hex[:8]}"
    ref = await create_daytona_volume(name)
    assert isinstance(ref, str) and len(ref) > 0

    # Delete may be forbidden for the current API key; tolerate 403 but
    # confirm the function reached the DELETE endpoint. Other failures re-raise.
    try:
        await delete_daytona_volume(ref)
    except ForbiddenException:
        pytest.skip("delete not permitted for this API key — create path verified")


@pytest.mark.asyncio
async def test_daytona_sandbox_mounts_volume_subpath():
    """Create a volume, create a sandbox with subpath, write a file, kill
    the sandbox, create a new one with the same subpath, verify file is still there.

    NOTE: Uses provision_daytona_sandbox (not create_daytona) because in the
    test environment ANTHROPIC_API_KEY is not set so the supervisor health check
    inside create_daytona always fails. provision_daytona_sandbox installs deps
    but does NOT start or health-check the supervisor, so exec_in_instance still
    works for simple shell commands. Both functions now accept volume_id+subpath,
    and provision_daytona_sandbox exercises the same VolumeMount code path.
    """
    from api.providers import (
        create_daytona_volume, delete_daytona_volume,
        provision_daytona_sandbox, destroy_daytona, exec_in_instance,
    )
    from daytona_api_client.exceptions import ForbiddenException

    # NOTE: ExecResult.stdout is a str (not bytes); assertions check res.stdout.
    vol_name = f"test-vol-mount-{uuid.uuid4().hex[:8]}"
    vol_ref = await create_daytona_volume(vol_name)
    try:
        subpath = f"agents/test-agent-{uuid.uuid4().hex[:8]}/home"
        inst1 = await provision_daytona_sandbox(
            agent_type="claude",
            volume_id=vol_ref,
            subpath=subpath,
        )
        try:
            # write a file into HOME (which is the mounted subpath)
            await exec_in_instance(inst1, "echo hello > /home/daytona/marker.txt")
            res = await exec_in_instance(inst1, "cat /home/daytona/marker.txt")
            assert "hello" in res.stdout, f"first read: {res}"
        finally:
            await destroy_daytona(inst1)

        # destroy done; create fresh sandbox with SAME subpath
        inst2 = await provision_daytona_sandbox(
            agent_type="claude",
            volume_id=vol_ref,
            subpath=subpath,
        )
        try:
            res2 = await exec_in_instance(inst2, "cat /home/daytona/marker.txt")
            assert "hello" in res2.stdout, f"persistence: {res2}"
        finally:
            await destroy_daytona(inst2)
    finally:
        try:
            await delete_daytona_volume(vol_ref)
        except ForbiddenException:
            pass  # delete not permitted for this API key
