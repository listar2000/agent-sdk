"""P0-D gate: DockerTransport lifecycle against the local docker daemon.

Covers the audit-derived constraints specifically:
- the in-container ``timeout`` wrapper actually kills the process (the bare
  docker-CLI timeout would not);
- file writes beyond the ~96KiB argv ceiling round-trip via stdin;
- env/cwd plumbing; readiness gate; destroy semantics.

Skips cleanly when docker is unavailable.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys

import pytest
import pytest_asyncio

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from api.native.transport import DockerTransport  # noqa: E402


def _docker_ok() -> bool:
    if not shutil.which("docker"):
        return False
    try:
        return subprocess.run(["docker", "info"], capture_output=True,
                              timeout=10).returncode == 0
    except Exception:
        return False


pytestmark = pytest.mark.skipif(not _docker_ok(), reason="docker unavailable")

IMAGE = "alpine:3.20"


@pytest.fixture(scope="module", autouse=True)
def _ensure_image():
    if subprocess.run(["docker", "image", "inspect", IMAGE],
                      capture_output=True).returncode != 0:
        subprocess.run(["docker", "pull", IMAGE], check=True, timeout=180)


@pytest_asyncio.fixture()
async def sandbox():
    t = DockerTransport()
    await t.create(image=IMAGE, labels={"agent_sdk_origin": "test",
                                        "native_transport_test": "1"})
    yield t
    await t.destroy()


@pytest.mark.asyncio
async def test_exec_basic_and_env_and_cwd(sandbox):
    r = await sandbox.exec("echo transport-$((6*7))")
    assert r.exit_code == 0 and not r.timed_out
    assert r.stdout.strip() == "transport-42"

    r = await sandbox.exec("echo $MY_SECRET", env={"MY_SECRET": "from-env-9"})
    assert r.stdout.strip() == "from-env-9"

    await sandbox.exec("mkdir -p /work/sub")
    r = await sandbox.exec("pwd", cwd="/work/sub")
    assert r.stdout.strip() == "/work/sub"


@pytest.mark.asyncio
async def test_exec_failing_command_reports_exit_code(sandbox):
    r = await sandbox.exec("ls /nonexistent-native-xyz")
    assert r.exit_code != 0 and not r.timed_out
    assert "nonexistent-native-xyz" in r.stderr


@pytest.mark.asyncio
async def test_exec_timeout_kills_in_container_process(sandbox):
    """The audit finding: a host-only timeout leaves the process running.
    The wrapper must (a) report timed_out and (b) actually terminate it."""
    # Distinctive marker in the command so the leftover check can't collide
    # with the wrapper's own watchdog `sleep <timeout>` in ps output.
    r = await sandbox.exec("sleep 30 && echo NEVER_RAN_MARKER", timeout_s=1)
    assert r.timed_out and r.exit_code in (124, 137, 143)
    leftover = await sandbox.exec(
        "ps -o args | grep -v grep | grep -c NEVER_RAN_MARKER || true")
    assert leftover.stdout.strip() == "0", (
        f"command tree survived the timeout: {leftover.stdout!r}")


@pytest.mark.asyncio
async def test_write_read_roundtrip_beyond_argv_ceiling(sandbox):
    """200KiB payload — over the ~96KiB base64-in-argv limit; must stream."""
    payload = os.urandom(200 * 1024)
    await sandbox.write_file("/work/deep/dir/blob.bin", payload)
    back = await sandbox.read_file("/work/deep/dir/blob.bin")
    assert back == payload

    r = await sandbox.exec("wc -c < /work/deep/dir/blob.bin")
    assert r.stdout.strip() == str(len(payload))


@pytest.mark.asyncio
async def test_read_missing_file_raises(sandbox):
    with pytest.raises(FileNotFoundError):
        await sandbox.read_file("/no/such/file.txt")


@pytest.mark.asyncio
async def test_is_alive_and_destroy():
    t = DockerTransport()
    await t.create(image=IMAGE, labels={"agent_sdk_origin": "test"})
    assert await t.is_alive()
    await t.destroy()
    assert not await t.is_alive()


@pytest.mark.asyncio
async def test_exec_without_sandbox_raises():
    t = DockerTransport()
    with pytest.raises(RuntimeError, match="no sandbox"):
        await t.exec("true")
