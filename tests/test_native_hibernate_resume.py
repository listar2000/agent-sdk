"""Hibernate/resume + cleanup for native sandboxes — the resource-lifecycle
contract: reap hibernates (docker stop, files survive), resume reattaches
the SAME container, delete actually destroys it.

Live docker; skips when unavailable.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys

import pytest
import pytest_asyncio

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from api.native.session import NativeSession  # noqa: E402
from api.native.transport import DockerTransport  # noqa: E402
from api.sandbox.state import NativeSandboxState, Recipe  # noqa: E402


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


def _container_state(cid: str) -> str:
    r = subprocess.run(["docker", "inspect", "-f", "{{.State.Status}}", cid],
                       capture_output=True, text=True)
    return "" if r.returncode != 0 else r.stdout.strip()


@pytest.fixture(scope="module", autouse=True)
def _img():
    if subprocess.run(["docker", "image", "inspect", IMAGE],
                      capture_output=True).returncode != 0:
        subprocess.run(["docker", "pull", IMAGE], check=True, timeout=180)


def _session() -> NativeSession:
    s = NativeSession(session_id="sess-hib",
                      state=NativeSandboxState(provider="docker",
                                               recipe=Recipe(agent_type="native")))
    s._started = True
    s._cwd = "/work"
    # avoid DB; persist is a no-op for the test
    async def _noop():
        return None
    s._persist_state = _noop  # type: ignore

    async def _factory():
        t = DockerTransport(workdir="/work")
        await t.create(image=IMAGE, labels={"agent_sdk_origin": "test",
                                            "native_session": "sess-hib"})
        await t.exec("mkdir -p /work", cwd="/")
        return t
    s._transport_factory = _factory
    return s


@pytest.mark.asyncio
async def test_hibernate_keeps_container_resume_reattaches_with_files():
    s = _session()
    try:
        t = await s._ensure_sandbox()
        cid = t.container_id
        # write a workspace file
        await t.write_file("marker.txt", b"survives-hibernate")
        assert _container_state(cid) == "running"

        # HIBERNATE (what the reaper's release() calls)
        await s.stop()
        assert _container_state(cid) == "exited", "stop() must hibernate, not remove"

        # simulate the pool dropping the runtime object: shutdown() must NOT
        # destroy the container (in-memory teardown only)
        await s.shutdown()
        assert _container_state(cid) == "exited", "shutdown() must not remove the container"

        # RESUME: a fresh session instance (cold path), same persisted ref
        s2 = NativeSession(session_id="sess-hib",
                           state=NativeSandboxState(provider="docker",
                                                    sandbox_ref=cid))
        s2._started = True
        s2._cwd = "/work"
        async def _noop2():
            return None
        s2._persist_state = _noop2  # type: ignore

        t2 = await s2._ensure_sandbox()
        assert t2.container_id == cid, "resume must reattach the SAME container"
        assert _container_state(cid) == "running", "resume must restart it"
        # workspace file survived hibernation
        assert (await t2.read_file("marker.txt")) == b"survives-hibernate"

        # DELETE cleans up
        await s2.destroy()
        assert _container_state(cid) == "", "destroy() must remove the container"
    finally:
        if s.state.sandbox_ref:
            subprocess.run(["docker", "rm", "-f", s.state.sandbox_ref],
                           capture_output=True)


@pytest.mark.asyncio
async def test_stale_ref_falls_through_to_create():
    """A sandbox_ref to a gone container must not wedge resume — create fresh."""
    s = NativeSession(session_id="sess-stale",
                      state=NativeSandboxState(provider="docker",
                                               sandbox_ref="deadbeefgone",
                                               recipe=Recipe(agent_type="native")))
    s._started = True
    s._cwd = "/work"
    async def _noop():
        return None
    s._persist_state = _noop  # type: ignore
    try:
        t = await s._ensure_sandbox()
        assert t.container_id and t.container_id != "deadbeefgone"
        assert _container_state(t.container_id) == "running"
    finally:
        if s.state.sandbox_ref and s.state.sandbox_ref != "deadbeefgone":
            subprocess.run(["docker", "rm", "-f", s.state.sandbox_ref],
                           capture_output=True)
