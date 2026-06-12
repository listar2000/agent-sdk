"""Hibernate/resume + cleanup for native sandboxes — the resource-lifecycle
contract: reap hibernates (docker stop, files survive), resume reattaches
the SAME container, delete actually destroys it.

Live docker; skips when unavailable.
"""

from __future__ import annotations

import contextlib
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


@pytest.mark.asyncio
async def test_resume_issues_single_inspect(monkeypatch):
    """B1: warm resume of a running container costs exactly ONE docker
    inspect (status), not exists()+is_alive() = 2-3 calls."""
    from api.native import transport as _t

    calls = []
    real = _t._run_docker

    async def _counting(*args, **kw):
        calls.append(args[0])
        return await real(*args, **kw)

    s = _session()
    try:
        t = await s._ensure_sandbox()
        cid = t.container_id
        # fresh session, container running; monkeypatch only the resume path
        monkeypatch.setattr(_t, "_run_docker", _counting)
        s2 = NativeSession(session_id="sess-hib2",
                           state=NativeSandboxState(provider="docker",
                                                    sandbox_ref=cid))
        s2._started = True
        s2._cwd = "/work"
        async def _noop():
            return None
        s2._persist_state = _noop  # type: ignore
        await s2._ensure_sandbox()
        assert calls.count("inspect") == 1, f"resume used {calls.count('inspect')} inspects: {calls}"
        # running container → no start needed
        assert "start" not in calls
    finally:
        if s.state.sandbox_ref:
            subprocess.run(["docker", "rm", "-f", s.state.sandbox_ref],
                           capture_output=True)


@pytest.mark.asyncio
async def test_inspect_error_does_not_cold_create(monkeypatch):
    """B2 fail-closed: a transient inspect failure must NOT create a fresh
    sandbox over a possibly-live container (native has no volume — a spurious
    create silently loses the workspace)."""
    from api.native import transport as _t

    created = {"n": 0}
    real_create = _t.DockerTransport.create

    async def _track_create(self, **kw):
        created["n"] += 1
        return await real_create(self, **kw)

    async def _error_status(self):
        return "error"

    monkeypatch.setattr(_t.DockerTransport, "status", _error_status)
    monkeypatch.setattr(_t.DockerTransport, "create", _track_create)

    s = NativeSession(session_id="sess-err",
                      state=NativeSandboxState(provider="docker",
                                               sandbox_ref="someref",
                                               recipe=Recipe(agent_type="native")))
    s._started = True
    s._cwd = "/work"
    async def _noop():
        return None
    s._persist_state = _noop  # type: ignore

    with pytest.raises(RuntimeError, match="transient"):
        await s._ensure_sandbox()
    assert created["n"] == 0, "must not cold-create on transient inspect error"


@pytest.mark.asyncio
async def test_exec_self_heals_externally_stopped_container():
    """Live: an external `docker stop` of a live session's container is
    transparently recovered — exec resumes the SAME container and retries,
    keeping the session warm (no cold-recovery)."""
    s = _session()
    try:
        t = await s._ensure_sandbox()
        cid = t.container_id
        await t.write_file("m.txt", b"warm")
        # external stop out from under the live transport
        subprocess.run(["docker", "stop", "-t", "1", cid], capture_output=True, timeout=30)
        assert _container_state(cid) == "exited"
        # exec must self-heal: resume same container + run the command
        r = await t.exec("cat m.txt")
        assert r.exit_code == 0 and r.stdout.strip() == "warm"
        assert _container_state(cid) == "running"
        assert t.container_id == cid, "must resume the SAME container, not replace it"
        # write_file self-heals too
        subprocess.run(["docker", "stop", "-t", "1", cid], capture_output=True, timeout=30)
        await t.write_file("m2.txt", b"after-stop")
        assert (await t.read_file("m2.txt")) == b"after-stop"
    finally:
        if s.state.sandbox_ref:
            subprocess.run(["docker", "rm", "-f", s.state.sandbox_ref],
                           capture_output=True)


@pytest.mark.asyncio
async def test_reconcile_reaps_native_docker_orphan(monkeypatch):
    """Boot reconcile MUST reclaim a crash-orphaned native docker container
    and leave a live one untouched.

    Native containers carry the ``native_session`` label, NOT
    ``agent-sdk.sandbox-id``; before the fix, reconcile_on_startup filtered
    only the latter and so could never enumerate (let alone reap) a native
    orphan — it leaked across every server restart. Mirrors the
    modal/daytona native tag-for-reconcile contract, end-to-end against a
    real container.
    """
    from api import db as dbmod
    from api.providers import docker as dockermod

    live = DockerTransport(workdir="/work")
    orphan = DockerTransport(workdir="/work")
    await live.create(image=IMAGE, labels={"agent_sdk_origin": "test",
                                           "native_session": "sess-recon-live"})
    await orphan.create(image=IMAGE, labels={"agent_sdk_origin": "test",
                                             "native_session": "sess-recon-orphan"})
    try:
        # reconcile is GLOBAL: protect every OTHER agent-sdk container (parallel
        # -n auto workers, the live container) by treating them all as live —
        # live_refs = every present agent-sdk container EXCEPT our orphan.
        def _present_full_ids() -> set[str]:
            ids: set[str] = set()
            for lk in ("agent-sdk.sandbox-id", "native_session"):
                r = subprocess.run(
                    ["docker", "ps", "-aq", "--no-trunc", "--filter", f"label={lk}"],
                    capture_output=True, text=True)
                ids |= {x for x in r.stdout.split() if x}
            return ids
        protected = _present_full_ids() - {orphan.container_id}

        async def _live_refs():
            return protected
        monkeypatch.setattr(dbmod, "live_sandbox_refs", _live_refs)

        await dockermod.reconcile_on_startup()

        assert _container_state(orphan.container_id) == "", (
            "reconcile did NOT reap the orphaned native docker container — "
            "native_session-labeled containers must be enumerated by the boot "
            "reconciler (the modal/daytona native parity)")
        assert _container_state(live.container_id) == "running", (
            "reconcile wrongly reaped a LIVE native container (full-id match "
            "against live_refs regressed)")
    finally:
        for t in (live, orphan):
            with contextlib.suppress(Exception):
                await t.destroy()
