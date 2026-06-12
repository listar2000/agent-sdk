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
async def test_hibernate_is_immediate_not_grace_bound():
    """Native hibernate must SIGKILL immediately (`docker stop -t 0`): the
    sleep-infinity PID-1 ignores SIGTERM (no default disposition for PID-1), so
    any grace period is pure dead time docker waits out before SIGKILLing (~2s
    with `-t 2`) — slower than the supervisor hibernate. The huge gap (~0.1s vs
    ~2s) keeps this off the flaky edge. Workspace survival is covered by
    test_hibernate_keeps_container_resume_reattaches_with_files."""
    import time as _time
    s = _session()
    cid = None
    try:
        t = await s._ensure_sandbox()
        cid = t.container_id
        t0 = _time.monotonic()
        await t.hibernate()
        dt = _time.monotonic() - t0
        assert _container_state(cid) == "exited"
        assert dt < 1.0, (
            f"native hibernate took {dt:.2f}s — grace not skipped? `sleep "
            f"infinity` ignores SIGTERM, so `docker stop` must use -t 0")
    finally:
        if cid:
            subprocess.run(["docker", "rm", "-f", cid], capture_output=True)


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
        # reconcile is GLOBAL and origin-blind: protect every OTHER agent-sdk
        # container (parallel -n auto workers, the live container) by treating
        # them all as live. CRITICAL: reconcile enumerates (ps) BEFORE it calls
        # live_sandbox_refs(), so the mock RE-ENUMERATES at call time — every
        # container reconcile saw (incl. any a concurrent worker just created)
        # is captured and protected; only our orphan is excluded. A pre-snapshot
        # set would leave a window where a sibling test's fresh container is
        # mis-classified as an orphan and force-removed.
        def _present_full_ids() -> set[str]:
            ids: set[str] = set()
            for lk in ("agent-sdk.sandbox-id", "native_session"):
                r = subprocess.run(
                    ["docker", "ps", "-aq", "--no-trunc", "--filter", f"label={lk}"],
                    capture_output=True, text=True)
                ids |= {x for x in r.stdout.split() if x}
            return ids

        async def _live_refs():
            return _present_full_ids() - {orphan.container_id}
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


@pytest.mark.asyncio
async def test_removed_container_under_live_session_recreates():
    """A live native session whose container is REMOVED out-from-under it
    (docker prune / OOM-reap / — on modal, the routine hard-timeout ceiling)
    must NOT wedge: exec raises SandboxGoneError instead of failing silently,
    and _ensure_sandbox(refresh=True) drops the dead transport and provisions a
    fresh one. (Docker's writable layer is gone with the container — cold
    recovery; volume-backed workspace recovery is a modal/daytona property.)"""
    from api.native.transport import SandboxGoneError
    s = _session()
    extra = None
    try:
        t = await s._ensure_sandbox()
        cid = t.container_id
        assert _container_state(cid) == "running"
        # remove the container out from under the live, cached transport
        subprocess.run(["docker", "rm", "-f", cid], capture_output=True, timeout=30)
        assert _container_state(cid) == ""
        # the dead transport SIGNALS gone (not a silent failed result)
        with pytest.raises(SandboxGoneError):
            await t.exec("true")
        # session recovers: refresh recreates a fresh, working container
        t2 = await s._ensure_sandbox(refresh=True)
        extra = t2.container_id
        assert t2.container_id and t2.container_id != cid
        r = await t2.exec("echo recovered")
        assert r.exit_code == 0 and "recovered" in r.stdout
    finally:
        for ref in {s.state.sandbox_ref, extra}:
            if ref:
                subprocess.run(["docker", "rm", "-f", ref], capture_output=True)


@pytest.mark.asyncio
async def test_sandbox_exec_recovers_when_container_removed():
    """The /sandbox/exec route (NativeSession.sandbox_exec) must ALSO recover a
    sandbox removed under the live session — not 500 with SandboxGoneError —
    mirroring the loop's _invoke_tool recover-and-retry."""
    s = _session()
    extra = None
    try:
        t = await s._ensure_sandbox()
        cid = t.container_id
        subprocess.run(["docker", "rm", "-f", cid], capture_output=True, timeout=30)
        assert _container_state(cid) == ""
        # must recreate + succeed, not raise
        res = await s.sandbox_exec("echo back", timeout=30)
        assert res["exit_code"] == 0 and "back" in res["stdout"]
        extra = s._transport.container_id
        assert extra and extra != cid
    finally:
        for ref in {s.state.sandbox_ref, extra}:
            if ref:
                subprocess.run(["docker", "rm", "-f", ref], capture_output=True)


@pytest.mark.asyncio
async def test_exec_escalates_to_recreate_when_resume_cannot_restore(monkeypatch):
    """A 'dead' container (kernel/storage fault) maps to status()=="stopped"
    but cannot `docker start` — resume() is a silent no-op, so without an
    escape hatch exec would wedge forever re-hitting "is not running" (which
    never matches the removed-marker, so the recreate gate never fires). exec
    must instead escalate to SandboxGoneError once resume fails to restore a
    runnable container, so the session recreates. Reproduced realistically with
    a genuinely-stopped container whose resume() is neutered to stand in for an
    unrestartable/dead container."""
    from api.native.transport import SandboxGoneError
    s = _session()
    try:
        t = await s._ensure_sandbox()
        cid = t.container_id
        subprocess.run(["docker", "stop", "-t", "1", cid], capture_output=True, timeout=30)
        assert _container_state(cid) == "exited"

        async def _dead_resume():   # docker start is a silent no-op on a corpse
            return None
        monkeypatch.setattr(t, "resume", _dead_resume)

        # exec must NOT wedge returning "is not running" forever — it escalates.
        with pytest.raises(SandboxGoneError):
            await t.exec("echo should-not-wedge")
        # and the session can then recreate a fresh, working container
        t2 = await s._ensure_sandbox(refresh=True)
        assert t2.container_id and t2.container_id != cid
        r = await t2.exec("echo recovered")
        assert r.exit_code == 0 and "recovered" in r.stdout
    finally:
        for ref in {s.state.sandbox_ref, cid}:
            if ref:
                subprocess.run(["docker", "rm", "-f", ref], capture_output=True)


@pytest.mark.asyncio
async def test_ensure_sandbox_recreates_dead_container_on_reattach(monkeypatch):
    """PRODUCTION reattach path (no _transport_factory): a session whose
    persisted docker sandbox is 'dead' (maps to status()=="stopped" but resume
    cannot restart it) must DESTROY the corpse and recreate fresh — not
    resume-and-return it. Resuming-and-returning would wedge the session forever
    (every turn re-execs the dead container → SandboxGoneError → reattach →
    resume → dead → ...) and leak the dead container. This is the session-level
    half of dead-container recovery that the transport-level SandboxGoneError
    escalation depends on (the unit dead-wedge test takes the factory shortcut
    and never exercised this path)."""
    s = NativeSession(session_id="sess-dead-reattach",
                      state=NativeSandboxState(provider="docker",
                                               recipe=Recipe(agent_type="native")))
    s._started = True
    s._cwd = "/work"

    async def _noop():
        return None
    s._persist_state = _noop  # type: ignore
    # deliberately NO _transport_factory → exercise the real reattach path

    seed = DockerTransport(workdir="/work")
    await seed.create(image=IMAGE, labels={"agent_sdk_origin": "test",
                                           "native_session": "sess-dead-reattach"})
    cid = seed.container_id
    s.state.sandbox_ref = cid
    subprocess.run(["docker", "stop", "-t", "1", cid], capture_output=True, timeout=30)
    assert _container_state(cid) == "exited"

    # neuter resume on ALL DockerTransport instances so the REATTACHED transport
    # also cannot restart the corpse — a faithful 'dead container' (docker start
    # is a silent no-op). _create_transport/destroy are untouched.
    async def _dead_resume(self):
        return None
    monkeypatch.setattr(DockerTransport, "resume", _dead_resume)

    t2 = None
    try:
        t2 = await s._ensure_sandbox()
        assert t2.container_id and t2.container_id != cid, (
            "must recreate a fresh sandbox, not resume-and-return the corpse")
        assert _container_state(t2.container_id) == "running"
        assert _container_state(cid) == "", (
            "the dead container must be destroyed on recreate (no orphan leak)")
        r = await t2.exec("echo recovered")
        assert r.exit_code == 0 and "recovered" in r.stdout
    finally:
        for ref in {cid, s.state.sandbox_ref,
                    getattr(t2, "container_id", None)}:
            if ref:
                subprocess.run(["docker", "rm", "-f", ref], capture_output=True)


@pytest.mark.asyncio
async def test_exec_marker_in_command_stderr_does_not_false_trip_recovery():
    """A HEALTHY container's own command may legitimately print "no such
    container" / "is not running" to stderr (e.g. an agent running
    `docker logs X` against a missing inner container, or grepping a log).
    docker exec forwards that inner stderr into the SAME buffer the recovery
    heuristics scan, so a bare substring match would misfire — spuriously
    recreating the sandbox, or (worse) RE-RUNNING a non-idempotent command.
    An authoritative inspect (status()) gates both verdicts, so the sentinel
    is treated as ordinary command output and the SAME live container stays."""
    s = _session()
    try:
        t = await s._ensure_sandbox()
        cid = t.container_id
        # CASE 1: the "removed" sentinel in the command's own stderr must NOT
        # be read as a gone sandbox — no SandboxGoneError, no recreate.
        r = await t.exec("echo 'Error: No such container: deadbeef' >&2; exit 1")
        assert r.exit_code == 1
        assert "no such container" in r.stderr.lower()
        assert t.container_id == cid, "must NOT recreate — container is alive"
        assert _container_state(cid) == "running"
        # CASE 2: the "stopped" sentinel + a side effect proves the command is
        # NOT silently re-run (no spurious resume+retry double-invocation).
        r = await t.exec(
            "echo one >> hits.txt; echo 'Unit x is not running' >&2; exit 1")
        assert r.exit_code == 1
        hits = (await t.read_file("hits.txt")).decode()
        assert hits.count("one") == 1, f"command ran twice (double-invoked): {hits!r}"
        assert t.container_id == cid
        assert _container_state(cid) == "running"
    finally:
        if s.state.sandbox_ref:
            subprocess.run(["docker", "rm", "-f", s.state.sandbox_ref],
                           capture_output=True)
