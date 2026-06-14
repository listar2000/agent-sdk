"""P1: DaytonaTransport interface + NativeSession provider dispatch.

Mocked against the daytona provider primitives — NO live VM provisioning
(daytona is slow + quota-sensitive + orphan-cleanup is currently broken).
Proves the transport interface and the provider-uniform resume/create flow;
the live daytona golden is a separate attended step.
"""

from __future__ import annotations

import os
import sys

import pytest


from api.native import transport as T  # noqa: E402
from api.native.session import NativeSession  # noqa: E402
from api.providers._shared import ExecResult  # noqa: E402
from api.sandbox.state import NativeSandboxState, Recipe  # noqa: E402


# ── DaytonaTransport interface (mocked provider fns) ────────────────────────

@pytest.mark.asyncio
async def test_daytona_transport_lifecycle(monkeypatch):
    calls = []

    class _FakeInst:
        sandbox_ref = "dt-sandbox-abc"

    async def _provision(**kw):
        calls.append(("provision", kw))
        return _FakeInst()

    async def _exec(inst, cmd, timeout=30):
        calls.append(("exec", cmd))
        # echo-back so write/read base64 round-trips
        if cmd.startswith("base64 < ") or "base64 < " in cmd:
            return ExecResult(stdout="aGk=", stderr="", exit_code=0)  # "hi"
        return ExecResult(stdout=f"ran:{cmd}", stderr="", exit_code=0)

    statuses = iter(["running"])

    async def _status(ref):
        calls.append(("status", ref))
        return next(statuses, "running")

    async def _stop(inst):
        calls.append(("stop", inst.sandbox_ref))

    async def _start(ref):
        calls.append(("start", ref))

    async def _destroy(inst):
        calls.append(("destroy", inst.sandbox_ref))

    import api.providers.daytona as dt
    monkeypatch.setattr(dt, "provision_daytona_sandbox", _provision)
    monkeypatch.setattr(dt, "exec_in_sandbox", _exec)
    monkeypatch.setattr(dt, "get_daytona_sandbox_status", _status)
    monkeypatch.setattr(dt, "stop_daytona", _stop)
    monkeypatch.setattr(dt, "start_daytona", _start)
    monkeypatch.setattr(dt, "destroy_daytona", _destroy)

    t = T.DaytonaTransport(workdir="/home/daytona")
    ref = await t.create(root="/home/daytona", volume_id="v1", subpath="agents/a1")
    assert ref == "dt-sandbox-abc" and t.ref == "dt-sandbox-abc"
    assert any(c[0] == "provision" for c in calls)

    r = await t.exec("echo hi")
    assert r.exit_code == 0 and "ran:" in r.stdout

    # status vocabulary matches DockerTransport (running|stopped|missing|error)
    assert await t.status() == "running"

    # hibernate = pause, resume = start, destroy = delete
    await t.hibernate(); assert ("stop", ref) in calls
    await t.resume(); assert ("start", ref) in calls
    await t.destroy(); assert ("destroy", ref) in calls

    # write/read round-trip via base64-over-exec
    await t.write_file("note.txt", b"hi")
    data = await t.read_file("note.txt")
    assert data == b"hi"


@pytest.mark.asyncio
async def test_daytona_status_missing_and_error(monkeypatch):
    import api.providers.daytona as dt
    seq = iter(["missing", "error"])

    async def _status(ref):
        return next(seq)
    monkeypatch.setattr(dt, "get_daytona_sandbox_status", _status)
    t = T.DaytonaTransport(sandbox_ref="x")
    assert await t.status() == "missing"
    assert await t.status() == "error"
    # no ref → missing without a call
    assert await T.DaytonaTransport().status() == "missing"


# ── NativeSession provider dispatch ─────────────────────────────────────────

def _native_session(provider: str) -> NativeSession:
    s = NativeSession(session_id="s-disp",
                      state=NativeSandboxState(provider=provider,
                                               recipe=Recipe(agent_type="native")))
    s._started = True
    s._cwd = "/home/daytona" if provider == "daytona" else "/work"
    s._volume_ref = "vol-1"
    s._subpath = "agents/a1"
    async def _noop():
        return None
    s._persist_state = _noop  # type: ignore
    return s


@pytest.mark.asyncio
async def test_dispatch_creates_daytona_transport(monkeypatch):
    created = {}

    class _FakeDaytona:
        provider = "daytona"
        def __init__(self, sandbox_ref=None, workdir="/home/daytona", env=None):
            self.sandbox_ref = sandbox_ref; self.workdir = workdir
        @property
        def ref(self):
            return self.sandbox_ref
        async def create(self, *, root=None, volume_id=None, subpath=None):
            created.update(root=root, volume_id=volume_id, subpath=subpath)
            self.sandbox_ref = "dt-new"; return "dt-new"

    monkeypatch.setattr(T, "DaytonaTransport", _FakeDaytona)
    s = _native_session("daytona")
    t = await s._ensure_sandbox()
    assert isinstance(t, _FakeDaytona)
    assert s.state.sandbox_ref == "dt-new"
    # passed the session's volume + subpath (volume-backed daytona)
    assert created == {"root": "/home/daytona", "volume_id": "vol-1",
                       "subpath": "agents/a1"}


@pytest.mark.asyncio
async def test_dispatch_resume_is_provider_uniform(monkeypatch):
    """A pre-existing daytona ref → status()=='stopped' → resume() → verify it
    came up ('running') → reuse, NO create. (The post-resume status() re-check
    is what lets a 'dead' sandbox that can't resume fall through to recreate.)"""
    events = []

    class _FakeDaytona:
        def __init__(self, sandbox_ref=None, workdir="/home/daytona", env=None):
            self.sandbox_ref = sandbox_ref
            self._statuses = iter(["stopped", "running"])
        @property
        def ref(self):
            return self.sandbox_ref
        async def status(self):
            events.append("status"); return next(self._statuses, "running")
        async def resume(self):
            events.append("resume")
        async def create(self, **kw):
            events.append("create"); raise AssertionError("must not create on resume")

    monkeypatch.setattr(T, "DaytonaTransport", _FakeDaytona)
    s = _native_session("daytona")
    s.state.sandbox_ref = "dt-existing"
    t = await s._ensure_sandbox()
    assert events == ["status", "resume", "status"] and t.ref == "dt-existing"


@pytest.mark.asyncio
async def test_dispatch_resume_that_raises_falls_through_to_recreate(monkeypatch):
    """daytona's resume() (start_daytona) RE-RAISES on a failed start (502
    retries exhausted / readiness timeout) — unlike docker's best-effort
    `docker start`. The session-level resume MUST catch that and fall through to
    destroy+recreate (volume-safe) — not propagate out of _ensure_sandbox and
    wedge the session forever while leaking the dead VM."""
    events = []

    class _FakeDaytona:
        def __init__(self, sandbox_ref=None, workdir="/home/daytona", env=None):
            self.sandbox_ref = sandbox_ref

        @property
        def ref(self):
            return self.sandbox_ref

        async def status(self):
            events.append("status"); return "stopped"

        async def resume(self):
            events.append("resume")
            raise RuntimeError("start_daytona: 502 retries exhausted")

        async def destroy(self):
            events.append("destroy")

        async def create(self, *, root=None, volume_id=None, subpath=None):
            events.append("create"); self.sandbox_ref = "dt-new"; return "dt-new"

    monkeypatch.setattr(T, "DaytonaTransport", _FakeDaytona)
    s = _native_session("daytona")
    s.state.sandbox_ref = "dt-dead"
    t = await s._ensure_sandbox()
    # caught the raise → destroyed the dead VM → recreated on the volume
    assert events == ["status", "resume", "destroy", "create"]
    assert t.ref == "dt-new" and s.state.sandbox_ref == "dt-new"


# ── Modal native bare-sandbox entrypoint (pure-function safety) ─────────────

def test_bare_entrypoint_symlinks_agent_home_onto_volume():
    """The normal native-modal root (/home/agent) is symlinked onto the volume
    so workspace bytes survive terminate→recreate."""
    from api.providers.modal import _build_bare_entrypoint
    script = _build_bare_entrypoint(subpath="agents/a1", root="/home/agent")
    assert "mkdir -p /v/agents/a1" in script
    assert "ln -s /v/agents/a1 /home/agent" in script
    assert "rm -rf /home/agent" in script
    assert script.strip().endswith("exec sleep infinity")


def test_bare_entrypoint_refuses_to_clobber_critical_dirs():
    """A misconfigured root must NEVER `rm -rf` a system dir. For /tmp, /, etc.
    the convenience symlink is skipped (absolute /v paths still persist)."""
    from api.providers.modal import _build_bare_entrypoint
    for danger in ("/tmp", "/", "/usr", "/home", "/var", "/v"):
        script = _build_bare_entrypoint(subpath="agents/a1", root=danger)
        assert f"rm -rf {danger}" not in script, f"clobbers {danger}!"
        assert "ln -s" not in script, f"symlinked over {danger}!"
        # still ensures the volume workspace dir exists + stays alive
        assert "mkdir -p /v/agents/a1" in script
        assert script.strip().endswith("exec sleep infinity")


def test_bare_entrypoint_skips_symlink_when_root_already_on_volume():
    from api.providers.modal import _build_bare_entrypoint
    script = _build_bare_entrypoint(subpath="agents/a1", root="/v/agents/a1")
    assert "ln -s" not in script and "rm -rf" not in script


@pytest.mark.asyncio
async def test_bare_sandbox_always_tags_for_reconcile(monkeypatch):
    """create_bare_sandbox must tag the sandbox with its object_id even when
    no sandbox_ref is passed, so reconcile_on_startup can reap orphans."""
    import api.providers.modal as md

    from types import SimpleNamespace
    tagged = {}

    class _SB:
        object_id = "sb-bare-1"

        @property
        def set_tags(self):  # create_bare uses sb.set_tags.aio(...)  (#193)
            async def _aio(t):
                tagged.update(t)
            return SimpleNamespace(aio=_aio)

        def terminate(self):
            pass

    async def _app():
        return type("A", (), {"app_id": "ap-1"})()

    async def _img():
        return object()

    async def _vol(ref):
        return object()

    async def _create_aio(*a, **k):
        return _SB()

    monkeypatch.setattr(md, "_get_app", _app)
    monkeypatch.setattr(md, "_get_image", _img)
    monkeypatch.setattr(md, "_get_volume", _vol)
    monkeypatch.setattr(md, "_to_modal_resources", lambda r: {})
    monkeypatch.setattr(md, "_require_modal", lambda: (
        SimpleNamespace(Sandbox=SimpleNamespace(
            create=SimpleNamespace(aio=_create_aio))),
        None))
    # Sandbox.create is async (modal .aio) since #193.
    monkeypatch.setenv("AGENT_SDK_ORIGIN", "test")
    inst = await md.create_bare_sandbox(volume_ref="vol-1", subpath="agents/a1")
    assert inst.sandbox_ref == "sb-bare-1"
    assert tagged.get("agent-sdk.sandbox-id") == "sb-bare-1", (
        "bare sandbox not tagged with object_id — reconcile can't reap orphans")
    assert tagged.get("agent_sdk_origin") == "test", (
        "bare sandbox not tagged with origin — cleanup_orphans can't isolate it")


# ── ModalTransport (mocked — recreate-on-missing lifecycle) ─────────────────

@pytest.mark.asyncio
async def test_modal_transport_interface(monkeypatch):
    import api.providers.modal as md

    class _Inst:
        sandbox_ref = "modal-sb-1"

    async def _create(**kw):
        return _Inst()

    async def _exec(inst, cmd, timeout=30):
        if "base64 < " in cmd:
            return ExecResult(stdout="aGk=", stderr="", exit_code=0)  # "hi"
        return ExecResult(stdout=f"ran:{cmd}", stderr="", exit_code=0)

    seq = iter(["running", "missing"])

    async def _status(ref):
        return next(seq, "missing")

    stopped = []

    async def _stop(inst):
        stopped.append(inst.sandbox_ref)

    # Native modal uses the BARE (no-supervisor) create, not create_sandbox.
    monkeypatch.setattr(md, "create_bare_sandbox", _create)
    monkeypatch.setattr(md, "exec_in_sandbox", _exec)
    monkeypatch.setattr(md, "get_sandbox_status", _status)
    monkeypatch.setattr(md, "stop_sandbox", _stop)

    t = T.ModalTransport(workdir="/v")
    ref = await t.create(volume_ref="vol-1", subpath="agents/a1")
    assert ref == "modal-sb-1" and t.ref == "modal-sb-1"

    r = await t.exec("echo hi")
    assert r.exit_code == 0 and "ran:" in r.stdout

    # status: running then missing (modal terminate → missing, NOT stopped)
    assert await t.status() == "running"
    assert await t.status() == "missing"

    # hibernate = terminate; resume is a no-op (session recreates on missing);
    # destroy = terminate
    await t.hibernate(); assert "modal-sb-1" in stopped
    assert await t.resume() is None
    await t.destroy()

    await t.write_file("note.txt", b"hi")
    assert await t.read_file("note.txt") == b"hi"


@pytest.mark.asyncio
async def test_dispatch_modal_recreate_on_missing(monkeypatch):
    """Modal hibernate→resume: status=='missing' drives the session's
    missing-branch (destroy stale + create fresh on the same volume), since
    modal can't pause/resume. New ref, workspace persists via the Volume."""
    events = []

    class _FakeModal:
        def __init__(self, sandbox_ref=None, workdir="/v", env=None):
            self.sandbox_ref = sandbox_ref
        @property
        def ref(self):
            return self.sandbox_ref
        async def status(self):
            events.append("status"); return "missing"
        async def destroy(self):
            events.append("destroy")
        async def create(self, *, volume_ref=None, subpath=None, root=None):
            events.append(("create", volume_ref, subpath))
            self.sandbox_ref = "modal-new"; return "modal-new"

    monkeypatch.setattr(T, "ModalTransport", _FakeModal)
    s = NativeSession(session_id="s-modal",
                      state=NativeSandboxState(provider="modal",
                                               sandbox_ref="modal-old",
                                               recipe=Recipe(agent_type="native")))
    s._started = True
    s._cwd = "/v"
    s._volume_ref = "vol-9"
    s._subpath = "agents/a9"
    async def _noop():
        return None
    s._persist_state = _noop  # type: ignore

    t = await s._ensure_sandbox()
    # missing → destroy stale → create fresh on same volume+subpath
    assert events == ["status", "destroy", ("create", "vol-9", "agents/a9")]
    assert t.ref == "modal-new" and s.state.sandbox_ref == "modal-new"


@pytest.mark.asyncio
async def test_invoke_tool_recreates_on_sandbox_gone():
    """The loop's _invoke_tool must catch SandboxGoneError, recreate the
    sandbox (ensure_sandbox(refresh=True)) and retry the tool once — so a
    sandbox dying under a live session self-heals instead of wedging."""
    from api.native.loop import _invoke_tool
    from api.native.transport import SandboxGoneError

    calls = {"n": 0}

    class _Tool:
        async def invoke(self, transport, args):
            calls["n"] += 1
            if calls["n"] == 1:
                raise SandboxGoneError("gone")
            return f"ran on {transport}"

    refreshed = {"n": 0}

    async def _ensure(*, refresh=False, replace=None):
        # the loop now passes the DEAD transport via replace= (concurrency-safe
        # recreate); count either spelling as a recovery
        if refresh or replace is not None:
            refreshed["n"] += 1
        return "fresh-transport"

    result, transport = await _invoke_tool(_Tool(), "dead-transport", {}, "bash", _ensure)
    assert result == "ran on fresh-transport"
    assert transport == "fresh-transport"
    assert refreshed["n"] == 1   # recreated exactly once
    assert calls["n"] == 2       # retried exactly once


@pytest.mark.asyncio
async def test_invoke_tool_normal_failure_is_data():
    """A non-gone tool failure is returned as an error string (turn continues),
    and does NOT trigger a recreate."""
    from api.native.loop import _invoke_tool

    class _Tool:
        async def invoke(self, transport, args):
            raise ValueError("boom")

    refreshed = {"n": 0}

    async def _ensure(*, refresh=False, replace=None):
        refreshed["n"] += 1
        return "x"

    result, transport = await _invoke_tool(_Tool(), "t", {}, "bash", _ensure)
    assert "error: ValueError: boom" in result
    assert transport == "t" and refreshed["n"] == 0


# ── DaytonaTransport.exec recovery mapping (the SandboxGoneError contract) ───
# Previously untested: the docker live-recovery golden cited these as covered,
# but no test exercised the daytona exec-raises → status() → resume/SandboxGone
# branches at any level.

@pytest.mark.asyncio
async def test_daytona_exec_missing_raises_sandbox_gone(monkeypatch):
    """A daytona VM hard-killed under a live session: exec raises →
    status()=='missing' → SandboxGoneError, so the session recreates on the
    volume (workspace survives) rather than surfacing a generic error."""
    import api.providers.daytona as dt

    async def _exec(inst, cmd, timeout=30):
        raise RuntimeError("daytona 404 not found")

    async def _status(ref):
        return "missing"
    monkeypatch.setattr(dt, "exec_in_sandbox", _exec)
    monkeypatch.setattr(dt, "get_daytona_sandbox_status", _status)
    t = T.DaytonaTransport(sandbox_ref="dt-x", workdir="/home/daytona")
    with pytest.raises(T.SandboxGoneError):
        await t.exec("echo hi")


@pytest.mark.asyncio
async def test_daytona_exec_self_heals_on_stopped(monkeypatch):
    """Externally-paused daytona VM: exec raises → status()=='stopped' →
    resume() → retry succeeds → status()=='running' → result returned warm."""
    import api.providers.daytona as dt
    calls = {"exec": 0, "start": 0}

    async def _exec(inst, cmd, timeout=30):
        calls["exec"] += 1
        if calls["exec"] == 1:
            raise RuntimeError("sandbox is paused")
        return ExecResult(stdout="ran", stderr="", exit_code=0)

    statuses = iter(["stopped", "running"])

    async def _status(ref):
        return next(statuses, "running")

    async def _start(ref):
        calls["start"] += 1
    monkeypatch.setattr(dt, "exec_in_sandbox", _exec)
    monkeypatch.setattr(dt, "get_daytona_sandbox_status", _status)
    monkeypatch.setattr(dt, "start_daytona", _start)
    t = T.DaytonaTransport(sandbox_ref="dt-x", workdir="/home/daytona")
    r = await t.exec("echo hi")
    assert r.exit_code == 0 and "ran" in r.stdout
    assert calls["start"] == 1 and calls["exec"] == 2


@pytest.mark.asyncio
async def test_daytona_exec_escalates_when_resume_cannot_restore(monkeypatch):
    """Parity with DockerTransport.exec: a daytona VM that resume() cannot
    restore (the retry still fails / it never reports 'running') must escalate
    to SandboxGoneError — so the session recreates on the volume — instead of
    leaking a generic exception that _invoke_tool treats as tool-error DATA,
    wedging the session forever on a dead VM."""
    import api.providers.daytona as dt

    async def _exec(inst, cmd, timeout=30):
        raise RuntimeError("VM unrecoverable")

    # classify=stopped, then the failed-retry status() re-check also non-running
    statuses = iter(["stopped", "stopped"])

    async def _status(ref):
        return next(statuses, "stopped")

    async def _start(ref):
        return None
    monkeypatch.setattr(dt, "exec_in_sandbox", _exec)
    monkeypatch.setattr(dt, "get_daytona_sandbox_status", _status)
    monkeypatch.setattr(dt, "start_daytona", _start)
    t = T.DaytonaTransport(sandbox_ref="dt-x", workdir="/home/daytona")
    with pytest.raises(T.SandboxGoneError):
        await t.exec("echo hi")


@pytest.mark.asyncio
async def test_daytona_exec_successful_retry_not_escalated_on_status_lag(monkeypatch):
    """A SUCCESSFUL post-resume retry must be RETURNED — never escalated to
    SandboxGoneError — even if status() briefly lags to non-'running' right
    after resume. The successful exec is itself proof the VM is up; a status()
    second-guess would wrongly recreate a perfectly healthy VM (regression guard
    for the efb50fd `res is None OR status!=running` over-eager escalation)."""
    import api.providers.daytona as dt
    calls = {"exec": 0}

    async def _exec(inst, cmd, timeout=30):
        calls["exec"] += 1
        if calls["exec"] == 1:
            raise RuntimeError("sandbox is paused")
        return ExecResult(stdout="ok", stderr="", exit_code=0)

    async def _status(ref):
        # always reports 'stopped' — if the code wrongly re-checks status after
        # a SUCCESSFUL retry it would see this lag and spuriously escalate
        return "stopped"

    async def _start(ref):
        return None
    monkeypatch.setattr(dt, "exec_in_sandbox", _exec)
    monkeypatch.setattr(dt, "get_daytona_sandbox_status", _status)
    monkeypatch.setattr(dt, "start_daytona", _start)
    t = T.DaytonaTransport(sandbox_ref="dt-x", workdir="/home/daytona")
    r = await t.exec("echo hi")   # must NOT raise SandboxGoneError
    assert r.exit_code == 0 and "ok" in r.stdout
    assert calls["exec"] == 2


@pytest.mark.asyncio
async def test_daytona_exec_error_status_fails_closed(monkeypatch):
    """A transient daytona status 'error' must NOT be read as gone: exec
    re-raises the ORIGINAL error (fail-closed), never SandboxGoneError, so the
    session won't cold-create over a possibly-live VM and lose the volume
    workspace. (Deliberately asymmetric with the 'stopped'/'missing' branches.)"""
    import api.providers.daytona as dt

    async def _exec(inst, cmd, timeout=30):
        raise RuntimeError("transient status-api blip")

    async def _status(ref):
        return "error"
    monkeypatch.setattr(dt, "exec_in_sandbox", _exec)
    monkeypatch.setattr(dt, "get_daytona_sandbox_status", _status)
    t = T.DaytonaTransport(sandbox_ref="dt-x", workdir="/home/daytona")
    with pytest.raises(RuntimeError, match="transient status-api blip"):
        await t.exec("echo hi")
    assert not isinstance(RuntimeError, T.SandboxGoneError)


@pytest.mark.asyncio
async def test_daytona_create_destroys_on_readiness_failure(monkeypatch):
    """Parity with docker/modal create(): if the post-provision readiness mkdir
    fails, create() must DESTROY the freshly-provisioned VM before propagating —
    else a PAID idle daytona sandbox leaks until the next boot reconcile (its ref
    is never persisted, so nothing else would tear it down)."""
    import api.providers.daytona as dt

    class _Inst:
        sandbox_ref = "dt-fresh"

    async def _provision(**kw):
        return _Inst()

    async def _exec(inst, cmd, timeout=30):
        raise RuntimeError("control-plane blip during mkdir")

    async def _status(ref):
        return "running"   # fresh VM is up; exec recovery re-raises the error

    destroyed = []

    async def _destroy(inst):
        destroyed.append(inst.sandbox_ref)
    monkeypatch.setattr(dt, "provision_daytona_sandbox", _provision)
    monkeypatch.setattr(dt, "exec_in_sandbox", _exec)
    monkeypatch.setattr(dt, "get_daytona_sandbox_status", _status)
    monkeypatch.setattr(dt, "destroy_daytona", _destroy)

    t = T.DaytonaTransport(workdir="/home/daytona")
    with pytest.raises(RuntimeError, match="control-plane blip"):
        await t.create(root="/home/daytona", volume_id="v1", subpath="agents/a1")
    assert destroyed == ["dt-fresh"], (
        "create() must destroy the leaked VM when readiness fails")


@pytest.mark.asyncio
async def test_dispatch_authoritative_resume_skips_redundant_status(monkeypatch):
    """daytona's resume (start_daytona) BLOCKS until the VM is ready and
    re-raises on failure, so a clean return already proves running —
    _ensure_sandbox must not spend a second control-plane round-trip
    re-checking status() (reap→resume efficiency parity with the supervisor
    path, which does a single start call)."""
    events = []

    class _FakeDaytona:
        resume_is_authoritative = True

        def __init__(self, sandbox_ref=None, workdir="/home/daytona", env=None):
            self.sandbox_ref = sandbox_ref

        @property
        def ref(self):
            return self.sandbox_ref

        async def status(self):
            events.append("status"); return "stopped"

        async def resume(self):
            events.append("resume")   # start_daytona already waited-ready

        async def destroy(self):
            events.append("destroy")

    monkeypatch.setattr(T, "DaytonaTransport", _FakeDaytona)
    s = _native_session("daytona")
    s.state.sandbox_ref = "dt-hib"
    t = await s._ensure_sandbox()
    assert t.ref == "dt-hib"
    assert events == ["status", "resume"], (
        f"authoritative resume must skip the post-resume status() "
        f"round-trip, got {events}")


@pytest.mark.asyncio
async def test_dispatch_nonauthoritative_resume_still_verifies(monkeypatch):
    """PIN: docker/modal resume can return on a still-dead sandbox — their
    post-resume status() re-check is load-bearing and must be preserved
    (resume_is_authoritative=False)."""
    events = []
    statuses = iter(["stopped", "running"])

    class _FakeDocker:
        resume_is_authoritative = False

        def __init__(self, container_id=None, workdir="/", env=None):
            self.container_id = container_id

        @property
        def ref(self):
            return self.container_id

        async def status(self):
            events.append("status"); return next(statuses)

        async def resume(self):
            events.append("resume")

        async def destroy(self):
            events.append("destroy")

    monkeypatch.setattr(T, "DockerTransport", _FakeDocker)
    s = _native_session("docker")
    s.state.sandbox_ref = "cid-hib"
    t = await s._ensure_sandbox()
    assert t.ref == "cid-hib"
    assert events == ["status", "resume", "status"]


def test_real_transport_resume_authority_flags():
    """The flags encode each provider's actual resume contract — drift here
    silently reintroduces either the daytona double round-trip or a docker
    dead-container false-positive resume."""
    assert T.DaytonaTransport.resume_is_authoritative is True
    assert T.DockerTransport.resume_is_authoritative is False
    assert T.ModalTransport.resume_is_authoritative is False
