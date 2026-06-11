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

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

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
        def __init__(self, sandbox_ref=None, workdir="/home/daytona"):
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
    """A pre-existing daytona ref → status()=='stopped' → resume(), no create."""
    events = []

    class _FakeDaytona:
        def __init__(self, sandbox_ref=None, workdir="/home/daytona"):
            self.sandbox_ref = sandbox_ref
        @property
        def ref(self):
            return self.sandbox_ref
        async def status(self):
            events.append("status"); return "stopped"
        async def resume(self):
            events.append("resume")
        async def create(self, **kw):
            events.append("create"); raise AssertionError("must not create on resume")

    monkeypatch.setattr(T, "DaytonaTransport", _FakeDaytona)
    s = _native_session("daytona")
    s.state.sandbox_ref = "dt-existing"
    t = await s._ensure_sandbox()
    assert events == ["status", "resume"] and t.ref == "dt-existing"


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

    monkeypatch.setattr(md, "create_sandbox", _create)
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
        def __init__(self, sandbox_ref=None, workdir="/v"):
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
