"""Characterization tests for the supervisor-session start()/stop() template.

Written BEFORE collapsing docker/unix_local/modal session subclasses into
SupervisorSandboxSession: each test pins a divergence the collapse must
preserve bit-for-bit (the table-driven template encodes them as class
attrs/hooks). Daytona is excluded — its two-phase start is genuinely
different and keeps its own class.
"""

from __future__ import annotations

import pytest

from api.providers import ProviderInstance
from api.providers.docker.session import DockerSandboxSession
from api.providers.unix_local.session import UnixLocalSandboxSession
from api.sandbox.state import (
    DockerSandboxState,
    Recipe,
    UnixLocalSandboxState,
)


def _instance(provider, ref, port=9000, url=None):
    return ProviderInstance(
        provider=provider, url=url or f"http://127.0.0.1:{port}",
        root="/", sandbox_ref=ref, port=port,
    )


def _wire(monkeypatch, sess, mod, *, status="missing", health_ok=True,
          created_ref="fresh-1"):
    """Standard harness: fake bootstrap/status/create/health/attach,
    capture create kwargs + lifecycle calls."""
    seen: dict = {"create_kwargs": None, "calls": []}

    async def _bootstrap():
        return "vol-1"

    async def _status(ref):
        seen["calls"].append(("status", ref))
        return status

    async def _start_sandbox(ref):
        seen["calls"].append(("start_sandbox", ref))

    async def _create(**kw):
        seen["create_kwargs"] = kw
        seen["calls"].append(("create", created_ref))
        return _instance(mod.__name__.rsplit(".", 1)[-1], created_ref)

    async def _destroy(inst):
        seen["calls"].append(("destroy", inst.sandbox_ref))

    async def _health(url, max_retries=10, interval=0.3):
        seen["calls"].append(("health", max_retries, interval))
        return health_ok

    async def _attach():
        seen["calls"].append(("attach",))

    monkeypatch.setattr(sess, "_bootstrap_session", _bootstrap)
    monkeypatch.setattr(sess, "_attach_acp", _attach)
    monkeypatch.setattr(mod, "get_sandbox_status", _status)
    monkeypatch.setattr(mod, "start_sandbox", _start_sandbox, raising=False)
    monkeypatch.setattr(mod, "create_sandbox", _create)
    monkeypatch.setattr(mod, "destroy_sandbox", _destroy)
    monkeypatch.setattr("api.providers._shared._wait_for_health", _health)
    return seen


# ── create-kwargs divergences ────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_docker_create_passes_session_id_as_sandbox_ref(monkeypatch):
    """docker's create gets sandbox_ref=session_id — it becomes the
    agent-sdk.sandbox-id container label that reconcile_on_startup filters
    on. Losing it silently breaks orphan reconciliation."""
    import api.providers.docker as dk
    sess = DockerSandboxSession(
        session_id="sess-dk-1", state=DockerSandboxState(recipe=Recipe()))
    seen = _wire(monkeypatch, sess, dk)
    await sess.start()
    assert seen["create_kwargs"]["sandbox_ref"] == "sess-dk-1"
    assert "resources" in seen["create_kwargs"]


@pytest.mark.asyncio
async def test_unix_local_create_passes_no_sandbox_ref_and_no_resources(monkeypatch):
    import api.providers.unix_local as lc
    sess = UnixLocalSandboxSession(
        session_id="sess-lc-1", state=UnixLocalSandboxState(recipe=Recipe()))
    seen = _wire(monkeypatch, sess, lc)
    await sess.start()
    assert "sandbox_ref" not in seen["create_kwargs"]
    assert "resources" not in seen["create_kwargs"]


# ── stopped → revive-in-place ────────────────────────────────────────────────

@pytest.mark.asyncio
@pytest.mark.parametrize("cls,state_cls,mod_name", [
    (DockerSandboxSession, DockerSandboxState, "docker"),
    (UnixLocalSandboxSession, UnixLocalSandboxState, "unix_local"),
])
async def test_stopped_sandbox_is_revived_not_recreated(
        monkeypatch, cls, state_cls, mod_name):
    import importlib
    mod = importlib.import_module(f"api.providers.{mod_name}")
    state = state_cls(recipe=Recipe())
    state.sandbox_ref = "existing-1"
    state.listen_port = 9000
    sess = cls(session_id="sess-rev", state=state)
    seen = _wire(monkeypatch, sess, mod, status="stopped")
    await sess.start()
    assert ("start_sandbox", "existing-1") in seen["calls"]
    assert seen["create_kwargs"] is None          # no cold create
    assert sess.state.sandbox_ref == "existing-1"  # same-sandbox invariant


# ── wedged reattach: who destroys, who only clears ───────────────────────────

def _wedged_then_fresh_health(seen, monkeypatch):
    """Reattach health FAILS (wedged), the subsequent fresh-create health
    PASSES — the real wedged-recovery shape (the dead supervisor fails, the
    cold-created one comes up)."""
    results = iter([False])

    async def _health(url, max_retries=10, interval=0.3):
        seen["calls"].append(("health", max_retries, interval))
        return next(results, True)

    monkeypatch.setattr("api.providers._shared._wait_for_health", _health)


@pytest.mark.asyncio
async def test_docker_wedged_reattach_destroys_then_cold_creates(monkeypatch):
    """Wedged reattach (sandbox 'running' but supervisor unhealthy) must NOT
    500 the caller. docker DESTROYS the wedged container, clears the ref, and
    cold-creates a fresh sandbox IN THE SAME start(). Pre-fix this raised
    '...not responding', which escaped get_session as a 500 on POST /message
    and stranded the turn (golden: test_wedged_reattach_cold_recovers_not_500).
    """
    import api.providers.docker as dk
    state = DockerSandboxState(recipe=Recipe())
    state.sandbox_ref = "wedged-1"
    state.listen_port = 9000
    sess = DockerSandboxSession(session_id="sess-wedge", state=state)
    seen = _wire(monkeypatch, sess, dk, status="running")
    _wedged_then_fresh_health(seen, monkeypatch)

    await sess.start()                                  # recovers, no raise

    assert ("destroy", "wedged-1") in seen["calls"]     # wedged container torn down
    assert ("create", "fresh-1") in seen["calls"]       # cold-created fresh
    assert sess.state.sandbox_ref == "fresh-1"          # now backed by the fresh one


@pytest.mark.asyncio
async def test_unix_local_wedged_reattach_clears_then_cold_creates(monkeypatch):
    """unix_local has no container to destroy: it clears the wedged ref and
    cold-creates fresh (no destroy call). Also no 500."""
    import api.providers.unix_local as lc
    state = UnixLocalSandboxState(recipe=Recipe())
    state.sandbox_ref = "wedged-2"
    state.listen_port = 9001
    sess = UnixLocalSandboxSession(session_id="sess-wedge2", state=state)
    seen = _wire(monkeypatch, sess, lc, status="running")
    _wedged_then_fresh_health(seen, monkeypatch)

    await sess.start()                                  # recovers, no raise

    assert not any(c[0] == "destroy" for c in seen["calls"])
    assert ("create", "fresh-1") in seen["calls"]
    assert sess.state.sandbox_ref == "fresh-1"


# ── fresh-create health failure must NOT clear the ref ──────────────────────

@pytest.mark.asyncio
async def test_docker_fresh_create_health_failure_keeps_ref(monkeypatch):
    """Only the REATTACHED branch clears/destroys on bad health — a fresh
    create that fails health keeps its ref (pool start-failure path owns
    the cleanup; see test_pool_releases_compute_on_start_failure)."""
    import api.providers.docker as dk
    sess = DockerSandboxSession(
        session_id="sess-fresh", state=DockerSandboxState(recipe=Recipe()))
    seen = _wire(monkeypatch, sess, dk, status="missing", health_ok=False)
    with pytest.raises(RuntimeError, match="not responding"):
        await sess.start()
    assert not any(c[0] == "destroy" for c in seen["calls"])
    assert sess.state.sandbox_ref == "fresh-1"


# ── stop(): snapshot path + ref clearing ─────────────────────────────────────

@pytest.mark.asyncio
@pytest.mark.parametrize("cls,state_cls,mod_name,snap", [
    (DockerSandboxSession, DockerSandboxState, "docker", "/v/snapshot.tar"),
    (UnixLocalSandboxSession, UnixLocalSandboxState, "unix_local",
     "/tmp/agentsdk-snapshot.tar"),
])
async def test_stop_snapshots_then_clears_ref(
        monkeypatch, cls, state_cls, mod_name, snap):
    import importlib
    mod = importlib.import_module(f"api.providers.{mod_name}")
    state = state_cls(recipe=Recipe())
    state.sandbox_ref = "stop-me"
    state.listen_port = 9000
    sess = cls(session_id="sess-stop", state=state)
    if cls is DockerSandboxSession:
        sess._container_id = "stop-me"   # docker's stop guard
    sess._supervisor_url = "http://127.0.0.1:9000"

    snaps: list[str] = []
    stops: list[str] = []

    async def _snap(path):
        snaps.append(path)

    async def _stop(inst):
        stops.append(inst.sandbox_ref or "")

    monkeypatch.setattr(sess, "_write_snapshot", _snap)
    monkeypatch.setattr(mod, "stop_sandbox", _stop)
    await sess.stop()
    assert snaps == [snap]
    assert stops == ["stop-me"]
    assert sess.state.sandbox_ref is None
    assert sess.state.listen_port is None


@pytest.mark.asyncio
async def test_docker_stop_noops_without_in_process_start(monkeypatch):
    """docker's stop guards on the in-memory _container_id (set only by a
    start() in THIS process) — a reloaded-but-never-started session skips
    snapshot+stop. unix_local/modal guard on state.sandbox_ref instead."""
    state = DockerSandboxState(recipe=Recipe())
    state.sandbox_ref = "cold-ref"
    sess = DockerSandboxSession(session_id="sess-cold", state=state)

    called: list[str] = []

    async def _snap(path):
        called.append("snap")

    monkeypatch.setattr(sess, "_write_snapshot", _snap)
    await sess.stop()
    assert called == []
    assert sess.state.sandbox_ref == "cold-ref"   # untouched
