"""DaytonaSandboxSession.start() must DESTROY a freshly cold-created VM when
its supervisor / ACP bringup fails — not leak it as a running VM.

Sibling of test_daytona_reattach_destroys_old_vm.py. The reattach-fallback in
``_resolve_or_create_sandbox`` already destroys an unreachable OLD VM (the
primary daytona flood). The remaining hole was the cold-create path: start()
cold-creates a brand-new VM (``create_sandbox`` → sets ``sandbox_ref``), then
``start_supervisor_in_sandbox`` health-waits and RAISES on failure (supervisor
never came up — boot error / disk full / OOM). The unfixed code let that raise
propagate with the fresh VM still RUNNING — burning compute and leaking against
the account disk quota until reconcile (boot-only) or cleanup_orphans (defaults
to origin=test, so production is never reaped). State lives on the /vol
snapshot, not the VM, so the VM is pure waste.

BEHAVIOURAL test: a fake daytona provider keeps a ``live`` registry of which
VMs exist. We drive the real ``start()`` into the cold-create path, make the
supervisor bringup fail, and assert the OBSERVABLE OUTCOME — the freshly
created VM is no longer ``live`` (destroyed) and ``sandbox_ref`` is cleared.
Pre-fix FAILS (the fresh VM is still ``live`` — leaked); post-fix PASSES.

Run::

    .venv/bin/python -m pytest \
      tests/test_daytona_fresh_create_supervisor_fail_destroys_vm.py -n auto
"""
from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

import api.providers.daytona as dt_provider
from api.providers.daytona.session import DaytonaSandboxSession
from api.sandbox.state import DaytonaSandboxState, Recipe


@pytest.mark.asyncio
async def test_fresh_create_supervisor_fail_destroys_vm(monkeypatch):
    # No prior sandbox_ref → start() takes the cold-create path.
    sess = DaytonaSandboxSession(
        session_id="s1",
        state=DaytonaSandboxState(recipe=Recipe(), sandbox_ref=None),
    )

    # ``_bootstrap_session`` normally fills these from the DB volume row.
    async def _fake_bootstrap():
        sess._volume_ref = "vol-1"
        sess._subpath = "sessions/s1"
        sess._spawn_env = {}

    monkeypatch.setattr(sess, "_bootstrap_session", _fake_bootstrap)

    # Observable provider state: which VMs still exist.
    live: dict[str, str] = {}

    async def _aget(ref):
        return SimpleNamespace(id=ref)

    async def _fake_create_sandbox(**k):
        from api.providers import ProviderInstance
        live["fresh-ref"] = "started"  # the brand-new VM, now running
        return ProviderInstance(provider="daytona", url="", root="", sandbox_ref="fresh-ref")

    async def _fake_get_client():
        return SimpleNamespace(get=_aget)

    async def _fake_start_supervisor(*a, **k):
        # Supervisor never comes healthy — the exact failure that left a
        # running VM behind.
        raise RuntimeError("supervisor failed health check")

    async def _fake_destroy(instance):
        live.pop(instance.sandbox_ref, None)

    monkeypatch.setattr(dt_provider, "create_sandbox", _fake_create_sandbox)
    monkeypatch.setattr(dt_provider, "_get_async_daytona_client", _fake_get_client)
    monkeypatch.setattr(dt_provider, "start_supervisor_in_sandbox", _fake_start_supervisor)
    monkeypatch.setattr(dt_provider, "destroy_daytona", _fake_destroy)

    # start() must surface the bringup failure (a fresh VM that won't boot is
    # not silently recoverable here).
    with pytest.raises(RuntimeError, match="health check"):
        await sess.start()

    # The fresh VM was created...
    assert "fresh-ref" not in live or True  # (created then must be destroyed)

    # Destroy is fire-and-forget — yield to the loop so it settles.
    for _ in range(50):
        if "fresh-ref" not in live:
            break
        await asyncio.sleep(0.01)

    # BEHAVIOURAL INVARIANT: the freshly created VM must no longer exist.
    assert "fresh-ref" not in live, (
        "DAYTONA VM LEAK: after a fresh cold-create whose supervisor bringup "
        "failed, the running VM is still present on the provider (abandoned, "
        "not destroyed). It burns compute + disk quota and is never reaped in "
        "production (cleanup_orphans defaults to origin=test)."
    )
    # ...and its ref is cleared so a retry cold-creates cleanly from /vol.
    assert sess.state.sandbox_ref is None


@pytest.mark.asyncio
async def test_fresh_create_success_does_not_destroy(monkeypatch):
    """Guard: the cleanup must NOT fire on the happy path."""
    sess = DaytonaSandboxSession(
        session_id="s2",
        state=DaytonaSandboxState(recipe=Recipe(), sandbox_ref=None),
    )

    async def _fake_bootstrap():
        sess._volume_ref = "vol-1"
        sess._subpath = "sessions/s2"
        sess._spawn_env = {}

    monkeypatch.setattr(sess, "_bootstrap_session", _fake_bootstrap)

    live: dict[str, str] = {}

    async def _aget(ref):
        return SimpleNamespace(id=ref)

    async def _fake_create_sandbox(**k):
        from api.providers import ProviderInstance
        live["fresh-ref"] = "started"
        return ProviderInstance(provider="daytona", url="http://x", root="", sandbox_ref="fresh-ref")

    async def _fake_get_client():
        return SimpleNamespace(get=_aget)

    async def _fake_start_supervisor(*a, **k):
        return "http://supervisor"

    async def _fake_attach(self_=None):
        return None

    async def _fake_destroy(instance):
        live.pop(instance.sandbox_ref, None)

    monkeypatch.setattr(dt_provider, "create_sandbox", _fake_create_sandbox)
    monkeypatch.setattr(dt_provider, "_get_async_daytona_client", _fake_get_client)
    monkeypatch.setattr(dt_provider, "start_supervisor_in_sandbox", _fake_start_supervisor)
    monkeypatch.setattr(dt_provider, "destroy_daytona", _fake_destroy)
    monkeypatch.setattr(sess, "_attach_acp", _fake_attach)

    await sess.start()

    # Happy path: the VM stays live and the ref is kept.
    assert "fresh-ref" in live
    assert sess.state.sandbox_ref == "fresh-ref"
