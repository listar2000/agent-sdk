"""Fix 1 — ``DaytonaSandboxSession._resolve_or_create_sandbox`` must DESTROY
the old VM when reattach fails, not abandon it.

This was the PRIMARY production flood (>1000 leaked daytona VMs). On reattach
failure (``restart_daytona_supervisor`` raises — errored / OOM / wedged VM),
the unfixed code set ``sandbox_ref = None`` and cold-created a replacement,
leaving the old VM labelled ``agent_sdk_origin`` for ``cleanup_orphans.py`` —
which defaults to ``origin=test`` so production was never reaped.

BEHAVIOURAL test: a fake daytona provider keeps a ``live`` registry of which
sandboxes still exist. We drive the real ``_resolve_or_create_sandbox`` and
assert the OBSERVABLE OUTCOME — the errored VM is no longer ``live`` (it was
destroyed, however that's done) and a replacement was cold-created — rather
than asserting any particular method was called. Pre-fix FAILS (the old VM is
still ``live`` — leaked); post-fix PASSES.

Run::

    .venv/bin/python -m pytest tests/test_daytona_reattach_destroys_old_vm.py -n auto
"""
from __future__ import annotations

import asyncio
import os
import sys
from types import SimpleNamespace

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from api.providers.daytona.session import DaytonaSandboxSession  # noqa: E402
from api.sandbox.state import DaytonaSandboxState, Recipe  # noqa: E402


@pytest.mark.asyncio
async def test_reattach_failure_destroys_old_vm():
    sess = DaytonaSandboxSession(
        session_id="s1",
        state=DaytonaSandboxState(recipe=Recipe(), sandbox_ref="old-dead-ref"),
    )
    # ``_bootstrap_session`` normally fills these from the DB volume row; set
    # them directly so we can call ``_resolve_or_create_sandbox`` in isolation.
    sess._volume_ref = "vol-1"
    sess._subpath = "sessions/s1"
    sess._spawn_env = {}

    # Observable provider state: which VMs still exist. The errored VM is here
    # at the start; the test's invariant is purely about its FINAL membership.
    live: dict[str, str] = {"old-dead-ref": "error"}

    async def _aget(ref):
        return SimpleNamespace(id=ref)

    class _FakeDt:
        async def restart_daytona_supervisor(self, *a, **k):
            # The errored / unreachable VM the prod flood left behind.
            raise RuntimeError("Failed to start sandbox: Sandbox is in an errored state")

        async def destroy_daytona(self, instance):
            # Whatever the recovery path chooses to do, "destroy" means the VM
            # is gone from the provider.
            live.pop(instance.sandbox_ref, None)

        async def create_sandbox(self, **k):
            from api.providers import ProviderInstance
            live["new-ref"] = "started"
            return ProviderInstance(provider="daytona", url="", root="", sandbox_ref="new-ref")

        async def _get_async_daytona_client(self):
            return SimpleNamespace(get=_aget)

    sandbox = await sess._resolve_or_create_sandbox(_FakeDt())

    # Recovery preserved: a replacement VM was cold-created.
    assert sandbox.id == "new-ref"
    assert "new-ref" in live

    # The destroy is fire-and-forget (so recovery isn't blocked on daytona's
    # up-to-60s delete-confirm) — yield to the loop so it settles.
    for _ in range(50):
        if "old-dead-ref" not in live:
            break
        await asyncio.sleep(0.01)

    # BEHAVIOURAL INVARIANT: the errored VM must no longer exist on the
    # provider. Pre-fix it is abandoned (still ``live``) and leaks against the
    # account disk quota with no automated prod reclaim.
    assert "old-dead-ref" not in live, (
        "FIX 1 — DAYTONA VM LEAK: after a reattach failure the errored VM is "
        "still present on the provider (abandoned, not destroyed). A labelled "
        "abandoned VM is never reaped in production (cleanup_orphans defaults "
        "to origin=test) — the >1000-VM flood."
    )
