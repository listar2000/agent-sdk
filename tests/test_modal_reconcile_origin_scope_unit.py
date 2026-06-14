"""modal reconcile_on_startup must not reap a different origin's sandbox.

Test and production share the Modal app, so ``Sandbox.list(app_id=...)`` returns
both. Without the origin check a test-origin server's boot reconcile would see
production sandboxes (their refs aren't in the test DB) and terminate them.
"""
from __future__ import annotations

from types import SimpleNamespace

import pytest


def _sb(oid, *, tag, origin):
    tags = {}
    if tag:
        tags["agent-sdk.sandbox-id"] = tag
    if origin:
        tags["agent_sdk_origin"] = origin
    return SimpleNamespace(object_id=oid, get_tags=lambda: dict(tags),
                           terminate=lambda: reaped.add(oid))


reaped: set[str] = set()


def _install(monkeypatch, sandboxes):
    from api import db as dbmod
    from api.providers import modal as mmod
    reaped.clear()
    monkeypatch.setenv("AGENT_SDK_ORIGIN", "test")
    fake_modal = SimpleNamespace(
        Sandbox=SimpleNamespace(list=lambda app_id=None: list(sandboxes)))
    monkeypatch.setattr(mmod, "_require_modal", lambda: (fake_modal, None))

    async def _app():
        return SimpleNamespace(app_id="ap")

    async def _live():
        return set()  # nothing live → everything that's ours is an orphan

    monkeypatch.setattr(mmod, "_get_app", _app)
    monkeypatch.setattr(dbmod, "live_sandbox_refs", _live)
    return mmod


@pytest.mark.asyncio
async def test_reconcile_skips_other_origin(monkeypatch):
    prod = _sb("sb-prod", tag="sb-prod", origin="production")
    mine = _sb("sb-mine", tag="sb-mine", origin="test")
    mmod = _install(monkeypatch, [prod, mine])

    await mmod.reconcile_on_startup()

    assert "sb-prod" not in reaped, (
        "test-origin reconcile terminated a PRODUCTION sandbox — cross-origin reap")
    assert "sb-mine" in reaped, "a same-origin orphan must still be reaped"
