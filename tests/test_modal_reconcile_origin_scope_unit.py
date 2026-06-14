"""modal reconcile_on_startup must NOT reap a sandbox from a different origin.

Test and production share the Modal app (``_APP_NAME = "agent-sdk"``), so
``Sandbox.list(app_id=...)`` returns sandboxes from BOTH origins. Daytona's
reconcile filters its list by the ``agent_sdk_origin`` label; Modal's list
can't, so it must filter per-sandbox by the ``_ORIGIN_TAG``. Without that, a
test-origin server's boot reconcile sees production sandboxes (whose refs aren't
in the test DB's ``live_sandbox_refs``), classifies them as orphans, and
TERMINATES them — a test deploy wiping production compute. This is the Modal
analogue of the daytona staging↔prod origin-collision incident.

BEHAVIOURAL test: a production-origin sandbox, reconcile running as origin=test,
ref absent from live_refs. It must NOT be terminated. Pre-fix (origin-blind) it
IS terminated. Plus a guard: a same-origin orphan is still reaped.
"""
from __future__ import annotations

from types import SimpleNamespace

import pytest


class _FakeSb:
    def __init__(self, oid, ref_tag, origin):
        self.object_id = oid
        from api.providers.modal import _TAG_KEY, _ORIGIN_TAG
        self._tags = {_TAG_KEY: ref_tag, _ORIGIN_TAG: origin}
        self.terminated = False

    def get_tags(self):
        return self._tags

    def terminate(self):
        self.terminated = True


def _install_modal(monkeypatch, sandboxes, live_refs):
    from api import db as dbmod
    from api.providers import modal as modalmod

    class _FakeSandboxNS:
        @staticmethod
        def list(app_id=None, tags=None):
            # Mirror the real Sandbox.list: "only Sandboxes that have at least
            # those tags are returned." Origin scoping happens HERE (server
            # side), so a reconcile that fails to pass the origin tag would
            # (wrongly) see every origin's sandboxes.
            out = []
            for sb in sandboxes:
                if tags is None or all(sb._tags.get(k) == v for k, v in tags.items()):
                    out.append(sb)
            return out

    class _FakeModal:
        Sandbox = _FakeSandboxNS

    monkeypatch.setattr(modalmod, "_require_modal", lambda: (_FakeModal, None))

    async def _fake_app():
        return SimpleNamespace(app_id="app-1")

    monkeypatch.setattr(modalmod, "_get_app", _fake_app)

    async def _live_refs():
        return set(live_refs)

    monkeypatch.setattr(dbmod, "live_sandbox_refs", _live_refs)
    return modalmod


@pytest.mark.asyncio
async def test_modal_reconcile_skips_other_origin(monkeypatch):
    monkeypatch.setenv("AGENT_SDK_ORIGIN", "test")
    # A PRODUCTION sandbox — not in the test server's live_refs.
    prod_sb = _FakeSb("sb-prod", "sb-prod", "production")
    modalmod = _install_modal(monkeypatch, [prod_sb], set())

    await modalmod.reconcile_on_startup()

    assert prod_sb.terminated is False, (
        "test-origin reconcile TERMINATED a production-origin Modal sandbox — "
        "cross-origin reap (a test deploy wiping prod compute)"
    )


@pytest.mark.asyncio
async def test_modal_reconcile_reaps_same_origin_orphan(monkeypatch):
    monkeypatch.setenv("AGENT_SDK_ORIGIN", "test")
    orphan = _FakeSb("sb-orphan", "sb-orphan", "test")
    modalmod = _install_modal(monkeypatch, [orphan], set())

    await modalmod.reconcile_on_startup()

    assert orphan.terminated is True, "a same-origin orphan must still be reaped"
