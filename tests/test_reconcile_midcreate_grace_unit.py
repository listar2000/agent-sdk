"""reconcile_on_startup must NOT reap a sibling replica's in-flight create.

The race (multi-replica): replica A is mid-create — the sandbox is already
created + tagged/labelled on the provider, but A hasn't committed its
``sandbox_ref`` to the DB yet (the pool persists it only after start() finishes:
supervisor boot + ACP attach, which is seconds). Replica B boots and runs
``reconcile_on_startup``, which lists account-global sandboxes and reaps any not
in ``live_sandbox_refs()``. Without a guard, B reaps A's in-flight sandbox →
A's create breaks. During a rolling deploy (replicas booting while peers serve
creates) this fires repeatedly.

Fix: a two-pass grace — collect orphan CANDIDATES, wait, re-query live sessions,
and reap only candidates STILL orphaned. An in-flight create commits its ref
during the grace and is spared.

BEHAVIOURAL test: ``live_sandbox_refs`` returns empty on pass 1 and the ref on
pass 2 (the commit landing during the grace). The provider must NOT reap it.
Pre-fix (single pass) FAILS — it reaps on the one query. Plus a guard: a
genuine orphan (never committed) is still reaped.
"""
from __future__ import annotations

from types import SimpleNamespace

import pytest


# --------------------------------------------------------------------------- #
# modal
# --------------------------------------------------------------------------- #

class _FakeSb:
    def __init__(self, oid, ref_tag):
        self.object_id = oid
        from api.providers.modal import _TAG_KEY, _ORIGIN_TAG
        self._tags = {_TAG_KEY: ref_tag, _ORIGIN_TAG: "test"}
        self.terminated = False

    def get_tags(self):
        return self._tags

    def terminate(self):
        self.terminated = True


def _install_modal(monkeypatch, sandboxes, live_refs_seq):
    from api import db as dbmod
    from api.providers import modal as modalmod

    class _FakeSandboxNS:
        @staticmethod
        def list(app_id=None):
            return list(sandboxes)

    class _FakeModal:
        Sandbox = _FakeSandboxNS

    monkeypatch.setattr(modalmod, "_require_modal", lambda: (_FakeModal, None))

    async def _fake_app():
        return SimpleNamespace(app_id="app-1")

    monkeypatch.setattr(modalmod, "_get_app", _fake_app)
    monkeypatch.setattr(modalmod, "_RECONCILE_MIDCREATE_GRACE_S", 0)

    calls = {"n": 0}

    async def _live_refs():
        i = min(calls["n"], len(live_refs_seq) - 1)
        calls["n"] += 1
        return set(live_refs_seq[i])

    monkeypatch.setattr(dbmod, "live_sandbox_refs", _live_refs)
    return modalmod, calls


@pytest.mark.asyncio
async def test_modal_reconcile_spares_midcreate(monkeypatch):
    sb = _FakeSb("sb-mid", "sb-mid")
    # empty on pass 1, committed on pass 2 (ref landed during the grace)
    modalmod, calls = _install_modal(monkeypatch, [sb], [set(), {"sb-mid"}])

    await modalmod.reconcile_on_startup()

    assert sb.terminated is False, (
        "reconcile reaped a mid-create modal sandbox that committed its "
        "sandbox_ref during the grace — a peer replica's in-flight create"
    )
    assert calls["n"] == 2, "reconcile must re-query live_sandbox_refs after the grace"


@pytest.mark.asyncio
async def test_modal_reconcile_reaps_genuine_orphan(monkeypatch):
    sb = _FakeSb("sb-orphan", "sb-orphan")
    # never in live_refs — a real orphan
    modalmod, _ = _install_modal(monkeypatch, [sb], [set(), set()])

    await modalmod.reconcile_on_startup()

    assert sb.terminated is True, "reconcile must still reap a genuine orphan"


# --------------------------------------------------------------------------- #
# daytona
# --------------------------------------------------------------------------- #

def _install_daytona(monkeypatch, items, live_refs_seq):
    from api import db as dbmod
    from api.providers import daytona as dtmod

    deleted = []

    class _FakeClient:
        async def delete(self, sb):
            deleted.append(sb.id)

    async def _fake_client():
        return _FakeClient()

    async def _fake_list(client, labels):
        return (list(items), False)

    monkeypatch.setattr(dtmod, "_get_async_daytona_client", _fake_client)
    monkeypatch.setattr(dtmod, "_list_labeled_sandboxes", _fake_list)
    monkeypatch.setattr(dtmod, "_RECONCILE_MIDCREATE_GRACE_S", 0)

    calls = {"n": 0}

    async def _live_refs():
        i = min(calls["n"], len(live_refs_seq) - 1)
        calls["n"] += 1
        return set(live_refs_seq[i])

    monkeypatch.setattr(dbmod, "live_sandbox_refs", _live_refs)
    return dtmod, deleted, calls


@pytest.mark.asyncio
async def test_daytona_reconcile_spares_midcreate(monkeypatch):
    sb = SimpleNamespace(id="sb-mid")
    dtmod, deleted, calls = _install_daytona(monkeypatch, [sb], [set(), {"sb-mid"}])

    await dtmod.reconcile_on_startup()

    assert deleted == [], (
        "reconcile deleted a mid-create daytona sandbox that committed its "
        "sandbox_ref during the grace — a peer replica's in-flight create"
    )
    assert calls["n"] == 2, "reconcile must re-query live_sandbox_refs after the grace"


@pytest.mark.asyncio
async def test_daytona_reconcile_reaps_genuine_orphan(monkeypatch):
    sb = SimpleNamespace(id="sb-orphan")
    dtmod, deleted, _ = _install_daytona(monkeypatch, [sb], [set(), set()])

    await dtmod.reconcile_on_startup()

    assert deleted == ["sb-orphan"], "reconcile must still reap a genuine orphan"
