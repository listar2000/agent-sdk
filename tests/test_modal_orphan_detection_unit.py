"""modal detect_orphan_sandboxes + the multi-provider orphan monitor.

The orphan monitor (read-only leak detector) only scanned daytona, so modal
leaked compute — including the untagged-supervisor leak class — was invisible
to the metrics/logs. modal now has a detect_orphan_sandboxes (read-only,
origin-scoped, never terminates) and the monitor scans both providers.

All mocked — no cloud, no DB.
"""
from __future__ import annotations

from types import SimpleNamespace

import pytest

pytestmark = pytest.mark.asyncio


def _coro(fn):
    """A modal-style method exposing ``.aio`` (async), mirroring synchronicity."""
    async def _a(*a, **k):
        return fn(*a, **k)
    return SimpleNamespace(aio=_a)


def _list_ns(items):
    """Fake ``modal.Sandbox.list`` whose ``.aio`` is an async generator."""
    def _aio(app_id=None):
        async def _gen():
            for sb in items:
                yield sb
        return _gen()
    return SimpleNamespace(aio=_aio)


def _sb(oid, *, tag, origin):
    tags = {}
    if tag:
        tags["agent-sdk.sandbox-id"] = tag
    if origin:
        tags["agent_sdk_origin"] = origin
    return SimpleNamespace(object_id=oid, get_tags=_coro(lambda: dict(tags)))


async def test_modal_detect_counts_only_our_origin_orphans(monkeypatch):
    from api.providers import modal as mmod

    monkeypatch.setenv("AGENT_SDK_ORIGIN", "test")
    sandboxes = [
        _sb("sb-orphan-a", tag="sb-orphan-a", origin="test"),   # ours, no row → orphan
        _sb("sb-orphan-b", tag="sb-orphan-b", origin="test"),   # ours, no row → orphan
        _sb("sb-live", tag="sb-live", origin="test"),           # ours, live → not orphan
        _sb("sb-prod", tag="sb-prod", origin="production"),     # other origin → ignore
        _sb("sb-untagged", tag=None, origin="test"),            # untagged → ignore
    ]
    # ``Sandbox.list.aio`` lists the WHOLE app (every origin); origin scoping is
    # the per-sandbox ``_ORIGIN_TAG`` check inside detect_orphan_sandboxes.
    fake_modal = SimpleNamespace(Sandbox=SimpleNamespace(list=_list_ns(sandboxes)))
    monkeypatch.setattr(mmod, "_require_modal", lambda: (fake_modal, None))

    async def _app():
        return SimpleNamespace(app_id="ap-test")

    monkeypatch.setattr(mmod, "_get_app", _app)

    report = await mmod.detect_orphan_sandboxes(live_refs={"sb-live"})

    orphan_ids = {oid for oid, _ in report["orphans"]}
    assert orphan_ids == {"sb-orphan-a", "sb-orphan-b"}, (
        "only our-origin, tagged, no-session-row sandboxes are orphans — never "
        "another origin's or an untagged sandbox")
    assert report["total_seen"] == 3, "total_seen = our-origin tagged sandboxes"
    assert report["capped"] is False
    # DETECTION ONLY — none of the fakes have a terminate(), so a reap attempt
    # would AttributeError; reaching here proves nothing was terminated.


async def test_modal_detect_never_queries_db_when_live_refs_passed(monkeypatch):
    """The monitor passes a shared live_refs — detect must not re-query."""
    from api.providers import modal as mmod

    monkeypatch.setenv("AGENT_SDK_ORIGIN", "test")
    fake_modal = SimpleNamespace(Sandbox=SimpleNamespace(list=_list_ns([])))
    monkeypatch.setattr(mmod, "_require_modal", lambda: (fake_modal, None))

    async def _app():
        return SimpleNamespace(app_id="ap-test")

    monkeypatch.setattr(mmod, "_get_app", _app)
    report = await mmod.detect_orphan_sandboxes(live_refs=set())
    assert report["total_seen"] == 0 and report["orphans"] == []


async def test_orphan_monitor_scans_both_providers(monkeypatch):
    """The monitor reports a leak metric per provider, sharing one live_refs."""
    from api.sandbox import orphan_monitor as om

    live_calls = {"n": 0}

    async def _detect_daytona(live_refs=None):
        return {"total_seen": 2, "orphans": [("d-1", "stopped")],
                "state_hist": {"stopped": 1}, "capped": False}

    async def _detect_modal(live_refs=None):
        return {"total_seen": 3, "orphans": [("m-1", ""), ("m-2", "")],
                "state_hist": {}, "capped": False}

    monkeypatch.setattr(om, "_detectors",
                        lambda: [("daytona", _detect_daytona), ("modal", _detect_modal)])

    recorded: list[dict] = []

    class _FakeMetrics:
        async def record_leak(self, kind, **kw):
            recorded.append({"kind": kind, **kw})

    import api.metrics as metrics
    monkeypatch.setattr(metrics, "get_metrics", lambda: _FakeMetrics())

    # Drive exactly one tick: sleep returns immediately, then cancel after.
    ticks = {"n": 0}

    async def _sleep(_s):
        ticks["n"] += 1
        if ticks["n"] > 1:
            raise __import__("asyncio").CancelledError()

    monkeypatch.setattr(om.asyncio, "sleep", _sleep)

    async def _live():
        live_calls["n"] += 1
        return {"d-live", "m-live"}

    import api.db as dbmod
    monkeypatch.setattr(dbmod, "live_sandbox_refs", _live)

    await om._orphan_monitor_loop()

    provs = {r["provider"] for r in recorded}
    assert provs == {"daytona", "modal"}, "both providers must report leaks"
    assert live_calls["n"] == 1, "live_sandbox_refs queried once, shared across providers"
