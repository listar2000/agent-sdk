"""Boot-reconcile orphan reclaim must fan out, not serialise N RTTs.

``reconcile_on_startup`` (daytona + modal) reclaims sandboxes that no live
session row owns. A crash can strand many orphans at once, and reclaiming
them one blocking control-plane call at a time serialises N round-trips on
the server's startup path:

  * daytona: ``await daytona.delete(sb)`` per orphan
  * modal:   ``get_tags`` per sandbox THEN ``terminate`` per orphan (2N)

Both now fan out via ``_shared.bounded_gather`` (bounded so we parallelise
without stampeding the shared client pool). These mocked tests assert the
reclaim actually runs concurrently — they are RED on the old sequential
loops (max in-flight stays 1) and GREEN once the fan-out lands.

All mocked; no live cloud. Fast.
"""
from __future__ import annotations

import asyncio
import threading
import time
from types import SimpleNamespace

import pytest

pytestmark = pytest.mark.asyncio


# ---------------------------------------------------------------------------
# bounded_gather — the shared primitive
# ---------------------------------------------------------------------------

async def test_bounded_gather_caps_concurrency():
    from api.providers._shared import bounded_gather

    in_flight = 0
    max_seen = 0

    async def _task():
        nonlocal in_flight, max_seen
        in_flight += 1
        max_seen = max(max_seen, in_flight)
        await asyncio.sleep(0.02)
        in_flight -= 1
        return "ok"

    out = await bounded_gather([_task() for _ in range(10)], limit=3)
    assert out == ["ok"] * 10
    assert max_seen <= 3, f"limit=3 must cap in-flight at 3, saw {max_seen}"
    assert max_seen >= 2, "with 10 tasks and limit 3, tasks must overlap"


async def test_bounded_gather_isolates_failures_and_keeps_order():
    from api.providers._shared import bounded_gather

    async def _ok(i):
        await asyncio.sleep(0)
        return i

    async def _boom(i):
        raise ValueError(f"boom-{i}")

    out = await bounded_gather([_ok(0), _boom(1), _ok(2)], limit=8)
    assert out[0] == 0 and out[2] == 2
    assert isinstance(out[1], ValueError), "a failure must not cancel siblings"


async def test_bounded_gather_empty():
    from api.providers._shared import bounded_gather
    assert await bounded_gather([]) == []


# ---------------------------------------------------------------------------
# daytona reconcile
# ---------------------------------------------------------------------------

async def test_daytona_reconcile_reaps_orphans_concurrently(monkeypatch):
    from api import db as dbmod
    from api.providers import daytona as dmod

    n = 8
    sandboxes = [SimpleNamespace(id=f"sandbox-{i:02d}") for i in range(n)]

    state = {"in_flight": 0, "max": 0}
    deleted: set[str] = set()

    async def _delete(sb):
        state["in_flight"] += 1
        state["max"] = max(state["max"], state["in_flight"])
        await asyncio.sleep(0.02)
        deleted.add(sb.id)
        state["in_flight"] -= 1

    async def _client():
        return SimpleNamespace(delete=_delete)

    async def _list(daytona, labels):
        return sandboxes, False

    async def _live():
        return set()  # nothing live → every sandbox is an orphan

    monkeypatch.setattr(dmod, "_get_async_daytona_client", _client)
    monkeypatch.setattr(dmod, "_list_labeled_sandboxes", _list)
    monkeypatch.setattr(dbmod, "live_sandbox_refs", _live)

    await dmod.reconcile_on_startup()

    assert deleted == {sb.id for sb in sandboxes}, "every orphan must be reaped"
    assert state["max"] >= 2, (
        f"daytona reconcile reclaimed orphans sequentially (max in-flight "
        f"{state['max']}) — boot reclaim must fan out so a crash that strands "
        f"many orphans doesn't serialise N control-plane RTTs")


async def test_daytona_reconcile_skips_live_sandboxes(monkeypatch):
    """Fan-out must not change the live/orphan classification."""
    from api import db as dbmod
    from api.providers import daytona as dmod

    sandboxes = [SimpleNamespace(id=f"s-{i}") for i in range(4)]
    deleted: set[str] = set()

    async def _delete(sb):
        deleted.add(sb.id)

    async def _client():
        return SimpleNamespace(delete=_delete)

    async def _list(daytona, labels):
        return sandboxes, False

    async def _live():
        return {"s-1", "s-3"}

    monkeypatch.setattr(dmod, "_get_async_daytona_client", _client)
    monkeypatch.setattr(dmod, "_list_labeled_sandboxes", _list)
    monkeypatch.setattr(dbmod, "live_sandbox_refs", _live)

    await dmod.reconcile_on_startup()
    assert deleted == {"s-0", "s-2"}, "live sandboxes must be left untouched"


async def test_daytona_utility_reaper_destroys_concurrently(monkeypatch):
    """The idle utility-sandbox reaper must fan out its destroys so one slow
    teardown doesn't stall the rest of the batch (or the reaper tick)."""
    from api.providers import daytona as dmod

    n = 6
    state = {"in_flight": 0, "max": 0}
    destroyed: set[str] = set()

    async def _destroy(inst):
        state["in_flight"] += 1
        state["max"] = max(state["max"], state["in_flight"])
        await asyncio.sleep(0.02)
        destroyed.add(inst.sandbox_ref)
        state["in_flight"] -= 1

    monkeypatch.setattr(dmod, "destroy_daytona", _destroy)

    stale = [(f"vol-{i}", SimpleNamespace(sandbox_ref=f"vol-{i}")) for i in range(n)]
    await dmod._reap_stale_utility(stale)

    assert destroyed == {f"vol-{i}" for i in range(n)}, "all stale must be reaped"
    assert state["max"] >= 2, (
        f"utility reaper destroyed sequentially (max in-flight {state['max']}) "
        f"— a single slow destroy must not block reaping the rest")


# ---------------------------------------------------------------------------
# modal reconcile
# ---------------------------------------------------------------------------

async def test_modal_reconcile_scans_and_reaps_concurrently(monkeypatch):
    from api import db as dbmod
    from api.providers import modal as mmod
    from api.providers.modal import _TAG_KEY, _ORIGIN_TAG

    monkeypatch.setenv("AGENT_SDK_ORIGIN", "test")
    n = 8
    tag_state = {"in_flight": 0, "max": 0}
    lock = threading.Lock()
    terminated: set[str] = set()

    def _make_sb(i):
        oid = f"sb-{i:02d}"

        def _get_tags():
            with lock:
                tag_state["in_flight"] += 1
                tag_state["max"] = max(tag_state["max"], tag_state["in_flight"])
            time.sleep(0.02)
            with lock:
                tag_state["in_flight"] -= 1
            return {_TAG_KEY: oid, _ORIGIN_TAG: "test"}

        def _terminate():
            terminated.add(oid)

        return SimpleNamespace(object_id=oid, get_tags=_get_tags,
                               terminate=_terminate)

    sandboxes = [_make_sb(i) for i in range(n)]

    fake_modal = SimpleNamespace(
        Sandbox=SimpleNamespace(list=lambda app_id=None: list(sandboxes)))

    monkeypatch.setattr(mmod, "_require_modal", lambda: (fake_modal, None))

    async def _app():
        return SimpleNamespace(app_id="ap-test")

    async def _live():
        return set()  # all orphans

    monkeypatch.setattr(mmod, "_get_app", _app)
    monkeypatch.setattr(dbmod, "live_sandbox_refs", _live)

    await mmod.reconcile_on_startup()

    assert terminated == {sb.object_id for sb in sandboxes}, "all orphans reaped"
    assert tag_state["max"] >= 2, (
        f"modal reconcile scanned tags sequentially (max in-flight "
        f"{tag_state['max']}) — each get_tags is an RTT, so the scan must "
        f"fan out across sandboxes at boot")


async def test_modal_reconcile_leaves_live_and_untagged(monkeypatch):
    from api import db as dbmod
    from api.providers import modal as mmod
    from api.providers.modal import _TAG_KEY, _ORIGIN_TAG

    monkeypatch.setenv("AGENT_SDK_ORIGIN", "test")
    terminated: set[str] = set()

    def _make_sb(oid, tag, origin="test"):
        tags = {}
        if tag:
            tags[_TAG_KEY] = tag
        if origin:
            tags[_ORIGIN_TAG] = origin
        return SimpleNamespace(
            object_id=oid,
            get_tags=lambda: dict(tags),
            terminate=lambda: terminated.add(oid))

    sandboxes = [
        _make_sb("sb-orphan", "sb-orphan"),               # ours, not live → reap
        _make_sb("sb-live", "sb-live"),                   # ours, live → keep
        _make_sb("sb-untagged", None),                    # untagged → keep
        _make_sb("sb-prod", "sb-prod", origin="production"),  # other origin → keep
    ]
    fake_modal = SimpleNamespace(
        Sandbox=SimpleNamespace(list=lambda app_id=None: list(sandboxes)))
    monkeypatch.setattr(mmod, "_require_modal", lambda: (fake_modal, None))

    async def _app():
        return SimpleNamespace(app_id="ap-test")

    async def _live():
        return {"sb-live"}

    monkeypatch.setattr(mmod, "_get_app", _app)
    monkeypatch.setattr(dbmod, "live_sandbox_refs", _live)

    await mmod.reconcile_on_startup()
    assert terminated == {"sb-orphan"}, (
        "only our-origin, tagged, non-live sandboxes may be terminated — never "
        "an untagged or cross-origin (e.g. production) sandbox")
