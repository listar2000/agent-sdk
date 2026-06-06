"""Unit tests for the daytona orphan-DETECTION monitor.

Mirrors the other daytona unit tests: a mocked async client + mocked
``db.live_sandbox_refs``, no live daytona. Asserts the orphan set, the state
histogram, the capped flag, and that detection mode deletes NOTHING.
"""
from __future__ import annotations

import os
import sys
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

_SRC = os.path.join(os.path.dirname(__file__), "..", "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)


def _sb(sid, state="started"):
    return SimpleNamespace(id=sid, state=state)


def _wire(monkeypatch, *, list_obj=None, list_fn=None, live_refs=()):
    """Patch the async daytona client (.list + .delete) and db.live_sandbox_refs.
    Returns the .delete AsyncMock so a test can assert it was never called."""
    from api.providers import daytona
    import api.db as dbmod

    delete = AsyncMock()
    _list = AsyncMock(side_effect=list_fn) if list_fn else AsyncMock(return_value=list_obj)
    client = SimpleNamespace(list=_list, delete=delete)
    monkeypatch.setattr(daytona, "_get_async_daytona_client", AsyncMock(return_value=client))
    monkeypatch.setattr(dbmod, "live_sandbox_refs", AsyncMock(return_value=set(live_refs)))
    return daytona, delete


@pytest.mark.asyncio
async def test_orphans_are_refs_with_no_session_row(monkeypatch):
    page = SimpleNamespace(
        items=[_sb("live-1"), _sb("orphan-1", "stopped"), _sb("orphan-2", "error")],
        total_pages=1,
    )
    daytona, delete = _wire(monkeypatch, list_obj=page, live_refs={"live-1"})

    report = await daytona.detect_orphan_sandboxes()

    assert report["total_seen"] == 3
    assert {sid for sid, _ in report["orphans"]} == {"orphan-1", "orphan-2"}
    assert report["state_hist"] == {"stopped": 1, "error": 1}
    assert report["capped"] is False
    delete.assert_not_called()  # detection-only NEVER deletes


@pytest.mark.asyncio
async def test_skips_null_id_and_normalizes_enum_state(monkeypatch):
    page = SimpleNamespace(
        items=[_sb(None), SimpleNamespace(id="o-1", state=SimpleNamespace(value="STOPPED"))],
        total_pages=1,
    )
    daytona, delete = _wire(monkeypatch, list_obj=page, live_refs=set())

    report = await daytona.detect_orphan_sandboxes()

    assert [sid for sid, _ in report["orphans"]] == ["o-1"]
    assert report["state_hist"] == {"stopped": 1}  # .value, lower()
    delete.assert_not_called()


@pytest.mark.asyncio
async def test_pagination_cap_sets_capped(monkeypatch):
    from api.providers import daytona as d
    monkeypatch.setattr(d, "_DAYTONA_MAX_PAGES", 2)

    def _page(labels=None, page=1):
        return SimpleNamespace(items=[_sb(f"o-{page}")], total_pages=5)

    daytona, delete = _wire(monkeypatch, list_fn=_page, live_refs=set())

    report = await daytona.detect_orphan_sandboxes()

    assert report["capped"] is True
    assert report["total_seen"] == 2  # only 2 pages walked
    delete.assert_not_called()


@pytest.mark.asyncio
async def test_explicit_live_refs_skips_db(monkeypatch):
    from api.providers import daytona
    import api.db as dbmod
    page = SimpleNamespace(items=[_sb("o-1")], total_pages=1)
    monkeypatch.setattr(
        daytona, "_get_async_daytona_client",
        AsyncMock(return_value=SimpleNamespace(list=AsyncMock(return_value=page), delete=AsyncMock())),
    )
    db_spy = AsyncMock(return_value=set())
    monkeypatch.setattr(dbmod, "live_sandbox_refs", db_spy)

    report = await daytona.detect_orphan_sandboxes(live_refs=set())

    assert [sid for sid, _ in report["orphans"]] == ["o-1"]
    db_spy.assert_not_called()  # caller-supplied live_refs => no DB hit
