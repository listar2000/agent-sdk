"""get_sandbox_status must emit per-provider op telemetry.

The status probe drives the recovery decision (reattach vs cold-create) and
now retries transient control-plane errors (#179/#181). Timing it under
``(provider, "sdk.status")`` makes that path observable — a status check that
suddenly takes seconds / retries is an early control-plane-trouble signal.
This was untimed on both providers; these tests pin the new telemetry.

Mocked (no cloud, no DB) — they capture the reporter's record_op call.
"""
from __future__ import annotations

from types import SimpleNamespace

import pytest

pytestmark = pytest.mark.asyncio


@pytest.fixture
def _capture_ops(monkeypatch):
    from api import metrics

    recorded: list[dict] = []

    async def _rec(**kw):
        recorded.append(kw)

    monkeypatch.setattr(metrics._REPORTER, "record_op", _rec)
    return recorded


async def test_daytona_status_emits_telemetry(monkeypatch, _capture_ops):
    from api.providers import daytona as dmod

    async def _client():
        async def _get(ref):
            return SimpleNamespace(state="started")
        return SimpleNamespace(get=_get)

    monkeypatch.setattr(dmod, "_get_async_daytona_client", _client)

    status = await dmod.get_daytona_sandbox_status("sb-1")
    assert status == "running"  # behavior preserved through the decorator

    ops = [r for r in _capture_ops if r.get("operation") == "sdk.status"]
    assert ops, "daytona status must record an sdk.status op"
    assert ops[-1]["provider"] == "daytona"
    assert ops[-1]["ok"] is True
    assert ops[-1]["duration_ms"] >= 0


async def test_modal_status_emits_telemetry(monkeypatch, _capture_ops):
    from api.providers import modal as mmod

    async def _poll_aio():
        return None  # running

    async def _lookup(ref):
        return SimpleNamespace(poll=SimpleNamespace(aio=_poll_aio))

    monkeypatch.setattr(mmod, "_lookup_sandbox", _lookup)

    status = await mmod.get_sandbox_status("sb-1")
    assert status == "running"

    ops = [r for r in _capture_ops if r.get("operation") == "sdk.status"]
    assert ops, "modal status must record an sdk.status op"
    assert ops[-1]["provider"] == "modal"
    assert ops[-1]["ok"] is True
