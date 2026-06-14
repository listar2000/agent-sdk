"""The daytona delete-confirm poll must back off, not hammer the control plane.

``_daytona_sandbox_op(op="delete")`` waits for the sandbox to leave daytona's
index by polling ``daytona.get`` until it 404s. Each probe is a control-plane
GET, and the control plane is daytona's scaling ceiling. A fixed 0.5s interval
burns ~20-120 GETs per delete; deletes fire per session-lifecycle AND on every
recovery cleanup (reattach-fallback, fresh-create teardown), so under churn that
floods the very bottleneck. The poll uses exponential backoff (0.5s →×1.5→ 5s
cap) — same 60s budget, ~6x fewer GETs.

BEHAVIOURAL test: drive a delete whose sandbox goes 404 after a few polls, with
``asyncio.sleep`` mocked to record the intervals. The intervals must strictly
increase (backoff). Pre-fix (fixed 0.5s) they're constant → FAILS.
"""
from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest


class _FakeClient:
    def __init__(self, raise_on_get_call: int):
        self.get_calls = 0
        self.raise_on = raise_on_get_call
        self.deleted = False

    async def get(self, ref):
        self.get_calls += 1
        if self.get_calls >= self.raise_on:
            raise RuntimeError("404 sandbox not found")  # gone
        return SimpleNamespace(id=ref)

    async def delete(self, sb):
        self.deleted = True


@pytest.mark.asyncio
async def test_delete_confirm_uses_backoff(monkeypatch):
    from api.providers import daytona as dtmod
    from api.providers import ProviderInstance

    # get() #1 is the initial fetch; the poll then calls get() until it 404s.
    # 404 on the 6th call → 4 poll sleeps before the sandbox is seen gone.
    client = _FakeClient(raise_on_get_call=6)

    async def _fake_client():
        return client

    monkeypatch.setattr(dtmod, "_get_async_daytona_client", _fake_client)

    sleeps: list[float] = []

    async def _fake_sleep(d):
        sleeps.append(d)

    monkeypatch.setattr(asyncio, "sleep", _fake_sleep)

    inst = ProviderInstance(provider="daytona", url="", root="", sandbox_ref="sb-x")
    await dtmod._daytona_sandbox_op(inst, "delete")

    # Correctness preserved: it deleted and polled to confirmation.
    assert client.deleted is True
    assert len(sleeps) >= 3, f"expected several confirm polls, got {sleeps}"
    # The fix: each confirm interval is strictly larger than the previous.
    for a, b in zip(sleeps, sleeps[1:]):
        assert b > a, f"delete-confirm poll must back off; intervals were {sleeps}"
    assert sleeps[0] == pytest.approx(0.5)
    # And it never exceeds the cap.
    assert max(sleeps) <= 5.0


@pytest.mark.asyncio
async def test_delete_confirm_breaks_when_gone(monkeypatch):
    """Guard: the poll stops as soon as the sandbox is gone (correctness)."""
    from api.providers import daytona as dtmod
    from api.providers import ProviderInstance

    client = _FakeClient(raise_on_get_call=2)  # gone immediately after delete

    async def _fake_client():
        return client

    monkeypatch.setattr(dtmod, "_get_async_daytona_client", _fake_client)

    async def _fake_sleep(d):
        pass

    monkeypatch.setattr(asyncio, "sleep", _fake_sleep)

    inst = ProviderInstance(provider="daytona", url="", root="", sandbox_ref="sb-x")
    await dtmod._daytona_sandbox_op(inst, "delete")
    assert client.deleted is True
    # initial get (#1) + one poll get (#2, which 404s) — no wasted polling
    assert client.get_calls == 2
