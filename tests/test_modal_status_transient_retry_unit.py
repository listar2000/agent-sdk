"""modal get_sandbox_status must not destroy a healthy sandbox on a blip.

The recovery path (``supervisor_session.start``) treats a ``"error"`` status
as unrecoverable: it cold-creates a fresh sandbox and orphans the old one.
Modal's status probe goes through ``from_id`` (``SandboxWait``, high-variance)
+ ``poll`` (an RPC), so a single transient control-plane error must NOT be
reported as ``"error"`` — it would needlessly destroy + recreate a live
sandbox. A *definitive* ``SandboxMissingError`` still returns ``"missing"``
immediately (no retry).

These tests pin: transient errors are retried (then succeed), a missing
record short-circuits without burning the retry budget, and persistent
errors still surface as ``"error"``. All mocked — no cloud.
"""
from __future__ import annotations

from types import SimpleNamespace

import pytest

pytestmark = pytest.mark.asyncio


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch):
    """Skip the inter-attempt backoff so the tests are instant."""
    import api.providers.modal as mmod

    async def _fast_sleep(_s):
        return None

    monkeypatch.setattr(mmod.asyncio, "sleep", _fast_sleep)


async def test_status_retries_transient_then_running(monkeypatch):
    import api.providers.modal as mmod

    calls = {"n": 0}

    async def _lookup(ref):
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("transient SandboxWait blip")
        return SimpleNamespace(poll=lambda: None)  # running

    monkeypatch.setattr(mmod, "_lookup_sandbox", _lookup)

    status = await mmod.get_sandbox_status("sb-123")
    assert status == "running", (
        "a transient control-plane error must be retried, not reported as "
        "'error' (which destroys + recreates a healthy sandbox)")
    assert calls["n"] == 2, "should have retried exactly once before succeeding"


async def test_status_missing_short_circuits(monkeypatch):
    import api.providers.modal as mmod
    from api.providers._shared import SandboxMissingError

    calls = {"n": 0}

    async def _lookup(ref):
        calls["n"] += 1
        raise SandboxMissingError("gone")

    monkeypatch.setattr(mmod, "_lookup_sandbox", _lookup)

    status = await mmod.get_sandbox_status("sb-123")
    assert status == "missing"
    assert calls["n"] == 1, "a definitive 'missing' must not burn the retry budget"


async def test_status_persistent_error_surfaces(monkeypatch):
    import api.providers.modal as mmod

    calls = {"n": 0}

    async def _lookup(ref):
        calls["n"] += 1
        raise RuntimeError("control plane down")

    monkeypatch.setattr(mmod, "_lookup_sandbox", _lookup)

    status = await mmod.get_sandbox_status("sb-123")
    assert status == "error", "a persistent error must still surface as 'error'"
    assert calls["n"] == mmod._STATUS_PROBE_ATTEMPTS, "should exhaust the retry budget"


async def test_status_retries_transient_poll_error(monkeypatch):
    """A poll() (not from_id) blip is also retried."""
    import api.providers.modal as mmod

    state = {"n": 0}

    def _poll():
        state["n"] += 1
        if state["n"] == 1:
            raise RuntimeError("poll RPC blip")
        return None  # running

    async def _lookup(ref):
        return SimpleNamespace(poll=_poll)

    monkeypatch.setattr(mmod, "_lookup_sandbox", _lookup)

    assert await mmod.get_sandbox_status("sb-123") == "running"
    assert state["n"] == 2
