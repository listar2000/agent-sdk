"""daytona get_sandbox_status must not destroy a healthy sandbox on a blip.

The recovery path (``supervisor_session.start``) treats a ``"error"`` status
as unrecoverable: it cold-creates a fresh sandbox and abandons the old one.
A transient ``client.get()`` failure (network blip / daytona 5xx / timeout)
must NOT be reported as ``"error"`` — it would needlessly destroy + replace a
live (or paused) sandbox. A *definitive* "not found"/404 still returns
``"missing"`` immediately (no retry), and a successful get that reports an
``error`` STATE is definitive too (not a transient fetch failure).

Symmetric with the modal fix (test_modal_status_transient_retry_unit). All
mocked — no cloud.
"""
from __future__ import annotations

from types import SimpleNamespace

import pytest

pytestmark = pytest.mark.asyncio


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch):
    import api.providers.daytona as dmod

    async def _fast_sleep(_s):
        return None

    monkeypatch.setattr(dmod.asyncio, "sleep", _fast_sleep)


def _client_with_get(get_fn):
    async def _get_client():
        return SimpleNamespace(get=get_fn)
    return _get_client


async def test_status_retries_transient_then_running(monkeypatch):
    import api.providers.daytona as dmod

    calls = {"n": 0}

    async def _get(ref):
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("transient 503 from control plane")
        return SimpleNamespace(state="started")

    monkeypatch.setattr(dmod, "_get_async_daytona_client", _client_with_get(_get))

    assert await dmod.get_daytona_sandbox_status("sb-1") == "running", (
        "a transient get() error must be retried, not reported as 'error' "
        "(which destroys + replaces a healthy sandbox)")
    assert calls["n"] == 2, "should have retried exactly once before succeeding"


async def test_status_not_found_short_circuits(monkeypatch):
    import api.providers.daytona as dmod

    calls = {"n": 0}

    async def _get(ref):
        calls["n"] += 1
        raise RuntimeError("Sandbox not found (404)")

    monkeypatch.setattr(dmod, "_get_async_daytona_client", _client_with_get(_get))

    assert await dmod.get_daytona_sandbox_status("sb-1") == "missing"
    assert calls["n"] == 1, "a definitive 'not found' must not burn the retry budget"


async def test_status_persistent_error_surfaces(monkeypatch):
    import api.providers.daytona as dmod

    calls = {"n": 0}

    async def _get(ref):
        calls["n"] += 1
        raise RuntimeError("connection reset")

    monkeypatch.setattr(dmod, "_get_async_daytona_client", _client_with_get(_get))

    assert await dmod.get_daytona_sandbox_status("sb-1") == "error"
    assert calls["n"] == dmod._DAYTONA_STATUS_ATTEMPTS, "should exhaust the budget"


async def test_status_error_state_not_retried(monkeypatch):
    """A successful get reporting state=='error' is definitive — return
    'error' at once, don't burn retries on a real errored sandbox."""
    import api.providers.daytona as dmod

    calls = {"n": 0}

    async def _get(ref):
        calls["n"] += 1
        return SimpleNamespace(state="error")

    monkeypatch.setattr(dmod, "_get_async_daytona_client", _client_with_get(_get))

    assert await dmod.get_daytona_sandbox_status("sb-1") == "error"
    assert calls["n"] == 1, "an errored STATE is not a transient fetch failure"


async def test_status_transitional_still_maps(monkeypatch):
    """Retry wrapper must not change the careful state mapping."""
    import api.providers.daytona as dmod

    async def _get(ref):
        return SimpleNamespace(state="stopping")

    monkeypatch.setattr(dmod, "_get_async_daytona_client", _client_with_get(_get))
    assert await dmod.get_daytona_sandbox_status("sb-1") == "stopped"
