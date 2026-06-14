"""Unit tests for the daytona sandbox-handle exec cache.

The exec path (``exec_in_sandbox`` — the per-tool-call primitive for
native-on-daytona) used to ``daytona.get()`` a fresh sandbox handle before
EVERY exec — a redundant ~101 ms control-plane round-trip, since a handle is
fully reusable (it routes by sandbox id, verified live to survive a restart).
It now caches the handle. These pin: one ``get()`` across many execs; a broken
exec channel refreshes the handle and retries; delete evicts; the cache is
LRU-bounded so it can't leak across many sessions.
"""
from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest


def _inst(ref):
    from api.providers._shared import ProviderInstance as PI
    return PI(provider="daytona", url="", root="/tmp", sandbox_ref=ref)


@pytest.fixture(autouse=True)
def _clear_cache():
    from api.providers import daytona
    daytona._SANDBOX_HANDLE_CACHE.clear()
    yield
    daytona._SANDBOX_HANDLE_CACHE.clear()


@pytest.mark.asyncio
async def test_exec_caches_handle_one_get_across_many_execs(monkeypatch):
    from api.providers import daytona

    gets = {"n": 0}
    execs = {"n": 0}

    async def _exec(cmd, timeout=30):
        execs["n"] += 1
        return SimpleNamespace(result="ok", stderr="", exit_code=0)

    fake_sb = SimpleNamespace(id="sb-1", process=SimpleNamespace(exec=_exec))

    async def _get(ref):
        gets["n"] += 1
        return fake_sb

    monkeypatch.setattr(daytona, "_get_async_daytona_client",
                        AsyncMock(return_value=SimpleNamespace(get=_get)))

    for _ in range(4):
        r = await daytona.exec_in_sandbox(_inst("sb-1"), "echo x")
        assert r.exit_code == 0
    assert execs["n"] == 4
    # THE win: 4 execs, ONE control-plane get() (the per-exec get is gone)
    assert gets["n"] == 1, f"expected 1 get() across 4 execs, got {gets['n']}"


@pytest.mark.asyncio
async def test_exec_refreshes_handle_when_channel_raises(monkeypatch):
    from api.providers import daytona

    gets = {"n": 0}

    async def _exec_bad(cmd, timeout=30):
        raise ConnectionError("channel dead")

    async def _exec_good(cmd, timeout=30):
        return SimpleNamespace(result="ok", stderr="", exit_code=0)

    handles = [
        SimpleNamespace(id="sb-2", process=SimpleNamespace(exec=_exec_bad)),
        SimpleNamespace(id="sb-2", process=SimpleNamespace(exec=_exec_good)),
    ]

    async def _get(ref):
        h = handles[min(gets["n"], len(handles) - 1)]
        gets["n"] += 1
        return h

    monkeypatch.setattr(daytona, "_get_async_daytona_client",
                        AsyncMock(return_value=SimpleNamespace(get=_get)))

    r = await daytona.exec_in_sandbox(_inst("sb-2"), "echo x")
    assert r.exit_code == 0                  # succeeded after refresh
    assert gets["n"] == 2                     # first (bad) + refresh (good)


@pytest.mark.asyncio
async def test_evict_drops_cached_handle():
    from api.providers import daytona
    daytona._SANDBOX_HANDLE_CACHE["sb-3"] = object()
    daytona._evict_sandbox_handle("sb-3")
    assert "sb-3" not in daytona._SANDBOX_HANDLE_CACHE
    daytona._evict_sandbox_handle(None)       # no-op, no raise


@pytest.mark.asyncio
async def test_cache_is_lru_bounded(monkeypatch):
    from api.providers import daytona

    async def _get(ref):
        return SimpleNamespace(id=ref,
                               process=SimpleNamespace(exec=AsyncMock()))

    monkeypatch.setattr(daytona, "_get_async_daytona_client",
                        AsyncMock(return_value=SimpleNamespace(get=_get)))
    monkeypatch.setattr(daytona, "_SANDBOX_HANDLE_CACHE_MAX", 3)

    for i in range(5):
        await daytona._cached_sandbox_handle(f"sb-{i}")
    # bounded to the cap; oldest evicted
    assert len(daytona._SANDBOX_HANDLE_CACHE) == 3
    assert "sb-0" not in daytona._SANDBOX_HANDLE_CACHE
    assert "sb-4" in daytona._SANDBOX_HANDLE_CACHE
