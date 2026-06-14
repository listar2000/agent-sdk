"""Unit tests for the modal sandbox-handle exec cache.

``exec_in_sandbox`` (the per-tool-call exec primitive for native-on-modal) used
to resolve the handle via ``modal.Sandbox.from_id`` — a control-plane RPC
(``client.stub.SandboxWait``) — before EVERY exec. It now caches the handle.
These pin: one lookup across many execs; a broken channel re-resolves + retries;
a missing sandbox propagates; the cache is LRU-bounded.
"""
from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from api.providers._shared import ProviderInstance, SandboxMissingError


def _inst(ref):
    return ProviderInstance(provider="modal", url="", root="/tmp", sandbox_ref=ref)


def _fake_proc(rc=0, out="ok", err=""):
    return SimpleNamespace(
        wait=lambda timeout=None: rc,
        stdout=SimpleNamespace(read=lambda: out),
        stderr=SimpleNamespace(read=lambda: err),
    )


@pytest.fixture(autouse=True)
def _clear_cache():
    from api.providers import modal as m
    m._SANDBOX_HANDLE_CACHE.clear()
    yield
    m._SANDBOX_HANDLE_CACHE.clear()


@pytest.mark.asyncio
async def test_exec_caches_handle_one_lookup_across_many_execs(monkeypatch):
    from api.providers import modal as m

    lookups = {"n": 0}
    fake_sb = SimpleNamespace(exec=lambda *a, **k: _fake_proc(rc=0, out="x"))

    async def _lookup(ref):
        lookups["n"] += 1
        return fake_sb

    monkeypatch.setattr(m, "_lookup_sandbox", _lookup)

    for _ in range(4):
        r = await m.exec_in_sandbox(_inst("sb-1"), "echo x")
        assert r.exit_code == 0 and "x" in r.stdout
    # THE win: 4 execs, ONE from_id lookup (the per-exec RPC is gone)
    assert lookups["n"] == 1, f"expected 1 lookup across 4 execs, got {lookups['n']}"


@pytest.mark.asyncio
async def test_exec_refreshes_handle_when_channel_raises(monkeypatch):
    from api.providers import modal as m

    lookups = {"n": 0}

    def _bad_exec(*a, **k):
        raise ConnectionError("channel dead")

    handles = [
        SimpleNamespace(exec=_bad_exec),
        SimpleNamespace(exec=lambda *a, **k: _fake_proc(rc=0, out="ok")),
    ]

    async def _lookup(ref):
        h = handles[min(lookups["n"], len(handles) - 1)]
        lookups["n"] += 1
        return h

    monkeypatch.setattr(m, "_lookup_sandbox", _lookup)

    r = await m.exec_in_sandbox(_inst("sb-2"), "echo x")
    assert r.exit_code == 0
    assert lookups["n"] == 2  # first (bad) + refresh (good)


@pytest.mark.asyncio
async def test_exec_missing_sandbox_propagates(monkeypatch):
    from api.providers import modal as m

    async def _lookup(ref):
        raise SandboxMissingError("gone")

    monkeypatch.setattr(m, "_lookup_sandbox", _lookup)
    with pytest.raises(SandboxMissingError):
        await m.exec_in_sandbox(_inst("sb-3"), "echo x")


@pytest.mark.asyncio
async def test_cache_is_lru_bounded(monkeypatch):
    from api.providers import modal as m

    async def _lookup(ref):
        return SimpleNamespace(id=ref)

    monkeypatch.setattr(m, "_lookup_sandbox", _lookup)
    monkeypatch.setattr(m, "_SANDBOX_HANDLE_CACHE_MAX", 3)
    for i in range(5):
        await m._cached_sandbox_handle(f"sb-{i}")
    assert len(m._SANDBOX_HANDLE_CACHE) == 3
    assert "sb-0" not in m._SANDBOX_HANDLE_CACHE and "sb-4" in m._SANDBOX_HANDLE_CACHE
