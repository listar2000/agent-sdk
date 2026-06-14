"""modal tunnel fetch is async (sb.tunnels.aio), not asyncio.to_thread.

``sb.tunnels(60)`` WAITS up to 60s for the tunnel to become ready — it was
run via asyncio.to_thread, holding a threadpool worker that whole time. Under
a burst of ACP session creates / recoveries that capped at the shared
threadpool. Async (.aio) frees the worker. This pins resolve_supervisor_url
uses the .aio variant (create_sandbox uses the same call, verified live).

Mocked — no cloud.
"""
from __future__ import annotations

from types import SimpleNamespace

import pytest

pytestmark = pytest.mark.asyncio


class _AioCallable:
    def __init__(self, result=None, exc=None):
        self.result = result
        self.exc = exc
        self.sync_called = False
        self.aio_called = False

    def __call__(self, *a, **k):
        self.sync_called = True
        if self.exc:
            raise self.exc
        return self.result

    async def aio(self, *a, **k):
        self.aio_called = True
        if self.exc:
            raise self.exc
        return self.result


async def test_resolve_supervisor_url_uses_async_tunnels(monkeypatch):
    import api.providers.modal as mmod
    from api.providers.modal import _SUPERVISOR_CONTAINER_PORT

    tunnels = _AioCallable(
        result={_SUPERVISOR_CONTAINER_PORT: SimpleNamespace(url="https://sup.modal.host/")})
    fake_sb = SimpleNamespace(tunnels=tunnels)

    async def _lookup(ref):
        return fake_sb

    monkeypatch.setattr(mmod, "_lookup_sandbox", _lookup)

    url = await mmod.resolve_supervisor_url("sb-1")
    assert url == "https://sup.modal.host/"
    assert tunnels.aio_called is True and tunnels.sync_called is False, (
        "must fetch the tunnel via sb.tunnels.aio, not the sync to_thread path")


async def test_resolve_supervisor_url_missing_returns_none(monkeypatch):
    import api.providers.modal as mmod
    from api.providers._shared import SandboxMissingError

    async def _lookup(ref):
        raise SandboxMissingError("gone")

    monkeypatch.setattr(mmod, "_lookup_sandbox", _lookup)
    assert await mmod.resolve_supervisor_url("sb-x") is None


async def test_resolve_supervisor_url_no_tunnel_returns_none(monkeypatch):
    import api.providers.modal as mmod

    fake_sb = SimpleNamespace(tunnels=_AioCallable(result={}))  # no port entry

    async def _lookup(ref):
        return fake_sb

    monkeypatch.setattr(mmod, "_lookup_sandbox", _lookup)
    assert await mmod.resolve_supervisor_url("sb-1") is None
