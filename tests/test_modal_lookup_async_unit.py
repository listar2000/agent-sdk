"""_lookup_sandbox uses modal's async from_id.aio, not asyncio.to_thread.

from_id (a SandboxWait RPC, high-variance) is on the exec/status/recovery hot
paths; threading it capped concurrent lookups at the shared default threadpool.
Async (.aio) removes that ceiling (measured ~1.3x at 40 concurrent). Error
semantics unchanged: a missing sandbox → SandboxMissingError; other errors
re-raise.

Mocked — no cloud.
"""
from __future__ import annotations

from types import SimpleNamespace

import pytest

pytestmark = pytest.mark.asyncio


class _Resolver:
    """Stands in for modal.Sandbox.from_id — callable (the old to_thread path)
    AND has .aio (the new async path), so a test can assert which was used."""

    def __init__(self, *, result=None, exc=None):
        self.result = result
        self.exc = exc
        self.sync_called = False
        self.aio_called = False

    def __call__(self, ref):
        self.sync_called = True
        if self.exc:
            raise self.exc
        return self.result

    async def aio(self, ref):
        self.aio_called = True
        if self.exc:
            raise self.exc
        return self.result


def _patch_modal(monkeypatch, resolver):
    import api.providers.modal as mmod
    fake_modal = SimpleNamespace(Sandbox=SimpleNamespace(from_id=resolver))
    monkeypatch.setattr(mmod, "_require_modal", lambda: (fake_modal, None))
    return mmod


async def test_lookup_uses_async_from_id(monkeypatch):
    fake_sb = SimpleNamespace(object_id="sb-1")
    r = _Resolver(result=fake_sb)
    mmod = _patch_modal(monkeypatch, r)

    out = await mmod._lookup_sandbox("sb-1")
    assert out is fake_sb
    assert r.aio_called is True, "must use the async from_id.aio path"
    assert r.sync_called is False, "must NOT use the sync (to_thread) path"


async def test_lookup_missing_raises_sandbox_missing(monkeypatch):
    from api.providers._shared import SandboxMissingError
    r = _Resolver(exc=RuntimeError("No Sandbox with ID 'sb-x' found"))
    mmod = _patch_modal(monkeypatch, r)

    with pytest.raises(SandboxMissingError):
        await mmod._lookup_sandbox("sb-x")
    assert r.aio_called is True


async def test_lookup_other_error_reraises(monkeypatch):
    from api.providers._shared import SandboxMissingError
    r = _Resolver(exc=RuntimeError("transient gRPC blip"))
    mmod = _patch_modal(monkeypatch, r)

    with pytest.raises(RuntimeError) as ei:
        await mmod._lookup_sandbox("sb-1")
    assert not isinstance(ei.value, SandboxMissingError), "non-missing errors re-raise as-is"
