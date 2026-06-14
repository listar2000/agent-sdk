"""get_sandbox_status must poll via the async API (``sb.poll.aio()``), not
``asyncio.to_thread(sb.poll)``.

The status probe runs on the recovery path — on a server restart with N modal
sessions, N concurrent ``get_sandbox_status`` calls fire at once. With the
sync poll wrapped in ``to_thread`` each call pins a shared-threadpool worker
for the poll RPC, so the burst caps at the threadpool size (~min(32, cpu+4)).
``sb.poll.aio()`` removes that thread, completing the status async path
alongside the async ``_lookup_sandbox`` (from_id.aio).
"""
from __future__ import annotations

import pytest

import api.providers.modal as modal_mod
from api.providers._shared import SandboxMissingError


class _PollMock:
    """Records whether the sync ``__call__`` or the async ``.aio`` path ran."""

    def __init__(self, rc):
        self.rc = rc
        self.sync_called = False
        self.aio_called = False

    def __call__(self):
        self.sync_called = True
        return self.rc

    async def aio(self):
        self.aio_called = True
        return self.rc


class _FakeSandbox:
    def __init__(self, poll: _PollMock):
        self.poll = poll


@pytest.mark.asyncio
async def test_get_sandbox_status_uses_async_poll(monkeypatch):
    poll = _PollMock(None)  # None => still running
    sb = _FakeSandbox(poll)

    async def fake_lookup(ref):
        return sb

    monkeypatch.setattr(modal_mod, "_lookup_sandbox", fake_lookup)

    status = await modal_mod.get_sandbox_status("sb-modal-x")

    assert status == "running"
    # The whole point: async poll, never the thread-bound sync poll.
    assert poll.aio_called is True
    assert poll.sync_called is False


@pytest.mark.asyncio
async def test_get_sandbox_status_exited_is_missing(monkeypatch):
    poll = _PollMock(0)  # exited => returncode set
    sb = _FakeSandbox(poll)

    async def fake_lookup(ref):
        return sb

    monkeypatch.setattr(modal_mod, "_lookup_sandbox", fake_lookup)

    assert await modal_mod.get_sandbox_status("sb-modal-x") == "missing"
    assert poll.aio_called is True


@pytest.mark.asyncio
async def test_get_sandbox_status_missing_lookup(monkeypatch):
    async def fake_lookup(ref):
        raise SandboxMissingError("gone")

    monkeypatch.setattr(modal_mod, "_lookup_sandbox", fake_lookup)
    assert await modal_mod.get_sandbox_status("sb-modal-x") == "missing"


@pytest.mark.asyncio
async def test_get_sandbox_status_poll_error(monkeypatch):
    class _Boom:
        async def aio(self):
            raise RuntimeError("grpc blip")

        def __call__(self):
            raise RuntimeError("grpc blip")

    sb = _FakeSandbox(_Boom())

    async def fake_lookup(ref):
        return sb

    monkeypatch.setattr(modal_mod, "_lookup_sandbox", fake_lookup)
    assert await modal_mod.get_sandbox_status("sb-modal-x") == "error"


@pytest.mark.asyncio
async def test_get_sandbox_status_empty_ref():
    assert await modal_mod.get_sandbox_status("") == "missing"
