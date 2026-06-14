"""_run_volume_shell must spawn its utility sandbox via the async modal API
(``Sandbox.create.aio`` + ``wait.aio`` / ``poll.aio`` / async stream iteration /
``terminate.aio``), not the sync SDK wrapped in ``asyncio.to_thread``.

Each volume file-op (tree listing, file read on a stopped session) spins a
short-lived sandbox whose create+run+terminate lifecycle is held for seconds.
Wrapped in ``to_thread`` that pins a shared-threadpool worker the whole time;
under concurrent volume ops (or alongside other threadpool work) the burst
caps at the threadpool size. The async path holds no thread.

The mocks below support BOTH the sync and the async call shapes, so the test
fails cleanly on the *assertion* (async-not-used) when run against the old
sync code rather than erroring on a missing attribute.
"""
from __future__ import annotations

import pytest

import api.providers.modal as modal_mod


class _DualMethod:
    """Callable (sync) that also exposes ``.aio`` (async); records which ran."""

    def __init__(self, result=None):
        self.result = result
        self.sync_called = False
        self.aio_called = False

    def __call__(self, *a, **k):
        self.sync_called = True
        return self.result

    async def aio(self, *a, **k):
        self.aio_called = True
        return self.result


class _DualStream:
    """Stream that supports sync ``.read()`` and async iteration."""

    def __init__(self, data: bytes):
        self.data = data

    def read(self):
        return self.data

    def __aiter__(self):
        async def _gen():
            yield self.data
        return _gen()


class _FakeVolSandbox:
    object_id = "sb-vol-async-1"

    def __init__(self, out: bytes, err: bytes, rc: int):
        self.returncode = rc
        self.stdout = _DualStream(out)
        self.stderr = _DualStream(err)
        self.wait = _DualMethod(None)
        self.poll = _DualMethod(rc)
        self.terminate = _DualMethod(None)


class _DualCreate:
    """Stands in for ``modal.Sandbox.create`` — sync call AND ``.aio``."""

    def __init__(self, sb: _FakeVolSandbox):
        self.sb = sb
        self.sync_called = False
        self.aio_called = False

    def __call__(self, *a, **k):
        self.sync_called = True
        return self.sb

    async def aio(self, *a, **k):
        self.aio_called = True
        return self.sb


def _install(monkeypatch, sb: _FakeVolSandbox) -> _DualCreate:
    create = _DualCreate(sb)

    class _FakeSandboxNS:
        pass

    _FakeSandboxNS.create = create

    class _FakeModal:
        Sandbox = _FakeSandboxNS

    monkeypatch.setattr(modal_mod, "_require_modal", lambda: (_FakeModal, None))

    async def _fake_app():
        return object()

    async def _fake_img():
        return object()

    async def _fake_vol(ref):
        return object()

    monkeypatch.setattr(modal_mod, "_get_app", _fake_app)
    monkeypatch.setattr(modal_mod, "_get_volume_image", _fake_img)
    monkeypatch.setattr(modal_mod, "_get_volume", _fake_vol)
    return create


@pytest.mark.asyncio
async def test_run_volume_shell_uses_async_create_and_io(monkeypatch):
    sb = _FakeVolSandbox(out=b"tree-out", err=b"warn", rc=0)
    create = _install(monkeypatch, sb)

    rc, out, err = await modal_mod._run_volume_shell("vol-ref", "find /v", timeout=30)

    assert (rc, out, err) == (0, b"tree-out", b"warn")
    # The point of the change: async create + async lifecycle, no thread.
    assert create.aio_called is True
    assert create.sync_called is False
    assert sb.wait.aio_called is True and sb.wait.sync_called is False
    assert sb.poll.aio_called is True
    assert sb.terminate.aio_called is True


@pytest.mark.asyncio
async def test_run_volume_shell_nonzero_rc(monkeypatch):
    sb = _FakeVolSandbox(out=b"", err=b"boom", rc=5)
    _install(monkeypatch, sb)

    rc, out, err = await modal_mod._run_volume_shell("vol-ref", "false", timeout=10)
    assert rc == 5
    assert err == b"boom"
    # terminated even on a failing shell (no sandbox leak)
    assert sb.terminate.aio_called is True
