"""create_bare_sandbox (native modal) uses the async create/tag API, not to_thread.

Sandbox.create holds its thread for the ~2s scheduling wait, so a burst of
native session starts (autoscale) capped at the shared threadpool. Now it uses
modal's `Sandbox.create.aio` / `set_tags.aio` (no threads), like the exec path
(#190). The live async path is verified separately on real modal; this pins
that the async (.aio) variants are the ones invoked, with correct tagging.

Mocked — no cloud.
"""
from __future__ import annotations

from types import SimpleNamespace

import pytest

pytestmark = pytest.mark.asyncio


class _AioCallable:
    """Callable (the old to_thread path) AND has .aio (the new path), so a test
    can assert which one create_bare_sandbox actually used."""

    def __init__(self, result):
        self.result = result
        self.sync_called = False
        self.aio_called = False
        self.aio_args = None

    def __call__(self, *a, **k):
        self.sync_called = True
        return self.result

    async def aio(self, *a, **k):
        self.aio_called = True
        self.aio_args = (a, k)
        return self.result


async def test_create_bare_uses_async_create_and_tags(monkeypatch):
    import api.providers.modal as mmod

    monkeypatch.setenv("AGENT_SDK_ORIGIN", "test")

    set_tags = _AioCallable(None)
    terminate = _AioCallable(None)
    fake_sb = SimpleNamespace(object_id="sb-bare-xyz", set_tags=set_tags, terminate=terminate)
    create = _AioCallable(fake_sb)
    fake_modal = SimpleNamespace(Sandbox=SimpleNamespace(create=create))

    monkeypatch.setattr(mmod, "_require_modal", lambda: (fake_modal, None))

    async def _app():
        return SimpleNamespace(app_id="ap")

    async def _image():
        return SimpleNamespace()

    async def _vol(_ref):
        return SimpleNamespace()

    monkeypatch.setattr(mmod, "_get_app", _app)
    monkeypatch.setattr(mmod, "_get_image", _image)
    monkeypatch.setattr(mmod, "_get_volume", _vol)

    inst = await mmod.create_bare_sandbox(
        volume_ref="vol-1", subpath="agents/a1", root="/v/work")

    assert inst.sandbox_ref == "sb-bare-xyz"
    assert create.aio_called is True and create.sync_called is False, (
        "must create via Sandbox.create.aio, not the sync to_thread path")
    assert set_tags.aio_called is True, "must tag via set_tags.aio"
    from api.providers.modal import _TAG_KEY, _ORIGIN_TAG
    tag_dict = set_tags.aio_args[0][0]  # first positional arg
    assert tag_dict.get(_TAG_KEY) == "sb-bare-xyz"
    assert tag_dict.get(_ORIGIN_TAG) == "test"
