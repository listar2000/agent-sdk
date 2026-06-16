"""modal create_sandbox (supervisor) must ALWAYS tag the sandbox.

supervisor_session.start() calls create_sandbox WITHOUT a sandbox_ref (Modal
assigns the object_id), so the old ``if sandbox_ref:`` gate meant set_tags
never ran for supervisor sessions. An untagged Modal sandbox is invisible to:

  * reconcile_on_startup — skips sandboxes with no ``agent-sdk.sandbox-id`` tag
  * cleanup_orphans.py    — filters by the ``agent_sdk_origin`` tag

so an orphaned supervisor sandbox leaked until the 1 h hard timeout. The fix
mirrors create_bare_sandbox: always tag, with an ``object_id`` fallback for
``_TAG_KEY`` plus the ``_ORIGIN_TAG``.

This drives create_sandbox with its cloud I/O mocked and asserts set_tags is
called with both tags even when sandbox_ref is None. Mocked — no cloud.
"""
from __future__ import annotations

from types import SimpleNamespace

import pytest

pytestmark = pytest.mark.asyncio


class _AioStream:
    """Async-iterable stand-in for a modal stdout|stderr stream."""

    def __init__(self, text: str = ""):
        self._text = text

    def __aiter__(self):
        text = self._text

        async def _gen():
            if text:
                yield text

        return _gen()


class _FakeProc:
    def __init__(self, sb):
        self._sb = sb
        self.stdout = _AioStream()
        self.stderr = _AioStream()

    @property
    def wait(self):
        async def _a():
            return 0
        return SimpleNamespace(aio=_a)


class _DualTunnels:
    """``sb.tunnels`` mock exposing ``.aio`` (create_sandbox calls
    ``sb.tunnels.aio(60)``)."""

    def _result(self):
        from api.providers.modal import _SUPERVISOR_CONTAINER_PORT
        return {_SUPERVISOR_CONTAINER_PORT: SimpleNamespace(url="https://fake.modal.host")}

    async def aio(self, timeout):
        return self._result()


class _FakeSandbox:
    object_id = "sb-modal-fresh-001"

    def __init__(self):
        self.tag_calls: list[dict] = []
        self.tunnels = _DualTunnels()
        self.stdout = _AioStream()
        self.stderr = _AioStream()

    @property
    def set_tags(self):
        async def _a(tags):
            self.tag_calls.append(dict(tags))
        return SimpleNamespace(aio=_a)

    @property
    def exec(self):
        async def _a(*a, **k):
            return _FakeProc(self)
        return SimpleNamespace(aio=_a)

    @property
    def terminate(self):
        async def _a():
            return None
        return SimpleNamespace(aio=_a)


async def _drive_create(monkeypatch, *, sandbox_ref, origin="test"):
    import api.providers.modal as mmod
    from api.providers import _shared

    monkeypatch.setenv("AGENT_SDK_ORIGIN", origin)

    sb = _FakeSandbox()

    async def _create_aio(*a, **k):
        return sb

    fake_modal = SimpleNamespace(Sandbox=SimpleNamespace(create=SimpleNamespace(aio=_create_aio)))

    monkeypatch.setattr(mmod, "_require_modal", lambda: (fake_modal, None))

    async def _app():
        return SimpleNamespace(app_id="ap-test")

    async def _image():
        return SimpleNamespace()

    async def _vol(_ref):
        return SimpleNamespace()

    async def _healthy(_url, **_kw):
        return True

    monkeypatch.setattr(mmod, "_get_app", _app)
    monkeypatch.setattr(mmod, "_get_image", _image)
    monkeypatch.setattr(mmod, "_get_volume", _vol)
    monkeypatch.setattr(mmod, "_wait_for_health", _healthy)
    # create_sandbox resolves the ACP bin via this _shared helper (reads
    # package.json) — stub it so the test needs no runtime tree.
    monkeypatch.setattr(_shared, "_runtime_acp_bin_relative", lambda _t: "node_modules/x/bin.js")

    inst = await mmod.create_sandbox(
        volume_ref="vol-1", subpath="sessions/s1", agent_type="opencode",
        sandbox_ref=sandbox_ref,
    )
    return sb, inst


async def test_create_sandbox_tags_even_without_sandbox_ref(monkeypatch):
    from api.providers.modal import _TAG_KEY, _ORIGIN_TAG

    sb, inst = await _drive_create(monkeypatch, sandbox_ref=None, origin="test")

    assert sb.tag_calls, (
        "create_sandbox must tag the sandbox even when called without a "
        "sandbox_ref — otherwise supervisor sandboxes are untagged and leak "
        "past both reconcile and cleanup_orphans")
    tags = sb.tag_calls[-1]
    assert tags.get(_TAG_KEY) == sb.object_id, "must fall back to object_id"
    assert tags.get(_ORIGIN_TAG) == "test", "must carry the origin tag for cleanup"
    assert inst.sandbox_ref == sb.object_id


async def test_create_sandbox_uses_sandbox_ref_when_given(monkeypatch):
    from api.providers.modal import _TAG_KEY, _ORIGIN_TAG

    sb, _ = await _drive_create(monkeypatch, sandbox_ref="explicit-ref", origin="production")

    tags = sb.tag_calls[-1]
    assert tags.get(_TAG_KEY) == "explicit-ref", "explicit ref wins over object_id"
    assert tags.get(_ORIGIN_TAG) == "production"
