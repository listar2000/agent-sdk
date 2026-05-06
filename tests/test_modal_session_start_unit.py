import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from api.providers import ProviderInstance
from api.providers.modal.session import ModalSandboxSession
from api.sandbox.state import ModalSandboxState, Recipe


@pytest.mark.asyncio
async def test_modal_start_cleans_up_fresh_sandbox_when_attach_fails(monkeypatch):
    import api.providers.modal as md_provider

    state = ModalSandboxState(recipe=Recipe())
    sess = ModalSandboxSession(session_id="sess-modal-attach-fail", state=state)

    async def _bootstrap():
        return "vol-1"

    async def _create_sandbox(**_kw):
        return ProviderInstance(
            provider="modal",
            url="https://modal.test",
            root="/v",
            sandbox_ref="sb-created",
            port=9100,
        )

    calls: list[str] = []

    async def _stop_sandbox(inst: ProviderInstance):
        calls.append(inst.sandbox_ref or "")

    async def _attach_fail():
        raise RuntimeError("attach boom")

    monkeypatch.setattr(sess, "_bootstrap_session", _bootstrap)
    monkeypatch.setattr(sess, "_attach_acp", _attach_fail)
    monkeypatch.setattr(md_provider, "create_sandbox", _create_sandbox)
    monkeypatch.setattr(md_provider, "stop_sandbox", _stop_sandbox)
    monkeypatch.setattr(
        "api.providers.modal.session._ATTACH_RETRY_DELAY_S",
        0.0,
    )

    with pytest.raises(RuntimeError, match="attach boom"):
        await sess.start()

    assert calls == ["sb-created"]
    assert sess.state.sandbox_ref is None
    assert sess.state.listen_port is None
    assert sess.supervisor_url is None


@pytest.mark.asyncio
async def test_modal_start_retries_attach_before_success(monkeypatch):
    import api.providers.modal as md_provider

    state = ModalSandboxState(recipe=Recipe())
    sess = ModalSandboxSession(session_id="sess-modal-attach-retry", state=state)

    async def _bootstrap():
        return "vol-1"

    async def _create_sandbox(**_kw):
        return ProviderInstance(
            provider="modal",
            url="https://modal.test",
            root="/v",
            sandbox_ref="sb-retry",
            port=9100,
        )

    attach_attempts = {"count": 0}

    async def _attach_flaky():
        attach_attempts["count"] += 1
        if attach_attempts["count"] < 3:
            raise RuntimeError("attach not ready")

    async def _stop_sandbox(_inst: ProviderInstance):
        raise AssertionError("stop_sandbox should not be called on retry success")

    monkeypatch.setattr(sess, "_bootstrap_session", _bootstrap)
    monkeypatch.setattr(sess, "_attach_acp", _attach_flaky)
    monkeypatch.setattr(md_provider, "create_sandbox", _create_sandbox)
    monkeypatch.setattr(md_provider, "stop_sandbox", _stop_sandbox)
    monkeypatch.setattr(
        "api.providers.modal.session._ATTACH_RETRY_DELAY_S",
        0.0,
    )

    await sess.start()

    assert attach_attempts["count"] == 3
    assert sess.state.sandbox_ref == "sb-retry"
    assert sess.supervisor_url == "https://modal.test"
