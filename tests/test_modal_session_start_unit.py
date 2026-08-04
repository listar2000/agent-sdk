import os
import sys

import pytest


from api.acp_client import ACP_AUTHENTICATION_REQUIRED, AcpError
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
async def test_modal_start_cold_creates_when_reattach_target_is_wedged(monkeypatch):
    """Recovery race under load: a killed PID-1 supervisor takes its sandbox
    down, but Modal's control plane still reports the ref 'running' for a few
    seconds. start() must NOT 500 the caller — it abandons the wedged ref and
    cold-creates a fresh sandbox on the volume in the SAME call.

    Regression guard: before the fix start() raised
    ``RuntimeError("Modal supervisor not responding")`` here (the reattach
    health wait failed AFTER committing to the ref), which surfaced as a 500
    on POST /message and lost the turn on the persistent SSE — the
    [claude-modal] failure in test_persistent_sse_supervisor_killed_immediate_message
    under -n auto.
    """
    import api.providers.modal as md_provider

    state = ModalSandboxState(recipe=Recipe(), sandbox_ref="sb-stale", listen_port=9100)
    sess = ModalSandboxSession(session_id="sess-modal-wedged-reattach", state=state)

    async def _bootstrap():
        return "vol-1"

    async def _status(_ref):
        return "running"          # STALE: control plane lags the kill

    async def _resolve(_ref):
        return "https://stale.modal.test"

    async def _dead_health(url, **_kw):
        # The wedged reattach target never answers health.
        return url != "https://stale.modal.test"

    created: list[dict] = []

    async def _create_sandbox(**kw):
        created.append(kw)
        return ProviderInstance(
            provider="modal",
            url="https://fresh.modal.test",
            root="/v",
            sandbox_ref="sb-fresh",
            port=9200,
        )

    async def _attach_ok():
        return None

    monkeypatch.setattr(sess, "_bootstrap_session", _bootstrap)
    monkeypatch.setattr(sess, "_attach_acp", _attach_ok)
    monkeypatch.setattr(md_provider, "get_sandbox_status", _status)
    monkeypatch.setattr(md_provider, "resolve_supervisor_url", _resolve)
    monkeypatch.setattr(md_provider, "create_sandbox", _create_sandbox)
    monkeypatch.setattr("api.providers._shared._wait_for_health", _dead_health)

    # Must recover in-place, not raise.
    await sess.start()

    assert len(created) == 1, "wedged reattach should fall through to one cold-create"
    assert sess.state.sandbox_ref == "sb-fresh"
    assert sess.state.listen_port == 9200
    assert sess.supervisor_url == "https://fresh.modal.test"


@pytest.mark.asyncio
async def test_modal_start_reattaches_when_supervisor_healthy(monkeypatch):
    """The happy reattach path still works: a running sandbox whose supervisor
    answers health is reused — NO cold-create. Pins that folding the health
    probe into the reattach decision didn't break normal resume."""
    import api.providers.modal as md_provider

    state = ModalSandboxState(recipe=Recipe(), sandbox_ref="sb-live", listen_port=9100)
    sess = ModalSandboxSession(session_id="sess-modal-live-reattach", state=state)

    async def _bootstrap():
        return "vol-1"

    async def _status(_ref):
        return "running"

    async def _resolve(_ref):
        return "https://live.modal.test"

    async def _healthy(_url, **_kw):
        return True

    async def _create_should_not_run(**_kw):
        raise AssertionError("healthy reattach must not cold-create")

    async def _attach_ok():
        return None

    monkeypatch.setattr(sess, "_bootstrap_session", _bootstrap)
    monkeypatch.setattr(sess, "_attach_acp", _attach_ok)
    monkeypatch.setattr(md_provider, "get_sandbox_status", _status)
    monkeypatch.setattr(md_provider, "resolve_supervisor_url", _resolve)
    monkeypatch.setattr(md_provider, "create_sandbox", _create_should_not_run)
    monkeypatch.setattr("api.providers._shared._wait_for_health", _healthy)

    await sess.start()

    assert sess.state.sandbox_ref == "sb-live"        # reused, not replaced
    assert sess.supervisor_url == "https://live.modal.test"


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


@pytest.mark.asyncio
async def test_modal_attach_does_not_retry_non_retryable_acp_error(monkeypatch):
    state = ModalSandboxState(recipe=Recipe())
    sess = ModalSandboxSession(session_id="sess-modal-auth", state=state)

    attempts = {"count": 0}

    async def _attach_terminal():
        attempts["count"] += 1
        try:
            raise AcpError(
                ACP_AUTHENTICATION_REQUIRED,
                "Authentication required",
            )
        except AcpError as exc:
            raise RuntimeError("session/new failed") from exc

    monkeypatch.setattr(sess, "_attach_acp", _attach_terminal)

    with pytest.raises(RuntimeError, match="session/new failed"):
        await sess._attach_with_retry()

    assert attempts["count"] == 1
