"""Phase 2: pin ModalSandboxSession's contracts. Completes the
provider matrix (daytona + docker + unix_local + modal)."""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from api.sandbox import ModalSandboxState, Recipe, UnknownSandboxState
from api.sandbox.providers.modal import ModalSandboxSession


def test_constructor_coerces_unknown_state_to_modal():
    s = ModalSandboxSession(
        session_id="s1",
        state=UnknownSandboxState(recipe=Recipe(agent_type="claude")),
    )
    assert isinstance(s.state, ModalSandboxState)


@pytest.mark.asyncio
async def test_start_with_no_sandbox_id_calls_create():
    session = ModalSandboxSession(
        session_id="s1",
        state=ModalSandboxState(recipe=Recipe()),
    )
    fake_instance = MagicMock(
        sandbox_id="modal-sb-abc",
        url="http://modal-sb-abc.modal.host:8080",
        port=8080,
    )
    with patch("api.providers.modal.create_sandbox",
               new=AsyncMock(return_value=fake_instance)) as create, \
         patch("api.providers._shared._wait_for_health",
               new=AsyncMock(return_value=True)):
        await session.start()

    create.assert_awaited_once()
    assert session.state.sandbox_id == "modal-sb-abc"
    assert session.liveness.state == "alive"


@pytest.mark.asyncio
async def test_start_reattaches_when_running():
    session = ModalSandboxSession(
        session_id="s1",
        state=ModalSandboxState(
            sandbox_id="modal-existing",
            listen_port=8080,
            recipe=Recipe(),
        ),
    )
    with patch("api.providers.modal.get_sandbox_status",
               new=AsyncMock(return_value="running")), \
         patch("api.providers.modal.create_sandbox",
               new=AsyncMock(side_effect=AssertionError("must not create"))), \
         patch("api.providers._shared._wait_for_health",
               new=AsyncMock(return_value=True)):
        await session.start()
    assert session.state.sandbox_id == "modal-existing"


@pytest.mark.asyncio
async def test_running_returns_false_before_start():
    s = ModalSandboxSession(
        session_id="s1",
        state=ModalSandboxState(recipe=Recipe()),
    )
    assert not await s.running()


@pytest.mark.asyncio
async def test_shutdown_idempotent():
    s = ModalSandboxSession(
        session_id="s1",
        state=ModalSandboxState(recipe=Recipe()),
    )
    s._supervisor_url = "http://anything"
    await s.shutdown()
    assert s._supervisor_url is None
    await s.shutdown()


def test_factory_registers_modal():
    from api.sandbox import factory as fac
    s = fac.make_session(
        "s1", ModalSandboxState(recipe=Recipe(agent_type="claude")),
    )
    assert isinstance(s, ModalSandboxSession)
