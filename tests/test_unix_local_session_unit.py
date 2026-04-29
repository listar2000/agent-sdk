"""Phase 2: pin UnixLocalSandboxSession's contracts. Smaller than the
docker test — local provider is the simplest of the three."""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from api.sandbox import Recipe, UnixLocalSandboxState, UnknownSandboxState
from api.sandbox.providers.unix_local import UnixLocalSandboxSession


def test_constructor_coerces_unknown_state_to_unix_local():
    s = UnixLocalSandboxSession(
        session_id="s1",
        state=UnknownSandboxState(recipe=Recipe(agent_type="claude")),
    )
    assert isinstance(s.state, UnixLocalSandboxState)


@pytest.mark.asyncio
async def test_start_with_no_sandbox_id_calls_create():
    session = UnixLocalSandboxSession(
        session_id="s1",
        state=UnixLocalSandboxState(recipe=Recipe()),
    )
    fake_instance = MagicMock(
        sandbox_id="12345",  # pid as string
        url="http://127.0.0.1:2470",
        port=2470,
    )
    with patch("api.providers.local.create_sandbox",
               new=AsyncMock(return_value=fake_instance)) as create, \
         patch("api.providers._shared._wait_for_health",
               new=AsyncMock(return_value=True)):
        await session.start()

    create.assert_awaited_once()
    assert session.state.sandbox_id == "12345"
    assert session.liveness.state == "alive"


@pytest.mark.asyncio
async def test_running_returns_false_before_start():
    s = UnixLocalSandboxSession(
        session_id="s1",
        state=UnixLocalSandboxState(recipe=Recipe()),
    )
    assert not await s.running()


@pytest.mark.asyncio
async def test_shutdown_idempotent():
    s = UnixLocalSandboxSession(
        session_id="s1",
        state=UnixLocalSandboxState(recipe=Recipe()),
    )
    s._supervisor_url = "http://anything"
    await s.shutdown()
    assert s._supervisor_url is None
    await s.shutdown()  # no-op


def test_factory_registers_unix_local():
    from api.sandbox import factory as fac
    s = fac.make_session(
        "s1", UnixLocalSandboxState(recipe=Recipe(agent_type="claude")),
    )
    assert isinstance(s, UnixLocalSandboxSession)
