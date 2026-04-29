"""Phase 2: pin DockerSandboxSession's contracts.

Mock-driven (no docker daemon needed). Validates the abstraction holds
across multiple providers — adding a provider really is one file +
factory registration.
"""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from api.sandbox import DockerSandboxState, Recipe, UnknownSandboxState
from api.sandbox.providers.docker import DockerSandboxSession


def test_constructor_coerces_unknown_state_to_docker():
    session = DockerSandboxSession(
        session_id="s1",
        state=UnknownSandboxState(recipe=Recipe(agent_type="claude")),
    )
    assert isinstance(session.state, DockerSandboxState)
    assert session.state.recipe.agent_type == "claude"
    assert session.state.sandbox_id is None


def test_constructor_keeps_existing_docker_state():
    state = DockerSandboxState(
        sandbox_id="container-abc123",
        listen_port=2469,
        recipe=Recipe(shared_mounts=["projects"]),
    )
    session = DockerSandboxSession(session_id="s1", state=state)
    assert session.state.sandbox_id == "container-abc123"
    assert session.state.listen_port == 2469
    assert session.state.recipe.shared_mounts == ["projects"]


@pytest.mark.asyncio
async def test_running_returns_false_before_start():
    session = DockerSandboxSession(
        session_id="s1",
        state=DockerSandboxState(recipe=Recipe()),
    )
    assert not await session.running()


@pytest.mark.asyncio
async def test_start_with_no_sandbox_id_calls_create():
    """Cold path: no sandbox_id → create_sandbox."""
    session = DockerSandboxSession(
        session_id="s1",
        state=DockerSandboxState(recipe=Recipe(agent_type="claude")),
    )
    fake_instance = MagicMock(
        sandbox_id="container-new-123",
        url="http://127.0.0.1:2469",
        port=2469,
    )
    with patch("api.providers.docker.create_sandbox",
               new=AsyncMock(return_value=fake_instance)) as create, \
         patch("api.providers._shared._wait_for_health",
               new=AsyncMock(return_value=True)):
        await session.start()

    create.assert_awaited_once()
    assert session.state.sandbox_id == "container-new-123"
    assert session.state.listen_port == 2469
    assert session._supervisor_url == "http://127.0.0.1:2469"
    assert session.liveness.state == "alive"


@pytest.mark.asyncio
async def test_start_reattaches_when_container_still_running():
    """Container alive on docker: skip create_sandbox, reuse instance."""
    session = DockerSandboxSession(
        session_id="s1",
        state=DockerSandboxState(
            sandbox_id="container-existing",
            listen_port=2469,
            recipe=Recipe(),
        ),
    )
    with patch("api.providers.docker.get_sandbox_status",
               new=AsyncMock(return_value="running")), \
         patch("api.providers.docker.create_sandbox",
               new=AsyncMock(side_effect=AssertionError("must not create"))), \
         patch("api.providers._shared._wait_for_health",
               new=AsyncMock(return_value=True)):
        await session.start()

    assert session.state.sandbox_id == "container-existing"
    assert session._supervisor_url == "http://127.0.0.1:2469"


@pytest.mark.asyncio
async def test_start_falls_through_to_create_when_container_stopped():
    """Container not running on docker: create fresh."""
    session = DockerSandboxSession(
        session_id="s1",
        state=DockerSandboxState(
            sandbox_id="container-dead",
            listen_port=2469,
            recipe=Recipe(),
        ),
    )
    fake_instance = MagicMock(
        sandbox_id="container-fresh",
        url="http://127.0.0.1:2470",
        port=2470,
    )
    with patch("api.providers.docker.get_sandbox_status",
               new=AsyncMock(return_value="stopped")), \
         patch("api.providers.docker.create_sandbox",
               new=AsyncMock(return_value=fake_instance)) as create, \
         patch("api.providers._shared._wait_for_health",
               new=AsyncMock(return_value=True)):
        await session.start()

    create.assert_awaited_once()
    assert session.state.sandbox_id == "container-fresh"
    assert session.state.listen_port == 2470


@pytest.mark.asyncio
async def test_shutdown_clears_state_and_is_idempotent():
    session = DockerSandboxSession(
        session_id="s1",
        state=DockerSandboxState(recipe=Recipe()),
    )
    session._container_id = "container-x"
    session._supervisor_url = "http://anything"

    await session.shutdown()
    assert session._container_id is None
    assert session._supervisor_url is None
    await session.shutdown()  # no-op, no exception


def test_factory_registers_docker_provider():
    """The factory's eager registration includes docker now."""
    from api.sandbox import factory as fac
    from api.sandbox import DockerSandboxState

    session = fac.make_session(
        "s1", DockerSandboxState(recipe=Recipe(agent_type="claude")),
    )
    assert isinstance(session, DockerSandboxSession)
