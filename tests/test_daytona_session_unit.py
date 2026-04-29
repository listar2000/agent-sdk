"""Phase 2 sub-task 2: pin DaytonaSandboxSession's contracts.

Mock-driven (no daytona SDK calls). Two surfaces:
  * ``_parse_sse_block`` — JSON-RPC envelope → typed event dict
  * Lifecycle method routing in ``start()`` — reattach vs cold create

The full provisioning flow is covered by the existing daytona
integration tests in ``test_sandbox_stop_delete_recovery.py``; that
suite stays green throughout the refactor.
"""
from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from api.sandbox import DaytonaSandboxState, Recipe, UnknownSandboxState
from api.sandbox.providers.daytona import DaytonaSandboxSession, _parse_sse_block


# ---------------------------------------------------------------------------
# _parse_sse_block — the ACP-event-to-dict translator
# ---------------------------------------------------------------------------


def _block(payload: dict) -> str:
    return f"data: {json.dumps(payload)}"


def test_parse_done_event_extracts_stop_reason():
    block = _block({"jsonrpc": "2.0", "id": "rpc-1", "result": {"stopReason": "end_turn"}})
    assert _parse_sse_block(block, "rpc-1") == {"type": "done", "stop_reason": "end_turn"}


def test_parse_text_event_from_session_update():
    block = _block({
        "jsonrpc": "2.0",
        "method": "session/update",
        "params": {"update": {
            "sessionUpdate": "agent_message_chunk",
            "content": {"type": "text", "text": "Hello!"},
        }},
    })
    # session/update notifications have no id, so they always pass the rpc filter.
    assert _parse_sse_block(block, "rpc-1") == {"type": "text", "text": "Hello!"}


def test_parse_error_event():
    block = _block({"jsonrpc": "2.0", "id": "rpc-1", "error": {"code": -1, "message": "boom"}})
    out = _parse_sse_block(block, "rpc-1")
    assert out["type"] == "error"
    assert out["error"]["code"] == -1


def test_parse_filters_other_rpc_id():
    """Events for a different rpc_id are dropped (concurrent ACP traffic)."""
    block = _block({"jsonrpc": "2.0", "id": "rpc-OTHER", "result": {"stopReason": "end_turn"}})
    assert _parse_sse_block(block, "rpc-1") is None


def test_parse_heartbeat_block_returns_none():
    """Comment-only blocks (`: heartbeat`) have no `data:` line."""
    assert _parse_sse_block(": heartbeat", "rpc-1") is None


def test_parse_malformed_json_returns_none():
    assert _parse_sse_block("data: {not json", "rpc-1") is None


def test_parse_empty_block_returns_none():
    assert _parse_sse_block("", "rpc-1") is None


def test_parse_unknown_session_update_type_passes_through():
    """Unknown sessionUpdate variants fall through with raw payload."""
    block = _block({
        "jsonrpc": "2.0",
        "method": "session/update",
        "params": {"update": {"sessionUpdate": "future_thing", "data": "x"}},
    })
    out = _parse_sse_block(block, "rpc-1")
    assert out["type"] == "future_thing"
    assert out["raw"]["data"] == "x"


# ---------------------------------------------------------------------------
# DaytonaSandboxSession constructor + state coercion
# ---------------------------------------------------------------------------


def test_constructor_coerces_unknown_state_to_daytona():
    """Pool may hand us an UnknownSandboxState; we should coerce."""
    session = DaytonaSandboxSession(
        session_id="s1",
        state=UnknownSandboxState(recipe=Recipe(agent_type="claude")),
    )
    assert isinstance(session.state, DaytonaSandboxState)
    assert session.state.recipe.agent_type == "claude"
    assert session.state.sandbox_id is None  # nothing provisioned yet


def test_constructor_keeps_existing_daytona_state():
    """Daytona state passed in is kept verbatim — sandbox_id preserved."""
    state = DaytonaSandboxState(
        sandbox_id="dt-existing",
        listen_port=9100,
        recipe=Recipe(dockerfile="/df", shared_mounts=["projects"]),
    )
    session = DaytonaSandboxSession(session_id="s1", state=state)
    assert session.state.sandbox_id == "dt-existing"
    assert session.state.recipe.shared_mounts == ["projects"]


# ---------------------------------------------------------------------------
# Lifecycle: start() routing — reattach vs cold create
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_start_with_no_sandbox_id_calls_provision():
    """Cold path: state has no sandbox_id → provision_daytona_sandbox."""
    session = DaytonaSandboxSession(
        session_id="s1",
        state=DaytonaSandboxState(recipe=Recipe(agent_type="claude")),
    )
    # Mock the daytona provider primitives.
    fake_instance = MagicMock(sandbox_id="dt-new-123", url="")
    fake_sandbox = MagicMock(id="dt-new-123")

    # Patch start_supervisor_in_sandbox to return a URL.
    with patch("api.providers.daytona.provision_daytona_sandbox",
               new=AsyncMock(return_value=fake_instance)) as provision, \
         patch("api.providers.daytona.start_supervisor_in_sandbox",
               new=AsyncMock(return_value="https://9100-test.daytonaproxy01.net")), \
         patch("api.providers._shared._wait_for_health",
               new=AsyncMock(return_value=True)), \
         patch("daytona_sdk.Daytona") as DaytonaCls:
        DaytonaCls.return_value.get = MagicMock(return_value=fake_sandbox)
        import os
        os.environ["DAYTONA_API_KEY"] = "test-key"
        await session.start()

    provision.assert_awaited_once()
    assert session.state.sandbox_id == "dt-new-123"
    assert session._supervisor_url == "https://9100-test.daytonaproxy01.net"
    assert session.liveness.state == "alive"


@pytest.mark.asyncio
async def test_start_with_existing_sandbox_id_calls_restart():
    """Warm-cold path: state has sandbox_id → restart_daytona_supervisor."""
    session = DaytonaSandboxSession(
        session_id="s1",
        state=DaytonaSandboxState(
            sandbox_id="dt-existing",
            recipe=Recipe(agent_type="claude"),
        ),
    )
    fake_instance = MagicMock(url="https://9100-restored.daytonaproxy01.net",
                               sandbox_id="dt-existing")
    fake_sandbox = MagicMock(id="dt-existing")

    with patch("api.providers.daytona.restart_daytona_supervisor",
               new=AsyncMock(return_value=fake_instance)) as restart, \
         patch("api.providers.daytona.start_supervisor_in_sandbox",
               new=AsyncMock(return_value="https://9100-restored.daytonaproxy01.net")), \
         patch("api.providers._shared._wait_for_health",
               new=AsyncMock(return_value=True)), \
         patch("daytona_sdk.Daytona") as DaytonaCls:
        DaytonaCls.return_value.get = MagicMock(return_value=fake_sandbox)
        import os
        os.environ["DAYTONA_API_KEY"] = "test-key"
        await session.start()

    restart.assert_awaited_once()
    assert session.state.sandbox_id == "dt-existing"  # unchanged


@pytest.mark.asyncio
async def test_start_falls_through_to_create_when_sandbox_not_found():
    """Existing sandbox_id but daytona says 'not found' → fresh create."""
    session = DaytonaSandboxSession(
        session_id="s1",
        state=DaytonaSandboxState(
            sandbox_id="dt-deleted",
            recipe=Recipe(agent_type="claude"),
        ),
    )
    fake_instance = MagicMock(sandbox_id="dt-fresh", url="")
    fake_sandbox = MagicMock(id="dt-fresh")

    with patch("api.providers.daytona.restart_daytona_supervisor",
               new=AsyncMock(side_effect=Exception("Sandbox with ID or name dt-deleted not found"))), \
         patch("api.providers.daytona.provision_daytona_sandbox",
               new=AsyncMock(return_value=fake_instance)) as provision, \
         patch("api.providers.daytona.start_supervisor_in_sandbox",
               new=AsyncMock(return_value="https://9100-fresh.daytonaproxy01.net")), \
         patch("api.providers._shared._wait_for_health",
               new=AsyncMock(return_value=True)), \
         patch("daytona_sdk.Daytona") as DaytonaCls:
        DaytonaCls.return_value.get = MagicMock(return_value=fake_sandbox)
        import os
        os.environ["DAYTONA_API_KEY"] = "test-key"
        await session.start()

    # Restart was tried first (with the stale id).
    provision.assert_awaited_once()
    assert session.state.sandbox_id == "dt-fresh"


# ---------------------------------------------------------------------------
# running() / liveness probe wiring
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_running_returns_false_before_start():
    """No supervisor URL yet → liveness probe returns False."""
    session = DaytonaSandboxSession(
        session_id="s1",
        state=DaytonaSandboxState(recipe=Recipe(agent_type="claude")),
    )
    assert not await session.running()


@pytest.mark.asyncio
async def test_shutdown_clears_supervisor_handle_and_is_idempotent():
    session = DaytonaSandboxSession(
        session_id="s1",
        state=DaytonaSandboxState(recipe=Recipe(agent_type="claude")),
    )
    session._supervisor_url = "https://anything"
    session._daytona_sandbox = MagicMock()

    await session.shutdown()
    assert session._supervisor_url is None
    assert session._daytona_sandbox is None
    # Calling again must not raise.
    await session.shutdown()
