"""Pin the persist-side SSE parser to the canonical ``parse_acp_event``.

The persist path (``_persist_prompt_events`` → ``execute_prompt`` →
``_parse_sse_block``) and the SDK path (``Agent.astream`` →
``parse_acp_event``) MUST emit the same event taxonomy or the SSE/log
parity tests in ``tests/test_interrupt_integration.py`` fail.

Three previously-leaked bugs that this pins:

1. ``agent_thought_chunk`` was logged as the literal string instead of
   ``reasoning`` — silently dropped from canonical log.
2. ``agent_message_chunk`` with empty text produced an empty
   ``assistant_message`` row that didn't exist in the SSE stream.
3. ``available_commands_update`` (and other meta updates) were logged
   as themselves rather than skipped.
"""
from __future__ import annotations

import json

import pytest

from api.providers.daytona.session import _parse_sse_block


def _wrap(update: dict) -> str:
    """Wrap a session/update payload as an SSE ``data:`` block."""
    return "data: " + json.dumps({
        "jsonrpc": "2.0",
        "method": "session/update",
        "params": {"update": update},
    })


def _wrap_result(rpc_id: str, result: dict) -> str:
    return "data: " + json.dumps({"jsonrpc": "2.0", "id": rpc_id, "result": result})


# ---------------------------------------------------------------------------
# The reasoning bug — must map agent_thought_chunk → "reasoning"
# ---------------------------------------------------------------------------

def test_agent_thought_chunk_maps_to_reasoning():
    block = _wrap({
        "sessionUpdate": "agent_thought_chunk",
        "content": {"type": "text", "text": "let me think"},
    })
    ev = _parse_sse_block(block, "rpc-1")
    assert ev == {"type": "reasoning", "text": "let me think"}


def test_agent_thought_chunk_with_thinking_field_maps_to_reasoning():
    """Some adapters use ``content.thinking`` instead of ``content.text``."""
    block = _wrap({
        "sessionUpdate": "agent_thought_chunk",
        "content": {"thinking": "internal monologue"},
    })
    ev = _parse_sse_block(block, "rpc-1")
    assert ev == {"type": "reasoning", "text": "internal monologue"}


# ---------------------------------------------------------------------------
# The empty-text bug — must filter content with no text
# ---------------------------------------------------------------------------

def test_agent_message_chunk_empty_text_returns_none():
    block = _wrap({
        "sessionUpdate": "agent_message_chunk",
        "content": {"type": "text", "text": ""},
    })
    assert _parse_sse_block(block, "rpc-1") is None


# ---------------------------------------------------------------------------
# The meta-update bug — must skip non-event updates
# ---------------------------------------------------------------------------

def test_available_commands_update_returns_none():
    block = _wrap({
        "sessionUpdate": "available_commands_update",
        "available_commands": [{"name": "Bash"}],
    })
    assert _parse_sse_block(block, "rpc-1") is None


# ---------------------------------------------------------------------------
# Sanity — regular events still flow through
# ---------------------------------------------------------------------------

def test_agent_message_chunk_real_text_returns_text_event():
    block = _wrap({
        "sessionUpdate": "agent_message_chunk",
        "content": {"type": "text", "text": "hello"},
    })
    assert _parse_sse_block(block, "rpc-1") == {"type": "text", "text": "hello"}


def test_done_result_returns_done_event():
    block = _wrap_result("rpc-1", {"stopReason": "end_turn"})
    assert _parse_sse_block(block, "rpc-1") == {"type": "done", "stop_reason": "end_turn"}


def test_rpc_id_filter_skips_other_prompts():
    block = _wrap_result("OTHER", {"stopReason": "end_turn"})
    assert _parse_sse_block(block, "rpc-1") is None


def test_heartbeat_returns_none():
    assert _parse_sse_block(": heartbeat", "rpc-1") is None
    assert _parse_sse_block("", "rpc-1") is None


# ---------------------------------------------------------------------------
# The persist-side log mapping must match the parser's output types.
# Catches the failure where ``_EVENT_TYPE_TO_LOG`` keys drift away from
# what the parser actually emits.
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("etype,expected_log_type", [
    ("text", "assistant_message"),
    ("reasoning", "reasoning"),
    ("tool", "tool_call"),
    ("tool_result", "tool_result"),
    ("usage", "usage"),
    ("error", "error"),
    ("done", "turn_end"),
])
def test_event_type_to_log_covers_parser_outputs(etype, expected_log_type):
    from api.server import _EVENT_TYPE_TO_LOG
    assert _EVENT_TYPE_TO_LOG.get(etype) == expected_log_type, (
        f"_EVENT_TYPE_TO_LOG must map parser output {etype!r} to "
        f"{expected_log_type!r} so persist and SSE produce the same canonical log"
    )
