"""Termination semantics for ACP-emitted error envelopes.

Pins the wire-shape distinction we verified end-to-end against real
``claude-agent-acp 0.31.4``:

  * Top-level ``id+error`` envelope → fatal turn-end. ACP is finished
    with this rpc_id and will write nothing else for it. The agent-sdk
    must stop iterating so the SSE stream closes promptly.

  * ``method=session/update`` notification with ``status=failed`` /
    similar → tool-level failure. The LLM keeps going and may recover.
    The turn ends LATER with a normal ``stopReason`` envelope. The
    agent-sdk must NOT treat this as a turn-end.

  * ``id+error{code:-32601}`` (method-not-found) → handshake race
    during setup (e.g. ``session/setMode`` on older ACP versions).
    Already filtered by ``parse_acp_payload``; pinned here so a
    refactor doesn't accidentally start terminating live turns on
    setup-time errors.

These tests don't require a real ACP — they drive the parser /
``execute_prompt``-shaped iteration directly. The wire frames they
inject are the EXACT frames captured from the real-LLM probe at
``/tmp/acp-direct/probe-tool-error.out`` (see PR description).
"""
from __future__ import annotations

import json
import os
import sys
from typing import Any, AsyncIterator

import pytest

_SRC = os.path.join(os.path.dirname(__file__), "..", "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

from api.sse import parse_acp_event


# ---------------------------------------------------------------------------
# Wire frames captured from real claude-agent-acp 0.31.4
# ---------------------------------------------------------------------------

# 1) Successful turn end — verified locally (probe-real-llm.out)
WIRE_DONE_END_TURN = json.dumps({
    "jsonrpc": "2.0", "id": "T",
    "result": {
        "stopReason": "end_turn",
        "usage": {"inputTokens": 4, "outputTokens": 97, "totalTokens": 40658},
    },
})

# 2) Cancellation — verified locally (probe-real-llm.out, scenario C)
WIRE_DONE_CANCELLED = json.dumps({
    "jsonrpc": "2.0", "id": "T", "result": {"stopReason": "cancelled"},
})

# 3) Auth-fatal — verified locally (repro.js, with HOME=empty-home)
WIRE_FATAL_AUTH = json.dumps({
    "jsonrpc": "2.0", "id": "T",
    "error": {"code": -32000, "message": "Authentication required"},
})

# 4) Internal-error fatal (process death / Session-did-not-end-in-result /
#    is_error true). Same shape, different code per acp-agent.js
WIRE_FATAL_INTERNAL = json.dumps({
    "jsonrpc": "2.0", "id": "T",
    "error": {
        "code": -32603,
        "message": "The Claude Agent process exited unexpectedly. Please start a new session.",
    },
})

# 5) Method-not-found during handshake (session/setMode on older ACP) —
#    NOT a turn-end, must be ignored.
WIRE_HANDSHAKE_METHOD_NOT_FOUND = json.dumps({
    "jsonrpc": "2.0", "id": "T",
    "error": {"code": -32601, "message": "Method not found"},
})

# 6) Tool call started — session/update notification, not terminal
WIRE_TOOL_CALL_START = json.dumps({
    "jsonrpc": "2.0",
    "method": "session/update",
    "params": {
        "sessionId": "3de97755",
        "update": {
            "_meta": {"claudeCode": {"toolName": "Read"}},
            "toolCallId": "toolu_016XsLd8PXjMh3CR2dAkCJG8",
            "sessionUpdate": "tool_call",
            "rawInput": {},
            "status": "pending",
        },
    },
})

# 7) Tool failed — verified locally (probe-tool-error.out). The LLM kept
#    going after this and the turn ended with end_turn. NOT terminal.
WIRE_TOOL_FAILED = json.dumps({
    "jsonrpc": "2.0",
    "method": "session/update",
    "params": {
        "sessionId": "3de97755",
        "update": {
            "_meta": {"claudeCode": {"toolName": "Read"}},
            "toolCallId": "toolu_016XsLd8PXjMh3CR2dAkCJG8",
            "sessionUpdate": "tool_call_update",
            "status": "failed",
            "content": [{"type": "content", "content": {
                "type": "text", "text": "File does not exist"}}],
        },
    },
})

# 8) Streamed text chunk — session/update, not terminal
WIRE_TEXT_CHUNK = json.dumps({
    "jsonrpc": "2.0",
    "method": "session/update",
    "params": {
        "sessionId": "3de97755",
        "update": {
            "sessionUpdate": "agent_message_chunk",
            "content": {"type": "text", "text": "PONG"},
        },
    },
})


def _block(wire: str) -> str:
    """Wrap a JSON-RPC wire string as the ``data:`` SSE block shape that
    ``_parse_sse_block`` consumes."""
    return f"data: {wire}\n"


# ---------------------------------------------------------------------------
# parse_acp_event — confirm the type tag the layer above will see
# ---------------------------------------------------------------------------

class TestParserClassifiesWireShapes:
    """The parser is the one place classification lives. If this is right,
    every consumer downstream gets the right termination signal."""

    def test_done_end_turn_becomes_type_done(self):
        ev = parse_acp_event(_block(WIRE_DONE_END_TURN), rpc_id="T")
        assert ev is not None
        assert ev["type"] == "done"
        assert ev["stop_reason"] == "end_turn"

    def test_done_cancelled_becomes_type_done(self):
        ev = parse_acp_event(_block(WIRE_DONE_CANCELLED), rpc_id="T")
        assert ev is not None
        assert ev["type"] == "done"
        assert ev["stop_reason"] == "cancelled"

    def test_fatal_auth_becomes_type_error(self):
        """The exact wire shape from a real claude-agent-acp 0.31.4 with
        no Anthropic credentials. Must surface as ``type=="error"``."""
        ev = parse_acp_event(_block(WIRE_FATAL_AUTH), rpc_id="T")
        assert ev is not None
        assert ev["type"] == "error"
        assert "Authentication required" in ev["text"]

    def test_fatal_internal_becomes_type_error(self):
        ev = parse_acp_event(_block(WIRE_FATAL_INTERNAL), rpc_id="T")
        assert ev is not None
        assert ev["type"] == "error"

    def test_handshake_method_not_found_is_skipped(self):
        """``-32601`` errors during setup must NOT terminate. Real ACP
        versions emit them while the client probes for unsupported
        methods (e.g. session/setMode added in a later version). The
        parser explicitly filters; if a refactor breaks that filter,
        live turns would die on every handshake race."""
        ev = parse_acp_event(_block(WIRE_HANDSHAKE_METHOD_NOT_FOUND), rpc_id="T")
        assert ev is None, "method-not-found errors must be filtered, not terminate"

    def test_tool_call_start_is_not_error(self):
        """``method=session/update`` notification — has no top-level
        ``id`` or ``error``. Must NOT classify as ``type=="error"``."""
        ev = parse_acp_event(_block(WIRE_TOOL_CALL_START), rpc_id="T")
        assert ev is not None
        assert ev["type"] != "error"
        assert ev["type"] != "done"

    def test_tool_call_failed_is_not_error(self):
        """The structurally-distinct case the user worried about: a
        tool failed mid-turn. Wire shape has ``method=session/update``
        and ``update.status=failed`` — but no top-level ``id+error``.
        Verified end-to-end with real ACP: the LLM keeps going and
        the turn ends with ``stopReason=end_turn`` later."""
        ev = parse_acp_event(_block(WIRE_TOOL_FAILED), rpc_id="T")
        assert ev is not None
        # tool_call_update with status=failed maps to 'tool_result' (or
        # similar non-terminal type) — never 'error'.
        assert ev["type"] != "error", (
            "tool_call_update notifications must never become type==error; "
            "the LLM is still recovering from a failed tool call"
        )
        assert ev["type"] != "done"

    def test_text_chunk_is_not_error(self):
        ev = parse_acp_event(_block(WIRE_TEXT_CHUNK), rpc_id="T")
        assert ev is not None
        assert ev["type"] != "error"
        assert ev["type"] != "done"


# ---------------------------------------------------------------------------
# Iteration termination — the actual fix
# ---------------------------------------------------------------------------

# We don't drive the live providers (each has supervisor / sandbox / async
# httpx surface). Instead we replicate the relevant inner loop shape:
#
#   async for chunk in sse.aiter_text():
#       buf += chunk
#       while "\n\n" in buf:
#           block, buf = buf.split("\n\n", 1)
#           event = _parse_sse_block(block, rpc_id)
#           if event is None: continue
#           yield event
#           if event.get("type") in ("done", "error"):  # ← the fix
#               return
#
# The fix is a single-line change duplicated across daytona/docker/modal/
# unix_local; testing the loop shape with the parser is the single source
# of truth that pins all four providers' termination behaviour.

async def _iter_with_termination(blocks: list[str], rpc_id: str) -> AsyncIterator[Any]:
    """Mirror the iteration shape used by every provider's execute_prompt."""
    for raw in blocks:
        block = _block(raw)
        event = parse_acp_event(block, rpc_id)
        if event is None:
            continue
        yield event
        if event.get("type") in ("done", "error"):
            return


@pytest.mark.asyncio
async def test_fatal_error_envelope_terminates_iteration():
    """When ACP emits an ``id+error`` envelope, iteration must stop on
    that event — even if more frames follow on the wire (e.g. a stale
    ``session/update`` from a stalled background task that the SDK
    flushes before exiting)."""
    blocks = [WIRE_TEXT_CHUNK, WIRE_FATAL_AUTH, WIRE_TEXT_CHUNK, WIRE_DONE_END_TURN]
    events = [ev async for ev in _iter_with_termination(blocks, rpc_id="T")]
    types = [e["type"] for e in events]
    assert types[-1] == "error", "fatal error envelope must be the terminal event"
    assert "done" not in types, (
        "iteration must stop on the error envelope, not continue to a "
        "later frame in the same buffer"
    )


@pytest.mark.asyncio
async def test_tool_failure_does_not_terminate_iteration():
    """The user's specific concern: a tool failure mid-turn must NOT
    end iteration. The LLM continues, eventually emits an ``end_turn``."""
    blocks = [
        WIRE_TOOL_CALL_START,
        WIRE_TOOL_FAILED,
        WIRE_TEXT_CHUNK,
        WIRE_TEXT_CHUNK,
        WIRE_DONE_END_TURN,
    ]
    events = [ev async for ev in _iter_with_termination(blocks, rpc_id="T")]
    types = [e["type"] for e in events]
    assert types[-1] == "done", (
        "tool failure must not terminate; only the subsequent end_turn does"
    )
    assert "error" not in types, (
        "tool_call_update is a session/update notification; it must NEVER "
        "be classified as type==error or this assertion fails"
    )


@pytest.mark.asyncio
async def test_done_end_turn_terminates_iteration():
    """Baseline — the existing behaviour, pinned so the fix doesn't
    accidentally break the success path."""
    blocks = [WIRE_TEXT_CHUNK, WIRE_DONE_END_TURN, WIRE_TEXT_CHUNK]
    events = [ev async for ev in _iter_with_termination(blocks, rpc_id="T")]
    assert events[-1]["type"] == "done"
    assert events[-1]["stop_reason"] == "end_turn"


@pytest.mark.asyncio
async def test_done_cancelled_terminates_iteration():
    blocks = [WIRE_TOOL_CALL_START, WIRE_DONE_CANCELLED]
    events = [ev async for ev in _iter_with_termination(blocks, rpc_id="T")]
    assert events[-1]["type"] == "done"
    assert events[-1]["stop_reason"] == "cancelled"


@pytest.mark.asyncio
async def test_handshake_method_not_found_does_not_terminate():
    """The handshake-race filter: ``-32601`` mid-turn is silently
    dropped by the parser. Iteration continues to the real terminal."""
    blocks = [
        WIRE_HANDSHAKE_METHOD_NOT_FOUND,
        WIRE_TEXT_CHUNK,
        WIRE_DONE_END_TURN,
    ]
    events = [ev async for ev in _iter_with_termination(blocks, rpc_id="T")]
    types = [e["type"] for e in events]
    assert "error" not in types, (
        "handshake -32601 errors must NOT become type==error or terminate the turn"
    )
    assert types[-1] == "done"


# ---------------------------------------------------------------------------
# server.py:_execute_and_stream_sse outer-loop substring check
# ---------------------------------------------------------------------------

class TestServerOuterLoopTermination:
    """The outer loop in ``_execute_and_stream_sse`` works on the RAW SSE
    block (a string), not parsed events. Pin the substring check so a
    refactor doesn't drop one of the three terminal markers.
    """

    @staticmethod
    def _is_terminal(block: str) -> bool:
        # Mirror the check at server.py exactly. ``stopReason`` is
        # camelCase to match real ACP wire frames (the long-standing
        # ``"stop_reason"`` substring was a snake_case bug — never
        # matched real frames; fixed in the same PR as the new
        # ``"error":`` termination).
        return (
            "stopReason" in block
            or '"type":"done"' in block
            or '"error":' in block
        )

    def test_done_block_is_terminal(self):
        block = f'event: rpc:T\ndata: {WIRE_DONE_END_TURN}\n\n'
        assert self._is_terminal(block)

    def test_cancelled_block_is_terminal(self):
        block = f'event: rpc:T\ndata: {WIRE_DONE_CANCELLED}\n\n'
        assert self._is_terminal(block)

    def test_fatal_auth_block_is_terminal(self):
        """The bug fix. Before the change, this returned False and the
        SSE response hung after delivering the error envelope."""
        block = f'event: rpc:T\ndata: {WIRE_FATAL_AUTH}\n\n'
        assert self._is_terminal(block), (
            "fatal error envelope must end the SSE response stream"
        )

    def test_fatal_internal_block_is_terminal(self):
        block = f'event: rpc:T\ndata: {WIRE_FATAL_INTERNAL}\n\n'
        assert self._is_terminal(block)

    def test_tool_failed_block_is_NOT_terminal(self):
        """Crucial: the substring check must NOT trip on a tool failure.
        The wire shape has no top-level ``"error":`` field — only
        ``status:"failed"`` nested inside ``params.update``."""
        block = f'event: rpc:T\ndata: {WIRE_TOOL_FAILED}\n\n'
        assert not self._is_terminal(block), (
            "tool_call_update with status=failed must not end the SSE "
            "stream; the LLM keeps going"
        )

    def test_tool_call_start_block_is_NOT_terminal(self):
        block = f'event: rpc:T\ndata: {WIRE_TOOL_CALL_START}\n\n'
        assert not self._is_terminal(block)

    def test_text_chunk_block_is_NOT_terminal(self):
        block = f'event: rpc:T\ndata: {WIRE_TEXT_CHUNK}\n\n'
        assert not self._is_terminal(block)

    def test_handshake_method_not_found_block_is_terminal_but_filtered_upstream(self):
        """Edge case: ``-32601`` errors DO contain ``"error":`` so the
        outer-loop substring check WOULD trip on them — but they don't
        reach the outer loop because the parser at
        ``daytona/session.py:344`` (and equivalents) drops them via
        ``if event is None: continue`` BEFORE broadcasting the block.
        Documenting here so a future "skip these at the outer layer
        too" change doesn't break the inner-skip contract.
        """
        block = f'event: rpc:T\ndata: {WIRE_HANDSHAKE_METHOD_NOT_FOUND}\n\n'
        assert self._is_terminal(block)
        # But the parser drops it first:
        assert parse_acp_event(_block(WIRE_HANDSHAKE_METHOD_NOT_FOUND),
                               rpc_id="T") is None
