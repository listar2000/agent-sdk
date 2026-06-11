"""Native frame synthesis — byte-level parity with the real ACP parsers.

Pins the wire contract from docs/native_runtime_design.md §2: for every
event the native loop emits, ``parse_acp_event(block, rpc_id)`` must
reconstruct the canonical event. Any change to api/sse.py parsing that
breaks native frames fails here loudly (and vice versa).

Promoted from the design-phase validation spike (42/42).
"""

from __future__ import annotations

import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from api.sse import parse_acp_event  # noqa: E402
from api.native import frames  # noqa: E402

RPC = "rpc-frames-test"
SID = "sess-native-test"


def parse(block: str):
    return parse_acp_event(block, RPC)


def test_text_roundtrip():
    ev = parse(frames.text_block(SID, "Hello world"))
    assert ev["type"] == "text" and ev["text"] == "Hello world"


def test_reasoning_roundtrip():
    ev = parse(frames.reasoning_block(SID, "thinking..."))
    assert ev["type"] == "reasoning" and ev["text"] == "thinking..."


def test_tool_args_ride_rawinput():
    args = {"command": "echo hi", "timeout_s": 30}
    ev = parse(frames.tool_block(SID, "call_abc123", "bash", args))
    assert ev["type"] == "tool"
    assert ev["tool_name"] == "bash"
    assert ev["tool_call_id"] == "call_abc123"
    # parse_acp_event builds args exclusively from rawInput (sse.py) —
    # this is the only channel; regression here means SDK sees args=None.
    assert ev["args"] == args


def test_tool_result_slot_and_identity():
    ev = parse(frames.tool_result_block(SID, "call_abc123", "bash", "hi\n"))
    assert ev["type"] == "tool_result"
    assert ev["tool_name"] == "bash"
    assert ev["tool_call_id"] == "call_abc123"
    # A tool_call_update with no extractable result is DROPPED entirely by
    # the parser — result slot must always be present.
    assert ev["result"] is not None


def test_adversarial_tool_result_content_parses():
    """Result text containing JSON-looking error/stopReason keys must still
    parse as a normal tool_result (stringified-into-text contract)."""
    nasty = json.dumps({"error": "boom", "stopReason": "fake"}) + ' and "error": literal'
    ev = parse(frames.tool_result_block(SID, "call_x", "bash", nasty))
    assert ev["type"] == "tool_result"
    # JSON string-escaping protects the '"error":' substring class: the raw
    # block must not contain an unescaped top-level-looking '"error":'
    # outside the stringified content. (Bare 'stopReason' in content is
    # handled by the precise terminal detection from P0-B, not by frames.)
    block = frames.tool_result_block(SID, "call_x", "bash", nasty)
    payload = json.loads(block[len("data: "):])
    assert "error" not in payload  # no top-level error key


def test_usage_cost_unwrap_no_leak():
    ev = parse(frames.usage_block(SID, 10, 5, 0.001))
    assert ev["type"] == "usage"
    u = ev["usage"]
    assert u["inputTokens"] == 10 and u["outputTokens"] == 5
    assert u["totalCostUsd"] == 0.001
    assert "sessionUpdate" not in u


def test_done_roundtrip_all_stop_reasons():
    for stop in ("end_turn", "cancelled", "max_turns"):
        ev = parse(frames.done_block(RPC, stop))
        assert ev["type"] == "done" and ev["stop_reason"] == stop


def test_done_wrong_rpc_dropped():
    # Envelope id must byte-equal the rpc — mismatches are silently skipped
    # by the parser, so a wrong id would hang every golden.
    assert parse_acp_event(frames.done_block("other-rpc", "end_turn"), RPC) is None


def test_error_roundtrip_and_32601_filter():
    ev = parse(frames.error_block(RPC, "agent exploded", "LoopError"))
    assert ev["type"] == "error"
    assert "agent exploded" in ev["text"]
    assert ev["kind"] == "LoopError"
    # -32601 is filtered as handshake noise — frames must never use it.
    handshake = "data: " + json.dumps(
        {"jsonrpc": "2.0", "id": RPC,
         "error": {"code": -32601, "message": "method not found"}},
        separators=(",", ":"))
    assert parse_acp_event(handshake, RPC) is None


def test_block_for_event_dispatch_total():
    """block_for_event maps every canonical type; unknown types raise."""
    cases = [
        {"type": "text", "text": "t"},
        {"type": "reasoning", "text": "r"},
        {"type": "tool", "tool_call_id": "c1", "tool_name": "bash",
         "args": {"command": "ls"}},
        {"type": "tool_result", "tool_call_id": "c1", "tool_name": "bash",
         "result": "ok"},
        {"type": "usage", "usage": {"inputTokens": 1, "outputTokens": 2,
                                    "totalCostUsd": 0.0}},
        {"type": "done", "stop_reason": "end_turn"},
        {"type": "error", "text": "x", "kind": "K"},
    ]
    for ev in cases:
        block = frames.block_for_event(ev, RPC, SID)
        parsed = parse_acp_event(block, RPC)
        assert parsed is not None and parsed["type"] == ev["type"], (
            f"{ev['type']}: {parsed!r}")
    try:
        frames.block_for_event({"type": "mystery"}, RPC, SID)
    except KeyError:
        pass
    else:
        raise AssertionError("unknown event type must raise KeyError")


def test_block_format_is_single_line_data_prefixed():
    b = frames.text_block(SID, "x")
    assert b.startswith("data: ") and "\n" not in b
