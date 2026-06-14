"""Native frame synthesis — round-trip parity with the real ACP parsers.

Pins the wire contract from docs/native_runtime_design.md §2: for every event
the native loop emits, ``parse_acp_event(block_for_event(event), rpc_id)`` must
reconstruct the canonical event. Any change to api/sse.py parsing that breaks
native frames fails here loudly (and vice versa).

(Frames are now a single ``json.dumps`` of the canonical dict — the earlier
hand-concatenated fast path + its byte-parity corpus were removed as
over-optimized; see frames.py's history note. The contract that matters is the
round-trip below, not the exact bytes.)
"""

from __future__ import annotations

import json

from api.sse import parse_acp_event  # noqa: E402
from api.native import frames  # noqa: E402

RPC = "rpc-frames-test"
SID = "sess-native-test"


def parse(block: str):
    return parse_acp_event(block, RPC)


def block(ev: dict) -> str:
    return frames.block_for_event(ev, RPC, SID)


def test_text_roundtrip():
    ev = parse(block({"type": "text", "text": "Hello world"}))
    assert ev["type"] == "text" and ev["text"] == "Hello world"


def test_reasoning_roundtrip():
    ev = parse(block({"type": "reasoning", "text": "thinking..."}))
    assert ev["type"] == "reasoning" and ev["text"] == "thinking..."


def test_tool_args_ride_rawinput():
    args = {"command": "echo hi", "timeout_s": 30}
    ev = parse(block({"type": "tool", "tool_call_id": "call_abc123",
                      "tool_name": "bash", "args": args}))
    assert ev["type"] == "tool"
    assert ev["tool_name"] == "bash"
    assert ev["tool_call_id"] == "call_abc123"
    # parse_acp_event builds args exclusively from rawInput (sse.py) —
    # this is the only channel; regression here means SDK sees args=None.
    assert ev["args"] == args


def test_tool_result_slot_and_identity():
    ev = parse(block({"type": "tool_result", "tool_call_id": "call_abc123",
                      "tool_name": "bash", "result": "hi\n"}))
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
    b = block({"type": "tool_result", "tool_call_id": "call_x",
               "tool_name": "bash", "result": nasty})
    ev = parse(b)
    assert ev["type"] == "tool_result"
    # JSON string-escaping protects the '"error":' substring class: the raw
    # block must not carry a top-level ``error`` key outside the stringified
    # content (which would trip the stream's terminal detection).
    payload = json.loads(b[len("data: "):])
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
    # Envelope id must equal the rpc — mismatches are silently skipped by the
    # parser, so a wrong id would hang every golden.
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
        parsed = parse_acp_event(block(ev), RPC)
        assert parsed is not None and parsed["type"] == ev["type"], (
            f"{ev['type']}: {parsed!r}")
    try:
        frames.block_for_event({"type": "mystery"}, RPC, SID)
    except KeyError:
        pass
    else:
        raise AssertionError("unknown event type must raise KeyError")


def test_block_format_is_single_line_data_prefixed():
    b = block({"type": "text", "text": "x"})
    assert b.startswith("data: ") and "\n" not in b


# ── adversarial round-trip: nasty content survives synthesis → parse ─────────
# The native loop never emits empty text (``if content:`` guard), so the corpus
# is non-empty; everything else (quotes, backslashes, control chars, unicode,
# brace-injection, JSON-looking payloads) must round-trip exactly.
_ADVERSARIAL = [
    "Hello world", ' "quoted" ', "back\\slash", "new\nline\ttab",
    "ctrl\x01\x1f chars", "unicode é ñ 漢字 🚀", "}}}}injection {{{{",
    '{"error":"x","stopReason":"fake"}', " " * 40,
]
_SIDS = [SID, 's"id', "s\\x", "é-🚀", ""]


def test_adversarial_text_reasoning_roundtrip():
    for sid in _SIDS:
        for s in _ADVERSARIAL:
            for typ in ("text", "reasoning"):
                ev = {"type": typ, "text": s}
                b = frames.block_for_event(ev, RPC, sid)
                assert parse_acp_event(b, RPC) == ev, (sid, s, typ)


def test_adversarial_tool_roundtrip():
    args_corpus = [None, {}, {"command": "ls -la", "n": 3},
                   {"k": 'v"x', "u": "é🚀", "nested": {"a": [1, 2]}},
                   {"}}}": "]injection["}]
    for sid in _SIDS:
        for tcid in ("c1", 'c"x'):
            for name in ("bash", 'wr"ite', "漢字"):
                for r in _ADVERSARIAL:
                    ev = {"type": "tool_result", "tool_call_id": tcid,
                          "tool_name": name, "result": r}
                    got = parse_acp_event(frames.block_for_event(ev, RPC, sid), RPC)
                    assert got["type"] == "tool_result"
                    assert got["tool_call_id"] == tcid and got["tool_name"] == name
                    # the parser returns the structured content slot; the nasty
                    # result text is stringified into it and recoverable intact
                    assert got["result"][0]["content"]["text"] == r
                for a in args_corpus:
                    ev = {"type": "tool", "tool_call_id": tcid,
                          "tool_name": name, "args": a}
                    got = parse_acp_event(frames.block_for_event(ev, RPC, sid), RPC)
                    assert got["type"] == "tool" and got["args"] == (a or {})
