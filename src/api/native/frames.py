"""ACP-shaped frame synthesis for the native runtime.

The native loop produces canonical event dicts (the ``parse_acp_event``
output taxonomy). Every ``/events`` and ``/message+stream`` consumer,
however, re-parses RAW broadcast blocks — and ``/message+stream``'s terminal
detection inspects raw block text (server.py). So for each event the session
broadcasts ``(rpc_id, block)`` where ``block`` is an ACP-shaped frame, exactly
as supervisor.js would have relayed from a real ACP child.

Each block is just the canonical dict run through ``json.dumps`` (compact
separators). Parity is enforced by ``tests/test_native_frames.py``:
``parse_acp_event(block_for_event(event), rpc_id) == event`` for every type,
including adversarial content. Template shapes are pinned by the design doc
(docs/native_runtime_design.md §2) and were derived from the real frames in
tests/test_acp_error_terminates_turn.py.

Invariants the templates guarantee (see design F2/F5-F8):
- ``args`` rides ``rawInput`` (the only channel ``parse_acp_event`` reads).
- tool_call_update always carries a non-None result slot and repeats
  toolName/toolCallId (omitting either degrades or drops the event).
- usage is wrapped in ``cost`` and must be emitted BEFORE done.
- done/error envelopes carry ``id == rpc_id``; error code is never -32601
  (filtered as handshake noise by parse_acp_payload).
- Tool results are STRINGIFIED into text content: a raw ``"error":`` or
  ``result.stopReason`` key in a non-terminal frame would trip the stream's
  terminal detection, so frames stay conservative.

History note: an earlier version hand-built byte-concatenated string templates
(``_TEXT_PRE``/``_MID``/``_SUF`` …) plus a per-session ``FrameEncoder`` cache to
shave per-token synthesis from ~2.2 µs to ~0.26 µs. That saved only ~2% of one
core even at 100 concurrent streaming sessions (per-token work is dwarfed by the
model's network-paced token cadence) and cost a byte-fragile second source of
truth that had to be kept in lockstep with these dicts by hand. Removed in favor
of the single dict path below.
"""

from __future__ import annotations

import json
from typing import Any

_COMPACT = (",", ":")


def _block(payload: dict) -> str:
    """One SSE block line, no trailing delimiter — routes add framing."""
    return "data: " + json.dumps(payload, separators=_COMPACT)


def _update(session_id: str, update: dict) -> str:
    return _block({
        "jsonrpc": "2.0",
        "method": "session/update",
        "params": {"sessionId": session_id, "update": update},
    })


def usage_block(session_id: str, input_tokens: int, output_tokens: int,
                total_cost_usd: float) -> str:
    return _update(session_id, {
        "sessionUpdate": "usage_update",
        "cost": {
            "inputTokens": input_tokens,
            "outputTokens": output_tokens,
            "totalCostUsd": total_cost_usd,
        },
    })


def done_block(rpc_id: str, stop_reason: str) -> str:
    return _block({"jsonrpc": "2.0", "id": rpc_id,
                   "result": {"stopReason": stop_reason}})


def error_block(rpc_id: str, message: str, kind: str = "NativeError") -> str:
    return _block({"jsonrpc": "2.0", "id": rpc_id,
                   "error": {"code": -32603, "message": message,
                             "data": {"kind": kind}}})


def block_for_event(event: dict[str, Any], rpc_id: str, session_id: str) -> str:
    """Map one canonical event dict to its broadcast block.

    Raises KeyError on unknown event types — the loop's vocabulary is
    closed; an unmapped type is a programming error, not data.
    """
    t = event["type"]
    if t == "text":
        return _update(session_id, {
            "sessionUpdate": "agent_message_chunk",
            "content": {"type": "text", "text": event["text"]},
        })
    if t == "reasoning":
        return _update(session_id, {
            "sessionUpdate": "agent_thought_chunk",
            "content": {"type": "text", "text": event["text"]},
        })
    if t == "tool":
        return _update(session_id, {
            "sessionUpdate": "tool_call",
            "toolCallId": event["tool_call_id"],
            "toolName": event["tool_name"],
            "rawInput": event.get("args") or {},
            "status": "pending",
        })
    if t == "tool_result":
        return _update(session_id, {
            "sessionUpdate": "tool_call_update",
            "toolCallId": event["tool_call_id"],
            "toolName": event["tool_name"],
            "status": "completed",
            "content": [{"type": "content",
                         "content": {"type": "text",
                                     "text": str(event["result"])}}],
        })
    if t == "usage":
        u = event["usage"]
        return usage_block(session_id, u.get("inputTokens", 0),
                           u.get("outputTokens", 0),
                           u.get("totalCostUsd", 0.0))
    if t == "done":
        return done_block(rpc_id, event["stop_reason"])
    if t == "error":
        return error_block(rpc_id, event.get("text") or "native loop error",
                           event.get("kind") or "NativeError")
    raise KeyError(f"no frame template for event type {t!r}")
