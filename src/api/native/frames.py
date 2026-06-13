"""ACP-shaped frame synthesis for the native runtime.

The native loop produces canonical event dicts (the ``parse_acp_event``
output taxonomy). Every ``/events`` and ``/message+stream`` consumer,
however, re-parses RAW broadcast blocks — and ``/message+stream``'s terminal
detection inspects raw block text (server.py). So for each event the session
broadcasts ``(rpc_id, block)`` where ``block`` is a byte-compatible
ACP-shaped frame, exactly as supervisor.js would have relayed from a real
ACP child.

Parity is enforced by construction: ``tests/test_native_frames.py`` asserts
``parse_acp_event(block_for(event), rpc_id) == event`` for every template,
including adversarial content. Template shapes are pinned by the design doc
(docs/native_runtime_design.md §2) and were derived from the real frames in
tests/test_acp_error_terminates_turn.py.

Invariants the templates guarantee (see design F2/F5-F8):
- ``args`` rides ``rawInput`` (the only channel ``parse_acp_event`` reads).
- tool_call_update always carries a non-None result slot and repeats
  toolName/toolCallId (omitting either degrades or drops the event).
- usage is wrapped in ``cost`` and must be emitted BEFORE done.
- done/error envelopes carry ``id == rpc_id`` byte-equal; error code is
  never -32601 (filtered as handshake noise by parse_acp_payload).
- Tool results are STRINGIFIED into text content: a raw ``"error":`` or
  ``result.stopReason`` key in a non-terminal frame would trip the stream's
  terminal detection (precise detection lands in P0-B, but frames stay
  conservative so older servers behave too).
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


# ── fast path for the per-TOKEN templates ───────────────────────────────────
# ``text``/``reasoning`` are emitted once per LLM stream delta — the hottest
# events by far (hundreds-to-thousands per turn vs a handful of tool/usage
# events). Each is a fixed envelope around ONE dynamic string, so instead of
# building a 4-level nested dict and walking it through the JSON encoder every
# token, we concatenate constant fragments around a single ``json.dumps`` of
# the dynamic string. ``json.dumps(s)`` produces byte-for-byte the same escaped,
# quoted form that string takes inside the dict dump (string encoding is
# independent of ``separators`` and of nesting), so the wire bytes are
# identical — pinned by tests/test_native_frames.py's byte-parity corpus.
# Measured ~5× faster than the dict-dump path; the FrameEncoder below caches
# the per-session prefix for a further ~1.7×. The multi-field templates
# (tool/tool_result/usage) keep the dict path — they're rare and their brace
# sequences aren't worth hand-deriving.
_TEXT_PRE = ('data: {"jsonrpc":"2.0","method":"session/update","params":'
             '{"sessionId":')
_TEXT_MID = (',"update":{"sessionUpdate":"agent_message_chunk","content":'
             '{"type":"text","text":')
_REASON_MID = (',"update":{"sessionUpdate":"agent_thought_chunk","content":'
               '{"type":"text","text":')
_TEXT_SUF = "}}}}"


def text_block(session_id: str, text: str) -> str:
    return (_TEXT_PRE + json.dumps(session_id) + _TEXT_MID
            + json.dumps(text) + _TEXT_SUF)


def reasoning_block(session_id: str, text: str) -> str:
    return (_TEXT_PRE + json.dumps(session_id) + _REASON_MID
            + json.dumps(text) + _TEXT_SUF)


# tool / tool_result fire once per tool call — rarer than text tokens, but a
# tool-heavy turn emits a pair per call, so they get the same fast string-concat
# treatment: constant fragments around a single ``json.dumps`` of each dynamic
# field (the args dict still needs a compact dump for ``rawInput``; result text
# is one string). Byte-identical to the dict-dump form — pinned by the
# independent reference in tests/test_native_frames.py.
_TOOL_PRE = ('data: {"jsonrpc":"2.0","method":"session/update","params":'
             '{"sessionId":')
_TOOL_MID = ',"update":{"sessionUpdate":"tool_call","toolCallId":'
_TOOL_NAME = ',"toolName":'
_TOOL_RAWIN = ',"rawInput":'
_TOOL_SUF = ',"status":"pending"}}}'
_TR_MID = ',"update":{"sessionUpdate":"tool_call_update","toolCallId":'
_TR_STATUS = (',"status":"completed","content":[{"type":"content","content":'
              '{"type":"text","text":')
_TR_SUF = "}}]}}}"


def tool_block(session_id: str, tool_call_id: str, tool_name: str,
               args: dict | None) -> str:
    return (_TOOL_PRE + json.dumps(session_id) + _TOOL_MID
            + json.dumps(tool_call_id) + _TOOL_NAME + json.dumps(tool_name)
            + _TOOL_RAWIN + json.dumps(args or {}, separators=_COMPACT)
            + _TOOL_SUF)


def tool_result_block(session_id: str, tool_call_id: str, tool_name: str,
                      result_text: str) -> str:
    return (_TOOL_PRE + json.dumps(session_id) + _TR_MID
            + json.dumps(tool_call_id) + _TOOL_NAME + json.dumps(tool_name)
            + _TR_STATUS + json.dumps(result_text) + _TR_SUF)


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
        return text_block(session_id, event["text"])
    if t == "reasoning":
        return reasoning_block(session_id, event["text"])
    if t == "tool":
        return tool_block(session_id, event["tool_call_id"],
                          event["tool_name"], event.get("args"))
    if t == "tool_result":
        return tool_result_block(session_id, event["tool_call_id"],
                                 event["tool_name"], str(event["result"]))
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


class FrameEncoder:
    """Per-session frame builder — caches the session-constant prefix of the
    per-token templates so streaming a delta is one ``json.dumps`` of the text
    plus two string concatenations, never a dict build + encode.

    ``session_id`` is fixed for a session's whole life, so ``json.dumps`` of it
    (and the fixed envelope around it) is computed ONCE here instead of on every
    event. Output is byte-identical to the stateless module functions (same
    fragments, same ``json.dumps`` of the dynamic fields); the remaining cold
    templates (usage/done/error — once per turn) delegate to ``block_for_event``.
    NativeSession holds one of these per session.
    """

    __slots__ = ("session_id", "_text_pre", "_reason_pre", "_tool_pre",
                 "_tr_pre")

    def __init__(self, session_id: str) -> None:
        self.session_id = session_id
        sid = json.dumps(session_id)
        self._text_pre = _TEXT_PRE + sid + _TEXT_MID
        self._reason_pre = _TEXT_PRE + sid + _REASON_MID
        self._tool_pre = _TOOL_PRE + sid + _TOOL_MID
        self._tr_pre = _TOOL_PRE + sid + _TR_MID

    def block_for_event(self, event: dict[str, Any], rpc_id: str) -> str:
        t = event["type"]
        if t == "text":
            return self._text_pre + json.dumps(event["text"]) + _TEXT_SUF
        if t == "reasoning":
            return self._reason_pre + json.dumps(event["text"]) + _TEXT_SUF
        if t == "tool":
            return (self._tool_pre + json.dumps(event["tool_call_id"])
                    + _TOOL_NAME + json.dumps(event["tool_name"]) + _TOOL_RAWIN
                    + json.dumps(event.get("args") or {}, separators=_COMPACT)
                    + _TOOL_SUF)
        if t == "tool_result":
            return (self._tr_pre + json.dumps(event["tool_call_id"])
                    + _TOOL_NAME + json.dumps(event["tool_name"]) + _TR_STATUS
                    + json.dumps(str(event["result"])) + _TR_SUF)
        return block_for_event(event, rpc_id, self.session_id)
