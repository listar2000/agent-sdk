"""Shared SSE (Server-Sent Events) parsing utilities."""

import json
from collections.abc import AsyncIterator
from typing import Any

import httpx

# ACP session update type constants
UT_MESSAGE_DELTA = "agent_message_delta"
UT_MESSAGE_CHUNK = "agent_message_chunk"
UT_THOUGHT_CHUNK = "agent_thought_chunk"
UT_TOOL_STARTED = "execute_tool_started"
UT_TOOL_CALL = "tool_call"
UT_TOOL_CALL_UPDATE = "tool_call_update"
UT_USAGE_UPDATED = "usage_updated"
UT_USAGE_UPDATE = "usage_update"

# Read timeout for the session/prompt POST in the SSE prompt-drive
# (``BaseSandboxSession.execute_prompt``). The SSE GET itself uses
# ``read=None`` (the stream stays open for the whole turn); only the POST
# that kicks off the prompt is bounded by this. Provider-agnostic — the
# supervisor.js HTTP contract is identical across daytona/docker/modal/
# unix_local.
_SSE_READ_TIMEOUT_S = 60.0


async def iter_sse_blocks(response: httpx.Response) -> AsyncIterator[str]:
    """Yield SSE blocks from an httpx streaming response."""
    buffer = ""
    async for chunk in response.aiter_text():
        # Normalize \r\n and bare \r to \n (SSE spec allows all three)
        buffer += chunk.replace("\r\n", "\n").replace("\r", "\n")
        while "\n\n" in buffer:
            block, buffer = buffer.split("\n\n", 1)
            yield block


def parse_sse_data(block: str) -> dict[str, Any] | None:
    """Extract and parse JSON from an SSE data block.

    Handles both "data: ..." and "data:..." prefix forms.
    Multi-line data fields are joined before parsing.
    """
    data_lines = []
    for line in block.split("\n"):
        if line.startswith("data: "):
            data_lines.append(line[6:])
        elif line.startswith("data:"):
            data_lines.append(line[5:])
    if not data_lines:
        return None
    try:
        return json.loads("\n".join(data_lines))
    except json.JSONDecodeError:
        return None


def extract_sse_tag(block: str) -> str | None:
    """Read the server-side rpc_id tag from an SSE block.

    The server stamps each block with ``event: rpc:<rpc_id>`` before the
    ``data:`` line (see `/sessions/{id}/events` handler). Returns the rpc_id,
    or None if the block is untagged (heartbeat, bootstrap, or a consumer
    that bypassed the proxy).
    """
    for line in block.split("\n"):
        if line.startswith("event: rpc:"):
            return line[len("event: rpc:"):].strip()
    return None


def parse_acp_payload(payload: dict, rpc_id: str | None) -> tuple[str, dict | None]:
    """Shared JSON-RPC parsing for ACP SSE blocks.

    Returns (kind, data) where kind is one of:
      "done_result" — payload is the result dict (has stopReason)
      "error"       — payload is the error dict
      "update"      — payload is the update dict from session/update notification
      "skip"        — caller should return None
    """
    if "id" in payload and "result" in payload:
        result = payload["result"]
        if isinstance(result, dict) and "stopReason" in result:
            if rpc_id is None or payload["id"] == rpc_id:
                return "done_result", result
        return "skip", None

    if "id" in payload and "error" in payload:
        if rpc_id is not None and payload["id"] != rpc_id:
            return "skip", None
        # Ignore method-not-found errors from setup (e.g. session/setMode on older versions)
        if payload["error"].get("code") == -32601:
            return "skip", None
        return "error", payload["error"]

    if payload.get("method") != "session/update":
        return "skip", None

    return "update", payload.get("params", {}).get("update", {})


def extract_tool_call_id(update: dict) -> str | None:
    """Best-effort tool-call id extraction across adapters.

    Claude Code's native ``tool_use`` block carries an id like
    ``toolu_01CxQuegzYETU618WAS5ofDJ`` that links tool calls to their
    results.  Different adapters surface it in different places; check
    the common ones.
    """
    if not isinstance(update, dict):
        return None

    # Top-level ACP fields
    for key in ("toolCallId", "toolUseId", "tool_call_id", "tool_use_id", "id"):
        v = update.get(key)
        if isinstance(v, str) and v:
            return v

    # _meta.claudeCode.* (where extract_tool_name also looks)
    meta = update.get("_meta", {})
    claude_meta = meta.get("claudeCode", {}) if isinstance(meta, dict) else {}
    if isinstance(claude_meta, dict):
        for key in ("toolUseId", "toolCallId", "tool_use_id", "tool_call_id", "id"):
            v = claude_meta.get(key)
            if isinstance(v, str) and v:
                return v

    # rawInput sometimes echoes the original tool_use block
    raw_input = update.get("rawInput")
    if isinstance(raw_input, dict):
        for key in ("toolUseId", "tool_use_id", "id"):
            v = raw_input.get(key)
            if isinstance(v, str) and v.startswith("toolu_"):
                return v

    return None


def classify_message_content(content: object) -> tuple[str, str] | None:
    """Classify a session/update content block.

    Returns ``("text", value)``, ``("reasoning", value)``, or ``None``.
    Handles both delta-style (``content.thinking``) and block-style
    (``content.type == "thinking"``) reasoning emitted by claude-code.
    Reasoning takes precedence over plain text.
    """
    if not isinstance(content, dict):
        return None
    thinking = content.get("thinking")
    if isinstance(thinking, str) and thinking:
        return "reasoning", thinking
    text = content.get("text")
    if not text:
        return None
    ctype = content.get("type")
    if ctype == "thinking":
        return "reasoning", text
    if ctype is None or ctype == "text":
        return "text", text
    return None


def extract_tool_response(update: dict) -> object | None:
    """Best-effort tool result/output extraction across adapters.

    Returns whatever the adapter put in the result slot — string, dict,
    list, etc. — or ``None`` if the update doesn't carry a result.
    """
    if not isinstance(update, dict):
        return None

    # Claude Code path
    meta = update.get("_meta", {})
    claude_meta = meta.get("claudeCode", {}) if isinstance(meta, dict) else {}
    if isinstance(claude_meta, dict):
        for key in ("toolResponse", "toolResult", "tool_response", "tool_result"):
            v = claude_meta.get(key)
            if v is not None:
                return v

    # Generic ACP fields
    for key in ("toolResponse", "toolResult", "tool_response", "tool_result",
                "output", "result", "content"):
        v = update.get(key)
        if v is not None:
            return v

    return None


def extract_tool_name(update: dict) -> str:
    """Best-effort tool name extraction across different agent adapters."""
    meta = update.get("_meta", {})
    claude_meta = meta.get("claudeCode", {}) if isinstance(meta, dict) else {}
    tool_name = claude_meta.get("toolName")
    if isinstance(tool_name, str) and tool_name:
        return tool_name

    for key in ("toolName", "tool", "name", "kind"):
        value = update.get(key)
        if isinstance(value, str) and value:
            return value

    raw_input = update.get("rawInput")
    if isinstance(raw_input, dict):
        for key in ("toolName", "tool", "name", "kind"):
            value = raw_input.get(key)
            if isinstance(value, str) and value:
                return value

        parsed_cmd = raw_input.get("parsed_cmd")
        if isinstance(parsed_cmd, list) and parsed_cmd:
            first_cmd = parsed_cmd[0]
            if isinstance(first_cmd, dict):
                parsed_type = first_cmd.get("type")
                if isinstance(parsed_type, str) and parsed_type and parsed_type != "unknown":
                    return parsed_type

    title = update.get("title")
    if isinstance(title, str) and title:
        if title.startswith("Run "):
            return "execute"
        first_word = title.split(" ", 1)[0].strip().lower()
        if first_word:
            return first_word

    return "unknown"


def parse_acp_event(block: str, rpc_id: str | None = None) -> dict | None:
    """Parse an SSE block into a structured event dict for astream().

    - text:        {"type": "text", "text": "..."}
    - reasoning:   {"type": "reasoning", "text": "..."}
    - tool:        {"type": "tool", "tool_name": "...", "tool_call_id": "...", "args": ..., "raw": {...}}
    - tool_result: {"type": "tool_result", "tool_name": "...", "tool_call_id": "...", "result": ..., "raw": {...}}
    - done:        {"type": "done", "stop_reason": "..."}
    - error:       {"type": "error", "text": "...", "kind": "...", "data": {...}}
    - usage:       {"type": "usage", "usage": {...}}
    """
    payload = parse_sse_data(block)
    if payload is None:
        return None

    kind, data = parse_acp_payload(payload, rpc_id)

    if kind == "done_result":
        return {"type": "done", "stop_reason": data["stopReason"]}
    if kind == "error":
        err_data = data.get("data") or {}
        return {
            "type": "error",
            "text": data.get("message", "Unknown error"),
            "kind": err_data.get("kind") if isinstance(err_data, dict) else None,
            "data": err_data if isinstance(err_data, dict) else {},
        }
    if kind == "skip" or data is None:
        return None

    # kind == "update"
    ut = data.get("sessionUpdate", "")

    if ut in (UT_MESSAGE_DELTA, UT_MESSAGE_CHUNK):
        classified = classify_message_content(data.get("content"))
        if classified is not None:
            return {"type": classified[0], "text": classified[1]}

    if ut == UT_THOUGHT_CHUNK:
        content = data.get("content", {})
        text = content.get("text") or content.get("thinking") or ""
        if text:
            return {"type": "reasoning", "text": text}

    if ut in (UT_TOOL_CALL, UT_TOOL_STARTED):
        return {
            "type": "tool",
            "tool_name": extract_tool_name(data),
            "tool_call_id": extract_tool_call_id(data),
            "args": data.get("rawInput"),
            "raw": data,
        }

    if ut == UT_TOOL_CALL_UPDATE:
        tool_response = extract_tool_response(data)
        if tool_response is not None:
            return {
                "type": "tool_result",
                "tool_name": extract_tool_name(data),
                "tool_call_id": extract_tool_call_id(data),
                "result": tool_response,
                "raw": data,
            }

    if ut in (UT_USAGE_UPDATED, UT_USAGE_UPDATE):
        return {"type": "usage", "usage": data.get("cost", data)}

    return None
