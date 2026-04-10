"""Shared SSE (Server-Sent Events) parsing utilities."""

import json
from collections.abc import AsyncIterator
from typing import Any

import httpx

# ACP session update type constants
UT_MESSAGE_DELTA = "agent_message_delta"
UT_MESSAGE_CHUNK = "agent_message_chunk"
UT_MESSAGE_CREATED = "agent_message_created"
UT_TOOL_STARTED = "execute_tool_started"
UT_TOOL_COMPLETED = "execute_tool_completed"
UT_TOOL_CALL = "tool_call"
UT_TOOL_CALL_UPDATE = "tool_call_update"
UT_USAGE_UPDATED = "usage_updated"
UT_USAGE_UPDATE = "usage_update"
UT_COMMANDS_UPDATE = "available_commands_update"


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


def parse_acp_text(block: str, rpc_id: str | None = None) -> dict | None:
    """Parse an SSE block containing ACP JSON-RPC.

    Returns {"type": "text"|"tool"|"done"|"error", "text": ...} or None.
    """
    payload = parse_sse_data(block)
    if payload is None:
        return None

    kind, data = parse_acp_payload(payload, rpc_id)

    if kind == "done_result":
        return {"type": "done", "text": ""}
    if kind == "error":
        return {"type": "error", "text": data.get("message", "Unknown error")}
    if kind == "skip" or data is None:
        return None

    # kind == "update"
    ut = data.get("sessionUpdate", "")

    if ut in (UT_MESSAGE_DELTA, UT_MESSAGE_CHUNK):
        text = data.get("content", {}).get("text", "")
        if text:
            return {"type": "text", "text": text}

    if ut in (UT_TOOL_CALL, UT_TOOL_STARTED):
        tool_name = extract_tool_name(data)
        return {"type": "tool", "text": f"\n[tool: {tool_name}]\n"}

    return None


def parse_acp_event(block: str, rpc_id: str | None = None) -> dict | None:
    """Parse an SSE block into a structured event dict for astream_events().

    Returns richer event dicts than parse_acp_text:
    - text:  {"type": "text", "text": "..."}
    - tool:  {"type": "tool", "tool_name": "...", "args": ..., "raw": {...}}
    - tool_result: {"type": "tool_result", "tool_name": "...", "result": ..., "raw": {...}}
    - done:  {"type": "done", "stop_reason": "..."}
    - error: {"type": "error", "text": "..."}
    - usage: {"type": "usage", "usage": {...}}
    """
    payload = parse_sse_data(block)
    if payload is None:
        return None

    kind, data = parse_acp_payload(payload, rpc_id)

    if kind == "done_result":
        return {"type": "done", "stop_reason": data["stopReason"]}
    if kind == "error":
        return {"type": "error", "text": data.get("message", "Unknown error")}
    if kind == "skip" or data is None:
        return None

    # kind == "update"
    ut = data.get("sessionUpdate", "")

    if ut in (UT_MESSAGE_DELTA, UT_MESSAGE_CHUNK):
        text = data.get("content", {}).get("text", "")
        if text:
            return {"type": "text", "text": text}

    if ut in (UT_TOOL_CALL, UT_TOOL_STARTED):
        return {
            "type": "tool",
            "tool_name": extract_tool_name(data),
            "args": data.get("rawInput"),
            "raw": data,
        }

    if ut == UT_TOOL_CALL_UPDATE:
        meta = data.get("_meta", {}).get("claudeCode", {})
        tool_response = meta.get("toolResponse")
        if tool_response:
            return {
                "type": "tool_result",
                "tool_name": extract_tool_name(data),
                "result": tool_response,
                "raw": data,
            }

    if ut in (UT_USAGE_UPDATED, UT_USAGE_UPDATE):
        return {"type": "usage", "usage": data.get("cost", data)}

    return None
