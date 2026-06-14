"""Built-in tools for the native runtime — thin wrappers over a
``SandboxTransport`` (api/native/transport.py). Each tool is a function
schema (OpenAI/LiteLLM tool-calling format) plus an async ``invoke`` that
turns validated args into a string result the model sees next turn.

The toolset deliberately mirrors the CLI agents' effect surface so the
tool-effects golden runs unchanged: bash, read_file, write_file, edit_file.
Everything executes IN THE SANDBOX via the transport; nothing runs on the
server host.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Awaitable, Callable

log = logging.getLogger(__name__)

#: per-tool output cap fed back to the model (chars). The transport already
#: caps raw bytes; this bounds the prompt-token cost of a chatty tool.
_RESULT_CHAR_CAP = 30_000


def _cap(text: str) -> str:
    if len(text) <= _RESULT_CHAR_CAP:
        return text
    head = text[: _RESULT_CHAR_CAP - 80]
    return head + f"\n…[truncated {len(text) - len(head)} chars]"


@dataclass
class Tool:
    name: str
    description: str
    parameters: dict          # JSON schema for the function's args
    invoke: Callable[[Any, dict], Awaitable[str]]  # (transport, args) -> result

    def __post_init__(self) -> None:
        # Precompute the (immutable) OpenAI/LiteLLM function schema once at
        # construction. ``run_turn`` reads ``t.schema`` for every tool on every
        # turn; rebuilding the nested dict each time is pure GC churn. Safe to
        # share one dict across turns: run_turn already reuses ONE schema list
        # across every model round of a turn, so the provider doesn't mutate it
        # (a destructive mutation would break multi-round tool turns today).
        self.schema = {
            "type": "function",
            "function": {
                "name": self.name,
                "description": self.description,
                "parameters": self.parameters,
            },
        }


# ── tool implementations ────────────────────────────────────────────────────

async def _bash(transport, args: dict) -> str:
    command = args.get("command")
    if not isinstance(command, str) or not command.strip():
        return "error: 'command' (non-empty string) is required"
    timeout_s = int(args.get("timeout_s") or 300)
    res = await transport.exec(command, cwd=args.get("cwd"), timeout_s=timeout_s)
    parts = []
    if res.stdout:
        parts.append(res.stdout.rstrip("\n"))
    if res.stderr:
        parts.append("[stderr]\n" + res.stderr.rstrip("\n"))
    if res.timed_out:
        parts.append(f"[timed out after {timeout_s}s]")
    parts.append(f"[exit code {res.exit_code}]")
    return _cap("\n".join(parts) if parts else f"[exit code {res.exit_code}]")


async def _read_file(transport, args: dict) -> str:
    path = args.get("path")
    if not isinstance(path, str) or not path:
        return "error: 'path' is required"
    try:
        data = await transport.read_file(path)
    except FileNotFoundError as e:
        return f"error: {e}"
    try:
        return _cap(data.decode("utf-8"))
    except UnicodeDecodeError:
        return f"error: {path} is not UTF-8 text ({len(data)} bytes)"


async def _write_file(transport, args: dict) -> str:
    path = args.get("path")
    content = args.get("content")
    if not isinstance(path, str) or not path:
        return "error: 'path' is required"
    if not isinstance(content, str):
        return "error: 'content' (string) is required"
    await transport.write_file(path, content.encode("utf-8"))
    return f"wrote {len(content)} chars to {path}"


async def _edit_file(transport, args: dict) -> str:
    """Exact string replacement — read, replace, write back. ``old`` must
    occur exactly once unless ``replace_all`` is set (mirrors the CLI edit
    tools' uniqueness contract so the model can't silently corrupt a file)."""
    path = args.get("path")
    old = args.get("old")
    new = args.get("new")
    if not isinstance(path, str) or not path:
        return "error: 'path' is required"
    if not isinstance(old, str) or not isinstance(new, str):
        return "error: 'old' and 'new' (strings) are required"
    try:
        text = (await transport.read_file(path)).decode("utf-8")
    except FileNotFoundError as e:
        return f"error: {e}"
    except UnicodeDecodeError:
        return f"error: {path} is not UTF-8 text"
    count = text.count(old)
    if count == 0:
        return f"error: 'old' not found in {path}"
    if count > 1 and not args.get("replace_all"):
        return (f"error: 'old' occurs {count}× in {path}; pass "
                f"replace_all=true or include more surrounding context")
    updated = text.replace(old, new)
    await transport.write_file(path, updated.encode("utf-8"))
    where = f"{count} occurrences" if args.get("replace_all") else "1 occurrence"
    return f"edited {path} ({where})"


# ── registry ────────────────────────────────────────────────────────────────

_BUILTINS: dict[str, Tool] = {
    "bash": Tool(
        name="bash",
        description="Run a shell command in the sandbox and return its "
                    "stdout/stderr and exit code.",
        parameters={
            "type": "object",
            "properties": {
                "command": {"type": "string", "description": "shell command"},
                "cwd": {"type": "string",
                        "description": "working directory (optional)"},
                "timeout_s": {"type": "integer",
                              "description": "max seconds (default 300)"},
            },
            "required": ["command"],
        },
        invoke=_bash,
    ),
    "read_file": Tool(
        name="read_file",
        description="Read a UTF-8 text file from the sandbox.",
        parameters={
            "type": "object",
            "properties": {"path": {"type": "string"}},
            "required": ["path"],
        },
        invoke=_read_file,
    ),
    "write_file": Tool(
        name="write_file",
        description="Create or overwrite a file in the sandbox with the "
                    "given content.",
        parameters={
            "type": "object",
            "properties": {
                "path": {"type": "string"},
                "content": {"type": "string"},
            },
            "required": ["path", "content"],
        },
        invoke=_write_file,
    ),
    "edit_file": Tool(
        name="edit_file",
        description="Replace an exact string in a sandbox file. 'old' must "
                    "be unique unless replace_all is true.",
        parameters={
            "type": "object",
            "properties": {
                "path": {"type": "string"},
                "old": {"type": "string", "description": "exact text to replace"},
                "new": {"type": "string", "description": "replacement text"},
                "replace_all": {"type": "boolean"},
            },
            "required": ["path", "old", "new"],
        },
        invoke=_edit_file,
    ),
}


def build_toolset(tool_names: list[str] | None = None) -> dict[str, Tool]:
    """Resolve a spec's ``tool_names`` to Tool objects. None = all builtins.
    Unknown names raise (a typo'd tool name is a config error, not silent)."""
    if tool_names is None:
        return dict(_BUILTINS)
    out: dict[str, Tool] = {}
    for n in tool_names:
        if n not in _BUILTINS:
            raise KeyError(f"unknown native tool {n!r}; "
                           f"available: {sorted(_BUILTINS)}")
        out[n] = _BUILTINS[n]
    return out
