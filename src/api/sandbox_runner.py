#!/usr/bin/env python3
"""Runner script for executing claude-agent-sdk queries inside a Daytona sandbox.

This script is copied into the sandbox by the AFE server and executed via
sandbox.process.exec(). It runs a single query() call and outputs results
as JSON to stdout.

Usage:
    python sandbox_runner.py '<message>' '<options_json>'

    # Or via stdin:
    echo '{"message": "...", "options": {...}}' | python sandbox_runner.py
"""

import asyncio
import json
import sys
import os


async def run_query(message: str, options: dict) -> dict:
    """Run a claude-agent-sdk query and collect results."""
    try:
        from claude_agent_sdk import query, ClaudeAgentOptions, AssistantMessage, ResultMessage
    except ImportError:
        return {
            "error": "claude-agent-sdk not installed. Run: pip install claude-agent-sdk",
            "text": "",
            "resume_id": None,
            "tools": [],
        }

    accumulated_text = ""
    tools_used = []
    resume_id = None

    # Build options object
    allowed_tools = options.get("allowed_tools", ["Read", "Edit", "Write", "Bash", "Glob", "Grep"])

    opts_kwargs = {
        "allowed_tools": allowed_tools,
        "permission_mode": "bypassPermissions",
        "model": options.get("model", "claude-sonnet-4-6"),
        "resume": options.get("resume"),
        "cwd": options.get("cwd", "/workspace"),
    }
    if options.get("effort"):
        opts_kwargs["effort"] = options["effort"]
    query_options = ClaudeAgentOptions(**opts_kwargs)

    # Add system prompt if provided
    system_prompt = options.get("system_prompt")
    if system_prompt:
        query_options.system_prompt = {
            "type": "preset",
            "preset": "claude_code",
            "append": system_prompt,
        }

    try:
        async for msg in query(prompt=message, options=query_options):
            if isinstance(msg, AssistantMessage):
                for block in msg.content:
                    if hasattr(block, "text"):
                        if accumulated_text and not accumulated_text[-1].isspace() and block.text and not block.text[0].isspace():
                            accumulated_text += " "
                        accumulated_text += block.text
                    elif hasattr(block, "name"):
                        tools_used.append({
                            "role": "tool",
                            "tool_call_id": getattr(block, "id", ""),
                            "tool_name": block.name,
                            "tool_args": getattr(block, "input", {}),
                            "created_at": __import__("time").time(),
                        })
            elif isinstance(msg, ResultMessage):
                resume_id = msg.session_id
    except Exception as e:
        return {
            "error": str(e),
            "text": accumulated_text,
            "resume_id": resume_id,
            "tools": tools_used,
        }

    return {
        "text": accumulated_text,
        "resume_id": resume_id,
        "tools": tools_used,
        "error": None,
    }


def main():
    if len(sys.argv) >= 3:
        message = sys.argv[1]
        options = json.loads(sys.argv[2])
    elif not sys.stdin.isatty():
        data = json.loads(sys.stdin.read())
        message = data["message"]
        options = data.get("options", {})
    else:
        print(json.dumps({"error": "No input provided", "text": "", "resume_id": None, "tools": []}))
        sys.exit(1)

    result = asyncio.run(run_query(message, options))
    # Output as a single JSON line — the server parses this
    print(json.dumps(result))


if __name__ == "__main__":
    main()
