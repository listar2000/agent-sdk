"""Smoke-test Codex on Modal through a local Agent SDK server.

Prerequisites:
  1. Codex is logged in locally (``codex login --device-auth``).
  2. The Agent SDK server is running on http://localhost:7778.
  3. Modal credentials are configured in ~/.modal.toml.

Run:
  .venv/bin/python examples/test_codex_modal.py
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from pathlib import Path


sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from agent_sdk import Agent, ApiClient  # noqa: E402


async def run(api_url: str, auth_cache_path: Path) -> None:
    try:
        auth_cache = auth_cache_path.read_text()
        if not isinstance(json.loads(auth_cache), dict):
            raise ValueError("top-level value must be a JSON object")
    except (OSError, json.JSONDecodeError, ValueError) as exc:
        raise SystemExit(f"Invalid Codex auth cache at {auth_cache_path}: {exc}")

    agent = Agent(
        "codex-modal-smoke",
        agent_type="codex",
        provider="modal",
        api_url=api_url,
        secrets={"CODEX_AUTH_JSON": auth_cache},
    )

    try:
        reply = await agent.arun(
            "Use the shell to create codex_modal_probe.txt in the current "
            "working directory containing exactly codex-modal-ok, then reply "
            "with exactly: done"
        )
        contents = await agent.sandbox.read_file("codex_modal_probe.txt")
        if contents.strip() != "codex-modal-ok":
            raise RuntimeError(
                "Codex replied but the Modal filesystem effect was incorrect: "
                f"{contents!r}"
            )
        print(f"Codex reply: {reply.strip()}")
        print("Modal filesystem check: PASS")
    finally:
        session_id = agent.session_id
        if session_id:
            async with ApiClient(api_url) as cleanup:
                await cleanup.delete_session(session_id)
        await agent.aclose()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--api-url",
        default="http://localhost:7778",
        help="Agent SDK server URL (default: http://localhost:7778)",
    )
    parser.add_argument(
        "--auth-cache",
        type=Path,
        default=Path.home() / ".codex" / "auth.json",
        help="Codex login cache path (default: ~/.codex/auth.json)",
    )
    args = parser.parse_args()
    asyncio.run(run(args.api_url, args.auth_cache.expanduser()))


if __name__ == "__main__":
    main()
