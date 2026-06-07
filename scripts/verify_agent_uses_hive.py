#!/usr/bin/env python3
"""Have a REAL agent (empty cli_tools/skills → relies on the baked image) USE
the hivespace CLI and report which skills it has. Prints the agent's verbatim
reply + the tool calls it made, so we can see it actually invoked hivespace.

Env: CLAUDE_CODE_OAUTH_TOKEN. Server must run on the baked snapshot.
"""
import asyncio
import os
import sys
import time

from agent_sdk import ApiClient

BASE = os.environ.get("AGENT_SDK_BASE_URL", "http://localhost:7778")
OAUTH = os.environ["CLAUDE_CODE_OAUTH_TOKEN"]

PROMPT = (
    "You're in a sandbox. Use your Bash tool to do two things and then report:\n"
    "1) Run `hivespace --help` and quote the first 2 lines of its output.\n"
    "2) Run `ls ~/.claude/skills` and list the skill directory names you find.\n"
    "Be concise — just the two results."
)


async def main() -> int:
    async with ApiClient(BASE) as sdk:
        sess = await sdk.create_session(
            provider="daytona", agent_type="claude", model="haiku",
            secrets={"CLAUDE_CODE_OAUTH_TOKEN": OAUTH},
        )  # NOTE: no cli_tools, no skills — everything comes from the baked image
        sid = sess["session_id"]
        print(f"[session] {sid}  (cli_tools=∅, skills=∅ → baked image only)")
        try:
            await sdk.send_message(sid, PROMPT)
            deadline = time.time() + 180
            seen = set()
            final = None
            while time.time() < deadline:
                for e in await sdk.get_session_log(sid, limit=200):
                    key = (e.get("id"), e.get("event_type"))
                    if key in seen:
                        continue
                    seen.add(key)
                    et = e.get("event_type")
                    pl = e.get("payload") or {}
                    if et == "tool_call":
                        cmd = str(pl).replace("\n", " ")
                        print(f"   [tool_call] {cmd[:160]}")
                    elif et == "assistant_message":
                        final = pl
                    elif et == "error":
                        print("   [error]", str(pl)[:300])
                if final is not None:
                    break
                await asyncio.sleep(2)
            print("\n=== AGENT REPLY ===")
            print(final.get("text") if isinstance(final, dict) else final)
        finally:
            await sdk.delete_session(sid)
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
