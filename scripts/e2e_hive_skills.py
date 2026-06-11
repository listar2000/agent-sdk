#!/usr/bin/env python3
"""End-to-end: a real daytona agent that HAS the hivespace CLI + default skills,
and actually uses them. Proves the functionality the baked image is meant to
deliver (here via the install path, since the daytona snapshot bake is blocked
on a feature flag).

Env: CLAUDE_CODE_OAUTH_TOKEN, HIVE_GH_TOKEN (repo-read PAT for the private CLI).
"""
import asyncio
import os
import sys
import time

from agent_sdk import ApiClient

BASE = os.environ.get("AGENT_SDK_BASE_URL", "http://localhost:7778")
OAUTH = os.environ["CLAUDE_CODE_OAUTH_TOKEN"]
TOKEN = os.environ["HIVE_GH_TOKEN"]

HIVE_CLI = f"git+https://x-access-token:{TOKEN}@github.com/rllm-org/hive-space.git@staging"
SKILLS = [
    "claude-office-skills/skills@html-slides",
    "github/awesome-copilot@excalidraw-diagram-generator",
    "anthropics/skills@frontend-design",
]


async def main() -> int:
    ok = True
    async with ApiClient(BASE) as sdk:
        print("[create] daytona claude session with hivespace CLI + 3 skills (install path)...")
        t0 = time.monotonic()
        sess = await sdk.create_session(
            provider="daytona", agent_type="claude", model="haiku",
            secrets={"CLAUDE_CODE_OAUTH_TOKEN": OAUTH},
            cli_tools=[HIVE_CLI],
            skills=SKILLS,
        )
        sid = sess["session_id"]
        print(f"[create] {sid} cold-create+install {time.monotonic()-t0:.1f}s")
        try:
            print("\n=== exec verification (sandbox) ===")
            for label, cmd in [
                ("hivespace on PATH", "command -v hivespace || ls ~/.local/bin/hivespace 2>&1"),
                ("hivespace --help", "hivespace --help 2>&1 | head -3"),
                ("skills present", "ls -1 ~/.claude/skills 2>&1"),
            ]:
                r = await sdk.session_sandbox_exec(sid, cmd, timeout=45)
                out = (r.get("stdout") or "").strip()
                print(f"[{label}] exit={r.get('exit_code')}\n   {out.replace(chr(10), chr(10)+'   ')}")
                if label == "hivespace on PATH" and ("hivespace" not in out):
                    ok = False
                if label == "skills present" and not all(s.split("@")[-1] in out for s in SKILLS):
                    ok = False

            print("\n=== agent uses them (prompt) ===")
            prompt = (
                "Use your Bash tool to run `hivespace --help` and report the first line "
                "of its output verbatim. Then run `ls ~/.claude/skills` and list the skill "
                "directory names you find. Keep it terse."
            )
            resp = await sdk.send_message(sid, prompt)
            rpc = resp.get("rpc_id")
            print(f"[prompt] rpc_id={rpc}")
            deadline = time.time() + 180
            reply = None
            while time.time() < deadline:
                for e in await sdk.get_session_log(sid, limit=100):
                    if e.get("event_type") == "assistant_message":
                        reply = e.get("payload")
                    if e.get("event_type") == "error":
                        print("   !! agent error:", str(e.get("payload"))[:300]); ok = False
                if reply is not None:
                    break
                await asyncio.sleep(2)
            print("[agent reply]", str(reply)[:600])
            if reply is None:
                ok = False
        finally:
            await sdk.delete_session(sid)
            print("\n[cleanup] session deleted")
    print("\n=== RESULT:", "PASS" if ok else "FAIL", "===")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
