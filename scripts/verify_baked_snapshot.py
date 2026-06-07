#!/usr/bin/env python3
"""Verify a baked daytona snapshot: hive CLI on PATH, default skills present,
NO token in the published image, and the agent still works end-to-end.

Assumes a server is running (default http://localhost:7778) that was launched
with DAYTONA_SNAPSHOT=<the baked snapshot> so daytona sessions use it.

  CLAUDE_CODE_OAUTH_TOKEN must be set (agent creds).
"""
import asyncio
import os
import sys
import time

from agent_sdk import ApiClient

BASE = os.environ.get("AGENT_SDK_BASE_URL", "http://localhost:7778")
OAUTH = os.environ.get("CLAUDE_CODE_OAUTH_TOKEN")

# The hivespace CLI binary is named ``hivespace`` (package builds from the
# private hive-space repo). Token scan looks for the actual PAT prefix VALUE,
# not the literal string ``x-access-token`` (which appears in the hivespace
# source code itself — a false positive).
CHECKS = [
    ("which hivespace", "command -v hivespace || ls ~/.local/bin/hivespace 2>/dev/null || echo __NONE__"),
    ("hivespace --help", "hivespace --help 2>&1 | head -2 || echo __FAIL__"),
    ("skills dir", "ls -1 ~/.claude/skills 2>/dev/null || echo __NO_SKILLS__"),
    ("HOME", "echo HOME=$HOME"),
    ("token-value scan ~/.local", "grep -rIl 'github_pat_\\|ghs_\\|x-access-token:gh' ~/.local 2>/dev/null | head -5 || true; echo __SCAN_DONE__"),
]


async def main() -> int:
    if not OAUTH:
        print("FATAL: CLAUDE_CODE_OAUTH_TOKEN not set", file=sys.stderr)
        return 2
    ok = True
    async with ApiClient(BASE) as sdk:
        sess = await sdk.create_session(
            provider="daytona", agent_type="claude", model="haiku",
            secrets={"CLAUDE_CODE_OAUTH_TOKEN": OAUTH},
        )
        sid = sess["session_id"]
        print(f"[session] {sid}")
        try:
            print("\n=== sandbox exec checks ===")
            for label, cmd in CHECKS:
                r = await sdk.session_sandbox_exec(sid, cmd, timeout=45)
                out = (r.get("stdout") or "").strip()
                err = (r.get("stderr") or "").strip()
                print(f"[{label}] exit={r.get('exit_code')}")
                if out:
                    print("   out: " + out.replace("\n", "\n        "))
                if err:
                    print("   err: " + err[:200])
                if label == "which hivespace" and "__NONE__" in out:
                    ok = False
                if label == "skills dir" and "__NO_SKILLS__" in out:
                    ok = False
                if label == "token-value scan ~/.local":
                    leaked = [ln for ln in out.splitlines() if ln and "__SCAN_DONE__" not in ln]
                    if leaked:
                        ok = False
                        print("   !! TOKEN LEAK in baked image:", leaked)

            print("\n=== agent functional check (prompt) ===")
            resp = await sdk.send_message(
                sid, "Reply with exactly the single word PONG and nothing else.",
            )
            rpc = resp.get("rpc_id")
            print(f"[prompt] rpc_id={rpc} status={resp.get('status')}")
            deadline = time.time() + 120
            reply = None
            while time.time() < deadline:
                events = await sdk.get_session_log(sid, limit=80)
                for e in events:
                    if e.get("event_type") == "assistant_message":
                        reply = e.get("payload")
                    if e.get("event_type") == "error":
                        print("   !! agent error event:", str(e.get("payload"))[:300])
                        ok = False
                if reply is not None:
                    break
                await asyncio.sleep(2)
            print("[assistant_message payload]", str(reply)[:400])
            if reply is None:
                ok = False
                print("   !! no assistant reply within 120s")
        finally:
            await sdk.delete_session(sid)
            print("\n[cleanup] session deleted")
    print("\n=== RESULT:", "PASS" if ok else "FAIL", "===")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
