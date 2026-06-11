#!/usr/bin/env python3
"""Measure the per-command latency of `npx -y skills add` and `uv tool install`
on a COLD install vs a WARM no-op re-run — the warm pass is exactly what a
pre-baked image pays when hive's pre_start re-runs the (already-installed)
commands.

Creates a bare daytona sandbox, runs the install set twice in-sandbox with
millisecond timing, prints both passes, cleans up.

Env: AGENT_SERVER_URL (:7778), ANTHROPIC_API_KEY, BENCH_HIVE_CLI.
"""
from __future__ import annotations
import asyncio, os, httpx

SERVER = os.environ.get("AGENT_SERVER_URL", "http://localhost:7778")
API_KEY = os.environ.get("ANTHROPIC_API_KEY")
CLI = os.environ.get("BENCH_HIVE_CLI", "git+https://github.com/tiangolo/typer")

SCRIPT = r'''
set +e
export HOME=/home/daytona
mkdir -p $HOME/.claude/skills
t() { local lbl="$1"; shift; local s=$(date +%s%3N); "$@" >/dev/null 2>&1; local rc=$?; local e=$(date +%s%3N); echo "  $((e-s))ms rc=$rc  $lbl"; }
S1="claude-office-skills/skills@html-slides"
S2="github/awesome-copilot@excalidraw-diagram-generator"
S3="anthropics/skills@frontend-design"
echo "=== COLD (first install — what cold-create pays today) ==="
t "skill html-slides   " npx -y skills add "$S1" --yes -g
t "skill excalidraw    " npx -y skills add "$S2" --yes -g
t "skill frontend-design" npx -y skills add "$S3" --yes -g
t "uv tool (cli)       " uv tool install "%CLI%"
echo "=== WARM (no-op re-run — what a pre-baked image would pay) ==="
t "skill html-slides   " npx -y skills add "$S1" --yes -g
t "skill excalidraw    " npx -y skills add "$S2" --yes -g
t "skill frontend-design" npx -y skills add "$S3" --yes -g
t "uv tool (cli)       " uv tool install "%CLI%"
'''.replace("%CLI%", CLI)


async def main() -> None:
    body = {"provider": "daytona", "agent_type": "claude",
            "secrets": {"ANTHROPIC_API_KEY": API_KEY} if API_KEY else {}}
    async with httpx.AsyncClient() as c:
        print("creating bare daytona sandbox...")
        r = await c.post(f"{SERVER}/sessions", json=body, timeout=300)
        r.raise_for_status()
        sid = r.json()["session_id"]
        print(f"session={sid[:8]}  running install x2 (cold then warm)...\n")
        try:
            resp = await c.post(
                f"{SERVER}/sessions/{sid}/sandbox/exec",
                json={"command": SCRIPT, "timeout": 295}, timeout=320,
            )
            out = resp.json()
            print(out.get("stdout", "") or "(no stdout)")
            if out.get("timed_out"):
                print("!! exec timed out")
            if out.get("stderr"):
                print("--- stderr (tail) ---\n" + out["stderr"][-400:])
        finally:
            await c.delete(f"{SERVER}/sessions/{sid}", timeout=60)


if __name__ == "__main__":
    asyncio.run(main())
