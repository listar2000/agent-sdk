#!/usr/bin/env python3
"""Does installing the 3 default skills in ONE `npx skills add` call beat 3
separate calls? Times batched-cold vs separate-cold on two fresh daytona
sandboxes (so both are genuine cold installs), and dumps `--help` to see if
multi-source is even supported.

Env: AGENT_SERVER_URL (:7778), ANTHROPIC_API_KEY.
"""
from __future__ import annotations
import asyncio, os, httpx

SERVER = os.environ.get("AGENT_SERVER_URL", "http://localhost:7778")
API_KEY = os.environ.get("ANTHROPIC_API_KEY")

S = ["claude-office-skills/skills@html-slides",
     "github/awesome-copilot@excalidraw-diagram-generator",
     "anthropics/skills@frontend-design"]

HELP = "npx -y skills add --help 2>&1 | head -30 || true"

SEPARATE = r'''
set +e; export HOME=/home/daytona; mkdir -p $HOME/.claude/skills
ms(){ date +%s%3N; }
s=$(ms)
npx -y skills add "%S0%" --yes -g >/dev/null 2>&1; echo "  sep1 rc=$? $(( $(ms)-s ))ms (cum)"
npx -y skills add "%S1%" --yes -g >/dev/null 2>&1; echo "  sep2 rc=$? $(( $(ms)-s ))ms (cum)"
npx -y skills add "%S2%" --yes -g >/dev/null 2>&1; echo "  sep3 rc=$? $(( $(ms)-s ))ms (cum)"
echo "TOTAL_SEPARATE $(( $(ms)-s ))ms"
'''.replace("%S0%", S[0]).replace("%S1%", S[1]).replace("%S2%", S[2])

BATCHED = r'''
set +e; export HOME=/home/daytona; mkdir -p $HOME/.claude/skills
ms(){ date +%s%3N; }
s=$(ms)
npx -y skills add "%S0%" "%S1%" "%S2%" --yes -g; rc=$?
echo "TOTAL_BATCHED rc=$rc $(( $(ms)-s ))ms"
'''.replace("%S0%", S[0]).replace("%S1%", S[1]).replace("%S2%", S[2])


async def run(c, label, cmd, want_stderr=False):
    body = {"provider": "daytona", "agent_type": "claude",
            "secrets": {"ANTHROPIC_API_KEY": API_KEY} if API_KEY else {}}
    r = await c.post(f"{SERVER}/sessions", json=body, timeout=300)
    r.raise_for_status()
    sid = r.json()["session_id"]
    print(f"--- {label}  (session={sid[:8]}) ---")
    try:
        resp = await c.post(f"{SERVER}/sessions/{sid}/sandbox/exec",
                            json={"command": cmd, "timeout": 200}, timeout=230)
        o = resp.json()
        print(o.get("stdout", "").rstrip() or "(no stdout)")
        if want_stderr and o.get("stderr"):
            print("  stderr:", o["stderr"][-300:].replace("\n", " "))
    finally:
        await c.delete(f"{SERVER}/sessions/{sid}", timeout=60)


async def main():
    async with httpx.AsyncClient() as c:
        await run(c, "skills add --help", HELP)
        print()
        await run(c, "3x SEPARATE (cold)", SEPARATE)
        print()
        await run(c, "1x BATCHED (cold)", BATCHED, want_stderr=True)


if __name__ == "__main__":
    asyncio.run(main())
