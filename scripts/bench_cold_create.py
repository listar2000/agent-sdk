#!/usr/bin/env python3
"""Time daytona cold-create end-to-end and decompose the steps.

Mirrors what hive-space does on agent onboarding:
  1. POST /sessions                    → cold-create the sandbox (the "agent")
  2. POST /sessions/{id}/message+stream → send the first message, stream reply

"Works" = the session is created AND the agent streams a non-error turn-end
(clean ``stopReason``). Prints the per-step breakdown of the cold-create
wall-clock, parsed from the ``[BENCH] daytona.cold_create`` server log lines:

  resolve_or_create_sandbox   (A) VM allocation + 3-volume mount
  start_supervisor            (B) supervisor boot + signed-URL + health wait
  acp_attach                  (C) ACP handshake + session/new
  TOTAL                       (A+B+C, server-side)

Usage:
    SERVER_LOG_PATH=/tmp/agent-sdk-server.log \
      .venv/bin/python scripts/bench_cold_create.py [N] [--concurrent]

    N             number of cold-creates (default 1)
    --concurrent  run all N at once (else sequential)

Env:
    AGENT_SERVER_URL          default http://localhost:7778
    CLAUDE_CODE_OAUTH_TOKEN   forwarded as the agent secret (claude agent_type)
    SERVER_LOG_PATH           uvicorn log scraped for [BENCH] lines
    TURN_TIMEOUT_S            hard cap on the first turn (default 120)
"""
from __future__ import annotations

import asyncio
import os
import re
import sys
import time

import httpx

SERVER = os.environ.get("AGENT_SERVER_URL", "http://localhost:7778")
# Agent LLM credential forwarded as a session secret. ANTHROPIC_API_KEY is
# preferred — a CLAUDE_CODE_OAUTH_TOKEN whose org lacks Claude API access
# returns "organization does not have access to Claude" on the turn.
_API_KEY = os.environ.get("ANTHROPIC_API_KEY")
_OAUTH = os.environ.get("CLAUDE_CODE_OAUTH_TOKEN")
LOG_PATH = os.environ.get("SERVER_LOG_PATH", "/tmp/agent-sdk-server.log")
TURN_TIMEOUT_S = float(os.environ.get("TURN_TIMEOUT_S", "120"))

_BENCH_RE = re.compile(
    r"\[BENCH\] daytona\.cold_create session=(?P<sid>\S+) "
    r"phase=(?P<phase>\S+) s=(?P<dur>[\d.]+)"
)
_PHASE_ORDER = [
    "resolve_or_create_sandbox",
    "start_supervisor",
    "acp_attach",
    "TOTAL",
]
_TEXT_RE = re.compile(r'"text":"((?:[^"\\]|\\.)*)"')

# A representative hivespace agent's provisioning load (see
# hive-space agents.py: DEFAULT_AGENT_SKILLS + cli_tools + pre_start_commands).
# These run inside daytona create_sandbox (phase A) on cold-create. The 3
# skills are public (npx skills add). The hivespace CLI install needs a
# private token, so it's represented by a public git+https uv tool of similar
# shape (clone+build). Set BENCH_HIVE_CLI to override.
_HIVE_SKILLS = [
    "claude-office-skills/skills@html-slides",
    "github/awesome-copilot@excalidraw-diagram-generator",
    "anthropics/skills@frontend-design",
]
_HIVE_CLI_TOOLS = [os.environ.get("BENCH_HIVE_CLI", "git+https://github.com/tiangolo/typer")]
_HIVE_PRE_START = [
    'mkdir -p $HOME/.hivespace/agents && echo "{}" > $HOME/.hivespace/config.json',
    'mkdir -p /home/daytona && printf "# instructions\\n" > /home/daytona/CLAUDE.md',
    '[ -f /home/daytona/memory.md ] || printf "# Agent Memory\\n" > /home/daytona/memory.md',
]
_HIVE = False


def _phases_for(session_id8: str) -> dict[str, float]:
    out: dict[str, float] = {}
    if not os.path.exists(LOG_PATH):
        return out
    with open(LOG_PATH, errors="ignore") as fh:
        for line in fh:
            m = _BENCH_RE.search(line)
            if m and m.group("sid") == session_id8:
                out[m.group("phase")] = float(m.group("dur"))
    return out


async def _ask_stream(
    client: httpx.AsyncClient, session_id: str, msg: str
) -> tuple[bool, str, float]:
    """POST /message+stream and consume the single-turn SSE.

    Returns (ok, reply_text, seconds). ``ok`` is True iff the stream ended
    on a clean turn-end (stopReason / done) rather than an error/timeout.
    """
    t0 = time.monotonic()
    texts: list[str] = []
    ok = False
    try:
        async with client.stream(
            "POST", f"{SERVER}/sessions/{session_id}/message+stream",
            json={"message": msg}, timeout=httpx.Timeout(connect=10, read=TURN_TIMEOUT_S, write=10, pool=10),
        ) as resp:
            if resp.status_code != 200:
                body = (await resp.aread()).decode(errors="ignore")
                return False, f"<http {resp.status_code}: {body[:120]}>", time.monotonic() - t0
            async for block in resp.aiter_text():
                for m in _TEXT_RE.finditer(block):
                    texts.append(m.group(1))
                if '"error":' in block:
                    ok = False
                    break
                if "stopReason" in block or '"type":"done"' in block:
                    ok = True
                    break
                if time.monotonic() - t0 > TURN_TIMEOUT_S:
                    break
    except Exception as e:
        return False, f"<stream error: {e}>", time.monotonic() - t0
    # Drop the echoed prompt text; keep the assistant's words.
    reply = " ".join(t for t in texts if msg[:12] not in t).strip()
    return ok, reply, time.monotonic() - t0


async def run_one(i: int, quiet: bool = False) -> dict | None:
    body: dict = {"provider": "daytona", "agent_type": "claude"}
    if _API_KEY:
        body["secrets"] = {"ANTHROPIC_API_KEY": _API_KEY}
    elif _OAUTH:
        body["secrets"] = {"CLAUDE_CODE_OAUTH_TOKEN": _OAUTH}
    if _HIVE:
        body["skills"] = _HIVE_SKILLS
        body["cli_tools"] = _HIVE_CLI_TOOLS
        body["pre_start_commands"] = _HIVE_PRE_START
    tag = f"[{i}]"
    async with httpx.AsyncClient() as client:
        t0 = time.monotonic()
        try:
            r = await client.post(f"{SERVER}/sessions", json=body, timeout=300)
        except Exception as e:
            print(f"{tag} create FAILED: {e}")
            return None
        create_s = time.monotonic() - t0
        if r.status_code != 200:
            print(f"{tag} create FAILED: {r.status_code} {r.text[:200]}")
            return None
        session_id = r.json()["session_id"]
        sid8 = session_id[:8]

        ok, reply, turn1_s = await _ask_stream(
            client, session_id, "Reply with exactly one word: READY."
        )
        status = "OK " if ok else "BAD"
        print(f"{tag} {status} create={create_s:6.2f}s turn1={turn1_s:6.2f}s "
              f"session={sid8} reply={reply[:30]!r}")

        await asyncio.sleep(0.3)
        phases = _phases_for(sid8)
        if not quiet and phases:
            for ph in _PHASE_ORDER:
                if ph in phases:
                    bar = "#" * int(phases[ph] * 2)
                    print(f"      {ph:28s} {phases[ph]:6.2f}s  {bar}")

        try:
            await client.delete(f"{SERVER}/sessions/{session_id}", timeout=60)
        except Exception:
            pass
        return {"ok": ok, "create_s": create_s, "turn1_s": turn1_s, **phases}


async def main() -> None:
    global _HIVE
    argv = sys.argv[1:]
    concurrent = "--concurrent" in argv
    _HIVE = "--hive" in argv
    nums = [a for a in argv if not a.startswith("--")]
    n = int(nums[0]) if nums else 1
    print(f"bench_cold_create: server={SERVER} runs={n} "
          f"mode={'concurrent' if concurrent else 'sequential'} "
          f"profile={'hive (skills+cli+pre_start)' if _HIVE else 'bare'} log={LOG_PATH}")
    if not (_API_KEY or _OAUTH):
        print("  WARNING: no ANTHROPIC_API_KEY / CLAUDE_CODE_OAUTH_TOKEN — agent turn will fail")

    t0 = time.monotonic()
    if concurrent:
        results = await asyncio.gather(*(run_one(i + 1, quiet=True) for i in range(n)))
    else:
        results = [await run_one(i + 1) for i in range(n)]
    wall = time.monotonic() - t0
    results = [r for r in results if r]

    ok_n = sum(1 for r in results if r.get("ok"))
    print(f"\n=== summary: {ok_n}/{n} agents created+replied in {wall:.1f}s wall "
          f"({'concurrent' if concurrent else 'sequential'}) ===")
    if results:
        import statistics
        for k in ["create_s"] + _PHASE_ORDER + ["turn1_s"]:
            vals = [r[k] for r in results if k in r and r[k] == r[k]]
            if vals:
                med = statistics.median(vals)
                mx = max(vals)
                print(f"  {k:28s} median={med:6.2f}s  max={mx:6.2f}s  n={len(vals)}")


if __name__ == "__main__":
    asyncio.run(main())
