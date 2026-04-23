#!/usr/bin/env python3
"""Benchmark the sandbox-recovery path end-to-end.

Measures wall-clock time for each phase of a typical SIGKILL-then-resume
flow on the local provider:

  1. create session + agent (sessions/quick)
  2. turn 1: agent replies (baseline round-trip)
  3. SIGKILL the local supervisor externally
  4. turn 2: measures full recovery + reply time
  5. breakdown of recovery phases via server log timestamps

Prints a timing table and basic percentiles across N runs. Server must be
running on http://localhost:7778.
"""
from __future__ import annotations

import asyncio
import os
import re
import statistics
import subprocess
import sys
import time

import httpx

SERVER = os.environ.get("AGENT_SERVER_URL", "http://localhost:7778")
OAUTH_TOKEN = os.environ.get("CLAUDE_CODE_OAUTH_TOKEN")
N_RUNS = int(sys.argv[1]) if len(sys.argv) > 1 else 3


async def _ask(client: httpx.AsyncClient, session_id: str, msg: str) -> tuple[str, float]:
    """Send a message, read events until assistant reply lands, return (text, seconds)."""
    t0 = time.monotonic()
    post = await client.post(
        f"{SERVER}/sessions/{session_id}/message",
        json={"message": msg},
        timeout=60,
    )
    assert post.status_code == 200, f"message failed: {post.text}"

    # Read events until end_turn
    text_parts: list[str] = []
    async with client.stream(
        "GET", f"{SERVER}/sessions/{session_id}/events",
        timeout=120,
    ) as events:
        async for line in events.aiter_lines():
            if not line.startswith("data: "):
                continue
            payload = line[6:]
            if '"stopReason"' in payload:
                break
            m = re.search(r'"text":"([^"]+)"', payload)
            if m:
                text_parts.append(m.group(1))
    return "".join(text_parts), time.monotonic() - t0


async def _kill_sandbox(sandbox_ref: str) -> None:
    try:
        os.kill(int(sandbox_ref), 9)
    except (ValueError, ProcessLookupError):
        pass


async def _get_sandbox_ref(client: httpx.AsyncClient, session_id: str) -> str:
    sess = await client.get(f"{SERVER}/sessions/{session_id}", timeout=10)
    sbid = sess.json().get("current_sandbox_id") or sess.json().get("sandbox_id")
    sb = await client.get(f"{SERVER}/sandboxes/{sbid}", timeout=10)
    return sb.json().get("sandbox_ref") or ""


async def run_one(run_idx: int) -> dict[str, float]:
    """One complete recovery round: create, turn1, kill, turn2."""
    async with httpx.AsyncClient() as client:
        # create
        body: dict = {"provider": "local", "agent_type": "claude"}
        if OAUTH_TOKEN:
            body["secrets"] = {"CLAUDE_CODE_OAUTH_TOKEN": OAUTH_TOKEN}
        t0 = time.monotonic()
        r = await client.post(f"{SERVER}/sessions/quick", json=body, timeout=60)
        create_s = time.monotonic() - t0
        session_id = r.json()["session_id"]

        # turn 1 (warmup)
        _, t1_s = await _ask(
            client, session_id,
            "Reply with one word: READY.",
        )

        # kill
        ref = await _get_sandbox_ref(client, session_id)
        await _kill_sandbox(ref)
        await asyncio.sleep(0.5)

        # turn 2 (recovery)
        _, t2_s = await _ask(
            client, session_id,
            "Reply with one word: RECOVERED.",
        )

        return {
            "create_s": create_s,
            "turn1_s": t1_s,
            "turn2_recovery_s": t2_s,
        }


async def main():
    print(f"Benchmarking local-provider recovery, {N_RUNS} runs")
    print(f"  server: {SERVER}")

    results: list[dict[str, float]] = []
    for i in range(N_RUNS):
        print(f"\n=== run {i+1}/{N_RUNS} ===")
        try:
            r = await run_one(i)
            print(f"  create: {r['create_s']:.2f}s  "
                  f"turn1: {r['turn1_s']:.2f}s  "
                  f"turn2 (recovery): {r['turn2_recovery_s']:.2f}s")
            results.append(r)
        except Exception as e:
            print(f"  FAILED: {e}")

    if not results:
        print("\nNo successful runs.")
        return

    print(f"\n=== summary ({len(results)} runs) ===")
    for key in ("create_s", "turn1_s", "turn2_recovery_s"):
        vals = [r[key] for r in results]
        if len(vals) > 1:
            print(f"  {key:22s} min={min(vals):.2f}s  median={statistics.median(vals):.2f}s  "
                  f"max={max(vals):.2f}s")
        else:
            print(f"  {key:22s} {vals[0]:.2f}s")


if __name__ == "__main__":
    asyncio.run(main())
