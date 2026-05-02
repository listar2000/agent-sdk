#!/usr/bin/env python3
"""Benchmark sandbox-recovery latency end-to-end.

Usage:
    python scripts/bench_recovery.py [N_RUNS] [--provider=local|daytona]

Measures wall-clock for each phase of a SIGKILL-then-resume flow:

  1. create session + agent (/sessions)
  2. turn 1 (baseline LLM round-trip)
  3. kill supervisor externally:
       - local:   os.kill(pid, 9)
       - daytona: sandbox.process.exec("pkill -9 -f supervisor.js")
  4. turn 2 (full recovery + reply)
  5. parse ``[BENCH] daytona.start_supervisor`` lines from the server log
     to break down the supervisor-start cost (daytona only).

Prints a timing table and percentiles across N runs. Server must be
running on http://localhost:7778. For daytona, set DAYTONA_API_KEY +
CLAUDE_CODE_OAUTH_TOKEN and point SERVER_LOG_PATH at the uvicorn log
(defaults to /tmp/server-r*.log — most recent by mtime).
"""
from __future__ import annotations

import asyncio
import glob
import os
import re
import statistics
import subprocess
import sys
import time
from pathlib import Path

import httpx

SERVER = os.environ.get("AGENT_SERVER_URL", "http://localhost:7778")
OAUTH_TOKEN = os.environ.get("CLAUDE_CODE_OAUTH_TOKEN")
DAYTONA_API_KEY = os.environ.get("DAYTONA_API_KEY")


def _parse_args() -> tuple[int, str]:
    """Return (n_runs, provider)."""
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    flags = {a for a in sys.argv[1:] if a.startswith("--")}
    n_runs = int(args[0]) if args else 3
    provider = "unix_local"
    for f in flags:
        if f.startswith("--provider="):
            provider = f.split("=", 1)[1]
    return n_runs, provider


def _latest_server_log() -> Path | None:
    explicit = os.environ.get("SERVER_LOG_PATH")
    if explicit and os.path.exists(explicit):
        return Path(explicit)
    candidates = sorted(
        (Path(p) for p in glob.glob("/tmp/server-r*.log")),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    return candidates[0] if candidates else None


_BENCH_RE = re.compile(
    r"\[BENCH\] daytona\.start_supervisor sandbox=(?P<sid>\S+) "
    r"(?:phase=(?P<phase>\S+) s=(?P<dur>[\d.]+)|TOTAL s=(?P<tot>[\d.]+))"
)


def _read_bench_phases(log_path: Path | None, after_epoch: float) -> list[dict]:
    """Parse [BENCH] log lines emitted since `after_epoch`."""
    if log_path is None or not log_path.exists():
        return []
    # Log timestamps look like ``2026-04-23 19:12:27,381 INFO ...``; convert
    # to epoch to filter. Only parse lines that start in the current run.
    runs: dict[str, dict] = {}  # keyed by sandbox id
    for line in log_path.read_text(errors="ignore").splitlines():
        m = _BENCH_RE.search(line)
        if not m:
            continue
        sid = m.group("sid")
        r = runs.setdefault(sid, {"phases": {}})
        if m.group("phase"):
            r["phases"][m.group("phase")] = float(m.group("dur"))
        elif m.group("tot"):
            r["total"] = float(m.group("tot"))
    # Return most recent N (rough — no precise filter by time, the caller
    # already knows which sandbox IDs were minted in this bench run).
    return [{"sandbox": s, **r} for s, r in runs.items()]


async def _ask(client: httpx.AsyncClient, session_id: str, msg: str) -> tuple[str, float]:
    """Send a message, read events until stopReason, return (text, seconds)."""
    t0 = time.monotonic()
    post = await client.post(
        f"{SERVER}/sessions/{session_id}/message",
        json={"message": msg}, timeout=60,
    )
    assert post.status_code == 200, f"message failed: {post.text}"
    parts: list[str] = []
    async with client.stream(
        "GET", f"{SERVER}/sessions/{session_id}/events", timeout=120,
    ) as events:
        async for line in events.aiter_lines():
            if not line.startswith("data: "):
                continue
            payload = line[6:]
            if '"stopReason"' in payload:
                break
            m = re.search(r'"text":"([^"]+)"', payload)
            if m:
                parts.append(m.group(1))
    return "".join(parts), time.monotonic() - t0


async def _kill_sandbox(provider: str, sandbox_ref: str) -> None:
    if provider == "unix_local":
        try:
            os.kill(int(sandbox_ref), 9)
        except (ValueError, ProcessLookupError):
            pass
    elif provider == "daytona":
        # Kill the supervisor process inside the daytona sandbox, leaving the
        # sandbox itself alive — mirrors the prod "supervisor OOM" scenario.
        from daytona_sdk import Daytona, DaytonaConfig
        daytona = Daytona(DaytonaConfig(api_key=DAYTONA_API_KEY))
        loop = asyncio.get_event_loop()
        sb = await loop.run_in_executor(None, lambda: daytona.get(sandbox_ref))
        await loop.run_in_executor(
            None,
            lambda: sb.process.exec("pkill -9 -f supervisor.js || true", timeout=10),
        )


async def _get_sandbox_ref(client: httpx.AsyncClient, session_id: str) -> str:
    sess = await client.get(f"{SERVER}/sessions/{session_id}", timeout=10)
    sbid = sess.json().get("current_sandbox_id") or sess.json().get("sandbox_id")
    sb = await client.get(f"{SERVER}/sandboxes/{sbid}", timeout=10)
    return sb.json().get("sandbox_ref") or ""


async def run_one(provider: str) -> dict[str, float]:
    """One recovery round."""
    async with httpx.AsyncClient() as client:
        body: dict = {"provider": provider, "agent_type": "claude"}
        if OAUTH_TOKEN:
            body["secrets"] = {"CLAUDE_CODE_OAUTH_TOKEN": OAUTH_TOKEN}
        t0 = time.monotonic()
        r = await client.post(f"{SERVER}/sessions", json=body, timeout=180)
        create_s = time.monotonic() - t0
        assert r.status_code == 200, r.text
        session_id = r.json()["session_id"]

        _, t1_s = await _ask(client, session_id, "Reply with one word: READY.")

        ref = await _get_sandbox_ref(client, session_id)
        await _kill_sandbox(provider, ref)
        await asyncio.sleep(0.5)

        _, t2_s = await _ask(client, session_id, "Reply with one word: RECOVERED.")

        return {"create_s": create_s, "turn1_s": t1_s, "turn2_recovery_s": t2_s}


async def main():
    n_runs, provider = _parse_args()
    if provider == "daytona" and not (DAYTONA_API_KEY and OAUTH_TOKEN):
        print("daytona requires DAYTONA_API_KEY + CLAUDE_CODE_OAUTH_TOKEN")
        return
    print(f"Benchmarking {provider}-provider recovery, {n_runs} runs")
    print(f"  server: {SERVER}")

    log_path = _latest_server_log()
    start_epoch = time.time()

    results: list[dict[str, float]] = []
    for i in range(n_runs):
        print(f"\n=== run {i+1}/{n_runs} ===")
        try:
            r = await run_one(provider)
            print(f"  create: {r['create_s']:.2f}s  "
                  f"turn1: {r['turn1_s']:.2f}s  "
                  f"turn2 (recovery): {r['turn2_recovery_s']:.2f}s")
            results.append(r)
        except Exception as e:
            print(f"  FAILED: {e}")

    if not results:
        print("\nNo successful runs.")
        return

    print(f"\n=== summary ({len(results)} runs, provider={provider}) ===")
    for key in ("create_s", "turn1_s", "turn2_recovery_s"):
        vals = [r[key] for r in results]
        if len(vals) > 1:
            print(f"  {key:22s} min={min(vals):.2f}s  median={statistics.median(vals):.2f}s  "
                  f"max={max(vals):.2f}s")
        else:
            print(f"  {key:22s} {vals[0]:.2f}s")

    if provider == "daytona":
        phases = _read_bench_phases(log_path, start_epoch)
        if phases:
            print(f"\n=== daytona.start_supervisor phase breakdown "
                  f"({len(phases)} sandboxes sampled from {log_path}) ===")
            by_phase: dict[str, list[float]] = {}
            totals: list[float] = []
            for p in phases:
                for ph, dur in p.get("phases", {}).items():
                    by_phase.setdefault(ph, []).append(dur)
                if "total" in p:
                    totals.append(p["total"])
            for ph, durs in sorted(by_phase.items()):
                print(f"  {ph:22s} median={statistics.median(durs):.2f}s  "
                      f"min={min(durs):.2f}s  max={max(durs):.2f}s  n={len(durs)}")
            if totals:
                print(f"  {'TOTAL':22s} median={statistics.median(totals):.2f}s  "
                      f"min={min(totals):.2f}s  max={max(totals):.2f}s  n={len(totals)}")
        else:
            print(f"\n(no [BENCH] lines found in {log_path}; server may be on older code)")


if __name__ == "__main__":
    asyncio.run(main())
