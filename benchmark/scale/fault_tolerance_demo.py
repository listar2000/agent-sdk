"""Kill-a-replica demo: prove multi-replica's actual value.

Drive N concurrent prompts. Midway through, SIGKILL one replica.
Single-replica deploy: ALL in-flight prompts die.
Multi-replica + lease deploy: only sessions whose lease was on the
killed replica fail (or recover after TTL); the rest keep running.
"""
from __future__ import annotations

import asyncio
import json
import os
import signal
import statistics
import subprocess
import time
from pathlib import Path

import httpx

REPO = Path(__file__).resolve().parents[2]
LB_URL = "http://localhost:7778"
N_SESSIONS = int(os.environ.get("N_SESSIONS", "32"))
PROVIDER = os.environ.get("PROVIDER", "daytona")
PROMPT = "Write three sentences about distributed systems. Keep it short."


def _secrets() -> dict:
    out = {}
    for k in ("CLAUDE_CODE_OAUTH_TOKEN", "ANTHROPIC_API_KEY"):
        v = os.environ.get(k)
        if v:
            out[k] = v
    return out


def _replicas_running() -> list[int]:
    pids = []
    try:
        out = subprocess.check_output(
            ["pgrep", "-f", "uvicorn api.server:app --host 127.0.0.1 --port 779"],
            text=True,
        )
        pids = [int(x) for x in out.strip().splitlines() if x.strip()]
    except subprocess.CalledProcessError:
        pass
    return pids


async def _create(c: httpx.AsyncClient, i: int) -> str:
    body = {
        "name": f"ft-{i}", "provider": PROVIDER,
        "agent_type": "claude", "model": "haiku",
        "secrets": _secrets(),
    }
    r = await c.post(f"{LB_URL}/sessions", json=body, timeout=300)
    r.raise_for_status()
    return r.json()["session_id"]


async def _drive(c: httpx.AsyncClient, sid: str) -> dict:
    res = {"sid": sid, "done": None, "first_evt": None, "events": 0, "error": None}
    t0 = time.perf_counter()
    try:
        async with c.stream(
            "POST", f"{LB_URL}/sessions/{sid}/message+stream",
            json={"message": PROMPT}, timeout=180,
        ) as resp:
            resp.raise_for_status()
            buf = ""
            async for chunk in resp.aiter_text():
                buf += chunk
                while "\n\n" in buf:
                    block, buf = buf.split("\n\n", 1)
                    if res["first_evt"] is None:
                        res["first_evt"] = time.perf_counter() - t0
                    res["events"] += 1
                    if "stopReason" in block or '"type":"done"' in block:
                        res["done"] = time.perf_counter() - t0
                        return res
    except Exception as e:
        res["error"] = f"{type(e).__name__}: {e}"[:200]
        res["done"] = time.perf_counter() - t0
    return res


async def _scenario(label: str, kill_at_s: float | None) -> dict:
    print(f"\n[{label}] N={N_SESSIONS}, kill_at_s={kill_at_s}")
    pids_before = _replicas_running()
    print(f"  replicas before: {pids_before}")

    async with httpx.AsyncClient(
        follow_redirects=True,
        limits=httpx.Limits(max_keepalive_connections=N_SESSIONS * 2,
                            max_connections=N_SESSIONS * 4),
    ) as c:
        print("  creating sessions ...")
        t_create = time.perf_counter()
        sids = await asyncio.gather(*[_create(c, i) for i in range(N_SESSIONS)],
                                    return_exceptions=True)
        good_sids = [s for s in sids if isinstance(s, str)]
        print(f"  created {len(good_sids)} sessions in {time.perf_counter()-t_create:.1f}s")

        async def _kill_later():
            if kill_at_s is None or not pids_before:
                return
            await asyncio.sleep(kill_at_s)
            victim = pids_before[0]
            print(f"  *** SIGKILL replica pid={victim} ***")
            try:
                os.kill(victim, signal.SIGKILL)
            except Exception as e:
                print(f"  kill failed: {e}")

        kill_task = asyncio.create_task(_kill_later())

        t_drive = time.perf_counter()
        results = await asyncio.gather(*[_drive(c, s) for s in good_sids],
                                       return_exceptions=True)
        wall = time.perf_counter() - t_drive
        await kill_task

        pids_after = _replicas_running()
        print(f"  replicas after:  {pids_after}")

        ok = [r for r in results if isinstance(r, dict) and r.get("done") is not None and r.get("error") is None]
        fail = [r for r in results if not isinstance(r, dict) or r.get("error")]
        done_times = [r["done"] for r in ok]

        def pct(xs, q):
            if not xs:
                return None
            if len(xs) < 2:
                return xs[0]
            return round(statistics.quantiles(xs, n=100, method="inclusive")[q - 1], 3)

        await asyncio.gather(*[
            c.delete(f"{LB_URL}/sessions/{s}") for s in good_sids
        ], return_exceptions=True)

        return {
            "label": label,
            "n": N_SESSIONS,
            "created": len(good_sids),
            "ok": len(ok),
            "fail": len(fail),
            "fail_rate": round(len(fail) / max(N_SESSIONS, 1), 3),
            "wall_s": round(wall, 2),
            "done_p50_s": pct(done_times, 50),
            "done_p95_s": pct(done_times, 95),
            "killed_one": kill_at_s is not None,
            "replicas_before": len(pids_before),
            "replicas_after": len(pids_after),
            "errors_sample": [r.get("error") for r in fail[:3]],
        }


async def main():
    label = os.environ.get("FT_LABEL", "ft_default")
    kill_at = os.environ.get("FT_KILL_AT_S")
    kill_at_s = float(kill_at) if kill_at else None
    summary = await _scenario(label, kill_at_s)
    print("\n=== RESULT ===")
    for k, v in summary.items():
        print(f"  {k:20} {v}")
    out = Path(os.environ.get("RESULT_PATH", str(REPO / "logs/bench-perf/ft.jsonl")))
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("a") as f:
        f.write(json.dumps({"ts": time.time(), **summary}) + "\n")


if __name__ == "__main__":
    asyncio.run(main())
