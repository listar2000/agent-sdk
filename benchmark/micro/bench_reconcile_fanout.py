"""Micro-benchmark: startup orphan-reclaim fan-out (no cloud, no creds).

Quantifies the win from fanning out ``reconcile_on_startup``'s per-orphan
control-plane calls via ``_shared.bounded_gather`` instead of a sequential
``for`` loop. Models each reclaim RTT as a fixed ``asyncio.sleep`` and
compares wall-clock for N orphans:

  * sequential  — ``for sb in orphans: await delete(sb)``  → ~N * rtt
  * bounded(16) — ``bounded_gather([delete(sb) ...])``     → ~ceil(N/16) * rtt

Pure asyncio + a sleep stand-in for the SDK call, so it needs no Modal /
Daytona credentials and runs in well under a second. The absolute numbers
are the model latency; the POINT is the ratio (≈ min(N, limit)x faster).

    .venv/bin/python benchmark/micro/bench_reconcile_fanout.py

Knobs (env): ``BENCH_ORPHANS`` (default 64), ``BENCH_RTT_MS`` (default 30),
``BENCH_LIMIT`` (default 16 — matches AGENT_SDK_RECONCILE_CONCURRENCY).
"""
from __future__ import annotations

import asyncio
import os
import time


async def _seq(orphans, rtt):
    for _ in orphans:
        await asyncio.sleep(rtt)


async def _fanout(orphans, rtt, limit):
    from api.providers._shared import bounded_gather

    async def _one():
        await asyncio.sleep(rtt)

    await bounded_gather([_one() for _ in orphans], limit=limit)


async def main() -> None:
    n = int(os.environ.get("BENCH_ORPHANS", "64"))
    rtt = float(os.environ.get("BENCH_RTT_MS", "30")) / 1000.0
    limit = int(os.environ.get("BENCH_LIMIT", "16"))
    orphans = list(range(n))

    t = time.perf_counter()
    await _seq(orphans, rtt)
    seq_ms = (time.perf_counter() - t) * 1000

    t = time.perf_counter()
    await _fanout(orphans, rtt, limit)
    fan_ms = (time.perf_counter() - t) * 1000

    print(f"reconcile fan-out: N={n} orphans, modelled rtt={rtt*1000:.0f}ms, "
          f"limit={limit}")
    print(f"  sequential    : {seq_ms:7.0f} ms")
    print(f"  bounded_gather: {fan_ms:7.0f} ms")
    print(f"  >>> {seq_ms / fan_ms:.1f}x faster boot reclaim "
          f"(theoretical ceiling {min(n, limit)}x)")


if __name__ == "__main__":
    asyncio.run(main())
