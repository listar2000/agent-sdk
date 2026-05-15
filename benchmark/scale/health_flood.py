"""Hammer /health to measure pure server throughput + p95.

No claude, no daytona — just the FastAPI router + our middleware +
the health handler. This isolates SERVER CPU scaling from
everything else. If 4 replicas don't beat 1 replica here, the server
isn't a bottleneck at any load profile we care about.

Env:
  API           default http://localhost:7778
  N_CONCURRENT  number of parallel in-flight requests (default 256)
  N_TOTAL       total requests to send (default 10000)
"""
from __future__ import annotations

import asyncio
import os
import statistics
import time

import httpx


API = os.environ.get("API", "http://localhost:7778")
N_CONCURRENT = int(os.environ.get("N_CONCURRENT", "256"))
N_TOTAL = int(os.environ.get("N_TOTAL", "10000"))


async def main() -> None:
    limits = httpx.Limits(max_keepalive_connections=N_CONCURRENT,
                          max_connections=N_CONCURRENT * 2)
    latencies: list[float] = []
    errors = 0
    sem = asyncio.Semaphore(N_CONCURRENT)
    async with httpx.AsyncClient(limits=limits, timeout=30.0,
                                 follow_redirects=False) as c:

        async def _one():
            nonlocal errors
            async with sem:
                t0 = time.perf_counter()
                try:
                    r = await c.get(f"{API}/health")
                    if r.status_code == 200:
                        latencies.append((time.perf_counter() - t0) * 1000)
                    else:
                        errors += 1
                except Exception:
                    errors += 1

        t_start = time.perf_counter()
        await asyncio.gather(*[_one() for _ in range(N_TOTAL)])
        wall = time.perf_counter() - t_start

    def pct(xs, q):
        if not xs: return None
        return round(statistics.quantiles(xs, n=100, method="inclusive")[q - 1], 2)

    print(f"API={API} N_CONCURRENT={N_CONCURRENT} N_TOTAL={N_TOTAL}")
    print(f"  wall_s       {wall:.3f}")
    print(f"  ok           {len(latencies)}")
    print(f"  err          {errors}")
    print(f"  rps          {round(len(latencies) / wall, 1)}")
    print(f"  latency_p50  {pct(latencies, 50)} ms")
    print(f"  latency_p95  {pct(latencies, 95)} ms")
    print(f"  latency_p99  {pct(latencies, 99)} ms")
    print(f"  latency_max  {round(max(latencies), 2) if latencies else None} ms")


if __name__ == "__main__":
    asyncio.run(main())
