"""Benchmark: per-call httpx.AsyncClient vs module-shared AsyncClient.

Mirrors the agent-sdk pattern in api/server.py:_proxy_from_session
(new client per request) vs the recommended fix (shared client).

Spins up a tiny local FastAPI/uvicorn server that returns a small JSON
payload. Hits it with N concurrent requests using each pattern, repeats
to amortize startup, reports RPS + p50/p99 latency.
"""
import asyncio, statistics, time
import httpx
from contextlib import asynccontextmanager
from fastapi import FastAPI
import uvicorn
import threading

app = FastAPI()

@app.get("/ping")
async def ping():
    return {"ok": True}


def serve():
    uvicorn.run(app, host="127.0.0.1", port=8765, log_level="warning")


async def per_call(url: str, n: int) -> list[float]:
    lats: list[float] = []
    async def one():
        t0 = time.perf_counter()
        async with httpx.AsyncClient(timeout=10) as c:
            await c.get(url)
        lats.append((time.perf_counter() - t0) * 1000)
    await asyncio.gather(*[one() for _ in range(n)])
    return lats


async def shared(url: str, n: int) -> list[float]:
    lats: list[float] = []
    async with httpx.AsyncClient(timeout=10) as c:
        async def one():
            t0 = time.perf_counter()
            await c.get(url)
            lats.append((time.perf_counter() - t0) * 1000)
        await asyncio.gather(*[one() for _ in range(n)])
    return lats


def pct(xs, p):
    xs = sorted(xs)
    return xs[int(len(xs) * p)]


async def main() -> None:
    url = "http://127.0.0.1:8765/ping"
    # warmup
    async with httpx.AsyncClient() as c:
        for _ in range(20):
            await c.get(url)

    print(f"{'mode':>10} {'concurrency':>11} {'wall_ms':>9} {'rps':>9} {'p50_ms':>8} {'p99_ms':>8}")
    for conc in (1, 10, 50, 200, 500):
        for label, fn in (("per_call", per_call), ("shared", shared)):
            t0 = time.perf_counter()
            lats = await fn(url, conc)
            wall = (time.perf_counter() - t0) * 1000
            rps = conc / (wall / 1000)
            print(f"{label:>10} {conc:>11} {wall:>9.1f} {rps:>9.1f} {pct(lats, 0.5):>8.2f} {pct(lats, 0.99):>8.2f}")


if __name__ == "__main__":
    t = threading.Thread(target=serve, daemon=True)
    t.start()
    time.sleep(1.5)  # let uvicorn bind
    asyncio.run(main())
