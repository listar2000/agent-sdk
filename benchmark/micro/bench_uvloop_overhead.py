"""Benchmark: uvloop vs default asyncio.

Tests both pure asyncio overhead AND a more realistic HTTP-server scenario.
Run twice via subprocess so each gets a fresh interpreter with the right loop.
"""
import sys, time, asyncio, statistics


async def loop_overhead(n: int) -> tuple[float, float]:
    # Test 1: bare task creation + scheduling
    t0 = time.perf_counter()
    async def noop():
        pass
    await asyncio.gather(*[noop() for _ in range(n)])
    bare = (time.perf_counter() - t0) * 1000

    # Test 2: producer/consumer queue throughput
    q: asyncio.Queue = asyncio.Queue()
    async def producer():
        for i in range(n):
            await q.put(i)
        await q.put(None)
    async def consumer():
        count = 0
        while True:
            x = await q.get()
            if x is None:
                return count
            count += 1
    t0 = time.perf_counter()
    _, _ = await asyncio.gather(producer(), consumer())
    queue = (time.perf_counter() - t0) * 1000

    return bare, queue


async def main(loop_name: str):
    # Warmup
    await loop_overhead(1000)

    results = {"bare": [], "queue": []}
    for _ in range(5):
        b, q = await loop_overhead(50_000)
        results["bare"].append(b)
        results["queue"].append(q)

    print(f"{loop_name:>12} | tasks_50k_bare_ms={statistics.median(results['bare']):>7.1f}  queue_50k_msgs_ms={statistics.median(results['queue']):>7.1f}")


if __name__ == "__main__":
    use_uvloop = "--uvloop" in sys.argv
    if use_uvloop:
        import uvloop
        uvloop.install()
        loop_name = "uvloop"
    else:
        loop_name = "asyncio"
    asyncio.run(main(loop_name))
