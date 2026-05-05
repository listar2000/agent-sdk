"""Multi-process load gen — avoids the single-client bottleneck.

Spawns N loader processes, aggregates their counts, prints aggregate RPS.
"""
import asyncio, time, sys
import httpx
import multiprocessing as mp


URL = "http://127.0.0.1:8766/work"
DURATION_S = 5.0


async def loader(conc_per_proc: int) -> int:
    count = 0
    deadline = time.perf_counter() + DURATION_S
    async with httpx.AsyncClient(
        timeout=10,
        limits=httpx.Limits(max_keepalive_connections=conc_per_proc, max_connections=conc_per_proc),
    ) as client:
        async def worker():
            nonlocal count
            while time.perf_counter() < deadline:
                r = await client.get(URL)
                if r.status_code == 200:
                    count += 1
        await asyncio.gather(*[worker() for _ in range(conc_per_proc)])
    return count


def child(conc: int, q):
    n = asyncio.run(loader(conc))
    q.put(n)


def run(n_procs: int, conc_per_proc: int) -> int:
    q: mp.Queue = mp.Queue()
    procs = [mp.Process(target=child, args=(conc_per_proc, q)) for _ in range(n_procs)]
    t0 = time.perf_counter()
    for p in procs: p.start()
    for p in procs: p.join()
    elapsed = time.perf_counter() - t0
    total = sum(q.get() for _ in procs)
    return total, elapsed


if __name__ == "__main__":
    # warmup
    import requests
    for _ in range(50):
        requests.get(URL)

    print(f"{'procs':>5} {'conc/proc':>10} {'total_req':>10} {'wall_s':>7} {'rps':>10}")
    for n_procs, c in [(1, 16), (4, 16), (8, 32)]:
        total, elapsed = run(n_procs, c)
        rps = total / elapsed
        print(f"{n_procs:>5} {c:>10} {total:>10} {elapsed:>7.2f} {rps:>10.1f}")
