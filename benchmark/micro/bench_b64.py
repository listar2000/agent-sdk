"""Even more honest b64 benchmark.

Two questions:
  Q1: How much does inline b64encode/decode block the loop?
      → measure how many "background heartbeats" complete during a single
        b64 call, vs during a no-op of the same wall time.
  Q2: Does to_thread parallelise across the 32-thread pool?
      → run N concurrent calls; compare wall time scaling vs N=1.
"""
import asyncio, base64, os, statistics, time

SIZES = [1, 10, 50, 100]  # MB
TRIALS = 5


async def measure_loop_blocking(payload: bytes, mode: str) -> tuple[float, int]:
    """Return (work_ms, hb_count_during_work).
    A high hb_count means the loop stayed responsive."""
    hb_count = 0
    stop = asyncio.Event()

    async def heartbeat():
        nonlocal hb_count
        while not stop.is_set():
            await asyncio.sleep(0)
            hb_count += 1

    hb_task = asyncio.create_task(heartbeat())
    await asyncio.sleep(0.01)
    hb_count = 0  # reset after warmup
    t0 = time.perf_counter()
    if mode == "inline":
        base64.b64encode(payload)
    elif mode == "to_thread":
        await asyncio.to_thread(base64.b64encode, payload)
    elif mode == "noop":
        # control: same wall time, but loop stays free
        await asyncio.sleep((len(payload) / (200 * 1024 * 1024)))  # rough
    work_ms = (time.perf_counter() - t0) * 1000
    measured_hb = hb_count
    stop.set()
    await hb_task
    return work_ms, measured_hb


async def measure_concurrent(payload: bytes, n: int, mode: str) -> float:
    """Wall time for N concurrent encodes."""
    t0 = time.perf_counter()
    if mode == "inline":
        # sequential because sync calls don't yield
        for _ in range(n):
            base64.b64encode(payload)
    elif mode == "to_thread":
        await asyncio.gather(*[asyncio.to_thread(base64.b64encode, payload) for _ in range(n)])
    return (time.perf_counter() - t0) * 1000


async def main() -> None:
    # Warm executor
    await asyncio.to_thread(lambda: None)

    print("=== Q1: How much does inline b64 block the loop? ===")
    print(f"{'size_MB':>7} {'mode':>12} {'work_ms':>10} {'hb_during_work':>16}")
    for size_mb in SIZES:
        payload = os.urandom(size_mb * 1024 * 1024)
        for mode in ("inline", "to_thread"):
            results = [await measure_loop_blocking(payload, mode) for _ in range(TRIALS)]
            work = statistics.median(r[0] for r in results)
            hbs = statistics.median(r[1] for r in results)
            print(f"{size_mb:>7} {mode:>12} {work:>10.1f} {hbs:>16}")

    print()
    print("=== Q2: Concurrent throughput (16 calls) ===")
    print(f"{'size_MB':>7} {'mode':>12} {'wall_ms':>10} {'speedup_vs_inline':>18}")
    for size_mb in SIZES:
        payload = os.urandom(size_mb * 1024 * 1024)
        results = {}
        for mode in ("inline", "to_thread"):
            times = [await measure_concurrent(payload, 16, mode) for _ in range(TRIALS)]
            results[mode] = statistics.median(times)
        print(f"{size_mb:>7} {'inline':>12} {results['inline']:>10.1f} {1.0:>18.2f}")
        print(f"{size_mb:>7} {'to_thread':>12} {results['to_thread']:>10.1f} {results['inline']/results['to_thread']:>18.2f}")


if __name__ == "__main__":
    asyncio.run(main())
