"""Real-load benchmark against agent-sdk server with haiku model.

Spawns N concurrent sessions, each sending a single trivial prompt.
Measures end-to-end latency (POST /sessions returns + first chunk +
done) and aggregate throughput.

Run while the server is already up at http://localhost:7778.
"""
import asyncio, json, os, sys, time, statistics
import httpx

API = os.environ.get("AGENT_SDK_URL", "http://localhost:7778")
N_SESSIONS = int(os.environ.get("N_SESSIONS", "8"))
PROMPT = "Reply with exactly the single word: OK"


async def one_session(client: httpx.AsyncClient, idx: int) -> dict:
    times: dict = {"idx": idx}
    t0 = time.perf_counter()

    # 1. Create session (eager — provisions sandbox + supervisor)
    r = await client.post(f"{API}/sessions", json={
        "provider": "unix_local",
        "model": "haiku",
        "agent_type": "claude",
    }, timeout=120)
    r.raise_for_status()
    sid = r.json()["session_id"]
    times["session_create_s"] = time.perf_counter() - t0

    # 2. Send prompt + stream response
    t1 = time.perf_counter()
    first_chunk_at = None
    n_chunks = 0
    n_text_bytes = 0
    async with client.stream(
        "POST", f"{API}/sessions/{sid}/message+stream",
        json={"message": PROMPT}, timeout=120,
    ) as resp:
        resp.raise_for_status()
        async for chunk in resp.aiter_bytes():
            if first_chunk_at is None:
                first_chunk_at = time.perf_counter()
            n_chunks += 1
            n_text_bytes += len(chunk)
    times["first_chunk_s"] = (first_chunk_at - t1) if first_chunk_at else None
    times["prompt_total_s"] = time.perf_counter() - t1
    times["chunks"] = n_chunks
    times["bytes"] = n_text_bytes
    times["total_s"] = time.perf_counter() - t0

    # 3. Cleanup
    try:
        await client.delete(f"{API}/sessions/{sid}", timeout=30)
    except Exception:
        pass
    return times


def pct(xs, p):
    xs = sorted(xs)
    return xs[min(int(len(xs) * p), len(xs) - 1)]


async def main():
    print(f"API: {API}")
    print(f"Concurrent sessions: {N_SESSIONS}")
    print(f"Prompt: {PROMPT!r}")
    # Health check
    async with httpx.AsyncClient() as c:
        r = await c.get(f"{API}/health", timeout=5)
        print(f"Health: {r.json()}")

    async with httpx.AsyncClient(
        limits=httpx.Limits(max_connections=N_SESSIONS * 2)
    ) as client:
        wall_t0 = time.perf_counter()
        results = await asyncio.gather(
            *[one_session(client, i) for i in range(N_SESSIONS)],
            return_exceptions=True,
        )
        wall = time.perf_counter() - wall_t0

    successes = [r for r in results if isinstance(r, dict)]
    failures = [r for r in results if not isinstance(r, dict)]

    print(f"\n=== {len(successes)}/{N_SESSIONS} succeeded, wall={wall:.2f}s ===")
    if failures:
        for f in failures[:3]:
            print(f"  FAILURE: {type(f).__name__}: {f}")
    if not successes:
        sys.exit(1)

    create = [r["session_create_s"] for r in successes]
    first_chunk = [r["first_chunk_s"] for r in successes if r.get("first_chunk_s") is not None]
    prompt_total = [r["prompt_total_s"] for r in successes]
    total = [r["total_s"] for r in successes]

    def show(name, xs):
        print(f"  {name:>20}  p50={pct(xs,0.5):>6.2f}s  p99={pct(xs,0.99):>6.2f}s  max={max(xs):>6.2f}s")
    show("session_create", create)
    show("first_chunk", first_chunk)
    show("prompt_total", prompt_total)
    show("e2e_total", total)
    print(f"  {'aggregate_throughput':>20}  {len(successes) / wall:.2f} sessions/sec")


if __name__ == "__main__":
    asyncio.run(main())
