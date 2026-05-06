"""Same as load_agent_sdk.py but uses provider=daytona to exercise
the sync-SDK ThreadPoolExecutor path. Each session create round-trips
to the Daytona control plane (~10-30s) — this is exactly where the
default 32-thread executor cap matters.
"""
import asyncio, os, sys, time, statistics
import httpx

API = os.environ.get("AGENT_SDK_URL", "http://localhost:7778")
N_SESSIONS = int(os.environ.get("N_SESSIONS", "8"))
PROMPT = "Reply with exactly the single word: OK"


async def one_session(client: httpx.AsyncClient, idx: int) -> dict:
    times: dict = {"idx": idx}
    t0 = time.perf_counter()
    try:
        r = await client.post(f"{API}/sessions", json={
            "provider": "daytona",
            "model": "haiku",
            "agent_type": "claude",
        }, timeout=180)
        r.raise_for_status()
    except Exception as e:
        return {"idx": idx, "error": f"create: {type(e).__name__}: {e}"}
    sid = r.json()["session_id"]
    times["session_create_s"] = time.perf_counter() - t0

    t1 = time.perf_counter()
    first_chunk_at = None
    try:
        async with client.stream(
            "POST", f"{API}/sessions/{sid}/message+stream",
            json={"message": PROMPT}, timeout=180,
        ) as resp:
            resp.raise_for_status()
            async for chunk in resp.aiter_bytes():
                if first_chunk_at is None:
                    first_chunk_at = time.perf_counter()
    except Exception as e:
        try:
            await client.delete(f"{API}/sessions/{sid}", timeout=60)
        except Exception:
            pass
        return {"idx": idx, "error": f"stream: {type(e).__name__}: {e}",
                "session_create_s": times["session_create_s"]}
    times["first_chunk_s"] = (first_chunk_at - t1) if first_chunk_at else None
    times["prompt_total_s"] = time.perf_counter() - t1
    times["total_s"] = time.perf_counter() - t0

    try:
        await client.delete(f"{API}/sessions/{sid}", timeout=60)
    except Exception:
        pass
    return times


def pct(xs, p):
    if not xs: return float('nan')
    xs = sorted(xs)
    return xs[min(int(len(xs) * p), len(xs) - 1)]


async def main():
    print(f"API: {API}")
    print(f"Provider: daytona, Concurrent sessions: {N_SESSIONS}")
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

    successes = [r for r in results if isinstance(r, dict) and "error" not in r]
    failures = [r for r in results if isinstance(r, dict) and "error" in r]
    exceptions = [r for r in results if not isinstance(r, dict)]

    print(f"\n=== {len(successes)}/{N_SESSIONS} ok, {len(failures)} app-err, "
          f"{len(exceptions)} exc, wall={wall:.2f}s ===")
    for f in failures[:5]:
        print(f"  app-err idx={f['idx']}: {f['error']}")
    for e in exceptions[:3]:
        print(f"  exc: {type(e).__name__}: {e}")
    if not successes:
        sys.exit(1)

    create = [r["session_create_s"] for r in successes]
    fc = [r["first_chunk_s"] for r in successes if r.get("first_chunk_s") is not None]
    pt = [r["prompt_total_s"] for r in successes]
    total = [r["total_s"] for r in successes]

    def show(name, xs):
        if not xs: print(f"  {name:>20}  no data"); return
        print(f"  {name:>20}  p50={pct(xs,0.5):>6.2f}s  p99={pct(xs,0.99):>6.2f}s  max={max(xs):>6.2f}s")
    show("session_create", create)
    show("first_chunk", fc)
    show("prompt_total", pt)
    show("e2e_total", total)
    print(f"  {'aggregate_tput':>20}  {len(successes) / wall:.2f} sessions/sec")


if __name__ == "__main__":
    asyncio.run(main())
