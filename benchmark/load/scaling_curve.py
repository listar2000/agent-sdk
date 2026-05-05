"""Run the workload at increasing concurrency and report how per-op
latency degrades. Reveals contention points (DB pool, executor,
shared httpx client, etc.).

Each "level" runs N concurrent sessions through a tiny workflow
(create + 1 prompt + 3 file_tree calls + delete). Reports p50/p99
for the prompt and file ops at each level.
"""
import asyncio, os, statistics, sys, time
import httpx

API = os.environ.get("API", "http://localhost:7778")
PROVIDER = os.environ.get("PROVIDER", "unix_local")
LEVELS = [int(x) for x in os.environ.get("LEVELS", "1,5,10,20,40").split(",")]
PROMPT = os.environ.get("PROMPT", "Reply with exactly the single word: OK")


async def run_session(c, idx):
    samples = {}
    t0 = time.perf_counter()
    r = await c.post(f"{API}/sessions", json={
        "name": f"scale-{idx}",
        "provider": PROVIDER,
        "config": {"agent_type": "claude", "model": "haiku"},
        "secrets": {"CLAUDE_CODE_OAUTH_TOKEN": os.environ.get("CLAUDE_CODE_OAUTH_TOKEN", "")},
    }, timeout=120)
    r.raise_for_status()
    sid = r.json()["session_id"]
    samples["create_ms"] = (time.perf_counter() - t0) * 1000

    try:
        # one prompt
        t0 = time.perf_counter()
        async with c.stream("POST", f"{API}/sessions/{sid}/message+stream",
                            json={"message": PROMPT}, timeout=120) as rs:
            rs.raise_for_status()
            async for _ in rs.aiter_lines():
                pass
        samples["prompt_ms"] = (time.perf_counter() - t0) * 1000

        # 3 file_tree calls (hammer the shared httpx pool)
        for i in range(3):
            t0 = time.perf_counter()
            r = await c.get(f"{API}/sessions/{sid}/files/tree", timeout=30)
            samples[f"tree_{i}_ms"] = (time.perf_counter() - t0) * 1000
    finally:
        try:
            await c.delete(f"{API}/sessions/{sid}", timeout=120)
        except Exception:
            pass
    return samples


def pct(vs, p):
    if not vs: return None
    s = sorted(vs)
    return s[min(int(len(s) * p), len(s) - 1)]


async def run_level(n):
    async with httpx.AsyncClient(timeout=120,
        limits=httpx.Limits(max_connections=n*4, max_keepalive_connections=n*2)) as c:
        h = await c.get(f"{API}/health"); h.raise_for_status()
        t0 = time.perf_counter()
        results = await asyncio.gather(*[run_session(c, i) for i in range(n)],
                                       return_exceptions=True)
        wall = time.perf_counter() - t0

    ok = [r for r in results if isinstance(r, dict)]
    err = len(results) - len(ok)
    metrics = {}
    if ok:
        for k in ok[0]:
            vs = [r[k] for r in ok if k in r]
            metrics[k] = (pct(vs, 0.5), pct(vs, 0.99))
    return wall, len(ok), err, metrics


async def main():
    print(f"{'N':>4} {'wall_s':>8} {'ok/err':>8} "
          f"{'create p50':>11} {'create p99':>11} "
          f"{'prompt p50':>11} {'prompt p99':>11} "
          f"{'tree p50':>10} {'tree p99':>10} "
          f"{'tput sps':>9}")
    for n in LEVELS:
        wall, ok, err, m = await run_level(n)
        cm50, cm99 = m.get("create_ms", (None, None))
        pm50, pm99 = m.get("prompt_ms", (None, None))
        # average tree across the 3 calls
        tree_p50s = [m[k][0] for k in m if k.startswith("tree_") and m[k][0] is not None]
        tree_p99s = [m[k][1] for k in m if k.startswith("tree_") and m[k][1] is not None]
        tm50 = statistics.median(tree_p50s) if tree_p50s else None
        tm99 = max(tree_p99s) if tree_p99s else None
        tput = ok / wall if wall else 0
        def f(x): return f"{x:.0f}" if x is not None else "n/a"
        print(f"{n:>4} {wall:>8.2f} {ok}/{err:<5} "
              f"{f(cm50):>11} {f(cm99):>11} "
              f"{f(pm50):>11} {f(pm99):>11} "
              f"{f(tm50):>10} {f(tm99):>10} "
              f"{tput:>9.2f}")


if __name__ == "__main__":
    asyncio.run(main())
