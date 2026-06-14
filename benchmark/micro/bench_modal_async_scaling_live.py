"""LIVE benchmark: the modal async-ification's scalability win (threadpool ceiling).

This is the capstone for the modal async arc — PRs #190 (exec), #191 (lookup),
#193 (bare-create), #194 (tunnels), #195 (status poll), #196 (volume file-ops).
Each converted a modal SDK call from ``asyncio.to_thread(sync_call)`` to the
SDK's native ``.aio`` coroutine.

WHY IT MATTERS. ``asyncio.to_thread`` runs on the default executor, capped at
``min(32, cpu_count + 4)`` worker threads. Every concurrent modal op wrapped in
``to_thread`` therefore competes for those ~32 slots: once more than that many
ops are in flight, the surplus *queues* — the burst serialises into
``ceil(C / workers)`` waves. The ``.aio`` path holds no thread, so C concurrent
ops issue C concurrent RPCs and complete in ~one round-trip regardless of C.
That is the difference between "caps at the threadpool" and "super scalable".

This measures it directly: on one real sandbox, fire a burst of C concurrent
``Sandbox.from_id`` lookups two ways —

  * SYNC  : ``asyncio.gather(*[to_thread(from_id, sid) for _ in range(C)])``
  * ASYNC : ``asyncio.gather(*[from_id.aio(sid)         for _ in range(C)])``

— at increasing C, and reports wall-clock + throughput for each. ``from_id`` is
the cleanest probe (one ``SandboxWait`` RPC, no sandbox churn, fires every call);
the *pattern* is identical for every converted op.

**Opt-in — makes real cloud calls (creates one sandbox, runs lookups, deletes
it), so it is NOT in the default suite or CI.** Gated on modal creds:

    set -a; source ~/.env; set +a   # MODAL_TOKEN_ID / MODAL_TOKEN_SECRET
    .venv/bin/python benchmark/micro/bench_modal_async_scaling_live.py

Knobs (env): ``BENCH_CONC`` (default "8,16,32,48,64"), ``BENCH_TRIALS``
(default 3). Keep the top concurrency within a few× of the threadpool size and
well under any account API rate limit so the ASYNC burst stays RPC-bound (not
limit-bound) — otherwise you measure modal's API ceiling, not the threadpool.

Measured 2026-06 (32-core box → 32 threadpool workers; one account/region;
absolute numbers vary by yours). sync wall / async wall / async speedup:
  C=  8    102m /  98m / 1.03x   (both under the threadpool — parity)
  C= 32    109m / 106m / 1.03x   (burst exactly fills the threadpool — parity)
  C= 48    207m / 111m / 1.87x   (sync = 2 waves: 32 + 16; async = 1 wave)
  C= 64    211m / 120m / 1.76x   (sync = 2 waves: 32 + 32; async = 1 wave)
  → async wall stays ~flat (98->120ms) as C grows 8->64; sync DOUBLES the moment
    C crosses 32 (109->207ms) — it serialises into ceil(C/32) waves. That step
    is exactly the threadpool ceiling the whole async arc removed.
"""

from __future__ import annotations

import asyncio
import os
import statistics
import time


def _have(*keys: str) -> bool:
    return all(os.environ.get(k) for k in keys)


def _threadpool_workers() -> int:
    # Mirrors CPython's default ThreadPoolExecutor sizing for asyncio.to_thread.
    return min(32, (os.cpu_count() or 1) + 4)


async def _burst_sync(from_id, sid: str, c: int) -> float:
    """C concurrent lookups via to_thread (threadpool-bound). Returns wall-sec."""
    t0 = time.perf_counter()
    await asyncio.gather(*[asyncio.to_thread(from_id, sid) for _ in range(c)])
    return time.perf_counter() - t0


async def _burst_async(from_id_aio, sid: str, c: int) -> float:
    """C concurrent lookups via .aio (no thread held). Returns wall-sec."""
    t0 = time.perf_counter()
    await asyncio.gather(*[from_id_aio(sid) for _ in range(c)])
    return time.perf_counter() - t0


async def main() -> None:
    if not _have("MODAL_TOKEN_ID", "MODAL_TOKEN_SECRET"):
        print("[modal] no MODAL_TOKEN_ID/SECRET in env — skipping")
        return

    import modal

    conc = [int(x) for x in os.environ.get("BENCH_CONC", "8,16,32,48,64").split(",")]
    trials = int(os.environ.get("BENCH_TRIALS", "3"))
    workers = _threadpool_workers()

    app = await asyncio.to_thread(
        modal.App.lookup, "agent-sdk-bench-asyncscaling", create_if_missing=True
    )
    img = modal.Image.debian_slim()
    print(f"[modal] threadpool workers = {workers}; creating probe sandbox ...")
    sb = await modal.Sandbox.create.aio(
        "sh", "-c", "sleep 600", app=app, image=img, timeout=660
    )
    sid = sb.object_id
    from_id = modal.Sandbox.from_id
    from_id_aio = modal.Sandbox.from_id.aio
    try:
        # Warm up both paths (auth handshake / connection setup) off the clock.
        await _burst_async(from_id_aio, sid, 4)
        await _burst_sync(from_id, sid, 4)

        print(f"[modal] sandbox {sid} | trials={trials}\n")
        hdr = f"{'C':>4} {'sync wall':>10} {'async wall':>11} {'speedup':>8} {'sync op/s':>10} {'async op/s':>11}"
        print(hdr)
        print("-" * len(hdr))
        rows = []
        for c in conc:
            s = statistics.median([await _burst_sync(from_id, sid, c) for _ in range(trials)])
            a = statistics.median([await _burst_async(from_id_aio, sid, c) for _ in range(trials)])
            speedup = s / a if a > 0 else float("nan")
            rows.append((c, s, a, speedup))
            print(f"{c:>4} {s*1000:>9.0f}m {a*1000:>10.0f}m {speedup:>7.2f}x "
                  f"{c/s:>10.0f} {c/a:>11.0f}")

        print()
        over = [r for r in rows if r[0] > workers]
        if over:
            peak = max(over, key=lambda r: r[0])
            print(f"=> at C={peak[0]} (> {workers} threadpool workers): "
                  f"async is {peak[3]:.2f}x the throughput of the thread-bound path.")
            print("   sync serialises into ceil(C/workers) waves; async stays ~one round-trip.")
        else:
            print(f"=> all C <= {workers} threadpool workers — raise BENCH_CONC above "
                  f"{workers} to see the ceiling.")
    finally:
        await sb.terminate.aio()
        print("\n[modal] probe sandbox terminated")


if __name__ == "__main__":
    asyncio.run(main())
