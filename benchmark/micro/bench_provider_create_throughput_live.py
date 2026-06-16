"""LIVE benchmark: concurrent cold-create throughput (daytona / modal).

The core scalability question for a provider is: when a replica spins up many
sessions at once (autoscale, a burst of new conversations), does per-create
latency stay flat (scales) or balloon (contention — a lock, a client pool, a
control-plane queue)? This measures exactly that: fire N sandbox creates
concurrently and report the latency distribution + effective throughput, then
do the same for a trivial exec in each (concurrent exec), then delete them.

It is a *throughput* probe, complementary to bench_provider_exec_live.py
(which isolates the per-exec resolution round-trip on ONE sandbox).

**Opt-in — makes real cloud calls (creates N sandboxes, execs, deletes them),
so it is NOT in the default suite or CI.** Gated on provider creds. Bounded by
``BENCH_CREATE_N`` (default 8) to keep quota modest — raise it to probe the
contention curve. Self-cleans every sandbox in a finally.

    set -a; source ~/.env; set +a       # DAYTONA_API_KEY / MODAL_TOKEN_*
    BENCH_CREATE_N=8 .venv/bin/python benchmark/micro/bench_provider_create_throughput_live.py

Knobs (env): ``BENCH_CREATE_N`` (default 8), ``BENCH_PROVIDERS`` (default
"daytona,modal" — runs whichever have creds).

Reading the result: throughput = N / wall_clock (creates that actually
overlapped). If p95 ≈ p50 as N grows, creates are independent (scales). If p95
≫ p50 / wall_clock ≈ N×p50, they're serialising somewhere (contention).

Observed 2026-06 (N=8, one account/region — absolute numbers vary, esp.
daytona which depends on account queue depth / orphan pressure):
  daytona create : QUEUE-BOUND — p50 ~6.6s, p95 ~9.9s, ~0.8 ops/s. Daytona's
                   control plane serialises provisioning (see the comment in
                   provision_daytona_sandbox); this is daytona's scalability
                   ceiling. Mitigation is account hygiene (orphan reaping),
                   not a client-side change — creates are deliberately NOT
                   throttled our side. Tail varies a lot with account load.
  modal create   : SCALES — p50 ~0.24s, p95 ~0.29s, ~27 ops/s, tight. Modal
                   provisions concurrently with no queue penalty.
  both exec (warm): SCALE — daytona ~25 ops/s, modal ~15 ops/s, tight p50≈p95.
                   (The first-exec latency on a fresh sandbox is container
                   warm-up, NOT an exec-concurrency limit — hence the untimed
                   warm pass before the timed exec phase.)
"""
from __future__ import annotations

import asyncio
import os
import statistics
import time


def _have(*keys: str) -> bool:
    return all(os.environ.get(k) for k in keys)


def _dist(samples: list[float]) -> str:
    s = sorted(samples)
    p = lambda q: s[min(len(s) - 1, max(0, int(len(s) * q) - 1))] * 1000
    return (f"p50 {statistics.median(s)*1000:5.0f}ms  p95 {p(0.95):5.0f}ms  "
            f"max {s[-1]*1000:5.0f}ms")


async def _timed(thunk):
    """Await ``thunk()``; return (result_or_exc, elapsed_s, ok)."""
    t = time.perf_counter()
    try:
        r = await thunk()
        return r, time.perf_counter() - t, True
    except Exception as e:
        return e, time.perf_counter() - t, False


def _report(tag: str, n: int, lat: list[float], wall: float, errors: int) -> None:
    ok = len(lat)
    print(f"[{tag}] N={n} concurrent  ok={ok} err={errors}")
    if lat:
        print(f"[{tag}]   per-op latency: {_dist(lat)}")
        print(f"[{tag}]   wall={wall*1000:.0f}ms  throughput={ok/max(wall,1e-9):.2f} ops/s  "
              f"(serial would be ~{sum(lat)*1000:.0f}ms)")


async def _bench_daytona(n: int) -> None:
    from api.providers.daytona import _get_async_daytona_client
    from daytona_sdk import CreateSandboxFromSnapshotParams

    daytona = await _get_async_daytona_client()
    snap = open(".runtime-snapshot-tag").read().strip()
    origin = os.environ.get("AGENT_SDK_ORIGIN", "test")
    print(f"[daytona] creating {n} sandboxes concurrently from {snap} ...")

    async def _mk():
        return await daytona.create(CreateSandboxFromSnapshotParams(
            snapshot=snap, auto_stop_interval=0,
            labels={"agent_sdk_origin": origin}), timeout=300)

    t = time.perf_counter()
    results = await asyncio.gather(*[_timed(_mk) for _ in range(n)])
    wall = time.perf_counter() - t
    sandboxes = [r for r, _, ok in results if ok]
    lat = [el for _, el, ok in results if ok]
    _report("daytona create", n, lat, wall, n - len(sandboxes))
    try:
        if sandboxes:
            async def _ex(sb):
                return await sb.process.exec("echo x", timeout=15)
            # Warm each sandbox once (untimed) so the timed phase measures pure
            # concurrent-exec throughput, not first-exec container warm-up.
            await asyncio.gather(*[_timed(lambda sb=sb: _ex(sb)) for sb in sandboxes])
            t = time.perf_counter()
            ex = await asyncio.gather(*[_timed(lambda sb=sb: _ex(sb)) for sb in sandboxes])
            _report("daytona exec (warm)", len(sandboxes),
                    [el for _, el, ok in ex if ok], time.perf_counter() - t,
                    sum(1 for _, _, ok in ex if not ok))
    finally:
        await asyncio.gather(*[_timed(lambda sb=sb: daytona.delete(sb)) for sb in sandboxes])
        print(f"[daytona] deleted {len(sandboxes)} sandbox(es)")


async def _bench_modal(n: int) -> None:
    from api.providers.modal import _require_modal

    modal, _ = _require_modal()
    snap = open(".modal-snapshot-tag").read().strip()
    app = await asyncio.to_thread(
        lambda: modal.App.lookup("agent-sdk-bench-create", create_if_missing=True))
    img = modal.Image.from_id(snap)
    print(f"[modal] creating {n} sandboxes concurrently from {snap} ...")

    async def _mk():
        return await asyncio.to_thread(
            lambda: modal.Sandbox.create("sleep", "infinity", app=app, image=img, timeout=600))

    t = time.perf_counter()
    results = await asyncio.gather(*[_timed(_mk) for _ in range(n)])
    wall = time.perf_counter() - t
    sandboxes = [r for r, _, ok in results if ok]
    lat = [el for _, el, ok in results if ok]
    _report("modal create", n, lat, wall, n - len(sandboxes))
    try:
        if sandboxes:
            def _run(sb):
                p = sb.exec("sh", "-c", "echo x"); p.wait(); p.stdout.read()
            # Warm each sandbox once (untimed) — isolate exec concurrency from
            # first-exec container warm-up on a freshly-scheduled sandbox.
            await asyncio.gather(
                *[_timed(lambda sb=sb: asyncio.to_thread(_run, sb)) for sb in sandboxes])
            t = time.perf_counter()
            ex = await asyncio.gather(
                *[_timed(lambda sb=sb: asyncio.to_thread(_run, sb)) for sb in sandboxes])
            _report("modal exec (warm)", len(sandboxes),
                    [el for _, el, ok in ex if ok], time.perf_counter() - t,
                    sum(1 for _, _, ok in ex if not ok))
    finally:
        await asyncio.gather(
            *[_timed(lambda sb=sb: asyncio.to_thread(sb.terminate)) for sb in sandboxes])
        print(f"[modal] terminated {len(sandboxes)} sandbox(es)")


async def main() -> None:
    n = int(os.environ.get("BENCH_CREATE_N", "8"))
    want = set(os.environ.get("BENCH_PROVIDERS", "daytona,modal").split(","))
    ran = False
    if "daytona" in want and _have("DAYTONA_API_KEY"):
        await _bench_daytona(n); ran = True
    elif "daytona" in want:
        print("[daytona] SKIP — DAYTONA_API_KEY not set")
    if "modal" in want and _have("MODAL_TOKEN_ID", "MODAL_TOKEN_SECRET"):
        await _bench_modal(n); ran = True
    elif "modal" in want:
        print("[modal] SKIP — MODAL_TOKEN_ID / MODAL_TOKEN_SECRET not set")
    if not ran:
        print("nothing ran — set provider creds (e.g. source ~/.env)")


if __name__ == "__main__":
    asyncio.run(main())
