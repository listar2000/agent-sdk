"""LIVE benchmark: per-exec sandbox-handle resolution overhead (daytona / modal).

Validates the handle-cache wins — daytona #173, modal #174. The provider exec
path (``exec_in_sandbox`` — the per-tool-call primitive for native-on-daytona /
native-on-modal) used to resolve a FRESH sandbox handle before EVERY exec:

  * daytona: ``daytona.get(ref)``           — a control-plane GET
  * modal:   ``modal.Sandbox.from_id(ref)``  — a ``SandboxWait`` RPC

A handle is reusable, so that resolution was a redundant per-exec round-trip.
This measures it directly: on a real sandbox, time exec WITH per-call resolution
vs a CACHED handle. The delta is the round-trip the cache removes.

**Opt-in — makes real cloud calls (creates a sandbox, runs execs, deletes it),
so it is NOT in the default suite or CI.** Gated on provider creds:

    set -a; source ~/.env; set +a   # DAYTONA_API_KEY / MODAL_TOKEN_* etc.
    .venv/bin/python benchmark/micro/bench_provider_exec_live.py

Knobs (env): ``BENCH_EXEC_N`` (default 15), ``BENCH_PROVIDERS`` (default
"daytona,modal" — runs whichever have creds).

Measured 2026-06 (one account/region; absolute numbers vary by yours):
  daytona: per-exec get() ~98-101 ms  (cached exec ~94 ms → get is ~half an exec)
  modal:   per-exec from_id HIGHLY VARIABLE — 90 to 600+ ms across runs (the
           SandboxWait RPC isn't a flat get). One run: 858→254 ms (~604 ms / ~70%
           removed); another: ~90 ms. So the modal cache win is *at least* ~90 ms
           and often much larger — run this against your account to see your tail.
"""

from __future__ import annotations

import asyncio
import os
import statistics
import time


def _have(*keys: str) -> bool:
    return all(os.environ.get(k) for k in keys)


def _stat(samples: list[float]) -> str:
    return (f"{statistics.median(samples)*1000:5.0f} ms "
            f"(p95 {sorted(samples)[max(0, int(len(samples)*0.95)-1)]*1000:.0f})")


async def _bench_daytona(n: int) -> None:
    from api.providers.daytona import _get_async_daytona_client
    from daytona_sdk import CreateSandboxFromSnapshotParams

    daytona = await _get_async_daytona_client()
    snap = open(".runtime-snapshot-tag").read().strip()
    print(f"[daytona] creating sandbox from snapshot {snap} ...")
    sb = await daytona.create(CreateSandboxFromSnapshotParams(
        snapshot=snap, auto_stop_interval=0,
        labels={"agent_sdk_origin": os.environ.get("AGENT_SDK_ORIGIN", "test")}),
        timeout=300)
    ref = sb.id
    try:
        await sb.process.exec("echo warm", timeout=15)
        cached, resolve = [], []
        for _ in range(n):
            t = time.perf_counter(); await sb.process.exec("echo x", timeout=15)
            cached.append(time.perf_counter() - t)
            t = time.perf_counter()
            h = await daytona.get(ref); await h.process.exec("echo x", timeout=15)
            resolve.append(time.perf_counter() - t)
        per_exec_get = statistics.median(resolve) - statistics.median(cached)
        print(f"[daytona] cached exec:        {_stat(cached)}")
        print(f"[daytona] get()+exec (before):{_stat(resolve)}")
        print(f"[daytona] >>> per-exec get() removed by cache: ~{per_exec_get*1000:.0f} ms/exec")
    finally:
        try:
            await daytona.delete(await daytona.get(ref)); print(f"[daytona] deleted {ref[:12]}")
        except Exception as e:
            print("[daytona] cleanup warn:", e)


async def _bench_modal(n: int) -> None:
    from api.providers.modal import _require_modal, _lookup_sandbox

    modal, _ = _require_modal()
    snap = open(".modal-snapshot-tag").read().strip()
    print(f"[modal] creating sandbox from image {snap} ...")
    app = await asyncio.to_thread(
        lambda: modal.App.lookup("agent-sdk-bench-exec", create_if_missing=True))
    img = modal.Image.from_id(snap)
    sb = await asyncio.to_thread(
        lambda: modal.Sandbox.create("sleep", "infinity", app=app, image=img, timeout=600))
    ref = sb.object_id
    try:
        cached, resolve = [], []
        for _ in range(n):
            def _run(h):
                p = h.exec("sh", "-c", "echo x"); p.wait(); p.stdout.read()
            t = time.perf_counter(); await asyncio.to_thread(_run, sb)
            cached.append(time.perf_counter() - t)
            t = time.perf_counter()
            h = await _lookup_sandbox(ref); await asyncio.to_thread(_run, h)
            resolve.append(time.perf_counter() - t)
        per_exec_lookup = statistics.median(resolve) - statistics.median(cached)
        print(f"[modal] cached exec:          {_stat(cached)}")
        print(f"[modal] from_id+exec (before):{_stat(resolve)}")
        print(f"[modal] >>> per-exec from_id removed by cache: ~{per_exec_lookup*1000:.0f} ms/exec")
    finally:
        try:
            await asyncio.to_thread(sb.terminate); print(f"[modal] terminated {ref}")
        except Exception as e:
            print("[modal] cleanup warn:", e)


async def main() -> None:
    n = int(os.environ.get("BENCH_EXEC_N", "15"))
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
