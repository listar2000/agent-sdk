"""Benchmark modal sandbox cold-create vs snapshot-restore.

Three paths timed:
  1. Sandbox.create(image=stock) — cold from an image.
  2. Sandbox.create(image=fs_image) where fs_image came from
     Sandbox.snapshot_filesystem() on a warmed sandbox.
  3. Sandbox._experimental_from_snapshot(memory_snapshot).

For each path we measure:
  * ``create_to_running``: how long ``Sandbox.create`` blocks (or the
    equivalent "is the sandbox alive enough to exec" boundary).
  * ``ready_for_exec``: how long until a trivial ``exec("echo ok")``
    round-trip completes.

Each path is run twice; the first run includes any one-time provider-side
caching (image build, snapshot indexing), the second run is the
steady-state cost.

Run:
    python scripts/bench_modal_snapshot.py
"""
from __future__ import annotations

import os
import sys
import time
import traceback

# Path-resolve so this script works from any cwd.
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if os.path.join(_ROOT, "src") not in sys.path:
    sys.path.insert(0, os.path.join(_ROOT, "src"))

import modal


APP_NAME = "agent-sdk-bench-snapshot"


def _now_ms() -> float:
    return time.perf_counter()


def _time_phase(label: str, fn) -> "tuple[float, object]":
    t0 = _now_ms()
    out = fn()
    dt = _now_ms() - t0
    print(f"  {label}: {dt:.2f}s")
    return dt, out


def _wait_for_exec(sb) -> float:
    """Time round-trip of a trivial exec, returns seconds."""
    t0 = _now_ms()
    proc = sb.exec("echo", "ok")
    proc.wait()
    return _now_ms() - t0


def _read_str(reader) -> str:
    """Modal SDK 1.4.x returns bytes from stdout/stderr.read(); older
    versions returned str. Normalise."""
    out = reader.read()
    if isinstance(out, bytes):
        return out.decode("utf-8", errors="replace")
    return out or ""


def main() -> int:
    print(f"modal SDK: {modal.__version__}")
    app = modal.App.lookup(APP_NAME, create_if_missing=True)

    # Use the SAME image the agent-sdk modal provider uses in production
    # so the cold-create numbers are representative of the user's
    # observed 30-60s — not a trivial debian_slim that Modal caches in
    # under 10s. Image is built remotely via ``Image.from_dockerfile``;
    # first build is slow (multi-minute), subsequent builds hit Modal's
    # content-hash layer cache.
    repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    dockerfile = os.path.join(repo_root, "Dockerfile")
    if not os.path.exists(dockerfile):
        raise RuntimeError(f"Dockerfile not found at {dockerfile}")
    print(f"building image from {dockerfile}...")
    t0 = _now_ms()
    image = modal.Image.from_dockerfile(dockerfile)
    print(f"  image object built (local): {_now_ms() - t0:.2f}s "
          f"(actual remote build happens at first Sandbox.create)")

    # ── PHASE 1: cold from image ─────────────────────────────────────
    print("\n[phase 1] cold create from image (stock)")
    cold_runs: list[tuple[float, float]] = []  # (create, exec)
    warm_sb = None
    for i in range(2):
        t0 = _now_ms()
        sb = modal.Sandbox.create(
            "sleep", "300",
            app=app, image=image, timeout=600,
            # Required so the warm sandbox can call
            # ``_experimental_snapshot()`` later (Modal rejects the call
            # unless the sandbox was created with this flag).
            _experimental_enable_snapshot=True,
        )
        create_t = _now_ms() - t0
        exec_t = _wait_for_exec(sb)
        print(f"  run {i+1}: create={create_t:.2f}s ready={create_t+exec_t:.2f}s "
              f"(exec_rt={exec_t:.2f}s)")
        cold_runs.append((create_t, exec_t))
        if warm_sb is None:
            warm_sb = sb  # keep the first one alive for snapshotting
        else:
            sb.terminate()

    # ── PHASE 2: warm the sandbox + take filesystem snapshot ─────────
    print("\n[phase 2] warm + snapshot_filesystem")
    proc = warm_sb.exec("bash", "-c", "echo hello > /tmp/marker.txt; mkdir -p /tmp/state")
    proc.wait()
    t0 = _now_ms()
    fs_image = warm_sb.snapshot_filesystem(timeout=120)
    snap_fs_t = _now_ms() - t0
    print(f"  snapshot_filesystem: {snap_fs_t:.2f}s")

    # ── PHASE 3: cold from filesystem snapshot ───────────────────────
    print("\n[phase 3] create from filesystem snapshot")
    fs_runs: list[tuple[float, float]] = []
    for i in range(2):
        t0 = _now_ms()
        sb = modal.Sandbox.create(
            "sleep", "300",
            app=app, image=fs_image, timeout=600,
        )
        create_t = _now_ms() - t0
        exec_t = _wait_for_exec(sb)
        # Sanity: marker should be visible.
        check = sb.exec("cat", "/tmp/marker.txt")
        check.wait()
        marker_visible = "hello" in _read_str(check.stdout)
        print(f"  run {i+1}: create={create_t:.2f}s ready={create_t+exec_t:.2f}s "
              f"(exec_rt={exec_t:.2f}s, marker_visible={marker_visible})")
        fs_runs.append((create_t, exec_t))
        sb.terminate()

    # ── PHASE 4: memory snapshot (experimental) ──────────────────────
    print("\n[phase 4] _experimental_snapshot + restore")
    mem_runs: list[tuple[float, float]] = []
    try:
        t0 = _now_ms()
        mem_snap = warm_sb._experimental_snapshot()
        snap_mem_t = _now_ms() - t0
        print(f"  _experimental_snapshot: {snap_mem_t:.2f}s")

        for i in range(2):
            t0 = _now_ms()
            sb = modal.Sandbox._experimental_from_snapshot(mem_snap)
            create_t = _now_ms() - t0
            exec_t = _wait_for_exec(sb)
            print(f"  run {i+1}: restore={create_t:.2f}s ready={create_t+exec_t:.2f}s "
                  f"(exec_rt={exec_t:.2f}s)")
            mem_runs.append((create_t, exec_t))
            sb.terminate()
    except Exception as e:
        print(f"  experimental snapshot path failed: {e}")
        traceback.print_exc()

    # ── Cleanup ──────────────────────────────────────────────────────
    try:
        warm_sb.terminate()
    except Exception:
        pass

    # ── Summary ──────────────────────────────────────────────────────
    print("\n=== SUMMARY (mean of 2 runs each, seconds) ===")
    def _mean(xs: list[tuple[float, float]]) -> str:
        if not xs:
            return "n/a"
        creates = sum(c for c, _ in xs) / len(xs)
        readys = sum(c + e for c, e in xs) / len(xs)
        return f"create={creates:.2f}  ready={readys:.2f}"
    print(f"cold-from-image          {_mean(cold_runs)}")
    print(f"create-from-fs-snapshot  {_mean(fs_runs)}")
    print(f"restore-from-mem-snapshot {_mean(mem_runs)}")
    print(f"\nsnapshot_filesystem cost: {snap_fs_t:.2f}s (one-time per snapshot)")
    if mem_runs:
        print(f"_experimental_snapshot cost: {snap_mem_t:.2f}s (one-time per snapshot)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
