"""Contention benchmark — N parallel workers, each in its own subpath.

Question: when N workers each hammer their own subdirectory of the SAME
backend (one Modal volume / one Daytona sandbox / one Modal sandbox), do
operations scale linearly, or do they contend?

For each backend and each ``--workers`` count, this script:

  1. Provisions one shared backend resource (volume / sandbox).
  2. Spawns N workers concurrently. Each worker:
       - lives in subpath ``w_{i}/`` (no path overlap with other workers)
       - writes K files (size S each)
       - reads them back
       - deletes them
     All ops within a worker also run with intra-worker concurrency C.
  3. Records per-op latency for every operation across all workers.
  4. Reports aggregate wall time + per-op p50/p95/p99 + per-worker
     phase wall time (so you can see if any single worker stalls).

Compare results across ``--workers 1,2,5,10``. Linear scaling → wall time
stays flat as workers grow (per-op p50 doesn't move). Serialization →
wall time grows ~linearly with workers.

Run:
    .venv/bin/python benchmark/fs/bench_contention.py \
        --backends modal_vol --workers 1,5,10 --files-per-worker 20

Loads creds from ~/.env if present.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import os
import shutil
import statistics
import sys
import tempfile
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from bench_fs import (  # type: ignore[import-not-found]
    DaytonaSandboxBackend,
    ModalSandboxBackend,
    ModalVolumeBackend,
    Backend,
    load_env,
    make_local_file,
    DEFAULT_ENV_FILE,
    _fmt_rate,
)


# ---------------------------------------------------------------------------
# Per-worker workload
# ---------------------------------------------------------------------------

@dataclass
class WorkerResult:
    worker_id: int
    write_seconds: float = 0.0
    read_seconds: float = 0.0
    delete_seconds: float = 0.0
    write_op_latencies: list[float] = field(default_factory=list)
    read_op_latencies: list[float] = field(default_factory=list)
    delete_op_latencies: list[float] = field(default_factory=list)
    error: str | None = None


async def _run_worker(
    backend: Backend,
    worker_id: int,
    *,
    files: int,
    file_size: int,
    intra_concurrency: int,
    local_src: Path,
    sink_dir: Path,
) -> WorkerResult:
    res = WorkerResult(worker_id=worker_id)
    subpath = f"w_{worker_id:02d}"
    remotes = [f"{subpath}/f{j:04d}-{uuid.uuid4().hex[:4]}.bin" for j in range(files)]

    sem = asyncio.Semaphore(intra_concurrency)

    async def _do_write(r: str) -> float:
        async with sem:
            t0 = time.perf_counter()
            await backend.write(r, local_src)
            return time.perf_counter() - t0

    async def _do_read(idx: int, r: str) -> float:
        async with sem:
            ld = sink_dir / f"w{worker_id}_f{idx:04d}.bin"
            t0 = time.perf_counter()
            await backend.read(r, ld)
            ld.unlink(missing_ok=True)
            return time.perf_counter() - t0

    async def _do_delete(r: str) -> float:
        async with sem:
            t0 = time.perf_counter()
            await backend.delete(r)
            return time.perf_counter() - t0

    try:
        t0 = time.perf_counter()
        res.write_op_latencies = list(
            await asyncio.gather(*[_do_write(r) for r in remotes])
        )
        res.write_seconds = time.perf_counter() - t0

        t0 = time.perf_counter()
        res.read_op_latencies = list(
            await asyncio.gather(*[_do_read(i, r) for i, r in enumerate(remotes)])
        )
        res.read_seconds = time.perf_counter() - t0

        t0 = time.perf_counter()
        res.delete_op_latencies = list(
            await asyncio.gather(*[_do_delete(r) for r in remotes])
        )
        res.delete_seconds = time.perf_counter() - t0
    except Exception as e:
        res.error = repr(e)
    return res


# ---------------------------------------------------------------------------
# Phase aggregation
# ---------------------------------------------------------------------------

@dataclass
class PhaseStats:
    backend: str
    workers: int
    files_per_worker: int
    file_size: int
    op: str
    wall_seconds: float          # wallclock from gather start to gather end
    total_bytes: int
    op_count: int
    latencies: list[float] = field(default_factory=list)
    per_worker_seconds: list[float] = field(default_factory=list)
    error: str | None = None

    @property
    def mb_per_s(self) -> float:
        if self.wall_seconds <= 0 or self.total_bytes <= 0:
            return 0.0
        return (self.total_bytes / (1024 * 1024)) / self.wall_seconds

    def percentiles(self) -> dict[str, float]:
        if not self.latencies:
            return {}
        xs = sorted(self.latencies)
        def q(p: float) -> float:
            i = max(0, min(len(xs) - 1, int(round(p * (len(xs) - 1)))))
            return xs[i]
        return {
            "min": xs[0],
            "p50": q(0.5),
            "p95": q(0.95),
            "p99": q(0.99),
            "max": xs[-1],
            "mean": statistics.fmean(xs),
        }


async def run_contention(
    backend: Backend,
    *,
    workers: int,
    files_per_worker: int,
    file_size: int,
    intra_concurrency: int,
    local_src: Path,
    sink_dir: Path,
    log,
) -> list[PhaseStats]:
    log(f"  [{backend.name}] workers={workers} files/worker={files_per_worker} "
        f"intra_conc={intra_concurrency}")

    t0_all = time.perf_counter()
    worker_results = await asyncio.gather(*[
        _run_worker(
            backend, i,
            files=files_per_worker,
            file_size=file_size,
            intra_concurrency=intra_concurrency,
            local_src=local_src,
            sink_dir=sink_dir,
        )
        for i in range(workers)
    ])
    log(f"    all workers finished in {time.perf_counter() - t0_all:.2f}s")

    # Phase wall is gathered as max of worker phase wall — but per-worker
    # phases run in lock-step (write→read→delete) so the global wall is
    # roughly worker_max(write) + worker_max(read) + worker_max(delete).
    # We instead compute each phase's wall as max(worker.phase_seconds)
    # because all workers start their write phase simultaneously.
    out: list[PhaseStats] = []
    for op_name, get_lat, get_phase in (
        ("write", lambda w: w.write_op_latencies, lambda w: w.write_seconds),
        ("read",  lambda w: w.read_op_latencies,  lambda w: w.read_seconds),
        ("delete",lambda w: w.delete_op_latencies,lambda w: w.delete_seconds),
    ):
        all_lat: list[float] = []
        per_worker: list[float] = []
        err: str | None = None
        for wr in worker_results:
            if wr.error and not err:
                err = wr.error
            all_lat.extend(get_lat(wr))
            per_worker.append(get_phase(wr))
        out.append(PhaseStats(
            backend=backend.name,
            workers=workers,
            files_per_worker=files_per_worker,
            file_size=file_size,
            op=op_name,
            wall_seconds=max(per_worker) if per_worker else 0.0,
            total_bytes=workers * files_per_worker * file_size,
            op_count=workers * files_per_worker,
            latencies=all_lat,
            per_worker_seconds=per_worker,
            error=err,
        ))
    return out


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------

def render(phases: list[PhaseStats]) -> str:
    rows = [
        "| backend | workers | op | wall (s) | ops | bytes (MiB) | MiB/s | "
        "p50 (s) | p95 | p99 | max | per-worker max |",
        "|---|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for p in phases:
        if p.error:
            rows.append(f"| {p.backend} | {p.workers} | {p.op} | ERROR | "
                        f"— | — | — | — | — | — | — | {p.error[:80]} |")
            continue
        pct = p.percentiles()
        per_w_max = max(p.per_worker_seconds) if p.per_worker_seconds else 0.0
        mib = p.total_bytes / (1024 * 1024)
        rows.append(
            f"| {p.backend} | {p.workers} | {p.op} | {p.wall_seconds:.2f} | "
            f"{p.op_count} | {mib:.2f} | {_fmt_rate(p.mb_per_s)} | "
            f"{pct.get('p50', 0):.3f} | {pct.get('p95', 0):.3f} | "
            f"{pct.get('p99', 0):.3f} | {pct.get('max', 0):.3f} | "
            f"{per_w_max:.2f} |"
        )
    return "\n".join(rows)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

ALL_BACKENDS = ["daytona_sb", "modal_sb", "modal_vol"]


def make_backend(name: str) -> Backend:
    if name == "daytona_sb":
        return DaytonaSandboxBackend()
    if name == "modal_sb":
        return ModalSandboxBackend()
    if name == "modal_vol":
        return ModalVolumeBackend()
    raise SystemExit(f"unknown backend: {name}")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Contention benchmark")
    p.add_argument("--backends", default="modal_vol",
                   help=f"comma list of {ALL_BACKENDS}; default modal_vol")
    p.add_argument("--workers", default="1,5,10",
                   help="comma list of worker counts to test")
    p.add_argument("--files-per-worker", type=int, default=20)
    p.add_argument("--file-size", type=int, default=64 * 1024)
    p.add_argument("--intra-concurrency", type=int, default=8,
                   help="max in-flight ops within a single worker")
    p.add_argument("--executor-workers", type=int, default=256)
    p.add_argument("--env-file", default=str(DEFAULT_ENV_FILE))
    p.add_argument("--output", default=None)
    return p.parse_args()


async def amain() -> int:
    args = parse_args()
    load_env(Path(args.env_file))

    backends = [b.strip() for b in args.backends.split(",") if b.strip()]
    for b in backends:
        if b not in ALL_BACKENDS:
            raise SystemExit(f"unknown backend: {b}")

    worker_counts = [int(x) for x in args.workers.split(",") if x.strip()]

    asyncio.get_running_loop().set_default_executor(
        ThreadPoolExecutor(max_workers=args.executor_workers)
    )

    staging = Path(tempfile.mkdtemp(prefix="contention-staging-"))
    sink = Path(tempfile.mkdtemp(prefix="contention-sink-"))
    local_src = staging / f"src-{args.file_size}.bin"
    make_local_file(local_src, args.file_size)

    def log(m: str) -> None:
        print(m, flush=True)

    log(f"backends={backends} workers={worker_counts} "
        f"files/worker={args.files_per_worker} size={args.file_size} "
        f"intra_conc={args.intra_concurrency}")

    all_phases: list[PhaseStats] = []

    try:
        for bname in backends:
            log(f"\n=== backend: {bname} — setup ===")
            backend = make_backend(bname)
            try:
                t0 = time.perf_counter()
                await backend.setup()
                log(f"  setup ok in {time.perf_counter() - t0:.1f}s")
            except Exception as e:
                log(f"  setup FAILED: {e!r}")
                continue
            try:
                for nw in worker_counts:
                    log(f"\n--- {bname} / workers={nw} ---")
                    phases = await run_contention(
                        backend,
                        workers=nw,
                        files_per_worker=args.files_per_worker,
                        file_size=args.file_size,
                        intra_concurrency=args.intra_concurrency,
                        local_src=local_src,
                        sink_dir=sink,
                        log=log,
                    )
                    all_phases.extend(phases)
            finally:
                log(f"\n=== backend: {bname} — teardown ===")
                with contextlib.suppress(Exception):
                    await backend.teardown()
    finally:
        shutil.rmtree(staging, ignore_errors=True)
        shutil.rmtree(sink, ignore_errors=True)

    table = render(all_phases)
    print("\n=== RESULTS ===\n")
    print(table)

    if args.output:
        body = (
            "# Contention benchmark\n\n"
            f"- backends: {backends}\n"
            f"- worker counts: {worker_counts}\n"
            f"- files/worker={args.files_per_worker}, "
            f"file_size={args.file_size}, intra_concurrency={args.intra_concurrency}\n\n"
            "## Results\n\n"
            + table + "\n"
        )
        Path(args.output).write_text(body)
        log(f"\nwrote {args.output}")

    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(amain()))
