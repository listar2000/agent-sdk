"""Filesystem-tier benchmark: Daytona sandbox FS vs Modal sandbox FS vs Modal volume.

Measures wall-clock latency and effective throughput for write / read / delete
across three backends:

  - daytona_sb   : daytona-sdk Sandbox.fs.{upload,download,delete}_file
  - modal_sb     : modal.Sandbox.filesystem.{copy_from_local,copy_to_local,remove}
  - modal_vol    : modal.Volume.batch_upload + read_file + remove_file

Workloads:
  - small : N files (default 50) of S bytes (default 64 KiB) — sequential ops
  - 200m  : single 200 MiB file
  - 1g    : single 1 GiB file
  - 2g    : single 2 GiB file (opt-in)

Run:
    .venv/bin/python benchmark/fs/bench_fs.py --workloads small,200m,1g

Loads creds from ~/.env (DAYTONA_API_KEY, MODAL_TOKEN_ID, MODAL_TOKEN_SECRET)
when present so this script is runnable standalone.
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
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Awaitable, Callable

# ---------------------------------------------------------------------------
# Config / .env loading
# ---------------------------------------------------------------------------

DEFAULT_ENV_FILE = Path.home() / ".env"


def load_env(path: Path) -> None:
    if not path.exists():
        return
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, _, v = line.partition("=")
        k = k.strip()
        v = v.strip().strip("'").strip('"')
        os.environ.setdefault(k, v)


# ---------------------------------------------------------------------------
# Local file generation
# ---------------------------------------------------------------------------

CHUNK = 1024 * 1024  # 1 MiB chunk we recycle for large-file generation

_chunk_cache: bytes | None = None


def _get_chunk() -> bytes:
    global _chunk_cache
    if _chunk_cache is None:
        _chunk_cache = os.urandom(CHUNK)
    return _chunk_cache


def make_local_file(path: Path, size_bytes: int) -> None:
    """Write ``size_bytes`` of pseudo-random data to ``path``.

    Uses a single 1 MiB urandom block written repeatedly — incompressible enough
    for a fair test, but generation cost is bounded.
    """
    chunk = _get_chunk()
    remaining = size_bytes
    with path.open("wb") as f:
        while remaining > 0:
            n = min(CHUNK, remaining)
            f.write(chunk[:n])
            remaining -= n


# ---------------------------------------------------------------------------
# Workloads
# ---------------------------------------------------------------------------

@dataclass
class Workload:
    name: str
    # Total bytes manipulated by the workload (informative for throughput).
    total_bytes: int
    # If single-file: size of the file. Otherwise None for many-files.
    single_size: int | None
    # If many-files: count, per-file size, and concurrent in-flight ops.
    file_count: int = 1
    file_size: int = 0
    concurrency: int = 1


def build_workloads(
    requested: list[str],
    *,
    small_count: int,
    small_size: int,
    small_concurrency: int,
) -> list[Workload]:
    out: list[Workload] = []
    for w in requested:
        if w == "small":
            out.append(Workload(
                name="small",
                total_bytes=small_count * small_size,
                single_size=None,
                file_count=small_count,
                file_size=small_size,
                concurrency=small_concurrency,
            ))
        elif w == "200m":
            n = 200 * 1024 * 1024
            out.append(Workload(name="200m", total_bytes=n, single_size=n))
        elif w == "1g":
            n = 1024 * 1024 * 1024
            out.append(Workload(name="1g", total_bytes=n, single_size=n))
        elif w == "2g":
            n = 2 * 1024 * 1024 * 1024
            out.append(Workload(name="2g", total_bytes=n, single_size=n))
        else:
            raise SystemExit(f"unknown workload: {w}")
    return out


# ---------------------------------------------------------------------------
# Result record
# ---------------------------------------------------------------------------

@dataclass
class OpResult:
    backend: str
    workload: str
    op: str           # write | read | delete | setup | error
    seconds: float    # total wall time for the workload phase
    bytes_processed: int
    # When the phase contains many independent ops (small workload), keep each
    # per-op latency so we can show a distribution. Empty for single-file ops.
    per_op_seconds: list[float] = field(default_factory=list)
    concurrency: int = 1
    error: str | None = None

    @property
    def mb_per_s(self) -> float:
        if self.seconds <= 0 or self.bytes_processed <= 0:
            return 0.0
        return (self.bytes_processed / (1024 * 1024)) / self.seconds

    def percentiles(self) -> dict[str, float]:
        if not self.per_op_seconds:
            return {}
        xs = sorted(self.per_op_seconds)
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


# ---------------------------------------------------------------------------
# Backend protocol
# ---------------------------------------------------------------------------

class Backend:
    name: str

    async def setup(self) -> None: ...
    async def teardown(self) -> None: ...

    # File ops — paths are remote-relative (the backend prepends its own root).
    async def write(self, remote_path: str, local_path: Path) -> None: ...
    async def read(self, remote_path: str, local_path: Path) -> int:
        """Read remote_path and write into local_path. Returns bytes read."""
        ...
    async def delete(self, remote_path: str) -> None: ...


# ---------------------------------------------------------------------------
# Daytona sandbox FS backend
# ---------------------------------------------------------------------------

class DaytonaSandboxBackend(Backend):
    name = "daytona_sb"

    def __init__(self, *, image: str | None = None, cpu: int = 4, memory: int = 4) -> None:
        self.image = image or os.environ.get("DAYTONA_BENCH_IMAGE", "alpine:3.20")
        self.cpu = cpu
        self.memory = memory
        self._daytona: Any = None
        self._sandbox: Any = None

    async def setup(self) -> None:
        from daytona_sdk import Daytona, DaytonaConfig, CreateSandboxFromImageParams
        from daytona_sdk.common.sandbox import Resources
        api_key = os.environ.get("DAYTONA_API_KEY")
        if not api_key:
            raise RuntimeError("DAYTONA_API_KEY not set")
        loop = asyncio.get_running_loop()
        self._daytona = Daytona(DaytonaConfig(api_key=api_key))
        labels = {"agent_sdk_origin": "test", "purpose": "fs-bench"}
        params = CreateSandboxFromImageParams(
            image=self.image,
            labels=labels,
            auto_stop_interval=10,
            auto_delete_interval=15,
            resources=Resources(cpu=self.cpu, memory=self.memory),
        )
        self._sandbox = await loop.run_in_executor(
            None, lambda: self._daytona.create(params, timeout=180),
        )
        # Make the bench dir up front.
        await loop.run_in_executor(
            None,
            lambda: self._sandbox.fs.create_folder("/tmp/bench", "755"),
        )

    async def teardown(self) -> None:
        loop = asyncio.get_running_loop()
        if self._sandbox is not None:
            with contextlib.suppress(Exception):
                await loop.run_in_executor(
                    None, lambda: self._daytona.delete(self._sandbox),
                )

    def _remote(self, p: str) -> str:
        return f"/tmp/bench/{p}"

    async def write(self, remote_path: str, local_path: Path) -> None:
        loop = asyncio.get_running_loop()
        target = self._remote(remote_path)
        await loop.run_in_executor(
            None,
            lambda: self._sandbox.fs.upload_file(str(local_path), target),
        )

    async def read(self, remote_path: str, local_path: Path) -> int:
        loop = asyncio.get_running_loop()
        target = self._remote(remote_path)
        # Daytona supports streaming-to-file via the 2-arg form.
        await loop.run_in_executor(
            None,
            lambda: self._sandbox.fs.download_file(target, str(local_path)),
        )
        return local_path.stat().st_size

    async def delete(self, remote_path: str) -> None:
        loop = asyncio.get_running_loop()
        target = self._remote(remote_path)
        await loop.run_in_executor(
            None, lambda: self._sandbox.fs.delete_file(target),
        )


# ---------------------------------------------------------------------------
# Modal sandbox FS backend
# ---------------------------------------------------------------------------

class ModalSandboxBackend(Backend):
    name = "modal_sb"

    def __init__(self, *, cpu: float = 4.0, memory: int = 4096) -> None:
        self.cpu = cpu
        self.memory = memory
        self._app: Any = None
        self._sandbox: Any = None
        self._fs: Any = None

    async def setup(self) -> None:
        import modal
        # Modal SDK is API-async via synchronicity — we wrap blocking calls
        # in to_thread so async timing stays clean.
        def _create() -> tuple[Any, Any]:
            app = modal.App.lookup("fs-bench", create_if_missing=True)
            image = modal.Image.debian_slim()
            sb = modal.Sandbox.create(
                "sleep", "infinity",
                app=app,
                image=image,
                timeout=60 * 60,
                cpu=self.cpu,
                memory=self.memory,
            )
            return app, sb

        self._app, self._sandbox = await asyncio.to_thread(_create)
        self._fs = self._sandbox.filesystem
        await asyncio.to_thread(
            self._fs.make_directory, "/tmp/bench", create_parents=True,
        )

    async def teardown(self) -> None:
        if self._sandbox is not None:
            with contextlib.suppress(Exception):
                await asyncio.to_thread(self._sandbox.terminate)

    def _remote(self, p: str) -> str:
        return f"/tmp/bench/{p}"

    async def write(self, remote_path: str, local_path: Path) -> None:
        target = self._remote(remote_path)
        await asyncio.to_thread(self._fs.copy_from_local, str(local_path), target)

    async def read(self, remote_path: str, local_path: Path) -> int:
        target = self._remote(remote_path)
        await asyncio.to_thread(self._fs.copy_to_local, target, str(local_path))
        return local_path.stat().st_size

    async def delete(self, remote_path: str) -> None:
        target = self._remote(remote_path)
        await asyncio.to_thread(self._fs.remove, target, recursive=False)


# ---------------------------------------------------------------------------
# Modal volume backend
# ---------------------------------------------------------------------------

class ModalVolumeBackend(Backend):
    name = "modal_vol"

    def __init__(self, *, volume_name: str | None = None) -> None:
        self.volume_name = volume_name or f"fs-bench-{uuid.uuid4().hex[:8]}"
        self._volume: Any = None

    async def setup(self) -> None:
        import modal
        from modal_proto import api_pb2

        def _create() -> Any:
            return modal.Volume.from_name(
                self.volume_name,
                create_if_missing=True,
                version=api_pb2.VolumeFsVersion.VOLUME_FS_VERSION_V2,
            )

        self._volume = await asyncio.to_thread(_create)

    async def teardown(self) -> None:
        if self._volume is None:
            return
        import modal
        with contextlib.suppress(Exception):
            await asyncio.to_thread(
                modal.Volume.objects.delete, self.volume_name, allow_missing=True,
            )

    async def write(self, remote_path: str, local_path: Path) -> None:
        # batch_upload is the canonical fast path for Modal volumes — it
        # stages files and commits on context exit.
        def _do() -> None:
            with self._volume.batch_upload(force=True) as bu:
                bu.put_file(str(local_path), remote_path)
        await asyncio.to_thread(_do)

    async def read(self, remote_path: str, local_path: Path) -> int:
        def _do() -> int:
            n = 0
            with local_path.open("wb") as f:
                for chunk in self._volume.read_file(remote_path):
                    f.write(chunk)
                    n += len(chunk)
            return n
        return await asyncio.to_thread(_do)

    async def delete(self, remote_path: str) -> None:
        await asyncio.to_thread(self._volume.remove_file, remote_path)


# ---------------------------------------------------------------------------
# Timing helpers
# ---------------------------------------------------------------------------

async def timed(coro_factory: Callable[[], Awaitable[Any]]) -> tuple[float, Any]:
    t0 = time.perf_counter()
    res = await coro_factory()
    return time.perf_counter() - t0, res


# ---------------------------------------------------------------------------
# Per-workload runner
# ---------------------------------------------------------------------------

async def run_workload(
    backend: Backend,
    workload: Workload,
    *,
    staging_dir: Path,
    sink_dir: Path,
    log: Callable[[str], None],
) -> list[OpResult]:
    results: list[OpResult] = []

    if workload.single_size is not None:
        # Single large file.
        local_src = staging_dir / f"{workload.name}.src"
        local_dst = sink_dir / f"{workload.name}.dst"
        if not local_src.exists() or local_src.stat().st_size != workload.single_size:
            log(f"  generating local {workload.name} file ({workload.single_size / (1024*1024):.0f} MiB)")
            make_local_file(local_src, workload.single_size)

        remote = f"{workload.name}-{uuid.uuid4().hex[:6]}.bin"
        try:
            log(f"  [{backend.name}/{workload.name}] write")
            dur, _ = await timed(lambda: backend.write(remote, local_src))
            results.append(OpResult(backend.name, workload.name, "write", dur, workload.single_size))

            log(f"  [{backend.name}/{workload.name}] read")
            dur, n = await timed(lambda: backend.read(remote, local_dst))
            results.append(OpResult(backend.name, workload.name, "read", dur, int(n)))

            log(f"  [{backend.name}/{workload.name}] delete")
            dur, _ = await timed(lambda: backend.delete(remote))
            results.append(OpResult(backend.name, workload.name, "delete", dur, workload.single_size))
        except Exception as e:
            log(f"  [{backend.name}/{workload.name}] ERROR: {e!r}")
            results.append(OpResult(
                backend.name, workload.name, "error", 0.0, 0, error=repr(e),
            ))
        finally:
            if local_dst.exists():
                local_dst.unlink()
        return results

    # Many small files: run concurrently so we measure real-world throughput
    # rather than per-call serial overhead.
    local_src = staging_dir / f"smallseed-{workload.file_size}.bin"
    if not local_src.exists() or local_src.stat().st_size != workload.file_size:
        make_local_file(local_src, workload.file_size)

    remotes = [f"small/{i:04d}-{uuid.uuid4().hex[:4]}.bin" for i in range(workload.file_count)]
    sink = sink_dir / "small"
    sink.mkdir(parents=True, exist_ok=True)

    sem = asyncio.Semaphore(workload.concurrency)

    async def _timed_write(r: str) -> float:
        async with sem:
            t0 = time.perf_counter()
            await backend.write(r, local_src)
            return time.perf_counter() - t0

    async def _timed_read(i: int, r: str) -> tuple[float, int]:
        ld = sink / f"{i:04d}.bin"
        async with sem:
            t0 = time.perf_counter()
            n = await backend.read(r, ld)
            return time.perf_counter() - t0, int(n)

    async def _timed_delete(r: str) -> float:
        async with sem:
            t0 = time.perf_counter()
            await backend.delete(r)
            return time.perf_counter() - t0

    try:
        log(f"  [{backend.name}/small] write x{workload.file_count} (conc={workload.concurrency})")
        t0 = time.perf_counter()
        durs = await asyncio.gather(*[_timed_write(r) for r in remotes])
        results.append(OpResult(
            backend.name, workload.name, "write",
            time.perf_counter() - t0, workload.total_bytes,
            per_op_seconds=list(durs), concurrency=workload.concurrency,
        ))

        log(f"  [{backend.name}/small] read x{workload.file_count} (conc={workload.concurrency})")
        t0 = time.perf_counter()
        rd = await asyncio.gather(*[_timed_read(i, r) for i, r in enumerate(remotes)])
        results.append(OpResult(
            backend.name, workload.name, "read",
            time.perf_counter() - t0,
            sum(n for _, n in rd),
            per_op_seconds=[s for s, _ in rd],
            concurrency=workload.concurrency,
        ))

        log(f"  [{backend.name}/small] delete x{workload.file_count} (conc={workload.concurrency})")
        t0 = time.perf_counter()
        durs = await asyncio.gather(*[_timed_delete(r) for r in remotes])
        results.append(OpResult(
            backend.name, workload.name, "delete",
            time.perf_counter() - t0, workload.total_bytes,
            per_op_seconds=list(durs), concurrency=workload.concurrency,
        ))
    except Exception as e:
        log(f"  [{backend.name}/small] ERROR: {e!r}")
        results.append(OpResult(
            backend.name, workload.name, "error", 0.0, 0, error=repr(e),
        ))
    finally:
        shutil.rmtree(sink, ignore_errors=True)

    return results


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------

def _fmt_mib(b: int) -> str:
    mib = b / (1024 * 1024)
    if mib >= 100:
        return f"{mib:.0f}"
    if mib >= 1:
        return f"{mib:.1f}"
    return f"{mib:.3f}"


def _fmt_rate(mib_per_s: float) -> str:
    if mib_per_s >= 100:
        return f"{mib_per_s:.0f}"
    if mib_per_s >= 1:
        return f"{mib_per_s:.1f}"
    return f"{mib_per_s:.3f}"


def render_table(results: list[OpResult]) -> str:
    rows = []
    rows.append("| backend | workload | op | seconds | MiB | MiB/s | conc |")
    rows.append("|---------|----------|----|--------:|----:|------:|-----:|")
    for r in results:
        if r.error:
            rows.append(
                f"| {r.backend} | {r.workload} | ERROR | — | — | — | — | "
                f"{r.error[:80]} |"
            )
            continue
        rows.append(
            f"| {r.backend} | {r.workload} | {r.op} "
            f"| {r.seconds:.2f} | {_fmt_mib(r.bytes_processed)} "
            f"| {_fmt_rate(r.mb_per_s)} | {r.concurrency} |"
        )
    return "\n".join(rows)


def render_percentile_table(results: list[OpResult]) -> str:
    """Per-op latency distribution (only meaningful for many-files workloads)."""
    rows = [
        "| backend | workload | op | n | min (s) | p50 | p95 | p99 | max | mean |",
        "|---------|----------|----|--:|--------:|----:|----:|----:|----:|-----:|",
    ]
    any_rows = False
    for r in results:
        if not r.per_op_seconds:
            continue
        p = r.percentiles()
        rows.append(
            f"| {r.backend} | {r.workload} | {r.op} | {len(r.per_op_seconds)} "
            f"| {p['min']:.3f} | {p['p50']:.3f} | {p['p95']:.3f} "
            f"| {p['p99']:.3f} | {p['max']:.3f} | {p['mean']:.3f} |"
        )
        any_rows = True
    if not any_rows:
        return ""
    return "\n".join(rows)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

ALL_BACKENDS = ["daytona_sb", "modal_sb", "modal_vol"]
ALL_WORKLOADS = ["small", "200m", "1g", "2g"]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Filesystem benchmark for Daytona / Modal")
    p.add_argument(
        "--backends", default=",".join(ALL_BACKENDS),
        help=f"comma-separated subset of {ALL_BACKENDS}",
    )
    p.add_argument(
        "--workloads", default="small,200m,1g",
        help=f"comma-separated subset of {ALL_WORKLOADS} (2g is opt-in)",
    )
    p.add_argument("--small-count", type=int, default=200)
    p.add_argument("--small-size", type=int, default=64 * 1024,
                   help="bytes per small file (default 64 KiB)")
    p.add_argument("--small-concurrency", type=int, default=64,
                   help="max in-flight ops for the small-files workload (default 64)")
    p.add_argument("--executor-workers", type=int, default=128,
                   help="size of the asyncio thread-pool executor (default 128)")
    p.add_argument("--env-file", default=str(DEFAULT_ENV_FILE))
    p.add_argument("--output", default=None,
                   help="optional path to write a markdown results report")
    p.add_argument("--keep-staging", action="store_true",
                   help="don't delete the local staging dir on exit (lets multi-run reuse the local files)")
    p.add_argument("--staging-dir", default=None,
                   help="reuse this staging dir for the local source files (overrides --keep-staging)")
    return p.parse_args()


def make_backend(name: str) -> Backend:
    if name == "daytona_sb":
        return DaytonaSandboxBackend()
    if name == "modal_sb":
        return ModalSandboxBackend()
    if name == "modal_vol":
        return ModalVolumeBackend()
    raise SystemExit(f"unknown backend: {name}")


async def amain() -> int:
    args = parse_args()
    load_env(Path(args.env_file))

    backends = [b for b in args.backends.split(",") if b]
    for b in backends:
        if b not in ALL_BACKENDS:
            raise SystemExit(f"unknown backend: {b}")

    workloads = build_workloads(
        [w for w in args.workloads.split(",") if w],
        small_count=args.small_count,
        small_size=args.small_size,
        small_concurrency=args.small_concurrency,
    )

    # The default asyncio thread pool maxes out at ~32 workers, which would
    # cap the small-files concurrency long before the SDK does. Bump it.
    from concurrent.futures import ThreadPoolExecutor
    asyncio.get_running_loop().set_default_executor(
        ThreadPoolExecutor(max_workers=args.executor_workers)
    )

    if args.staging_dir:
        staging = Path(args.staging_dir)
        staging.mkdir(parents=True, exist_ok=True)
        cleanup_staging = False
    else:
        staging = Path(tempfile.mkdtemp(prefix="fsbench-staging-"))
        cleanup_staging = not args.keep_staging
    sink = Path(tempfile.mkdtemp(prefix="fsbench-sink-"))

    def log(msg: str) -> None:
        print(msg, flush=True)

    log(f"staging_dir={staging}")
    log(f"sink_dir={sink}")
    log(f"workloads={[w.name for w in workloads]}")
    log(f"backends={backends}")

    all_results: list[OpResult] = []

    try:
        for bname in backends:
            backend = make_backend(bname)
            log(f"\n=== backend: {bname} — setup ===")
            t0 = time.perf_counter()
            try:
                await backend.setup()
            except Exception as e:
                log(f"  setup FAILED: {e!r}")
                all_results.append(OpResult(bname, "-", "setup", 0.0, 0, error=repr(e)))
                continue
            setup_dur = time.perf_counter() - t0
            log(f"  setup ok in {setup_dur:.1f}s")
            all_results.append(OpResult(bname, "-", "setup", setup_dur, 0))

            try:
                for w in workloads:
                    log(f"\n--- {bname} / {w.name} ---")
                    rs = await run_workload(
                        backend, w, staging_dir=staging, sink_dir=sink, log=log,
                    )
                    all_results.extend(rs)
            finally:
                log(f"\n=== backend: {bname} — teardown ===")
                t0 = time.perf_counter()
                with contextlib.suppress(Exception):
                    await backend.teardown()
                log(f"  teardown in {time.perf_counter() - t0:.1f}s")
    finally:
        if cleanup_staging:
            shutil.rmtree(staging, ignore_errors=True)
        shutil.rmtree(sink, ignore_errors=True)

    table = render_table(all_results)
    pct = render_percentile_table(all_results)
    print("\n=== AGGREGATE ===\n")
    print(table)
    if pct:
        print("\n=== PER-OP LATENCY (concurrent workloads) ===\n")
        print(pct)

    if args.output:
        body = (
            "# Filesystem benchmark results\n\n"
            f"- backends: {backends}\n"
            f"- workloads: {[w.name for w in workloads]}\n"
            f"- small_count={args.small_count}, small_size={args.small_size}, "
            f"small_concurrency={args.small_concurrency}\n\n"
            "## Aggregate (wall time per phase)\n\n"
            + table + "\n"
        )
        if pct:
            body += "\n## Per-op latency distribution\n\n" + pct + "\n"
        Path(args.output).write_text(body)
        log(f"\nwrote {args.output}")

    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(amain()))
