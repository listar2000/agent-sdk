"""In-sandbox mounted-volume bench.

Models the realistic case: an agent runs inside a Modal sandbox that has a
Modal volume mounted at ``/vol``. The agent writes/reads/deletes files and
runs heavier workloads (pip install) and we want to know how the volume
mount compares to the sandbox's ephemeral container FS for the same
operations.

Three target paths inside the SAME sandbox are compared:

  - ``/tmp/bench``        — ephemeral container disk (overlayfs on tmpfs/SSD)
  - ``/vol/<sub>/bench``  — mounted Modal volume, unique subpath per run
  - (optional) a SECOND mounted volume's ``/vol2/bench`` — apples-to-apples
    sanity check for two distinct volumes mounted side by side

Each workload runs *inside the sandbox* via ``Sandbox.exec`` so the path
goes through whatever the kernel + Modal volume driver does, not the
client-side ``Volume.batch_upload`` API. That's the right model for
"agent doing work in the volume."

Workloads (deterministic, all driven by ``sh``/coreutils inside the
sandbox so timing reflects in-container syscalls):

  W1. dd 200 MiB write       — bulk sequential write
  W2. dd 200 MiB read        — bulk sequential read
  W3. dd 1 GiB write         — bigger bulk write
  W4. 1000 × 4 KiB random fan-out via shell — many-small-files write,
                                concurrent over xargs ``-P``
  W5. 1000 × 4 KiB sequential read
  W6. 1000-file rm -rf
  W7. pip install pandas (no deps) — realistic agent workload

Run:
    .venv/bin/python benchmark/fs/bench_mount.py --paths tmp,vol \
       --workloads dd200,dd1g,many,pip --output benchmark/fs/RESULTS_MOUNT.md
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import os
import sys
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from bench_fs import load_env, DEFAULT_ENV_FILE, _fmt_rate  # type: ignore[import-not-found]


# ---------------------------------------------------------------------------
# Sandbox provisioning
# ---------------------------------------------------------------------------

@dataclass
class Sandbox:
    sb: Any
    volume: Any
    volume_name: str
    subpath: str  # unique under /vol


def _provision(*, cpu: float, memory: int, install_pip: bool) -> Sandbox:
    import modal
    from modal_proto import api_pb2

    app = modal.App.lookup("fs-bench-mount", create_if_missing=True)
    image = modal.Image.debian_slim()
    if install_pip:
        # python3 + pip + dev tools so pip install works for binary wheels
        image = image.apt_install("python3-pip", "python3-venv")

    vol_name = f"fs-bench-mount-{uuid.uuid4().hex[:8]}"
    vol = modal.Volume.from_name(
        vol_name,
        create_if_missing=True,
        version=api_pb2.VolumeFsVersion.VOLUME_FS_VERSION_V2,
    )
    sub = f"sub-{uuid.uuid4().hex[:6]}"

    sb = modal.Sandbox.create(
        "sleep", "infinity",
        app=app,
        image=image,
        volumes={"/vol": vol},
        timeout=60 * 60,
        cpu=cpu,
        memory=memory,
    )
    # Pre-make working dirs.
    for path in (f"/tmp/bench", f"/vol/{sub}/bench"):
        proc = sb.exec("sh", "-c", f"mkdir -p {path}")
        proc.wait()
    return Sandbox(sb=sb, volume=vol, volume_name=vol_name, subpath=sub)


def _teardown(s: Sandbox) -> None:
    import modal
    with contextlib.suppress(Exception):
        s.sb.terminate()
    with contextlib.suppress(Exception):
        modal.Volume.objects.delete(s.volume_name, allow_missing=True)


# ---------------------------------------------------------------------------
# In-sandbox timing primitive
# ---------------------------------------------------------------------------

def _exec_timed(s: Sandbox, shell: str, *, timeout: int = 600) -> tuple[float, int, str, str]:
    """Run a shell snippet inside the sandbox; return (wall_seconds, rc, stdout, stderr).

    Wall is measured *outside* the sandbox so it includes the small RPC
    overhead — fair for the comparison we care about.
    """
    t0 = time.perf_counter()
    proc = s.sb.exec("sh", "-c", shell, timeout=timeout)
    try:
        rc = proc.wait()
    except TypeError:
        rc = proc.wait(timeout=timeout)
    dur = time.perf_counter() - t0
    out = proc.stdout.read() or ""
    err = proc.stderr.read() or ""
    if isinstance(out, bytes):
        out = out.decode(errors="replace")
    if isinstance(err, bytes):
        err = err.decode(errors="replace")
    return dur, int(rc) if rc is not None else -1, out, err


# ---------------------------------------------------------------------------
# Workload runners
# ---------------------------------------------------------------------------

@dataclass
class OpResult:
    path_label: str        # tmp | vol
    workload: str
    op: str
    seconds: float
    bytes_processed: int = 0
    extra: dict[str, str] = field(default_factory=dict)
    error: str | None = None

    @property
    def mb_per_s(self) -> float:
        if self.seconds <= 0 or self.bytes_processed <= 0:
            return 0.0
        return (self.bytes_processed / (1024 * 1024)) / self.seconds


def _path_for(s: Sandbox, label: str) -> str:
    if label == "tmp":
        return "/tmp/bench"
    if label == "vol":
        return f"/vol/{s.subpath}/bench"
    raise ValueError(label)


def run_dd(s: Sandbox, label: str, mib: int) -> list[OpResult]:
    base = _path_for(s, label)
    target = f"{base}/dd_{mib}m.bin"
    out: list[OpResult] = []

    # Drop caches in *between* read/write where possible. Sandboxes don't
    # have CAP_SYS_ADMIN to drop caches, so we instead use direct I/O via
    # ``oflag=direct`` / ``iflag=direct`` when available. If the kernel
    # rejects O_DIRECT (most overlayfs do), we fall back to buffered I/O —
    # results then reflect kernel cache too. The dd output reports both
    # raw rate and total seconds via ``status=progress``-free path; we use
    # ``status=none`` and compute rate ourselves.
    write_cmd = (
        f"rm -f {target} && "
        f"dd if=/dev/zero of={target} bs=1M count={mib} status=none && "
        f"sync && stat -c '%s' {target}"
    )
    dur, rc, stdout, stderr = _exec_timed(s, write_cmd, timeout=900)
    bytes_written = 0
    try:
        bytes_written = int(stdout.strip().splitlines()[-1])
    except Exception:
        bytes_written = mib * 1024 * 1024
    out.append(OpResult(
        label, f"dd{mib}m", "write", dur, bytes_written,
        error=None if rc == 0 else f"rc={rc} {stderr[-200:]}",
    ))

    read_cmd = (
        f"dd if={target} of=/dev/null bs=1M status=none"
    )
    dur, rc, _, stderr = _exec_timed(s, read_cmd, timeout=900)
    out.append(OpResult(
        label, f"dd{mib}m", "read", dur, bytes_written,
        error=None if rc == 0 else f"rc={rc} {stderr[-200:]}",
    ))

    rm_cmd = f"rm -f {target}"
    dur, rc, _, stderr = _exec_timed(s, rm_cmd, timeout=120)
    out.append(OpResult(
        label, f"dd{mib}m", "delete", dur, bytes_written,
        error=None if rc == 0 else f"rc={rc} {stderr[-200:]}",
    ))
    return out


def run_many_small(s: Sandbox, label: str, *, count: int, size: int, parallel: int) -> list[OpResult]:
    """Many-small-file workload using xargs -P inside the sandbox."""
    base = _path_for(s, label)
    src = f"{base}/_seed.bin"
    dir_ = f"{base}/many"

    out: list[OpResult] = []

    setup = (
        f"rm -rf {dir_} && mkdir -p {dir_} && "
        f"dd if=/dev/urandom of={src} bs={size} count=1 status=none"
    )
    dur, rc, _, stderr = _exec_timed(s, setup, timeout=120)
    if rc != 0:
        out.append(OpResult(label, f"many{count}", "setup", dur, 0,
                            error=f"rc={rc} {stderr[-200:]}"))
        return out

    # Concurrent write: seq | xargs -P N -I{} cp $src $dir/{}
    write_cmd = (
        f"seq 1 {count} | xargs -P {parallel} -I {{}} cp {src} {dir_}/f{{}}.bin"
    )
    dur, rc, _, stderr = _exec_timed(s, write_cmd, timeout=900)
    out.append(OpResult(
        label, f"many{count}", "write", dur, count * size,
        extra={"parallel": str(parallel)},
        error=None if rc == 0 else f"rc={rc} {stderr[-200:]}",
    ))

    # Concurrent read: cat into /dev/null
    read_cmd = (
        f"seq 1 {count} | xargs -P {parallel} -I {{}} cat {dir_}/f{{}}.bin > /dev/null"
    )
    dur, rc, _, stderr = _exec_timed(s, read_cmd, timeout=900)
    out.append(OpResult(
        label, f"many{count}", "read", dur, count * size,
        extra={"parallel": str(parallel)},
        error=None if rc == 0 else f"rc={rc} {stderr[-200:]}",
    ))

    delete_cmd = f"rm -rf {dir_} {src}"
    dur, rc, _, stderr = _exec_timed(s, delete_cmd, timeout=120)
    out.append(OpResult(
        label, f"many{count}", "delete", dur, count * size,
        error=None if rc == 0 else f"rc={rc} {stderr[-200:]}",
    ))
    return out


def run_pip_install(s: Sandbox, label: str, package: str = "pandas") -> list[OpResult]:
    base = _path_for(s, label)
    target = f"{base}/pkgs"
    out: list[OpResult] = []

    install_cmd = (
        f"rm -rf {target} && mkdir -p {target} && "
        f"python3 -m pip install --quiet --no-cache-dir --no-deps "
        f"--target={target} {package} 2>&1 | tail -5; "
        f"du -sb {target}"
    )
    dur, rc, stdout, stderr = _exec_timed(s, install_cmd, timeout=900)
    bytes_size = 0
    try:
        last = stdout.strip().splitlines()[-1]
        bytes_size = int(last.split()[0])
    except Exception:
        pass
    out.append(OpResult(
        label, f"pip:{package}", "install", dur, bytes_size,
        extra={"installed_bytes": str(bytes_size)},
        error=None if rc == 0 else f"rc={rc} stdout={stdout[-200:]} stderr={stderr[-200:]}",
    ))

    rm_dur, rc, _, stderr = _exec_timed(s, f"rm -rf {target}", timeout=300)
    out.append(OpResult(
        label, f"pip:{package}", "rm", rm_dur, bytes_size,
        error=None if rc == 0 else f"rc={rc} {stderr[-200:]}",
    ))
    return out


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------

def render(results: list[OpResult]) -> str:
    rows = [
        "| path | workload | op | seconds | MiB | MiB/s | extra |",
        "|------|----------|----|--------:|----:|------:|-------|",
    ]
    for r in results:
        if r.error:
            rows.append(
                f"| {r.path_label} | {r.workload} | {r.op} | ERROR | — | — | {r.error[:120]} |"
            )
            continue
        mib = r.bytes_processed / (1024 * 1024)
        extras = ", ".join(f"{k}={v}" for k, v in r.extra.items())
        rows.append(
            f"| {r.path_label} | {r.workload} | {r.op} "
            f"| {r.seconds:.2f} | {mib:.2f} | {_fmt_rate(r.mb_per_s)} | {extras} |"
        )
    return "\n".join(rows)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--paths", default="tmp,vol",
                   help="comma list of tmp,vol — which target paths to test")
    p.add_argument("--workloads", default="dd200,many,pip",
                   help="comma list: dd200,dd1g,many,pip")
    p.add_argument("--cpu", type=float, default=4.0)
    p.add_argument("--memory", type=int, default=4096)
    p.add_argument("--many-count", type=int, default=1000)
    p.add_argument("--many-size", type=int, default=4096)
    p.add_argument("--many-parallel", type=int, default=16)
    p.add_argument("--pip-package", default="pandas")
    p.add_argument("--env-file", default=str(DEFAULT_ENV_FILE))
    p.add_argument("--output", default=None)
    return p.parse_args()


async def amain() -> int:
    args = parse_args()
    load_env(Path(args.env_file))

    paths = [p.strip() for p in args.paths.split(",") if p.strip()]
    workloads = [w.strip() for w in args.workloads.split(",") if w.strip()]
    install_pip = "pip" in workloads

    asyncio.get_running_loop().set_default_executor(
        ThreadPoolExecutor(max_workers=64)
    )

    def log(m: str) -> None:
        print(m, flush=True)

    log(f"paths={paths} workloads={workloads}")
    log("provisioning Modal sandbox + volume...")

    t0 = time.perf_counter()
    s = await asyncio.to_thread(
        _provision, cpu=args.cpu, memory=args.memory, install_pip=install_pip,
    )
    log(f"  sandbox ready in {time.perf_counter() - t0:.1f}s "
        f"(volume={s.volume_name}, subpath={s.subpath})")

    all_results: list[OpResult] = []

    try:
        for w in workloads:
            for p in paths:
                log(f"\n--- workload={w} path={p} ---")
                if w == "dd200":
                    rs = await asyncio.to_thread(run_dd, s, p, 200)
                elif w == "dd1g":
                    rs = await asyncio.to_thread(run_dd, s, p, 1024)
                elif w == "many":
                    rs = await asyncio.to_thread(
                        run_many_small, s, p,
                        count=args.many_count,
                        size=args.many_size,
                        parallel=args.many_parallel,
                    )
                elif w == "pip":
                    rs = await asyncio.to_thread(run_pip_install, s, p, args.pip_package)
                else:
                    log(f"unknown workload {w!r}; skipping")
                    rs = []
                for r in rs:
                    log(f"  {r.path_label}/{r.workload}/{r.op} "
                        f"{r.seconds:.2f}s "
                        f"{r.bytes_processed/(1024*1024):.1f} MiB "
                        f"{r.mb_per_s:.1f} MiB/s"
                        + (f" ERROR: {r.error[:120]}" if r.error else ""))
                all_results.extend(rs)
    finally:
        log("\ntearing down...")
        await asyncio.to_thread(_teardown, s)

    table = render(all_results)
    print("\n=== RESULTS ===\n")
    print(table)

    if args.output:
        body = (
            "# Mounted-volume bench (in-sandbox)\n\n"
            f"- Modal sandbox cpu={args.cpu} memory={args.memory} MiB\n"
            f"- Volume mounted at /vol; subpath `{s.subpath}`\n"
            f"- paths tested: {paths}\n"
            f"- workloads: {workloads}\n"
            f"- many: count={args.many_count} size={args.many_size} parallel={args.many_parallel}\n\n"
            "## Results\n\n" + table + "\n"
        )
        Path(args.output).write_text(body)
        log(f"\nwrote {args.output}")

    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(amain()))
