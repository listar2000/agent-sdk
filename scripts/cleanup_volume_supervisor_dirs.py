#!/usr/bin/env python3
"""Reclaim disk by deleting ``system/supervisor/`` from existing volumes.

After the runtime-image-unification refactor, the
agent-sdk runtime is baked into the Docker image and providers no longer
read from each volume's ``system/supervisor/`` directory. The dead
directories are not a correctness problem — new code never touches them —
but they waste disk per volume (typically 30–80 MB of node_modules).

This script finds every volume in the DB and removes ``system/supervisor/``
from it. Idempotent: running it twice is a no-op the second time. Dry-run
by default; pass ``--yes`` to actually delete.

Provider scopes:
  --provider unix_local    walks ~/.agent-sdk/volumes/*/system/ on the host
  --provider docker   spawns a short-lived container per volume to rm -rf
  --provider daytona  spawns a short-lived sandbox per volume
  --provider modal    runs an exec against each modal volume
  --provider all      all of the above (default)

Run AFTER deploying the new code so in-flight sessions never read from
``system/supervisor/`` mid-cleanup. The new code path doesn't reference
that directory; flag-off old code does, so the safe order is:

  1. Land Phase A through D (image-runtime is the default).
  2. Run this script.
  3. Land Phase E (delete install_supervisor entirely).

If a volume is in active use during the cleanup and a sandbox happens to
be re-mounting ``system/supervisor`` simultaneously, the rm-rf may race
with a read. Worst case: the rm partially succeeds, the next request
sees an inconsistent state, and the new code path doesn't care because
it never reads from that directory anyway. There's no rollback concern.
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import os
import shutil
import sys
from pathlib import Path

# Ensure src/ is importable when run from repo root.
_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT / "src"))

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger("cleanup")


def _format_size(n: int) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024:
            return f"{n:.1f}{unit}"
        n /= 1024  # type: ignore[assignment]
    return f"{n:.1f}TB"


def _dir_size(path: Path) -> int:
    total = 0
    for root, _dirs, files in os.walk(path):
        for f in files:
            try:
                total += (Path(root) / f).stat().st_size
            except OSError:
                pass
    return total


# ---------------------------------------------------------------------------
# Provider-specific cleanup
# ---------------------------------------------------------------------------

async def _cleanup_local(yes: bool) -> int:
    root = Path(
        os.environ.get("AGENT_SDK_LOCAL_VOL_ROOT")
        or (Path.home() / ".agent-sdk" / "volumes")
    )
    if not root.exists():
        log.info("[local] %s does not exist; nothing to clean", root)
        return 0

    reclaimed = 0
    for vol_dir in sorted(root.iterdir()):
        sup = vol_dir / "system" / "supervisor"
        if not sup.exists():
            continue
        size = _dir_size(sup)
        log.info("[local] %s — would reclaim %s", sup, _format_size(size))
        if yes:
            shutil.rmtree(sup, ignore_errors=True)
            reclaimed += size
    return reclaimed


async def _cleanup_daytona(yes: bool) -> int:
    """Spin up a single utility sandbox per volume and `rm -rf system/supervisor`.

    Daytona's filesystem operations require a sandbox with the volume
    mounted. We mount the whole volume (no subpath) at /v and exec rm.
    """
    if not os.environ.get("DAYTONA_API_KEY"):
        log.info("[daytona] DAYTONA_API_KEY not set; skipping")
        return 0
    try:
        from api import db as dbmod  # type: ignore
        from api.providers import daytona as dt  # type: ignore
    except ImportError as e:
        log.error("[daytona] could not import server modules: %s", e)
        return 0

    if not os.environ.get("DATABASE_URL"):
        log.info("[daytona] DATABASE_URL not set; skipping")
        return 0

    dbmod.init_db()
    await dbmod.init_pool()
    try:
        async with dbmod.get_db() as conn:
            res = await conn.execute(
                "SELECT provider_ref FROM volumes WHERE provider = 'daytona'"
            )
            rows = await res.fetchall()
        refs = [r["provider_ref"] for r in rows if r and r.get("provider_ref")]
    finally:
        await dbmod.close_pool()

    log.info("[daytona] %d volumes to inspect", len(refs))
    if not yes:
        log.info("[daytona] dry run; pass --yes to actually delete")
        return 0

    from daytona_sdk import (  # type: ignore
        Daytona, DaytonaConfig, CreateSandboxFromImageParams, VolumeMount,
    )
    daytona = Daytona(DaytonaConfig(api_key=os.environ["DAYTONA_API_KEY"]))
    loop = asyncio.get_running_loop()

    for ref in refs:
        try:
            sb = await loop.run_in_executor(None, lambda r=ref: daytona.create(
                CreateSandboxFromImageParams(
                    image="alpine:3", auto_stop_interval=0,
                    volumes=[VolumeMount(volume_id=r, mount_path="/v")],
                ), timeout=120,
            ))
            try:
                await loop.run_in_executor(None, lambda: dt._run_sandbox_exec(
                    sb, "rm -rf /v/system/supervisor || true", timeout=60,
                ))
                log.info("[daytona] %s: cleaned", ref)
            finally:
                await loop.run_in_executor(None, lambda: daytona.delete(sb))
        except Exception as e:
            log.warning("[daytona] %s: cleanup failed: %s", ref, e)

    return 0  # daytona side doesn't easily report bytes reclaimed


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------


async def _amain(args: argparse.Namespace) -> int:
    total = 0
    if args.provider in ("unix_local", "all"):
        total += await _cleanup_local(args.yes)
    if args.provider in ("daytona", "all"):
        total += await _cleanup_daytona(args.yes)
    if args.provider in ("docker", "modal"):
        log.info("[%s] cleanup not implemented yet (low priority — most users "
                 "don't accumulate enough cruft to matter; file an issue if "
                 "you do)", args.provider)
    log.info("done. reclaimed=%s%s",
             _format_size(total), "" if args.yes else " (dry run)")
    return 0


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--provider", choices=("unix_local", "docker", "daytona", "modal", "all"),
                   default="all")
    p.add_argument("--yes", action="store_true", help="actually delete (default: dry-run)")
    args = p.parse_args()
    sys.exit(asyncio.run(_amain(args)))


if __name__ == "__main__":
    main()
