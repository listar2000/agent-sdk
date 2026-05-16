#!/usr/bin/env python3
"""Reap test orphan sandboxes across providers.

Each provider tags sandboxes it provisions with one shared label:

    agent_sdk_origin = <AGENT_SDK_ORIGIN env, default "production">

All local-dev launchers (``scripts/launch_server_test.sh``,
``scripts/launch_server_docker.sh``, and ``docker compose up``) default
``AGENT_SDK_ORIGIN=test`` so test sandboxes are tagged ``"test"`` and
can be reaped without touching real production traffic. Production
deploys (Railway via ``Dockerfile``) leave the env unset and the server
falls back to ``"production"``.

Daytona pauses-not-deletes on session release, so paused-but-not-deleted
sandboxes pile up against the account's disk quota across CI runs. Docker
containers stop on release; their rootfs sticks around until ``docker rm``.
This script handles both.

Usage:

    # Dry run across daytona + docker + unix_local (default --provider=all):
    python scripts/cleanup_orphans.py

    # Actually reap (across all providers):
    python scripts/cleanup_orphans.py --yes

    # One provider only:
    python scripts/cleanup_orphans.py --provider docker --yes

    # Different origin (e.g. a crashed prod server's orphans):
    python scripts/cleanup_orphans.py --origin production --yes

Environment:
  DAYTONA_API_KEY  required for the daytona path (skipped silently if absent)
  AGENT_SDK_ORIGIN read by the script's default --origin if --origin not given
                   (matches what the server stamped at create time)
"""
from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys


def _reap_daytona(origin: str, *, dry_run: bool) -> int:
    """Delete daytona sandboxes labeled with ``agent_sdk_origin=<origin>``.
    Returns the count reaped (or seen, in dry-run mode)."""
    api_key = os.environ.get("DAYTONA_API_KEY")
    if not api_key:
        print("[daytona] DAYTONA_API_KEY not set — skipping")
        return 0
    try:
        from daytona_sdk import Daytona, DaytonaConfig
    except ImportError:
        print("[daytona] daytona-sdk not installed — skipping")
        return 0

    daytona = Daytona(DaytonaConfig(api_key=api_key))
    items = _daytona_list_all(daytona, labels={"agent_sdk_origin": origin})

    if not items:
        print(f"[daytona] no sandboxes with agent_sdk_origin={origin!r}")
        return 0

    print(f"[daytona] found {len(items)} sandbox(es) with agent_sdk_origin={origin!r}:")
    for sb in items:
        state = getattr(sb, "state", "?")
        state_str = state.value if hasattr(state, "value") else str(state)
        print(f"  {sb.id[:24]} state={state_str}")

    if dry_run:
        return len(items)

    failed = 0
    for sb in items:
        try:
            daytona.delete(sb)
            print(f"[daytona]   deleted {sb.id[:24]}")
        except Exception as e:
            failed += 1
            print(f"[daytona]   FAILED  {sb.id[:24]}: {e}")
    if failed:
        print(f"[daytona] {failed} delete(s) failed")
    return len(items) - failed


def _daytona_list_all(daytona, *, labels: dict[str, str]) -> list:
    """Walk all pages of ``daytona.list(labels=...)``.

    Returns a flat list of Sandbox objects. The daytona-sdk's ``list``
    returns a ``PaginatedSandboxes`` whose ``items`` field is just the
    first page (default 100); without pagination, ``cleanup_orphans``
    silently leaves later pages behind and the "Reaped 100 resource(s)"
    counter looks complete even when there are 200+ orphans against
    the account quota. Walk pages explicitly until items < page_size
    or we see total_pages exhausted.
    """
    out: list = []
    page = 1
    while True:
        result = daytona.list(labels=labels, page=page)
        # PaginatedSandboxes exposes ``items`` plus ``total`` / ``total_pages``
        # via attributes or as tuple entries on older SDK versions. Probe
        # both shapes so a daytona-sdk bump doesn't silently regress.
        items: list = []
        total_pages: int | None = None
        if hasattr(result, "items"):
            items = list(result.items)
            total_pages = getattr(result, "total_pages", None)
        else:
            for tup in result:
                if not isinstance(tup, tuple) or len(tup) != 2:
                    continue
                if tup[0] == "items":
                    items = list(tup[1])
                elif tup[0] == "total_pages":
                    total_pages = tup[1]
        if not items:
            break
        out.extend(items)
        if total_pages is None or page >= total_pages:
            break
        page += 1
    return out


def _reap_docker(origin: str, *, dry_run: bool) -> int:
    """``docker rm -f`` containers labeled with ``agent_sdk_origin=<origin>``.

    Includes stopped containers (the supervisor exits when its sandbox is
    released, leaving the container row behind so ``docker inspect`` can
    still report the exited state — until this reaper picks it up).
    """
    if shutil.which("docker") is None:
        print("[docker] docker not installed — skipping")
        return 0
    try:
        # ``-a`` includes stopped containers. ``-q`` for IDs only.
        ids = subprocess.check_output(
            ["docker", "ps", "-a", "-q",
             "--filter", f"label=agent_sdk_origin={origin}"],
            timeout=15,
        ).decode().split()
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as e:
        print(f"[docker] could not list containers: {e}")
        return 0

    if not ids:
        print(f"[docker] no containers with agent_sdk_origin={origin!r}")
        return 0

    print(f"[docker] found {len(ids)} container(s) with agent_sdk_origin={origin!r}:")
    for cid in ids:
        print(f"  {cid[:12]}")

    if dry_run:
        return len(ids)

    failed = 0
    for cid in ids:
        try:
            subprocess.check_call(
                ["docker", "rm", "-f", cid],
                stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, timeout=30,
            )
            print(f"[docker]   removed {cid[:12]}")
        except subprocess.CalledProcessError as e:
            failed += 1
            print(f"[docker]   FAILED  {cid[:12]}: {e.stderr.decode(errors='replace').strip()[:200]}")
        except subprocess.TimeoutExpired:
            failed += 1
            print(f"[docker]   FAILED  {cid[:12]}: timed out")
    if failed:
        print(f"[docker] {failed} delete(s) failed")
    return len(ids) - failed


def _reap_local(*, dry_run: bool) -> int:
    """Reap orphan local supervisor.js processes whose parent has died.

    Local sandboxes are subprocesses of the dev server (supervisor.js
    + node + the agent CLI). The dev server's idle reaper kills them
    cleanly on graceful shutdown; if the server crashes / is SIGKILLed,
    the supervisors get reparented to PID 1 and live indefinitely.

    Heuristic — kill processes that ALL match:
      * argv contains ``supervisor.js``
      * argv contains ``claude-agent-acp`` or another known --acp target
      * parent PID is 1 (orphaned, adopted by init)

    Skips supervisors with a live parent (a running dev server)
    so a developer's working session isn't killed.
    """
    if sys.platform not in ("linux", "darwin"):
        return 0
    try:
        # Linux ``ps`` supports ``-o ppid=,pid=,args=``; macOS ``ps`` likewise.
        out = subprocess.check_output(
            ["ps", "-eo", "ppid=,pid=,args="],
            timeout=5,
        ).decode()
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
        return 0

    orphans: list[tuple[int, str]] = []
    for line in out.splitlines():
        try:
            ppid_s, rest = line.strip().split(None, 1)
            pid_s, args = rest.split(None, 1)
            ppid = int(ppid_s)
            pid = int(pid_s)
        except (ValueError, IndexError):
            continue
        if "supervisor.js" not in args:
            continue
        # Only consider true orphans (parent reaped by init).
        if ppid != 1:
            continue
        # Defence-in-depth: confirm this looks like our supervisor.
        if not any(
            tag in args for tag in (
                "claude-agent-acp", "codex-acp",
                "agent-sdk", ".agent-sdk", "node_modules/.bin/",
            )
        ):
            continue
        orphans.append((pid, args[:160]))

    if not orphans:
        print("[local] no orphan supervisor.js processes")
        return 0

    print(f"[local] found {len(orphans)} orphan supervisor.js process(es):")
    for pid, args in orphans:
        print(f"  pid={pid}  {args}")

    if dry_run:
        return len(orphans)

    failed = 0
    for pid, _args in orphans:
        try:
            os.kill(pid, 15)  # SIGTERM
            print(f"[local]   SIGTERM pid={pid}")
        except ProcessLookupError:
            pass
        except Exception as e:
            failed += 1
            print(f"[local]   FAILED pid={pid}: {e}")
    if failed:
        print(f"[local] {failed} kill(s) failed")
    return len(orphans) - failed


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument(
        "--origin", default=os.environ.get("AGENT_SDK_ORIGIN", "test"),
        help="agent_sdk_origin label to match (default: $AGENT_SDK_ORIGIN or 'test')",
    )
    p.add_argument(
        "--provider", choices=("daytona", "docker", "unix_local", "all"),
        default="all",
        help="restrict to one provider (default: all)",
    )
    p.add_argument(
        "--yes", action="store_true",
        help="actually reap (default is dry-run)",
    )
    args = p.parse_args()

    dry = not args.yes
    total = 0
    if args.provider in ("daytona", "all"):
        total += _reap_daytona(args.origin, dry_run=dry)
    if args.provider in ("docker", "all"):
        total += _reap_docker(args.origin, dry_run=dry)
    if args.provider in ("unix_local", "all"):
        total += _reap_local(dry_run=dry)

    if dry:
        print(f"\n(dry run — pass --yes to reap; would touch {total} resource(s))")
    else:
        print(f"\nReaped {total} resource(s).")


if __name__ == "__main__":
    main()
