#!/usr/bin/env python3
"""Delete agent-sdk-tagged Daytona sandboxes that the test suite (or a
crashed server) left behind.

Every sandbox provisioned by ``src/api/providers/daytona.py`` is stamped
at create time with these labels:

    agent_sdk            = "1"
    agent_sdk_origin     = "production" | "test" | <whatever AGENT_SDK_ORIGIN was>
    agent_sdk_purpose    = "session" | "install" | "volume-op"
    agent_sdk_sandbox_id = <db row id>            (only for purpose=session)

This script lists sandboxes filtered by those labels and deletes them.
Default mode is ``--dry-run`` — review the list first; pass ``--yes`` to
actually delete. Filter narrowly: by default we only look at
``origin=test`` so a misfire on a shared account can't take production
compute down.

Usage:

    # See what's orphaned (test-origin, all purposes):
    python scripts/cleanup_daytona_orphans.py

    # Delete them:
    python scripts/cleanup_daytona_orphans.py --yes

    # Wider net — delete every agent-sdk sandbox regardless of origin
    # (DANGEROUS in shared accounts; only for solo dev environments):
    python scripts/cleanup_daytona_orphans.py --origin '*' --yes

    # Target one specific sandbox by its agent-sdk id:
    python scripts/cleanup_daytona_orphans.py \
        --sandbox-id 6da4ab5e-67d3-4c2c-bbc4-069f406e2949 --yes

Requires ``DAYTONA_API_KEY`` in env (the same one the server uses).
"""
from __future__ import annotations

import argparse
import os
import sys
from typing import Iterable

try:
    from daytona_sdk import Daytona, DaytonaConfig
except ImportError:
    sys.exit("daytona-sdk not installed. Run: pip install daytona-sdk")


def _client() -> Daytona:
    api_key = os.environ.get("DAYTONA_API_KEY")
    if not api_key:
        sys.exit("DAYTONA_API_KEY not set")
    return Daytona(DaytonaConfig(api_key=api_key))


def _list_orphans(daytona: Daytona, *, origin: str, purpose: str | None,
                  sandbox_id: str | None) -> list:
    """Return Daytona sandboxes matching the agent-sdk label filter.

    ``origin == "*"`` widens to "any agent-sdk sandbox regardless of origin"
    (uses the universal ``agent_sdk=1`` label). Anything else is exact-match
    on ``agent_sdk_origin``.
    """
    if sandbox_id:
        labels = {"agent_sdk_sandbox_id": sandbox_id}
    elif origin == "*":
        labels = {"agent_sdk": "1"}
    else:
        labels = {"agent_sdk_origin": origin}
    if purpose:
        labels["agent_sdk_purpose"] = purpose
    page = daytona.list(labels=labels)
    items = getattr(page, "items", None) or list(page)
    return list(items)


def _summarize(sandboxes: Iterable) -> None:
    for sb in sandboxes:
        labels = getattr(sb, "labels", {}) or {}
        purpose = labels.get("agent_sdk_purpose", "?")
        origin = labels.get("agent_sdk_origin", "?")
        sb_id = labels.get("agent_sdk_sandbox_id", "")
        sb_id_short = sb_id[:8] if sb_id else "-"
        state = getattr(sb, "state", "?")
        state_str = state.value if hasattr(state, "value") else str(state)
        print(f"  {sb.id[:24]} state={state_str:8s} "
              f"purpose={purpose:9s} origin={origin:11s} "
              f"sandbox_id={sb_id_short}")


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--origin", default="test",
                   help="agent_sdk_origin label to match (default: 'test'; "
                        "use '*' to match any agent-sdk sandbox)")
    p.add_argument("--purpose", default=None,
                   help="filter by purpose: session | install | volume-op")
    p.add_argument("--sandbox-id", default=None,
                   help="exact agent-sdk sandbox id (overrides --origin)")
    p.add_argument("--yes", action="store_true",
                   help="actually delete (default is dry-run)")
    args = p.parse_args()

    daytona = _client()
    orphans = _list_orphans(
        daytona, origin=args.origin, purpose=args.purpose,
        sandbox_id=args.sandbox_id,
    )

    if not orphans:
        print(f"No matches for origin={args.origin!r} purpose={args.purpose!r}.")
        return

    print(f"Found {len(orphans)} sandbox(es):")
    _summarize(orphans)

    if not args.yes:
        print("\n(dry run — pass --yes to delete)")
        return

    print()
    failures: list[tuple[str, str]] = []
    for sb in orphans:
        try:
            daytona.delete(sb)
            print(f"  deleted {sb.id[:24]}")
        except Exception as e:
            failures.append((sb.id, str(e)))
            print(f"  FAILED  {sb.id[:24]}: {e}")
    if failures:
        sys.exit(f"\n{len(failures)} delete(s) failed.")
    print(f"\nDeleted {len(orphans)} sandbox(es).")


if __name__ == "__main__":
    main()
