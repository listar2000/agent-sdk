#!/usr/bin/env python3
"""Delete agent-sdk Daytona sandboxes left behind by the test suite.

Every sandbox provisioned by ``src/api/providers/daytona.py`` is stamped
at create time with one label:

    agent_sdk_origin = <AGENT_SDK_ORIGIN env, default "production">

Run the test server with ``AGENT_SDK_ORIGIN=test`` so test sandboxes
are tagged ``"test"``; production sandboxes default to ``"production"``
and are not touched by the default filter.

Usage:

    # See what's orphaned (dry-run):
    python scripts/cleanup_daytona_orphans.py

    # Delete them:
    python scripts/cleanup_daytona_orphans.py --yes

    # Different origin (e.g., a crashed prod server's sandboxes):
    python scripts/cleanup_daytona_orphans.py --origin production --yes

Requires ``DAYTONA_API_KEY`` in env (the same one the server uses).
"""
from __future__ import annotations

import argparse
import os
import sys

try:
    from daytona_sdk import Daytona, DaytonaConfig
except ImportError:
    sys.exit("daytona-sdk not installed. Run: pip install daytona-sdk")


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--origin", default="test",
                   help="agent_sdk_origin label to match (default: 'test')")
    p.add_argument("--yes", action="store_true",
                   help="actually delete (default is dry-run)")
    args = p.parse_args()

    api_key = os.environ.get("DAYTONA_API_KEY")
    if not api_key:
        sys.exit("DAYTONA_API_KEY not set")
    daytona = Daytona(DaytonaConfig(api_key=api_key))

    page = daytona.list(labels={"agent_sdk_origin": args.origin})
    items = list(getattr(page, "items", None) or page)

    if not items:
        print(f"No sandboxes with agent_sdk_origin={args.origin!r}.")
        return

    print(f"Found {len(items)} sandbox(es) with agent_sdk_origin={args.origin!r}:")
    for sb in items:
        state = getattr(sb, "state", "?")
        state_str = state.value if hasattr(state, "value") else str(state)
        print(f"  {sb.id[:24]} state={state_str}")

    if not args.yes:
        print("\n(dry run — pass --yes to delete)")
        return

    print()
    failures = 0
    for sb in items:
        try:
            daytona.delete(sb)
            print(f"  deleted {sb.id[:24]}")
        except Exception as e:
            failures += 1
            print(f"  FAILED  {sb.id[:24]}: {e}")
    if failures:
        sys.exit(f"\n{failures} delete(s) failed.")
    print(f"\nDeleted {len(items)} sandbox(es).")


if __name__ == "__main__":
    main()
