#!/usr/bin/env python3
"""Delete a specific list of Daytona sandbox UUIDs (one per line on stdin or
in a file). Intended for one-off cleanup of orphans whose IDs were already
captured from server logs — does not enumerate the account, so it dodges the
"broad list-all" restriction.

Usage:

    # From a file:
    python scripts/delete_daytona_sandboxes_by_id.py /tmp/test_daytona_sandboxes.txt --yes

    # Or from stdin:
    cat ids.txt | python scripts/delete_daytona_sandboxes_by_id.py - --yes

Default is dry-run (no --yes) — shows which IDs would be touched, doesn't
delete. Sandboxes that don't exist are silently skipped (already-deleted
is success). Other errors are reported per-id.

Requires DAYTONA_API_KEY in env.
"""
from __future__ import annotations

import argparse
import os
import sys

try:
    from daytona_sdk import Daytona, DaytonaConfig
except ImportError:
    sys.exit("daytona-sdk not installed. Run: pip install daytona-sdk")


def _read_ids(path: str) -> list[str]:
    if path == "-":
        text = sys.stdin.read()
    else:
        with open(path) as f:
            text = f.read()
    ids = []
    for line in text.splitlines():
        sid = line.strip()
        # Skip empty / comment lines.
        if not sid or sid.startswith("#"):
            continue
        ids.append(sid)
    return ids


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("path", help="file with one daytona sandbox UUID per line, "
                                "or '-' for stdin")
    p.add_argument("--yes", action="store_true",
                   help="actually delete (default is dry-run)")
    args = p.parse_args()

    api_key = os.environ.get("DAYTONA_API_KEY")
    if not api_key:
        sys.exit("DAYTONA_API_KEY not set")
    daytona = Daytona(DaytonaConfig(api_key=api_key))

    ids = _read_ids(args.path)
    if not ids:
        print("No IDs to process.")
        return

    print(f"Will process {len(ids)} sandbox id(s).")
    if not args.yes:
        for sid in ids[:10]:
            print(f"  {sid}")
        if len(ids) > 10:
            print(f"  ... and {len(ids) - 10} more")
        print("\n(dry run — pass --yes to delete)")
        return

    deleted = skipped = failed = 0
    for sid in ids:
        try:
            sb = daytona.get(sid)
        except Exception as e:
            msg = str(e).lower()
            if "not found" in msg or "404" in msg:
                skipped += 1
                continue
            failed += 1
            print(f"  GET FAILED {sid}: {e}")
            continue
        try:
            daytona.delete(sb)
            deleted += 1
            print(f"  deleted {sid}")
        except Exception as e:
            failed += 1
            print(f"  DELETE FAILED {sid}: {e}")

    print(f"\nDone: deleted={deleted} already_gone={skipped} failed={failed}")
    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
