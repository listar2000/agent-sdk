"""Build a Modal sandbox filesystem snapshot from the production image.

Why
---
Cold-create from ``Image.from_dockerfile`` takes ~85s on Modal because the
image must be built remotely and pulled to the worker on first
container schedule. Cold-create from a pre-built filesystem snapshot
takes ~2s — Modal already has the layers materialised on its fast
path.

This script:

  1. Builds the image from the repo's root ``Dockerfile``.
  2. Spawns one short-lived sandbox so Modal fully materialises the
     image artifact.
  3. Calls ``Sandbox.snapshot_filesystem()`` to mint a snapshot Image.
  4. Writes the snapshot's ``object_id`` to ``.modal-snapshot-tag``
     at the repo root.

The agent-sdk modal provider (``src/api/providers/modal.py:_get_image``)
prefers the snapshot when ``.modal-snapshot-tag`` exists and falls
back to ``Image.from_dockerfile`` otherwise (dev / first-boot).

Run after every change that affects the runtime image:

  * ``Dockerfile``
  * ``src/supervisor/package.json`` / ``supervisor.js``
  * ``src/api/providers/_shared.py:_ACP_NPM_SPECS``

Same trigger conditions as ``scripts/release.sh`` documents for the
daytona snapshot.

Usage:
    python scripts/release_modal_snapshot.py
"""
from __future__ import annotations

import os
import sys
import time

import modal


APP_NAME = "agent-sdk-snapshot-builder"
TAG_FILE_NAME = ".modal-snapshot-tag"


def main() -> int:
    repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    dockerfile = os.path.join(repo_root, "Dockerfile")
    tag_file = os.path.join(repo_root, TAG_FILE_NAME)

    if not os.path.exists(dockerfile):
        print(f"ERROR: Dockerfile not found at {dockerfile}", file=sys.stderr)
        return 1

    print(f"modal SDK: {modal.__version__}")
    print(f"using Dockerfile: {dockerfile}")

    app = modal.App.lookup(APP_NAME, create_if_missing=True)
    image = modal.Image.from_dockerfile(dockerfile)

    print("\n[1/3] spawning warm sandbox (this triggers the remote image build "
          "if not already cached on Modal)...")
    t0 = time.perf_counter()
    sb = modal.Sandbox.create(
        "sleep", "120",
        app=app,
        image=image,
        timeout=600,
    )
    # Force-realise the container by running a trivial exec — guarantees
    # the FS we're about to snapshot is fully populated, not mid-pull.
    proc = sb.exec("echo", "ready")
    proc.wait()
    print(f"  sandbox ready: {time.perf_counter() - t0:.2f}s")

    try:
        print("\n[2/3] snapshotting filesystem...")
        t0 = time.perf_counter()
        fs_image = sb.snapshot_filesystem(timeout=120)
        snap_id = fs_image.object_id
        print(f"  snapshot_filesystem: {time.perf_counter() - t0:.2f}s")
        print(f"  snapshot image_id: {snap_id}")
    finally:
        try:
            sb.terminate()
        except Exception:
            pass

    print(f"\n[3/3] writing {tag_file} ...")
    with open(tag_file, "w") as f:
        f.write(snap_id + "\n")
    print(f"  wrote: {snap_id}")
    print("\nDone. Commit .modal-snapshot-tag along with any image-affecting change.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
