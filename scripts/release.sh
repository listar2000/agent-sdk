#!/usr/bin/env bash
# Build the agent-sdk runtime image, build a Daytona snapshot from it,
# and pin both tags so providers default to known-good runtimes without
# manual env-var setup.
#
# This script is the source of truth for "what runtime ships" (see
# docs/runtime-image-unification.md). Each successful run produces:
#
#   1. A local Docker image tagged ``agent-sdk:<git-sha>`` (or
#      ``<git-sha>-dirty-<ts>`` when uncommitted changes are present and
#      RELEASE_ALLOW_DIRTY=1). The docker provider consumes this directly
#      from the local daemon — no registry push required.
#
#   2. A Daytona snapshot named ``agent-sdk-<git-sha>``, built from
#      ./Dockerfile via the daytona Python SDK
#      (``Image.from_dockerfile``). Daytona handles the build remotely;
#      we don't push to a registry.
#
#   3. ``.runtime-image-tag`` and ``.runtime-snapshot-tag`` files
#      committed to the repo so docker / daytona / modal providers
#      auto-resolve the right runtime without per-environment env vars.
#
# Optional: ``RELEASE_PUSH=1`` also pushes to ``$AGENT_SDK_REGISTRY/agent-sdk:<sha>``
# (default ghcr.io/<git-org>) for multi-machine CI / cross-account access.
# Skipped by default since neither docker nor daytona need a registry.
#
# Usage:
#
#   scripts/release.sh                          # build local + snapshot, no push
#   RELEASE_PUSH=1 scripts/release.sh           # also push image to registry
#   RELEASE_ALLOW_DIRTY=1 scripts/release.sh    # build with uncommitted changes
#   AGENT_SDK_REGISTRY=ghcr.io/myorg scripts/release.sh   # override registry

set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "${SCRIPT_DIR}/.." && pwd)"
cd "$REPO_ROOT"

REGISTRY="${AGENT_SDK_REGISTRY:-ghcr.io/rllm-org}"
SHA="$(git rev-parse --short HEAD)"

# Refuse dirty trees by default — the SHA wouldn't reflect the image's
# contents. RELEASE_ALLOW_DIRTY=1 opts in (useful during iteration);
# dirty builds get a timestamp suffix so they're visibly distinct from
# committed ones.
DIRTY_SUFFIX=""
if ! git diff --quiet --ignore-submodules HEAD; then
  if [[ "${RELEASE_ALLOW_DIRTY:-0}" != "1" ]]; then
    echo "release.sh: refusing to build with uncommitted changes (the SHA tag" >&2
    echo "  would lie about what's in the image). Commit or stash first," >&2
    echo "  or set RELEASE_ALLOW_DIRTY=1." >&2
    exit 1
  fi
  DIRTY_SUFFIX="-dirty-$(date +%s)"
fi

LOCAL_TAG="agent-sdk:${SHA}${DIRTY_SUFFIX}"
SNAPSHOT_NAME="agent-sdk-${SHA}${DIRTY_SUFFIX}"

# ── 1. Build local docker image ─────────────────────────────────────────
echo "[release] building $LOCAL_TAG"
docker build -t "$LOCAL_TAG" .
echo "$LOCAL_TAG" > "$REPO_ROOT/.runtime-image-tag"
echo "[release] wrote .runtime-image-tag := $LOCAL_TAG"

# ── 2. Build / register Daytona snapshot ────────────────────────────────
if [[ -n "${DAYTONA_API_KEY:-}" ]]; then
  echo "[release] registering Daytona snapshot $SNAPSHOT_NAME (remote build, ~5 min)"
  "$REPO_ROOT/.venv/bin/python" - <<PYEOF
import os, sys
from daytona_sdk import Daytona, DaytonaConfig, CreateSnapshotParams, Image
client = Daytona(DaytonaConfig(api_key=os.environ["DAYTONA_API_KEY"]))
result = client.snapshot.create(
    CreateSnapshotParams(
        name="$SNAPSHOT_NAME",
        image=Image.from_dockerfile("Dockerfile"),
    )
)
print(f"[release] snapshot {result.name} state={result.state}", file=sys.stderr)
PYEOF
  echo "$SNAPSHOT_NAME" > "$REPO_ROOT/.runtime-snapshot-tag"
  echo "[release] wrote .runtime-snapshot-tag := $SNAPSHOT_NAME"
else
  echo "[release] DAYTONA_API_KEY unset; skipping Daytona snapshot register"
fi

# ── 3. Optional: push to registry ───────────────────────────────────────
if [[ "${RELEASE_PUSH:-0}" == "1" ]]; then
  REMOTE_TAG="${REGISTRY}/agent-sdk:${SHA}${DIRTY_SUFFIX}"
  echo "[release] tagging + pushing $REMOTE_TAG"
  docker tag "$LOCAL_TAG" "$REMOTE_TAG"
  docker push "$REMOTE_TAG"
  echo "[release] pushed $REMOTE_TAG"
else
  echo "[release] RELEASE_PUSH unset; skipping registry push (docker + daytona use local artifacts)"
fi

echo "[release] done. Commit .runtime-image-tag (and .runtime-snapshot-tag if present)."
