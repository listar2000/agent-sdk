#!/usr/bin/env bash
# Build the agent-sdk runtime artifacts and pin tags so providers default
# to known-good runtimes without per-environment env-var setup.
#
# Three artifacts, each independently optional:
#
#   1. Local Docker image (``agent-sdk:<git-sha>``) — consumed by the
#      docker provider directly from the local daemon. Built only when
#      ``docker`` is on PATH; silently skipped otherwise so daytona/modal
#      developers without Docker can still ship snapshots.
#
#   2. Daytona snapshot (``agent-sdk-<git-sha>``) — built from
#      ``./Dockerfile`` via ``Image.from_dockerfile`` (Daytona's remote
#      builder). Requires ``DAYTONA_API_KEY``.
#
#   3. Modal filesystem snapshot — spawn a warm sandbox from the
#      Dockerfile then call ``Sandbox.snapshot_filesystem()``. Requires a
#      configured Modal profile (``~/.modal.toml``) and the ``modal``
#      Python SDK.
#
# Each artifact's tag is committed to the repo so docker / daytona /
# modal providers auto-resolve the right runtime without env vars.
#
# Usage:
#
#   scripts/release.sh                          # build whichever providers are reachable
#   scripts/release.sh --provider daytona       # only build the Daytona snapshot
#   scripts/release.sh --provider modal         # only build the Modal snapshot
#   scripts/release.sh --provider docker        # only build the local Docker image
#   scripts/release.sh --provider all           # explicit "all" (same as no flag)
#
#   RELEASE_PUSH=1 scripts/release.sh           # also push docker image to registry
#   RELEASE_ALLOW_DIRTY=1 scripts/release.sh    # build with uncommitted changes
#   AGENT_SDK_REGISTRY=ghcr.io/myorg scripts/release.sh   # override registry

set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "${SCRIPT_DIR}/.." && pwd)"
cd "$REPO_ROOT"

# ── Args ────────────────────────────────────────────────────────────────
PROVIDER_FILTER="all"
while [[ $# -gt 0 ]]; do
  case "$1" in
    --provider)
      PROVIDER_FILTER="${2:-}"
      shift 2
      ;;
    --provider=*)
      PROVIDER_FILTER="${1#--provider=}"
      shift
      ;;
    -h|--help)
      sed -n '2,30p' "$0" | sed 's/^# \?//'
      exit 0
      ;;
    *)
      echo "release.sh: unknown arg $1 (use --provider docker|daytona|modal|all)" >&2
      exit 2
      ;;
  esac
done

case "${PROVIDER_FILTER}" in
  all|docker|daytona|modal) ;;
  *)
    echo "release.sh: --provider must be one of: all, docker, daytona, modal (got ${PROVIDER_FILTER!r})" >&2
    exit 2
    ;;
esac

REGISTRY="${AGENT_SDK_REGISTRY:-ghcr.io/rllm-org}"
SHA="$(git rev-parse --short HEAD)"

# Refuse dirty trees by default — the SHA wouldn't reflect the artifact's
# contents. RELEASE_ALLOW_DIRTY=1 opts in (useful during iteration);
# dirty builds get a timestamp suffix so they're visibly distinct from
# committed ones.
DIRTY_SUFFIX=""
if ! git diff --quiet --ignore-submodules HEAD; then
  if [[ "${RELEASE_ALLOW_DIRTY:-0}" != "1" ]]; then
    echo "release.sh: refusing to build with uncommitted changes (the SHA tag" >&2
    echo "  would lie about what's in the artifact). Commit or stash first," >&2
    echo "  or set RELEASE_ALLOW_DIRTY=1." >&2
    exit 1
  fi
  DIRTY_SUFFIX="-dirty-$(date +%s)"
fi

LOCAL_TAG="agent-sdk:${SHA}${DIRTY_SUFFIX}"
SNAPSHOT_NAME="agent-sdk-${SHA}${DIRTY_SUFFIX}"
DOCKERFILE="${REPO_ROOT}/Dockerfile"
VENV_PYTHON="${REPO_ROOT}/.venv/bin/python"

want() {
  # Single source of truth for "is this provider in scope this run?"
  [[ "${PROVIDER_FILTER}" == "all" || "${PROVIDER_FILTER}" == "$1" ]]
}

# ── 1. Build local docker image ─────────────────────────────────────────
build_docker() {
  if ! command -v docker >/dev/null 2>&1; then
    echo "[release] docker not on PATH — skipping local image build"
    return 0
  fi
  # Defang the WSL-Docker-Desktop credsStore leak: a ``credsStore`` entry
  # pointing at ``desktop.exe`` makes ``docker build`` fail with
  # ``exec: docker-credential-desktop.exe: file not found`` even for
  # public ``docker.io`` images. Use a scoped DOCKER_CONFIG so we don't
  # mutate the user's real config.
  local docker_config_dir=""
  if [[ -f "${HOME}/.docker/config.json" ]] \
     && grep -q '"credsStore"[[:space:]]*:[[:space:]]*"desktop' "${HOME}/.docker/config.json"; then
    docker_config_dir="$(mktemp -d)"
    echo '{}' > "${docker_config_dir}/config.json"
    echo "[release] detected Docker Desktop credsStore in ~/.docker/config.json;"
    echo "[release]   using throwaway DOCKER_CONFIG=${docker_config_dir} for this build"
    export DOCKER_CONFIG="${docker_config_dir}"
  fi

  echo "[release] building $LOCAL_TAG"
  # Don't take the whole script down if docker build fails — daytona/modal
  # snapshots build remotely and don't need the local image.
  if ! docker build -t "$LOCAL_TAG" .; then
    echo "[release] docker build failed — continuing without a local image" >&2
    [[ -n "$docker_config_dir" ]] && rm -rf "$docker_config_dir"
    return 0
  fi
  echo "$LOCAL_TAG" > "$REPO_ROOT/.runtime-image-tag"
  echo "[release] wrote .runtime-image-tag := $LOCAL_TAG"

  # Optional: push to registry. Daytona/modal don't need a registry —
  # only multi-machine docker-provider setups do.
  if [[ "${RELEASE_PUSH:-0}" == "1" ]]; then
    REMOTE_TAG="${REGISTRY}/agent-sdk:${SHA}${DIRTY_SUFFIX}"
    echo "[release] tagging + pushing $REMOTE_TAG"
    docker tag "$LOCAL_TAG" "$REMOTE_TAG"
    docker push "$REMOTE_TAG"
    echo "[release] pushed $REMOTE_TAG"
  else
    echo "[release] RELEASE_PUSH unset; skipping registry push"
  fi

  [[ -n "$docker_config_dir" ]] && rm -rf "$docker_config_dir"
}

# ── 2. Build / register Daytona snapshot ────────────────────────────────
build_daytona() {
  if [[ -z "${DAYTONA_API_KEY:-}" ]]; then
    echo "[release] DAYTONA_API_KEY unset — skipping Daytona snapshot"
    return 0
  fi
  if [[ ! -x "${VENV_PYTHON}" ]]; then
    echo "[release] ${VENV_PYTHON} missing — run scripts/launch_server_test.sh once to bootstrap, then re-run." >&2
    return 1
  fi
  echo "[release] registering Daytona snapshot $SNAPSHOT_NAME (remote build, ~5 min)"
  SNAPSHOT_NAME="$SNAPSHOT_NAME" DOCKERFILE="$DOCKERFILE" "${VENV_PYTHON}" - <<'PYEOF'
import os, sys, time
from daytona_sdk import Daytona, DaytonaConfig, CreateSnapshotParams, Image, Resources

snapshot_name = os.environ["SNAPSHOT_NAME"]
dockerfile = os.environ["DOCKERFILE"]
client = Daytona(DaytonaConfig(api_key=os.environ["DAYTONA_API_KEY"]))
t0 = time.time()
# Bake a small per-sandbox footprint into the snapshot. Daytona snapshots
# lock resources at creation time; CreateSandboxFromSnapshotParams has no
# ``resources`` field, so passing per-session resources would force the
# slower image-path fallback (which needs DAYTONA_IMAGE on the registry).
# 1 vCPU / 1 GiB RAM / 3 GiB disk is plenty for a claude session, and
# leaves ~500 concurrent grader sandboxes inside a 500 GiB account quota.
result = client.snapshot.create(
    CreateSnapshotParams(
        name=snapshot_name,
        image=Image.from_dockerfile(dockerfile),
        resources=Resources(cpu=1, memory=1, disk=3),
    ),
)
print(f"[release] daytona snapshot {result.name} state={result.state} elapsed={time.time() - t0:.1f}s",
      file=sys.stderr)
PYEOF
  echo "$SNAPSHOT_NAME" > "$REPO_ROOT/.runtime-snapshot-tag"
  echo "[release] wrote .runtime-snapshot-tag := $SNAPSHOT_NAME"
}

# ── 3. Build / register Modal filesystem snapshot ───────────────────────
build_modal() {
  if [[ ! -f "${HOME}/.modal.toml" ]]; then
    echo "[release] ~/.modal.toml not found — skipping Modal snapshot (run 'modal setup' to enable)"
    return 0
  fi
  if [[ ! -x "${VENV_PYTHON}" ]]; then
    echo "[release] ${VENV_PYTHON} missing — run scripts/launch_server_test.sh once to bootstrap, then re-run." >&2
    return 1
  fi
  if ! "${VENV_PYTHON}" -c 'import modal' >/dev/null 2>&1; then
    echo "[release] modal SDK not installed in venv — skipping Modal snapshot"
    return 0
  fi
  echo "[release] building Modal filesystem snapshot (warm sandbox + snapshot_filesystem, ~3-5 min)"
  DOCKERFILE="$DOCKERFILE" "${VENV_PYTHON}" - <<'PYEOF'
import os
import sys
import time

import modal

APP_NAME = "agent-sdk-snapshot-builder"
TAG_FILE_NAME = ".modal-snapshot-tag"

repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__))) \
    if "__file__" in dir() else os.environ.get("REPO_ROOT", os.getcwd())
# When run via heredoc there's no __file__; fall back to CWD (release.sh
# already cd'd to REPO_ROOT before invoking this).
repo_root = os.getcwd()
dockerfile = os.environ["DOCKERFILE"]
tag_file = os.path.join(repo_root, TAG_FILE_NAME)

print(f"modal SDK: {modal.__version__}")
print(f"using Dockerfile: {dockerfile}")

app = modal.App.lookup(APP_NAME, create_if_missing=True)
image = modal.Image.from_dockerfile(dockerfile)

print("\n[1/3] spawning warm sandbox (triggers the remote image build "
      "if not already cached on Modal)...")
t0 = time.perf_counter()
sb = modal.Sandbox.create(
    "sleep", "120",
    app=app,
    image=image,
    timeout=600,
)
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
PYEOF
}

# ── Dispatch ────────────────────────────────────────────────────────────
ran_any=0
if want docker;  then build_docker;  ran_any=1; fi
if want daytona; then build_daytona; ran_any=1; fi
if want modal;   then build_modal;   ran_any=1; fi

if [[ $ran_any -eq 0 ]]; then
  echo "release.sh: no provider matched --provider=${PROVIDER_FILTER}" >&2
  exit 2
fi

echo "[release] done. Commit any updated tag files (.runtime-image-tag, .runtime-snapshot-tag, .modal-snapshot-tag)."
