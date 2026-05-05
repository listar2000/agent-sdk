#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "${SCRIPT_DIR}/.." && pwd)"
VENV_DIR="${REPO_ROOT}/.venv"
VENV_PYTHON="${VENV_DIR}/bin/python"

# Default to the test origin so forgotten flags can't pollute production.
# Production deploys (Railway, etc.) set AGENT_SDK_ORIGIN=production explicitly
# and don't go through this script. Override with
# ``AGENT_SDK_ORIGIN=production scripts/launch_server_docker.sh`` if you really
# need a production-tagged server locally.
export AGENT_SDK_ORIGIN="${AGENT_SDK_ORIGIN:-test}"

if command -v python3 >/dev/null 2>&1; then
    SYSTEM_PYTHON="python3"
elif command -v python >/dev/null 2>&1; then
    SYSTEM_PYTHON="python"
else
    echo "ERROR: Python 3.11+ is required but no python executable was found."
    exit 1
fi

if ! "${SYSTEM_PYTHON}" -c 'import sys; raise SystemExit(0 if sys.version_info >= (3, 11) else 1)'; then
    echo "ERROR: Python 3.11+ is required to run this project."
    exit 1
fi

cd "${REPO_ROOT}"

# Load env vars (API keys, SSL cert, etc.). Prefer the repo-local .env,
# fall back to the user's ~/.env.
for env_file in "${REPO_ROOT}/.env" "${HOME}/.env"; do
    if [ -f "${env_file}" ]; then
        set -a
        # shellcheck disable=SC1090
        source "${env_file}"
        set +a
    fi
done

# Re-assert the test default in case .env unset it.
export AGENT_SDK_ORIGIN="${AGENT_SDK_ORIGIN:-test}"
echo "AGENT_SDK_ORIGIN=${AGENT_SDK_ORIGIN}"

needs_install=0
recreate_venv=0
if [ ! -x "${VENV_PYTHON}" ]; then
    recreate_venv=1
    needs_install=1
elif ! "${VENV_PYTHON}" -c 'import sys; raise SystemExit(0 if sys.version_info >= (3, 11) else 1)'; then
    echo "Rebuilding ${VENV_DIR} with Python 3.11+ ..."
    recreate_venv=1
    needs_install=1
elif ! "${VENV_PYTHON}" -c 'import fastapi, uvicorn, psycopg, psycopg_pool, daytona_sdk' >/dev/null 2>&1; then
    needs_install=1
fi

if [ "${recreate_venv}" -eq 1 ]; then
    echo "Creating virtualenv at ${VENV_DIR} ..."
    if [ -d "${VENV_DIR}" ]; then
        "${SYSTEM_PYTHON}" -m venv --clear "${VENV_DIR}"
    else
        "${SYSTEM_PYTHON}" -m venv "${VENV_DIR}"
    fi
fi

if [ "${needs_install}" -eq 1 ]; then
    echo "Installing server dependencies into ${VENV_DIR} ..."
    "${VENV_PYTHON}" -m pip install --upgrade pip
    "${VENV_PYTHON}" -m pip install -e "${REPO_ROOT}"
fi

# Ensure Postgres is running via Docker
if ! docker compose ps db 2>/dev/null | grep -q "running"; then
    echo "Starting Postgres..."
    docker compose up -d db
    echo "Waiting for Postgres to be healthy..."
    until docker compose ps db 2>/dev/null | grep -q "healthy"; do
        sleep 1
    done
fi
echo "Postgres is ready on port 5433."

# Stop the Docker server if it's running (to avoid port conflicts)
if docker compose ps server 2>/dev/null | grep -q "running"; then
    echo "Stopping Docker server to free port 7778..."
    docker compose stop server
fi

# Kill any existing process on 7778
PIDS=()
while IFS= read -r pid; do
    PIDS+=("${pid}")
done < <(lsof -ti :7778 2>/dev/null || true)

if [ "${#PIDS[@]}" -gt 0 ]; then
    echo "Killing existing process on port 7778 (PID(s) ${PIDS[*]})..."
    kill "${PIDS[@]}" 2>/dev/null || true
    sleep 1
fi

export DATABASE_URL=postgresql://postgres:postgres@localhost:5433/agent_sdk_server

# Local provider's source-tree runtime path. See
# scripts/launch_server_test.sh for the full rationale.
if command -v npm >/dev/null 2>&1; then
  echo "Ensuring src/supervisor npm deps are installed..."
  # Don't pass --omit=optional. opencode-ai's postinstall requires the
  # platform-specific opencode-linux-x64 (an optional dep) and fails with
  # "Cannot find module 'opencode-linux-x64/package.json'" otherwise.
  (cd "${REPO_ROOT}/src/supervisor" && npm install --silent) || true
fi

# Daytona / docker / modal providers auto-resolve from .runtime-image-tag
# and .runtime-snapshot-tag (committed by scripts/release.sh). Print them
# for visibility; if missing, those providers will fail with a clear
# "run scripts/release.sh" error.
if [[ -f "${REPO_ROOT}/.runtime-image-tag" ]]; then
  echo "Runtime image: $(cat "${REPO_ROOT}/.runtime-image-tag")"
fi
if [[ -f "${REPO_ROOT}/.runtime-snapshot-tag" ]]; then
  echo "Daytona snapshot: $(cat "${REPO_ROOT}/.runtime-snapshot-tag")"
fi

echo "Starting local server on http://localhost:7778 ..."
exec "${VENV_PYTHON}" -m uvicorn src.api.server:app --host 0.0.0.0 --port 7778
