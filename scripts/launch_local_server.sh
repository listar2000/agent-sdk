#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "${SCRIPT_DIR}/.." && pwd)"
VENV_DIR="${REPO_ROOT}/.venv"
VENV_PYTHON="${VENV_DIR}/bin/python"

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

# Load env vars (API keys, SSL cert, etc.)
if [ -f "${HOME}/.env" ]; then
    set -a
    source "${HOME}/.env"
    set +a
fi

# Vertex env vars (CLAUDE_CODE_USE_VERTEX, ANTHROPIC_VERTEX_BASE_URL, etc.)
# are set by the managed claude binary in your shell. This script inherits
# them automatically when run from such a shell. If they're missing and no
# ANTHROPIC_API_KEY is set, warn the user.
if [ -z "${ANTHROPIC_API_KEY:-}" ] && [ -z "${CLAUDE_CODE_USE_VERTEX:-}" ]; then
    echo "WARNING: No ANTHROPIC_API_KEY or CLAUDE_CODE_USE_VERTEX found."
    echo "  Run this script from a terminal managed by the claude binary,"
    echo "  or set ANTHROPIC_API_KEY in ~/.env."
fi

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

echo "Starting local server on http://localhost:7778 ..."
exec "${VENV_PYTHON}" -m uvicorn src.api.server:app --host 0.0.0.0 --port 7778
