#!/usr/bin/env bash
# Launch the agent-sdk API server with a project-local Postgres (no Docker).
#
# Postgres is installed via conda into .postgres-env/ and its data dir lives at
# .postgres-data/. Both are gitignored. Port 5433 is used to match the Docker
# variant, so DATABASE_URL is compatible.
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "${SCRIPT_DIR}/.." && pwd)"
VENV_DIR="${REPO_ROOT}/.venv"
VENV_PYTHON="${VENV_DIR}/bin/python"
PG_ENV_DIR="${REPO_ROOT}/.postgres-env"
PG_DATA_DIR="${REPO_ROOT}/.postgres-data"
PG_LOG="${PG_DATA_DIR}/server.log"
PG_PORT=5433
PG_DB=agent_sdk_server

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
# fall back to the user's ~/.env — this matches `set -a; source .env` that
# users run manually before hitting the examples.
for env_file in "${REPO_ROOT}/.env" "${HOME}/.env"; do
    if [ -f "${env_file}" ]; then
        set -a
        # shellcheck disable=SC1090
        source "${env_file}"
        set +a
    fi
done

# ── Python venv ─────────────────────────────────────────────────────────────
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

# ── Conda-managed Postgres (project-local, no sudo) ─────────────────────────
CONDA_BIN="$(command -v conda || true)"
if [ -z "${CONDA_BIN}" ] && [ -x "${HOME}/miniconda3/bin/conda" ]; then
    CONDA_BIN="${HOME}/miniconda3/bin/conda"
fi
if [ -z "${CONDA_BIN}" ]; then
    echo "ERROR: conda not found. Install miniconda/anaconda or use scripts/launch_server_docker.sh instead."
    exit 1
fi

if [ ! -x "${PG_ENV_DIR}/bin/postgres" ]; then
    echo "Installing Postgres into ${PG_ENV_DIR} (one-time, ~2–3 min)..."
    "${CONDA_BIN}" create -y -p "${PG_ENV_DIR}" -c conda-forge postgresql >/dev/null
fi

PG_BIN="${PG_ENV_DIR}/bin"
export PATH="${PG_BIN}:${PATH}"

if [ ! -s "${PG_DATA_DIR}/PG_VERSION" ]; then
    echo "Initializing Postgres data dir at ${PG_DATA_DIR} ..."
    mkdir -p "${PG_DATA_DIR}"
    "${PG_BIN}/initdb" -D "${PG_DATA_DIR}" -U postgres --auth-host=trust --auth-local=trust >/dev/null
fi

# Start postgres if not already listening on PG_PORT
if ! "${PG_BIN}/pg_isready" -h localhost -p "${PG_PORT}" -q 2>/dev/null; then
    echo "Starting Postgres on port ${PG_PORT} ..."
    "${PG_BIN}/pg_ctl" -D "${PG_DATA_DIR}" -l "${PG_LOG}" \
        -o "-p ${PG_PORT} -k ${PG_DATA_DIR}" start >/dev/null
    for _ in $(seq 1 30); do
        "${PG_BIN}/pg_isready" -h localhost -p "${PG_PORT}" -q && break
        sleep 0.5
    done
fi

if ! "${PG_BIN}/pg_isready" -h localhost -p "${PG_PORT}" -q; then
    echo "ERROR: Postgres failed to start. See ${PG_LOG}."
    exit 1
fi
echo "Postgres is ready on port ${PG_PORT}."

# Create the database if it doesn't exist
if ! "${PG_BIN}/psql" -h localhost -p "${PG_PORT}" -U postgres -tAc \
        "SELECT 1 FROM pg_database WHERE datname='${PG_DB}'" | grep -q 1; then
    echo "Creating database ${PG_DB} ..."
    "${PG_BIN}/createdb" -h localhost -p "${PG_PORT}" -U postgres "${PG_DB}"
fi

# ── Kill stale server on 7778 ───────────────────────────────────────────────
PIDS=()
while IFS= read -r pid; do
    PIDS+=("${pid}")
done < <(lsof -ti :7778 2>/dev/null || true)

if [ "${#PIDS[@]}" -gt 0 ]; then
    echo "Killing existing process on port 7778 (PID(s) ${PIDS[*]})..."
    kill "${PIDS[@]}" 2>/dev/null || true
    sleep 1
fi

export DATABASE_URL="postgresql://postgres@localhost:${PG_PORT}/${PG_DB}"

echo "Starting local server on http://localhost:7778 ..."
exec "${VENV_PYTHON}" -m uvicorn src.api.server:app --host 0.0.0.0 --port 7778
