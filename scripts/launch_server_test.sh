#!/usr/bin/env bash
# Launch the agent-sdk API server for local dev and the integration / golden
# test suite, with a project-local Postgres (no Docker).
#
# Defaults ``AGENT_SDK_ORIGIN=test`` so every daytona sandbox created during
# the run is labelled ``agent_sdk_origin=test``. ``cleanup_orphans.py`` keys
# off this label, so production sandboxes are never touched. Override with
# ``AGENT_SDK_ORIGIN=production scripts/launch_server_test.sh`` if you really
# need to run a production-tagged server locally.
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

# Default to the test origin so forgotten flags can't pollute production.
# Production deploys (Railway, etc.) set AGENT_SDK_ORIGIN=production explicitly
# and don't go through this script.
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
# fall back to the user's ~/.env — this matches `set -a; source .env` that
# users run manually before hitting the examples.
for env_file in "${HOME}/.env" "${REPO_ROOT}/.env"; do
    if [ -f "${env_file}" ]; then
        set -a
        # shellcheck disable=SC1090
        source "${env_file}"
        set +a
    fi
done

# Re-assert the test default in case .env unset it (set -a only exports
# variables present in the file; an explicit `unset` or empty assignment
# in .env could clobber AGENT_SDK_ORIGIN).
export AGENT_SDK_ORIGIN="${AGENT_SDK_ORIGIN:-test}"
echo "AGENT_SDK_ORIGIN=${AGENT_SDK_ORIGIN}"

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
    "${PG_BIN}/initdb" -D "${PG_DATA_DIR}" -U postgres --auth=trust >/dev/null
fi

# Always enforce trust auth so no password prompts occur
cat > "${PG_DATA_DIR}/pg_hba.conf" << 'EOF'
local   all   all              trust
host    all   all   127.0.0.1/32   trust
host    all   all   ::1/128        trust
EOF

# Start postgres if not already listening on PG_PORT; reload config if already running
if ! "${PG_BIN}/pg_isready" -h localhost -p "${PG_PORT}" -q 2>/dev/null; then
    echo "Starting Postgres on port ${PG_PORT} ..."
    "${PG_BIN}/pg_ctl" -D "${PG_DATA_DIR}" -l "${PG_LOG}" \
        -o "-p ${PG_PORT} -k ${PG_DATA_DIR}" start >/dev/null
    for _ in $(seq 1 30); do
        "${PG_BIN}/pg_isready" -h localhost -p "${PG_PORT}" -q && break
        sleep 0.5
    done
else
    "${PG_BIN}/pg_ctl" -D "${PG_DATA_DIR}" reload >/dev/null 2>&1 || true
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
# Use ``api.server`` (not ``src.api.server``) so other modules that
# do ``from api import db`` see the SAME ``api.db`` module — otherwise
# the server's ``init_pool()`` and ``api.sandbox.db_bindings.get_db()``
# end up with two different ``_pool`` globals.
export PYTHONPATH="${REPO_ROOT}/src${PYTHONPATH:+:${PYTHONPATH}}"

# Local provider's source-tree runtime path is <repo>/src/supervisor —
# providers/_shared.py:_detect_runtime_path falls back to this when
# /opt/agent-sdk/runtime/ isn't present (which is the source-tree case
# when running uvicorn directly, not from inside the agent-sdk image).
# Make sure the npm deps are populated so create_sandbox can resolve
# the ACP bins via package.json#bin. Idempotent (~1-2s warm cache).
if command -v npm >/dev/null 2>&1; then
  echo "Ensuring src/supervisor npm deps are installed (source-tree fallback)..."
  # Don't pass --omit=optional. opencode-ai's postinstall requires the
  # platform-specific opencode-linux-x64 (an optional dep) and fails with
  # "Cannot find module 'opencode-linux-x64/package.json'" otherwise.
  (cd "${REPO_ROOT}/src/supervisor" && npm install --silent) || {
    echo "npm install failed; the local provider's create_sandbox will" >&2
    echo "crash with 'ACP binary missing'. Install Node.js >=18 and rerun." >&2
  }
fi

# Daytona / docker / modal providers auto-resolve the runtime image and
# snapshot from .runtime-image-tag and .runtime-snapshot-tag (committed
# by scripts/release.sh). If you're testing daytona/docker/modal locally
# and haven't run release.sh yet, those providers will fail with a clear
# error pointing at scripts/release.sh.
if [[ -f "${REPO_ROOT}/.runtime-image-tag" ]]; then
  echo "Runtime image: $(cat "${REPO_ROOT}/.runtime-image-tag")"
fi
if [[ -f "${REPO_ROOT}/.runtime-snapshot-tag" ]]; then
  echo "Daytona snapshot: $(cat "${REPO_ROOT}/.runtime-snapshot-tag")"
fi

# Default to 4 replicas + LB: the golden suite is meant to run against the
# multi-replica topology (that's what surfaces the lease / cross-replica
# recovery lock-in bugs). Set AGENT_SDK_REPLICAS=1 for a plain single
# uvicorn when you don't need the LB.
: "${AGENT_SDK_REPLICAS:=4}"
: "${AGENT_SDK_PUBLIC_PORT:=7778}"
: "${AGENT_SDK_BACKEND_PORT_BASE:=7791}"

# uvicorn is always single-worker. Scale via AGENT_SDK_REPLICAS + LB —
# multi-worker SO_REUSEPORT routes requests randomly across workers,
# which defeats the lease's session-locality and pays the 307 tax on
# most requests.

if [[ "${AGENT_SDK_REPLICAS}" -le 1 ]]; then
  # Single-replica path — opt in with AGENT_SDK_REPLICAS=1.
  echo "Starting local server on http://localhost:${AGENT_SDK_PUBLIC_PORT} ..."
  exec "${VENV_PYTHON}" -m uvicorn api.server:app --host 0.0.0.0 \
      --port "${AGENT_SDK_PUBLIC_PORT}"
fi

# Multi-replica + LB path. Spawns N single-worker uvicorn replicas on
# AGENT_SDK_BACKEND_PORT_BASE..(BASE+N-1) and benchmark/scale/lb.py in
# front on AGENT_SDK_PUBLIC_PORT. The LB does consistent-hash routing
# on /sessions/{id}/...; the per-session Postgres lease + 307 redirect
# handles ownership safety so the LB itself can be dumb.
#
# Tear-down: trap forwards SIGINT/SIGTERM to the whole process group
# so Ctrl-C tears down all replicas + the LB.

PIDS=()
_cleanup() {
  for pid in "${PIDS[@]:-}"; do
    [[ -z "${pid}" ]] && continue
    kill -TERM "${pid}" 2>/dev/null || true
  done
  for _ in 1 2 3 4 5 6; do
    sleep 0.5
    local alive=0
    for pid in "${PIDS[@]:-}"; do
      kill -0 "${pid}" 2>/dev/null && alive=1
    done
    [[ "${alive}" -eq 0 ]] && return
  done
  for pid in "${PIDS[@]:-}"; do
    kill -KILL "${pid}" 2>/dev/null || true
  done
}
trap _cleanup EXIT INT TERM

mkdir -p "${REPO_ROOT}/logs"

backends_csv=""
for ((i = 0; i < AGENT_SDK_REPLICAS; i++)); do
  port=$((AGENT_SDK_BACKEND_PORT_BASE + i))
  name="r${i}"
  log_path="${REPO_ROOT}/logs/server-${name}.log"
  : > "${log_path}"
  echo "  launching replica ${name} on :${port} -> ${log_path}"
  AGENT_SDK_REPLICA_ID="${name}" \
    AGENT_SDK_PORT="${port}" \
    AGENT_SDK_INTERNAL_HOST="127.0.0.1" \
    "${VENV_PYTHON}" -m uvicorn api.server:app \
      --host 127.0.0.1 --port "${port}" \
      > "${log_path}" 2>&1 &
  PIDS+=("$!")
  if [[ -n "${backends_csv}" ]]; then backends_csv="${backends_csv},"; fi
  backends_csv="${backends_csv}http://127.0.0.1:${port}"
done

# Wait for every replica to answer /health before bringing the LB up.
for pid in "${PIDS[@]}"; do
  : "${pid}"  # validate
done

deadline=$(( $(date +%s) + 30 ))
while (( $(date +%s) < deadline )); do
  all_up=1
  for ((i = 0; i < AGENT_SDK_REPLICAS; i++)); do
    port=$((AGENT_SDK_BACKEND_PORT_BASE + i))
    curl -fsS "http://127.0.0.1:${port}/health" >/dev/null 2>&1 || { all_up=0; break; }
  done
  [[ "${all_up}" -eq 1 ]] && break
  sleep 0.5
done
if [[ "${all_up}" -ne 1 ]]; then
  echo "ERROR: not all replicas became healthy in 30s; see logs/server-r*.log" >&2
  exit 1
fi

# Start the LB. Default is ``nginx`` (cookie-sticky upstream, same
# routing as ``deploy/nginx/`` on Railway). ``AGENT_SDK_LB=python``
# falls back to ``benchmark/scale/lb.py`` for local dev when you don't
# want to depend on nginx being on PATH.
lb_log="${REPO_ROOT}/logs/server-lb.log"
: > "${lb_log}"
: "${AGENT_SDK_LB:=nginx}"
if [[ "${AGENT_SDK_LB}" == "nginx" ]]; then
  # Auto-install nginx via conda into .nginx-env/ if not on PATH —
  # same brainfree pattern as the Postgres bootstrap above. Skips
  # the install if a system nginx is already available.
  if ! command -v nginx >/dev/null 2>&1; then
    NGINX_ENV_DIR="${REPO_ROOT}/.nginx-env"
    if [[ ! -x "${NGINX_ENV_DIR}/bin/nginx" ]]; then
      echo "Installing nginx into ${NGINX_ENV_DIR} (one-time, ~1 min)..."
      "${CONDA_BIN}" create -y -p "${NGINX_ENV_DIR}" -c conda-forge nginx >/dev/null
    fi
    export PATH="${NGINX_ENV_DIR}/bin:${PATH}"
  fi
  # Render the nginx config dynamically so any N + port base works.
  # Same routing as deploy/nginx/nginx.conf (cookie-failover override +
  # consistent-hash on session_id, with X-Session-Id header for POST).
  nginx_conf="${REPO_ROOT}/logs/server-nginx.conf"
  nginx_pid="${REPO_ROOT}/logs/server-nginx.pid"
  {
    echo "daemon off;"
    echo "worker_processes 2;"
    echo "error_log stderr warn;"
    echo "pid ${nginx_pid};"
    echo "events { worker_connections 4096; }"
    echo "http {"
    echo "  access_log off;"
    echo "  map \$request_uri \$url_sid {"
    echo "    \"~^/sessions/(?<sid>[0-9a-f-]+)\" \$sid;"
    echo "    default \"\";"
    echo "  }"
    echo "  map \$url_sid \$route_key {"
    echo "    \"\"      \$http_x_session_id;"
    echo "    default \$url_sid;"
    echo "  }"
    echo "  map \$cookie_agent_sdk_route \$sticky_backend {"
    for ((i = 0; i < AGENT_SDK_REPLICAS; i++)); do
      port=$((AGENT_SDK_BACKEND_PORT_BASE + i))
      echo "    \"r${i}\"    127.0.0.1:${port};"
    done
    echo "    default \"\";"
    echo "  }"
    echo "  upstream agent_sdk_hash {"
    echo "    hash \$route_key consistent;"
    for ((i = 0; i < AGENT_SDK_REPLICAS; i++)); do
      port=$((AGENT_SDK_BACKEND_PORT_BASE + i))
      echo "    server 127.0.0.1:${port};"
    done
    echo "    keepalive 128;"
    echo "  }"
    echo "  upstream agent_sdk_rr {"
    for ((i = 0; i < AGENT_SDK_REPLICAS; i++)); do
      port=$((AGENT_SDK_BACKEND_PORT_BASE + i))
      echo "    server 127.0.0.1:${port};"
    done
    echo "    keepalive 128;"
    echo "  }"
    echo "  proxy_http_version 1.1;"
    echo "  proxy_set_header   Connection \"\";"
    echo "  proxy_set_header   Host              \$host;"
    echo "  proxy_set_header   X-Real-IP         \$remote_addr;"
    echo "  proxy_set_header   X-Forwarded-For   \$proxy_add_x_forwarded_for;"
    echo "  proxy_set_header   X-Forwarded-Proto \$scheme;"
    echo "  proxy_buffering    off;"
    echo "  proxy_request_buffering off;"
    echo "  proxy_read_timeout 1h;"
    echo "  proxy_send_timeout 1h;"
    echo "  server {"
    echo "    listen ${AGENT_SDK_PUBLIC_PORT};"
    echo "    location ~ ^/sessions/[0-9a-f-]+(/.*)?\$ {"
    echo "      if (\$sticky_backend != \"\") { proxy_pass http://\$sticky_backend; break; }"
    echo "      proxy_pass http://agent_sdk_hash;"
    echo "    }"
    echo "    location = /sessions {"
    echo "      if (\$http_x_session_id != \"\") { proxy_pass http://agent_sdk_hash; break; }"
    echo "      proxy_pass http://agent_sdk_rr;"
    echo "    }"
    echo "    location / { proxy_pass http://agent_sdk_rr; }"
    echo "  }"
    echo "}"
  } > "${nginx_conf}"
  echo "  launching nginx LB on :${AGENT_SDK_PUBLIC_PORT} -> ${backends_csv}"
  echo "  nginx config: ${nginx_conf}"
  nginx -p "${REPO_ROOT}" -c "${nginx_conf}" > "${lb_log}" 2>&1 &
  PIDS+=("$!")
else
  echo "  launching python LB on :${AGENT_SDK_PUBLIC_PORT} -> ${backends_csv}"
  BACKENDS="${backends_csv}" PORT="${AGENT_SDK_PUBLIC_PORT}" \
    "${VENV_PYTHON}" "${REPO_ROOT}/benchmark/scale/lb.py" \
    > "${lb_log}" 2>&1 &
  PIDS+=("$!")
fi

# Wait for LB.
deadline=$(( $(date +%s) + 15 ))
while (( $(date +%s) < deadline )); do
  curl -fsS "http://127.0.0.1:${AGENT_SDK_PUBLIC_PORT}/health" >/dev/null 2>&1 && break
  sleep 0.3
done
if ! curl -fsS "http://127.0.0.1:${AGENT_SDK_PUBLIC_PORT}/health" >/dev/null 2>&1; then
  echo "ERROR: LB did not become ready in 15s; see ${lb_log}" >&2
  exit 1
fi

echo "Stack ready:"
echo "  client URL : http://localhost:${AGENT_SDK_PUBLIC_PORT}"
echo "  replicas   : ${AGENT_SDK_REPLICAS} on ports ${AGENT_SDK_BACKEND_PORT_BASE}..$(( AGENT_SDK_BACKEND_PORT_BASE + AGENT_SDK_REPLICAS - 1 ))"
echo "  LB         : benchmark/scale/lb.py (consistent-hash on session_id)"
echo "  logs       : logs/server-r*.log + logs/server-lb.log"
echo
echo "Press Ctrl-C to tear down."
wait
