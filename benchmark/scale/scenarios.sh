#!/usr/bin/env bash
# Scale benchmark orchestrator.
#
# Runs the scale driver against the local server in a sequence of scenarios
# so the Wave-1/2/3 contributions can be compared back-to-back without
# restarting the server between waves (Wave 1 effects don't need a restart;
# Wave 2 changes worker count which DOES require a restart).
#
# Assumes:
#   - Postgres is up (see scripts/launch_server_test.sh)
#   - ~/.env carries CLAUDE_CODE_OAUTH_TOKEN (and DAYTONA_API_KEY/MODAL_TOKEN_*
#     if you use --provider daytona/modal)
#
# Usage:
#   benchmark/scale/scenarios.sh                       # full sweep (unix_local)
#   benchmark/scale/scenarios.sh --provider daytona    # full sweep on daytona
#   benchmark/scale/scenarios.sh --n 32 --turns 1      # custom load
#   benchmark/scale/scenarios.sh --only baseline       # one scenario

set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "${SCRIPT_DIR}/../.." && pwd)"
VENV_PYTHON="${REPO_ROOT}/.venv/bin/python"

PROVIDER="${PROVIDER:-unix_local}"
N_SESSIONS="${N_SESSIONS:-16}"
N_TURNS="${N_TURNS:-1}"
WARMUP="${WARMUP:-2}"
SUITE="${SUITE:-all}"   # one of: all | baseline | wave1 | wave1+2 | wave1+2+3
RESULT_PATH="${RESULT_PATH:-${REPO_ROOT}/benchmark/scale/results.jsonl}"
# Bench port — default 7779 to avoid clobbering a dev server on 7778.
BENCH_PORT="${BENCH_PORT:-7779}"

# Default scenarios. Each is a (label, server-env, server-disable-flags) tuple
# encoded by the case statement below.
declare -a SCENARIOS=(baseline wave1 wave1_w4 wave1_w4_lease)

# Argument parsing (light — env vars are the primary knob).
while [[ $# -gt 0 ]]; do
  case "$1" in
    --provider) PROVIDER="$2"; shift 2 ;;
    --n)        N_SESSIONS="$2"; shift 2 ;;
    --turns)    N_TURNS="$2"; shift 2 ;;
    --only)     SCENARIOS=("$2"); shift 2 ;;
    --results)  RESULT_PATH="$2"; shift 2 ;;
    *) echo "unknown arg: $1" >&2; exit 2 ;;
  esac
done

# Load creds from ~/.env (for daytona / modal / claude oauth token).
if [[ -f "${HOME}/.env" ]]; then
  set -a
  # shellcheck disable=SC1090
  source "${HOME}/.env"
  set +a
fi

# Source the repo .env too if present (overrides home, mirroring launch_server_test.sh).
if [[ -f "${REPO_ROOT}/.env" ]]; then
  set -a
  # shellcheck disable=SC1090
  source "${REPO_ROOT}/.env"
  set +a
fi

export AGENT_SDK_ORIGIN="${AGENT_SDK_ORIGIN:-test}"

# Default to the project-local conda Postgres that launch_server_test.sh
# manages (port 5433, no password). Caller may override DATABASE_URL.
export DATABASE_URL="${DATABASE_URL:-postgresql://postgres@localhost:5433/agent_sdk_server}"

# Server-launch helper. Each scenario can pass extra env to control wave
# enablement: AGENT_SDK_SUPERVISOR_FLUSH_MS=0 disables Wave 1a coalescing
# (legacy 1-event-per-chunk behaviour), AGENT_SDK_LOG_FLUSH_MS=0 disables
# Wave 1b batching by making the batcher fire on every add, and
# AGENT_SDK_DISABLE_LEASE=1 disables Wave 3 (the pool skips the claim).
SERVER_LOG="${REPO_ROOT}/logs/scale-server.log"
mkdir -p "${REPO_ROOT}/logs"

_start_server() {
  local workers="$1"
  shift
  # extra env=value args follow
  local pid_file="${REPO_ROOT}/logs/scale-server.pid"
  _stop_server || true
  echo "--- starting server (workers=${workers}; extra: $*) ---"
  (
    cd "${REPO_ROOT}"
    PYTHONPATH="${REPO_ROOT}/src${PYTHONPATH:+:${PYTHONPATH}}" \
    DATABASE_URL="${DATABASE_URL}" \
    AGENT_SDK_ORIGIN="${AGENT_SDK_ORIGIN}" \
    AGENT_SDK_WORKERS="${workers}" \
    AGENT_SDK_PORT="${BENCH_PORT}" \
      "$@" \
      "${VENV_PYTHON}" -m uvicorn api.server:app --host 0.0.0.0 --port ${BENCH_PORT} \
        --workers "${workers}" \
        > "${SERVER_LOG}" 2>&1 &
    echo $! > "${pid_file}"
  )
  # Wait for /health
  local deadline=$(( $(date +%s) + 60 ))
  while (( $(date +%s) < deadline )); do
    if curl -fsS "http://localhost:${BENCH_PORT}/health" >/dev/null 2>&1; then
      echo "    server up at :${BENCH_PORT}"
      return 0
    fi
    sleep 0.5
  done
  echo "server failed to come up; tail log:"
  tail -n 40 "${SERVER_LOG}"
  return 1
}

_stop_server() {
  local pid_file="${REPO_ROOT}/logs/scale-server.pid"
  if [[ -f "${pid_file}" ]]; then
    local pid
    pid=$(cat "${pid_file}")
    if kill -0 "${pid}" 2>/dev/null; then
      kill -TERM "${pid}" 2>/dev/null || true
      # uvicorn graceful shutdown can take a few seconds; poll then SIGKILL.
      for _ in 1 2 3 4 5 6 7 8 9 10; do
        kill -0 "${pid}" 2>/dev/null || break
        sleep 0.5
      done
      kill -KILL "${pid}" 2>/dev/null || true
    fi
    rm -f "${pid_file}"
  fi
  # Belt-and-braces — orphan uvicorn workers from a prior run on OUR
  # bench port. Scoped to ``--port ${BENCH_PORT}`` so a dev server bound
  # to a different port (e.g. 7778) survives.
  pkill -f "uvicorn api.server:app .*--port ${BENCH_PORT}" 2>/dev/null || true
}

_run_driver() {
  local scenario_label="$1"
  echo "--- driver: ${scenario_label} (PROVIDER=${PROVIDER} N=${N_SESSIONS} TURNS=${N_TURNS}) ---"
  PYTHONPATH="${REPO_ROOT}/src${PYTHONPATH:+:${PYTHONPATH}}" \
    API="http://localhost:${BENCH_PORT}" \
    AGENT_SDK_PORT="${BENCH_PORT}" \
    PROVIDER="${PROVIDER}" \
    AGENT_TYPE="${AGENT_TYPE:-claude}" \
    MODEL="${MODEL:-haiku}" \
    N_SESSIONS="${N_SESSIONS}" \
    N_TURNS="${N_TURNS}" \
    WARMUP="${WARMUP}" \
    SCENARIO="${scenario_label}" \
    TAG="${TAG:-}" \
    RESULT_PATH="${RESULT_PATH}" \
    "${VENV_PYTHON}" "${SCRIPT_DIR}/driver.py"
}

trap _stop_server EXIT

for scenario in "${SCENARIOS[@]}"; do
  case "${scenario}" in
    baseline)
      # Wave 1 disabled: supervisor flush=0 (immediate), batcher flush=0,
      # lease disabled. Single uvicorn worker.
      _start_server 1 env \
        AGENT_SDK_SUPERVISOR_FLUSH_MS=0 \
        AGENT_SDK_LOG_FLUSH_MS=0 \
        AGENT_SDK_DISABLE_LEASE=1
      _run_driver "baseline"
      ;;
    wave1)
      # Wave 1 ON, single worker, no lease.
      _start_server 1 env \
        AGENT_SDK_SUPERVISOR_FLUSH_MS=40 \
        AGENT_SDK_LOG_FLUSH_MS=100 \
        AGENT_SDK_DISABLE_LEASE=1
      _run_driver "wave1"
      ;;
    wave1_w4)
      # Wave 1 + 4 workers, lease still off (sequential bench, multi-worker).
      _start_server 4 env \
        AGENT_SDK_SUPERVISOR_FLUSH_MS=40 \
        AGENT_SDK_LOG_FLUSH_MS=100 \
        AGENT_SDK_DISABLE_LEASE=1
      _run_driver "wave1_w4"
      ;;
    wave1_w4_lease)
      # Wave 1 + 4 workers + lease ON. Tests the 307 redirect path under
      # concurrent prompts; for a same-host bench every worker shares the
      # same private addr (127.0.0.1:7778) so 307 round-trips through the
      # kernel SO_REUSEPORT pool — useful as a smoke test, not as a
      # perf proxy for a true multi-replica deploy.
      _start_server 4 env \
        AGENT_SDK_SUPERVISOR_FLUSH_MS=40 \
        AGENT_SDK_LOG_FLUSH_MS=100
      _run_driver "wave1_w4_lease"
      ;;
    *)
      echo "unknown scenario: ${scenario}" >&2
      exit 2
      ;;
  esac
done

echo "=== done; results in ${RESULT_PATH} ==="
