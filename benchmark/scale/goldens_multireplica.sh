#!/usr/bin/env bash
# Run the recovery goldens against a 4-replica + LB stack.
#
# Validates that the Wave-3 lease + 307 + Wave-1 coalescing/batching
# don't break the recovery semantics that
# ``tests/test_golden.py`` pins. The SDK is
# pointed at the LB (a single URL) so its existing test machinery
# doesn't need to know about the multi-replica topology.
#
# Args:
#   --providers     comma-separated list (default: unix_local; add
#                   daytona,modal if creds + runtime snapshot tags
#                   are in place)
#   --replicas      number of replicas (default 4)
#
# Env:
#   DATABASE_URL    (defaults to project test DB)
#   CLAUDE_CODE_OAUTH_TOKEN  required for claude tests
#   DAYTONA_API_KEY          required for daytona
#   MODAL_TOKEN_ID/SECRET    required for modal

set -euo pipefail
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd -- "${SCRIPT_DIR}/../.." && pwd)"
VENV_PY="${REPO}/.venv/bin/python"
DB="${DATABASE_URL:-postgresql://postgres@localhost:5433/agent_sdk_test_scale}"

[[ -f "${HOME}/.env" ]] && { set -a; source "${HOME}/.env"; set +a; }
[[ -f "${REPO}/.env" ]] && { set -a; source "${REPO}/.env"; set +a; }

PROVIDERS="unix_local"
N_REPLICAS=4
while [[ $# -gt 0 ]]; do
  case "$1" in
    --providers) PROVIDERS="$2"; shift 2 ;;
    --replicas)  N_REPLICAS="$2"; shift 2 ;;
    *) echo "unknown arg: $1" >&2; exit 2 ;;
  esac
done

PORT_BASE=7791
LB_PORT=7790
mkdir -p "${REPO}/logs"

declare -a REPLICA_PIDS
LB_PID=""

cleanup() {
  if [[ -n "${LB_PID}" ]] && kill -0 "${LB_PID}" 2>/dev/null; then
    kill -TERM "${LB_PID}" 2>/dev/null || true
    sleep 0.3
    kill -KILL "${LB_PID}" 2>/dev/null || true
  fi
  for pid in "${REPLICA_PIDS[@]:-}"; do
    if [[ -n "${pid}" ]] && kill -0 "${pid}" 2>/dev/null; then
      kill -TERM "${pid}" 2>/dev/null || true
    fi
  done
  sleep 0.5
  for pid in "${REPLICA_PIDS[@]:-}"; do
    if [[ -n "${pid}" ]] && kill -0 "${pid}" 2>/dev/null; then
      kill -KILL "${pid}" 2>/dev/null || true
    fi
  done
}
trap cleanup EXIT

# Reset test DB once.
PGPASSWORD=postgres psql -h localhost -p 5433 -U postgres -d agent_sdk_test_scale \
  -c "DELETE FROM session_log; DELETE FROM sessions; DELETE FROM agents; DELETE FROM volumes;" \
  >/dev/null 2>&1 || true

# Make sure schema is current.
DATABASE_URL="${DB}" PYTHONPATH="${REPO}/src" \
  "${VENV_PY}" -c "from api.db import init_db; init_db(); print('schema ok')"

# Start N replicas.
BACKENDS=""
for i in $(seq 0 $((N_REPLICAS - 1))); do
  port=$((PORT_BASE + i))
  name="r${i}"
  log_path="${REPO}/logs/golden-${name}.log"
  : > "${log_path}"
  (
    cd "${REPO}"
    DATABASE_URL="${DB}" \
    PYTHONPATH="${REPO}/src" \
    AGENT_SDK_REPLICA_ID="${name}" \
    AGENT_SDK_PORT="${port}" \
    AGENT_SDK_INTERNAL_HOST="127.0.0.1" \
    AGENT_SDK_LEASE_TTL_S=30 \
    AGENT_SDK_LEASE_HEARTBEAT_S=10 \
    AGENT_SDK_SUPERVISOR_FLUSH_MS=40 \
    AGENT_SDK_LOG_FLUSH_MS=100 \
    AGENT_SDK_ORIGIN=test \
      "${VENV_PY}" -m uvicorn api.server:app --host 127.0.0.1 --port "${port}" \
        > "${log_path}" 2>&1 &
    echo $!
  ) > "${REPO}/logs/golden-${name}.pid"
  REPLICA_PIDS+=("$(cat "${REPO}/logs/golden-${name}.pid")")
  if [[ -n "${BACKENDS}" ]]; then BACKENDS="${BACKENDS},"; fi
  BACKENDS="${BACKENDS}http://127.0.0.1:${port}"
done

# Start LB.
(
  cd "${REPO}"
  BACKENDS="${BACKENDS}" PORT="${LB_PORT}" \
    PYTHONPATH="${REPO}/src" \
    "${VENV_PY}" "${REPO}/benchmark/scale/lb.py" \
    > "${REPO}/logs/golden-lb.log" 2>&1 &
  echo $!
) > "${REPO}/logs/golden-lb.pid"
LB_PID="$(cat "${REPO}/logs/golden-lb.pid")"

# Wait for everything to be healthy.
echo "waiting for ${N_REPLICAS} replicas + LB ..."
deadline=$(($(date +%s) + 60))
while (( $(date +%s) < deadline )); do
  ok=true
  for p in "${REPLICA_PIDS[@]}"; do
    if ! kill -0 "${p}" 2>/dev/null; then ok=false; break; fi
  done
  if ${ok}; then
    if curl -fsS "http://127.0.0.1:${LB_PORT}/health" >/dev/null 2>&1; then
      echo "  stack up"
      break
    fi
  fi
  sleep 0.5
done
if ! curl -fsS "http://127.0.0.1:${LB_PORT}/health" >/dev/null 2>&1; then
  echo "stack failed to start. last lines from each replica:"
  for i in $(seq 0 $((N_REPLICAS - 1))); do
    echo "--- r${i} ---"
    tail -n 20 "${REPO}/logs/golden-r${i}.log" || true
  done
  echo "--- lb ---"
  tail -n 20 "${REPO}/logs/golden-lb.log" || true
  exit 1
fi

# Run the goldens against the LB.
LB_URL="http://127.0.0.1:${LB_PORT}"
echo ""
echo "=== running goldens against LB at ${LB_URL} ==="
echo "    providers: ${PROVIDERS}"

# Build pytest -k filter from provider list (golden tests are parametrized
# over [daytona|docker|unix_local|modal][claude|opencode]).
provider_filter="$(echo "${PROVIDERS}" | tr ',' '|')"

DATABASE_URL="${DB}" \
TEST_DATABASE_URL="${DB}" \
PYTHONPATH="${REPO}/src" \
AGENT_API_URL="${LB_URL}" \
AGENT_SERVER_URL="${LB_URL}" \
  "${VENV_PY}" -m pytest "${REPO}/tests/test_golden.py" \
    -n auto -k "${provider_filter}" --tb=line 2>&1 | tee "${REPO}/logs/golden-results.log" | tail -40

echo ""
echo "=== goldens done. lease + redirect summary from replicas ==="
for i in $(seq 0 $((N_REPLICAS - 1))); do
  log="${REPO}/logs/golden-r${i}.log"
  hb=$(grep -c "heartbeat" "${log}" 2>/dev/null || echo 0)
  notowner=$(grep -c "NotOwner" "${log}" 2>/dev/null || echo 0)
  echo "  r${i}: NotOwner=${notowner}"
done
