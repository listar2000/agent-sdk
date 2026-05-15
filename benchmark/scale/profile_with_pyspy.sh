#!/usr/bin/env bash
# Run profile_multireplica.py in each deploy mode under py-spy, capturing
# flame graphs per scenario. The bench sleeps 5s after the server is up
# so py-spy attaches before traffic hits.
#
# Output:
#   benchmark/scale/flame-workers4.svg
#   benchmark/scale/flame-lb-replicas.svg
#
# py-spy needs sudo on Linux. Adjust SUDO= if you've configured a
# password-less sudo for this user.

set -euo pipefail
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd -- "${SCRIPT_DIR}/../.." && pwd)"
VENV_PY="${REPO}/.venv/bin/python"
PY_SPY="${REPO}/.venv/bin/py-spy"
DB="${DATABASE_URL:-postgresql://postgres@localhost:5433/agent_sdk_test_scale}"
SUDO="${SUDO:-sudo -n}"
DURATION="${DURATION:-30}"
N_SESSIONS="${N_SESSIONS:-16}"

[[ -f "${HOME}/.env" ]] && { set -a; source "${HOME}/.env"; set +a; }

_wipe_db() {
  PGPASSWORD=postgres psql -h localhost -p 5433 -U postgres \
    -d agent_sdk_test_scale -c \
    "DELETE FROM session_log; DELETE FROM sessions; DELETE FROM agents; DELETE FROM volumes;" \
    >/dev/null 2>&1
}

_run() {
  local mode="$1" svg="$2"
  echo "=== profiling mode=${mode} → ${svg} ==="
  _wipe_db
  # Run the bench in the background; py-spy attaches mid-run.
  DEPLOY_MODE="${mode}" DATABASE_URL="${DB}" \
    N_REPLICAS=4 WORKERS_PER_REPLICA=1 N_SESSIONS="${N_SESSIONS}" \
    "${VENV_PY}" "${REPO}/benchmark/scale/profile_multireplica.py" \
    > "${REPO}/logs/profile-${mode}.log" 2>&1 &
  local bench_pid=$!

  # Wait until the bench has launched its replicas and started prompts.
  # We tail the bench log for "driving" then attach py-spy to ALL python
  # children of bench_pid (uvicorn replicas + LB).
  local deadline=$(( $(date +%s) + 60 ))
  while (( $(date +%s) < deadline )); do
    if grep -q "driving" "${REPO}/logs/profile-${mode}.log" 2>/dev/null; then
      break
    fi
    sleep 0.5
  done

  # Find all python children of bench_pid (uvicorn replicas + LB).
  local pids
  pids=$(pgrep -P "${bench_pid}" -d ' ' 2>/dev/null || true)
  if [[ -z "${pids}" ]]; then
    echo "    no children found; aborting py-spy attach"
  else
    # py-spy can only attach to one PID at a time; record the busiest one
    # (heuristic: first child, which is replica r0). Use --subprocesses
    # so its workers are also sampled if any.
    local first
    first=$(echo "${pids}" | awk '{print $1}')
    echo "    attaching py-spy to pid=${first} for ${DURATION}s"
    ${SUDO} "${PY_SPY}" record -o "${svg}" -d "${DURATION}" \
      --subprocesses -p "${first}" || echo "    py-spy failed"
  fi

  # Wait for the bench to finish.
  wait "${bench_pid}" || true
  echo "    bench results:"
  grep -E "wall_s|chars_per_sec|done_p95|redirects_total|intra_replica|log_rows_total|ok " \
    "${REPO}/logs/profile-${mode}.log" || true
}

_run workers "${REPO}/benchmark/scale/flame-workers4.svg"
_run lb      "${REPO}/benchmark/scale/flame-lb-replicas.svg"
echo "done. flame graphs:"
ls -la "${REPO}/benchmark/scale/"*.svg 2>/dev/null
