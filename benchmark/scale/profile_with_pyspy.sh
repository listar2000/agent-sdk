#!/usr/bin/env bash
# Run profile_multireplica.py (4 replicas + LB) under py-spy, capturing
# a flame graph of the busiest replica during the bench window.
#
# Output:
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

SVG="${REPO}/benchmark/scale/flame-lb-replicas.svg"
echo "=== profiling 4×replicas + LB → ${SVG} ==="
_wipe_db
DATABASE_URL="${DB}" \
  N_REPLICAS=4 N_SESSIONS="${N_SESSIONS}" \
  "${VENV_PY}" "${REPO}/benchmark/scale/profile_multireplica.py" \
  > "${REPO}/logs/profile-lb.log" 2>&1 &
bench_pid=$!

# Wait until the bench has launched its replicas and started prompts,
# then attach py-spy to the busiest replica (heuristic: first child).
deadline=$(( $(date +%s) + 60 ))
while (( $(date +%s) < deadline )); do
  if grep -q "driving" "${REPO}/logs/profile-lb.log" 2>/dev/null; then
    break
  fi
  sleep 0.5
done

pids=$(pgrep -P "${bench_pid}" -d ' ' 2>/dev/null || true)
if [[ -z "${pids}" ]]; then
  echo "    no children found; aborting py-spy attach"
else
  first=$(echo "${pids}" | awk '{print $1}')
  echo "    attaching py-spy to pid=${first} for ${DURATION}s"
  ${SUDO} "${PY_SPY}" record -o "${SVG}" -d "${DURATION}" \
    --subprocesses -p "${first}" || echo "    py-spy failed"
fi

wait "${bench_pid}" || true
echo "    bench results:"
grep -E "wall_s|chars_per_sec|done_p95|redirects_total|intra_replica|log_rows_total|ok " \
  "${REPO}/logs/profile-lb.log" || true

echo "done. flame graph: ${SVG}"
