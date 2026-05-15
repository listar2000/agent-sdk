#!/usr/bin/env bash
# Run the goldens N times per provider, capture per-run result, fail on
# any non-zero-failure run. Replicas + LB must already be running on
# 7790-7794. Logs go to logs/lockin/<provider>-<run>.log.

set -uo pipefail
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd -- "${SCRIPT_DIR}/../.." && pwd)"
VENV_PY="${REPO}/.venv/bin/python"
DB="postgresql://postgres@localhost:5433/agent_sdk_test_scale"
LB="http://127.0.0.1:7790"
RUNS_PER="${RUNS_PER:-3}"

[[ -f "${HOME}/.env" ]] && { set -a; source "${HOME}/.env"; set +a; }
mkdir -p "${REPO}/logs/lockin"

declare -i fails=0
declare -a fail_list=()

_one() {
  local provider="$1" run="$2" filter="$3" tmo="$4"
  local logf="${REPO}/logs/lockin/${provider}-${run}.log"
  PGPASSWORD=postgres psql -h localhost -p 5433 -U postgres -d agent_sdk_test_scale \
    -c "DELETE FROM session_log; DELETE FROM sessions; DELETE FROM agents; DELETE FROM volumes;" \
    >/dev/null 2>&1
  DATABASE_URL="${DB}" TEST_DATABASE_URL="${DB}" \
  PYTHONPATH="${REPO}/src" AGENT_API_URL="${LB}" AGENT_SERVER_URL="${LB}" \
    timeout "${tmo}" "${VENV_PY}" -m pytest \
    "${REPO}/tests/test_sandbox_stop_delete_recovery.py" \
    -n auto -k "${filter}" --tb=long > "${logf}" 2>&1
  local rc=$?
  local last
  last="$(tail -1 "${logf}")"
  if grep -q " failed" "${logf}" || (( rc != 0 )); then
    echo "[FAIL] ${provider}-${run}  rc=${rc}  ${last}"
    fails+=1
    fail_list+=("${provider}-${run}")
  else
    echo "[PASS] ${provider}-${run}  ${last}"
  fi
}

for r in $(seq 1 "${RUNS_PER}"); do
  _one unix_local "${r}" "unix_local" 600
done
for r in $(seq 1 "${RUNS_PER}"); do
  _one modal "${r}" "modal and claude" 900
done
for r in $(seq 1 "${RUNS_PER}"); do
  _one daytona "${r}" "daytona and claude" 900
done

echo ""
echo "=== summary: ${fails} failure(s) across $((RUNS_PER * 3)) runs ==="
if (( fails > 0 )); then
  for r in "${fail_list[@]}"; do
    echo "--- ${r} traceback head ---"
    grep -A4 "FAILED\|TimeoutError\|AssertionError\|RuntimeError" \
      "${REPO}/logs/lockin/${r}.log" | head -40
    echo ""
  done
  exit 1
fi
exit 0
