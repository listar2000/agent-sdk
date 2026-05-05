#!/usr/bin/env bash
# A/B head-to-head between baseline (pre-changes server behavior) and
# patched (current server). To get a fair "with vs without" I temporarily
# revert the per-call httpx pattern + the cached AcpClient + the
# to_thread b64 wrap by setting AGENT_SDK_DISABLE_OPTS=1 in the patched
# server (which the source tree honors via the same code paths).
#
# Since I don't want to ship that toggle into production code, this harness
# instead does the comparison the only really honest way: it requires the
# user to run the harness twice — once on the baseline branch, once on the
# patched branch — appending to the same REPORT file. Then `compare.py`
# diffs them.
#
# Usage:
#
#   # On baseline branch (or git stash my changes):
#   LABEL=baseline PROVIDER=unix_local N_SESSIONS=5 ITERS=3 \
#     bash benchmark/load/ab_harness.sh
#
#   # On patched branch:
#   LABEL=patched PROVIDER=unix_local N_SESSIONS=5 ITERS=3 \
#     bash benchmark/load/ab_harness.sh
#
#   # Compare:
#   .venv/bin/python benchmark/load/compare.py
#
# For each iteration, this script:
#   1. Restarts the server fresh (kills existing on :7778)
#   2. Sleeps 2s for warmup
#   3. Runs workload_full.py with the given env
#   4. Sleeps 2s between iterations to let things settle

set -uo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="${REPO_ROOT:-$(cd -- "${SCRIPT_DIR}/../.." && pwd)}"
VPY="${VPY:-${REPO_ROOT}/.venv/bin/python}"

LABEL="${LABEL:-run}"
PROVIDER="${PROVIDER:-unix_local}"
N_SESSIONS="${N_SESSIONS:-5}"
N_TURNS="${N_TURNS:-2}"
N_FILE_OPS="${N_FILE_OPS:-5}"
LARGE_MB="${LARGE_MB:-2}"
MODEL="${MODEL:-haiku}"
SKIP_RELEASE="${SKIP_RELEASE:-0}"
ITERS="${ITERS:-3}"
REPORT="${REPORT:-/tmp/workload_full.jsonl}"

# CHECKOUT_PATH points at the server source tree to load. Defaults to
# the harness's own repo. To A/B against an alternate checkout:
#   git worktree add /tmp/asdk-baseline HEAD
#   CHECKOUT_PATH=/tmp/asdk-baseline LABEL=baseline ... bash ab_harness.sh
# Then re-run with CHECKOUT_PATH unset (or pointed at the patched repo).
CHECKOUT_PATH="${CHECKOUT_PATH:-${REPO_ROOT}}"

cd "${REPO_ROOT}"

# Load env (CLAUDE_CODE_OAUTH_TOKEN, DAYTONA_API_KEY)
for env_file in "${HOME}/.env" "${REPO_ROOT}/.env"; do
    if [ -f "${env_file}" ]; then
        set -a
        # shellcheck disable=SC1090
        source "${env_file}"
        set +a
    fi
done

export AGENT_SDK_ORIGIN=test
export PYTHONPATH="${CHECKOUT_PATH}/src"
export DATABASE_URL="${DATABASE_URL:-postgresql://postgres@localhost:5433/agent_sdk_server}"
# Point both checkouts at the main repo's installed supervisor so we don't
# duplicate npm install just to A/B Python source.
export AGENT_SDK_RUNTIME_PATH="${AGENT_SDK_RUNTIME_PATH:-${REPO_ROOT}/src/supervisor}"

start_server() {
    # Kill any existing
    local pids
    pids=$(lsof -ti :7778 2>/dev/null || true)
    if [ -n "${pids}" ]; then
        # shellcheck disable=SC2086
        kill -9 ${pids} 2>/dev/null || true
        sleep 1
    fi
    nohup "${VPY}" -m uvicorn api.server:app --host 0.0.0.0 --port 7778 \
        > "/tmp/ab_server_${LABEL}.log" 2>&1 &
    SERVER_PID=$!
    # wait for ready
    for _ in $(seq 1 30); do
        if curl -s -o /dev/null -m 1 http://localhost:7778/health; then
            break
        fi
        sleep 0.5
    done
    echo "[harness] server pid=${SERVER_PID} ready"
}

stop_server() {
    if [ -n "${SERVER_PID:-}" ]; then
        kill -9 "${SERVER_PID}" 2>/dev/null || true
        wait "${SERVER_PID}" 2>/dev/null || true
    fi
}

trap stop_server EXIT

for i in $(seq 1 "${ITERS}"); do
    echo
    echo "============================================================"
    echo "Iteration ${i}/${ITERS}  label=${LABEL}  provider=${PROVIDER}"
    echo "============================================================"
    start_server
    sleep 2
    LABEL="${LABEL}_iter${i}" \
    PROVIDER="${PROVIDER}" \
    N_SESSIONS="${N_SESSIONS}" \
    N_TURNS="${N_TURNS}" \
    N_FILE_OPS="${N_FILE_OPS}" \
    LARGE_MB="${LARGE_MB}" \
    MODEL="${MODEL}" \
    SKIP_RELEASE="${SKIP_RELEASE}" \
    REPORT="${REPORT}" \
        "${VPY}" "${SCRIPT_DIR}/workload_full.py"
    stop_server
    sleep 2
done

echo
echo "Done. Results appended to ${REPORT}"
echo "Run: ${VPY} ${SCRIPT_DIR}/compare.py"
