#!/usr/bin/env bash
# Sweep AGENT_SDK_SUPERVISOR_FLUSH_MS × AGENT_SDK_LOG_FLUSH_MS to find a
# good default for the production deploy. Runs the real-agent profiler at
# each combo and appends one JSONL row per run.
#
# Approx cost: ~9 combos × 8 sessions × 2-5s/prompt ≈ 6 minutes wall +
# claude-haiku tokens (cheap). Configure SUPERVISOR_MS_GRID and
# LOG_MS_GRID via env to trim or expand.
#
# Output: benchmark/scale/tune_results.jsonl, one row per (sup_ms, log_ms)
# combo. Plot externally or grep for the sweet spot.

set -euo pipefail
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "${SCRIPT_DIR}/../.." && pwd)"
VENV_PY="${REPO_ROOT}/.venv/bin/python"

DB_URL="${DATABASE_URL:-postgresql://postgres@localhost:5433/agent_sdk_test_scale}"
PORT="${BENCH_PORT:-7779}"
N_SESSIONS="${N_SESSIONS:-8}"
RESULT_PATH="${RESULT_PATH:-${REPO_ROOT}/benchmark/scale/tune_results.jsonl}"
SUPERVISOR_MS_GRID="${SUPERVISOR_MS_GRID:-0 40 150}"
LOG_MS_GRID="${LOG_MS_GRID:-0 100 250}"

# Source ~/.env for CLAUDE_CODE_OAUTH_TOKEN.
[[ -f "${HOME}/.env" ]] && { set -a; source "${HOME}/.env"; set +a; }
[[ -f "${REPO_ROOT}/.env" ]] && { set -a; source "${REPO_ROOT}/.env"; set +a; }

mkdir -p "${REPO_ROOT}/logs"
PID_FILE="${REPO_ROOT}/logs/tune-server.pid"
LOG_FILE="${REPO_ROOT}/logs/tune-server.log"

_stop_server() {
  if [[ -f "${PID_FILE}" ]]; then
    pid=$(cat "${PID_FILE}")
    if kill -0 "${pid}" 2>/dev/null; then
      kill -TERM "${pid}" 2>/dev/null || true
      for _ in 1 2 3 4 5 6 7 8 9 10; do
        kill -0 "${pid}" 2>/dev/null || break
        sleep 0.5
      done
      kill -KILL "${pid}" 2>/dev/null || true
    fi
    rm -f "${PID_FILE}"
  fi
}

_start_server() {
  local sup_ms="$1" log_ms="$2"
  _stop_server
  echo "[tune] starting server (sup_ms=${sup_ms}, log_ms=${log_ms})"
  (
    cd "${REPO_ROOT}"
    DATABASE_URL="${DB_URL}" \
    PYTHONPATH="${REPO_ROOT}/src" \
    AGENT_SDK_SUPERVISOR_FLUSH_MS="${sup_ms}" \
    AGENT_SDK_LOG_FLUSH_MS="${log_ms}" \
    AGENT_SDK_DISABLE_LEASE=1 \
    AGENT_SDK_PORT="${PORT}" \
      "${VENV_PY}" -m uvicorn api.server:app --host 127.0.0.1 --port "${PORT}" \
        --workers 1 \
        > "${LOG_FILE}" 2>&1 &
    echo $! > "${PID_FILE}"
  )
  # Wait for /health
  for _ in $(seq 1 60); do
    if curl -fsS "http://127.0.0.1:${PORT}/health" >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.5
  done
  echo "[tune] server failed to come up"
  tail -20 "${LOG_FILE}"
  return 1
}

trap _stop_server EXIT

echo "[tune] grid: SUP=${SUPERVISOR_MS_GRID} × LOG=${LOG_MS_GRID}; N=${N_SESSIONS}"
echo "[tune] results → ${RESULT_PATH}"
# Truncate prior results so the sweep is self-contained.
: > "${RESULT_PATH}"

for sup_ms in ${SUPERVISOR_MS_GRID}; do
  for log_ms in ${LOG_MS_GRID}; do
    _start_server "${sup_ms}" "${log_ms}"
    sleep 0.5  # let lifespan settle
    TAG="sup${sup_ms}_log${log_ms}" \
      API="http://127.0.0.1:${PORT}" \
      DB_URL="${DB_URL}" \
      N_SESSIONS="${N_SESSIONS}" \
      RESULT_PATH="${RESULT_PATH}" \
      PYTHONPATH="${REPO_ROOT}/src" \
      "${VENV_PY}" "${SCRIPT_DIR}/profile_realagent.py" || true
  done
done

_stop_server
echo "[tune] done. results:"
"${VENV_PY}" - <<PY
import json
rows = []
for line in open("${RESULT_PATH}"):
    line = line.strip()
    if not line: continue
    rows.append(json.loads(line))
fmt = "{tag:18} ok={ok:>2} ev={events_total:>4} chunks={text_chunks_total:>4} chars/chunk={cpc:>5} p95={p95:>6}s log_rows={log_rows_total:>4}"
for r in rows:
    print(fmt.format(
        tag=r.get("tag", ""),
        ok=r.get("ok", 0),
        events_total=r.get("events_total", 0),
        text_chunks_total=r.get("text_chunks_total", 0),
        cpc=r.get("chars_per_chunk_avg", 0),
        p95=r.get("done_p95_s", 0),
        log_rows_total=r.get("log_rows_total", 0),
    ))
PY
