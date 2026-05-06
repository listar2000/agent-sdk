#!/usr/bin/env bash
# Compare uvicorn HTTP throughput across loop+parser combos against a
# minimal FastAPI handler. Multi-process load gen so the SERVER, not the
# client, is the bottleneck. Run from the agent-sdk repo root or with
# REPO_ROOT set; defaults assume the standard .venv layout.
set -uo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="${REPO_ROOT:-$(cd -- "${SCRIPT_DIR}/../.." && pwd)}"
VPY="${VPY:-${REPO_ROOT}/.venv/bin/python}"

cd "${SCRIPT_DIR}"

run_combo() {
    local label="$1"
    local loop="$2"
    local http="$3"
    echo "=== $label (loop=$loop, http=$http) ==="
    "$VPY" -m uvicorn _minimal_app:app \
        --host 127.0.0.1 --port 8766 --loop "$loop" --http "$http" \
        --log-level warning &
    local PID=$!
    for _ in $(seq 1 30); do
        if curl -s -o /dev/null -m 1 http://127.0.0.1:8766/ping; then break; fi
        sleep 0.2
    done
    sleep 0.3
    "$VPY" load_uvicorn.py
    kill "$PID" 2>/dev/null
    wait "$PID" 2>/dev/null
    echo
}

run_combo "default(asyncio+h11)" asyncio h11
run_combo "uvloop+httptools"     uvloop  httptools
