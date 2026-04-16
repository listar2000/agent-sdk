#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

# Load env vars (API keys, SSL cert, etc.)
if [ -f ~/.env ]; then
    source ~/.env
fi

# Vertex env vars (CLAUDE_CODE_USE_VERTEX, ANTHROPIC_VERTEX_BASE_URL, etc.)
# are set by the managed claude binary in your shell. This script inherits
# them automatically when run from such a shell. If they're missing and no
# ANTHROPIC_API_KEY is set, warn the user.
if [ -z "${ANTHROPIC_API_KEY:-}" ] && [ -z "${CLAUDE_CODE_USE_VERTEX:-}" ]; then
    echo "WARNING: No ANTHROPIC_API_KEY or CLAUDE_CODE_USE_VERTEX found."
    echo "  Run this script from a terminal managed by the claude binary,"
    echo "  or set ANTHROPIC_API_KEY in ~/.env."
fi

# Ensure Postgres is running via Docker
if ! docker compose ps db 2>/dev/null | grep -q "running"; then
    echo "Starting Postgres..."
    docker compose up -d db
    echo "Waiting for Postgres to be healthy..."
    until docker compose ps db 2>/dev/null | grep -q "healthy"; do
        sleep 1
    done
fi
echo "Postgres is ready on port 5433."

# Stop the Docker server if it's running (to avoid port conflicts)
if docker compose ps server 2>/dev/null | grep -q "running"; then
    echo "Stopping Docker server to free port 7778..."
    docker compose stop server
fi

# Kill any existing process on 7778
PID=$(lsof -i :7778 -t 2>/dev/null || true)
if [ -n "$PID" ]; then
    echo "Killing existing process on port 7778 (PID $PID)..."
    kill "$PID" 2>/dev/null || true
    sleep 1
fi

export DATABASE_URL=postgresql://postgres:postgres@localhost:5433/agent_sdk_server

echo "Starting local server on http://localhost:7778 ..."
exec .venv/bin/uvicorn src.api.server:app --host 0.0.0.0 --port 7778
