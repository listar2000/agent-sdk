# Local Development

## Prerequisites

- Docker and Docker Compose
- Python 3.11+
- An `ANTHROPIC_API_KEY` (for running agents with Claude)

## Quick Start

### 1. Create `.env` file

```bash
echo "ANTHROPIC_API_KEY=sk-ant-..." > .env
```

### 2. Start the server

```bash
docker compose up --build -d
```

This starts:
- **Postgres** on port 5433 (host) / 5432 (internal)
- **API server** on port 7778

The server auto-creates all database tables on startup (`CREATE TABLE IF NOT EXISTS`).

### 3. Verify

```bash
curl http://localhost:7778/health
# {"status":"ok"}
```

### 4. Run an agent

```python
from agent_sdk import Agent

agent = Agent(
    "my-agent",
    provider="local",
    api_url="http://localhost:7778",
)
print(agent.run("What OS are you running on?"))
```

### 5. Stop

```bash
docker compose down        # stop, keep data
docker compose down -v     # stop, delete database
```

## Without Docker (server only)

If you want to run the server directly (e.g., for debugging):

```bash
# Start Postgres separately
docker run -d --name agent-sdk-db \
  -e POSTGRES_DB=agent_sdk_server \
  -e POSTGRES_USER=postgres \
  -e POSTGRES_PASSWORD=postgres \
  -p 5433:5432 \
  postgres:16-alpine

# Run the server
export DATABASE_URL=postgresql://postgres:postgres@localhost:5433/agent_sdk_server
export ANTHROPIC_API_KEY=sk-ant-...
pip install -e .
uvicorn src.api.server:app --port 7778 --reload
```

## API Structure

### Sandbox endpoints (infrastructure)

```
POST   /sandboxes                    — create sandbox
GET    /sandboxes                    — list
GET    /sandboxes/{id}               — get info
DELETE /sandboxes/{id}               — destroy
POST   /sandboxes/{id}/stop          — stop (preserves filesystem on Daytona)
POST   /sandboxes/{id}/start         — resume stopped sandbox
```

Sandbox endpoints operate directly on the infrastructure. No session required.

### Session endpoints (conversation)

```
POST   /sessions/quick               — create agent + sandbox + session
POST   /sessions/{id}/message        — send prompt
GET    /sessions/{id}/events         — SSE event stream
POST   /sessions/{id}/cancel         — cancel running prompt
POST   /sessions/{id}/config         — set model, mode, thinking level
GET    /sessions/{id}/status         — session status
GET    /sessions/{id}/log            — event log
POST   /sessions/{id}/sandbox/exec   — run a shell command in session sandbox
```

Conversation endpoints (`/sessions/{id}/message`, `/sessions/{id}/events`,
`/sessions/{id}/cancel`, `/sessions/{id}/config`, `/sessions/{id}/resume`,
`/sessions/{id}/sandbox/exec`) auto-recover if a session was reaped by the idle
timeout — the server looks up the session in the DB, restarts the sandbox if
stopped, and reloads the conversation.

## Database

The server uses Postgres. Tables are created automatically on startup via
`CREATE TABLE IF NOT EXISTS`. Idempotent migrations in `src/api/db.py`
(`_MIGRATIONS`) also run on startup to upgrade existing databases safely.

Tables:
- `agents` — agent configurations (id, name, config JSONB)
- `sandboxes` — sandbox records (id, provider, sandbox_ref, status)
- `sessions` — session records (id, agent_id, sandbox_id, inner_session_id)
- `session_log` — event log (session_id, event_type, payload JSONB)

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `DATABASE_URL` | `postgresql://localhost:5432/agent_sdk_server` | Postgres connection string |
| `ANTHROPIC_API_KEY` | — | Required for Claude agents |
| `OPENAI_API_KEY` | — | Required for Codex agents |
| `SANDBOX_IDLE_TIMEOUT` | `300` | Seconds before idle sessions are reaped |
| `SANDBOX_REAPER_TICK` | `60` | Idle reaper scan interval in seconds |
| `SSE_HEARTBEAT_INTERVAL` | `30` | SSE heartbeat interval in seconds |

## Running Tests

```bash
PYTHONPATH=src python -m pytest tests/ -v
```

Tests use mocked DB and sandbox providers — no Docker or Postgres needed.
