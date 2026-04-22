# Local Development

## Prerequisites

- Docker and Docker Compose
- Python 3.11+
- A `CLAUDE_CODE_OAUTH_TOKEN` or `ANTHROPIC_API_KEY` for running Claude agents

## Quick Start

### 1. Create `.env` file

```bash
echo "CLAUDE_CODE_OAUTH_TOKEN=..." > .env   # preferred
# echo "ANTHROPIC_API_KEY=sk-ant-..." >> .env  # fallback
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
export CLAUDE_CODE_OAUTH_TOKEN=...   # or ANTHROPIC_API_KEY=sk-ant-...
pip install -e .
uvicorn src.api.server:app --port 7778 --reload
```

## API Structure

### Volume endpoints (persistent storage)

```
POST   /volumes                              — create volume
POST   /volumes/provision                    — create + wait for ready
GET    /volumes                              — list (?provider= filter)
GET    /volumes/{id_or_name}                 — get info
DELETE /volumes/{id_or_name}?force=false     — delete (409 if sessions reference it unless force=true)
GET    /volumes/{id_or_name}/files/tree      — browse volume contents
GET    /volumes/{id_or_name}/files/read      — read a file on the volume
POST   /volumes/{id_or_name}/files/edit      — write a file on the volume
```

File ops are backed internally by a short-lived utility sandbox — callers never
need to manage one to seed data.

### Sandbox endpoints (ephemeral compute)

```
POST   /sandboxes                    — create sandbox (requires volume_id, subpath)
POST   /sandboxes/provision          — create + wait for ready
GET    /sandboxes                    — list
GET    /sandboxes/{id}               — get info
DELETE /sandboxes/{id}               — destroy (sessions referencing it survive with current_sandbox_id = NULL)
POST   /sandboxes/{id}/stop          — stop (preserves volume data)
POST   /sandboxes/{id}/start         — resume stopped sandbox
```

Sandbox endpoints operate directly on the infrastructure. No session required.
Every sandbox is `(volume_id, subpath)`-scoped at creation: the volume is
mounted at `/home/daytona`, with an additional read-only `shared/` mount at
`/mnt/shared`.

### Session endpoints (conversation)

```
POST   /sessions                     — create session bound to a volume (no sandbox yet, lazy)
POST   /sessions/quick               — create agent + volume binding + sandbox + session in one call
POST   /sessions/{id}/message        — send prompt (lazily provisions a sandbox if needed)
GET    /sessions/{id}/events         — SSE event stream (now also emits sandbox_reattach / sandbox_lost)
POST   /sessions/{id}/cancel         — cancel running prompt
POST   /sessions/{id}/config         — set model, mode, thinking level
GET    /sessions/{id}/status         — session status
GET    /sessions/{id}/log            — event log
POST   /sessions/{id}/resume         — resume (works even after sandbox was killed)
POST   /sessions/{id}/start-sandbox  — pre-warm a sandbox eagerly
POST   /sessions/{id}/stop-sandbox   — kill current sandbox (next /message lazy-provisions)
POST   /sessions/{id}/reset-sandbox  — kill current + provision a fresh one
POST   /sessions/{id}/sandbox/exec   — run a shell command in session sandbox
```

`POST /sessions` requires `volume_id` and does **not** provision a sandbox —
the compute is created lazily on the first `/message` or `/resume`.
`POST /sessions/quick` also requires `volume_id`.

Conversation endpoints (`/sessions/{id}/message`, `/sessions/{id}/events`,
`/sessions/{id}/cancel`, `/sessions/{id}/config`, `/sessions/{id}/resume`,
`/sessions/{id}/sandbox/exec`) auto-recover if the sandbox was reaped or killed
— the server reprovisions a new sandbox bound to the session's volume, so
conversation history on disk survives.

## Database

The server uses Postgres. Tables are created automatically on startup via
`CREATE TABLE IF NOT EXISTS`. Idempotent migrations in `src/api/db.py`
(`_MIGRATIONS`) also run on startup to upgrade existing databases safely.

Tables:
- `agents` — agent configurations (id, name, config JSONB)
- `volumes` — persistent storage records (id, name, provider, provider_ref, status)
- `sandboxes` — ephemeral compute records (id, provider, sandbox_ref, status, volume_id, subpath)
- `sessions` — session records (id, agent_id, volume_id, current_sandbox_id, inner_session_id, env, secrets)
- `session_log` — event log (session_id, event_type, payload JSONB)

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `DATABASE_URL` | `postgresql://localhost:5432/agent_sdk_server` | Postgres connection string |
| `CLAUDE_CODE_OAUTH_TOKEN` | — | Preferred auth for Claude agents |
| `ANTHROPIC_API_KEY` | — | Fallback auth for Claude agents |
| `OPENAI_API_KEY` | — | Required for Codex agents |
| `SANDBOX_IDLE_TIMEOUT` | `300` | Seconds before idle sessions are reaped |
| `SANDBOX_REAPER_TICK` | `60` | Idle reaper scan interval in seconds |
| `SSE_HEARTBEAT_INTERVAL` | `30` | SSE heartbeat interval in seconds |

## Running Tests

```bash
PYTHONPATH=src python -m pytest tests/ -v
```

Tests use mocked DB and sandbox providers — no Docker or Postgres needed.
