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

## Helper scripts

`scripts/launch_server_docker.sh` and `scripts/launch_server_local.sh` bootstrap everything (venv, Postgres, uvicorn):

- `launch_server_docker.sh` — Postgres via `docker compose`, server on `:7778`.
- `launch_server_local.sh` — project-local conda-installed Postgres (no Docker needed), server on `:7778`.

Both load env vars from `.env` (repo-local) or `~/.env` before starting.

## Without Docker or the helper scripts

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

See [`docs/api.md`](api.md) for the full endpoint reference and [`assets/rest-api.html`](../assets/rest-api.html) for a visual map. Three resource groups:

- **Volumes** (`/volumes/*`) — durable storage, with file ops backed by a short-lived utility sandbox.
- **Sandboxes** (`/sandboxes/*`) — ephemeral compute. Every sandbox is `(volume_id, subpath)`-scoped at creation; the volume is mounted at `/home/daytona` with a read-only `shared/` subpath at `/mnt/shared`.
- **Sessions** (`/sessions/*`) — conversation. Bind to a volume; `current_sandbox_id` is swapped as sandboxes come and go. Explicit sandbox control via `/sessions/{id}/{start,stop,reset}-sandbox`.

`POST /sessions` and `POST /sessions/quick` accept an optional `volume_id`; if omitted, a per-provider default volume is created/reused.

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
