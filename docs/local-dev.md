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

See [`docs/api.md`](api.md) for the full endpoint reference and [`assets/rest-api.html`](../assets/rest-api.html) for a visual map. Two top-level resource groups:

- **Volumes** (`/volumes/*`) — durable storage, with file ops that operate directly on the provider's volume primitives (no sandbox required).
- **Sessions** (`/sessions/*`) — conversation. Bind to a volume; the active compute lease is owned by the in-process `SessionPool` and the provider sandbox identity (`sandbox_ref`) lives in `sessions.sandbox_state` (JSONB). Hibernation via `/sessions/{id}/release`; cold-recovery is implicit on the next call. There is no separate `/sandboxes` resource — `GET /sessions/{id}/sandbox` returns the metadata.

`POST /sessions` accepts an optional `volume_id`; if omitted, a per-provider default volume is created/reused. Eager by default; pass `"provision": false` for the lazy session-shell flow.

## Database

The server uses Postgres. Tables are created automatically on startup via
`CREATE TABLE IF NOT EXISTS`. Idempotent migrations in `src/api/db.py`
(`_MIGRATIONS`) also run on startup to upgrade existing databases safely.

Tables:
- `agents` — agent configurations (id, name, config JSONB)
- `volumes` — persistent storage records (id, name, provider, provider_ref, status)
- `sessions` — session records (id, agent_id, volume_id, inner_session_id, env, secrets, cwd, pre_start_commands, sandbox_state JSONB)
- `session_log` — event log (session_id, event_type, payload JSONB)

There is no `sandboxes` table — sandbox identity (provider sandbox ref, listen port, snapshot path, recipe) is stored in `sessions.sandbox_state` JSONB and managed by the in-process `SessionPool`.

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `DATABASE_URL` | `postgresql://localhost:5432/agent_sdk_server` | Postgres connection string |
| `CLAUDE_CODE_OAUTH_TOKEN` | — | Preferred auth for Claude agents |
| `ANTHROPIC_API_KEY` | — | Fallback auth for Claude agents |
| `OPENAI_API_KEY` | — | Required for Codex agents |
| `AGENT_SDK_REAPER_IDLE_S` | `180` | Seconds an active session can sit idle before the pool reaper hibernates it |
| `AGENT_SDK_REAPER_INTERVAL_S` | `60` | Pool reaper scan interval in seconds |
| `AGENT_SDK_ORIGIN` | `production` | Tag applied to provisioned daytona sandboxes (set to `test` by `scripts/launch_server_test.sh` so cleanup tooling can isolate test traffic) |

## Running Tests

```bash
.venv/bin/python -m pytest tests/ -n auto
```

`-n auto` (pytest-xdist) is mandatory — sequential runs of the daytona/docker
golden suites take 8–15 min and waste iteration time. It is fine even when
filtering with `-k`; xdist negotiates worker count down to the number of
selected items.

For the golden tests that need a live server, launch via
`scripts/launch_server_test.sh` (NOT `launch_server_local.sh` directly) — the
test wrapper sets `AGENT_SDK_ORIGIN=test` so daytona sandboxes are labelled
isolatable from production traffic.

Unit tests (most of `tests/`) use mocked DB and provider modules — no Docker or
Postgres needed.
