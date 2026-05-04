# Local Development

## Prerequisites

- Docker + Compose
- Python 3.11+
- `CLAUDE_CODE_OAUTH_TOKEN` or `ANTHROPIC_API_KEY`

## Quick start

```bash
echo "CLAUDE_CODE_OAUTH_TOKEN=..." > .env   # or ANTHROPIC_API_KEY=sk-ant-...
docker compose up --build -d
curl http://localhost:7778/health           # → {"status":"ok"}
```

Postgres binds to `5433` on the host; the API to `7778`. `init_db()`
runs `CREATE TABLE IF NOT EXISTS` + idempotent migrations on startup.

```bash
docker compose down        # keep data
docker compose down -v     # drop the database volume
```

## Run an agent

```python
from agent_sdk import Agent

agent = Agent("my-agent", provider="local", api_url="http://localhost:7778")
print(agent.run("What OS are you running on?"))
```

## Helper scripts

- `scripts/launch_server_docker.sh` — Postgres via `docker compose`, server on `:7778`.
- `scripts/launch_server_local.sh` — project-local conda Postgres (no Docker), server on `:7778`.

Both load `.env` (repo) and `~/.env` before starting.

## Without the helpers

```bash
docker run -d --name agent-sdk-db \
  -e POSTGRES_DB=agent_sdk_server -e POSTGRES_USER=postgres -e POSTGRES_PASSWORD=postgres \
  -p 5433:5432 postgres:16-alpine

export DATABASE_URL=postgresql://postgres:postgres@localhost:5433/agent_sdk_server
export CLAUDE_CODE_OAUTH_TOKEN=...
pip install -e .
uvicorn src.api.server:app --port 7778 --reload
```

## API surface

See [`docs/api.md`](api.md) and [`assets/rest-api.html`](../assets/rest-api.html). Two top-level resources:

- **Volumes** (`/volumes/*`) — durable storage; file ops run directly against the provider primitives, no sandbox needed.
- **Sessions** (`/sessions/*`) — conversation, bound to a volume. Compute lease owned by the in-process `SessionPool`; sandbox identity lives in `sessions.sandbox_state` JSONB. Hibernate via `POST /sessions/{id}/release`; cold-recovery is implicit on the next call. `GET /sessions/{id}/sandbox` returns sandbox metadata (no `/sandboxes` resource).

`POST /sessions` accepts an optional `volume_id` (a default `default-{provider}` volume is created/reused if omitted). Eager by default; `"provision": false` for the lazy session-shell flow.

## Database

Postgres. Tables created on startup via `CREATE TABLE IF NOT EXISTS`;
idempotent migrations in `src/api/db.py::_MIGRATIONS` run on the same path.

| Table | Purpose |
|---|---|
| `agents` | agent configs (id, name, config JSONB) |
| `volumes` | persistent storage records (id, name, provider, provider_ref, status) |
| `sessions` | session records (incl. `inner_session_id`, `env`, `secrets`, `cwd`, `workspace`, `pre_start_commands`, `sandbox_state` JSONB) |
| `session_log` | event log (session_id, event_type, payload JSONB) |

No `sandboxes` table — sandbox identity (`sandbox_ref`, listen port,
snapshot path, recipe) is in `sessions.sandbox_state` JSONB, owned by
the in-process `SessionPool`.

## Environment variables

| Variable | Default | Purpose |
|---|---|---|
| `DATABASE_URL` | `postgresql://localhost:5432/agent_sdk_server` | Postgres conn string |
| `CLAUDE_CODE_OAUTH_TOKEN` | — | Preferred Claude auth |
| `ANTHROPIC_API_KEY` | — | Fallback Claude auth |
| `OPENAI_API_KEY` | — | Required for Codex |
| `AGENT_SDK_REAPER_IDLE_S` | `180` | Idle window before pool hibernates a session |
| `AGENT_SDK_REAPER_INTERVAL_S` | `60` | Pool reaper scan interval |
| `AGENT_SDK_ORIGIN` | `production` | Daytona sandbox label (`scripts/launch_server_test.sh` sets `test`) |

## Tests

```bash
.venv/bin/python -m pytest tests/ -n auto
```

`-n auto` is mandatory (sequential daytona/docker takes 8–15 min). Fine
with `-k` filters; xdist negotiates worker count down.

For golden tests that need a live server, use `scripts/launch_server_test.sh`
(NOT `launch_server_local.sh` directly) — it sets `AGENT_SDK_ORIGIN=test`
so daytona sandboxes are isolatable from production.

Most unit tests use mocked DB / providers — no Docker or Postgres required.
