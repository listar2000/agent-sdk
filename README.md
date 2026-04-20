# Agent SDK

Python SDK and orchestration server for running Claude Code, Codex, OpenCode, and other ACP-compatible agents in sandboxes (local, Docker, or Daytona cloud).

## Run the server with Docker

```bash
echo "ANTHROPIC_API_KEY=sk-ant-..." > .env
# Optional for cloud sandboxes:
echo "DAYTONA_API_KEY=dtn_..." >> .env

docker compose up --build -d

curl http://localhost:7778/health
# {"status":"ok"}
```

This starts:
- Postgres on port 5433
- API server on port 7778
- Chat UI at http://localhost:7778/ui

Stop with `docker compose down`. Delete the database volume with `docker compose down -v`.

See [`docs/local-dev.md`](docs/local-dev.md) for more.

## Deploy to Railway

The repo has a `Dockerfile` and `railway.toml` ready for Railway.

1. Create a new Railway project pointing at this repo
2. Add a Postgres service — Railway sets `DATABASE_URL` automatically
3. Set env vars on the API service:
   - `ANTHROPIC_API_KEY`
   - `DAYTONA_API_KEY` (if using cloud sandboxes)
   - `DAYTONA_SNAPSHOT` (optional, defaults to `hive-large`; set another snapshot name to override, or `image` / empty / `0` / `false` to use the legacy Docker image / Dockerfile path)
   - `SANDBOX_IDLE_TIMEOUT` (optional, seconds — default 300)
4. Deploy

For production you'll want `provider="daytona"` for sandboxes since Railway containers are ephemeral. Local/Docker sandboxes only work for dev.

## Database

The server uses Postgres. On every startup, `init_db()` runs:
1. `CREATE TABLE IF NOT EXISTS` for fresh setups
2. Idempotent `ALTER TABLE` migrations to bring existing databases up to date

Migrations live in `_MIGRATIONS` in `src/api/db.py`. Each must use `IF EXISTS` / `IF NOT EXISTS` so they're safe to run repeatedly. Add new ones to the bottom of the list — they run automatically on the next deploy.

Tables: `agents`, `sandboxes`, `sessions`, `session_log`.

## Use the SDK

```bash
pip install httpx fastapi uvicorn psycopg[binary] psycopg_pool
```

```python
from agent_sdk import Agent

agent = Agent(
    "worker",
    provider="local",
    tools=["Bash", "Read", "Write"],
    prompt="You are a helpful agent.",
)

# full response
response = await agent.arun("Create a file called hello.py")

# streaming — Event objects, str(ev) gives text
async for ev in agent.astream("Analyze this codebase"):
    print(ev, end="", flush=True)
    # ev["type"] is "text", "reasoning", "tool", "done", etc.

# sync wrapper
response = agent.run("Say hello")

# interrupt the running prompt and redirect
response = await agent.arun("stop — focus on X instead", interrupt=True)

# fire-and-forget submit (returns rpc_id, use events() to listen)
rpc_id = await agent.send("do this next")
rpc_id = await agent.send("cancel and do this", interrupt=True)
```

All methods accept `interrupt=True` to cancel the running prompt before submitting. `send()` returns the `rpc_id` immediately without waiting for the response — pair it with `agent.events()` for a long-lived event listener.

The SDK talks to the server at `http://localhost:7778` by default. Override with `api_url=` or `AGENT_API_URL=`.

## Per-request Claude credentials

If the caller wants a specific Claude account used for its sandbox (instead of the server's default `ANTHROPIC_API_KEY` / `CLAUDE_CODE_OAUTH_TOKEN`), pass the token on the `Agent`:

```python
from agent_sdk import Agent

agent = Agent("worker", provider="daytona", oauth_token=user_oauth_token)
# or: api_key="sk-ant-..." / CLAUDE_CODE_OAUTH_TOKEN env var / ANTHROPIC_API_KEY env var
await agent.arun("Hello")
```

Credentials travel in the request body to `/sessions/quick` (and resume), are applied as per-sandbox env vars inside the supervisor, and are never persisted to the agent-sdk database. When the caller supplies OAuth, the server scrubs its own `ANTHROPIC_API_KEY` from that sandbox so there's no silent fallback to shared credentials. The SDK refuses to send credentials to a plaintext-HTTP server — use `https://` or a `localhost` URL.

Obtaining the token itself (running `claude setup-token` or an equivalent OAuth flow) is the caller's responsibility — the agent-sdk only forwards what it's handed.

## Session persistence

Sessions survive server restarts. The server persists `{session_id, agent_id, sandbox_id, inner_session_id}` to Postgres. Resume from another process with just the session_id:

```python
agent = Agent("restored", session_id="abc123")
response = await agent.arun("What were we discussing?")
```

The server looks up the session in the DB, restarts the sandbox if stopped, and replays the conversation history via `session/load`.

## Architecture

```
┌───────────┐      ┌──────────────────┐      ┌─────────────────────┐
│ SDK       │─────▶│ API server       │─────▶│ supervisor.js       │
│ (Agent)   │      │ (orchestrator)   │      │ (stdio ⇄ POST+SSE   │
└───────────┘      └──────────────────┘      │  bridge to ACP)     │
      │                    │                 └──────────┬──────────┘
      │              Postgres                           │ stdio
      │              (sessions, agents, sandboxes)      ▼
      │                                        claude-agent-acp
   /sessions/*   — conversation                  or codex-acp
                  (message, events, resume)
```

- **Sessions** hold conversation state (agent config, message history). Auto-recover from the DB when reaped.
- **Supervisor** is a thin node process (`src/supervisor/supervisor.js`) that spawns the agent's ACP binary and exposes it over `/v1/acp/{id}` POST+SSE. One supervisor per provider-instance.
- **Providers**: `local` (supervisor subprocess on host), `docker` (supervisor in ephemeral container), `daytona` (supervisor in a Daytona sandbox). Provider-pluggable, same HTTP surface across all three.

## Providers

| Provider | How it works | Survives reap? |
|---|---|---|
| `local` | Subprocess on the host | No — process killed |
| `docker` | Docker container | No — container removed |
| `daytona` | Daytona cloud workspace | Yes — workspace stopped, filesystem preserved |

For persistent sessions that survive long idle periods, use Daytona.

## Docs

- [API reference](docs/api.md) — REST endpoints
- [Local dev](docs/local-dev.md) — Docker setup, env vars

## Layout

```
src/
  agent_sdk/         Python SDK client (Agent class)
    client.py
    errors.py
    persist.py       SQLite session persistence
  api/               Orchestration server
    server.py          FastAPI endpoints
    providers.py       local / docker / daytona supervisor bootstrap
    acp_client.py      JSON-RPC ACP client (POST + SSE)
    db.py              Postgres CRUD
    sse.py             SSE parsing
    models.py          Dataclasses
    redact.py          Secret redaction for logs
  supervisor/        Node stdio ⇄ HTTP bridge to claude-agent-acp / codex-acp
    supervisor.js
    package.json
    Dockerfile
tests/               pytest
examples/            Demo scripts
docs/                Docs
docker-compose.yml   Postgres + API server
Dockerfile
```

## Tests

```bash
PYTHONPATH=src python -m pytest tests/ -q
```

Tests use mocked DB and sandbox providers — no Docker or Postgres needed.
