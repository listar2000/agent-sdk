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

# async (recommended)
response = await agent.arun("Create a file called hello.py")

# streaming
async for chunk in agent.astream("Analyze this codebase"):
    print(chunk, end="", flush=True)

# sync wrapper
response = agent.run("Say hello")
```

The SDK talks to the server at `http://localhost:7778` by default. Override with `api_url=` or `AGENT_API_URL=`.

## Session persistence

Sessions survive server restarts. The server persists `{session_id, agent_id, sandbox_id, inner_session_id}` to Postgres. Resume from another process with just the session_id:

```python
agent = Agent("restored", session_id="abc123")
response = await agent.arun("What were we discussing?")
```

The server looks up the session in the DB, restarts the sandbox if stopped, and replays the conversation history via `session/load`.

## Sandbox operations

Agents can interact with their sandbox directly without going through Claude:

```python
# Filesystem
files = await agent.list_dir("/app")
content = await agent.read_file("/app/main.py")
await agent.write_file("/app/main.py", "print('hi')")

# Execute commands
result = await agent.exec("python", args=["main.py"])
output = await agent.shell("ls -la")

# Processes
proc = await agent.start_process("python", args=["server.py"])
logs = await agent.get_process_logs(proc["id"])
await agent.stop_process(proc["id"])

# Desktop (when sandbox has a display)
png = await agent.screenshot()
await agent.mouse_click(100, 200)
await agent.keyboard_type("hello")
```

## Architecture

```
┌───────────┐      ┌──────────────────┐      ┌─────────────────┐
│ SDK       │─────▶│ API server       │─────▶│ sandbox-agent   │
│ (Agent)   │      │ (orchestrator)   │      │ (in sandbox)    │
└───────────┘      └──────────────────┘      └─────────────────┘
      │                    │                        │
      │              Postgres (sessions,     Runs Claude/Codex,
      │               agents, sandboxes)     streams via SSE
      │
   /sessions/*   — conversation (message, events, resume)
   /sandboxes/*  — infrastructure (fs, exec, desktop, processes)
```

- **Sessions** hold conversation state (agent config, message history). Auto-recover from the DB when reaped.
- **Sandboxes** are infrastructure. Provider-pluggable (local subprocess, Docker, Daytona).
- **Providers**: `local` (subprocess on host), `docker` (container), `daytona` (cloud workspace).

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
- [Rivet sandbox-agent SDK reference](docs/rivet_sandbox_agent_sdk_reference.md) — upstream ACP protocol details

## Layout

```
src/
  agent_sdk/         Python SDK client (Agent class)
    client.py
    errors.py
    persist.py       SQLite session persistence
  api/               Orchestration server
    server.py          FastAPI endpoints
    sandbox.py         Sandbox layer (stateless wrapper around providers)
    providers.py       local / docker / daytona
    sandbox_agent_client.py  ACP client
    db.py              Postgres CRUD
    sse.py             SSE parsing
    models.py          Dataclasses
    redact.py          Secret redaction for logs
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
