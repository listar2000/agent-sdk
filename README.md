# Agent SDK

Python SDK and orchestration server for running Claude Code, Codex, OpenCode, and other ACP-compatible agents in sandboxes (local, Docker, Daytona cloud, Modal).

## Run the server with Docker

```bash
echo "CLAUDE_CODE_OAUTH_TOKEN=..." > .env   # preferred
# echo "ANTHROPIC_API_KEY=sk-ant-..." >> .env  # fallback if no OAuth token
# Optional for cloud sandboxes:
echo "DAYTONA_API_KEY=dtn_..." >> .env

docker compose up --build -d

curl http://localhost:7778/health
# {"status":"ok","sessions":0,"busy_sessions":0}
```

This starts:
- Postgres on port 5433
- API server on port 7778
- Chat UI at http://localhost:7778/ui

Stop with `docker compose down`. Delete the database volume with `docker compose down -v`.

See [`docs/local-dev.md`](docs/local-dev.md) for more. For a managed venv + Postgres bootstrap, use `./scripts/launch_server_docker.sh` (Postgres via `docker compose`) or `./scripts/launch_server_local.sh` (project-local conda-installed Postgres, no Docker needed).

## Deploy to Railway

The repo has a `Dockerfile` and `railway.toml` ready for Railway.

1. Create a new Railway project pointing at this repo
2. Add a Postgres service — Railway sets `DATABASE_URL` automatically
3. Set env vars on the API service:
   - `CLAUDE_CODE_OAUTH_TOKEN` (preferred) or `ANTHROPIC_API_KEY` (fallback)
   - `DAYTONA_API_KEY` (if using cloud sandboxes)
   - `DAYTONA_SNAPSHOT` (optional, defaults to `hive-large`; set another snapshot name to override, or `image` / empty / `0` / `false` to use the legacy Docker image / Dockerfile path)
   - `AGENT_SDK_REAPER_IDLE_S` (optional, seconds — default 180; idle session hibernation window)
   - `AGENT_SDK_REAPER_INTERVAL_S` (optional, seconds — default 60; reaper scan interval)
4. Deploy

For production you'll want `provider="daytona"` for sandboxes since Railway containers are ephemeral. Local/Docker sandboxes only work for dev.

## Database

The server uses Postgres. On every startup, `init_db()` runs:
1. `CREATE TABLE IF NOT EXISTS` for fresh setups
2. Idempotent `ALTER TABLE` migrations to bring existing databases up to date

Migrations live in `_MIGRATIONS` in `src/api/db.py`. Each must use `IF EXISTS` / `IF NOT EXISTS` so they're safe to run repeatedly. Add new ones to the bottom of the list — they run automatically on the next deploy.

Tables: `agents`, `volumes`, `sessions`, `session_log`. Sandbox identity is **not** a separate table — it lives in `sessions.sandbox_state` (JSONB), owned by the in-process `SessionPool`.

## Use the SDK

```bash
pip install httpx
```

```python
from agent_sdk import Agent

agent = Agent(
    "worker",
    provider="local",
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

Agent identity is pure — `agent_type`, `model`, `mcp_servers`, `skills`, `mode`, `thought_level`. Per-session knobs (`cwd`, `env`, `secrets`) and provisioning knobs (`dockerfile`, `shared_mounts`, `root`, `pre_start_commands`, `volume_id`) live on the session, not on the agent.

## Per-request Claude credentials

The preferred credential is a Claude Code OAuth token. If the caller wants a specific account used for its sandbox (instead of the server's default), pass it on the `Agent`:

```python
from agent_sdk import Agent

agent = Agent("worker", provider="daytona", oauth_token=user_oauth_token)
# fallback order: oauth_token= > CLAUDE_CODE_OAUTH_TOKEN env > api_key= > ANTHROPIC_API_KEY env
await agent.arun("Hello")
```

Credentials travel in the `secrets` request field to `POST /sessions` and `POST /sessions/{id}/resume`, are stored on the session row for future recovery, are redacted from read APIs, and are applied as per-sandbox env vars inside the supervisor. When OAuth is supplied, the server scrubs its own `ANTHROPIC_API_KEY` from that sandbox so there's no silent fallback to shared credentials. The SDK refuses to send credentials to a plaintext-HTTP server — use `https://` or a `localhost` URL.

Obtaining the OAuth token (`claude setup-token` or equivalent) is the caller's responsibility — the agent-sdk only forwards what it's handed.

## Session persistence

Sessions survive server restarts **and** sandbox death. The server persists `{session_id, agent_id, volume_id, sandbox_state, inner_session_id}` to Postgres. The agent's HOME (`~/.claude`, transcripts, workspace) lives on the volume, not the sandbox. When a sandbox is reaped, crashes, or is explicitly deleted, the session row survives with `sandbox_state.sandbox_ref = null`; the next `/message` lazily reprovisions a new sandbox that mounts the same volume, and the agent CLI resumes from the transcript still on disk.

Resume from another process with just the session_id:

```python
agent = Agent("restored", session_id="abc123")
response = await agent.arun("What were we discussing?")
```

The server looks up the session in the DB, ensures a live sandbox (reprovisioning against the session's volume if needed) via the `SessionPool`, and replays the conversation history via ACP `session/load`.

## Architecture

```
┌───────────┐      ┌──────────────────┐      ┌─────────────────────┐
│ SDK       │─────▶│ API server       │─────▶│ supervisor.js       │
│ (Agent)   │      │ (orchestrator)   │      │ (stdio ⇄ POST+SSE   │
└───────────┘      │  + SessionPool   │      │  bridge to ACP)     │
      │            └──────────────────┘      └──────────┬──────────┘
      │                    │                            │ stdio
      │              Postgres                           ▼
      │              (agents, volumes, sessions,    claude-agent-acp
      │               session_log)                  or codex-acp
   /sessions/*   — conversation                          │
                  (message, events, resume,    mounts volume subpath as HOME
                   release, config, acp/call)
```

- **Volumes** are durable storage — created once, live for months, hold `~/.claude`, transcripts, workspace. Provider-scoped (`POST /volumes`).
- **Sandboxes** are ephemeral compute leases. Each sandbox mounts a volume at a subpath (for sessions, `agents/<agent_id>/home`) and can be killed freely. Sandbox identity is an opaque provider `sandbox_ref` stored in `sessions.sandbox_state` (JSONB) — there is no separate `sandboxes` table.
- **Sessions** hold conversation state and bind to a `volume_id` (immutable). The active compute lease is owned by the `SessionPool` (in-process, at-most-one `SandboxSession` per session_id). `sandbox_state` JSONB is the durable cold-recovery fingerprint.
- **SessionPool** is the single recovery surface. `pool.get_session(sid)` cold-creates from `sandbox_state` if no lease exists, or returns the warm one if it does. `pool.release(sid)` snapshots the volume + drops the compute lease (idempotent hibernation).
- **Supervisor** is a thin node process (`src/supervisor/supervisor.js`) that spawns the agent's ACP binary and exposes it over `/v1/acp/{id}` POST+SSE. One supervisor per sandbox; it installs onto the volume so reattach doesn't repay the `npm install` cost.
- **Providers**: `local` (supervisor subprocess on host), `docker` (supervisor in ephemeral container), `daytona` (supervisor in a Daytona sandbox), `modal` (supervisor in a Modal sandbox). Provider-pluggable, same HTTP surface across all four.

## Providers

| Provider | How the sandbox runs | Sandbox survives stop? |
|---|---|---|
| `local` | Subprocess on the host | No — process killed |
| `docker` | Docker container | No — container removed |
| `daytona` | Daytona cloud workspace | Yes — workspace stopped, filesystem preserved |
| `modal` | Modal sandbox | No — sandbox terminated; volume preserved |

Session data (HOME, transcripts, workspace) lives on the volume, so the session survives sandbox death on every provider — the next `/message` reprovisions a new sandbox that mounts the same volume. The column above is sandbox-level behavior only.

## Docs

- [API reference](docs/api.md) — REST endpoints + `ApiClient` method table
- [Local dev](docs/local-dev.md) — Docker setup, env vars
- [Session runtime model](docs/session-runtime-refactor.md) — the SessionPool / SandboxSession lifecycle
- [Runtime image unification](docs/runtime-image-unification.md) — design doc for moving runtime artifacts off volumes into a baked image

## Layout

```
src/
  agent_sdk/         Python SDK client
    api_client.py      ApiClient (operator persona — flat, one method per route)
    client.py          Agent (user persona — single-session UX)
    errors.py
    persist.py         SQLite session persistence
  api/               Orchestration server
    server.py          FastAPI endpoints
    db.py              Postgres CRUD (agents, volumes, sessions, session_log)
    models.py          AgentRecord, VolumeRecord, AgentConfig
    acp_client.py      JSON-RPC ACP client (POST + SSE)
    sse.py             SSE parsing
    redact.py          Secret redaction for logs
    providers/         Volume backend per provider
      _shared.py
      local.py
      docker.py
      daytona.py
      modal.py
    sandbox/           SessionPool + ephemeral SandboxSession
      pool.py            SessionPool — single recovery surface
      session.py         BaseSandboxSession (start/run/stop/shutdown)
      state.py           Pydantic discriminated SandboxState (per-provider)
      factory.py         state.type → SandboxSession class dispatch
      runtime.py         Process-singleton get_pool() + idle reaper
      liveness.py        Per-session liveness oracle
      providers/         Per-provider SandboxSession (daytona, docker, unix_local, modal)
  supervisor/        Node stdio ⇄ HTTP bridge to claude-agent-acp / codex-acp
    supervisor.js
    package.json
    Dockerfile
tests/               pytest
examples/            Demo scripts
docs/                Docs
assets/              data-model.html, rest-api.html, architecture diagrams
ui/                  Browser-served HTML (chat, dashboard, fs, volumes)
docker-compose.yml   Postgres + API server
Dockerfile
```

## Tests

Run pytest with `-n auto` (pytest-xdist) — sequential runs of the daytona/docker
golden suites take 8–15 min and waste iteration time:

```bash
.venv/bin/python -m pytest tests/ -n auto
```

`-n auto` is fine even when filtering with `-k` — pytest-xdist negotiates worker
count down to the number of selected items.

For the golden tests that need a live server, launch via `scripts/launch_server_test.sh`
(NOT `launch_server_local.sh` directly) — the test wrapper sets `AGENT_SDK_ORIGIN=test`
so daytona sandboxes get labelled `agent_sdk_origin=test` and stay isolatable
from real production traffic.
