# Agent SDK

Python SDK and orchestration server for running Claude Code, Codex, OpenCode, and other ACP-compatible agents in sandboxes (`unix_local`, `docker`, `daytona`, `modal`).

## Run with Docker

```bash
echo "CLAUDE_CODE_OAUTH_TOKEN=..." > .env       # or ANTHROPIC_API_KEY=sk-ant-...
echo "DAYTONA_API_KEY=dtn_..." >> .env          # optional, for cloud sandboxes
docker compose up --build -d
curl http://localhost:7778/health               # → {"status":"ok",...}
```

Postgres on `5433`, API on `7778`, chat UI at `http://localhost:7778/ui`. Stop with `docker compose down` (`-v` also drops the database volume).

For a managed venv + Postgres, use `scripts/launch_server_docker.sh` (Docker Postgres) or `scripts/launch_server_local.sh` (project-local conda Postgres, no Docker). See [`docs/local-dev.md`](docs/local-dev.md).

## Deploy to Railway

`Dockerfile` + `railway.toml` are ready. Point a Railway project at this repo, add a Postgres service (`DATABASE_URL` is set automatically), then on the API service set:

- `CLAUDE_CODE_OAUTH_TOKEN` (or `ANTHROPIC_API_KEY`)
- `DAYTONA_API_KEY` (cloud sandboxes)
- `DAYTONA_SNAPSHOT` (optional; defaults to `.runtime-snapshot-tag` committed by `scripts/release.sh`. Override with another snapshot name, or `image` / `0` / `false` to fall back to `DAYTONA_IMAGE` / `AGENT_SDK_IMAGE` / `.runtime-image-tag`)
- `AGENT_SDK_REAPER_IDLE_S` (default 180) and `AGENT_SDK_REAPER_INTERVAL_S` (default 60)

Use `provider="daytona"` in production — Railway containers are ephemeral.

## Database

Postgres. `init_db()` runs `CREATE TABLE IF NOT EXISTS` + idempotent `ALTER TABLE` migrations (`_MIGRATIONS` in `src/api/db.py`) on every startup. Migrations must be `IF [NOT] EXISTS`-safe; append new ones to the bottom of the list.

Tables: `agents`, `volumes`, `sessions`, `session_log`. Sandbox identity lives in `sessions.sandbox_state` JSONB, owned by the in-process `SessionPool` — there is no `sandboxes` table.

## Use the SDK

```python
from agent_sdk import Agent

agent = Agent("worker", provider="local")

response = await agent.arun("Create a file called hello.py")

async for ev in agent.astream("Analyze this codebase"):
    print(ev, end="", flush=True)              # str(ev) → text; ev["type"] → "text"|"reasoning"|"tool"|"done"|...

response = agent.run("Say hello")              # sync wrapper
response = await agent.arun("focus on X instead", interrupt=True)

rpc_id = await agent.send("do this next")      # fire-and-forget; pair with agent.events()
rpc_id = await agent.send("cancel and do this", interrupt=True)
```

`interrupt=True` is client-side: cancel the running prompt, wait for the terminal block, then submit. `send()` returns immediately. Default server is `http://localhost:7778`; override with `api_url=` or `AGENT_API_URL=`.

Agent identity is pure: `agent_type`, `model`, `mcp_servers`, `skills`, `mode`, `thought_level`. Per-session knobs (`cwd`, `env`, `secrets`, `workspace`) and provisioning knobs (`dockerfile`, `shared_mounts`, `root`, `pre_start_commands`, `volume_id`) live on the session.

## Shared workspace (multi-agent collaboration)

Two or more agents (or sessions of different agents) can bind the same HOME directory by passing the same `workspace=` name. Each runs on its own sandbox; the volume's `workspaces/<name>/` subpath is mounted as `/home/agent` so they collaborate on one codebase, share files in real time, and see each other's writes via the kernel.

```python
a = Agent("alice", provider="docker", workspace="team-alpha")
b = Agent("bob",   provider="docker", workspace="team-alpha")
await asyncio.gather(a.arun("write notes.md"), b.arun("read notes.md"))
```

Override per-session (one Agent identity, multiple projects):

```python
agent = Agent("user", provider="docker", workspace="default-project")
s1 = agent.create_session()                            # workspaces/default-project
s2 = agent.create_session(workspace="other-project")   # workspaces/other-project
```

`workspace` is opt-in. When unset, HOME falls back to the per-agent `agents/<agent_id>/` (today's behavior). Names are normalized to `[a-z0-9][a-z0-9._-]{0,63}` server-side. Supported on `unix_local`, `docker`, and `modal`; rejected with HTTP 400 on `daytona` (S3-FUSE + tarball snapshots can't coordinate concurrent writers across two sandboxes).

## Per-request Claude credentials

Pass an OAuth token on the `Agent` to use a specific account instead of the server's default:

```python
agent = Agent("worker", provider="daytona", oauth_token=user_oauth_token)
# fallback order: oauth_token= > CLAUDE_CODE_OAUTH_TOKEN env > api_key= > ANTHROPIC_API_KEY env
```

Credentials travel in the `secrets` field on `POST /sessions` and `/resume`, are stored on the session row (redacted from read APIs), and are applied as per-sandbox env vars in the supervisor. When OAuth is supplied, the server scrubs its own `ANTHROPIC_API_KEY` from that sandbox so there's no silent fallback. The SDK refuses to send credentials over plaintext HTTP — use `https://` or a `localhost` URL. Obtaining the token (`claude setup-token`) is the caller's responsibility.

## Session persistence

Sessions survive server restarts AND sandbox death. The server persists `{session_id, agent_id, volume_id, sandbox_state, inner_session_id}` to Postgres; the agent's HOME (`~/.claude`, transcripts, workspace) lives on the volume, not the sandbox. When a sandbox dies or is reaped, the session row remains; the next `/message` lazily reprovisions a new sandbox that mounts the same volume, and the CLI resumes from the on-disk transcript.

Resume from another process with just the session_id:

```python
agent = Agent("restored", session_id="abc123")
response = await agent.arun("What were we discussing?")
```

The server looks up the session, ensures a live sandbox via the `SessionPool`, and replays history via ACP `session/load`.

## Architecture

```
┌───────────┐      ┌──────────────────┐      ┌─────────────────────┐
│ SDK       │─────▶│ API server       │─────▶│ supervisor.js       │
│ (Agent)   │      │  + SessionPool   │      │  (POST+SSE ⇄ ACP)   │
└───────────┘      └──────────────────┘      └──────────┬──────────┘
                          │                             │ stdio
                    Postgres                            ▼
                    (agents, volumes,            claude-agent-acp /
                     sessions, session_log)       codex-acp / ...
```

- **Volumes** — durable storage. Hold `~/.claude`, transcripts, workspace. Provider-scoped.
- **Sandboxes** — ephemeral compute leases. Each mounts a volume at a subpath (`agents/<agent_id>/home`); identity is an opaque `sandbox_ref` in `sessions.sandbox_state` JSONB.
- **Sessions** — conversation state, bound to an immutable `volume_id`. Active lease owned by `SessionPool` (in-process, at-most-one `SandboxSession` per session_id). `sandbox_state` is the durable cold-recovery fingerprint.
- **SessionPool** — the single recovery surface. `get_session(sid)` cold-creates from state or returns the warm one. `release(sid)` snapshots + drops the lease (idempotent hibernation).
- **Supervisor** — thin Node process (`src/supervisor/supervisor.js`) that spawns the ACP binary and exposes it over `/v1/acp/{id}` POST+SSE. One per sandbox; runtime ships baked in the agent-sdk image at `/opt/agent-sdk/runtime/`.
- **Providers** — `unix_local` (host subprocess), `docker` (ephemeral container), `daytona` (Daytona sandbox), `modal` (Modal sandbox). Same HTTP surface across all four.

## Providers

| Provider | Sandbox runs as | Sandbox survives stop? |
|---|---|---|
| `unix_local` | Host subprocess | No — process killed |
| `docker` | Ephemeral container | No — container removed |
| `daytona` | Daytona workspace | Yes — workspace paused, FS preserved |
| `modal` | Modal sandbox | No — sandbox terminated; volume preserved |

Session data lives on the volume, so the session survives sandbox death on every provider — the next `/message` reprovisions and remounts. The column above is sandbox-level only.

## Docs

- [API reference](docs/api.md) — REST endpoints + `ApiClient` table
- [Local dev](docs/local-dev.md) — Docker setup, env vars

## Tests

```bash
.venv/bin/python -m pytest tests/ -n auto
```

`-n auto` is mandatory (sequential daytona/docker is 8–15 min). Fine with `-k` filters.

For golden tests against a live server, use `scripts/launch_server_test.sh` (NOT `launch_server_local.sh` directly) — it sets `AGENT_SDK_ORIGIN=test` so daytona sandboxes are labelled isolatable from production.
