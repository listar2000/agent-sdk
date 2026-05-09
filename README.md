# Agent SDK

Python SDK and orchestration server for running Claude Code, Codex, OpenCode, and other ACP-compatible agents in sandboxes (`unix_local`, `docker`, `daytona`, `modal`).

## Run

```bash
echo "CLAUDE_CODE_OAUTH_TOKEN=..." > .env       # or ANTHROPIC_API_KEY=sk-ant-...
echo "OPENROUTER_API_KEY=..."     >> .env       # for --agent opencode
echo "DAYTONA_API_KEY=dtn_..."    >> .env       # optional, for cloud sandboxes

scripts/launch_server_test.sh &
python examples/demo.py --test                              # claude + unix_local
python examples/demo.py daytona --test                      # claude + daytona
python examples/demo.py --test --agent opencode             # opencode + unix_local
python examples/demo.py daytona --test --agent opencode     # opencode + daytona
```

For a managed venv + Postgres without compose, use `scripts/launch_server_docker.sh` (Docker Postgres) or `scripts/launch_server_test.sh` (project-local conda Postgres). All three local-dev paths default `AGENT_SDK_ORIGIN=test` so daytona sandboxes are isolatable from production; override with `AGENT_SDK_ORIGIN=production <launcher>`.

### Environment variables

| Variable | Default | Purpose |
|---|---|---|
| `DATABASE_URL` | `postgresql://localhost:5432/agent_sdk_server` | Postgres conn string |
| `CLAUDE_CODE_OAUTH_TOKEN` / `ANTHROPIC_API_KEY` | — | Claude auth (OAuth preferred) |
| `OPENAI_API_KEY` | — | Required for Codex |
| `AGENT_SDK_REAPER_IDLE_S` / `_INTERVAL_S` | `180` / `60` | Pool hibernation |
| `AGENT_SDK_ORIGIN` | `production` (server default); `test` from local-dev launchers | Daytona label `cleanup_orphans.py` keys off |

## Deploy to Railway

`Dockerfile` + `railway.toml` are ready. Point a Railway project at the repo, add a Postgres service (`DATABASE_URL` is automatic), and on the API service set `CLAUDE_CODE_OAUTH_TOKEN` (or `ANTHROPIC_API_KEY`) and `DAYTONA_API_KEY`. `DAYTONA_SNAPSHOT` is optional; defaults to `.runtime-snapshot-tag` (committed by `scripts/release.sh`). Use `provider="daytona"` — Railway containers are ephemeral.

## Use the SDK

```python
from agent_sdk import Agent

agent = Agent("worker", provider="unix_local")
response = await agent.arun("Create hello.py")

async for ev in agent.astream("Analyze this codebase"):
    print(ev, end="", flush=True)              # str(ev) → text; ev["type"] → text|reasoning|tool|done|...

await agent.arun("focus on X instead", interrupt=True)   # cancel + resubmit
await agent.send("do this next")                          # fire-and-forget; pair with .events()
```

Default server is `http://localhost:7778`; override via `api_url=` or `AGENT_API_URL=`.

Agent identity is pure: `agent_type`, `model`, `mcp_servers`, `skills`, `mode`, `thought_level`. Per-session knobs (`cwd`, `env`, `secrets`, `workspace`) and provisioning knobs (`dockerfile`, `shared_mounts`, `root`, `pre_start_commands`, `volume_id`) live on the session.

### Session persistence

Sessions survive server restarts AND sandbox death. The server persists `{session_id, agent_id, volume_id, sandbox_state, inner_session_id}` to Postgres; the agent's HOME lives on the volume, not the sandbox. The next `/message` after a sandbox dies lazily reprovisions and resumes from the on-disk transcript. Resume from anywhere:

```python
agent = Agent("restored", session_id="abc123")
await agent.arun("What were we discussing?")
```

### Shared workspace

Two agents (or sessions) bind the same HOME by passing the same `workspace=` name. Each runs on its own sandbox; the volume's `workspaces/<name>/` subpath is mounted as `/home/agent`.

```python
a = Agent("alice", provider="docker", workspace="team-alpha")
b = Agent("bob",   provider="docker", workspace="team-alpha")
await asyncio.gather(a.arun("write notes.md"), b.arun("read notes.md"))
```

Names are `[a-z0-9][a-z0-9._-]{0,63}`. Supported on `unix_local`, `docker`, `modal`; rejected (HTTP 400) on `daytona` — S3-FUSE + tarball snapshots can't coordinate concurrent writers.

### Per-request Claude credentials

```python
agent = Agent("worker", provider="daytona", oauth_token=user_oauth_token)
# fallback: oauth_token= > CLAUDE_CODE_OAUTH_TOKEN env > api_key= > ANTHROPIC_API_KEY env
```

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

- **Volumes** — durable storage (`~/.claude`, transcripts, workspace), provider-scoped.
- **Sandboxes** — ephemeral compute. Mount a volume at `agents/<agent_id>/home`; identity is an opaque `sandbox_ref` in `sessions.sandbox_state` JSONB. No `sandboxes` table.
- **Sessions** — conversation state, bound to an immutable `volume_id`. `SessionPool` owns the at-most-one warm `SandboxSession` per `session_id`; `release(sid)` snapshots and drops the lease.
- **Supervisor** — `src/supervisor/supervisor.js` spawns the ACP binary and exposes it over `/v1/acp/{id}` POST+SSE. One per sandbox; runtime baked into the agent-sdk image at `/opt/agent-sdk/runtime/`.

Session data lives on the volume, so sessions survive sandbox death on every provider — the column above is sandbox-level only.

## Tests

```bash
.venv/bin/python -m pytest tests/ -n auto
```

`-n auto` is mandatory (sequential daytona/docker is 8–15 min). For golden tests against a live server, use `scripts/launch_server_test.sh`.

## Docs

- [API reference](docs/api.md) — REST endpoints + `ApiClient` table
