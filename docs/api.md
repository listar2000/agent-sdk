# REST API

Base URL: `http://localhost:7778`. Two top-level resources:

- **Volumes** — durable storage; CRUD + file ops, no sandbox required.
- **Sessions** — conversation, bound to a volume. Compute lease lives in the in-process `SessionPool`; sandbox identity (`sandbox_ref`) is in `sessions.sandbox_state` JSONB. There is no `/sandboxes` resource — `GET /sessions/{id}/sandbox` returns the metadata.

Plus a thin **Agents** group for registering an agent identity without provisioning compute.

## Health & UI

```
GET /health        → {"status":"ok","sessions":1,"busy_sessions":0}
GET /ui            — chat
GET /ui/dashboard  — admin / validation
GET /ui/files      — per-session FS browser
GET /ui/volumes    — volume inspector
```

`sessions` = pool-leased sessions; `busy_sessions` = subset with at least one event subscriber.

## Sessions

### `POST /sessions` — create

Eager by default (provisions sandbox, attaches supervisor + ACP, persists `sandbox_state`); pass `"provision": false` for the lazy flow (row only; sandbox materialises on first `/message` or `/resume`). Fields can sit at the top level OR under `config`; top-level wins.

| Field | Belongs on | Notes |
|---|---|---|
| `agent_type`, `model`, `mcp_servers`, `skills`, `mode`, `thought_level` | agent | Persisted on `agents.config`; replayed on cold-recovery. |
| `cwd`, `env`, `secrets`, `workspace` | session | Per-conversation. `env`/`secrets` are PATCH-shaped (omitted=keep, `{}`=clear, `{...}`=replace). `cwd` keys the JSONL hash. |
| `dockerfile`, `dockerfile_content`, `shared_mounts`, `root`, `pre_start_commands`, `volume_id` | sandbox | Frozen at provision; survive replacement via `sandbox_state.recipe`. |

`volume_id` is optional — when omitted, a `default-{provider}` volume is created/reused. `pre_start_commands` (effective list = `skills_install_commands + caller_pre_start_commands`) run inside the sandbox before the supervisor starts and re-run on replacement (Type 2) recovery, not on same-VM restart (Type 1). `local` ignores them.

`workspace`: when set, HOME inside the sandbox becomes `<volume>/workspaces/<workspace>/` instead of `<volume>/agents/<agent_id>/`; two sessions on the same volume + same `workspace` see each other's writes. Server normalises (lowercases, trims) and rejects shapes outside `[a-z0-9][a-z0-9._-]{0,63}` with HTTP 400. Daytona returns 400 (S3-FUSE + tarball snapshots can't coordinate concurrent writers).

```json
{
  "name": "worker", "provider": "local",
  "agent_type": "claude", "model": "claude-sonnet-4-6",
  "cwd": "/tmp", "workspace": "team-alpha",
  "mcp_servers": {"name": {"type":"local","command":"...","args":[]}},
  "skills": ["rllm-org/hive#staging"],
  "shared_mounts": ["shared-data"],
  "pre_start_commands": ["uv tool install hive-evolve"]
}

→ {agent_id, session_id, id, volume_id, sandbox_ref, inner_session_id, connected: true}
```

Lazy returns the same shape with `sandbox_ref: null`, `connected: false`, and accepts an optional `agent_id` to reuse an existing config. `sandbox_ref` is opaque (Daytona UUID, Docker container ID, local PID-as-string).

### `POST /sessions/{id}/message` — send

```json
{"message":"analyze the dataset"}  → {"rpc_id":"...","status":"queued"}
```

Returns immediately; events go to `session_log` and any `/events` subscribers. The `interrupt` flag is a no-op here — call `POST /cancel` first, then submit.

### `POST /sessions/{id}/message+stream` — send + stream

Submits a prompt and streams the reply as SSE. Same wire format as `/events`, scoped to one `rpc_id`. `: heartbeat\n\n` keeps idle connections open. `Agent.astream()` uses this.

### `GET /sessions/{id}/events` — multi-subscriber stream

Every event broadcast by the session, across all prompts, plus heartbeats every ~20 s. Multiple concurrent connections each get a copy.

```
event: rpc:<rpc_id>
<raw acp block>
```

Untagged blocks are still possible (heartbeats, setup chatter). Wrappers may emit `notifications/session/update`; treat both forms equivalently.

| sessionUpdate | Payload |
|---|---|
| `agent_message_delta` / `agent_message_chunk` | `{content: {text, type?: "text"}}` |
| `agent_message_delta` / `agent_message_chunk` (thinking) | `{content: {thinking, type: "thinking"}}` |
| `agent_thought_chunk` | `{content: {text\|thinking}}` |
| `tool_call` / `execute_tool_started` | `{_meta: {claudeCode: {toolName, toolUseId}}, rawInput}` |
| `tool_call_update` | `{_meta: {claudeCode: {toolResponse\|toolResult, toolName, toolUseId}}}` |
| `usage_updated` / `usage_update` | `{cost: {amount, currency}}` |

Terminal blocks:

```json
{"jsonrpc":"2.0","id":"<rpc_id>","result":{"stopReason":"end_turn|cancelled"}}
{"jsonrpc":"2.0","id":"<rpc_id>","error":{"code":-32000,"message":"...","data":{
  "kind":"sandbox_process_died|sandbox_internal_error|http_error|sandbox_unreachable|timeout|unknown",
  "exception_type":"...","http_status":500,"upstream_body":"...","rpc_id":"<rpc_id>"
}}}
```

### Lifecycle

```
POST /sessions/{id}/resume    — idempotent pre-warm (cold-creates from sandbox_state if no lease exists). Optional body: env, secrets (PATCH).
POST /sessions/{id}/release   — snapshot + drop lease. Idempotent.
DELETE /sessions/{id}         — destroys the sandbox (not paused) + deletes the row. 204 even when missing.
POST /sessions/{id}/cancel    — best-effort `session/cancel`. No active lease → {"status":"ok","detail":"no active lease"}.
```

`/message`, `/message+stream`, `/events`, `/cancel`, `/config`, `/files/*`, `/sandbox/exec`, `/acp/call` all auto-recover via the pool — no need to call `/resume` first. Row reads (`/sessions`, `GET /sessions/{id}`) do NOT trigger recovery; `/sessions/{id}/status` and `/sessions/{id}/sandbox` DO.

`DELETE`'s destructive contract is pinned by `tests/test_golden.py::test_delete_session_destroys_sandbox`. For paused-on-release residue (idle reaper / explicit `/release`), `scripts/cleanup_orphans.py` reclaims compute later — defaults to `--origin test`, `--provider all`.

### `POST /sessions/{id}/config`

```json
{"mode":"bypassPermissions","model":"claude-sonnet-4-6","thought_level":"high"}
```

Patches the three persisted-and-replayed knobs. Applied to the live ACP session AND persisted on `agents.config` so cold-recovery replays them. For anything else (vendor extensions, debugging) use `/acp/call`.

### `POST /sessions/{id}/acp/call`

```json
{"method":"session/whatever","params":{...},"notify":false}
```

Forwards JSON-RPC to the supervisor. Auto-injects the inner `sessionId` into `params`. `notify=true` sends as a notification (no response). Transient — lost on the next sandbox restart; for replay-on-recovery use `/config` or bake into the recipe.

### Reads

```
GET /sessions                  — list pool-leased sessions
GET /sessions/{id}             — row (env + redacted secret keys + sandbox_ref + pre_start_commands)
GET /sessions/{id}/status      — runtime status (brings up SandboxSession if hibernated)
GET /sessions/{id}/sandbox     — {provider, sandbox_ref, status, root, url, marker_path}
GET /sessions/{id}/log?limit=N — event log, oldest first
```

`/status` fields: `session_id`, `agent_id`, `inner_session_id`, `sandbox_ref`, `session_subscriber_count`, `last_activity` / `idle_seconds`, `has_client`, `supervisor_url` / `supervisor_port` (null on daytona). Plus back-compat constants for `ui/dashboard.html` (`agent_busy`, `active_rpc_id`, `pending_count`, `rpc_subscriber_count`, `shutdown_requested`, `available_commands`) — not load-bearing.

#### Session log event types

Each row: `event_type`, `payload`, `created_at`, `session_id`. `prompt_id` ≡ the `rpc_id` from `POST /message`; `tool_call_id` links `tool_call` to `tool_result`.

| event_type | Payload |
|---|---|
| `user_message` / `assistant_message` | `{text, prompt_id}` |
| `reasoning` | `{text, prompt_id}` (Claude thinking blocks) |
| `tool_call` / `tool_result` | `{tool, tool_call_id, prompt_id, args?\|result}` |
| `usage` | `{prompt_id, ...cost fields}` |
| `turn_end` | `{stop_reason, prompt_id, usage?}` |
| `error` | `{message, kind, prompt_id, traceback?}` |

### Session sandbox helpers

```
POST /sessions/{id}/sandbox/exec    {"command":"pwd","timeout":30}
                                    → {stdout, stderr, exit_code, *_truncated, timed_out}

GET    /sessions/{id}/files/tree
GET    /sessions/{id}/files/read?path=…
POST   /sessions/{id}/files/edit       — overwrite or {old_string,new_string,replace_all?}
POST   /sessions/{id}/files/upload     — {path, content (base64)}
POST   /sessions/{id}/files/{delete,rename}
GET    /sessions/{id}/files/download?path=…
```

`exec` routes through the pool (auto-reprovisions a reaped sandbox); does not require an active ACP session. File ops are proxied through the supervisor.

## Volumes

```
POST   /volumes                              — create + wait for status="ready"
GET    /volumes                              — list (?provider= filter)
GET    /volumes/{id_or_name}                 — get (name lookup OK)
DELETE /volumes/{id_or_name}?force=false     — 409 if any session refs it; force=true cascades

GET    /volumes/{id}/files/{tree,read,exists,download}?path=…
POST   /volumes/{id}/files/edit              — {path, content} OR {path, old_string, new_string, replace_all?}
POST   /volumes/{id}/files/{upload,mkdir,delete}
POST   /volumes/{id}/files/rename            — {path, new_path, overwrite=true}
```

File ops hit provider primitives directly — no live sandbox. On `rename` with `overwrite=false`, providers use an atomic no-overwrite primitive; on collision the API returns 409 `{"error":"exists","path":new_path}` and leaves `path` untouched. Providers without atomic no-overwrite return a clear unsupported error rather than falling back to a pre-check.

Volume names: `[A-Za-z0-9][A-Za-z0-9._-]{0,63}`.

## Agents (config only)

```
POST   /agents       — register agent config (no sandbox)
GET    /agents       — list
GET    /agents/{id}  — get
DELETE /agents/{id}  — delete
```

`POST /agents` rejects non-agent keys (`cwd`, `env`, `dockerfile`, `dockerfile_content`, `shared_mounts`) with 400. Agent config is `agent_type`, `model`, `mcp_servers`, `skills`, `mode`, `thought_level`.

## Admin

```
GET /admin/sessions   — pool snapshot for the dashboard
```

## Python SDK

`Event`: streaming methods yield `Event` (a dict subclass). `str(ev)` → text; `ev["type"]` ∈ `{text, reasoning, tool, tool_result, usage, done, error}`.

### `Agent`

```python
agent.arun(message, *, interrupt=False) -> str            # full response
agent.astream(message, *, interrupt=False) -> AsyncIterator[Event]   # POST /message+stream
agent.run(message, timeout=None, *, interrupt=False) -> str          # sync wrapper
agent.send(message, *, interrupt=False) -> str            # fire-and-forget; returns rpc_id
agent.cancel()                                            # best-effort
agent.configure(**kwargs)                                 # mode / model / thought_level
agent.events()                                            # async ctx mgr; long-lived /events
agent.aclose() / async with                               # POST /release + close httpx
```

`interrupt=True` is client-side: cancel, wait for terminal block, then submit. Multiple concurrent `events()` contexts each get a fan-out copy. Error events are **yielded**, not raised; connection failure raises `StreamError`.

### `ApiClient` — operator persona

`agent_sdk.ApiClient` is a flat async wrapper over every REST route. Use it from services that operate on other people's sessions (admin tooling, hive bootstrap, bench scripts). `Agent` is the right choice when your code IS the user.

```python
async with ApiClient(base_url="https://...", token="optional-bearer") as sc:
    session = await sc.create_session(provider="daytona", model="claude-sonnet-4-6")
    await sc.send_message(session["session_id"], "hello")
    await sc.release_session(session["session_id"])
```

Stateless w.r.t. resources (every method takes IDs); one `httpx.AsyncClient` per instance; one method per route. Errors raise `httpx.HTTPStatusError` with the server's `{"error": ...}` body attached. Pass `http_client=` for custom transports.

| Resource | Methods |
|---|---|
| Volumes | `create_volume`, `list_volumes(provider=None)`, `get_volume`, `delete_volume(force=False)` |
| Volume files | `volume_file_{tree,read,download,exists,upload,mkdir,delete}(volume_id, path[, content])`; `volume_file_write(volume_id, path, content)`; `volume_file_edit(volume_id, path, *, old_string, new_string, replace_all=False)`; `volume_file_rename(volume_id, path, new_path, overwrite=True)` |
| Agents | `create_agent(**body)` |
| Sessions — lifecycle | `create_session(**body)` (eager; `provision=False` for lazy); `list_sessions`; `get_session(id)` / `get_session_status(id)` / `get_session_sandbox(id)` / `get_session_log(id, limit=500)`; `resume_session(id, **body)`; `release_session(id)`; `delete_session(id)` (idempotent) |
| Sessions — runtime | `send_message(id, text, interrupt=False)`; `send_message_stream(id, text, interrupt=False)` async-iter; `cancel_session(id)`; `set_session_config(id, **config)`; `acp_call(id, method, params=None, *, notify=False)`; `session_sandbox_exec(id, command, timeout=30)`; `stream_events(id)` async-iter |
| Session files | `session_file_{tree,read,download,upload,delete,rename}(id, ...)`; `session_file_edit(id, path, *, old_string, new_string, replace_all=False)` |

For ACP knobs not covered by typed wrappers, use `acp_call`.
