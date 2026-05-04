# REST API

Base URL: `http://localhost:7778`.

Two top-level resources:

- **Volumes** — durable storage; CRUD + file ops, no sandbox required.
- **Sessions** — conversation, bound to a volume. Compute lease owned by the in-process `SessionPool`; sandbox identity (`sandbox_ref`) lives in `sessions.sandbox_state` JSONB. There is no `/sandboxes` resource — `GET /sessions/{id}/sandbox` returns the metadata.

Plus a thin **Agents** group for registering an agent identity without provisioning compute.

## Health

```
GET /health
→ {"status": "ok", "sessions": 1, "busy_sessions": 0}
```

`sessions` = sessions currently leased by the pool. `busy_sessions` = the subset with at least one event subscriber.

## Chat UI

```
GET /ui            — chat
GET /ui/dashboard  — admin / validation
GET /ui/files      — per-session FS browser
GET /ui/volumes    — volume inspector
```

## Sessions

### Create — `POST /sessions`

Eager by default: provisions a sandbox, attaches supervisor + ACP, persists `sandbox_state`. Pass `"provision": false` for the lazy flow (session row only; sandbox materialises on first `/message` or `/resume`).

Config fields belong to one of three groups:

| Field | Belongs on | Notes |
|---|---|---|
| `agent_type`, `model`, `mcp_servers`, `skills`, `mode`, `thought_level` | agent | Persisted on `agents.config`; replayed on cold-recovery. |
| `cwd`, `env`, `secrets`, `workspace` | session | Per-conversation. `env`/`secrets` are PATCH-shaped (omitted=keep, `{}`=clear, `{...}`=replace). `cwd` keys the JSONL hash. `workspace` overrides HOME — see below. |
| `dockerfile`, `dockerfile_content`, `shared_mounts`, `root`, `pre_start_commands`, `volume_id` | sandbox | Frozen at provision time; survive replacement via the recipe on `sandbox_state`. |

Fields can sit at the top level OR under `config`; top-level wins. `volume_id` is optional — when omitted, a `default-{provider}` volume is created/reused.

```json
{
  "name": "worker",
  "provider": "local",
  "agent_type": "claude",
  "model": "claude-sonnet-4-6",
  "cwd": "/tmp",
  "workspace": "team-alpha",
  "mcp_servers": {"name": {"type": "local", "command": "...", "args": []}},
  "skills": ["rllm-org/hive#staging"],
  "shared_mounts": ["shared-data"],
  "pre_start_commands": ["uv tool install hive-evolve"]
}
```

#### `workspace` — shared HOME across agents

When set, the session's HOME inside the sandbox becomes `<volume>/workspaces/<workspace>/` instead of the per-agent `<volume>/agents/<agent_id>/`. Two or more sessions (from any agents) on the same volume + same `workspace` value mount the same directory and see each other's writes via the kernel.

- **Normalization**: server lowercases + trims, must match `[a-z0-9][a-z0-9._-]{0,63}`. Bad shape → HTTP 400.
- **Provider support**: `unix_local`, `docker`, `modal`. Daytona returns HTTP 400 (S3-FUSE projection + `/vol/snapshot.tar` round-trip can't safely coordinate two writers).
- **Storage**: top-level column `sessions.workspace TEXT NULL`. Surfaced in `GET /sessions/{id}` and the create-response.
- **JSONL history**: workspace IS the unit of shared state — Claude's `~/.claude/projects/...` lives in HOME, so two sessions in the same workspace share JSONL history; two sessions in different workspaces don't.

`pre_start_commands` run inside the sandbox before the supervisor starts (install CLIs, lay down config). `local` ignores them (no sandbox boundary). The effective list is `skills_install_commands + caller_pre_start_commands`, persisted on `sandbox_state.recipe.pre_start_commands`. They re-run on replacement (Type 2) recovery; not on same-VM restart (Type 1) — original side effects survive on disk.

Returns (eager):

```json
{
  "agent_id": "uuid", "session_id": "uuid", "id": "uuid",
  "volume_id": "vol-uuid",
  "sandbox_ref": "<opaque provider ref>",
  "inner_session_id": "<acp inner session id>",
  "connected": true
}
```

Lazy returns the same shape with `sandbox_ref: null`, `connected: false`, and accepts an optional `agent_id` to reuse an existing agent config.

`sandbox_ref` is opaque (Daytona UUID, Docker container ID, local PID-as-string).

### Send message — `POST /sessions/{id}/message`

```json
{"message": "analyze the dataset"}
→ {"rpc_id": "...", "status": "queued"}
```

Returns immediately; events go to `session_log` and any `/events` subscribers.

`interrupt` flag is a no-op here — call `POST /cancel` first, then submit.

### Send + stream — `POST /sessions/{id}/message+stream`

Submits a prompt and streams the reply as SSE in the response body. Same wire format as `/events`, scoped to one `rpc_id`. `: heartbeat\n\n` keeps idle connections open. `Agent.astream()` uses this.

### Multi-subscriber stream — `GET /sessions/{id}/events`

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
{"jsonrpc":"2.0","id":"<rpc_id>","result":{"stopReason":"end_turn"}}
{"jsonrpc":"2.0","id":"<rpc_id>","result":{"stopReason":"cancelled"}}
{"jsonrpc":"2.0","id":"<rpc_id>","error":{"code":-32000,"message":"...","data":{
  "kind":"sandbox_process_died|sandbox_internal_error|http_error|sandbox_unreachable|timeout|unknown",
  "exception_type":"HTTPStatusError","http_status":500,"upstream_body":"...","rpc_id":"<rpc_id>"
}}}
```

### Resume — `POST /sessions/{id}/resume`

Idempotent pre-warm. Routes through `SessionPool.get_session`: cold-creates from `sandbox_state` if no lease exists, returns the live one if it does. ACP `session/load` happens inside `SandboxSession.start()`. Optional body: `env`, `secrets` with PATCH semantics.

```json
{"session_id":"uuid","agent_id":"uuid","sandbox_ref":"...","inner_session_id":"uuid","status":"resumed"}
```

`/message`, `/message+stream`, `/events`, `/cancel`, `/config`, `/files/*`, `/sandbox/exec`, `/acp/call` all auto-recover via the pool — no need to call `/resume` first. Row reads (`/sessions`, `GET /sessions/{id}`) do NOT trigger recovery; `/sessions/{id}/status` and `/sessions/{id}/sandbox` DO.

### Release (hibernate) — `POST /sessions/{id}/release`

Snapshot to volume + drop the pool's compute lease. Idempotent.

```json
{"lifecycle":"hibernated","snapshot_path":"<volume-relative path or null>","snapshot_version":0}
```

Backed by `SessionPool.release(session_id)`. The reaper invokes the same path on idle sessions (default 180 s; `AGENT_SDK_REAPER_IDLE_S`).

### Delete — `DELETE /sessions/{id}`

Destroys the underlying sandbox (not paused) and deletes the session row. Returns 204 even when missing — safe as a "make sure this is gone" primitive. Pinned by `tests/test_sandbox_stop_delete_recovery.py::test_delete_session_destroys_sandbox`.

For paused-on-release residue (idle reaper / explicit `/release`), `scripts/cleanup_orphans.py` reclaims compute later. Defaults to `--origin test`.

### Cancel — `POST /sessions/{id}/cancel`

Best-effort `session/cancel` (notification) to the live ACP child. The `done` event arrives on the same SSE subscribers as `/message`. No active lease → `{"status":"ok","detail":"no active lease"}`.

### Config — `POST /sessions/{id}/config`

```json
{"mode":"bypassPermissions","model":"claude-sonnet-4-6","thought_level":"high"}
```

Patches the three persisted-and-replayed knobs (`model`, `mode`, `thought_level`). Applied to the live ACP session AND persisted on `agents.config` so cold-recovery replays them. For anything else (vendor extensions, debugging) use `/acp/call` instead of growing a typed wrapper.

### ACP passthrough — `POST /sessions/{id}/acp/call`

```json
{"method":"session/whatever","params":{...},"notify":false}
```

Forwards a JSON-RPC call to the supervisor. Auto-injects the inner `sessionId` into `params`. `notify=true` sends as a notification (no response). Returns `{"result": <ACP result>}` for non-notify calls.

Transient — lost on the next sandbox restart. For replay-on-recovery, use `/config` or bake into the recipe at create time.

### Other

```
GET /sessions                  — list pool-leased sessions
GET /sessions/{id}             — session row (env + redacted secret keys + sandbox_ref + pre_start_commands)
GET /sessions/{id}/status      — runtime status (brings up SandboxSession if hibernated)
GET /sessions/{id}/sandbox     — {provider, sandbox_ref, status, root, url, marker_path}
GET /sessions/{id}/log?limit=500  — event log, oldest first
```

`GET /sessions/{id}/status` fields:

| Field | Description |
|---|---|
| `session_id`, `agent_id`, `inner_session_id` | identifiers |
| `sandbox_ref` | bound sandbox ref, or `null` |
| `session_subscriber_count` | session-scoped SSE subscribers on the SandboxSession |
| `last_activity` / `idle_seconds` | monotonic timestamp + seconds since (or `null`) |
| `has_client` | whether a live supervisor URL exists |
| `supervisor_url`, `supervisor_port` | URL exposed; port for port-based providers (null on daytona) |
| `agent_busy`, `active_rpc_id`, `pending_count`, `rpc_subscriber_count`, `shutdown_requested`, `available_commands` | back-compat constants for `ui/dashboard.html`; not load-bearing |

#### Session log event types

Each row: `event_type`, `payload`, `created_at`, `session_id`.

| event_type | Payload |
|---|---|
| `user_message` | `{text, prompt_id}` |
| `assistant_message` | `{text, prompt_id}` |
| `reasoning` | `{text, prompt_id}` (Claude thinking blocks) |
| `tool_call` | `{tool, tool_call_id, prompt_id, args?}` |
| `tool_result` | `{tool, tool_call_id, prompt_id, result}` |
| `usage` | `{prompt_id, ...cost fields}` |
| `turn_end` | `{stop_reason, prompt_id, usage?}` |
| `error` | `{message, kind, prompt_id, traceback?}` |

`prompt_id` ≡ the `rpc_id` returned by `POST /message`; ties events for one round-trip together. `tool_call_id` links `tool_call` to `tool_result`.

## Session sandbox helper

```
POST /sessions/{id}/sandbox/exec
{"command": "pwd", "timeout": 30}
→ {"stdout":"...","stderr":"...","exit_code":0,"stdout_truncated":false,"stderr_truncated":false,"timed_out":false}
```

Routes through the pool (auto-reprovisions a reaped sandbox). Does not require an active ACP session.

## Session-scoped filesystem

Read/write inside the session's *current* sandbox (proxied through the supervisor; the supervisor itself is reprovisioned on demand).

```
GET    /sessions/{id}/files/tree
GET    /sessions/{id}/files/read?path=…
POST   /sessions/{id}/files/edit       — overwrite or {old_string,new_string,replace_all?}
POST   /sessions/{id}/files/upload     — {path, content (base64)}
POST   /sessions/{id}/files/delete     — {path}
POST   /sessions/{id}/files/rename     — {path, new_path}
GET    /sessions/{id}/files/download?path=…
```

## Volumes

```
POST   /volumes                              — create + wait for status="ready"
GET    /volumes                              — list (?provider= filter)
GET    /volumes/{id_or_name}                 — get (name lookup OK)
DELETE /volumes/{id_or_name}?force=false     — 409 if any session refs it; force=true cascades
GET    /volumes/{id_or_name}/files/tree      — ?path=
GET    /volumes/{id_or_name}/files/read      — ?path=
GET    /volumes/{id_or_name}/files/exists    — ?path=
GET    /volumes/{id_or_name}/files/download  — ?path=
POST   /volumes/{id_or_name}/files/edit      — {path, content} OR {path, old_string, new_string, replace_all?}
POST   /volumes/{id_or_name}/files/upload    — {path, content (base64)}
POST   /volumes/{id_or_name}/files/mkdir     — {path}
POST   /volumes/{id_or_name}/files/delete    — {path}
POST   /volumes/{id_or_name}/files/rename    — {path, new_path, overwrite=true}
```

File ops hit provider primitives directly — no live sandbox. On `rename` with `overwrite=false`, providers use an atomic no-overwrite primitive; on collision the API returns 409 `{"error":"exists","path":new_path}` and leaves `path` untouched. Providers without atomic no-overwrite return a clear unsupported error rather than falling back to a pre-check.

```json
POST /volumes
{"name":"my-vol","provider":"daytona"}
→ {id, name, provider, provider_ref, status}
```

Volume names: `[A-Za-z0-9][A-Za-z0-9._-]{0,63}`.

## Agents (config only)

```
POST   /agents       — register agent config (no sandbox)
GET    /agents       — list
GET    /agents/{id}  — get
DELETE /agents/{id}  — delete
```

`POST /agents` rejects keys that don't belong on the agent (`cwd`, `env`, `dockerfile`, `dockerfile_content`, `shared_mounts`) with 400. Agent config is `agent_type`, `model`, `mcp_servers`, `skills`, `mode`, `thought_level`.

## Admin

```
GET /admin/sessions   — pool snapshot for the dashboard
```

## Python SDK

### `Event`

Streaming methods yield `Event` (a dict subclass). `str(ev)` → human text; `ev["type"]` / `ev["text"]` for structured access. Types: `text`, `reasoning`, `tool`, `tool_result`, `usage`, `done`, `error`.

### `Agent`

```python
agent.arun(message, *, interrupt=False) -> str            # full response
agent.astream(message, *, interrupt=False) -> AsyncIterator[Event]   # POST /message+stream
agent.run(message, timeout=None, *, interrupt=False) -> str          # sync wrapper for arun
agent.send(message, *, interrupt=False) -> str            # fire-and-forget; returns rpc_id
agent.cancel()                                            # best-effort
agent.configure(**kwargs)                                 # mode / model / thought_level
agent.events()                                            # async ctx mgr; long-lived /events stream
agent.aclose() / async with                               # POST /release + close httpx client
```

`interrupt=True` is client-side: cancel, wait for terminal block, then submit. Multiple concurrent `events()` contexts are allowed; each gets a fan-out copy. Error events are **yielded**, not raised; connection failure raises `StreamError`.

### `ApiClient` — operator persona

`agent_sdk.ApiClient` is a flat async wrapper over every REST route. Use it from services that operate on other people's sessions (admin tooling, hive bootstrap, bench scripts). `Agent` is the right choice when your code IS the user.

```python
async with ApiClient(base_url="https://...", token="optional-bearer") as sc:
    session = await sc.create_session(provider="daytona", model="claude-sonnet-4-6")
    await sc.send_message(session["session_id"], "hello")
    await sc.release_session(session["session_id"])
```

Stateless w.r.t. resources (every method takes IDs); one `httpx.AsyncClient` per instance; one method per route. Errors raise `httpx.HTTPStatusError` with the server's `{"error": ...}` body attached. Pass `http_client=` for custom transports.

| Resource | Method | Endpoint |
|---|---|---|
| Volumes | `create_volume(**body)` | `POST /volumes` |
| | `list_volumes(provider=None)` | `GET /volumes?provider=` |
| | `get_volume(id_or_name)` | `GET /volumes/{id}` |
| | `delete_volume(id_or_name, force=False)` | `DELETE /volumes/{id}` |
| Volume files | `volume_file_{tree,read,download,exists}(volume_id, path)` | `GET /volumes/{id}/files/*` |
| | `volume_file_write(volume_id, path, content)` | `POST /volumes/{id}/files/edit` (overwrite) |
| | `volume_file_edit(volume_id, path, *, old_string, new_string, replace_all=False)` | `POST /volumes/{id}/files/edit` (search/replace) |
| | `volume_file_{upload,mkdir,delete}(volume_id, path[, content])` | `POST /volumes/{id}/files/*` |
| | `volume_file_rename(volume_id, path, new_path, overwrite=True)` | `POST /volumes/{id}/files/rename` |
| Agents | `create_agent(**body)` | `POST /agents` |
| Sessions — lifecycle | `create_session(**body)` | `POST /sessions` (eager; `provision=False` for lazy) |
| | `list_sessions()` / `get_session(id)` / `get_session_status(id)` / `get_session_sandbox(id)` / `get_session_log(id, limit=500)` | `GET /sessions[/{id}[/...]]` |
| | `resume_session(id, **body)` | `POST /sessions/{id}/resume` |
| | `release_session(id)` | `POST /sessions/{id}/release` |
| | `delete_session(id)` | `DELETE /sessions/{id}` (idempotent) |
| Sessions — runtime | `send_message(id, text, interrupt=False)` | `POST /sessions/{id}/message` |
| | `send_message_stream(id, text, interrupt=False)` async iter | `POST /sessions/{id}/message+stream` |
| | `cancel_session(id)` | `POST /sessions/{id}/cancel` |
| | `set_session_config(id, **config)` | `POST /sessions/{id}/config` |
| | `acp_call(id, method, params=None, *, notify=False)` | `POST /sessions/{id}/acp/call` |
| | `session_sandbox_exec(id, command, timeout=30)` | `POST /sessions/{id}/sandbox/exec` |
| | `stream_events(id)` async iter | `GET /sessions/{id}/events` |
| Session files | `session_file_{tree,read,download}(id[, path])` | `GET /sessions/{id}/files/*` |
| | `session_file_edit(id, path, *, old_string, new_string, replace_all=False)` | `POST /sessions/{id}/files/edit` |
| | `session_file_{upload,delete,rename}(id, ...)` | `POST /sessions/{id}/files/*` |

For ACP knobs not covered by typed wrappers, use `acp_call`.
