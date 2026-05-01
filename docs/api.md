# REST API

Base URL: `http://localhost:7778`

The API has two top-level resource groups:
- **Volumes** — durable storage (CRUD, file ops). Each volume holds `~/.claude`, transcripts, and workspace for the agents that mount it.
- **Sessions** — agent conversation (messages, events, resume, cancel, config, status, logs, files, sandbox metadata). Each session binds to a volume; its compute lease is owned by an in-process `SessionPool` and the provider sandbox identity (`sandbox_ref`) lives in `sessions.sandbox_state` (JSONB). There is no separate `sandboxes` resource — see [`assets/data-model.html`](../assets/data-model.html).

There is also a thin **Agents** group for registering an agent identity without provisioning compute.

## Health

```
GET /health
```
```json
{
  "status": "ok",
  "sessions": 1,
  "busy_sessions": 0
}
```

`sessions` is the count of sessions currently leased by the SessionPool (i.e. with live compute). `busy_sessions` is the subset that currently have at least one event subscriber attached.

## Chat UI

```
GET /ui            — chat
GET /ui/dashboard  — admin / validation dashboard
GET /ui/files      — filesystem browser (per session)
GET /ui/volumes    — volume inspector
```

## Sessions

### Create a session — eager (default) or lazy

```
POST /sessions
```

Single endpoint for session creation. Defaults to **eager**: provisions a sandbox via the SessionPool, attaches the supervisor + ACP, persists the resulting `sandbox_state` JSONB. Pass `"provision": false` in the body for the **lazy** flow — session row only, sandbox materialises on the first `/message` or `/resume` call (the SessionPool cold-creates on demand).

Config fields belong to one of three groups:

| Field | Belongs on | Notes |
|---|---|---|
| `agent_type`, `model`, `mcp_servers`, `skills`, `mode`, `thought_level` | agent (config) | Pure identity. Persisted on `agents.config` so cold-recovery replays them. |
| `cwd`, `env`, `secrets` | session | Per-conversation; `cwd` keys the JSONL hash. `env`/`secrets` are PATCH-shaped (omitted = keep, `{}` = clear, `{...}` = replace). |
| `dockerfile`, `dockerfile_content`, `shared_mounts`, `root`, `pre_start_commands`, `volume_id` | sandbox / provisioning | Frozen at provision time; survive replacement via the recipe persisted on `sandbox_state`. |

Fields may be supplied at the top level OR nested under `config`. If both are present, top-level wins. `volume_id` is optional — when omitted, a per-provider default volume named `default-{provider}` is created/reused transparently.

```json
{
  "name": "worker",
  "provider": "local",
  "volume_id": "vol-uuid",
  "agent_type": "claude",
  "model": "claude-sonnet-4-6",
  "cwd": "/tmp",
  "mcp_servers": {"name": {"type": "local", "command": "...", "args": []}},
  "skills": ["rllm-org/hive#staging", "vercel-labs/agent-skills"],
  "shared_mounts": ["shared-data"],
  "pre_start_commands": ["uv tool install hive-evolve"]
}
```

`pre_start_commands` are shell commands that run inside the sandbox before the ACP supervisor starts — use them to install CLIs, lay down config files, etc. For `docker`/`daytona`/`modal` they run inside the sandbox; for `local` they are ignored (the server host already runs `skills` install natively, and local has no sandbox boundary to run caller-supplied commands in safely). The effective list passed to the provider is `skills_install_commands + caller_pre_start_commands` in that order.

The merged list is stored on the session row's `sandbox_state.recipe.pre_start_commands` and is re-run on **Type 2 recovery** (replacement sandbox — external delete, unrecoverable provider error). **Type 1 recovery** (same VM restarted via `start_sandbox`) does not re-run them — the original side effects are still on disk. Skill-install commands are merged at create time and survive on the recipe; editing skills on a long-lived agent takes effect on the next sandbox replacement. Inspect what's persisted via `GET /sessions/{id}` (response includes `pre_start_commands`).

Returns (eager):
```json
{
  "agent_id": "uuid",
  "session_id": "uuid",
  "id": "uuid",
  "volume_id": "vol-uuid",
  "sandbox_ref": "<opaque provider sandbox ref>",
  "inner_session_id": "<acp inner session id>",
  "connected": true
}
```

Returns (lazy, `"provision": false`):
```json
{
  "id": "uuid",
  "agent_id": "uuid",
  "volume_id": "vol-uuid",
  "sandbox_ref": null,
  "connected": false
}
```

`sandbox_ref` is the **provider sandbox ref** (an opaque string — Daytona UUID, Docker container ID, local PID-as-string, etc.), not a server-side PK. There is no separate `current_sandbox_id` field — the sandboxes table was removed.

Lazy mode accepts an optional `agent_id` field to reuse an existing agent config instead of creating a new one.

### Send message (non-blocking)

```
POST /sessions/{session_id}/message
```
```json
{"message": "analyze the dataset"}
```

Returns `{rpc_id, status}` immediately. The request body is consumed in a background task; events are persisted to `session_log` and broadcast to any `/events` subscribers.

**Interrupt flag** — currently a no-op on this endpoint. Use `POST /sessions/{id}/cancel` to abort the in-flight prompt before submitting a new one.

### Send message + stream reply (single round-trip)

```
POST /sessions/{session_id}/message+stream
```
```json
{"message": "analyze the dataset"}
```

Submits a prompt and streams the reply as SSE in the response body. Same wire format as `GET /events`, scoped to a single `rpc_id`. `: heartbeat\n\n` lines keep idle connections open through nginx / cloudflare. This is what `Agent.astream()` uses.

### SSE event stream (multi-subscriber)

```
GET /sessions/{session_id}/events
```

Yields every event broadcast by the session's `SandboxSession` — across all prompts on the session — plus heartbeat sentinels every ~20 s of idle. Multiple concurrent `/events` connections each get a copy.

The server emits per-rpc-tagged blocks:

```
event: rpc:<rpc_id>
<raw acp block>
```

Untagged blocks are still possible (heartbeats, setup chatter).

Event types inside `session/update` notifications:
Some wrappers may emit `notifications/session/update`; clients should treat both forms equivalently.

| sessionUpdate | Payload |
|---|---|
| `agent_message_delta` | `{content: {text: "...", type?: "text"}}` |
| `agent_message_chunk` | `{content: {text: "...", type?: "text"}}` |
| `agent_message_delta` / `agent_message_chunk` (thinking) | `{content: {thinking: "...", type: "thinking"}}` |
| `agent_thought_chunk` | `{content: {text\|thinking: "..."}}` |
| `tool_call` | `{_meta: {claudeCode: {toolName, toolUseId}}, rawInput: {...}}` |
| `execute_tool_started` | `{_meta: {claudeCode: {toolName, toolUseId}}, rawInput: {...}}` |
| `tool_call_update` | `{_meta: {claudeCode: {toolResponse\|toolResult, toolName, toolUseId}}}` |
| `usage_updated` / `usage_update` | `{cost: {amount, currency}}` |

Prompt done:
```json
{"jsonrpc": "2.0", "id": "<rpc_id>", "result": {"stopReason": "end_turn"}}
```

When interrupted, the cancelled prompt completes with:
```json
{"jsonrpc": "2.0", "id": "<rpc_id>", "result": {"stopReason": "cancelled"}}
```

Prompt error:
```json
{
  "jsonrpc": "2.0", "id": "<rpc_id>",
  "error": {
    "code": -32000,
    "message": "<human-readable summary>",
    "data": {
      "kind": "sandbox_process_died | sandbox_internal_error | http_error | sandbox_unreachable | timeout | unknown",
      "exception_type": "HTTPStatusError",
      "http_status": 500,
      "upstream_body": "...",
      "rpc_id": "<rpc_id>"
    }
  }
}
```

Heartbeats (`: heartbeat\n\n`) are sent during idle to keep the connection alive.

### Resume session

```
POST /sessions/{session_id}/resume
```

Idempotent pre-warm: routes through `SessionPool.get_session(session_id)` — cold-creates a `SandboxSession` from `sandbox_state` if no lease exists, or returns the live one if it does. The ACP `session/load` happens inside `SandboxSession.start()`.

Optional body accepts `env` and `secrets` with PATCH semantics (`missing` keeps stored values, `{}` clears, an object replaces). Looks up everything else from the DB and ensures a live sandbox — reprovisioning against the session's volume if the prior sandbox was killed or reaped.

Returns:
```json
{
  "session_id": "uuid",
  "agent_id": "uuid",
  "sandbox_ref": "<opaque provider sandbox ref>",
  "inner_session_id": "uuid",
  "status": "resumed"
}
```

Conversation endpoints (`/message`, `/message+stream`, `/events`, `/cancel`, `/config`, `/files/*`, `/sandbox/exec`, `/acp/call`) auto-recover reaped or killed sandboxes by routing through `pool.get_session()`. You don't need to call resume explicitly before those calls. In-memory introspection endpoints like `/sessions` and `GET /sessions/{id}` (the row read) do not trigger recovery; `GET /sessions/{id}/status` and `GET /sessions/{id}/sandbox` DO trigger recovery.

### Release session (hibernate)

```
POST /sessions/{session_id}/release
```

Snapshot the filesystem to the volume + drop the SessionPool's compute lease. Idempotent — releasing an already-released session is a no-op. The next pool-mediated call cold-recovers from the snapshot.

Returns:
```json
{
  "lifecycle": "hibernated",
  "snapshot_path": "<volume-relative path or null>",
  "snapshot_version": 0
}
```

Internally backed by `SessionPool.release(session_id)`. The reaper invokes the same path on idle sessions (default 180 s; tune via `AGENT_SDK_REAPER_IDLE_S`).

### Delete session

```
DELETE /sessions/{session_id}
```

Releases the pool lease (snapshot + drop compute, idempotent) and deletes the session row. Returns 204 even when the session doesn't exist, so this is safe as a "make sure this is gone" primitive without a prior existence check.

The underlying daytona/docker/local sandbox is *paused*, not destroyed — label-based cleanup scripts (`scripts/cleanup_daytona_orphans.py`) reclaim the compute later.

### Cancel running prompt

```
POST /sessions/{session_id}/cancel
```

Best-effort: sends `session/cancel` (JSON-RPC notification) to the supervisor's ACP child via the SessionPool. The ACP child aborts the turn; the `done` event arrives on the same SSE subscribers that `POST /message` opened. No active lease → returns `{"status": "ok", "detail": "no active lease"}`.

### Set session config

```
POST /sessions/{session_id}/config
```
```json
{"mode": "bypassPermissions", "model": "claude-sonnet-4-6", "thought_level": "high"}
```

Patches the three persisted-and-replayed-on-recovery knobs: `model`, `mode`, `thought_level`. They are applied to the live ACP session AND persisted on `agents.config` so cold-recovery (Type-2) replays them via the supervisor's `_attach_acp` step. For any other ACP knob — new `configId`s Claude grows, vendor extensions, debugging — use `POST /sessions/{id}/acp/call` instead so we don't grow a new typed wrapper per knob.

### Generic ACP passthrough

```
POST /sessions/{session_id}/acp/call
```
```json
{"method": "session/whatever", "params": {...}, "notify": false}
```

Forwards a JSON-RPC call to the session's ACP supervisor. Auto-injects the inner `sessionId` into `params` so callers don't need to track it. `notify=true` sends as a JSON-RPC notification (no response, no rpc_id). Returns `{"result": <ACP result dict>}` for non-notify calls.

**Transient** — survives only the current ACP session, lost on the next sandbox restart. For anything that must replay on cold-recovery, persist via `POST /sessions/{id}/config` (model/mode/thought_level) or by baking it into the recipe at create time.

### Other

```
GET /sessions                        — list active (pool-leased) sessions
GET /sessions/{id}                   — read the session row (env + redacted secret keys + sandbox_ref + pre_start_commands)
GET /sessions/{id}/status            — runtime status (routes through SessionPool, brings up the SandboxSession if hibernated)
GET /sessions/{id}/sandbox           — sandbox metadata (provider, sandbox_ref, status, root, url for port-based providers, marker_path for local)
GET /sessions/{id}/log?limit=500     — event log (oldest first)
```

`GET /sessions/{id}/status` includes the following fields:

| Field | Description |
|---|---|
| `session_id` | Session ID |
| `agent_id` | Agent ID |
| `sandbox_ref` | Currently-bound sandbox ref (opaque provider string), or `null` if none is live |
| `inner_session_id` | Agent-native session ID used for resume/load |
| `agent_busy` | Always `false` (kept for response-shape back-compat with the dashboard; per-prompt SSE replaced the persistent reader) |
| `active_rpc_id` | Always `null` (same reason as `agent_busy`) |
| `pending_count` | Always `0` (same reason) |
| `session_subscriber_count` | Number of session-scoped SSE subscribers attached to the SandboxSession |
| `rpc_subscriber_count` | Always `0` (per-rpc subscribers were folded into the session-scoped fan-out) |
| `last_activity` | Monotonic timestamp of last observed chunk on the session, or `null` if never |
| `idle_seconds` | Seconds since `last_activity`, or `null` if no activity has been observed |
| `has_client` | Whether the SandboxSession has a live supervisor URL (proxy for "is the ACP child up") |
| `shutdown_requested` | Always `false` (constant) |
| `available_commands` | Always `[]` (was set by the legacy persistent SSE reader) |
| `supervisor_url` | URL the SandboxSession exposes its supervisor at, or `null` |
| `supervisor_port` | Listen port for port-based providers (docker / local / modal), or `null` for daytona |

`GET /sessions/{id}/sandbox` returns the same shape the legacy `GET /sandboxes/{id}` route used to — `{session_id, provider, sandbox_ref, status, root, url, marker_path}` — so test helpers and admin UIs that need sandbox metadata can stay in session-id space without a sandbox-row-id round trip.

#### Session log event types

Each row has `event_type`, `payload`, `created_at`, and `session_id`.

| event_type | Payload fields |
|---|---|
| `user_message` | `{text, prompt_id}` |
| `assistant_message` | `{text, prompt_id}` |
| `reasoning` | `{text, prompt_id}` — Claude's thinking/reasoning blocks |
| `tool_call` | `{tool, tool_call_id, prompt_id, args?}` |
| `tool_result` | `{tool, tool_call_id, prompt_id, result}` |
| `usage` | `{prompt_id, ...cost fields from agent}` |
| `turn_end` | `{stop_reason, prompt_id, usage?}` |
| `error` | `{message, kind, prompt_id, traceback?}` |

`prompt_id` is the `rpc_id` returned by `POST /sessions/{id}/message` and ties every event within a single prompt round-trip together.

`tool_call_id` links a `tool_call` row to its corresponding `tool_result` row (matches the `id` field in Claude's `tool_use` content blocks).

## Session sandbox helper

```
POST /sessions/{session_id}/sandbox/exec
```

```json
{"command": "pwd", "timeout": 30}
```

Returns:

```json
{
  "stdout": "...",
  "stderr": "...",
  "exit_code": 0,
  "stdout_truncated": false,
  "stderr_truncated": false,
  "timed_out": false
}
```

Routes through the SessionPool, so the sandbox is reprovisioned automatically if it was reaped. Does not require an active ACP session.

## Session-scoped filesystem

Read/write inside the session's *current* sandbox (proxied through the supervisor, which is itself reprovisioned on demand). Sandbox identity is hidden — `session_id` addresses the live sandbox, no matter how many replacements have happened.

```
GET    /sessions/{id}/files/tree
GET    /sessions/{id}/files/read?path=…
POST   /sessions/{id}/files/edit              — body: same shape as the volume edit (overwrite or search/replace)
POST   /sessions/{id}/files/upload            — body: {path, content (base64)}
POST   /sessions/{id}/files/delete            — body: {path}
POST   /sessions/{id}/files/rename            — body: {path, new_path}
GET    /sessions/{id}/files/download?path=…   — raw bytes
```

## Volumes

Durable storage, independent of any sandbox. Created once, live for months. Every session binds to a volume.

```
POST   /volumes                              — create + wait for status="ready"
GET    /volumes                              — list (optional ?provider= filter)
GET    /volumes/{id_or_name}                 — get (name lookup supported for convenience)
DELETE /volumes/{id_or_name}?force=false     — delete (409 if any session still references it; force=true cascades)
GET    /volumes/{id_or_name}/files/tree      — browse volume contents (e.g. ?path=shared/)
GET    /volumes/{id_or_name}/files/read      — read a file (?path=…)
GET    /volumes/{id_or_name}/files/exists    — check if a file/dir exists (?path=…)
GET    /volumes/{id_or_name}/files/download  — raw bytes (?path=…)
POST   /volumes/{id_or_name}/files/edit      — write or search/replace a file (body: {path, content} OR {path, old_string, new_string, replace_all?})
POST   /volumes/{id_or_name}/files/upload    — body: {path, content (base64)}
POST   /volumes/{id_or_name}/files/mkdir     — body: {path}
POST   /volumes/{id_or_name}/files/delete    — body: {path}
POST   /volumes/{id_or_name}/files/rename    — body: {path, new_path, overwrite=true}
```

File ops operate directly against the provider's volume primitives — no live sandbox required. For `POST /volumes/{id_or_name}/files/rename`, `overwrite` defaults to `true` for compatibility. When `overwrite=false`, the provider uses an atomic no-overwrite primitive for regular files; if `new_path` already exists the API returns HTTP 409 with `{"error": "exists", "path": new_path}` and leaves `path` untouched. Providers that cannot guarantee atomic no-overwrite semantics for a case return a clear unsupported error instead of falling back to a pre-check.

`POST /volumes` body:

```json
{"name": "my-vol", "provider": "daytona"}
```

Returns the volume record `{id, name, provider, provider_ref, status}`. Volume names must match `[A-Za-z0-9][A-Za-z0-9._-]{0,63}`.

## Agents (config only)

Register an agent identity without provisioning compute. Used when the caller wants to validate or stage an agent before paying provisioning cost.

```
POST   /agents                     — register agent config (no sandbox)
GET    /agents                     — list
GET    /agents/{id}                — get
DELETE /agents/{id}                — delete
```

`POST /agents` rejects keys that no longer belong to agent config — `cwd`, `env`, `dockerfile`, `dockerfile_content`, `shared_mounts` — with a 400 explaining that they must be set on `POST /sessions` instead. Agent config is `agent_type`, `model`, `mcp_servers`, `skills`, `mode`, `thought_level`.

## Admin

```
GET /admin/sessions
```

Pool snapshot for the dashboard / debugging — lists every active session lease and (separately) every supervisor instance the pool currently runs. Response shape is preserved from the legacy implementation so `ui/dashboard.html` doesn't need to change.

## Client API (Python SDK)

### `Event` class

All streaming methods yield `Event` objects (a dict subclass). `str(ev)` returns the human-readable text, so `print(ev, end="")` works naturally. Structured access via `ev["type"]`, `ev["text"]`, etc.

Event types: `text`, `reasoning`, `tool`, `tool_result`, `usage`, `done` (terminal), `error`.

### `Agent.arun(message, *, interrupt=False) -> str`

Send a message and return the full response text. If `interrupt=True`, cancels the running prompt first.

```python
response = await agent.arun("analyze this")
response = await agent.arun("redirect", interrupt=True)
```

### `Agent.astream(message, *, interrupt=False) -> AsyncIterator[Event]`

Send a message and stream events via `POST /sessions/{id}/message+stream` (single round-trip). Yields `Event` dicts; `print(ev, end="")` works naturally.

```python
async for ev in agent.astream("explain X"):
    print(ev, end="")                     # text output via str(ev)
    if ev["type"] == "tool":              # structured access
        print(f"calling {ev['tool_name']}")
```

### `Agent.run(message, timeout=None, *, interrupt=False) -> str`

Sync wrapper for `arun`.

### `Agent.send(message, *, interrupt=False) -> str`

Submit a message without waiting for the response. Returns the `rpc_id` immediately. Use with `events()` to listen for results.

```python
rpc_id = await agent.send("do this next")
rpc_id = await agent.send("stop, do this", interrupt=True)
```

### `Agent.cancel()`

Cancel the running prompt (best-effort).

### `Agent.configure(**kwargs)`

Set session config dynamically. Accepts: `mode`, `model`, `thought_level`.

### `Agent.events()` — async context manager

Open a long-lived SSE subscription and iterate over all session events. Each event is an `Event` dict. Error events are **yielded**, not raised.

```python
async with agent.events() as stream:
    async for ev in stream:
        print(ev, end="")
        if ev["type"] == "done":
            break
```

Multiple concurrent `events()` contexts are allowed; each gets a fan-out copy. On connection failure, the generator raises `StreamError`.

### `Agent.aclose()` / `async with`

Closing an `Agent` calls `POST /sessions/{id}/release` to snapshot + drop the pool's compute lease, then closes the underlying `httpx.AsyncClient`. The reaper would catch idle sessions eventually, but releasing on close frees compute immediately and writes a fresh snapshot.

## Server-side client (operator persona)

`agent_sdk.ApiClient` is a thin async wrapper over every REST route on this server. Use it from services that create, destroy, and introspect OTHER people's sessions — e.g. hive's workspace-agent bootstrap, `scripts/bench_recovery.py`, admin tooling. `Agent` stays the right choice when your code IS the user talking to its own session; `ApiClient` is the right choice when your code is the operator.

```python
from agent_sdk import ApiClient

async with ApiClient(
    base_url="https://agent-sdk.example.com",
    token="optional-admin-bearer",
) as sc:
    session = await sc.create_session(provider="daytona", model="claude-sonnet-4-6")
    await sc.send_message(session["session_id"], "hello")
    await sc.release_session(session["session_id"])  # snapshot + drop compute
```

### Design

- **Stateless w.r.t. resources.** No per-session / per-volume state lives on the instance. Every method takes the IDs it acts on as parameters — a single instance is safe to share across thousands of concurrent operations against unrelated sessions.
- **Stateful only for transport.** The instance holds one `httpx.AsyncClient` (connection pool + bearer header). No locks, no retries, no idempotency keys.
- **Flat surface, one method per endpoint.** No sub-namespaces. The method name mirrors the REST path; the body is pass-through. Adding a new route = adding one method.
- **Shared error mapping with `Agent`.** HTTP ≥400 responses raise `httpx.HTTPStatusError` with the server's `{"error": ...}` body attached — same as the user-facing `Agent` class.
- **Dependency-injection hook.** `ApiClient(base_url, http_client=...)` accepts a pre-built `httpx.AsyncClient` so callers with custom proxies, mock transports, or test harnesses don't have to subclass.

### Method list

Grouped by resource. Bodies are documented under the corresponding REST endpoint in the sections above.

| Resource | Method | Endpoint |
|---|---|---|
| Volumes | `create_volume(**body)` | `POST /volumes` |
| | `list_volumes(provider=None)` | `GET /volumes?provider=...` |
| | `get_volume(id_or_name)` | `GET /volumes/{id}` |
| | `delete_volume(id_or_name, force=False)` | `DELETE /volumes/{id}` |
| Volume files | `volume_file_tree(volume_id, path="")` | `GET /volumes/{id}/files/tree` |
| | `volume_file_read(volume_id, path)` | `GET /volumes/{id}/files/read` |
| | `volume_file_download(volume_id, path)` | `GET /volumes/{id}/files/download` |
| | `volume_file_exists(volume_id, path)` | `GET /volumes/{id}/files/exists` |
| | `volume_file_write(volume_id, path, content)` | `POST /volumes/{id}/files/edit` (overwrite) |
| | `volume_file_edit(volume_id, path, *, old_string, new_string, replace_all=False)` | `POST /volumes/{id}/files/edit` (search/replace) |
| | `volume_file_upload(volume_id, path, content)` | `POST /volumes/{id}/files/upload` |
| | `volume_file_mkdir(volume_id, path)` | `POST /volumes/{id}/files/mkdir` |
| | `volume_file_delete(volume_id, path)` | `POST /volumes/{id}/files/delete` |
| | `volume_file_rename(volume_id, path, new_path, overwrite=True)` | `POST /volumes/{id}/files/rename` |
| Agents | `create_agent(**body)` | `POST /agents` |
| Sessions — lifecycle | `create_session(**body)` | `POST /sessions` (eager default; pass `provision=False` for lazy) |
| | `list_sessions()` | `GET /sessions` |
| | `get_session(id)` | `GET /sessions/{id}` |
| | `get_session_status(id)` | `GET /sessions/{id}/status` |
| | `get_session_sandbox(id)` | `GET /sessions/{id}/sandbox` |
| | `get_session_log(id, limit=500)` | `GET /sessions/{id}/log` |
| | `resume_session(id, **body)` | `POST /sessions/{id}/resume` |
| | `release_session(id)` | `POST /sessions/{id}/release` |
| | `delete_session(id)` | `DELETE /sessions/{id}` (idempotent — 204 even when missing) |
| Sessions — runtime | `send_message(id, text, interrupt=False)` | `POST /sessions/{id}/message` |
| | `send_message_stream(id, text, interrupt=False)` async iterator | `POST /sessions/{id}/message+stream` (raw SSE bytes) |
| | `cancel_session(id)` | `POST /sessions/{id}/cancel` |
| | `set_session_config(id, **config)` | `POST /sessions/{id}/config` |
| | `acp_call(id, method, params=None, *, notify=False)` | `POST /sessions/{id}/acp/call` |
| | `session_sandbox_exec(id, command, timeout=30)` | `POST /sessions/{id}/sandbox/exec` |
| | `stream_events(id)` async iterator | `GET /sessions/{id}/events` (raw SSE bytes) |
| Session files | `session_file_tree(id)` | `GET /sessions/{id}/files/tree` |
| | `session_file_read(id, path)` | `GET /sessions/{id}/files/read` |
| | `session_file_edit(id, path, *, old_string, new_string, replace_all=False)` | `POST /sessions/{id}/files/edit` |
| | `session_file_upload(id, path, content_b64)` | `POST /sessions/{id}/files/upload` |
| | `session_file_delete(id, path)` | `POST /sessions/{id}/files/delete` |
| | `session_file_rename(id, path, new_path)` | `POST /sessions/{id}/files/rename` |
| | `session_file_download(id, path)` | `GET /sessions/{id}/files/download` |

`ApiClient` is intentionally focused on the session/volume lifecycle. There is no longer a standalone `/sandboxes` resource — sandbox identity is internal to the SessionPool and surfaced via `GET /sessions/{id}/sandbox`. For advanced ACP use cases not covered by the typed wrappers, use `acp_call`.
