# REST API

Base URL: `http://localhost:7778`

The API has three resource groups:
- **Volumes** — durable storage (CRUD, file ops). Each volume holds `~/.claude`, transcripts, and workspace for the agents that mount it.
- **Sandboxes** — ephemeral compute leases (create, list, get, destroy, stop, start). Each sandbox mounts a volume at a subpath.
- **Sessions** — agent conversation (messages, events, resume, cancel, config, status, logs). Each session binds to a volume; its current sandbox is swapped when the old one dies.

See also: [`assets/data-model.html`](../assets/data-model.html) and [`assets/rest-api.html`](../assets/rest-api.html).

## Health

```
GET /health
```
```json
{
  "status": "ok",
  "sessions": 3,
  "busy_sessions": 1,
  "readers_alive": 3,
  "instances": 2
}
```

## Chat UI

```
GET /ui
```

Serves a browser-based chat interface for interacting with agent sessions.

## Sessions

### Create agent + sandbox + session in one call

```
POST /sessions/quick
```

Config fields may be passed either at the top level (`agent_type`, `model`, `prompt`, `tools`, `mcp_servers`, `skills`, `cwd`, `dockerfile`, `dockerfile_content`) or under `config`. If both are present, values in `config` win. `volume_id` is optional — if omitted, a per-provider default volume is created (or reused) transparently.

```json
{
  "name": "worker",
  "provider": "local",
  "volume_id": "vol-uuid",
  "agent_type": "claude",
  "model": "claude-sonnet-4-6",
  "cwd": "/tmp",
  "prompt": "You are a helpful agent.",
  "tools": ["Bash", "Read", "Write"],
  "mcp_servers": {"name": {"type": "local", "command": "...", "args": []}},
  "skills": ["rllm-org/hive#staging", "vercel-labs/agent-skills"]
}
```

Returns:
```json
{
  "agent_id": "uuid",
  "sandbox_id": "uuid",
  "current_sandbox_id": "uuid",
  "session_id": "uuid",
  "inner_session_id": "uuid",
  "connected": true
}
```

`sandbox_id` and `current_sandbox_id` are emitted with the same value for backward compatibility with pre-refactor clients.

### Create a session bound to a volume (lazy sandbox)

```
POST /sessions
```

Does **not** provision a sandbox — the compute is created lazily on the first `/message` or `/resume`. Accepts the same config fields as `/sessions/quick`, plus an optional `agent_id` to reuse an existing agent config instead of creating a new one. `volume_id` is optional (falls back to the per-provider default volume if omitted).

```json
{
  "agent_id": "uuid-optional",
  "volume_id": "vol-uuid",
  "name": "worker-2",
  "agent_type": "claude",
  "model": "claude-sonnet-4-6",
  "cwd": "/home/daytona",
  "secrets": {"CLAUDE_CODE_OAUTH_TOKEN": "..."}
}
```

Returns:

```json
{
  "id": "session-uuid",
  "agent_id": "uuid",
  "volume_id": "vol-uuid",
  "current_sandbox_id": null,
  "connected": false
}
```

### Send message (non-blocking)

```
POST /sessions/{session_id}/message
```
```json
{"message": "analyze the dataset"}
```

Returns `{rpc_id, status}`. The actual response streams via SSE.

**Interrupt flag** — cancel the running prompt and submit a new one:

```json
{"message": "stop — focus on X instead", "interrupt": true}
```

When `interrupt: true`, the server cancels the currently running prompt, waits for it to drain (`stopReason: cancelled`), then submits the new message. Already-queued prompts are not affected. Use `GET /sessions/{id}/events` (or `Agent.events()`) to receive the response events.


### SSE event stream

```
GET /sessions/{session_id}/events
```
The server proxies raw ACP SSE blocks and tags attributed prompt events with:
```
event: rpc:<rpc_id>
```
Untagged blocks are still possible (for example heartbeats or setup chatter).

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

In addition to ACP-proxied events, the server emits:

| event | Payload |
|---|---|
| `sandbox_reattach` | `{old_sandbox_id, new_sandbox_id}` — emitted before the first real event of a run when the session transparently reprovisioned its sandbox against the same volume. |
| `sandbox_lost` | `{sandbox_id, reason}` — emitted once on mid-run sandbox failure. The run aborts; caller decides whether to retry. |

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

Heartbeats (`: heartbeat\n\n`) are sent every 30s to keep the connection alive during long-running prompts.

### Prompt queue and interrupt

The server uses an explicit per-session scheduler.

Each `SessionState` carries:

- `active_rpc_id`: the prompt currently executing upstream, or `None`
- `pending_prompts: deque[PendingPrompt]`: FIFO queue of submitted work
- `_prompt_ready`: wakes the scheduler when new prompts arrive
- `_prompt_done`: signals that the active prompt reached a terminal state

`POST /sessions/{session_id}/message` always allocates a new `rpc_id`,
appends a `PendingPrompt`, and returns immediately. The background
scheduler loop is the only owner of `active_rpc_id`:

1. wait for `_prompt_ready`
2. pop the next pending prompt
3. set `active_rpc_id`
4. send `session/prompt` upstream
5. wait for a terminal response or terminal error
6. clear `active_rpc_id`, set `_prompt_done`, and continue

`agent_busy` is therefore just `active_rpc_id is not None`.

#### Interrupt flow

`interrupt: true` on `/sessions/{session_id}/message` means:

1. if a prompt is active, send `session/cancel`
2. wait for `_prompt_done` with a 10 second safety timeout
3. append the new prompt to the tail of `pending_prompts`

Interrupt preserves queue order. It cancels the running turn, but it
does not reorder or drop already-queued prompts. The standalone
`POST /sessions/{session_id}/cancel` endpoint performs the same
cancel-and-wait step without submitting a replacement prompt.

#### Event delivery

Each session has one long-lived upstream SSE reader. That reader:

- keeps the ACP stream open
- updates runtime state such as `last_event_id`
- logs parsed events to the database
- fans out raw SSE blocks to downstream subscribers

Downstream subscribers attach to the server fan-out, not directly to ACP:

- session-scoped subscribers receive every event for the session
- RPC-scoped subscribers receive only events tagged for one `rpc_id`

This keeps parsing and state transitions centralized while still letting
multiple clients watch the same session concurrently.

#### One important latency edge case

If a prompt launches a background task, ACP may delay the terminal
`done_result` until that background task reports completion. In that
window, text and usage may already be finished but the prompt is still
considered active, so the next queued prompt cannot start yet. See
`tests/test_acp_invariants.py::TestDoneHeldForBackgroundTasks`.

### Resume session

```
POST /sessions/{session_id}/resume
```

No body. Looks up everything from the DB and ensures a live sandbox — reprovisioning against the session's volume if the prior sandbox was killed or reaped. Returns:
```json
{
  "session_id": "uuid",
  "agent_id": "uuid",
  "current_sandbox_id": "uuid",
  "inner_session_id": "uuid",
  "status": "resumed"
}
```

Conversation endpoints (`/message`, `/events`, `/cancel`, `/config`, `/resume`, `/sandbox/exec`) auto-recover reaped or killed sandboxes by looking up the session in Postgres and ensuring a live sandbox mounted against the session's volume. You don't need to call resume explicitly before those calls. In-memory introspection endpoints like `/sessions` and `/sessions/{id}/status` do not trigger recovery.

### Sandbox control

Explicit lifecycle control over the session's ephemeral sandbox. None of these affect the session row or its bound volume.

```
POST /sessions/{session_id}/start-sandbox    — pre-warm a sandbox eagerly (idempotent)
POST /sessions/{session_id}/stop-sandbox     — kill current sandbox; next /message lazy-provisions a fresh one
POST /sessions/{session_id}/reset-sandbox    — kill current + provision a fresh replacement in one call
```

`start-sandbox` and `reset-sandbox` return `{"sandbox_id": "..."}` on success. `stop-sandbox` returns 204.

### Cancel running prompt

```
POST /sessions/{session_id}/cancel
```

### Set session config

```
POST /sessions/{session_id}/config
```
```json
{"mode": "bypassPermissions", "model": "claude-sonnet-4-6", "thought_level": "high"}
```

### Other

```
GET /sessions                        — list active sessions
GET /sessions/{id}/status            — runtime status
GET /sessions/{id}/log?limit=500     — event log (oldest first)
```

`GET /sessions/{id}/status` includes the following fields:

| Field | Description |
|---|---|
| `session_id` | Session ID |
| `agent_id` | Agent ID |
| `current_sandbox_id` | Currently-attached sandbox ID, or `null` if none is live |
| `inner_session_id` | Agent-native session ID used for resume/load |
| `agent_busy` | Whether a prompt is currently active (`active_rpc_id != null`) |
| `active_rpc_id` | Currently running prompt ID, if any |
| `pending_count` | Number of queued prompts |
| `session_subscriber_count` | Number of session-scoped SSE subscribers |
| `rpc_subscriber_count` | Number of RPC-scoped SSE subscribers |
| `last_activity` | Unix timestamp of last session activity |
| `idle_seconds` | Seconds since last terminal turn completion or activity |
| `has_client` | Whether an ACP client is currently attached |
| `shutdown_requested` | Whether runtime shutdown is set |

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
  "timed_out": false
}
```

## Volumes

Durable storage, independent of any sandbox. Created once, live for months. Every sandbox must be created against a volume (with a subpath), and every session binds to a volume.

```
POST   /volumes                              — create (body: {name, provider})
POST   /volumes/provision                    — create + wait for status="ready" (mirrors /sandboxes/provision)
GET    /volumes                              — list (optional ?provider= filter)
GET    /volumes/{id_or_name}                 — get (name lookup supported for convenience)
DELETE /volumes/{id_or_name}?force=false     — delete (409 if any session still references it; force=true cascades)
GET    /volumes/{id_or_name}/files/tree      — browse volume contents (e.g. ?path=shared/)
GET    /volumes/{id_or_name}/files/read      — read a file (?path=…)
POST   /volumes/{id_or_name}/files/edit      — write a file (body: {path, content})
```

File ops are backed internally by a short-lived utility sandbox that mounts the volume; callers never need to manage one to seed data.

`POST /volumes` body:

```json
{"name": "my-vol", "provider": "daytona"}
```

Returns the volume record including `{id, name, provider, provider_ref, status}`.

## Sandboxes

Sandbox endpoints don't require a session. They operate directly on the sandbox infrastructure. Every sandbox is `(volume_id, subpath)`-scoped at creation: the volume is mounted at `/home/daytona` and a read-only `shared/` subpath on the same volume is mounted at `/mnt/shared`.

```
POST   /sandboxes                    — create (requires {provider, volume_id, subpath, …})
POST   /sandboxes/provision          — create + wait for ready (same required fields)
GET    /sandboxes                    — list
GET    /sandboxes/{id}               — get info (includes volume_id, subpath)
DELETE /sandboxes/{id}               — destroy (sessions survive with current_sandbox_id = NULL)
POST   /sandboxes/{id}/stop          — stop (volume data preserved)
POST   /sandboxes/{id}/start         — resume stopped sandbox
```

`POST /sandboxes` body:

```json
{
  "provider": "daytona",
  "volume_id": "vol-uuid",
  "subpath": "agents/<agent_id>/home",
  "agent_type": "claude",
  "root": "/home/daytona"
}
```

For session-driven creation, the server uses `subpath = agents/<agent_id>/home` so that all sessions of the same agent share HOME on the volume.

## Agents (config only)

```
POST   /agents                     — register agent config (no sandbox)
GET    /agents                     — list
GET    /agents/{id}                — get
DELETE /agents/{id}                — delete
```

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

Send a message and stream events. If `interrupt=True`, cancels the running prompt first.

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
