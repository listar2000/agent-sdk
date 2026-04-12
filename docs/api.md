# REST API

Base URL: `http://localhost:7778`

The API has two resource groups:
- **Sandboxes** — infrastructure (container/workspace, filesystem, exec, desktop)
- **Sessions** — agent conversation (messages, events, resume)

## Health

```
GET /health  →  {"status": "ok"}
```

## Sessions

### Create agent + sandbox + session in one call

```
POST /sessions/quick
```

```json
{
  "name": "worker",
  "provider": "local",
  "agent_type": "claude",
  "model": "claude-sonnet-4-6",
  "cwd": "/tmp",
  "prompt": "You are a helpful agent.",
  "tools": ["Bash", "Read", "Write"],
  "mcp_servers": {"name": {"type": "local", "command": "...", "args": []}},
  "skills": {"name": {"sources": [{"source": "...", "type": "github"}]}}
}
```

Returns:
```json
{
  "agent_id": "uuid",
  "sandbox_id": "uuid",
  "session_id": "uuid",
  "inner_session_id": "uuid",
  "connected": true
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

### SSE event stream

```
GET /sessions/{session_id}/events
```

Event types inside `session/update` notifications:

| sessionUpdate | Payload |
|---|---|
| `agent_message_delta` | `{content: {text: "...", type?: "text"}}` |
| `agent_message_delta` (thinking) | `{content: {thinking: "...", type: "thinking"}}` |
| `tool_call` | `{_meta: {claudeCode: {toolName, toolUseId}}, rawInput: {...}}` |
| `tool_call_update` | `{_meta: {claudeCode: {toolResponse\|toolResult, toolName, toolUseId}}}` |
| `usage_updated` | `{cost: {amount, currency}}` |

Prompt done:
```json
{"jsonrpc": "2.0", "id": "<rpc_id>", "result": {"stopReason": "end_turn"}}
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

### Resume session

```
POST /sessions/{session_id}/resume
```

No body. Looks up everything from the DB and restarts the sandbox if stopped. Returns:
```json
{
  "session_id": "uuid",
  "agent_id": "uuid",
  "sandbox_id": "uuid",
  "inner_session_id": "uuid",
  "status": "resumed"
}
```

All session endpoints auto-recover reaped sessions — if a session was removed from memory by the idle reaper, the server transparently looks it up in the DB and rebuilds state. You don't need to call resume explicitly.

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
GET /sessions/{id}/log?limit=500     — event log (newest first)
```

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
| `error` | `{message, kind, prompt_id, traceback?}` |

`prompt_id` is the `rpc_id` returned by `POST /sessions/{id}/message` and ties every event within a single prompt round-trip together.

`tool_call_id` links a `tool_call` row to its corresponding `tool_result` row (matches the `id` field in Claude's `tool_use` content blocks).

## Sandboxes

Sandbox endpoints don't require a session. They operate directly on the sandbox infrastructure.

### Lifecycle

```
POST   /sandboxes                    — create (provider, dockerfile)
GET    /sandboxes                    — list
GET    /sandboxes/{id}               — get info
DELETE /sandboxes/{id}               — destroy permanently
POST   /sandboxes/{id}/stop          — stop (preserves filesystem on Daytona)
POST   /sandboxes/{id}/start         — resume stopped sandbox
GET    /sandboxes/{id}/health        — health check
```

### Filesystem

```
GET    /sandboxes/{id}/fs?path=/              — list directory
GET    /sandboxes/{id}/fs/file?path=/f.txt    — read file
PUT    /sandboxes/{id}/fs/file?path=/f.txt    — write file
DELETE /sandboxes/{id}/fs/file?path=/f.txt    — delete
POST   /sandboxes/{id}/fs/mkdir?path=/dir     — mkdir
POST   /sandboxes/{id}/fs/move                — move/rename
GET    /sandboxes/{id}/fs/stat?path=/f.txt    — stat
POST   /sandboxes/{id}/fs/upload?path=/       — upload tar archive
```

### Exec / Processes

```
POST /sandboxes/{id}/exec                     — run command, returns {exitCode, stdout, stderr}
POST /sandboxes/{id}/processes                — start persistent process
GET  /sandboxes/{id}/processes                — list
POST /sandboxes/{id}/processes/{pid}/stop     — stop
POST /sandboxes/{id}/processes/{pid}/kill     — kill
GET  /sandboxes/{id}/processes/{pid}/logs     — logs
```

### Desktop (when sandbox has a display)

```
GET  /sandboxes/{id}/desktop/screenshot       — returns PNG
POST /sandboxes/{id}/desktop/click            — {x, y, button}
POST /sandboxes/{id}/desktop/type             — {text}
POST /sandboxes/{id}/desktop/press            — {key}
POST /sandboxes/{id}/desktop/drag             — {startX, startY, endX, endY}
POST /sandboxes/{id}/desktop/scroll           — {x, y, scrollX, scrollY}
```

## Agents (config only)

```
POST   /agents                     — register agent config (no sandbox)
GET    /agents                     — list
GET    /agents/{id}                — get
DELETE /agents/{id}                — delete
GET    /agents/{id}/log?limit=100  — event log across all sessions
```
