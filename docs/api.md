# AFE REST API

Base URL: `http://localhost:7778`

## Agents (config only)

### Create agent
```
POST /agents
```
```json
{
  "name": "worker",
  "agent_type": "claude",
  "config": {
    "model": "claude-sonnet-4-6",
    "cwd": "/tmp",
    "prompt": "You are a helpful agent.",
    "tools": ["Read", "Bash"],
    "mcp_servers": {"my-mcp": {"type": "local", "command": "...", "args": []}},
    "skills": {"my-skill": {"sources": [{"source": "...", "type": "..."}]}}
  }
}
```
Returns `{id, name, config}`. Agent config is stored — no sandbox is provisioned yet.

### List / Get / Delete agents
```
GET    /agents
GET    /agents/{agent_id}
DELETE /agents/{agent_id}
```

### Quick create (agent + sandbox + session in one call)
```
POST /agents/quick
```
```json
{
  "name": "worker",
  "agent_type": "claude",
  "provider": "local",
  "cwd": "/tmp",
  "model": "claude-sonnet-4-6",
  "tools": ["Bash", "Read", "Write", "Glob", "Grep"],
  "mcp_servers": {
    "filesystem": {
      "type": "local",
      "command": "npx",
      "args": ["-y", "@modelcontextprotocol/server-filesystem", "/data"]
    }
  },
  "skills": {
    "default": {
      "sources": [{"type": "local", "source": "/path/to/skills"}]
    }
  }
}
```
Fields can be at top level (as shown above, used by the Agent SDK) or nested under `"config"`. MCP servers are passed to `session/new` in ACP array format. Skills are deployed to `{cwd}/.claude/commands/` via the sandbox filesystem API.
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

## Sandboxes

### Provision sandbox
```
POST /sandboxes
```
```json
{"provider": "local", "agent_type": "claude"}
```
Returns `{id, provider, sandbox_ref, status}`.

### List / Get / Delete sandboxes
```
GET    /sandboxes
GET    /sandboxes/{sandbox_id}
DELETE /sandboxes/{sandbox_id}
```

## Sessions

### Connect agent to sandbox
```
POST /sandboxes/{sandbox_id}/connect
```
```json
{"agent_id": "uuid"}
```
Creates ACP session + inner session. Returns `{session_id, inner_session_id, status}`.

### Resume session (by sandbox)
```
POST /sandboxes/{sandbox_id}/resume
```
```json
{"agent_id": "uuid", "inner_session_id": "uuid", "session_id": "uuid"}
```
Uses ACP `session/load` to restore conversation. The `session_id` field is optional — if provided, the server reuses it instead of generating a new one.

### Resume session (by session_id only)
```
POST /sessions/{session_id}/resume
```
No body required. Looks up agent_id, sandbox_id, and inner_session_id from the server DB. This is the simplest way to resume — the user only needs the session_id.

Returns:
```json
{
  "session_id": "uuid",
  "agent_id": "uuid",
  "sandbox_id": "uuid",
  "inner_session_id": "uuid",
  "status": "resumed"
}
```

### Send message
```
POST /sandboxes/{sandbox_id}/message
```
```json
{"session_id": "uuid", "message": "analyze the dataset"}
```
**Non-blocking.** Fires prompt in background. Response arrives via SSE.

Returns `{run_id, rpc_id, status}`.

### SSE event stream
```
GET /sandboxes/{sandbox_id}/events?session_id=uuid
```
Proxies ACP SSE from sandbox-agent. Key event types inside `session/update` notifications:

| sessionUpdate | Payload |
|---|---|
| `agent_message_delta` | `{content: {text: "..."}}` |
| `tool_call` | `{_meta: {claudeCode: {toolName: "Read"}}, rawInput: {...}}` |
| `tool_call_update` | `{_meta: {claudeCode: {toolResponse: {...}}}}` |
| `usage_updated` | `{cost: {amount, currency}}` |

Prompt done: `{"jsonrpc": "2.0", "id": "<rpc_id>", "result": {"stopReason": "end_turn"}}`

Supports `Last-Event-ID` for reconnection. After resume, replay events are skipped via the stored event cursor.

## Session Log (telemetry)

### Get session log
```
GET /sessions/{session_id}/log?limit=500
```
Returns full trace of agent actions for a session:
```json
[
  {"id": 1, "event_type": "user_message", "payload": {"text": "..."}, "created_at": 1712444800.0},
  {"id": 2, "event_type": "tool_call", "payload": {"tool": "Read", "args": {"file_path": "/tmp/f.txt"}}, "created_at": 1712444801.0},
  {"id": 3, "event_type": "tool_result", "payload": {"tool": "Read", "result": {"file": {...}}}, "created_at": 1712444802.0},
  {"id": 4, "event_type": "usage", "payload": {"amount": 0.05, "currency": "USD"}, "created_at": 1712444803.0},
  {"id": 5, "event_type": "assistant_message", "payload": {"text": "The file contains..."}, "created_at": 1712444804.0}
]
```

Event types: `user_message`, `assistant_message`, `tool_call`, `tool_result`, `usage`, `error`.

### Get agent log
```
GET /agents/{agent_id}/log?limit=100
```
Recent activity across all sessions for an agent. Same format, also includes `session_id` and `sandbox_id`.

## Sandbox Operations

### Filesystem
```
GET /sandboxes/{sandbox_id}/fs?path=/
GET /sandboxes/{sandbox_id}/fs/file?path=/file.txt
PUT /sandboxes/{sandbox_id}/fs/file?path=/file.txt
```

### Run command
```
POST /sandboxes/{sandbox_id}/exec
```
```json
{"command": "ls", "args": ["-la"], "cwd": "/tmp"}
```
Returns `{exitCode, stdout, stderr}`.

### Set session config
```
POST /sandboxes/{sandbox_id}/config
```
```json
{"mode": "bypassPermissions", "model": "claude-sonnet-4-6", "thought_level": "high"}
```

## Other

| Endpoint | Description |
|---|---|
| `GET /health` | `{"status": "ok"}` |
| `GET /chat` | Chat UI |
| `GET /kanban` | Kanban board |
| `GET /hive/items?task=` | Proxy for hive items |
| `GET /hive/items/{id}?task=` | Proxy for hive item detail |
