# Sandbox Integration Design

Date: 2026-04-05

## Executive Summary

This document specifies how Daytona-backed persistent sandboxes integrate into the auto_feature_engineer platform. Sandboxes are first-class resources with their own lifecycle, independent of agents. Multiple agents can share a single sandbox. The system supports three agent execution modes: local (no sandbox, current behavior), inline sandbox creation (`sandbox=True`), and attachment to an existing sandbox (`sandbox_id="..."`).

The central design insight is that the Daytona sandbox is a full Linux environment with internet access. Instead of routing tool calls through a proxy, we run the claude-agent-sdk (and the `claude` CLI it wraps) directly inside the sandbox. Tools like Bash, Read, Write, Edit, Glob, and Grep then execute naturally inside the sandbox filesystem with no remapping needed.

---

## 1. Data Model

### 1.1 New Table: `sandboxes`

```sql
CREATE TABLE IF NOT EXISTS sandboxes (
    id              TEXT PRIMARY KEY,       -- Daytona sandbox ID (assigned by Daytona on creation)
    name            TEXT UNIQUE,            -- Human-readable name, optional (NULL allowed)
    state           TEXT NOT NULL DEFAULT 'creating',
                                            -- 'creating' | 'started' | 'stopped' | 'error'
    daytona_state   TEXT,                   -- Last known Daytona-side state (for reconciliation)
    image           TEXT DEFAULT 'ubuntu:22.04',
    language        TEXT DEFAULT 'python',
    auto_stop_minutes INTEGER DEFAULT 15,   -- 0 = disabled
    labels          TEXT DEFAULT '{}',      -- JSON object
    env_vars        TEXT DEFAULT '{}',      -- JSON object
    resources       TEXT DEFAULT '{}',      -- JSON object: {"cpu": 2, "memory": 4, "disk": 20}
    agent_count     INTEGER DEFAULT 0,      -- Reference count of attached agents
    last_activity   INTEGER,                -- Unix timestamp of last agent activity
    created_at      INTEGER NOT NULL,
    updated_at      INTEGER NOT NULL,
    error_message   TEXT                    -- Last error message if state = 'error'
);
```

### 1.2 Modified Table: `agents`

Add a `sandbox_id` column:

```sql
ALTER TABLE agents ADD COLUMN sandbox_id TEXT REFERENCES sandboxes(id);
```

The full schema after migration:

```sql
CREATE TABLE IF NOT EXISTS agents (
    name        TEXT PRIMARY KEY,
    resume_id   TEXT,
    config      TEXT,               -- JSON blob (model, cwd, tools, prompt, ...)
    sandbox_id  TEXT                -- NULL = local execution, non-NULL = sandbox execution
);
```

### 1.3 Modified Table: `runs`

Add `sandbox_id` to track which sandbox (if any) was used for each run:

```sql
ALTER TABLE runs ADD COLUMN sandbox_id TEXT;
```

### 1.4 Migration Strategy

On server startup, `_db()` runs migration conditionally:

```python
# After existing CREATE TABLE statements:
try:
    conn.execute("ALTER TABLE agents ADD COLUMN sandbox_id TEXT")
except sqlite3.OperationalError:
    pass  # Column already exists

try:
    conn.execute("ALTER TABLE runs ADD COLUMN sandbox_id TEXT")
except sqlite3.OperationalError:
    pass

conn.execute("""
    CREATE TABLE IF NOT EXISTS sandboxes (
        id              TEXT PRIMARY KEY,
        name            TEXT UNIQUE,
        state           TEXT NOT NULL DEFAULT 'creating',
        daytona_state   TEXT,
        image           TEXT DEFAULT 'ubuntu:22.04',
        language        TEXT DEFAULT 'python',
        auto_stop_minutes INTEGER DEFAULT 15,
        labels          TEXT DEFAULT '{}',
        env_vars        TEXT DEFAULT '{}',
        resources       TEXT DEFAULT '{}',
        agent_count     INTEGER DEFAULT 0,
        last_activity   INTEGER,
        created_at      INTEGER NOT NULL,
        updated_at      INTEGER NOT NULL,
        error_message   TEXT
    )
""")
conn.commit()
```

---

## 2. REST API Endpoints

### 2.1 Sandbox Resource Endpoints

#### `POST /sandboxes` -- Create Sandbox

Creates a new Daytona sandbox. Returns immediately after Daytona confirms creation.

**Request:**
```json
{
    "name": "shared-workspace",
    "image": "ubuntu:22.04",
    "language": "python",
    "auto_stop_minutes": 15,
    "labels": {"project": "afe", "dataset": "ieee-fraud"},
    "env_vars": {"KAGGLE_KEY": "..."},
    "resources": {"cpu": 2, "memory": 4, "disk": 20}
}
```

All fields are optional. `name` defaults to `None` (anonymous sandbox). `image` defaults to `"ubuntu:22.04"`. `auto_stop_minutes` defaults to `15`.

**Response (201):**
```json
{
    "id": "sbx_abc123",
    "name": "shared-workspace",
    "state": "started",
    "image": "ubuntu:22.04",
    "language": "python",
    "auto_stop_minutes": 15,
    "labels": {"project": "afe", "dataset": "ieee-fraud"},
    "env_vars": {"KAGGLE_KEY": "..."},
    "resources": {"cpu": 2, "memory": 4, "disk": 20},
    "agent_count": 0,
    "agents": [],
    "last_activity": null,
    "created_at": 1712345678,
    "updated_at": 1712345678,
    "error_message": null
}
```

**Errors:**
- `502 Bad Gateway` -- Daytona API unreachable or returned an error. Body: `{"error": "Daytona API error: <detail>"}`.

**Implementation:**

```python
@app.post("/sandboxes", status_code=201)
async def create_sandbox(request: Request):
    data = await request.json()
    name = data.get("name")
    image = data.get("image", "ubuntu:22.04")
    language = data.get("language", "python")
    auto_stop = data.get("auto_stop_minutes", 15)
    labels = data.get("labels", {})
    env_vars = data.get("env_vars", {})
    resources = data.get("resources", {})

    # Unique name check
    if name:
        existing = _db().execute(
            "SELECT id FROM sandboxes WHERE name = ?", (name,)
        ).fetchone()
        if existing:
            return JSONResponse(
                {"error": f"Sandbox name '{name}' already exists"},
                status_code=409,
            )

    try:
        daytona = _get_daytona_client()
        sandbox = daytona.create(CreateSandboxParams(
            language=language,
            image=image,
            auto_stop_interval=0,  # We manage auto-stop ourselves
            labels={**labels, "afe_managed": "true"},
            env_vars=env_vars,
            resources=resources if resources else None,
        ))
    except Exception as e:
        return JSONResponse(
            {"error": f"Daytona API error: {str(e)}"},
            status_code=502,
        )

    now = int(time.time())
    row = {
        "id": sandbox.id,
        "name": name,
        "state": "started",
        "daytona_state": sandbox.state,
        "image": image,
        "language": language,
        "auto_stop_minutes": auto_stop,
        "labels": json.dumps(labels),
        "env_vars": json.dumps(env_vars),
        "resources": json.dumps(resources),
        "agent_count": 0,
        "last_activity": None,
        "created_at": now,
        "updated_at": now,
        "error_message": None,
    }

    conn = _db()
    conn.execute(
        """INSERT INTO sandboxes
           (id, name, state, daytona_state, image, language, auto_stop_minutes,
            labels, env_vars, resources, agent_count, last_activity,
            created_at, updated_at, error_message)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (row["id"], row["name"], row["state"], row["daytona_state"],
         row["image"], row["language"], row["auto_stop_minutes"],
         row["labels"], row["env_vars"], row["resources"],
         row["agent_count"], row["last_activity"],
         row["created_at"], row["updated_at"], row["error_message"]),
    )
    conn.commit()

    SANDBOXES[sandbox.id] = {**row, "daytona_obj": sandbox}

    return _sandbox_response(sandbox.id)
```

#### `GET /sandboxes` -- List Sandboxes

**Response (200):**
```json
[
    {
        "id": "sbx_abc123",
        "name": "shared-workspace",
        "state": "started",
        "image": "ubuntu:22.04",
        "language": "python",
        "auto_stop_minutes": 15,
        "labels": {"project": "afe"},
        "agent_count": 2,
        "agents": ["prep-worker", "analyst"],
        "last_activity": 1712345700,
        "created_at": 1712345678,
        "updated_at": 1712345700,
        "error_message": null
    }
]
```

The `agents` field is computed by querying agents with matching `sandbox_id`.

#### `GET /sandboxes/{sandbox_id}` -- Get Sandbox

**Response (200):** Same shape as a single item from the list endpoint.

**Errors:**
- `404 Not Found` -- `{"error": "sandbox not found"}`

#### `POST /sandboxes/{sandbox_id}/start` -- Start Sandbox

Starts a stopped sandbox. Idempotent: if the sandbox is already started, returns 200 with current state.

**Response (200):**
```json
{
    "id": "sbx_abc123",
    "state": "started",
    "previous_state": "stopped"
}
```

**Errors:**
- `404` -- Sandbox not found.
- `409 Conflict` -- Sandbox is in `creating` or `error` state and cannot be started. Body: `{"error": "Cannot start sandbox in state 'error'"}`.
- `502` -- Daytona API failure.

**Implementation notes:**
- Must acquire `SANDBOX_LOCKS[sandbox_id]` to prevent concurrent start/stop.
- If already `"started"`, return immediately (idempotent).
- If `"stopped"`, call `sandbox.start()` via Daytona, update state to `"started"`.

#### `POST /sandboxes/{sandbox_id}/stop` -- Stop Sandbox

Stops a running sandbox. Preserves all filesystem state. Idempotent.

**Response (200):**
```json
{
    "id": "sbx_abc123",
    "state": "stopped",
    "previous_state": "started"
}
```

**Errors:**
- `404` -- Sandbox not found.
- `409` -- Cannot stop sandbox in `creating` state.
- `502` -- Daytona API failure.

#### `DELETE /sandboxes/{sandbox_id}` -- Delete Sandbox

Permanently removes the sandbox from Daytona and from the local database.

**Behavior when agents are attached:**
- All attached agents have their `sandbox_id` set to `NULL`.
- Agents are NOT deleted -- they revert to local execution mode.
- An SSE event `SandboxDetached` is emitted for each affected agent.

**Response (200):**
```json
{
    "status": "deleted",
    "detached_agents": ["prep-worker", "analyst"]
}
```

**Errors:**
- `404` -- Sandbox not found.
- `502` -- Daytona API failure during deletion. In this case, the sandbox is marked as `"error"` in SQLite with the error message, but is NOT removed from the database. The caller can retry.

#### `POST /sandboxes/{sandbox_id}/exec` -- Execute Command

Executes a shell command inside the sandbox.

**Request:**
```json
{
    "command": "pip install pandas && python train.py",
    "timeout": 30
}
```

`timeout` is optional, in seconds, defaults to 60.

**Response (200):**
```json
{
    "exit_code": 0,
    "stdout": "Successfully installed pandas-2.1.0\n...",
    "sandbox_id": "sbx_abc123"
}
```

**Errors:**
- `404` -- Sandbox not found.
- `409` -- Sandbox not in `"started"` state.
- `504 Gateway Timeout` -- Command exceeded timeout.

#### `GET /sandboxes/{sandbox_id}/files?path=/workspace/` -- List Files

**Response (200):**
```json
{
    "path": "/workspace/",
    "entries": [
        {"name": "data.csv", "type": "file", "size": 1024},
        {"name": "models/", "type": "directory"}
    ]
}
```

#### `GET /sandboxes/{sandbox_id}/files/read?path=/workspace/data.csv` -- Read File

**Response (200):**
```json
{
    "path": "/workspace/data.csv",
    "content": "col1,col2\n1,2\n3,4\n"
}
```

For binary files, content is base64-encoded and `"encoding": "base64"` is included.

#### `POST /sandboxes/{sandbox_id}/files/write` -- Write File

**Request:**
```json
{
    "path": "/workspace/output.csv",
    "content": "col1,col2\n5,6\n"
}
```

**Response (200):**
```json
{
    "path": "/workspace/output.csv",
    "status": "written"
}
```

### 2.2 Modified Agent Endpoints

#### `POST /agents` -- Register Agent (Modified)

New optional fields in the request body:

| Field | Type | Default | Description |
|---|---|---|---|
| `sandbox` | `bool` | `false` | If `true`, create a new sandbox for this agent. |
| `sandbox_id` | `string` | `null` | Attach to an existing sandbox by ID. |
| `sandbox_config` | `object` | `{}` | Configuration for inline sandbox creation (image, env_vars, resources, labels). Only used when `sandbox=true`. |

**Mutual exclusion:** `sandbox=true` and `sandbox_id="..."` cannot both be set. Returns `400` if both are provided.

**Request -- inline sandbox creation:**
```json
{
    "name": "prep-worker",
    "model": "claude-sonnet-4-6",
    "tools": ["Bash", "Read", "Write", "Glob", "Grep"],
    "prompt": "You are a data preparation agent...",
    "sandbox": true,
    "sandbox_config": {
        "image": "ubuntu:22.04",
        "env_vars": {"KAGGLE_KEY": "..."},
        "resources": {"cpu": 2, "memory": 4}
    }
}
```

**Request -- attach to existing sandbox:**
```json
{
    "name": "analyst",
    "model": "claude-sonnet-4-6",
    "sandbox_id": "sbx_abc123"
}
```

**Request -- local execution (unchanged):**
```json
{
    "name": "reviewer",
    "model": "claude-opus-4-6"
}
```

**Response (200):**
```json
{
    "name": "prep-worker",
    "model": "claude-sonnet-4-6",
    "cwd": "/workspace",
    "tools": ["Bash", "Read", "Write", "Glob", "Grep"],
    "sandbox_id": "sbx_abc123",
    "sandbox_state": "started"
}
```

When `sandbox=true`, the agent's `cwd` is automatically set to `"/workspace"` (the Daytona sandbox working directory). When `sandbox_id` is provided and `cwd` is not explicitly set, `cwd` also defaults to `"/workspace"`.

**Implementation:**

```python
@app.post("/agents")
async def register_agent(request: Request):
    data = await request.json()
    agent_name = data.get("name", str(uuid.uuid4()))
    sandbox_inline = data.get("sandbox", False)
    sandbox_id = data.get("sandbox_id")
    sandbox_config = data.get("sandbox_config", {})

    if sandbox_inline and sandbox_id:
        return JSONResponse(
            {"error": "Cannot set both 'sandbox' and 'sandbox_id'"},
            status_code=400,
        )

    # Handle inline sandbox creation
    if sandbox_inline:
        try:
            sandbox_resp = await _create_sandbox_internal(
                name=None,  # Anonymous sandbox, named after agent
                labels={"agent": agent_name, "project": "afe"},
                **sandbox_config,
            )
            sandbox_id = sandbox_resp["id"]
        except Exception as e:
            return JSONResponse(
                {"error": f"Failed to create sandbox: {str(e)}"},
                status_code=502,
            )

    # Handle attachment to existing sandbox
    if sandbox_id:
        sbx = SANDBOXES.get(sandbox_id)
        if not sbx:
            return JSONResponse(
                {"error": f"Sandbox '{sandbox_id}' not found"},
                status_code=404,
            )
        # Increment reference count
        _sandbox_attach(sandbox_id, agent_name)

    cwd = data.get("cwd")
    if sandbox_id and not cwd:
        cwd = "/workspace"

    AGENTS[agent_name] = {
        "name": agent_name,
        "model": data.get("model", "claude-sonnet-4-6"),
        "cwd": cwd or os.getcwd(),
        "tools": data.get("tools", ALL_TOOLS),
        "prompt": data.get("prompt", ""),
        "sandbox_id": sandbox_id,
    }

    conn = _db()
    conn.execute(
        "INSERT OR REPLACE INTO agents (name, resume_id, config, sandbox_id) VALUES (?, ?, ?, ?)",
        (agent_name, None, json.dumps(AGENTS[agent_name]), sandbox_id),
    )
    conn.commit()

    _ensure_agent_worker(agent_name)
    # ... emit AgentRegistered event ...
    return AGENTS[agent_name]
```

#### `DELETE /agents/{agent_name}` -- Delete Agent (Modified)

When deleting an agent that is attached to a sandbox:
1. Decrement the sandbox's `agent_count`.
2. If `agent_count` reaches 0 and `auto_stop_minutes > 0`, start the auto-stop timer.
3. The sandbox itself is NOT deleted -- it continues to exist independently.

```python
@app.delete("/agents/{agent_name}")
async def delete_agent(agent_name: str):
    config = AGENTS.get(agent_name, {})
    sandbox_id = config.get("sandbox_id")

    # Detach from sandbox
    if sandbox_id:
        _sandbox_detach(sandbox_id, agent_name)

    # Existing cleanup logic (unchanged)
    AGENTS.pop(agent_name, None)
    AGENT_QUEUES.pop(agent_name, None)
    AGENT_SSE_SUBSCRIBERS.pop(agent_name, None)
    worker = AGENT_WORKERS.pop(agent_name, None)
    if worker and not worker.done():
        worker.cancel()

    conn = _db()
    conn.execute("DELETE FROM agents WHERE name = ?", (agent_name,))
    conn.execute("DELETE FROM runs WHERE agent_name = ?", (agent_name,))
    conn.commit()

    return {"status": "deleted", "sandbox_id": sandbox_id}
```

#### `POST /agents/{agent_name}/message` -- Send Message (Modified)

Before processing the message, auto-start the sandbox if it is stopped:

1. Look up the agent's `sandbox_id`.
2. If `sandbox_id` is set and sandbox state is `"stopped"`, start it.
3. If the sandbox is in `"creating"` state, wait up to 30 seconds for it to become `"started"`.
4. If the sandbox is in `"error"` state, return `409` immediately.
5. Record `last_activity` on the sandbox.
6. Reset the auto-stop timer.

The message processing itself is unchanged -- the sandbox routing happens inside `_process_agent_message` (see Section 3).

```python
@app.post("/agents/{agent_name}/message")
async def post_agent_message(
    agent_name: str,
    message: str = Form(...),
    source: str = Form("user"),
):
    if agent_name not in AGENTS:
        return JSONResponse({"error": "agent not found"}, status_code=404)

    config = AGENTS[agent_name]
    sandbox_id = config.get("sandbox_id")

    # Auto-start sandbox if needed
    if sandbox_id:
        try:
            await _ensure_sandbox_started(sandbox_id)
        except SandboxError as e:
            return JSONResponse({"error": str(e)}, status_code=409)

    _ensure_agent_worker(agent_name)
    session_id = config.get("session_id", agent_name)
    run_id = str(uuid.uuid4())

    await AGENT_QUEUES[agent_name].put({
        "message": message,
        "source": source,
        "session_id": session_id,
        "run_id": run_id,
    })

    _emit({
        "event": "RunStarted",
        "session_id": session_id,
        "run_id": run_id,
        "agent_name": agent_name,
        "sandbox_id": sandbox_id,
        "created_at": int(time.time()),
    })

    return JSONResponse({
        "session_id": session_id,
        "run_id": run_id,
        "status": "ok",
        "sandbox_id": sandbox_id,
    })
```

### 2.3 New SSE Events

| Event | Payload | When |
|---|---|---|
| `SandboxStarted` | `{sandbox_id, agent_name, started_at}` | Sandbox auto-started before message processing |
| `SandboxStopped` | `{sandbox_id, stopped_at}` | Sandbox auto-stopped after idle timeout |
| `SandboxDetached` | `{sandbox_id, agent_name}` | Agent detached from sandbox (sandbox deleted or agent moved) |
| `SandboxError` | `{sandbox_id, error, agent_name}` | Sandbox operation failed |

---

## 3. Server-Side Architecture

### 3.1 In-Memory State

New global dictionaries alongside existing ones:

```python
# Existing
AGENTS: dict[str, dict] = {}
AGENT_QUEUES: dict[str, asyncio.Queue] = {}
AGENT_SSE_SUBSCRIBERS: dict[str, list[asyncio.Queue]] = {}
AGENT_WORKERS: dict[str, asyncio.Task] = {}

# New
SANDBOXES: dict[str, dict] = {}
# Keys: sandbox_id
# Values: {
#     "id": str,
#     "name": str | None,
#     "state": str,
#     "daytona_obj": Sandbox | None,  # Live Daytona SDK object
#     "auto_stop_minutes": int,
#     "agent_count": int,
#     "agents": set[str],  # Set of attached agent names
#     "last_activity": int | None,
#     ... (mirrors DB row)
# }

SANDBOX_LOCKS: dict[str, asyncio.Lock] = {}
# Per-sandbox locks to serialize start/stop operations

SANDBOX_AUTO_STOP_TASKS: dict[str, asyncio.Task] = {}
# Per-sandbox auto-stop timer tasks
```

### 3.2 Daytona Client Lifecycle

The Daytona client is a singleton initialized lazily on first use:

```python
_daytona_client: Daytona | None = None


def _get_daytona_client() -> Daytona:
    """Lazy singleton. Uses DAYTONA_API_KEY and DAYTONA_SERVER_URL env vars."""
    global _daytona_client
    if _daytona_client is None:
        api_key = os.environ.get("DAYTONA_API_KEY")
        server_url = os.environ.get("DAYTONA_SERVER_URL")
        if not api_key:
            raise RuntimeError(
                "DAYTONA_API_KEY not set. "
                "Sandbox features require Daytona credentials."
            )
        _daytona_client = Daytona()
    return _daytona_client
```

If `DAYTONA_API_KEY` is not set, the server starts normally but all sandbox operations return `501 Not Implemented` with a clear error message. This preserves backward compatibility for users running without Daytona.

### 3.3 Startup: Loading Sandboxes From DB

```python
@app.on_event("startup")
async def load_persisted_agents():
    conn = _db()

    # Load sandboxes first
    for row in conn.execute("SELECT * FROM sandboxes").fetchall():
        sandbox_id = row["id"]
        agents_attached = conn.execute(
            "SELECT name FROM agents WHERE sandbox_id = ?", (sandbox_id,)
        ).fetchall()
        agent_names = {r["name"] for r in agents_attached}

        SANDBOXES[sandbox_id] = {
            "id": sandbox_id,
            "name": row["name"],
            "state": row["state"],
            "daytona_obj": None,  # Reconnected lazily
            "daytona_state": row["daytona_state"],
            "image": row["image"],
            "language": row["language"],
            "auto_stop_minutes": row["auto_stop_minutes"],
            "labels": json.loads(row["labels"]),
            "env_vars": json.loads(row["env_vars"]),
            "resources": json.loads(row["resources"]),
            "agent_count": len(agent_names),
            "agents": agent_names,
            "last_activity": row["last_activity"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
            "error_message": row["error_message"],
        }
        SANDBOX_LOCKS[sandbox_id] = asyncio.Lock()

    # Load agents (existing logic, extended)
    for row in conn.execute("SELECT name, resume_id, config, sandbox_id FROM agents").fetchall():
        name = row["name"]
        config = json.loads(row["config"]) if row["config"] else {}
        if name not in AGENTS:
            AGENTS[name] = config
            AGENTS[name]["resume_id"] = row["resume_id"]
            AGENTS[name]["sandbox_id"] = row["sandbox_id"]

    # Trigger async reconciliation (non-blocking)
    asyncio.create_task(_reconcile_sandboxes())
```

### 3.4 Tool Execution Routing

The key insight is that the claude-agent-sdk spawns a `claude` subprocess which executes tools locally relative to the process's working directory. If that subprocess runs **inside** the Daytona sandbox, all tools (Bash, Read, Write, Edit, Glob, Grep) naturally execute inside the sandbox filesystem with no remapping required.

**Selected approach: Run the agent inside the sandbox**

For sandboxed agents, `_process_agent_message` does not call the local `query()` function. Instead, it:

1. Ensures the sandbox is started.
2. Installs `claude-agent-sdk` inside the sandbox on first use (see Section 3.6).
3. Writes a `sandbox_runner.py` script to the sandbox.
4. Executes the runner via `sandbox.process.exec()`, passing the message and options as arguments.
5. Parses the JSON-newline output and emits SSE events.

Because the runner and the `claude` subprocess both execute inside the sandbox, all tool operations (Bash commands, file reads/writes, etc.) affect the sandbox filesystem directly. No MCP proxy, no tool remapping, no FUSE mount needed.

```python
async def _process_agent_message(message, source, agent_name, session_id, config, resume_id, run_id):
    """Run an Agent SDK query for a specific agent."""
    sandbox_id = config.get("sandbox_id")

    if sandbox_id:
        # Delegate entirely to sandbox execution
        return await _process_agent_message_in_sandbox(
            message, source, agent_name, session_id, config, resume_id, run_id, sandbox_id
        )

    # Local (non-sandbox) execution -- existing logic unchanged
    # ...
```

### 3.5 Auth Strategy for Sandbox Execution

The `claude` CLI and `claude-agent-sdk` running inside the sandbox need Anthropic API credentials. Two mechanisms are supported:

**Primary: `ANTHROPIC_API_KEY` env var**

Pass `ANTHROPIC_API_KEY` as an environment variable when creating or starting the sandbox. The sandbox process inherits it, and the `claude` subprocess picks it up automatically.

```python
# On sandbox creation, inject the API key
env_vars = {
    "ANTHROPIC_API_KEY": os.environ.get("ANTHROPIC_API_KEY", ""),
    **user_provided_env_vars,
}
```

**Fallback: OAuth access token from server process**

If the server itself is authenticated via OAuth (e.g., the user ran `claude login`), the server can extract the current access token from its own credential store and pass it to the sandbox. OAuth tokens expire roughly every hour; the server refreshes the token before each agent run and updates the sandbox environment variables with the fresh token.

```python
def _get_auth_env_for_sandbox() -> dict[str, str]:
    """Return auth env vars to inject into the sandbox for this run."""
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if api_key:
        return {"ANTHROPIC_API_KEY": api_key}

    # Attempt to extract OAuth token from local credential store
    token = _read_oauth_access_token()
    if token:
        return {"ANTHROPIC_API_KEY": token}  # SDK accepts OAuth tokens here too

    raise RuntimeError(
        "No Anthropic credentials available. "
        "Set ANTHROPIC_API_KEY or run 'claude login'."
    )
```

The auth env dict is merged into the sandbox's environment on every `sandbox.process.exec()` call that runs the agent runner, ensuring the token is always fresh even if it was refreshed mid-session.

### 3.6 Sandbox-Aware `_process_agent_message`

For sandboxed agents, `_process_agent_message` delegates to `_process_agent_message_in_sandbox`. The steps are:

1. Ensure sandbox is started (`_ensure_sandbox_started`).
2. Install `claude-agent-sdk` inside the sandbox on first run (idempotent `pip install` guarded by a marker file).
3. Write `sandbox_runner.py` to `/workspace/.afe/sandbox_runner.py` inside the sandbox (overwrite each time to pick up any updates).
4. Execute the runner via `sandbox.process.exec()`, passing the message, model, tools, resume ID, and session options as JSON via a temp file or env var.
5. Parse the newline-delimited JSON output and emit SSE events (`RunContent`, `ToolCallStarted`) exactly as the local path does.
6. Update `resume_id` from the runner's final `ResultMessage` output.

```python
_SANDBOX_SDK_MARKER = "/workspace/.afe/.sdk_installed"

async def _ensure_sdk_in_sandbox(sandbox: Any):
    """Install claude-agent-sdk in the sandbox if not already present."""
    check = sandbox.process.exec(f"test -f {_SANDBOX_SDK_MARKER} && echo ok || echo missing")
    if "missing" in check.result:
        sandbox.process.exec(
            "pip install claude-agent-sdk --quiet && "
            f"mkdir -p /workspace/.afe && touch {_SANDBOX_SDK_MARKER}",
            timeout=120,
        )


async def _process_agent_message_in_sandbox(
    message, source, agent_name, session_id, config, resume_id, run_id, sandbox_id
):
    accumulated_text = ""
    tools_used = []

    sbx = SANDBOXES.get(sandbox_id)
    if not sbx or sbx.get("state") != "started":
        raise RuntimeError(f"Sandbox {sandbox_id} is not started")

    daytona_obj = sbx["daytona_obj"]
    await _ensure_sdk_in_sandbox(daytona_obj)

    # Write runner script to sandbox
    runner_src = _get_sandbox_runner_source()
    daytona_obj.fs.write("/workspace/.afe/sandbox_runner.py", runner_src)

    # Serialize invocation options as JSON and write to sandbox
    options_payload = json.dumps({
        "message": message,
        "model": config.get("model", "claude-sonnet-4-6"),
        "tools": config.get("tools", ALL_TOOLS),
        "resume_id": resume_id,
        "session_id": session_id,
        "agent_name": agent_name,
        "sandbox_id": sandbox_id,
        "system_prompt": config.get("prompt", ""),
        "cwd": "/workspace",
    })
    daytona_obj.fs.write("/workspace/.afe/run_options.json", options_payload)

    # Build auth env for this run (always fresh)
    auth_env = _get_auth_env_for_sandbox()

    # Execute runner -- output is newline-delimited JSON events
    result = daytona_obj.process.exec(
        "python /workspace/.afe/sandbox_runner.py /workspace/.afe/run_options.json",
        env=auth_env,
        timeout=600,
    )

    # Parse output and emit SSE events
    new_resume_id = resume_id
    for line in result.result.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            continue

        event_type = event.get("event")
        if event_type == "RunContent":
            accumulated_text = event.get("content", accumulated_text)
            _emit({**event, "agent_name": agent_name, "session_id": session_id, "run_id": run_id})
        elif event_type == "ToolCallStarted":
            tools_used.extend(event.get("tools", []))
            _emit({**event, "agent_name": agent_name, "session_id": session_id, "run_id": run_id})
        elif event_type == "Result":
            new_resume_id = event.get("resume_id", resume_id)

    sbx["last_activity"] = int(time.time())
    _reset_auto_stop_timer(sandbox_id)

    conn = _db()
    conn.execute(
        "INSERT INTO runs (agent_name, session_id, source, input, content, tools, sandbox_id, created_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (agent_name, session_id, source, message, accumulated_text,
         json.dumps(tools_used), sandbox_id, int(time.time())),
    )
    conn.commit()
    return accumulated_text, new_resume_id
```

**`sandbox_runner.py`** is a small script that lives at `src/api/sandbox_runner.py` in the repo and is uploaded to `/workspace/.afe/sandbox_runner.py` at runtime. It calls `query()` from the locally-installed `claude-agent-sdk`, streams results, and writes newline-delimited JSON to stdout. Authentication uses the `ANTHROPIC_API_KEY` env var injected by the server.

```python
"""Runner script executed inside the Daytona sandbox.

Reads options from a JSON file, calls claude-agent-sdk query(), and
writes newline-delimited JSON events to stdout for the server to parse.
"""
import json
import sys
import asyncio
from claude_agent_sdk import query, ClaudeAgentOptions, AssistantMessage, ResultMessage


async def main(options_path: str):
    with open(options_path) as f:
        opts = json.load(f)

    accumulated_text = ""
    async for msg in query(
        prompt=opts["message"],
        options=ClaudeAgentOptions(
            allowed_tools=opts.get("tools"),
            permission_mode="bypassPermissions",
            model=opts.get("model", "claude-sonnet-4-6"),
            effort="max",
            resume=opts.get("resume_id"),
            cwd=opts.get("cwd", "/workspace"),
            system_prompt=opts.get("system_prompt") or None,
        ),
    ):
        if isinstance(msg, AssistantMessage):
            for block in msg.content:
                if hasattr(block, "text"):
                    accumulated_text += block.text
                    print(json.dumps({"event": "RunContent", "content": accumulated_text}), flush=True)
                elif hasattr(block, "name"):
                    tool_record = {
                        "role": "tool",
                        "tool_name": block.name,
                        "tool_args": getattr(block, "input", {}),
                    }
                    print(json.dumps({"event": "ToolCallStarted", "tools": [tool_record]}), flush=True)
        elif isinstance(msg, ResultMessage):
            print(json.dumps({"event": "Result", "resume_id": msg.session_id}), flush=True)


if __name__ == "__main__":
    asyncio.run(main(sys.argv[1]))
```

### 3.7 Auto-Stop Timer Mechanism

```python
async def _auto_stop_worker(sandbox_id: str, delay_minutes: int):
    """Background task: stop sandbox after idle timeout."""
    try:
        await asyncio.sleep(delay_minutes * 60)

        sbx = SANDBOXES.get(sandbox_id)
        if not sbx:
            return

        # Check if any attached agent is busy
        for agent_name in sbx.get("agents", set()):
            config = AGENTS.get(agent_name, {})
            if config.get("status") == "busy":
                # Reset timer -- agent is still working
                _reset_auto_stop_timer(sandbox_id)
                return

        # Check if activity happened since timer started
        now = int(time.time())
        last = sbx.get("last_activity") or 0
        elapsed_minutes = (now - last) / 60
        if elapsed_minutes < delay_minutes:
            # Activity happened after timer was set; restart with remaining time
            remaining = delay_minutes - elapsed_minutes
            SANDBOX_AUTO_STOP_TASKS[sandbox_id] = asyncio.create_task(
                _auto_stop_worker(sandbox_id, remaining)
            )
            return

        # Actually stop the sandbox
        await _stop_sandbox_internal(sandbox_id)
        _emit_global({
            "event": "SandboxStopped",
            "sandbox_id": sandbox_id,
            "stopped_at": int(time.time()),
        })

    except asyncio.CancelledError:
        return


def _reset_auto_stop_timer(sandbox_id: str):
    """Cancel existing timer and start a new one."""
    sbx = SANDBOXES.get(sandbox_id)
    if not sbx:
        return

    auto_stop = sbx.get("auto_stop_minutes", 15)
    if auto_stop <= 0:
        return  # Auto-stop disabled

    # Cancel existing timer
    existing = SANDBOX_AUTO_STOP_TASKS.get(sandbox_id)
    if existing and not existing.done():
        existing.cancel()

    # Start new timer
    SANDBOX_AUTO_STOP_TASKS[sandbox_id] = asyncio.create_task(
        _auto_stop_worker(sandbox_id, auto_stop)
    )
```

### 3.8 Reference Counting

```python
def _sandbox_attach(sandbox_id: str, agent_name: str):
    """Attach an agent to a sandbox. Increments reference count."""
    sbx = SANDBOXES.get(sandbox_id)
    if not sbx:
        return
    sbx.setdefault("agents", set()).add(agent_name)
    sbx["agent_count"] = len(sbx["agents"])

    conn = _db()
    conn.execute(
        "UPDATE sandboxes SET agent_count = ?, updated_at = ? WHERE id = ?",
        (sbx["agent_count"], int(time.time()), sandbox_id),
    )
    conn.commit()


def _sandbox_detach(sandbox_id: str, agent_name: str):
    """Detach an agent from a sandbox. Decrements reference count.
    Starts auto-stop timer if count reaches 0.
    """
    sbx = SANDBOXES.get(sandbox_id)
    if not sbx:
        return
    sbx.get("agents", set()).discard(agent_name)
    sbx["agent_count"] = len(sbx.get("agents", set()))

    conn = _db()
    conn.execute(
        "UPDATE sandboxes SET agent_count = ?, updated_at = ? WHERE id = ?",
        (sbx["agent_count"], int(time.time()), sandbox_id),
    )
    conn.commit()

    # Start auto-stop timer if no agents remain
    if sbx["agent_count"] == 0 and sbx.get("auto_stop_minutes", 15) > 0:
        _reset_auto_stop_timer(sandbox_id)
```

### 3.9 Internal Helper Functions

```python
class SandboxError(Exception):
    """Raised when a sandbox operation fails."""
    pass


async def _ensure_sandbox_started(sandbox_id: str, timeout: float = 30.0):
    """Ensure a sandbox is in 'started' state. Auto-starts if stopped.

    Raises SandboxError if the sandbox cannot be started.
    """
    sbx = SANDBOXES.get(sandbox_id)
    if not sbx:
        raise SandboxError(f"Sandbox {sandbox_id} not found")

    if sbx["state"] == "started":
        return  # Already running

    if sbx["state"] == "error":
        raise SandboxError(
            f"Sandbox {sandbox_id} is in error state: {sbx.get('error_message')}"
        )

    lock = SANDBOX_LOCKS.setdefault(sandbox_id, asyncio.Lock())
    async with lock:
        # Re-check after acquiring lock (another coroutine may have started it)
        if sbx["state"] == "started":
            return

        if sbx["state"] not in ("stopped", "creating"):
            raise SandboxError(
                f"Cannot start sandbox in state '{sbx['state']}'"
            )

        try:
            daytona_obj = sbx.get("daytona_obj")
            if not daytona_obj:
                daytona = _get_daytona_client()
                daytona_obj = daytona.get(sandbox_id)
                sbx["daytona_obj"] = daytona_obj

            daytona_obj.start()
            sbx["state"] = "started"
            sbx["daytona_state"] = "started"
            sbx["error_message"] = None

            now = int(time.time())
            conn = _db()
            conn.execute(
                "UPDATE sandboxes SET state = ?, daytona_state = ?, error_message = NULL, updated_at = ? WHERE id = ?",
                ("started", "started", now, sandbox_id),
            )
            conn.commit()

        except Exception as e:
            sbx["state"] = "error"
            sbx["error_message"] = str(e)
            conn = _db()
            conn.execute(
                "UPDATE sandboxes SET state = 'error', error_message = ?, updated_at = ? WHERE id = ?",
                (str(e), int(time.time()), sandbox_id),
            )
            conn.commit()
            raise SandboxError(f"Failed to start sandbox: {e}")


async def _stop_sandbox_internal(sandbox_id: str):
    """Stop a sandbox. Used by auto-stop timer and explicit stop endpoint."""
    sbx = SANDBOXES.get(sandbox_id)
    if not sbx or sbx["state"] != "started":
        return

    lock = SANDBOX_LOCKS.setdefault(sandbox_id, asyncio.Lock())
    async with lock:
        if sbx["state"] != "started":
            return

        try:
            daytona_obj = sbx.get("daytona_obj")
            if daytona_obj:
                daytona_obj.stop()
            sbx["state"] = "stopped"
            sbx["daytona_state"] = "stopped"

            now = int(time.time())
            conn = _db()
            conn.execute(
                "UPDATE sandboxes SET state = 'stopped', daytona_state = 'stopped', updated_at = ? WHERE id = ?",
                (now, sandbox_id),
            )
            conn.commit()
        except Exception as e:
            sbx["error_message"] = str(e)


def _sandbox_response(sandbox_id: str) -> dict:
    """Build the API response dict for a sandbox."""
    sbx = SANDBOXES.get(sandbox_id)
    if not sbx:
        return {}
    return {
        "id": sbx["id"],
        "name": sbx.get("name"),
        "state": sbx["state"],
        "image": sbx.get("image"),
        "language": sbx.get("language"),
        "auto_stop_minutes": sbx.get("auto_stop_minutes"),
        "labels": sbx.get("labels", {}),
        "agent_count": sbx.get("agent_count", 0),
        "agents": sorted(sbx.get("agents", set())),
        "last_activity": sbx.get("last_activity"),
        "created_at": sbx.get("created_at"),
        "updated_at": sbx.get("updated_at"),
        "error_message": sbx.get("error_message"),
    }
```

---

## 4. Agent SDK Client Changes

### 4.1 Modified `Agent.__init__`

```python
class Agent:
    def __init__(
        self,
        name: str,
        model: str | None = None,
        cwd: str | None = None,
        tools: list[str] | None = None,
        prompt: str | None = None,
        api_url: str | None = None,
        # New sandbox parameters
        sandbox: bool = False,
        sandbox_id: str | None = None,
        sandbox_config: dict | None = None,
    ):
        self.name = name
        self.sandbox = sandbox
        self.sandbox_id = sandbox_id
        self.sandbox_config = sandbox_config or {}

        # ... existing init logic unchanged ...
```

### 4.2 Modified `_registration_payload`

```python
def _registration_payload(self) -> dict[str, Any]:
    config: dict[str, Any] = {"name": self.name}
    if self.prompt is not None:
        config["prompt"] = self.prompt
    if self.model is not None:
        config["model"] = self.model
    if self.tools is not None:
        config["tools"] = self.tools
    if self.cwd is not None:
        config["cwd"] = self.cwd

    # Sandbox configuration
    if self.sandbox:
        config["sandbox"] = True
        if self.sandbox_config:
            config["sandbox_config"] = self.sandbox_config
    elif self.sandbox_id:
        config["sandbox_id"] = self.sandbox_id

    return config
```

### 4.3 New Method: `sandbox_status`

```python
async def sandbox_status(self) -> dict | None:
    """Get the status of this agent's sandbox, if attached.

    Returns None if the agent has no sandbox.
    """
    await self._ensure_registered()
    status = await self.status()
    sid = status.get("sandbox_id")
    if not sid:
        return None
    resp = await self._client.get(f"/sandboxes/{sid}")
    if resp.status_code == 200:
        return resp.json()
    return None
```

### 4.4 New Method: `sandbox_exec`

```python
async def sandbox_exec(self, command: str, timeout: int = 60) -> dict:
    """Execute a command directly in the agent's sandbox.

    Useful for setup commands (pip install, apt-get, etc.) outside of
    agent message processing.

    Returns {"exit_code": int, "stdout": str}.
    Raises RuntimeError if agent has no sandbox.
    """
    await self._ensure_registered()
    status = await self.status()
    sid = status.get("sandbox_id")
    if not sid:
        raise RuntimeError(f"Agent {self.name!r} has no sandbox attached")
    resp = await self._client.post(
        f"/sandboxes/{sid}/exec",
        json={"command": command, "timeout": timeout},
    )
    resp.raise_for_status()
    return resp.json()
```

### 4.5 Modified `send()` -- Transparent Sandbox Lifecycle

The `send()` method does NOT change. The server handles sandbox auto-start when `POST /agents/{name}/message` is called. The client is unaware of sandbox lifecycle details.

This is intentional: the SDK client should not need to know about sandbox states. The server guarantees that by the time the agent processes a message, the sandbox is started.

### 4.6 Factory Function Changes in `agents.py`

```python
def create_prep_agent(
    name: str = "prep-worker",
    cwd: str | None = None,
    sandbox: bool = False,
    sandbox_id: str | None = None,
    sandbox_config: dict | None = None,
) -> Agent:
    return Agent(
        name=name,
        model="sonnet",
        cwd=cwd,
        tools=["Bash", "Read", "Write", "Glob", "Grep"],
        prompt=_load_prompt("data_prep.md"),
        sandbox=sandbox,
        sandbox_id=sandbox_id,
        sandbox_config=sandbox_config,
    )


def create_analysis_agent(
    name: str = "analyst",
    cwd: str | None = None,
    sandbox: bool = False,
    sandbox_id: str | None = None,
    sandbox_config: dict | None = None,
) -> Agent:
    return Agent(
        name=name,
        model="sonnet",
        cwd=cwd,
        tools=["Bash", "Read", "Write", "Glob", "Grep", "WebSearch"],
        prompt=_load_prompt("data_analysis.md"),
        sandbox=sandbox,
        sandbox_id=sandbox_id,
        sandbox_config=sandbox_config,
    )


# Same pattern for create_impl_agent, create_review_agent, create_main_agent.


def create_all_agents(
    cwd: str | None = None,
    api_url: str | None = None,
    sandbox: bool = False,
    sandbox_id: str | None = None,
) -> dict[str, Agent]:
    """Create the standard set of agents.

    If sandbox=True, each agent gets its own sandbox.
    If sandbox_id is set, all agents share that sandbox.
    """
    common = {"cwd": cwd}
    if sandbox_id:
        common["sandbox_id"] = sandbox_id
    elif sandbox:
        common["sandbox"] = True

    return {
        "main": create_main_agent(**common),
        "prep-worker": create_prep_agent(**common),
        "analyst": create_analysis_agent(**common),
        "implementer": create_impl_agent(**common),
        "reviewer": create_review_agent(),  # Reviewer is read-only, no sandbox needed
    }
```

### 4.7 Usage Examples

```python
# Example 1: Agent with inline sandbox
agent = Agent("prep-worker", sandbox=True, sandbox_config={
    "image": "ubuntu:22.04",
    "env_vars": {"KAGGLE_KEY": "abc123"},
    "resources": {"cpu": 2, "memory": 4},
})
result = await agent.send("download and prepare the dataset")

# Example 2: Multiple agents sharing a sandbox
# First, create the sandbox via API or let the first agent create it
analyst = Agent("analyst", sandbox=True)
await analyst.send("begin analysis")  # Creates sandbox

# Get the sandbox ID from the analyst's status
status = await analyst.status()
sandbox_id = status["sandbox_id"]

# Attach prep-worker to the same sandbox
prep = Agent("prep-worker", sandbox_id=sandbox_id)
await prep.send("prepare the raw data")

# Example 3: Direct sandbox command execution
await agent.sandbox_exec("pip install pandas scikit-learn xgboost")
await agent.sandbox_exec("kaggle competitions download -c ieee-fraud-detection")

# Example 4: All agents sharing one sandbox
from afe import create_all_agents
agents = create_all_agents(sandbox=True)
# Each agent creates its own sandbox

# Or: shared sandbox
agents = create_all_agents(sandbox_id="sbx_abc123")
# All agents (except reviewer) share sbx_abc123
```

---

## 5. Lifecycle State Machine

### 5.1 Sandbox States

```
                    +-----------+
                    |           |
       create() -->| creating  |
                    |           |
                    +-----+-----+
                          |
                    Daytona confirms
                          |
                    +-----v-----+        stop()        +-----------+
                    |           |--------------------->|           |
                    |  started  |                      |  stopped  |
                    |           |<---------------------|           |
                    +-----+-----+        start()       +-----+-----+
                          |                                  |
                    delete()                           delete()
                          |                                  |
                    +-----v-----+                      +-----v-----+
                    |           |                      |           |
                    | (deleted) |                      | (deleted) |
                    |           |                      |           |
                    +-----------+                      +-----------+

                    Any state can transition to 'error' on failure.
                    'error' can transition to 'started' via start() retry.
```

### 5.2 State Transition Table

| Current State | Trigger | New State | Action |
|---|---|---|---|
| (none) | `POST /sandboxes` | `creating` | Call `daytona.create()` |
| `creating` | Daytona creation completes | `started` | Update DB |
| `creating` | Daytona creation fails | `error` | Store error message |
| `started` | `POST /sandboxes/{id}/stop` | `stopped` | Call `sandbox.stop()` |
| `started` | Auto-stop timer fires | `stopped` | Call `sandbox.stop()` |
| `started` | `DELETE /sandboxes/{id}` | (deleted) | Call `daytona.delete()`, remove from DB |
| `started` | Daytona error | `error` | Store error message |
| `stopped` | `POST /sandboxes/{id}/start` | `started` | Call `sandbox.start()` |
| `stopped` | Agent message (auto-start) | `started` | Call `sandbox.start()` |
| `stopped` | `DELETE /sandboxes/{id}` | (deleted) | Call `daytona.delete()`, remove from DB |
| `error` | `POST /sandboxes/{id}/start` | `started` | Retry `sandbox.start()` |
| `error` | `DELETE /sandboxes/{id}` | (deleted) | Attempt `daytona.delete()`, remove from DB regardless |

### 5.3 What Triggers Each Transition

**create (none -> creating -> started):**
- `POST /sandboxes` explicit creation.
- `POST /agents` with `sandbox=true` (inline creation).

**start (stopped -> started):**
- `POST /sandboxes/{id}/start` explicit start.
- `POST /agents/{name}/message` when agent's sandbox is stopped (auto-start).

**stop (started -> stopped):**
- `POST /sandboxes/{id}/stop` explicit stop.
- Auto-stop timer fires after all attached agents have been idle for `auto_stop_minutes`.

**delete (any -> removed):**
- `DELETE /sandboxes/{id}` explicit deletion.

**error (any -> error):**
- Daytona API failure during create, start, or stop.
- Sandbox process execution failure (disk full, OOM, etc.).

### 5.4 Reference Counting Logic

```
Agent A created with sandbox=true
  -> sandbox created, agent_count = 1, agents = {A}

Agent B created with sandbox_id = sandbox.id
  -> agent_count = 2, agents = {A, B}

Agent A deleted
  -> agent_count = 1, agents = {B}
  -> auto-stop timer NOT started (count > 0)

Agent B deleted
  -> agent_count = 0, agents = {}
  -> auto-stop timer STARTED (15 minutes)

15 minutes pass with no activity
  -> sandbox stopped automatically
```

### 5.5 Auto-Stop Timer Rules

1. **Timer starts** when `agent_count` drops to 0 (all agents detached) OR when the last attached agent transitions from `busy` to `idle`.
2. **Timer resets** on any of:
   - A new agent attaches to the sandbox.
   - Any attached agent receives a message (`POST /agents/{name}/message`).
   - Explicit `POST /sandboxes/{id}/exec` call.
3. **Timer fires** after `auto_stop_minutes` of inactivity. Before stopping:
   - Re-check that no agent is `busy`.
   - Re-check `last_activity` timestamp. If activity occurred after the timer was set, restart the timer with the remaining duration.
4. **Timer is cancelled** on:
   - Explicit `POST /sandboxes/{id}/start`.
   - Explicit `DELETE /sandboxes/{id}`.
   - `auto_stop_minutes = 0` (disabled).

### 5.6 Recovery on Server Restart

On server startup, after loading sandboxes from SQLite:

```python
async def _reconcile_sandboxes():
    """Reconcile local SQLite state with Daytona reality.

    Called once on startup as a background task.
    """
    daytona = None
    try:
        daytona = _get_daytona_client()
    except RuntimeError:
        # No Daytona credentials -- skip reconciliation
        return

    for sandbox_id, sbx in list(SANDBOXES.items()):
        try:
            remote = daytona.get(sandbox_id)
            sbx["daytona_obj"] = remote
            sbx["daytona_state"] = remote.state

            # Reconcile state mismatches
            if sbx["state"] == "started" and remote.state == "stopped":
                # Sandbox was stopped externally (e.g., Daytona auto-stop, manual)
                sbx["state"] = "stopped"
                _db().execute(
                    "UPDATE sandboxes SET state = 'stopped', daytona_state = 'stopped', updated_at = ? WHERE id = ?",
                    (int(time.time()), sandbox_id),
                )
            elif sbx["state"] == "stopped" and remote.state == "started":
                # Sandbox was started externally
                sbx["state"] = "started"
                _db().execute(
                    "UPDATE sandboxes SET state = 'started', daytona_state = 'started', updated_at = ? WHERE id = ?",
                    (int(time.time()), sandbox_id),
                )
            elif remote.state == "error":
                sbx["state"] = "error"
                _db().execute(
                    "UPDATE sandboxes SET state = 'error', daytona_state = 'error', updated_at = ? WHERE id = ?",
                    (int(time.time()), sandbox_id),
                )

        except Exception:
            # Sandbox no longer exists in Daytona
            sbx["state"] = "error"
            sbx["error_message"] = "Sandbox not found in Daytona after server restart"
            _db().execute(
                "UPDATE sandboxes SET state = 'error', error_message = ?, updated_at = ? WHERE id = ?",
                (sbx["error_message"], int(time.time()), sandbox_id),
            )

    _db().commit()

    # Rebuild reference counts from agents table
    conn = _db()
    for sandbox_id, sbx in SANDBOXES.items():
        rows = conn.execute(
            "SELECT name FROM agents WHERE sandbox_id = ?", (sandbox_id,)
        ).fetchall()
        sbx["agents"] = {r["name"] for r in rows}
        sbx["agent_count"] = len(sbx["agents"])
        conn.execute(
            "UPDATE sandboxes SET agent_count = ? WHERE id = ?",
            (sbx["agent_count"], sandbox_id),
        )
    conn.commit()

    # Start auto-stop timers for idle sandboxes
    for sandbox_id, sbx in SANDBOXES.items():
        if (sbx["state"] == "started"
            and sbx["agent_count"] == 0
            and sbx.get("auto_stop_minutes", 15) > 0):
            _reset_auto_stop_timer(sandbox_id)
```

---

## 6. Edge Cases and Error Handling

### 6.1 Server Restarts

**Problem:** Server state (SANDBOXES dict, auto-stop timers, Daytona SDK objects) is lost.

**Solution:**
- SQLite persists sandbox metadata and state.
- On startup, `_reconcile_sandboxes()` (Section 5.6) queries Daytona for each known sandbox and reconciles.
- Daytona SDK objects are re-obtained via `daytona.get(id)`.
- Auto-stop timers are re-established for idle sandboxes.
- Reference counts are rebuilt from the `agents.sandbox_id` column.

**Residual risk:** If the server was down for longer than `auto_stop_minutes`, Daytona's own auto-stop may have triggered (if we had set it). Since we set `auto_stop_interval=0` in Daytona and manage it ourselves, Daytona will NOT auto-stop. The sandbox continues running (and incurring cost) until the server restarts and reconciles. Mitigation: set a Daytona-side `auto_stop_interval` as a safety net (e.g., 60 minutes) in addition to our own timer.

### 6.2 Daytona API Failures

**Sandbox won't start:**
- `_ensure_sandbox_started()` catches the exception, sets state to `"error"`, stores the error message.
- The agent message endpoint returns `409` with the error.
- The user can retry by sending another message (which retries the start) or explicitly calling `POST /sandboxes/{id}/start`.

**Daytona API timeout:**
- All Daytona SDK calls should have a timeout. We wrap them:

```python
async def _daytona_with_timeout(fn, *args, timeout: float = 30.0):
    """Run a synchronous Daytona SDK call with a timeout."""
    loop = asyncio.get_running_loop()
    try:
        return await asyncio.wait_for(
            loop.run_in_executor(None, fn, *args),
            timeout=timeout,
        )
    except asyncio.TimeoutError:
        raise SandboxError(f"Daytona API timed out after {timeout}s")
```

**Daytona API is completely down:**
- All sandbox operations return `502`.
- Non-sandboxed agents continue working normally.
- Sandboxed agents cannot process messages (their `_ensure_sandbox_started` fails).

### 6.3 Agent Sends Message While Sandbox is Starting

**Scenario:** Agent A triggers sandbox start. While the sandbox is starting (lock held), Agent B (same sandbox) sends a message.

**Solution:** Both `_ensure_sandbox_started` calls acquire the same `SANDBOX_LOCKS[sandbox_id]`. Agent B blocks on the lock. When A's start completes and releases the lock, B re-checks state, sees `"started"`, and proceeds immediately.

```python
async def _ensure_sandbox_started(sandbox_id: str, timeout: float = 30.0):
    sbx = SANDBOXES.get(sandbox_id)
    if sbx["state"] == "started":
        return  # Fast path: no lock needed

    lock = SANDBOX_LOCKS.setdefault(sandbox_id, asyncio.Lock())
    async with lock:
        if sbx["state"] == "started":
            return  # Another coroutine started it while we waited
        # ... perform start ...
```

### 6.4 Two Agents Simultaneously Trigger Sandbox Start

Identical to 6.3. The per-sandbox `asyncio.Lock` serializes the start operations. The first acquirer starts the sandbox; the second acquirer finds it already started.

### 6.5 Sandbox Runs Out of Disk

**Detection:** `sandbox.process.exec()` commands will fail with non-zero exit codes. The agent's tool call will return an error result.

**Handling:**
- The agent sees the error in its tool result and can attempt recovery (delete files, etc.).
- The server does not proactively monitor disk usage.
- Future enhancement: periodic health check that queries `df -h` inside the sandbox and emits a `SandboxWarning` SSE event at 80% and 95% thresholds.

### 6.6 Network Partition Between Server and Daytona

**Symptoms:** All Daytona SDK calls hang or raise connection errors.

**Handling:**
- The `_daytona_with_timeout` wrapper ensures calls don't block indefinitely.
- Sandbox state in SQLite may become stale. Reconciliation on next successful contact will fix it.
- Agents attached to sandboxes cannot process messages during the partition.
- Non-sandboxed agents are unaffected.

### 6.7 Sandbox Deleted While Agent Is Processing

**Scenario:** An admin calls `DELETE /sandboxes/{id}` while Agent A is mid-execution inside that sandbox.

**Handling:**
- The delete endpoint sets state to `"stopped"` then deletes.
- The agent's in-flight tool calls to Daytona will fail (sandbox no longer exists).
- The agent's current run will produce a `RunError` event.
- The agent's `sandbox_id` is set to `NULL` (detached).
- Subsequent messages to the agent will execute locally (no sandbox).

### 6.8 Agent Re-registration (Upsert) With Different Sandbox

**Scenario:** Agent "prep-worker" was attached to sandbox A. User re-registers with `sandbox_id=B`.

**Handling:**
1. Detach from sandbox A (decrement ref count).
2. Attach to sandbox B (increment ref count).
3. Update agent config.

```python
# In register_agent:
old_sandbox = AGENTS.get(agent_name, {}).get("sandbox_id")
if old_sandbox and old_sandbox != sandbox_id:
    _sandbox_detach(old_sandbox, agent_name)
if sandbox_id and sandbox_id != old_sandbox:
    _sandbox_attach(sandbox_id, agent_name)
```

---

## 7. File Structure Changes

```
auto_feature_engineer/
  src/
    api/
      server.py                  # MODIFIED: sandbox endpoints, modified agent endpoints,
                                 #           sandbox lifecycle management, auto-stop timers,
                                 #           _process_agent_message_in_sandbox()
      sandbox_runner.py          # NEW: small script uploaded to and run inside the sandbox;
                                 #      calls query() and streams JSON events to stdout
      __init__.py
    afe/
      agent.py                   # MODIFIED: sandbox, sandbox_id, sandbox_config params;
                                 #           sandbox_status(), sandbox_exec() methods
      agents.py                  # MODIFIED: sandbox params in factory functions
      __init__.py                # MODIFIED: export new sandbox-related items
  tests/
    test_sandbox_endpoints.py    # NEW: integration tests for sandbox REST API
    test_sandbox_lifecycle.py    # NEW: unit tests for lifecycle state machine
    test_sandbox_runner.py       # NEW: tests for sandbox runner script and in-sandbox execution
  docs/
    sandbox-integration-design.md # THIS DOCUMENT
    api.md                       # MODIFIED: add sandbox endpoint documentation
```

### 7.1 Detailed File Changes

**`src/api/server.py`** -- Estimated +300 lines:
- Import `daytona_sdk` (conditional, with try/except for graceful degradation).
- Add `SANDBOXES`, `SANDBOX_LOCKS`, `SANDBOX_AUTO_STOP_TASKS` globals.
- Add `_get_daytona_client()` singleton.
- Add `SandboxError` exception class.
- Add `_sandbox_attach()`, `_sandbox_detach()`, `_sandbox_response()` helpers.
- Add `_ensure_sandbox_started()`, `_stop_sandbox_internal()`, `_create_sandbox_internal()`.
- Add `_auto_stop_worker()`, `_reset_auto_stop_timer()`.
- Add `_reconcile_sandboxes()`.
- Add `_get_auth_env_for_sandbox()`, `_ensure_sdk_in_sandbox()`, `_get_sandbox_runner_source()`.
- Add `_process_agent_message_in_sandbox()`.
- Add 10 new endpoints: `POST /sandboxes`, `GET /sandboxes`, `GET /sandboxes/{id}`, `POST /sandboxes/{id}/start`, `POST /sandboxes/{id}/stop`, `DELETE /sandboxes/{id}`, `POST /sandboxes/{id}/exec`, `GET /sandboxes/{id}/files`, `GET /sandboxes/{id}/files/read`, `POST /sandboxes/{id}/files/write`.
- Modify `register_agent()`: sandbox creation/attachment logic.
- Modify `delete_agent()`: sandbox detachment logic.
- Modify `post_agent_message()`: auto-start sandbox.
- Modify `_process_agent_message()`: branch to `_process_agent_message_in_sandbox()` when `sandbox_id` is set.
- Modify `load_persisted_agents()`: load sandboxes, trigger reconciliation.
- Modify `_db()`: add sandboxes table, alter agents/runs tables.

**`src/api/sandbox_runner.py`** -- New file, ~50 lines:
- Standalone script that the server uploads to `/workspace/.afe/sandbox_runner.py` at runtime.
- Reads options from a JSON file path passed as `sys.argv[1]`.
- Calls `claude-agent-sdk`'s `query()` with `cwd="/workspace"` and `permission_mode="bypassPermissions"`.
- Writes newline-delimited JSON events to stdout.
- No Daytona dependency -- authenticates via `ANTHROPIC_API_KEY` env var.

**`src/afe/agent.py`** -- Estimated +40 lines:
- Add `sandbox`, `sandbox_id`, `sandbox_config` to `__init__`.
- Update `_registration_payload()`.
- Add `sandbox_status()` method.
- Add `sandbox_exec()` method.

**`src/afe/agents.py`** -- Estimated +30 lines:
- Add `sandbox`, `sandbox_id`, `sandbox_config` params to all factory functions.
- Update `create_all_agents()` to support shared sandbox mode.

**`src/afe/__init__.py`** -- No new exports needed (sandbox methods are on Agent instances).

---

## 8. Dependencies

### 8.1 New Python Dependencies

Add to `pyproject.toml`:

```toml
dependencies = [
    "pyyaml",
    "claude-agent-sdk",
    "fastapi",
    "uvicorn[standard]",
    "httpx",
    "daytona-sdk",       # NEW
]
```

### 8.2 Environment Variables

| Variable | Required | Default | Description |
|---|---|---|---|
| `DAYTONA_API_KEY` | Only for sandbox features | (none) | Daytona API authentication key |
| `DAYTONA_SERVER_URL` | Only for sandbox features | (none) | Daytona server URL |

If neither is set, the server starts normally with sandbox endpoints returning `501`.

---

## 9. Implementation Sequence

### Phase 1: Data Model and Sandbox CRUD (1-2 days)
1. SQLite schema changes in `_db()`.
2. `SANDBOXES` in-memory state.
3. Daytona client singleton.
4. `POST/GET/DELETE /sandboxes` endpoints.
5. `POST /sandboxes/{id}/start` and `stop` endpoints.

### Phase 2: Agent-Sandbox Binding (1 day)
1. Modified `POST /agents` with `sandbox`/`sandbox_id` support.
2. `_sandbox_attach()` / `_sandbox_detach()` reference counting.
3. Modified `DELETE /agents/{name}` with detachment.

### Phase 3: In-Sandbox Agent Execution (1-2 days)
1. `sandbox_runner.py` -- small script that runs `query()` inside the sandbox.
2. `_ensure_sdk_in_sandbox()` -- idempotent SDK install on first use.
3. `_get_auth_env_for_sandbox()` -- credential injection (API key or OAuth token).
4. `_process_agent_message_in_sandbox()` -- upload runner, exec, parse JSON output.
5. Integration testing with a real Daytona sandbox.

### Phase 4: Lifecycle Automation (1 day)
1. Auto-start in `post_agent_message()`.
2. Auto-stop timer mechanism.
3. Startup reconciliation.

### Phase 5: SDK Client and File Operations (1 day)
1. Agent SDK client changes (`agent.py`, `agents.py`).
2. Sandbox file operation endpoints (`/files`, `/files/read`, `/files/write`).
3. `sandbox_exec()` and `sandbox_status()` methods.

### Phase 6: Testing and Documentation (1-2 days)
1. `test_sandbox_endpoints.py` -- endpoint integration tests.
2. `test_sandbox_lifecycle.py` -- state machine unit tests.
3. `test_sandbox_runner.py` -- runner script and in-sandbox execution tests.
4. Update `docs/api.md` with sandbox endpoints.

---

## 10. Risk Assessment

### High Risk

**`query()` in a headless sandbox environment.** The `claude-agent-sdk`'s `query()` function may rely on interactive terminal features (TTY detection, readline, credential prompts) that are unavailable in a headless `sandbox.process.exec()` call. Mitigation: run the runner with `--print` or API-only flags if available; set `TERM=dumb` and ensure `ANTHROPIC_API_KEY` is present so no interactive auth prompt is triggered. Verify early in Phase 3 with a minimal smoke test.

### Medium Risk

**Daytona SDK synchronous API in async server.** The Daytona Python SDK methods (`create`, `start`, `stop`, `get`) appear to be synchronous. Running them in an async FastAPI server requires `run_in_executor`. Mitigation: all Daytona calls go through `_daytona_with_timeout()` which uses `run_in_executor`.

**Sandbox startup latency.** Daytona claims sub-90ms start from snapshot. If actual latency is higher, auto-start on message send could introduce user-visible delays. Mitigation: the message endpoint returns immediately (non-blocking). The actual sandbox start happens in the agent worker before processing the message. The delay is absorbed by the queue processing time.

### Low Risk

**SQLite contention.** Adding sandbox writes to the existing single-connection SQLite setup increases contention. Current usage is low-volume (tens of agents, not thousands). Mitigation: use `check_same_thread=False` (already set) and keep transactions short.

**Reference count drift.** If the server crashes between attaching an agent and committing the ref count, the count could be wrong. Mitigation: `_reconcile_sandboxes()` on startup rebuilds counts from `agents.sandbox_id`.
