# Rivet Sandbox-Agent TypeScript SDK -- Complete Technical Reference

> **Purpose:** This document is a comprehensive specification of the Rivet sandbox-agent
> TypeScript SDK (`@sandbox-agent/sdk`). It is detailed enough for a developer to
> implement an equivalent Python SDK without consulting the TypeScript source.
>
> **SDK version:** `0.5.0-rc.2` (from `shared.ts`)
>
> **Source directory analysed:** `/tmp/sandbox-agent/sdks/typescript/src/`

---

## Table of Contents

1. [Architecture Overview](#1-architecture-overview)
2. [Type Definitions](#2-type-definitions)
3. [Error Classes](#3-error-classes)
4. [SandboxAgent Class](#4-sandboxagent-class)
5. [Session Class](#5-session-class)
6. [LiveAcpConnection Class](#6-liveacpconnection-class)
7. [ProcessTerminalSession Class](#7-processterminalsession-class)
8. [DesktopStreamSession Class](#8-desktopstreamsession-class)
9. [SandboxProvider Interface](#9-sandboxprovider-interface)
10. [Provider Implementations](#10-provider-implementations)
11. [Spawn Module](#11-spawn-module)
12. [Inspector Utility](#12-inspector-utility)
13. [ACP HTTP Client (Dependency)](#13-acp-http-client-dependency)
14. [SSE / Event Streaming](#14-sse--event-streaming)
15. [HTTP Endpoint Map](#15-http-endpoint-map)

---

## 1. Architecture Overview

```
 Caller code
     |
     v
 SandboxAgent  (main client -- REST + ACP)
     |
     +-- LiveAcpConnection (per-agent JSON-RPC over HTTP, via acp-http-client)
     |       |
     |       +-- AcpHttpClient  (StreamableHTTP transport: POST JSON-RPC + GET SSE)
     |
     +-- REST endpoints (file system, processes, desktop, agents, config)
     |
     +-- SessionPersistDriver (in-memory or custom)
     |
     +-- SandboxProvider? (optional: provisions / destroys remote sandbox)
```

**Key concepts:**

- `SandboxAgent` is the top-level client. It can be created via `connect()` (connect
  to an existing server) or `start()` (provision a new sandbox via a `SandboxProvider`,
  then connect).
- Each *agent* (e.g. `"claude"`, `"codex"`) gets one `LiveAcpConnection` -- a JSON-RPC
  bidirectional channel over the ACP (Agent Client Protocol) HTTP transport.
- *Sessions* are created inside an agent's ACP connection. The SDK manages a local
  persistence layer (`SessionPersistDriver`) that stores session records and events.
- Non-ACP operations (file system, process management, desktop, config) use plain REST
  calls through the same base URL.

---

## 2. Type Definitions

### 2.1 Core Scalar Types & Enums

```python
# Python equivalents

ProcessState = Literal["running", "exited"]
ProcessOwner = Literal["user", "desktop", "system"]
ProcessLogsStream = Literal["stdout", "stderr", "combined", "pty"]
FsEntryType = Literal["file", "directory"]
DesktopState = Literal["inactive", "install_required", "starting", "active", "stopping", "failed"]
DesktopMouseButton = Literal["left", "middle", "right"]
DesktopScreenshotFormat = str  # from OpenAPI, specific values depend on server
SessionEventSender = Literal["client", "agent"]
PermissionReply = Literal["once", "always", "reject"]
PermissionOptionKind = str  # "allow_once" | "allow_always" | "reject_once" | "reject_always"
```

### 2.2 Health

```python
@dataclass
class HealthResponse:
    status: str  # "ok" when healthy
```

### 2.3 Problem Details (RFC 7807)

```python
@dataclass
class ProblemDetails:
    type: str
    title: str
    status: int
    detail: Optional[str] = None
    instance: Optional[str] = None
    # May contain additional arbitrary keys
```

### 2.4 Session Types

```python
@dataclass
class SessionRecord:
    id: str
    agent: str
    agent_session_id: str
    last_connection_id: str
    created_at: int  # epoch ms
    destroyed_at: Optional[int] = None  # epoch ms
    sandbox_id: Optional[str] = None
    session_init: Optional[dict] = None  # NewSessionRequest minus _meta
    config_options: Optional[list] = None  # list of SessionConfigOption
    modes: Optional[SessionModeState] = None

@dataclass
class SessionEvent:
    id: str  # stable unique event id (UUID)
    event_index: int  # monotonic per session, for ordering
    session_id: str
    created_at: int  # epoch ms
    connection_id: str
    sender: SessionEventSender  # "client" | "agent"
    payload: dict  # AnyMessage (JSON-RPC envelope)

@dataclass
class ListPageRequest:
    cursor: Optional[str] = None
    limit: Optional[int] = None

@dataclass
class ListPage[T]:
    items: list[T]
    next_cursor: Optional[str] = None

@dataclass
class ListEventsRequest(ListPageRequest):
    session_id: str
```

### 2.5 Session Persistence Interface

```python
class SessionPersistDriver(Protocol):
    async def get_session(self, id: str) -> Optional[SessionRecord]: ...
    async def list_sessions(self, request: ListPageRequest = ...) -> ListPage[SessionRecord]: ...
    async def update_session(self, session: SessionRecord) -> None: ...
    async def list_events(self, request: ListEventsRequest) -> ListPage[SessionEvent]: ...
    async def insert_event(self, session_id: str, event: SessionEvent) -> None: ...
```

### 2.6 InMemorySessionPersistDriver

```python
class InMemorySessionPersistDriver:
    """Default persistence: stores sessions and events in memory with LRU eviction."""

    def __init__(
        self,
        max_sessions: int = 1024,
        max_events_per_session: int = 500,
    ): ...
```

**Behavior:**
- `list_sessions()` returns sorted by `(created_at ASC, id ASC)`.
- `update_session()` evicts the oldest session when over `max_sessions`.
- `list_events()` returns sorted by `(event_index ASC, id ASC)`.
- `insert_event()` trims oldest events per session when over `max_events_per_session`.
- All returned objects are deep clones (no shared references).
- Default list limit: 100 items per page.
- Pagination uses integer-offset cursors (stringified integers).

### 2.7 File System Types

```python
@dataclass
class FsEntry:
    entry_type: FsEntryType  # "file" | "directory"
    name: str
    path: str
    size: int
    modified: Optional[str] = None  # ISO timestamp string

@dataclass
class FsStat:
    entry_type: FsEntryType
    path: str
    size: int
    modified: Optional[str] = None

@dataclass
class FsWriteResponse:
    bytes_written: int
    path: str

@dataclass
class FsActionResponse:
    path: str

@dataclass
class FsMoveRequest:
    from_path: str  # JSON key: "from"
    to: str
    overwrite: Optional[bool] = None

@dataclass
class FsMoveResponse:
    from_path: str  # JSON key: "from"
    to: str

@dataclass
class FsUploadBatchResponse:
    paths: list[str]
    truncated: bool

# Query parameter types
@dataclass
class FsEntriesQuery:
    path: Optional[str] = None

@dataclass
class FsPathQuery:
    path: str

@dataclass
class FsDeleteQuery:
    path: str
    recursive: Optional[bool] = None

@dataclass
class FsUploadBatchQuery:
    path: Optional[str] = None
```

### 2.8 Process Types

```python
@dataclass
class ProcessInfo:
    id: str
    command: str
    args: list[str]
    status: ProcessState  # "running" | "exited"
    owner: ProcessOwner   # "user" | "desktop" | "system"
    interactive: bool
    tty: bool
    created_at_ms: int
    cwd: Optional[str] = None
    pid: Optional[int] = None
    exit_code: Optional[int] = None
    exited_at_ms: Optional[int] = None

@dataclass
class ProcessCreateRequest:
    command: str
    args: Optional[list[str]] = None
    cwd: Optional[str] = None
    env: Optional[dict[str, str]] = None
    interactive: Optional[bool] = None
    tty: Optional[bool] = None

@dataclass
class ProcessRunRequest:
    command: str
    args: Optional[list[str]] = None
    cwd: Optional[str] = None
    env: Optional[dict[str, str]] = None
    timeout_ms: Optional[int] = None
    max_output_bytes: Optional[int] = None

@dataclass
class ProcessRunResponse:
    exit_code: Optional[int]
    stdout: str
    stderr: str
    stdout_truncated: bool
    stderr_truncated: bool
    timed_out: bool
    duration_ms: int

@dataclass
class ProcessConfig:
    default_run_timeout_ms: int
    max_concurrent_processes: int
    max_input_bytes_per_request: int
    max_log_bytes_per_process: int
    max_output_bytes: int
    max_run_timeout_ms: int

@dataclass
class ProcessInputRequest:
    data: str
    encoding: Optional[str] = None

@dataclass
class ProcessInputResponse:
    bytes_written: int

@dataclass
class ProcessLogEntry:
    data: str
    encoding: str
    sequence: int
    stream: ProcessLogsStream
    timestamp_ms: int

@dataclass
class ProcessLogsQuery:
    follow: Optional[bool] = None
    since: Optional[int] = None  # epoch ms
    stream: Optional[ProcessLogsStream] = None
    tail: Optional[int] = None

@dataclass
class ProcessLogsResponse:
    entries: list[ProcessLogEntry]
    process_id: str
    stream: ProcessLogsStream

@dataclass
class ProcessListQuery:
    owner: Optional[ProcessOwner] = None

@dataclass
class ProcessListResponse:
    processes: list[ProcessInfo]

@dataclass
class ProcessSignalQuery:
    wait_ms: Optional[int] = None

@dataclass
class ProcessTerminalResizeRequest:
    cols: int
    rows: int

@dataclass
class ProcessTerminalResizeResponse:
    cols: int
    rows: int
```

### 2.9 Process Terminal WebSocket Frame Types

```python
# Client -> Server frames (JSON over text WebSocket)
ProcessTerminalClientFrame = Union[
    TerminalInputFrame,
    TerminalResizeFrame,
    TerminalCloseFrame,
]

@dataclass
class TerminalInputFrame:
    type: Literal["input"] = "input"
    data: str = ""          # UTF-8 string or base64 if encoding="base64"
    encoding: Optional[str] = None

@dataclass
class TerminalResizeFrame:
    type: Literal["resize"] = "resize"
    cols: int = 80
    rows: int = 24

@dataclass
class TerminalCloseFrame:
    type: Literal["close"] = "close"

# Server -> Client frames
# Text messages = JSON control frames; Binary messages = raw terminal output bytes

@dataclass
class TerminalReadyStatus:
    type: Literal["ready"] = "ready"
    process_id: str = ""

@dataclass
class TerminalExitStatus:
    type: Literal["exit"] = "exit"
    exit_code: Optional[int] = None

@dataclass
class TerminalErrorStatus:
    type: Literal["error"] = "error"
    message: str = ""

TerminalStatusMessage = Union[TerminalReadyStatus, TerminalExitStatus, TerminalErrorStatus]
```

### 2.10 Agent Types

```python
@dataclass
class AgentCapabilities:
    command_execution: bool
    error_events: bool
    file_attachments: bool
    file_changes: bool
    images: bool
    item_started: bool
    mcp_tools: bool
    permissions: bool
    plan_mode: bool
    questions: bool
    reasoning: bool
    session_lifecycle: bool
    shared_process: bool
    status: bool
    streaming_deltas: bool
    text_messages: bool
    tool_calls: bool
    tool_results: bool

@dataclass
class AgentInfo:
    id: str
    installed: bool
    credentials_available: bool
    capabilities: AgentCapabilities
    config_error: Optional[str] = None
    config_options: Optional[list] = None  # list of SessionConfigOption dicts
    path: Optional[str] = None
    server_status: Optional[ServerStatusInfo] = None
    version: Optional[str] = None

@dataclass
class AgentInstallRequest:
    agent_process_version: Optional[str] = None
    agent_version: Optional[str] = None
    reinstall: Optional[bool] = None

@dataclass
class AgentInstallResponse:
    already_installed: bool
    artifacts: list[dict]  # AgentInstallArtifact

@dataclass
class AgentListResponse:
    agents: list[AgentInfo]
```

### 2.11 ACP Types

```python
@dataclass
class AcpEnvelope:
    jsonrpc: str
    method: Optional[str] = None
    id: Optional[Any] = None
    params: Optional[Any] = None
    result: Optional[Any] = None
    error: Optional[Any] = None

@dataclass
class AcpServerInfo:
    agent: str
    server_id: str
    created_at_ms: int

@dataclass
class AcpServerListResponse:
    servers: list[AcpServerInfo]
```

### 2.12 MCP Config Types

```python
@dataclass
class McpConfigQuery:
    directory: str
    mcp_name: str  # JSON key: "mcpName"

# McpServerConfig is a union type
McpServerConfig = Union[McpLocalConfig, McpRemoteConfig]

@dataclass
class McpLocalConfig:
    type: Literal["local"] = "local"
    command: str = ""
    args: Optional[list[str]] = None
    cwd: Optional[str] = None
    enabled: Optional[bool] = None
    env: Optional[dict[str, str]] = None
    timeout_ms: Optional[int] = None

@dataclass
class McpRemoteConfig:
    type: Literal["remote"] = "remote"
    url: str = ""
    enabled: Optional[bool] = None
    bearer_token_env_var: Optional[str] = None
    headers: Optional[dict[str, str]] = None
    env_headers: Optional[dict[str, str]] = None
    timeout_ms: Optional[int] = None
    transport: Optional[str] = None
    oauth: Optional[dict] = None
```

### 2.13 Skills Config Types

```python
@dataclass
class SkillsConfigQuery:
    directory: str
    skill_name: str  # JSON key: "skillName"

@dataclass
class SkillSource:
    source: str
    type: str
    ref: Optional[str] = None
    skills: Optional[list[str]] = None
    subpath: Optional[str] = None

@dataclass
class SkillsConfig:
    sources: list[SkillSource]
```

### 2.14 Permission Types

```python
@dataclass
class SessionPermissionRequestOption:
    option_id: str
    name: str
    kind: str  # "allow_once" | "allow_always" | "reject_once" | "reject_always"

@dataclass
class SessionPermissionRequest:
    id: str           # SDK-generated unique ID for this permission prompt
    created_at: int   # epoch ms
    session_id: str   # local session ID
    agent_session_id: str
    available_replies: list[PermissionReply]  # ["once", "always", "reject"]
    options: list[SessionPermissionRequestOption]
    tool_call: dict   # raw RequestPermissionRequest.toolCall
    raw_request: dict  # full RequestPermissionRequest
```

### 2.15 Session Create/Resume Types

```python
@dataclass
class SessionCreateRequest:
    agent: str
    id: Optional[str] = None  # caller-supplied; auto-generated UUID if absent
    cwd: Optional[str] = None  # shorthand for session_init.cwd
    session_init: Optional[dict] = None  # NewSessionRequest minus _meta
    model: Optional[str] = None
    mode: Optional[str] = None
    thought_level: Optional[str] = None

@dataclass
class SessionResumeOrCreateRequest:
    id: str
    agent: str
    cwd: Optional[str] = None
    session_init: Optional[dict] = None
    model: Optional[str] = None
    mode: Optional[str] = None
    thought_level: Optional[str] = None

@dataclass
class SessionSendOptions:
    notification: Optional[bool] = None  # if True, sends as notification (no response)
```

### 2.16 Desktop Types (abbreviated)

```python
# Desktop status
@dataclass
class DesktopStatusResponse:
    error: Optional[DesktopErrorInfo]
    processes: list[DesktopProcessInfo]
    resolution: DesktopResolution
    state: DesktopState

@dataclass
class DesktopResolution:
    height: int
    width: int

@dataclass
class DesktopErrorInfo:
    code: str
    message: str

@dataclass
class DesktopProcessInfo:
    name: str
    running: bool
    log_path: Optional[str] = None
    pid: Optional[int] = None

# Window types
@dataclass
class DesktopWindowInfo:
    # Fields vary by server -- typically includes window_id, title, geometry, etc.
    pass

# Recording types
@dataclass
class DesktopRecordingInfo:
    # Server-defined fields: id, status, path, duration, etc.
    pass
```

---

## 3. Error Classes

### 3.1 SandboxAgentError

```python
class SandboxAgentError(Exception):
    """Raised when an HTTP request to the sandbox-agent server fails (non-2xx status)."""

    def __init__(self, status: int, problem: Optional[ProblemDetails], response: Response):
        self.status = status
        self.problem = problem
        self.response = response
        message = problem.title if problem else f"Request failed with status {status}"
        super().__init__(message)
```

### 3.2 SandboxDestroyedError

```python
class SandboxDestroyedError(Exception):
    """Raised when reconnecting to a sandbox that no longer exists."""

    def __init__(self, sandbox_id: str, provider: str, cause: Optional[Exception] = None):
        self.sandbox_id = sandbox_id
        self.provider = provider
        super().__init__(f"Sandbox '{provider}/{sandbox_id}' no longer exists and cannot be reconnected.")
```

### 3.3 UnsupportedSessionCategoryError

```python
class UnsupportedSessionCategoryError(Exception):
    """Raised when a session does not support a requested config category (e.g. "model", "mode")."""

    def __init__(self, session_id: str, category: str, available_categories: list[str]):
        self.session_id = session_id
        self.category = category
        self.available_categories = available_categories
```

### 3.4 UnsupportedSessionValueError

```python
class UnsupportedSessionValueError(Exception):
    """Raised when a session config option does not support the requested value."""

    def __init__(self, session_id: str, category: str, config_id: str,
                 requested_value: str, allowed_values: list[str]):
        self.session_id = session_id
        self.category = category
        self.config_id = config_id
        self.requested_value = requested_value
        self.allowed_values = allowed_values
```

### 3.5 UnsupportedSessionConfigOptionError

```python
class UnsupportedSessionConfigOptionError(Exception):
    """Raised when a session does not expose a given configId."""

    def __init__(self, session_id: str, config_id: str, available_config_ids: list[str]):
        self.session_id = session_id
        self.config_id = config_id
        self.available_config_ids = available_config_ids
```

### 3.6 UnsupportedPermissionReplyError

```python
class UnsupportedPermissionReplyError(Exception):
    """Raised when a permission request does not support the given reply type."""

    def __init__(self, permission_id: str, requested_reply: PermissionReply,
                 available_replies: list[PermissionReply]):
        self.permission_id = permission_id
        self.requested_reply = requested_reply
        self.available_replies = available_replies
```

### 3.7 AcpRpcError (from acp-http-client)

```python
class AcpRpcError(Exception):
    """Raised when a JSON-RPC method returns an error."""

    def __init__(self, code: int, message: str, data: Optional[Any] = None):
        self.code = code
        self.data = data
        # Well-known codes:
        # -32700: Parse error
        # -32600: Invalid request
        # -32601: Method not supported by agent
        # -32602: Invalid parameters
        # -32603: Internal agent error
        # -32000: Authentication required
        # -32002: Resource not found
```

---

## 4. SandboxAgent Class

The main client. Manages HTTP requests, ACP connections, sessions, and event persistence.

### 4.1 Constructor

```python
class SandboxAgent:
    def __init__(self, options: SandboxAgentConnectOptions):
        """
        Direct constructor. Prefer using connect() or start() factory methods.

        options can be one of two forms:
        1. { base_url: str, fetch?: Callable, ... }
        2. { fetch: Callable, base_url?: str, ... }  (fetch required if no base_url)

        Common fields:
          - headers: Optional[dict]  -- extra HTTP headers for all requests
          - persist: Optional[SessionPersistDriver]  -- defaults to InMemorySessionPersistDriver
          - replay_max_events: int  -- max events to replay on session resume (default 50)
          - replay_max_chars: int  -- max chars for replay text (default 12000)
          - signal: Optional[AbortSignal]  -- for cancellation
          - token: Optional[str]  -- Bearer token for auth
          - skip_health_check: bool  -- skip initial health polling (default False)
          - wait_for_health: Union[bool, HealthWaitOptions]  -- deprecated, use skip_health_check

        Immediately starts background health polling (unless skip_health_check=True).
        All subsequent requests block until health check succeeds.
        """
```

### 4.2 Static Factory Methods

#### `connect(options) -> SandboxAgent`

```python
@staticmethod
async def connect(options: SandboxAgentConnectOptions) -> SandboxAgent:
    """
    Create a client connected to an existing sandbox-agent server.
    Simply calls the constructor (exists for API symmetry with start()).
    """
```

#### `start(options) -> SandboxAgent`

```python
@staticmethod
async def start(options: SandboxAgentStartOptions) -> SandboxAgent:
    """
    Provision a new sandbox via a SandboxProvider, then connect.

    options:
      - sandbox: SandboxProvider  (required)
      - sandbox_id: Optional[str]  -- reconnect to existing sandbox (format: "{provider}/{rawId}")
      - skip_health_check: bool
      - fetch: Optional[Callable]
      - headers: Optional[dict]
      - persist: Optional[SessionPersistDriver]
      - replay_max_events: int
      - replay_max_chars: int
      - signal: Optional[AbortSignal]
      - token: Optional[str]

    Flow:
    1. If sandbox_id is provided, parse it as "{provider}/{rawId}" and validate provider name matches.
       Call provider.reconnect() and provider.ensure_server().
    2. Otherwise, call provider.create() to get a raw sandbox ID.
    3. Resolve fetch implementation via provider.get_fetch() or provider.get_url().
    4. Resolve inspector URL via provider.get_inspector_url() if available.
    5. Construct a SandboxAgent with the resolved fetch/baseUrl.
    6. On failure after create(), best-effort provider.destroy().

    The returned SandboxAgent has sandbox_id set (e.g. "e2b/abc123").
    """
```

### 4.3 Properties

```python
@property
def sandbox_id(self) -> Optional[str]:
    """Prefixed sandbox ID: '{provider}/{rawId}', or None if not provider-backed."""

@property
def sandbox(self) -> Optional[SandboxProvider]:
    """The SandboxProvider instance, or None."""

@property
def inspector_url(self) -> str:
    """URL to the sandbox-agent Inspector UI. Falls back to {base_url}/ui/."""
```

### 4.4 Lifecycle Methods

#### `dispose() -> None`

```python
async def dispose(self) -> None:
    """
    Gracefully shut down the client. Does NOT destroy the sandbox.
    - Cancels all pending permission requests (resolves them with 'cancelled').
    - Closes all LiveAcpConnections.
    - Aborts health-wait.
    """
```

#### `destroy_sandbox() -> None`

```python
async def destroy_sandbox(self) -> None:
    """
    Permanently destroy the provisioned sandbox, then dispose the client.
    Raises if not provider-backed.
    Calls provider.destroy(raw_sandbox_id), then dispose().
    """
```

#### `pause_sandbox() -> None`

```python
async def pause_sandbox(self) -> None:
    """
    Pause the sandbox (if provider supports pause), otherwise destroy it.
    Calls provider.pause() or falls back to provider.destroy().
    Then calls dispose().
    """
```

#### `kill_sandbox() -> None`

```python
async def kill_sandbox(self) -> None:
    """
    Force-kill the sandbox (if provider supports kill), otherwise destroy it.
    Calls provider.kill() or falls back to provider.destroy().
    Then calls dispose().
    """
```

### 4.5 Session Management

#### `create_session(request) -> Session`

```python
async def create_session(self, request: SessionCreateRequest) -> Session:
    """
    Create a new agent session.

    Flow:
    1. Validate agent is non-empty.
    2. Generate local session ID (UUID) unless request.id is provided.
    3. Get or create LiveAcpConnection for the agent.
    4. Build session_init from request.session_init or {cwd, mcpServers:[]}.
       cwd defaults to: request.cwd -> provider.default_cwd -> os.getcwd()
    5. Call live.create_remote_session() -> sends session/new JSON-RPC.
    6. Persist SessionRecord with agent_session_id from response.
    7. If request.mode set, call set_session_mode().
    8. If request.model set, call set_session_model().
    9. If request.thought_level set, call set_session_thought_level().
    10. On error during mode/model/thought setup, best-effort destroy_session().
    """
```

#### `resume_session(id) -> Session`

```python
async def resume_session(self, id: str) -> Session:
    """
    Resume an existing session. Used after reconnects or connection drops.

    Flow:
    1. Load session record from persistence. Error if not found.
    2. Get LiveAcpConnection for the session's agent.
    3. If already bound to current connection, return immediately.
    4. Collect last N events (replay_max_events) from persistence.
    5. Build replay text (JSON-serialized events, truncated to replay_max_chars).
    6. Create a new remote session (session/new) with same session_init.
    7. Queue the replay text to be prepended to the next prompt.
    8. Update persistence with new agent_session_id and connection_id.
    """
```

**Replay text format:**
```
Previous session history is replayed below as JSON-RPC envelopes. Use it as context before responding to the latest user prompt.
{"createdAt":...,"sender":"client","payload":{...}}
{"createdAt":...,"sender":"agent","payload":{...}}
[history truncated]
```

#### `resume_or_create_session(request) -> Session`

```python
async def resume_or_create_session(self, request: SessionResumeOrCreateRequest) -> Session:
    """
    Resume if session exists in persistence, otherwise create.
    The request.id is used to look up the existing session.
    Also applies mode/model/thought_level after resume if specified.
    """
```

#### `destroy_session(id) -> Session`

```python
async def destroy_session(self, id: str) -> Session:
    """
    Destroy a session.
    1. Cancel all pending permission requests for this session.
    2. Send session/cancel to the agent (best-effort, ignores errors).
    3. Mark session as destroyed (set destroyed_at timestamp).
    4. Return updated Session handle.

    NOTE: Direct session/cancel calls are forbidden (throws error).
    Only destroy_session() may cancel sessions.
    """
```

#### `list_sessions(request) -> ListPage[Session]`

```python
async def list_sessions(self, request: ListPageRequest = {}) -> ListPage[Session]:
    """List all sessions from persistence. Supports cursor pagination."""
```

#### `get_session(id) -> Optional[Session]`

```python
async def get_session(self, id: str) -> Optional[Session]:
    """Get a session by ID from persistence. Returns None if not found."""
```

#### `get_events(request) -> ListPage[SessionEvent]`

```python
async def get_events(self, request: ListEventsRequest) -> ListPage[SessionEvent]:
    """Get events for a session from persistence. Supports cursor pagination."""
```

### 4.6 Session Configuration

#### `set_session_mode(session_id, mode_id) -> {session, response}`

```python
async def set_session_mode(self, session_id: str, mode_id: str) -> dict:
    """
    Set the session's mode (e.g. "plan", "code").

    Returns: {"session": Session, "response": SetSessionModeResponse | None}

    Flow:
    1. Validate mode_id is non-empty.
    2. Load session record; extract known mode IDs from record.modes.
    3. If known modes exist and mode_id is not in them, raise UnsupportedSessionValueError.
    4. Try sending session/set_mode JSON-RPC.
    5. If agent returns -32601 (method not found), fall back to
       set_session_category_value("mode", mode_id) which uses config options.
    """
```

#### `set_session_config_option(session_id, config_id, value) -> {session, response}`

```python
async def set_session_config_option(self, session_id: str, config_id: str, value: str) -> dict:
    """
    Set a session config option by its ID.

    Returns: {"session": Session, "response": SetSessionConfigOptionResponse}

    Flow:
    1. Load session config options.
    2. Find option by config_id. Raise UnsupportedSessionConfigOptionError if not found.
    3. Extract allowed values. Raise UnsupportedSessionValueError if not in allowed set.
    4. Send session/set_config_option JSON-RPC.
    5. Persist updated configOptions from response (or optimistically update local cache).
    """
```

#### `set_session_model(session_id, model) -> {session, response}`

```python
async def set_session_model(self, session_id: str, model: str) -> dict:
    """Shortcut: finds the config option with category='model' and sets its value."""
```

#### `set_session_thought_level(session_id, thought_level) -> {session, response}`

```python
async def set_session_thought_level(self, session_id: str, thought_level: str) -> dict:
    """Shortcut: finds the config option with category='thought_level' and sets its value."""
```

#### `get_session_config_options(session_id) -> list[SessionConfigOption]`

```python
async def get_session_config_options(self, session_id: str) -> list:
    """
    Get available config options for a session.
    If not cached in persistence, fetches from agent info (get_agent with config=True)
    and caches the result.
    """
```

#### `get_session_modes(session_id) -> Optional[SessionModeState]`

```python
async def get_session_modes(self, session_id: str) -> Optional[dict]:
    """
    Get available modes for a session.
    Returns: {"current_mode_id": str, "available_modes": [{"id": str, "name": str, "description": str | None}]}
    Falls back to deriving modes from config options if not directly available.
    """
```

### 4.7 Messaging

#### `raw_send_session_method(session_id, method, params, options) -> {session, response}`

```python
async def raw_send_session_method(
    self,
    session_id: str,
    method: str,
    params: dict = {},
    options: SessionSendOptions = {},
) -> dict:
    """
    Send an arbitrary JSON-RPC method to a session's agent.

    Returns: {"session": Session, "response": Any}

    Blocks sending "session/cancel" (raises error; use destroy_session() instead).

    If the session's connection is stale, automatically calls resume_session() first.

    Special handling:
    - "session/prompt": injects replay text if queued, supports notification mode.
    - "session/cancel": sends cancel notification.
    - "session/set_mode": delegates to acp.set_session_mode().
    - "session/set_config_option": delegates to acp.set_session_config_option().
    - Other methods: uses acp.ext_method() or acp.ext_notification().
    """
```

### 4.8 Event Listeners

#### `on_session_event(session_id, listener) -> unsubscribe`

```python
def on_session_event(self, session_id: str, listener: Callable[[SessionEvent], None]) -> Callable[[], None]:
    """
    Register a listener for all events on a session (both client and agent messages).
    Events are emitted after being persisted.
    Returns an unsubscribe function.

    Events are the persisted envelopes (JSON-RPC messages) flowing through the
    ACP connection -- every inbound and outbound message for this session.
    """
```

#### `on_permission_request(session_id, listener) -> unsubscribe`

```python
def on_permission_request(
    self,
    session_id: str,
    listener: Callable[[SessionPermissionRequest], None],
) -> Callable[[], None]:
    """
    Register a listener for permission requests from the agent.
    The listener receives a SessionPermissionRequest object with a unique ID.
    The caller must respond using respond_permission() or raw_respond_permission().
    Returns an unsubscribe function.

    If no listeners are registered for a session, permissions are auto-cancelled.
    """
```

#### `respond_permission(permission_id, reply) -> None`

```python
async def respond_permission(self, permission_id: str, reply: PermissionReply) -> None:
    """
    Respond to a pending permission request.

    reply: "once" | "always" | "reject"

    Mapping:
    - "once"   -> selects option with kind="allow_once"
    - "always" -> prefers "allow_always", falls back to "allow_once"
    - "reject" -> prefers "reject_once", falls back to "reject_always"

    Raises UnsupportedPermissionReplyError if no matching option exists.
    """
```

#### `raw_respond_permission(permission_id, response) -> None`

```python
async def raw_respond_permission(self, permission_id: str, response: dict) -> None:
    """
    Respond to a permission request with a raw RequestPermissionResponse.
    Format: {"outcome": {"outcome": "selected", "optionId": "..."}}
    or:     {"outcome": {"outcome": "cancelled"}}
    """
```

### 4.9 Health

#### `get_health() -> HealthResponse`

```python
async def get_health(self) -> HealthResponse:
    """GET /v1/health -- returns {"status": "ok"} when healthy."""
```

### 4.10 Agent Management

#### `list_agents(options?) -> AgentListResponse`

```python
async def list_agents(self, options: AgentQueryOptions = None) -> AgentListResponse:
    """
    GET /v1/agents?config={bool}&no_cache={bool}

    Returns: {"agents": [AgentInfo, ...]}
    """
```
**Endpoint:** `GET /v1/agents`  
**Query params:** `config` (bool), `no_cache` (bool)

#### `get_agent(agent, options?) -> AgentInfo`

```python
async def get_agent(self, agent: str, options: AgentQueryOptions = None) -> AgentInfo:
    """
    GET /v1/agents/{agent}?config={bool}&no_cache={bool}

    Falls back to list_agents + filter if 404 (handles agent name mismatches).
    """
```
**Endpoint:** `GET /v1/agents/{agent}`

#### `install_agent(agent, request?) -> AgentInstallResponse`

```python
async def install_agent(self, agent: str, request: AgentInstallRequest = {}) -> AgentInstallResponse:
    """POST /v1/agents/{agent}/install"""
```
**Endpoint:** `POST /v1/agents/{agent}/install`

### 4.11 ACP Servers

#### `list_acp_servers() -> AcpServerListResponse`

```python
async def list_acp_servers(self) -> AcpServerListResponse:
    """GET /v1/acp -- list active ACP server instances."""
```
**Endpoint:** `GET /v1/acp/servers`  (actually `GET /v1/acp` based on the OpenAPI)

### 4.12 File System Operations

All file system endpoints use the `/v1/fs` prefix.

#### `list_fs_entries(query?) -> list[FsEntry]`

```python
async def list_fs_entries(self, query: FsEntriesQuery = {}) -> list[FsEntry]:
    """GET /v1/fs/entries?path={path}"""
```
**Endpoint:** `GET /v1/fs/entries`

#### `read_fs_file(query) -> bytes`

```python
async def read_fs_file(self, query: FsPathQuery) -> bytes:
    """
    GET /v1/fs/file?path={path}
    Accept: application/octet-stream
    Returns raw file bytes.
    """
```
**Endpoint:** `GET /v1/fs/file`

#### `write_fs_file(query, body) -> FsWriteResponse`

```python
async def write_fs_file(self, query: FsPathQuery, body: bytes) -> FsWriteResponse:
    """
    PUT /v1/fs/file?path={path}
    Content-Type: application/octet-stream
    Body: raw file bytes
    Returns: {"bytes_written": int, "path": str}
    """
```
**Endpoint:** `PUT /v1/fs/file`

#### `delete_fs_entry(query) -> FsActionResponse`

```python
async def delete_fs_entry(self, query: FsDeleteQuery) -> FsActionResponse:
    """DELETE /v1/fs/entry?path={path}&recursive={bool}"""
```
**Endpoint:** `DELETE /v1/fs/entry`

#### `mkdir_fs(query) -> FsActionResponse`

```python
async def mkdir_fs(self, query: FsPathQuery) -> FsActionResponse:
    """POST /v1/fs/mkdir?path={path}"""
```
**Endpoint:** `POST /v1/fs/mkdir`

#### `move_fs(request) -> FsMoveResponse`

```python
async def move_fs(self, request: FsMoveRequest) -> FsMoveResponse:
    """POST /v1/fs/move  body: {"from": str, "to": str, "overwrite": bool?}"""
```
**Endpoint:** `POST /v1/fs/move`

#### `stat_fs(query) -> FsStat`

```python
async def stat_fs(self, query: FsPathQuery) -> FsStat:
    """GET /v1/fs/stat?path={path}"""
```
**Endpoint:** `GET /v1/fs/stat`

#### `upload_fs_batch(body, query?) -> FsUploadBatchResponse`

```python
async def upload_fs_batch(self, body: bytes, query: FsUploadBatchQuery = None) -> FsUploadBatchResponse:
    """
    POST /v1/fs/upload-batch?path={path}
    Content-Type: application/x-tar
    Body: tar archive bytes
    Returns: {"paths": [str, ...], "truncated": bool}
    """
```
**Endpoint:** `POST /v1/fs/upload-batch`

### 4.13 MCP Configuration

#### `get_mcp_config(query) -> McpServerConfig`

```python
async def get_mcp_config(self, query: McpConfigQuery) -> McpServerConfig:
    """GET /v1/config/mcp?directory={dir}&mcpName={name}"""
```
**Endpoint:** `GET /v1/config/mcp`

#### `set_mcp_config(query, config) -> None`

```python
async def set_mcp_config(self, query: McpConfigQuery, config: McpServerConfig) -> None:
    """PUT /v1/config/mcp?directory={dir}&mcpName={name}  body: McpServerConfig"""
```
**Endpoint:** `PUT /v1/config/mcp`

#### `delete_mcp_config(query) -> None`

```python
async def delete_mcp_config(self, query: McpConfigQuery) -> None:
    """DELETE /v1/config/mcp?directory={dir}&mcpName={name}"""
```
**Endpoint:** `DELETE /v1/config/mcp`

### 4.14 Skills Configuration

#### `get_skills_config(query) -> SkillsConfig`

```python
async def get_skills_config(self, query: SkillsConfigQuery) -> SkillsConfig:
    """GET /v1/config/skills?directory={dir}&skillName={name}"""
```

#### `set_skills_config(query, config) -> None`

```python
async def set_skills_config(self, query: SkillsConfigQuery, config: SkillsConfig) -> None:
    """PUT /v1/config/skills?directory={dir}&skillName={name}"""
```

#### `delete_skills_config(query) -> None`

```python
async def delete_skills_config(self, query: SkillsConfigQuery) -> None:
    """DELETE /v1/config/skills?directory={dir}&skillName={name}"""
```

### 4.15 Process Management

#### `get_process_config() -> ProcessConfig`

```python
async def get_process_config(self) -> ProcessConfig:
    """GET /v1/processes/config"""
```
**Endpoint:** `GET /v1/processes/config`

#### `set_process_config(config) -> ProcessConfig`

```python
async def set_process_config(self, config: ProcessConfig) -> ProcessConfig:
    """POST /v1/processes/config"""
```
**Endpoint:** `POST /v1/processes/config`

#### `create_process(request) -> ProcessInfo`

```python
async def create_process(self, request: ProcessCreateRequest) -> ProcessInfo:
    """
    POST /v1/processes
    Creates a long-running process. Use get_process_logs() or
    follow_process_logs() to read output.
    """
```
**Endpoint:** `POST /v1/processes`

#### `run_process(request) -> ProcessRunResponse`

```python
async def run_process(self, request: ProcessRunRequest) -> ProcessRunResponse:
    """
    POST /v1/processes/run
    Run a command synchronously and return stdout/stderr/exitCode.
    Blocks until process completes or times out.
    """
```
**Endpoint:** `POST /v1/processes/run`

#### `list_processes(query?) -> ProcessListResponse`

```python
async def list_processes(self, query: ProcessListQuery = None) -> ProcessListResponse:
    """GET /v1/processes?owner={owner}"""
```
**Endpoint:** `GET /v1/processes`

#### `get_process(id) -> ProcessInfo`

```python
async def get_process(self, id: str) -> ProcessInfo:
    """GET /v1/processes/{id}"""
```
**Endpoint:** `GET /v1/processes/{id}`

#### `stop_process(id, query?) -> ProcessInfo`

```python
async def stop_process(self, id: str, query: ProcessSignalQuery = None) -> ProcessInfo:
    """POST /v1/processes/{id}/stop?waitMs={ms}"""
```
**Endpoint:** `POST /v1/processes/{id}/stop`

#### `kill_process(id, query?) -> ProcessInfo`

```python
async def kill_process(self, id: str, query: ProcessSignalQuery = None) -> ProcessInfo:
    """POST /v1/processes/{id}/kill?waitMs={ms}"""
```
**Endpoint:** `POST /v1/processes/{id}/kill`

#### `delete_process(id) -> None`

```python
async def delete_process(self, id: str) -> None:
    """DELETE /v1/processes/{id}"""
```
**Endpoint:** `DELETE /v1/processes/{id}`

#### `get_process_logs(id, query?) -> ProcessLogsResponse`

```python
async def get_process_logs(self, id: str, query: ProcessLogFollowQuery = {}) -> ProcessLogsResponse:
    """
    GET /v1/processes/{id}/logs?since={ms}&stream={stream}&tail={n}
    Returns accumulated log entries.
    """
```
**Endpoint:** `GET /v1/processes/{id}/logs`

#### `follow_process_logs(id, listener, query?) -> ProcessLogSubscription`

```python
async def follow_process_logs(
    self,
    id: str,
    listener: Callable[[ProcessLogEntry], None],
    query: ProcessLogFollowQuery = {},
) -> ProcessLogSubscription:
    """
    GET /v1/processes/{id}/logs?follow=true&...
    Accept: text/event-stream

    Opens an SSE stream of log entries. Each SSE event has:
      event: log
      data: <JSON ProcessLogEntry>

    Returns ProcessLogSubscription with:
      - close(): stops the stream
      - closed: awaitable that resolves when stream ends

    See Section 14 for SSE parsing details.
    """
```
**Endpoint:** `GET /v1/processes/{id}/logs` (with `follow=true`)

#### `send_process_input(id, request) -> ProcessInputResponse`

```python
async def send_process_input(self, id: str, request: ProcessInputRequest) -> ProcessInputResponse:
    """POST /v1/processes/{id}/input  body: {"data": str, "encoding"?: str}"""
```
**Endpoint:** `POST /v1/processes/{id}/input`

#### `resize_process_terminal(id, request) -> ProcessTerminalResizeResponse`

```python
async def resize_process_terminal(self, id: str, request: ProcessTerminalResizeRequest) -> ProcessTerminalResizeResponse:
    """POST /v1/processes/{id}/terminal/resize  body: {"cols": int, "rows": int}"""
```
**Endpoint:** `POST /v1/processes/{id}/terminal/resize`

### 4.16 Process Terminal WebSocket

#### `build_process_terminal_websocket_url(id, options?) -> str`

```python
def build_process_terminal_websocket_url(
    self,
    id: str,
    options: ProcessTerminalWebSocketUrlOptions = {},
) -> str:
    """
    Build the WebSocket URL for a process terminal.
    URL: ws(s)://{base_url}/v1/processes/{id}/terminal/ws?access_token={token}

    The access_token query param is set from options.access_token or self.token.
    HTTP URLs are converted to ws://, HTTPS to wss://.
    """
```
**Endpoint:** `WS /v1/processes/{id}/terminal/ws`

#### `connect_process_terminal_websocket(id, options?) -> WebSocket`

```python
def connect_process_terminal_websocket(
    self,
    id: str,
    options: ProcessTerminalConnectOptions = {},
) -> WebSocket:
    """Create and return a raw WebSocket connection."""
```

#### `connect_process_terminal(id, options?) -> ProcessTerminalSession`

```python
def connect_process_terminal(
    self,
    id: str,
    options: ProcessTerminalSessionOptions = {},
) -> ProcessTerminalSession:
    """Create a WebSocket and wrap it in a ProcessTerminalSession."""
```

### 4.17 Desktop Operations (Method Listing)

All desktop methods follow the same pattern: call `requestJson` or `requestRaw` with
the appropriate HTTP method and path.

| Method | HTTP | Endpoint | Request Body | Response |
|--------|------|----------|-------------|----------|
| `start_desktop(request?)` | POST | `/v1/desktop/start` | DesktopStartRequest | DesktopStatusResponse |
| `stop_desktop()` | POST | `/v1/desktop/stop` | -- | DesktopStatusResponse |
| `get_desktop_status()` | GET | `/v1/desktop/status` | -- | DesktopStatusResponse |
| `get_desktop_display_info()` | GET | `/v1/desktop/display/info` | -- | DesktopDisplayInfoResponse |
| `take_desktop_screenshot(query?)` | GET | `/v1/desktop/screenshot` | query params | bytes (image/*) |
| `take_desktop_region_screenshot(query)` | GET | `/v1/desktop/screenshot/region` | query params | bytes (image/*) |
| `get_desktop_mouse_position()` | GET | `/v1/desktop/mouse/position` | -- | DesktopMousePositionResponse |
| `move_desktop_mouse(request)` | POST | `/v1/desktop/mouse/move` | {x, y} | DesktopMousePositionResponse |
| `click_desktop(request)` | POST | `/v1/desktop/mouse/click` | {x, y, button?, clickCount?} | DesktopMousePositionResponse |
| `mouse_down_desktop(request)` | POST | `/v1/desktop/mouse/down` | {button?, x?, y?} | DesktopMousePositionResponse |
| `mouse_up_desktop(request)` | POST | `/v1/desktop/mouse/up` | {button?, x?, y?} | DesktopMousePositionResponse |
| `drag_desktop_mouse(request)` | POST | `/v1/desktop/mouse/drag` | {startX, startY, endX, endY, button?} | DesktopMousePositionResponse |
| `scroll_desktop(request)` | POST | `/v1/desktop/mouse/scroll` | {x, y, deltaX?, deltaY?} | DesktopMousePositionResponse |
| `type_desktop_text(request)` | POST | `/v1/desktop/keyboard/type` | {text, delayMs?} | DesktopActionResponse |
| `press_desktop_key(request)` | POST | `/v1/desktop/keyboard/press` | {key, modifiers?} | DesktopActionResponse |
| `key_down_desktop(request)` | POST | `/v1/desktop/keyboard/down` | {key} | DesktopActionResponse |
| `key_up_desktop(request)` | POST | `/v1/desktop/keyboard/up` | {key} | DesktopActionResponse |
| `list_desktop_windows()` | GET | `/v1/desktop/windows` | -- | DesktopWindowListResponse |
| `get_desktop_focused_window()` | GET | `/v1/desktop/windows/focused` | -- | DesktopWindowInfo |
| `focus_desktop_window(window_id)` | POST | `/v1/desktop/windows/{id}/focus` | -- | DesktopWindowInfo |
| `move_desktop_window(window_id, request)` | POST | `/v1/desktop/windows/{id}/move` | body | DesktopWindowInfo |
| `resize_desktop_window(window_id, request)` | POST | `/v1/desktop/windows/{id}/resize` | body | DesktopWindowInfo |
| `get_desktop_clipboard(query?)` | GET | `/v1/desktop/clipboard` | query | DesktopClipboardResponse |
| `set_desktop_clipboard(request)` | POST | `/v1/desktop/clipboard` | {text, selection?} | DesktopActionResponse |
| `launch_desktop_app(request)` | POST | `/v1/desktop/launch` | {app, args?, wait?} | DesktopLaunchResponse |
| `open_desktop_target(request)` | POST | `/v1/desktop/open` | {target} | DesktopOpenResponse |
| `get_desktop_stream_status()` | GET | `/v1/desktop/stream/status` | -- | DesktopStreamStatusResponse |
| `start_desktop_stream()` | POST | `/v1/desktop/stream/start` | -- | DesktopStreamStatusResponse |
| `stop_desktop_stream()` | POST | `/v1/desktop/stream/stop` | -- | DesktopStreamStatusResponse |
| `start_desktop_recording(request?)` | POST | `/v1/desktop/recording/start` | body | DesktopRecordingInfo |
| `stop_desktop_recording()` | POST | `/v1/desktop/recording/stop` | -- | DesktopRecordingInfo |
| `list_desktop_recordings()` | GET | `/v1/desktop/recordings` | -- | DesktopRecordingListResponse |
| `get_desktop_recording(id)` | GET | `/v1/desktop/recordings/{id}` | -- | DesktopRecordingInfo |
| `download_desktop_recording(id)` | GET | `/v1/desktop/recordings/{id}/download` | -- | bytes (video/mp4) |
| `delete_desktop_recording(id)` | DELETE | `/v1/desktop/recordings/{id}` | -- | void |

### 4.18 Desktop Streaming WebSocket

#### `build_desktop_stream_websocket_url(options?) -> str`

```python
def build_desktop_stream_websocket_url(self, options: dict = {}) -> str:
    """ws(s)://{base_url}/v1/desktop/stream/signaling?access_token={token}"""
```
**Endpoint:** `WS /v1/desktop/stream/signaling`

#### `connect_desktop_stream_websocket(options?) -> WebSocket`

```python
def connect_desktop_stream_websocket(self, options: DesktopStreamConnectOptions = {}) -> WebSocket:
    """Create raw WebSocket for desktop stream signaling."""
```

#### `connect_desktop_stream(options?) -> DesktopStreamSession`

```python
def connect_desktop_stream(self, options: DesktopStreamSessionOptions = {}) -> DesktopStreamSession:
    """Wraps the WebSocket in a DesktopStreamSession that manages WebRTC."""
```

### 4.19 Internal: Health Wait

**Constants:**
- `HEALTH_WAIT_MIN_DELAY_MS = 500`
- `HEALTH_WAIT_MAX_DELAY_MS = 15_000`
- `HEALTH_WAIT_LOG_AFTER_MS = 5_000` (warn to console after this)
- `HEALTH_WAIT_LOG_EVERY_MS = 10_000`
- `HEALTH_WAIT_ENSURE_SERVER_AFTER_FAILURES = 3` (call `provider.ensure_server()` after 3 consecutive failures)

**Algorithm:**
1. Poll `GET /v1/health` with exponential backoff (starting 500ms, doubling, capped at 15s).
2. After 5s of waiting, log a warning to console.
3. After 3 consecutive failures, call `provider.ensure_server()` if available.
4. On timeout (if configured), throw error.
5. All subsequent SDK calls block on `await_healthy()` until health succeeds.

### 4.20 Internal: HTTP Request Helpers

```python
async def _request_json(self, method: str, path: str, options: dict = {}) -> Any:
    """
    Make an HTTP request expecting JSON response.
    - Waits for health (unless skip_ready_wait).
    - Builds URL from base_url + path + query params.
    - Sets Authorization: Bearer {token} header.
    - Merges default headers.
    - If body is provided, serializes as JSON (Content-Type: application/json).
    - On non-2xx: reads ProblemDetails from response body, raises SandboxAgentError.
    - On 204: returns None.
    """

async def _request_raw(self, method: str, path: str, options: dict = {}) -> Response:
    """
    Make an HTTP request returning the raw Response object.
    Same auth/header logic as _request_json.
    Supports raw_body (BodyInit) with custom content_type.
    """
```

---

## 5. Session Class

A convenience wrapper around a `SessionRecord` with methods that delegate to `SandboxAgent`.

```python
class Session:
    def __init__(self, sandbox: SandboxAgent, record: SessionRecord): ...

    # Read-only properties
    @property
    def id(self) -> str: ...
    @property
    def agent(self) -> str: ...
    @property
    def agent_session_id(self) -> str: ...
    @property
    def last_connection_id(self) -> str: ...
    @property
    def created_at(self) -> int: ...
    @property
    def destroyed_at(self) -> Optional[int]: ...

    async def refresh(self) -> Session:
        """Re-fetch the session from SandboxAgent.get_session() and update local state."""

    async def raw_send(self, method: str, params: dict = {}, options: SessionSendOptions = {}) -> Any:
        """Delegate to sandbox.raw_send_session_method(). Returns the RPC response."""

    async def prompt(self, prompt: list) -> PromptResponse:
        """
        Send a prompt to the agent.
        prompt: list of content parts, e.g. [{"type": "text", "text": "..."}]
        Sends method "session/prompt" with {"prompt": prompt}.
        Returns PromptResponse from the agent.
        """

    async def set_mode(self, mode_id: str) -> Optional[SetSessionModeResponse]:
        """Delegate to sandbox.set_session_mode()."""

    async def set_config_option(self, config_id: str, value: str) -> SetSessionConfigOptionResponse:
        """Delegate to sandbox.set_session_config_option()."""

    async def set_model(self, model: str) -> SetSessionConfigOptionResponse:
        """Delegate to sandbox.set_session_model()."""

    async def set_thought_level(self, thought_level: str) -> SetSessionConfigOptionResponse:
        """Delegate to sandbox.set_session_thought_level()."""

    async def get_config_options(self) -> list[SessionConfigOption]:
        """Delegate to sandbox.get_session_config_options()."""

    async def get_modes(self) -> Optional[SessionModeState]:
        """Delegate to sandbox.get_session_modes()."""

    def on_event(self, listener: Callable[[SessionEvent], None]) -> Callable[[], None]:
        """Delegate to sandbox.on_session_event(). Returns unsubscribe function."""

    def on_permission_request(self, listener: Callable[[SessionPermissionRequest], None]) -> Callable[[], None]:
        """Delegate to sandbox.on_permission_request(). Returns unsubscribe function."""

    async def respond_permission(self, permission_id: str, reply: PermissionReply) -> None:
        """Delegate to sandbox.respond_permission()."""

    async def raw_respond_permission(self, permission_id: str, response: dict) -> None:
        """Delegate to sandbox.raw_respond_permission()."""

    def to_record(self) -> SessionRecord:
        """Return a copy of the underlying SessionRecord."""

    def apply(self, record: SessionRecord) -> None:
        """Update the internal record (used by SandboxAgent internals)."""
```

---

## 6. LiveAcpConnection Class

Manages a single ACP (Agent Client Protocol) connection to one agent. Created internally
by `SandboxAgent.get_live_connection()`.

```python
class LiveAcpConnection:
    connection_id: str  # UUID, unique per connection
    agent: str  # agent name (e.g. "claude")

    @staticmethod
    async def create(options: dict) -> LiveAcpConnection:
        """
        Factory method. Creates AcpHttpClient, initializes the connection,
        and auto-authenticates if needed.

        options:
          - base_url: str
          - token: Optional[str]
          - fetcher: Callable (fetch implementation)
          - headers: Optional[dict]
          - agent: str
          - server_id: str
          - on_observed_envelope: callback(connection, envelope, direction, local_session_id)
          - on_permission_request: async callback(connection, local_session_id, agent_session_id, request)

        ACP transport path: POST /v1/acp/{server_id}?agent={agent}
        SSE stream:         GET  /v1/acp/{server_id}

        Flow:
        1. Create AcpHttpClient with the given transport config.
        2. Call acp.initialize() with protocol_version and client_info.
        3. If auth methods returned, auto-authenticate (prefer env-based: "anthropic-api-key", etc.).
        """

    async def close(self) -> None:
        """Disconnect the ACP client."""

    def has_bound_session(self, local_session_id: str, agent_session_id: Optional[str] = None) -> bool:
        """Check if a local session ID is bound to this connection."""

    def bind_session(self, local_session_id: str, agent_session_id: str) -> None:
        """Map local <-> agent session IDs."""

    def queue_replay(self, local_session_id: str, replay_text: Optional[str]) -> None:
        """Queue replay text to prepend to the next prompt for this session."""

    async def create_remote_session(self, local_session_id: str, session_init: dict) -> NewSessionResponse:
        """
        Call acp.new_session(session_init).
        Binds the returned agent session ID to the local session ID.
        If the agent process exits during creation, raises descriptive error.
        """

    async def send_session_method(self, local_session_id: str, method: str, params: dict, options: SessionSendOptions) -> Any:
        """
        Send a JSON-RPC method to the agent on behalf of a session.

        Automatically maps local session ID -> agent session ID in params.

        Method routing:
        - "session/prompt": injects replay text if queued, calls acp.prompt() or acp.ext_notification()
        - "session/cancel": calls acp.cancel()
        - "session/set_mode": calls acp.set_session_mode()
        - "session/set_config_option": calls acp.set_session_config_option()
        - Other: calls acp.ext_method() or acp.ext_notification() (if notification=True)
        """
```

**Auto-authentication:**
The SDK attempts authentication for env-var-based methods only:
- `"codex-api-key"`
- `"openai-api-key"`
- `"anthropic-api-key"`

Interactive methods (e.g. `"claude-login"`) are skipped. Authentication is best-effort.

---

## 7. ProcessTerminalSession Class

Wraps a WebSocket connection to a process terminal (`/v1/processes/{id}/terminal/ws`).

```python
class ProcessTerminalSession:
    socket: WebSocket
    closed: Awaitable[None]  # resolves when WebSocket closes

    def __init__(self, socket: WebSocket):
        """
        Sets binaryType = 'arraybuffer' on the socket.
        Text messages are parsed as JSON control frames.
        Binary messages are raw terminal output bytes.
        """

    def on_ready(self, listener: Callable[[TerminalReadyStatus], None]) -> Callable[[], None]:
        """Fired when server sends {"type":"ready","processId":"..."}"""

    def on_data(self, listener: Callable[[bytes], None]) -> Callable[[], None]:
        """Fired for each binary message (raw terminal output bytes)."""

    def on_exit(self, listener: Callable[[TerminalExitStatus], None]) -> Callable[[], None]:
        """Fired when server sends {"type":"exit","exitCode":N}"""

    def on_error(self, listener: Callable[[Union[TerminalErrorStatus, Exception]], None]) -> Callable[[], None]:
        """Fired on {"type":"error","message":"..."} frames or WebSocket errors."""

    def on_close(self, listener: Callable[[], None]) -> Callable[[], None]:
        """Fired when the WebSocket connection closes."""

    def send_input(self, data: Union[str, bytes]) -> None:
        """
        Send input to the terminal.
        - str: sent as {"type":"input","data":"..."}
        - bytes: base64-encoded, sent as {"type":"input","data":"...","encoding":"base64"}
        """

    def resize(self, payload: TerminalResizePayload) -> None:
        """Send {"type":"resize","cols":N,"rows":N}"""

    def close(self) -> None:
        """
        Send {"type":"close"} frame, then close WebSocket.
        Safe to call multiple times.
        If socket is still CONNECTING, defers close until open.
        """
```

---

## 8. DesktopStreamSession Class

Wraps a WebSocket connection for desktop streaming via WebRTC (Neko protocol).

**Endpoint:** `WS /v1/desktop/stream/signaling`

```python
class DesktopStreamSession:
    socket: WebSocket
    closed: Awaitable[None]

    def __init__(self, socket: WebSocket, options: DesktopStreamConnectOptions = {}):
        """
        options:
          - RTCPeerConnection: class (default: globalThis.RTCPeerConnection)
          - rtc_config: RTCConfiguration dict
        """

    # Event listeners (all return unsubscribe functions)
    def on_ready(self, listener: Callable[[DesktopStreamReadyStatus], None]) -> Callable[[], None]:
        """Fired on system/init with screen_size. Status is cached for late listeners."""

    def on_track(self, listener: Callable[[MediaStream], None]) -> Callable[[], None]:
        """Fired when WebRTC track arrives. Cached for late listeners."""

    def on_connect(self, listener: Callable[[], None]) -> Callable[[], None]:
        """Fired when WebRTC connection is established."""

    def on_disconnect(self, listener: Callable[[], None]) -> Callable[[], None]:
        """Fired when socket closes."""

    def on_error(self, listener: Callable) -> Callable[[], None]: ...

    # Input methods (sent via WebRTC data channel using Neko binary protocol)
    def move_mouse(self, x: int, y: int) -> None: ...
    def mouse_down(self, button: Optional[str] = None, x: Optional[int] = None, y: Optional[int] = None) -> None: ...
    def mouse_up(self, button: Optional[str] = None, x: Optional[int] = None, y: Optional[int] = None) -> None: ...
    def scroll(self, x: int, y: int, delta_x: int = 0, delta_y: int = 0) -> None: ...
    def key_down(self, key: str) -> None: ...
    def key_up(self, key: str) -> None: ...

    def get_media_stream(self) -> Optional[MediaStream]: ...
    def close(self) -> None: ...
```

**Signaling protocol (Neko v3 over WebSocket):**
- `system/init` -> fires `on_ready`, sends `control/request` + `signal/request`
- `signal/provide` or `signal/offer` -> creates RTCPeerConnection, sets remote SDP, creates answer
- `signal/candidate` -> adds ICE candidate
- `signal/close` -> tears down peer connection
- `system/disconnect` -> emits error, closes

**Data channel binary protocol (Big Endian):**
- Header: 1 byte event + 2 bytes payload length
- Event codes: MOVE=0x01, SCROLL=0x02, KEY_DOWN=0x03, KEY_UP=0x04, BTN_DOWN=0x05, BTN_UP=0x06

---

## 9. SandboxProvider Interface

```python
class SandboxProvider(Protocol):
    name: str  # provider name, used as prefix in sandbox IDs

    async def create(self) -> str:
        """Provision a new sandbox. Returns provider-specific raw ID."""
        ...

    async def destroy(self, sandbox_id: str) -> None:
        """Permanently tear down a sandbox."""
        ...

    # All methods below are optional

    async def reconnect(self, sandbox_id: str) -> None:
        """Resume/reconnect to an existing sandbox before health checks."""
        ...

    async def pause(self, sandbox_id: str) -> None:
        """Gracefully stop/pause without deleting. Falls back to destroy() if absent."""
        ...

    async def kill(self, sandbox_id: str) -> None:
        """Force-delete. Falls back to destroy() if absent."""
        ...

    async def get_url(self, sandbox_id: str) -> str:
        """Return the sandbox-agent base URL."""
        ...

    async def get_fetch(self, sandbox_id: str) -> Callable:
        """Return a fetch implementation that routes to the sandbox."""
        ...

    async def get_inspector_url(self, sandbox_id: str, base_url: Optional[str] = None) -> str:
        """Return browser-ready Inspector URL."""
        ...

    async def ensure_server(self, sandbox_id: str) -> None:
        """
        Ensure the sandbox-agent server is running inside the sandbox.
        Called during health-wait after consecutive failures.
        Must be idempotent.
        """
        ...

    default_cwd: Optional[str]  # Default cwd for sessions (e.g. '/home/user')
```

---

## 10. Provider Implementations

### 10.1 Local Provider (`providers/local.ts`)

```python
def local(options: LocalProviderOptions = {}) -> SandboxProvider:
    """
    Spawns a local sandbox-agent binary as a subprocess.

    Options:
      host: str = "127.0.0.1"
      port: Optional[int]  -- auto-assigned if not specified
      token: Optional[str]  -- auto-generated if not specified
      binary_path: Optional[str]  -- auto-detected if not specified
      log: "inherit" | "pipe" | "silent" = "inherit"
      env: Optional[dict[str, str]]

    Provider methods:
      create() -> spawns subprocess, returns "{host}:{port}" as sandbox ID
      destroy(id) -> calls handle.dispose() (SIGTERM -> wait 5s -> SIGKILL)
      get_url(id) -> "http://{id}"
      get_fetch(id) -> custom fetch that injects Bearer token and routes to the local server
    """
```

### 10.2 Daytona Provider (`providers/daytona.ts`)

```python
def daytona(options: DaytonaProviderOptions = {}) -> SandboxProvider:
    """
    Uses Daytona SDK to provision cloud sandboxes.

    Options:
      create: dict or callable  -- overrides for Daytona.create()
      image: str = "rivetdev/sandbox-agent:0.5.0-rc.2-full"
      agent_port: int = 3000
      cwd: str = "/home/sandbox"
      preview_ttl_seconds: int = 14400 (4 hours)
      delete_timeout_seconds: Optional[int]

    Provider methods:
      create() -> creates Daytona sandbox, runs server start command, returns sandbox.id
      destroy(id) -> deletes sandbox
      get_url(id) -> gets signed preview URL for the agent port
      ensure_server(id) -> re-runs server start command
    """
```

**Server start command:** `nohup sandbox-agent server --no-token --host 0.0.0.0 --port {port} >/tmp/sandbox-agent.log 2>&1 &`

### 10.3 Docker Provider (`providers/docker.ts`)

```python
def docker(options: DockerProviderOptions = {}) -> SandboxProvider:
    """
    Uses dockerode to run sandbox-agent in a Docker container.

    Options:
      image: str = "rivetdev/sandbox-agent:0.5.0-rc.2-full"
      host: str = "127.0.0.1"
      agent_port: int = 3000
      env: list[str] or callable
      binds: list[str] or callable
      create_container_options: dict

    Container command: ["server", "--no-token", "--host", "0.0.0.0", "--port", "3000"]
    default_cwd = "/home/sandbox"
    """
```

### 10.4 E2B Provider (`providers/e2b.ts`)

```python
def e2b(options: E2BProviderOptions = {}) -> SandboxProvider:
    """
    Uses E2B Code Interpreter SDK.

    Options:
      create: dict or callable
      connect: dict or callable
      template: str or callable
      agent_port: int = 3000
      timeout_ms: int = 3_600_000 (1 hour)
      auto_pause: bool = True

    default_cwd = "/home/user"

    create() flow:
    1. Create E2B sandbox with template
    2. Install sandbox-agent via curl script
    3. Install default agents ("claude", "codex")
    4. Start server in background

    Supports: reconnect, pause, kill, ensure_server
    """
```

### 10.5 Modal Provider (`providers/modal.ts`)

```python
def modal(options: ModalProviderOptions = {}) -> SandboxProvider:
    """
    Uses Modal SDK.

    Options:
      create: dict or callable
      image: str or Image = "rivetdev/sandbox-agent:0.5.0-rc.2-full"
      agent_port: int = 3000

    default_cwd = "/root"

    create() flow:
    1. Create Modal app
    2. Create sandbox from image with encrypted port
    3. Exec sandbox-agent server in background

    get_url() uses Modal tunnel URL.
    """
```

### 10.6 Other Providers

Additional providers exist in the source (`agentcomputer.ts`, `cloudflare.ts`, `computesdk.ts`, `sprites.ts`, `vercel.ts`) but follow similar patterns. They are not documented in detail here.

### 10.7 Shared Constants (`providers/shared.ts`)

```python
SANDBOX_AGENT_VERSION = "0.5.0-rc.2"
DEFAULT_SANDBOX_AGENT_IMAGE = f"rivetdev/sandbox-agent:{SANDBOX_AGENT_VERSION}-full"
SANDBOX_AGENT_INSTALL_SCRIPT = f"https://releases.rivet.dev/sandbox-agent/{SANDBOX_AGENT_VERSION}/install.sh"
SANDBOX_AGENT_NPX_SPEC = f"@sandbox-agent/cli@{SANDBOX_AGENT_VERSION}"
DEFAULT_AGENTS = ["claude", "codex"]

def build_server_start_command(port: int) -> str:
    return f"nohup sandbox-agent server --no-token --host 0.0.0.0 --port {port} >/tmp/sandbox-agent.log 2>&1 &"
```

---

## 11. Spawn Module

Handles spawning a local `sandbox-agent` binary as a subprocess.

### 11.1 Types

```python
SandboxAgentSpawnLogMode = Literal["inherit", "pipe", "silent"]

@dataclass
class SandboxAgentSpawnOptions:
    enabled: Optional[bool] = None
    host: Optional[str] = None  # default "127.0.0.1"
    port: Optional[int] = None  # auto-assigned if not specified
    token: Optional[str] = None  # auto-generated (24 random hex bytes)
    binary_path: Optional[str] = None  # auto-detected
    timeout_ms: Optional[int] = None  # default 15_000
    log: Optional[SandboxAgentSpawnLogMode] = None  # default "inherit"
    env: Optional[dict[str, str]] = None

@dataclass
class SandboxAgentSpawnHandle:
    base_url: str    # "http://{host}:{port}"
    token: str
    child: Process   # subprocess handle
    dispose: Callable[[], Awaitable[None]]  # SIGTERM -> wait 5s -> SIGKILL
```

### 11.2 `spawn_sandbox_agent(options, fetcher?) -> SandboxAgentSpawnHandle`

**Binary resolution order:**
1. `SANDBOX_AGENT_BIN` environment variable
2. Platform-specific npm package: `@sandbox-agent/cli-{platform}-{arch}/bin/sandbox-agent`
3. `sandbox-agent` on `PATH`

**Spawn command:** `{binary} server --host {host} --port {port} --token {token}`

**Health wait:** Polls `GET {baseUrl}/v1/health` with Bearer token every 200ms for up to `timeout_ms` (default 15s).

**Process cleanup:** Registers handlers for `exit`, `SIGINT`, `SIGTERM` to kill the child process.

**Dispose behavior:** SIGTERM -> wait 5 seconds -> SIGKILL if still running.

---

## 12. Inspector Utility

```python
@dataclass
class InspectorUrlOptions:
    base_url: str
    token: Optional[str] = None
    headers: Optional[dict[str, str]] = None

def build_inspector_url(options: InspectorUrlOptions) -> str:
    """
    Builds: {base_url}/ui/?token={token}&headers={json_headers}
    Query params are omitted when not provided.
    Trailing slashes on base_url are normalized.
    """
```

---

## 13. ACP HTTP Client (Dependency)

The `acp-http-client` package provides the JSON-RPC transport layer. Key details:

### 13.1 Transport: StreamableHTTP

- **Outbound messages:** `POST /v1/acp/{server_id}` with JSON body
  - First POST includes query `?agent={agent_name}` for bootstrap
  - Response can be immediate JSON (200) or empty (202)
- **Inbound messages:** `GET /v1/acp/{server_id}` as SSE stream
  - Uses `Last-Event-Id` header for reconnection
  - Each SSE event contains a JSON-RPC envelope
- **Disconnect:** `DELETE /v1/acp/{server_id}` (best-effort, 2s timeout)

### 13.2 Connection Lifecycle

1. `initialize()` -> sends `initialize` JSON-RPC request
2. If `authMethods` returned, SDK calls `authenticate()` with appropriate method
3. `new_session()` -> starts a session on the agent
4. `prompt()` / `cancel()` / `set_session_mode()` / etc. -> session operations
5. `disconnect()` -> sends DELETE, closes streams

### 13.3 Key Methods

```python
class AcpHttpClient:
    async def initialize(self, request: dict = {}) -> InitializeResponse: ...
    async def authenticate(self, request: dict) -> dict: ...
    async def new_session(self, request: dict) -> NewSessionResponse: ...
    async def prompt(self, request: PromptRequest) -> PromptResponse: ...
    async def cancel(self, notification: dict) -> None: ...
    async def set_session_mode(self, request: dict) -> Optional[dict]: ...
    async def set_session_config_option(self, request: dict) -> dict: ...
    async def ext_method(self, method: str, params: dict) -> dict: ...
    async def ext_notification(self, method: str, params: dict) -> None: ...
    async def disconnect(self) -> None: ...
```

### 13.4 Envelope Observation

Every inbound and outbound envelope passes through `on_envelope(envelope, direction)` callback.
- `direction`: `"inbound"` (from agent) or `"outbound"` (from client)
- The SDK persists each observed envelope as a `SessionEvent`

---

## 14. SSE / Event Streaming

### 14.1 ACP Event Stream

**Endpoint:** `GET /v1/acp/{server_id}`

This is managed by the `AcpHttpClient`'s internal `StreamableHttpAcpTransport`.

**SSE format:**
```
id: {event_id}
data: {json_rpc_envelope}

```

**Reconnection:**
- The transport maintains `lastEventId`
- On reconnect, sends `Last-Event-Id: {id}` header
- The GET SSE loop runs continuously until the transport is closed

**Deduplication:**
- The transport tracks `seenResponseIds` to avoid processing duplicate responses
- Caps dedup set at 512 entries (sliding window)

### 14.2 Process Log Stream

**Endpoint:** `GET /v1/processes/{id}/logs?follow=true`  
**Accept:** `text/event-stream`

**SSE format:**
```
event: log
data: {"data":"...","encoding":"...","sequence":N,"stream":"stdout","timestampMs":N}

```

**Parsing algorithm (`consumeProcessLogSse`):**
1. Read from the response body stream.
2. Decode bytes as UTF-8, normalize `\r\n` to `\n`.
3. Buffer text until `\n\n` separator found.
4. For each chunk between separators:
   - Parse `event:` line (default: `"message"`).
   - Collect all `data:` lines, join with `\n`.
   - Skip comment lines (starting with `:`).
   - Only emit entries where `event == "log"`.
   - Parse the data as JSON `ProcessLogEntry`.
5. Abort on signal, ignore abort errors.

### 14.3 SDK Session Events (in-memory)

Session events are NOT streamed via SSE from the server. Instead:
- The SDK observes all ACP envelopes via the `onEnvelope` callback.
- Each envelope is persisted as a `SessionEvent` with a monotonic `eventIndex`.
- Registered `on_session_event()` listeners are called synchronously after persistence.
- Event index allocation scans existing events to find the max, then increments.
- Retries up to 3 times on UNIQUE constraint conflicts.
- Envelope persistence is serialized per-session (queued, not parallel).

---

## 15. HTTP Endpoint Map

Complete map of all endpoints the SDK calls:

### Health
| Method | Path | Description |
|--------|------|-------------|
| GET | `/v1/health` | Health check |

### ACP (Agent Client Protocol)
| Method | Path | Description |
|--------|------|-------------|
| GET | `/v1/acp/servers` | List ACP servers |
| POST | `/v1/acp/{server_id}?agent={name}` | Send JSON-RPC envelope (first call includes agent query) |
| GET | `/v1/acp/{server_id}` | SSE stream of inbound envelopes |
| DELETE | `/v1/acp/{server_id}` | Disconnect ACP session |

### Agents
| Method | Path | Description |
|--------|------|-------------|
| GET | `/v1/agents` | List agents |
| GET | `/v1/agents/{agent}` | Get agent info |
| POST | `/v1/agents/{agent}/install` | Install agent |

### File System
| Method | Path | Description |
|--------|------|-------------|
| GET | `/v1/fs/entries` | List directory entries |
| GET | `/v1/fs/file` | Read file (octet-stream) |
| PUT | `/v1/fs/file` | Write file (octet-stream body) |
| DELETE | `/v1/fs/entry` | Delete file or directory |
| POST | `/v1/fs/mkdir` | Create directory |
| POST | `/v1/fs/move` | Move/rename |
| GET | `/v1/fs/stat` | Stat file/directory |
| POST | `/v1/fs/upload-batch` | Upload tar archive |

### Configuration
| Method | Path | Description |
|--------|------|-------------|
| GET | `/v1/config/mcp` | Get MCP server config |
| PUT | `/v1/config/mcp` | Set MCP server config |
| DELETE | `/v1/config/mcp` | Delete MCP server config |
| GET | `/v1/config/skills` | Get skills config |
| PUT | `/v1/config/skills` | Set skills config |
| DELETE | `/v1/config/skills` | Delete skills config |

### Processes
| Method | Path | Description |
|--------|------|-------------|
| GET | `/v1/processes/config` | Get process limits |
| POST | `/v1/processes/config` | Set process limits |
| POST | `/v1/processes` | Create persistent process |
| POST | `/v1/processes/run` | Run command synchronously |
| GET | `/v1/processes` | List processes |
| GET | `/v1/processes/{id}` | Get process info |
| POST | `/v1/processes/{id}/stop` | Stop process (SIGTERM) |
| POST | `/v1/processes/{id}/kill` | Kill process (SIGKILL) |
| DELETE | `/v1/processes/{id}` | Delete process record |
| GET | `/v1/processes/{id}/logs` | Get/follow process logs |
| POST | `/v1/processes/{id}/input` | Send stdin input |
| POST | `/v1/processes/{id}/terminal/resize` | Resize terminal |
| WS | `/v1/processes/{id}/terminal/ws` | Terminal WebSocket |

### Desktop
| Method | Path | Description |
|--------|------|-------------|
| POST | `/v1/desktop/start` | Start desktop |
| POST | `/v1/desktop/stop` | Stop desktop |
| GET | `/v1/desktop/status` | Desktop status |
| GET | `/v1/desktop/display/info` | Display info |
| GET | `/v1/desktop/screenshot` | Take screenshot |
| GET | `/v1/desktop/screenshot/region` | Region screenshot |
| GET | `/v1/desktop/mouse/position` | Mouse position |
| POST | `/v1/desktop/mouse/move` | Move mouse |
| POST | `/v1/desktop/mouse/click` | Click |
| POST | `/v1/desktop/mouse/down` | Mouse down |
| POST | `/v1/desktop/mouse/up` | Mouse up |
| POST | `/v1/desktop/mouse/drag` | Drag |
| POST | `/v1/desktop/mouse/scroll` | Scroll |
| POST | `/v1/desktop/keyboard/type` | Type text |
| POST | `/v1/desktop/keyboard/press` | Press key |
| POST | `/v1/desktop/keyboard/down` | Key down |
| POST | `/v1/desktop/keyboard/up` | Key up |
| GET | `/v1/desktop/windows` | List windows |
| GET | `/v1/desktop/windows/focused` | Focused window |
| POST | `/v1/desktop/windows/{id}/focus` | Focus window |
| POST | `/v1/desktop/windows/{id}/move` | Move window |
| POST | `/v1/desktop/windows/{id}/resize` | Resize window |
| GET | `/v1/desktop/clipboard` | Read clipboard |
| POST | `/v1/desktop/clipboard` | Write clipboard |
| POST | `/v1/desktop/launch` | Launch app |
| POST | `/v1/desktop/open` | Open target |
| GET | `/v1/desktop/stream/status` | Stream status |
| POST | `/v1/desktop/stream/start` | Start stream |
| POST | `/v1/desktop/stream/stop` | Stop stream |
| WS | `/v1/desktop/stream/signaling` | Stream WebSocket |
| POST | `/v1/desktop/recording/start` | Start recording |
| POST | `/v1/desktop/recording/stop` | Stop recording |
| GET | `/v1/desktop/recordings` | List recordings |
| GET | `/v1/desktop/recordings/{id}` | Get recording |
| GET | `/v1/desktop/recordings/{id}/download` | Download recording |
| DELETE | `/v1/desktop/recordings/{id}` | Delete recording |

### Inspector UI
| Method | Path | Description |
|--------|------|-------------|
| GET | `/ui/` | Inspector web UI |

---

## Appendix A: Key Constants

| Constant | Value | Location |
|----------|-------|----------|
| `API_PREFIX` | `"/v1"` | client.ts |
| `FS_PATH` | `"/v1/fs"` | client.ts |
| `DEFAULT_BASE_URL` | `"http://sandbox-agent"` | client.ts |
| `DEFAULT_REPLAY_MAX_EVENTS` | `50` | client.ts |
| `DEFAULT_REPLAY_MAX_CHARS` | `12_000` | client.ts |
| `EVENT_INDEX_SCAN_EVENTS_LIMIT` | `500` | client.ts |
| `MAX_EVENT_INDEX_INSERT_RETRIES` | `3` | client.ts |
| `HEALTH_WAIT_MIN_DELAY_MS` | `500` | client.ts |
| `HEALTH_WAIT_MAX_DELAY_MS` | `15_000` | client.ts |
| `HEALTH_WAIT_LOG_AFTER_MS` | `5_000` | client.ts |
| `HEALTH_WAIT_LOG_EVERY_MS` | `10_000` | client.ts |
| `HEALTH_WAIT_ENSURE_SERVER_AFTER_FAILURES` | `3` | client.ts |
| `DEFAULT_MAX_SESSIONS` | `1024` | types.ts |
| `DEFAULT_MAX_EVENTS_PER_SESSION` | `500` | types.ts |
| `DEFAULT_LIST_LIMIT` | `100` | types.ts |
| `SANDBOX_AGENT_VERSION` | `"0.5.0-rc.2"` | shared.ts |
| `DEFAULT_AGENT_PORT` (most providers) | `3000` | providers/*.ts |

## Appendix B: Public Exports (index.ts)

```
Classes:
  SandboxAgent
  Session
  LiveAcpConnection
  ProcessTerminalSession
  DesktopStreamSession
  InMemorySessionPersistDriver

Error classes:
  SandboxAgentError
  SandboxDestroyedError
  UnsupportedSessionCategoryError
  UnsupportedSessionValueError
  UnsupportedSessionConfigOptionError
  UnsupportedPermissionReplyError
  AcpRpcError (re-exported from acp-http-client)

Functions:
  buildInspectorUrl

All types listed in Section 2 are exported as TypeScript types.
SandboxProvider interface is exported as a type.
SandboxAgentSpawnLogMode and SandboxAgentSpawnOptions are exported as types.
```

## Appendix C: Authentication Flow

When a `LiveAcpConnection` is created:
1. `acp.initialize()` is called, which returns `InitializeResponse`
2. If `initResult.authMethods` is non-empty, `autoAuthenticate()` is called
3. `autoAuthenticate()` looks for env-var-based auth methods:
   - `"codex-api-key"`
   - `"openai-api-key"`
   - `"anthropic-api-key"`
4. If found, calls `acp.authenticate({ methodId: envBased.id })`
5. Authentication is best-effort (errors are swallowed)

## Appendix D: Session Lifecycle State Machine

```
                create_session()
                      |
                      v
    +---------- [ACTIVE] <---------+
    |                |              |
    |    prompt() / set_mode()   resume_session()
    |                |              |
    |                v              |
    |          [ACTIVE]  ----------+
    |                |       (connection drop)
    |                |
    |        destroy_session()
    |                |
    |                v
    +-------> [DESTROYED]
              (destroyed_at set)
```

- `resume_session()` recreates the agent-side session and queues replay text.
- `destroy_session()` sends `session/cancel` and marks `destroyed_at`.
- A "stale connection" (connection ID mismatch) triggers automatic `resume_session()` on next `send`.
