"""REST API server — agent/sandbox/session orchestration layer.

Run: uvicorn src.api.server:app --port 7778
"""

import asyncio
import json
import logging
import os
import tempfile
import time
import uuid
from contextlib import asynccontextmanager
from pathlib import Path

import httpx
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse, Response, StreamingResponse

from .models import (
    AgentConfig, AgentRecord, SandboxRecord, SessionState,
    EVT_USER_MESSAGE, EVT_ASSISTANT_MESSAGE, EVT_TOOL_CALL, EVT_TOOL_RESULT, EVT_USAGE, EVT_ERROR,
    STATUS_RUNNING,
)
from .db import (
    init_db, init_pool, close_pool,
    upsert_agent, get_agent, list_agents, delete_agent,
    upsert_sandbox, get_sandbox, list_sandboxes, delete_sandbox,
    upsert_session, get_session,
    log_event, get_session_log, get_agent_log,
)
from .sandbox_agent_client import SandboxAgentClient, _mcp_dict_to_acp_array
from .sse import (
    parse_sse_data, iter_sse_blocks, parse_acp_payload,
    UT_MESSAGE_DELTA, UT_MESSAGE_CHUNK, UT_TOOL_CALL, UT_TOOL_STARTED,
    UT_TOOL_CALL_UPDATE, UT_USAGE_UPDATED, UT_USAGE_UPDATE,
)
from .providers import PORT_BASED_PROVIDERS, ProviderInstance, create_instance, destroy_instance, stop_instance, stop_daytona
from .redact import redact_secrets
from .sandbox import get_sandbox_client, ensure_sandbox_running

# Configure logging for the api namespace so uvicorn displays our logs.
# Level can be overridden via LOG_LEVEL env var.
logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logging.getLogger("api").setLevel(os.environ.get("LOG_LEVEL", "INFO"))
log = logging.getLogger(__name__)

_UI_DIR = Path(__file__).parent.parent.parent / "ui"
_CHAT_HTML: str | None = None
_KANBAN_HTML: str | None = None

# ---------------------------------------------------------------------------
# DB + in-memory state
# ---------------------------------------------------------------------------

# keyed by sandbox_id — keeps ProviderInstance alive for cleanup
_INSTANCES: dict[str, ProviderInstance] = {}

_sandbox_locks: dict[str, asyncio.Lock] = {}
_session_locks: dict[str, asyncio.Lock] = {}


def _get_sandbox_lock(sandbox_id: str) -> asyncio.Lock:
    return _sandbox_locks.setdefault(sandbox_id, asyncio.Lock())


def _get_session_lock(session_id: str) -> asyncio.Lock:
    return _session_locks.setdefault(session_id, asyncio.Lock())


# keyed by session_id
SESSIONS: dict[str, SessionState] = {}


async def _cancel_task(task) -> None:
    """Cancel an asyncio task and await its completion so cleanup code runs."""
    if task is None or task.done():
        return
    task.cancel()
    try:
        await task
    except (asyncio.CancelledError, Exception):
        pass


async def _close_session_gracefully(state: "SessionState", *, background: bool = False) -> None:
    """Close the HTTP connection to sandbox-agent.

    We just drop the connection — sandbox-agent detects the disconnect
    and cleans up child processes. The old DELETE /v1/acp/{id} approach
    hangs because sandbox-agent blocks waiting for Claude to shut down.
    """
    client = state.client
    if not client:
        return
    state.client = None
    try:
        await client.aclose()
    except Exception:
        pass


async def _shutdown_session_state(
    state: SessionState,
    *,
    remove: bool,
    mark_idle_at: float | None = None,
    background_close: bool = False,
) -> None:
    """Close a runtime session and optionally remove it from the active registry."""
    state.shutdown.set()
    state.agent_busy = False
    idle_at = time.time() if mark_idle_at is None else mark_idle_at
    state.turn_completed_at = idle_at
    state.last_activity = idle_at
    await _cancel_task(state._reader_task)
    await _close_session_gracefully(state, background=background_close)
    if remove and SESSIONS.get(state.session_id) is state:
        SESSIONS.pop(state.session_id, None)
        _session_locks.pop(state.session_id, None)


def _mark_turn_finished(state: SessionState, at: float | None = None) -> float:
    """Mark a session turn as terminal so idle reaping can proceed."""
    finished_at = time.time() if at is None else at
    state.agent_busy = False
    state.turn_completed_at = finished_at
    state.last_activity = finished_at
    return finished_at


def _session_idle_since(state: SessionState) -> float:
    """When did this session most recently become fully idle?"""
    return state.turn_completed_at or state.last_activity


# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------

IDLE_TIMEOUT_S = int(os.environ.get("SANDBOX_IDLE_TIMEOUT", "300"))  # 5 min default


async def _idle_reaper():
    """Background task: close idle sessions that have been inactive too long."""
    while True:
        await asyncio.sleep(60)
        now = time.time()
        log.info("idle reaper tick: sessions=%d", len(SESSIONS))

        for state in list(SESSIONS.values()):
            if state.agent_busy:
                continue
            idle_since = _session_idle_since(state)
            if now - idle_since < IDLE_TIMEOUT_S:
                continue
            log.info("idle reaper: closing session %s (idle %.0fs)",
                     state.session_id, now - idle_since)
            sandbox_id = state.sandbox_id
            await _shutdown_session_state(state, remove=True, mark_idle_at=now)
            # If no other sessions use this sandbox, stop the sandbox-agent.
            # For local/docker: kills the process (no filesystem to preserve).
            # For daytona: stops the workspace (filesystem preserved for resume).
            if not any(s.sandbox_id == sandbox_id for s in SESSIONS.values()):
                instance = _INSTANCES.pop(sandbox_id, None)
                if instance:
                    log.info("idle reaper: stopping sandbox %s (provider=%s)",
                             sandbox_id, instance.provider)
                    try:
                        await stop_instance(instance)
                        log.info("idle reaper: sandbox %s stopped", sandbox_id)
                    except Exception as e:
                        log.warning("reaper: failed to stop sandbox %s: %s", sandbox_id, e)


@asynccontextmanager
async def lifespan(app):
    init_db()
    await init_pool()
    reaper = asyncio.create_task(_idle_reaper())
    yield
    await _cancel_task(reaper)
    # Parallel session shutdown
    await asyncio.gather(
        *[_shutdown_session_state(s, remove=False) for s in SESSIONS.values()],
        return_exceptions=True,
    )
    SESSIONS.clear()
    # Parallel instance teardown
    async def _safe_destroy(inst):
        try:
            await destroy_instance(inst)
        except Exception as e:
            log.warning("shutdown cleanup failed: %s", e)
    await asyncio.gather(*[_safe_destroy(i) for i in _INSTANCES.values()])
    _INSTANCES.clear()
    await close_pool()

app = FastAPI(title="Agent Orchestration API", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.exception_handler(HTTPException)
async def _http_exception_handler(request: Request, exc: HTTPException):
    """Return dict details as-is so dependencies can control the response shape."""
    detail = exc.detail
    if isinstance(detail, dict):
        return JSONResponse(detail, status_code=exc.status_code)
    return JSONResponse({"error": detail}, status_code=exc.status_code)


# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------

@app.get("/health")
async def health():
    return {"status": "ok"}


# ---------------------------------------------------------------------------
# Persistent SSE reader — connects to sandbox-agent once per session
# ---------------------------------------------------------------------------

_SSE_SENTINEL = object()


async def _flush_text_parts(state: SessionState, parts: list[str]) -> None:
    """Fire-and-forget: flush accumulated assistant text to the log."""
    if not parts:
        return
    try:
        await log_event(
            session_id=state.session_id, agent_id=state.agent_id,
            sandbox_id=state.sandbox_id,
            event_type=EVT_ASSISTANT_MESSAGE,
            payload={"text": redact_secrets("".join(parts))},
        )
    except Exception:
        pass


def _maybe_auto_approve_permission(block: str, state: SessionState) -> None:
    """If the SSE block is a session/request_permission, auto-approve it."""
    payload = parse_sse_data(block)
    if not payload or payload.get("method") != "session/request_permission":
        return
    rpc_id = payload.get("id")
    if rpc_id is None:
        return
    params = payload.get("params", {})
    options = params.get("options", [])
    # Pick "allow_always" > "allow" > first option
    option_id = None
    for opt in options:
        if opt.get("kind") == "allow_always":
            option_id = opt["optionId"]
            break
    if not option_id:
        for opt in options:
            if opt.get("kind") == "allow_once":
                option_id = opt["optionId"]
                break
    if not option_id and options:
        option_id = options[0].get("optionId")
    if not option_id:
        return

    async def _grant():
        try:
            # Send JSON-RPC response (not request) back to the ACP endpoint
            url = f"/v1/acp/{state.acp_session_id}"
            resp_payload = {
                "jsonrpc": "2.0",
                "id": rpc_id,
                "result": {"optionId": option_id},
            }
            resp = await state.client._client.post(url, json=resp_payload)
            log.info("auto-approved permission for session %s (status=%d)", state.session_id, resp.status_code)
        except Exception as e:
            log.warning("auto-approve permission failed for session %s: %s", state.session_id, e)

    asyncio.create_task(_grant())


def _start_sse_reader(state: SessionState) -> None:
    """Start a background task that reads SSE from the sandbox-agent and
    broadcasts chunks to all subscriber queues. Called at session creation
    so events are captured before any prompt is sent."""
    if state._reader_alive:
        return
    state._reader_alive = True

    async def _reader():
        reader_buffer = ""
        text_parts: list[str] = []
        sse_http = None
        try:
            headers = {"Accept": "text/event-stream"}
            if state.last_event_id:
                headers["Last-Event-ID"] = state.last_event_id
            sse_http = httpx.AsyncClient(
                base_url=state.client.base_url, timeout=None,
            )
            async with sse_http.stream(
                "GET", f"/v1/acp/{state.acp_session_id}", headers=headers,
            ) as resp:
                async for chunk in resp.aiter_text():
                    if state.shutdown.is_set():
                        return
                    state.broadcast(chunk)
                    reader_buffer += chunk
                    while "\n\n" in reader_buffer:
                        block, reader_buffer = reader_buffer.split("\n\n", 1)
                        _process_sse_block(block, state, text_parts, log_events=True)
                        # Auto-approve permission requests for headless operation
                        _maybe_auto_approve_permission(block, state)
        except asyncio.CancelledError:
            pass
        except Exception as e:
            log.warning("SSE reader error for session %s: %s", state.session_id, e)
        finally:
            # Explicit httpx cleanup — safety net in case CancelledError
            # interrupted the async-with context manager's __aexit__.
            if sse_http is not None:
                try:
                    await sse_http.aclose()
                except Exception:
                    pass
            state._reader_alive = False
            log.warning("[SSE-READER] reader exited for session %s (shutdown=%s, agent_busy=%s, in_SESSIONS=%s)",
                     state.session_id, state.shutdown.is_set(), state.agent_busy,
                     state.session_id in SESSIONS)
            # If the reader died while the agent was busy (e.g., sandbox-agent
            # crash), the stopReason will never arrive. Clear agent_busy so the
            # session isn't permanently stuck.  Skip this on intentional shutdown
            # since _shutdown_session_state already handles cleanup.
            if state.agent_busy and not state.shutdown.is_set():
                log.warning("[SSE-READER] reader died while agent_busy — clearing busy flag for session %s",
                            state.session_id)
                state.agent_busy = False
                state.turn_completed_at = time.time()
            if text_parts:
                asyncio.create_task(_flush_text_parts(state, list(text_parts)))
                text_parts.clear()
            state.broadcast(_SSE_SENTINEL)

    state._reader_task = asyncio.create_task(_reader())


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

async def _apply_config_and_initialize(
    client: SandboxAgentClient,
    config: AgentConfig,
    acp_session_id: str,
    cwd: str,
) -> None:
    """Configure MCP servers + deploy skills, then initialize the ACP session."""
    setup_tasks = []
    for mcp_name, mcp_cfg in (config.mcp_servers or {}).items():
        setup_tasks.append(client.configure_mcp(mcp_name, mcp_cfg, directory=cwd))
    for skill_name, skill_cfg in (config.skills or {}).items():
        setup_tasks.append(client.configure_skills(skill_name, skill_cfg, directory=cwd))
    if setup_tasks:
        results = await asyncio.gather(*setup_tasks, return_exceptions=True)
        errors = [r for r in results if isinstance(r, Exception)]
        if errors:
            raise errors[0]
    if config.skills:
        await client.deploy_skills_from_config(config.skills, cwd=cwd)
    await client.initialize(
        acp_session_id, config.agent_type or "claude",
        cwd=cwd, mcp_servers=config.mcp_servers,
    )


# ---------------------------------------------------------------------------
# Request helpers
# ---------------------------------------------------------------------------

_CONFIG_KEYS = ("model", "prompt", "tools", "mcp_servers", "skills", "agent_type", "cwd", "dockerfile", "dockerfile_content")


def _merge_top_level_config(data: dict, config_data: dict) -> None:
    """Merge SDK top-level keys into config_data if not already present."""
    for key in _CONFIG_KEYS:
        if key in data and key not in config_data:
            config_data[key] = data[key]


def _materialize_dockerfile(data: dict, key: str = "dockerfile") -> str | None:
    """Write dockerfile_content from request data to a temp file. Returns path or None."""
    path = data.get(key)
    if path:
        return path
    content = data.get("dockerfile_content")
    if not content:
        return None
    tmp = tempfile.NamedTemporaryFile(suffix=".Dockerfile", delete=False, mode="w")
    tmp.write(content)
    tmp.close()
    return tmp.name


def _derive_sandbox_ref(instance: ProviderInstance, provider: str, sandbox_id: str) -> str:
    if provider in PORT_BASED_PROVIDERS:
        return str(instance.port)
    return instance.sandbox_id or sandbox_id


# ---------------------------------------------------------------------------
# Agent CRUD (config only, no sandbox)
# ---------------------------------------------------------------------------

@app.post("/agents")
async def create_agent(request: Request):
    data = await request.json()
    agent_id = str(uuid.uuid4())
    name = data.get("name")
    config_data = data.get("config", {})
    _merge_top_level_config(data, config_data)
    materialized = _materialize_dockerfile(config_data)
    if materialized:
        config_data["dockerfile"] = materialized
    config = AgentConfig.from_dict(config_data)
    record = AgentRecord(id=agent_id, name=name, config=config)
    await upsert_agent(record)
    return {"id": agent_id, "name": name, "config": config.to_dict()}


@app.get("/agents")
async def list_agents_route():
    agents = await list_agents()
    return [{"id": a.id, "name": a.name, "config": a.config.to_dict()} for a in agents]


@app.get("/agents/{agent_id}")
async def get_agent_route(agent_id: str):
    record = await get_agent(agent_id)
    if record is None:
        return JSONResponse({"error": "agent not found"}, status_code=404)
    return {"id": record.id, "name": record.name, "config": record.config.to_dict()}


@app.delete("/agents/{agent_id}")
async def delete_agent_route(agent_id: str):
    record = await get_agent(agent_id)
    if record is None:
        return JSONResponse({"error": "agent not found"}, status_code=404)
    await delete_agent(agent_id)
    return {"status": "deleted"}


# ---------------------------------------------------------------------------
# Sandbox CRUD
# ---------------------------------------------------------------------------

@app.post("/sandboxes")
async def create_sandbox(request: Request):
    data = await request.json()
    provider = data.get("provider", "local")
    agent_type = data.get("agent_type", "claude")
    dockerfile = _materialize_dockerfile(data)
    sandbox_id = str(uuid.uuid4())
    try:
        instance = await create_instance(provider, agent_type, dockerfile=dockerfile)
    except Exception as e:
        return JSONResponse({"error": f"Provider '{provider}' failed: {e}"}, status_code=502)

    sandbox_ref = _derive_sandbox_ref(instance, provider, sandbox_id)

    _INSTANCES[sandbox_id] = instance
    record = SandboxRecord(id=sandbox_id, provider=provider, sandbox_ref=sandbox_ref, status=STATUS_RUNNING)
    await upsert_sandbox(record)
    return {"id": sandbox_id, "provider": provider, "sandbox_ref": sandbox_ref, "status": "running"}


@app.get("/sandboxes")
async def list_sandboxes_route():
    sandboxes = await list_sandboxes()
    return [{"id": s.id, "provider": s.provider, "sandbox_ref": s.sandbox_ref, "status": s.status} for s in sandboxes]


@app.get("/sandboxes/{sandbox_id}")
async def get_sandbox_route(sandbox_id: str):
    record = await get_sandbox(sandbox_id)
    if record is None:
        return JSONResponse({"error": "sandbox not found"}, status_code=404)
    result = {"id": record.id, "provider": record.provider, "sandbox_ref": record.sandbox_ref, "status": record.status}
    if record.provider in PORT_BASED_PROVIDERS:
        try:
            result["url"] = record.derive_url()
        except Exception:
            pass
    return result


@app.delete("/sandboxes/{sandbox_id}")
async def delete_sandbox_route(sandbox_id: str):
    record = await get_sandbox(sandbox_id)
    if record is None:
        return JSONResponse({"error": "sandbox not found"}, status_code=404)

    # Hold the sandbox lock to prevent concurrent _ensure_sandbox_alive
    # from auto-restarting the sandbox while we're deleting it.
    async with _get_sandbox_lock(sandbox_id):
        # Clean up sessions BEFORE removing the instance so that
        # concurrent requests still see the sandbox as existing.
        for sid, state in list(SESSIONS.items()):
            if state.sandbox_id == sandbox_id:
                await _shutdown_session_state(state, remove=True)

        instance = _INSTANCES.pop(sandbox_id, None)
        _sandbox_locks.pop(sandbox_id, None)
        await delete_sandbox(sandbox_id)

    # Teardown the process/container outside the lock (may be slow)
    if instance:
        try:
            await destroy_instance(instance)
        except Exception as e:
            log.warning("teardown failed for sandbox %s: %s", sandbox_id, e)

    return {"status": "deleted"}


@app.post("/sandboxes/{sandbox_id}/stop")
async def stop_sandbox_route(sandbox_id: str):
    record = await get_sandbox(sandbox_id)
    if record is None:
        return JSONResponse({"error": "sandbox not found"}, status_code=404)

    # Hold the sandbox lock to prevent concurrent _ensure_sandbox_alive
    # from auto-restarting the sandbox while we're stopping it.
    async with _get_sandbox_lock(sandbox_id):
        instance = _INSTANCES.pop(sandbox_id, None)
        record.status = "stopped"
        await upsert_sandbox(record)

    # Stop the sandbox outside the lock (may be slow)
    if instance and record.provider == "daytona":
        await stop_daytona(instance)

    return {"status": "stopped"}


@app.post("/sandboxes/{sandbox_id}/start")
async def start_sandbox_route(sandbox_id: str):
    record = await get_sandbox(sandbox_id)
    if record is None:
        return JSONResponse({"error": "sandbox not found"}, status_code=404)

    try:
        url = await ensure_sandbox_running(sandbox_id)
    except Exception as e:
        return JSONResponse({"error": f"failed to start sandbox: {e}"}, status_code=500)

    record.status = "running"
    await upsert_sandbox(record)
    return {"status": "running", "url": url}


# ---------------------------------------------------------------------------
# Session operations (on sandbox)
# ---------------------------------------------------------------------------

async def _bg_log(session_id: str, agent_id: str, sandbox_id: str,
                  event_type: str, payload: dict) -> None:
    """Fire-and-forget DB log — suppresses all exceptions."""
    try:
        if "text" in payload:
            payload = {**payload, "text": redact_secrets(payload["text"])}
        await log_event(session_id=session_id, agent_id=agent_id,
                        sandbox_id=sandbox_id, event_type=event_type, payload=payload)
    except Exception:
        pass


def _process_sse_block(block: str, state: SessionState, text_parts: list,
                       *, log_events: bool = False) -> None:
    """Parse SSE block: accumulate text, update state, optionally log to DB.

    The reader calls this with log_events=True (single writer).
    Subscribers and replayers call with log_events=False (read-only).
    """
    # Track the SSE cursor from the single reader only — multiple
    # proxy subscribers must not write last_event_id concurrently.
    if log_events:
        for line in block.split("\n"):
            if line.startswith("id:"):
                state.last_event_id = line[3:].strip()
                break
        state.last_activity = time.time()

    payload = parse_sse_data(block)
    if payload is None:
        return

    kind, data = parse_acp_payload(payload, None)

    if kind == "update":
        update = data or {}
        ut = update.get("sessionUpdate", "")

        if ut in (UT_MESSAGE_DELTA, UT_MESSAGE_CHUNK):
            text = update.get("content", {}).get("text", "")
            if text:
                text_parts.append(text)

        elif log_events and ut in (UT_TOOL_CALL, UT_TOOL_STARTED):
            meta = update.get("_meta", {}).get("claudeCode", {})
            tool_payload: dict = {"tool": meta.get("toolName", "unknown")}
            raw_input = update.get("rawInput")
            if raw_input:
                tool_payload["args"] = raw_input
            asyncio.create_task(_bg_log(
                state.session_id, state.agent_id, state.sandbox_id,
                EVT_TOOL_CALL, tool_payload,
            ))

        elif log_events and ut == UT_TOOL_CALL_UPDATE:
            meta = update.get("_meta", {}).get("claudeCode", {})
            tool_response = meta.get("toolResponse")
            if tool_response:
                asyncio.create_task(_bg_log(
                    state.session_id, state.agent_id, state.sandbox_id,
                    EVT_TOOL_RESULT, {"tool": meta.get("toolName", "unknown"), "result": tool_response},
                ))
            elif update.get("rawInput"):
                asyncio.create_task(_bg_log(
                    state.session_id, state.agent_id, state.sandbox_id,
                    EVT_TOOL_CALL, {"tool": meta.get("toolName", "unknown"), "args": update["rawInput"]},
                ))

        elif log_events and ut in (UT_USAGE_UPDATED, UT_USAGE_UPDATE):
            asyncio.create_task(_bg_log(
                state.session_id, state.agent_id, state.sandbox_id,
                EVT_USAGE, update.get("cost", update),
            ))
        return

    if kind == "done_result":
        # JSON-RPC result with stopReason → agent turn finished
        if log_events:
            _mark_turn_finished(state)
        if text_parts:
            text = "".join(text_parts)
            text_parts.clear()
            if log_events:
                asyncio.create_task(_bg_log(
                    state.session_id, state.agent_id, state.sandbox_id,
                    EVT_ASSISTANT_MESSAGE, {"text": text},
                ))
        return

    if kind == "error" and log_events:
        _mark_turn_finished(state)
        err = data or {}
        asyncio.create_task(_bg_log(
            state.session_id, state.agent_id, state.sandbox_id,
            EVT_ERROR, {"message": err.get("message", str(err))[:500]},
        ))


async def _find_last_replay_event_id(
    base_url: str, acp_session_id: str, load_rpc_id: str, timeout_s: float = 60,
) -> str | None:
    """Read SSE events until the session/load RPC response appears, return its event ID.

    After session/load, the ACP server replays the old conversation as SSE events,
    ending with a JSON-RPC response whose id matches load_rpc_id.
    Times out after ``timeout_s`` seconds to avoid blocking the event loop forever.
    """
    async def _scan() -> str | None:
        last_id = None
        async with httpx.AsyncClient(base_url=base_url, timeout=httpx.Timeout(30, read=timeout_s + 10, pool=30)) as h:
            async with h.stream(
                "GET", f"/v1/acp/{acp_session_id}",
                headers={"Accept": "text/event-stream"},
            ) as resp:
                async for block in iter_sse_blocks(resp):
                    for line in block.split("\n"):
                        if line.startswith("id:"):
                            last_id = line[3:].strip()
                    payload = parse_sse_data(block)
                    if payload is not None:
                        if str(payload.get("id", "")) == load_rpc_id and "result" in payload:
                            return last_id
        return last_id

    try:
        return await asyncio.wait_for(_scan(), timeout=timeout_s)
    except asyncio.TimeoutError:
        log.warning("find last replay event timed out after %ss", timeout_s)
        return None
    except Exception as e:
        log.warning("find last replay event failed: %s", e)
        return None


async def _ensure_sandbox_alive(sandbox_id: str, sandbox_record: SandboxRecord,
                                agent_type: str = "claude", dockerfile: str | None = None) -> str:
    """Ensure the sandbox-agent is reachable. Restart if needed. Returns URL."""
    instance = _INSTANCES.get(sandbox_id)
    provider = sandbox_record.provider

    # For local/docker: check if subprocess is alive
    if provider in PORT_BASED_PROVIDERS:
        async with _get_sandbox_lock(sandbox_id):
            instance = _INSTANCES.get(sandbox_id)
            # Re-check DB — the sandbox may have been deleted while we waited
            # for the lock (delete_sandbox_route holds the same lock).
            fresh_record = await get_sandbox(sandbox_id)
            if fresh_record is None:
                raise RuntimeError("Sandbox was deleted")
            if instance:
                if instance.process is not None:
                    if instance.process.returncode is None:
                        return instance.url  # local: still running
                elif instance.container_id:
                    # Docker: health-check URL
                    from .providers import _wait_for_health
                    if await _wait_for_health(instance.url, max_retries=2, interval=0.5):
                        return instance.url

            # Clean up old container before creating replacement
            if instance:
                try:
                    await destroy_instance(instance)
                except Exception:
                    pass  # best-effort cleanup

            log.info("auto-restarting sandbox %s (provider=%s)", sandbox_id, provider)
            try:
                new_instance = await create_instance(provider, agent_type, dockerfile=dockerfile)
            except Exception as e:
                raise RuntimeError(f"Failed to restart sandbox: {e}")

            _INSTANCES[sandbox_id] = new_instance
            new_ref = str(new_instance.port) if new_instance.port is not None else sandbox_id
            await upsert_sandbox(SandboxRecord(
                id=sandbox_id, provider=provider, sandbox_ref=new_ref, status=STATUS_RUNNING,
            ))
            return new_instance.url

    # For daytona: health-check the URL, restart via Daytona SDK if down
    daytona_sandbox_id = sandbox_record.sandbox_ref  # Daytona's own sandbox ID

    if instance and instance.url:
        try:
            async with httpx.AsyncClient(timeout=5) as client:
                r = await client.get(f"{instance.url}/v1/health")
                if r.status_code == 200:
                    return instance.url
        except Exception:
            pass

    # Daytona sandbox URL lost or stale — get fresh signed URL, start sandbox if stopped
    log.info("recovering daytona sandbox %s (daytona_id=%s)", sandbox_id, daytona_sandbox_id)
    try:
        from .providers import SANDBOX_AGENT_PORT, _wait_for_health, _get_daytona_client
        daytona_client = _get_daytona_client()
        loop = asyncio.get_running_loop()
        sandbox_obj = await loop.run_in_executor(None, lambda: daytona_client.get(daytona_sandbox_id))

        # Start sandbox if stopped (no-op if already running)
        # sandbox_obj.state may be an enum — compare .value for string check
        raw_state = sandbox_obj.state
        state_str = raw_state.value if hasattr(raw_state, 'value') else str(raw_state)
        if state_str != "started":
            await loop.run_in_executor(None, sandbox_obj.start)

        # Get a fresh signed preview URL
        signed = await loop.run_in_executor(None, lambda: sandbox_obj.create_signed_preview_url(SANDBOX_AGENT_PORT, 24 * 3600))
        url = signed.url

        # If sandbox-agent isn't responding, re-launch it
        if not await _wait_for_health(url, max_retries=5, interval=1.0):
            await loop.run_in_executor(None, lambda: sandbox_obj.process.exec(
                f"nohup sandbox-agent server --no-token --host 0.0.0.0 --port {SANDBOX_AGENT_PORT} >/dev/null 2>&1 &"
            ))
            if not await _wait_for_health(url, max_retries=20, interval=1.0):
                raise RuntimeError("Daytona sandbox-agent failed to respond after restart")

        _INSTANCES[sandbox_id] = ProviderInstance(provider="daytona", url=url, sandbox_id=daytona_sandbox_id)
    except ImportError:
        raise RuntimeError("daytona-sdk not installed, cannot restart sandbox")
    except Exception as e:
        raise RuntimeError(f"Failed to recover daytona sandbox: {e}")

    return url


async def _do_resume(*, sandbox_id: str, agent_id: str, inner_session_id: str,
                     client_session_id: str | None = None):
    """Core resume logic shared by both resume endpoints.

    A per-session lock ensures that concurrent resumes for the same session
    are serialized: the second caller waits until the first finishes, then
    returns the already-active session instead of creating a duplicate ACP
    process.
    """
    session_id = client_session_id or str(uuid.uuid4())

    async with _get_session_lock(session_id):
        # If a previous resume already created a live session, return it.
        existing = SESSIONS.get(session_id)
        if existing and not existing.shutdown.is_set():
            return {
                "session_id": session_id, "agent_id": existing.agent_id,
                "sandbox_id": existing.sandbox_id,
                "inner_session_id": existing.inner_session_id,
                "status": "already_active",
            }

        agent_record = await get_agent(agent_id)
        if agent_record is None:
            log.error("_do_resume: agent %s not found in DB", agent_id)
            return JSONResponse({"error": "agent not found"}, status_code=404)

        sandbox_record = await get_sandbox(sandbox_id)
        if sandbox_record is None:
            log.error("_do_resume: sandbox %s not found in DB", sandbox_id)
            return JSONResponse({"error": "sandbox not found"}, status_code=404)

        # Auto-restart sandbox if the process died (idle reaper or crash)
        try:
            url = await _ensure_sandbox_alive(
                sandbox_id, sandbox_record,
                agent_type=agent_record.config.agent_type,
                dockerfile=agent_record.config.dockerfile,
            )
        except RuntimeError as e:
            log.error("_do_resume: _ensure_sandbox_alive failed for sandbox %s: %s", sandbox_id, e)
            return JSONResponse({"error": str(e)}, status_code=502)

        cwd = agent_record.config.cwd or "/tmp"

        acp_session_id = str(uuid.uuid4())

        client = SandboxAgentClient(url)
        load_rpc_id = str(uuid.uuid4())
        try:
            async def _init_and_load():
                await _apply_config_and_initialize(client, agent_record.config, acp_session_id, cwd)
                await client._send_rpc(acp_session_id, "session/load", {
                    "sessionId": inner_session_id,
                    "cwd": cwd,
                    "mcpServers": _mcp_dict_to_acp_array(agent_record.config.mcp_servers) if agent_record.config.mcp_servers else [],
                }, rpc_id=load_rpc_id)
                client.set_inner_session_id(acp_session_id, inner_session_id)
                # initialize() set bypassPermissions on the fresh inner session
                # from session/new. After session/load swaps to the persistent
                # inner session, we need to re-apply the mode so the resumed
                # conversation also runs in bypassPermissions.
                try:
                    if await client.has_capability(agent_record.config.agent_type or "claude", "permissions"):
                        await client.set_mode(acp_session_id, "bypassPermissions")
                except Exception as e:
                    log.warning("_do_resume: failed to set bypassPermissions after session/load: %s", e)
            await asyncio.wait_for(_init_and_load(), timeout=120)
        except Exception as e:
            log.error("_do_resume: session/load failed for sandbox %s inner %s: %s: %s",
                      sandbox_id, inner_session_id, type(e).__name__, e)
            try:
                await client.close_session(acp_session_id)
            except Exception:
                pass
            try:
                await client.aclose()
            except Exception:
                pass
            return JSONResponse({"error": f"Failed to resume session: {e}"}, status_code=502)

        last_event_id = await _find_last_replay_event_id(client.base_url, acp_session_id, load_rpc_id)

        # Shut down old session before replacing (atomic swap under sandbox lock
        # to prevent the idle reaper from popping the new session via stale refs)
        async with _get_sandbox_lock(sandbox_id):
            old_state = SESSIONS.get(session_id)
            if old_state:
                await _shutdown_session_state(old_state, remove=False, background_close=True)
            new_state = SessionState(
                session_id=session_id,
                agent_id=agent_id,
                sandbox_id=sandbox_id,
                acp_session_id=acp_session_id,
                inner_session_id=inner_session_id,
                client=client,
                last_event_id=last_event_id,
            )
            SESSIONS[session_id] = new_state
            _start_sse_reader(new_state)
        await upsert_session(session_id, agent_id, sandbox_id, inner_session_id)
        return {
            "session_id": session_id, "agent_id": agent_id,
            "sandbox_id": sandbox_id, "inner_session_id": inner_session_id,
            "status": "resumed",
        }


async def get_or_recover_session(session_id: str) -> SessionState:
    """Get a live session, recovering from DB if it was reaped.

    This is the single entry point for all session endpoints. Checks
    in-memory SESSIONS first, then falls back to DB lookup + _do_resume
    to restart the sandbox and reload conversation state.

    Raises HTTPException(404) if the session doesn't exist anywhere.
    Raises HTTPException(502) if recovery fails.
    """
    state = SESSIONS.get(session_id)
    if state and not state.shutdown.is_set():
        return state

    rec = await get_session(session_id)
    if rec is None:
        log.warning("session %s not found in DB", session_id)
        raise HTTPException(status_code=404, detail="session not found")

    agent_id = rec.get("agent_id")
    inner_session_id = rec.get("inner_session_id")
    sandbox_id = rec.get("sandbox_id")
    if not agent_id or not inner_session_id or not sandbox_id:
        log.warning("session %s record incomplete: %s", session_id, rec)
        raise HTTPException(status_code=404, detail="session record incomplete")

    log.info("recovering session %s (sandbox=%s, inner=%s)",
             session_id, sandbox_id, inner_session_id)
    result = await _do_resume(
        sandbox_id=sandbox_id,
        agent_id=agent_id,
        inner_session_id=inner_session_id,
        client_session_id=session_id,
    )

    if isinstance(result, JSONResponse):
        body = bytes(result.body).decode("utf-8", errors="replace")
        log.error("session %s recovery failed: status=%d body=%s",
                  session_id, result.status_code, body[:500])
        raise HTTPException(
            status_code=502,
            detail=f"session recovery failed: {body[:200]}",
        )

    recovered = SESSIONS.get(session_id)
    if recovered is None:
        log.error("session %s recovery succeeded but SESSIONS lookup returned None", session_id)
        raise HTTPException(status_code=502, detail="session recovery failed: state missing after resume")

    log.info("session %s recovered successfully", session_id)
    return recovered


# ---------------------------------------------------------------------------
# Sandbox filesystem / exec / config proxy endpoints
# ---------------------------------------------------------------------------

async def _proxy(coro):
    """Run a sandbox-agent call with standard 502 error handling."""
    try:
        return await coro
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=502)


@app.get("/sandboxes/{sandbox_id}/fs")
async def sandbox_list_dir(sandbox_id: str, path: str = "/"):
    try:
        client = await get_sandbox_client(sandbox_id)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=502)
    return await _proxy(client.list_dir(path))


@app.get("/sandboxes/{sandbox_id}/fs/file")
async def sandbox_read_file(sandbox_id: str, path: str):
    try:
        client = await get_sandbox_client(sandbox_id)
        content = await client.read_file(path)
        return PlainTextResponse(content)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=502)


@app.put("/sandboxes/{sandbox_id}/fs/file")
async def sandbox_write_file(sandbox_id: str, path: str, request: Request):
    try:
        client = await get_sandbox_client(sandbox_id)
        body = await request.body()
        await client.write_file(path, body.decode("utf-8", errors="replace"))
        return {"status": "ok"}
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=502)


@app.post("/sandboxes/{sandbox_id}/exec")
async def sandbox_run_command(sandbox_id: str, request: Request):
    try:
        data = await request.json()
    except Exception as e:
        return JSONResponse({"error": f"Invalid JSON body: {e}"}, status_code=400)
    command = data.get("command")
    if not command:
        return JSONResponse({"error": "Missing required field: command"}, status_code=400)
    try:
        client = await get_sandbox_client(sandbox_id)
        result = await client.run_command(command, args=data.get("args"), cwd=data.get("cwd"))
        return result
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=502)


@app.post("/sandboxes/{sandbox_id}/processes")
async def sandbox_start_process(sandbox_id: str, request: Request):
    try:
        data = await request.json()
    except Exception as e:
        return JSONResponse({"error": f"Invalid JSON body: {e}"}, status_code=400)
    command = data.get("command")
    if not command:
        return JSONResponse({"error": "Missing required field: command"}, status_code=400)
    try:
        client = await get_sandbox_client(sandbox_id)
        result = await client.start_process(command, args=data.get("args"), cwd=data.get("cwd"))
        return result
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=502)


@app.get("/sandboxes/{sandbox_id}/processes")
async def sandbox_list_processes(sandbox_id: str):
    try:
        client = await get_sandbox_client(sandbox_id)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=502)
    return await _proxy(client.list_processes())


@app.post("/sandboxes/{sandbox_id}/processes/{process_id}/stop")
async def sandbox_stop_process(sandbox_id: str, process_id: str):
    try:
        client = await get_sandbox_client(sandbox_id)
        await client.stop_process(process_id)
        return {"status": "ok"}
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=502)


@app.get("/sandboxes/{sandbox_id}/processes/{process_id}")
async def sandbox_process_info(sandbox_id: str, process_id: str):
    """Get detailed process info."""
    try:
        client = await get_sandbox_client(sandbox_id)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=502)
    return await _proxy(client.get_process_info(process_id))


@app.post("/sandboxes/{sandbox_id}/processes/{process_id}/input")
async def sandbox_send_input(sandbox_id: str, process_id: str, request: Request):
    """Send stdin input to a process."""
    try:
        client = await get_sandbox_client(sandbox_id)
        body = await request.body()
        await client.send_process_input(process_id, body.decode())
        return {"status": "ok"}
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=502)


@app.post("/sandboxes/{sandbox_id}/processes/{process_id}/kill")
async def sandbox_kill_process(sandbox_id: str, process_id: str):
    """Kill a process (SIGKILL)."""
    try:
        client = await get_sandbox_client(sandbox_id)
        await client.kill_process(process_id)
        return {"status": "ok"}
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=502)


@app.delete("/sandboxes/{sandbox_id}/processes/{process_id}")
async def sandbox_delete_process(sandbox_id: str, process_id: str):
    """Delete a process record."""
    try:
        client = await get_sandbox_client(sandbox_id)
        await client.delete_process(process_id)
        return {"status": "ok"}
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=502)


@app.get("/sandboxes/{sandbox_id}/processes/{process_id}/logs")
async def sandbox_process_logs(sandbox_id: str, process_id: str):
    try:
        client = await get_sandbox_client(sandbox_id)
        logs = await client.get_process_logs(process_id)
        return PlainTextResponse(logs)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=502)


@app.get("/sandboxes/{sandbox_id}/screenshot")
async def sandbox_screenshot(sandbox_id: str):
    try:
        client = await get_sandbox_client(sandbox_id)
        png_bytes = await client.screenshot()
        return Response(content=png_bytes, media_type="image/png")
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=502)


@app.get("/sandboxes/{sandbox_id}/desktop/screenshot")
async def sandbox_desktop_screenshot(sandbox_id: str, region: str | None = None):
    try:
        client = await get_sandbox_client(sandbox_id)
        region_dict = json.loads(region) if region else None
        png_bytes = await client.screenshot(region=region_dict)
        return Response(content=png_bytes, media_type="image/png")
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=502)


@app.post("/sandboxes/{sandbox_id}/desktop/click")
async def sandbox_mouse_click(sandbox_id: str, request: Request):
    try:
        client = await get_sandbox_client(sandbox_id)
        data = await request.json()
        await client.mouse_click(data.get("x", 0), data.get("y", 0), data.get("button", "left"))
        return {"status": "ok"}
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=502)


@app.post("/sandboxes/{sandbox_id}/desktop/type")
async def sandbox_keyboard_type(sandbox_id: str, request: Request):
    try:
        client = await get_sandbox_client(sandbox_id)
        data = await request.json()
        await client.keyboard_type(data.get("text", ""))
        return {"status": "ok"}
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=502)


@app.post("/sandboxes/{sandbox_id}/desktop/press")
async def sandbox_keyboard_press(sandbox_id: str, request: Request):
    try:
        client = await get_sandbox_client(sandbox_id)
        data = await request.json()
        await client.keyboard_press(data.get("key", ""))
        return {"status": "ok"}
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=502)


@app.post("/sandboxes/{sandbox_id}/desktop/mouse/down")
async def sandbox_mouse_down(sandbox_id: str, request: Request):
    try:
        client = await get_sandbox_client(sandbox_id)
        data = await request.json()
        await client.mouse_down(data.get("x", 0), data.get("y", 0), data.get("button", "left"))
        return {"status": "ok"}
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=502)

@app.post("/sandboxes/{sandbox_id}/desktop/mouse/up")
async def sandbox_mouse_up(sandbox_id: str, request: Request):
    try:
        client = await get_sandbox_client(sandbox_id)
        data = await request.json()
        await client.mouse_up(data.get("x", 0), data.get("y", 0), data.get("button", "left"))
        return {"status": "ok"}
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=502)

@app.post("/sandboxes/{sandbox_id}/desktop/mouse/move")
async def sandbox_mouse_move(sandbox_id: str, request: Request):
    try:
        client = await get_sandbox_client(sandbox_id)
        data = await request.json()
        await client.mouse_move(data.get("x", 0), data.get("y", 0))
        return {"status": "ok"}
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=502)

@app.post("/sandboxes/{sandbox_id}/desktop/keyboard/down")
async def sandbox_key_down(sandbox_id: str, request: Request):
    try:
        client = await get_sandbox_client(sandbox_id)
        data = await request.json()
        await client.key_down(data.get("key", ""))
        return {"status": "ok"}
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=502)

@app.post("/sandboxes/{sandbox_id}/desktop/keyboard/up")
async def sandbox_key_up(sandbox_id: str, request: Request):
    try:
        client = await get_sandbox_client(sandbox_id)
        data = await request.json()
        await client.key_up(data.get("key", ""))
        return {"status": "ok"}
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=502)


@app.get("/sandboxes/{sandbox_id}/desktop/clipboard")
async def sandbox_clipboard_read(sandbox_id: str):
    """Read clipboard content."""
    try:
        client = await get_sandbox_client(sandbox_id)
        text = await client.clipboard_read()
        return PlainTextResponse(text)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=502)

@app.post("/sandboxes/{sandbox_id}/desktop/clipboard")
async def sandbox_clipboard_write(sandbox_id: str, request: Request):
    """Write to clipboard."""
    try:
        client = await get_sandbox_client(sandbox_id)
        data = await request.json()
        await client.clipboard_write(data.get("text", ""))
        return {"status": "ok"}
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=502)


@app.get("/sandboxes/{sandbox_id}/desktop/windows")
async def sandbox_list_windows(sandbox_id: str):
    try:
        client = await get_sandbox_client(sandbox_id)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=502)
    return await _proxy(client.list_windows())

@app.post("/sandboxes/{sandbox_id}/desktop/windows/{window_id}/focus")
async def sandbox_focus_window(sandbox_id: str, window_id: str):
    try:
        client = await get_sandbox_client(sandbox_id)
        await client.focus_window(window_id)
        return {"status": "ok"}
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=502)

@app.post("/sandboxes/{sandbox_id}/desktop/start")
async def sandbox_desktop_start(sandbox_id: str):
    try:
        client = await get_sandbox_client(sandbox_id)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=502)
    return await _proxy(client.desktop_start())

@app.post("/sandboxes/{sandbox_id}/desktop/stop")
async def sandbox_desktop_stop(sandbox_id: str):
    try:
        client = await get_sandbox_client(sandbox_id)
        await client.desktop_stop()
        return {"status": "ok"}
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=502)

@app.get("/sandboxes/{sandbox_id}/desktop/status")
async def sandbox_desktop_status(sandbox_id: str):
    try:
        client = await get_sandbox_client(sandbox_id)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=502)
    return await _proxy(client.desktop_status())

@app.get("/sandboxes/{sandbox_id}/desktop/recordings")
async def sandbox_list_recordings(sandbox_id: str):
    try:
        client = await get_sandbox_client(sandbox_id)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=502)
    return await _proxy(client.list_recordings())


@app.post("/sandboxes/{sandbox_id}/desktop/drag")
async def sandbox_drag_mouse(sandbox_id: str, request: Request):
    try:
        client = await get_sandbox_client(sandbox_id)
        data = await request.json()
        await client.drag_mouse(data.get("startX", 0), data.get("startY", 0), data.get("endX", 0), data.get("endY", 0), data.get("button", "left"))
        return {"status": "ok"}
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=502)


@app.post("/sandboxes/{sandbox_id}/desktop/scroll")
async def sandbox_scroll_mouse(sandbox_id: str, request: Request):
    try:
        client = await get_sandbox_client(sandbox_id)
        data = await request.json()
        await client.scroll_mouse(data.get("x", 0), data.get("y", 0), data.get("scrollX", 0), data.get("scrollY", 0))
        return {"status": "ok"}
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=502)


@app.post("/sandboxes/{sandbox_id}/desktop/launch")
async def sandbox_launch_app(sandbox_id: str, request: Request):
    try:
        client = await get_sandbox_client(sandbox_id)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=502)
    data = await request.json()
    return await _proxy(client.launch_app(data.get("appName", "")))


@app.post("/sandboxes/{sandbox_id}/desktop/recordings/start")
async def sandbox_start_recording(sandbox_id: str):
    try:
        client = await get_sandbox_client(sandbox_id)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=502)
    return await _proxy(client.start_recording())


@app.post("/sandboxes/{sandbox_id}/desktop/recordings/stop")
async def sandbox_stop_recording(sandbox_id: str):
    try:
        client = await get_sandbox_client(sandbox_id)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=502)
    return await _proxy(client.stop_recording())


@app.get("/sandboxes/{sandbox_id}/desktop/display")
async def sandbox_display_info(sandbox_id: str):
    """Get display/resolution info from the sandbox desktop."""
    try:
        client = await get_sandbox_client(sandbox_id)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=502)
    return await _proxy(client.get_display_info())


@app.get("/sandboxes/{sandbox_id}/processes/config")
async def sandbox_process_config(sandbox_id: str):
    """Get process limits configuration."""
    try:
        client = await get_sandbox_client(sandbox_id)
        resp = await client._client.get("/v1/processes/config")
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=502)


@app.post("/sandboxes/{sandbox_id}/fs/upload")
async def sandbox_upload_files(sandbox_id: str, request: Request, path: str = "/"):
    try:
        client = await get_sandbox_client(sandbox_id)
        body = await request.body()
        await client.upload_files_raw(path, body)
        return {"status": "ok"}
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=502)


@app.delete("/sandboxes/{sandbox_id}/fs/file")
async def sandbox_delete_path(sandbox_id: str, path: str, recursive: bool = False):
    try:
        client = await get_sandbox_client(sandbox_id)
        await client.delete_path(path, recursive=recursive)
        return {"status": "ok"}
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=502)


@app.post("/sandboxes/{sandbox_id}/fs/mkdir")
async def sandbox_mkdir(sandbox_id: str, path: str):
    try:
        client = await get_sandbox_client(sandbox_id)
        await client.mkdir(path)
        return {"status": "ok"}
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=502)


@app.post("/sandboxes/{sandbox_id}/fs/move")
async def sandbox_move_file(sandbox_id: str, request: Request):
    try:
        client = await get_sandbox_client(sandbox_id)
        data = await request.json()
        src = data.get("source")
        dst = data.get("destination")
        if not src or not dst:
            return JSONResponse({"error": "source and destination required"}, status_code=400)
        await client.move_file(src, dst)
        return {"status": "ok"}
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=502)


@app.get("/sandboxes/{sandbox_id}/fs/stat")
async def sandbox_stat(sandbox_id: str, path: str):
    try:
        client = await get_sandbox_client(sandbox_id)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=502)
    return await _proxy(client.stat(path))


@app.get("/sandboxes/{sandbox_id}/health")
async def sandbox_health(sandbox_id: str):
    """Check if the sandbox-agent process is alive."""
    try:
        client = await get_sandbox_client(sandbox_id)
        result = await client.health()
        return result
    except Exception as e:
        return JSONResponse({"error": str(e), "healthy": False}, status_code=502)


@app.get("/sandboxes/{sandbox_id}/agents")
async def sandbox_list_agents(sandbox_id: str):
    """List agents available in the sandbox."""
    try:
        client = await get_sandbox_client(sandbox_id)
        agents = await client.list_agents()
        return [{"id": a.id, "installed": a.installed,
                 "credentials_available": a.credentials_available,
                 "capabilities": a.capabilities} for a in agents]
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=502)


@app.get("/sandboxes/{sandbox_id}/capabilities")
async def sandbox_agent_capabilities(sandbox_id: str, agent_type: str = Query(default="claude")):
    """Get capabilities for a specific agent type in this sandbox."""
    try:
        client = await get_sandbox_client(sandbox_id)
        info = await client.get_agent_info(agent_type)
        return {"agent_type": agent_type, "capabilities": info.get("capabilities", {})}
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=502)


@app.post("/sandboxes/{sandbox_id}/install-agent")
async def sandbox_install_agent(sandbox_id: str, request: Request):
    """Install an agent type in the sandbox (e.g. amp, pi, cursor)."""
    try:
        client = await get_sandbox_client(sandbox_id)
        data = await request.json()
        agent_type = data.get("agent_type", "claude")
        result = await client.install_agent(agent_type)
        return result
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=502)


@app.post("/sandboxes/{sandbox_id}/deploy-skill")
async def sandbox_deploy_skill(sandbox_id: str, request: Request):
    """Deploy a skill file to the sandbox."""
    try:
        client = await get_sandbox_client(sandbox_id)
        data = await request.json()
        name = data.get("name")
        content = data.get("content")
        cwd = data.get("cwd", "/tmp")
        if not name or not content:
            return JSONResponse({"error": "name and content required"}, status_code=400)
        path = f"{cwd}/.claude/commands/{name}.md"
        await client.write_file(path, content)
        return {"status": "ok", "path": path}
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=502)


@app.get("/sessions")
async def list_sessions_route():
    """List all active in-memory sessions with status."""
    now = time.time()
    return [
        {
            "session_id": s.session_id,
            "agent_id": s.agent_id,
            "sandbox_id": s.sandbox_id,
            "idle_seconds": round(now - (s.turn_completed_at or s.last_activity), 1),
            "shutdown_requested": s.shutdown.is_set(),
        }
        for s in SESSIONS.values()
    ]


@app.get("/sessions/{session_id}/status")
async def session_status(session_id: str):
    """Get session runtime status including last activity timestamp."""
    state = SESSIONS.get(session_id)
    if state is None:
        return JSONResponse({"error": "session not found"}, status_code=404)
    now = time.time()
    return {
        "session_id": state.session_id,
        "agent_id": state.agent_id,
        "sandbox_id": state.sandbox_id,
        "inner_session_id": state.inner_session_id,
        "agent_busy": state.agent_busy,
        "last_activity": state.last_activity,
        "idle_seconds": round(now - (state.turn_completed_at or state.last_activity), 1),
        "has_client": state.client is not None,
        "shutdown_requested": state.shutdown.is_set(),
        "pending_errors": 0,
    }


# ---------------------------------------------------------------------------
# Session log read endpoints
# ---------------------------------------------------------------------------

@app.get("/sessions/{session_id}/log")
async def get_session_log_route(session_id: str, limit: int = Query(default=500)):
    entries = await get_session_log(session_id, limit=limit)
    return [{"id": e.id, "event_type": e.event_type, "payload": e.payload,
             "created_at": e.created_at} for e in entries]


@app.get("/agents/{agent_id}/log")
async def get_agent_log_route(agent_id: str, limit: int = Query(default=100)):
    entries = await get_agent_log(agent_id, limit=limit)
    return [{"id": e.id, "session_id": e.session_id, "sandbox_id": e.sandbox_id,
             "event_type": e.event_type, "payload": e.payload,
             "created_at": e.created_at} for e in entries]


# ---------------------------------------------------------------------------
# Session endpoints (new — keyed by session_id, use get_or_recover_session)
# ---------------------------------------------------------------------------

@app.post("/sessions/{session_id}/resume")
async def session_resume(session_id: str):
    """Resume a session by ID. Auto-recovers sandbox if stopped."""
    try:
        state = await get_or_recover_session(session_id)
    except HTTPException as e:
        return JSONResponse({"error": e.detail}, status_code=e.status_code)
    return {
        "session_id": state.session_id,
        "agent_id": state.agent_id,
        "sandbox_id": state.sandbox_id,
        "inner_session_id": state.inner_session_id,
        "status": "resumed",
    }


@app.post("/sessions/quick")
async def sessions_quick_create(request: Request):
    """Create agent + provision sandbox + connect in one call.

    Returns {agent_id, sandbox_id, session_id, connected: true}.
    """
    data = await request.json()
    provider = data.get("provider", "local")
    agent_type = data.get("agent_type", "claude")
    name = data.get("name")
    config_data = data.get("config", {})
    _merge_top_level_config(data, config_data)
    cwd = config_data.get("cwd", data.get("cwd", "/tmp"))
    dockerfile = _materialize_dockerfile(config_data)

    agent_id = str(uuid.uuid4())
    config = AgentConfig.from_dict({**config_data, "agent_type": agent_type, "cwd": cwd})
    await upsert_agent(AgentRecord(id=agent_id, name=name, config=config))

    sandbox_id = str(uuid.uuid4())
    try:
        instance = await create_instance(provider, agent_type, dockerfile=dockerfile)
    except Exception as e:
        await delete_agent(agent_id)
        return JSONResponse({"error": f"Provider '{provider}' failed: {e}"}, status_code=502)

    sandbox_ref = _derive_sandbox_ref(instance, provider, sandbox_id)

    _INSTANCES[sandbox_id] = instance
    await upsert_sandbox(SandboxRecord(id=sandbox_id, provider=provider, sandbox_ref=sandbox_ref, status=STATUS_RUNNING))

    url = instance.url
    acp_session_id = str(uuid.uuid4())
    session_id = str(uuid.uuid4())

    client = SandboxAgentClient(url)
    try:
        await _apply_config_and_initialize(client, config, acp_session_id, cwd)
    except Exception as e:
        try:
            await client.aclose()
        except Exception:
            pass
        await delete_agent(agent_id)
        await delete_sandbox(sandbox_id)
        _INSTANCES.pop(sandbox_id, None)
        try:
            await destroy_instance(instance)
        except Exception as de:
            log.warning("sessions_quick_create cleanup: destroy_instance failed: %s", de)
        return JSONResponse({"error": f"Failed to connect to sandbox-agent: {e}"}, status_code=502)

    inner_session_id = client.get_inner_session_id(acp_session_id)
    state = SessionState(
        session_id=session_id,
        agent_id=agent_id,
        sandbox_id=sandbox_id,
        acp_session_id=acp_session_id,
        inner_session_id=inner_session_id,
        client=client,
    )
    SESSIONS[session_id] = state
    _start_sse_reader(state)
    await upsert_session(session_id, agent_id, sandbox_id, inner_session_id)

    return {
        "agent_id": agent_id,
        "sandbox_id": sandbox_id,
        "session_id": session_id,
        "inner_session_id": inner_session_id,
        "connected": True,
    }


@app.post("/sessions/{session_id}/message")
async def post_session_message(session_id: str, request: Request):
    """Send a prompt to a session. Recovers reaped sessions automatically."""
    data = await request.json()
    message = data.get("message")
    if not message:
        return JSONResponse({"error": "message required"}, status_code=400)

    try:
        state = await get_or_recover_session(session_id)
    except HTTPException as exc:
        return JSONResponse({"error": exc.detail}, status_code=exc.status_code)

    if not state.client or not state.acp_session_id:
        return JSONResponse({"error": "session not connected"}, status_code=409)

    if state.agent_busy:
        return JSONResponse({"error": "agent is busy processing a previous message"}, status_code=409)

    state.last_activity = time.time()
    rpc_id = str(uuid.uuid4())

    await log_event(
        session_id=session_id, agent_id=state.agent_id, sandbox_id=state.sandbox_id,
        event_type=EVT_USER_MESSAGE, payload={"text": message},
    )

    async def _run_prompt():
        try:
            await state.client.prompt(state.acp_session_id, message, rpc_id=rpc_id)
            state.resume_buffering()
        except Exception as e:
            state.resume_buffering()
            log.error("prompt failed for session %s: %s", session_id, e)
            state.errors.append(str(e))
            _mark_turn_finished(state)
            error_payload = json.dumps({
                "jsonrpc": "2.0", "id": rpc_id,
                "error": {"code": -32000, "message": str(e)[:200]},
            })
            if not state.shutdown.is_set():
                state.broadcast(f"data: {error_payload}\n\n")
            await log_event(
                session_id=session_id, agent_id=state.agent_id, sandbox_id=state.sandbox_id,
                event_type=EVT_ERROR, payload={"message": str(e)[:500]},
            )
        finally:
            state.agent_busy = False
            state.turn_completed_at = time.time()

    state.new_turn()
    state.agent_busy = True
    asyncio.create_task(_run_prompt())
    return JSONResponse({"rpc_id": rpc_id, "status": "ok"})


@app.get("/sessions/{session_id}/events")
async def session_events(session_id: str):
    """SSE stream for a session. Recovers reaped sessions automatically."""
    try:
        state = await get_or_recover_session(session_id)
    except HTTPException as exc:
        return JSONResponse({"error": exc.detail}, status_code=exc.status_code)

    if not state.client or not state.acp_session_id:
        return JSONResponse({"error": "session not connected"}, status_code=409)

    shutdown = state.shutdown

    async def _proxy_stream():
        heartbeat_interval = int(os.environ.get("SSE_HEARTBEAT_INTERVAL", "30"))

        # Restart persistent reader if it died (e.g., sandbox-agent restart)
        if not state._reader_alive:
            _start_sse_reader(state)

        # Subscribe this client — each client gets its own queue via fan-out.
        # Logging and state updates happen in the reader (single writer),
        # so the proxy only forwards raw SSE blocks to the HTTP client.
        my_q = state.subscribe()

        async def _heartbeat_loop():
            try:
                while True:
                    await asyncio.sleep(heartbeat_interval)
                    try:
                        my_q.put_nowait(None)
                    except asyncio.QueueFull:
                        pass
            except asyncio.CancelledError:
                pass

        heartbeat_task = asyncio.create_task(_heartbeat_loop())
        try:
            buffer = ""
            while True:
                item = await my_q.get()
                if item is _SSE_SENTINEL:
                    return
                if shutdown.is_set():
                    return
                if item is None:
                    yield ": heartbeat\n\n"
                    continue

                buffer += item
                while "\n\n" in buffer:
                    block, buffer = buffer.split("\n\n", 1)
                    yield block + "\n\n"
        except asyncio.CancelledError:
            pass
        except Exception as e:
            log.warning("SSE proxy error for session %s: %s", session_id, e)
        finally:
            await _cancel_task(heartbeat_task)
            state.unsubscribe(my_q)

    return StreamingResponse(
        _proxy_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )


@app.post("/sessions/{session_id}/cancel")
async def session_cancel(session_id: str):
    """Cancel the currently running prompt for a session."""
    try:
        state = await get_or_recover_session(session_id)
    except HTTPException as exc:
        return JSONResponse({"error": exc.detail}, status_code=exc.status_code)

    if not state.acp_session_id:
        return JSONResponse({"error": "no active session"}, status_code=409)
    await state.client.cancel_prompt(state.acp_session_id)
    return {"status": "ok"}


@app.post("/sessions/{session_id}/config")
async def session_set_config(session_id: str, request: Request):
    """Set mode/model/thought_level for a session."""
    try:
        state = await get_or_recover_session(session_id)
    except HTTPException as exc:
        return JSONResponse({"error": exc.detail}, status_code=exc.status_code)

    if not state.acp_session_id:
        return JSONResponse({"error": "no active session"}, status_code=409)
    try:
        data = await request.json()
        if "mode" in data:
            await state.client.set_mode(state.acp_session_id, data["mode"])
        if "model" in data:
            await state.client.set_model(state.acp_session_id, data["model"])
        if "thought_level" in data:
            await state.client.set_thought_level(state.acp_session_id, data["thought_level"])
        return {"status": "ok"}
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=502)


# ---------------------------------------------------------------------------
# UI + Hive proxy (unchanged)
# ---------------------------------------------------------------------------

@app.get("/chat")
async def chat_ui():
    global _CHAT_HTML
    if _CHAT_HTML is None:
        _CHAT_HTML = (_UI_DIR / "chat.html").read_text()
    return HTMLResponse(_CHAT_HTML)


@app.get("/kanban")
async def kanban_ui():
    global _KANBAN_HTML
    if _KANBAN_HTML is None:
        _KANBAN_HTML = (_UI_DIR / "kanban.html").read_text()
    return HTMLResponse(_KANBAN_HTML)


async def _run_hive_command(*args: str, task: str) -> JSONResponse:
    """Run a hive CLI command and return JSON response."""
    env = {**os.environ, "HIVE_TASK": task}
    proc = await asyncio.create_subprocess_exec(
        "hive", *args, "--json",
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE, env=env,
    )
    stdout, stderr = await proc.communicate()
    if proc.returncode != 0:
        return JSONResponse({"error": stderr.decode().strip()}, status_code=502)
    try:
        return JSONResponse(json.loads(stdout.decode()))
    except json.JSONDecodeError:
        return JSONResponse({"error": "Invalid JSON from hive CLI", "raw": stdout.decode()[:500]}, status_code=502)


@app.get("/hive/items")
async def hive_items(task: str = "hello-world"):
    return await _run_hive_command("item", "list", task=task)


@app.get("/hive/items/{item_id}")
async def hive_item_detail(item_id: str, task: str = "hello-world"):
    return await _run_hive_command("item", "view", item_id, task=task)
