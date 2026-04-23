"""REST API server — agent/sandbox/session orchestration layer.

Run: uvicorn src.api.server:app --port 7778
"""

import asyncio
import base64
import json
import logging
import os
import re
import shlex
import tempfile
import time
import traceback
import uuid
from contextlib import asynccontextmanager
from pathlib import Path

import httpx
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from fastapi.responses import (
    JSONResponse,
    PlainTextResponse,
    Response,
    StreamingResponse,
)

from .acp_client import AcpClient, _mcp_dict_to_acp_array
from .db import (
    add_supervisor_agent_type,
    close_pool,
    delete_agent,
    delete_sandbox,
    delete_volume,
    get_agent,
    get_any_session_for_sandbox,
    get_db,
    get_sandbox,
    get_session,
    get_session_log,
    get_volume,
    get_volume_by_name,
    init_db,
    init_pool,
    list_agents,
    list_sandboxes,
    list_volumes,
    log_event,
    update_session_env,
    update_session_secrets,
    set_session_current_sandbox,
    upsert_agent,
    upsert_sandbox,
    upsert_session,
    upsert_volume,
)
from .models import (
    _KICK_SENTINEL,
    EVT_ASSISTANT_MESSAGE,
    EVT_ERROR,
    EVT_REASONING,
    EVT_TOOL_CALL,
    EVT_TOOL_RESULT,
    EVT_USAGE,
    EVT_USER_MESSAGE,
    STATUS_RUNNING,
    STATUS_STOPPED,
    AgentConfig,
    AgentRecord,
    PendingPrompt,
    SandboxRecord,
    SessionState,
    VolumeRecord,
)
from . import providers as _providers_mod
from .providers import (
    PORT_BASED_PROVIDERS,
    ProviderInstance,
    SandboxMissingError,
    allocate_sandbox_port,
    create_instance,
    default_cwd_for_provider,
    destroy_instance,
    exec_in_instance,
    free_sandbox_port,
    kill_supervisor_in_sandbox,
    stop_instance,
)
from .providers._shared import _safe_path as _shared_safe_path
from .redact import redact_secrets
from .sse import (
    UT_COMMANDS_UPDATE,
    UT_MESSAGE_CHUNK,
    UT_MESSAGE_DELTA,
    UT_THOUGHT_CHUNK,
    UT_TOOL_CALL,
    UT_TOOL_CALL_UPDATE,
    UT_TOOL_STARTED,
    UT_USAGE_UPDATE,
    UT_USAGE_UPDATED,
    classify_message_content,
    extract_tool_call_id,
    extract_tool_name,
    extract_tool_response,
    parse_acp_payload,
    parse_sse_data,
)

log = logging.getLogger(__name__)


def _configure_logging() -> None:
    """Set up logging. Called once at server startup, not on import."""
    level = os.environ.get("LOG_LEVEL", "INFO")
    logging.basicConfig(
        level=level, format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        force=True,
    )
    logging.getLogger("api").setLevel(level)
    logging.getLogger("httpx").setLevel(logging.WARNING)


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


# Strong references to fire-and-forget background tasks so Python's GC can't
# collect them mid-flight ("Task was destroyed but it is pending!" bug — the
# event loop holds only a weak ref to tasks, so a caller that does
# ``asyncio.create_task(coro())`` without keeping the returned Task alive
# risks silent cancellation). Tasks self-discard from the set on completion.
_BG_TASKS: set[asyncio.Task] = set()


def _spawn_bg(coro) -> asyncio.Task:
    """Create a background task and hold a strong reference to it."""
    task = asyncio.create_task(coro)
    _BG_TASKS.add(task)
    task.add_done_callback(_BG_TASKS.discard)
    return task


async def _cancel_task(task) -> None:
    """Cancel an asyncio task and await its completion so cleanup code runs."""
    if task is None or task.done():
        return
    task.cancel()
    try:
        await task
    except (asyncio.CancelledError, Exception):
        pass


async def _close_session_gracefully(state: "SessionState") -> None:
    """Close the HTTP connection to the ACP supervisor.

    We just drop the connection — the supervisor detects the disconnect
    and cleans up child processes. The old DELETE /v1/acp/{id} approach
    hangs because the ACP process blocks waiting for the agent to shut down.
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
    force: bool = False,
) -> None:
    """Close a runtime session and optionally remove it from the active registry."""
    state.shutdown.set()
    # Re-check: new work may have arrived between the caller's idle check and here.
    # Skip this guard when force=True (e.g. ensure_runtime rebuilding a dead supervisor).
    if not force and (
        state.active_rpc_id is not None
        or state.pending_prompts
        or state._session_subscribers
    ):
        state.shutdown.clear()
        return
    idle_at = time.time() if mark_idle_at is None else mark_idle_at
    state.turn_completed_at = idle_at
    state.last_activity = idle_at
    state.active_rpc_id = None
    state.pending_prompts.clear()
    await _cancel_task(state._scheduler_task)
    await _cancel_task(state._reader_task)
    # Flush any pending DB log writes so turn_end / tool_result events that
    # were scheduled during the last prompt aren't silently dropped when the
    # session is reaped.  Bounded wait so a hung writer can't block shutdown.
    pending = state._log_chain
    if pending is not None and not pending.done():
        try:
            await asyncio.wait_for(pending, timeout=5)
        except (asyncio.CancelledError, asyncio.TimeoutError, Exception):
            pass
    await _close_session_gracefully(state)
    # Kill per-session supervisor if this session has its own. Only Daytona
    # runs per-session supervisors here; docker/local have one-supervisor-per-
    # sandbox handled by destroy_sandbox.
    if state.supervisor_port is not None:
        try:
            sb = await get_sandbox(state.sandbox_id)
            if sb and sb.provider == "daytona":
                from .providers.daytona import _get_daytona_client
                loop = asyncio.get_running_loop()
                sandbox = await loop.run_in_executor(
                    None, lambda: _get_daytona_client().get(sb.sandbox_ref)
                )
                await kill_supervisor_in_sandbox(sandbox, state.supervisor_port)
                free_sandbox_port(state.sandbox_id, state.supervisor_port)
        except Exception as e:
            log.warning("failed to kill supervisor port %d for session %s: %s",
                        state.supervisor_port, state.session_id, e)
    if remove and SESSIONS.get(state.session_id) is state:
        SESSIONS.pop(state.session_id, None)
        _session_locks.pop(state.session_id, None)


def _mark_turn_finished(state: SessionState, at: float | None = None) -> float:
    """Mark a session turn as terminal so idle reaping can proceed."""
    finished_at = time.time() if at is None else at
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
REAPER_TICK_S = int(os.environ.get("SANDBOX_REAPER_TICK", "60"))
_SSE_MAX_IDLE_RETRIES = (
    5  # max consecutive reconnect attempts when idle before giving up
)


async def _reap_one_tick(now: float) -> None:
    """Single iteration of the reap loop, factored out for testability.

    Closes any session that is idle past IDLE_TIMEOUT_S with no active or
    pending prompts and no subscribers. When the last session on a sandbox
    is reaped, the sandbox itself is snapshotted + stopped via
    ``snapshot_and_stop`` so workspace state is durable on the volume
    before the provider call lands.
    """
    busy = sum(1 for s in SESSIONS.values() if s.agent_busy)
    readers = sum(1 for s in SESSIONS.values() if s._reader_alive)
    subs = sum(len(s._session_subscribers) for s in SESSIONS.values())
    log.info(
        "idle reaper tick: sessions=%d busy=%d readers=%d subs=%d instances=%d",
        len(SESSIONS), busy, readers, subs, len(_INSTANCES),
    )

    for state in list(SESSIONS.values()):
        if (
            state.active_rpc_id is not None
            or state.pending_prompts
            or state._session_subscribers
        ):
            continue
        idle_since = _session_idle_since(state)
        if now - idle_since < IDLE_TIMEOUT_S:
            continue
        log.info(
            "idle reaper: closing session %s (idle %.0fs)",
            state.session_id,
            now - idle_since,
        )
        sandbox_id = state.sandbox_id
        await _shutdown_session_state(state, remove=True, mark_idle_at=now)
        # If no other sessions use this sandbox, stop the supervisor.
        # For local/docker: kills the process (no filesystem to preserve).
        # For daytona: stops the workspace (filesystem preserved for resume).
        if not any(s.sandbox_id == sandbox_id for s in SESSIONS.values()):
            _sandbox_locks.pop(sandbox_id, None)
            instance = _INSTANCES.pop(sandbox_id, None)
            if instance:
                log.info(
                    "idle reaper: snapshot+stop sandbox %s (provider=%s)",
                    sandbox_id, instance.provider,
                )
                try:
                    rec = await get_sandbox(sandbox_id)
                    if rec is not None:
                        await snapshot_and_stop(rec, instance)
                    else:
                        # Row already gone (concurrent delete) — just stop.
                        await stop_instance(instance)
                    log.info("idle reaper: sandbox %s snapshotted + stopped",
                             sandbox_id)
                    if rec is not None and rec.status != STATUS_STOPPED:
                        rec.status = STATUS_STOPPED
                        await upsert_sandbox(rec)
                except Exception as e:
                    log.warning(
                        "reaper: failed to snapshot+stop sandbox %s: %s",
                        sandbox_id, e,
                    )


async def _idle_reaper():
    """Background task: close idle sessions that have been inactive too long."""
    while True:
        await asyncio.sleep(REAPER_TICK_S)
        await _reap_one_tick(time.time())


@asynccontextmanager
async def lifespan(app):
    _configure_logging()
    init_db()
    await init_pool()

    # Startup reconciliation: cross-reference live provider state with
    # DB sandbox rows so orphan containers/processes are reaped and
    # survivors are reattached to _INSTANCES. Run per-provider reconciles
    # in parallel so a slow provider doesn't serialize boot: in practice
    # only Docker does real work (~seconds of ``docker ps``+``docker
    # inspect``); daytona/local are no-ops, but future providers that
    # talk to remote APIs should not queue behind docker.
    async def _safe_reconcile(prov: str) -> None:
        try:
            await _providers_mod.reconcile_sandboxes(prov)
        except Exception as e:
            log.warning("startup reconcile for %s failed: %s", prov, e)

    await asyncio.gather(*[_safe_reconcile(p) for p in ("docker", "daytona", "local")])

    reaper = asyncio.create_task(_idle_reaper())
    yield
    await _cancel_task(reaper)
    # Parallel session shutdown
    await asyncio.gather(
        *[_shutdown_session_state(s, remove=False) for s in SESSIONS.values()],
        return_exceptions=True,
    )
    SESSIONS.clear()

    # Parallel instance teardown. Use snapshot_and_stop (not raw stop_instance)
    # so every sandbox's workspace is durable on the volume before the
    # process exits — server shutdown is otherwise indistinguishable from a
    # crash from the client's perspective, and we don't want to lose the
    # last-turn state just because the API was restarted. Falls back to
    # stop_instance if the sandbox row isn't readable (e.g. DB already torn
    # down in a weird shutdown ordering).
    async def _safe_stop(sid, inst):
        try:
            rec = await get_sandbox(sid)
            if rec is not None:
                await snapshot_and_stop(rec, inst)
            else:
                await stop_instance(inst)
        except Exception as e:
            log.warning("shutdown cleanup failed for %s: %s", sid, e)

    await asyncio.gather(
        *[_safe_stop(sid, inst) for sid, inst in list(_INSTANCES.items())]
    )
    _INSTANCES.clear()
    await close_pool()


app = FastAPI(title="Agent Orchestration API", lifespan=lifespan)


@app.exception_handler(Exception)
async def _log_unhandled(request: Request, exc: Exception):
    from fastapi.responses import JSONResponse
    from fastapi.exception_handlers import http_exception_handler
    from starlette.exceptions import HTTPException as StarletteHTTPException
    if isinstance(exc, StarletteHTTPException):
        if exc.status_code >= 500:
            log.error("HTTP %s %s → %s: %s", request.method, request.url.path, exc.status_code, exc.detail)
        return await http_exception_handler(request, exc)
    log.error("Unhandled exception in %s %s", request.method, request.url.path, exc_info=exc)
    return JSONResponse({"error": str(exc)}, status_code=500)


app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.exception_handler(HTTPException)
async def _http_exception_handler(request: Request, exc: HTTPException):
    """Uniform error shape: ``{"error": ...}`` for string details, pass-through for dict."""
    detail = exc.detail
    if isinstance(detail, dict):
        return JSONResponse(detail, status_code=exc.status_code)
    return JSONResponse({"error": detail}, status_code=exc.status_code, headers=exc.headers)


async def _json_body(request: Request) -> dict:
    """Parse JSON body and require it to be an object.

    Handlers that ``data = await request.json(); data.get(...)`` used to
    blow up with a 500 ``AttributeError: 'str' object has no attribute 'get'``
    when the client sent a JSON scalar/array instead of an object.  Route
    all such reads through this helper so the failure is a clean 400 with
    the canonical ``{"error": ...}`` shape.
    """
    try:
        data = await request.json()
    except Exception as e:
        raise HTTPException(400, f"invalid JSON body: {e}")
    if not isinstance(data, dict):
        raise HTTPException(400, "request body must be a JSON object")
    return data


# ---------------------------------------------------------------------------
# Lookup preamble helpers — raise HTTPException(404) on missing records so the
# caller never has to write ``if rec is None: return JSONResponse(...)``.
# ---------------------------------------------------------------------------

async def _require_agent(agent_id: str) -> AgentRecord:
    rec = await get_agent(agent_id)
    if rec is None:
        raise HTTPException(404, "agent not found")
    return rec


async def _require_sandbox(sandbox_id: str) -> SandboxRecord:
    rec = await get_sandbox(sandbox_id)
    if rec is None:
        raise HTTPException(404, "sandbox not found")
    return rec


async def _require_session_row(session_id: str) -> dict:
    rec = await get_session(session_id)
    if rec is None:
        raise HTTPException(404, f"Session {session_id} not found")
    return rec


# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------


@app.get("/health")
async def health():
    return {
        "status": "ok",
        "sessions": len(SESSIONS),
        "busy_sessions": sum(1 for s in SESSIONS.values() if s.agent_busy),
        "readers_alive": sum(1 for s in SESSIONS.values() if s._reader_alive),
        "instances": len(_INSTANCES),
    }


# ---------------------------------------------------------------------------
# Persistent SSE reader — connects to supervisor once per session
# ---------------------------------------------------------------------------

_SSE_SENTINEL = object()


def _maybe_auto_approve_permission(payload: dict | None, state: SessionState) -> None:
    """If the SSE payload is a session/request_permission, auto-approve it."""
    if not payload or payload.get("method") != "session/request_permission":
        return
    rpc_id = payload.get("id")
    if rpc_id is None:
        return
    options = payload.get("params", {}).get("options", [])
    # Pick "allow_always" > "allow_once" > first option.
    by_kind = {opt.get("kind"): opt.get("optionId") for opt in options}
    option_id = (
        by_kind.get("allow_always")
        or by_kind.get("allow_once")
        or (options[0].get("optionId") if options else None)
    )
    if not option_id:
        return

    async def _grant():
        try:
            resp = await state.client._client.post(
                f"/v1/acp/{state.acp_session_id}",
                json={"jsonrpc": "2.0", "id": rpc_id, "result": {"optionId": option_id}},
            )
            log.info("auto-approved permission for session %s (status=%d)",
                     state.session_id, resp.status_code)
        except Exception as e:
            log.warning("auto-approve permission failed for session %s: %s",
                        state.session_id, e)

    _spawn_bg(_grant())


def _on_sse_reader_death(state: SessionState) -> None:
    """Handle unexpected SSE reader death: set shutdown, cancel inflight tasks,
    kick subscribers.  No-op if shutdown was already set (intentional shutdown)."""
    if state.shutdown.is_set():
        return
    log.warning(
        "[SSE-READER] fatal upstream reader loss for session %s; shutting down session "
        "(busy=%s, pending=%d, session_subscribers=%d, rpc_subscribers=%d)",
        state.session_id,
        state.agent_busy,
        len(state.pending_prompts),
        len(state._session_subscribers),
        sum(len(qs) for qs in state._rpc_subscribers.values()),
    )
    # Shutdown set BEFORE cancelling so cancelled tasks' handlers short-circuit.
    state.shutdown.set()
    if state.agent_busy:
        log.warning(
            "[SSE-READER] reader died while agent_busy — clearing busy flag for session %s",
            state.session_id,
        )
        state.turn_completed_at = time.time()
    state.active_rpc_id = None
    state.pending_prompts.clear()
    # Kick all subscribers (session + RPC-scoped)
    kicked = list(state._session_subscribers)
    state._session_subscribers.clear()
    for rpc_qs in state._rpc_subscribers.values():
        kicked.extend(rpc_qs)
    state._rpc_subscribers.clear()
    for q in kicked:
        state._kick_subscriber(q)


def _sse_reader_disconnect_is_recoverable(state: SessionState) -> bool:
    """Idle upstream SSE disconnects are recoverable; active-turn loss is not."""
    return (
        not state.shutdown.is_set()
        and state.active_rpc_id is None
        and not state.pending_prompts
    )


def _broadcast_one_block(
    state: SessionState,
    block: str,
    payload: dict | None,
    text_parts: list[str],
    thinking_parts: list[str],
) -> None:
    """Shared per-block attribution + processing + dispatch. Called from
    both the HTTP and WebSocket reader paths.

    Tagging rules:
      - Terminal envelopes carry their own rpc id in `payload.id` — use it.
      - Non-terminals attribute to `active_rpc_id` (the currently executing prompt).
    """
    # Ignore upstream SSE comments/heartbeats — they keep the transport alive
    # but do not belong to any rpc and must not reset activity timestamps.
    if payload is None:
        return
    if (
        isinstance(payload, dict)
        and "id" in payload
        and isinstance(payload.get("result"), dict)
        and "stopReason" in payload["result"]
    ):
        tag = payload["id"]
    else:
        tag = state.active_rpc_id
    _process_sse_block(
        block, state, text_parts, thinking_parts, log_events=True, payload=payload
    )
    _maybe_auto_approve_permission(payload, state)
    # Drop empty blocks (spurious \n\n separators from the upstream stream)
    # and blocks we can't attribute to any rpc (session-setup chatter
    # arriving after our reader subscribes but before any prompt).
    if not block.strip() or tag is None:
        return
    state.dispatch(tag, (tag, block + "\n\n"))


def _start_sse_reader(state: SessionState) -> None:
    """Start a background task that reads SSE from the upstream /v1/acp/{id}
    endpoint and broadcasts chunks to subscriber queues. Called at session
    creation so events are captured before any prompt is sent.

    Works with the supervisor which exposes the POST+SSE JSON-RPC surface.
    """
    if state._reader_alive:
        log.info("[SSE-READER] start requested but reader already alive for session %s",
                 state.session_id)
        return
    state._reader_alive = True
    log.info(
        "[SSE-READER] starting upstream reader for session %s (acp_session=%s, base_url=%s, last_event_id=%s)",
        state.session_id, state.acp_session_id,
        getattr(state.client, "base_url", "?"), state.last_event_id or "-",
    )

    async def _reader():
        text_parts: list[str] = []
        thinking_parts: list[str] = []
        reconnect_delay_s = 1.0
        attempt = 0
        try:
            while not state.shutdown.is_set():
                reader_buffer = ""
                sse_http = None
                disconnect_reason = "upstream_eof"
                try:
                    attempt += 1
                    headers = {"Accept": "text/event-stream"}
                    if state.last_event_id:
                        headers["Last-Event-ID"] = state.last_event_id
                    log.info("[SSE-READER] connecting upstream stream for session %s "
                             "(attempt=%d, last_event_id=%s)",
                             state.session_id, attempt, state.last_event_id or "-")
                    sse_http = httpx.AsyncClient(
                        base_url=state.client.base_url, timeout=None, proxy=None,
                    )
                    async with sse_http.stream(
                        "GET", f"/v1/acp/{state.acp_session_id}", headers=headers,
                    ) as resp:
                        resp.raise_for_status()
                        reconnect_delay_s = 1.0
                        attempt = 0
                        log.info("[SSE-READER] upstream stream connected for session %s "
                                 "(attempt=%d, status=%d)",
                                 state.session_id, attempt, resp.status_code)
                        async for chunk in resp.aiter_text():
                            if state.shutdown.is_set():
                                log.info("[SSE-READER] session %s shutting down; "
                                         "exiting reader loop", state.session_id)
                                return
                            reader_buffer += chunk
                            while "\n\n" in reader_buffer:
                                block, reader_buffer = reader_buffer.split("\n\n", 1)
                                _broadcast_one_block(
                                    state, block, parse_sse_data(block),
                                    text_parts, thinking_parts,
                                )
                except asyncio.CancelledError:
                    log.info("[SSE-READER] reader task cancelled for session %s",
                             state.session_id)
                    raise
                except Exception as e:
                    disconnect_reason = f"{type(e).__name__}: {e}"
                    log.warning("[SSE-READER] upstream reader error for session %s "
                                "on attempt %d: %s",
                                state.session_id, attempt, disconnect_reason)
                else:
                    log.warning("[SSE-READER] upstream stream ended for session %s "
                                "on attempt %d without an exception",
                                state.session_id, attempt)
                finally:
                    if sse_http is not None:
                        try:
                            await sse_http.aclose()
                        except Exception:
                            pass

                if state.shutdown.is_set():
                    return

                if (_sse_reader_disconnect_is_recoverable(state)
                        and attempt <= _SSE_MAX_IDLE_RETRIES):
                    log.warning("[SSE-READER] recoverable upstream disconnect for "
                                "session %s (%s); reconnecting in %.1fs (attempt %d/%d)",
                                state.session_id, disconnect_reason,
                                reconnect_delay_s, attempt, _SSE_MAX_IDLE_RETRIES)
                    await asyncio.sleep(reconnect_delay_s)
                    reconnect_delay_s = min(reconnect_delay_s * 2, 10.0)
                    continue

                # Retries exhausted — try to recover the sandbox before giving up.
                if not state.shutdown.is_set():
                    try:
                        sandbox_record = await get_sandbox(state.sandbox_id)
                        if sandbox_record is not None:
                            log.info(
                                "[SSE-READER] retries exhausted for session %s; attempting sandbox recovery",
                                state.session_id,
                            )
                            spawn_env = await _spawn_env_for_sandbox(state.sandbox_id)
                            new_url, _ = await _ensure_sandbox_alive(
                                state.sandbox_id, sandbox_record,
                                agent_type=state.agent_type, spawn_env=spawn_env,
                            )
                            new_acp_session_id = str(uuid.uuid4())
                            new_client = AcpClient(new_url)
                            agent_record = await get_agent(state.agent_id)
                            if agent_record is None:
                                raise RuntimeError(
                                    f"agent {state.agent_id} missing during SSE-reader recovery"
                                )
                            # Route through the shared helper — try session/load
                            # against the existing inner_sid first, only fall
                            # through to session/new when we genuinely have
                            # nothing to resume or when load fails loudly.
                            # Previously this path ALWAYS called
                            # _apply_config_and_initialize (session/new),
                            # silently overwriting state.inner_session_id and
                            # wiping conversation context on every external
                            # sandbox stop mid-conversation.
                            new_inner_sid, started_fresh = await _attach_acp_session(
                                new_client, new_acp_session_id, agent_record,
                                inner_sid=state.inner_session_id,
                                cwd=sandbox_record.root or "/tmp",
                            )
                            if started_fresh and new_inner_sid:
                                # Genuine fresh session — reflect in DB so a
                                # later ensure_session_live sees the right
                                # inner_sid when the next message arrives.
                                # volume_id is NOT NULL on sessions; pull it
                                # from the sandbox row we already fetched.
                                await upsert_session(
                                    state.session_id, state.agent_id,
                                    state.sandbox_id, new_inner_sid,
                                    volume_id=sandbox_record.volume_id,
                                )
                            old_client = state.client
                            state.client = new_client
                            state.supervisor_url = new_url
                            state.acp_session_id = new_acp_session_id
                            state.inner_session_id = new_inner_sid
                            try:
                                await old_client.aclose()
                            except Exception:
                                pass
                            log.info(
                                "[SSE-READER] sandbox recovered for session %s; new acp_session=%s",
                                state.session_id, new_acp_session_id,
                            )
                            attempt = 0
                            reconnect_delay_s = 1.0
                            continue
                    except Exception as recovery_err:
                        log.error(
                            "[SSE-READER] sandbox recovery failed for session %s: %s",
                            state.session_id, recovery_err, exc_info=True,
                        )

                log.warning(
                    "[SSE-READER] unrecoverable upstream disconnect for session %s (%s) "
                    "(shutdown=%s, agent_busy=%s, pending=%d, in_SESSIONS=%s)",
                    state.session_id, disconnect_reason, state.shutdown.is_set(),
                    state.agent_busy, len(state.pending_prompts),
                    state.session_id in SESSIONS,
                )
                _flush_buffered_text(state, text_parts, thinking_parts, state.active_rpc_id)
                _on_sse_reader_death(state)
                state.broadcast(_SSE_SENTINEL)
                return
        except asyncio.CancelledError:
            pass
        finally:
            state._reader_alive = False
            log.info("[SSE-READER] reader task stopped for session %s", state.session_id)

    state._reader_task = asyncio.create_task(_reader())


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


async def _apply_config_and_initialize(
    client: AcpClient,
    config: AgentConfig,
    acp_session_id: str,
    cwd: str,
) -> None:
    """Initialize the ACP session with MCP server config (session/new path)."""
    await client.initialize(
        acp_session_id,
        config.agent_type or "claude",
        cwd=cwd,
        mcp_servers=config.mcp_servers,
    )


async def _attach_acp_session(
    client: AcpClient,
    acp_session_id: str,
    agent_record: AgentRecord,
    *,
    inner_sid: str | None,
    cwd: str,
) -> tuple[str | None, bool]:
    """Attach to an ACP session — resume if possible, otherwise start fresh.

    Single source of truth for "how do we obtain an ``inner_session_id`` on
    this ACP connection". Every call site that needs an attached ACP session
    (POST /message recovery, SSE-reader upstream-disconnect recovery, and
    initial ``sessions_quick_create``) routes through here, so the invariant
    *"always try ``session/load`` when we have an ``inner_session_id``, only
    create a new one when genuinely fresh or when load actually fails"*
    lives in one place and can't be accidentally skipped.

    Behavior:
      - If ``inner_sid`` is truthy → handshake + ``session/load``. On
        success, the agent's prior conversation is preserved and this
        returns ``(inner_sid, False)``.
      - If ``inner_sid`` is ``None`` OR ``session/load`` raises → fall back
        to ``session/new`` via ``_apply_config_and_initialize`` and return
        ``(new_inner_sid, True)``.
      - The fallback-after-failure case logs a loud WARNING: it means the
        agent's conversation context was lost unexpectedly (e.g., the
        session JSONL isn't where Claude Code expects it) and callers
        should treat this as a bug signal rather than steady state.

    Idempotent in the useful sense: calling with the same ``inner_sid`` on
    a fresh ``AcpClient`` should always reattach to the same conversation.
    """
    agent_type = (
        (agent_record.config.agent_type or "claude")
        if agent_record.config else "claude"
    )
    mcp = agent_record.config.mcp_servers if agent_record.config else None

    if inner_sid:
        try:
            await client.handshake(acp_session_id, agent_type)
            await client._send_rpc(acp_session_id, "session/load", {
                "sessionId": inner_sid,
                "cwd": cwd,
                "mcpServers": _mcp_dict_to_acp_array(mcp) if mcp else [],
            })
            client.set_inner_session_id(acp_session_id, inner_sid)
            try:
                await client.set_mode(acp_session_id, "bypassPermissions")
            except Exception:
                pass
            return inner_sid, False
        except Exception as load_err:
            log.warning(
                "session/load failed (inner_sid=%s, cwd=%s): %r — "
                "falling back to session/new; CONVERSATION CONTEXT WILL BE LOST",
                inner_sid, cwd, load_err,
            )

    # Genuinely fresh (no inner_sid) or load failed — start a new session.
    await _apply_config_and_initialize(
        client, agent_record.config, acp_session_id, cwd,
    )
    return client.get_inner_session_id(acp_session_id), True


# ---------------------------------------------------------------------------
# Skills provisioning (npx skills)
# ---------------------------------------------------------------------------


def _normalize_skills(skills) -> list[str]:
    """Normalize skills config into a list of source strings for ``npx skills add``.

    Accepts:
      - list[str]:  ["rllm-org/hive#staging", "vercel-labs/agent-skills"]
      - dict:       {"hive": {"source": "rllm-org/hive#staging"}, ...}
    """
    if skills is None:
        return []
    if isinstance(skills, list):
        return [str(s) for s in skills]
    if isinstance(skills, dict):
        sources = []
        for name, cfg in skills.items():
            if isinstance(cfg, str):
                sources.append(cfg)
            elif isinstance(cfg, dict):
                src = cfg.get("source", "")
                ref = cfg.get("ref")
                if ref and "#" not in src:
                    src = f"{src}#{ref}"
                if src:
                    sources.append(src)
        return sources
    return []


def _skills_install_commands(skills) -> list[str]:
    """Return shell commands to install skills via ``npx skills add``."""
    sources = _normalize_skills(skills)
    return [f"npx -y skills add {shlex.quote(source)} --all -g" for source in sources]


async def _install_skills_locally(skills) -> None:
    """Install skills on the local host (for the local provider)."""
    for cmd in _skills_install_commands(skills):
        log.info("installing skill (local): %s", cmd)
        proc = await asyncio.create_subprocess_shell(
            cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=120)
        if proc.returncode != 0:
            raise RuntimeError(f"skill install failed: {stderr.decode()[:500]}")
        log.info("skill installed: %s", stdout.decode()[-200:].strip())


# ---------------------------------------------------------------------------
# Request helpers
# ---------------------------------------------------------------------------

_CONFIG_KEYS = (
    "model",
    "prompt",
    "tools",
    "mcp_servers",
    "skills",
    "agent_type",
    "cwd",
    "root",
    "dockerfile",
    "dockerfile_content",
)


def _forbid_auth_keys_in_env(env: dict | None, where: str) -> None:
    """Raise 400 if a client tries to smuggle credential keys through ``env``.

    ``env`` is stored plaintext and returned plain by GET endpoints; credentials
    must go in ``secrets`` instead. Applies to both top-level ``env`` and
    nested ``config.env``.

    Also enforces POSIX env var names — blocks shell-injection via the
    provider-layer ``_build_env_prefix`` which interpolates keys into
    ``sh -c`` commands (the value is shlex-quoted but the key is not, so a
    key like ``FOO;cmd;X`` would break out of ``env``'s arglist).
    """
    if not env:
        return
    from .providers import AUTH_KEYS
    from .providers._shared import _ENV_KEY_RE

    offenders = sorted(k for k in env if k in AUTH_KEYS)
    if offenders:
        raise HTTPException(400, f"{where}: auth keys {offenders} must be sent "
                                 "via 'secrets', not 'env' (env is stored plain "
                                 "and returned by GET).")
    bad_names = sorted(k for k in env if not (isinstance(k, str) and _ENV_KEY_RE.match(k)))
    if bad_names:
        raise HTTPException(400, f"{where}: invalid env var name(s) {bad_names}; "
                                 "must match [A-Za-z_][A-Za-z0-9_]*")


def _merge_top_level_config(data: dict, config_data: dict) -> None:
    """Merge SDK top-level keys into config_data if not already present."""
    for key in _CONFIG_KEYS:
        if key in data and key not in config_data:
            config_data[key] = data[key]


# Sentinel distinguishing "env key not present" from "env: {}" in the request body.
_ENV_MISSING = object()


def _pop_env_and_secrets(
    data: dict,
) -> tuple[dict[str, str] | None, dict[str, str] | None]:
    """Extract ``env`` (identity, stored) and ``secrets`` (auth material,
    stored-but-redacted-on-read) from a request body.

    PATCH-like semantics on both fields:
        - key missing → ``None``  (caller should keep stored value unchanged)
        - ``{}``       → ``{}``    (caller should wipe stored value)
        - ``{…}``      → the dict (caller should replace stored value)

    Any string/int/float value is coerced to str. SECURITY: both fields
    are popped in-place so they can't flow into ``config_data``,
    ``AgentConfig``, or request logs.  Keys are validated against the
    POSIX env var grammar — providers (daytona, docker) interpolate them
    into ``sh -c`` commands, and a key like ``FOO;rm -rf /;BAR`` would
    escape ``env``'s arglist and execute arbitrary commands inside the
    sandbox.  The provider-layer ``_build_env_prefix`` re-validates as
    defence-in-depth.
    """
    from .providers._shared import _ENV_KEY_RE

    def _coerce(d: object, where: str) -> dict[str, str]:
        if not isinstance(d, dict):
            return {}
        out: dict[str, str] = {}
        for k, v in d.items():
            if not isinstance(k, str):
                continue
            if not _ENV_KEY_RE.match(k):
                raise HTTPException(400, f"{where}: invalid env var name {k!r}; "
                                         "must match [A-Za-z_][A-Za-z0-9_]*")
            if isinstance(v, (str, int, float)):
                out[k] = str(v)
        return out

    def _extract(key: str) -> dict[str, str] | None:
        raw = data.pop(key, _ENV_MISSING)
        return None if raw is _ENV_MISSING else _coerce(raw, f"request body {key!r}")

    env = _extract("env")
    secrets = _extract("secrets")
    _forbid_auth_keys_in_env(env, "request body 'env'")
    return env, secrets


async def _build_spawn_env_from_row(rec: dict) -> dict[str, str]:
    """Assemble spawn_env (agent.env ∪ session.env ∪ session.secrets) from a
    session row already fetched from the DB."""
    agent_id = rec.get("agent_id")
    session_env = rec.get("env") or {}
    session_secrets = rec.get("secrets") or {}
    agent_env: dict[str, str] = {}
    if agent_id:
        agent_record = await get_agent(agent_id)
        if agent_record is not None:
            agent_env = agent_record.config.env or {}
    return _merge_env(agent_env, session_env, session_secrets)


async def _spawn_env_for_sandbox(sandbox_id: str) -> dict[str, str]:
    """Best-effort spawn_env for a sandbox-level operation (start/exec/etc).

    All sessions on a given sandbox share the same supervisor process, so any
    session on the sandbox has the right env/secrets. If no session exists yet
    (e.g. provisioned but unused), returns ``{}`` — strict-mode will still
    strip auth keys, so auto-recovery simply has nothing extra to inject.
    """
    rec = await get_any_session_for_sandbox(sandbox_id)
    if rec is None:
        return {}
    return await _build_spawn_env_from_row(rec)


def _merge_env(
    agent_env: dict[str, str] | None,
    session_env: dict[str, str] | None,
    secrets: dict[str, str] | None,
) -> dict[str, str]:
    """Build the env that lands in a supervisor subprocess.

    Precedence (later wins): agent.env → session.env → secrets.
    Returns a fresh dict; never mutates inputs.
    """
    return {**(agent_env or {}), **(session_env or {}), **(secrets or {})}


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


def _sandbox_record(
    sandbox_id: str,
    provider: str,
    instance: ProviderInstance,
    *,
    volume_id: str,
    subpath: str,
    root_fallback: str = "/tmp",
    status: str = STATUS_RUNNING,
) -> SandboxRecord:
    """Build a SandboxRecord from a freshly-provisioned ProviderInstance.

    Consolidates the seven call sites that construct an identical shape
    from ``(sandbox_id, provider, instance, volume_id, subpath)``.
    """
    return SandboxRecord(
        id=sandbox_id,
        provider=provider,
        sandbox_ref=instance.sandbox_id or sandbox_id,
        status=status,
        root=instance.root or root_fallback,
        volume_id=volume_id,
        subpath=subpath,
        listen_port=instance.port,
    )


# ---------------------------------------------------------------------------
# Agent CRUD (config only, no sandbox)
# ---------------------------------------------------------------------------


@app.post("/agents")
async def create_agent(request: Request):
    data = await _json_body(request)
    agent_id = str(uuid.uuid4())
    name = data.get("name")
    config_data = data.get("config", {})
    _merge_top_level_config(data, config_data)
    _forbid_auth_keys_in_env(config_data.get("env"), "config.env")
    if materialized := _materialize_dockerfile(config_data):
        config_data["dockerfile"] = materialized
    config = AgentConfig.from_dict(config_data)
    await upsert_agent(AgentRecord(id=agent_id, name=name, config=config))
    return {"id": agent_id, "name": name, "config": config.to_dict()}


@app.get("/agents")
async def list_agents_route():
    agents = await list_agents()
    return [{"id": a.id, "name": a.name, "config": a.config.to_dict()} for a in agents]


@app.get("/agents/{agent_id}")
async def get_agent_route(agent_id: str):
    record = await _require_agent(agent_id)
    return {"id": record.id, "name": record.name, "config": record.config.to_dict()}


@app.delete("/agents/{agent_id}")
async def delete_agent_route(agent_id: str):
    await _require_agent(agent_id)
    await delete_agent(agent_id)
    return {"status": "deleted"}


# ---------------------------------------------------------------------------
# Volume CRUD
# ---------------------------------------------------------------------------


_VOLUME_NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")
# Subpath: POSIX-ish relative path, no traversal, no shell/comma/newline.
# Docker ``--mount`` parses the value as comma-separated k=v; a subpath of
# ``foo,readonly`` would inject an unintended mount flag. Local provider
# further runs ``_safe_path`` on it. We pre-filter at the HTTP layer so all
# three providers see a path that can't smuggle metacharacters.
_SUBPATH_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._/\-]{0,255}$")


class _VolumeCreateBody(BaseModel):
    name: str
    provider: str


def _validate_volume_name(name: str) -> None:
    """Reject volume names that could escape server-generated contexts.

    ``name`` appears in docker ``volume create`` argv (no shell injection) and
    in the local provider as a filesystem path component where ``../`` or ``/``
    would escape ``AGENT_SDK_LOCAL_VOL_ROOT``. A tight allowlist keeps every
    provider happy.
    """
    if not isinstance(name, str) or not _VOLUME_NAME_RE.match(name):
        raise HTTPException(400, "volume name must match [A-Za-z0-9][A-Za-z0-9._-]{0,63}")


def _validate_subpath(subpath: str) -> None:
    """Reject subpaths that could inject into docker mount flags or escape."""
    if not isinstance(subpath, str) or not _SUBPATH_RE.match(subpath):
        raise HTTPException(
            400,
            "subpath must match [A-Za-z0-9][A-Za-z0-9._/-]{0,255} "
            "(no traversal, commas, or whitespace)",
        )
    # Defence-in-depth: reject ``..`` segment even though the regex already
    # blocks that as a whole-segment match.
    if any(seg == ".." for seg in subpath.split("/")):
        raise HTTPException(400, "subpath must not contain '..'")


async def _resolve_volume(id_or_name: str) -> "VolumeRecord":
    vol = await get_volume(id_or_name)
    if vol is None:
        vol = await get_volume_by_name(id_or_name)
    if vol is None:
        raise HTTPException(404, "Volume not found")
    return vol


async def _resolve_or_default_volume(
    volume_id: str | None, default_provider: str,
) -> "VolumeRecord":
    """Resolve an explicit volume id/name or fall back to the per-provider default.

    Four endpoints share this contract (``POST /sandboxes``, ``/sandboxes/provision``,
    ``/sessions``, ``/sessions/quick``). Raises ``HTTPException(404)`` for an
    unknown id/name and ``HTTPException(502)`` if default-volume provisioning fails.
    """
    if volume_id and isinstance(volume_id, str):
        return await _resolve_volume(volume_id)
    try:
        return await _get_or_create_default_volume(default_provider)
    except HTTPException:
        raise
    except Exception as e:
        log.error("_resolve_or_default_volume failed (provider=%s): %s", default_provider, e, exc_info=True)
        raise HTTPException(502, f"default volume provision failed: {e}")


async def _get_or_create_default_volume(provider: str) -> "VolumeRecord":
    """Return (creating if needed) the shared default volume for ``provider``.

    Naming: ``default-{provider}`` (e.g., ``default-local``, ``default-daytona``,
    ``default-docker``). This lets callers use the SDK without explicitly
    creating a volume per session — they get a shared, persistent workspace
    scoped to the provider.

    Idempotent: on concurrent first-time creation, the UNIQUE(name) constraint
    serializes one winner; the loser retries and reads the existing row.
    """
    if provider not in _providers_mod._PROVIDER_MODS:
        raise HTTPException(400, f"Unknown provider: {provider}")

    name = f"default-{provider}"
    vol = await get_volume_by_name(name)
    if vol is not None:
        return vol

    # Race window: another request may be creating the same default right now.
    # Do the provider-side create first (cheap idempotent op — daytona/docker
    # volume-create against an existing name either succeeds or 409s; local
    # os.makedirs(..., exist_ok=True) is trivially idempotent).
    try:
        provider_ref = await _providers_mod.create_volume(provider, name)
    except Exception:
        # Someone else may have just created it; re-read.
        vol = await get_volume_by_name(name)
        if vol is not None:
            return vol
        raise

    vol = VolumeRecord(
        id=f"vol_{uuid.uuid4().hex[:12]}",
        name=name,
        provider=provider,
        provider_ref=provider_ref,
        status="ready",
    )
    try:
        await upsert_volume(vol)
    except Exception:
        # Another worker won the UNIQUE(name) race; re-read their row.
        existing = await get_volume_by_name(name)
        if existing is not None:
            # Best-effort cleanup of our now-duplicate provider-side volume.
            # (Daytona volumes can be queried by id; ours has no Daytona-side
            # dup since provider-create succeeded before we hit upsert.)
            return existing
        raise
    return vol


@app.post("/volumes")
async def create_volume(body: _VolumeCreateBody):
    _validate_volume_name(body.name)
    # Reject duplicates up front so we never orphan a provider-side volume.
    if await get_volume_by_name(body.name) is not None:
        raise HTTPException(409, f"Volume '{body.name}' already exists")

    # Tests mocking provider creation should patch the per-provider function
    # (e.g. ``api.providers.daytona.create_daytona_volume``), not this dispatcher.
    if body.provider not in _providers_mod._PROVIDER_MODS:
        raise HTTPException(400, f"Unknown provider: {body.provider}")
    provider_ref = await _providers_mod.create_volume(body.provider, body.name)

    vol = VolumeRecord(
        id=f"vol_{uuid.uuid4().hex[:12]}",
        name=body.name,
        provider=body.provider,
        provider_ref=provider_ref,
        status="ready",
    )
    try:
        await upsert_volume(vol)
    except Exception:
        # Clean up the now-orphaned provider volume on DB failure.
        try:
            await _providers_mod.delete_volume(body.provider, provider_ref)
        except Exception as cleanup_err:
            log.warning(
                "orphaned %s volume %s: rollback delete failed: %s",
                body.provider, provider_ref, cleanup_err,
            )
        raise
    return vol


@app.post("/volumes/provision")
async def provision_volume(body: _VolumeCreateBody):
    """Create + wait for ready. For Daytona, create_daytona_volume already
    polls until the backend volume is in 'ready' state, so this is equivalent
    to POST /volumes today. Kept as a separate endpoint for API parity with
    /sandboxes/provision."""
    return await create_volume(body)


@app.get("/volumes")
async def list_volumes_route(provider: str | None = None):
    return await list_volumes(provider)


@app.get("/volumes/{id_or_name}")
async def get_volume_route(id_or_name: str):
    return await _resolve_volume(id_or_name)


@app.delete("/volumes/{id_or_name}", status_code=204)
async def delete_volume_route(id_or_name: str, force: bool = False):
    vol = await _resolve_volume(id_or_name)

    async with get_db() as conn:
        cur = await conn.execute(
            "SELECT count(*) AS n FROM sessions WHERE volume_id = %s", (vol.id,)
        )
        session_count = (await cur.fetchone())["n"]
        cur = await conn.execute(
            "SELECT count(*) AS n FROM sandboxes WHERE volume_id = %s", (vol.id,)
        )
        sandbox_count = (await cur.fetchone())["n"]

        if (session_count > 0 or sandbox_count > 0) and not force:
            raise HTTPException(
                409,
                f"Volume has {session_count} session(s) and {sandbox_count} "
                f"sandbox(es). Use ?force=true to cascade.",
            )
        if force:
            # Sessions first (sandbox FK is SET NULL), then sandboxes —
            # FK RESTRICT on volume blocks the final delete otherwise.
            if session_count > 0:
                await conn.execute(
                    "DELETE FROM sessions WHERE volume_id = %s", (vol.id,),
                )
            if sandbox_count > 0:
                await conn.execute(
                    "DELETE FROM sandboxes WHERE volume_id = %s", (vol.id,),
                )

    try:
        if vol.provider == "daytona":
            await _providers_mod.delete_daytona_volume(vol.provider_ref)
        else:
            await _providers_mod.delete_volume(vol.provider, vol.provider_ref)
    except Exception as e:
        # Swallow "gone already"-class errors (volume missing on provider side
        # — the DB row is the last copy). For Daytona also swallow 403s: the
        # user's intent is to remove this row; provider cleanup policy shouldn't
        # block that. Let other failures (in-use, etc.) propagate as 409.
        msg = str(e).lower()
        forbidden_ok = vol.provider == "daytona" and "forbidden" in msg
        gone_already = any(t in msg for t in ("not found", "no such", "404", "does not exist"))
        if forbidden_ok or gone_already:
            log.warning("volume %s (%s) provider delete skipped: %s", vol.id, vol.provider, e)
        else:
            raise HTTPException(409, f"provider-side volume delete failed: {e}")
    await delete_volume(vol.id)


# ---------------------------------------------------------------------------
# Volume file operations (tree / read / edit)
# ---------------------------------------------------------------------------


class _VolumeEditBody(BaseModel):
    path: str
    content: str  # plain text for v1


def _safe_path(p: str) -> str:
    """Normalize a volume-relative path; HTTP 400 on any violation.

    Thin adapter over :func:`api.providers._shared._safe_path` (which raises
    ``ValueError``) so the HTTP layer surfaces a 400 with a readable message.
    """
    try:
        return _shared_safe_path(None, p)
    except ValueError as e:
        raise HTTPException(400, str(e))


def _volume_fs_err(op: str, vol_provider: str, exc: Exception) -> HTTPException:
    """Common file-op error translator for the /volumes/.../files/* endpoints."""
    if isinstance(exc, FileNotFoundError):
        return HTTPException(404, f"File not found: {exc}")
    if isinstance(exc, NotImplementedError):
        return HTTPException(501, f"File ops on {vol_provider} not implemented: {exc}")
    return HTTPException(500, f"{op} failed: {exc}")


@app.get("/volumes/{id_or_name}/files/tree")
async def volume_files_tree(id_or_name: str, path: str = ""):
    vol = await _resolve_volume(id_or_name)
    rel = _safe_path(path)
    try:
        tree = await _providers_mod.volume_tree(vol.provider, vol.provider_ref, rel)
    except Exception as e:
        raise _volume_fs_err("Tree", vol.provider, e)
    return {"tree": tree}


@app.get("/volumes/{id_or_name}/files/read")
async def volume_files_read(id_or_name: str, path: str):
    vol = await _resolve_volume(id_or_name)
    rel = _safe_path(path)
    try:
        data = await _providers_mod.volume_read(vol.provider, vol.provider_ref, rel)
    except Exception as e:
        raise _volume_fs_err("Read", vol.provider, e)
    # v1 response contract: text content.
    try:
        return {"content": data.decode()}
    except UnicodeDecodeError:
        return {"content_base64": base64.b64encode(data).decode()}


@app.post("/volumes/{id_or_name}/files/edit", status_code=204)
async def volume_files_edit(id_or_name: str, body: _VolumeEditBody):
    vol = await _resolve_volume(id_or_name)
    rel = _safe_path(body.path)
    try:
        await _providers_mod.volume_write(
            vol.provider, vol.provider_ref, rel, body.content.encode()
        )
    except Exception as e:
        raise _volume_fs_err("Edit", vol.provider, e)


# ---------------------------------------------------------------------------
# Sandbox CRUD
# ---------------------------------------------------------------------------


@app.post("/sandboxes")
async def create_sandbox(request: Request):
    data = await _json_body(request)
    provider = data.get("provider", "local")
    agent_type = data.get("agent_type", "claude")
    root = data.get("root", "/tmp")
    subpath = data.get("subpath") or f"sandboxes/{uuid.uuid4().hex[:12]}/home"
    vol = await _resolve_or_default_volume(data.get("volume_id"), provider)
    _validate_subpath(subpath)
    if provider != vol.provider:
        raise HTTPException(
            400,
            f"provider {provider!r} does not match volume.provider {vol.provider!r}",
        )
    dockerfile = _materialize_dockerfile(data)
    sandbox_id = str(uuid.uuid4())
    try:
        instance = await create_instance(
            provider, agent_type, dockerfile=dockerfile, root=root,
            volume_id=vol.provider_ref, subpath=subpath,
            sandbox_id=sandbox_id,
        )
    except Exception as e:
        raise HTTPException(502, f"Provider '{provider}' failed: {e}")

    _INSTANCES[sandbox_id] = instance
    record = _sandbox_record(
        sandbox_id, provider, instance,
        volume_id=vol.id, subpath=subpath, root_fallback=root,
    )
    await upsert_sandbox(record)
    # Dual-key: ``sandbox_id`` for /sandboxes/provision parity; ``id`` stays
    # for the plain REST resource contract.
    return {
        "id": sandbox_id, "sandbox_id": sandbox_id,
        "provider": provider, "sandbox_ref": record.sandbox_ref,
        "status": "running", "root": record.root,
        "volume_id": vol.id, "subpath": subpath,
        "listen_port": instance.port, "url": instance.url or None,
    }


@app.get("/sandboxes")
async def list_sandboxes_route():
    sandboxes = await list_sandboxes()
    return [
        {
            "id": s.id,
            "provider": s.provider,
            "sandbox_ref": s.sandbox_ref,
            "status": s.status,
            "root": s.root,
        }
        for s in sandboxes
    ]


@app.get("/sandboxes/{sandbox_id}")
async def get_sandbox_route(sandbox_id: str):
    record = await _require_sandbox(sandbox_id)
    result = {
        "id": record.id,
        "provider": record.provider,
        "sandbox_ref": record.sandbox_ref,
        "status": record.status,
        "root": record.root,
    }
    if record.provider in PORT_BASED_PROVIDERS:
        try:
            result["url"] = record.derive_url()
        except Exception:
            pass
    return result


@app.delete("/sandboxes/{sandbox_id}")
async def delete_sandbox_route(sandbox_id: str):
    await _require_sandbox(sandbox_id)
    # Hold the sandbox lock to prevent concurrent auto-restart
    # from restarting the sandbox while we're deleting it.
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


@app.post("/sandboxes/provision")
async def provision_sandbox_route(request: Request):
    """Provision a sandbox on the provider selected by the request body.

    Body: ``{"volume_id": ..., "subpath": ..., "provider": "daytona"|"docker"|"local",
              "agent_type": ..., "config": {...}}``

    For Daytona the returned instance has no supervisor yet (started lazily by
    ``ensure_runtime``). For Docker/Local the supervisor is already running.
    Returns ``{sandbox_id, status}``.
    """
    data = await _json_body(request)
    agent_type = data.get("agent_type", "claude")
    config_data = data.get("config", {})
    _merge_top_level_config(data, config_data)
    _forbid_auth_keys_in_env(config_data.get("env"), "config.env")
    cwd = config_data.get("cwd", data.get("cwd", "/tmp"))
    root = config_data.get("root", data.get("root", cwd))
    dockerfile = _materialize_dockerfile(config_data)
    config = AgentConfig.from_dict(
        {**config_data, "agent_type": agent_type, "cwd": cwd}
    )

    subpath = data.get("subpath") or f"sandboxes/{uuid.uuid4().hex[:12]}/home"
    explicit_provider = data.get("provider")
    vol = await _resolve_or_default_volume(data.get("volume_id"), explicit_provider or "local")
    _validate_subpath(subpath)
    # Default the provider from the volume (so existing Daytona-only clients
    # don't have to pass it), but let an explicit body field override for tests.
    provider = explicit_provider or vol.provider
    if provider != vol.provider:
        raise HTTPException(
            400,
            f"provider {provider!r} does not match volume.provider {vol.provider!r}",
        )

    skill_cmds = _skills_install_commands(config.skills) if config.skills else []
    pre_start_commands = skill_cmds + (data.get("pre_start_commands") or [])

    # Install supervisor on the volume first — docker/local need this before
    # create_sandbox; daytona tolerates it (fast-path on cache hit).
    try:
        await ensure_volume_supervisor(vol.id, agent_type)
    except Exception as e:
        raise HTTPException(502, f"Failed to install supervisor on volume: {e}")

    # Pre-allocate the DB sandbox_id so we can thread it to the provider as a
    # container label — reconcile_on_startup cross-references live containers
    # against DB rows by this id.
    sandbox_id = f"sb_{uuid.uuid4().hex[:12]}"
    try:
        instance = await _providers_mod.provision_sandbox(
            provider,
            volume_ref=vol.provider_ref,
            subpath=subpath,
            agent_type=agent_type,
            dockerfile=dockerfile,
            pre_start_commands=pre_start_commands if pre_start_commands else None,
            root=root,
            sandbox_id=sandbox_id,
        )
    except Exception as e:
        if "circuit breaker" in str(e).lower():
            raise HTTPException(503, str(e), headers={"Retry-After": "30"})
        raise HTTPException(502, f"Failed to provision sandbox: {e}")

    _INSTANCES[sandbox_id] = instance
    await upsert_sandbox(_sandbox_record(
        sandbox_id, provider, instance,
        volume_id=vol.id, subpath=subpath, root_fallback=root,
    ))

    # Dual-key: ``sandbox_id`` is the historic shape, ``id`` matches the
    # plain /sandboxes POST response so both paths are interchangeable.
    return {
        "sandbox_id": sandbox_id, "id": sandbox_id,
        "status": "provisioned",
        "volume_id": vol.id, "subpath": subpath, "provider": provider,
    }


@app.post("/sandboxes/{sandbox_id}/stop")
async def stop_sandbox_route(sandbox_id: str):
    record = await _require_sandbox(sandbox_id)

    # Hold the sandbox lock to prevent concurrent auto-restart
    # from restarting the sandbox while we're stopping it.
    #
    # IMPORTANT: stop the provider instance BEFORE flipping status="stopped"
    # in the DB. A crash between the flip and a successful stop_instance
    # Only mark stopped after the provider confirms — otherwise the DB says
    # "stopped" while a live container still runs, and reconcile treats it as
    # an orphan. ``_INSTANCES`` must be popped *after* the DB update: if the
    # UPDATE fails, keep the entry so retry/reconcile can still locate the
    # container; otherwise the container is gone but the row says "running"
    # and the next ensure call would provision a duplicate.
    async with _get_sandbox_lock(sandbox_id):
        instance = _INSTANCES.get(sandbox_id)
        if instance:
            try:
                # snapshot_and_stop: workspace tarball lands on the volume
                # BEFORE the provider stops the sandbox, so the next
                # ensure_sandbox call can restore a fresh container from
                # the up-to-date snapshot.
                await snapshot_and_stop(record, instance)
            except Exception as e:
                log.warning("stop_sandbox_route: snapshot_and_stop failed for %s: %s",
                            sandbox_id, e)
                raise HTTPException(502, f"stop failed: {e}")

        record.status = "stopped"
        try:
            await upsert_sandbox(record)
        except Exception as e:
            # Provider is stopped but DB flip failed.  Leave _INSTANCES as-is
            # so the next reconcile or a client retry can still locate the
            # container (already exited; upsert_sandbox is idempotent).
            # Returning 502 prompts the client to retry.
            log.warning(
                "stop_sandbox_route: DB update failed for %s "
                "(container stopped; _INSTANCES kept): %s",
                sandbox_id, e,
            )
            raise HTTPException(502, f"stop succeeded but DB update failed: {e}")

        # DB update succeeded — drop the in-process entry now.
        _INSTANCES.pop(sandbox_id, None)

    return {"status": "stopped"}


@app.post("/sandboxes/{sandbox_id}/start")
async def start_sandbox_route(sandbox_id: str):
    record = await _require_sandbox(sandbox_id)
    try:
        url, _ = await _ensure_sandbox_alive(sandbox_id, record, agent_type="claude")
    except Exception as e:
        # 502 matches POST /sandboxes and POST /sandboxes/provision —
        # provider failures are upstream faults, not server bugs (500).
        raise HTTPException(502, f"failed to start sandbox: {e}")

    # Re-fetch the record: _ensure_sandbox_alive may have created a replacement
    # sandbox (for Daytona terminal-state / docker missing) and written a new
    # sandbox_ref + listen_port to the DB. Upserting our local snapshot would
    # clobber those updates with a stale sandbox_ref pointing at the dead
    # container. Fresh fetch is authoritative.
    fresh = await get_sandbox(sandbox_id)
    if fresh is None:
        # Shouldn't happen — _ensure_sandbox_alive just succeeded — but be
        # defensive: fall through to returning the working URL.
        return {"status": "running", "url": url}
    fresh.status = "running"
    await upsert_sandbox(fresh)
    return {"status": "running", "url": url}


# ---------------------------------------------------------------------------
# Admin
# ---------------------------------------------------------------------------


@app.get("/admin/sessions")
async def admin_list_sessions():
    """List in-memory sessions and instances. Useful for debugging cleanup."""
    return {
        "sessions": [
            {
                "session_id": s.session_id,
                "agent_id": s.agent_id,
                "current_sandbox_id": s.sandbox_id,
                "inner_session_id": s.inner_session_id,
                "agent_busy": s.agent_busy,
                "active_rpc_id": s.active_rpc_id,
                "pending_count": len(s.pending_prompts),
                "session_subscribers": len(s._session_subscribers),
                "rpc_subscribers": sum(len(qs) for qs in s._rpc_subscribers.values()),
                "shutdown": s.shutdown.is_set(),
            }
            for s in SESSIONS.values()
        ],
        "instances": [
            {
                "sandbox_id": sid,
                "provider": inst.provider,
                "url": inst.url,
                "port": inst.port,
                "container_id": inst.container_id[:12] if inst.container_id else None,
                "process_alive": (
                    inst.process is not None and inst.process.returncode is None
                ),
            }
            for sid, inst in _INSTANCES.items()
        ],
    }


@app.post("/admin/sessions/{session_id}/reap")
async def admin_reap_session(session_id: str):
    """Force-reap a session synchronously: same teardown the idle reaper does
    but without idle-time gating. Cancels the SSE reader, drops the session
    from SESSIONS, and stops the underlying provider instance (kills the
    supervisor + ACP child for local/docker; stops the workspace for daytona).

    The session_id remains valid afterwards — POST /sessions/{id}/resume
    will hit the recovery path: a fresh supervisor + ACP child get spawned
    and AcpClient.initialize runs session/load against the stored
    inner_session_id to restore conversation history.
    """
    state = SESSIONS.get(session_id)
    if state is None:
        raise HTTPException(404, "session not in memory")

    sandbox_id = state.sandbox_id
    await _shutdown_session_state(state, remove=True, mark_idle_at=time.time())

    stopped_provider: str | None = None
    if sandbox_id and not any(s.sandbox_id == sandbox_id for s in SESSIONS.values()):
        instance = _INSTANCES.pop(sandbox_id, None)
        if instance is not None:
            stopped_provider = instance.provider
            try:
                rec = await get_sandbox(sandbox_id)
                if rec is not None:
                    await snapshot_and_stop(rec, instance)
                else:
                    await stop_instance(instance)
            except Exception as e:
                log.warning(
                    "admin reap: snapshot_and_stop failed for %s: %s",
                    sandbox_id, e,
                )

    return {
        "session_id": session_id,
        "sandbox_id": sandbox_id,
        "provider_stopped": stopped_provider,
        "status": "reaped",
    }


# ---------------------------------------------------------------------------
# Session operations (on sandbox)
# ---------------------------------------------------------------------------


async def _bg_log(
    session_id: str, agent_id: str, sandbox_id: str, event_type: str, payload: dict
) -> None:
    """Fire-and-forget DB log — suppresses all exceptions."""
    try:
        if "text" in payload:
            payload = {**payload, "text": redact_secrets(payload["text"])}
        await log_event(
            session_id=session_id,
            agent_id=agent_id,
            sandbox_id=sandbox_id,
            event_type=event_type,
            payload=payload,
        )
    except Exception:
        pass


def _schedule_log(state: SessionState, event_type: str, payload: dict) -> None:
    """Enqueue a DB log write that runs *after* any previously-scheduled
    write for this session has completed.

    The chain guarantees per-session ordering: ``id`` and ``created_at``
    end up in the same order events were scheduled, so a UI sorting by
    either field sees a faithful timeline.  Independent sessions still
    write concurrently — the chain is per-state.
    """
    prev = state._log_chain

    async def _runner():
        if prev is not None:
            try:
                await prev
            except Exception:
                pass
        await _bg_log(
            state.session_id, state.agent_id, state.sandbox_id, event_type, payload
        )

    state._log_chain = asyncio.create_task(_runner())


def _flush_buffered_text(
    state: SessionState, text_parts: list, thinking_parts: list, prompt_id: str | None
) -> None:
    """Flush any accumulated assistant text and reasoning as separate log rows.

    Called at tool-call boundaries and at turn end so that text/tool ordering
    within a turn is preserved chronologically.  Goes through the per-session
    log chain so the rows land in the same order they were scheduled.
    """
    for parts, evt_type in (
        (text_parts, EVT_ASSISTANT_MESSAGE),
        (thinking_parts, EVT_REASONING),
    ):
        if not parts:
            continue
        text = redact_secrets("".join(parts))
        parts.clear()
        payload: dict = {"text": text}
        if prompt_id is not None:
            payload["prompt_id"] = prompt_id
        _schedule_log(state, evt_type, payload)


def _process_sse_block(
    block: str,
    state: SessionState,
    text_parts: list,
    thinking_parts: list,
    *,
    log_events: bool = False,
    payload: dict | None = None,
) -> None:
    """Parse SSE block: accumulate text/thinking, update state, optionally log to DB.

    `payload` can be passed in pre-parsed to avoid a second json.loads on the
    hot path (the reader loop already parses each block to decide the tag).
    """
    # Track the SSE cursor from the single reader only — multiple
    # proxy subscribers must not write last_event_id concurrently.
    if log_events:
        for line in block.split("\n"):
            if line.startswith("id:"):
                state.last_event_id = line[3:].strip()
                break
        state.last_activity = time.time()

    if payload is None:
        payload = parse_sse_data(block)
    if payload is None:
        return

    kind, data = parse_acp_payload(payload, None)
    prompt_id = state.active_rpc_id

    if kind == "update":
        update = data or {}
        ut = update.get("sessionUpdate", "")

        if ut in (UT_MESSAGE_DELTA, UT_MESSAGE_CHUNK):
            classified = classify_message_content(update.get("content"))
            if classified is not None:
                kind, value = classified
                if kind == "text":
                    text_parts.append(value)
                elif kind == "reasoning":
                    thinking_parts.append(value)

        elif ut == UT_THOUGHT_CHUNK:
            content = update.get("content", {})
            text = content.get("text") or content.get("thinking") or ""
            if text:
                thinking_parts.append(text)

        elif log_events and ut in (UT_TOOL_CALL, UT_TOOL_STARTED):
            # Flush text/thinking before the tool call so the interleave
            # within the turn is preserved.
            _flush_buffered_text(state, text_parts, thinking_parts, prompt_id)
            tool_payload: dict = {
                "tool": extract_tool_name(update),
                "tool_call_id": extract_tool_call_id(update),
                "prompt_id": prompt_id,
            }
            if title := update.get("title"):
                tool_payload["title"] = title
            if raw_input := update.get("rawInput"):
                tool_payload["args"] = raw_input
            _schedule_log(state, EVT_TOOL_CALL, tool_payload)

        elif log_events and ut == UT_TOOL_CALL_UPDATE:
            # tool_call_update usually carries a tool result; sometimes a
            # refined arg set. Persist result rows but never re-emit a
            # duplicate EVT_TOOL_CALL.
            tool_response = extract_tool_response(update)
            if tool_response is not None:
                result_payload: dict = {
                    "tool": extract_tool_name(update),
                    "tool_call_id": extract_tool_call_id(update),
                    "result": tool_response,
                    "prompt_id": prompt_id,
                }
                if title := update.get("title"):
                    result_payload["title"] = title
                _schedule_log(state, EVT_TOOL_RESULT, result_payload)

        elif ut == UT_COMMANDS_UPDATE:
            cmds = update.get("availableCommands")
            if isinstance(cmds, list):
                state.available_commands = cmds

        elif log_events and ut in (UT_USAGE_UPDATED, UT_USAGE_UPDATE):
            usage_payload = dict(update.get("cost") or update)
            usage_payload["prompt_id"] = prompt_id
            _schedule_log(state, EVT_USAGE, usage_payload)
        return

    if kind == "done_result":
        if not log_events:
            text_parts.clear()
            thinking_parts.clear()
            return
        _mark_turn_finished(state)
        _flush_buffered_text(state, text_parts, thinking_parts, prompt_id)
        result = data or {}
        done_payload: dict = {"stop_reason": result.get("stopReason"), "prompt_id": prompt_id}
        if usage := result.get("usage"):
            done_payload["usage"] = usage
        _schedule_log(state, "turn_end", done_payload)
        return

    if kind == "error" and log_events:
        _mark_turn_finished(state)
        # Flush any in-flight text/thinking before the error frame.
        _flush_buffered_text(state, text_parts, thinking_parts, prompt_id)
        err = data or {}
        err_data = err.get("data") if isinstance(err.get("data"), dict) else {}
        _schedule_log(state, EVT_ERROR, {
            "message": err.get("message", str(err))[:500],
            "kind": err_data.get("kind") if err_data else None,
            "prompt_id": prompt_id,
        })


_DAYTONA_UNRECOVERABLE_TOKENS = (
    "not found", "destroyed", "destroying",
    "terminal state", "unrecoverable", "unknown",
)


def _should_replace_daytona_sandbox(exc: Exception) -> bool:
    """True when a Daytona recovery error means the old sandbox is gone for good."""
    text = str(exc).lower()
    return any(token in text for token in _DAYTONA_UNRECOVERABLE_TOKENS)


async def snapshot_supervisor(
    sandbox: SandboxRecord, *, url: str | None = None,
) -> None:
    """Call POST /v1/snapshot on the sandbox's supervisor.

    Best-effort: non-200 responses and transport errors are logged, not
    raised. Callers (typically ``snapshot_and_stop``) proceed with
    teardown regardless — a transient volume error must not pin the
    sandbox alive, and the sandbox is about to die anyway.

    ``url`` lets the caller inject a pre-resolved supervisor URL (e.g.
    from ``SessionState.supervisor_url``). Without it we try the
    ``_INSTANCES`` cache, then port-based derivation. That covers
    docker/local today; Daytona callers pass ``url=`` explicitly for now
    until a URL-resolution step lands with the daytona call-sites.
    """
    if url is None:
        inst = _INSTANCES.get(sandbox.id)
        if inst and inst.url:
            url = inst.url
        elif sandbox.listen_port is not None:
            url = f"http://localhost:{sandbox.listen_port}"
        else:
            log.warning(
                "snapshot_supervisor: no URL for sandbox %s (provider=%s); skipping",
                sandbox.id, sandbox.provider,
            )
            return

    async with httpx.AsyncClient(timeout=30) as client:
        try:
            r = await client.post(f"{url}/v1/snapshot")
            if r.status_code != 200:
                log.warning(
                    "snapshot_supervisor: %s returned %d: %s",
                    sandbox.id, r.status_code, r.text[:200],
                )
        except Exception as e:
            log.warning(
                "snapshot_supervisor: POST failed for %s: %s", sandbox.id, e,
            )


async def snapshot_and_stop(
    sandbox: SandboxRecord,
    instance: ProviderInstance | None = None,
    *, url: str | None = None,
) -> None:
    """Snapshot the workspace, then stop the sandbox at the provider.

    The single server-initiated stop entry point under the snapshot-on-stop
    model: reap, /sandboxes/:id/stop, sandbox-replacement during recovery,
    agent-delete all route through here.

    Ordering is strict — snapshot before stop, even if the snapshot step
    itself fails. ``snapshot_supervisor`` already swallows its own errors,
    but we wrap again defensively so a bug in the helper cannot wedge the
    stop path. The sandbox is about to die; a lost snapshot is recoverable
    (the user loses a turn's worth of state, at most), a wedged stop is not.

    ``instance`` is used if provided; otherwise we look up _INSTANCES or
    synthesize a minimal ProviderInstance from the sandbox row so
    ``stop_instance`` can still target the provider.
    """
    try:
        await snapshot_supervisor(sandbox, url=url)
    except Exception as e:
        log.warning(
            "snapshot_and_stop: snapshot step failed for %s: %s; "
            "proceeding to stop anyway", sandbox.id, e,
        )

    inst = instance or _INSTANCES.get(sandbox.id)
    if inst is None:
        vol = await get_volume(sandbox.volume_id) if sandbox.volume_id else None
        provider = vol.provider if vol else sandbox.provider
        inst = ProviderInstance(
            provider=provider, url="",
            root=sandbox.root, sandbox_id=sandbox.sandbox_ref,
        )
    await stop_instance(inst)


async def _ensure_sandbox_alive(
    sandbox_id: str,
    sandbox_record: SandboxRecord,
    agent_type: str = "claude",
    dockerfile: str | None = None,
    spawn_env: dict[str, str] | None = None,
) -> tuple[str, bool]:
    """Ensure the supervisor is reachable. Restart if needed.

    Returns ``(url, replaced)`` where ``replaced=True`` means a fresh Daytona
    sandbox had to be created because the old one was unrecoverable.
    """
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
                from .providers import _wait_for_health
                alive = (
                    (instance.process is not None and instance.process.returncode is None)
                    or (instance.container_id
                        and await _wait_for_health(instance.url, max_retries=2, interval=0.5))
                )
                if alive:
                    return instance.url, False
                # Clean up old container before creating replacement.
                try:
                    await destroy_instance(instance)
                except Exception:
                    pass

            log.info("auto-restarting sandbox %s (provider=%s)", sandbox_id, provider)
            if spawn_env is None:
                spawn_env = await _spawn_env_for_sandbox(sandbox_id)

            # Resolve the volume so the replacement lands on the same mount
            # (Docker refuses empty subpath, Local would start outside the volume).
            if not fresh_record.volume_id:
                raise RuntimeError(
                    f"Sandbox {sandbox_id} has no volume_id; cannot auto-restart"
                )
            vol = await get_volume(fresh_record.volume_id)
            if vol is None:
                raise RuntimeError(
                    f"Sandbox {sandbox_id} references missing volume {fresh_record.volume_id}"
                )

            try:
                new_instance = await _providers_mod.provision_sandbox(
                    provider,
                    volume_ref=vol.provider_ref, subpath=fresh_record.subpath or "",
                    agent_type=agent_type, dockerfile=dockerfile,
                    root=sandbox_record.root, spawn_env=spawn_env,
                    sandbox_id=sandbox_id,
                )
            except Exception as e:
                raise RuntimeError(f"Failed to restart sandbox: {e}")

            _INSTANCES[sandbox_id] = new_instance
            await upsert_sandbox(_sandbox_record(
                sandbox_id, provider, new_instance,
                volume_id=sandbox_record.volume_id,
                subpath=sandbox_record.subpath,
                root_fallback=sandbox_record.root,
            ))
            return new_instance.url, False

    # For daytona: health-check the URL; if down, restart the supervisor
    # inside the existing sandbox (preserves filesystem + acp session state).
    async with _get_sandbox_lock(sandbox_id):
        instance = _INSTANCES.get(sandbox_id)
        if instance and instance.url:
            try:
                async with httpx.AsyncClient(timeout=5) as client:
                    r = await client.get(f"{instance.url}/v1/health")
                    if r.status_code == 200:
                        return instance.url, False
            except Exception:
                pass

        daytona_sandbox_id = sandbox_record.sandbox_ref
        log.info("recovering daytona sandbox %s (daytona_id=%s)",
                 sandbox_id, daytona_sandbox_id)
        if spawn_env is None:
            spawn_env = await _spawn_env_for_sandbox(sandbox_id)
        replaced = False
        try:
            from .providers import restart_daytona_supervisor
            new_instance = await restart_daytona_supervisor(
                daytona_sandbox_id, agent_type, root=sandbox_record.root,
                spawn_env=spawn_env,
            )
        except Exception as e:
            if not _should_replace_daytona_sandbox(e):
                raise RuntimeError(f"Failed to recover daytona sandbox: {e}")
            log.warning(
                "daytona sandbox %s unrecoverable for sandbox %s: %s; creating replacement",
                daytona_sandbox_id, sandbox_id, e,
            )
            # Preserve volume_id + subpath so the replacement mounts the same
            # storage. Resolve the Daytona volume provider_ref for the mount API.
            dt_volume_ref = None
            if sandbox_record.volume_id:
                _vol = await get_volume(sandbox_record.volume_id)
                if _vol is not None:
                    dt_volume_ref = _vol.provider_ref
            try:
                new_instance = await create_instance(
                    "daytona", agent_type, dockerfile=dockerfile,
                    root=sandbox_record.root, spawn_env=spawn_env,
                    volume_id=dt_volume_ref, subpath=sandbox_record.subpath,
                    sandbox_id=sandbox_id,
                )
            except Exception as create_err:
                raise RuntimeError(
                    f"Failed to create replacement daytona sandbox: {create_err}"
                )
            replaced = True

        _INSTANCES[sandbox_id] = new_instance
        await upsert_sandbox(_sandbox_record(
            sandbox_id, "daytona", new_instance,
            volume_id=sandbox_record.volume_id,
            subpath=sandbox_record.subpath,
            root_fallback=sandbox_record.root,
        ))
        return new_instance.url, replaced




async def ensure_volume_supervisor(volume_id: str, agent_type: str) -> None:
    """Idempotently install the supervisor + ACP binary on a volume.

    Cross-worker serialization uses a Postgres advisory lock keyed on
    ``hash((volume_id, agent_type))``.  Fast path: if the
    ``volumes.supervisor_agent_types`` cache already lists this agent_type,
    return immediately without touching the DB beyond the initial read.

    Locking model (slow path):

    1. Open a short transaction, re-check the cache, then release — the
       cheap double-check lets most racers short-circuit before touching
       the advisory-lock machinery at all.
    2. Acquire a session-scoped ``pg_advisory_lock`` on the SAME key on
       a dedicated autocommit connection — this is the cross-process
       "I'm installing" signal.  ``pg_advisory_lock`` (session-scoped,
       NOT ``pg_advisory_xact_lock``) is released automatically if the
       backend dies or the TCP connection drops, so a crashed worker
       can't wedge the key.
    3. Re-check the cache under the session lock (another worker may have
       installed between steps 1 and 2).
    4. Run the slow provider install with NO open transaction and NO
       connection held (the provider call talks to docker/daytona/local,
       not to the DB).
    5. Take a fresh short transaction to update the cache + release the
       session lock via ``pg_advisory_unlock``.

    Failure semantics: if the provider call raises, the session lock is
    released in the ``finally`` block so a retry can re-enter immediately.
    The volumes cache is only updated on success, so a failed install
    leaves no footprint to undo.

    Trade-off: the window between steps 1 and 2 is a race — two workers
    can both pass the double-check and then serialize on the session lock
    in step 2, doing the install twice.  That's harmless (the install is
    idempotent in its own right — the second worker will short-circuit at
    the step-3 cache re-check) but means we don't get strict "install once"
    semantics.  Acceptable because the alternative is holding a pool
    connection for minutes, which is more expensive operationally.
    """
    # Fast path (no lock): check cache first — 99% of calls hit this.
    vol = await get_volume(volume_id)
    if vol is None:
        raise HTTPException(500, f"Volume {volume_id} not found")
    if agent_type in (vol.supervisor_agent_types or []):
        return  # already installed

    # Compute a positive 63-bit key (pg advisory locks take a bigint; mask
    # off the sign bit for safety).
    lock_key = hash((volume_id, agent_type)) & 0x7FFFFFFFFFFFFFFF

    # Step 1 + 2: double-check cache, then acquire a session-scoped advisory
    # lock OUTSIDE a transaction so we can release the connection while
    # the slow provider call runs.  We pick up a dedicated connection for
    # the session lock so it's not tied to any pool's transaction.
    async with get_db() as conn:
        row = await (await conn.execute(
            "SELECT provider, provider_ref, supervisor_agent_types FROM volumes"
            " WHERE id = %s",
            (volume_id,),
        )).fetchone()
        if row is None:
            raise HTTPException(500, f"Volume {volume_id} not found")
        installed = list(row.get("supervisor_agent_types") or [])
        if agent_type in installed:
            return
        provider = row["provider"]
        provider_ref = row["provider_ref"]

    # Acquire a session-scoped advisory lock on a dedicated connection
    # switched to autocommit so it isn't stuck in a long-running transaction
    # while the slow provider call runs. ``pg_advisory_lock`` survives commits
    # and auto-releases on connection close, so a crashed worker can't wedge
    # the key. We MUST restore autocommit=False before returning the connection
    # to the pool — psycopg's pool has no reset callback.
    async with get_db() as lock_conn:
        _restore_autocommit = False
        try:
            await lock_conn.set_autocommit(True)
            _restore_autocommit = True
        except AttributeError:
            pass  # test fakes without set_autocommit
        except Exception as e:
            log.error("ensure_volume_supervisor: set_autocommit(True) failed "
                      "(volume=%s agent=%s): %s; falling back to transactional mode",
                      volume_id, agent_type, e)
        try:
            got_row = await (await lock_conn.execute(
                "SELECT pg_try_advisory_lock(%s)", (lock_key,)
            )).fetchone()
            if not (got_row and got_row.get("pg_try_advisory_lock")):
                log.info("ensure_volume_supervisor: waiting for another worker "
                         "(volume=%s agent=%s)", volume_id, agent_type)
                await lock_conn.execute("SELECT pg_advisory_lock(%s)", (lock_key,))

            # Step 3: re-check cache while holding the session lock.
            row2 = await (await lock_conn.execute(
                "SELECT supervisor_agent_types FROM volumes WHERE id = %s",
                (volume_id,),
            )).fetchone()
            installed2 = list((row2 or {}).get("supervisor_agent_types") or [])
            if agent_type in installed2:
                return

            log.info("ensure_volume_supervisor: installing %s on volume %s",
                     agent_type, volume_id)
            # Step 4: slow provider call with the connection in autocommit (not
            # in a transaction) so idle-in-transaction timers don't fire.
            await _providers_mod.install_supervisor(provider, provider_ref, agent_type)
            # Step 5: update the cache under the same session lock.
            await lock_conn.execute(
                "UPDATE volumes SET supervisor_agent_types = "
                "COALESCE(supervisor_agent_types, '[]'::jsonb) || to_jsonb(%s::text) "
                "WHERE id = %s AND NOT (supervisor_agent_types @> to_jsonb(%s::text))",
                (agent_type, volume_id, agent_type),
            )
            log.info("ensure_volume_supervisor: done installing %s on volume %s",
                     agent_type, volume_id)
        finally:
            # Release the advisory lock explicitly — a failure here is survivable
            # (the lock auto-releases on connection close).
            try:
                await lock_conn.execute("SELECT pg_advisory_unlock(%s)", (lock_key,))
            except Exception as e:
                log.warning("ensure_volume_supervisor: pg_advisory_unlock failed: %s", e)
            # Restore autocommit=False before the conn returns to the pool; if
            # restore fails, close the conn to prevent pool poisoning.
            if _restore_autocommit:
                try:
                    await lock_conn.set_autocommit(False)
                except Exception as e:
                    log.error("ensure_volume_supervisor: failed to restore "
                              "autocommit=False: %s; closing conn to prevent "
                              "pool poisoning", e)
                    try:
                        await lock_conn.close()
                    except Exception as close_err:
                        log.warning("ensure_volume_supervisor: lock_conn.close() "
                                    "failed: %s", close_err)


async def ensure_sandbox(session_row: dict) -> SandboxRecord:
    """Guarantees: returns a sandbox that is currently live on the provider.

    Idempotent: safe to call multiple times in a row. Creates/restarts/replaces
    as needed. Emits a ``sandbox_reattach`` event when the returned sandbox is
    a replacement for a previously-recorded one.

    Holds the per-session lock for the entire check-and-act sequence so
    concurrent callers don't double-provision.
    """
    session_id = session_row["id"]
    async with _get_session_lock(session_id):
        return await _ensure_sandbox_locked(session_row)


async def _ensure_sandbox_locked(session_row: dict) -> SandboxRecord:
    session_id = session_row["id"]
    # Re-read after acquiring lock in case a concurrent caller updated it.
    fresh = await get_session(session_id)
    if fresh is None:
        raise HTTPException(404, "Session not found")
    current_id = fresh.get("current_sandbox_id")

    # Case A: no sandbox yet — create one.
    if current_id is None:
        return await _provision_new(fresh, previous_id=None)

    sb = await get_sandbox(current_id)

    # Warm fast-path: if we have a live in-process ProviderInstance AND a
    # runtime SessionState attached to this sandbox, we can trust those as
    # evidence that the sandbox is running — no need to round-trip to the
    # provider (docker inspect / daytona API / etc.) for status, nor to
    # fetch the volume row again. The subsequent ``ensure_runtime`` call
    # does a short supervisor health probe which catches any real failure;
    # if the provider-level sandbox has silently died, that probe will
    # rebuild from scratch.
    if sb is not None and sb.status == STATUS_RUNNING:
        existing_state = SESSIONS.get(session_id)
        cached_inst = _INSTANCES.get(current_id)
        if (
            cached_inst is not None
            and existing_state is not None
            and not existing_state.shutdown.is_set()
            and existing_state.sandbox_id == current_id
            and existing_state.supervisor_url
        ):
            return sb

    # Case B: row was deleted — replace and emit reattach.
    if sb is None:
        await set_session_current_sandbox(session_id, None)
        return await _provision_new(fresh, previous_id=current_id)

    # Case C-F: row exists — probe provider state.
    vol = await get_volume(sb.volume_id)
    if vol is None:
        raise HTTPException(500, f"Sandbox's volume {sb.volume_id} missing")
    status = await _providers_mod.get_sandbox_status(vol.provider, sb.sandbox_ref)
    if status == "running":
        return sb
    if status == "stopped":
        await _providers_mod.start_sandbox(vol.provider, sb.sandbox_ref)
        sb.status = STATUS_RUNNING
        await upsert_sandbox(sb)
        return sb
    if status in ("missing", "error"):
        if status == "error":
            try:
                inst = ProviderInstance(provider=vol.provider, url="",
                                        root=sb.root, sandbox_id=sb.sandbox_ref)
                await _providers_mod.destroy_sandbox(vol.provider, inst)
            except Exception:
                pass
        await delete_sandbox(sb.id)
        await set_session_current_sandbox(session_id, None)
        return await _provision_new(fresh, previous_id=current_id)
    raise HTTPException(500, f"Unknown sandbox status: {status}")


async def _provision_new(session_row: dict, previous_id: str | None) -> SandboxRecord:
    """Create a fresh sandbox (provider selected from the session's volume).

    Dispatches through the uniform ``provision_sandbox`` wrapper so docker,
    local, and daytona all work. Supervisor is installed on the volume first
    (idempotent fast-path); the sandbox is then attached to it.
    """
    vol = await get_volume(session_row["volume_id"])
    if vol is None:
        raise HTTPException(500, f"Session's volume {session_row['volume_id']} missing")
    agent_id = session_row["agent_id"]
    subpath = f"agents/{agent_id}/home"
    agent = await get_agent(agent_id)
    agent_type = (agent.config.agent_type if agent and agent.config else "claude")

    # Ensure supervisor is installed on the volume before provisioning the sandbox.
    # This is idempotent: fast-path if already installed (cache hit in volumes table).
    await ensure_volume_supervisor(vol.id, agent_type)

    # Build the spawn env for the supervisor (agent.env + session.env + secrets).
    spawn_env = await _build_spawn_env_from_row(session_row)

    # Provider-specific default root. Daytona mounts per-agent at /home/daytona;
    # docker at /home/agent; local fills it in from the volume path.
    root = {"daytona": "/home/daytona", "docker": "/home/agent"}.get(vol.provider)

    # Generate the sandbox_id up-front so we can tag the underlying
    # container/process with it. Docker uses this as a label for
    # startup reconciliation (M5); other providers currently ignore it.
    new_sandbox_id = f"sb_{uuid.uuid4().hex[:12]}"

    inst = await _providers_mod.provision_sandbox(
        vol.provider,
        volume_ref=vol.provider_ref,
        subpath=subpath,
        agent_type=agent_type,
        spawn_env=spawn_env,
        root=root,
        sandbox_id=new_sandbox_id,
    )

    sb = _sandbox_record(
        new_sandbox_id, vol.provider, inst,
        volume_id=vol.id, subpath=subpath,
        root_fallback=root or "/tmp",
    )
    _INSTANCES[sb.id] = inst

    # Atomic: insert the sandbox row + link session->sandbox in one
    # transaction. A crash between the two writes would otherwise orphan
    # the sandbox (row exists on the provider + in sandboxes, but no
    # session points at it). Use a single pool connection so both
    # statements commit (or roll back) together.
    async with get_db() as conn:
        await conn.execute(
            "INSERT INTO sandboxes"
            " (id, provider, sandbox_ref, status, root, volume_id, subpath, listen_port)"
            " VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
            " ON CONFLICT(id) DO UPDATE SET provider=EXCLUDED.provider,"
            " sandbox_ref=EXCLUDED.sandbox_ref, status=EXCLUDED.status,"
            " root=EXCLUDED.root, volume_id=EXCLUDED.volume_id,"
            " subpath=EXCLUDED.subpath, listen_port=EXCLUDED.listen_port",
            (sb.id, sb.provider, sb.sandbox_ref, sb.status,
             sb.root, sb.volume_id, sb.subpath, sb.listen_port),
        )
        await conn.execute(
            "UPDATE sessions SET current_sandbox_id = %s WHERE id = %s",
            (sb.id, session_row["id"]),
        )

    if previous_id is not None:
        await log_event(
            session_id=session_row["id"],
            agent_id=agent_id,
            sandbox_id=sb.id,
            event_type="sandbox_reattach",
            payload={"old_sandbox_id": previous_id, "new_sandbox_id": sb.id},
        )
    return sb


async def ensure_runtime(session_row: dict, sandbox: SandboxRecord) -> SessionState:
    """Guarantees: returns a SessionState with a connected, initialized client.

    If the existing SESSIONS entry is healthy and attached to this sandbox,
    reuse it. Otherwise tear down any stale state and build fresh.

    Holds the per-session lock for the entire check-and-act sequence so
    concurrent callers don't double-provision.
    """
    session_id = session_row["id"]
    async with _get_session_lock(session_id):
        return await _ensure_runtime_locked(session_row, sandbox)


async def _ensure_runtime_locked(session_row: dict, sandbox: SandboxRecord) -> SessionState:
    session_id = session_row["id"]
    existing = SESSIONS.get(session_id)

    # Reuse if the existing state is attached to this sandbox and its
    # supervisor is reachable. Otherwise (stale sandbox, dead supervisor, or
    # no URL) tear down before rebuilding.
    if existing:
        reusable = (
            not existing.shutdown.is_set()
            and existing.sandbox_id == sandbox.id
            and existing.supervisor_url
        )
        if reusable:
            from .providers import _wait_for_health
            try:
                if await _wait_for_health(existing.supervisor_url, max_retries=2, interval=1):
                    return existing
            except Exception:
                pass
        await _shutdown_session_state(existing, remove=True, force=True)

    # Build fresh.
    agent_id = session_row["agent_id"]
    agent_record = await get_agent(agent_id)
    if agent_record is None:
        raise HTTPException(500, f"Agent {agent_id} missing")
    agent_type = agent_record.config.agent_type or "claude"

    spawn_env = await _build_spawn_env_from_row(session_row)
    root = sandbox.root or (agent_record.config.cwd if agent_record.config else None) or "/tmp"
    supervisor_port = allocate_sandbox_port(sandbox.id)
    effective_spawn_env = dict(spawn_env)
    effective_spawn_env.setdefault("HOME", root)

    vol = await get_volume(session_row["volume_id"])
    if vol is None:
        free_sandbox_port(sandbox.id, supervisor_port)
        raise HTTPException(500, f"Session's volume {session_row['volume_id']} missing")

    # Supervisor URL resolution:
    #  - For Docker/Local the supervisor starts at create_sandbox time; the
    #    live ProviderInstance in _INSTANCES carries the real URL. On cold
    #    start (_INSTANCES empty) we fall back to the DB-stored listen_port.
    #  - For Daytona the URL comes from the SDK-signed preview API and is
    #    minted per session inside ensure_supervisor_url().
    if vol.provider in PORT_BASED_PROVIDERS:
        cached = _INSTANCES.get(sandbox.id)
        if cached and cached.url:
            supervisor_url = cached.url
            # No per-session supervisor port on docker/local — one sandbox
            # == one supervisor; return the allocated counter to the pool.
            free_sandbox_port(sandbox.id, supervisor_port)
            supervisor_port = cached.port  # type: ignore[assignment]
        elif sandbox.listen_port is not None:
            supervisor_url = f"http://localhost:{sandbox.listen_port}"
            free_sandbox_port(sandbox.id, supervisor_port)
            supervisor_port = sandbox.listen_port
        else:
            free_sandbox_port(sandbox.id, supervisor_port)
            raise HTTPException(
                500,
                f"Sandbox {sandbox.id} (provider={vol.provider}) has no live "
                f"instance and no listen_port recorded; cannot resolve supervisor URL",
            )
    else:
        inst = ProviderInstance(
            provider=vol.provider, url="",
            root=root, sandbox_id=sandbox.sandbox_ref,
        )
        try:
            supervisor_url = await _providers_mod.ensure_supervisor_url(
                vol.provider, inst,
                agent_type=agent_type,
                root=root,
                spawn_env=effective_spawn_env,
                port=supervisor_port,
            )
        except SandboxMissingError:
            # Provider says the sandbox is gone (deleted out-of-band). Let the
            # caller (ensure_session_live) re-provision a replacement rather
            # than surfacing a 500.
            free_sandbox_port(sandbox.id, supervisor_port)
            raise
        except Exception as exc:
            free_sandbox_port(sandbox.id, supervisor_port)
            raise HTTPException(500, f"Failed to start supervisor: {exc}") from exc

    client = AcpClient(supervisor_url)
    acp_session_id = str(uuid.uuid4())
    # Use agent config cwd to avoid initializing into a directory that may
    # not exist on the sandbox filesystem.
    cwd = (agent_record.config.cwd or "/tmp") if agent_record.config else "/tmp"

    # Single source of truth for "get me an attached ACP session" — tries
    # session/load when an inner_sid exists and only falls through to
    # session/new when there's nothing to resume or when the load raised.
    inner_sid, started_fresh = await asyncio.wait_for(
        _attach_acp_session(
            client, acp_session_id, agent_record,
            inner_sid=session_row.get("inner_session_id"),
            cwd=cwd,
        ),
        timeout=120,
    )
    if started_fresh and inner_sid:
        await upsert_session(
            session_id, agent_id, sandbox.id, inner_sid,
            volume_id=session_row.get("volume_id"),
        )

    state = SessionState(
        session_id=session_id, agent_id=agent_id, sandbox_id=sandbox.id,
        acp_session_id=acp_session_id, inner_session_id=inner_sid,
        agent_type=agent_type, client=client,
        supervisor_url=supervisor_url, supervisor_port=supervisor_port,
    )
    SESSIONS[session_id] = state
    _start_session_tasks(state)
    return state


async def _tail_daytona_supervisor_log(
    sandbox_ref: str, port: int | None, lines: int = 120
) -> str:
    """Best-effort: read the tail of the Daytona supervisor log via process.exec."""
    if port is None:
        return "(no supervisor port)"
    from .providers.daytona import _get_daytona_client
    loop = asyncio.get_running_loop()
    client = _get_daytona_client()
    sb = await loop.run_in_executor(None, lambda: client.get(sandbox_ref))
    cmd = f"tail -n {lines} /tmp/sup-work-{port}/sup-{port}.log 2>&1 || echo '(no log)'"
    r = await loop.run_in_executor(None, lambda: sb.process.exec(cmd, timeout=10))
    return (r.result if hasattr(r, "result") else str(r)) or "(empty)"


async def _recover_missing_sandbox(
    session_id: str, stale: SandboxRecord
) -> tuple[dict, SandboxRecord]:
    """Clear stale sandbox state after the provider reports it's gone.

    Called exactly once when ``ensure_runtime`` raises ``SandboxMissingError``
    (i.e. Daytona says "sandbox not found" during supervisor spawn). Evicts
    the in-memory caches, deletes the stale DB row, unlinks it from the
    session, then re-reads and re-runs ``ensure_sandbox`` — which now takes
    Case A in ``_ensure_sandbox_locked`` (``current_sandbox_id is None``) and
    provisions a replacement on the same volume. Not a retry loop: one
    recoverable failure, one recovery, one forward path.
    """
    log.warning(
        "sandbox %s (ref=%s) missing on provider; provisioning replacement on same volume",
        stale.id, stale.sandbox_ref,
    )
    SESSIONS.pop(session_id, None)
    _INSTANCES.pop(stale.id, None)
    try:
        await delete_sandbox(stale.id)
    except Exception as del_err:
        log.warning("delete_sandbox(%s) failed: %s", stale.id, del_err)
    await set_session_current_sandbox(session_id, None)
    session = await _require_session_row(session_id)
    sandbox = await ensure_sandbox(session)
    return session, sandbox


async def ensure_session_live(session_id: str) -> tuple[dict, SandboxRecord, SessionState]:
    """One-shot: session → sandbox → runtime. Most endpoints use this.

    Handles the one recoverable lifecycle event that callers shouldn't have to
    know about: ``SandboxMissingError`` from ``ensure_runtime`` means the
    provider lost the sandbox out-of-band (e.g. external ``daytona.delete()``).
    We provision a replacement on the same volume and try the runtime build
    once more — a second failure is fatal, since it means provisioning itself
    is broken, not just the stale handle.
    """
    session = await _require_session_row(session_id)
    sandbox = await ensure_sandbox(session)
    try:
        runtime = await ensure_runtime(session, sandbox)
    except SandboxMissingError:
        session, sandbox = await _recover_missing_sandbox(session_id, sandbox)
        runtime = await ensure_runtime(session, sandbox)
    return session, sandbox, runtime



@app.get("/sessions")
async def list_sessions_route():
    """List all active in-memory sessions with status."""
    now = time.time()
    return [
        {
            "session_id": s.session_id,
            "agent_id": s.agent_id,
            "current_sandbox_id": s.sandbox_id,
            "idle_seconds": round(now - (s.turn_completed_at or s.last_activity), 1),
            "shutdown_requested": s.shutdown.is_set(),
        }
        for s in SESSIONS.values()
    ]


@app.get("/sessions/{session_id}")
async def get_session_route(session_id: str):
    """Return stored session metadata. Redacts secret values — only keys.

    ``env`` is returned in full (non-sensitive). ``secrets`` is returned as
    ``{"keys": [...]}`` (names only) so callers can confirm what's stored
    without leaking values. Values are never serialized to clients.
    """
    rec = await _require_session_row(session_id)
    env = rec.get("env") or {}
    secrets = rec.get("secrets") or {}
    return {
        "session_id": rec.get("id"),
        "agent_id": rec.get("agent_id"),
        "volume_id": rec.get("volume_id"),
        "current_sandbox_id": rec.get("current_sandbox_id"),
        "inner_session_id": rec.get("inner_session_id"),
        "env": env,
        "secrets": {"keys": sorted(secrets.keys())},
    }


@app.get("/sessions/{session_id}/status")
async def session_status(session_id: str):
    """Get session runtime status including last activity timestamp."""
    _, _, state = await ensure_session_live(session_id)
    now = time.time()
    return {
        "session_id": state.session_id,
        "agent_id": state.agent_id,
        "current_sandbox_id": state.sandbox_id,
        "inner_session_id": state.inner_session_id,
        "agent_busy": state.agent_busy,
        "active_rpc_id": state.active_rpc_id,
        "pending_count": len(state.pending_prompts),
        "session_subscriber_count": len(state._session_subscribers),
        "rpc_subscriber_count": sum(len(qs) for qs in state._rpc_subscribers.values()),
        "last_activity": state.last_activity,
        "idle_seconds": round(
            now - (state.turn_completed_at or state.last_activity), 1
        ),
        "has_client": state.client is not None,
        "shutdown_requested": state.shutdown.is_set(),
        "available_commands": state.available_commands,
        "supervisor_url": state.supervisor_url,
        "supervisor_port": state.supervisor_port,
    }


# ---------------------------------------------------------------------------
# Session log read endpoints
# ---------------------------------------------------------------------------


@app.get("/sessions/{session_id}/log")
async def get_session_log_route(session_id: str, limit: int = Query(default=500)):
    state = SESSIONS.get(session_id)
    pending = state._log_chain if state else None
    if pending is not None:
        try:
            await pending
        except Exception:
            pass
    entries = await get_session_log(session_id, limit=limit)
    return [
        {
            "id": e.id,
            "event_type": e.event_type,
            "payload": e.payload,
            "created_at": e.created_at,
        }
        for e in entries
    ]


# ---------------------------------------------------------------------------
# Session endpoints (keyed by session_id, use ensure_session_live)
# ---------------------------------------------------------------------------


@app.post("/sessions/{session_id}/resume")
async def session_resume(session_id: str, request: Request):
    """Resume a session by ID. Auto-recovers sandbox if stopped.

    Body may optionally carry ``env`` and ``secrets``:
      - ``env``:     PATCH semantics (missing=keep stored, {}=wipe, {...}=replace).
      - ``secrets``: same semantics — stored server-side (plaintext JSONB, see
        SECRETS_PLAINTEXT note). Used for this respawn and future auto-recoveries.
    """
    body_env: dict[str, str] | None = None
    body_secrets: dict[str, str] | None = None
    try:
        if request.headers.get("content-length", "0") != "0":
            body = await request.json()
            if isinstance(body, dict):
                body_env, body_secrets = _pop_env_and_secrets(body)
    except Exception:
        body_env = body_secrets = None

    # Persist updated env/secrets if the caller sent those fields. Failures
    # are logged but non-fatal — a read-only DB shouldn't block the resume.
    for label, updater, value in (
        ("env", update_session_env, body_env),
        ("secrets", update_session_secrets, body_secrets),
    ):
        if value is None:
            continue
        try:
            await updater(session_id, value)
        except Exception as e:
            log.warning("resume: update_session_%s failed for %s: %s", label, session_id, e)

    # ensure_session_live reads spawn_env from the DB row (via _build_spawn_env_from_row),
    # so the updated env/secrets persisted above are automatically picked up.
    _, sandbox, state = await ensure_session_live(session_id)
    return {
        "session_id": state.session_id,
        "agent_id": state.agent_id,
        # Same dual-key rationale as /sessions/quick: ``sandbox_id`` for the
        # REST/client convention, ``current_sandbox_id`` to match the DB
        # column + /sessions/{id} GET response shape.
        "sandbox_id": state.sandbox_id,
        "current_sandbox_id": state.sandbox_id,
        "inner_session_id": state.inner_session_id,
        "status": "resumed",
    }


@app.post("/sessions")
async def sessions_create(request: Request):
    """Create a new session bound to a volume (lazy sandbox provisioning).

    Body requires ``volume_id``; no sandbox is provisioned at this point.
    Returns ``{id, agent_id, volume_id, current_sandbox_id: null, connected: false}``.
    """
    data = await _json_body(request)
    # SECURITY: strip env/secrets first so they can't leak into agents.config.
    body_env, body_secrets = _pop_env_and_secrets(data)

    default_provider = data.get("provider") or data.get("config", {}).get("provider") or "local"
    volume_record = await _resolve_or_default_volume(data.get("volume_id"), default_provider)
    config_data = data.get("config", {})
    _merge_top_level_config(data, config_data)
    _forbid_auth_keys_in_env(config_data.get("env"), "config.env")
    # Default cwd lands on the volume mount (persistent), not /tmp — so
    # the agent's ~/ and working-dir files survive sandbox restart/replace.
    cwd = config_data.get("cwd", data.get("cwd", default_cwd_for_provider(default_provider)))

    agent_id = data.get("agent_id")
    if agent_id:
        await _require_agent(agent_id)
    else:
        agent_id = str(uuid.uuid4())
        await upsert_agent(AgentRecord(
            id=agent_id, name=data.get("name"),
            config=AgentConfig.from_dict(
                {**config_data, "agent_type": data.get("agent_type", "claude"), "cwd": cwd}
            ),
        ))

    session_id = str(uuid.uuid4())
    # Lazy mode: no sandbox provisioning here. current_sandbox_id = None.
    await upsert_session(
        session_id, agent_id, sandbox_id=None, inner_session_id=None,
        volume_id=volume_record.id,
        env=body_env or {}, secrets=body_secrets or {},
    )

    return {
        "id": session_id,
        "agent_id": agent_id,
        "volume_id": volume_record.id,
        "current_sandbox_id": None,
        "connected": False,
    }


@app.post("/sessions/quick")
async def sessions_quick_create(request: Request):
    """Create agent + provision sandbox + connect in one call.

    Body requires ``volume_id``.
    Returns {agent_id, sandbox_id, session_id, connected: true}.
    """
    data = await _json_body(request)
    # SECURITY: strip env/secrets first so they can't leak into agents.config.
    body_env, body_secrets = _pop_env_and_secrets(data)

    provider = data.get("provider", "local")
    volume_record = await _resolve_or_default_volume(data.get("volume_id"), provider)
    volume_id = volume_record.id
    agent_type = data.get("agent_type", "claude")
    config_data = data.get("config", {})
    _merge_top_level_config(data, config_data)
    _forbid_auth_keys_in_env(config_data.get("env"), "config.env")
    # Default cwd lands on the volume mount (persistent), not /tmp — so
    # the agent's ~/ and working-dir files survive sandbox restart/replace.
    cwd = config_data.get("cwd", data.get("cwd", default_cwd_for_provider(provider)))
    root = config_data.get("root", data.get("root", cwd))
    dockerfile = _materialize_dockerfile(config_data)

    agent_id = str(uuid.uuid4())
    config = AgentConfig.from_dict({**config_data, "agent_type": agent_type, "cwd": cwd})
    await upsert_agent(AgentRecord(id=agent_id, name=data.get("name"), config=config))

    session_env = body_env or {}
    session_secrets = body_secrets or {}
    spawn_env = _merge_env(config.env, session_env, session_secrets)

    # Install skills BEFORE starting the supervisor — claude-agent-acp
    # discovers skills at process startup. For local: install on host.
    # For docker/daytona: run install commands inside the sandbox before start.
    skill_cmds = _skills_install_commands(config.skills) if config.skills else []
    if skill_cmds and provider == "local":
        try:
            await _install_skills_locally(config.skills)
        except Exception as e:
            log.error("skill install failed, continuing without skills: %s", e)
            skill_cmds = []

    sandbox_id = str(uuid.uuid4())
    subpath = f"agents/{agent_id}/home"

    # Install supervisor on the volume before spawning the sandbox.
    # Docker/Local need supervisor.js + node_modules under the volume at create
    # time. Idempotent fast-path on cache hit.
    try:
        await ensure_volume_supervisor(volume_id, agent_type)
    except Exception as e:
        await delete_agent(agent_id)
        log.error("sessions_quick_create: ensure_volume_supervisor failed: %s", e, exc_info=True)
        raise HTTPException(502, f"Failed to install supervisor on volume: {e}")

    try:
        instance = await create_instance(
            provider, agent_type, dockerfile=dockerfile,
            pre_start_commands=skill_cmds if provider != "local" else None,
            root=root, spawn_env=spawn_env,
            volume_id=volume_record.provider_ref, subpath=subpath,
            sandbox_id=sandbox_id,
        )
    except Exception as e:
        await delete_agent(agent_id)
        log.error("sessions_quick_create: create_instance failed (provider=%s): %s", provider, e, exc_info=True)
        # 503 + Retry-After tells callers to back off on circuit-breaker trips.
        if "circuit breaker" in str(e).lower():
            raise HTTPException(503, str(e), headers={"Retry-After": "30"})
        raise HTTPException(502, f"Provider '{provider}' failed: {e}")

    _INSTANCES[sandbox_id] = instance
    await upsert_sandbox(_sandbox_record(
        sandbox_id, provider, instance,
        volume_id=volume_id, subpath=subpath, root_fallback=root,
    ))

    async def _cleanup_and_raise(msg_fmt: str, e: Exception) -> None:
        """Shared teardown for post-upsert failures in /sessions/quick."""
        await delete_agent(agent_id)
        await delete_sandbox(sandbox_id)
        _INSTANCES.pop(sandbox_id, None)
        try:
            await destroy_instance(instance)
        except Exception as de:
            log.warning("sessions_quick_create cleanup: destroy_instance failed: %s", de)
        raise HTTPException(502, msg_fmt.format(e=e))

    # For Daytona, create_instance returns url="" (supervisor started lazily);
    # fill it in now so AcpClient has a real endpoint. Docker/Local already
    # started the supervisor inside create_sandbox.
    url = instance.url
    if not url:
        supervisor_port = allocate_sandbox_port(sandbox_id)
        try:
            url = await _providers_mod.ensure_supervisor_url(
                provider, instance,
                agent_type=agent_type, root=instance.root or root,
                spawn_env=spawn_env, port=supervisor_port,
            )
        except Exception as e:
            free_sandbox_port(sandbox_id, supervisor_port)
            await _cleanup_and_raise("Failed to start supervisor: {e}", e)
    else:
        supervisor_port = instance.port

    acp_session_id = str(uuid.uuid4())
    session_id = str(uuid.uuid4())
    client = AcpClient(url)
    # Brand-new session → inner_sid=None, so _attach_acp_session goes
    # straight to session/new. Routing through the shared helper keeps
    # sessions_quick_create / _ensure_runtime_locked / SSE-reader recovery
    # all using a single attach path, so the invariant "session/load first
    # when we have an inner_sid" can't accidentally get skipped here later.
    synthetic_agent = AgentRecord(id=agent_id, name=data.get("name"), config=config)
    try:
        inner_session_id, _ = await _attach_acp_session(
            client, acp_session_id, synthetic_agent,
            inner_sid=None, cwd=cwd,
        )
    except Exception as e:
        try:
            await client.aclose()
        except Exception:
            pass
        await _cleanup_and_raise("Failed to connect to ACP supervisor: {e}", e)
    state = SessionState(
        session_id=session_id, agent_id=agent_id, sandbox_id=sandbox_id,
        acp_session_id=acp_session_id, inner_session_id=inner_session_id,
        agent_type=config.agent_type or "claude",
        client=client,
        supervisor_url=url, supervisor_port=supervisor_port,
    )
    SESSIONS[session_id] = state
    _start_session_tasks(state)
    await upsert_session(
        session_id, agent_id, sandbox_id, inner_session_id,
        volume_id=volume_id,
        env=session_env, secrets=session_secrets,
    )

    # Dual-key response: ``sandbox_id`` matches /sandboxes + client code;
    # ``current_sandbox_id`` matches the DB column + /sessions/{id} GET.
    return {
        "agent_id": agent_id,
        "sandbox_id": sandbox_id,
        "current_sandbox_id": sandbox_id,
        "session_id": session_id,
        "inner_session_id": inner_session_id,
        "connected": True,
    }


# ---------------------------------------------------------------------------
# Scheduler loop — single owner of active_rpc_id, no locks needed
# ---------------------------------------------------------------------------

_CANCEL_DRAIN_TIMEOUT = 10  # seconds — safety cap so cancel doesn't hang


def _start_session_tasks(state: SessionState) -> None:
    """Start SSE reader + scheduler loop for a session."""
    log.info(
        "[SSE-READER] initializing session tasks for session %s (reader_alive=%s)",
        state.session_id,
        state._reader_alive,
    )
    _start_sse_reader(state)
    if state._scheduler_task is None or (
        hasattr(state._scheduler_task, "done") and state._scheduler_task.done()
    ):
        state._scheduler_task = asyncio.create_task(_scheduler_loop(state))


async def _scheduler_loop(state: SessionState) -> None:
    """Process prompts one at a time. Sole writer of active_rpc_id."""
    try:
        while not state.shutdown.is_set():
            await state._prompt_ready.wait()
            if state.shutdown.is_set():
                return
            while state.pending_prompts and not state.shutdown.is_set():
                pending = state.pending_prompts.popleft()
                state.active_rpc_id = pending.rpc_id
                state._prompt_done.clear()
                await _execute_one_prompt(state, pending.rpc_id, pending.message)
                state.active_rpc_id = None
                _mark_turn_finished(state)
                state._prompt_done.set()
            state._prompt_ready.clear()
    except asyncio.CancelledError:
        pass
    except Exception:
        log.exception("scheduler loop died for session %s", state.session_id)


def _classify_prompt_error(e: Exception, body: str) -> str:
    """Categorize a prompt failure for the error payload's ``kind`` field."""
    if isinstance(e, httpx.HTTPStatusError):
        if e.response.status_code == 500:
            if "agent process exited" in body or "start a new session" in body:
                return "sandbox_process_died"
            return "sandbox_internal_error"
        return "http_error"
    if isinstance(e, httpx.ConnectError):
        return "sandbox_unreachable"
    if isinstance(e, httpx.ReadTimeout):
        return "timeout"
    return "unknown"


async def _execute_one_prompt(state: SessionState, rpc_id: str, message: str) -> None:
    """Execute a single prompt HTTP round-trip. Called only from the scheduler loop."""
    session_id = state.session_id
    await log_event(
        session_id=session_id, agent_id=state.agent_id, sandbox_id=state.sandbox_id,
        event_type=EVT_USER_MESSAGE, payload={"text": message, "prompt_id": rpc_id},
    )
    try:
        await state.client.prompt(state.acp_session_id, message, rpc_id=rpc_id)
        return
    except Exception as e:
        err = e
        tb = traceback.format_exc()
        log.exception("prompt failed for session %s", session_id)

    body = ""
    http_status: int | None = None
    if isinstance(err, httpx.HTTPStatusError):
        http_status = err.response.status_code
        try:
            body = err.response.text[:1000]
        except Exception:
            pass
    kind = _classify_prompt_error(err, body)
    summary = f"{type(err).__name__}: {err}" + (f" | {body}" if body else "")

    state.errors.append({
        "ts": time.time(), "rpc_id": rpc_id, "kind": kind,
        "error": summary, "traceback": tb,
    })
    _mark_turn_finished(state)

    error_payload = json.dumps({
        "jsonrpc": "2.0", "id": rpc_id,
        "error": {
            "code": -32000, "message": summary[:500],
            "data": {
                "kind": kind, "exception_type": type(err).__name__,
                "http_status": http_status, "upstream_body": body,
                "rpc_id": rpc_id,
            },
        },
    })
    if not state.shutdown.is_set():
        state.dispatch(rpc_id, (rpc_id, f"data: {error_payload}\n\n"))

    await log_event(
        session_id=session_id, agent_id=state.agent_id, sandbox_id=state.sandbox_id,
        event_type=EVT_ERROR,
        payload={"message": summary[:1000], "kind": kind,
                 "traceback": tb[:5000], "rpc_id": rpc_id},
    )


def _submit_prompt(state: SessionState, rpc_id: str, message: str) -> None:
    """Enqueue a prompt and wake the scheduler loop. Returns immediately."""
    state.pending_prompts.append(PendingPrompt(rpc_id=rpc_id, message=message))
    state._prompt_ready.set()


async def _cancel_and_drain(state: SessionState) -> None:
    """Cancel the active prompt and wait for it to reach a terminal state."""
    if not state.agent_busy:
        return
    await state.client.cancel_prompt(state.acp_session_id)
    try:
        await asyncio.wait_for(state._prompt_done.wait(), timeout=_CANCEL_DRAIN_TIMEOUT)
    except asyncio.TimeoutError:
        log.warning(
            "_cancel_and_drain: timed out waiting for rpc %s", state.active_rpc_id
        )


@app.post("/sessions/{session_id}/message")
async def post_session_message(session_id: str, request: Request):
    """Submit a prompt. Queued behind the active prompt automatically.

    Pass ``interrupt: true`` to cancel the running prompt and wait for it
    to reach a terminal state before the new prompt starts.
    """
    data = await _json_body(request)
    message = data.get("message")
    if not message:
        raise HTTPException(400, "message required")

    interrupt = data.get("interrupt", False)

    _, _, state = await ensure_session_live(session_id)

    if interrupt and state.agent_busy:
        await _cancel_and_drain(state)

    # If the upstream SSE reader died while idle, recreate it before
    # queuing the next prompt when subscribers are still waiting for events.
    if state._session_subscribers and not state._reader_alive:
        log.info(
            "[SSE-READER] auto-restarting upstream reader from /message for session %s "
            "(session_subscribers=%d)",
            state.session_id,
            len(state._session_subscribers),
        )
        _start_sse_reader(state)

    state.last_activity = time.time()
    rpc_id = str(uuid.uuid4())
    _submit_prompt(state, rpc_id, message)
    return {"rpc_id": rpc_id, "status": "ok"}


@app.get("/sessions/{session_id}/events")
async def session_events(session_id: str):
    """SSE stream for a session. Recovers reaped sessions automatically."""
    _, _, state = await ensure_session_live(session_id)
    shutdown = state.shutdown

    async def _proxy_stream():
        heartbeat_interval = int(os.environ.get("SSE_HEARTBEAT_INTERVAL", "30"))

        # Restart persistent reader if it died (e.g., supervisor restart)
        if not state._reader_alive:
            log.info(
                "[SSE-READER] auto-restarting upstream reader from /events for session %s",
                state.session_id,
            )
            _start_sse_reader(state)

        my_q = state.subscribe_session()

        async def _heartbeat_loop():
            # Per-connection heartbeat: put None directly on THIS subscriber's
            # queue instead of ``state.broadcast(None)`` (which was O(N^2) across
            # subscribers). Skip if the consumer is backlogged — real events in
            # the queue are already keeping the connection warm.
            try:
                hb_count = 0
                while True:
                    await asyncio.sleep(heartbeat_interval)
                    try:
                        my_q.put_nowait(None)
                    except asyncio.QueueFull:
                        pass
                    hb_count += 1
                    if hb_count % 4 == 0:  # log every ~2 min
                        log.debug("[HEARTBEAT] session %s: sent %d heartbeats, "
                                  "busy=%s, subs=%d, reader=%s",
                                  state.session_id[:8], hb_count,
                                  state.agent_busy, len(state._session_subscribers),
                                  state._reader_alive)
            except asyncio.CancelledError:
                pass

        heartbeat_task = asyncio.create_task(_heartbeat_loop())
        try:
            while True:
                item = await my_q.get()
                if item is _SSE_SENTINEL or item is _KICK_SENTINEL:
                    return
                if shutdown.is_set():
                    return
                if item is None:
                    yield ": heartbeat\n\n"
                    continue

                tag, block = item
                if tag is None:
                    yield block
                else:
                    yield f"event: rpc:{tag}\n{block}"
        except asyncio.CancelledError:
            pass
        except Exception as e:
            log.warning("SSE proxy error for session %s: %s", session_id, e)
        finally:
            await _cancel_task(heartbeat_task)
            state.unsubscribe_session(my_q)

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
    """Cancel the active prompt and wait for it to finish."""
    _, _, state = await ensure_session_live(session_id)
    if not state.agent_busy:
        return {"status": "ok", "detail": "not busy"}

    await state.client.cancel_prompt(state.acp_session_id)
    try:
        await asyncio.wait_for(state._prompt_done.wait(), timeout=_CANCEL_DRAIN_TIMEOUT)
    except asyncio.TimeoutError:
        log.warning("session_cancel: timed out waiting for rpc %s", state.active_rpc_id)
        raise HTTPException(504, "cancel timed out")
    return {"status": "ok"}


@app.post("/sessions/{session_id}/start-sandbox")
async def start_session_sandbox(session_id: str):
    """Eagerly provision a sandbox for a session (pre-warm). Idempotent."""
    _, sandbox, _ = await ensure_session_live(session_id)
    return {"sandbox_id": sandbox.id}


@app.post("/sessions/{session_id}/stop-sandbox", status_code=204)
async def stop_session_sandbox(session_id: str):
    """Kill the current sandbox. Next /message lazy-provisions a fresh one."""
    sess = await _require_session_row(session_id)
    sbid = sess.get("current_sandbox_id")
    if sbid is None:
        return  # 204, no-op — already stopped
    sb = await get_sandbox(sbid)
    if sb:
        # Snapshot the workspace before destroy so the next sandbox can
        # restore conversation state + session JSONLs + installed deps.
        # Best-effort: snapshot_supervisor swallows its own errors; a
        # failure here must not pin the sandbox alive.
        try:
            await snapshot_supervisor(sb)
        except Exception as e:
            log.warning(
                "stop_session_sandbox: snapshot failed for %s: %s; "
                "proceeding to destroy", sb.id, e,
            )
        inst = ProviderInstance(
            provider=sb.provider, url="",
            root=sb.root, sandbox_id=sb.sandbox_ref,
        )
        try:
            await _providers_mod.destroy_sandbox(sb.provider, inst)
        except Exception:
            pass  # best-effort
    await set_session_current_sandbox(session_id, None)
    await delete_sandbox(sbid)


@app.post("/sessions/{session_id}/reset-sandbox")
async def reset_session_sandbox(session_id: str):
    """Kill current sandbox and provision a fresh one."""
    sess = await _require_session_row(session_id)
    old_sbid = sess.get("current_sandbox_id")
    await stop_session_sandbox(session_id)
    # Re-read the row after stop (current_sandbox_id is now NULL), then provision
    # a replacement.  Call _provision_new directly (inside the session lock) so
    # the previous_id is passed through and the sandbox_reattach event is emitted.
    fresh_sess = await _require_session_row(session_id)
    async with _get_session_lock(session_id):
        sandbox = await _provision_new(fresh_sess, previous_id=old_sbid)
    return {"sandbox_id": sandbox.id}


@app.post("/sessions/{session_id}/config")
async def session_set_config(session_id: str, request: Request):
    """Set mode/model/thought_level for a session."""
    data = await _json_body(request)
    _, _, state = await ensure_session_live(session_id)
    try:
        if "mode" in data:
            await state.client.set_mode(state.acp_session_id, data["mode"])
        if "model" in data:
            await state.client.set_model(state.acp_session_id, data["model"])
        if "thought_level" in data:
            await state.client.set_thought_level(
                state.acp_session_id, data["thought_level"]
            )
        return {"status": "ok"}
    except Exception as e:
        raise HTTPException(502, str(e))


# ---------------------------------------------------------------------------
# Sandbox instance resolution
# ---------------------------------------------------------------------------


async def _resolve_sandbox_instance(sandbox_id: str) -> ProviderInstance:
    """Get a live ProviderInstance for a sandbox, auto-starting if needed.

    Raises HTTPException on failure. Uses agent_type="claude" for auto-start
    because all agent types share the same supervisor.js — only the ACP binary
    differs, and file browsing doesn't need ACP at all.
    """
    # Fast path: cached port-based instance is live as-is. Daytona preview
    # URLs can expire while the in-memory instance stays cached, so those
    # fall through to the liveness check below.
    instance = _INSTANCES.get(sandbox_id)
    if instance and instance.provider in PORT_BASED_PROVIDERS:
        return instance

    sandbox_record = await _require_sandbox(sandbox_id)
    try:
        await _ensure_sandbox_alive(sandbox_id, sandbox_record)
    except Exception as e:
        raise HTTPException(502, f"failed to start sandbox: {e}")
    instance = _INSTANCES.get(sandbox_id)
    if not instance:
        raise HTTPException(409, "sandbox not running")
    return instance


# ---------------------------------------------------------------------------
# Sandbox exec
# ---------------------------------------------------------------------------


@app.post("/sessions/{session_id}/sandbox/exec")
async def session_sandbox_exec(session_id: str, request: Request):
    """Run a command in the session's sandbox.

    Body: {"command": "...", "timeout": 30}
    Returns: {"stdout", "stderr", "exit_code", "stdout_truncated", "timed_out"}

    Auto-recovers: if the sandbox was reaped or stopped, restarts it
    before executing. Does not require an active ACP session.
    """
    data = await _json_body(request)
    command = data.get("command")
    if not command:
        raise HTTPException(400, "command required")
    timeout = min(data.get("timeout", 30), 300)

    _, sandbox, _ = await ensure_session_live(session_id)

    # Build a ProviderInstance from the SandboxRecord for exec.
    # exec_in_instance only needs provider + sandbox_id (=sandbox_ref) for Daytona.
    instance = ProviderInstance(
        provider=sandbox.provider,
        url="",
        root=sandbox.root,
        sandbox_id=sandbox.sandbox_ref,
    )

    try:
        result = await exec_in_instance(instance, command, timeout=timeout)
        return result.to_dict()
    except Exception as e:
        raise HTTPException(502, str(e))


# ---------------------------------------------------------------------------
# Sandbox filesystem browsing
# ---------------------------------------------------------------------------


async def _proxy_to_supervisor(
    sandbox_id: str, method: str, path: str, *,
    params: dict | None = None, json: dict | None = None,
    timeout: int = 30,
) -> Response:
    """Forward a request to the sandbox's supervisor and return its JSON response.

    Shared by every ``/sandboxes/{id}/files/*`` endpoint that returns JSON.
    For binary responses (see ``files/download``) the header-forwarding case is
    handled inline since it's unique.
    """
    instance = await _resolve_sandbox_instance(sandbox_id)
    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            r = await client.request(
                method, f"{instance.url}{path}", params=params, json=json,
            )
            return Response(
                content=r.content,
                status_code=r.status_code,
                media_type="application/json",
            )
    except httpx.HTTPError as e:
        raise HTTPException(status_code=502, detail=f"supervisor unreachable: {e}")


@app.get("/sandboxes/{sandbox_id}/files/tree")
async def sandbox_files_tree(sandbox_id: str):
    """Return the recursive directory tree of the sandbox root."""
    return await _proxy_to_supervisor(sandbox_id, "GET", "/v1/files/tree")


@app.get("/sandboxes/{sandbox_id}/files/read")
async def sandbox_files_read(sandbox_id: str, path: str):
    """Read a single file. Supervisor enforces path-traversal protection."""
    return await _proxy_to_supervisor(
        sandbox_id, "GET", "/v1/files/read", params={"path": path},
    )


@app.post("/sandboxes/{sandbox_id}/files/edit")
async def sandbox_files_edit(sandbox_id: str, request: Request):
    """Edit or create a file.

    Body: ``{"path": ..., "old_string": ..., "new_string": ..., "replace_all": bool}``.
    When ``old_string`` is empty, writes/creates the file with ``new_string`` as content.
    """
    return await _proxy_to_supervisor(
        sandbox_id, "POST", "/v1/files/edit",
        json=await _json_body(request),
    )


@app.post("/sandboxes/{sandbox_id}/files/upload")
async def sandbox_files_upload(sandbox_id: str, request: Request):
    """Upload a file. Body: ``{"path": ..., "content": "<base64>"}``."""
    return await _proxy_to_supervisor(
        sandbox_id, "POST", "/v1/files/upload",
        json=await _json_body(request), timeout=60,
    )


@app.post("/sandboxes/{sandbox_id}/files/delete")
async def sandbox_files_delete(sandbox_id: str, request: Request):
    """Delete a file or directory. Body: ``{"path": ...}``."""
    return await _proxy_to_supervisor(
        sandbox_id, "POST", "/v1/files/delete",
        json=await _json_body(request),
    )


@app.post("/sandboxes/{sandbox_id}/files/rename")
async def sandbox_files_rename(sandbox_id: str, request: Request):
    """Rename/move a file or directory. Body: ``{"path": ..., "new_path": ...}``."""
    return await _proxy_to_supervisor(
        sandbox_id, "POST", "/v1/files/rename",
        json=await _json_body(request),
    )


@app.get("/sandboxes/{sandbox_id}/files/download")
async def sandbox_files_download(sandbox_id: str, path: str):
    """Download a file as raw bytes (forwards content-type + disposition)."""
    instance = await _resolve_sandbox_instance(sandbox_id)
    try:
        async with httpx.AsyncClient(timeout=60) as client:
            r = await client.get(f"{instance.url}/v1/files/download", params={"path": path})
            return Response(
                content=r.content,
                status_code=r.status_code,
                media_type=r.headers.get("content-type", "application/octet-stream"),
                headers={"content-disposition": r.headers.get("content-disposition", "attachment")},
            )
    except httpx.HTTPError as e:
        raise HTTPException(status_code=502, detail=f"supervisor unreachable: {e}")


# ---------------------------------------------------------------------------
# Static UI
# ---------------------------------------------------------------------------

_UI_DIR = Path(__file__).parents[2] / "ui"
_UI_CACHE: dict[str, str] = {}


def _serve_ui_file(filename: str, label: str) -> Response:
    cached = _UI_CACHE.get(filename)
    if cached is None:
        try:
            cached = (_UI_DIR / filename).read_text()
        except FileNotFoundError:
            return PlainTextResponse(f"{label} not found", status_code=404)
        _UI_CACHE[filename] = cached
    return Response(content=cached, media_type="text/html")


@app.get("/ui")
async def serve_ui():
    """Serve the chat UI."""
    return _serve_ui_file("index.html", "UI")


@app.get("/ui/dashboard")
async def serve_dashboard():
    """Serve the validation dashboard."""
    return _serve_ui_file("dashboard.html", "Dashboard")


@app.get("/ui/files")
async def serve_files_ui():
    """Serve the filesystem browser UI."""
    return _serve_ui_file("fs.html", "Files UI")


@app.get("/ui/volumes")
async def serve_volumes_ui():
    """Serve the Volume Inspector UI."""
    return _serve_ui_file("volumes.html", "Volumes UI")
