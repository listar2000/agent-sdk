"""REST API server — agent/sandbox/session orchestration layer.

Run: uvicorn src.api.server:app --port 7778
"""

import asyncio
import base64
import json
import logging
import os
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
    allocate_sandbox_port,
    create_instance,
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
        level=level, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
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
) -> None:
    """Close a runtime session and optionally remove it from the active registry."""
    state.shutdown.set()
    # Re-check: new work may have arrived between the caller's idle check and here.
    if (
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
    await _close_session_gracefully(state)
    # Kill per-session supervisor if this session has its own.
    # Only Daytona runs multiple supervisors per sandbox (one per session);
    # docker/local always have one-supervisor-per-sandbox and teardown happens
    # via destroy_sandbox, not here.
    if state.supervisor_port is not None:
        try:
            sb = await get_sandbox(state.sandbox_id)
            provider = sb.provider if sb else None
            if provider == "daytona":
                from .providers.daytona import _get_daytona_client
                loop = asyncio.get_running_loop()
                daytona_client = _get_daytona_client()
                sandbox = await loop.run_in_executor(
                    None, lambda: daytona_client.get(sb.sandbox_ref)
                )
                await kill_supervisor_in_sandbox(sandbox, state.supervisor_port)
                free_sandbox_port(state.sandbox_id, state.supervisor_port)
            # For docker/local: no-op — the supervisor is the container/process
            # itself, and destroy_sandbox tears it down.
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


async def _idle_reaper():
    """Background task: close idle sessions that have been inactive too long."""
    while True:
        await asyncio.sleep(REAPER_TICK_S)
        now = time.time()
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
                        "idle reaper: stopping sandbox %s (provider=%s)",
                        sandbox_id,
                        instance.provider,
                    )
                    try:
                        await stop_instance(instance)
                        log.info("idle reaper: sandbox %s stopped", sandbox_id)
                        rec = await get_sandbox(sandbox_id)
                        if rec is not None and rec.status != STATUS_STOPPED:
                            rec.status = STATUS_STOPPED
                            await upsert_sandbox(rec)
                    except Exception as e:
                        log.warning(
                            "reaper: failed to stop sandbox %s: %s", sandbox_id, e
                        )


@asynccontextmanager
async def lifespan(app):
    _configure_logging()
    init_db()
    await init_pool()

    # Startup reconciliation: cross-reference live provider state with
    # DB sandbox rows so orphan containers/processes are reaped and
    # survivors are reattached to _INSTANCES. Guarded so a single
    # provider failure doesn't prevent boot.
    for _prov in ("docker", "daytona", "local"):
        try:
            await _providers_mod.reconcile_sandboxes(_prov)
        except Exception as e:
            log.warning("startup reconcile for %s failed: %s", _prov, e)

    reaper = asyncio.create_task(_idle_reaper())
    yield
    await _cancel_task(reaper)
    # Parallel session shutdown
    await asyncio.gather(
        *[_shutdown_session_state(s, remove=False) for s in SESSIONS.values()],
        return_exceptions=True,
    )
    SESSIONS.clear()

    # Parallel instance teardown. Use stop_instance (not destroy) so Daytona
    # sandboxes are stopped — not permanently deleted — across server restarts.
    # Without this, every container restart wipes all sandbox state, breaking
    # session recovery for all active users.
    async def _safe_stop(inst):
        try:
            await stop_instance(inst)
        except Exception as e:
            log.warning("shutdown cleanup failed: %s", e)

    await asyncio.gather(*[_safe_stop(i) for i in _INSTANCES.values()])
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
            log.info(
                "auto-approved permission for session %s (status=%d)",
                state.session_id,
                resp.status_code,
            )
        except Exception as e:
            log.warning(
                "auto-approve permission failed for session %s: %s", state.session_id, e
            )

    asyncio.create_task(_grant())


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
        log.info(
            "[SSE-READER] start requested but reader already alive for session %s",
            state.session_id,
        )
        return
    state._reader_alive = True
    log.info(
        "[SSE-READER] starting upstream reader for session %s (acp_session=%s, base_url=%s, last_event_id=%s)",
        state.session_id,
        state.acp_session_id,
        getattr(state.client, "base_url", "?"),
        state.last_event_id or "-",
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
                    log.info(
                        "[SSE-READER] connecting upstream stream for session %s (attempt=%d, last_event_id=%s)",
                        state.session_id,
                        attempt,
                        state.last_event_id or "-",
                    )
                    sse_http = httpx.AsyncClient(
                        base_url=state.client.base_url,
                        timeout=None,
                        proxy=None,
                    )
                    async with sse_http.stream(
                        "GET",
                        f"/v1/acp/{state.acp_session_id}",
                        headers=headers,
                    ) as resp:
                        resp.raise_for_status()
                        reconnect_delay_s = 1.0
                        attempt = 0
                        log.info(
                            "[SSE-READER] upstream stream connected for session %s (attempt=%d, status=%d)",
                            state.session_id,
                            attempt,
                            resp.status_code,
                        )
                        async for chunk in resp.aiter_text():
                            if state.shutdown.is_set():
                                log.info(
                                    "[SSE-READER] session %s shutting down; exiting reader loop",
                                    state.session_id,
                                )
                                return
                            reader_buffer += chunk
                            while "\n\n" in reader_buffer:
                                block, reader_buffer = reader_buffer.split("\n\n", 1)
                                payload = parse_sse_data(block)
                                _broadcast_one_block(
                                    state, block, payload, text_parts, thinking_parts
                                )
                except asyncio.CancelledError:
                    log.info(
                        "[SSE-READER] reader task cancelled for session %s",
                        state.session_id,
                    )
                    raise
                except Exception as e:
                    disconnect_reason = f"{type(e).__name__}: {e}"
                    log.warning(
                        "[SSE-READER] upstream reader error for session %s on attempt %d: %s",
                        state.session_id,
                        attempt,
                        disconnect_reason,
                    )
                else:
                    log.warning(
                        "[SSE-READER] upstream stream ended for session %s on attempt %d without an exception",
                        state.session_id,
                        attempt,
                    )
                finally:
                    if sse_http is not None:
                        try:
                            await sse_http.aclose()
                        except Exception:
                            pass

                if state.shutdown.is_set():
                    return

                if (
                    _sse_reader_disconnect_is_recoverable(state)
                    and attempt <= _SSE_MAX_IDLE_RETRIES
                ):
                    log.warning(
                        "[SSE-READER] recoverable upstream disconnect for session %s (%s); reconnecting in %.1fs (attempt %d/%d)",
                        state.session_id,
                        disconnect_reason,
                        reconnect_delay_s,
                        attempt,
                        _SSE_MAX_IDLE_RETRIES,
                    )
                    await asyncio.sleep(reconnect_delay_s)
                    reconnect_delay_s = min(reconnect_delay_s * 2, 10.0)
                    continue

                log.warning(
                    "[SSE-READER] unrecoverable upstream disconnect for session %s (%s) "
                    "(shutdown=%s, agent_busy=%s, pending=%d, in_SESSIONS=%s)",
                    state.session_id,
                    disconnect_reason,
                    state.shutdown.is_set(),
                    state.agent_busy,
                    len(state.pending_prompts),
                    state.session_id in SESSIONS,
                )
                _flush_buffered_text(
                    state, text_parts, thinking_parts, state.active_rpc_id
                )
                _on_sse_reader_death(state)
                state.broadcast(_SSE_SENTINEL)
                return
        except asyncio.CancelledError:
            pass
        finally:
            state._reader_alive = False
            log.info(
                "[SSE-READER] reader task stopped for session %s", state.session_id
            )

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
    """Initialize the ACP session with MCP server config."""
    await client.initialize(
        acp_session_id,
        config.agent_type or "claude",
        cwd=cwd,
        mcp_servers=config.mcp_servers,
    )


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
    """
    if not env:
        return
    from .providers import AUTH_KEYS
    offenders = sorted(k for k in env if k in AUTH_KEYS)
    if offenders:
        raise HTTPException(
            status_code=400,
            detail=(
                f"{where}: auth keys {offenders} must be sent via 'secrets', "
                "not 'env' (env is stored plain and returned by GET)."
            ),
        )


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

    Any string/int/float value is coerced to str. No allowlist on key names —
    it's the user's sandbox. SECURITY: both fields are popped in-place so they
    can't flow into ``config_data``, ``AgentConfig``, or request logs.
    """
    def _coerce(d: object) -> dict[str, str]:
        if not isinstance(d, dict):
            return {}
        out: dict[str, str] = {}
        for k, v in d.items():
            if isinstance(k, str) and isinstance(v, (str, int, float)):
                out[k] = str(v)
        return out

    raw_env = data.pop("env", _ENV_MISSING)
    raw_secrets = data.pop("secrets", _ENV_MISSING)
    env = None if raw_env is _ENV_MISSING else _coerce(raw_env)
    secrets = None if raw_secrets is _ENV_MISSING else _coerce(raw_secrets)
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


async def _build_spawn_env_for_session(session_id: str) -> dict[str, str]:
    """Assemble the spawn_env dict for a stored session. Returns ``{}`` if the
    session doesn't exist (caller should handle that separately)."""
    rec = await get_session(session_id)
    if rec is None:
        return {}
    return await _build_spawn_env_from_row(rec)


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
    """Build the env dict that lands in a supervisor subprocess.

    Precedence (later wins): agent.env → session.env → secrets.
    Returns a fresh dict; never mutates inputs. Returns ``{}`` only if all
    three are empty/None — caller can decide whether that's an error.
    """
    out: dict[str, str] = {}
    if agent_env:
        out.update(agent_env)
    if session_env:
        out.update(session_env)
    if secrets:
        out.update(secrets)
    return out


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
    _forbid_auth_keys_in_env(config_data.get("env"), "config.env")
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
# Volume CRUD
# ---------------------------------------------------------------------------


class _VolumeCreateBody(BaseModel):
    name: str
    provider: str


def _gen_volume_id() -> str:
    return f"vol_{uuid.uuid4().hex[:12]}"


async def _resolve_volume(id_or_name: str) -> "VolumeRecord":
    vol = await get_volume(id_or_name)
    if vol is None:
        vol = await get_volume_by_name(id_or_name)
    if vol is None:
        raise HTTPException(404, "Volume not found")
    return vol


@app.post("/volumes")
async def create_volume(body: _VolumeCreateBody):
    # Reject duplicates up front so we never orphan a provider-side volume.
    if await get_volume_by_name(body.name) is not None:
        raise HTTPException(409, f"Volume '{body.name}' already exists")

    # Mi4: single dispatch path through the uniform ``create_volume`` helper.
    # Tests that used to patch ``api.providers.create_daytona_volume`` should
    # instead patch ``api.providers.daytona.create_daytona_volume`` (the
    # underlying function the dispatcher calls for provider="daytona") —
    # that's the function the dispatcher resolves via ``_PROVIDER_MODS``.
    if body.provider not in _providers_mod._PROVIDER_MODS:
        raise HTTPException(400, f"Unknown provider: {body.provider}")
    provider_ref = await _providers_mod.create_volume(body.provider, body.name)

    vol = VolumeRecord(
        id=_gen_volume_id(),
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
            # Sessions first (FK RESTRICT on sandbox is already SET NULL via
            # Task 4), then sandboxes (FK RESTRICT on volume blocks the final
            # delete unless we clear them).
            if session_count > 0:
                await conn.execute(
                    "DELETE FROM sessions WHERE volume_id = %s", (vol.id,),
                )
            if sandbox_count > 0:
                await conn.execute(
                    "DELETE FROM sandboxes WHERE volume_id = %s", (vol.id,),
                )

    if vol.provider == "daytona":
        try:
            await _providers_mod.delete_daytona_volume(vol.provider_ref)
        except Exception as e:
            # Tolerate 403 (delete-forbidden) and 404 (already gone): the
            # user's intent is to remove this volume from our records;
            # provider-side cleanup policies shouldn't block that.
            msg = str(e).lower()
            if "forbidden" not in msg and "not found" not in msg and "404" not in msg:
                raise
            log.warning("volume %s provider delete skipped: %s", vol.id, e)
    else:
        # Mi8: docker/local volumes previously leaked on DELETE because only
        # the daytona branch cleaned up the provider side.  Call the uniform
        # dispatcher; swallow only "not found"-class errors (volume is
        # already gone on the provider side; the DB row is the last copy).
        # Let "in use" errors propagate — an out-of-band container mounting
        # the volume is a real conflict the user should see, not a silent
        # orphan.  Matches the narrow whitelist the daytona branch uses.
        try:
            await _providers_mod.delete_volume(vol.provider, vol.provider_ref)
        except Exception as e:
            msg = str(e).lower()
            if ("not found" in msg or "no such" in msg or "404" in msg
                    or "does not exist" in msg):
                log.warning(
                    "volume %s (%s) already gone on provider: %s",
                    vol.id, vol.provider, e,
                )
            else:
                raise HTTPException(
                    status_code=409,
                    detail=f"provider-side volume delete failed: {e}",
                )
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


@app.get("/volumes/{id_or_name}/files/tree")
async def volume_files_tree(id_or_name: str, path: str = ""):
    vol = await _resolve_volume(id_or_name)
    rel = _safe_path(path)
    try:
        tree = await _providers_mod.volume_tree(vol.provider, vol.provider_ref, rel)
    except NotImplementedError as e:
        raise HTTPException(501, f"File ops on {vol.provider} not implemented: {e}")
    return {"tree": tree}


@app.get("/volumes/{id_or_name}/files/read")
async def volume_files_read(id_or_name: str, path: str):
    vol = await _resolve_volume(id_or_name)
    rel = _safe_path(path)
    try:
        data = await _providers_mod.volume_read(vol.provider, vol.provider_ref, rel)
    except FileNotFoundError as e:
        raise HTTPException(404, f"File not found: {e}")
    except NotImplementedError as e:
        raise HTTPException(501, f"File ops on {vol.provider} not implemented: {e}")
    except Exception as e:
        raise HTTPException(500, f"Read failed: {e}")
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
    except NotImplementedError as e:
        raise HTTPException(501, f"File ops on {vol.provider} not implemented: {e}")
    except Exception as e:
        raise HTTPException(500, f"Edit failed: {e}")


# ---------------------------------------------------------------------------
# Sandbox CRUD
# ---------------------------------------------------------------------------


@app.post("/sandboxes")
async def create_sandbox(request: Request):
    data = await request.json()
    provider = data.get("provider", "local")
    agent_type = data.get("agent_type", "claude")
    root = data.get("root", "/tmp")
    volume_id = data.get("volume_id")
    subpath = data.get("subpath")
    if not volume_id or not subpath:
        return JSONResponse(
            {"error": "volume_id and subpath are required"}, status_code=400,
        )
    # ``_resolve_volume`` raises HTTPException(404) on miss; ``vol`` is
    # always a VolumeRecord here. The defensive ``vol is not None`` guard
    # the old code had was dead. (MI5)
    vol = await _resolve_volume(volume_id)
    if provider != vol.provider:
        return JSONResponse(
            {"error": f"provider {provider!r} does not match volume.provider {vol.provider!r}"},
            status_code=400,
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
        return JSONResponse(
            {"error": f"Provider '{provider}' failed: {e}"}, status_code=502
        )

    # Canonical sandbox_ref is the provider's native id (container_id / pid /
    # daytona sandbox id); the port travels separately in listen_port so
    # docker/local lookups (by container_id) still work.
    sandbox_ref = instance.sandbox_id or sandbox_id

    _INSTANCES[sandbox_id] = instance
    record = SandboxRecord(
        id=sandbox_id, provider=provider, sandbox_ref=sandbox_ref, status=STATUS_RUNNING,
        root=instance.root or root, volume_id=vol.id, subpath=subpath,
        listen_port=instance.port,
    )
    await upsert_sandbox(record)
    return {
        "id": sandbox_id,
        "provider": provider,
        "sandbox_ref": sandbox_ref,
        "status": "running",
        "root": record.root,
        "volume_id": vol.id,
        "subpath": subpath,
        "listen_port": instance.port,
        "url": instance.url or None,
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
    record = await get_sandbox(sandbox_id)
    if record is None:
        return JSONResponse({"error": "sandbox not found"}, status_code=404)
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
    record = await get_sandbox(sandbox_id)
    if record is None:
        return JSONResponse({"error": "sandbox not found"}, status_code=404)

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
    data = await request.json()
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

    volume_id = data.get("volume_id")
    subpath = data.get("subpath")
    if not volume_id or not subpath:
        return JSONResponse(
            {"error": "volume_id and subpath are required"}, status_code=400,
        )
    vol = await _resolve_volume(volume_id)
    # Default the provider from the volume (so existing Daytona-only clients
    # don't have to pass it), but let an explicit body field override for tests.
    provider = data.get("provider") or vol.provider
    # Mi5: ``provider != vol.provider`` is only reachable when the client
    # explicitly overrides ``provider`` in the body to something that
    # disagrees with the volume.  Tests exercise this to verify the
    # cross-provider rejection; normal clients omit ``provider`` and the
    # default (``vol.provider``) makes this branch unreachable.
    if provider != vol.provider:
        return JSONResponse(
            {"error": f"provider {provider!r} does not match volume.provider {vol.provider!r}"},
            status_code=400,
        )

    skill_cmds = _skills_install_commands(config.skills) if config.skills else []
    pre_start_commands = skill_cmds + (data.get("pre_start_commands") or [])

    # Install supervisor on the volume first — docker/local need this before
    # create_sandbox; daytona tolerates it (fast-path on cache hit).
    try:
        await ensure_volume_supervisor(vol.id, agent_type)
    except Exception as e:
        return JSONResponse(
            {"error": f"Failed to install supervisor on volume: {e}"},
            status_code=502,
        )

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
            return JSONResponse(
                {"error": str(e)}, status_code=503,
                headers={"Retry-After": "30"},
            )
        return JSONResponse({"error": f"Failed to provision sandbox: {e}"}, status_code=502)

    _INSTANCES[sandbox_id] = instance
    await upsert_sandbox(SandboxRecord(
        id=sandbox_id, provider=provider,
        sandbox_ref=instance.sandbox_id or sandbox_id, status=STATUS_RUNNING,
        root=instance.root or root,
        volume_id=vol.id, subpath=subpath,
        listen_port=instance.port,
    ))

    return {"sandbox_id": sandbox_id, "status": "provisioned",
            "volume_id": vol.id, "subpath": subpath, "provider": provider}


@app.post("/sandboxes/{sandbox_id}/stop")
async def stop_sandbox_route(sandbox_id: str):
    record = await get_sandbox(sandbox_id)
    if record is None:
        return JSONResponse({"error": "sandbox not found"}, status_code=404)

    # Hold the sandbox lock to prevent concurrent auto-restart
    # from restarting the sandbox while we're stopping it.
    #
    # IMPORTANT: stop the provider instance BEFORE flipping status="stopped"
    # in the DB. A crash between the flip and a successful stop_instance
    # would leave "stopped" in the DB with a live container still running,
    # and docker reconcile used to treat that as an orphan (cycle 4 MA3).
    # Reconcile now preserves stopped+live pairs, but we still want the DB
    # to reflect reality — only mark stopped after the provider confirms.
    #
    # Mi7: ``_INSTANCES`` is only popped AFTER the DB update succeeds.  If
    # the UPDATE fails we keep the entry so reconcile / retry can still
    # find the container via the in-process map; otherwise the container
    # has exited (stop_instance returned) but the DB row still says
    # "running" and the map is empty — a subsequent _ensure_sandbox_alive
    # would provision a second container with the same label.
    async with _get_sandbox_lock(sandbox_id):
        instance = _INSTANCES.get(sandbox_id)
        if instance:
            try:
                await stop_instance(instance)
            except Exception as e:
                log.warning("stop_sandbox_route: stop_instance failed for %s: %s",
                            sandbox_id, e)
                return JSONResponse(
                    {"error": f"stop failed: {e}"}, status_code=502,
                )

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
            return JSONResponse(
                {"error": f"stop succeeded but DB update failed: {e}"},
                status_code=502,
            )

        # DB update succeeded — drop the in-process entry now.
        _INSTANCES.pop(sandbox_id, None)

    return {"status": "stopped"}


@app.post("/sandboxes/{sandbox_id}/start")
async def start_sandbox_route(sandbox_id: str):
    record = await get_sandbox(sandbox_id)
    if record is None:
        return JSONResponse({"error": "sandbox not found"}, status_code=404)

    try:
        url, _ = await _ensure_sandbox_alive(sandbox_id, record, agent_type="claude")
    except Exception as e:
        return JSONResponse({"error": f"failed to start sandbox: {e}"}, status_code=500)

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
        return JSONResponse({"error": "session not in memory"}, status_code=404)

    sandbox_id = state.sandbox_id
    await _shutdown_session_state(state, remove=True, mark_idle_at=time.time())

    stopped_provider: str | None = None
    if sandbox_id and not any(s.sandbox_id == sandbox_id for s in SESSIONS.values()):
        instance = _INSTANCES.pop(sandbox_id, None)
        if instance is not None:
            stopped_provider = instance.provider
            try:
                await stop_instance(instance)
            except Exception as e:
                log.warning(
                    "admin reap: stop_instance failed for %s: %s", sandbox_id, e
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
            # Flush any accumulated text/thinking before the tool call so the
            # text-tool interleave within the turn is preserved.
            _flush_buffered_text(state, text_parts, thinking_parts, prompt_id)
            tool_payload: dict = {
                "tool": extract_tool_name(update),
                "tool_call_id": extract_tool_call_id(update),
                "prompt_id": prompt_id,
            }
            title = update.get("title")
            if title:
                tool_payload["title"] = title
            raw_input = update.get("rawInput")
            if raw_input:
                tool_payload["args"] = raw_input
            _schedule_log(state, EVT_TOOL_CALL, tool_payload)

        elif log_events and ut == UT_TOOL_CALL_UPDATE:
            # tool_call_update can carry either a tool result (most common
            # — toolResponse / output / etc.) or a refined args set after
            # the initial tool_call.  Persist whichever is present, but
            # never re-emit a duplicate EVT_TOOL_CALL row.
            tool_call_id = extract_tool_call_id(update)
            tool_response = extract_tool_response(update)
            if tool_response is not None:
                result_payload: dict = {
                    "tool": extract_tool_name(update),
                    "tool_call_id": tool_call_id,
                    "result": tool_response,
                    "prompt_id": prompt_id,
                }
                title = update.get("title")
                if title:
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
        if log_events:
            _mark_turn_finished(state)
            _flush_buffered_text(state, text_parts, thinking_parts, prompt_id)
            result = data or {}
            done_payload: dict = {
                "stop_reason": result.get("stopReason"),
                "prompt_id": prompt_id,
            }
            usage = result.get("usage")
            if usage:
                done_payload["usage"] = usage
            _schedule_log(state, "turn_end", done_payload)
        else:
            text_parts.clear()
            thinking_parts.clear()
        return

    if kind == "error" and log_events:
        _mark_turn_finished(state)
        # Flush any in-flight text/thinking before the error frame.
        _flush_buffered_text(state, text_parts, thinking_parts, prompt_id)
        err = data or {}
        err_data = err.get("data") if isinstance(err.get("data"), dict) else {}
        _schedule_log(
            state,
            EVT_ERROR,
            {
                "message": err.get("message", str(err))[:500],
                "kind": err_data.get("kind") if err_data else None,
                "prompt_id": prompt_id,
            },
        )


def _should_replace_daytona_sandbox(exc: Exception) -> bool:
    """True when a Daytona recovery error means the old sandbox is gone for good."""
    text = str(exc).lower()
    return any(
        token in text
        for token in (
            "not found",
            "destroyed",
            "destroying",
            "terminal state",
            "unrecoverable",
            "unknown",
        )
    )


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
                        return instance.url, False  # local: still running
                elif instance.container_id:
                    # Docker: health-check URL
                    from .providers import _wait_for_health

                    if await _wait_for_health(
                        instance.url, max_retries=2, interval=0.5
                    ):
                        return instance.url, False

            # Clean up old container before creating replacement
            if instance:
                try:
                    await destroy_instance(instance)
                except Exception:
                    pass  # best-effort cleanup

            log.info("auto-restarting sandbox %s (provider=%s)", sandbox_id, provider)
            if spawn_env is None:
                spawn_env = await _spawn_env_for_sandbox(sandbox_id)

            # Resolve the volume so the replacement lands on the same mount
            # — Docker refuses an empty subpath and Local would start outside
            # the volume otherwise.
            if fresh_record.volume_id:
                vol = await get_volume(fresh_record.volume_id)
                if vol is None:
                    raise RuntimeError(
                        f"Sandbox {sandbox_id} references missing volume {fresh_record.volume_id}"
                    )
                volume_ref = vol.provider_ref
            else:
                raise RuntimeError(
                    f"Sandbox {sandbox_id} has no volume_id; cannot auto-restart"
                )
            subpath = fresh_record.subpath or ""

            try:
                new_instance = await _providers_mod.provision_sandbox(
                    provider,
                    volume_ref=volume_ref,
                    subpath=subpath,
                    agent_type=agent_type,
                    dockerfile=dockerfile,
                    root=sandbox_record.root,
                    spawn_env=spawn_env,
                    sandbox_id=sandbox_id,
                )
            except Exception as e:
                raise RuntimeError(f"Failed to restart sandbox: {e}")

            _INSTANCES[sandbox_id] = new_instance
            await upsert_sandbox(
                SandboxRecord(
                    id=sandbox_id,
                    provider=provider,
                    sandbox_ref=new_instance.sandbox_id or sandbox_id,
                    status=STATUS_RUNNING,
                    root=sandbox_record.root,
                    volume_id=sandbox_record.volume_id,
                    subpath=sandbox_record.subpath,
                    listen_port=new_instance.port,
                )
            )
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
        log.info(
            "recovering daytona sandbox %s (daytona_id=%s)",
            sandbox_id,
            daytona_sandbox_id,
        )
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
                daytona_sandbox_id,
                sandbox_id,
                e,
            )
            # Preserve the volume_id + subpath so the replacement mounts the
            # same storage. `create_instance` accepts both kwargs; we resolve
            # the Daytona volume provider_ref for the mount API.
            dt_volume_ref: str | None = None
            if sandbox_record.volume_id:
                _vol = await get_volume(sandbox_record.volume_id)
                if _vol is not None:
                    dt_volume_ref = _vol.provider_ref
            try:
                new_instance = await create_instance(
                    "daytona", agent_type, dockerfile=dockerfile,
                    root=sandbox_record.root,
                    spawn_env=spawn_env,
                    volume_id=dt_volume_ref,
                    subpath=sandbox_record.subpath,
                    sandbox_id=sandbox_id,
                )
            except Exception as create_err:
                raise RuntimeError(
                    f"Failed to create replacement daytona sandbox: {create_err}"
                )
            replaced = True

        _INSTANCES[sandbox_id] = new_instance
        await upsert_sandbox(
            SandboxRecord(
                id=sandbox_id,
                provider="daytona",
                sandbox_ref=new_instance.sandbox_id or sandbox_id,
                status=STATUS_RUNNING,
                root=sandbox_record.root,
                volume_id=sandbox_record.volume_id,
                subpath=sandbox_record.subpath,
                listen_port=new_instance.port,
            )
        )
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
    # switched to autocommit so the connection isn't stuck in a
    # long-running transaction while the slow provider call runs.
    # ``pg_advisory_lock`` survives commits (it's session-scoped) and
    # auto-releases on connection close, so a crashed worker can't
    # wedge the key.
    async with get_db() as lock_conn:
        # Autocommit mode: queries commit immediately and the connection
        # isn't parked "idle in transaction" during the slow install.
        # ``pg_advisory_lock`` is still session-scoped so the lock
        # outlives individual statement commits.  We MUST restore the
        # original autocommit setting before the connection returns to
        # the pool — psycopg's AsyncConnectionPool has no reset callback,
        # so a leaked `autocommit=True` silently breaks the
        # borrow-transaction-commit contract for every later borrower.
        _restore_autocommit = False
        try:
            await lock_conn.set_autocommit(True)
            _restore_autocommit = True
        except AttributeError:
            # Some test fakes expose a shim that doesn't implement
            # set_autocommit — tolerate it, the transaction-in-progress
            # semantics then match the previous code.
            pass
        except Exception as e:
            # Real psycopg connection rejected the flip — log loudly so
            # ops can investigate (advisory lock still works, but the
            # connection will be "idle in transaction" during the slow
            # install, which may trip idle-in-transaction timeouts).
            log.error(
                "ensure_volume_supervisor: set_autocommit(True) failed on "
                "pooled conn (volume=%s agent=%s): %s; falling back to "
                "transactional mode", volume_id, agent_type, e,
            )
        try:
            got_row = await (await lock_conn.execute(
                "SELECT pg_try_advisory_lock(%s)", (lock_key,)
            )).fetchone()
            if not (got_row and got_row.get("pg_try_advisory_lock")):
                log.info(
                    "ensure_volume_supervisor: waiting for another worker "
                    "(volume=%s agent=%s)", volume_id, agent_type,
                )
                await lock_conn.execute(
                    "SELECT pg_advisory_lock(%s)", (lock_key,)
                )

            # Step 3: re-check cache while holding the session lock.
            row2 = await (await lock_conn.execute(
                "SELECT supervisor_agent_types FROM volumes WHERE id = %s",
                (volume_id,),
            )).fetchone()
            installed2 = list((row2 or {}).get("supervisor_agent_types") or [])
            if agent_type in installed2:
                return

            log.info(
                "ensure_volume_supervisor: installing %s on volume %s",
                agent_type, volume_id,
            )
            # Step 4: slow provider call.  The DB connection is held (we
            # need it to keep the session-scoped advisory lock alive) but
            # it's in autocommit mode so it's NOT in a transaction — no
            # "idle in transaction" timeout, no dirty buffers.
            await _providers_mod.install_supervisor(
                provider, provider_ref, agent_type,
            )

            # Step 5: update the cache under the same session lock.
            await lock_conn.execute(
                "UPDATE volumes SET supervisor_agent_types = "
                "COALESCE(supervisor_agent_types, '[]'::jsonb) || to_jsonb(%s::text) "
                "WHERE id = %s AND NOT (supervisor_agent_types @> to_jsonb(%s::text))",
                (agent_type, volume_id, agent_type),
            )
            log.info(
                "ensure_volume_supervisor: done installing %s on volume %s",
                agent_type, volume_id,
            )
        finally:
            # Release the session lock explicitly so the connection is
            # clean when it returns to the pool.  A failure here is
            # survivable — the lock auto-releases on connection close.
            try:
                await lock_conn.execute(
                    "SELECT pg_advisory_unlock(%s)", (lock_key,)
                )
            except Exception as e:
                log.warning(
                    "ensure_volume_supervisor: pg_advisory_unlock failed: %s", e,
                )
            # Restore the pre-borrow autocommit mode BEFORE the async
            # context manager returns the connection to the pool. If the
            # restore itself fails, we must NOT return this connection to
            # the pool in autocommit=True — close it instead so the pool
            # opens a fresh one. Leaving a leaked autocommit=True in the
            # pool silently breaks every later borrower's commit contract.
            if _restore_autocommit:
                try:
                    await lock_conn.set_autocommit(False)
                except Exception as e:
                    log.error(
                        "ensure_volume_supervisor: failed to restore "
                        "autocommit=False on pooled conn: %s; closing to "
                        "prevent pool poisoning", e,
                    )
                    try:
                        await lock_conn.close()
                    except Exception as close_err:
                        log.warning(
                            "ensure_volume_supervisor: lock_conn.close() "
                            "after failed autocommit restore failed: %s",
                            close_err,
                        )


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

    # Provider-specific default root. Daytona mounts the per-agent subpath at
    # /home/daytona; docker mounts it at /home/agent; local's home is the host path.
    if vol.provider == "daytona":
        root = "/home/daytona"
    elif vol.provider == "docker":
        root = "/home/agent"
    else:
        # local: home is filled in by the provider from the volume path.
        root = None

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

    sb = SandboxRecord(
        id=new_sandbox_id,
        provider=vol.provider,
        sandbox_ref=inst.sandbox_id,
        status=STATUS_RUNNING,
        root=inst.root or root or "/tmp",
        volume_id=vol.id,
        subpath=subpath,
        listen_port=inst.port,
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

    # Reuse if the existing state is attached to this sandbox and the
    # supervisor is reachable.
    if (
        existing
        and not existing.shutdown.is_set()
        and existing.sandbox_id == sandbox.id
        and existing.supervisor_url
    ):
        try:
            from .providers import _wait_for_health
            ok = await _wait_for_health(existing.supervisor_url, max_retries=2, interval=1)
            if ok:
                return existing
        except Exception:
            pass
        # Supervisor dead — tear down and rebuild.
        await _shutdown_session_state(existing, remove=True)

    elif existing:
        # Stale: different sandbox or no URL — shut it down before rebuilding.
        await _shutdown_session_state(existing, remove=True)

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
        except Exception as exc:
            free_sandbox_port(sandbox.id, supervisor_port)
            raise HTTPException(500, f"Failed to start supervisor: {exc}") from exc

    client = AcpClient(supervisor_url)
    acp_session_id = str(uuid.uuid4())
    inner_sid = session_row.get("inner_session_id")
    # Use agent config cwd to avoid initializing into a directory that may
    # not exist on the sandbox filesystem.
    cwd = (agent_record.config.cwd or "/tmp") if agent_record.config else "/tmp"
    mcp = agent_record.config.mcp_servers if agent_record.config else None

    if inner_sid:
        # Reconnect to existing conversation on disk.
        await client.handshake(acp_session_id, agent_type)
        await client._send_rpc(acp_session_id, "session/load", {
            "sessionId": inner_sid, "cwd": cwd,
            "mcpServers": _mcp_dict_to_acp_array(mcp) if mcp else [],
        })
        client.set_inner_session_id(acp_session_id, inner_sid)
    else:
        # Fresh conversation — wrap in wait_for with generous timeout.
        await asyncio.wait_for(
            _apply_config_and_initialize(client, agent_record.config, acp_session_id, cwd),
            timeout=120,
        )
        inner_sid = client.get_inner_session_id(acp_session_id)
        if inner_sid:
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


async def require_session(session_id: str) -> dict:
    """Fetch session row or 404."""
    rec = await get_session(session_id)
    if rec is None:
        raise HTTPException(404, f"Session {session_id} not found")
    return rec


async def ensure_session_live(session_id: str) -> tuple[dict, SandboxRecord, SessionState]:
    """One-shot: session → sandbox → runtime. Most endpoints use this."""
    session = await require_session(session_id)
    sandbox = await ensure_sandbox(session)
    runtime = await ensure_runtime(session, sandbox)
    return session, sandbox, runtime






# ---------------------------------------------------------------------------
# (sandbox proxy endpoints removed — use ACP tools inside a turn instead)
# ---------------------------------------------------------------------------


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
    rec = await get_session(session_id)
    if rec is None:
        return JSONResponse({"error": "session not found"}, status_code=404)
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
    try:
        _, _, state = await ensure_session_live(session_id)
    except HTTPException as exc:
        return JSONResponse({"error": exc.detail}, status_code=exc.status_code)
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
        body_env = None
        body_secrets = None

    # Persist updated env/secrets if caller sent those fields.
    if body_env is not None:
        try:
            await update_session_env(session_id, body_env)
        except Exception as e:
            log.warning("resume: update_session_env failed for %s: %s", session_id, e)
    if body_secrets is not None:
        try:
            await update_session_secrets(session_id, body_secrets)
        except Exception as e:
            log.warning("resume: update_session_secrets failed for %s: %s", session_id, e)

    # ensure_session_live reads spawn_env from the DB row (via _build_spawn_env_from_row),
    # so the updated env/secrets persisted above are automatically picked up.
    try:
        _, sandbox, state = await ensure_session_live(session_id)
    except HTTPException as e:
        return JSONResponse({"error": e.detail}, status_code=e.status_code)
    return {
        "session_id": state.session_id,
        "agent_id": state.agent_id,
        "current_sandbox_id": state.sandbox_id,
        "inner_session_id": state.inner_session_id,
        "status": "resumed",
    }


@app.post("/sessions")
async def sessions_create(request: Request):
    """Create a new session bound to a volume (lazy sandbox provisioning).

    Body requires ``volume_id``; no sandbox is provisioned at this point.
    The sandbox will be provisioned lazily on first message (Task 13).
    Returns {id, agent_id, volume_id, current_sandbox_id: null, connected: false}.
    """
    data = await request.json()
    # SECURITY: strip env/secrets before any merge, log, or DB write so they
    # can't leak into agents.config JSONB.
    body_env, body_secrets = _pop_env_and_secrets(data)

    volume_id = data.get("volume_id")
    if not volume_id or not isinstance(volume_id, str):
        return JSONResponse(
            {"error": "volume_id is required"}, status_code=422
        )

    volume_record = await get_volume(volume_id)
    if volume_record is None:
        return JSONResponse({"error": "volume not found"}, status_code=404)

    agent_id = data.get("agent_id")
    agent_type = data.get("agent_type", "claude")
    name = data.get("name")
    config_data = data.get("config", {})
    _merge_top_level_config(data, config_data)
    _forbid_auth_keys_in_env(config_data.get("env"), "config.env")
    cwd = config_data.get("cwd", data.get("cwd", "/tmp"))

    # If agent_id was provided, look it up; otherwise create a new agent.
    if agent_id:
        agent_record = await get_agent(agent_id)
        if agent_record is None:
            return JSONResponse({"error": "agent not found"}, status_code=404)
    else:
        agent_id = str(uuid.uuid4())
        config = AgentConfig.from_dict(
            {**config_data, "agent_type": agent_type, "cwd": cwd}
        )
        await upsert_agent(AgentRecord(id=agent_id, name=name, config=config))

    session_env = body_env or {}
    session_secrets = body_secrets or {}

    session_id = str(uuid.uuid4())

    # Lazy mode: no sandbox provisioning here. current_sandbox_id = None.
    await upsert_session(
        session_id, agent_id, sandbox_id=None, inner_session_id=None,
        volume_id=volume_id,
        env=session_env, secrets=session_secrets,
    )

    return {
        "id": session_id,
        "agent_id": agent_id,
        "volume_id": volume_id,
        "current_sandbox_id": None,
        "connected": False,
    }


@app.post("/sessions/quick")
async def sessions_quick_create(request: Request):
    """Create agent + provision sandbox + connect in one call.

    Body requires ``volume_id``.
    Returns {agent_id, sandbox_id, session_id, connected: true}.
    """
    data = await request.json()
    # SECURITY: strip env/secrets before any merge, log, or DB write so they
    # can't leak into agents.config JSONB.
    body_env, body_secrets = _pop_env_and_secrets(data)

    volume_id = data.get("volume_id")
    if not volume_id or not isinstance(volume_id, str):
        return JSONResponse(
            {"error": "volume_id is required"}, status_code=422
        )

    volume_record = await get_volume(volume_id)
    if volume_record is None:
        return JSONResponse({"error": "volume not found"}, status_code=404)

    provider = data.get("provider", "local")
    agent_type = data.get("agent_type", "claude")
    name = data.get("name")
    config_data = data.get("config", {})
    _merge_top_level_config(data, config_data)
    _forbid_auth_keys_in_env(config_data.get("env"), "config.env")
    cwd = config_data.get("cwd", data.get("cwd", "/tmp"))
    root = config_data.get("root", data.get("root", cwd))
    dockerfile = _materialize_dockerfile(config_data)

    agent_id = str(uuid.uuid4())
    config = AgentConfig.from_dict(
        {**config_data, "agent_type": agent_type, "cwd": cwd}
    )
    await upsert_agent(AgentRecord(id=agent_id, name=name, config=config))

    session_env = body_env or {}
    session_secrets = body_secrets or {}
    spawn_env = _merge_env(config.env, session_env, session_secrets)

    # Install skills BEFORE starting the supervisor — claude-agent-acp
    # discovers skills at process startup, not at session creation time.
    # For local: install on host before spawning the supervisor.
    # For docker/daytona: pass install commands to run inside the sandbox
    # before the supervisor process starts.
    skill_cmds = _skills_install_commands(config.skills) if config.skills else []
    if skill_cmds and provider == "local":
        try:
            await _install_skills_locally(config.skills)
        except Exception as e:
            log.error("skill install failed, continuing without skills: %s", e)
            skill_cmds = []  # don't pass to create_instance

    sandbox_id = str(uuid.uuid4())
    subpath = f"agents/{agent_id}/home"

    # Install supervisor on the volume before spawning the sandbox.
    # Docker/Local need supervisor.js + node_modules under <vol>/system/supervisor/
    # to exist at create time or create_sandbox raises "supervisor.js missing".
    # Idempotent fast-path on cache hit.
    try:
        await ensure_volume_supervisor(volume_id, agent_type)
    except Exception as e:
        await delete_agent(agent_id)
        return JSONResponse(
            {"error": f"Failed to install supervisor on volume: {e}"},
            status_code=502,
        )

    try:
        instance = await create_instance(
            provider,
            agent_type,
            dockerfile=dockerfile,
            pre_start_commands=skill_cmds if provider != "local" else None,
            root=root,
            spawn_env=spawn_env,
            volume_id=volume_record.provider_ref,
            subpath=subpath,
            sandbox_id=sandbox_id,
        )
    except Exception as e:
        await delete_agent(agent_id)
        # Return 503 with Retry-After for circuit-breaker trips so callers
        # know to back off instead of retrying immediately.
        if "circuit breaker" in str(e).lower():
            return JSONResponse(
                {"error": str(e)},
                status_code=503,
                headers={"Retry-After": "30"},
            )
        return JSONResponse(
            {"error": f"Provider '{provider}' failed: {e}"}, status_code=502
        )

    # Store the provider-native id (container_id / pid / daytona sandbox id);
    # the listen port is persisted separately so docker/local lookups keep
    # working after a server restart.
    sandbox_ref = instance.sandbox_id or sandbox_id

    _INSTANCES[sandbox_id] = instance
    await upsert_sandbox(
        SandboxRecord(
            id=sandbox_id,
            provider=provider,
            sandbox_ref=sandbox_ref,
            status=STATUS_RUNNING,
            root=instance.root or root,
            volume_id=volume_id,
            subpath=subpath,
            listen_port=instance.port,
        )
    )

    url = instance.url
    acp_session_id = str(uuid.uuid4())
    session_id = str(uuid.uuid4())

    client = AcpClient(url)
    try:
        await _apply_config_and_initialize(
            client,
            config,
            acp_session_id,
            cwd,
        )
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
            log.warning(
                "sessions_quick_create cleanup: destroy_instance failed: %s", de
            )
        return JSONResponse(
            {"error": f"Failed to connect to ACP supervisor: {e}"}, status_code=502
        )

    inner_session_id = client.get_inner_session_id(acp_session_id)
    state = SessionState(
        session_id=session_id,
        agent_id=agent_id,
        sandbox_id=sandbox_id,
        acp_session_id=acp_session_id,
        inner_session_id=inner_session_id,
        agent_type=config.agent_type or "claude",
        client=client,
    )
    SESSIONS[session_id] = state
    _start_session_tasks(state)
    await upsert_session(
        session_id, agent_id, sandbox_id, inner_session_id,
        volume_id=volume_id,
        env=session_env, secrets=session_secrets,
    )

    return {
        "agent_id": agent_id,
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


async def _execute_one_prompt(state: SessionState, rpc_id: str, message: str) -> None:
    """Execute a single prompt HTTP round-trip. Called only from the scheduler loop."""
    session_id = state.session_id
    await log_event(
        session_id=session_id,
        agent_id=state.agent_id,
        sandbox_id=state.sandbox_id,
        event_type=EVT_USER_MESSAGE,
        payload={"text": message, "prompt_id": rpc_id},
    )
    try:
        await state.client.prompt(state.acp_session_id, message, rpc_id=rpc_id)
    except Exception as e:
        tb = traceback.format_exc()
        log.exception("prompt failed for session %s", session_id)

        body = ""
        http_status: int | None = None
        if isinstance(e, httpx.HTTPStatusError):
            http_status = e.response.status_code
            try:
                body = e.response.text[:1000]
            except Exception:
                pass

        if (
            isinstance(e, httpx.HTTPStatusError)
            and http_status == 500
            and ("agent process exited" in body or "start a new session" in body)
        ):
            kind = "sandbox_process_died"
        elif isinstance(e, httpx.HTTPStatusError) and http_status == 500:
            kind = "sandbox_internal_error"
        elif isinstance(e, httpx.HTTPStatusError):
            kind = "http_error"
        elif isinstance(e, httpx.ConnectError):
            kind = "sandbox_unreachable"
        elif isinstance(e, httpx.ReadTimeout):
            kind = "timeout"
        else:
            kind = "unknown"

        summary = f"{type(e).__name__}: {e}"
        if body:
            summary += f" | {body}"

        state.errors.append(
            {
                "ts": time.time(),
                "rpc_id": rpc_id,
                "kind": kind,
                "error": summary,
                "traceback": tb,
            }
        )
        _mark_turn_finished(state)

        error_payload = json.dumps(
            {
                "jsonrpc": "2.0",
                "id": rpc_id,
                "error": {
                    "code": -32000,
                    "message": summary[:500],
                    "data": {
                        "kind": kind,
                        "exception_type": type(e).__name__,
                        "http_status": http_status,
                        "upstream_body": body,
                        "rpc_id": rpc_id,
                    },
                },
            }
        )
        if not state.shutdown.is_set():
            state.dispatch(rpc_id, (rpc_id, f"data: {error_payload}\n\n"))

        await log_event(
            session_id=session_id,
            agent_id=state.agent_id,
            sandbox_id=state.sandbox_id,
            event_type=EVT_ERROR,
            payload={
                "message": summary[:1000],
                "kind": kind,
                "traceback": tb[:5000],
                "rpc_id": rpc_id,
            },
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
    data = await request.json()
    message = data.get("message")
    if not message:
        return JSONResponse({"error": "message required"}, status_code=400)

    interrupt = data.get("interrupt", False)

    try:
        _, _, state = await ensure_session_live(session_id)
    except HTTPException as exc:
        return JSONResponse({"error": exc.detail}, status_code=exc.status_code)

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
    return JSONResponse({"rpc_id": rpc_id, "status": "ok"})


@app.get("/sessions/{session_id}/events")
async def session_events(session_id: str):
    """SSE stream for a session. Recovers reaped sessions automatically."""
    try:
        _, _, state = await ensure_session_live(session_id)
    except HTTPException as exc:
        return JSONResponse({"error": exc.detail}, status_code=exc.status_code)

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
            try:
                hb_count = 0
                while True:
                    await asyncio.sleep(heartbeat_interval)
                    state.broadcast(None)  # heartbeat to all subscribers
                    hb_count += 1
                    if hb_count % 4 == 0:  # log every ~2 min
                        log.debug(
                            "[HEARTBEAT] session %s: sent %d heartbeats, "
                            "busy=%s, subs=%d, reader=%s",
                            state.session_id[:8], hb_count,
                            state.agent_busy, len(state._session_subscribers),
                            state._reader_alive,
                        )
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
    try:
        _, _, state = await ensure_session_live(session_id)
    except HTTPException as exc:
        return JSONResponse({"error": exc.detail}, status_code=exc.status_code)

    if not state.agent_busy:
        return {"status": "ok", "detail": "not busy"}

    await state.client.cancel_prompt(state.acp_session_id)
    try:
        await asyncio.wait_for(state._prompt_done.wait(), timeout=_CANCEL_DRAIN_TIMEOUT)
    except asyncio.TimeoutError:
        log.warning("session_cancel: timed out waiting for rpc %s", state.active_rpc_id)
        return JSONResponse({"error": "cancel timed out"}, status_code=504)
    return {"status": "ok"}


@app.post("/sessions/{session_id}/start-sandbox")
async def start_session_sandbox(session_id: str):
    """Eagerly provision a sandbox for a session (pre-warm). Idempotent."""
    try:
        _, sandbox, _ = await ensure_session_live(session_id)
    except HTTPException as exc:
        return JSONResponse({"error": exc.detail}, status_code=exc.status_code)
    return {"sandbox_id": sandbox.id}


@app.post("/sessions/{session_id}/stop-sandbox", status_code=204)
async def stop_session_sandbox(session_id: str):
    """Kill the current sandbox. Next /message lazy-provisions a fresh one."""
    sess = await require_session(session_id)
    sbid = sess.get("current_sandbox_id")
    if sbid is None:
        return  # 204, no-op — already stopped
    sb = await get_sandbox(sbid)
    if sb:
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
    sess = await require_session(session_id)
    old_sbid = sess.get("current_sandbox_id")
    await stop_session_sandbox(session_id)
    # Re-read the row after stop (current_sandbox_id is now NULL), then provision
    # a replacement.  Call _provision_new directly (inside the session lock) so
    # the previous_id is passed through and the sandbox_reattach event is emitted.
    fresh_sess = await require_session(session_id)
    async with _get_session_lock(session_id):
        sandbox = await _provision_new(fresh_sess, previous_id=old_sbid)
    return {"sandbox_id": sandbox.id}


@app.post("/sessions/{session_id}/config")
async def session_set_config(session_id: str, request: Request):
    """Set mode/model/thought_level for a session."""
    try:
        _, _, state = await ensure_session_live(session_id)
    except HTTPException as exc:
        return JSONResponse({"error": exc.detail}, status_code=exc.status_code)

    try:
        data = await request.json()
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
        return JSONResponse({"error": str(e)}, status_code=502)


# ---------------------------------------------------------------------------
# Sandbox instance resolution
# ---------------------------------------------------------------------------


async def _resolve_sandbox_instance(sandbox_id: str) -> ProviderInstance:
    """Get a live ProviderInstance for a sandbox, auto-starting if needed.

    Raises HTTPException on failure. Uses agent_type="claude" for auto-start
    because all agent types share the same supervisor.js — only the ACP binary
    differs, and file browsing doesn't need ACP at all.
    """
    instance = _INSTANCES.get(sandbox_id)
    sandbox_record = None
    if instance:
        if instance.provider in PORT_BASED_PROVIDERS:
            return instance
        # Daytona preview URLs can expire while the in-memory instance stays
        # cached. Refresh through the normal liveness path before tree/read.
        sandbox_record = await get_sandbox(sandbox_id)
        if not sandbox_record:
            raise HTTPException(status_code=404, detail="sandbox not found")
        try:
            await _ensure_sandbox_alive(sandbox_id, sandbox_record)
            instance = _INSTANCES.get(sandbox_id)
        except Exception as e:
            raise HTTPException(status_code=502, detail=f"failed to start sandbox: {e}")
        if not instance:
            raise HTTPException(status_code=409, detail="sandbox not running")
        return instance

    if sandbox_record is None:
        sandbox_record = await get_sandbox(sandbox_id)
    if not sandbox_record:
        raise HTTPException(status_code=404, detail="sandbox not found")
    try:
        await _ensure_sandbox_alive(sandbox_id, sandbox_record)
        instance = _INSTANCES.get(sandbox_id)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"failed to start sandbox: {e}")

    if not instance:
        raise HTTPException(status_code=409, detail="sandbox not running")
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
    data = await request.json()
    command = data.get("command")
    if not command:
        return JSONResponse({"error": "command required"}, status_code=400)
    timeout = min(data.get("timeout", 30), 300)

    try:
        _, sandbox, _ = await ensure_session_live(session_id)
    except HTTPException as exc:
        return JSONResponse({"error": exc.detail}, status_code=exc.status_code)

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
        return JSONResponse({"error": str(e)}, status_code=502)


# ---------------------------------------------------------------------------
# Sandbox filesystem browsing
# ---------------------------------------------------------------------------


@app.get("/sandboxes/{sandbox_id}/files/tree")
async def sandbox_files_tree(sandbox_id: str):
    """Return the recursive directory tree of the sandbox filesystem.

    Browses the supervisor's configured --root (the sandbox root).
    The root is not caller-controlled — the supervisor owns that decision.
    """
    instance = await _resolve_sandbox_instance(sandbox_id)
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            r = await client.get(f"{instance.url}/v1/files/tree")
            return Response(
                content=r.content,
                status_code=r.status_code,
                media_type="application/json",
            )
    except httpx.HTTPError as e:
        raise HTTPException(status_code=502, detail=f"supervisor unreachable: {e}")


@app.get("/sandboxes/{sandbox_id}/files/read")
async def sandbox_files_read(sandbox_id: str, path: str):
    """Read a single file from the sandbox filesystem.

    The `path` is relative to the sandbox root. The supervisor
    enforces path traversal protection — callers cannot escape the root.
    """
    instance = await _resolve_sandbox_instance(sandbox_id)
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            r = await client.get(
                f"{instance.url}/v1/files/read",
                params={"path": path},
            )
            return Response(
                content=r.content,
                status_code=r.status_code,
                media_type="application/json",
            )
    except httpx.HTTPError as e:
        raise HTTPException(status_code=502, detail=f"supervisor unreachable: {e}")


@app.post("/sandboxes/{sandbox_id}/files/edit")
async def sandbox_files_edit(sandbox_id: str, request: Request):
    """Edit or create a file in the sandbox filesystem.

    Body: {"path": "relative/path", "old_string": "...", "new_string": "...", "replace_all": false}
    When old_string is empty, writes/creates the file with new_string as content.
    The supervisor enforces path traversal protection.
    """
    instance = await _resolve_sandbox_instance(sandbox_id)
    body = await request.json()
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            r = await client.post(
                f"{instance.url}/v1/files/edit",
                json=body,
            )
            return Response(
                content=r.content,
                status_code=r.status_code,
                media_type="application/json",
            )
    except httpx.HTTPError as e:
        raise HTTPException(status_code=502, detail=f"supervisor unreachable: {e}")


@app.post("/sandboxes/{sandbox_id}/files/upload")
async def sandbox_files_upload(sandbox_id: str, request: Request):
    """Upload a file to the sandbox. Body: {"path": "...", "content": "<base64>"}"""
    instance = await _resolve_sandbox_instance(sandbox_id)
    body = await request.json()
    try:
        async with httpx.AsyncClient(timeout=60) as client:
            r = await client.post(f"{instance.url}/v1/files/upload", json=body)
            return Response(content=r.content, status_code=r.status_code, media_type="application/json")
    except httpx.HTTPError as e:
        raise HTTPException(status_code=502, detail=f"supervisor unreachable: {e}")


@app.post("/sandboxes/{sandbox_id}/files/delete")
async def sandbox_files_delete(sandbox_id: str, request: Request):
    """Delete a file or directory. Body: {"path": "..."}"""
    instance = await _resolve_sandbox_instance(sandbox_id)
    body = await request.json()
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            r = await client.post(f"{instance.url}/v1/files/delete", json=body)
            return Response(content=r.content, status_code=r.status_code, media_type="application/json")
    except httpx.HTTPError as e:
        raise HTTPException(status_code=502, detail=f"supervisor unreachable: {e}")


@app.post("/sandboxes/{sandbox_id}/files/rename")
async def sandbox_files_rename(sandbox_id: str, request: Request):
    """Rename/move a file or directory. Body: {"path": "...", "new_path": "..."}"""
    instance = await _resolve_sandbox_instance(sandbox_id)
    body = await request.json()
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            r = await client.post(f"{instance.url}/v1/files/rename", json=body)
            return Response(content=r.content, status_code=r.status_code, media_type="application/json")
    except httpx.HTTPError as e:
        raise HTTPException(status_code=502, detail=f"supervisor unreachable: {e}")


@app.get("/sandboxes/{sandbox_id}/files/download")
async def sandbox_files_download(sandbox_id: str, path: str):
    """Download a file as raw bytes."""
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
_UI_HTML: str | None = None
_FS_HTML: str | None = None


@app.get("/ui")
async def serve_ui():
    """Serve the chat UI."""
    global _UI_HTML
    if _UI_HTML is None:
        try:
            _UI_HTML = (_UI_DIR / "index.html").read_text()
        except FileNotFoundError:
            return PlainTextResponse("UI not found", status_code=404)
    return Response(content=_UI_HTML, media_type="text/html")


@app.get("/ui/dashboard")
async def serve_dashboard():
    """Serve the validation dashboard."""
    try:
        return Response(
            content=(Path(__file__).parents[2] / "ui" / "dashboard.html").read_text(),
            media_type="text/html",
        )
    except FileNotFoundError:
        return PlainTextResponse("Dashboard not found", status_code=404)


@app.get("/ui/files")
async def serve_files_ui():
    """Serve the filesystem browser UI."""
    global _FS_HTML
    if _FS_HTML is None:
        try:
            _FS_HTML = (_UI_DIR / "fs.html").read_text()
        except FileNotFoundError:
            return PlainTextResponse("Files UI not found", status_code=404)
    return Response(content=_FS_HTML, media_type="text/html")
