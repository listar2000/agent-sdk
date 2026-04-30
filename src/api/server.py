"""REST API server — agent/sandbox/session orchestration layer.

Run: uvicorn src.api.server:app --port 7778
"""

import asyncio
import base64
import hashlib
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
from dataclasses import dataclass
from pathlib import Path

import httpx
from psycopg.types.json import Json
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
    free_sandbox_port,
    kill_supervisor_in_sandbox,
    stop_instance,
)
from .providers._shared import _PROVIDER_VOLUME_HOME
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
    # Wake any /events handler waiting on a subscriber queue; without
    # this the UI stream stays blocked on an orphaned queue forever.
    state.kick_all()
    if remove and SESSIONS.get(state.session_id) is state:
        SESSIONS.pop(state.session_id, None)
        # Intentionally do NOT pop _session_locks: _ensure_runtime_locked
        # calls this from inside the held lock (e.g., server.py:2829 when
        # rebuilding a stale state). Popping here would let a concurrent
        # caller's _get_session_lock(sid) hit an empty dict and `setdefault`
        # a fresh Lock, breaking serialization. Two _ensure_runtime_locked
        # bodies would then run in parallel, mint two acp_session_ids, and
        # spawn two SSE readers for the same session — the daytona persistent-
        # SSE-after-delete race. The orphan Lock entry is small (an empty
        # waiters deque); a separate GC pass can prune cold ones if needed.


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
_SSE_MAX_IDLE_RETRIES = int(os.environ.get("SSE_MAX_IDLE_RETRIES", "2"))
# Upper bound on consecutive reconnect attempts before triggering sandbox
# recovery. Was 5 (~25s of backoff) — cut to 2 (~3s) because the unified
# recovery path (_recover_after_disconnect → _ensure_state_live) is cheap
# when the URL actually IS healthy (one 2-retry health probe, then return)
# and the 5-retry ladder was dominated by time wasted hammering a dead
# daytona preview URL.

# Per-stream read timeout for the upstream SSE drain. The supervisor
# (src/supervisor/supervisor.js) sends a `: heartbeat\n\n` SSE comment
# every SSE_HEARTBEAT_MS=25_000 ms, so under healthy operation we get a
# chunk at least every 25 s. Without a read timeout, daytona's signed
# preview proxy holds the TCP connection alive for ~5 minutes after the
# supervisor inside the sandbox dies — the SSE stream looks "open" but
# silent, and an in-flight prompt POST hangs on the same dead supervisor
# until the proxy's idle timeout finally drops the connection. Setting
# this to ~2x the heartbeat interval lets us declare the stream dead
# within ~60 s instead of ~300 s, which keeps recovery within the
# 180 s test budget for `test_session_resume_after_delete[daytona]`
# even when the freshly-provisioned replacement sandbox dies again.
_SSE_READ_TIMEOUT_S = float(os.environ.get("SSE_READ_TIMEOUT", "60"))


def _compute_supervisor_version() -> str:
    """Short hash of supervisor.js content. Used to invalidate the per-volume
    install cache when the supervisor source changes — without this, an updated
    supervisor.js (e.g., the agent_memory.tar visibility-poll fix) wouldn't be
    re-deployed to volumes whose ``supervisor_agent_types`` cache already lists
    the agent_type. Auto-deploys on next server restart, no manual DB write."""
    try:
        sup_path = Path(__file__).resolve().parents[1] / "supervisor" / "supervisor.js"
        return hashlib.sha256(sup_path.read_bytes()).hexdigest()[:8]
    except Exception:
        # If supervisor.js is somehow missing (test stubs, packaging quirks),
        # fall back to a placeholder so the cache key remains stable. The
        # fast path will then always treat the cache as authoritative.
        return "unknown"


_SUPERVISOR_VERSION = _compute_supervisor_version()


def _versioned_agent_type(agent_type: str) -> str:
    """Cache key combining agent_type with the current supervisor.js hash.

    A bare ``agent_type`` (e.g., ``"claude"``) cached under an older
    supervisor.js would silently keep stale logic on the volume forever.
    Including the hash means the install_supervisor cache is implicitly
    invalidated whenever supervisor.js changes — a future call to
    ``ensure_volume_supervisor`` for the same agent_type sees a different
    cache key, falls through to install, and writes the new versioned
    key on success.
    """
    return f"{agent_type}@{_SUPERVISOR_VERSION}"



async def _hibernate_session(state: SessionState) -> None:
    """Stop the sandbox compute, then either keep ``SessionState`` for a
    fast UI resume or evict it if no UI is watching.

    The next POST ``/message`` against this session goes through
    ``_ensure_runtime_locked`` → ``_ensure_state_live`` → ``_rebind_state``,
    which sees ``supervisor_url=None`` and a DB row with ``STATUS_STOPPED``
    and revives the same sandbox in place. After eviction, the next
    request rebuilds ``SessionState`` from the DB row first.

    Caller MUST hold ``_get_session_lock(state.session_id)`` — this races
    with /message's ensure path otherwise.

    Pre: ``state.active_rpc_id is None`` and ``state.pending_prompts`` is
    empty. The endpoint gates on this; the reaper does too.

    Subscriber policy:
      • Subscribers attached → keep state, leave queues bound, silent
        stream until next turn rebinds the SSE reader. Eviction fires
        from ``/events`` finally when the LAST subscriber drops.
      • No subscribers → evict immediately (no UI to benefit from cached
        state; otherwise headless callers would leak ``SessionState``
        forever since their drop trigger never fires).
    """
    sandbox_id = state.sandbox_id
    sb = await get_sandbox(sandbox_id)

    # Snapshot the supervisor BEFORE stopping compute so the next session/load
    # on resume finds the JSONLs on the volume.
    await _request_supervisor_snapshot(_INSTANCES.get(sandbox_id))

    # Stop compute uniformly across providers via the DB row. Robust to
    # a stale or missing _INSTANCES entry. For daytona this stops the
    # workspace (pauses billing); for docker/local this kills the
    # container/process.
    if sb is not None:
        try:
            await _providers_mod.stop_sandbox(sb.provider, _synthesize_instance(sb))
        except Exception as e:
            log.warning("hibernate: stop_sandbox failed for %s: %s", sandbox_id, e)
        if sb.status != STATUS_STOPPED:
            sb.status = STATUS_STOPPED
            await upsert_sandbox(sb)

    # Drop the in-process instance entry AFTER provider stop / DB flip so a
    # crash mid-stop leaves enough state for reconcile to find the container.
    _INSTANCES.pop(sandbox_id, None)
    state.lifecycle = "hibernated"

    # Return the per-session supervisor port to the per-sandbox allocator
    # (provider-agnostic). Today only daytona allocates one; others run a
    # single supervisor per sandbox at a fixed port and never set this field.
    if state.supervisor_port is not None:
        free_sandbox_port(sandbox_id, state.supervisor_port)

    # Cancel the SSE reader — the upstream stream just closed and we don't
    # want it racing rebind. The scheduler stays parked on _prompt_ready;
    # ensure_state_live restarts the reader when the next message arrives.
    await _cancel_task(state._reader_task)
    state._reader_task = None
    state._reader_alive = False
    state._reader_connected = False
    state.supervisor_url = None
    state.supervisor_port = None

    state.last_activity = time.time()
    log.info("hibernated session %s (sandbox %s, provider=%s, subs=%d)",
             state.session_id, sandbox_id,
             sb.provider if sb else "?",
             len(state._session_subscribers))

    # If nobody is watching this session, skip the "keep state in memory"
    # half — there is no UI to benefit from a fast rebind, and a headless
    # caller would otherwise leak SessionState forever (the
    # subscriber-drop eviction trigger never fires when no subscriber
    # ever existed). Symmetric with _maybe_evict_hibernated; the gates
    # we already passed at the call site (no active_rpc, no pending,
    # session lock held) cover the same conditions.
    if not state._session_subscribers and SESSIONS.get(state.session_id) is state:
        log.info("hibernated session %s has no subscribers — evicting state",
                 state.session_id)
        await _shutdown_session_state(state, remove=True, force=True)


async def _ensure_provider_sandbox_stopped(sandbox_id: str | None) -> None:
    """Defensively retry ``stop_sandbox`` at the provider before we drop
    our last in-memory reference to a sandbox.

    Backstops the silent-leak class: ``_hibernate_session`` swallows
    transient ``stop_sandbox`` failures (logs WARNING, flips DB to
    STOPPED, sets ``lifecycle="hibernated"``, pops ``_INSTANCES``), so
    the eviction paths that fire later believe the sandbox is stopped
    when it may still be RUNNING in the cloud (Daytona, Modal). Without
    this retry the workspace orphans in the provider with zero
    server-side memory of it.

    No-op when:
      * ``sandbox_id`` is None.
      * ``_INSTANCES`` still has an entry — the caller's hot-path teardown
        owns destruction of live instances.
      * Another live ``SessionState`` still references the sandbox.
      * The sandbox row is gone from the DB (already cleaned up).
    """
    if not sandbox_id:
        return
    if sandbox_id in _INSTANCES:
        return
    if any(s.sandbox_id == sandbox_id for s in SESSIONS.values()):
        return
    sb = await get_sandbox(sandbox_id)
    if sb is None:
        return
    try:
        await _providers_mod.stop_sandbox(sb.provider, _synthesize_instance(sb))
    except Exception as e:
        log.warning(
            "defensive stop_sandbox at eviction failed for %s (provider=%s): %s",
            sandbox_id, sb.provider, e,
        )


async def _maybe_evict_hibernated(state: SessionState) -> None:
    """If a hibernated session has lost its last subscriber, fully evict
    the in-memory ``SessionState``.

    Called from the ``/events`` ``finally`` block right after
    ``unsubscribe_session``. Cheap when not applicable — the early returns
    avoid acquiring the session lock unless we're actually going to evict.

    Conversation history and the sandboxes DB row stay intact; the next
    request rebuilds ``SessionState`` from the row via the normal recovery
    path.
    """
    if state._session_subscribers:
        return
    if state.shutdown.is_set():
        return  # already torn down
    if not state.is_hibernated:
        return  # sandbox is still up — keep state for the running session
    async with _get_session_lock(state.session_id):
        # Re-check under lock: a new /events handler may have subscribed,
        # or a /message may have woken the sandbox between our checks.
        if state._session_subscribers or not state.is_hibernated:
            return
        if state.active_rpc_id is not None or state.pending_prompts:
            return
        if SESSIONS.get(state.session_id) is not state:
            return  # already evicted by another path
        log.info(
            "evicting hibernated session %s after last subscriber dropped",
            state.session_id,
        )
        sandbox_id = state.sandbox_id
        await _shutdown_session_state(state, remove=True, force=True)
        # Defensive stop AFTER shutdown so the helper's "still in use"
        # check (any(s.sandbox_id == ...) over SESSIONS) doesn't see
        # this very session and bail out.
        await _ensure_provider_sandbox_stopped(sandbox_id)


async def _reap_one_tick(now: float) -> None:
    """Single iteration of the reap loop, factored out for testability.

    Hibernates any session idle past ``IDLE_TIMEOUT_S`` with no active or
    pending prompts. Subscribers no longer pin the sandbox open — an
    actively-watched but quiet UI gets hibernated too, and the session
    state survives the pause so resume is a single rebind. Full eviction
    of the in-memory state is driven by the last subscriber dropping
    (see ``_maybe_evict_hibernated``), not by this loop.

    Sessions whose sandbox is already stopped (``sandbox_id not in
    _INSTANCES``) are skipped — there's nothing left to hibernate.
    Durability lives at turn-end in the supervisor; the snapshot here is
    a belt-and-suspenders pre-stop save.
    """
    busy = sum(1 for s in SESSIONS.values() if s.agent_busy)
    readers = sum(1 for s in SESSIONS.values() if s._reader_alive)
    subs = sum(len(s._session_subscribers) for s in SESSIONS.values())
    log.info(
        "idle reaper tick: sessions=%d busy=%d readers=%d subs=%d instances=%d",
        len(SESSIONS), busy, readers, subs, len(_INSTANCES),
    )

    for state in list(SESSIONS.values()):
        if state.active_rpc_id is not None or state.pending_prompts:
            continue
        idle_since = _session_idle_since(state)
        if now - idle_since < IDLE_TIMEOUT_S:
            continue
        async with _get_session_lock(state.session_id):
            # Re-check under the lock — a concurrent /message may have
            # picked up work between the outer guards and here.
            if SESSIONS.get(state.session_id) is not state:
                continue
            if state.active_rpc_id is not None or state.pending_prompts:
                continue
            if not state.is_hibernated:
                # Running session: hibernate compute. _hibernate_session
                # also evicts state inline if no subscribers are attached.
                log.info(
                    "idle reaper: hibernating session %s (idle %.0fs, subs=%d)",
                    state.session_id, now - idle_since,
                    len(state._session_subscribers),
                )
                state.turn_completed_at = now
                await _hibernate_session(state)
            elif not state._session_subscribers:
                # Already-hibernated stuck state with no subscribers — the
                # subscriber-drop trigger would never fire here, so the
                # reaper is the only path. Evict directly.
                log.info(
                    "idle reaper: evicting hibernated idle session %s "
                    "(idle %.0fs, no subscribers)",
                    state.session_id, now - idle_since,
                )
                sandbox_id = state.sandbox_id
                await _shutdown_session_state(
                    state, remove=True, force=True, mark_idle_at=now,
                )
                # See note in _maybe_evict_hibernated — defensive stop
                # must follow shutdown so the "still in use" guard
                # doesn't trip on the session being evicted.
                await _ensure_provider_sandbox_stopped(sandbox_id)
            # else: hibernated + has subscribers — wait for them to drop;
            # /events finally → _maybe_evict_hibernated handles eviction.


def _sweep_session_locks() -> int:
    """Drop ``_session_locks`` entries for sessions no longer in SESSIONS,
    provided the lock is fully idle.

    The existing comment at ``_shutdown_session_state`` justifies *not*
    popping locks at session-evict time: a concurrent ``_ensure_runtime_locked``
    that's mid-acquire on the same session_id would otherwise see a fresh
    Lock from ``_get_session_lock(sid).setdefault`` and run in parallel with
    the original holder. We preserve that invariant here by sweeping ONLY
    when the lock is unlocked AND has no waiters — at that point no caller
    holds a stale reference, so a future ``_get_session_lock(sid)`` minting
    a new Lock cannot race.

    ``asyncio.Lock._waiters`` is CPython-internal but stable (it's a
    ``collections.deque`` of suspended ``acquire`` futures). The alternative
    — bookkeeping a "last touched" timestamp at every acquire — is hotter
    than this once-per-reaper-tick sweep.
    """
    if not _session_locks:
        return 0
    pruned = 0
    for sid in list(_session_locks):
        if sid in SESSIONS:
            continue
        lock = _session_locks.get(sid)
        if lock is None or lock.locked():
            continue
        if getattr(lock, "_waiters", None):
            continue
        # Re-check under the same tick — a concurrent /sessions create may
        # have just landed an entry into SESSIONS.
        if sid in SESSIONS:
            continue
        if _session_locks.pop(sid, None) is not None:
            pruned += 1
    return pruned


async def _idle_reaper():
    """Background task: close idle sessions that have been inactive too long."""
    while True:
        await asyncio.sleep(REAPER_TICK_S)
        await _reap_one_tick(time.time())
        pruned = _sweep_session_locks()
        if pruned:
            log.info("idle reaper: pruned %d cold session_locks (now %d)",
                     pruned, len(_session_locks))


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

    # All four providers go through the dispatch; ones without a
    # reconcile_on_startup hook (daytona, local, currently modal too) no-op.
    # Listing modal here means the moment its module gains a reconcile hook
    # we don't have to remember to wire it up.
    await asyncio.gather(*[_safe_reconcile(p) for p in ("docker", "daytona", "local", "modal")])

    reaper = asyncio.create_task(_idle_reaper())
    yield
    await _cancel_task(reaper)
    # force=True: a UI still holding /events would otherwise turn each
    # shutdown into a no-op and the reader's retry ladder blocks drain.
    await asyncio.gather(
        *[_shutdown_session_state(s, remove=False, force=True) for s in SESSIONS.values()],
        return_exceptions=True,
    )
    SESSIONS.clear()

    # Parallel instance teardown. Durability is already on the volume
    # from per-turn snapshots; stop is just SIGTERM here. Falls through
    # to stop_instance regardless of DB row state — the provider owns
    # liveness truth.
    async def _safe_stop(sid, inst):
        try:
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
    state.kick_all()


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


@dataclass
class _SseDisconnectInfo:
    """Result of one upstream SSE connection attempt.

    ``connected_successfully`` is True iff we got past raise_for_status and
    started reading chunks (regardless of whether drain later errored). Used
    by the outer loop to reset the retry budget — a 30-second drain followed
    by a transient error counts as "had one good connection," not "20 bad
    attempts."

    ``received_any_chunk`` distinguishes "drained data" from "connected and
    sat silent until the read timeout fired". Without that distinction a
    connect-then-silent loop would reset the retry budget on every iteration
    (because connected=True), preventing _recover_after_disconnect from ever
    firing when the supervisor is dead but daytona's proxy keeps the TCP
    connection alive.
    """
    reason: str                       # "upstream_eof" or "ExceptionType: msg"
    connected_successfully: bool
    supervisor_dead: bool             # cheap port-based fast-fail signal
    received_any_chunk: bool = False


async def _sse_reader_connect_and_drain(
    state: SessionState,
    attempt: int,
    text_parts: list[str],
    thinking_parts: list[str],
) -> _SseDisconnectInfo:
    """Open one upstream stream and drain it. Returns when the stream ends.

    CancelledError propagates to the caller; the outer reader's try/finally
    is the single owner of cancellation cleanup.
    """
    reader_buffer = ""
    disconnect_reason = "upstream_eof"
    connected = False
    received_any_chunk = False
    # Bound the per-chunk read at 2x the supervisor's heartbeat interval.
    # supervisor.js sends a `: heartbeat\n\n` SSE comment every 25 s, so
    # any 60 s gap means the supervisor (or the path to it) is gone — and
    # we must not wait the ~5 min daytona proxy idle timeout to find out.
    sse_http = httpx.AsyncClient(
        base_url=state.client.base_url,
        timeout=httpx.Timeout(
            connect=10.0, read=_SSE_READ_TIMEOUT_S, write=10.0, pool=10.0,
        ),
        proxy=None,
    )
    try:
        try:
            headers = {"Accept": "text/event-stream"}
            if state.last_event_id:
                headers["Last-Event-ID"] = state.last_event_id
            log.info("[SSE-READER] connecting upstream stream for session %s "
                     "(attempt=%d, last_event_id=%s)",
                     state.session_id, attempt, state.last_event_id or "-")
            async with sse_http.stream(
                "GET", f"/v1/acp/{state.acp_session_id}", headers=headers,
            ) as resp:
                resp.raise_for_status()
                connected = True
                state._reader_connected = True
                log.info("[SSE-READER] upstream stream connected for session %s "
                         "(attempt=%d, status=%d)",
                         state.session_id, attempt, resp.status_code)
                async for chunk in resp.aiter_text():
                    received_any_chunk = True
                    if state.shutdown.is_set():
                        log.info("[SSE-READER] session %s shutting down; "
                                 "exiting drain", state.session_id)
                        break
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
        state._reader_connected = False
        try:
            await sse_http.aclose()
        except Exception:
            pass

    # Fast-fail on confirmed-dead supervisors: port-based providers expose a
    # subprocess we can cheaply check, so we can skip the full retry ladder
    # (~35s of backoff) when the supervisor is definitely gone.
    cached_inst = _INSTANCES.get(state.sandbox_id)
    supervisor_dead = (
        cached_inst is not None
        and cached_inst.process is not None
        and not _instance_process_alive(cached_inst)
    )
    # Daytona analogue: if we connected but never saw a single chunk before
    # the read timeout fired, the supervisor (or the path to it) is dead —
    # heartbeats every 25 s mean any silent 60 s window is unrecoverable.
    # Treat this as supervisor_dead so the outer loop bypasses the connect
    # retry ladder and goes straight to _recover_after_disconnect.
    if connected and not received_any_chunk:
        supervisor_dead = True
    return _SseDisconnectInfo(
        reason=disconnect_reason,
        connected_successfully=connected,
        supervisor_dead=supervisor_dead,
        received_any_chunk=received_any_chunk,
    )


def _start_sse_reader(state: SessionState) -> None:
    """Start a background task that reads SSE from the upstream /v1/acp/{id}
    endpoint and broadcasts chunks to subscriber queues. Called at session
    creation so events are captured before any prompt is sent.

    The reader's recovery state machine (retry, recover, give-up) is
    expressed at the top of ``_reader``; the per-connection mechanics live
    in ``_sse_reader_connect_and_drain``.
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
                attempt += 1
                info = await _sse_reader_connect_and_drain(
                    state, attempt, text_parts, thinking_parts,
                )
                if info.connected_successfully and info.received_any_chunk:
                    # Successful connect that actually drained data resets
                    # the retry budget for the next failure — a 30-second
                    # healthy drain followed by a transient drop counts as
                    # one fresh attempt, not N. We DO NOT reset on
                    # connect-without-data: with a per-chunk read timeout
                    # in place, a connect-then-silent loop on a dead
                    # supervisor (daytona's proxy keeps the TCP open after
                    # the supervisor dies) would otherwise reset the budget
                    # on every iteration and never reach _recover.
                    attempt = 0
                    reconnect_delay_s = 1.0

                if state.shutdown.is_set():
                    return

                if (_sse_reader_disconnect_is_recoverable(state)
                        and attempt <= _SSE_MAX_IDLE_RETRIES
                        and not info.supervisor_dead):
                    log.warning("[SSE-READER] recoverable upstream disconnect for "
                                "session %s (%s); reconnecting in %.1fs (attempt %d/%d)",
                                state.session_id, info.reason,
                                reconnect_delay_s, attempt, _SSE_MAX_IDLE_RETRIES)
                    await asyncio.sleep(reconnect_delay_s)
                    reconnect_delay_s = min(reconnect_delay_s * 2, 10.0)
                    continue

                if info.supervisor_dead:
                    log.info(
                        "[SSE-READER] supervisor process dead for session %s — "
                        "skipping retry ladder, going straight to sandbox recovery",
                        state.session_id,
                    )

                # Retries exhausted — try to recover the sandbox before giving up.
                if await _recover_after_disconnect(state):
                    attempt = 0
                    reconnect_delay_s = 1.0
                    continue

                log.warning(
                    "[SSE-READER] unrecoverable upstream disconnect for session %s (%s) "
                    "(shutdown=%s, agent_busy=%s, pending=%d, in_SESSIONS=%s)",
                    state.session_id, info.reason, state.shutdown.is_set(),
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


async def _build_pre_start_commands(
    config, provider: str, user_cmds: list[str] | None,
) -> list[str] | None:
    """Build the combined pre-start command list for provisioning.

    Concatenates skill-install commands (from ``config.skills``) with
    caller-supplied ``user_cmds``, preserving order so skills land first.
    For the ``local`` provider we install skills on the host and return
    ``None`` — the local sandbox shares HOME with the server, so skill
    install runs once on the host and user commands there would execute
    with server privileges (deliberately unsupported).
    """
    skill_cmds = _skills_install_commands(config.skills) if config.skills else []
    if provider == "local":
        if skill_cmds:
            try:
                await _install_skills_locally(config.skills)
            except Exception as e:
                log.error("skill install failed, continuing without skills: %s", e)
        return None
    combined = skill_cmds + list(user_cmds or [])
    return combined or None


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
)

# Keys that were once inside AgentConfig but now live on session / sandbox
# rows. `/agents` POST rejects them with 400 so callers migrate cleanly;
# `/sessions` and `/sessions` consume them and route to the right row.
_AGENT_REJECTED_KEYS = ("cwd", "env", "dockerfile", "dockerfile_content", "shared_mounts")


def _merge_top_level_config(data: dict, config_data: dict) -> None:
    """Merge SDK top-level keys into config_data if not already present."""
    for key in _CONFIG_KEYS:
        if key in data and key not in config_data:
            config_data[key] = data[key]


def _coerce_env_dict(d: object, where: str) -> dict[str, str]:
    """Coerce a raw env/secrets value to a str→str dict.

    Non-dict inputs (null, string, list, …) collapse to ``{}``.
    Non-string keys are silently dropped.  Non-POSIX keys raise 400.
    String/int/float values are coerced via str(); other value types are
    silently dropped.
    """
    from .providers._shared import _ENV_KEY_RE

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
    from .providers import AUTH_KEYS

    env = _coerce_env_dict(data.pop("env"), "request body 'env'") if "env" in data else None
    secrets = _coerce_env_dict(data.pop("secrets"), "request body 'secrets'") if "secrets" in data else None

    if env:
        offenders = sorted(k for k in env if k in AUTH_KEYS)
        if offenders:
            raise HTTPException(400, f"request body 'env': auth keys {offenders} must be sent "
                                     "via 'secrets', not 'env' (env is stored plain "
                                     "and returned by GET).")

    return env, secrets


def _build_spawn_env_from_row(rec: dict) -> dict[str, str]:
    """Assemble spawn_env (session.env ∪ session.secrets) from a session row.

    Agent-level env is no longer a thing after the config-ownership split —
    session.env is the only non-secret source, session.secrets stacks on top.
    """
    return _merge_env(rec.get("env") or {}, rec.get("secrets") or {})


async def _spawn_env_for_sandbox(sandbox_id: str) -> dict[str, str]:
    """Best-effort spawn_env for a sandbox-level operation (start/exec/etc).

    All sessions on a given sandbox share the same supervisor process, so any
    session on the sandbox has the right env/secrets. If no session exists yet
    (e.g. provisioned but unused), returns ``{}`` — strict-mode will still
    strip auth keys, so auto-recovery simply has nothing extra to inject.
    """
    rec = await get_any_session_for_sandbox(sandbox_id)
    return _build_spawn_env_from_row(rec) if rec else {}


def _merge_env(*sources: dict[str, str] | None) -> dict[str, str]:
    """Merge env dicts (later sources win); None is treated as empty."""
    out: dict[str, str] = {}
    for s in sources:
        if s:
            out.update(s)
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


def _sandbox_record(
    sandbox_id: str,
    provider: str,
    instance: ProviderInstance,
    *,
    volume_id: str,
    subpath: str,
    root_fallback: str = "/tmp",
    status: str = STATUS_RUNNING,
    dockerfile: str | None = None,
    shared_mounts: list[str] | None = None,
) -> SandboxRecord:
    """Build a SandboxRecord from a freshly-provisioned ProviderInstance.

    ``dockerfile`` + ``shared_mounts`` are the provisioning identity of the
    sandbox — frozen at create time, read unchanged by later recoveries.
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
        dockerfile=dockerfile,
        shared_mounts=shared_mounts or [],
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
    # cwd / env / dockerfile / shared_mounts moved off AgentConfig — reject
    # them at the boundary so stale clients get a clear 400 instead of
    # silently-discarded fields.
    for k in _AGENT_REJECTED_KEYS:
        if k in data or k in config_data:
            raise HTTPException(
                400,
                f"'{k}' no longer belongs to agent config. "
                "cwd → session; env → session; dockerfile + shared_mounts → sandbox. "
                "Set these on POST /sessions or /sessions instead.",
            )
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

    Three endpoints share this contract (``POST /sandboxes``, ``/sessions``,
    ``/sessions``). Raises ``HTTPException(404)`` for an unknown
    id/name and ``HTTPException(502)`` if default-volume provisioning fails.
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
        # daytona.delete_volume is aliased to delete_daytona_volume; the
        # dispatcher (_providers_mod.delete_volume) routes correctly for
        # all providers, so no need to special-case daytona here.
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


class _VolumeUploadBody(BaseModel):
    path: str
    content: str  # base64-encoded


class _VolumePathBody(BaseModel):
    path: str


class _VolumeRenameBody(BaseModel):
    path: str
    new_path: str


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


@app.get("/volumes/{id_or_name}/files/download")
async def volume_files_download(id_or_name: str, path: str):
    """Download a volume file as raw bytes."""
    vol = await _resolve_volume(id_or_name)
    rel = _safe_path(path)
    try:
        data = await _providers_mod.volume_download(vol.provider, vol.provider_ref, rel)
    except Exception as e:
        raise _volume_fs_err("Download", vol.provider, e)

    filename = path.rsplit("/", 1)[-1] or "download"
    return Response(
        content=data,
        media_type="application/octet-stream",
        headers={"content-disposition": f'attachment; filename="{filename}"'},
    )


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


@app.post("/volumes/{id_or_name}/files/upload", status_code=204)
async def volume_files_upload(id_or_name: str, body: _VolumeUploadBody):
    vol = await _resolve_volume(id_or_name)
    rel = _safe_path(body.path)
    try:
        payload = base64.b64decode(body.content, validate=True)
    except Exception as e:
        raise HTTPException(400, f"invalid base64 content: {e}")
    try:
        await _providers_mod.volume_upload(vol.provider, vol.provider_ref, rel, payload)
    except Exception as e:
        raise _volume_fs_err("Upload", vol.provider, e)


@app.post("/volumes/{id_or_name}/files/mkdir", status_code=204)
async def volume_files_mkdir(id_or_name: str, body: _VolumePathBody):
    vol = await _resolve_volume(id_or_name)
    rel = _safe_path(body.path)
    try:
        await _providers_mod.volume_mkdir(vol.provider, vol.provider_ref, rel)
    except Exception as e:
        raise _volume_fs_err("Mkdir", vol.provider, e)


@app.post("/volumes/{id_or_name}/files/delete", status_code=204)
async def volume_files_delete(id_or_name: str, body: _VolumePathBody):
    vol = await _resolve_volume(id_or_name)
    rel = _safe_path(body.path)
    try:
        await _providers_mod.volume_delete(vol.provider, vol.provider_ref, rel)
    except Exception as e:
        raise _volume_fs_err("Delete", vol.provider, e)


@app.post("/volumes/{id_or_name}/files/rename", status_code=204)
async def volume_files_rename(id_or_name: str, body: _VolumeRenameBody):
    vol = await _resolve_volume(id_or_name)
    src = _safe_path(body.path)
    dst = _safe_path(body.new_path)
    try:
        await _providers_mod.volume_rename(vol.provider, vol.provider_ref, src, dst)
    except Exception as e:
        raise _volume_fs_err("Rename", vol.provider, e)


# ---------------------------------------------------------------------------
# Sandbox CRUD
# ---------------------------------------------------------------------------


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
    # Expose the supervisor's current PID for local provider so callers
    # that need to send signals (e.g. test harnesses) can do so without
    # coupling to the shape of sandbox_ref. Docker's container_id + daytona's
    # sandbox_id fill the same role on their own providers. Also expose
    # the alive-marker file path so an external "delete" can remove just
    # the marker without disturbing home (preserving volume semantics).
    if record.provider == "local":
        inst = _INSTANCES.get(record.id)
        if inst is not None and inst.process is not None:
            pid = getattr(inst.process, "pid", None)
            if pid is not None:
                result["pid"] = pid
        from .providers.local import _SPAWN_ARGS as _LOCAL_SPAWN_ARGS
        args = _LOCAL_SPAWN_ARGS.get(record.sandbox_ref)
        if args and args.get("marker_path"):
            result["marker_path"] = args["marker_path"]
    return result


@app.delete("/sandboxes/{sandbox_id}")
async def delete_sandbox_route(sandbox_id: str):
    await _require_sandbox(sandbox_id)
    async with _get_sandbox_lock(sandbox_id):
        # Snapshot before tearing down the compute so the next sandbox
        # provisioned on this volume's subpath can session/load with the
        # latest workspace state (per-turn snapshot was dropped in
        # 2026-04-23; this is the durability boundary).
        instance = _INSTANCES.get(sandbox_id)
        await _request_supervisor_snapshot(instance)

        # force=True so a UI holding a persistent /events stream doesn't
        # block the shutdown and leave a zombie state pointing at the
        # deleted sandbox. session.current_sandbox_id NULL-outs itself
        # via ON DELETE SET NULL on sandboxes_current_sandbox_id_fkey.
        for state in list(SESSIONS.values()):
            if state.sandbox_id == sandbox_id:
                await _shutdown_session_state(state, remove=True, force=True)

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


@app.post("/sandboxes")
async def create_sandbox(request: Request):
    """Create a sandbox on the provider selected by the request body.

    Body: ``{"volume_id": ..., "subpath": ..., "provider": "daytona"|"docker"|"local",
              "agent_type": ..., "config": {...}}``

    For Daytona the returned instance has no supervisor yet (started lazily by
    ``ensure_runtime``). For Docker/Local the supervisor is already running.
    Returns ``{id, sandbox_id, sandbox_ref, status, provider, root, volume_id,
    subpath, listen_port, url}``.
    """
    data = await _json_body(request)
    agent_type = data.get("agent_type", "claude")
    config_data = data.get("config", {})
    _merge_top_level_config(data, config_data)
    # Sandbox-level inputs come straight from the body now, not via AgentConfig.
    cwd = data.get("cwd", config_data.pop("cwd", "/tmp"))
    root = data.get("root", config_data.pop("root", cwd))
    dockerfile = _materialize_dockerfile({**config_data, **data})
    shared_mounts = data.get("shared_mounts") or config_data.pop("shared_mounts", None) or []
    config_data.pop("dockerfile", None)
    config_data.pop("dockerfile_content", None)
    config = AgentConfig.from_dict({**config_data, "agent_type": agent_type})

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

    pre_start_commands = await _build_pre_start_commands(
        config, provider, data.get("pre_start_commands") or [],
    )

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
        instance = await _provision_with_cache_retry(
            (vol.id, agent_type), _providers_mod.provision_sandbox,
            provider,
            volume_ref=vol.provider_ref, subpath=subpath,
            agent_type=agent_type, dockerfile=dockerfile,
            pre_start_commands=pre_start_commands,
            root=root, sandbox_id=sandbox_id,
            shared_mounts=shared_mounts or None,
        )
    except Exception as e:
        if "circuit breaker" in str(e).lower():
            raise HTTPException(503, str(e), headers={"Retry-After": "30"})
        raise HTTPException(502, f"Failed to provision sandbox: {e}")

    _INSTANCES[sandbox_id] = instance
    record = _sandbox_record(
        sandbox_id, provider, instance,
        volume_id=vol.id, subpath=subpath, root_fallback=root,
        dockerfile=dockerfile,
        shared_mounts=list(shared_mounts) if shared_mounts else [],
    )
    await upsert_sandbox(record)

    # Dual-key ``sandbox_id``/``id`` for back-compat with callers that learned
    # either key shape. ``sandbox_ref``/``root``/``listen_port``/``url`` match
    # the old POST /sandboxes response fields so migrating to this endpoint is
    # a straight rename — no field lookups to rewrite.
    return {
        "id": sandbox_id, "sandbox_id": sandbox_id,
        "provider": provider, "sandbox_ref": record.sandbox_ref,
        "status": "running",
        "root": record.root, "volume_id": vol.id, "subpath": subpath,
        "listen_port": instance.port, "url": instance.url or None,
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
            # Snapshot before stop so the next boot's session/load finds
            # the JSONLs on the volume (per-turn snapshot was dropped).
            await _request_supervisor_snapshot(instance)
            try:
                await stop_instance(instance)
            except Exception as e:
                log.warning("stop_sandbox_route: stop failed for %s: %s",
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


@app.post("/sandboxes/{sandbox_id}/snapshot")
async def snapshot_sandbox_route(sandbox_id: str):
    """Write the sandbox's workspace to the volume. Use before risky ops
    or when the user wants an explicit "save point". /sandboxes/{id}/stop,
    /sessions/{id}/hibernate, and the idle reaper already do this
    implicitly — call this endpoint only for mid-session saves.
    """
    await _require_sandbox(sandbox_id)
    instance = _INSTANCES.get(sandbox_id)
    if instance is None or not instance.url:
        raise HTTPException(409, "sandbox not running")
    if not await _request_supervisor_snapshot(instance.url):
        raise HTTPException(502, "supervisor snapshot failed")
    return {"status": "ok"}


@app.post("/sandboxes/{sandbox_id}/start")
async def start_sandbox_route(sandbox_id: str):
    """**Type 1 only** — revive an existing sandbox in place.

    Calls ``_type1_recover`` directly (no Type 2 fallback). If the
    underlying provider sandbox is missing or unrecoverable, returns
    ``409 Conflict`` rather than silently provisioning a replacement.

    Use ``POST /sessions/{id}/reset-sandbox`` for an explicit Type 2
    replacement, or ``POST /sandboxes`` to create a fresh sandbox.
    """
    record = await _require_sandbox(sandbox_id)
    async with _get_sandbox_lock(sandbox_id):
        # Fast path — already running.
        instance = _INSTANCES.get(sandbox_id)
        if instance is not None and await _instance_is_alive(instance):
            return {"status": "running", "url": instance.url}

        spawn_env = await _spawn_env_for_sandbox(sandbox_id)
        try:
            revived = await _type1_recover(
                sandbox_id, record, instance, "claude", spawn_env,
            )
        except Exception as e:
            # 502 matches POST /sandboxes — provider failures are upstream
            # faults, not server bugs (500).
            raise HTTPException(502, f"failed to start sandbox: {e}")

        if revived is None:
            raise HTTPException(
                409,
                "sandbox cannot be started in place — the underlying provider "
                "sandbox is missing or unrecoverable. Use "
                "POST /sessions/{id}/reset-sandbox for a Type 2 replacement, "
                "or POST /sandboxes to create a fresh one.",
            )

        _INSTANCES[sandbox_id] = revived
        # Type 1 keeps the same sandbox_ref + listen_port — the row only
        # needs its status flipped back to running.
        record.status = "running"
        await upsert_sandbox(record)
        return {"status": "running", "url": revived.url}


# ---------------------------------------------------------------------------
# Admin
# ---------------------------------------------------------------------------


@app.get("/admin/sessions")
async def admin_list_sessions():
    """List in-memory sessions and instances. Useful for debugging cleanup."""
    session_rows = list(SESSIONS.values())

    async def _sandbox_ref(s):
        try:
            rec = await get_sandbox(s.sandbox_id) if s.sandbox_id else None
            return rec.sandbox_ref if rec else None
        except Exception:
            return None

    refs = await asyncio.gather(*(_sandbox_ref(s) for s in session_rows))

    return {
        "sessions": [
            {
                "session_id": s.session_id,
                "agent_id": s.agent_id,
                "current_sandbox_id": s.sandbox_id,
                "sandbox_ref": ref,
                "inner_session_id": s.inner_session_id,
                "agent_busy": s.agent_busy,
                "active_rpc_id": s.active_rpc_id,
                "pending_count": len(s.pending_prompts),
                "session_subscribers": len(s._session_subscribers),
                "rpc_subscribers": sum(len(qs) for qs in s._rpc_subscribers.values()),
                "shutdown": s.shutdown.is_set(),
            }
            for s, ref in zip(session_rows, refs)
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
    await _shutdown_session_state(state, remove=True, force=True, mark_idle_at=time.time())

    stopped_provider: str | None = None
    if sandbox_id and not any(s.sandbox_id == sandbox_id for s in SESSIONS.values()):
        instance = _INSTANCES.pop(sandbox_id, None)
        if instance is not None:
            stopped_provider = instance.provider
            try:
                await stop_instance(instance)
            except Exception as e:
                log.warning("admin reap: stop failed for %s: %s",
                            sandbox_id, e)
        else:
            # _INSTANCES already empty (session was hibernated before reap).
            # Hibernate may have left the provider sandbox RUNNING if its
            # stop_sandbox call failed transiently. Retry stop here so the
            # operator's "make sure this is gone" hammer actually gets it
            # gone at the cloud provider, not just out of server memory.
            sb = await get_sandbox(sandbox_id)
            if sb is not None:
                try:
                    await _providers_mod.stop_sandbox(
                        sb.provider, _synthesize_instance(sb),
                    )
                    stopped_provider = sb.provider
                except Exception as e:
                    log.warning(
                        "admin reap: defensive stop_sandbox failed for %s "
                        "(provider=%s): %s",
                        sandbox_id, sb.provider, e,
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
    # A sandbox whose supervisor can't come up healthy is functionally
    # dead — a fresh sandbox on the same volume + subpath heals it
    # (session/load reads the persisted JSONL on the new sandbox).
    # Without these two tokens, _type1_recover keeps re-raising and the
    # caller never tries Type 2; users see 500s on every reconnect to
    # that wedged sandbox.
    "failed health check", "did not become",
)


def _should_replace_daytona_sandbox(exc: Exception) -> bool:
    """True when a Daytona recovery error means the old sandbox is gone for good."""
    text = str(exc).lower()
    return any(token in text for token in _DAYTONA_UNRECOVERABLE_TOKENS)


def _synthesize_instance(sandbox: SandboxRecord) -> ProviderInstance:
    """Minimal ProviderInstance suitable for stop_sandbox / destroy_sandbox."""
    return ProviderInstance(
        provider=sandbox.provider, url="",
        root=sandbox.root, sandbox_id=sandbox.sandbox_ref,
    )


async def _request_supervisor_snapshot(
    target: "ProviderInstance | str | None", timeout: float = 60.0,
) -> bool:
    """Fire POST /v1/snapshot on the supervisor. Returns True on 200.

    Accepts either a ``ProviderInstance`` (common) or a raw URL string.
    ``None`` / missing URL short-circuits so the 4 pre-stop call sites can
    drop their own ``if instance and instance.url`` guards.

    Called before stopping or reaping a sandbox so the next boot's
    session/load finds the JSONLs on the volume. Per-turn snapshots were
    dropped in 2026-04-23; the durability invariant now lives here.
    Best-effort — a failure logs but doesn't block the stop.
    """
    url = target if isinstance(target, str) else (target.url if target else "")
    if not url:
        return False
    try:
        async with httpx.AsyncClient(timeout=timeout) as c:
            r = await c.post(f"{url}/v1/snapshot")
            if r.status_code == 200:
                return True
            log.warning("supervisor snapshot returned %d: %s", r.status_code, r.text[:200])
    except Exception as e:
        log.warning("supervisor snapshot failed (%s): %s", url, e)
    return False


async def _instance_is_alive(inst: ProviderInstance) -> bool:
    """Cheap liveness check on a cached ProviderInstance.

    Port-based (local): trust the subprocess reference. Everything else
    (docker container, daytona preview URL) uses an HTTP health probe — the
    URL can expire or the supervisor inside a live sandbox can die
    independently.
    """
    if inst.process is not None:
        try: inst.process.poll()   # reap zombies, update returncode
        except Exception: pass
        return inst.process.returncode is None
    if not inst.url:
        return False
    from .providers import _wait_for_health
    try:
        return await _wait_for_health(inst.url, max_retries=2, interval=0.5)
    except Exception:
        return False




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
        "pre_start_commands": rec.get("pre_start_commands") or [],
    }


@app.get("/sessions/{session_id}/status")
async def session_status(session_id: str):
    """Get session status. Pool-backed; no compute provisioning."""
    from api.sandbox import deserialize, get_pool
    from api.sandbox.db_bindings import load_sandbox_state
    sess = await get_session(session_id)
    if sess is None:
        raise HTTPException(404, f"session {session_id} not found")
    state_payload = await load_sandbox_state(session_id)
    state = deserialize(state_payload)
    pool = get_pool()
    return {
        "session_id": session_id,
        "agent_id": sess["agent_id"],
        "current_sandbox_id": getattr(state, "sandbox_id", None),
        "inner_session_id": sess.get("inner_session_id"),
        "lifecycle": "active" if pool.has_active(session_id) else "hibernated",
        "snapshot_path": state.snapshot_path,
        "snapshot_version": state.snapshot_version,
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
    """Resume a session by ID. Pool acquires compute (cold-restores
    from snapshot if needed). Body may optionally carry ``env`` and
    ``secrets`` — persisted to the session row before pool.get_session
    so the new lease boots with the updated values."""
    from api.sandbox import get_pool

    body_env: dict[str, str] | None = None
    body_secrets: dict[str, str] | None = None
    try:
        if request.headers.get("content-length", "0") != "0":
            body = await request.json()
            if isinstance(body, dict):
                body_env, body_secrets = _pop_env_and_secrets(body)
    except Exception:
        body_env = body_secrets = None

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

    sess = await get_session(session_id)
    if sess is None:
        raise HTTPException(404, f"session {session_id} not found")
    session = await get_pool().get_session(session_id)
    return {
        "session_id": session_id,
        "agent_id": sess["agent_id"],
        "sandbox_id": getattr(session.state, "sandbox_id", None),
        "current_sandbox_id": getattr(session.state, "sandbox_id", None),
        "inner_session_id": sess.get("inner_session_id"),
        "status": "resumed",
    }


@app.post("/sessions")
async def sessions_create(request: Request):
    """Create a session. DB row only — compute is acquired lazily by
    the pool on first ``POST /message``. Per docs/ephemeral-sandbox-design.md
    §8 the legacy "eager" path that pre-warmed compute synchronously
    is gone; for explicit pre-warm, follow the create with a
    ``POST /sessions/{id}/start-sandbox``.

    Body fields: ``volume_id``, ``agent_id``, ``provider``, ``config``,
    ``env``, ``secrets``, ``cwd``, ``root``, ``dockerfile``,
    ``shared_mounts``, ``pre_start_commands``.
    """
    data = await _json_body(request)
    lazy = await _sessions_create_lazy(data)
    if data.get("provision", False):
        # Optional pre-warm via pool. Doesn't change the response shape;
        # subsequent POST /message would have done the same work anyway.
        try:
            from api.sandbox import get_pool
            session = await get_pool().get_session(lazy["id"])
            lazy["current_sandbox_id"] = session.state.sandbox_id
            lazy["connected"] = True
        except Exception as e:
            log.warning("pre-warm pool.get_session failed for %s: %s", lazy["id"], e)
    return lazy


async def _sessions_create_lazy(data: dict) -> dict:
    """Create a session row only — no sandbox, no ACP, no scheduler.

    Used when the UI wants to render a session shell before paying the
    provisioning cost (daytona: ~15-30 s; local: ~2-3 s). Sandbox appears
    on the first ``/sessions/{id}/start-sandbox`` or ``/message``.
    """
    # SECURITY: strip env/secrets first so they can't leak into agents.config.
    body_env, body_secrets = _pop_env_and_secrets(data)

    default_provider = data.get("provider") or data.get("config", {}).get("provider") or "local"
    volume_record = await _resolve_or_default_volume(data.get("volume_id"), default_provider)
    config_data = data.get("config", {})
    _merge_top_level_config(data, config_data)
    config_data.pop("dockerfile", None)
    config_data.pop("dockerfile_content", None)
    config_data.pop("shared_mounts", None)
    config_data.pop("root", None)

    agent_id = data.get("agent_id")
    if agent_id:
        await _require_agent(agent_id)
    else:
        agent_id = str(uuid.uuid4())
        await upsert_agent(AgentRecord(
            id=agent_id, name=data.get("name"),
            config=AgentConfig.from_dict(
                {**config_data, "agent_type": data.get("agent_type", "claude")}
            ),
        ))

    # Pull session cwd out of the body; agent config is pure identity now.
    # The default matches the per-provider home_dir that the FIRST sandbox
    # provision will spawn with, so session/new and every later session/load
    # use the same path (the JSONL hash key). For local, this is the
    # per-agent volume subpath; for docker/daytona, a fixed mount point.
    default_cwd = (
        str(Path(volume_record.provider_ref) / f"agents/{agent_id}")
        if default_provider == "local"
        else default_cwd_for_provider(default_provider)
    )
    cwd = data.get("cwd", config_data.pop("cwd", default_cwd))

    session_id = str(uuid.uuid4())
    lazy_user_pre_start = data.get("pre_start_commands") or []
    await upsert_session(
        session_id, agent_id, sandbox_id=None, inner_session_id=None,
        volume_id=volume_record.id,
        env=body_env or {}, secrets=body_secrets or {},
        cwd=cwd,
        pre_start_commands=list(lazy_user_pre_start),
    )

    return {
        "id": session_id,
        "agent_id": agent_id,
        "volume_id": volume_record.id,
        "current_sandbox_id": None,
        "connected": False,
    }




@app.post("/sessions/{session_id}/message")
async def post_session_message(session_id: str, request: Request):
    """Submit a prompt. Returns ``{rpc_id, status}``; events flow via
    GET /events.

    Routes through the SessionPool (per docs/ephemeral-sandbox-design.md
    §6/§7). One pool entry per session; per-session lock serialises
    concurrent POSTs (no separate scheduler queue). The supervisor SSE
    stream opens for this prompt only and closes at stopReason.
    """
    from api.sandbox import get_pool

    data = await _json_body(request)
    message = data.get("message")
    if not message:
        raise HTTPException(400, "message required")

    rpc_id = str(uuid.uuid4())
    pool = get_pool()
    session = await pool.get_session(session_id)

    async def _drain():
        try:
            async for _event in session.execute_prompt(message, rpc_id=rpc_id):
                pass  # broadcast happens inside execute_prompt
        except Exception as e:
            log.exception("execute_prompt failed for session %s rpc=%s",
                          session_id, rpc_id)
            session._broadcast({
                "type": "error", "rpc_id": rpc_id,
                "error": {"message": str(e),
                          "exception_type": type(e).__name__},
            })

    asyncio.create_task(_drain())
    return {"rpc_id": rpc_id, "status": "ok"}


@app.get("/sessions/{session_id}/events")
async def session_events(session_id: str):
    """SSE stream for a session.

    Subscribes to the SessionPool's per-session subscriber fan-out.
    Multi-subscriber preserved (per docs §15.1). Late joiners only see
    events from subscribe-time onward; past events come from
    ``session_log`` (separate concern).
    """
    from api.sandbox import get_pool

    pool = get_pool()
    session = await pool.get_session(session_id)

    async def _gen():
        async for event in session.subscribe():
            yield f"data: {json.dumps(event)}\n\n"

    return StreamingResponse(
        _gen(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )


@app.post("/sessions/{session_id}/start-sandbox")
async def start_session_sandbox(session_id: str):
    """Pre-warm: acquire a compute lease for this session. Idempotent —
    if the pool already has an active lease, returns it."""
    from api.sandbox import get_pool
    session = await get_pool().get_session(session_id)
    return {"sandbox_id": getattr(session.state, "sandbox_id", None)}



@app.post("/sessions/{session_id}/message+stream")
async def post_message_stream_route(session_id: str, request: Request):
    """**New phase-2 endpoint** (per docs/ephemeral-sandbox-design.md §7).

    Submits a prompt and streams the reply directly as SSE — collapses
    today's two-step ``POST /message`` (returns rpc_id) +
    ``GET /events`` (subscribe) into a single request/response stream.
    Multi-subscriber ``GET /events`` is preserved alongside; this
    endpoint is for callers that just want the reply they're waiting
    on.

    Backed by ``api.sandbox.SessionPool`` + the per-prompt supervisor
    SSE stream inside ``SandboxSession.execute_prompt`` — no persistent
    server↔supervisor connection.

    The legacy ``POST /sessions/{id}/message`` (returning rpc_id) and
    ``GET /sessions/{id}/events`` (multi-subscriber) routes are
    untouched and continue to work for callers that need them.
    """
    from fastapi.responses import StreamingResponse

    from api.sandbox import get_pool

    body = await _json_body(request)
    message = body.get("message")
    if not message:
        raise HTTPException(400, "message required")

    pool = get_pool()

    async def _gen():
        try:
            session = await pool.get_session(session_id)
            async for event in session.execute_prompt(message):
                yield f"data: {json.dumps(event)}\n\n"
        except Exception as e:
            err = {"type": "error", "error": {
                "message": str(e), "exception_type": type(e).__name__,
            }}
            yield f"data: {json.dumps(err)}\n\n"

    return StreamingResponse(_gen(), media_type="text/event-stream")


@app.post("/sessions/{session_id}/release")
async def release_session_route(session_id: str):
    """**New phase-2 endpoint** (per docs/ephemeral-sandbox-design.md §7).

    Snapshots the session's compute and releases the lease. Backed by
    ``api.sandbox.SessionPool.release``. Idempotent — calling on a
    session that has no active lease is a no-op returning the current
    snapshot pointer.

    Differs from the legacy ``/hibernate`` and ``/stop-sandbox`` routes:
    those operate on the in-memory ``SessionState`` + ``sandboxes`` row;
    this operates on the new ``SandboxSession`` pool. Both will work
    during the migration; phase 3 deletes the legacy routes once
    callers have migrated.
    """
    from api.sandbox import deserialize, get_pool
    from api.sandbox.db_bindings import load_sandbox_state

    pool = get_pool()
    await pool.release(session_id)

    payload = await load_sandbox_state(session_id)
    state = deserialize(payload)
    return {
        "lifecycle": "hibernated",
        "snapshot_path": state.snapshot_path,
        "snapshot_version": state.snapshot_version,
    }


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
    return await _download_from_instance(instance, path)


async def _download_from_instance(instance: ProviderInstance, path: str) -> Response:
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
# Session-scoped filesystem browsing (sandbox identity hidden from callers)
# ---------------------------------------------------------------------------


@app.get("/sessions/{session_id}/files/tree")
async def session_files_tree(session_id: str):
    """Return the recursive directory tree of the session's sandbox."""
    return await _proxy_from_session(session_id, "GET", "/v1/files/tree")


@app.get("/sessions/{session_id}/files/read")
async def session_files_read(session_id: str, path: str):
    """Read a single file from the session's sandbox."""
    return await _proxy_from_session(
        session_id, "GET", "/v1/files/read", params={"path": path},
    )


@app.post("/sessions/{session_id}/files/edit")
async def session_files_edit(session_id: str, request: Request):
    """Edit or create a file. Body: same shape as ``/sandboxes/{id}/files/edit``."""
    return await _proxy_from_session(
        session_id, "POST", "/v1/files/edit",
        json=await _json_body(request),
    )


@app.post("/sessions/{session_id}/files/upload")
async def session_files_upload(session_id: str, request: Request):
    """Upload a file. Body: ``{"path": ..., "content": "<base64>"}``."""
    return await _proxy_from_session(
        session_id, "POST", "/v1/files/upload",
        json=await _json_body(request), timeout=60,
    )


@app.post("/sessions/{session_id}/files/delete")
async def session_files_delete(session_id: str, request: Request):
    """Delete a file or directory. Body: ``{"path": ...}``."""
    return await _proxy_from_session(
        session_id, "POST", "/v1/files/delete",
        json=await _json_body(request),
    )


@app.post("/sessions/{session_id}/files/rename")
async def session_files_rename(session_id: str, request: Request):
    """Rename/move a file or directory. Body: ``{"path": ..., "new_path": ...}``."""
    return await _proxy_from_session(
        session_id, "POST", "/v1/files/rename",
        json=await _json_body(request),
    )


@app.get("/sessions/{session_id}/files/download")
async def session_files_download(session_id: str, path: str):
    """Download a file as raw bytes from the session's sandbox."""
    instance = await _resolve_session_instance(session_id)
    return await _download_from_instance(instance, path)


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
