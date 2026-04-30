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
    VolumeFileExistsError,
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

    # Cutover (Phase 2b): hibernate idle pool sessions via SessionPool's
    # reaper. Legacy ``_idle_reaper`` continues to scan SESSIONS dict for
    # back-compat with sessions that haven't migrated through pool yet.
    from api.sandbox import start_reaper, shutdown_pool
    await start_reaper()

    reaper = asyncio.create_task(_idle_reaper())
    yield
    await _cancel_task(reaper)
    try:
        await shutdown_pool()
    except Exception as e:
        log.warning("shutdown_pool failed: %s", e)
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


async def _rebind_state(state: SessionState, sandbox_record: SandboxRecord) -> None:
    """Rebind `state` to a fresh supervisor + ACP session, preserving its
    SessionState object (and thus its subscriber list). Caller holds the
    session lock.

    Unifies the recovery dance used by (a) the SSE-reader loop after
    exhausting retries, and (b) ensure_runtime when it discovers a dead
    supervisor. Preserving `_session_subscribers` is what keeps a UI's
    persistent /events stream alive across an external sandbox kill.
    """
    # Parallel DB reads — agent + session row are independent lookups.
    agent_record, session_row = await asyncio.gather(
        get_agent(state.agent_id),
        _require_session_row(state.session_id),
    )
    if agent_record is None:
        raise RuntimeError(f"agent {state.agent_id} missing during rebind")
    # Session row owns env/secrets/cwd now; sandbox row owns shared_mounts.
    spawn_env = _build_spawn_env_from_row(session_row)
    new_url, _ = await _ensure_sandbox_alive(
        state.sandbox_id, sandbox_record,
        agent_type=state.agent_type, spawn_env=spawn_env,
    )
    # cwd MUST match what session/new used — it determines the JSONL hash.
    # session.cwd is populated at session-create time and frozen thereafter.
    cwd = session_row.get("cwd") or "/tmp"
    new_client = AcpClient(new_url)
    new_acp_session_id = str(uuid.uuid4())
    new_inner_sid, started_fresh = await _attach_acp_session(
        new_client, new_acp_session_id, agent_record,
        inner_sid=state.inner_session_id, cwd=cwd,
    )
    if started_fresh and new_inner_sid:
        await upsert_session(
            state.session_id, state.agent_id, state.sandbox_id,
            new_inner_sid, volume_id=sandbox_record.volume_id,
        )
    old_client = state.client
    state.client = new_client
    state.supervisor_url = new_url
    state.acp_session_id = new_acp_session_id
    state.inner_session_id = new_inner_sid
    state.last_event_id = None  # old cursor is meaningless on the new session
    state.lifecycle = "live"
    # Close the old client in the background — the next turn doesn't need
    # to wait for the TCP teardown.
    if old_client is not None:
        async def _close_old():
            try: await old_client.aclose()
            except Exception: pass
        _spawn_bg(_close_old())
    log.info("[REBIND] session %s → new supervisor %s (acp=%s)",
             state.session_id, new_url, new_acp_session_id)


async def _ensure_state_live(state: SessionState, sandbox: SandboxRecord) -> None:
    """Ensure `state` points at a live, healthy supervisor. Caller holds
    the session lock.

    Fast paths, in order:
      (1) reader is actively streaming an upstream connection → trust it,
          no probe needed (saves ~100ms on the hot /message path).
      (2) port-based fast-fail: cached subprocess confirmed dead → skip
          probe, go straight to rebind.
      (3) health-probe the current URL; return if it answers.
    Slow path: rebind in place (preserves subscribers).
    """
    if state.supervisor_url and state._reader_connected:
        return
    from .providers import _wait_for_health
    if state.supervisor_url and state._reader_alive:
        cached = _INSTANCES.get(state.sandbox_id)
        if cached is None or _instance_process_alive(cached):
            # One quick probe, no backoff. If it fails we go to rebind
            # (which has its own retry ladder), so extra retries here
            # just delay the inevitable by ~1s per attempt.
            try:
                if await _wait_for_health(state.supervisor_url, max_retries=1, interval=0):
                    return
            except Exception:
                pass
    await _rebind_state(state, sandbox)
    if not state._reader_alive:
        _start_sse_reader(state)


async def _recover_after_disconnect(state: SessionState) -> bool:
    """SSE-reader entry point: rebind state to a fresh supervisor. Returns
    True on success. Acquires the session lock so a concurrent /message
    path doesn't race this recovery."""
    try:
        async with _get_session_lock(state.session_id):
            sandbox_record = await get_sandbox(state.sandbox_id)
            if sandbox_record is None:
                return False
            await _ensure_state_live(state, sandbox_record)
        return True
    except Exception as e:
        log.error("[SSE-READER] rebind failed for session %s: %s",
                  state.session_id, e, exc_info=True)
        return False


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
    overwrite: bool = True


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


@app.get("/volumes/{id_or_name}/files/exists")
async def volume_files_exists(id_or_name: str, path: str):
    vol = await _resolve_volume(id_or_name)
    rel = _safe_path(path)
    try:
        exists = await _providers_mod.volume_exists(vol.provider, vol.provider_ref, rel)
    except Exception as e:
        raise _volume_fs_err("Exists", vol.provider, e)
    return {"exists": exists}


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
        kwargs = {} if body.overwrite else {"overwrite": False}
        await _providers_mod.volume_rename(vol.provider, vol.provider_ref, src, dst, **kwargs)
    except VolumeFileExistsError:
        return JSONResponse(
            {"error": "exists", "path": body.new_path},
            status_code=409,
        )
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
    record = await _require_sandbox(sandbox_id)

    # Phase 2 cutover bridge: if this sandbox is owned by a SessionPool
    # session (created via POST /message → pool.get_session), release it
    # through the pool first so the SandboxSession.stop() snapshot fires
    # before the legacy destroy below. Match on ``sandbox_ref`` (provider
    # UUID) — that's what the pool stores in ``state.sandbox_id`` — not
    # the DB row PK we got in the URL.
    try:
        from api.sandbox import get_pool
        pool = get_pool()
        pool_session = pool.find_by_sandbox_id(record.sandbox_ref)
        if pool_session is not None:
            await pool.release(pool_session.session_id)
    except Exception as e:
        log.warning("DELETE /sandboxes %s: pool.release failed: %s", sandbox_id, e)

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


# ─────────────────────────────────────────────────────────────────────────────
# Sandbox recovery — two flavors, one rule:
#
#   *If the underlying sandbox/VM still exists, revive it in place (Type 1).
#    Otherwise provision a fresh one against the same DB row (Type 2).*
#
# Dispatch lives in ``_ensure_sandbox_alive``. It calls ``_type1_recover``
# first; only if that returns ``None`` does it fall through to
# ``_type2_recover`` (Type 2 only by construction).
#
#   Type 1 — in-place revive, same sandbox_ref. Cheap.
#     Applicable when:
#       • port-based + provider says status="stopped"
#             → ``provider.start_sandbox(ref)`` unpauses the container,
#               supervisor restarts on the same port.
#       • daytona — sandbox_ref still resolves
#             → ``restart_daytona_supervisor(ref)`` respawns the supervisor
#               process inside the existing daytona sandbox (and starts the
#               sandbox itself if it's in "stopped" state — that case is
#               handled inside the function).
#     Side effects:
#       • pre_start_commands are NOT re-run — original side effects are still
#         on the local filesystem.
#       • snapshot restore is short-circuited by the
#         ``/tmp/agent-sdk-bootstrapped`` sentinel in supervisor.js — the
#         local ext4 already has the latest workspace bytes.
#
#   Type 2 — replacement, new sandbox_ref. Expensive.
#     Triggered when Type 1 is not applicable or fails:
#       • port-based + status missing/error/running, or start_sandbox raised
#       • daytona + restart_daytona_supervisor raised an error that
#         ``_should_replace_daytona_sandbox`` classifies as "this sandbox
#         is gone — start over"
#     Side effects:
#       • pre_start_commands ARE re-run (replayed from the persisted session
#         row at provision time — see ``_build_pre_start_commands``).
#       • snapshot.tar (workspace, lifecycle-snapshotted) and agent_memory.tar
#         (per-turn JSONLs) are extracted on first boot to repopulate the
#         agent's HOME on the fresh local ext4.
#
# The DB ``sandboxes.id`` row stays stable across both — Type 1 changes
# nothing; Type 2 rewrites ``sandbox_ref`` (and possibly ``listen_port``) but
# preserves ``volume_id``, ``subpath``, ``dockerfile``, ``shared_mounts``.
# Sessions point at the row by id, so no session ever sees a Type 2 transition
# as anything more than a ``sandbox_reattach`` event before the next prompt.
# ─────────────────────────────────────────────────────────────────────────────


async def _type2_recover(
    sandbox_id: str, rec: SandboxRecord, agent_type: str,
    spawn_env: dict,
) -> ProviderInstance:
    """**Type 2 replacement** — provision a fresh provider instance for
    ``sandbox_id``, keeping the same DB row.

    Called from ``_ensure_sandbox_alive`` only after Type 1 has been ruled
    out (sandbox missing/unrecoverable). The DB ``sandboxes.id`` row stays
    stable; ``sandbox_ref`` (and possibly ``listen_port``) are rewritten
    to point at the fresh provider resource. ``dockerfile`` +
    ``shared_mounts`` + ``volume_id`` + ``subpath`` come from the existing
    row — the sandbox owns its provisioning identity, so a replacement
    always gets the same image + mount layout.
    """
    provider = rec.provider
    dockerfile = rec.dockerfile
    shared_mounts = list(rec.shared_mounts) if rec.shared_mounts else None

    if provider in PORT_BASED_PROVIDERS:
        if not rec.volume_id:
            raise RuntimeError(f"Sandbox {sandbox_id} has no volume_id")
        vol = await get_volume(rec.volume_id)
        if vol is None:
            raise RuntimeError(f"Sandbox {sandbox_id} references missing volume {rec.volume_id}")
        try:
            inst = await _providers_mod.provision_sandbox(
                provider, volume_ref=vol.provider_ref, subpath=rec.subpath or "",
                agent_type=agent_type, dockerfile=dockerfile,
                root=rec.root, spawn_env=spawn_env, sandbox_id=sandbox_id,
                shared_mounts=shared_mounts,
            )
        except Exception as e:
            raise RuntimeError(f"Failed to provision Type 2 replacement sandbox: {e}")
        return inst

    # Daytona Type 2: brand-new daytona sandbox.
    dt_volume_ref = None
    if rec.volume_id:
        v = await get_volume(rec.volume_id)
        if v is not None:
            dt_volume_ref = v.provider_ref
    try:
        inst = await create_instance(
            "daytona", agent_type, dockerfile=dockerfile,
            root=rec.root, spawn_env=spawn_env,
            volume_id=dt_volume_ref, subpath=rec.subpath,
            sandbox_id=sandbox_id,
            shared_mounts=shared_mounts,
        )
    except Exception as e:
        raise RuntimeError(f"Failed to create replacement daytona sandbox: {e}")
    # Daytona ``create_sandbox`` does NOT start the supervisor (by design —
    # the agent HOME mount + volume cache extraction happen later). Without
    # this step the returned instance has url="" and the rebind path builds
    # an AcpClient with an empty base URL, which blows up on the very next
    # httpx call ("Request URL is missing an 'http://' or 'https://'
    # protocol"). See test_persistent_sse_external_delete_then_message.
    if not inst.url:
        # Daytona supervisor uses a fixed in-sandbox port (9100) — the
        # Daytona signed preview maps host URLs to container ports, so
        # every supervisor we spawn inside any daytona sandbox listens on
        # this same port. Matches restart_daytona_supervisor's constant.
        try:
            inst_url = await _providers_mod.ensure_supervisor_url(
                "daytona", inst, agent_type=agent_type,
                root=rec.root, spawn_env=spawn_env, port=9100,
            )
        except Exception as e:
            raise RuntimeError(
                f"Failed to start supervisor on replacement daytona sandbox: {e}"
            )
        inst.url = inst_url
    return inst


async def _type1_recover(
    sandbox_id: str,
    rec: SandboxRecord,
    instance: ProviderInstance | None,
    agent_type: str,
    spawn_env: dict[str, str],
) -> ProviderInstance | None:
    """Attempt **Type 1** in-place revive — preserve the existing sandbox.

    Returns a live ``ProviderInstance`` on success, ``None`` if Type 1 is
    not applicable (caller should fall through to Type 2). Raises only on
    errors that should NOT be retried as Type 2 (e.g. a daytona sandbox
    that's hard-broken in a way a fresh sandbox won't fix).

    Applicability rule: **the sandbox itself still exists.**
      * port-based + status="stopped":
            ``start_sandbox(ref)`` — unpause the container, supervisor
            restarts on the same ref + port.
      * daytona (any state where sandbox_ref still resolves):
            ``restart_daytona_supervisor(ref)`` — respawn the supervisor
            process inside the existing daytona sandbox; the function
            internally handles "sandbox in stopped state" by issuing
            a sandbox.start() before respawning.

    Returns None when:
      * port-based + status missing/error/running (running is a fast-path
        miss caught by the caller before this function runs)
      * port-based + start_sandbox raised
      * daytona Type 1 raised an error that ``_should_replace_daytona_sandbox``
        classifies as "this sandbox is gone — Type 2 will heal it"
    """
    provider = rec.provider

    if provider in PORT_BASED_PROVIDERS:
        try:
            status = await _providers_mod.get_sandbox_status(provider, rec.sandbox_ref)
        except Exception as e:
            log.info("get_sandbox_status(%s) raised: %s — Type 1 unavailable, fall through to Type 2",
                     sandbox_id, e)
            return None
        if status != "stopped":
            return None
        try:
            await _providers_mod.start_sandbox(provider, rec.sandbox_ref)
        except Exception as e:
            log.warning("Type 1 start_sandbox(%s) failed: %s — falling through to Type 2",
                        sandbox_id, e)
            return None
        url = rec.derive_url()
        # Wait for the supervisor inside the just-restarted container/process
        # to bind its port. Local's start_sandbox does this internally; docker's
        # ``docker start`` returns as soon as the container is up but the node
        # process inside still needs hundreds of ms to listen. Without this
        # probe, the immediate rebind that follows races and hits ReadError —
        # the caller then falls back to a (destructive) full rebuild and the
        # /message returns 500 even though the supervisor was about to be
        # ready.
        from .providers import _wait_for_health
        if not await _wait_for_health(url, max_retries=20, interval=0.5):
            log.warning(
                "Type 1 supervisor at %s did not become healthy after start_sandbox; "
                "falling through to Type 2", url,
            )
            return None
        # Success — rebuild the in-memory ProviderInstance against the
        # same sandbox_ref + port. No DB write needed (the row didn't change).
        #
        # Crucially, do NOT call destroy_instance(old) here. provider
        # destroy_sandbox is destructive: docker → ``docker rm -f`` the
        # container we just restarted; local → pops _PROCESSES/_SPAWN_ARGS
        # for the freshly-respawned ref, untracking it. The old in-memory
        # ``ProviderInstance`` Python object is GC'd when the caller
        # overwrites _INSTANCES[sandbox_id] — that's the only cleanup
        # needed after an in-place restart.
        revived = ProviderInstance(
            provider=provider, url=url, root=rec.root,
            sandbox_id=rec.sandbox_ref, port=rec.listen_port,
        )
        if provider == "local":
            from .providers.local import _PROCESSES as _LOCAL_PROCS
            revived.process = _LOCAL_PROCS.get(rec.sandbox_ref)
        if provider == "docker":
            # Carry the container_id forward so a later Type 2 transition
            # still has the right ID to act on.
            revived.container_id = (
                instance.container_id if instance is not None else rec.sandbox_ref
            )
        return revived

    if provider == "daytona":
        from .providers import restart_daytona_supervisor
        try:
            return await restart_daytona_supervisor(
                rec.sandbox_ref, agent_type, root=rec.root, spawn_env=spawn_env,
            )
        except Exception as e:
            if _should_replace_daytona_sandbox(e):
                log.warning("daytona sandbox %s unrecoverable (%s); falling through to Type 2",
                            rec.sandbox_ref, e)
                return None
            raise RuntimeError(f"Failed to recover daytona sandbox: {e}")

    return None


async def _ensure_sandbox_alive(
    sandbox_id: str,
    sandbox_record: SandboxRecord,
    agent_type: str = "claude",
    spawn_env: dict[str, str] | None = None,
) -> tuple[str, bool]:
    """Ensure the supervisor for ``sandbox_id`` is reachable. Single dispatch
    point for both Type 1 (in-place revive) and Type 2 (replacement) recovery.

    Returns ``(url, replaced)``. ``replaced=True`` ⇔ Type 2 ran, meaning the
    underlying sandbox_ref is brand-new. Currently no caller reads the flag,
    but it's preserved as a telemetry signal.

    Dispatch:
        1. Fast path — instance alive in ``_INSTANCES``: return.
        2. **Type 1** via ``_type1_recover`` — if the existing sandbox can
           be revived, use it.
        3. **Type 2** via ``_type2_recover`` — provision a fresh
           sandbox against the same DB row + same volume.

    Locked under ``_sandbox_lock(sandbox_id)`` so concurrent callers don't
    double-recover.
    """
    provider = sandbox_record.provider
    async with _get_sandbox_lock(sandbox_id):
        fresh = await get_sandbox(sandbox_id)
        if fresh is None:
            raise RuntimeError("Sandbox was deleted")

        # ── Fast path ────────────────────────────────────────────────
        instance = _INSTANCES.get(sandbox_id)
        if instance is not None and await _instance_is_alive(instance):
            return instance.url, False

        if spawn_env is None:
            spawn_env = await _spawn_env_for_sandbox(sandbox_id)

        # ── Type 1: in-place revive ──────────────────────────────────
        revived = await _type1_recover(sandbox_id, fresh, instance, agent_type, spawn_env)
        if revived is not None:
            _INSTANCES[sandbox_id] = revived
            log.info("Type 1 revive succeeded for sandbox %s (provider=%s)", sandbox_id, provider)
            return revived.url, False

        # ── Type 2: provision a replacement ──────────────────────────
        # Tear down the dead port-based instance so its port is free for the
        # new one. Daytona's stale ProviderInstance is just a URL holder;
        # nothing to destroy.
        if instance is not None and provider in PORT_BASED_PROVIDERS:
            try: await destroy_instance(instance)
            except Exception: pass
        log.info("Type 2 replacement for sandbox %s (provider=%s)", sandbox_id, provider)
        new_instance = await _type2_recover(
            sandbox_id, fresh, agent_type, spawn_env,
        )
        _INSTANCES[sandbox_id] = new_instance
        # Preserve dockerfile + shared_mounts on the replacement row so future
        # recoveries keep the same provisioning identity.
        await upsert_sandbox(_sandbox_record(
            sandbox_id, provider, new_instance,
            volume_id=sandbox_record.volume_id, subpath=sandbox_record.subpath,
            root_fallback=sandbox_record.root,
            dockerfile=fresh.dockerfile,
            shared_mounts=list(fresh.shared_mounts or []),
        ))
        return new_instance.url, True




_STALE_CACHE_MARKERS = (
    "supervisor.js missing",
    "ACP binary missing",
    "call install_supervisor",
)


async def _provision_with_cache_retry(cache_key, fn, /, *args, **kwargs):
    """Call ``fn(*args, **kwargs)``; on stale-install-cache markers,
    clear the supervisor-install cache for ``cache_key`` (a
    ``(volume_id, agent_type)`` tuple), reinstall, and retry once.

    ``volumes.supervisor_agent_types`` says installed but the on-disk
    state can diverge (ephemeral volume wiped on container restart,
    failed install that still marked the cache). One clean-and-retry
    self-heals; a second failure surfaces the real error.

    Positional-only for the key so ``*args``/``**kwargs`` forwarded to
    ``fn`` can contain any names (including ``volume_id`` /
    ``agent_type``) without colliding with our own parameters.
    """
    vol_id, agent_type = cache_key
    try:
        return await fn(*args, **kwargs)
    except RuntimeError as e:
        if not any(marker in str(e) for marker in _STALE_CACHE_MARKERS):
            raise
        # Clear the cache entry for the CURRENT supervisor version so
        # ensure_volume_supervisor falls through to install on the retry.
        # If the cache somehow still holds an older versioned key, leave
        # it alone — it's already invalid (won't match the live cache_key)
        # and a future install will overwrite it.
        versioned = _versioned_agent_type(agent_type)
        log.warning("stale-cache on volume %s agent=%s (key=%s): %s — reinstalling",
                    vol_id, agent_type, versioned, e)
        async with get_db() as conn:
            await conn.execute(
                "UPDATE volumes SET supervisor_agent_types = "
                "COALESCE(supervisor_agent_types, '[]'::jsonb) - %s WHERE id = %s",
                (versioned, vol_id),
            )
        await ensure_volume_supervisor(vol_id, agent_type)
        return await fn(*args, **kwargs)


async def ensure_volume_supervisor(volume_id: str, agent_type: str) -> None:
    """Idempotently install the supervisor + ACP binary on a volume.

    Cross-worker serialization uses a Postgres advisory lock keyed on a
    stable digest of ``(volume_id, agent_type)``.  Fast path: if the
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
    # Cache key is versioned by supervisor.js content hash so a stale entry
    # under a prior version (e.g., before the agent_memory.tar visibility-poll
    # fix) doesn't keep the old logic pinned on the volume.
    cache_key = _versioned_agent_type(agent_type)

    # Fast path (no lock): check cache — 99% of calls hit this.
    vol = await get_volume(volume_id)
    if vol is None:
        raise HTTPException(500, f"Volume {volume_id} not found")
    if cache_key in (vol.supervisor_agent_types or []):
        return  # already installed at this supervisor version

    # Compute a stable positive 63-bit key (pg advisory locks take a bigint;
    # mask off the sign bit for safety).
    digest = hashlib.sha256(f"{volume_id}\0{cache_key}".encode()).digest()
    lock_key = int.from_bytes(digest[:8], "big") & 0x7FFFFFFFFFFFFFFF

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
        if cache_key in installed:
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
            if cache_key in installed2:
                return

            log.info("ensure_volume_supervisor: installing %s on volume %s",
                     cache_key, volume_id)
            # Step 4: slow provider call with the connection in autocommit (not
            # in a transaction) so idle-in-transaction timers don't fire.
            await _providers_mod.install_supervisor(provider, provider_ref, agent_type)
            # Step 5: update the cache under the same session lock.
            await lock_conn.execute(
                "UPDATE volumes SET supervisor_agent_types = "
                "COALESCE(supervisor_agent_types, '[]'::jsonb) || to_jsonb(%s::text) "
                "WHERE id = %s AND NOT (supervisor_agent_types @> to_jsonb(%s::text))",
                (cache_key, volume_id, cache_key),
            )
            log.info("ensure_volume_supervisor: done installing %s on volume %s",
                     cache_key, volume_id)
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


def _instance_process_alive(inst: ProviderInstance) -> bool:
    """Process-local liveness check. No network. For the hot-path fast-path.

    Port-based instances carry a live Popen (local) or container_id
    (docker) — poll() reaps zombies so returncode is trustworthy after
    a kill -9. Daytona instances have no process object; trust the URL,
    _wait_for_health downstream catches stale URLs.
    """
    if inst.process is not None:
        try: inst.process.poll()
        except Exception: pass
        return inst.process.returncode is None
    return bool(inst.url)


async def _ensure_sandbox_locked(session_row: dict) -> SandboxRecord:
    session_id = session_row["id"]
    fresh = await get_session(session_id)
    if fresh is None:
        raise HTTPException(404, "Session not found")
    current_id = fresh.get("current_sandbox_id")

    # Case A: no sandbox yet — provision one.
    if current_id is None:
        return await _provision_new(fresh, previous_id=None)

    sb = await get_sandbox(current_id)

    # Case B: row deleted out-of-band — provision replacement.
    if sb is None:
        await set_session_current_sandbox(session_id, None)
        return await _provision_new(fresh, previous_id=current_id)

    # Hot-path fast-path: trust a live cached instance + attached SessionState.
    # No provider round-trip. ensure_runtime will re-verify with a health
    # probe if this turns out stale; kill -9 is caught by process.poll().
    cached = _INSTANCES.get(current_id)
    state = SESSIONS.get(session_id)
    if (sb.status == STATUS_RUNNING
            and cached is not None
            and state is not None and not state.shutdown.is_set()
            and state.sandbox_id == current_id and state.supervisor_url
            and _instance_process_alive(cached)):
        return sb

    # Cold path: probe provider status and act.
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
        # Snapshot the provisioning identity BEFORE delete_sandbox wipes
        # the row — without this, the replacement boots with empty
        # shared_mounts and the snapshot dockerfile, so /mnt/<name> dirs
        # the agent expected silently disappear and any custom image is
        # downgraded to the default. Symptom in production: orchestrator
        # container's /mnt/7 is empty after a sandbox went missing.
        saved_dockerfile = sb.dockerfile
        saved_shared_mounts = list(sb.shared_mounts) if sb.shared_mounts else None
        if status == "error":
            try:
                await _providers_mod.destroy_sandbox(vol.provider, _synthesize_instance(sb))
            except Exception:
                pass
        await delete_sandbox(sb.id)
        await set_session_current_sandbox(session_id, None)
        return await _provision_new(
            fresh, previous_id=current_id,
            dockerfile=saved_dockerfile,
            shared_mounts=saved_shared_mounts,
        )
    raise HTTPException(500, f"Unknown sandbox status: {status}")


@dataclass
class _ProvisionedSandbox:
    """Result of the shared provisioning core: live instance + (un-persisted)
    sandbox record. Caller decides how to write to the DB."""
    instance: ProviderInstance
    record: SandboxRecord


async def _resolve_sandbox_root(provider: str, explicit: str | None) -> str | None:
    """Pick the sandbox HOME path for `provider`.

    Returns:
      * the caller's explicit root if set
      * None for ``local`` (provider fills it from the volume subpath itself)
      * the canonical ``_PROVIDER_VOLUME_HOME`` entry otherwise

    Raises HTTPException(500) for an unknown provider — better than silently
    passing ``root=None`` downstream and letting a sandbox land outside its
    volume mount.
    """
    if explicit is not None:
        return explicit
    if provider == "local":
        return None
    root = _PROVIDER_VOLUME_HOME.get(provider)
    if root is None:
        raise HTTPException(
            500,
            f"no default root for provider {provider!r}; "
            "register one in _shared._PROVIDER_VOLUME_HOME",
        )
    return root


async def _provision_sandbox_core(
    *,
    volume: VolumeRecord,
    agent_id: str,
    agent_config: AgentConfig,
    spawn_env: dict[str, str],
    user_pre_start: list[str],
    dockerfile: str | None,
    shared_mounts: list[str] | None,
    explicit_root: str | None = None,
    sandbox_id: str | None = None,
) -> _ProvisionedSandbox:
    """Single-source provisioning: ensure supervisor → resolve root →
    provision_sandbox → build SandboxRecord.

    Caller is responsible for persisting the returned record (the two
    callers — _provision_new and the eager session-create path — write
    in different transactional contexts: _provision_new bundles the
    sandbox INSERT with the session-row UPDATE, the eager flow does
    them separately because the session row is written later).
    """
    agent_type = agent_config.agent_type or "claude"
    subpath = f"agents/{agent_id}"

    # Supervisor install is idempotent (cache hit in volumes table on reruns).
    await ensure_volume_supervisor(volume.id, agent_type)

    root = await _resolve_sandbox_root(volume.provider, explicit_root)
    new_sandbox_id = sandbox_id or f"sb_{uuid.uuid4().hex[:12]}"

    # Re-merge raw user commands with current agent skills so recovery
    # always uses the latest skill configuration.
    pre_start_commands = await _build_pre_start_commands(
        agent_config, volume.provider, user_pre_start
    )

    instance = await _provision_with_cache_retry(
        (volume.id, agent_type), _providers_mod.provision_sandbox,
        volume.provider,
        volume_ref=volume.provider_ref, subpath=subpath,
        agent_type=agent_type, spawn_env=spawn_env,
        root=root, sandbox_id=new_sandbox_id,
        dockerfile=dockerfile, shared_mounts=shared_mounts or None,
        pre_start_commands=pre_start_commands,
    )

    record = _sandbox_record(
        new_sandbox_id, volume.provider, instance,
        volume_id=volume.id, subpath=subpath,
        root_fallback=instance.root or root or "/tmp",
        dockerfile=dockerfile,
        shared_mounts=shared_mounts,
    )
    return _ProvisionedSandbox(instance=instance, record=record)


async def _provision_new(
    session_row: dict, previous_id: str | None,
    *,
    dockerfile: str | None = None,
    shared_mounts: list[str] | None = None,
) -> SandboxRecord:
    """Create a fresh sandbox (provider selected from the session's volume).

    ``dockerfile`` + ``shared_mounts`` define the new sandbox's provisioning
    identity. Callers supply them from: (a) the request body (new session),
    or (b) a prior sandbox row being replaced (reset). Sandbox row is the
    durable source of truth once created.
    """
    agent_id = session_row["agent_id"]
    vol, agent = await asyncio.gather(
        get_volume(session_row["volume_id"]),
        get_agent(agent_id),
    )
    if vol is None:
        raise HTTPException(500, f"Session's volume {session_row['volume_id']} missing")
    if agent is None or agent.config is None:
        raise HTTPException(500, f"Session's agent {agent_id} missing")

    provisioned = await _provision_sandbox_core(
        volume=vol,
        agent_id=agent_id,
        agent_config=agent.config,
        spawn_env=_build_spawn_env_from_row(session_row),
        user_pre_start=list(session_row.get("pre_start_commands") or []),
        dockerfile=dockerfile,
        shared_mounts=shared_mounts,
        explicit_root=None,  # use provider default
    )
    sb = provisioned.record
    _INSTANCES[sb.id] = provisioned.instance

    # Atomic: insert the sandbox row + link session->sandbox in one
    # transaction so a crash between writes can't orphan the sandbox.
    async with get_db() as conn:
        await conn.execute(
            "INSERT INTO sandboxes"
            " (id, provider, sandbox_ref, status, root, volume_id, subpath,"
            "  listen_port, dockerfile, shared_mounts)"
            " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            " ON CONFLICT(id) DO UPDATE SET provider=EXCLUDED.provider,"
            " sandbox_ref=EXCLUDED.sandbox_ref, status=EXCLUDED.status,"
            " root=EXCLUDED.root, volume_id=EXCLUDED.volume_id,"
            " subpath=EXCLUDED.subpath, listen_port=EXCLUDED.listen_port,"
            " dockerfile=EXCLUDED.dockerfile, shared_mounts=EXCLUDED.shared_mounts",
            (sb.id, sb.provider, sb.sandbox_ref, sb.status,
             sb.root, sb.volume_id, sb.subpath, sb.listen_port,
             sb.dockerfile, Json(list(sb.shared_mounts or []))),
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

    # If an existing state is attached to this same sandbox and hasn't
    # been shut down, rebind it in place (preserving subscribers) rather
    # than tearing down + rebuilding. This keeps a persistent /events
    # connection alive across an external sandbox kill: the UI's subscriber
    # queue lives on `existing._session_subscribers`, so swapping the
    # supervisor URL on the same SessionState object delivers the next
    # turn's events to the UI without requiring it to reconnect and
    # re-subscribe (the window during which the NEW state has zero
    # subscribers is where events were previously lost).
    if existing:
        if existing.shutdown.is_set() or existing.sandbox_id != sandbox.id:
            await _shutdown_session_state(existing, remove=True, force=True)
        else:
            try:
                await _ensure_state_live(existing, sandbox)
                return existing
            except SandboxMissingError:
                raise
            except Exception:
                log.exception("in-place rebind failed for session %s; "
                              "falling back to full rebuild", session_id)
                await _shutdown_session_state(existing, remove=True, force=True)

    # Build fresh. Parallel DB reads — agent + volume are independent.
    agent_id = session_row["agent_id"]
    agent_record, vol = await asyncio.gather(
        get_agent(agent_id),
        get_volume(session_row["volume_id"]),
    )
    if agent_record is None:
        raise HTTPException(500, f"Agent {agent_id} missing")
    if vol is None:
        raise HTTPException(500, f"Session's volume {session_row['volume_id']} missing")
    agent_type = agent_record.config.agent_type or "claude"

    # Build spawn_env from the session row (agent no longer owns env).
    root = sandbox.root or "/tmp"
    effective_spawn_env = {**_build_spawn_env_from_row(session_row), "HOME": root}

    # Supervisor URL resolution:
    #  - Port-based (local/docker): URL is known from _INSTANCES or the DB row.
    #  - Daytona: URL is a per-session SDK-signed preview minted here.
    if vol.provider in PORT_BASED_PROVIDERS:
        cached = _INSTANCES.get(sandbox.id)
        if cached and cached.url:
            supervisor_url, supervisor_port = cached.url, cached.port
        elif sandbox.listen_port is not None:
            supervisor_url, supervisor_port = sandbox.derive_url(), sandbox.listen_port
        else:
            raise HTTPException(
                500,
                f"Sandbox {sandbox.id} (provider={vol.provider}) has no live "
                f"instance and no listen_port recorded; cannot resolve supervisor URL",
            )
    else:
        supervisor_port = allocate_sandbox_port(sandbox.id)
        inst = ProviderInstance(
            provider=vol.provider, url="",
            root=root, sandbox_id=sandbox.sandbox_ref,
        )
        try:
            supervisor_url = await _providers_mod.ensure_supervisor_url(
                vol.provider, inst, agent_type=agent_type,
                root=root, spawn_env=effective_spawn_env, port=supervisor_port,
            )
        except Exception as exc:
            free_sandbox_port(sandbox.id, supervisor_port)
            if isinstance(exc, SandboxMissingError):
                # Let ensure_session_live re-provision rather than 500ing.
                raise
            raise HTTPException(500, f"Failed to start supervisor: {exc}") from exc

    client = AcpClient(supervisor_url)
    acp_session_id = str(uuid.uuid4())
    # cwd determines the hash under which Claude Code writes the session
    # JSONL (~/.claude/projects/<hash>/<inner_sid>.jsonl). Lives on the
    # session row — frozen at session-create time and read unchanged by
    # every subsequent rebind / replacement, so session/load always finds
    # the right JSONL. HOME is the sandbox root regardless of cwd, so
    # ``~/.claude/projects`` still lands on the persistent volume.
    cwd = session_row.get("cwd") or "/tmp"

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


async def ensure_session_live(session_id: str) -> tuple[dict, SandboxRecord, SessionState]:
    """One-shot: session → sandbox → runtime. Most endpoints use this.

    ``SandboxMissingError`` from ``ensure_runtime`` means the provider lost
    the sandbox out-of-band (e.g. external ``daytona.delete()``). Drop all
    cached references, null out ``session.current_sandbox_id``, and retry:
    ``ensure_sandbox`` then takes Case A and provisions a replacement on
    the same volume. One recovery; a second failure surfaces as fatal.
    """
    session = await _require_session_row(session_id)
    sandbox = await ensure_sandbox(session)
    try:
        return session, sandbox, await ensure_runtime(session, sandbox)
    except SandboxMissingError:
        pass
    log.warning("sandbox %s (ref=%s) missing on provider; provisioning replacement",
                sandbox.id, sandbox.sandbox_ref)
    SESSIONS.pop(session_id, None)
    _INSTANCES.pop(sandbox.id, None)
    try: await delete_sandbox(sandbox.id)
    except Exception as e: log.warning("delete_sandbox(%s) failed: %s", sandbox.id, e)
    await set_session_current_sandbox(session_id, None)
    session = await _require_session_row(session_id)
    sandbox = await ensure_sandbox(session)
    return session, sandbox, await ensure_runtime(session, sandbox)



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
        # Same dual-key rationale as /sessions: ``sandbox_id`` for the
        # REST/client convention, ``current_sandbox_id`` to match the DB
        # column + /sessions/{id} GET response shape.
        "sandbox_id": state.sandbox_id,
        "current_sandbox_id": state.sandbox_id,
        "inner_session_id": state.inner_session_id,
        "status": "resumed",
    }


@app.post("/sessions")
async def sessions_create(request: Request):
    """Create a session. Eager by default (provision sandbox + connect).

    Body:
      - ``provision`` (bool, default ``true``): when ``false``, skip sandbox
        provisioning and return a session shell with ``current_sandbox_id =
        null``. The sandbox materialises on the first downstream call that
        needs one (``/sessions/{id}/start-sandbox`` or ``/message``).
      - Every other field (``volume_id``, ``agent_id``, ``provider``,
        ``config``, ``env``, ``secrets``, ``cwd``, ``root``, ``dockerfile``,
        ``shared_mounts``) — see the dispatched-to helper for details.

    Collapses the old ``POST /sessions`` (lazy) and ``POST /sessions``
    (eager) into one endpoint with consistent naming.
    """
    data = await _json_body(request)
    if data.get("provision", True):
        return await _sessions_create_eager(data)
    return await _sessions_create_lazy(data)


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


async def _sessions_create_eager(data: dict) -> dict:
    """Create agent + provision sandbox + connect ACP in one call.

    Returns ``{agent_id, sandbox_id, current_sandbox_id, session_id,
    inner_session_id, connected: true}`` — ready to POST /message against.
    """
    # SECURITY: strip env/secrets first so they can't leak into agents.config.
    body_env, body_secrets = _pop_env_and_secrets(data)

    provider = data.get("provider", "local")
    volume_record = await _resolve_or_default_volume(data.get("volume_id"), provider)
    volume_id = volume_record.id
    agent_type = data.get("agent_type", "claude")
    config_data = data.get("config", {})
    _merge_top_level_config(data, config_data)

    # Pull session-level and sandbox-level fields out of the request body
    # before building AgentConfig (which is pure identity now).
    # ``root`` and ``cwd`` default to ``None`` — each provider fills in a
    # sensible default (local: the per-agent volume subpath; docker:
    # /home/agent; daytona: /home/daytona). Hardcoding a /tmp default here
    # caused initial sandboxes to write outside the volume while replacements
    # landed on the volume, breaking volume-persistence tests.
    cwd = data.get("cwd", config_data.pop("cwd", None))
    root = data.get("root", config_data.pop("root", None))
    dockerfile = _materialize_dockerfile({**config_data, **data})
    shared_mounts = data.get("shared_mounts") or config_data.pop("shared_mounts", None) or []
    # Drop any dockerfile_content key that may have landed in config_data;
    # _materialize_dockerfile already consumed it above.
    config_data.pop("dockerfile_content", None)
    config_data.pop("dockerfile", None)

    agent_id = str(uuid.uuid4())
    config = AgentConfig.from_dict({**config_data, "agent_type": agent_type})
    await upsert_agent(AgentRecord(id=agent_id, name=data.get("name"), config=config))

    session_env = body_env or {}
    session_secrets = body_secrets or {}
    spawn_env = _merge_env(session_env, session_secrets)

    user_pre_start = list(data.get("pre_start_commands") or [])
    sandbox_id = str(uuid.uuid4())

    # Single-source provisioning. _provision_sandbox_core handles
    # ensure_volume_supervisor, root resolution against _PROVIDER_VOLUME_HOME,
    # and the underlying provision_sandbox call. The eager flow's only
    # divergence from _provision_new is what we do AFTER provisioning
    # (resolve supervisor URL → ACP attach → session row), not how we
    # provision.
    try:
        provisioned = await _provision_sandbox_core(
            volume=volume_record,
            agent_id=agent_id,
            agent_config=config,
            spawn_env=spawn_env,
            user_pre_start=user_pre_start,
            dockerfile=dockerfile,
            shared_mounts=shared_mounts,
            explicit_root=root,
            sandbox_id=sandbox_id,
        )
    except HTTPException:
        await delete_agent(agent_id)
        raise
    except Exception as e:
        await delete_agent(agent_id)
        log.error("sessions_quick_create: provisioning failed (provider=%s): %s",
                  provider, e, exc_info=True)
        if "circuit breaker" in str(e).lower():
            raise HTTPException(503, str(e), headers={"Retry-After": "30"})
        raise HTTPException(502, f"Provider '{provider}' failed: {e}")

    instance = provisioned.instance
    _INSTANCES[sandbox_id] = instance
    # Effective root drives both the sandbox row and the session's cwd —
    # session/new must run with the same path the supervisor's HOME points
    # at so volume-persisted JSONLs land where Claude expects to find them.
    effective_root = provisioned.record.root
    if cwd is None:
        cwd = effective_root
    await upsert_sandbox(provisioned.record)

    async def _cleanup_and_raise(msg_fmt: str, e: Exception) -> None:
        """Shared teardown for post-upsert failures in /sessions."""
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
        cwd=cwd,
        pre_start_commands=list(user_pre_start),
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
    if state._scheduler_task is None or state._scheduler_task.done():
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
    """Execute a single prompt HTTP round-trip. Called only from the scheduler loop.

    Unconditionally re-runs ``ensure_session_live`` right before the
    submit. Closes the kill-then-send race: the POST /message handler's
    own ensure call happens before the prompt is enqueued, so a sandbox
    that died between enqueue and scheduler pickup would otherwise get
    the prompt sent to a dead supervisor. The ensure call here is cheap
    on the hot path (_reader_connected skips the health probe) and
    guarantees the client + acp_session_id we use are live.
    """
    session_id = state.session_id
    await log_event(
        session_id=session_id, agent_id=state.agent_id, sandbox_id=state.sandbox_id,
        event_type=EVT_USER_MESSAGE, payload={"text": message, "prompt_id": rpc_id},
    )
    def _is_transient_supervisor_failure(e: Exception) -> bool:
        # Transport-level: supervisor died mid-handshake.
        if isinstance(e, (httpx.ConnectError, httpx.RemoteProtocolError,
                          httpx.ReadError)):
            return True
        # Daytona signed proxy URL invalidation manifests as 502/503/504 on
        # the supervisor's HTTP surface even while the supervisor process
        # is alive — the proxy returns gateway errors for a few seconds
        # after a fresh URL is minted for the same port. A rebind picks up
        # the latest URL, retry then succeeds.
        if isinstance(e, httpx.HTTPStatusError):
            return e.response.status_code in (502, 503, 504)
        return False

    try:
        _, _, state = await ensure_session_live(session_id)
        await state.client.prompt(state.acp_session_id, message, rpc_id=rpc_id)
        return
    except Exception as first_err:
        if not _is_transient_supervisor_failure(first_err):
            err = first_err
            tb = traceback.format_exc()
            log.exception("prompt failed for session %s", session_id)
        else:
            # The supervisor died (or its signed Daytona URL was invalidated)
            # between `_reader_connected` observing it up and our prompt
            # submit. Clear the flag so the next ensure_session_live is
            # forced through the rebind path, then retry once. Handles both
            # the kill-then-immediately-send race for UI flows that don't add
            # any delay AND the Daytona stale-signed-URL 502 churn during
            # cascading recovery.
            log.warning(
                "prompt for session %s failed with %s; forcing rebind + retry",
                session_id, type(first_err).__name__,
            )
            state._reader_connected = False
            try:
                _, _, state = await ensure_session_live(session_id)
                await state.client.prompt(state.acp_session_id, message, rpc_id=rpc_id)
                return
            except Exception as e:
                err = e
                tb = traceback.format_exc()
                log.exception("retry after rebind also failed for session %s", session_id)

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
    """Submit a prompt. Returns ``{rpc_id, status}`` immediately; events
    flow via GET /events (multi-subscriber) or via the response body of
    POST /message+stream (single-call).

    Routes through ``api.sandbox.SessionPool`` (per
    ``docs/ephemeral-sandbox-design.md`` §6 / §7): pool.get_session
    cold-starts or warm-reuses the SandboxSession; execute_prompt opens
    a per-prompt supervisor SSE for this prompt only and broadcasts to
    /events subscribers via session._broadcast.
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
                pass  # events broadcast inside execute_prompt
        except Exception as e:
            log.exception("execute_prompt failed for session %s rpc=%s",
                          session_id, rpc_id)
            session._broadcast({
                "type": "error", "rpc_id": rpc_id,
                "error": {"message": str(e), "exception_type": type(e).__name__},
            })

    # Hold a strong reference so the task isn't GC'd mid-flight.
    task = asyncio.create_task(_drain())
    _BG_TASKS.add(task)
    task.add_done_callback(_BG_TASKS.discard)
    return {"rpc_id": rpc_id, "status": "ok"}


# Track in-flight POST /message background drains so asyncio doesn't GC them.
_BG_TASKS: set[asyncio.Task] = set()


@app.get("/sessions/{session_id}/events")
async def session_events(session_id: str):
    """SSE stream for a session. Multi-subscriber: many concurrent
    /events connections to the same session each receive a copy of every
    event (per ``docs/ephemeral-sandbox-design.md`` §15.1).

    Subscribes via ``SandboxSession.subscribe()`` which:
      * yields the per-session replay buffer first (so a UI reconnecting
        after a transient disconnect doesn't miss events that fired
        during the gap)
      * then streams live broadcasts from ``execute_prompt``
      * yields the ``_HEARTBEAT`` sentinel during idle so intermediaries
        (nginx / cloudflare / browser EventSource) don't close the
        connection between prompts.
    """
    from api.sandbox import get_pool
    from api.sandbox.session import _HEARTBEAT

    pool = get_pool()
    session = await pool.get_session(session_id)

    async def _gen():
        async for item in session.subscribe():
            # Subscribers receive one of:
            #   - _HEARTBEAT sentinel after an idle window — emit SSE comment
            #   - (rpc_id, raw_block) tuple from execute_prompt — emit
            #     ``event: rpc:<id>\n<block>\n\n`` so test/UI can correlate
            #   - parsed event dict from non-prompt sources — emit as data:
            if item is _HEARTBEAT:
                yield ": heartbeat\n\n"
            elif isinstance(item, tuple) and len(item) == 2:
                rpc_id, block = item
                yield f"event: rpc:{rpc_id}\n{block}\n\n"
            else:
                yield f"data: {json.dumps(item)}\n\n"

    return StreamingResponse(
        _gen(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )


@app.post("/sessions/{session_id}/message+stream")
async def post_session_message_stream(session_id: str, request: Request):
    """Submit a prompt and stream the reply as SSE in a single round-trip.

    Convenience over the legacy two-step (``POST /message`` returns
    ``rpc_id``; client opens ``GET /events`` to consume). This endpoint
    returns the SSE stream as the response body — same protocol shape
    as ``GET /events``, scoped to a single prompt.

    Body: ``{"message": str, "interrupt": bool?}``. ``interrupt`` is
    accepted for API parity but currently a no-op on the pool path
    (in-flight prompts are scoped to their own ``execute_prompt``
    coroutine; cancel-and-drain semantics belong with a future
    ``/cancel`` cutover).

    Returns: ``text/event-stream`` of the SSE blocks for this prompt
    only. Wire format matches ``GET /events`` —
    ``event: rpc:<id>\\n<raw_block>\\n\\n`` — so the SDK's
    ``parse_acp_event`` works unchanged. ``: heartbeat\\n\\n`` lines
    keep idle connections open through nginx / cloudflare.

    Both POST /message and GET /events continue to work unchanged for
    callers that need separate submit + multi-subscriber semantics.
    """
    from api.sandbox import get_pool
    from api.sandbox.session import _HEARTBEAT

    data = await _json_body(request)
    message = data.get("message")
    if not message:
        raise HTTPException(400, "message required")

    pool = get_pool()
    session = await pool.get_session(session_id)
    rpc_id = str(uuid.uuid4())

    async def _stream():
        # Subscribe BEFORE kicking off execute_prompt so broadcasts
        # from the supervisor's first chunks land in our queue. The
        # subscribe() generator registers the queue synchronously
        # before its first ``await q.get()``, so create_task'ing
        # _drive after entering the loop is race-free: drive only
        # runs once the event loop yields at our q.get().
        sub_iter = session.subscribe()

        async def _drive():
            try:
                async for _event in session.execute_prompt(message, rpc_id=rpc_id):
                    pass  # broadcasts fan out via session._broadcast
            except Exception as e:
                log.exception(
                    "execute_prompt failed for session %s rpc=%s",
                    session_id, rpc_id,
                )
                session._broadcast({
                    "type": "error", "rpc_id": rpc_id,
                    "error": {"message": str(e), "exception_type": type(e).__name__},
                })

        drive_task: asyncio.Task | None = None
        try:
            async for item in sub_iter:
                if drive_task is None:
                    # First iteration entered subscribe() body and
                    # registered our queue; safe to start driving.
                    drive_task = asyncio.create_task(_drive())
                if item is _HEARTBEAT:
                    yield ": heartbeat\n\n"
                    continue
                if isinstance(item, tuple) and len(item) == 2:
                    tag, block = item
                    # Filter to this prompt only — concurrent /events
                    # subscribers may have triggered other prompts whose
                    # blocks share the queue.
                    if tag != rpc_id:
                        continue
                    yield f"event: rpc:{tag}\n{block}\n\n"
                    if "stop_reason" in block or '"type":"done"' in block:
                        return
                # Parsed-dict broadcasts (errors, non-prompt notifications)
                # are emitted as ``data:`` blocks for SDK parity with /events.
                elif isinstance(item, dict):
                    if item.get("rpc_id") != rpc_id:
                        continue
                    yield f"data: {json.dumps(item)}\n\n"
                    if item.get("type") == "error":
                        return
        finally:
            if drive_task is not None and not drive_task.done():
                drive_task.cancel()
                try:
                    await drive_task
                except (asyncio.CancelledError, Exception):
                    pass

    return StreamingResponse(
        _stream(),
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
    # Raise 504 on timeout; _cancel_and_drain only logs.
    await state.client.cancel_prompt(state.acp_session_id)
    try:
        await asyncio.wait_for(state._prompt_done.wait(), timeout=_CANCEL_DRAIN_TIMEOUT)
    except asyncio.TimeoutError:
        raise HTTPException(504, f"cancel timed out (rpc {state.active_rpc_id})")
    return {"status": "ok"}


@app.post("/sessions/{session_id}/start-sandbox")
async def start_session_sandbox(session_id: str):
    """Eagerly provision a sandbox for a session (pre-warm). Idempotent."""
    _, sandbox, _ = await ensure_session_live(session_id)
    return {"sandbox_id": sandbox.id}


async def _hibernate_session_id(session_id: str, force: bool) -> dict:
    """Resolve a session_id to its sandbox + SessionState and hibernate.

    Backs both ``POST /sessions/{id}/hibernate`` and (since deprecation)
    ``POST /sessions/{id}/stop-sandbox``. Raises HTTPException on caller
    errors (session/sandbox missing, busy without force).

    Returns ``{status, sandbox_id, session_in_memory}`` describing the
    resulting state. ``status`` is one of ``hibernated`` |
    ``already_stopped``.
    """
    sess = await _require_session_row(session_id)
    sbid = sess.get("current_sandbox_id")
    if sbid is None:
        raise HTTPException(409, "session has no current sandbox")

    state = SESSIONS.get(session_id)
    if state is None:
        # No live state to preserve — just stop compute and flip the row.
        sb = await get_sandbox(sbid)
        if sb is None:
            raise HTTPException(404, f"sandbox {sbid} not found")
        if sb.status == STATUS_STOPPED:
            return {"status": "already_stopped", "sandbox_id": sbid,
                    "session_in_memory": False}
        await _request_supervisor_snapshot(_INSTANCES.get(sbid))
        try:
            await _providers_mod.stop_sandbox(sb.provider, _synthesize_instance(sb))
        except Exception as e:
            log.warning("hibernate: stop_sandbox failed for %s: %s", sbid, e)
        sb.status = STATUS_STOPPED
        await upsert_sandbox(sb)
        _INSTANCES.pop(sbid, None)
        return {"status": "hibernated", "sandbox_id": sbid,
                "session_in_memory": False}

    async with _get_session_lock(session_id):
        # Re-fetch under the lock — a concurrent /reap or eviction may
        # have dropped the state between our SESSIONS.get and the lock.
        state = SESSIONS.get(session_id)
        if state is None:
            return {"status": "hibernated", "sandbox_id": sbid,
                    "session_in_memory": False}

        if state.active_rpc_id is not None or state.pending_prompts:
            if not force:
                raise HTTPException(
                    409,
                    f"session is busy (active_rpc={state.active_rpc_id}, "
                    f"pending={len(state.pending_prompts)}); "
                    "pass ?force=true to cancel-and-drain first",
                )
            await _cancel_and_drain(state)
            # Drop any prompts that arrived while draining; caller asked
            # for hibernate, not "process the backlog then hibernate".
            state.pending_prompts.clear()

        sb = await get_sandbox(sbid)
        if sb is not None and sb.status == STATUS_STOPPED and sbid not in _INSTANCES:
            return {"status": "already_stopped", "sandbox_id": sbid,
                    "session_in_memory": True}

        await _hibernate_session(state)

    return {"status": "hibernated", "sandbox_id": sbid,
            "session_in_memory": True}


@app.post("/sessions/{session_id}/hibernate")
async def hibernate_session_route(session_id: str, request: Request):
    """Hibernate a session: stop the sandbox compute, but keep the live
    ``SessionState`` in memory and the sandboxes DB row intact.

    Use case: pause an idle session to stop paying for compute, then resume
    instantly when the user comes back. The next POST ``/message`` against
    this session goes through ``_ensure_runtime_locked`` →
    ``_ensure_state_live`` → ``_rebind_state``, which sees the stopped row
    and revives the SAME sandbox (same dockerfile, shared_mounts, volume,
    inner_session_id) — no re-provision and no state rebuild.

    **Compared to ``/sessions/{id}/stop-sandbox``** *(deprecated alias —
    same behavior since 2026-04-28)*: prefer this endpoint in new code.

    **Compared to ``/sessions/{id}/reset-sandbox``**: that destroys and
    reprovisions the sandbox (different ``sandbox_ref``); hibernate
    preserves provisioning identity completely.

    Subscribers stay attached — their SSE stream goes silent until the
    next turn rebinds the upstream reader. The browser's EventSource does
    not see a disconnect. If the LAST subscriber drops while the session
    is hibernated, the in-memory ``SessionState`` is evicted automatically
    (the next request rebuilds from the DB row).

    Returns 409 if the session has an in-flight RPC or queued prompts.
    Pass ``?force=true`` to cancel-and-drain in-flight work first.

    Idempotent: hibernating an already-stopped sandbox returns
    ``status=already_stopped`` with 200.
    """
    force = request.query_params.get("force", "").lower() in {"1", "true", "yes"}
    return await _hibernate_session_id(session_id, force)


@app.post("/sessions/{session_id}/stop-sandbox", status_code=204)
async def stop_session_sandbox(session_id: str, request: Request):
    """**Deprecated** — alias for ``POST /sessions/{id}/hibernate``.

    Behavior equivalent to ``/hibernate`` since 2026-04-28: stops the
    sandbox compute, keeps the sandboxes DB row, and (newly) keeps the
    in-memory ``SessionState`` so the next ``/message`` rebinds in place
    instead of paying for a state rebuild.

    Existing callers: no migration required, but new code should target
    ``/hibernate`` directly. Returns 204 with empty body, unchanged.

    Use ``/reset-sandbox`` for the tear-down-and-recreate semantic.
    """
    force = request.query_params.get("force", "").lower() in {"1", "true", "yes"}
    try:
        await _hibernate_session_id(session_id, force)
    except HTTPException as e:
        # Preserve the legacy 204 contract: a missing current_sandbox_id
        # was a no-op, not a 409. Re-raise everything else.
        if e.status_code == 409 and e.detail == "session has no current sandbox":
            return
        raise
    # current_sandbox_id STAYS pointing at the row — that's how resume
    # finds it. For the old "wipe on stop" behavior, use /reset-sandbox.


@app.post("/sessions/{session_id}/reset-sandbox")
async def reset_session_sandbox(session_id: str, request: Request):
    """Destroy current sandbox and provision a fresh one. Accepts optional
    body ``{dockerfile, dockerfile_content, shared_mounts}`` to change the
    provisioning identity; otherwise inherits from the old sandbox row."""
    data: dict = {}
    try:
        data = await _json_body(request)
    except HTTPException:
        pass  # empty body is fine
    sess = await _require_session_row(session_id)
    old_sbid = sess.get("current_sandbox_id")
    old_sb = await get_sandbox(old_sbid) if old_sbid else None

    # Destroy compute + delete row (unlike stop, this is the real tear-down).
    if old_sb:
        try:
            await _providers_mod.destroy_sandbox(old_sb.provider, _synthesize_instance(old_sb))
        except Exception:
            pass
    _INSTANCES.pop(old_sbid, None) if old_sbid else None
    state = SESSIONS.get(session_id)
    if state is not None:
        await _shutdown_session_state(state, remove=True, force=True)
    if old_sbid:
        await set_session_current_sandbox(session_id, None)
        await delete_sandbox(old_sbid)

    # Body overrides, else inherit from the old sandbox row.
    new_dockerfile = _materialize_dockerfile(data) if data else None
    if new_dockerfile is None:
        new_dockerfile = old_sb.dockerfile if old_sb else None
    if "shared_mounts" in data:
        new_shared_mounts = data.get("shared_mounts") or []
    else:
        new_shared_mounts = list(old_sb.shared_mounts) if old_sb else []

    fresh_sess = await _require_session_row(session_id)
    async with _get_session_lock(session_id):
        sandbox = await _provision_new(
            fresh_sess, previous_id=old_sbid,
            dockerfile=new_dockerfile, shared_mounts=new_shared_mounts,
        )
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


async def _resolve_sandbox_instance(
    sandbox_id: str,
    *,
    agent_type: str = "claude",
    spawn_env: dict[str, str] | None = None,
) -> ProviderInstance:
    """Get a live ProviderInstance for a sandbox, auto-starting if needed.

    Raises HTTPException on failure. Session-scoped callers pass the session's
    agent_type/spawn_env so recovery restarts the same runtime shape.
    """
    # Fast path: cached port-based instance is live as-is. Daytona preview
    # URLs can expire while the in-memory instance stays cached, so those
    # fall through to the liveness check below.
    instance = _INSTANCES.get(sandbox_id)
    if instance and instance.provider in PORT_BASED_PROVIDERS:
        return instance

    sandbox_record = await _require_sandbox(sandbox_id)
    try:
        await _ensure_sandbox_alive(
            sandbox_id, sandbox_record,
            agent_type=agent_type, spawn_env=spawn_env,
        )
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

    response = await _proxy_from_session(
        session_id, "POST", "/v1/exec",
        json={"command": command, "timeout": timeout},
        timeout=timeout + 5,
    )
    if response.status_code >= 400:
        return response
    try:
        payload = json.loads(response.body)
    except Exception:
        return response
    if not isinstance(payload, dict):
        return response
    payload.setdefault("stdout_truncated", False)
    payload.setdefault("stderr_truncated", False)
    payload.setdefault("timed_out", False)
    return payload


# ---------------------------------------------------------------------------
# Sandbox filesystem browsing
# ---------------------------------------------------------------------------


async def _resolve_session_instance(session_id: str) -> ProviderInstance:
    """Resolve a session_id to its current sandbox's ProviderInstance.

    Hides sandbox identity from callers — the whole point of the
    session-scoped file and sandbox APIs. Does NOT start the ACP runtime;
    only ensures the sandbox supervisor itself is live.
    """
    session = await _require_session_row(session_id)
    sandbox = await ensure_sandbox(session)
    agent = await get_agent(session["agent_id"])
    agent_type = (agent.config.agent_type if agent and agent.config else "claude")
    return await _resolve_sandbox_instance(
        sandbox.id,
        agent_type=agent_type,
        spawn_env=_build_spawn_env_from_row(session),
    )


async def _proxy_instance(
    instance: ProviderInstance, method: str, path: str, *,
    params: dict | None = None, json: dict | None = None,
    timeout: int = 30,
) -> Response:
    """Forward a request to a sandbox's supervisor via its ProviderInstance."""
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
    return await _proxy_instance(instance, method, path, params=params, json=json, timeout=timeout)


async def _proxy_from_session(
    session_id: str, method: str, path: str, *,
    params: dict | None = None, json: dict | None = None,
    timeout: int = 30,
) -> Response:
    """Session-scoped twin of ``_proxy_to_supervisor``."""
    instance = await _resolve_session_instance(session_id)
    return await _proxy_instance(instance, method, path, params=params, json=json, timeout=timeout)


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
