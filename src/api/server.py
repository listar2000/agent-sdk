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
    update_session_env,
    update_session_secrets,
    set_session_current_sandbox,
    upsert_agent,
    upsert_sandbox,
    upsert_session,
    upsert_volume,
)
from .models import (
    STATUS_RUNNING,
    AgentConfig,
    AgentRecord,
    SandboxRecord,
    SessionState,
    VolumeRecord,
)
from . import providers as _providers_mod
from .providers import (
    PORT_BASED_PROVIDERS,
    ProviderInstance,
    VolumeFileExistsError,
    allocate_sandbox_port,
    create_instance,
    default_cwd_for_provider,
    destroy_instance,
    free_sandbox_port,
    kill_supervisor_in_sandbox,
    stop_instance,
)
from .providers._shared import _safe_path as _shared_safe_path

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

    # SessionPool's idle reaper hibernates pool sessions per
    # docs/ephemeral-sandbox-design.md §6. The legacy SESSIONS-dict
    # reaper was deleted in Phase 4 — pool.reap_idle is now the only
    # eviction path.
    from api.sandbox import start_reaper, shutdown_pool
    await start_reaper()

    yield
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


# Endpoints scheduled for removal once SessionPool covers the same surface
# end-to-end. Keys are the FastAPI path-template strings; the matching
# request emits ``Deprecation: true`` + ``Sunset: <date>`` + a ``Link``
# pointing at the replacement, per RFC 8594. Callers (notably the SDK)
# log a DeprecationWarning when they observe these.
_DEPRECATED_ROUTES: dict[str, str] = {
    "/sandboxes": "POST /sessions creates sessions + their compute lazily through SessionPool",
    "/sandboxes/{sandbox_id}/start": "Pool brings the compute back on the next POST /sessions/{id}/message",
    "/sandboxes/{sandbox_id}/stop": "POST /sessions/{id}/release snapshots + pauses via SessionPool",
    "/sandboxes/{sandbox_id}/snapshot": "POST /sessions/{id}/release writes the snapshot via SessionPool",
    "/sandboxes/{sandbox_id}/files/tree": "GET /sessions/{session_id}/files/tree",
    "/sandboxes/{sandbox_id}/files/read": "GET /sessions/{session_id}/files/read",
    "/sandboxes/{sandbox_id}/files/edit": "POST /sessions/{session_id}/files/edit",
    "/sandboxes/{sandbox_id}/files/upload": "POST /sessions/{session_id}/files/upload",
    "/sandboxes/{sandbox_id}/files/delete": "POST /sessions/{session_id}/files/delete",
    "/sandboxes/{sandbox_id}/files/rename": "POST /sessions/{session_id}/files/rename",
    "/sandboxes/{sandbox_id}/files/download": "GET /sessions/{session_id}/files/download",
    "/sessions/{session_id}/start-sandbox": "POST /sessions/{id}/message — pool provisions on demand",
    "/sessions/{session_id}/reset-sandbox": "POST /sessions/{id}/release then POST /sessions/{id}/message",
}
_DEPRECATION_SUNSET = "Wed, 31 Dec 2026 00:00:00 GMT"


@app.middleware("http")
async def _deprecation_headers_middleware(request: Request, call_next):
    """Stamp RFC 8594 deprecation headers onto responses for routes in
    ``_DEPRECATED_ROUTES``. Lookup is by FastAPI's matched path template
    (so it doesn't depend on the concrete sandbox/session id), and only
    fires when the route actually matched — 404s pass through clean."""
    response = await call_next(request)
    route = request.scope.get("route")
    template = getattr(route, "path", None)
    note = _DEPRECATED_ROUTES.get(template) if template else None
    if note is not None:
        response.headers["Deprecation"] = "true"
        response.headers["Sunset"] = _DEPRECATION_SUNSET
        response.headers["Link"] = f'<{template}>; rel="deprecation"'
        response.headers["X-Deprecation-Note"] = note
    return response


def _warn_deprecated(route_template: str) -> None:
    """Server-side ``warnings.warn(DeprecationWarning)`` for a deprecated
    handler. Pairs with the HTTP ``Deprecation`` header (which only
    surfaces to HTTP callers): a Python program importing this module
    and calling the handler in-process (or watching the warnings stream)
    sees the same migration pointer the SDK does."""
    import warnings as _warnings
    note = _DEPRECATED_ROUTES.get(route_template, "endpoint is deprecated")
    _warnings.warn(
        f"{route_template} is deprecated (sunset: {_DEPRECATION_SUNSET}) — {note}",
        DeprecationWarning,
        stacklevel=3,
    )


async def _session_id_from_sandbox_id(sandbox_id: str) -> str:
    """Reverse-lookup helper for the deprecated ``/sandboxes/{id}/...``
    routes that need to forward into the SessionPool — those endpoints
    take the sandbox row id, but the pool keys on session_id.

    Strategy: try ``sessions.current_sandbox_id`` first (DB-backed
    pointer kept in sync by the dual-write trigger). Falls back to a
    pool reverse-lookup keyed on ``state.sandbox_id`` (the provider
    UUID, looked up via the sandbox row's ``sandbox_ref``) so
    pool-mediated sandboxes that haven't been pinned to a session row
    are still resolvable.

    Raises 404 if no session owns this sandbox.
    """
    async with get_db() as conn:
        row = await (await conn.execute(
            "SELECT id FROM sessions WHERE current_sandbox_id = %s LIMIT 1",
            (sandbox_id,),
        )).fetchone()
    if row is not None:
        return row["id"]
    record = await get_sandbox(sandbox_id)
    if record is not None and record.sandbox_ref:
        from api.sandbox import get_pool
        pool_session = get_pool().find_by_sandbox_id(record.sandbox_ref)
        if pool_session is not None:
            return pool_session.session_id
    raise HTTPException(404, f"sandbox {sandbox_id} not bound to any session")


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
    """**Deprecated** — use ``POST /sessions`` (creates a session +
    SessionPool-managed compute lazily on first ``POST /message``).

    Implementation kept for legacy callers that need a standalone
    sandbox without a session. The pool model treats every sandbox as
    session-owned, so this endpoint will be deleted after the
    deprecation window (sunset 2026-12-31).

    Body: ``{"volume_id": ..., "subpath": ..., "provider": "daytona"|"docker"|"local",
              "agent_type": ..., "config": {...}}``

    For Daytona the returned instance has no supervisor yet (started lazily by
    ``ensure_runtime``). For Docker/Local the supervisor is already running.
    Returns ``{id, sandbox_id, sandbox_ref, status, provider, root, volume_id,
    subpath, listen_port, url}``.
    """
    _warn_deprecated("/sandboxes")
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
    """**Deprecated** — use ``POST /sessions/{session_id}/release``.

    Resolves ``session_id`` from the sandbox row and delegates to the
    ``/release`` handler so the implementation stays in one place.
    Returns the legacy ``{"status": "stopped"}`` shape for callers
    that haven't migrated their response parsing yet.
    """
    _warn_deprecated("/sandboxes/{sandbox_id}/stop")
    await _require_sandbox(sandbox_id)
    sid = await _session_id_from_sandbox_id(sandbox_id)
    await release_session_route(sid)
    return {"status": "stopped"}


@app.post("/sandboxes/{sandbox_id}/snapshot")
async def snapshot_sandbox_route(sandbox_id: str):
    """**Deprecated** — use ``POST /sessions/{session_id}/release``.

    The pool's release path is the only snapshot+pause flow — no
    snapshot-without-pause primitive (per-turn snapshots in
    ``supervisor.js`` already cover the "save without stopping"
    case). Forwards to the ``/release`` handler.
    """
    _warn_deprecated("/sandboxes/{sandbox_id}/snapshot")
    await _require_sandbox(sandbox_id)
    sid = await _session_id_from_sandbox_id(sandbox_id)
    await release_session_route(sid)
    return {"status": "ok"}


@app.post("/sandboxes/{sandbox_id}/start")
async def start_sandbox_route(sandbox_id: str, request: Request):
    """**Deprecated** — use ``POST /sessions/{session_id}/resume`` (or
    just ``POST /message``, which provisions on demand).

    Resolves ``session_id`` from the sandbox row and delegates to
    ``/resume`` so the SessionPool revival path stays in one place.
    Returns the legacy ``{"status": "running", "url": ...}`` shape.
    """
    _warn_deprecated("/sandboxes/{sandbox_id}/start")
    await _require_sandbox(sandbox_id)
    sid = await _session_id_from_sandbox_id(sandbox_id)
    await session_resume(sid, request)
    from api.sandbox import get_pool
    pool_session = get_pool()._active.get(sid)  # noqa: SLF001 — the lease just acquired
    url = pool_session.supervisor_url if pool_session is not None else None
    return {"status": "running", "url": url}


# ---------------------------------------------------------------------------
# Admin
# ---------------------------------------------------------------------------


@app.get("/admin/sessions")
async def admin_list_sessions():
    """List in-memory pool sessions. Useful for the dashboard + cleanup
    debugging.

    Reads from the SessionPool's ``_active`` dict — that's the only
    in-memory session registry now (legacy ``SESSIONS``/``_INSTANCES``
    were either deleted or are no longer written to). The legacy
    response shape is preserved so ``ui/dashboard.html`` doesn't
    have to change: ``sessions[].agent_busy``/``active_rpc_id``/etc.
    are constants since the pool's per-prompt SSE replaced the
    persistent reader's busy-flag bookkeeping.
    """
    from api.sandbox import get_pool
    pool = get_pool()
    return {
        "sessions": [
            {
                "session_id": sid,
                "agent_id": sess._agent_id,
                "current_sandbox_id": getattr(sess.state, "sandbox_id", None),
                "sandbox_ref": getattr(sess.state, "sandbox_id", None),
                "inner_session_id": sess._inner_session_id,
                # ``agent_busy`` historically meant "a prompt is mid-flight";
                # the pool model tracks per-prompt SSE inside ``execute_prompt``
                # without exposing busy bookkeeping. The most meaningful
                # observable proxy is "someone is watching events" — that's
                # what the dashboard's "running" badge actually reads.
                "agent_busy": len(sess._subscribers) > 0,
                "active_rpc_id": None,
                "pending_count": 0,
                "session_subscribers": len(sess._subscribers),
                "rpc_subscribers": 0,
                "shutdown": False,
            }
            for sid, sess in pool._active.items()  # noqa: SLF001 — admin readout
        ],
        "instances": [
            {
                "sandbox_id": getattr(sess.state, "sandbox_id", None),
                "provider": getattr(sess.state, "type", "unknown"),
                "url": sess.supervisor_url,
                "port": getattr(sess.state, "listen_port", None),
                "container_id": None,
                "process_alive": sess.supervisor_url is not None,
            }
            for _sid, sess in pool._active.items()  # noqa: SLF001
            if sess.supervisor_url is not None
        ],
    }


# ---------------------------------------------------------------------------
# Session operations (on sandbox)
# ---------------------------------------------------------------------------



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
    """Get session runtime status including last activity timestamp.

    Routes through the SessionPool — ``pool.get_session`` brings the
    SandboxSession up if it's been reaped. The legacy SessionState
    fields ``agent_busy`` / ``active_rpc_id`` / ``pending_count`` /
    ``session_subscriber_count`` / ``rpc_subscriber_count`` /
    ``has_client`` / ``available_commands`` no longer have meaningful
    pool equivalents (per-prompt SSE replaced the persistent reader,
    and the pool's own subscriber list isn't a queue) — those keys
    are kept in the response for shape-back-compat with constants."""
    from api.sandbox import get_pool

    pool_session = await get_pool().get_session(session_id)
    state = pool_session.state
    last_chunk = pool_session.liveness._last_chunk_at
    now = time.time()
    return {
        "session_id": session_id,
        "agent_id": pool_session._agent_id,
        "current_sandbox_id": getattr(state, "sandbox_id", None),
        "inner_session_id": pool_session._inner_session_id,
        "agent_busy": False,
        "active_rpc_id": None,
        "pending_count": 0,
        "session_subscriber_count": len(pool_session._subscribers),
        "rpc_subscriber_count": 0,
        "last_activity": last_chunk,
        "idle_seconds": round(now - last_chunk, 1) if last_chunk else None,
        "has_client": pool_session.supervisor_url is not None,
        "shutdown_requested": False,
        "available_commands": [],
        "supervisor_url": pool_session.supervisor_url,
        "supervisor_port": getattr(state, "listen_port", None),
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
    """Pre-warm a session through the SessionPool — idempotent.

    Equivalent to ``pool.get_session(session_id)``: cold-starts the
    SandboxSession from ``sandbox_state`` JSONB if no lease exists,
    or reattaches to the live one if it does. The ACP ``session/load``
    happens inside ``SandboxSession.start()``.

    Body may optionally carry ``env`` and ``secrets`` (PATCH semantics)
    that will be persisted to the session row before the pool revives
    the sandbox; ``_bootstrap_session`` reads them when constructing
    the spawn environment for the supervisor.
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

    from api.sandbox import get_pool

    pool = get_pool()
    pool_session = await pool.get_session(session_id)
    sandbox_id = getattr(pool_session.state, "sandbox_id", None)
    return {
        "session_id": session_id,
        "agent_id": pool_session._agent_id,
        # Dual-key for back-compat: ``sandbox_id`` is the REST/client
        # convention; ``current_sandbox_id`` matches the DB column +
        # /sessions/{id} GET response shape.
        "sandbox_id": sandbox_id,
        "current_sandbox_id": sandbox_id,
        "inner_session_id": pool_session._inner_session_id,
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
    """Create agent + provision compute via SessionPool + attach ACP in one call.

    Returns ``{agent_id, sandbox_id, current_sandbox_id, session_id, id,
    inner_session_id, volume_id, connected: true}`` — ready to POST
    /message against immediately.

    Implementation: writes the agent + session rows + initial
    ``sandbox_state`` JSONB, then calls ``pool.get_session(session_id)``
    which runs the cold-create path (provisions sandbox, brings up
    supervisor, runs ACP ``session/new``, persists ``inner_session_id``
    on the session row).
    """
    from psycopg.types.json import Json

    from api.sandbox import (
        DaytonaSandboxState,
        DockerSandboxState,
        ModalSandboxState,
        Recipe,
        UnixLocalSandboxState,
        get_pool,
        serialize,
    )

    # SECURITY: strip env/secrets first so they can't leak into agents.config.
    body_env, body_secrets = _pop_env_and_secrets(data)

    provider = data.get("provider", "local")
    volume_record = await _resolve_or_default_volume(data.get("volume_id"), provider)
    agent_type = data.get("agent_type", "claude")
    config_data = data.get("config", {})
    _merge_top_level_config(data, config_data)

    cwd = data.get("cwd", config_data.pop("cwd", None))
    root = data.get("root", config_data.pop("root", None))
    dockerfile = _materialize_dockerfile({**config_data, **data})
    shared_mounts = data.get("shared_mounts") or config_data.pop("shared_mounts", None) or []
    config_data.pop("dockerfile_content", None)
    config_data.pop("dockerfile", None)

    agent_id = str(uuid.uuid4())
    config = AgentConfig.from_dict({**config_data, "agent_type": agent_type})
    await upsert_agent(AgentRecord(id=agent_id, name=data.get("name"), config=config))

    user_pre_start = list(data.get("pre_start_commands") or [])

    # Default cwd matches the per-provider HOME the first sandbox boots into,
    # so session/new and every later session/load share the JSONL hash key.
    if cwd is None:
        cwd = (
            str(Path(volume_record.provider_ref) / f"agents/{agent_id}")
            if provider == "local"
            else default_cwd_for_provider(provider)
        )

    state_cls = {
        "daytona": DaytonaSandboxState,
        "docker": DockerSandboxState,
        "local": UnixLocalSandboxState,
        "unix_local": UnixLocalSandboxState,
        "modal": ModalSandboxState,
    }.get(provider)
    if state_cls is None:
        await delete_agent(agent_id)
        raise HTTPException(400, f"unsupported provider: {provider!r}")
    initial_state = state_cls(recipe=Recipe(
        agent_type=agent_type,
        dockerfile=dockerfile,
        shared_mounts=list(shared_mounts) if shared_mounts else [],
        root=root,
        pre_start_commands=user_pre_start,
    ))

    session_id = str(uuid.uuid4())
    await upsert_session(
        session_id, agent_id, sandbox_id=None, inner_session_id=None,
        volume_id=volume_record.id,
        env=body_env or {}, secrets=body_secrets or {},
        cwd=cwd,
        pre_start_commands=user_pre_start,
    )
    # Pre-populate sandbox_state so pool.get_session knows the recipe on
    # first call. The dual-write trigger fires on UPDATE OF
    # current_sandbox_id/agent_id/pre_start_commands/volume_id (NOT on
    # sandbox_state itself), so this UPDATE doesn't get clobbered.
    async with get_db() as conn:
        await conn.execute(
            "UPDATE sessions SET sandbox_state = %s WHERE id = %s",
            (Json(serialize(initial_state)), session_id),
        )

    pool = get_pool()
    try:
        pool_session = await pool.get_session(session_id)
    except HTTPException:
        await delete_agent(agent_id)
        raise
    except Exception as e:
        await delete_agent(agent_id)
        log.error("sessions_create_eager: pool.get_session failed (provider=%s): %s",
                  provider, e, exc_info=True)
        if "circuit breaker" in str(e).lower():
            raise HTTPException(503, str(e), headers={"Retry-After": "30"})
        raise HTTPException(502, f"Provider '{provider}' failed: {e}")

    # Back-compat shim: legacy callers (and the test helpers) still read
    # ``current_sandbox_id`` and call ``GET /sandboxes/{id}`` against the
    # sandboxes table. The pool only updates ``sandbox_state`` JSONB, so
    # we mirror the row here. Removed once the sandboxes table itself
    # goes away (the next cleanup PR).
    provider_ref = getattr(pool_session.state, "sandbox_id", None)
    sandbox_row_id = f"sb_{uuid.uuid4().hex[:12]}"
    if provider_ref:
        await upsert_sandbox(SandboxRecord(
            id=sandbox_row_id, provider=provider, sandbox_ref=provider_ref,
            status="running",
            root=(pool_session.state.recipe.root or "/tmp"),
            volume_id=volume_record.id,
            subpath=pool_session._subpath or f"agents/{agent_id}",
            listen_port=getattr(pool_session.state, "listen_port", None),
            dockerfile=dockerfile,
            shared_mounts=list(shared_mounts) if shared_mounts else [],
        ))
        await set_session_current_sandbox(session_id, sandbox_row_id)

    return {
        "agent_id": agent_id,
        "sandbox_id": sandbox_row_id if provider_ref else None,
        "current_sandbox_id": sandbox_row_id if provider_ref else None,
        "session_id": session_id,
        "id": session_id,
        "volume_id": volume_record.id,
        "inner_session_id": pool_session._inner_session_id,
        "connected": True,
    }


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
    """Cancel the in-flight prompt on this session, if any.

    Best-effort: sends ``session/cancel`` (JSON-RPC notification) to
    the supervisor's ACP child via the SessionPool. The ACP child
    aborts the turn; the ``done`` event arrives on the same SSE
    subscribers that ``POST /message`` opened. If no session is
    currently leased by the pool, this is a no-op (returns 200) —
    same shape as the legacy "not busy" branch.
    """
    from api.sandbox import get_pool

    pool = get_pool()
    pool_session = pool._active.get(session_id)  # noqa: SLF001 — read-only peek
    if pool_session is None:
        return {"status": "ok", "detail": "no active lease"}
    await pool_session.cancel_active_prompt()
    return {"status": "ok"}


@app.post("/sessions/{session_id}/start-sandbox")
async def start_session_sandbox(session_id: str):
    """**Deprecated** — POST /sessions/{id}/message provisions on demand.

    Forwards to ``SessionPool.get_session(session_id)`` so callers that
    relied on eager pre-warming still get the same effect: the pool
    constructs the SandboxSession and runs ``start()`` (which boots
    the supervisor + attaches ACP) before this returns.
    """
    _warn_deprecated("/sessions/{session_id}/start-sandbox")
    from api.sandbox import get_pool
    pool_session = await get_pool().get_session(session_id)
    return {"sandbox_id": getattr(pool_session.state, "sandbox_id", None)}


@app.post("/sessions/{session_id}/release")
async def release_session_route(session_id: str):
    """Snapshot + drop the SessionPool's lease on this session's compute.

    Backed by ``api.sandbox.SessionPool.release``: writes a fresh
    filesystem snapshot to the volume and pauses (never deletes) the
    sandbox. Idempotent — a session with no active lease is a no-op.

    Distinct from the legacy ``/hibernate`` and ``/stop-sandbox`` routes:
    those operate on the in-memory ``SessionState`` + ``sandboxes`` row
    plumbing. This endpoint targets the SessionPool that POST /message
    and GET /events already use, so the snapshot here is exactly what
    the next pool-mediated prompt restores from.
    """
    from api.sandbox import deserialize, get_pool
    from api.sandbox.db_bindings import load_sandbox_state

    pool = get_pool()
    await pool.release(session_id)

    payload = await load_sandbox_state(session_id)
    state = deserialize(payload)
    return {
        "lifecycle": "hibernated",
        "snapshot_path": getattr(state, "snapshot_path", None),
        "snapshot_version": getattr(state, "snapshot_version", 0),
    }


@app.post("/sessions/{session_id}/reset-sandbox")
async def reset_session_sandbox(session_id: str, request: Request):
    """**Deprecated** — use ``POST /sessions/{id}/release`` then
    ``POST /sessions/{id}/message`` (the pool's cold-create path).

    Implementation: drops the pool's lease (no snapshot — we're
    discarding state on purpose), nulls ``sandbox_state.sandbox_id``
    so the next ``get_session`` cold-creates instead of reattaching,
    then re-leases via the pool.

    Body fields ``dockerfile`` / ``shared_mounts`` are accepted for
    back-compat but ignored — the pool's recipe comes from the
    persisted ``sandbox_state`` JSONB and changing it requires a
    direct write to that column.
    """
    _warn_deprecated("/sessions/{session_id}/reset-sandbox")
    try:
        await _json_body(request)
    except HTTPException:
        pass  # empty body is fine

    from psycopg.types.json import Json
    from api.sandbox import deserialize, get_pool, serialize
    from api.sandbox.db_bindings import load_sandbox_state

    pool = get_pool()
    # Drop the active lease (compute will be torn down by shutdown_all
    # path inside release; without a snapshot since we're resetting).
    try:
        await pool.release(session_id)
    except Exception:
        pass

    # Null sandbox_id so get_session takes the cold-create branch
    # instead of trying to reattach to the (about-to-be-destroyed) one.
    payload = await load_sandbox_state(session_id)
    state = deserialize(payload)
    state.sandbox_id = None
    state.snapshot_path = None
    state.snapshot_version = 0
    async with get_db() as conn:
        await conn.execute(
            "UPDATE sessions SET sandbox_state = %s WHERE id = %s",
            (Json(serialize(state)), session_id),
        )

    pool_session = await pool.get_session(session_id)
    return {"sandbox_id": getattr(pool_session.state, "sandbox_id", None)}


@app.post("/sessions/{session_id}/config")
async def session_set_config(session_id: str, request: Request):
    """Set mode/model/thought_level for a session via the SessionPool."""
    data = await _json_body(request)
    from api.sandbox import get_pool

    pool_session = await get_pool().get_session(session_id)
    try:
        if "mode" in data:
            await pool_session.set_mode(data["mode"])
        if "model" in data:
            await pool_session.set_model(data["model"])
        if "thought_level" in data:
            await pool_session.set_thought_level(data["thought_level"])
        return {"status": "ok"}
    except Exception as e:
        raise HTTPException(502, str(e))


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
    """Resolve a session_id to a ProviderInstance pointing at its
    supervisor URL via the SessionPool. ``pool.get_session()`` brings
    the compute up if needed; ``supervisor_url`` is set as part of
    ``SandboxSession.start()``.

    The ProviderInstance shim is kept (vs. returning the URL string
    directly) so ``_proxy_instance`` / ``_download_from_instance``
    don't need signature changes."""
    from api.sandbox import get_pool

    pool_session = await get_pool().get_session(session_id)
    state = pool_session.state
    return ProviderInstance(
        provider=getattr(state, "type", "unknown"),
        url=pool_session.supervisor_url or "",
        root=(state.recipe.root if state.recipe else None) or "/tmp",
        sandbox_id=getattr(state, "sandbox_id", None),
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


async def _proxy_from_session(
    session_id: str, method: str, path: str, *,
    params: dict | None = None, json: dict | None = None,
    timeout: int = 30,
) -> Response:
    """Forward a request to the session's supervisor (resolved through
    the SessionPool) and return its JSON response. Used by every
    session-scoped file proxy."""
    instance = await _resolve_session_instance(session_id)
    return await _proxy_instance(instance, method, path, params=params, json=json, timeout=timeout)


@app.get("/sandboxes/{sandbox_id}/files/tree")
async def sandbox_files_tree(sandbox_id: str):
    """**Deprecated** — use ``GET /sessions/{session_id}/files/tree``."""
    _warn_deprecated("/sandboxes/{sandbox_id}/files/tree")
    return await session_files_tree(await _session_id_from_sandbox_id(sandbox_id))


@app.get("/sandboxes/{sandbox_id}/files/read")
async def sandbox_files_read(sandbox_id: str, path: str):
    """**Deprecated** — use ``GET /sessions/{session_id}/files/read``."""
    _warn_deprecated("/sandboxes/{sandbox_id}/files/read")
    return await session_files_read(await _session_id_from_sandbox_id(sandbox_id), path)


@app.post("/sandboxes/{sandbox_id}/files/edit")
async def sandbox_files_edit(sandbox_id: str, request: Request):
    """**Deprecated** — use ``POST /sessions/{session_id}/files/edit``."""
    _warn_deprecated("/sandboxes/{sandbox_id}/files/edit")
    return await session_files_edit(await _session_id_from_sandbox_id(sandbox_id), request)


@app.post("/sandboxes/{sandbox_id}/files/upload")
async def sandbox_files_upload(sandbox_id: str, request: Request):
    """**Deprecated** — use ``POST /sessions/{session_id}/files/upload``."""
    _warn_deprecated("/sandboxes/{sandbox_id}/files/upload")
    return await session_files_upload(await _session_id_from_sandbox_id(sandbox_id), request)


@app.post("/sandboxes/{sandbox_id}/files/delete")
async def sandbox_files_delete(sandbox_id: str, request: Request):
    """**Deprecated** — use ``POST /sessions/{session_id}/files/delete``."""
    _warn_deprecated("/sandboxes/{sandbox_id}/files/delete")
    return await session_files_delete(await _session_id_from_sandbox_id(sandbox_id), request)


@app.post("/sandboxes/{sandbox_id}/files/rename")
async def sandbox_files_rename(sandbox_id: str, request: Request):
    """**Deprecated** — use ``POST /sessions/{session_id}/files/rename``."""
    _warn_deprecated("/sandboxes/{sandbox_id}/files/rename")
    return await session_files_rename(await _session_id_from_sandbox_id(sandbox_id), request)


@app.get("/sandboxes/{sandbox_id}/files/download")
async def sandbox_files_download(sandbox_id: str, path: str):
    """**Deprecated** — use ``GET /sessions/{session_id}/files/download``."""
    _warn_deprecated("/sandboxes/{sandbox_id}/files/download")
    return await session_files_download(await _session_id_from_sandbox_id(sandbox_id), path)


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
