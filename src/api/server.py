"""REST API server — agent / volume / session orchestration layer.

Sandbox identity is implicit and owned in-process by the
``api.sandbox.SessionPool``
There is no ``/sandboxes`` resource; ``GET /sessions/{id}/sandbox``
returns the metadata.

Run: uvicorn src.api.server:app --port 7778
"""

import asyncio
import json
import logging
import os
import time
import uuid
from contextlib import asynccontextmanager
from pathlib import Path

import httpx
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import (
    JSONResponse,
    StreamingResponse,
)

from .event_buffer import start_batcher, stop_batcher
from .timing import extract_session_id, log_request, timed_phase
from .db import (
    close_pool,
    delete_agent,
    delete_session,
    get_agent,
    get_session,
    init_db,
    init_pool,
    read_sandbox_state,
    update_session_env,
    update_session_pre_start_commands,
    update_session_secrets,
    upsert_agent,
    upsert_session,
    write_sandbox_state,
)
from .models import (
    AgentConfig,
    AgentRecord,
)
from . import providers as _providers_mod
from .providers import (
    default_cwd_for_provider,
)

log = logging.getLogger(__name__)


def _configure_logging() -> None:
    """Set up logging. Called once at server startup, not on import.

    Format includes milliseconds in the timestamp so request timings line
    up against `[%(name)s]` phase logs at sub-second resolution — without
    this you can't tell from the log whether two ``[r0] /message+stream``
    starts happened 20 ms apart or in the same tick.
    """
    level = os.environ.get("LOG_LEVEL", "INFO")
    logging.basicConfig(
        level=level,
        format="%(asctime)s.%(msecs)03d %(levelname)s %(name)s: %(message)s",
        datefmt="%H:%M:%S",
        force=True,
    )
    logging.getLogger("api").setLevel(level)
    logging.getLogger("httpx").setLevel(logging.WARNING)


# ---------------------------------------------------------------------------
# Lightweight cluster counters — periodic snapshot, NOT per-request.
# The goal is to spot scaling bottlenecks ("the DB pool is saturated",
# "executor is queueing", "we're doing 50 redirects/min so the hash
# routing is broken") without per-request log spam.
# ---------------------------------------------------------------------------

_SNAPSHOT_INTERVAL_S = float(os.environ.get("AGENT_SDK_SNAPSHOT_S", "30"))


async def _cluster_snapshot_loop() -> None:
    """Periodic per-replica state snapshot. ONE line every N seconds.

    Surfaces the few signals that actually move at scale:
      * ``active`` — sessions held by this replica's pool right now
      * ``db_pool`` — psycopg async-pool size / available connections
      * ``exec_queue`` — work items waiting in the default ThreadPoolExecutor
    """
    from .identity import replica_id
    from api.sandbox import get_pool
    while True:
        try:
            await asyncio.sleep(_SNAPSHOT_INTERVAL_S)
            pool = get_pool()
            active = len(pool._active)  # noqa: SLF001
            busy = sum(1 for s in pool._active.values() if s._subscribers)  # noqa: SLF001
            # psycopg pool internals: ``get_stats`` returns counters like
            # ``pool_size`` / ``pool_available`` / ``requests_waiting``.
            # Wrapped in try because the helper is opt-in and the column
            # name has drifted between psycopg-pool versions.
            from . import db as _db
            db_stats = ""
            try:
                p = getattr(_db, "_pool", None)
                if p is not None and hasattr(p, "get_stats"):
                    s = p.get_stats()
                    db_stats = (
                        f"db_pool={s.get('pool_size', '?')}/"
                        f"{s.get('pool_max', '?')} "
                        f"db_wait={s.get('requests_waiting', 0)}"
                    )
            except Exception:
                db_stats = ""
            # Executor queue depth — saturation here is THE signal that
            # a sync provider SDK (daytona/docker) is back-pressuring.
            # Default to 0 (not "?") when the executor hasn't been
            # lazily created yet — same semantically, less noisy.
            exec_queue = "0"
            try:
                loop = asyncio.get_running_loop()
                executor = loop._default_executor  # noqa: SLF001
                if executor is not None and hasattr(executor, "_work_queue"):
                    exec_queue = str(executor._work_queue.qsize())  # noqa: SLF001
            except Exception:
                pass
            log.info(
                "[%s] snapshot: active=%d busy=%d exec_queue=%s %s",
                replica_id(), active, busy, exec_queue, db_stats,
            )
        except asyncio.CancelledError:
            return
        except Exception as e:
            log.warning("cluster snapshot tick failed: %s", e)


# ---------------------------------------------------------------------------
# DB + in-memory state
# ---------------------------------------------------------------------------

# Strong references to fire-and-forget background tasks (the per-prompt
# persisters spawned by POST /message). The event loop only holds weak
# refs to tasks, so a caller that does ``asyncio.create_task(coro())``
# without keeping the returned Task alive risks silent cancellation.
# Tasks self-discard from the set on completion.
_BG_TASKS: set[asyncio.Task] = set()


# Supervisor-proxy infra (shared httpx client + _proxy_from_session /
# _download_from_session / _resolve_supervisor_url) lives in api.http_client
# — cycle-free so the session_files router can proxy too. The client is
# created/closed in the lifespan below (set_client / aclose). Re-exported here
# so the in-server routes (exec, reload, ...) keep resolving.
from . import http_client as _http_client_mod  # noqa: E402
from .http_client import (  # noqa: E402,F401
    _download_from_session,
    _proxy_from_session,
    _resolve_supervisor_url,
)




@asynccontextmanager
async def lifespan(app):
    _configure_logging()
    # Startup banner — pin replica + pid + addr so a merged tail across
    # replicas (or a single replica restart) is greppable. The same
    # ``replica_id()`` appears in every request line / phase log so you
    # can trace one session end-to-end with a single filter.
    from .identity import owner_addr, owner_id, replica_id
    log.info(
        "[%s] startup: pid=%d owner_id=%s addr=%s slow_threshold=%.0fms",
        replica_id(), os.getpid(), owner_id(), owner_addr(),
        float(os.environ.get("AGENT_SDK_SLOW_MS", "500")),
    )
    # Default ThreadPoolExecutor caps at ``min(32, cpu_count + 4)``. Every
    # sync provider SDK call (Daytona create/get/start/delete, unix_local
    # filesystem ops) goes through this pool via run_in_executor /
    # to_thread.
    #
    # We *don't* override by default: bench (40 concurrent Daytona
    # cold-creates on a 32-CPU host, matching the production pod) showed
    # 32 threads consistently outperforming 128 — past cpu_count, threads
    # mostly fight the GIL during the SDK's response-parsing phase.
    # Set AGENT_SDK_EXECUTOR_MAX to a positive integer to override (e.g.
    # if you find yourself on a tiny pod where the auto cap is too small,
    # or have evidence of executor saturation from a slow-Daytona day).
    _exec_max = int(os.environ.get("AGENT_SDK_EXECUTOR_MAX", "0"))
    if _exec_max > 0:
        import concurrent.futures as _cf
        asyncio.get_running_loop().set_default_executor(
            _cf.ThreadPoolExecutor(max_workers=_exec_max, thread_name_prefix="asdk-io")
        )
    # Startup phase timing as a single summary line (fires once per
    # process — useful for spotting slow init_pool / slow reconcile at
    # boot, but cheap to keep because it never recurs).
    _t0 = time.perf_counter()
    _phases: dict[str, float] = {}
    _p0 = time.perf_counter(); init_db(); _phases["db"] = (time.perf_counter() - _p0) * 1000
    _p0 = time.perf_counter(); await init_pool(); _phases["pool"] = (time.perf_counter() - _p0) * 1000
    _p0 = time.perf_counter(); await start_batcher(); _phases["batcher"] = (time.perf_counter() - _p0) * 1000

    _http_client_mod.set_client(httpx.AsyncClient(
        timeout=60,
        limits=httpx.Limits(max_keepalive_connections=200, max_connections=400),
    ))

    # Startup reconciliation: kill orphan containers labeled with a
    # sandbox_ref whose DB row is gone or marked deleted. Per-provider in
    # parallel so a slow provider doesn't serialise boot. In practice
    # only Docker does real work; daytona/local/modal are no-ops today.
    async def _safe_reconcile(prov: str) -> None:
        try:
            await _providers_mod.reconcile_sandboxes(prov)
        except Exception as e:
            log.warning("startup reconcile for %s failed: %s", prov, e)

    _p0 = time.perf_counter()
    await asyncio.gather(*[_safe_reconcile(p) for p in ("docker", "daytona", "unix_local", "modal")])
    _phases["reconcile"] = (time.perf_counter() - _p0) * 1000

    # SessionPool owns idle eviction now (per
    # ).
    from api.sandbox import shutdown_pool, start_reaper, start_worker_heartbeat
    _p0 = time.perf_counter(); await start_reaper(); _phases["reaper"] = (time.perf_counter() - _p0) * 1000
    _p0 = time.perf_counter(); await start_worker_heartbeat(); _phases["worker_hb"] = (time.perf_counter() - _p0) * 1000
    _phase_str = " ".join(f"{k}={v:.0f}ms" for k, v in _phases.items())
    log.info(
        "[%s] startup ready in %.0fms: %s",
        replica_id(), (time.perf_counter() - _t0) * 1000, _phase_str,
    )

    # Periodic cluster-state snapshot — the single most useful log line
    # for spotting scaling bottlenecks at a glance:
    #   active sessions / DB pool busy / executor queue / 307 count since last
    # Logs every AGENT_SDK_SNAPSHOT_S seconds (default 30). One line per
    # replica; grep ``[r0] snapshot`` to follow a single replica.
    _snapshot_task = asyncio.create_task(_cluster_snapshot_loop())
    _BG_TASKS.add(_snapshot_task)
    _snapshot_task.add_done_callback(_BG_TASKS.discard)

    yield
    try:
        await shutdown_pool()
    except Exception as e:
        log.warning("shutdown_pool failed: %s", e)
    try:
        await stop_batcher()
    except Exception as e:
        log.warning("stop_batcher failed: %s", e)
    await close_pool()
    await _http_client_mod.aclose()


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


# Per-request timing line. ``api.timing`` decides the log level:
#   * polling endpoints (/health, /admin/*, /*/status, /*/sandbox) → DEBUG
#   * slow requests (≥ AGENT_SDK_SLOW_MS, default 500ms) → WARNING
#   * 5xx responses → WARNING
#   * everything else → INFO
# StreamingResponses log time-to-headers (lease + first chunk), NOT
# total stream duration; the matching phase log inside ``message+stream``
# covers full turn-to-done time.
@app.middleware("http")
async def _request_timing(request: Request, call_next):
    t0 = time.perf_counter()
    sid = extract_session_id(request.url.path)
    try:
        response = await call_next(request)
    except Exception:
        log_request(
            method=request.method, path=request.url.path,
            status="ERR", duration_ms=(time.perf_counter() - t0) * 1000,
            session_id=sid,
        )
        raise
    log_request(
        method=request.method, path=request.url.path,
        status=response.status_code,
        duration_ms=(time.perf_counter() - t0) * 1000,
        session_id=sid,
    )
    return response


@app.exception_handler(HTTPException)
async def _http_exception_handler(request: Request, exc: HTTPException):
    """Uniform error shape: ``{"error": ...}`` for string details, pass-through for dict."""
    detail = exc.detail
    if isinstance(detail, dict):
        return JSONResponse(detail, status_code=exc.status_code)
    return JSONResponse({"error": detail}, status_code=exc.status_code, headers=exc.headers)


# No NotOwner handler. The per-session lease was retired in favor of
# per-worker liveness: we trust the LB's consistent-hash to route a given
# session to the same replica every time, and accept the narrow
# split-brain window at rebalance (mitigated in a follow-up via volume
# flock — see PR description). The previous handler emitted 307s for
# wrong-replica requests; with no per-session owner_id we can no longer
# point at "the right replica" — but we no longer need to.


# JSON-body parsing + 404 lookups live in ``api.deps`` (cycle-free so routers
# can share them). Re-exported so existing call sites + tests keep resolving.
from .deps import (  # noqa: E402,F401
    _json_body,
    _require_agent,
    _require_session_row,
)


# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------


@app.get("/health")
async def health():
    from api.sandbox import get_pool
    pool = get_pool()
    active = pool._active  # noqa: SLF001 — read-only peek into the pool registry
    return {
        "status": "ok",
        "sessions": len(active),
        "busy_sessions": sum(1 for s in active.values() if s._subscribers),
    }


# ---------------------------------------------------------------------------
# Skills provisioning (npx skills)
# ---------------------------------------------------------------------------


# Provisioning helpers moved to ``api.services.provisioning`` (refactor
# slice 2). Re-exported here so existing ``from api.server import
# _skills_install_commands`` call sites + tests keep resolving, and so the
# internal create/reload paths below can call them unqualified. No behavior
# change — these are the same functions, just relocated.
from .services.provisioning import (  # noqa: E402,F401
    _build_pre_start_commands,
    _cli_install_commands,
    _normalize_cli_tools,
    _normalize_skills,
    _resources_for_provider,
    _skills_install_commands,
)


# ---------------------------------------------------------------------------
# Request helpers
# ---------------------------------------------------------------------------

# Request-body parsing/validation moved to ``api.services.config_parse``
# (refactor slice 3). Re-exported so call sites + tests keep resolving.
from .services.config_parse import (  # noqa: E402,F401
    _coerce_env_dict,
    _extract_workspace,
    _materialize_dockerfile,
    _merge_top_level_config,
    _pop_env_and_secrets,
    _validate_subpath,
    _validate_volume_name,
)

# ---------------------------------------------------------------------------
# Agent CRUD routes live in api.routers.agents (refactor slice 6b)
# ---------------------------------------------------------------------------

from .routers import agents as _agents_router  # noqa: E402

app.include_router(_agents_router.router)


# ---------------------------------------------------------------------------
# Volume CRUD
# ---------------------------------------------------------------------------


# Volume resolution helpers live in ``api.deps`` (shared with the sessions
# create path). Re-exported so the volume FILE routes below + tests keep
# resolving.
from .deps import (  # noqa: E402,F401
    _get_or_create_default_volume,
    _resolve_or_default_volume,
    _resolve_volume,
)

# Volume CRUD routes live in api.routers.volumes (refactor slice 6d).
from .routers import volumes as _volumes_router  # noqa: E402

app.include_router(_volumes_router.router)


# ---------------------------------------------------------------------------
# Volume file operations -> api.routers.volume_files (refactor slice 6e)
# ---------------------------------------------------------------------------

from .routers import volume_files as _volume_files_router  # noqa: E402

app.include_router(_volume_files_router.router)


# ---------------------------------------------------------------------------
# Admin
# ---------------------------------------------------------------------------


from .routers import admin as _admin_router  # noqa: E402

app.include_router(_admin_router.router)


# ---------------------------------------------------------------------------
# Session operations (on sandbox)
# ---------------------------------------------------------------------------




# ---------------------------------------------------------------------------
# Read-only / simple session routes -> api.routers.sessions_read (slice 6h)
# ---------------------------------------------------------------------------

from .routers import sessions_read as _sessions_read_router  # noqa: E402

app.include_router(_sessions_read_router.router)


# ---------------------------------------------------------------------------
# Session endpoints (keyed by session_id, route through SessionPool)
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
    sandbox_ref = getattr(pool_session.state, "sandbox_ref", None)
    return {
        "session_id": session_id,
        "agent_id": pool_session._agent_id,
        "sandbox_ref": sandbox_ref,
        "inner_session_id": pool_session.inner_session_id,
        "status": "resumed",
    }


# ``_extract_workspace`` lives in ``api.services.config_parse`` (re-exported above).


def _reject_daytona_sibling_when_active(agent_id: str | None, provider: str) -> None:
    """Daytona-specific multi-session guard.

    On Daytona, a Volume is mounted into each Sandbox via S3-FUSE. Each
    mount has its own page cache, so writes from sibling A's mount are
    not visible in sibling B's mount until A has flushed to S3 AND B
    has invalidated its cache (neither happens automatically between
    prompts). Multi-session on Daytona therefore needs a separate
    architectural fix (one shared sandbox, multi-supervisor) before it
    can be safe.

    For now, fail-fast: if the caller is creating a sibling under an
    existing ``agent_id`` and there's already a live SandboxSession for
    that agent on Daytona, 409. Single-session-per-agent on Daytona
    (and unlimited siblings on docker/local/modal) keeps working.

    No-op for non-Daytona providers and for first-session creates
    (``agent_id`` is None).
    """
    from api.sandbox import get_pool

    if provider != "daytona" or not agent_id:
        return
    live = get_pool().find_by_agent_id(agent_id)
    if live:
        raise HTTPException(
            status_code=409,
            detail=(
                "Daytona supports one live session per agent today. "
                "Release the existing session "
                f"({live[0].session_id}) via DELETE /sessions/{{id}} or "
                "POST /sessions/{id}/release before creating a sibling. "
                "(Multi-session on Daytona requires a shared-sandbox + "
                "multi-supervisor architecture; not yet shipped.)"
            ),
        )


@app.post("/sessions")
async def sessions_create(request: Request):
    """Create a session. Eager by default (provision sandbox + connect).

    Body:
      - ``provision`` (bool, default ``true``): when ``false``, skip sandbox
        provisioning and return a session shell with `sandbox_ref = null`.
        The sandbox materialises on the first downstream call that needs
        one (``/sessions/{id}/resume`` or ``/message``).
      - Every other field (``volume_id``, ``agent_id``, ``provider``,
        ``config``, ``env``, ``secrets``, ``cwd``, ``root``, ``dockerfile``,
        ``shared_mounts``) — see the dispatched-to helper for details.

    Collapses the old ``POST /sessions`` (lazy) and ``POST /sessions``
    (eager) into one endpoint with consistent naming.
    """
    data = await _json_body(request)
    # Client-supplied session_id. The SDK generates a UUID up front and
    # sends it BOTH in the request body (``id``) and in the
    # ``X-Session-Id`` header so the LB can consistent-hash on it (the
    # body isn't visible to nginx; the header is). If both are present
    # they must match — otherwise routing and storage disagree. Fall
    # back to server-generated UUID if neither is set (backward-compat
    # for old SDK builds).
    header_id = request.headers.get("x-session-id")
    body_id = data.get("id")
    if header_id and body_id and header_id != body_id:
        raise HTTPException(
            400,
            "X-Session-Id header does not match body 'id'",
        )
    supplied_id = header_id or body_id
    if supplied_id is not None:
        try:
            uuid.UUID(supplied_id)
        except (ValueError, TypeError):
            raise HTTPException(400, f"invalid session id format: {supplied_id!r}")
        existing = await get_session(supplied_id)
        if existing is not None:
            raise HTTPException(409, f"session id {supplied_id} already exists")
        data["id"] = supplied_id
    if data.get("provision", True):
        return await _sessions_create_eager(data)
    return await _sessions_create_lazy(data)


async def _sessions_create_lazy(data: dict) -> dict:
    """Create a session row only — no sandbox, no ACP, no scheduler.

    Used when the UI wants to render a session shell before paying the
    provisioning cost (daytona: ~15-30 s; local: ~2-3 s). The sandbox
    appears on the first ``POST /sessions/{id}/message`` (the pool
    cold-creates on demand).
    """
    # SECURITY: strip env/secrets first so they can't leak into agents.config.
    body_env, body_secrets = _pop_env_and_secrets(data)

    default_provider = data.get("provider") or data.get("config", {}).get("provider") or "unix_local"
    workspace = _extract_workspace(data, default_provider)
    volume_record = await _resolve_or_default_volume(data.get("volume_id"), default_provider)
    config_data = data.get("config", {})
    _merge_top_level_config(data, config_data)
    config_data.pop("dockerfile", None)
    config_data.pop("dockerfile_content", None)
    config_data.pop("shared_mounts", None)
    config_data.pop("root", None)
    config_data.pop("workspace", None)
    # ``extra_options`` is session-scoped (matches workspace); pop out of
    # config_data so it doesn't land in AgentConfig.
    extra_options = data.get("extra_options")
    if extra_options is None:
        extra_options = config_data.pop("extra_options", None)
    else:
        config_data.pop("extra_options", None)

    agent_id = data.get("agent_id")
    if agent_id:
        await _require_agent(agent_id)
        _reject_daytona_sibling_when_active(agent_id, default_provider)
    else:
        agent_id = str(uuid.uuid4())
        await upsert_agent(AgentRecord(
            id=agent_id, name=data.get("name"),
            config=AgentConfig.from_dict(
                {**config_data, "agent_type": data.get("agent_type", "opencode")}
            ),
        ))

    # Pull session cwd out of the body; agent config is pure identity now.
    # The default matches the per-provider home_dir that the FIRST sandbox
    # provision will spawn with, so session/new and every later session/load
    # use the same path (the JSONL hash key). For unix_local, this is the
    # per-agent volume subpath (or the workspace subpath when set); for
    # docker/daytona, a fixed mount point.
    if default_provider == "unix_local":
        home_subpath = f"workspaces/{workspace}" if workspace else f"agents/{agent_id}"
        default_cwd = str(Path(volume_record.provider_ref) / home_subpath)
    else:
        default_cwd = default_cwd_for_provider(default_provider)
    cwd = data.get("cwd", config_data.pop("cwd", default_cwd))

    session_id = data.get("id") or str(uuid.uuid4())
    lazy_user_pre_start = data.get("pre_start_commands") or []
    await upsert_session(
        session_id, agent_id, inner_session_id=None,
        volume_id=volume_record.id,
        env=body_env or {}, secrets=body_secrets or {},
        cwd=cwd,
        pre_start_commands=list(lazy_user_pre_start),
        workspace=workspace,
        extra_options=extra_options,
    )

    return {
        "id": session_id,
        "agent_id": agent_id,
        "volume_id": volume_record.id,
        "workspace": workspace,
        "sandbox_ref": None,
        "connected": False,
    }


async def _sessions_create_eager(data: dict) -> dict:
    """Create agent + provision compute via SessionPool + attach ACP in one call.

    Returns ``{agent_id, sandbox_ref, session_id, id, inner_session_id,
    volume_id, connected: true}`` — ready to POST /message against
    immediately. `sandbox_ref` is the provider sandbox ref (an opaque
    string), not the legacy ``sb_<hex>`` synthesized PK.

    Implementation: writes the agent + session rows + initial
    ``sandbox_state`` JSONB, then calls ``pool.get_session(session_id)``
    which runs the cold-create path (provisions sandbox, brings up
    supervisor, runs ACP ``session/new``, persists ``inner_session_id``
    on the session row).
    """
    from api.sandbox import Recipe, get_pool, state_for_provider

    # SECURITY: strip env/secrets first so they can't leak into agents.config.
    body_env, body_secrets = _pop_env_and_secrets(data)

    provider = data.get("provider", "unix_local")
    # Validate provider before any DB writes so a typo doesn't leave an
    # orphan agent row behind. ``state_for_provider`` is the single source
    # of truth for which provider names cold_create accepts.
    try:
        state_for_provider(provider, Recipe())
    except ValueError as e:
        raise HTTPException(400, str(e))

    workspace = _extract_workspace(data, provider)
    volume_record = await _resolve_or_default_volume(data.get("volume_id"), provider)
    agent_type = data.get("agent_type", "opencode")
    config_data = data.get("config", {})
    _merge_top_level_config(data, config_data)

    cwd = data.get("cwd", config_data.pop("cwd", None))
    root = data.get("root", config_data.pop("root", None))
    dockerfile = _materialize_dockerfile({**config_data, **data})
    shared_mounts = data.get("shared_mounts") or config_data.pop("shared_mounts", None) or []
    config_data.pop("dockerfile_content", None)
    config_data.pop("dockerfile", None)
    config_data.pop("workspace", None)
    # ``extra_options`` is session-scoped (matches workspace); pop it before
    # building AgentConfig so it doesn't appear as agent-identity config.
    extra_options = data.get("extra_options")
    if extra_options is None:
        extra_options = config_data.pop("extra_options", None)
    else:
        config_data.pop("extra_options", None)
    resources_data = data.get("resources")
    if resources_data is None:
        resources_data = config_data.pop("resources", None)
    else:
        config_data.pop("resources", None)
    try:
        resources = _resources_for_provider(provider, resources_data)
    except ValueError as e:
        raise HTTPException(400, str(e))

    # Mirror the lazy path's agent_id reuse: when the caller passes an
    # existing agent_id, this is "create another session under the same
    # agent" (multi-session SDK). Validate and reuse; otherwise mint a
    # fresh agent. Track which branch we took so the rollback path below
    # only deletes agents we created here.
    requested_agent_id = data.get("agent_id")
    if requested_agent_id:
        await _require_agent(requested_agent_id)
        _reject_daytona_sibling_when_active(requested_agent_id, provider)
        agent_id = requested_agent_id
        config = AgentConfig.from_dict({**config_data, "agent_type": agent_type})
        agent_was_created_here = False
    else:
        agent_id = str(uuid.uuid4())
        config = AgentConfig.from_dict({**config_data, "agent_type": agent_type})
        await upsert_agent(AgentRecord(id=agent_id, name=data.get("name"), config=config))
        agent_was_created_here = True

    user_pre_start = list(data.get("pre_start_commands") or [])
    # Merge skill-install commands ahead of user commands, so skills land
    # before any user setup that depends on them. For ``unix_local`` the
    # merge function installs skills on the host directly and returns the
    # user_pre_start unchanged (unix_local sandboxes share HOME with the
    # server).
    merged_pre_start = await _build_pre_start_commands(
        config, provider, user_pre_start,
    ) or []

    # Default cwd matches the per-provider HOME the first sandbox boots into,
    # so session/new and every later session/load share the JSONL hash key.
    # When workspace is set, HOME is ``workspaces/<ws>`` instead of
    # ``agents/<agent_id>`` — match that here so cwd lands in the right place.
    if cwd is None:
        if provider == "unix_local":
            home_subpath = f"workspaces/{workspace}" if workspace else f"agents/{agent_id}"
            cwd = str(Path(volume_record.provider_ref) / home_subpath)
        else:
            cwd = default_cwd_for_provider(provider)

    # Recipe carries the MERGED list (skills + user) so Type-2 recovery
    # re-runs both — the pool reads recipe.pre_start_commands directly,
    # it does not re-derive skills from agents.config.skills.
    recipe = Recipe(
        agent_type=agent_type,
        dockerfile=dockerfile,
        shared_mounts=list(shared_mounts) if shared_mounts else [],
        root=root,
        pre_start_commands=merged_pre_start,
        resources=resources,
        # Optional credential-refresh hook. When set, the SessionPool
        # spawns a background task per active session that polls this
        # URL and writes the returned files into the sandbox. Stays on
        # the recipe so it survives hibernation + recovery.
        credential_refresh_url=data.get("credential_refresh_url"),
        credential_refresh_token=data.get("credential_refresh_token"),
    )

    session_id = data.get("id") or str(uuid.uuid4())
    await upsert_session(
        session_id, agent_id, inner_session_id=None,
        volume_id=volume_record.id,
        env=body_env or {}, secrets=body_secrets or {},
        cwd=cwd,
        # Column stores RAW USER commands (not the merged skill+user
        # result). Skills come from ``agents.config.skills``; the merged
        # list lives on ``sandbox_state.recipe.pre_start_commands`` and
        # is re-derived on every reload. Matches the lazy path
        # (``server.py`` ~L1635) and the contract documented in
        # ``tests/test_pre_start_commands_persist.py``.
        pre_start_commands=user_pre_start,
        workspace=workspace,
        extra_options=extra_options,
    )
    pool = get_pool()
    try:
        # One phase log per cold_create — the slowest single call in the
        # session lifecycle (daytona ~15-30s, modal ~10-20s, local ~2-3s).
        # Slow cold_creates trip the WARNING level so they pop out of the
        # log without per-step instrumentation.
        async with timed_phase(
            "sessions.cold_create",
            session_id=session_id[:8], provider=provider,
        ):
            pool_session = await pool.cold_create(
                session_id, provider=provider, recipe=recipe,
            )
    except HTTPException:
        if agent_was_created_here:
            await delete_agent(agent_id)
        raise
    except Exception as e:
        if agent_was_created_here:
            await delete_agent(agent_id)
        log.error("sessions_create_eager: pool.cold_create failed (provider=%s): %s",
                  provider, e, exc_info=True)
        if "circuit breaker" in str(e).lower():
            raise HTTPException(503, str(e), headers={"Retry-After": "30"})
        raise HTTPException(502, f"Provider '{provider}' failed: {e}")

    # The pool's ``sandbox_state.sandbox_ref`` IS the sandbox identity now —
    # opaque provider ref (e.g. "abc-uuid" for Daytona, "local-abc12" for
    # unix_local). No separate ``sb_<hex>`` PK, no sandboxes-table row,
    # no dual-write trigger to mirror state into a parallel table.
    provider_ref = getattr(pool_session.state, "sandbox_ref", None)

    # Forward model/mode/thought_level so callers don't have to follow
    # POST /sessions with a separate POST /config. Read both top-level
    # and config_data because ``_merge_top_level_config`` already moved
    # ``model`` into config_data. Best-effort.
    await _forward_session_config(pool_session, data, config_data)

    return {
        "agent_id": agent_id,
        # `sandbox_ref` is the provider sandbox ref now, not the legacy
        # synthesized ``sb_<hex>`` PK. SDK uses it as an opaque string
        # identifier for resume/persistence — the change in meaning is
        # transparent to callers that only check non-None / equality.
        "sandbox_ref": provider_ref,
        "session_id": session_id,
        "id": session_id,
        "volume_id": volume_record.id,
        "workspace": workspace,
        "inner_session_id": pool_session.inner_session_id,
        "connected": True,
    }


_SESSION_CONFIG_FIELDS = (
    ("model", "set_model"),
    ("mode", "set_mode"),
    ("thought_level", "set_thought_level"),
)


async def _forward_session_config(
    pool_session,
    data: dict,
    config_data: dict | None = None,
) -> None:
    """Apply caller-provided ``model`` / ``mode`` / ``thought_level`` to a
    pool session via ACP ``set_*`` AND persist them on ``agents.config``
    so cold-recovery (Type-2) replays them via ``_attach_acp``.

    Without persistence, a mid-flight ``set_mode("plan")`` would land
    on the current ACP session but a sandbox restart would silently
    revert to default. Same bug class the model field already fixed.

    Best-effort: a transient ACP failure logs and continues. Fields are
    looked up first in ``data`` (top-level body — what the SDK sends),
    then in ``config_data`` (nested body — what ``_merge_top_level_config``
    may have promoted ``model`` into)."""
    cfg = config_data or {}
    applied: dict[str, str] = {}
    for key, method in _SESSION_CONFIG_FIELDS:
        val = data.get(key)
        if val is None:
            val = cfg.get(key)
        if val is None:
            continue
        try:
            await getattr(pool_session, method)(val)
            applied[key] = val
        except Exception as e:
            log.warning("forward %s(%r) to session %s failed: %s",
                        method, val, pool_session.session_id, e)
    # Persist to agents.config so the next cold-recovery replays them.
    # Skip if the ACP push failed for everything (don't promise persistence
    # we don't have). model already lives on AgentConfig.model; mode +
    # thought_level land on the new fields added for this purpose.
    if applied and pool_session._agent_id:
        try:
            agent = await get_agent(pool_session._agent_id)
            if agent and agent.config:
                changed = False
                for key, val in applied.items():
                    if getattr(agent.config, key, None) != val:
                        setattr(agent.config, key, val)
                        changed = True
                if changed:
                    await upsert_agent(agent)
        except Exception:
            log.exception(
                "persist session config to agents.config failed for %s",
                pool_session.session_id,
            )


# ---------------------------------------------------------------------------
# Turn execution engine -> api.services.turn_runner (refactor slice 7)
# ---------------------------------------------------------------------------
from .services.turn_runner import (  # noqa: E402,F401
    _EVENT_TYPE_TO_LOG,
    _PromptGate,
    _log_one,
    _persist_prompt_events,
    _persist_user_message,
)


@app.post("/sessions/{session_id}/message")
async def post_session_message(session_id: str, request: Request):
    """Fire-and-forget execution. Returns ``{rpc_id, status}`` immediately;
    events get persisted to ``session_log`` and broadcast to any
    ``/events`` subscribers. Internally the same SSE generator that
    backs ``POST /message+stream`` runs in a background task with the
    response body discarded — single execution path for both endpoints.

    Body: ``{"message": str, "interrupt": bool?}``. With ``interrupt=true``
    the in-flight prompt (if any) is cancelled — same effect as
    ``POST /sessions/{id}/cancel`` followed by this POST — so callers
    don't have to round-trip twice.

    Session resolution happens BEFORE the 200 reply (vs deferring into
    the background drain) so a hard-failure surfaces immediately to
    the client instead of returning 200 with a silently-broken stream.
    """
    data = await _json_body(request)
    message = data.get("message")
    if not message:
        raise HTTPException(400, "message required")

    rpc_id = str(uuid.uuid4())

    # Resolve (cold-recover if needed) before returning 200.
    from api.sandbox import get_pool
    pool_session = await get_pool().get_session(session_id)

    if data.get("interrupt"):
        # Best-effort: cancel the running ACP turn so this prompt
        # supersedes it. The cancelled turn's ``done`` event arrives via
        # the existing SSE stream with ``stop_reason="cancelled"`` and
        # is logged like any other turn_end.
        try:
            await pool_session.cancel_active_prompt()
        except Exception:
            log.exception("interrupt cancel failed for session %s", session_id)

    async def _drain() -> None:
        async for _ in _execute_and_stream_sse_for(pool_session, message, rpc_id):
            pass

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
    event.

    Live-only: subscribers receive events broadcast after they register.
    Historical events live in ``session_log`` and are served by
    ``GET /sessions/{id}/log``; clients that need both history and live
    updates load /log on mount, then open /events for the tail. (Earlier
    versions seeded a per-session replay buffer onto new subscribers; that
    double-delivered every event a cold-loading UI just fetched from
    /log. Recovery from a mid-turn SSE drop now goes via re-fetching
    /log rather than server-side replay.)

    Subscribes via ``SandboxSession.subscribe()`` which:
      * streams live broadcasts from ``execute_prompt``
      * yields the ``_HEARTBEAT`` sentinel during idle so intermediaries
        (nginx / cloudflare / browser EventSource) don't close the
        connection between prompts.

    Session resolution fires before the StreamingResponse is built so a
    hard-failure surfaces immediately rather than as a 200 with an empty
    body.
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
            #     (carry the rpc tag if the dict has one — error frames
            #     broadcast from ``_persist_prompt_events`` failure paths
            #     do; without the tag the UI's per-rpc dispatch drops
            #     them and the user sees silence — the data-research
            #     / Task Builder repro).
            if item is _HEARTBEAT:
                yield ": heartbeat\n\n"
            elif isinstance(item, tuple) and len(item) == 2:
                rpc_id, block = item
                yield f"event: rpc:{rpc_id}\n{block}\n\n"
            else:
                tag = item.get("rpc_id") if isinstance(item, dict) else None
                if tag:
                    yield f"event: rpc:{tag}\ndata: {json.dumps(item)}\n\n"
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

    Convenience over the two-step (``POST /message`` returns ``rpc_id``;
    client opens ``GET /events`` to consume). This endpoint returns the
    SSE stream as the response body — same wire format as ``GET /events``
    (``event: rpc:<id>\\n<raw_block>\\n\\n``), scoped to a single
    prompt. ``: heartbeat\\n\\n`` lines keep idle connections open
    through nginx / cloudflare.

    Body: ``{"message": str, "interrupt": bool?}``. ``interrupt`` is
    accepted for API parity but currently a no-op on the pool path —
    use ``POST /sessions/{id}/cancel`` to abort an in-flight turn.

    Both POST /message and GET /events continue to work unchanged for
    callers that need separate submit + multi-subscriber semantics.

    Session resolution happens BEFORE the StreamingResponse is constructed
    so a hard-failure surfaces as a normal HTTP error rather than as a
    200 with an empty body once the generator runs.
    """
    data = await _json_body(request)
    message = data.get("message")
    if not message:
        raise HTTPException(400, "message required")

    rpc_id = str(uuid.uuid4())

    from api.sandbox import get_pool
    session = await get_pool().get_session(session_id)

    return StreamingResponse(
        _execute_and_stream_sse_for(session, message, rpc_id),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )


# SSE streaming of a turn lives in api.services.turn_runner (slice 7b).
from .services.turn_runner import (  # noqa: E402,F401
    _execute_and_stream_sse_for,
    _is_terminal_frame,
    _sse_frame_for,
)


@app.post("/sessions/{session_id}/cancel")
async def session_cancel(session_id: str):
    """Cancel the in-flight prompt on this session, if any.

    Best-effort: sends ``session/cancel`` (JSON-RPC notification) to
    the supervisor's ACP child. Looks the session up via
    ``pool.get_session`` so cancel requests against a session owned by
    a peer replica route there (307 from the global exception handler)
    rather than no-oping on this replica's empty local cache.
    """
    from api.sandbox import get_pool

    pool_session = await get_pool().get_session(session_id)
    await pool_session.cancel_active_prompt()
    return {"status": "ok"}


@app.post("/sessions/{session_id}/release")
async def release_session_route(session_id: str):
    """Snapshot + drop the SessionPool's lease on this session's compute.

    Backed by ``api.sandbox.SessionPool.release``: writes a fresh
    filesystem snapshot to the volume and pauses (never deletes) the
    sandbox. Idempotent — a session with no active lease is a no-op.
    The next pool-mediated prompt restores from this snapshot.
    """
    from api.sandbox import deserialize, get_pool

    pool = get_pool()
    await pool.release(session_id)

    payload = await read_sandbox_state(session_id)
    state = deserialize(payload)
    return {
        "lifecycle": "hibernated",
        "snapshot_path": getattr(state, "snapshot_path", None),
        "snapshot_version": getattr(state, "snapshot_version", 0),
    }


@app.delete("/sessions/{session_id}", status_code=204)
async def delete_session_route(session_id: str):
    """Release the pool lease, destroy the underlying sandbox, and delete
    the session row.

    Idempotent — missing session returns 204, not 404, so callers can
    use this as a "make sure this session is gone" primitive without
    branching on prior state.

    The sandbox is **destroyed**, not paused. ``pool.release`` only
    pauses (correct for the hibernate / idle-reaper paths where a future
    prompt resumes the same sandbox). On DELETE the session row is
    dropped, so nothing can ever resume; leaving the sandbox paused
    leaks compute against the provider's quota with no automatic
    cleanup (``cleanup_orphans.py`` defaults to ``--origin test`` so
    production orphans need manual reaping).
    """
    from api import providers as _prov
    from api.sandbox import deserialize, get_pool

    # Capture sandbox ref + provider type from the DB BEFORE pool.release
    # wipes the in-memory state. Idempotency: a missing row returns None
    # from read_sandbox_state, and we fall through to delete_session
    # which is also idempotent.
    sandbox_ref: str | None = None
    provider_type: str | None = None
    try:
        payload = await read_sandbox_state(session_id)
        if payload is not None:
            state = deserialize(payload)
            sandbox_ref = getattr(state, "sandbox_ref", None)
            # ``state.type`` is the Pydantic discriminator
            # (``"unix_local"`` / ``"docker"`` / ``"daytona"`` /
            # ``"modal"``) — same key space as ``_PROVIDER_MODS``,
            # so this is a direct lookup.
            provider_type = getattr(state, "type", None)
    except Exception as e:
        log.warning("DELETE /sessions/%s: read state failed: %s",
                    session_id, e)

    try:
        await get_pool().release(session_id)
    except Exception as e:
        log.warning("DELETE /sessions/%s: pool.release failed: %s",
                    session_id, e)

    # Destroy the sandbox via the provider's uniform ``destroy_sandbox``
    # entry point. Best-effort — if the provider can't reach the sandbox
    # (already gone, network blip), we still drop the session row so the
    # caller's idempotency contract holds.
    if provider_type and sandbox_ref:
        try:
            mod = _prov._PROVIDER_MODS.get(provider_type)
            if mod is not None:
                await mod.destroy_sandbox(_prov.ProviderInstance(
                    provider=provider_type, url="", root="",
                    sandbox_ref=sandbox_ref,
                ))
        except Exception as e:
            log.warning("DELETE /sessions/%s: provider destroy failed (%s %s): %s",
                        session_id, provider_type, sandbox_ref[:16], e)

    # Drop the session row. ``ON DELETE CASCADE`` on session_log handles
    # the log rows; ``sandbox_state`` JSONB lives on the sessions row
    # itself so it goes with the row.
    await delete_session(session_id)


@app.post("/sessions/{session_id}/config")
async def session_set_config(session_id: str, request: Request):
    """Set mode/model/thought_level for a session via the SessionPool.

    These three are the persisted, replayed-on-recovery knobs (see
    ``_attach_acp``). Anything else ACP exposes — new ``configId``s
    Claude grows, vendor-specific extensions — should go through
    ``POST /sessions/{id}/acp/call`` so we don't grow a new typed
    field per knob.
    """
    data = await _json_body(request)
    from api.sandbox import get_pool

    pool_session = await get_pool().get_session(session_id)
    await _forward_session_config(pool_session, data)
    return {"status": "ok"}


@app.post("/sessions/{session_id}/reload")
async def session_reload(session_id: str, request: Request):
    """Hot-reload skills / MCP servers / CLI tools / pre-start on a live session.

    Body (PATCH-shaped — omit fields you don't want to change)::

        {
          "skills":             [...] | {...},
          "mcp_servers":        {...},
          "cli_tools":          [...] | {...},
          "secrets":            {...},
          "pre_start_commands": [...]
        }

    Steps:
      1. Update ``agents.config.{skills, mcp_servers, cli_tools}`` —
         persistent across cold-recovery. Update ``sessions.secrets``
         and ``sessions.pre_start_commands`` (both session-scoped) so
         the next supervisor spawn picks up new env values and the
         next Type-2 cold-recovery runs the new install set.
      2. Re-derive the merged ``pre_start_commands`` =
         ``_cli_install_commands(cli_tools)`` +
         ``_skills_install_commands(skills)`` +
         ``sessions.pre_start_commands`` (raw user portion — either
         the value just passed in, or the existing stored value)
         and overwrite ``sandbox_state.recipe.pre_start_commands`` so
         the next Type-2 recovery re-runs the new install set.
      3. Exec the install commands AND any newly-supplied user
         pre-start commands on the LIVE sandbox so they land on disk
         now — release+resume below is Type-1 and Type-1 does NOT
         re-run ``pre_start_commands``. User commands are NOT assumed
         idempotent: they only hot-exec when freshly supplied in this
         request, never on every reload.
      4. ``release`` only. Returns immediately. The NEXT user
         message cold-recovers the supervisor with the updated
         secrets in ``spawn_env``; ACP re-attaches with the new MCP
         set (``session.py:_attach_acp`` reads
         ``agent.config.mcp_servers`` and forwards to
         ``client.attach``). Conversation continuity is preserved
         via ``session/load``. Lazy on purpose — bringing the
         supervisor back up here would add 15-30s of sync latency on
         daytona/modal cold-recover for no benefit; the user's next
         prompt pays the cost they'd pay anyway.

    Old skills / CLI tools are NOT uninstalled — their files stay on
    disk until the volume is wiped. Removal is a follow-up.
    """
    data = await _json_body(request)
    mutable = {"skills", "mcp_servers", "cli_tools", "secrets", "pre_start_commands"}
    if not (mutable & data.keys()):
        raise HTTPException(
            400, f"body must include at least one of {sorted(mutable)}",
        )

    # ``secrets`` and ``pre_start_commands`` are session-scoped (live on
    # the sessions row, not agents.config). Pop them before persisting
    # the agent so they don't accidentally flow into ``AgentConfig``.
    new_secrets = data.pop("secrets", None)
    new_pre_start = data.pop("pre_start_commands", None)
    if new_pre_start is not None:
        if not isinstance(new_pre_start, list) or not all(
            isinstance(c, str) for c in new_pre_start
        ):
            raise HTTPException(
                400, "reload body 'pre_start_commands' must be a list of strings",
            )
    session_row = await _require_session_row(session_id)
    agent_id = session_row["agent_id"]
    agent = await _require_agent(agent_id)

    # 1. Persist on agent config.
    if "skills" in data:
        agent.config.skills = data["skills"]
    if "mcp_servers" in data:
        agent.config.mcp_servers = data["mcp_servers"]
    if "cli_tools" in data:
        agent.config.cli_tools = data["cli_tools"]
    await upsert_agent(agent)
    # 1b. Persist secrets on the session row. PATCH-shaped: ``{}`` clears,
    #     ``{...}`` replaces. Validated via the same dict-coercion the
    #     ``/sessions/{id}`` resume path uses so auth-key offenders are
    #     rejected here too instead of silently landing on the row.
    if new_secrets is not None:
        coerced = _coerce_env_dict(new_secrets, "reload body 'secrets'")
        await update_session_secrets(session_id, coerced)
    # 1c. Persist pre_start_commands (raw user portion) on the session row.
    #     PATCH-shaped: ``[]`` clears, ``[...]`` replaces. Matches the
    #     contract documented at ``upsert_session`` — column stores raw
    #     user commands, never the merged skill+cli+user result.
    if new_pre_start is not None:
        await update_session_pre_start_commands(session_id, list(new_pre_start))

    # 2. Re-derive merged pre_start, write to recipe in sandbox_state.
    #    Column stores raw user commands (post-2026-05 contract); skill +
    #    cli installs are layered in at use time. Order matches
    #    ``_build_pre_start_commands``: cli + skills + user.
    user_pre_start = (
        list(new_pre_start) if new_pre_start is not None
        else list(session_row.get("pre_start_commands") or [])
    )
    cli_install_cmds = (
        _cli_install_commands(agent.config.cli_tools)
        if agent.config.cli_tools else []
    )
    skill_install_cmds = (
        _skills_install_commands(agent.config.skills)
        if agent.config.skills else []
    )
    merged = cli_install_cmds + skill_install_cmds + user_pre_start
    state_jsonb = await read_sandbox_state(session_id)
    if state_jsonb is not None:
        recipe = state_jsonb.get("recipe") or {}
        recipe["pre_start_commands"] = merged
        state_jsonb["recipe"] = recipe
        await write_sandbox_state(session_id, state_jsonb)

    # 3. Exec the install set on the live sandbox so it's hot.
    #    Both ``npx skills add`` and ``uv tool install`` are idempotent
    #    on already-installed sources, so running the full set (not just
    #    the delta) keeps the code simple. ``mkdir -p
    #    $HOME/.claude/skills`` mirrors the daytona pre-start wrapper.
    #    Newly-supplied user pre-start commands are appended LAST (same
    #    order as ``_build_pre_start_commands``: cli + skills + user) and
    #    only when the caller passed ``pre_start_commands`` in this
    #    request — user commands aren't assumed idempotent, so re-running
    #    them on every reload would be unsafe.
    #    Best-effort: a single failed exec doesn't abort the reload —
    #    release+resume below still runs.
    live_cmds = (
        # CLI installs first so user tools depending on them work right away.
        cli_install_cmds
        # Skills install, with the mkdir guard.
        + [f"mkdir -p $HOME/.claude/skills && {c}" for c in skill_install_cmds]
        # User pre-start (only when explicitly supplied this request).
        + (list(new_pre_start) if new_pre_start is not None else [])
    )
    for cmd in live_cmds:
        try:
            resp = await _proxy_from_session(
                session_id, "POST", "/v1/exec",
                json={"command": cmd, "timeout": 180},
                timeout=200,
            )
            if resp.status_code >= 400:
                log.warning(
                    "reload: live install exec returned HTTP %d for %r",
                    resp.status_code, cmd,
                )
        except Exception:
            log.exception("reload: live install exec raised for %r", cmd)

    # 4. Release. The next user message cold-recovers the supervisor:
    #    it rescans ``~/.claude/skills/``, sees newly-installed CLIs
    #    on PATH, and ACP attach passes the new MCP set (via the fix
    #    at session.py:_attach_acp) + the new secrets in spawn_env.
    #    Lazy on purpose — bringing the supervisor back up here would
    #    add 15-30s of sync latency on daytona/modal cold-recover for
    #    no benefit; the user's next prompt pays the cost they'd pay
    #    anyway. Matches hive-space's release_session-then-next-message
    #    pattern.
    from api.sandbox import get_pool
    await get_pool().release(session_id)

    return {
        "status": "ok",
        "skills": agent.config.skills,
        "mcp_servers": agent.config.mcp_servers,
        "cli_tools": agent.config.cli_tools,
        # secrets are session-scoped + sensitive — surface only the key
        # set in the response (mirrors ``GET /sessions/{id}``'s redaction).
        "secret_keys": sorted(new_secrets.keys()) if new_secrets else None,
        # User portion (raw) — what's stored on the session row.
        "user_pre_start_commands": user_pre_start,
        # Merged install set (cli + skills + user) — what's written to
        # ``sandbox_state.recipe.pre_start_commands`` for Type-2 recovery.
        "pre_start_commands": merged,
    }


@app.post("/sessions/{session_id}/acp/call")
async def session_acp_call(session_id: str, request: Request):
    """Generic passthrough to the session's ACP supervisor.

    Body: ``{"method": "session/...", "params": {...}, "notify": false}``.
    Auto-injects the inner ``sessionId`` into ``params`` so callers
    don't need to track it. ``notify=true`` sends as a JSON-RPC
    notification (no response, no rpc_id) — required for
    ``session/cancel`` and other handlers ACP routes via
    ``notificationHandler``.

    Used for anything the typed wrappers don't cover: Claude's
    ever-growing ``configOptions`` set, vendor-specific extensions,
    debugging, etc. NOT a replacement for the persisted config knobs
    (model / mode / thought_level) — those go through
    ``POST /sessions/{id}/config`` so cold-recovery replays them.
    Anything called here is transient — survives only the current
    ACP session, lost on the next restart.
    """
    data = await _json_body(request)
    method = data.get("method")
    if not method or not isinstance(method, str):
        raise HTTPException(400, "method (str) required")
    params = data.get("params") or {}
    notify = bool(data.get("notify"))
    from api.sandbox import get_pool
    pool_session = await get_pool().get_session(session_id)
    try:
        result = await pool_session.acp_call(method, params, notify=notify)
    except RuntimeError as e:
        raise HTTPException(503, str(e))
    return {"result": result}


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


# ---------------------------------------------------------------------------
# Session-scoped filesystem browsing -> api.routers.session_files (slice 6f)
# ---------------------------------------------------------------------------

from .routers import session_files as _session_files_router  # noqa: E402

app.include_router(_session_files_router.router)


# ---------------------------------------------------------------------------
# Static UI — routes live in api.routers.ui (refactor slice 6)
# ---------------------------------------------------------------------------

from .routers import ui as _ui_router  # noqa: E402

app.include_router(_ui_router.router)
