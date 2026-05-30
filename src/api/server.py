"""REST API server — agent / volume / session orchestration layer.

Sandbox identity is implicit and owned in-process by the
``api.sandbox.SessionPool``
There is no ``/sandboxes`` resource; ``GET /sessions/{id}/sandbox``
returns the metadata.

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
import uuid
from contextlib import asynccontextmanager
from pathlib import Path
from urllib.parse import quote

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

from .event_buffer import get_batcher, start_batcher, stop_batcher
from .timing import extract_session_id, log_request, timed_phase
from .db import (
    close_pool,
    count_sessions_by_volume,
    delete_agent,
    delete_session,
    delete_sessions_by_volume,
    delete_volume,
    get_agent,
    get_session,
    get_session_log,
    get_volume,
    get_volume_by_name,
    init_db,
    init_pool,
    list_agents,
    list_sessions,
    list_volumes,
    log_event,
    read_sandbox_state,
    update_session_env,
    update_session_pre_start_commands,
    update_session_secrets,
    upsert_agent,
    upsert_session,
    upsert_volume,
    write_sandbox_state,
)
from .models import (
    EVT_ASSISTANT_MESSAGE,
    EVT_ERROR,
    EVT_REASONING,
    EVT_TOOL_CALL,
    EVT_TOOL_RESULT,
    EVT_USAGE,
    EVT_USER_MESSAGE,
    AgentConfig,
    AgentRecord,
    VolumeRecord,
)
from . import providers as _providers_mod
from .sandbox import SessionNotFoundError
from .providers import (
    VolumeFileExistsError,
    default_cwd_for_provider,
    get_volume_adapter,
    _normalize_workspace,
)
from .providers._shared import _safe_path as _shared_safe_path
from .redact import redact_pre_start_commands, redact_secrets

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


# Module-shared httpx client for the supervisor-proxy hot paths
# (_proxy_from_session, _download_from_session). httpx pools connections
# per-host internally, so file-browse sequences against the same session
# reuse the existing TCP+TLS handshake instead of paying ~12ms setup per
# call. Bench (50 concurrent /ping calls): per-request client = 80 RPS,
# shared client = 599 RPS. Opened in lifespan, closed at shutdown.
_HTTP_CLIENT: httpx.AsyncClient | None = None




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

    global _HTTP_CLIENT
    _HTTP_CLIENT = httpx.AsyncClient(
        timeout=60,
        limits=httpx.Limits(max_keepalive_connections=200, max_connections=400),
    )

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
    if _HTTP_CLIENT is not None:
        await _HTTP_CLIENT.aclose()


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


@app.exception_handler(SessionNotFoundError)
async def _session_not_found_handler(request: Request, exc: SessionNotFoundError):
    """A resolved session whose ``sessions`` row is gone (deleted, swept,
    or never existed) is a 404 — not a 500. ``pool.get_session`` raises
    this instead of cold-bootstrapping a ghost session. Returning 404
    lets the UI distinguish "this session is gone, stop reconnecting"
    from a transient 5xx it should retry. Without it a stale EventSource
    pointed at a deleted session_id hammers /events every 2s forever and
    each tick dumps a RuntimeError stack into the logs.
    """
    return JSONResponse({"error": f"session {exc} not found"}, status_code=404)


# No NotOwner handler. The per-session lease was retired in favor of
# per-worker liveness: we trust the LB's consistent-hash to route a given
# session to the same replica every time, and accept the narrow
# split-brain window at rebalance (mitigated in a follow-up via volume
# flock — see PR description). The previous handler emitted 307s for
# wrong-replica requests; with no per-session owner_id we can no longer
# point at "the right replica" — but we no longer need to.


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
    """Return shell commands to install skills via ``npx skills add``.

    A source like ``owner/repo@skill-name`` is a single-skill filter. Pass
    ``--all`` only when no ``@<skill>`` suffix is given, so the filter is
    respected — otherwise ``--all`` overrides it and pulls every skill
    from the repo (e.g. ``github/awesome-copilot`` ships hundreds).
    ``npx -y`` only approves npx package resolution; ``skills add`` needs
    its own ``--yes`` flag to avoid the agent-selection prompt.
    """
    sources = _normalize_skills(skills)
    cmds: list[str] = []
    for source in sources:
        flags = "--yes -g" if "@" in source else "--yes --all -g"
        cmds.append(f"npx -y skills add {shlex.quote(source)} {flags}")
    return cmds


def _normalize_cli_tools(cli_tools) -> list[str]:
    """Normalize cli_tools config into a list of source strings for ``uv tool install``.

    Accepts:
      - list[str]:  ["hive-evolve", "git+https://github.com/owner/repo@v1"]
      - dict:       {"hive": {"source": "git+https://...", "version": "1.2.3"}, ...}

    Dict-form ``version`` becomes a ``==<version>`` suffix when the source has
    no version specifier already (PEP 440 / uv syntax). VCS sources with a
    ``@<ref>`` already pinned are passed through unchanged.
    """
    if cli_tools is None:
        return []
    if isinstance(cli_tools, list):
        return [str(s) for s in cli_tools if s]
    if isinstance(cli_tools, dict):
        sources: list[str] = []
        for _name, cfg in cli_tools.items():
            if isinstance(cfg, str):
                sources.append(cfg)
            elif isinstance(cfg, dict):
                src = cfg.get("source", "")
                version = cfg.get("version")
                if not src:
                    continue
                if version and "==" not in src and not (
                    "git+" in src and "@" in src.split("/")[-1]
                ):
                    src = f"{src}=={version}"
                sources.append(src)
        return sources
    return []


def _cli_install_commands(cli_tools) -> list[str]:
    """Return shell commands to install CLI tools via ``uv tool install``.

    Assumes ``uv`` is on PATH (baked into the runtime image — see Dockerfile).
    Per-tool binaries land in ``$HOME/.local/bin/`` which the supervisor wires
    into the ACP child / ``/v1/exec`` PATH so the agent can invoke them.

    ``uv tool install`` is idempotent: skipped silently when the source is
    already at the requested version. Callers wanting forced upgrade should
    pin a version in the spec (``hive==2.0.0`` or VCS ``@<new-ref>``).
    """
    sources = _normalize_cli_tools(cli_tools)
    return [f"uv tool install {shlex.quote(s)}" for s in sources]


def _resources_for_provider(provider: str, resources_data):
    """Build and validate per-session resources, applying provider defaults."""
    from api.sandbox.state import Resources, validate_resources_for_provider

    if resources_data is None and provider == "modal":
        resources = Resources(gpu="T4")
    else:
        resources = Resources(**resources_data) if resources_data else None
    validate_resources_for_provider(provider, resources)
    return resources


async def _build_pre_start_commands(
    config, provider: str, user_cmds: list[str] | None,
) -> list[str] | None:
    """Build the combined pre-start command list for provisioning.

    Layer order (CLI tools FIRST, then skills, then user):
        cli_install_commands + skill_install_commands + user_cmds

    Rationale: ``cli_tools`` (e.g. ``hive``, ``gh``) are foundational —
    user-supplied ``pre_start_commands`` may invoke them (``hive setup``,
    ``gh auth login`` ...). Skills are independent of both, kept after
    CLI for symmetry with the historical merge order.

    For ``unix_local`` we run skill + CLI installs on the host directly and
    return ``None`` — the unix_local sandbox shares HOME with the server,
    so caller-supplied user commands would execute with server privileges
    (deliberately unsupported). Host-installed binaries land in
    ``$HOME/.local/bin`` (host), reachable from the supervisor because its
    PATH inherits the launching shell's.
    """
    cli_cmds = _cli_install_commands(config.cli_tools) if config.cli_tools else []
    skill_cmds = _skills_install_commands(config.skills) if config.skills else []
    if provider == "unix_local":
        for cmd in cli_cmds + skill_cmds:
            try:
                log.info("installing on host (unix_local): %s", cmd)
                proc = await asyncio.create_subprocess_shell(
                    cmd, stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                )
                stdout, stderr = await asyncio.wait_for(
                    proc.communicate(), timeout=180,
                )
                if proc.returncode != 0:
                    raise RuntimeError(
                        f"host install failed: {stderr.decode()[:500]}"
                    )
                log.info("host install OK: %s", stdout.decode()[-200:].strip())
            except Exception as e:
                log.error("host install failed, continuing without it: %s", e)
                break
        return None
    combined = cli_cmds + skill_cmds + list(user_cmds or [])
    return combined or None


# ---------------------------------------------------------------------------
# Request helpers
# ---------------------------------------------------------------------------

_CONFIG_KEYS = (
    "model",
    "mcp_servers",
    "skills",
    "cli_tools",
    "agent_type",
    "mode",
    "thought_level",
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

    session_count = await count_sessions_by_volume(vol.id)
    if session_count > 0 and not force:
        raise HTTPException(
            409,
            f"Volume has {session_count} session(s). "
            f"Use ?force=true to cascade.",
        )
    if force and session_count > 0:
        # FK RESTRICT on volume blocks the final delete otherwise.
        await delete_sessions_by_volume(vol.id)

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
    """Body for ``POST /volumes/{id}/files/edit``. Two shapes:

    * Overwrite: ``{path, content}`` — write ``content`` as the
      new file body (creating the file if needed).
    * Search/replace: ``{path, old_string, new_string, replace_all?}``
      — read the file, ``str.replace`` the substring, write back.
      Server-side at the provider's volume layer; no sandbox needed.
      ``replace_all`` defaults to false (single replacement; raises if
      ``old_string`` matches more than once, mirroring the supervisor's
      session-scoped /files/edit semantics).

    Validation: at least one of ``content`` / ``old_string`` must be
    present. ``content`` and ``old_string`` are mutually exclusive."""
    path: str
    content: str | None = None
    old_string: str | None = None
    new_string: str | None = None
    replace_all: bool = False


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


# Threshold (bytes) above which a sync CPU op is offloaded to the default
# thread pool. Below it, inline is faster (no thread dispatch).
#
# 4 MB picked from bench: at 2 MB the wrap cost (~3ms dispatch) was
# observable as a regression on a single-tenant load test (no other
# requests competing for the loop, so isolation has no benefit, only
# overhead). At 4+ MB the inline loop-block (~20ms+) clearly outweighs
# dispatch cost. The wrap is purely an isolation fix in production —
# base64 doesn't release the GIL so it can't speed up the work itself,
# only keep the loop responsive for sibling requests.
_INLINE_BYTES_THRESHOLD = 4 * 1024 * 1024


async def _maybe_in_thread(fn, payload, *args, **kwargs):
    """Run ``fn(payload, *args, **kwargs)`` inline if payload is small,
    or via ``asyncio.to_thread`` if large. Used for base64 codec on
    /volumes/.../files/{read,upload} where payloads can be many MB.
    """
    if len(payload) < _INLINE_BYTES_THRESHOLD:
        return fn(payload, *args, **kwargs)
    return await asyncio.to_thread(fn, payload, *args, **kwargs)


@app.get("/volumes/{id_or_name}/files/tree")
async def volume_files_tree(id_or_name: str, path: str = ""):
    vol = await _resolve_volume(id_or_name)
    adapter = get_volume_adapter(vol.provider, vol.provider_ref)
    rel = _safe_path(path)
    try:
        tree = await adapter.tree(rel)
    except Exception as e:
        raise _volume_fs_err("Tree", vol.provider, e)
    return {"tree": tree}


@app.get("/volumes/{id_or_name}/files/read")
async def volume_files_read(id_or_name: str, path: str):
    vol = await _resolve_volume(id_or_name)
    adapter = get_volume_adapter(vol.provider, vol.provider_ref)
    rel = _safe_path(path)
    try:
        data = await adapter.read(rel)
    except Exception as e:
        raise _volume_fs_err("Read", vol.provider, e)
    # v1 response contract: text content.
    try:
        return {"content": data.decode()}
    except UnicodeDecodeError:
        # Offload large encodes so the loop stays free for other requests.
        # Threshold ~1 MB: smaller payloads finish in <1ms inline (thread
        # overhead would slow them); above that the encode can hold the
        # loop for tens of ms — bench at 100 MB inline = 94ms loop block.
        encoded = await _maybe_in_thread(base64.b64encode, data)
        return {"content_base64": encoded.decode()}


@app.get("/volumes/{id_or_name}/files/download")
async def volume_files_download(id_or_name: str, path: str):
    """Download a volume file as raw bytes."""
    vol = await _resolve_volume(id_or_name)
    adapter = get_volume_adapter(vol.provider, vol.provider_ref)
    rel = _safe_path(path)
    try:
        data = await adapter.download(rel)
    except Exception as e:
        raise _volume_fs_err("Download", vol.provider, e)

    filename = path.rsplit("/", 1)[-1] or "download"
    ascii_filename = filename.encode("ascii", "ignore").decode() or "download"
    return Response(
        content=data,
        media_type="application/octet-stream",
        headers={
            "content-disposition": (
                f'attachment; filename="{ascii_filename}"; '
                f"filename*=UTF-8''{quote(filename)}"
            )
        },
    )


@app.get("/volumes/{id_or_name}/files/exists")
async def volume_files_exists(id_or_name: str, path: str):
    vol = await _resolve_volume(id_or_name)
    adapter = get_volume_adapter(vol.provider, vol.provider_ref)
    rel = _safe_path(path)
    try:
        exists = await adapter.exists(rel)
    except Exception as e:
        raise _volume_fs_err("Exists", vol.provider, e)
    return {"exists": exists}


@app.post("/volumes/{id_or_name}/files/edit", status_code=204)
async def volume_files_edit(id_or_name: str, body: _VolumeEditBody):
    vol = await _resolve_volume(id_or_name)
    adapter = get_volume_adapter(vol.provider, vol.provider_ref)
    rel = _safe_path(body.path)

    # Validate the two shapes are not mixed.
    if body.content is not None and body.old_string is not None:
        raise HTTPException(
            400, "supply either ``content`` (overwrite) or "
                 "``old_string``+``new_string`` (search/replace), not both",
        )
    if body.content is None and body.old_string is None:
        raise HTTPException(
            400, "must supply either ``content`` (overwrite) or "
                 "``old_string`` (search/replace)",
        )

    try:
        if body.content is not None:
            await adapter.write(rel, body.content.encode())
            return
        # Search/replace at the volume layer (no sandbox required):
        # read → str.replace → write. Same semantics as the
        # supervisor's session-scoped /files/edit, but driven directly
        # against the provider's volume primitives so callers don't
        # need a live sandbox to edit files on the volume.
        existing = (await adapter.read(rel)).decode("utf-8", errors="replace")
        old = body.old_string or ""
        new = body.new_string or ""
        if not body.replace_all:
            occurrences = existing.count(old)
            if occurrences == 0:
                raise HTTPException(404, f"old_string not found in {rel!r}")
            if occurrences > 1:
                raise HTTPException(
                    409,
                    f"old_string matches {occurrences} times in {rel!r}; "
                    "pass replace_all=true to replace all",
                )
            updated = existing.replace(old, new, 1)
        else:
            updated = existing.replace(old, new)
        await adapter.write(rel, updated.encode())
    except HTTPException:
        raise
    except Exception as e:
        raise _volume_fs_err("Edit", vol.provider, e)


@app.post("/volumes/{id_or_name}/files/upload", status_code=204)
async def volume_files_upload(id_or_name: str, body: _VolumeUploadBody):
    vol = await _resolve_volume(id_or_name)
    adapter = get_volume_adapter(vol.provider, vol.provider_ref)
    rel = _safe_path(body.path)
    try:
        # Offload large decodes so the loop stays responsive for other
        # concurrent requests; small payloads stay inline to avoid the
        # thread-dispatch overhead. base64 doesn't release the GIL, so
        # this isn't a speedup — it's an isolation fix.
        payload = await _maybe_in_thread(
            base64.b64decode, body.content, validate=True,
        )
    except Exception as e:
        raise HTTPException(400, f"invalid base64 content: {e}")
    try:
        await adapter.upload(rel, payload)
    except Exception as e:
        raise _volume_fs_err("Upload", vol.provider, e)


@app.post("/volumes/{id_or_name}/files/mkdir", status_code=204)
async def volume_files_mkdir(id_or_name: str, body: _VolumePathBody):
    vol = await _resolve_volume(id_or_name)
    adapter = get_volume_adapter(vol.provider, vol.provider_ref)
    rel = _safe_path(body.path)
    try:
        await adapter.mkdir(rel)
    except Exception as e:
        raise _volume_fs_err("Mkdir", vol.provider, e)


@app.post("/volumes/{id_or_name}/files/delete", status_code=204)
async def volume_files_delete(id_or_name: str, body: _VolumePathBody):
    vol = await _resolve_volume(id_or_name)
    adapter = get_volume_adapter(vol.provider, vol.provider_ref)
    rel = _safe_path(body.path)
    try:
        await adapter.delete(rel)
    except Exception as e:
        raise _volume_fs_err("Delete", vol.provider, e)


@app.post("/volumes/{id_or_name}/files/rename", status_code=204)
async def volume_files_rename(id_or_name: str, body: _VolumeRenameBody):
    vol = await _resolve_volume(id_or_name)
    adapter = get_volume_adapter(vol.provider, vol.provider_ref)
    src = _safe_path(body.path)
    dst = _safe_path(body.new_path)
    try:
        await adapter.rename(src, dst, overwrite=body.overwrite)
    except VolumeFileExistsError:
        return JSONResponse(
            {"error": "exists", "path": body.new_path},
            status_code=409,
        )
    except Exception as e:
        raise _volume_fs_err("Rename", vol.provider, e)


# ---------------------------------------------------------------------------
# Admin
# ---------------------------------------------------------------------------


@app.get("/admin/sessions")
async def admin_list_sessions():
    """List sessions for the dashboard + cleanup debugging.

    Cluster-aware. Only sessions with a currently-held lease are
    returned here (the "active" list); cold / inactive sessions are
    available at ``/admin/sessions/inactive``. ``agent_busy`` is read
    from the DB ``busy_at`` column with a 60s TTL filter so a crashed
    replica can't leave a stuck flag — the next lease claim resets it
    too as a belt-and-braces.
    """
    from api.sandbox import get_pool
    pool = get_pool()
    my_addr = pool._owner_addr  # noqa: SLF001
    rows = await list_sessions(limit=10000)
    sessions_out: list[dict] = []
    instances_out: list[dict] = []
    for r in rows:
        if not r.get("leased"):
            # Cold / unleased — surfaced via /admin/sessions/inactive.
            continue
        sid = r["id"]
        sb = r.get("sandbox_state") or {}
        sandbox_ref = sb.get("sandbox_ref") if isinstance(sb, dict) else None
        provider = sb.get("type", "unknown") if isinstance(sb, dict) else "unknown"
        listen_port = sb.get("listen_port") if isinstance(sb, dict) else None
        cached = pool._active.get(sid)  # noqa: SLF001 — admin readout
        is_mine = cached is not None
        sessions_out.append({
            "session_id": sid,
            "agent_id": r["agent_id"],
            "sandbox_ref": sandbox_ref,
            "inner_session_id": r.get("inner_session_id"),
            # Cluster-wide busy signal — TTL-filtered at the DB layer so
            # crashed replicas can't leave a stuck flag.
            "agent_busy": bool(r.get("busy")),
            "session_subscribers": len(cached._subscribers) if cached else 0,
            "lease_owner_id": r.get("lease_owner_id"),
            "lease_owner_addr": r.get("lease_owner_addr"),
            "owned_by_me": is_mine,
        })
        if sandbox_ref:
            instances_out.append({
                "sandbox_ref": sandbox_ref,
                "provider": provider,
                "url": cached.supervisor_url if cached else None,
                "port": listen_port,
                "container_id": None,
                "process_alive": is_mine and cached.supervisor_url is not None,
            })
    return {"sessions": sessions_out, "instances": instances_out, "this_replica": my_addr}


@app.get("/admin/sessions/inactive")
async def admin_list_inactive_sessions(
    q: str | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=1000),
):
    """DB session rows whose lease is expired or absent — cluster-wide.
    Optional ``q`` name-substring filter and ``limit`` cap (default 100,
    max 1000) are applied at the DB layer.

    Cluster-aware via the ``leased`` derived column on
    ``list_sessions``: ``leased = lease_owner_id IS NOT NULL AND
    lease_expires_at > now()``. A peer replica's leased session won't
    show up here regardless of which replica answers the query.
    """
    return {"sessions": [
        {
            "session_id": r["id"],
            "agent_id": r["agent_id"],
            "inner_session_id": r["inner_session_id"],
            "sandbox_ref": (r["sandbox_state"] or {}).get("sandbox_ref"),
        }
        for r in await list_sessions(q=q, limit=limit) if not r.get("leased")
    ]}


# ---------------------------------------------------------------------------
# Session operations (on sandbox)
# ---------------------------------------------------------------------------




@app.get("/sessions")
async def list_sessions_route():
    """List sessions currently leased by the SessionPool. Hibernated +
    cold sessions don't appear here — query the DB / GET /sessions/{id}
    directly for those."""
    from api.sandbox import get_pool
    now = time.time()
    out = []
    for s in get_pool()._active.values():  # noqa: SLF001
        # Report the reaper's clock (compute-only) so idle_seconds matches
        # when the session will actually hibernate — not viewer activity.
        last = s.liveness._last_compute_at  # noqa: SLF001
        out.append({
            "session_id": s.session_id,
            "agent_id": s._agent_id,  # noqa: SLF001
            "sandbox_ref": getattr(s.state, "sandbox_ref", None),
            "idle_seconds": round(now - last, 1) if last else None,
            "shutdown_requested": False,
        })
    return out


@app.get("/sessions/{session_id}")
async def get_session_route(session_id: str):
    """Return stored session metadata. Redacts secret values — only keys.

    ``env`` is returned in full (non-sensitive). ``secrets`` is returned as
    ``{"keys": [...]}`` (names only) so callers can confirm what's stored
    without leaking values. Values are never serialized to clients.

    ``pre_start_commands`` are run through ``redact_pre_start_commands``
    because callers commonly embed config payloads via
    ``echo <base64-blob> | base64 -d > /path/file.json``, and the inner
    blob can contain credentials (e.g. hivespace's per-agent JSON cfg
    holds the agent's token). Stripping the blob keeps the field useful
    for "is this set / how many" debugging without leaking content.
    """
    rec = await _require_session_row(session_id)
    env = rec.get("env") or {}
    secrets = rec.get("secrets") or {}
    sb_state = rec.get("sandbox_state") or {}
    return {
        "session_id": rec.get("id"),
        "agent_id": rec.get("agent_id"),
        "volume_id": rec.get("volume_id"),
        "workspace": rec.get("workspace"),
        "sandbox_ref": sb_state.get("sandbox_ref") if isinstance(sb_state, dict) else None,
        "inner_session_id": rec.get("inner_session_id"),
        "env": env,
        "secrets": {"keys": sorted(secrets.keys())},
        "pre_start_commands": redact_pre_start_commands(rec.get("pre_start_commands") or []),
    }


@app.get("/sessions/{session_id}/status")
async def session_status(session_id: str):
    """Session runtime status. Read-only — does NOT cold-recover a
    hibernated session. UI status polls would otherwise unhibernate the
    sandbox on every poll, defeating the reaper.

    Tries the pool's live cache first (peek mode). If not cached, falls
    back to a DB read of ``sessions.sandbox_state`` JSONB plus the
    ``sessions`` row. Live-only fields (``last_activity``, subscriber
    count, ``has_client``, ``supervisor_url``) become None / 0 / False
    when the session isn't live in the pool."""
    from api.sandbox import get_pool

    now = time.time()
    try:
        pool_session = await get_pool().get_session(session_id, peek=True)
    except KeyError:
        sess = await get_session(session_id)
        if sess is None:
            raise HTTPException(404, f"Session {session_id} not found")
        sb_state = sess.get("sandbox_state") or {}
        sandbox_ref = sb_state.get("sandbox_ref") if isinstance(sb_state, dict) else None
        return {
            "session_id": session_id,
            "agent_id": sess.get("agent_id"),
            "sandbox_ref": sandbox_ref,
            "inner_session_id": sess.get("inner_session_id"),
            "agent_busy": False,
            "session_subscriber_count": 0,
            "last_activity": None,
            "idle_seconds": None,
            "has_client": False,
            "supervisor_url": None,
            "supervisor_port": sb_state.get("listen_port") if isinstance(sb_state, dict) else None,
        }
    state = pool_session.state
    # Compute-only clock so idle_seconds reflects reaper timing, not viewer
    # traffic (an open /events or status poll no longer skews this).
    last_chunk = pool_session.liveness._last_compute_at
    return {
        "session_id": session_id,
        "agent_id": pool_session._agent_id,
        "sandbox_ref": getattr(state, "sandbox_ref", None),
        "inner_session_id": pool_session.inner_session_id,
        "agent_busy": False,
        "session_subscriber_count": len(pool_session._subscribers),
        "last_activity": last_chunk,
        "idle_seconds": round(now - last_chunk, 1) if last_chunk else None,
        "has_client": pool_session.supervisor_url is not None,
        "supervisor_url": pool_session.supervisor_url,
        "supervisor_port": getattr(state, "listen_port", None),
    }


@app.post("/sessions/{session_id}/reap")
async def session_reap(session_id: str, request: Request):
    """Hibernate this session IFF it meets the idle reaper's criteria.

    Ops / diagnostic route: reclaim a sandbox you know is idle now,
    without waiting for the background reaper's next tick. Also the
    deterministic, per-session seam the golden reaper test drives (the
    global reaper's timing can't be exercised per-session under
    ``-n auto``).

    Query ``idle_s`` sets an EXPLICIT idle threshold and is authoritative
    when provided — it overrides the per-provider windows (so ``idle_s=0``
    really does hibernate any session that isn't actively producing
    output, even on modal whose background window is 30 min). When
    ``idle_s`` is omitted, the configured background windows apply,
    including the modal-specific one. Session-scoped path so the
    consistent-hash routing lands it on the owning replica; a session
    not active here returns ``{hibernated: false, reason: 'not_active'}``.
    Unlike ``/release`` (which hibernates unconditionally) this runs the
    SAME decision the reaper uses, so it reflects the real reap policy.
    """
    from api.sandbox import get_pool
    from api.sandbox.runtime import _MODAL_REAPER_IDLE_S, _REAPER_IDLE_S
    raw = request.query_params.get("idle_s")
    if raw is not None:
        # Explicit threshold wins — no per-provider override, so the
        # caller's number means exactly what it says on every provider.
        try:
            idle_s = float(raw)
        except ValueError:
            raise HTTPException(400, f"idle_s must be a number, got {raw!r}")
        provider_idle_s = None
    else:
        idle_s = _REAPER_IDLE_S
        provider_idle_s = {"modal": _MODAL_REAPER_IDLE_S}
    return await get_pool().reap_session(
        session_id, idle_s, provider_idle_s=provider_idle_s,
    )


@app.get("/sessions/{session_id}/sandbox")
async def session_sandbox_info(session_id: str):
    """Sandbox metadata. Read-only — does NOT cold-recover a hibernated
    session. Falls back to a DB read of ``sessions.sandbox_state`` JSONB
    when the session isn't in the live pool.

    Returns the same shape as ``GET /sandboxes/{id}`` (provider,
    sandbox_ref, status, root, url for port-based providers,
    marker_path for local) so test helpers and admin UIs that need
    sandbox info can stay in session-id space and avoid the
    sandbox-row-id round trip. ``url`` is omitted when the session
    isn't live (no supervisor running)."""
    from api.sandbox import deserialize, get_pool
    try:
        pool_session = await get_pool().get_session(session_id, peek=True)
    except KeyError:
        # Not in live pool — read from DB
        sb_payload = await read_sandbox_state(session_id)
        if sb_payload is None:
            raise HTTPException(404, f"Session {session_id} not found")
        state = deserialize(sb_payload)
        provider = getattr(state, "type", "unknown")
        sandbox_ref = getattr(state, "sandbox_ref", None)
        result: dict = {
            "session_id": session_id,
            "provider": provider,
            "sandbox_ref": sandbox_ref,
            "status": "hibernated" if sandbox_ref else "missing",
            "root": (state.recipe.root if state.recipe else None) or "/tmp",
        }
        if provider == "unix_local" and sandbox_ref:
            from .providers.unix_local import _load_record
            marker, _rec = await asyncio.to_thread(_load_record, sandbox_ref)
            if marker is not None:
                result["marker_path"] = str(marker)
        return result
    state = pool_session.state
    # Provider name is the canonical ``state.type`` discriminator —
    # ``"unix_local"`` for the unix subprocess provider; no legacy
    # ``"local"`` alias.
    provider = getattr(state, "type", "unknown")
    sandbox_ref = getattr(state, "sandbox_ref", None)
    result: dict = {
        "session_id": session_id,
        "provider": provider,
        "sandbox_ref": sandbox_ref,
        "status": "running" if sandbox_ref else "missing",
        "root": (state.recipe.root if state.recipe else None) or "/tmp",
    }
    url = pool_session.supervisor_url
    if url:
        result["url"] = url
    if provider == "unix_local" and sandbox_ref:
        from .providers.unix_local import _load_record
        marker, _rec = await asyncio.to_thread(_load_record, sandbox_ref)
        if marker is not None:
            result["marker_path"] = str(marker)
    return result


# ---------------------------------------------------------------------------
# Session log read endpoints
# ---------------------------------------------------------------------------


@app.get("/sessions/{session_id}/log")
async def get_session_log_route(session_id: str, limit: int = Query(default=500)):
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


def _extract_workspace(data: dict, provider: str) -> str | None:
    """Read + normalize ``workspace`` from a session-create body.

    Returns the canonical lowercased name, or ``None`` if unset/blank.
    Raises ``HTTPException(400)`` on invalid name shape and
    ``HTTPException(400)`` on daytona — daytona's S3-FUSE mounts can't
    coordinate concurrent writes across two sandboxes (same constraint
    that drives ``_reject_daytona_sibling_when_active``), and shared
    workspaces hit that exact race even when the two sandboxes belong
    to *different* agents. Lift the rejection once a multi-supervisor
    architecture lands.
    """
    raw = data.get("workspace")
    if raw is None or raw == "":
        return None
    if provider == "daytona":
        raise HTTPException(
            400,
            "shared workspace is not supported on the daytona provider yet "
            "(S3-FUSE caches don't coordinate cross-sandbox writes); use "
            "docker, unix_local, or modal",
        )
    try:
        return _normalize_workspace(raw)
    except ValueError as e:
        raise HTTPException(400, str(e))


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


async def _persist_user_message(
    session,
    message: str,
    rpc_id: str,
    attachments: list[dict] | None = None,
) -> None:
    """Write the EVT_USER_MESSAGE row for a freshly-submitted prompt.

    Best-effort — a DB hiccup must not block the prompt from being sent
    to the supervisor. The matching turn-end / tool / text rows are
    written by ``_persist_prompt_events`` as ``execute_prompt`` yields.

    Routes through the per-process ``SessionLogBatcher`` when available
    (the production path under lifespan); falls back to a direct INSERT
    in test contexts that bypass ``start_batcher`` so unit tests keep
    seeing user_message rows synchronously.

    ``attachments`` is an opaque list of dicts the caller wants to
    persist alongside the prompt text — used by hivespace to round-trip
    file metadata (id, url, sandbox_path, filename, …) so the chat UI
    can re-render images / file chips on cold-load without consulting a
    parallel DB. Treated as opaque here; no schema enforcement.
    """
    payload: dict = {"text": redact_secrets(message), "prompt_id": rpc_id}
    if attachments:
        payload["attachments"] = list(attachments)
    try:
        batcher = get_batcher()
        if batcher is not None:
            await batcher.add(
                session_id=session.session_id,
                agent_id=session._agent_id or "",
                event_type=EVT_USER_MESSAGE,
                payload=payload,
            )
        else:
            await log_event(
                session_id=session.session_id,
                agent_id=session._agent_id or "",
                event_type=EVT_USER_MESSAGE,
                payload=payload,
            )
    except Exception:
        log.exception("user_message log_event failed for session %s rpc=%s",
                      session.session_id, rpc_id)


# execute_prompt yields events whose ``type`` matches what
# ``api.sse.parse_acp_event`` emits — same taxonomy as the SDK
# ``astream`` and the /events SSE consumers. Any type missing from
# this map is logged as-is (forward-compat with new ACP update kinds).
_EVENT_TYPE_TO_LOG = {
    "text": EVT_ASSISTANT_MESSAGE,
    "reasoning": EVT_REASONING,
    "tool": EVT_TOOL_CALL,
    "tool_result": EVT_TOOL_RESULT,
    "usage": EVT_USAGE,
    "error": EVT_ERROR,
    "done": "turn_end",
}


async def _persist_prompt_events(
    session,
    message: str,
    rpc_id: str,
    attachments: list[dict] | None = None,
) -> None:
    """Drive ``execute_prompt`` and write coalesced rows to ``session_log``.

    Consecutive ``text`` and ``reasoning`` chunks are buffered and written
    as ONE row per logical block (flush on type-change, tool call,
    usage, error, done, or end-of-stream). Discrete events (tool, tool
    result, usage, error, done) pass through as-is. This matches what
    SSE consumers see after canonicalization and makes ``/sessions/{id}/log``
    semantically equivalent to the SSE stream — neither per-chunk noise
    nor "one fat blob per turn."

    Each row carries the rpc_id so the log can be sliced by turn. Single
    write failures are non-fatal — log and keep draining so a transient
    DB hiccup doesn't drop the rest of the turn.
    """
    agent_id = session._agent_id or ""

    text_buf: list[str] = []
    think_buf: list[str] = []

    async def _write(event: dict) -> None:
        etype = event.get("type", "event")
        # Flatten ``raw`` (the original ACP update payload) into the row
        # so the dashboard's permissive renderer finds tool/result/usage
        # fields without needing the nested ``raw`` indirection.
        payload = {k: v for k, v in event.items() if k != "type"}
        if isinstance(payload.get("raw"), dict):
            payload.update(payload.pop("raw"))
        if "text" in payload:
            payload["text"] = redact_secrets(payload["text"])
        payload["prompt_id"] = rpc_id
        try:
            batcher = get_batcher()
            if batcher is not None:
                await batcher.add(
                    session_id=session.session_id,
                    agent_id=agent_id,
                    event_type=_EVENT_TYPE_TO_LOG.get(etype, etype),
                    payload=payload,
                )
            else:
                await log_event(
                    session_id=session.session_id,
                    agent_id=agent_id,
                    event_type=_EVENT_TYPE_TO_LOG.get(etype, etype),
                    payload=payload,
                )
        except Exception:
            log.exception("log_event(%s) failed for session %s rpc=%s",
                          etype, session.session_id, rpc_id)

    async def _flush_buffers() -> None:
        if text_buf:
            await _write({"type": "text", "text": "".join(text_buf)})
            text_buf.clear()
        if think_buf:
            await _write({"type": "reasoning", "text": "".join(think_buf)})
            think_buf.clear()

    # Per-session prompt serialisation: only one execute_prompt drives
    # the supervisor at a time. Without this, two concurrent POST
    # /message calls produce two parallel persist tasks racing on
    # ``session_log`` writes and the row order diverges from SSE
    # arrival order (the
    # ``test_interrupt_mid_tool_parity ['cancelled','end_turn'] vs
    # ['end_turn','cancelled']`` flake under -n auto). FIFO is
    # preserved across queued prompts; ``interrupt=True`` cancels the
    # active turn so the lock releases promptly without reordering.
    #
    # All cleanup paths (final flush on success, error-row write, hard-
    # cancel buffer flush) MUST run while the lock is held — otherwise
    # the next prompt's persist task can interleave its writes with
    # this prompt's tail and the log row order de-syncs from SSE.
    async def _drive_one(active_session) -> tuple[bool, Exception | None]:
        """Drive execute_prompt on a specific session; return
        (terminal_seen, last_exception). ``terminal_seen=True`` means we
        consumed a ``done`` or ``error`` event — the rpc is complete and
        no retry is appropriate. Otherwise ``False`` + exception means
        the supervisor died mid-flight and the caller should retry on
        the pool's current session."""
        terminal = False
        try:
            async for event in active_session.execute_prompt(message, rpc_id=rpc_id):
                if not isinstance(event, dict):
                    continue
                t = event.get("type")
                if t == "text":
                    if think_buf:
                        await _flush_buffers()
                    text_buf.append(event.get("text", ""))
                elif t == "reasoning":
                    if text_buf:
                        await _flush_buffers()
                    think_buf.append(event.get("text", ""))
                elif t == "usage":
                    await _write(event)
                else:
                    await _flush_buffers()
                    await _write(event)
                    if t in ("done", "error"):
                        terminal = True
            await _flush_buffers()
            return True, None
        except Exception as e:
            return terminal, e

    async with session._prompt_lock:
        # Log the user_message INSIDE the lock so log row order tracks
        # actual execution order.
        await _persist_user_message(session, message, rpc_id, attachments)
        # Mark the prompt in flight so the idle reaper never hibernates this
        # session mid-turn — covers a long, chunk-silent tool call whose
        # compute clock would otherwise go stale. Balanced across the
        # recovery swap below and released in the finally.
        session.liveness.observe_prompt_start()
        try:
            ok, exc = await _drive_one(session)
            # Supervisor died mid-prompt? If the pool already cold-recovered
            # the session (a sibling request observed alive=False and swapped
            # in a new SandboxSession), retry once on the fresh session —
            # this is the race that lost ``rpc=41095a61`` events on modal
            # ``test_message_immediately_after_stop``: the error broadcast
            # would otherwise land on a dict that was cleared during the
            # migration, and the SDK would time out waiting for an event
            # that never arrives.
            if not ok and exc is not None:
                try:
                    from api.sandbox import get_pool as _gp
                    replacement = await _gp().get_session(session.session_id)
                except Exception:
                    replacement = None
                if replacement is not None and replacement is not session:
                    log.info(
                        "execute_prompt retry: session %s recovered rpc=%s",
                        session.session_id, rpc_id,
                    )
                    text_buf.clear(); think_buf.clear()
                    # Move the in-flight marker onto the session the pool now
                    # owns so each session's counter balances independently
                    # (old: +1 at top then -1 here; new: +1 here then -1 in
                    # the finally).
                    session.liveness.observe_prompt_end()
                    session = replacement  # downstream writes use the new one
                    session.liveness.observe_prompt_start()
                    ok, exc = await _drive_one(replacement)
            if not ok:
                e = exc if exc is not None else RuntimeError(
                    "stream ended without terminal event"
                )
                log.exception(
                    "execute_prompt failed for session %s rpc=%s: %s",
                    session.session_id, rpc_id, e,
                )
                await _flush_buffers()
                await _write({
                    "type": "error",
                    "message": str(e)[:500], "kind": type(e).__name__,
                })
                # Broadcast to whichever session the pool currently has —
                # NOT necessarily the one we started with. The old session's
                # ``_subscribers`` dict may have been migrated to the new
                # session by ``pool.get_session``'s subscriber hand-off;
                # broadcasting to the stale ref reaches an empty dict.
                from api.sandbox import get_pool as _gp2
                current = _gp2()._active.get(session.session_id, session)  # noqa: SLF001
                current._broadcast({
                    "type": "error", "rpc_id": rpc_id,
                    "jsonrpc": "2.0", "id": rpc_id,
                    "error": {
                        "code": -32603,
                        "message": str(e),
                        "data": {
                            "kind": type(e).__name__,
                            "exception_type": type(e).__name__,
                        },
                    },
                })
        finally:
            # Release the in-flight marker on whichever session is current
            # (the original, or the replacement after a recovery swap) so
            # the reaper can hibernate it once it goes idle. The counter is
            # floored at 0, so this is safe even on the unbalanced error
            # paths.
            session.liveness.observe_prompt_end()
            # Hard-cancel path: ``CancelledError`` is a ``BaseException``
            # in Python 3.8+ and bypasses ``except Exception``. Without
            # this finally an asyncio Task cancellation (server shutdown,
            # session DELETE) drops the in-flight buffer. ``asyncio.shield``
            # keeps the flush running even if the surrounding task is in
            # a cancelling state.
            if text_buf or think_buf:
                try:
                    await asyncio.shield(_flush_buffers())
                except Exception:
                    log.exception(
                        "final flush failed for session %s rpc=%s — buffer lost",
                        session.session_id, rpc_id,
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
    # Opaque list of attachment metadata dicts. Persisted on the
    # ``user_message`` event so cold-loads can re-render the chat
    # without a parallel hivespace fetch.
    attachments = data.get("attachments")
    if attachments is not None and not isinstance(attachments, list):
        raise HTTPException(400, "attachments must be a list")

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
        async for _ in _execute_and_stream_sse_for(pool_session, message, rpc_id, attachments):
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
    attachments = data.get("attachments")
    if attachments is not None and not isinstance(attachments, list):
        raise HTTPException(400, "attachments must be a list")

    rpc_id = str(uuid.uuid4())

    from api.sandbox import get_pool
    session = await get_pool().get_session(session_id)

    return StreamingResponse(
        _execute_and_stream_sse_for(session, message, rpc_id, attachments),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )


async def _execute_and_stream_sse_for(
    session,
    message: str,
    rpc_id: str,
    attachments: list[dict] | None = None,
):
    """Stream branch with an ALREADY-RESOLVED session.

    Used by ``POST /message+stream`` so session resolution (cold-recover
    on the receiving replica) happens in the route handler — surfaces
    failures before the StreamingResponse goes on the wire.
    """
    from api.sandbox.session import _HEARTBEAT

    # ``_persist_user_message`` was previously called HERE, but that
    # races concurrent queued prompts: three POSTs land three
    # user_message rows before any turn_end. ``_persist_prompt_events``
    # now writes user_message inside its prompt_lock, so log row
    # order matches actual execution order.

    # Eager registration so drive_task can start immediately — the
    # generator-form ``subscribe()`` defers queue registration to the
    # first iteration, which means a producer started before iterating
    # would broadcast into a queue that hasn't been registered yet
    # AND the consumer would block up to _HEARTBEAT_INTERVAL_S (20s)
    # waiting for the empty queue to surface a sentinel before drive
    # ever runs. The two-step split eliminates that 20s phantom delay.
    sid, q = session.register_subscriber()

    # Cluster-visible busy flag — ``busy_at`` on the sessions row is
    # read by /admin/sessions with a 60s TTL so a crashed replica
    # can't leave it stuck (lease takeover also resets it).
    from api.sandbox import get_pool as _get_pool
    from api import db as _db
    try:
        await _db.set_session_busy(session.session_id, busy=True)
    except Exception:
        log.warning("set_session_busy(True) failed for %s", session.session_id)

    async def _drive():
        await _persist_prompt_events(session, message, rpc_id, attachments)

    drive_task = asyncio.create_task(_drive())
    # Wrap the full turn so we get one log line per prompt with the
    # actual wall-clock duration (the request middleware only sees
    # time-to-headers for StreamingResponse). Slow turns surface as
    # WARNING in the log without per-frame instrumentation.
    _turn_t0 = time.perf_counter()
    try:
        async for item in session.iterate_subscriber(sid, q):
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
                # Terminal:
                #   * ``"stopReason"`` — JSON-RPC ``result`` envelope
                #     emitted by ACP for a clean turn-end (end_turn /
                #     cancelled / max_tokens / max_turn_requests). The
                #     existing snake_case ``"stop_reason"`` substring
                #     was a long-standing bug — ACP wires camelCase, so
                #     the check never fired on real frames; success-
                #     termination depended on client disconnect.
                #   * ``"error":`` — top-level JSON-RPC error envelope
                #     emitted by ACP for a fatal turn-end (auth failure
                #     / internal error / process death). Verified end-
                #     to-end with claude-agent-acp 0.31.4.
                # Tool-call failures arrive as ``method=session/update``
                # notifications and never produce a top-level ``error``
                # field; ``-32601`` handshake errors are filtered by
                # ``parse_acp_payload`` before broadcast (see
                # ``api/sse.py:86``) so they don't reach this check.
                if (
                    "stopReason" in block
                    or '"type":"done"' in block
                    or '"error":' in block
                ):
                    return
            elif isinstance(item, dict):
                if item.get("rpc_id") != rpc_id:
                    continue
                # Carry the rpc tag so per-rpc consumers (UI, tests'
                # _PersistentSse) can dispatch error broadcasts the same
                # way they dispatch ACP frames. Without this the error
                # is yielded as untagged ``data:`` and silently dropped
                # by tag-filtering consumers — the production Task
                # Builder silent-failure repro.
                yield f"event: rpc:{rpc_id}\ndata: {json.dumps(item)}\n\n"
                if item.get("type") == "error":
                    return
    finally:
        # The generator returns the moment the ``done`` block reaches
        # us — but the persister (driven by execute_prompt's yield) is
        # one async hop behind, still awaiting log_event(turn_end).
        # Await it (bounded) so the turn_end row lands before we
        # close. Never cancel: a mid-write cancel leaves the DB
        # connection in BAD state and the pool has to discard it.
        if drive_task is not None and not drive_task.done():
            try:
                await asyncio.wait_for(asyncio.shield(drive_task), timeout=10)
            except (asyncio.TimeoutError, Exception):
                pass
        try:
            await _db.set_session_busy(session.session_id, busy=False)
        except Exception:
            log.warning("set_session_busy(False) failed for %s", session.session_id)
        _turn_ms = (time.perf_counter() - _turn_t0) * 1000
        # Direct log (not timed_phase) so the rpc_id is in-line for
        # cross-correlation with /events subscribers and DB session_log
        # rows. Turns are inherently long (5-30s typical), so the
        # warning threshold is its own knob — AGENT_SDK_SLOW_TURN_MS,
        # default 60s. Everything else is INFO.
        from .identity import replica_id as _rid
        _slow_turn = float(os.environ.get("AGENT_SDK_SLOW_TURN_MS", "60000"))
        _lvl = logging.WARNING if _turn_ms >= _slow_turn else logging.INFO
        log.log(
            _lvl, "[%s] turn done session=%s rpc=%s %.0fms",
            _rid(), session.session_id[:8], rpc_id[:8], _turn_ms,
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


async def _resolve_supervisor_url(session_id: str) -> str:
    """Resolve a session_id to its supervisor URL via the SessionPool.
    ``pool.get_session()`` brings the compute up if needed;
    ``supervisor_url`` is set as part of ``SandboxSession.start()``."""
    from api.sandbox import get_pool
    return (await get_pool().get_session(session_id)).supervisor_url or ""


async def _proxy_from_session(
    session_id: str, method: str, path: str, *,
    params: dict | None = None, json: dict | None = None,
    timeout: int = 30,
) -> Response:
    """Forward a request to the session's supervisor (resolved through
    the SessionPool) and return its JSON response. Used by every
    session-scoped file proxy. Uses the module-shared ``_HTTP_CLIENT`` so
    repeat calls reuse the keep-alive connection to that supervisor."""
    url = await _resolve_supervisor_url(session_id)
    if _HTTP_CLIENT is None:
        raise HTTPException(503, "server not yet initialised")
    try:
        r = await _HTTP_CLIENT.request(
            method, f"{url}{path}", params=params, json=json, timeout=timeout,
        )
        return Response(
            content=r.content,
            status_code=r.status_code,
            media_type="application/json",
        )
    except httpx.HTTPError as e:
        raise HTTPException(status_code=502, detail=f"supervisor unreachable: {e}")


async def _download_from_session(session_id: str, path: str) -> Response:
    """Stream a download from the session's supervisor."""
    url = await _resolve_supervisor_url(session_id)
    if _HTTP_CLIENT is None:
        raise HTTPException(503, "server not yet initialised")
    try:
        r = await _HTTP_CLIENT.get(
            f"{url}/v1/files/download", params={"path": path}, timeout=60,
        )
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
    return await _download_from_session(session_id, path)


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
