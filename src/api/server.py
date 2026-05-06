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
import traceback
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import quote

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
    close_pool,
    count_sessions_by_volume,
    delete_agent,
    delete_session,
    delete_sessions_by_volume,
    delete_volume,
    get_agent,
    get_db,
    get_session,
    get_session_log,
    get_volume,
    get_volume_by_name,
    init_db,
    init_pool,
    list_agents,
    list_volumes,
    log_event,
    read_sandbox_state,
    update_session_env,
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
from .providers import (
    PORT_BASED_PROVIDERS,
    ProviderInstance,
    VolumeFileExistsError,
    default_cwd_for_provider,
    get_volume_adapter,
    _normalize_workspace,
)
from .providers._shared import _safe_path as _shared_safe_path
from .redact import redact_pre_start_commands, redact_secrets

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
    init_db()
    await init_pool()

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

    await asyncio.gather(*[_safe_reconcile(p) for p in ("docker", "daytona", "unix_local", "modal")])

    # SessionPool owns idle eviction now (per
    # ).
    from api.sandbox import start_reaper, shutdown_pool
    await start_reaper()

    yield
    try:
        await shutdown_pool()
    except Exception as e:
        log.warning("shutdown_pool failed: %s", e)
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
# Skills provisioning (skills CLI)
# ---------------------------------------------------------------------------


def _normalize_skills(skills) -> list[str]:
    """Normalize skills config into a list of source strings for ``skills add``.

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
    """Return shell commands to install skills via the baked ``skills`` CLI.

    A source like ``owner/repo@skill-name`` is a single-skill filter. Pass
    ``--all`` only when no ``@<skill>`` suffix is given, so the filter is
    respected — otherwise ``--all`` overrides it and pulls every skill
    from the repo (e.g. ``github/awesome-copilot`` ships hundreds).
    """
    sources = _normalize_skills(skills)
    cmds: list[str] = []
    for source in sources:
        flags = "-g" if "@" in source else "--all -g"
        cmds.append(f"skills add {shlex.quote(source)} {flags}")
    return cmds


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
    For the ``unix_local`` provider we install skills on the host and return
    ``None`` — the unix_local sandbox shares HOME with the server, so skill
    install runs once on the host and user commands there would execute
    with server privileges (deliberately unsupported).
    """
    skill_cmds = _skills_install_commands(config.skills) if config.skills else []
    if provider == "unix_local":
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
    "mcp_servers",
    "skills",
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


# Sandbox CRUD routes removed: the standalone ``sandboxes`` table is gone;
# session-scoped routes (``GET /sessions/{id}/sandbox``,
# ``DELETE /sessions/{id}``) replace them. Reverse lookups by sandbox_ref
# go through ``SessionPool.find_by_sandbox_ref``.


# ---------------------------------------------------------------------------
# Admin
# ---------------------------------------------------------------------------


@app.get("/admin/sessions")
async def admin_list_sessions():
    """List in-memory pool sessions for the dashboard + cleanup debugging.

    The legacy response shape is preserved so ``ui/dashboard.html``
    doesn't have to change: ``sessions[].agent_busy`` /
    ``active_rpc_id`` / etc. are constants — the pool's per-prompt SSE
    replaced the persistent reader's busy-flag bookkeeping.
    """
    from api.sandbox import get_pool
    pool = get_pool()
    return {
        "sessions": [
            {
                "session_id": sid,
                "agent_id": sess._agent_id,
                "sandbox_ref": getattr(sess.state, "sandbox_ref", None),
                "inner_session_id": sess.inner_session_id,
                # "active subscriber" is the closest pool-level proxy for
                # the dashboard's "running" badge — there's no per-prompt
                # busy flag in the pool (per-prompt SSE replaces it).
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
                "sandbox_ref": getattr(sess.state, "sandbox_ref", None),
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




@app.get("/sessions")
async def list_sessions_route():
    """List sessions currently leased by the SessionPool. Hibernated +
    cold sessions don't appear here — query the DB / GET /sessions/{id}
    directly for those."""
    from api.sandbox import get_pool
    now = time.time()
    out = []
    for s in get_pool()._active.values():  # noqa: SLF001
        last = s.liveness._last_chunk_at  # noqa: SLF001
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
    when the session isn't live in the pool.

    Several response keys (``agent_busy`` / ``active_rpc_id`` /
    ``pending_count`` / ``rpc_subscriber_count`` / ``available_commands``)
    are constants — the pool has no equivalent bookkeeping after
    per-prompt SSE replaced the persistent reader. Kept for response-
    shape back-compat with the dashboard."""
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
            "active_rpc_id": None,
            "pending_count": 0,
            "session_subscriber_count": 0,
            "rpc_subscriber_count": 0,
            "last_activity": None,
            "idle_seconds": None,
            "has_client": False,
            "shutdown_requested": False,
            "available_commands": [],
            "supervisor_url": None,
            "supervisor_port": sb_state.get("listen_port") if isinstance(sb_state, dict) else None,
        }
    state = pool_session.state
    last_chunk = pool_session.liveness._last_chunk_at
    return {
        "session_id": session_id,
        "agent_id": pool_session._agent_id,
        "sandbox_ref": getattr(state, "sandbox_ref", None),
        "inner_session_id": pool_session.inner_session_id,
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
        # ``_SPAWN_ARGS`` was removed in the on-disk-marker refactor (PR #57);
        # ``_load_record`` now resolves the marker path on the fly by globbing
        # ``<vol_root>/*/system/sandboxes/<ref>.json``.
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

    agent_id = data.get("agent_id")
    if agent_id:
        await _require_agent(agent_id)
        _reject_daytona_sibling_when_active(agent_id, default_provider)
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
    # use the same path (the JSONL hash key). For unix_local, this is the
    # per-agent volume subpath (or the workspace subpath when set); for
    # docker/daytona, a fixed mount point.
    if default_provider == "unix_local":
        home_subpath = f"workspaces/{workspace}" if workspace else f"agents/{agent_id}"
        default_cwd = str(Path(volume_record.provider_ref) / home_subpath)
    else:
        default_cwd = default_cwd_for_provider(default_provider)
    cwd = data.get("cwd", config_data.pop("cwd", default_cwd))

    session_id = str(uuid.uuid4())
    lazy_user_pre_start = data.get("pre_start_commands") or []
    await upsert_session(
        session_id, agent_id, inner_session_id=None,
        volume_id=volume_record.id,
        env=body_env or {}, secrets=body_secrets or {},
        cwd=cwd,
        pre_start_commands=list(lazy_user_pre_start),
        workspace=workspace,
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
    from api.sandbox.state import Resources, validate_resources_for_provider

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
    agent_type = data.get("agent_type", "claude")
    config_data = data.get("config", {})
    _merge_top_level_config(data, config_data)

    cwd = data.get("cwd", config_data.pop("cwd", None))
    root = data.get("root", config_data.pop("root", None))
    dockerfile = _materialize_dockerfile({**config_data, **data})
    shared_mounts = data.get("shared_mounts") or config_data.pop("shared_mounts", None) or []
    config_data.pop("dockerfile_content", None)
    config_data.pop("dockerfile", None)
    config_data.pop("workspace", None)
    resources_data = data.get("resources") or config_data.pop("resources", None)
    try:
        resources = Resources(**resources_data) if resources_data else None
        validate_resources_for_provider(provider, resources)
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
    )

    session_id = str(uuid.uuid4())
    await upsert_session(
        session_id, agent_id, inner_session_id=None,
        volume_id=volume_record.id,
        env=body_env or {}, secrets=body_secrets or {},
        cwd=cwd,
        # Mirror the recipe so the column matches what's persisted on the
        # session row's sandbox_state JSONB. Pool reads from JSONB; this
        # column is consumed by /sessions/{id} (GET) introspection.
        pre_start_commands=merged_pre_start,
        workspace=workspace,
    )
    pool = get_pool()
    try:
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


async def _persist_user_message(session, message: str, rpc_id: str) -> None:
    """Write the EVT_USER_MESSAGE row for a freshly-submitted prompt.

    Best-effort — a DB hiccup must not block the prompt from being sent
    to the supervisor. The matching turn-end / tool / text rows are
    written by ``_persist_prompt_events`` as ``execute_prompt`` yields.
    """
    try:
        await log_event(
            session_id=session.session_id,
            agent_id=session._agent_id or "",
            event_type=EVT_USER_MESSAGE,
            payload={"text": redact_secrets(message), "prompt_id": rpc_id},
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


async def _persist_prompt_events(session, message: str, rpc_id: str) -> None:
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
    async with session._prompt_lock:
        # Log the user_message INSIDE the lock so log row order tracks
        # actual execution order. Writing it outside (as the SSE caller
        # does, before spawning ``_drive``) lets queued prompts produce
        # interleaved ``user_message_A, user_message_B, user_message_C,
        # turn_end_A, ...`` — the test ``test_queued_prompts_parity``
        # asserts user_message[i] < turn_end[i] which that ordering
        # violates.
        await _persist_user_message(session, message, rpc_id)
        try:
            async for event in session.execute_prompt(message, rpc_id=rpc_id):
                if not isinstance(event, dict):
                    continue
                t = event.get("type")
                # Coalesce consecutive text/reasoning chunks; flush on
                # type-change so order and adjacency are preserved.
                if t == "text":
                    if think_buf:
                        await _flush_buffers()
                    text_buf.append(event.get("text", ""))
                elif t == "reasoning":
                    if text_buf:
                        await _flush_buffers()
                    think_buf.append(event.get("text", ""))
                elif t == "usage":
                    # Usage updates can fire mid-stream and don't break
                    # the surrounding text/reasoning block — write usage
                    # as a discrete row without flushing buffers, matching
                    # the SSE canonicalization in ``_sse_to_canonical``.
                    await _write(event)
                else:
                    # Tool/tool_result/done/error terminate the current
                    # text/think block before writing themselves, so
                    # order is stable across consumers.
                    await _flush_buffers()
                    await _write(event)
            # Stream ended without a terminal event (rare — usually
            # ``done`` closes it); flush anything still buffered.
            await _flush_buffers()
        except Exception as e:
            log.exception("execute_prompt failed for session %s rpc=%s",
                          session.session_id, rpc_id)
            await _flush_buffers()
            await _write({
                "type": "error",
                "message": str(e)[:500], "kind": type(e).__name__,
            })
            # Broadcast as a JSON-RPC error envelope so consumers that
            # parse SSE blocks via ``parse_acp_event`` (UI, the test
            # ``_PersistentSse`` reader, the SDK ``astream`` adapter)
            # recognise it as an ``error`` frame and surface it. The
            # ``rpc_id`` / ``type`` keys remain for the older dict-shape
            # consumers in ``_execute_and_stream_sse`` that filter by
            # rpc_id before yielding to SSE.
            session._broadcast({
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
    """
    data = await _json_body(request)
    message = data.get("message")
    if not message:
        raise HTTPException(400, "message required")

    rpc_id = str(uuid.uuid4())

    if data.get("interrupt"):
        # Best-effort: cancel the running ACP turn so this prompt
        # supersedes it. The cancelled turn's ``done`` event arrives via
        # the existing SSE stream with ``stop_reason="cancelled"`` and
        # is logged like any other turn_end.
        from api.sandbox import get_pool
        try:
            pool_session = await get_pool().get_session(session_id)
            await pool_session.cancel_active_prompt()
        except Exception:
            log.exception("interrupt cancel failed for session %s", session_id)

    async def _drain() -> None:
        async for _ in _execute_and_stream_sse(session_id, message, rpc_id):
            pass

    task = asyncio.create_task(_drain())
    _BG_TASKS.add(task)
    task.add_done_callback(_BG_TASKS.discard)
    return {"rpc_id": rpc_id, "status": "ok"}


async def _log_session_acquire_error(session_id: str, rpc_id: str,
                                     err: Exception) -> None:
    """Best-effort error log when pool.get_session fails before we have
    a session object to broadcast through. Writes the error to
    session_log so /sessions/{id}/log readers see it.
    """
    try:
        await log_event(
            session_id=session_id, agent_id="",
            event_type=EVT_ERROR,
            payload={
                "prompt_id": rpc_id,
                "kind": type(err).__name__,
                "message": str(err)[:500],
                "phase": "pool.get_session",
            },
        )
    except Exception:
        log.exception("failed to log session-acquire error for %s rpc=%s",
                      session_id, rpc_id)


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
    """
    data = await _json_body(request)
    message = data.get("message")
    if not message:
        raise HTTPException(400, "message required")

    rpc_id = str(uuid.uuid4())
    return StreamingResponse(
        _execute_and_stream_sse(session_id, message, rpc_id),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )


async def _execute_and_stream_sse(session_id: str, message: str, rpc_id: str):
    """Canonical execution path: cold-recover (if needed) → log
    user_message → subscribe + drive → emit per-rpc SSE blocks.

    Used as the response body of ``POST /message+stream`` and as the
    sole drain inside the background task fired by ``POST /message``.
    Single source of truth for "execute one prompt and persist its
    events" — both endpoints exercise identical persistence + broadcast
    behaviour.

    Yields SSE lines (``event:``/``data:``/``: heartbeat``) terminated
    by ``\\n\\n``. The first yield is an immediate heartbeat so a
    streaming client knows the request is alive while ``pool.get_session``
    cold-recovers (30-60s on Daytona under contended control plane).
    """
    from api.sandbox import get_pool
    from api.sandbox.session import _HEARTBEAT

    yield ": heartbeat\n\n"

    try:
        session = await get_pool().get_session(session_id)
    except Exception as e:
        log.exception("pool.get_session(%s) failed for rpc=%s",
                      session_id, rpc_id)
        await _log_session_acquire_error(session_id, rpc_id, e)
        err = {"type": "error", "rpc_id": rpc_id,
               "error": {"message": str(e)[:500],
                         "exception_type": type(e).__name__}}
        yield f"data: {json.dumps(err)}\n\n"
        return

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

    async def _drive():
        await _persist_prompt_events(session, message, rpc_id)

    drive_task = asyncio.create_task(_drive())
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


@app.post("/sessions/{session_id}/cancel")
async def session_cancel(session_id: str):
    """Cancel the in-flight prompt on this session, if any.

    Best-effort: sends ``session/cancel`` (JSON-RPC notification) to
    the supervisor's ACP child via the SessionPool. The ACP child
    aborts the turn; the ``done`` event arrives on the same SSE
    subscribers that ``POST /message`` opened. No active lease →
    returns ``{"status": "ok", "detail": "no active lease"}``.
    """
    from api.sandbox import get_pool

    pool = get_pool()
    pool_session = pool._active.get(session_id)  # noqa: SLF001 — read-only peek
    if pool_session is None:
        return {"status": "ok", "detail": "no active lease"}
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
