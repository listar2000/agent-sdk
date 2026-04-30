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
    EVT_ASSISTANT_MESSAGE,
    EVT_ERROR,
    EVT_REASONING,
    EVT_TOOL_CALL,
    EVT_TOOL_RESULT,
    EVT_USAGE,
    EVT_USER_MESSAGE,
    AgentConfig,
    AgentRecord,
    SandboxRecord,
    VolumeRecord,
)
from . import providers as _providers_mod
from .providers import (
    PORT_BASED_PROVIDERS,
    ProviderInstance,
    VolumeFileExistsError,
    default_cwd_for_provider,
)
from .providers._shared import _safe_path as _shared_safe_path
from .redact import redact_secrets

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

_sandbox_locks: dict[str, asyncio.Lock] = {}


def _get_sandbox_lock(sandbox_id: str) -> asyncio.Lock:
    return _sandbox_locks.setdefault(sandbox_id, asyncio.Lock())


# Strong references to fire-and-forget background tasks (the per-prompt
# persisters spawned by POST /message). The event loop only holds weak
# refs to tasks, so a caller that does ``asyncio.create_task(coro())``
# without keeping the returned Task alive risks silent cancellation.
# Tasks self-discard from the set on completion.
_BG_TASKS: set[asyncio.Task] = set()






@asynccontextmanager
async def lifespan(app):
    _configure_logging()
    init_db()
    await init_pool()

    # Startup reconciliation: kill orphan containers labeled with a
    # sandbox_id whose DB row is gone or marked deleted. Per-provider in
    # parallel so a slow provider doesn't serialise boot. In practice
    # only Docker does real work; daytona/local/modal are no-ops today.
    async def _safe_reconcile(prov: str) -> None:
        try:
            await _providers_mod.reconcile_sandboxes(prov)
        except Exception as e:
            log.warning("startup reconcile for %s failed: %s", prov, e)

    await asyncio.gather(*[_safe_reconcile(p) for p in ("docker", "daytona", "local", "modal")])

    # SessionPool owns idle eviction now (per
    # docs/ephemeral-sandbox-design.md §6).
    from api.sandbox import start_reaper, shutdown_pool
    await start_reaper()

    yield
    try:
        await shutdown_pool()
    except Exception as e:
        log.warning("shutdown_pool failed: %s", e)
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
            # Overwrite mode — single provider call.
            await _providers_mod.volume_write(
                vol.provider, vol.provider_ref, rel, body.content.encode(),
            )
            return
        # Search/replace at the volume layer (no sandbox required):
        # read → str.replace → write. Same semantics as the
        # supervisor's session-scoped /files/edit, but driven directly
        # against the provider's volume primitives so callers don't
        # need a live sandbox to edit files on the volume.
        existing = (
            await _providers_mod.volume_read(vol.provider, vol.provider_ref, rel)
        ).decode("utf-8", errors="replace")
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
        await _providers_mod.volume_write(
            vol.provider, vol.provider_ref, rel, updated.encode(),
        )
    except HTTPException:
        raise
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
    # Local provider exposes the alive-marker path so an external "delete"
    # simulation can remove just the marker without disturbing home —
    # preserves the volume-persistence test invariants. PID isn't surfaced
    # anymore (was tied to the legacy ``_INSTANCES`` cache); test helpers
    # discover the supervisor by its listening port instead.
    if record.provider == "local":
        from .providers.local import _SPAWN_ARGS as _LOCAL_SPAWN_ARGS
        args = _LOCAL_SPAWN_ARGS.get(record.sandbox_ref)
        if args and args.get("marker_path"):
            result["marker_path"] = args["marker_path"]
    return result


@app.delete("/sandboxes/{sandbox_id}")
async def delete_sandbox_route(sandbox_id: str):
    record = await _require_sandbox(sandbox_id)

    # Every sandbox is now pool-owned (the legacy POST /sandboxes route
    # that created stand-alone sandboxes is gone). Release through the
    # pool so SandboxSession.stop() snapshots + tears down the compute,
    # then drop the sandbox row. Match the pool by ``sandbox_ref`` (the
    # provider UUID stored in ``state.sandbox_id``), not by the DB row
    # PK we got from the URL.
    try:
        from api.sandbox import get_pool
        pool = get_pool()
        pool_session = pool.find_by_sandbox_id(record.sandbox_ref)
        if pool_session is not None:
            await pool.release(pool_session.session_id)
    except Exception as e:
        log.warning("DELETE /sandboxes %s: pool.release failed: %s", sandbox_id, e)

    async with _get_sandbox_lock(sandbox_id):
        _sandbox_locks.pop(sandbox_id, None)
        await delete_sandbox(sandbox_id)

    return {"status": "deleted"}


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
                "current_sandbox_id": getattr(sess.state, "sandbox_id", None),
                "sandbox_ref": getattr(sess.state, "sandbox_id", None),
                "inner_session_id": sess._inner_session_id,
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
            "current_sandbox_id": getattr(s.state, "sandbox_id", None),
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
    """Session runtime status including last activity. Routes through
    SessionPool — brings the SandboxSession up if it's been reaped.

    Several response keys (``agent_busy`` / ``active_rpc_id`` /
    ``pending_count`` / ``rpc_subscriber_count`` / ``available_commands``)
    are constants — the pool has no equivalent bookkeeping after
    per-prompt SSE replaced the persistent reader. Kept for response-
    shape back-compat with the dashboard."""
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


@app.get("/sessions/{session_id}/sandbox")
async def session_sandbox_info(session_id: str):
    """Sandbox metadata read straight from the SessionPool — no
    sandboxes-table dependency.

    Returns the same shape as ``GET /sandboxes/{id}`` (provider,
    sandbox_ref, status, root, url for port-based providers,
    marker_path for local) so test helpers and admin UIs that need
    sandbox info can stay in session-id space and avoid the
    sandbox-row-id round trip. Brings the SandboxSession up if it's
    been hibernated."""
    from api.sandbox import get_pool
    pool_session = await get_pool().get_session(session_id)
    state = pool_session.state
    provider = getattr(state, "type", "unknown")
    sandbox_ref = getattr(state, "sandbox_id", None)
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
    if provider == "local" and sandbox_ref:
        from .providers.local import _SPAWN_ARGS as _LOCAL_SPAWN_ARGS
        args = _LOCAL_SPAWN_ARGS.get(sandbox_ref)
        if args and args.get("marker_path"):
            result["marker_path"] = args["marker_path"]
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
    provisioning cost (daytona: ~15-30 s; local: ~2-3 s). The sandbox
    appears on the first ``POST /sessions/{id}/message`` (the pool
    cold-creates on demand).
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

    # Back-compat shim: GET /sandboxes/{id} + DELETE /sandboxes/{id}
    # (used by recovery test cleanup) still read the sandboxes table,
    # which the pool's sandbox_state JSONB doesn't populate. Mirror the
    # row here. Drop once those callers move to session-scoped routes.
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

    # Forward model/mode/thought_level so callers don't have to follow
    # POST /sessions with a separate POST /config. Read both top-level
    # and config_data because ``_merge_top_level_config`` already moved
    # ``model`` into config_data. Best-effort.
    await _forward_session_config(pool_session, data, config_data)

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
    pool session via ACP ``set_*``. Best-effort: a transient ACP failure
    logs and continues. Fields are looked up first in ``data`` (top-level
    body — what the SDK sends), then in ``config_data`` (nested body —
    what ``_merge_top_level_config`` may have promoted ``model`` into)."""
    cfg = config_data or {}
    for key, method in _SESSION_CONFIG_FIELDS:
        val = data.get(key)
        if val is None:
            val = cfg.get(key)
        if val is None:
            continue
        try:
            await getattr(pool_session, method)(val)
        except Exception as e:
            log.warning("forward %s(%r) to session %s failed: %s",
                        method, val, pool_session.session_id, e)


async def _resolve_log_sandbox_id(session) -> str | None:
    """Resolve the ``sandboxes.id`` row PK for this pool session, suitable
    for ``session_log.sandbox_id`` (FK → ``sandboxes(id)``).

    The pool tracks the *provider* sandbox ref in ``state.sandbox_id``
    (e.g. ``local-<uuid12>``), not the DB row PK (``sb_<hex>``). The
    session row's ``current_sandbox_id`` is the right value here, written
    by the back-compat shim in ``_sessions_create_eager`` and refreshed
    by the pool-managed reset path.

    Returns ``None`` if no sandbox row is currently linked — callers pass
    that through unchanged; ``session_log.sandbox_id`` is nullable.
    """
    sess = await get_session(session.session_id)
    if sess is None:
        return None
    return sess.get("current_sandbox_id")


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
            sandbox_id=await _resolve_log_sandbox_id(session),
            event_type=EVT_USER_MESSAGE,
            payload={"text": redact_secrets(message), "prompt_id": rpc_id},
        )
    except Exception:
        log.exception("user_message log_event failed for session %s rpc=%s",
                      session.session_id, rpc_id)


# execute_prompt yields {type: ...} dicts; map their type strings to the
# session_log EVT_ row schema the dashboard / SDK already understand. Any
# type missing from this map is logged as-is (forward-compat with new
# ACP update kinds).
_EVENT_TYPE_TO_LOG = {
    "text": EVT_ASSISTANT_MESSAGE,
    "agent_message_chunk": EVT_ASSISTANT_MESSAGE,
    "thought_chunk": EVT_REASONING,
    "tool_call": EVT_TOOL_CALL,
    "tool_call_update": EVT_TOOL_RESULT,
    "usage_update": EVT_USAGE,
    "usage_updated": EVT_USAGE,
    "error": EVT_ERROR,
    "done": "turn_end",
}


async def _persist_prompt_events(session, message: str, rpc_id: str) -> None:
    """Drive ``execute_prompt`` and write one ``session_log`` row per
    event it yields. Replaces the legacy SSE-reader log chain (deleted
    with the rest of that plumbing in this PR).

    Each row carries the rpc_id so ``/sessions/{id}/log`` can be sliced
    by turn. Failure of a single write is non-fatal — we log and keep
    draining so a transient DB hiccup doesn't drop the rest of the turn.
    """
    sandbox_id = await _resolve_log_sandbox_id(session)
    agent_id = session._agent_id or ""

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
                agent_id=agent_id, sandbox_id=sandbox_id,
                event_type=_EVENT_TYPE_TO_LOG.get(etype, etype),
                payload=payload,
            )
        except Exception:
            log.exception("log_event(%s) failed for session %s rpc=%s",
                          etype, session.session_id, rpc_id)

    try:
        async for event in session.execute_prompt(message, rpc_id=rpc_id):
            if isinstance(event, dict):
                await _write(event)
    except Exception as e:
        log.exception("execute_prompt failed for session %s rpc=%s",
                      session.session_id, rpc_id)
        await _write({
            "type": "error",
            "message": str(e)[:500], "kind": type(e).__name__,
        })
        session._broadcast({
            "type": "error", "rpc_id": rpc_id,
            "error": {"message": str(e), "exception_type": type(e).__name__},
        })


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
    await _persist_user_message(session, message, rpc_id)

    # Hold a strong reference so the task isn't GC'd mid-flight.
    task = asyncio.create_task(_persist_prompt_events(session, message, rpc_id))
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
    from api.sandbox import get_pool
    from api.sandbox.session import _HEARTBEAT

    data = await _json_body(request)
    message = data.get("message")
    if not message:
        raise HTTPException(400, "message required")

    pool = get_pool()
    session = await pool.get_session(session_id)
    rpc_id = str(uuid.uuid4())
    await _persist_user_message(session, message, rpc_id)

    async def _stream():
        # Subscribe BEFORE kicking off execute_prompt so broadcasts
        # from the supervisor's first chunks land in our queue. The
        # subscribe() generator registers the queue synchronously
        # before its first ``await q.get()``, so create_task'ing
        # _drive after entering the loop is race-free: drive only
        # runs once the event loop yields at our q.get().
        sub_iter = session.subscribe()

        # The persister both drives execute_prompt AND writes session_log
        # rows for each yielded event — same shared drain so /message and
        # /message+stream produce identical log timelines.
        async def _drive():
            await _persist_prompt_events(session, message, rpc_id)

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
            # _stream returns the moment the ``done`` block reaches the
            # subscriber queue — but the persister (driven by
            # execute_prompt's yield) is one async hop behind it, still
            # awaiting log_event(turn_end). Await it (bounded) so the
            # turn_end row lands before the response generator closes.
            # Never cancel: a mid-write cancel leaves the DB connection
            # in BAD state and the pool has to discard it. The timeout
            # is the only escape hatch.
            if drive_task is not None and not drive_task.done():
                try:
                    await asyncio.wait_for(asyncio.shield(drive_task), timeout=10)
                except (asyncio.TimeoutError, Exception):
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


@app.delete("/sessions/{session_id}", status_code=204)
async def delete_session_route(session_id: str):
    """Release the pool lease and delete the session row.

    Idempotent — missing session returns 204, not 404, so callers can
    use this as a "make sure this session is gone" primitive without
    branching on prior state. The underlying daytona/docker/local
    sandbox is *paused* (via ``pool.release``), not destroyed —
    matches ``DELETE /sandboxes/{id}`` semantics, and label-based
    cleanup scripts (``cleanup_daytona_orphans.py``) reclaim the
    compute later.
    """
    from api.sandbox import get_pool
    try:
        await get_pool().release(session_id)
    except Exception as e:
        log.warning("DELETE /sessions/%s: pool.release failed: %s",
                    session_id, e)
    # Drop the session row + any sandbox row currently linked to it via
    # the back-compat shim. ``ON DELETE CASCADE`` on session_log handles
    # that side; ``current_sandbox_id`` is FK with ``ON DELETE SET NULL``
    # but we want to clean the row up entirely, so target it explicitly.
    sess = await get_session(session_id)
    if sess is not None:
        sb_id = sess.get("current_sandbox_id")
        if sb_id:
            try:
                await delete_sandbox(sb_id)
            except Exception as e:
                log.warning("DELETE /sessions/%s: delete_sandbox(%s) failed: %s",
                            session_id, sb_id, e)
        async with get_db() as conn:
            await conn.execute("DELETE FROM sessions WHERE id = %s", (session_id,))


@app.post("/sessions/{session_id}/config")
async def session_set_config(session_id: str, request: Request):
    """Set mode/model/thought_level for a session via the SessionPool."""
    data = await _json_body(request)
    from api.sandbox import get_pool

    pool_session = await get_pool().get_session(session_id)
    await _forward_session_config(pool_session, data)
    return {"status": "ok"}


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
    session-scoped file proxy."""
    url = await _resolve_supervisor_url(session_id)
    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            r = await client.request(
                method, f"{url}{path}", params=params, json=json,
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
    try:
        async with httpx.AsyncClient(timeout=60) as client:
            r = await client.get(f"{url}/v1/files/download", params={"path": path})
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
