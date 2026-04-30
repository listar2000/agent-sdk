"""REST API server — agent/sandbox/session orchestration layer.

Run: uvicorn api.server:app --port 7778
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

from .db import (
    add_supervisor_agent_type,
    close_pool,
    delete_agent,
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
    list_volumes,
    log_event,
    update_session_env,
    update_session_secrets,
    upsert_agent,
    upsert_session,
    upsert_volume,
)
from .models import (
    STATUS_RUNNING,
    AgentConfig,
    AgentRecord,
    SandboxRecord,
    VolumeRecord,
)
from . import providers as _providers_mod
from .providers import ProviderInstance, default_cwd_for_provider
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

# Per-process state caches (SESSIONS dict, _INSTANCES dict,
# _session_locks, _sandbox_locks) are gone — replaced by
# api.sandbox.SessionPool. Pool owns its own per-session lock.


# Strong references to fire-and-forget background tasks so Python's GC can't
# collect them mid-flight ("Task was destroyed but it is pending!" bug — the
# event loop holds only a weak ref to tasks, so a caller that does
# ``asyncio.create_task(coro())`` without keeping the returned Task alive
# risks silent cancellation). Tasks self-discard from the set on completion.
_BG_TASKS: set[asyncio.Task] = set()


async def _cancel_task(task) -> None:
    """Cancel an asyncio task and await its completion so cleanup code runs."""
    if task is None or task.done():
        return
    task.cancel()
    try:
        await task
    except (asyncio.CancelledError, Exception):
        pass



@asynccontextmanager
async def lifespan(app):
    """App lifespan: init DB + pool, run startup reconciliation, then
    on shutdown release every active SandboxSession (snapshot first)."""
    from api.sandbox import shutdown_pool

    _configure_logging()
    init_db()
    await init_pool()

    async def _safe_reconcile(prov: str) -> None:
        try:
            await _providers_mod.reconcile_sandboxes(prov)
        except Exception as e:
            log.warning("startup reconcile for %s failed: %s", prov, e)

    await asyncio.gather(*[_safe_reconcile(p) for p in ("docker", "daytona", "local", "modal")])

    yield

    # Shutdown: snapshot + release every active session in the pool.
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
        # In-memory cache stats are now owned by the SessionPool;
        # /admin/sessions surfaces them via pool.has_active.
        "sessions": 0,
    }



# ---------------------------------------------------------------------------
# Shared helpers
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
# `/sessions` consumes them and routes them to the right row/state.
_AGENT_REJECTED_KEYS = (
    "cwd",
    "env",
    "dockerfile",
    "dockerfile_content",
    "shared_mounts",
)


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


def _merge_env(*sources: dict[str, str] | None) -> dict[str, str]:
    """Merge env dicts (later sources win); None is treated as empty."""
    out: dict[str, str] = {}
    for s in sources:
        if s:
            out.update(s)
    return out


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


@app.get("/sessions")
async def list_sessions_route():
    """List all sessions in the DB."""
    rows = await list_sessions()
    return [{"session_id": r["id"], "agent_id": r["agent_id"]} for r in rows]


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


# --------------------------------------------------------------------------- #
# Filesystem proxy: forwards /v1/files/* to the supervisor on a live sandbox  #
# --------------------------------------------------------------------------- #


async def _supervisor_url_for_session(session_id: str) -> str:
    """Resolve a session_id to a live supervisor URL.

    Cold-starts the SandboxSession if it isn't in the pool. Per the
    ephemeral design (docs §6) the pool is the only authority on which
    compute is alive."""
    from api.sandbox import get_pool
    session = await get_pool().get_session(session_id)
    url = session.supervisor_url
    if url is None:
        raise HTTPException(503, f"session {session_id} has no supervisor url")
    return url


def _supervisor_url_for_sandbox(sandbox_id: str) -> str:
    """Reverse-lookup a provider sandbox_id to a supervisor URL.

    Sandbox identity isn't durable in the ephemeral model, so we only
    resolve sandboxes the pool currently holds. If nothing is active
    with this id, return 404."""
    from api.sandbox import get_pool
    sess = get_pool().find_by_sandbox_id(sandbox_id)
    if sess is None or sess.supervisor_url is None:
        raise HTTPException(404, f"sandbox {sandbox_id} not active")
    return sess.supervisor_url


async def _proxy_files(
    supervisor_url: str, method: str, path: str, *, timeout: float = 30, **kw,
) -> Response:
    async with httpx.AsyncClient(timeout=timeout) as client:
        r = await client.request(method, f"{supervisor_url}{path}", **kw)
    return Response(
        content=r.content,
        status_code=r.status_code,
        media_type=r.headers.get("content-type", "application/json"),
    )


async def _proxy_download(supervisor_url: str, path: str) -> Response:
    async with httpx.AsyncClient(timeout=60) as client:
        r = await client.get(f"{supervisor_url}/v1/files/download", params={"path": path})
    return Response(
        content=r.content,
        status_code=r.status_code,
        media_type=r.headers.get("content-type", "application/octet-stream"),
        headers={"content-disposition": r.headers.get("content-disposition", "attachment")},
    )


@app.get("/sandboxes/{sandbox_id}/files/tree")
async def sandbox_files_tree(sandbox_id: str):
    return await _proxy_files(_supervisor_url_for_sandbox(sandbox_id), "GET", "/v1/files/tree")


@app.get("/sandboxes/{sandbox_id}/files/read")
async def sandbox_files_read(sandbox_id: str, path: str):
    return await _proxy_files(
        _supervisor_url_for_sandbox(sandbox_id), "GET", "/v1/files/read", params={"path": path},
    )


@app.post("/sandboxes/{sandbox_id}/files/edit")
async def sandbox_files_edit(sandbox_id: str, request: Request):
    return await _proxy_files(
        _supervisor_url_for_sandbox(sandbox_id), "POST", "/v1/files/edit",
        json=await _json_body(request),
    )


@app.post("/sandboxes/{sandbox_id}/files/upload")
async def sandbox_files_upload(sandbox_id: str, request: Request):
    return await _proxy_files(
        _supervisor_url_for_sandbox(sandbox_id), "POST", "/v1/files/upload",
        json=await _json_body(request), timeout=60,
    )


@app.post("/sandboxes/{sandbox_id}/files/delete")
async def sandbox_files_delete(sandbox_id: str, request: Request):
    return await _proxy_files(
        _supervisor_url_for_sandbox(sandbox_id), "POST", "/v1/files/delete",
        json=await _json_body(request),
    )


@app.post("/sandboxes/{sandbox_id}/files/rename")
async def sandbox_files_rename(sandbox_id: str, request: Request):
    return await _proxy_files(
        _supervisor_url_for_sandbox(sandbox_id), "POST", "/v1/files/rename",
        json=await _json_body(request),
    )


@app.get("/sandboxes/{sandbox_id}/files/download")
async def sandbox_files_download(sandbox_id: str, path: str):
    return await _proxy_download(_supervisor_url_for_sandbox(sandbox_id), path)


# ---------------------------------------------------------------------------
# Session-scoped filesystem browsing (sandbox identity hidden from callers)
# ---------------------------------------------------------------------------


@app.get("/sessions/{session_id}/files/tree")
async def session_files_tree(session_id: str):
    return await _proxy_files(await _supervisor_url_for_session(session_id), "GET", "/v1/files/tree")


@app.get("/sessions/{session_id}/files/read")
async def session_files_read(session_id: str, path: str):
    return await _proxy_files(
        await _supervisor_url_for_session(session_id), "GET", "/v1/files/read",
        params={"path": path},
    )


@app.post("/sessions/{session_id}/files/edit")
async def session_files_edit(session_id: str, request: Request):
    return await _proxy_files(
        await _supervisor_url_for_session(session_id), "POST", "/v1/files/edit",
        json=await _json_body(request),
    )


@app.post("/sessions/{session_id}/files/upload")
async def session_files_upload(session_id: str, request: Request):
    return await _proxy_files(
        await _supervisor_url_for_session(session_id), "POST", "/v1/files/upload",
        json=await _json_body(request), timeout=60,
    )


@app.post("/sessions/{session_id}/files/delete")
async def session_files_delete(session_id: str, request: Request):
    return await _proxy_files(
        await _supervisor_url_for_session(session_id), "POST", "/v1/files/delete",
        json=await _json_body(request),
    )


@app.post("/sessions/{session_id}/files/rename")
async def session_files_rename(session_id: str, request: Request):
    return await _proxy_files(
        await _supervisor_url_for_session(session_id), "POST", "/v1/files/rename",
        json=await _json_body(request),
    )


@app.get("/sessions/{session_id}/files/download")
async def session_files_download(session_id: str, path: str):
    return await _proxy_download(await _supervisor_url_for_session(session_id), path)


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
