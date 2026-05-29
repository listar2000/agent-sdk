"""Read-only / simple session routes: list, metadata, status, sandbox info,
log, and the on-demand reap. None of these touch the create spine or the
prompt drive, so they extract cleanly. Cycle-free: deps + db + redact + lazy
api.sandbox peeks, never ``api.server``. The create/message/lifecycle spine
stays in server.py (slice 7 / SessionService).
"""
from __future__ import annotations

import asyncio
import time

from fastapi import APIRouter, HTTPException, Query, Request

from api.db import get_session, get_session_log, read_sandbox_state
from api.deps import _require_session_row
from api.redact import redact_pre_start_commands

router = APIRouter()


@router.get("/sessions")
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


@router.get("/sessions/{session_id}")
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


@router.get("/sessions/{session_id}/status")
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


@router.post("/sessions/{session_id}/reap")
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


@router.get("/sessions/{session_id}/sandbox")
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
            from api.providers.unix_local import _load_record
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
        from api.providers.unix_local import _load_record
        marker, _rec = await asyncio.to_thread(_load_record, sandbox_ref)
        if marker is not None:
            result["marker_path"] = str(marker)
    return result


@router.get("/sessions/{session_id}/log")
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
