"""Admin / debug routes: cluster-aware read-only views of session state for
the dashboard + cleanup tooling. Cycle-free: db reads + a lazy pool peek,
never ``api.server``.
"""
from __future__ import annotations

from fastapi import APIRouter, Query

from api.db import list_sessions

router = APIRouter()


@router.get("/admin/sessions")
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


@router.get("/admin/sessions/inactive")
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
