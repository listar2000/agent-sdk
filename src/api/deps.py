"""Shared request-handling dependencies for the API edge.

JSON-body parsing and 404-raising record lookups used by both ``api.server``
and the ``api.routers`` modules. Deliberately cycle-free — imports only
``fastapi`` + ``api.db`` + ``api.models``, never ``api.server`` — so routers
can depend on it to break the server<->routers import cycle. Re-exported from
``api.server`` so existing internal call sites and tests keep resolving.
"""
from __future__ import annotations

from fastapi import HTTPException, Request

from api.db import get_agent, get_session
from api.models import AgentRecord


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


# Lookup preamble helpers — raise HTTPException(404) on missing records so the
# caller never has to write ``if rec is None: return JSONResponse(...)``.

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
