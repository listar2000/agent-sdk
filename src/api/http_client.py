"""Shared httpx client + supervisor-proxy helpers.

The module-shared ``AsyncClient`` is created in the server's lifespan
(``set_client``) and closed at shutdown (``aclose``); the proxy functions read
it at call time. httpx pools connections per-host, so file-browse / exec
sequences against the same session reuse the existing TCP+TLS handshake
(bench, 50 concurrent calls: per-request client = 80 RPS, shared = 599 RPS).

Cycle-free: imports fastapi + httpx + (lazily) ``api.sandbox.get_pool`` —
never ``api.server`` — so routers can proxy to a session's supervisor without
a server<->routers import cycle.
"""
from __future__ import annotations

import httpx
from fastapi import HTTPException
from fastapi.responses import Response

# Set by the server lifespan via ``set_client``; read at call time by the
# proxy helpers below so it picks up the lifespan-created client.
_HTTP_CLIENT: httpx.AsyncClient | None = None


def set_client(client: httpx.AsyncClient) -> None:
    global _HTTP_CLIENT
    _HTTP_CLIENT = client


def get_client() -> httpx.AsyncClient | None:
    return _HTTP_CLIENT


async def aclose() -> None:
    global _HTTP_CLIENT
    if _HTTP_CLIENT is not None:
        await _HTTP_CLIENT.aclose()
        _HTTP_CLIENT = None


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
    session-scoped file proxy. Uses the module-shared client so repeat
    calls reuse the keep-alive connection to that supervisor."""
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
