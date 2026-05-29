"""Session-scoped filesystem browsing (sandbox identity hidden from callers).

Thin proxies to the session's supervisor ``/v1/files/*`` endpoints via the
shared client in ``api.http_client``. Cycle-free: imports deps + http_client,
never ``api.server``. Behavior identical to the former inline handlers.
"""
from __future__ import annotations

from fastapi import APIRouter, Request

from api.deps import _json_body
from api.http_client import _download_from_session, _proxy_from_session

router = APIRouter()


@router.get("/sessions/{session_id}/files/tree")
async def session_files_tree(session_id: str):
    """Return the recursive directory tree of the session's sandbox."""
    return await _proxy_from_session(session_id, "GET", "/v1/files/tree")


@router.get("/sessions/{session_id}/files/read")
async def session_files_read(session_id: str, path: str):
    """Read a single file from the session's sandbox."""
    return await _proxy_from_session(
        session_id, "GET", "/v1/files/read", params={"path": path},
    )


@router.post("/sessions/{session_id}/files/edit")
async def session_files_edit(session_id: str, request: Request):
    """Edit or create a file. Body: same shape as ``/sandboxes/{id}/files/edit``."""
    return await _proxy_from_session(
        session_id, "POST", "/v1/files/edit",
        json=await _json_body(request),
    )


@router.post("/sessions/{session_id}/files/upload")
async def session_files_upload(session_id: str, request: Request):
    """Upload a file. Body: ``{"path": ..., "content": "<base64>"}``."""
    return await _proxy_from_session(
        session_id, "POST", "/v1/files/upload",
        json=await _json_body(request), timeout=60,
    )


@router.post("/sessions/{session_id}/files/delete")
async def session_files_delete(session_id: str, request: Request):
    """Delete a file or directory. Body: ``{"path": ...}``."""
    return await _proxy_from_session(
        session_id, "POST", "/v1/files/delete",
        json=await _json_body(request),
    )


@router.post("/sessions/{session_id}/files/rename")
async def session_files_rename(session_id: str, request: Request):
    """Rename/move a file or directory. Body: ``{"path": ..., "new_path": ...}``."""
    return await _proxy_from_session(
        session_id, "POST", "/v1/files/rename",
        json=await _json_body(request),
    )


@router.get("/sessions/{session_id}/files/download")
async def session_files_download(session_id: str, path: str):
    """Download a file as raw bytes from the session's sandbox."""
    return await _download_from_session(session_id, path)
