"""Volume file operations: tree / read / download / exists / edit / upload /
mkdir / delete / rename — served directly against a provider's volume adapter
(no sandbox required). Split out of the volumes block (refactor slice 6e) as
its own concern. Cycle-free: imports deps + providers, never ``api.server``.
"""
from __future__ import annotations

import asyncio
import base64
from urllib.parse import quote

from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse, Response
from pydantic import BaseModel

from api.deps import _resolve_volume
from api.providers import VolumeFileExistsError, get_volume_adapter
from api.providers._shared import _safe_path as _shared_safe_path

router = APIRouter()


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


@router.get("/volumes/{id_or_name}/files/tree")
async def volume_files_tree(id_or_name: str, path: str = ""):
    vol = await _resolve_volume(id_or_name)
    adapter = get_volume_adapter(vol.provider, vol.provider_ref)
    rel = _safe_path(path)
    try:
        tree = await adapter.tree(rel)
    except Exception as e:
        raise _volume_fs_err("Tree", vol.provider, e)
    return {"tree": tree}


@router.get("/volumes/{id_or_name}/files/read")
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


@router.get("/volumes/{id_or_name}/files/download")
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


@router.get("/volumes/{id_or_name}/files/exists")
async def volume_files_exists(id_or_name: str, path: str):
    vol = await _resolve_volume(id_or_name)
    adapter = get_volume_adapter(vol.provider, vol.provider_ref)
    rel = _safe_path(path)
    try:
        exists = await adapter.exists(rel)
    except Exception as e:
        raise _volume_fs_err("Exists", vol.provider, e)
    return {"exists": exists}


@router.post("/volumes/{id_or_name}/files/edit", status_code=204)
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


@router.post("/volumes/{id_or_name}/files/upload", status_code=204)
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


@router.post("/volumes/{id_or_name}/files/mkdir", status_code=204)
async def volume_files_mkdir(id_or_name: str, body: _VolumePathBody):
    vol = await _resolve_volume(id_or_name)
    adapter = get_volume_adapter(vol.provider, vol.provider_ref)
    rel = _safe_path(body.path)
    try:
        await adapter.mkdir(rel)
    except Exception as e:
        raise _volume_fs_err("Mkdir", vol.provider, e)


@router.post("/volumes/{id_or_name}/files/delete", status_code=204)
async def volume_files_delete(id_or_name: str, body: _VolumePathBody):
    vol = await _resolve_volume(id_or_name)
    adapter = get_volume_adapter(vol.provider, vol.provider_ref)
    rel = _safe_path(body.path)
    try:
        await adapter.delete(rel)
    except Exception as e:
        raise _volume_fs_err("Delete", vol.provider, e)


@router.post("/volumes/{id_or_name}/files/rename", status_code=204)
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
