"""Volume CRUD routes: create / list / get / delete.

Volume FILE operations (tree/read/edit/upload/...) live in
``api.routers.volume_files``. Cycle-free: imports deps / config_parse / db /
providers / models, never ``api.server``. Behavior identical to the former
inline ``@app.*("/volumes*")`` handlers.
"""
from __future__ import annotations

import logging
import uuid

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from api import providers as _providers_mod
from api.db import (
    count_sessions_by_volume,
    delete_sessions_by_volume,
    delete_volume,
    get_volume_by_name,
    list_volumes,
    upsert_volume,
)
from api.deps import _resolve_volume
from api.models import VolumeRecord
from api.services.config_parse import _validate_volume_name

log = logging.getLogger(__name__)
router = APIRouter()


class _VolumeCreateBody(BaseModel):
    name: str
    provider: str


@router.post("/volumes")
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


@router.get("/volumes")
async def list_volumes_route(provider: str | None = None):
    return await list_volumes(provider)


@router.get("/volumes/{id_or_name}")
async def get_volume_route(id_or_name: str):
    return await _resolve_volume(id_or_name)


@router.delete("/volumes/{id_or_name}", status_code=204)
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
