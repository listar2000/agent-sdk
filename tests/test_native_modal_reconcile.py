"""Live native×modal boot-reconcile.

``reconcile_on_startup`` must reclaim a crash-orphaned native modal sandbox
(tagged ``agent-sdk.sandbox-id`` + ``agent_sdk_origin`` at create time) and
leave a live one untouched — the modal analogue of
``test_reconcile_reaps_native_docker_orphan``. This is the end-to-end
native boot-reconcile contract for the recreate-on-missing provider, where
orphan reclaim matters most (terminate is the only reclaim, no pause).

Live modal; skips when the SDK / creds are absent. Slow (real sandbox
provisioning). Parallel-safe: it treats every OTHER app sandbox present at
snapshot time as live, so only its own orphan is eligible for reaping.
"""

from __future__ import annotations

import contextlib
import os
import sys
import uuid

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))


def _modal_ok() -> bool:
    if not os.path.exists(os.path.expanduser("~/.modal.toml")):
        return False
    try:
        import modal  # noqa: F401
        return True
    except Exception:
        return False


pytestmark = [
    pytest.mark.skipif(not _modal_ok(),
                       reason="modal SDK + ~/.modal.toml required"),
    pytest.mark.asyncio,
]


async def _status(ref: str) -> str:
    from api.providers.modal import get_sandbox_status
    return await get_sandbox_status(ref)


async def test_reconcile_reaps_native_modal_orphan(monkeypatch):
    import asyncio

    os.environ["AGENT_SDK_ORIGIN"] = "test"
    from api import db as dbmod
    from api.native.transport import ModalTransport
    from api.providers import modal as modalmod
    from api.providers.modal import (
        _get_app,
        _require_modal,
        create_volume,
        delete_volume,
    )

    vname = f"reconcile-test-{uuid.uuid4().hex[:8]}"
    vol = await create_volume(vname)
    live = ModalTransport(workdir="/v/agents/live")
    orphan = ModalTransport(workdir="/v/agents/orphan")
    live_ref = await live.create(volume_ref=vol, subpath="agents/live")
    orphan_ref = await orphan.create(volume_ref=vol, subpath="agents/orphan")
    try:
        # reconcile is GLOBAL over the shared app: protect every OTHER sandbox
        # (parallel workers, other devs) by treating all present-except-orphan
        # as live. Snapshot immediately before the call to minimise the race
        # window where a brand-new sandbox could appear unprotected.
        modal, _ = _require_modal()
        app = await _get_app()

        def _present_ids() -> set[str]:
            return {sb.object_id
                    for sb in modal.Sandbox.list(app_id=app.app_id)}

        protected = (await asyncio.to_thread(_present_ids)) - {orphan_ref}
        assert live_ref in protected, "live sandbox must be in the protected set"

        async def _live_refs():
            return protected
        monkeypatch.setattr(dbmod, "live_sandbox_refs", _live_refs)

        await modalmod.reconcile_on_startup()

        # Modal terminate is async — poll until the orphan goes 'missing'.
        gone = False
        for _ in range(20):
            if await _status(orphan_ref) == "missing":
                gone = True
                break
            await asyncio.sleep(1.5)
        assert gone, (
            "reconcile did NOT terminate the orphaned native modal sandbox — "
            "native bare sandboxes must be tagged so the boot reconciler can "
            "reclaim them (the docker/daytona native parity)")
        assert await _status(live_ref) == "running", (
            "reconcile wrongly terminated a LIVE native modal sandbox "
            "(object_id match against live_refs regressed)")
    finally:
        for r in (live_ref, orphan_ref):
            with contextlib.suppress(Exception):
                await ModalTransport(sandbox_ref=r).destroy()
        with contextlib.suppress(Exception):
            await delete_volume(vol)
