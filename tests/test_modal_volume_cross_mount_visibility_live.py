"""LIVE guard: a Modal mount does NOT auto-reflect another mount's writes.

This documents the correctness fact that BLOCKS a Modal utility-sandbox cache
(the daytona-style "one warm sandbox per volume, reused across file-ops"
optimization). Measured 2026-06-14: a long-lived sandbox A does NOT see a
write that a later sandbox B makes to the same volume — A's mount reflects
the volume state at A's create time plus A's own writes, but not B's commits
(Modal mounts need an explicit reload for cross-mount visibility).

Consequence: a cached/reused utility sandbox would serve STALE volume reads
for any session that wrote after the utility sandbox was created. So the
modal volume adapter MUST keep its per-op-fresh-sandbox design — every op
mounts fresh and therefore sees the latest committed state. Do NOT add a
warm-sandbox cache to ``_run_volume_shell``.

The test reproduces the exact cache scenario and asserts the unsafe-for-cache
behavior, so it's a guard: if Modal ever makes mounts cross-mount live, this
fails and the caching optimization can be revisited.

  1. create sandbox A (long-lived) mounting volume V at /v
  2. create sandbox B mounting V, have B write a unique marker into /v
  3. read the marker FROM A — assert it is NOT visible (stale mount)

Live modal; skips without creds. Slow (real sandboxes). Self-cleans.
"""
from __future__ import annotations

import os
import uuid

import pytest


def _modal_ok() -> bool:
    if not (os.environ.get("MODAL_TOKEN_ID") and os.environ.get("MODAL_TOKEN_SECRET")):
        if not os.path.exists(os.path.expanduser("~/.modal.toml")):
            return False
    try:
        import modal  # noqa: F401
        return True
    except Exception:
        return False


pytestmark = [
    pytest.mark.skipif(not _modal_ok(), reason="modal SDK + creds required"),
    pytest.mark.asyncio,
]


async def test_modal_cross_mount_write_visibility(monkeypatch):
    import asyncio

    os.environ.setdefault("AGENT_SDK_ORIGIN", "test")
    from api.providers.modal import (
        _require_modal, _get_app, _get_volume_image, create_volume, delete_volume,
    )

    modal, _ = _require_modal()
    app = await _get_app()
    img = await _get_volume_image()
    vname = f"xmount-probe-{uuid.uuid4().hex[:8]}"
    vol = await create_volume(vname)
    marker = f"hello-{uuid.uuid4().hex[:12]}"

    def _mk():
        return modal.Sandbox.create(
            "sleep", "infinity", app=app, image=img,
            volumes={"/v": modal.Volume.from_name(vname)}, timeout=300)

    sb_a = sb_b = None
    try:
        # A exists FIRST and stays up (the would-be cached utility sandbox).
        sb_a = await asyncio.to_thread(_mk)

        def _exec(sb, cmd):
            p = sb.exec("sh", "-c", cmd); p.wait()
            return (p.stdout.read() or "") + (p.stderr.read() or "")

        # A confirms the marker is absent right now.
        before = await asyncio.to_thread(_exec, sb_a, "cat /v/marker.txt 2>&1 || echo MISSING")
        assert "MISSING" in before or marker not in before

        # B (created AFTER A) writes the marker.
        sb_b = await asyncio.to_thread(_mk)
        await asyncio.to_thread(_exec, sb_b, f"printf %s {marker} > /v/marker.txt; sync")

        # Give B's write a generous propagation window. If it were ever going
        # to appear in A's mount, 10s is ample.
        seen = ""
        for _ in range(20):
            seen = await asyncio.to_thread(_exec, sb_a, "cat /v/marker.txt 2>&1 || echo MISSING")
            if marker in seen:
                break
            await asyncio.sleep(0.5)

        visible = marker in seen
        print(f"\n[modal xmount] B's write visible to pre-existing A: {visible!r} "
              f"(read={seen.strip()!r})")

        # Sanity: a FRESH mount (sandbox C, created after B's write) MUST see
        # it — proving the write committed and the staleness is mount-age, not
        # a lost write.
        sb_c = await asyncio.to_thread(_mk)
        try:
            fresh = await asyncio.to_thread(_exec, sb_c, "cat /v/marker.txt 2>&1 || echo MISSING")
        finally:
            await asyncio.to_thread(sb_c.terminate)
        assert marker in fresh, (
            "a freshly-mounted sandbox must see B's committed write — else the "
            "probe itself is broken (write never committed)")

        # The guard: the PRE-EXISTING mount did NOT see B's later write. This is
        # why the modal volume adapter must stay per-op-fresh (no warm-sandbox
        # cache) — a reused sandbox would serve stale reads. If Modal ever makes
        # mounts cross-mount live, this assert fails → revisit the cache.
        assert not visible, (
            "Modal mount became cross-mount live-consistent — a pre-existing "
            "sandbox now sees another mount's later write. The utility-sandbox "
            "cache optimization (warm sandbox reused across volume ops) is now "
            "SAFE and can be revisited for _run_volume_shell.")
    finally:
        for sb in (sb_a, sb_b):
            if sb is not None:
                try:
                    await asyncio.to_thread(sb.terminate)
                except Exception:
                    pass
        try:
            await delete_volume(vname)
        except Exception:
            pass
