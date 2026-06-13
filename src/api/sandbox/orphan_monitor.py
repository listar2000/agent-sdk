"""Background orphan-sandbox DETECTION monitor.

Every ``AGENT_SDK_ORPHAN_MONITOR_INTERVAL_S`` (default 30 min) it lists the
provider's labelled sandboxes, diffs them against the live session rows
(``db.live_sandbox_refs``), and logs the orphan count + state breakdown. A
sandbox with no session row is leaked compute against the account disk quota,
so this surfaces a slow leak (e.g. a recovery path that abandons a VM instead
of destroying it) in the logs long before it floods the account.

DETECTION ONLY — this loop never deletes. Reclamation stays explicit
(``scripts/cleanup_orphans.py`` / ``reconcile_on_startup``). An auto-reap here
would need a Postgres lease (this loop runs in every replica and sees
account-global state) plus a mid-create age guard; that's a deliberate
follow-up, not wired here.

Lives in its own module (not ``runtime.py``) so the feature is a clean,
self-contained add: ``server.py`` only calls ``start_orphan_monitor`` /
``stop_orphan_monitor`` from its lifespan.
"""
from __future__ import annotations

import asyncio
import logging
import os

log = logging.getLogger(__name__)

# 30-min scan cadence + warn threshold. Reap is intentionally NOT wired (see
# the module docstring) so this is a safe, read-only observability loop.
_INTERVAL_S = float(os.environ.get("AGENT_SDK_ORPHAN_MONITOR_INTERVAL_S", "1800"))
_WARN_AT = int(os.environ.get("AGENT_SDK_ORPHAN_MONITOR_WARN", "25"))

_monitor_task: asyncio.Task | None = None


async def start_orphan_monitor() -> None:
    """Start the detection monitor. Idempotent — a no-op while a prior task is
    still running. Returns immediately; the first scan runs at T+interval so
    boot stays fast (and boot orphans are already handled by reconcile)."""
    global _monitor_task
    if _monitor_task is not None and not _monitor_task.done():
        return
    _monitor_task = asyncio.create_task(_orphan_monitor_loop())


async def stop_orphan_monitor() -> None:
    """Cancel the monitor. Idempotent; safe to call at shutdown."""
    global _monitor_task
    if _monitor_task is None:
        return
    _monitor_task.cancel()
    try:
        await _monitor_task
    except (asyncio.CancelledError, Exception):
        pass
    _monitor_task = None


async def _orphan_monitor_loop() -> None:
    # Lazy import so the daytona SDK stays off the module-load path.
    from api.providers.daytona import detect_orphan_sandboxes

    origin = os.environ.get("AGENT_SDK_ORIGIN", "production")
    while True:
        # CancelledError must escape ONLY the sleep; the work body has its own
        # guard so one failed scan never kills the loop.
        try:
            await asyncio.sleep(_INTERVAL_S)
        except asyncio.CancelledError:
            return
        try:
            report = await detect_orphan_sandboxes()
            orphans = report["orphans"]
            total = report["total_seen"]
            n = len(orphans)
            sample = ",".join(sid[:16] for sid, _ in orphans[:5])
            emit = log.warning if (report["capped"] or n >= _WARN_AT) else log.info
            emit(
                "orphan-monitor: provider=daytona origin=%s total=%d live=%d "
                "orphans=%d states=%s capped=%s sample=[%s]",
                origin, total, total - n, n, report["state_hist"],
                report["capped"], sample,
            )
            if n > 0:
                # Orphans are sandboxes with no session row — already-leaked
                # compute against the account quota. Record the count so the
                # dashboard tracks "how much is leaking" over time.
                try:
                    from api.metrics import get_metrics
                    await get_metrics().record_leak(
                        "orphan_detected", provider="daytona",
                        count=n, total_seen=total, capped=report["capped"],
                        states=report["state_hist"],
                    )
                except Exception:
                    pass
        except Exception:
            log.exception("orphan-monitor tick failed")
