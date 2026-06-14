"""Background orphan-sandbox DETECTION monitor.

Every ``AGENT_SDK_ORPHAN_MONITOR_INTERVAL_S`` (default 30 min) it lists each
provider's own-origin sandboxes, diffs them against the live session rows
(``db.live_sandbox_refs``, queried once and shared), and logs the orphan count
+ state breakdown per provider. A sandbox with no session row is leaked compute
against the account quota, so this surfaces a slow leak (e.g. a recovery path
that abandons a VM, or the untagged-supervisor leak class) in the logs long
before it floods the account. Scans whichever providers expose a
``detect_orphan_sandboxes`` and have their SDK present (daytona + modal today).

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


def _detectors() -> list[tuple[str, object]]:
    """(provider, detect_fn) pairs to scan. Each provider's SDK import is
    optional — a provider whose SDK/creds are absent is silently skipped, so
    the monitor degrades to whatever is configured."""
    out: list[tuple[str, object]] = []
    for prov, mod in (("daytona", "api.providers.daytona"),
                      ("modal", "api.providers.modal")):
        try:
            import importlib
            out.append((prov, importlib.import_module(mod).detect_orphan_sandboxes))
        except Exception:
            pass
    return out


async def _emit_report(provider: str, origin: str, report: dict) -> None:
    orphans = report["orphans"]
    total = report["total_seen"]
    n = len(orphans)
    sample = ",".join(sid[:16] for sid, _ in orphans[:5])
    emit = log.warning if (report["capped"] or n >= _WARN_AT) else log.info
    emit(
        "orphan-monitor: provider=%s origin=%s total=%d live=%d "
        "orphans=%d states=%s capped=%s sample=[%s]",
        provider, origin, total, total - n, n, report["state_hist"],
        report["capped"], sample,
    )
    if n > 0:
        # Orphans are sandboxes with no session row — already-leaked compute
        # against the account quota. Record the count so the dashboard tracks
        # "how much is leaking" over time, per provider.
        try:
            from api.metrics import get_metrics
            await get_metrics().record_leak(
                "orphan_detected", provider=provider,
                count=n, total_seen=total, capped=report["capped"],
                states=report["state_hist"],
            )
        except Exception:
            pass


async def _orphan_monitor_loop() -> None:
    # Lazy import so provider SDKs stay off the module-load path.
    from api import db as dbmod

    detectors = _detectors()
    origin = os.environ.get("AGENT_SDK_ORIGIN", "production")
    while True:
        # CancelledError must escape ONLY the sleep; the work body has its own
        # guard so one failed scan never kills the loop.
        try:
            await asyncio.sleep(_INTERVAL_S)
        except asyncio.CancelledError:
            return
        # One live-session query shared across all providers (it's global).
        try:
            live_refs = await dbmod.live_sandbox_refs()
        except Exception:
            log.exception("orphan-monitor: live_sandbox_refs failed")
            continue
        for provider, detect in detectors:
            try:
                report = await detect(live_refs=live_refs)
                await _emit_report(provider, origin, report)
            except Exception:
                log.exception("orphan-monitor tick failed for %s", provider)
