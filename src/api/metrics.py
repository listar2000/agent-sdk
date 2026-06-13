"""Operational telemetry — errors, silent recoveries, resource leaks, and
per-provider operation timing — with **Postgres as the single source of
truth**.

Design (deliberately simple): there is **no in-memory aggregate store**.
Every captured event is written as one row to Postgres (``error_events`` or
``op_events``); every read (``GET /metrics``, ``/admin/errors``,
``/admin/ops``) is a SQL query over those tables. Consequence: the result is
**replica-independent** — whichever replica the load balancer routes a
request to runs the same query against the same DB and returns the same
answer. Nothing diverges per-replica; nothing is lost on restart.

Capture paths (each failure recorded once — ``record_error`` stamps a
sentinel on the exception instance so the same error bubbling through
route → HTTP handler → logging net counts once):

  * **errors**     — HTTP handlers (>=500), turn broadcast, and a logging
                     net on the ``api`` logger (WARNING+, category inferred
                     from the logger name) catching best-effort
                     ``log.warning/exception`` with zero call-site changes.
  * **recoveries** — silent auto-heals (cold_recover / resume / mid_turn_swap
                     / reattach_fallback); category=``recovery``.
  * **leaks**      — abandoned compute (destroy/stop/reap failures, orphan
                     detections); category=``leak``.
  * **ops**        — per-provider operation latency + ok/fail (cold_create,
                     resume, cold_recover, release, destroy, reap, and the
                     per-SDK-call ``sdk.*`` breakdown via timed_provider_op).

``record_*`` are async and ``await`` the INSERT at the (async) call sites.
The logging net is synchronous, so it schedules the write onto the captured
event loop. Writes are best-effort and never raise into the caller.

Tradeoff vs. the old in-memory design: a flapping warning loop writes one
row per occurrence (more truthful, but more rows during an incident). If
that ever bites, add a DB-side guard — don't reintroduce an in-memory store.
"""
from __future__ import annotations

import asyncio
import contextlib
import functools
import logging
import os
import time

from .identity import replica_id
from .redact import redact_secrets

log = logging.getLogger("api.metrics")

_DISABLE = os.environ.get("AGENT_SDK_METRICS_DISABLE") == "1"

# Exception-instance sentinel — set on first capture, checked everywhere for
# exactly-once accounting per exception object.
_SENTINEL = "__asdk_metric_seen__"

# Logger prefixes the net must NOT capture (``api.timing`` emits slow/5xx
# request lines at WARNING; ``api.metrics`` must never re-enter itself).
_NET_EXCLUDE = ("api.timing", "api.metrics")


def _category_for_logger(name: str) -> tuple[str, str | None]:
    """Infer ``(category, provider)`` from a logger name like
    ``api.providers.daytona.session`` or ``api.sandbox.pool``."""
    provider = None
    for p in ("daytona", "modal", "docker", "unix_local", "native"):
        if f".{p}" in name or name.endswith(p):
            provider = p
            break
    if ".providers." in name:
        return "provider", provider
    if name.endswith("acp_client") or ".acp" in name:
        return "acp", provider
    if ".sandbox.pool" in name:
        return "pool", provider
    if "orphan" in name:
        return "orphan", provider
    if ".sandbox" in name:
        return "sandbox", provider
    if name.endswith("api.turn"):
        return "turn", provider
    return "logged", provider


def _refine_category(category: str, message: str) -> str:
    """Nudge generic categories using message keywords — a daytona
    ``log.warning("reattach ... failed")`` lands as ``reattach``."""
    if category in ("provider", "sandbox", "logged"):
        m = (message or "").lower()
        if "reattach" in m:
            return "reattach"
        if "cold-recover" in m or "cold recover" in m:
            return "sandbox_create"
    return category


def _build_error_row(
    exc: BaseException | None,
    *,
    category: str,
    provider: str | None,
    session_id: str | None,
    agent_id: str | None,
    http_status: int | None,
    phase: str | None,
    message: str | None,
    context: dict,
) -> dict | None:
    """Build a redacted ``error_events`` row, or ``None`` if this exception
    was already recorded (exactly-once via the instance sentinel). Pure +
    synchronous so the sentinel is set before any ``await``."""
    if exc is not None:
        if getattr(exc, _SENTINEL, False):
            return None
        try:
            setattr(exc, _SENTINEL, True)
        except Exception:
            pass  # a few builtins forbid attrs; record anyway
    exc_type = type(exc).__name__ if exc is not None else context.pop("exc_type", None)
    if message is None and exc is not None:
        message = str(exc)
    message = redact_secrets(message or "")[:1000]
    return {
        "ts": time.time(), "replica_id": replica_id(),
        "category": _refine_category(category, message), "provider": provider,
        "session_id": session_id, "agent_id": agent_id, "exc_type": exc_type,
        "http_status": http_status, "phase": phase, "message": message,
        "context": {k: v for k, v in (context or {}).items() if v is not None},
    }


def _build_op_row(*, provider, operation, duration_ms, ok, session_id, error) -> dict:
    return {
        "ts": time.time(), "replica_id": replica_id(), "provider": provider,
        "operation": operation, "duration_ms": round(duration_ms, 2), "ok": bool(ok),
        "session_id": session_id,
        "error": redact_secrets(error)[:500] if error else None,
    }


class MetricsReporter:
    """Writes telemetry rows to Postgres. Holds no aggregate state — only the
    event loop reference (so the sync logging net can schedule writes) and the
    installed log handler."""

    def __init__(self) -> None:
        self._loop: asyncio.AbstractEventLoop | None = None
        self._handler: logging.Handler | None = None

    # -- capture: errors / recoveries / leaks ------------------------------
    async def record_error(self, exc: BaseException | None = None, *, category: str,
                           provider: str | None = None, session_id: str | None = None,
                           agent_id: str | None = None, http_status: int | None = None,
                           phase: str | None = None, message: str | None = None,
                           **context) -> None:
        if _DISABLE:
            return
        row = _build_error_row(
            exc, category=category, provider=provider, session_id=session_id,
            agent_id=agent_id, http_status=http_status, phase=phase,
            message=message, context=context)
        if row is None:
            return  # already recorded upstream — exactly-once
        await self._insert_error(row)

    async def record_recovery(self, kind: str, *, provider: str | None = None,
                              session_id: str | None = None, **context) -> None:
        """A server-side recovery that succeeded with no error surfaced to the
        caller (cold_recover, resume, mid_turn_swap, reattach_fallback)."""
        if _DISABLE:
            return
        row = _build_error_row(
            None, category="recovery", provider=provider, session_id=session_id,
            agent_id=None, http_status=None, phase=kind,
            message=f"recovery: {kind}", context=context)
        await self._insert_error(row)

    async def record_leak(self, kind: str, *, provider: str | None = None,
                          session_id: str | None = None, exc: BaseException | None = None,
                          **context) -> None:
        """Compute that was (or may have been) abandoned: a failed
        destroy/stop, a reaper release failure, or orphan detection."""
        if _DISABLE:
            return
        if exc is not None:
            try:
                setattr(exc, _SENTINEL, True)  # keep the net from re-counting it
            except Exception:
                pass
        msg = f"leak: {kind}" + (f" — {exc}" if exc else "")
        row = _build_error_row(
            None, category="leak", provider=provider, session_id=session_id,
            agent_id=None, http_status=None, phase=kind, message=msg, context=context)
        row["exc_type"] = type(exc).__name__ if exc else None
        await self._insert_error(row)

    async def _insert_error(self, row: dict) -> None:
        from . import db
        try:
            await db.insert_error_events([row])
        except Exception:
            # Never raise into the caller; never log at WARNING+ on api.* or
            # the net would re-enter.
            log.debug("error_events insert failed — dropped", exc_info=True)

    # -- capture: operation timing -----------------------------------------
    async def record_op(self, *, provider: str | None, operation: str,
                        duration_ms: float, ok: bool, session_id: str | None = None,
                        error: str | None = None) -> None:
        if _DISABLE:
            return
        row = _build_op_row(provider=provider, operation=operation,
                            duration_ms=duration_ms, ok=ok, session_id=session_id,
                            error=error)
        from . import db
        try:
            await db.insert_op_events([row])
        except Exception:
            log.debug("op_events insert failed — dropped", exc_info=True)

    @contextlib.asynccontextmanager
    async def timed_op(self, *, provider: str | None, operation: str,
                       session_id: str | None = None):
        """Time a provider/pool operation; record its outcome on exit, re-raise
        on failure."""
        t0 = time.perf_counter()
        err: BaseException | None = None
        try:
            yield
        except BaseException as e:
            err = e
            raise
        finally:
            with contextlib.suppress(Exception):
                await self.record_op(
                    provider=provider, operation=operation,
                    duration_ms=(time.perf_counter() - t0) * 1000,
                    ok=err is None, session_id=session_id,
                    error=str(err) if err is not None else None)

    # -- logging net bridge (sync -> loop) ---------------------------------
    def _record_from_net(self, exc, *, category, provider, message, logger, level) -> None:
        """Called synchronously by ``_MetricsLogHandler.emit`` (possibly from
        an executor thread). Schedules the async write onto the captured loop
        — fire-and-forget, best-effort."""
        coro = self.record_error(exc, category=category, provider=provider,
                                 message=message, logger=logger, level=level)
        loop = self._loop
        try:
            if loop is not None and loop.is_running():
                asyncio.run_coroutine_threadsafe(coro, loop)
                return
            running = asyncio.get_running_loop()
            running.create_task(coro)
        except RuntimeError:
            coro.close()  # no loop available (e.g. a sync unit test) — drop

    # -- lifecycle ---------------------------------------------------------
    def set_loop(self, loop: asyncio.AbstractEventLoop) -> None:
        self._loop = loop

    def install(self) -> None:
        """Attach the logging net to the ``api`` logger. Idempotent."""
        if _DISABLE or self._handler is not None:
            return
        self._handler = _MetricsLogHandler(self)
        logging.getLogger("api").addHandler(self._handler)

    def uninstall(self) -> None:
        if self._handler is not None:
            logging.getLogger("api").removeHandler(self._handler)
            self._handler = None


class _MetricsLogHandler(logging.Handler):
    """Safety net: schedule a write for any WARNING+ ``api.*`` log not already
    captured by an explicit ``record_error`` (checked via the exception
    sentinel). Category/provider inferred from the logger name."""

    def __init__(self, reporter: MetricsReporter) -> None:
        super().__init__(level=logging.WARNING)
        self._reporter = reporter

    def emit(self, record: logging.LogRecord) -> None:
        try:
            name = record.name
            if not name.startswith("api"):
                return
            if any(name == e or name.startswith(e + ".") for e in _NET_EXCLUDE):
                return
            exc = record.exc_info[1] if record.exc_info else None
            if exc is not None and getattr(exc, _SENTINEL, False):
                return  # an explicit path already counted this exception
            category, provider = _category_for_logger(name)
            self._reporter._record_from_net(
                exc, category=category, provider=provider,
                message=record.getMessage(), logger=name, level=record.levelname)
        except Exception:
            # A logging handler must never raise.
            pass


def timed_provider_op(provider: str, operation: str):
    """Decorator for an async provider SDK entry point (daytona/modal
    ``create_sandbox``, ``stop``, ``destroy``, …). Records its latency +
    ok/fail under ``(provider, "sdk.<operation>")`` so cold_create and friends
    decompose into the individual SDK calls."""
    def deco(fn):
        @functools.wraps(fn)
        async def wrapper(*args, **kwargs):
            async with _REPORTER.timed_op(provider=provider, operation=f"sdk.{operation}"):
                return await fn(*args, **kwargs)
        return wrapper
    return deco


# Module singleton — one reporter per worker process. The loop is captured and
# the net installed from the FastAPI lifespan.
_REPORTER = MetricsReporter()


def get_metrics() -> MetricsReporter:
    return _REPORTER
