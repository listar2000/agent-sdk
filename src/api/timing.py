"""Request + phase timing helpers, with a replica prefix for multi-replica
log tails.

Used by ``api.server``'s HTTP middleware (one line per request, with
status + duration) and by hot-path phase timers inside ``sessions_create``
/ ``message+stream`` / lifespan startup. All logs go to ``api.timing``;
filter with ``LOG_LEVEL=DEBUG`` to see polling endpoints (``/health``,
``/admin/*``) too.

Bottleneck workflow: ``grep WARNING logs/server-r*.log`` surfaces every
slow request and every slow phase (anything past ``AGENT_SDK_SLOW_MS``,
default 500 ms) so you can spot a daytona cold-create stalling at 30s or
an ACP attach hanging without sifting through the full stream.
"""
from __future__ import annotations

import contextlib
import logging
import os
import time
from typing import AsyncIterator

from .identity import replica_id

log = logging.getLogger("api.timing")

# Anything slower than this gets bumped to WARNING regardless of method —
# the "things worth eyeballing" filter for `grep WARNING`. Tune via env.
_SLOW_MS = float(os.environ.get("AGENT_SDK_SLOW_MS", "500"))

# Paths whose noise drowns out signal: LB health probes (every few seconds),
# dashboard polling. Logged at DEBUG unless they're slow.
_POLLING_PATHS = ("/health",)
_POLLING_PREFIXES = ("/admin/",)
_POLLING_SUFFIXES = ("/status", "/sandbox")


def _is_polling(path: str) -> bool:
    if path in _POLLING_PATHS:
        return True
    if any(path.startswith(p) for p in _POLLING_PREFIXES):
        return True
    if any(path.endswith(s) for s in _POLLING_SUFFIXES):
        return True
    return False


def _level_for(duration_ms: float, *, polling: bool, errored: bool) -> int:
    if errored:
        return logging.WARNING
    if duration_ms >= _SLOW_MS:
        return logging.WARNING
    if polling:
        return logging.DEBUG
    return logging.INFO


@contextlib.asynccontextmanager
async def timed_phase(name: str, **fields) -> AsyncIterator[None]:
    """Async context manager: logs wall-clock duration on exit.

    Logs at INFO; bumps to WARNING when duration crosses ``_SLOW_MS``.
    Exceptions log at WARNING with ``err=<exc-type>`` and re-raise.

    Args:
      name: Phase label, e.g. ``"pool.cold_create"``.
      **fields: ``key=value`` pairs included in the log line for
        correlation (e.g. ``session_id="abc"``, ``provider="daytona"``).
    """
    t0 = time.perf_counter()
    err: str | None = None
    try:
        yield
    except BaseException as exc:
        err = type(exc).__name__
        raise
    finally:
        dt_ms = (time.perf_counter() - t0) * 1000
        level = _level_for(dt_ms, polling=False, errored=err is not None)
        parts = [f"{k}={v}" for k, v in fields.items() if v is not None]
        if err:
            parts.append(f"err={err}")
        suffix = (" " + " ".join(parts)) if parts else ""
        log.log(level, "[%s] %s %.1fms%s", replica_id(), name, dt_ms, suffix)


def log_request(
    *, method: str, path: str, status: int | str, duration_ms: float,
    session_id: str | None = None,
) -> None:
    """One-line request log, called from the HTTP middleware. Status may
    be an int (normal response) or a string (``"ERR"`` for exceptions
    that escaped the handler before a response was produced)."""
    errored = isinstance(status, str) or (isinstance(status, int) and status >= 500)
    level = _level_for(duration_ms, polling=_is_polling(path), errored=errored)
    sid = f" sid={session_id[:8]}" if session_id else ""
    log.log(
        level, "[%s] %s %s %s %.1fms%s",
        replica_id(), method, path, status, duration_ms, sid,
    )


def extract_session_id(path: str) -> str | None:
    """Pluck the session_id out of a ``/sessions/<sid>/...`` URL path so
    the middleware can correlate the request line with the phase logs
    further down (which already include ``session_id=...``). Returns
    ``None`` for any other path shape — including ``POST /sessions``
    (no id in the URL); the response handler will log the sid separately."""
    if not path.startswith("/sessions/"):
        return None
    parts = path.split("/", 3)
    if len(parts) < 3 or not parts[2]:
        return None
    return parts[2]
