"""Per-process batcher for ``session_log`` INSERTs.

Replaces the per-event ``db.log_event`` call with a buffered writer that
flushes via ``executemany`` every ~100ms. Cuts INSERT round-trips by
~1-2 orders of magnitude under load (1000 concurrent sessions × ~5 evt/s
becomes ~10 batched INSERTs/s instead of ~5000).

Ordering contract: per-session order is preserved because each session's
writes are serialised by ``SandboxSession._prompt_lock`` — they reach the
batcher in causal order. The flush emits rows in arrival order; Postgres
``id BIGSERIAL`` is monotonic by INSERT order within a single
``executemany``, so the log row id matches that order. Cross-session
ordering is not promised (never was — different sessions interleave
arbitrarily under concurrent prompts).

Durability tradeoff: up to ``flush_ms`` of buffered events are lost on a
hard Python crash. Acceptable because ``GET /sessions/{id}/events`` is
already lossy on disconnect, and ``GET /sessions/{id}/log`` consumers
poll for the next batch on retry.
"""
from __future__ import annotations

import asyncio
import logging
import os
from typing import Any

from psycopg.types.json import Json

log = logging.getLogger(__name__)


_FLUSH_MS = int(os.environ.get("AGENT_SDK_LOG_FLUSH_MS", "100"))
_MAX_BATCH = int(os.environ.get("AGENT_SDK_LOG_MAX_BATCH", "500"))


class SessionLogBatcher:
    """Buffer ``session_log`` rows and flush via ``executemany``.

    One instance per uvicorn worker process. Started in lifespan, stopped
    on shutdown with a final drain. Thread-safe under asyncio (single
    event loop); not multi-threaded.
    """

    def __init__(self, *, flush_ms: int = _FLUSH_MS, max_batch: int = _MAX_BATCH) -> None:
        self.flush_ms = flush_ms
        self.max_batch = max_batch
        self._pending: list[tuple[str, str, str, Any]] = []
        self._wake = asyncio.Event()
        self._stop = False
        self._task: asyncio.Task | None = None

    async def start(self) -> None:
        if self._task is not None and not self._task.done():
            return
        self._task = asyncio.create_task(self._run(), name="session_log_batcher")

    async def stop(self) -> None:
        self._stop = True
        self._wake.set()
        if self._task is not None:
            try:
                await asyncio.wait_for(self._task, timeout=5.0)
            except (asyncio.TimeoutError, asyncio.CancelledError, Exception):
                pass
            self._task = None
        # Final drain so a pending batch lands before close_pool() pulls
        # the DB out from under us.
        await self._flush_once()

    async def add(
        self,
        *,
        session_id: str,
        agent_id: str,
        event_type: str,
        payload: dict,
    ) -> None:
        """Enqueue one row. Non-blocking. If the buffer reaches
        ``max_batch``, wake the flusher early — the bounded enqueue
        bounds memory at maybe 500 rows × ~1 KB = 500 KB worst case.
        """
        self._pending.append((session_id, agent_id, event_type, payload))
        if len(self._pending) >= self.max_batch:
            self._wake.set()

    async def _run(self) -> None:
        # Periodic flusher; also wakes early on max-batch.
        while not self._stop:
            try:
                await asyncio.wait_for(self._wake.wait(), timeout=self.flush_ms / 1000)
            except asyncio.TimeoutError:
                pass
            self._wake.clear()
            await self._flush_once()

    async def _flush_once(self) -> None:
        if not self._pending:
            return
        # Hand off the current buffer; new adds during the I/O land in a
        # fresh list and flush on the next tick.
        batch = self._pending
        self._pending = []
        # Lazy import to avoid a load-time cycle with api.db.
        from . import db as _db

        try:
            async with _db.get_db() as conn:
                async with conn.cursor() as cur:
                    await cur.executemany(
                        "INSERT INTO session_log (session_id, agent_id, event_type, payload)"
                        " VALUES (%s, %s, %s, %s)",
                        [(sid, aid, et, Json(p)) for (sid, aid, et, p) in batch],
                    )
        except Exception:
            log.exception("session_log batch flush failed (n=%d) — events dropped", len(batch))


# Module-level singleton — exactly one batcher per worker process. Started
# from the FastAPI lifespan; readable via ``get_batcher()`` for the persist
# hot path.
_BATCHER: SessionLogBatcher | None = None


def get_batcher() -> SessionLogBatcher | None:
    """Return the running batcher, or None if start() hasn't been called.

    Callers in the persist hot path should fall back to ``db.log_event``
    when this is None (e.g. test contexts that bypass lifespan)."""
    return _BATCHER


async def start_batcher() -> SessionLogBatcher:
    """Idempotent. Lifespan calls this once at startup."""
    global _BATCHER
    if _BATCHER is None:
        _BATCHER = SessionLogBatcher()
        await _BATCHER.start()
    return _BATCHER


async def stop_batcher() -> None:
    """Drain + stop the batcher. Lifespan calls this once at shutdown."""
    global _BATCHER
    if _BATCHER is not None:
        try:
            await _BATCHER.stop()
        finally:
            _BATCHER = None
