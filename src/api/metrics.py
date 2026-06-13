"""Central operational telemetry: errors, silent recoveries, resource
leaks, and per-provider operation timing/flakiness.

Everything the server does that's worth monitoring flows through here so a
single ``GET /metrics`` (live, this replica) + ``GET /admin/errors`` /
``GET /admin/ops`` (durable, cross-replica) answer the operational
questions: *what's failing, how often does the server silently auto-heal,
how much compute is leaking, where's the bottleneck, and how flaky / slow
are daytona and modal.*

Signals
-------
* **errors** — failures by category/provider/status (HTTP 5xx, provider,
  ACP, turn, pool, sandbox_create, reattach, …). Captured once per
  exception (sentinel dedup) via the HTTP handlers, the turn path, and a
  logging net on the ``api`` logger that catches every best-effort
  ``log.warning/exception`` with zero call-site changes.
* **recoveries** — *silent* server-side auto-heals: cold-recover of a dead
  cached session, resume-from-hibernation, mid-turn session swap, daytona
  reattach→cold-create fallback. These succeed (no user-visible error) but
  signal churn / underlying instability.
* **leaks** — compute that was, or may have been, abandoned:
  destroy-on-failed-start failures, daytona stop failures, reaper release
  failures, and orphan-monitor detections (sandboxes with no session row).
* **ops** — every provider/pool operation (cold_create, resume,
  cold_recover, release, destroy, reap) with duration + ok/fail, bucketed
  per provider. This is the latency + flakiness history.

Sinks: in-memory counters + a recent ring (live), and the Postgres
``error_events`` (errors/recoveries/leaks) + ``op_events`` (timings) tables
via non-blocking batchers (mirrors ``event_buffer.SessionLogBatcher``).
"""
from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import threading
import time
from collections import Counter, defaultdict, deque

from .identity import replica_id
from .redact import redact_secrets

log = logging.getLogger("api.metrics")

# ---------------------------------------------------------------------------
# Tunables
# ---------------------------------------------------------------------------
_DISABLE = os.environ.get("AGENT_SDK_METRICS_DISABLE") == "1"
_RING_SIZE = int(os.environ.get("AGENT_SDK_METRICS_RING", "500"))
# Identical (category, provider, exc_type, message-prefix) events landing
# within this window are counted but NOT re-inserted to Postgres / the ring,
# so a hot warning loop can't flood the table. Counters stay accurate.
_DEDUP_S = float(os.environ.get("AGENT_SDK_METRICS_DEDUP_S", "5"))
_FLUSH_MS = int(os.environ.get("AGENT_SDK_METRICS_FLUSH_MS", "250"))
_MAX_BATCH = int(os.environ.get("AGENT_SDK_METRICS_MAX_BATCH", "200"))
# Bound in-memory backlog if Postgres is unreachable — drop oldest on overflow.
_QUEUE_MAX = int(os.environ.get("AGENT_SDK_METRICS_QUEUE_MAX", "10000"))

# Exception-instance sentinel: set on first capture, checked everywhere to
# guarantee exactly-once accounting per exception object.
_SENTINEL = "__asdk_metric_seen__"

# Logger-name prefixes the net must NOT capture: ``api.timing`` emits slow /
# 5xx request lines at WARNING (handled separately as perf), and
# ``api.metrics`` must never re-enter itself.
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
    """Nudge generic categories toward a more useful label using message
    keywords — e.g. a daytona ``log.warning("reattach ... failed")`` lands
    as ``reattach`` rather than the catch-all ``provider``."""
    if category in ("provider", "sandbox", "logged"):
        m = (message or "").lower()
        if "reattach" in m:
            return "reattach"
        if "cold-recover" in m or "cold recover" in m:
            return "sandbox_create"
    return category


# ---------------------------------------------------------------------------
# Durable sink — generic batched INSERT
# ---------------------------------------------------------------------------
class _RowBatcher:
    """Deque-backed buffer flushed via a named ``api.db`` insert function.

    ``add`` is called from arbitrary threads — provider SDK calls run in the
    executor and log from those threads — so it relies on ``deque.append``
    being atomic under the GIL; the asyncio flush loop drains with
    ``popleft``. No cross-thread asyncio signaling is needed, which is why
    this doesn't reuse SessionLogBatcher's ``asyncio.Event``.
    """

    def __init__(self, insert_attr: str, *, flush_ms: int = _FLUSH_MS,
                 max_batch: int = _MAX_BATCH) -> None:
        self._insert_attr = insert_attr  # name of the api.db coroutine
        self.flush_ms = flush_ms
        self.max_batch = max_batch
        self._pending: deque[dict] = deque(maxlen=_QUEUE_MAX)
        self._stop = False
        self._task: asyncio.Task | None = None

    def add(self, rec: dict) -> None:
        self._pending.append(rec)

    async def start(self) -> None:
        if self._task is not None and not self._task.done():
            return
        self._stop = False
        self._task = asyncio.create_task(self._run(), name=f"batcher_{self._insert_attr}")

    async def stop(self) -> None:
        self._stop = True
        if self._task is not None:
            try:
                await asyncio.wait_for(self._task, timeout=5.0)
            except (asyncio.TimeoutError, asyncio.CancelledError, Exception):
                pass
            self._task = None
        await self._flush_once()

    async def _run(self) -> None:
        while not self._stop:
            await asyncio.sleep(self.flush_ms / 1000)
            await self._flush_once()

    async def _flush_once(self) -> None:
        if not self._pending:
            return
        batch: list[dict] = []
        while self._pending and len(batch) < self.max_batch:
            try:
                batch.append(self._pending.popleft())
            except IndexError:
                break
        if not batch:
            return
        from . import db as _db
        try:
            await getattr(_db, self._insert_attr)(batch)
        except Exception:
            # Persistence must never raise into a caller, and must NOT log at
            # WARNING+ on the ``api`` tree or the net would re-enter.
            log.debug("%s flush failed (n=%d) — dropped", self._insert_attr, len(batch),
                      exc_info=True)


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------
class MetricsRegistry:
    """Thread-safe in-memory aggregator + durable batcher front-ends."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        # errors
        self._by_category: Counter = Counter()
        self._by_provider: Counter = Counter()
        self._by_status: Counter = Counter()
        # recoveries (silent auto-heals — NOT counted as errors)
        self._recovery_total = 0
        self._by_recovery: Counter = Counter()
        self._by_recovery_provider: Counter = Counter()
        # leaks (abandoned / possibly-abandoned compute)
        self._leak_total = 0
        self._by_leak: Counter = Counter()
        self._by_leak_provider: Counter = Counter()
        # shared recent timeline (errors + recoveries + leaks)
        self._recent: deque[dict] = deque(maxlen=_RING_SIZE)
        # per-(provider, operation) timing + flakiness
        self._op_stats: dict[tuple, dict] = defaultdict(
            lambda: {"count": 0, "fail": 0, "durs": deque(maxlen=500)})
        # request perf
        self._requests_total = 0
        self._requests_5xx = 0
        self._req_latency: deque[float] = deque(maxlen=2000)
        self._phase_samples: dict[str, deque] = defaultdict(lambda: deque(maxlen=500))
        self._phase_fail: Counter = Counter()
        self._turn_ok = 0
        self._turn_fail = 0
        self._started_at = time.time()
        self._last_seen: dict[str, float] = {}
        self._err_batcher = _RowBatcher("insert_error_events")
        self._op_batcher = _RowBatcher("insert_op_events")
        self._handler: logging.Handler | None = None

    # -- internal -----------------------------------------------------------
    def _persist_event(self, rec: dict) -> None:
        """Dedup-throttle then push to the recent ring + error_events table.
        Counters are bumped by the caller (under its own lock); this only
        guards the ring/DB against identical floods."""
        ts = rec["ts"]
        key = (f'{rec["category"]}|{rec.get("provider")}|'
               f'{rec.get("exc_type")}|{(rec.get("message") or "")[:120]}')
        with self._lock:
            last = self._last_seen.get(key, 0.0)
            if ts - last < _DEDUP_S:
                return
            if len(self._last_seen) > 5000:
                self._last_seen.clear()
            self._last_seen[key] = ts
            self._recent.append(rec)
        self._err_batcher.add(rec)

    def _mk_rec(self, *, category, provider, session_id, agent_id, exc_type,
                http_status, phase, message, context) -> dict:
        return {
            "ts": time.time(), "replica_id": replica_id(), "category": category,
            "provider": provider, "session_id": session_id, "agent_id": agent_id,
            "exc_type": exc_type, "http_status": http_status, "phase": phase,
            "message": redact_secrets(message or "")[:1000],
            "context": {k: v for k, v in (context or {}).items() if v is not None},
        }

    # -- capture: errors ----------------------------------------------------
    def record_error(
        self,
        exc: BaseException | None = None,
        *,
        category: str,
        provider: str | None = None,
        session_id: str | None = None,
        agent_id: str | None = None,
        http_status: int | None = None,
        phase: str | None = None,
        message: str | None = None,
        **context,
    ) -> None:
        """Record one failure. Idempotent per exception instance."""
        if _DISABLE:
            return
        if exc is not None:
            if getattr(exc, _SENTINEL, False):
                return  # already recorded upstream — exactly-once
            try:
                setattr(exc, _SENTINEL, True)
            except Exception:
                pass  # a few builtin exceptions forbid attrs; record anyway
        exc_type = type(exc).__name__ if exc is not None else context.pop("exc_type", None)
        if message is None and exc is not None:
            message = str(exc)
        rec = self._mk_rec(category="", provider=provider, session_id=session_id,
                           agent_id=agent_id, exc_type=exc_type, http_status=http_status,
                           phase=phase, message=message, context=context)
        rec["category"] = _refine_category(category, rec["message"])
        with self._lock:
            self._by_category[rec["category"]] += 1
            if provider:
                self._by_provider[provider] += 1
            if http_status:
                self._by_status[str(http_status)] += 1
        self._persist_event(rec)

    # -- capture: recoveries (silent auto-heals) ----------------------------
    def record_recovery(self, kind: str, *, provider: str | None = None,
                         session_id: str | None = None, **context) -> None:
        """A server-side recovery that succeeded without surfacing an error
        to the caller (cold-recover, resume, mid-turn swap, reattach
        fallback). Tracked separately from errors — it's a churn / latency
        signal, not a failure."""
        if _DISABLE:
            return
        rec = self._mk_rec(category="recovery", provider=provider, session_id=session_id,
                           agent_id=None, exc_type=None, http_status=None, phase=kind,
                           message=f"recovery: {kind}", context=context)
        with self._lock:
            self._recovery_total += 1
            self._by_recovery[kind] += 1
            if provider:
                self._by_recovery_provider[provider] += 1
        self._persist_event(rec)

    # -- capture: leaks -----------------------------------------------------
    def record_leak(self, kind: str, *, provider: str | None = None,
                    session_id: str | None = None, exc: BaseException | None = None,
                    **context) -> None:
        """Compute that was (or may have been) abandoned: a failed
        destroy/stop, a reaper release failure, or orphan detection. This is
        the 'how much is leaking' signal."""
        if _DISABLE:
            return
        if exc is not None:
            try:
                setattr(exc, _SENTINEL, True)  # keep the net from re-counting it
            except Exception:
                pass
        rec = self._mk_rec(category="leak", provider=provider, session_id=session_id,
                           agent_id=None, exc_type=type(exc).__name__ if exc else None,
                           http_status=None, phase=kind,
                           message=f"leak: {kind}" + (f" — {exc}" if exc else ""),
                           context=context)
        with self._lock:
            self._leak_total += 1
            self._by_leak[kind] += 1
            if provider:
                self._by_leak_provider[provider] += 1
        self._persist_event(rec)

    # -- capture: provider/pool operation timing ----------------------------
    def record_op(self, *, provider: str | None, operation: str, duration_ms: float,
                  ok: bool, session_id: str | None = None, error: str | None = None) -> None:
        """One provider/pool operation outcome (cold_create, resume,
        cold_recover, release, destroy, reap). Feeds per-provider latency
        percentiles + failure rate."""
        if _DISABLE:
            return
        with self._lock:
            st = self._op_stats[(provider, operation)]
            st["count"] += 1
            if not ok:
                st["fail"] += 1
            st["durs"].append(duration_ms)
        self._op_batcher.add({
            "ts": time.time(), "replica_id": replica_id(), "provider": provider,
            "operation": operation, "duration_ms": round(duration_ms, 2), "ok": ok,
            "session_id": session_id,
            "error": redact_secrets(error)[:500] if error else None,
        })

    @contextlib.asynccontextmanager
    async def timed_op(self, *, provider: str | None, operation: str,
                       session_id: str | None = None):
        """Async context manager: time a provider/pool operation and record
        its outcome (ok/fail + duration), then re-raise on failure."""
        t0 = time.perf_counter()
        err: BaseException | None = None
        try:
            yield
        except BaseException as e:
            err = e
            raise
        finally:
            self.record_op(
                provider=provider, operation=operation,
                duration_ms=(time.perf_counter() - t0) * 1000,
                ok=err is None, session_id=session_id,
                error=str(err) if err is not None else None,
            )

    # -- capture: request / phase / turn perf -------------------------------
    def record_request(self, *, path: str, status, duration_ms: float) -> None:
        if _DISABLE:
            return
        is_5xx = status == "ERR" or (isinstance(status, int) and status >= 500)
        with self._lock:
            self._requests_total += 1
            if is_5xx:
                self._requests_5xx += 1
            self._req_latency.append(duration_ms)

    def record_phase(self, *, name: str, duration_ms: float, ok: bool,
                     provider: str | None = None) -> None:
        if _DISABLE:
            return
        key = f"{name}@{provider}" if provider else name
        with self._lock:
            self._phase_samples[key].append(duration_ms)
            if not ok:
                self._phase_fail[key] += 1

    def record_turn(self, ok: bool) -> None:
        if _DISABLE:
            return
        with self._lock:
            if ok:
                self._turn_ok += 1
            else:
                self._turn_fail += 1

    # -- read ---------------------------------------------------------------
    def snapshot(self) -> dict:
        with self._lock:
            lat = sorted(self._req_latency)
            phases = {}
            for name, samples in self._phase_samples.items():
                s = sorted(samples)
                phases[name] = {"count": len(s), "p50": _pct(s, 0.50),
                                "p95": _pct(s, 0.95), "fail": self._phase_fail.get(name, 0)}
            ops = {}
            for (provider, operation), st in self._op_stats.items():
                s = sorted(st["durs"])
                count = st["count"]
                ops[f"{operation}@{provider}"] = {
                    "count": count, "fail": st["fail"],
                    "fail_rate": round(st["fail"] / count, 4) if count else 0.0,
                    "p50": _pct(s, 0.50), "p95": _pct(s, 0.95),
                }
            return {
                "replica_id": replica_id(),
                "uptime_s": round(time.time() - self._started_at, 1),
                "errors": {
                    "total": sum(self._by_category.values()),
                    "by_category": dict(self._by_category),
                    "by_provider": dict(self._by_provider),
                    "by_http_status": dict(self._by_status),
                },
                "recoveries": {
                    "total": self._recovery_total,
                    "by_kind": dict(self._by_recovery),
                    "by_provider": dict(self._by_recovery_provider),
                },
                "leaks": {
                    "total": self._leak_total,
                    "by_kind": dict(self._by_leak),
                    "by_provider": dict(self._by_leak_provider),
                },
                "ops": ops,
                "requests": {
                    "total": self._requests_total,
                    "errors_5xx": self._requests_5xx,
                    "error_rate": round(self._requests_5xx / self._requests_total, 4)
                    if self._requests_total else 0.0,
                    "latency_ms": {"p50": _pct(lat, 0.50), "p95": _pct(lat, 0.95)},
                },
                "turns": {"ok": self._turn_ok, "fail": self._turn_fail},
                "phases": phases,
                "recent": list(self._recent)[-100:],
            }

    def reset(self) -> None:
        """Clear all in-memory state. Test hook (the module singleton is
        shared across tests). Does not touch the batcher tasks or handler."""
        with self._lock:
            for c in (self._by_category, self._by_provider, self._by_status,
                      self._by_recovery, self._by_recovery_provider,
                      self._by_leak, self._by_leak_provider, self._phase_fail):
                c.clear()
            self._recent.clear()
            self._req_latency.clear()
            self._phase_samples.clear()
            self._op_stats.clear()
            self._last_seen.clear()
            self._recovery_total = self._leak_total = 0
            self._requests_total = self._requests_5xx = 0
            self._turn_ok = self._turn_fail = 0
            self._started_at = time.time()

    # -- lifecycle ----------------------------------------------------------
    def install(self) -> None:
        """Attach the logging net to the ``api`` logger. Sync — safe to call
        before the event loop exists (e.g. early in lifespan)."""
        if _DISABLE or self._handler is not None:
            return
        self._handler = _MetricsLogHandler(self)
        logging.getLogger("api").addHandler(self._handler)

    def uninstall(self) -> None:
        if self._handler is not None:
            logging.getLogger("api").removeHandler(self._handler)
            self._handler = None

    async def start_batcher(self) -> None:
        if _DISABLE:
            return
        await self._err_batcher.start()
        await self._op_batcher.start()

    async def stop(self) -> None:
        await self._err_batcher.stop()
        await self._op_batcher.stop()
        self.uninstall()


def _pct(sorted_samples, p: float):
    if not sorted_samples:
        return None
    i = min(len(sorted_samples) - 1, int(len(sorted_samples) * p))
    return round(sorted_samples[i], 1)


class _MetricsLogHandler(logging.Handler):
    """Safety net: record any WARNING+ ``api.*`` log that wasn't already
    captured by an explicit ``record_error`` call (checked via the exception
    sentinel). Category/provider are inferred from the logger name."""

    def __init__(self, registry: "MetricsRegistry") -> None:
        super().__init__(level=logging.WARNING)
        self._reg = registry

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
            self._reg.record_error(
                exc, category=category, provider=provider,
                message=record.getMessage(), logger=name, level=record.levelname,
            )
        except Exception:
            # A logging handler must never raise — it would break the very
            # log call that triggered it.
            pass


# Module singleton — one registry per worker process. Constructed eagerly
# (no I/O); the batcher tasks start and the handler installs from the
# FastAPI lifespan.
_METRICS = MetricsRegistry()


def get_metrics() -> MetricsRegistry:
    return _METRICS
