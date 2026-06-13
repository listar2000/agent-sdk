"""Tests for the error/metrics reporter (``api.metrics``).

Unit tests (no DB) cover the capture invariants: exactly-once-per-exception
dedup, secret redaction, and the logging-net's logger-name inference +
sentinel-skip. The behavioral test drives a real 500 through the ASGI stack
and asserts the failure lands in BOTH ``/metrics`` (live) and the
``error_events`` table (durable) — the end-to-end wiring.
"""
from __future__ import annotations

import logging
import os
import uuid
from unittest.mock import patch

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from api.metrics import MetricsRegistry, _category_for_logger


# ---------------------------------------------------------------------------
# Unit — capture invariants (no DB, no server)
# ---------------------------------------------------------------------------

def test_record_error_dedups_per_exception_instance():
    """The same exception bubbling through timed_phase -> route -> HTTP
    handler must count once, not N times."""
    reg = MetricsRegistry()
    exc = RuntimeError("boom")
    reg.record_error(exc, category="provider", provider="daytona", http_status=502)
    reg.record_error(exc, category="provider", provider="daytona", http_status=502)
    reg.record_error(exc, category="http_5xx", http_status=500)  # different category, same obj
    snap = reg.snapshot()
    assert snap["errors"]["total"] == 1
    assert snap["errors"]["by_category"] == {"provider": 1}


def test_distinct_exceptions_count_separately():
    reg = MetricsRegistry()
    for i in range(3):
        reg.record_error(RuntimeError(f"boom {i}"), category="turn")
    assert reg.snapshot()["errors"]["by_category"]["turn"] == 3


def test_message_is_redacted_before_storage():
    reg = MetricsRegistry()
    token = "sk-ant-" + "A" * 40
    reg.record_error(RuntimeError(f"auth failed with {token}"), category="acp")
    msg = reg.snapshot()["recent"][0]["message"]
    assert token not in msg
    assert "[REDACTED]" in msg


def test_dedup_window_throttles_ring_but_not_counters():
    """Identical repeats inside the dedup window still increment counters
    (accurate totals) but are not re-inserted into the ring / DB (no flood)."""
    reg = MetricsRegistry()
    # Distinct exception objects so the per-instance sentinel doesn't apply,
    # but identical (category, provider, type, message) so the window does.
    for _ in range(5):
        reg.record_error(RuntimeError("same message"), category="pool")
    snap = reg.snapshot()
    assert snap["errors"]["by_category"]["pool"] == 5   # counted every time
    assert len(snap["recent"]) == 1                      # ring throttled


@pytest.mark.parametrize("logger_name,expected_cat,expected_prov", [
    ("api.providers.daytona.session", "provider", "daytona"),
    ("api.providers.modal", "provider", "modal"),
    ("api.sandbox.pool", "pool", None),
    ("api.sandbox.session", "sandbox", None),
    ("api.acp_client", "acp", None),
    ("api.turn", "turn", None),
    ("api.server", "logged", None),
])
def test_category_inferred_from_logger_name(logger_name, expected_cat, expected_prov):
    cat, prov = _category_for_logger(logger_name)
    assert (cat, prov) == (expected_cat, expected_prov)


def test_logging_net_captures_bare_warnings_and_excludes_timing():
    """A best-effort log.warning that never re-raises (daytona reattach
    abandon) is captured with provider inferred; api.timing slow-request
    lines are NOT (they're perf, recorded separately)."""
    reg = MetricsRegistry()
    reg.install()
    try:
        logging.getLogger("api.providers.daytona.session").warning(
            "reattach to ws-1 failed (gone); destroying + cold-creating")
        logging.getLogger("api.timing").warning("[r0] GET /x 500 900.0ms")
    finally:
        reg.uninstall()
    snap = reg.snapshot()
    assert snap["errors"]["by_provider"].get("daytona") == 1
    assert snap["errors"]["by_category"].get("reattach") == 1   # message-refined
    assert "http_5xx" not in snap["errors"]["by_category"]       # timing excluded


def test_logging_net_skips_already_recorded_exception():
    """An exception recorded explicitly (e.g. by the turn path) must not be
    double-counted when its log.exception also hits the net."""
    reg = MetricsRegistry()
    reg.install()
    try:
        exc = RuntimeError("turn failed")
        reg.record_error(exc, category="turn", session_id="s-1")
        logging.getLogger("api.turn").exception("execute_prompt failed", exc_info=exc)
    finally:
        reg.uninstall()
    assert reg.snapshot()["errors"]["total"] == 1


def test_record_recovery_is_tracked_separately_from_errors():
    """Silent recoveries are a distinct signal — they must NOT inflate the
    error total (otherwise an auto-heal looks like a failure)."""
    reg = MetricsRegistry()
    reg.record_recovery("cold_recover", provider="daytona", session_id="s1")
    reg.record_recovery("mid_turn_swap", session_id="s2")
    snap = reg.snapshot()
    assert snap["recoveries"]["total"] == 2
    assert snap["recoveries"]["by_kind"] == {"cold_recover": 1, "mid_turn_swap": 1}
    assert snap["recoveries"]["by_provider"] == {"daytona": 1}
    assert snap["errors"]["total"] == 0  # recoveries are not errors


def test_record_leak_tracks_provider_and_marks_exception_seen():
    reg = MetricsRegistry()
    exc = RuntimeError("destroy timed out")
    reg.record_leak("destroy_failed", provider="modal", session_id="s3", exc=exc)
    snap = reg.snapshot()
    assert snap["leaks"]["total"] == 1
    assert snap["leaks"]["by_kind"]["destroy_failed"] == 1
    assert snap["leaks"]["by_provider"] == {"modal": 1}
    # The exc is stamped so the logging net (which also sees the log.exception
    # at the leak site) won't double-count it as a generic error.
    assert getattr(exc, "__asdk_metric_seen__", False) is True


@pytest.mark.asyncio
async def test_timed_op_records_per_provider_latency_and_flakiness():
    reg = MetricsRegistry()
    async def _noop():
        return None
    async with reg.timed_op(provider="daytona", operation="cold_create"):
        await _noop()
    with pytest.raises(TimeoutError):
        async with reg.timed_op(provider="modal", operation="cold_create"):
            raise TimeoutError("modal slow")
    ops = reg.snapshot()["ops"]
    assert ops["cold_create@daytona"]["fail_rate"] == 0.0
    assert ops["cold_create@modal"]["fail_rate"] == 1.0
    assert ops["cold_create@daytona"]["p50"] is not None


def test_phase_timing_is_bucketed_per_provider():
    reg = MetricsRegistry()
    reg.record_phase(name="sessions.cold_create", duration_ms=15000, ok=True, provider="daytona")
    reg.record_phase(name="sessions.cold_create", duration_ms=8000, ok=False, provider="modal")
    phases = reg.snapshot()["phases"]
    assert "sessions.cold_create@daytona" in phases
    assert phases["sessions.cold_create@modal"]["fail"] == 1


def test_perf_signals_in_snapshot():
    reg = MetricsRegistry()
    reg.record_request(path="/sessions", status=200, duration_ms=10.0)
    reg.record_request(path="/sessions", status=502, duration_ms=30.0)
    reg.record_request(path="/x", status="ERR", duration_ms=5.0)
    reg.record_phase(name="sessions.cold_create", duration_ms=15000.0, ok=False)
    reg.record_turn(True)
    reg.record_turn(False)
    snap = reg.snapshot()
    assert snap["requests"]["total"] == 3
    assert snap["requests"]["errors_5xx"] == 2          # 502 + ERR
    assert snap["phases"]["sessions.cold_create"]["fail"] == 1
    assert snap["turns"] == {"ok": 1, "fail": 1}


# ---------------------------------------------------------------------------
# Behavioral — a real 500 through the ASGI stack lands in /metrics AND the DB
# ---------------------------------------------------------------------------

_DB = os.environ.get("TEST_DATABASE_URL")


@pytest.mark.skipif(_DB is None, reason="TEST_DATABASE_URL not set")
@pytest.mark.xdist_group("db")
@pytest.mark.asyncio
async def test_http_500_is_captured_live_and_durably(clean_db):
    if _DB:
        os.environ["DATABASE_URL"] = _DB
    from api import db as dbmod
    from api import server as srv
    from api.metrics import get_metrics

    reg = get_metrics()
    reg.reset()
    reg._err_batcher._pending.clear()  # drop rows queued by earlier (unrun-batcher) tests

    transport = ASGITransport(app=srv.app, raise_app_exceptions=False)
    # Force GET /health to 500 by making its pool lookup raise — exercises the
    # global Exception handler + the request-timing middleware's ERR path.
    with patch("api.sandbox.get_pool", side_effect=RuntimeError("induced boom")):
        async with AsyncClient(transport=transport, base_url="http://t") as c:
            resp = await c.get("/health")
    assert resp.status_code == 500

    # 1) Live, in-memory snapshot (this replica)
    snap = reg.snapshot()
    assert snap["errors"]["by_category"].get("http_5xx", 0) >= 1
    assert snap["requests"]["errors_5xx"] >= 1
    assert any("induced boom" in (r["message"] or "") for r in snap["recent"])

    # 2) Durable — flush the batcher by hand (lifespan didn't start it) and
    #    confirm the row reached Postgres, queryable via the admin route's fn.
    await reg._err_batcher._flush_once()
    events = await dbmod.get_error_events(category="http_5xx", limit=10)
    assert any(e["http_status"] == 500 and "induced boom" in (e["message"] or "")
               for e in events), events


@pytest.mark.skipif(_DB is None, reason="TEST_DATABASE_URL not set")
@pytest.mark.xdist_group("db")
@pytest.mark.asyncio
async def test_admin_errors_endpoint_filters_by_category(clean_db):
    if _DB:
        os.environ["DATABASE_URL"] = _DB
    from api import server as srv
    from api.metrics import get_metrics

    reg = get_metrics()
    reg.reset()
    reg._err_batcher._pending.clear()
    # Seed two categories straight through the reporter, then flush.
    reg.record_error(RuntimeError("prov fail " + uuid.uuid4().hex), category="provider", provider="daytona", http_status=502)
    reg.record_error(RuntimeError("turn fail " + uuid.uuid4().hex), category="turn", session_id="s-x")
    await reg._err_batcher._flush_once()

    transport = ASGITransport(app=srv.app, raise_app_exceptions=False)
    async with AsyncClient(transport=transport, base_url="http://t") as c:
        all_resp = (await c.get("/admin/errors")).json()
        prov_resp = (await c.get("/admin/errors?provider=daytona")).json()

    assert all_resp["by_category"].get("provider", 0) >= 1
    assert all_resp["by_category"].get("turn", 0) >= 1
    assert all(e["provider"] == "daytona" for e in prov_resp["events"])
    assert prov_resp["events"], "provider filter returned nothing"


@pytest.mark.skipif(_DB is None, reason="TEST_DATABASE_URL not set")
@pytest.mark.xdist_group("db")
@pytest.mark.asyncio
async def test_op_stats_durable_and_admin_ops_endpoint(clean_db):
    if _DB:
        os.environ["DATABASE_URL"] = _DB
    from api import server as srv
    from api.metrics import get_metrics

    reg = get_metrics()
    reg.reset()
    reg._op_batcher._pending.clear()
    # Two daytona cold_creates (one slow, one failed) + one modal success.
    async def _sleep_ms(ms):
        import asyncio
        await asyncio.sleep(ms / 1000)
    async with reg.timed_op(provider="daytona", operation="cold_create"):
        await _sleep_ms(8)
    with pytest.raises(RuntimeError):
        async with reg.timed_op(provider="daytona", operation="cold_create"):
            raise RuntimeError("daytona create failed")
    async with reg.timed_op(provider="modal", operation="cold_create"):
        await _sleep_ms(3)
    await reg._op_batcher._flush_once()

    transport = ASGITransport(app=srv.app, raise_app_exceptions=False)
    async with AsyncClient(transport=transport, base_url="http://t") as c:
        body = (await c.get("/admin/ops")).json()
    stats = {(o["provider"], o["operation"]): o for o in body["ops"]}
    dt = stats[("daytona", "cold_create")]
    assert dt["count"] == 2 and dt["fails"] == 1
    assert dt["fail_rate"] == 0.5
    assert dt["p50_ms"] is not None
    assert stats[("modal", "cold_create")]["fail_rate"] == 0.0
