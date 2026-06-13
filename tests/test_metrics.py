"""Tests for the Postgres-backed metrics reporter (``api.metrics``).

Unit tests (no DB) cover the pure row builders + logging-net inference.
DB tests cover the full path: ``record_*`` writes a row, and the reads
(``get_metrics_summary``, ``/metrics``, ``/admin/errors``, ``/admin/ops``)
compute from Postgres — so the result is replica-independent.
"""
from __future__ import annotations

import logging
import os
import uuid
from unittest.mock import patch

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from api.metrics import (
    MetricsReporter, _build_error_row, _build_op_row, _category_for_logger,
    timed_provider_op, get_metrics,
)

_SENTINEL = "__asdk_metric_seen__"


def _err_row(exc=None, **kw):
    base = dict(category="provider", provider=None, session_id=None, agent_id=None,
                http_status=None, phase=None, message=None, context={})
    base.update(kw)
    return _build_error_row(exc, **base)


# ---------------------------------------------------------------------------
# Unit — pure builders + net inference (no DB, no server)
# ---------------------------------------------------------------------------

def test_error_row_redacts_message():
    token = "sk-ant-" + "A" * 40
    row = _err_row(message=f"auth failed with {token}", category="acp")
    assert token not in row["message"] and "[REDACTED]" in row["message"]


def test_error_row_sentinel_is_exactly_once():
    exc = RuntimeError("boom")
    assert _err_row(exc, category="provider") is not None   # first: builds a row
    assert _err_row(exc, category="http_5xx") is None        # second: deduped
    assert getattr(exc, _SENTINEL) is True


def test_error_row_refines_reattach_category():
    row = _err_row(category="provider", provider="daytona",
                   message="reattach to ws-1 failed; destroying + cold-creating")
    assert row["category"] == "reattach"


def test_op_row_shape():
    row = _build_op_row(provider="modal", operation="sdk.create_sandbox",
                        duration_ms=12.345, ok=False, session_id="s", error="boom")
    assert row["operation"] == "sdk.create_sandbox"
    assert row["ok"] is False and row["duration_ms"] == 12.35


@pytest.mark.parametrize("logger_name,cat,prov", [
    ("api.providers.daytona.session", "provider", "daytona"),
    ("api.providers.modal", "provider", "modal"),
    ("api.sandbox.pool", "pool", None),
    ("api.sandbox.orphan_monitor", "orphan", None),
    ("api.acp_client", "acp", None),
    ("api.turn", "turn", None),
])
def test_category_inferred_from_logger_name(logger_name, cat, prov):
    assert _category_for_logger(logger_name) == (cat, prov)


def test_logging_net_infers_provider_and_excludes_timing(monkeypatch):
    """The net captures best-effort ``api.*`` WARNING+ logs (inferring
    category/provider from the logger), and ignores ``api.timing``."""
    r = MetricsReporter()
    captured = []
    monkeypatch.setattr(r, "_record_from_net", lambda exc, **kw: captured.append(kw))
    r.install()
    try:
        logging.getLogger("api.providers.daytona.session").warning(
            "reattach to ws-1 failed; destroying + cold-creating")
        logging.getLogger("api.timing").warning("[r0] GET /x 500 900ms")  # excluded
    finally:
        r.uninstall()
    assert len(captured) == 1
    assert captured[0]["provider"] == "daytona"
    assert captured[0]["category"] == "provider"
    assert "reattach" in captured[0]["message"]


def test_logging_net_skips_already_recorded_exception(monkeypatch):
    r = MetricsReporter()
    captured = []
    monkeypatch.setattr(r, "_record_from_net", lambda exc, **kw: captured.append(kw))
    r.install()
    try:
        exc = RuntimeError("turn failed")
        setattr(exc, _SENTINEL, True)  # an explicit path already recorded it
        logging.getLogger("api.turn").exception("execute_prompt failed", exc_info=exc)
    finally:
        r.uninstall()
    assert captured == []


# ---------------------------------------------------------------------------
# DB — write a row, read it back from Postgres
# ---------------------------------------------------------------------------

_DB = os.environ.get("TEST_DATABASE_URL")
if _DB:
    os.environ["DATABASE_URL"] = _DB


@pytest.mark.skipif(_DB is None, reason="TEST_DATABASE_URL not set")
@pytest.mark.xdist_group("db")
@pytest.mark.asyncio
async def test_record_writes_rows_and_summary_reads_from_postgres(clean_db):
    from api import db
    r = MetricsReporter()
    await r.record_error(RuntimeError("boom prov"), category="provider", provider="daytona", http_status=502)
    await r.record_recovery("cold_recover", provider="daytona", session_id="s1")
    await r.record_recovery("mid_turn_swap", session_id="s2")
    await r.record_leak("orphan_detected", provider="daytona", count=7)
    async with r.timed_op(provider="daytona", operation="cold_create"):
        pass
    with pytest.raises(RuntimeError):
        async with r.timed_op(provider="modal", operation="cold_create"):
            raise RuntimeError("modal create failed")

    summary = await db.get_metrics_summary(since_s=None)
    # errors exclude recoveries/leaks
    assert summary["errors"]["by_category"].get("provider") == 1
    assert summary["errors"]["total"] == 1
    # recoveries tracked separately
    assert summary["recoveries"]["total"] == 2
    assert summary["recoveries"]["by_kind"] == {"cold_recover": 1, "mid_turn_swap": 1}
    # leaks
    assert summary["leaks"]["by_kind"].get("orphan_detected") == 1
    # ops per-provider flakiness
    ops = {(o["provider"], o["operation"]): o for o in summary["ops"]}
    assert ops[("daytona", "cold_create")]["fail_rate"] == 0.0
    assert ops[("modal", "cold_create")]["fail_rate"] == 1.0


@pytest.mark.skipif(_DB is None, reason="TEST_DATABASE_URL not set")
@pytest.mark.xdist_group("db")
@pytest.mark.asyncio
async def test_record_error_dedups_per_exception_in_db(clean_db):
    from api import db
    r = MetricsReporter()
    exc = RuntimeError("the same boom")
    await r.record_error(exc, category="provider", provider="daytona")
    await r.record_error(exc, category="http_5xx")  # same instance -> no second row
    rows = await db.get_error_events(limit=50)
    assert len([e for e in rows if "the same boom" in (e["message"] or "")]) == 1


@pytest.mark.skipif(_DB is None, reason="TEST_DATABASE_URL not set")
@pytest.mark.xdist_group("db")
@pytest.mark.asyncio
async def test_timed_provider_op_decorator_records_sdk_op(clean_db):
    from api import db
    get_metrics()  # ensure singleton exists; decorator uses it

    @timed_provider_op("daytona", "create_sandbox")
    async def create(x):
        return x * 2

    assert create.__name__ == "create"
    assert await create(21) == 42
    ops = {(o["provider"], o["operation"]): o for o in await db.get_op_stats()}
    assert ops[("daytona", "sdk.create_sandbox")]["count"] == 1


# ---------------------------------------------------------------------------
# DB + ASGI — endpoints compute from Postgres (replica-independent)
# ---------------------------------------------------------------------------

@pytest.mark.skipif(_DB is None, reason="TEST_DATABASE_URL not set")
@pytest.mark.xdist_group("db")
@pytest.mark.asyncio
async def test_http_500_is_captured_durably_and_metrics_endpoint(clean_db):
    from api import server as srv
    transport = ASGITransport(app=srv.app, raise_app_exceptions=False)
    # Force GET /health to 500; the async handler awaits the durable insert
    # before returning, so the row is queryable immediately after.
    with patch("api.sandbox.get_pool", side_effect=RuntimeError("induced boom")):
        async with AsyncClient(transport=transport, base_url="http://t") as c:
            resp = await c.get("/health")
            assert resp.status_code == 500
            metrics = (await c.get("/metrics?window_s=0")).json()
            admin = (await c.get("/admin/errors?category=http_5xx")).json()

    assert metrics["errors"]["by_category"].get("http_5xx", 0) >= 1
    assert any("induced boom" in (e["message"] or "") for e in admin["events"])


@pytest.mark.skipif(_DB is None, reason="TEST_DATABASE_URL not set")
@pytest.mark.xdist_group("db")
@pytest.mark.asyncio
async def test_admin_ops_endpoint(clean_db):
    from api import server as srv
    r = MetricsReporter()
    await r.record_op(provider="daytona", operation="cold_create", duration_ms=8.0, ok=True)
    await r.record_op(provider="daytona", operation="cold_create", duration_ms=30.0, ok=False, error="x")
    transport = ASGITransport(app=srv.app, raise_app_exceptions=False)
    async with AsyncClient(transport=transport, base_url="http://t") as c:
        body = (await c.get("/admin/ops")).json()
    dt = {(o["provider"], o["operation"]): o for o in body["ops"]}[("daytona", "cold_create")]
    assert dt["count"] == 2 and dt["fails"] == 1 and dt["fail_rate"] == 0.5
