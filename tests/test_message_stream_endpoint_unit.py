"""Phase 2 sub-task 3c: unit test for POST /sessions/{id}/message+stream.

Mocks the pool so this stays a unit test (no daytona). Verifies:
  * 400 on missing ``message`` body
  * Routes through pool.get_session
  * Streams events from session.execute_prompt as SSE ``data: ...``
  * stopReason ends the stream
"""
from __future__ import annotations

import json
import os
import sys
from collections.abc import AsyncIterator
from typing import Any

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from unittest.mock import AsyncMock, MagicMock, patch

_SRC = os.path.join(os.path.dirname(__file__), "..", "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

_DB = os.environ.get("TEST_DATABASE_URL")
pytestmark = pytest.mark.skipif(_DB is None, reason="TEST_DATABASE_URL not set")
if _DB:
    os.environ["DATABASE_URL"] = _DB

from api import server as srv  # noqa: E402


@pytest_asyncio.fixture
async def setup(clean_db):
    yield


def _parse_sse(body: str) -> list[dict[str, Any]]:
    """Parse a text/event-stream body into a list of decoded events."""
    out: list[dict[str, Any]] = []
    for block in body.split("\n\n"):
        block = block.strip()
        if not block:
            continue
        for line in block.splitlines():
            if line.startswith("data: "):
                out.append(json.loads(line[6:]))
    return out


@pytest.mark.asyncio
async def test_message_stream_returns_400_when_message_missing(setup):
    async with AsyncClient(
        transport=ASGITransport(app=srv.app),
        base_url="http://test",
    ) as client:
        resp = await client.post("/sessions/s1/message+stream", json={})
    assert resp.status_code == 400


@pytest.mark.asyncio
async def test_message_stream_routes_through_pool_and_streams_events(setup):
    """Mock pool.get_session returning a fake session whose
    execute_prompt yields three events; the endpoint must stream all
    three as SSE data blocks."""

    events_yielded = [
        {"type": "text", "text": "Hi"},
        {"type": "text", "text": "!"},
        {"type": "done", "stop_reason": "end_turn"},
    ]

    async def fake_execute_prompt(message: str) -> AsyncIterator[Any]:
        for ev in events_yielded:
            yield ev

    fake_session = MagicMock()
    fake_session.execute_prompt = fake_execute_prompt

    fake_pool = MagicMock()
    fake_pool.get_session = AsyncMock(return_value=fake_session)

    with patch("api.sandbox.get_pool", return_value=fake_pool), \
         patch("api.sandbox.runtime.get_pool", return_value=fake_pool):
        async with AsyncClient(
            transport=ASGITransport(app=srv.app),
            base_url="http://test",
        ) as client:
            resp = await client.post(
                "/sessions/s1/message+stream",
                json={"message": "hello"},
            )

    assert resp.status_code == 200
    assert resp.headers["content-type"].startswith("text/event-stream")
    received = _parse_sse(resp.text)
    assert received == events_yielded
    fake_pool.get_session.assert_awaited_once_with("s1")


@pytest.mark.asyncio
async def test_message_stream_emits_error_event_when_pool_fails(setup):
    """If pool.get_session raises, the endpoint surfaces the failure as
    an SSE ``{type:'error', error:{...}}`` event and closes the stream
    cleanly — never propagates an exception across the response boundary."""
    fake_pool = MagicMock()
    fake_pool.get_session = AsyncMock(side_effect=RuntimeError("provision failed"))

    with patch("api.sandbox.get_pool", return_value=fake_pool), \
         patch("api.sandbox.runtime.get_pool", return_value=fake_pool):
        async with AsyncClient(
            transport=ASGITransport(app=srv.app),
            base_url="http://test",
        ) as client:
            resp = await client.post(
                "/sessions/s1/message+stream",
                json={"message": "hello"},
            )

    assert resp.status_code == 200
    events = _parse_sse(resp.text)
    assert len(events) == 1
    assert events[0]["type"] == "error"
    assert events[0]["error"]["exception_type"] == "RuntimeError"
    assert "provision failed" in events[0]["error"]["message"]
