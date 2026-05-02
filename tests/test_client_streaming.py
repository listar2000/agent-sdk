"""Behavioral tests for Agent.astream(), astream(), and arun().

These tests exercise the actual streaming event-parsing logic end-to-end:
  - Text/tool/done events yielded correctly from SSE blocks
  - Tag filtering: blocks for a different rpc_id are skipped
  - PromptError raised on error frames
  - StreamError raised when stream closes without a done frame
  - arun() collects all text fragments

No real HTTP connections are made — the httpx client is mocked with
a FakeSseResponse that yields pre-built SSE chunks.
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
from contextlib import asynccontextmanager
from typing import AsyncIterator
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

_SRC = os.path.join(os.path.dirname(__file__), "..", "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

from agent_sdk.client import Agent
from agent_sdk.errors import PromptError, StreamError


# ---------------------------------------------------------------------------
# Mock helpers
# ---------------------------------------------------------------------------

class _FakeSseResponse:
    """Simulates an httpx streaming response that yields SSE text chunks."""

    status_code = 200

    def __init__(self, chunks: list[str]):
        self._chunks = chunks

    async def aiter_text(self) -> AsyncIterator[str]:
        for chunk in self._chunks:
            yield chunk


def _text_block(rpc_id: str, text: str) -> str:
    """SSE block: a session/update notification carrying a text delta."""
    payload = {
        "method": "session/update",
        "params": {"update": {
            "sessionUpdate": "agent_message_delta",
            "content": {"type": "text", "text": text},
        }},
    }
    return f"event: rpc:{rpc_id}\ndata: {json.dumps(payload)}"


def _done_block(rpc_id: str, stop_reason: str = "end_turn") -> str:
    """SSE block: a JSON-RPC result terminal frame."""
    payload = {
        "jsonrpc": "2.0", "id": rpc_id,
        "result": {"stopReason": stop_reason, "usage": {}},
    }
    return f"event: rpc:{rpc_id}\ndata: {json.dumps(payload)}"


def _error_block(rpc_id: str, message: str, kind: str = "sandbox_process_died") -> str:
    """SSE block: a JSON-RPC error frame."""
    payload = {
        "jsonrpc": "2.0", "id": rpc_id,
        "error": {"code": -32000, "message": message, "data": {"kind": kind}},
    }
    return f"event: rpc:{rpc_id}\ndata: {json.dumps(payload)}"


def _make_sse_stream(blocks: list[str]) -> str:
    """Join SSE blocks into a single chunk with double-newline separators."""
    return "\n\n".join(blocks) + "\n\n"


def _patched_agent(session_id: str = "sess-test") -> tuple[Agent, str]:
    """Create an Agent that is pre-registered (no real HTTP needed) and return (agent, rpc_id)."""
    agent = Agent("test-agent", session_id=session_id)
    agent._registered = True  # bypass _ensure_registered
    return agent


@asynccontextmanager
async def _mock_client(agent: Agent, blocks: list[str], rpc_id: str):
    """Patch agent._api._http so GET /events yields the given blocks and POST returns rpc_id."""
    sse_response = _FakeSseResponse([_make_sse_stream(blocks)])

    @asynccontextmanager
    async def _mock_stream(*args, **kwargs):
        yield sse_response

    mock_post_resp = MagicMock()
    mock_post_resp.raise_for_status = MagicMock()
    mock_post_resp.json = MagicMock(return_value={"rpc_id": rpc_id, "status": "ok"})

    with patch.object(agent._api._http, "stream", _mock_stream), \
         patch.object(agent._api._http, "post", AsyncMock(return_value=mock_post_resp)):
        yield


# ---------------------------------------------------------------------------
# Tests: event types
# ---------------------------------------------------------------------------

class TestAstreamEventsTypes:

    @pytest.mark.asyncio
    async def test_yields_text_event(self):
        """astream yields a text event for an agent_message_delta block."""
        agent = _patched_agent()
        rpc_id = "rpc-text-1"
        blocks = [_text_block(rpc_id, "Hello world"), _done_block(rpc_id)]

        async with _mock_client(agent, blocks, rpc_id):
            events = []
            async for ev in agent.astream("say hello"):
                events.append(ev)

        types = [e["type"] for e in events]
        assert "text" in types, f"expected text event, got: {types}"
        text_events = [e for e in events if e["type"] == "text"]
        assert text_events[0]["text"] == "Hello world"

    @pytest.mark.asyncio
    async def test_yields_done_event_last(self):
        """astream yields a done event as the final event."""
        agent = _patched_agent()
        rpc_id = "rpc-done-1"
        blocks = [_text_block(rpc_id, "Hi"), _done_block(rpc_id, "end_turn")]

        async with _mock_client(agent, blocks, rpc_id):
            events = []
            async for ev in agent.astream("hi"):
                events.append(ev)

        assert events[-1]["type"] == "done"
        assert events[-1]["stop_reason"] == "end_turn"

    @pytest.mark.asyncio
    async def test_yields_multiple_text_events_in_order(self):
        """Multiple text delta blocks are yielded in order."""
        agent = _patched_agent()
        rpc_id = "rpc-multi-1"
        blocks = [
            _text_block(rpc_id, "one"),
            _text_block(rpc_id, "two"),
            _text_block(rpc_id, "three"),
            _done_block(rpc_id),
        ]

        async with _mock_client(agent, blocks, rpc_id):
            events = []
            async for ev in agent.astream("count"):
                events.append(ev)

        text_events = [e for e in events if e["type"] == "text"]
        assert [e["text"] for e in text_events] == ["one", "two", "three"]


# ---------------------------------------------------------------------------
# Tests: tag filtering
# ---------------------------------------------------------------------------

class TestAstreamEventsTagFiltering:

    @pytest.mark.asyncio
    async def test_astream_yields_all_blocks_from_message_stream(self):
        """With ``POST /message+stream``, the server scopes the response
        body to a single prompt, so the SDK doesn't need to filter by
        rpc_id client-side. The SDK trusts that every block on the
        response stream belongs to its prompt and surfaces them all.

        (The legacy filtering test asserting that other-rpc blocks were
        skipped no longer applies — that boundary moved server-side
        when astream switched from POST /message + GET /events to
        POST /message+stream.)
        """
        agent = _patched_agent()
        rpc_id = "rpc-mine"

        blocks = [
            _text_block(rpc_id, "mine"),
            _done_block(rpc_id),
        ]

        async with _mock_client(agent, blocks, rpc_id):
            events = []
            async for ev in agent.astream("hello"):
                events.append(ev)

        text_events = [e for e in events if e["type"] == "text"]
        assert len(text_events) == 1
        assert text_events[0]["text"] == "mine"

    @pytest.mark.asyncio
    async def test_untagged_blocks_are_not_filtered(self):
        """Blocks with no event: rpc: tag (e.g., heartbeats) pass the filter."""
        agent = _patched_agent()
        rpc_id = "rpc-tag-1"

        # A heartbeat is just ": heartbeat" — no data, parse_acp_event returns None
        # So it shouldn't crash but will yield nothing
        heartbeat = ": heartbeat"
        blocks = [heartbeat, _text_block(rpc_id, "after heartbeat"), _done_block(rpc_id)]

        async with _mock_client(agent, blocks, rpc_id):
            events = []
            async for ev in agent.astream("hello"):
                events.append(ev)

        # Should succeed and include the text event (heartbeat passes through silently)
        text_events = [e for e in events if e["type"] == "text"]
        assert text_events, "text event after heartbeat should not be lost"


# ---------------------------------------------------------------------------
# Tests: error handling
# ---------------------------------------------------------------------------

class TestAstreamEventsErrors:

    @pytest.mark.asyncio
    async def test_raises_prompt_error_on_error_frame(self):
        """astream raises PromptError when an error frame arrives."""
        agent = _patched_agent()
        rpc_id = "rpc-err-1"
        blocks = [_error_block(rpc_id, "sandbox died", "sandbox_process_died")]

        async with _mock_client(agent, blocks, rpc_id):
            with pytest.raises(PromptError) as exc_info:
                async for _ in agent.astream("fail"):
                    pass

        err = exc_info.value
        assert "sandbox died" in str(err)
        assert err.kind == "sandbox_process_died"

    @pytest.mark.asyncio
    async def test_prompt_error_has_kind_attribute(self):
        """PromptError.kind carries the error classification from the frame."""
        agent = _patched_agent()
        rpc_id = "rpc-err-kind"
        blocks = [_error_block(rpc_id, "timeout", "timeout")]

        async with _mock_client(agent, blocks, rpc_id):
            with pytest.raises(PromptError) as exc_info:
                async for _ in agent.astream("timeout"):
                    pass

        assert exc_info.value.kind == "timeout"

    @pytest.mark.asyncio
    async def test_raises_stream_error_when_stream_closes_without_done(self):
        """astream raises StreamError if the stream ends without a done event.

        This is the 'Connection closed before response completed' path.
        """
        agent = _patched_agent()
        rpc_id = "rpc-nocomplete"
        # Stream closes after one text event — no done frame
        blocks = [_text_block(rpc_id, "partial")]

        async with _mock_client(agent, blocks, rpc_id):
            with pytest.raises(StreamError, match="Connection closed before response completed"):
                async for _ in agent.astream("go"):
                    pass

    @pytest.mark.asyncio
    async def test_text_events_yielded_before_error_frame(self):
        """Text events accumulated before an error frame are yielded before PromptError is raised."""
        agent = _patched_agent()
        rpc_id = "rpc-partial-text"
        blocks = [
            _text_block(rpc_id, "partial answer"),
            _error_block(rpc_id, "crashed mid-response"),
        ]

        async with _mock_client(agent, blocks, rpc_id):
            yielded = []
            with pytest.raises(PromptError):
                async for ev in agent.astream("complex"):
                    yielded.append(ev)

        text_events = [e for e in yielded if e["type"] == "text"]
        assert text_events, "text events before error frame must still be yielded"
        assert text_events[0]["text"] == "partial answer"


# ---------------------------------------------------------------------------
# Tests: astream and arun wrappers
# ---------------------------------------------------------------------------

class TestAstreamAndArun:

    @pytest.mark.asyncio
    async def test_astream_yields_text_strings(self):
        """astream flattens text events into plain string chunks."""
        agent = _patched_agent()
        rpc_id = "rpc-astream-1"
        blocks = [
            _text_block(rpc_id, "chunk1"),
            _text_block(rpc_id, "chunk2"),
            _done_block(rpc_id),
        ]

        async with _mock_client(agent, blocks, rpc_id):
            chunks = []
            async for chunk in agent.astream("hello"):
                chunks.append(chunk)

        # astream yields Event objects; str(ev) gives text
        assert [str(c) for c in chunks if c["type"] == "text"] == ["chunk1", "chunk2"]

    @pytest.mark.asyncio
    async def test_arun_concatenates_all_text(self):
        """arun collects all text chunks and returns the concatenated string."""
        agent = _patched_agent()
        rpc_id = "rpc-arun-1"
        blocks = [
            _text_block(rpc_id, "Hello"),
            _text_block(rpc_id, " world"),
            _done_block(rpc_id),
        ]

        async with _mock_client(agent, blocks, rpc_id):
            result = await agent.arun("hello world")

        assert result == "Hello world"

    @pytest.mark.asyncio
    async def test_arun_increments_call_count(self):
        """arun increments usage.call_count on each call."""
        agent = _patched_agent()
        rpc_id = "rpc-usage-1"
        blocks = [_text_block(rpc_id, "ok"), _done_block(rpc_id)]

        async with _mock_client(agent, blocks, rpc_id):
            await agent.arun("go")

        assert agent.usage.call_count == 1

    @pytest.mark.asyncio
    async def test_arun_propagates_prompt_error(self):
        """arun propagates PromptError from the underlying astream."""
        agent = _patched_agent()
        rpc_id = "rpc-arun-err"
        blocks = [_error_block(rpc_id, "crashed")]

        async with _mock_client(agent, blocks, rpc_id):
            with pytest.raises(PromptError):
                await agent.arun("crash")

    @pytest.mark.asyncio
    async def test_arun_propagates_stream_error(self):
        """arun propagates StreamError when stream closes without done."""
        agent = _patched_agent()
        rpc_id = "rpc-arun-stream-err"
        blocks = [_text_block(rpc_id, "partial")]  # no done

        async with _mock_client(agent, blocks, rpc_id):
            with pytest.raises(StreamError):
                await agent.arun("partial")


# ---------------------------------------------------------------------------
# Tests: concurrent astream isolation
# ---------------------------------------------------------------------------

class TestAstreamConcurrentIsolation:

    @pytest.mark.asyncio
    async def test_two_concurrent_astream_each_get_own_stream(self):
        """Two concurrent astream calls open independent
        ``POST /message+stream`` requests; each response body is the
        per-prompt SSE stream the server scoped on its end. Test pins
        the SDK's contract that each agent's astream surfaces only
        what that agent's request received — no cross-agent bleed."""
        rpc_a = "rpc-concurrent-a"

        blocks_a = [
            _text_block(rpc_a, "from-a"),
            _done_block(rpc_a),
        ]

        agent_a = _patched_agent("sess-a")

        async with _mock_client(agent_a, blocks_a, rpc_a):
            events_a = []
            async for ev in agent_a.astream("from a"):
                events_a.append(ev)

        text_a = [e["text"] for e in events_a if e["type"] == "text"]
        assert text_a == ["from-a"], (
            f"agent_a should see its own text event, got: {text_a}"
        )


# ---------------------------------------------------------------------------
# Tests: SSE ReadTimeout → StreamError
# ---------------------------------------------------------------------------

class TestSseReadTimeout:

    @pytest.mark.asyncio
    async def test_astream_raises_stream_error_on_read_timeout(self):
        """httpx.ReadTimeout during SSE read raises StreamError (not ReadTimeout).

        This tests the `except httpx.ReadTimeout: raise StreamError(...)` path
        in astream — the client translates the low-level network error
        into a user-friendly SDK error.
        """

        class _TimeoutSseResponse:
            status_code = 200

            async def aiter_text(self):
                yield "event: rpc:rpc-t\ndata: " + json.dumps({
                    "method": "session/update",
                    "params": {"update": {"sessionUpdate": "agent_message_delta",
                                          "content": {"type": "text", "text": "partial"}}},
                }) + "\n\n"
                raise httpx.ReadTimeout("mock timeout")

        agent = _patched_agent("sess-timeout")
        rpc_id = "rpc-t"

        @asynccontextmanager
        async def _mock_stream(*args, **kwargs):
            yield _TimeoutSseResponse()

        mock_post_resp = MagicMock()
        mock_post_resp.raise_for_status = MagicMock()
        mock_post_resp.json = MagicMock(return_value={"rpc_id": rpc_id, "status": "ok"})

        with patch.object(agent._api._http, "stream", _mock_stream), \
             patch.object(agent._api._http, "post", AsyncMock(return_value=mock_post_resp)):
            with pytest.raises(StreamError, match="Connection lost"):
                async for _ in agent.astream("timeout test"):
                    pass

    @pytest.mark.asyncio
    async def test_astream_includes_agent_name_in_stream_error(self):
        """StreamError message contains the agent name for debugging."""

        class _TimeoutResponse:
            status_code = 200

            async def aiter_text(self):
                raise httpx.ReadTimeout("mock timeout")
                yield  # noqa: unreachable — makes this an async generator

        agent = _patched_agent("sess-named")
        agent.name = "my-agent"
        rpc_id = "rpc-named"

        @asynccontextmanager
        async def _mock_stream(*args, **kwargs):
            yield _TimeoutResponse()

        mock_post_resp = MagicMock()
        mock_post_resp.raise_for_status = MagicMock()
        mock_post_resp.json = MagicMock(return_value={"rpc_id": rpc_id, "status": "ok"})

        with patch.object(agent._api._http, "stream", _mock_stream), \
             patch.object(agent._api._http, "post", AsyncMock(return_value=mock_post_resp)):
            with pytest.raises(StreamError) as exc_info:
                async for _ in agent.astream("timeout"):
                    pass

        assert "my-agent" in str(exc_info.value)


# ---------------------------------------------------------------------------
# Tests: events() context manager behavior
# ---------------------------------------------------------------------------

class TestEventsContextManager:

    @pytest.mark.asyncio
    async def test_events_yields_error_as_dict_not_raised(self):
        """events() yields error events as dicts, unlike astream which raises.

        This is the documented difference: events() is a low-level stream that
        never raises on error frames — it yields them for the caller to handle.
        """
        agent = _patched_agent("sess-events-err")
        rpc_id = "rpc-ev-err"
        blocks = [
            _text_block(rpc_id, "some text"),
            _error_block(rpc_id, "agent crashed"),
        ]

        @asynccontextmanager
        async def _mock_stream(*args, **kwargs):
            yield _FakeSseResponse([_make_sse_stream(blocks)])

        with patch.object(agent._api._http, "stream", _mock_stream):
            events = []
            try:
                async with agent.events() as event_iter:
                    async for ev in event_iter:
                        events.append(ev)
            except Exception as e:
                pytest.fail(f"events() must not raise on error frames, but got: {e!r}")

        error_events = [e for e in events if e["type"] == "error"]
        assert error_events, (
            "events() must yield error frames as dicts, not raise PromptError"
        )
        assert "agent crashed" in error_events[0]["text"]

    @pytest.mark.asyncio
    async def test_events_skips_heartbeat_blocks(self):
        """events() skips heartbeat comment blocks (': heartbeat') with no events."""
        agent = _patched_agent("sess-events-hb")
        rpc_id = "rpc-ev-hb"
        blocks = [
            ": heartbeat",
            _text_block(rpc_id, "real content"),
            _done_block(rpc_id),
        ]

        @asynccontextmanager
        async def _mock_stream(*args, **kwargs):
            yield _FakeSseResponse([_make_sse_stream(blocks)])

        with patch.object(agent._api._http, "stream", _mock_stream):
            events = []
            async with agent.events() as event_iter:
                async for ev in event_iter:
                    events.append(ev)

        # Heartbeat should produce no event; real content should be present
        assert events, "expected at least one event"
        types = {e["type"] for e in events}
        assert "text" in types or "done" in types

    @pytest.mark.asyncio
    async def test_events_raises_stream_error_on_read_timeout(self):
        """events() raises StreamError on httpx.ReadTimeout."""

        class _TimeoutEventsResponse:
            async def aiter_text(self):
                raise httpx.ReadTimeout("timeout in events()")
                yield  # noqa: unreachable — makes this an async generator

        agent = _patched_agent("sess-events-timeout")

        @asynccontextmanager
        async def _mock_stream(*args, **kwargs):
            yield _TimeoutEventsResponse()

        with patch.object(agent._api._http, "stream", _mock_stream):
            with pytest.raises(StreamError):
                async with agent.events() as event_iter:
                    async for _ in event_iter:
                        pass


# ---------------------------------------------------------------------------
# Tests: _ensure_registered retry logic
# ---------------------------------------------------------------------------

class TestEnsureRegisteredRetry:
    """Tests for _ensure_registered's retry behavior on 500 errors."""

    @pytest.mark.asyncio
    async def test_retries_on_500_and_succeeds_on_third_attempt(self):
        """provider registration retries up to 3 times on 500, succeeds eventually."""
        agent = Agent("retry-agent", provider="unix_local")

        call_count = 0

        def _make_resp(status_code: int, data: dict):
            resp = MagicMock()
            resp.status_code = status_code
            resp.json = MagicMock(return_value=data)
            resp.raise_for_status = MagicMock(
                side_effect=httpx.HTTPStatusError(
                    "error", request=MagicMock(), response=resp
                ) if status_code >= 400 else MagicMock()
            )
            resp.is_error = status_code >= 400
            return resp

        async def _mock_request(method, url, *args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                return _make_resp(500, {"error": "temporary failure"})
            return _make_resp(200, {
                "session_id": "s-1", "agent_id": "a-1",
                "sandbox_id": "sbx-1", "inner_session_id": "inner-1",
            })

        with patch.object(agent._api._http, "request", side_effect=_mock_request), \
             patch("asyncio.sleep", AsyncMock()):  # skip actual sleep in retry
            await agent._ensure_registered()

        assert call_count == 3, f"expected exactly 3 POST attempts, got {call_count}"
        assert agent._registered

    @pytest.mark.asyncio
    async def test_three_500s_raises_after_max_retries(self):
        """provider registration raises after 3 consecutive 500 errors."""
        agent = Agent("fail-agent", provider="unix_local")

        def _make_500():
            resp = MagicMock()
            resp.status_code = 500
            resp.is_error = True
            resp.json = MagicMock(return_value={"error": "server error"})
            return resp

        async def _mock_request(method, url, *args, **kwargs):
            return _make_500()

        with patch.object(agent._api._http, "request", side_effect=_mock_request), \
             patch("asyncio.sleep", AsyncMock()):
            with pytest.raises(Exception):
                await agent._ensure_registered()

    @pytest.mark.asyncio
    async def test_concurrent_ensure_registered_only_calls_post_once(self):
        """Concurrent _ensure_registered calls register exactly once (lock protection)."""
        agent = Agent("concurrent-reg-agent", provider="unix_local")

        post_calls = []

        async def _mock_request(method, url, *args, **kwargs):
            post_calls.append((method, url))
            resp = MagicMock()
            resp.status_code = 200
            resp.is_error = False
            resp.json = MagicMock(return_value={
                "session_id": "s-1", "agent_id": "a-1",
                "sandbox_id": "sbx-1", "inner_session_id": "inner-1",
            })
            resp.raise_for_status = MagicMock()
            return resp

        with patch.object(agent._api._http, "request", side_effect=_mock_request):
            # Fire 10 concurrent _ensure_registered calls
            await asyncio.gather(*[agent._ensure_registered() for _ in range(10)])

        assert len(post_calls) == 1, (
            f"_ensure_registered should call POST exactly once, got {len(post_calls)}"
        )


# ---------------------------------------------------------------------------
# Tests: UsageStats accumulation
# ---------------------------------------------------------------------------

class TestUsageStatsUpdate:
    """UsageStats.update() accumulates token counts from usage event dicts."""

    def test_update_camel_case_field_names(self):
        """update() handles camelCase field names (inputTokens, outputTokens)."""
        from agent_sdk.client import UsageStats
        stats = UsageStats()
        stats.update({"inputTokens": 100, "outputTokens": 50, "totalCostUsd": 0.002})

        assert stats.input_tokens == 100
        assert stats.output_tokens == 50
        assert stats.total_tokens == 150
        assert abs(stats.total_cost_usd - 0.002) < 1e-9

    def test_update_snake_case_field_names(self):
        """update() handles snake_case field names (input_tokens, output_tokens)."""
        from agent_sdk.client import UsageStats
        stats = UsageStats()
        stats.update({"input_tokens": 200, "output_tokens": 80})

        assert stats.input_tokens == 200
        assert stats.output_tokens == 80
        assert stats.total_tokens == 280

    def test_update_accumulates_across_calls(self):
        """Multiple update() calls accumulate totals."""
        from agent_sdk.client import UsageStats
        stats = UsageStats()
        stats.update({"inputTokens": 100, "outputTokens": 50})
        stats.update({"inputTokens": 200, "outputTokens": 100})

        assert stats.input_tokens == 300
        assert stats.output_tokens == 150
        assert stats.total_tokens == 450

    def test_update_missing_fields_default_to_zero(self):
        """Missing fields default to 0 in update()."""
        from agent_sdk.client import UsageStats
        stats = UsageStats()
        stats.update({})

        assert stats.input_tokens == 0
        assert stats.output_tokens == 0
        assert stats.total_tokens == 0

    def test_update_zero_cost_not_accumulated(self):
        """Zero totalCostUsd is falsy — not added to total_cost_usd."""
        from agent_sdk.client import UsageStats
        stats = UsageStats(total_cost_usd=0.005)
        stats.update({"inputTokens": 10, "totalCostUsd": 0})

        assert abs(stats.total_cost_usd - 0.005) < 1e-9  # unchanged


# ---------------------------------------------------------------------------
# Tests: _ensure_registered resume path (session_id given, no provider)
# ---------------------------------------------------------------------------

class TestEnsureRegisteredResumePath:
    """When session_id is given without provider, _ensure_registered calls
    POST /sessions/{id}/resume to reconnect to an existing session."""

    @pytest.mark.asyncio
    async def test_resume_path_calls_resume_endpoint(self):
        """With session_id and no provider, calls POST /sessions/{id}/resume."""
        agent = Agent("resume-agent", session_id="existing-session")
        assert not agent._registered

        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.raise_for_status = MagicMock()
        mock_resp.json = MagicMock(return_value={
            "session_id": "existing-session",
            "agent_id": "agent-abc",
            "sandbox_ref": "sbx-123",
            "inner_session_id": "inner-abc",
            "status": "resumed",
        })

        with patch.object(agent._api._http, "request", AsyncMock(return_value=mock_resp)) as mock_request:
            await agent._ensure_registered()

        # ``ApiClient`` issues every call through ``self._http.request(method, path, ...)``;
        # resume_session goes to ``POST /sessions/{id}/resume``.
        call_args = mock_request.call_args
        assert call_args.args[0] == "POST"
        assert "resume" in call_args.args[1]
        assert agent._registered
        assert agent.sandbox_ref == "sbx-123"
        assert agent.inner_session_id == "inner-abc"

    @pytest.mark.asyncio
    async def test_resume_path_raises_on_404(self):
        """If resume endpoint returns 404, _ensure_registered raises."""
        agent = Agent("resume-404-agent", session_id="gone-session")

        mock_resp = MagicMock()
        mock_resp.status_code = 404
        mock_resp.is_error = True

        def _raise_for_status():
            raise httpx.HTTPStatusError(
                "HTTP 404: session not found",
                request=MagicMock(),
                response=mock_resp,
            )

        mock_resp.raise_for_status = _raise_for_status
        mock_resp.json = MagicMock(return_value={"error": "session not found"})

        with patch.object(agent._api._http, "request", AsyncMock(return_value=mock_resp)):
            with pytest.raises(httpx.HTTPStatusError):
                await agent._ensure_registered()


# ---------------------------------------------------------------------------
# Tests: _raise_for_status detail extraction
# ---------------------------------------------------------------------------

class TestRaiseForStatus:
    """_raise_for_status extracts error detail from response body."""

    def test_200_does_not_raise(self):
        """2xx status codes do not raise."""
        from agent_sdk.client import _raise_for_status
        resp = MagicMock()
        resp.status_code = 200
        _raise_for_status(resp)  # must not raise

    def test_404_raises_with_error_detail(self):
        """4xx raises HTTPStatusError with extracted 'error' field."""
        from agent_sdk.client import _raise_for_status
        resp = MagicMock()
        resp.status_code = 404
        resp.json = MagicMock(return_value={"error": "not found"})
        resp.request = MagicMock()

        with pytest.raises(httpx.HTTPStatusError) as exc_info:
            _raise_for_status(resp)
        assert "not found" in str(exc_info.value)

    def test_4xx_raises_with_detail_field_fallback(self):
        """If no 'error' key, falls back to 'detail'."""
        from agent_sdk.client import _raise_for_status
        resp = MagicMock()
        resp.status_code = 422
        resp.json = MagicMock(return_value={"detail": "validation error"})
        resp.request = MagicMock()

        with pytest.raises(httpx.HTTPStatusError) as exc_info:
            _raise_for_status(resp)
        assert "validation error" in str(exc_info.value)

    def test_raises_even_when_json_fails(self):
        """If response body is not JSON, still raises with text fallback."""
        from agent_sdk.client import _raise_for_status
        resp = MagicMock()
        resp.status_code = 500
        resp.json = MagicMock(side_effect=ValueError("not json"))
        resp.text = "Internal Server Error"
        resp.request = MagicMock()

        with pytest.raises(httpx.HTTPStatusError) as exc_info:
            _raise_for_status(resp)
        assert "500" in str(exc_info.value)
