"""Pin the persist-side SSE parser to the canonical ``parse_acp_event``.

The persist path (``_persist_prompt_events`` → ``execute_prompt`` →
``parse_acp_event``) and the SDK path (``Agent.astream`` →
``parse_acp_event``) MUST emit the same event taxonomy or the SSE/log
parity tests in ``tests/test_interrupt_integration.py`` fail. Both now
share the single ``api.sse.parse_acp_event`` entry point.

Three previously-leaked bugs that this pins:

1. ``agent_thought_chunk`` was logged as the literal string instead of
   ``reasoning`` — silently dropped from canonical log.
2. ``agent_message_chunk`` with empty text produced an empty
   ``assistant_message`` row that didn't exist in the SSE stream.
3. ``available_commands_update`` (and other meta updates) were logged
   as themselves rather than skipped.
"""
from __future__ import annotations

import json

import pytest

# The persist-side parser is now the single canonical ``parse_acp_event``
# (the old ``_parse_sse_block`` pass-through wrapper was deleted). Alias it
# here so the parity assertions below read against the same entry point the
# SSE prompt-drive uses.
from api.sse import parse_acp_event as _parse_sse_block


def _wrap(update: dict) -> str:
    """Wrap a session/update payload as an SSE ``data:`` block."""
    return "data: " + json.dumps({
        "jsonrpc": "2.0",
        "method": "session/update",
        "params": {"update": update},
    })


def _wrap_result(rpc_id: str, result: dict) -> str:
    return "data: " + json.dumps({"jsonrpc": "2.0", "id": rpc_id, "result": result})


# ---------------------------------------------------------------------------
# The reasoning bug — must map agent_thought_chunk → "reasoning"
# ---------------------------------------------------------------------------

def test_agent_thought_chunk_maps_to_reasoning():
    block = _wrap({
        "sessionUpdate": "agent_thought_chunk",
        "content": {"type": "text", "text": "let me think"},
    })
    ev = _parse_sse_block(block, "rpc-1")
    assert ev == {"type": "reasoning", "text": "let me think"}


def test_agent_thought_chunk_with_thinking_field_maps_to_reasoning():
    """Some adapters use ``content.thinking`` instead of ``content.text``."""
    block = _wrap({
        "sessionUpdate": "agent_thought_chunk",
        "content": {"thinking": "internal monologue"},
    })
    ev = _parse_sse_block(block, "rpc-1")
    assert ev == {"type": "reasoning", "text": "internal monologue"}


# ---------------------------------------------------------------------------
# The empty-text bug — must filter content with no text
# ---------------------------------------------------------------------------

def test_agent_message_chunk_empty_text_returns_none():
    block = _wrap({
        "sessionUpdate": "agent_message_chunk",
        "content": {"type": "text", "text": ""},
    })
    assert _parse_sse_block(block, "rpc-1") is None


# ---------------------------------------------------------------------------
# The meta-update bug — must skip non-event updates
# ---------------------------------------------------------------------------

def test_available_commands_update_returns_none():
    block = _wrap({
        "sessionUpdate": "available_commands_update",
        "available_commands": [{"name": "Bash"}],
    })
    assert _parse_sse_block(block, "rpc-1") is None


# ---------------------------------------------------------------------------
# Sanity — regular events still flow through
# ---------------------------------------------------------------------------

def test_agent_message_chunk_real_text_returns_text_event():
    block = _wrap({
        "sessionUpdate": "agent_message_chunk",
        "content": {"type": "text", "text": "hello"},
    })
    assert _parse_sse_block(block, "rpc-1") == {"type": "text", "text": "hello"}


def test_done_result_returns_done_event():
    block = _wrap_result("rpc-1", {"stopReason": "end_turn"})
    assert _parse_sse_block(block, "rpc-1") == {"type": "done", "stop_reason": "end_turn"}


def test_rpc_id_filter_skips_other_prompts():
    block = _wrap_result("OTHER", {"stopReason": "end_turn"})
    assert _parse_sse_block(block, "rpc-1") is None


def test_heartbeat_returns_none():
    assert _parse_sse_block(": heartbeat", "rpc-1") is None
    assert _parse_sse_block("", "rpc-1") is None


# ---------------------------------------------------------------------------
# The persist-side log mapping must match the parser's output types.
# Catches the failure where ``_EVENT_TYPE_TO_LOG`` keys drift away from
# what the parser actually emits.
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("etype,expected_log_type", [
    ("text", "assistant_message"),
    ("reasoning", "reasoning"),
    ("tool", "tool_call"),
    ("tool_result", "tool_result"),
    ("usage", "usage"),
    ("error", "error"),
    ("done", "turn_end"),
])
def test_event_type_to_log_covers_parser_outputs(etype, expected_log_type):
    from api.server import _EVENT_TYPE_TO_LOG
    assert _EVENT_TYPE_TO_LOG.get(etype) == expected_log_type, (
        f"_EVENT_TYPE_TO_LOG must map parser output {etype!r} to "
        f"{expected_log_type!r} so persist and SSE produce the same canonical log"
    )


class _NoopLiveness:
    def observe_prompt_start(self) -> None:
        pass

    def observe_prompt_end(self) -> None:
        pass


# ---------------------------------------------------------------------------
# Persist coalescing — one row per logical block, not per ACP chunk.
# Mirrors what the SDK's ``astream`` accumulates and what ``/events``
# subscribers see after canonicalization.
# ---------------------------------------------------------------------------

class _NoopLiveness:
    """Stand-in for ``Liveness`` on the test fakes. ``_persist_prompt_events``
    drives the in-flight gate (``observe_prompt_start``/``observe_prompt_end``)
    and the idle reaper reads ``_last_compute_at``; the fakes run no real
    compute, so these are no-ops over a static clock."""

    _last_compute_at = None

    def observe_prompt_start(self) -> None: ...
    def observe_prompt_end(self) -> None: ...
    def observe_chunk(self) -> None: ...
    def observe_activity(self) -> None: ...
    def observe_close(self) -> None: ...


class _FakeSession:
    """Minimal stand-in: ``execute_prompt`` yields a fixed event list,
    ``_broadcast`` is a no-op, agent_id/session_id are constants. Owns
    its own ``_prompt_lock`` so the persist path's ``async with`` holds.
    """

    def __init__(self, events: list[dict]) -> None:
        import asyncio as _a
        self._events = events
        self._agent_id = "agent-x"
        self.session_id = "sess-x"
        self._prompt_lock = _a.Lock()
        self.liveness = _NoopLiveness()

    async def execute_prompt(self, message: str, *, rpc_id: str):
        for e in self._events:
            yield e

    def _broadcast(self, _evt: dict) -> None:
        pass


def _capture_log_writes(monkeypatch) -> list[tuple[str, dict]]:
    """Patch ``api.server.log_event`` to record (event_type, payload) tuples
    instead of touching the DB. Returns the captured list."""
    rows: list[tuple[str, dict]] = []

    async def _fake_log_event(*, session_id, agent_id, event_type, payload):
        rows.append((event_type, payload))

    from api import server as srv
    monkeypatch.setattr(srv, "log_event", _fake_log_event)
    return rows


@pytest.mark.asyncio
async def test_persist_logs_empty_done_turn_for_rca(monkeypatch, caplog):
    rows = _capture_log_writes(monkeypatch)
    sess = _FakeSession([
        {"type": "usage", "usage": {"amount": 1.25, "currency": "USD"}},
        {"type": "done", "stop_reason": "end_turn"},
    ])
    from api.server import _persist_prompt_events

    with caplog.at_level("WARNING", logger="api.server"):
        await _persist_prompt_events(sess, "hi", "rpc-empty")

    assert [r[0] for r in rows if r[0] != "user_message"] == [
        "usage", "turn_end",
    ]
    messages = [r.message for r in caplog.records]
    assert any("empty prompt turn" in m for m in messages)
    assert any("rpc-empty" in m and "message_chars=2" in m for m in messages)


@pytest.mark.asyncio
async def test_persist_coalesces_consecutive_reasoning_chunks(monkeypatch):
    rows = _capture_log_writes(monkeypatch)
    sess = _FakeSession([
        {"type": "reasoning", "text": "step 1 "},
        {"type": "reasoning", "text": "step 2 "},
        {"type": "reasoning", "text": "step 3"},
        {"type": "done", "stop_reason": "end_turn"},
    ])
    from api.server import _persist_prompt_events
    await _persist_prompt_events(sess, "hi", "rpc-1")

    types = [r[0] for r in rows if r[0] != "user_message"]
    assert types == ["reasoning", "turn_end"], (
        f"3 consecutive reasoning chunks must collapse to one row + turn_end; got {types}"
    )
    reasoning_row = next(r for r in rows if r[0] == "reasoning")
    assert reasoning_row[1]["text"] == "step 1 step 2 step 3"


@pytest.mark.asyncio
async def test_persist_coalesces_consecutive_text_chunks(monkeypatch):
    rows = _capture_log_writes(monkeypatch)
    sess = _FakeSession([
        {"type": "text", "text": "Hello "},
        {"type": "text", "text": "world"},
        {"type": "usage", "usage": {"in": 10, "out": 5}},
        {"type": "done", "stop_reason": "end_turn"},
    ])
    from api.server import _persist_prompt_events
    await _persist_prompt_events(sess, "hi", "rpc-1")

    types = [r[0] for r in rows if r[0] != "user_message"]
    # ``usage`` is non-flushing — text chunks coalesce, usage writes
    # mid-buffer, then ``done`` flushes the text and writes turn_end.
    assert types == ["usage", "assistant_message", "turn_end"], (
        f"text chunks must collapse around non-flushing usage; got {types}"
    )
    am_row = next(r for r in rows if r[0] == "assistant_message")
    assert am_row[1]["text"] == "Hello world"


@pytest.mark.asyncio
async def test_persist_serializes_concurrent_prompts_on_same_session(monkeypatch):
    """Two POST /message calls on the same session must run sequentially.

    Without ``session._prompt_lock`` the two persist tasks raced on
    ``session_log`` writes and the row order diverged from SSE arrival
    order — surfaced as the ``test_interrupt_mid_tool_parity
    ['cancelled','end_turn'] vs ['end_turn','cancelled']`` flake.
    """
    import asyncio as _a

    rows = _capture_log_writes(monkeypatch)

    class _OrderedEvents:
        """Yields a tagged ``text`` chunk then a small sleep so the second
        concurrent call has a chance to interleave inside the persist
        loop if the lock isn't doing its job."""
        def __init__(self, tag: str) -> None:
            self._tag = tag
            self._agent_id = "agent-x"
            self.session_id = "sess-x"
            self.liveness = _NoopLiveness()
            # Shared across both call paths in this test — the same
            # lock instance enforces serialisation.
            pass

        async def execute_prompt(self, message: str, *, rpc_id: str):
            yield {"type": "text", "text": f"{self._tag}-1"}
            await _a.sleep(0.05)
            yield {"type": "text", "text": f"{self._tag}-2"}
            yield {"type": "done", "stop_reason": "end_turn"}

        def _broadcast(self, _evt: dict) -> None:
            pass

    sess_a = _OrderedEvents("A")
    sess_b = _OrderedEvents("B")
    # Same lock => same logical session.
    shared_lock = _a.Lock()
    sess_a._prompt_lock = shared_lock
    sess_b._prompt_lock = shared_lock
    sess_a.liveness = _NoopLiveness()
    sess_b.liveness = _NoopLiveness()

    from api.server import _persist_prompt_events
    # Fire two concurrent persist tasks against the shared lock.
    t_a = _a.create_task(_persist_prompt_events(sess_a, "msg-a", "rpc-a"))
    t_b = _a.create_task(_persist_prompt_events(sess_b, "msg-b", "rpc-b"))
    await _a.gather(t_a, t_b)

    # All A's rows must come before all B's rows (or vice versa) — they
    # must NOT interleave. Locate the boundary by the prompt_id payload.
    prompt_ids = [r[1].get("prompt_id") for r in rows if r[0] == "assistant_message"]
    boundary = None
    for i in range(1, len(prompt_ids)):
        if prompt_ids[i] != prompt_ids[i - 1]:
            boundary = i
            break
    # If only one prompt's text rows are present (because text chunks
    # collapsed into a single row each), both prompts will have exactly
    # one assistant_message — that's still ordered.
    assert boundary is None or all(
        prompt_ids[i] == prompt_ids[boundary] for i in range(boundary, len(prompt_ids))
    ), f"prompts must not interleave; saw assistant_message prompt_ids={prompt_ids}"


@pytest.mark.asyncio
async def test_persist_flushes_buffer_on_hard_cancel(monkeypatch):
    """Hard cancel (asyncio Task.cancel) must not drop unflushed text.

    ``CancelledError`` is a ``BaseException`` in Python 3.8+, so the
    ``except Exception`` arm wouldn't run — the ``finally`` block with
    ``asyncio.shield`` is the safety net.
    """
    import asyncio as _asyncio

    rows = _capture_log_writes(monkeypatch)

    class _SlowEvents:
        """Yields a few text chunks then sleeps forever, so the outer
        task can be cancelled mid-stream with content still buffered."""
        def __init__(self) -> None:
            self._agent_id = "agent-x"
            self.session_id = "sess-x"
            self._prompt_lock = _asyncio.Lock()
            self.liveness = _NoopLiveness()

        async def execute_prompt(self, message: str, *, rpc_id: str):
            yield {"type": "text", "text": "partial "}
            yield {"type": "text", "text": "answer"}
            # Hold the iterator open so the outer Task.cancel races
            # against the buffer.
            await _asyncio.sleep(60)

        def _broadcast(self, _evt: dict) -> None:
            pass

    from api.server import _persist_prompt_events
    task = _asyncio.create_task(
        _persist_prompt_events(_SlowEvents(), "hi", "rpc-cancel"),
    )
    # Give the iterator a chance to buffer the two text chunks.
    await _asyncio.sleep(0.05)
    task.cancel()
    try:
        await task
    except _asyncio.CancelledError:
        pass

    # The finally block should have flushed the partial assistant
    # message even though the outer task was cancelled.
    assert any(r[0] == "assistant_message" for r in rows), (
        f"hard-cancel must flush buffered text; got {[r[0] for r in rows]}"
    )
    am = next(r for r in rows if r[0] == "assistant_message")
    assert am[1]["text"] == "partial answer"


@pytest.mark.asyncio
async def test_persist_usage_mid_reasoning_does_not_split_block(monkeypatch):
    """Mirrors the ``test_simple_prompt_parity`` ACP order: reasoning
    chunks, then usage_update, then assistant text, then done. Usage
    must NOT flush the reasoning buffer — otherwise SSE and log
    canonicalization disagree on event order.
    """
    rows = _capture_log_writes(monkeypatch)
    sess = _FakeSession([
        {"type": "reasoning", "text": "deliberating"},
        {"type": "usage", "usage": {}},
        {"type": "text", "text": "answer"},
        {"type": "done", "stop_reason": "end_turn"},
    ])
    from api.server import _persist_prompt_events
    await _persist_prompt_events(sess, "hi", "rpc-1")

    types = [r[0] for r in rows if r[0] != "user_message"]
    assert types == ["usage", "reasoning", "assistant_message", "turn_end"], (
        f"reasoning must outlive a non-flushing usage row; got {types}"
    )


@pytest.mark.asyncio
async def test_persist_flushes_on_type_change(monkeypatch):
    rows = _capture_log_writes(monkeypatch)
    sess = _FakeSession([
        {"type": "reasoning", "text": "thinking"},
        {"type": "text", "text": "answer"},
        {"type": "reasoning", "text": "more"},
        {"type": "text", "text": "final"},
        {"type": "done", "stop_reason": "end_turn"},
    ])
    from api.server import _persist_prompt_events
    await _persist_prompt_events(sess, "hi", "rpc-1")

    types = [r[0] for r in rows if r[0] != "user_message"]
    assert types == [
        "reasoning", "assistant_message", "reasoning", "assistant_message", "turn_end",
    ], f"interleaved blocks must each produce one row; got {types}"


@pytest.mark.asyncio
async def test_persist_flushes_before_tool(monkeypatch):
    rows = _capture_log_writes(monkeypatch)
    sess = _FakeSession([
        {"type": "text", "text": "I will run "},
        {"type": "text", "text": "this"},
        {"type": "tool", "tool_name": "Bash", "args": {"cmd": "ls"}},
        {"type": "tool_result", "tool_name": "Bash", "result": "x.txt"},
        {"type": "text", "text": "Done."},
        {"type": "usage", "usage": {}},
        {"type": "done", "stop_reason": "end_turn"},
    ])
    from api.server import _persist_prompt_events
    await _persist_prompt_events(sess, "hi", "rpc-1")

    types = [r[0] for r in rows if r[0] != "user_message"]
    # Tool call flushes the leading text; tool result is a discrete row;
    # trailing text + usage + done — usage doesn't flush, so the text
    # block lands AFTER usage on its terminal-event flush (matches SSE
    # canonical ordering).
    assert types == [
        "assistant_message", "tool_call", "tool_result",
        "usage", "assistant_message", "turn_end",
    ], f"tool calls flush text; usage does not; got {types}"
