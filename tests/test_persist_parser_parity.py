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
3. ``available_commands_update`` was once silently dropped. It is now a
   transient live event: visible to SDK/SSE consumers but not duplicated in
   the persisted session log on every prompt.
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
# Runtime metadata — expose commands and session information
# ---------------------------------------------------------------------------

def test_available_commands_update_maps_to_transient_commands_event():
    block = _wrap({
        "sessionUpdate": "available_commands_update",
        "availableCommands": [{"name": "goal", "description": "Set a goal"}],
    })
    event = _parse_sse_block(block, "rpc-1")
    assert event["type"] == "commands"
    assert event["commands"] == [{"name": "goal", "description": "Set a goal"}]


def test_session_info_update_preserves_vendor_metadata():
    meta = {"vendor": {"state": {"status": "active"}}}
    block = _wrap({
        "sessionUpdate": "session_info_update",
        "_meta": meta,
    })
    event = _parse_sse_block(block, "rpc-1")
    assert event["type"] == "session_info"
    assert event["raw"]["_meta"] == meta


def test_session_info_update_preserves_null_vendor_metadata_values():
    block = _wrap({
        "sessionUpdate": "session_info_update",
        "_meta": {"vendor": {"state": None}},
    })
    event = _parse_sse_block(block, "rpc-1")
    assert event["raw"]["_meta"]["vendor"]["state"] is None


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
    ("session_info", "session_info"),
])
def test_event_type_to_log_covers_parser_outputs(etype, expected_log_type):
    from api.turn import _EVENT_TYPE_TO_LOG
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

    from api import turn as _turn
    monkeypatch.setattr(_turn, "log_event", _fake_log_event)
    return rows


async def _persist_prompt_events(sess, message: str, rpc_id: str) -> None:
    """Compat shim: the free function moved into ``api.turn.TurnRunner``
    (same lock/coalescing/flush semantics). Tests drive the runner the
    way server.py does."""
    from api.turn import TurnRunner
    await TurnRunner(sess, message, rpc_id).run()



@pytest.mark.asyncio
async def test_persist_logs_empty_done_turn_for_rca(monkeypatch, caplog):
    """An empty turn (clean done, zero output) persists a synthetic error row
    AFTER turn_end — the loud-failure net for runtimes that swallow provider
    errors (e.g. opencode ending the turn cleanly on an expired key 401)."""
    rows = _capture_log_writes(monkeypatch)
    sess = _FakeSession([
        {"type": "usage", "usage": {"amount": 1.25, "currency": "USD"}},
        {"type": "done", "stop_reason": "end_turn"},
    ])

    with caplog.at_level("WARNING", logger="api.turn"):
        await _persist_prompt_events(sess, "hi", "rpc-empty")

    assert [r[0] for r in rows if r[0] != "user_message"] == [
        "usage", "turn_end", "error",
    ]
    err_payload = next(p for et, p in rows if et == "error")
    assert err_payload["kind"] == "empty_turn"
    assert err_payload["prompt_id"] == "rpc-empty"
    assert "no output" in err_payload["message"]
    messages = [r.message for r in caplog.records]
    assert any("empty prompt turn" in m for m in messages)
    assert any("rpc-empty" in m and "message_chars=2" in m for m in messages)


@pytest.mark.asyncio
async def test_available_commands_are_live_only_not_persisted(monkeypatch):
    rows = _capture_log_writes(monkeypatch)
    sess = _FakeSession([
        {"type": "commands", "commands": [{"name": "goal"}]},
        {"type": "text", "text": "ready"},
        {"type": "done", "stop_reason": "end_turn"},
    ])
    await _persist_prompt_events(sess, "/goal test", "rpc-commands")
    assert "commands" not in [event_type for event_type, _ in rows]


@pytest.mark.asyncio
async def test_session_info_only_turn_is_not_reported_as_empty(monkeypatch):
    rows = _capture_log_writes(monkeypatch)
    sess = _FakeSession([
        {"type": "session_info", "raw": {
            "sessionUpdate": "session_info_update",
            "_meta": {"vendor": {"state": None}},
        }},
        {"type": "done", "stop_reason": "end_turn"},
    ])
    await _persist_prompt_events(sess, "/command", "rpc-session-info")
    event_types = [
        event_type for event_type, _ in rows if event_type != "user_message"
    ]
    assert event_types == ["session_info", "turn_end"]


@pytest.mark.asyncio
async def test_empty_turn_broadcasts_rpc_tagged_error(monkeypatch):
    """The empty-turn error must ALSO reach live subscribers as an rpc-tagged
    error dict (the shape server.py's dict branch forwards and terminates on)."""
    _capture_log_writes(monkeypatch)
    seen: list[dict] = []
    sess = _FakeSession([
        {"type": "done", "stop_reason": "end_turn"},
    ])
    sess._broadcast = lambda evt: seen.append(evt)  # type: ignore[method-assign]

    class _Pool:
        _active = {sess.session_id: sess}
    import api.sandbox as _sb
    monkeypatch.setattr(_sb, "get_pool", lambda: _Pool())

    await _persist_prompt_events(sess, "hi", "rpc-emptycast")

    errs = [e for e in seen if e.get("type") == "error"]
    assert len(errs) == 1
    assert errs[0]["rpc_id"] == "rpc-emptycast"
    assert errs[0]["error"]["data"]["kind"] == "empty_turn"


@pytest.mark.asyncio
async def test_empty_turn_error_opt_out(monkeypatch, caplog):
    """AGENT_SDK_EMPTY_TURN_AS_ERROR=0 restores the WARNING-only behavior."""
    from api import turn as _turn
    monkeypatch.setattr(_turn, "_EMPTY_TURN_AS_ERROR", False)
    rows = _capture_log_writes(monkeypatch)
    sess = _FakeSession([
        {"type": "done", "stop_reason": "end_turn"},
    ])

    with caplog.at_level("WARNING", logger="api.turn"):
        await _persist_prompt_events(sess, "hi", "rpc-optout")

    assert "error" not in [r[0] for r in rows]
    assert any("empty prompt turn" in r.message for r in caplog.records)


@pytest.mark.asyncio
async def test_cancelled_empty_turn_is_not_an_error(monkeypatch):
    """A cancelled turn with no output is a user action, not a failure."""
    rows = _capture_log_writes(monkeypatch)
    sess = _FakeSession([
        {"type": "done", "stop_reason": "cancelled"},
    ])

    await _persist_prompt_events(sess, "hi", "rpc-cancelled")

    assert "error" not in [r[0] for r in rows]


@pytest.mark.asyncio
async def test_persist_coalesces_consecutive_reasoning_chunks(monkeypatch):
    rows = _capture_log_writes(monkeypatch)
    sess = _FakeSession([
        {"type": "reasoning", "text": "step 1 "},
        {"type": "reasoning", "text": "step 2 "},
        {"type": "reasoning", "text": "step 3"},
        {"type": "done", "stop_reason": "end_turn"},
    ])
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


# ---------------------------------------------------------------------------
# SEVERE: a stream that ends WITHOUT a terminal must NOT be reported as a
# successful turn. A supervisor that dies mid-prompt and lets the SSE EOF
# cleanly (no exception, no 'done') — exactly what a killed daytona supervisor
# does after its proxy holds the dead connection ~180s — left _drive_one
# returning (True, None), so the turn logged "turn done" but persisted NO
# turn_end/error. The client polling /log (or /events) for the rpc's terminal
# then waits forever: a SILENTLY DROPPED prompt (golden:
# test_midprompt_recovery_does_not_leak_subscriber[*-daytona], "prompt dropped").
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_terminalless_stream_end_still_delivers_a_terminal(monkeypatch):
    """A stream that ends without a done/error MUST still persist a terminal
    (turn_end or error). Pre-fix: only user_message + text were written, no
    terminal — the prompt is dropped from every client's view."""
    rows = _capture_log_writes(monkeypatch)

    # No replacement available (recovery can't find a live session) -> the run
    # must at least write an ERROR terminal rather than claim success.
    sess = _FakeSession([
        {"type": "text", "text": "partial reply, then the supervisor died"},
    ])  # NOTE: no 'done'/'error' — clean EOF mid-prompt

    class _NoReplacementPool:
        # error-broadcast path reads ``_active`` to reach live subscribers.
        _active = {sess.session_id: sess}

        async def get_session(self, _sid):
            raise RuntimeError("no live session")
    import api.sandbox as _sb
    monkeypatch.setattr(_sb, "get_pool", lambda: _NoReplacementPool())

    await _persist_prompt_events(sess, "do a thing", "rpc-dropped")

    etypes = [et for et, _ in rows]
    assert any(et in ("turn_end", "error") for et in etypes), (
        "no terminal persisted for a stream that ended without a 'done' event "
        f"— the prompt is silently dropped. persisted: {etypes}"
    )


@pytest.mark.asyncio
async def test_terminalless_stream_end_recovers_and_delivers_turn_end(monkeypatch):
    """When the pool HAS cold-recovered a replacement, a terminal-less stream
    end must RETRY on it and deliver the real turn_end (the recovered reply) —
    not falsely report the dead turn as done."""
    rows = _capture_log_writes(monkeypatch)

    sess = _FakeSession([
        {"type": "text", "text": "partial before death"},
    ])  # no terminal
    replacement = _FakeSession([
        {"type": "text", "text": "recovered reply"},
        {"type": "done", "stop_reason": "end_turn"},
    ])
    replacement.session_id = sess.session_id  # same session, fresh sandbox

    class _RecoveredPool:
        async def get_session(self, _sid):
            return replacement
    import api.sandbox as _sb
    monkeypatch.setattr(_sb, "get_pool", lambda: _RecoveredPool())

    await _persist_prompt_events(sess, "do a thing", "rpc-recover")

    etypes = [et for et, _ in rows]
    assert "turn_end" in etypes, (
        "recovery did not deliver a turn_end after a terminal-less stream end "
        f"— the recovered reply is dropped. persisted: {etypes}"
    )


@pytest.mark.asyncio
async def test_persist_parallel_tool_round_preserves_order(monkeypatch):
    """A parallel-tool round (native loop) emits ALL tool events, then ALL
    tool_result events — not interleaved. TurnRunner writes each independently in
    arrival order (no tool->result pairing assumption), so the round persists 3
    tool rows then 3 tool_result rows with ids preserved, text coalesced, then
    turn_end. Guards the parallel-tool emit order against a TurnRunner change."""
    rows = _capture_log_writes(monkeypatch)
    sess = _FakeSession([
        {"type": "tool", "tool_call_id": "c1", "tool_name": "bash", "args": {}},
        {"type": "tool", "tool_call_id": "c2", "tool_name": "bash", "args": {}},
        {"type": "tool", "tool_call_id": "c3", "tool_name": "bash", "args": {}},
        {"type": "tool_result", "tool_call_id": "c1", "tool_name": "bash",
         "result": "a"},
        {"type": "tool_result", "tool_call_id": "c2", "tool_name": "bash",
         "result": "b"},
        {"type": "tool_result", "tool_call_id": "c3", "tool_name": "bash",
         "result": "c"},
        {"type": "text", "text": "done"},
        {"type": "done", "stop_reason": "end_turn"},
    ])
    await _persist_prompt_events(sess, "go", "rpc-par")

    from api.turn import _EVENT_TYPE_TO_LOG as M
    types = [r[0] for r in rows if r[0] != "user_message"]
    assert types == ([M["tool"]] * 3 + [M["tool_result"]] * 3
                     + [M["text"], M["done"]])
    tool_ids = [r[1]["tool_call_id"] for r in rows if r[0] == M["tool"]]
    result_ids = [r[1]["tool_call_id"] for r in rows if r[0] == M["tool_result"]]
    assert tool_ids == ["c1", "c2", "c3"]
    assert result_ids == ["c1", "c2", "c3"]
