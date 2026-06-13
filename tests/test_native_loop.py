"""P0-E gate: the native loop drives correctly with a mocked LiteLLM and a
fake transport — no LLM, no docker. Asserts the emitted event sequence,
message-array growth, tool execution, and terminal stop reasons.
"""

from __future__ import annotations

import os
import sys

import pytest


from api.native.loop import NativeAgentSpec, initial_messages, run_turn  # noqa: E402
from api.native.tools import build_toolset  # noqa: E402


# ── fakes ───────────────────────────────────────────────────────────────────

class _Delta:
    def __init__(self, content=None, reasoning_content=None, tool_calls=None):
        self.content = content
        self.reasoning_content = reasoning_content
        self.tool_calls = tool_calls


class _Choice:
    def __init__(self, delta):
        self.delta = delta


class _Chunk:
    def __init__(self, delta=None, usage=None):
        self.choices = [_Choice(delta)] if delta is not None else []
        self.usage = usage


class _TCDelta:
    def __init__(self, index, id=None, name=None, arguments=None):
        self.index = index
        self.id = id
        self.function = type("F", (), {"name": name, "arguments": arguments})()


class _Usage:
    def __init__(self, p, c, cost=0.0):
        self.prompt_tokens = p
        self.completion_tokens = c
        self.response_cost = cost


def _stream(chunks):
    async def _gen(**kwargs):
        async def _it():
            for c in chunks:
                yield c
        return _it()
    return _gen


class _FakeTransport:
    def __init__(self):
        self.files: dict[str, bytes] = {}
        self.execs: list[str] = []

    async def exec(self, command, *, cwd=None, env=None, timeout_s=300):
        from api.native.transport import TransportExecResult
        self.execs.append(command)
        return TransportExecResult(stdout=f"ran:{command}", stderr="",
                                   exit_code=0, timed_out=False)

    async def read_file(self, path, *, max_bytes=8 * 1024 * 1024):
        if path not in self.files:
            raise FileNotFoundError(path)
        return self.files[path]

    async def write_file(self, path, data):
        self.files[path] = data


async def _collect(spec, messages, tools, transport, chunks_per_call):
    """Run a turn whose mocked completion returns a different chunk list on
    each successive model call (so a tool round-trip can be scripted)."""
    events = []

    async def emit(ev):
        events.append(ev)

    calls = {"n": 0}

    async def completion(**kwargs):
        i = calls["n"]
        calls["n"] += 1
        chunks = chunks_per_call[i]

        async def _it():
            for c in chunks:
                yield c
        return _it()

    result = await run_turn(spec, messages, tools, transport, emit,
                            completion=completion)
    return events, result


# ── tests ───────────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_text_only_turn():
    spec = NativeAgentSpec(instructions="be brief")
    msgs = initial_messages(spec, None)
    msgs.append({"role": "user", "content": "hi"})
    events, result = await _collect(spec, msgs, {}, None, [[
        _Chunk(_Delta(content="Hel")),
        _Chunk(_Delta(content="lo")),
        _Chunk(usage=_Usage(10, 2, 0.001)),
    ]])
    types = [e["type"] for e in events]
    assert types == ["text", "text", "usage", "done"]
    assert events[-1]["stop_reason"] == "end_turn"
    assert "".join(e["text"] for e in events if e["type"] == "text") == "Hello"
    u = next(e for e in events if e["type"] == "usage")["usage"]
    assert u["inputTokens"] == 10 and u["outputTokens"] == 2
    assert result.messages[0]["role"] == "system"
    assert result.messages[-1]["role"] == "assistant"


@pytest.mark.asyncio
async def test_reasoning_emitted():
    spec = NativeAgentSpec()
    msgs = [{"role": "user", "content": "think"}]
    events, _ = await _collect(spec, msgs, {}, None, [[
        _Chunk(_Delta(reasoning_content="hmm ")),
        _Chunk(_Delta(content="done")),
    ]])
    assert [e["type"] for e in events] == ["reasoning", "text", "done"]


@pytest.mark.asyncio
async def test_tool_call_roundtrip():
    spec = NativeAgentSpec()
    tools = build_toolset(["bash"])
    transport = _FakeTransport()
    msgs = [{"role": "user", "content": "run ls"}]
    # call 1: model emits a bash tool call (streamed in arg fragments)
    # call 2: model emits final text
    events, result = await _collect(spec, msgs, tools, transport, [
        [
            _Chunk(_Delta(tool_calls=[_TCDelta(0, id="call_1", name="bash")])),
            _Chunk(_Delta(tool_calls=[_TCDelta(0, arguments='{"command":')])),
            _Chunk(_Delta(tool_calls=[_TCDelta(0, arguments=' "ls /"}')])),
        ],
        [_Chunk(_Delta(content="done"))],
    ])
    types = [e["type"] for e in events]
    assert types == ["tool", "tool_result", "text", "done"]
    tool_ev = events[0]
    assert tool_ev["tool_name"] == "bash"
    assert tool_ev["tool_call_id"] == "call_1"
    assert tool_ev["args"] == {"command": "ls /"}
    assert "ran:ls /" in events[1]["result"]
    assert transport.execs == ["ls /"]
    # message array: user, assistant(tool_calls), tool, assistant(text)
    roles = [m["role"] for m in result.messages]
    assert roles == ["user", "assistant", "tool", "assistant"]
    assert result.messages[1]["tool_calls"][0]["function"]["name"] == "bash"
    assert result.messages[2]["tool_call_id"] == "call_1"


@pytest.mark.asyncio
async def test_large_streamed_tool_arg_reassembles_and_is_subquadratic():
    """A model streaming a big tool argument (e.g. write_file content) across
    many small deltas must reassemble exactly — and accumulate in O(total), not
    the O(total²) that repeated string ``+=`` cost. Both the correctness and the
    scaling are pinned here: the arg fragments are split fine enough that an
    O(n²) accumulation would blow the time budget."""
    import json as _json
    import time as _time

    # ~100k tiny fragments of a 2.5MB argument — the worst case for repeated
    # attribute ``+=`` (CPython's in-place str-concat optimization does NOT
    # apply when the accumulator is held by an attribute, so it stays O(n²)).
    big = "x" * 2_500_000
    payload = _json.dumps({"path": "/big.txt", "content": big})
    frag = 25
    fragments = [payload[i:i + frag] for i in range(0, len(payload), frag)]
    chunks = [_Chunk(_Delta(tool_calls=[_TCDelta(0, id="c1", name="write_file")]))]
    chunks += [_Chunk(_Delta(tool_calls=[_TCDelta(0, arguments=f)]))
               for f in fragments]

    spec = NativeAgentSpec()
    tools = build_toolset(["write_file"])
    transport = _FakeTransport()
    msgs = [{"role": "user", "content": "write it"}]

    t0 = _time.perf_counter()
    events, result = await _collect(spec, msgs, tools, transport, [
        chunks, [_Chunk(_Delta(content="done"))],
    ])
    elapsed = _time.perf_counter() - t0

    # exact reassembly: the tool saw the full content and wrote all the bytes
    assert transport.files["/big.txt"] == big.encode()
    # the persisted assistant tool_call carries the full argument string
    assert result.messages[1]["tool_calls"][0]["function"]["arguments"] == payload
    # O(total) accumulation drives this in ~60ms; the old attribute ``+=`` is
    # O(total²) (~3s here) and misses the budget by a wide, non-flaky margin.
    assert elapsed < 2.0, f"tool-arg accumulation too slow ({elapsed:.2f}s)"


@pytest.mark.asyncio
async def test_unknown_tool_returns_error_not_raise():
    spec = NativeAgentSpec()
    tools = build_toolset(["bash"])
    msgs = [{"role": "user", "content": "x"}]
    events, _ = await _collect(spec, msgs, tools, _FakeTransport(), [
        [_Chunk(_Delta(tool_calls=[
            _TCDelta(0, id="c1", name="nonexistent", arguments="{}")]))],
        [_Chunk(_Delta(content="ok"))],
    ])
    tr = next(e for e in events if e["type"] == "tool_result")
    assert "unknown tool" in tr["result"]


@pytest.mark.asyncio
async def test_max_turns_terminates():
    spec = NativeAgentSpec(max_turns=2)
    tools = build_toolset(["bash"])
    # every call asks for a tool → loop must stop at max_turns
    one_call = [_Chunk(_Delta(tool_calls=[
        _TCDelta(0, id="c", name="bash", arguments='{"command":"ls"}')]))]
    events, result = await _collect(spec, [{"role": "user", "content": "loop"}],
                                    tools, _FakeTransport(),
                                    [one_call, one_call, one_call])
    assert result.stop_reason == "max_turns"
    assert events[-1]["type"] == "done" and events[-1]["stop_reason"] == "max_turns"


@pytest.mark.asyncio
async def test_tool_invoke_exception_becomes_result():
    spec = NativeAgentSpec()
    tools = build_toolset(["bash"])

    class _Boom(_FakeTransport):
        async def exec(self, *a, **k):
            raise RuntimeError("kaboom")

    events, _ = await _collect(spec, [{"role": "user", "content": "x"}],
                               tools, _Boom(), [
        [_Chunk(_Delta(tool_calls=[
            _TCDelta(0, id="c1", name="bash", arguments='{"command":"ls"}')]))],
        [_Chunk(_Delta(content="ok"))],
    ])
    tr = next(e for e in events if e["type"] == "tool_result")
    assert "kaboom" in tr["result"]


# ── tool unit behavior (no loop) ────────────────────────────────────────────

@pytest.mark.asyncio
async def test_edit_file_uniqueness_contract():
    from api.native.tools import _BUILTINS
    edit = _BUILTINS["edit_file"]
    t = _FakeTransport()
    t.files["/f.txt"] = b"alpha beta alpha"
    # non-unique without replace_all -> error, file untouched
    r = await edit.invoke(t, {"path": "/f.txt", "old": "alpha", "new": "X"})
    assert "occurs 2" in r
    assert t.files["/f.txt"] == b"alpha beta alpha"
    # replace_all
    r = await edit.invoke(t, {"path": "/f.txt", "old": "alpha", "new": "X",
                              "replace_all": True})
    assert t.files["/f.txt"] == b"X beta X"
    # unique replace
    t.files["/g.txt"] = b"one two three"
    await edit.invoke(t, {"path": "/g.txt", "old": "two", "new": "2"})
    assert t.files["/g.txt"] == b"one 2 three"


@pytest.mark.asyncio
async def test_write_then_read_file_tools():
    from api.native.tools import _BUILTINS
    t = _FakeTransport()
    r = await _BUILTINS["write_file"].invoke(t, {"path": "/a.txt", "content": "hi"})
    assert "wrote 2 chars" in r
    r = await _BUILTINS["read_file"].invoke(t, {"path": "/a.txt"})
    assert r == "hi"
    r = await _BUILTINS["read_file"].invoke(t, {"path": "/missing"})
    assert r.startswith("error:")


def test_build_toolset_unknown_raises():
    with pytest.raises(KeyError):
        build_toolset(["bash", "not_a_tool"])
    assert set(build_toolset(None)) == {"bash", "read_file", "write_file", "edit_file"}


def test_tool_schema_precomputed_and_correct():
    """``Tool.schema`` is precomputed once (run_turn reads it per turn), so it's
    the SAME object across accesses — a regression to a per-access property
    (rebuilding the nested dict every turn = GC churn) fails the identity check.
    The shape must still match the OpenAI/LiteLLM function-tool contract."""
    bash = build_toolset(["bash"])["bash"]
    assert bash.schema is bash.schema           # cached, not rebuilt per access
    # toolset copies share the same singleton Tool, so the same schema object
    assert build_toolset(None)["bash"].schema is bash.schema
    s = bash.schema
    assert s["type"] == "function"
    assert s["function"]["name"] == "bash"
    assert s["function"]["description"]
    assert s["function"]["parameters"]["type"] == "object"
    assert "command" in s["function"]["parameters"]["properties"]


@pytest.mark.asyncio
async def test_run_turn_passes_num_retries_for_transient_resilience():
    """The native loop asks LiteLLM to retry transient INITIAL-call failures
    (rate limits / 5xx / connection resets) so a blip doesn't fail the whole
    turn — default on (2), configurable, and 0 omits it (provider default)."""
    captured = {}

    async def completion(**kwargs):
        captured.clear()
        captured.update(kwargs)

        async def _it():
            yield _Chunk(_Delta(content="ok"))
        return _it()

    async def _emit(ev):
        pass

    msg = [{"role": "user", "content": "hi"}]

    # default: num_retries=2 reaches the model call
    await run_turn(NativeAgentSpec(), list(msg), {}, None, _emit,
                   completion=completion)
    assert captured.get("num_retries") == 2

    # config override
    spec = NativeAgentSpec.from_config(model=None, native={"num_retries": 5})
    assert spec.num_retries == 5
    await run_turn(spec, list(msg), {}, None, _emit, completion=completion)
    assert captured.get("num_retries") == 5

    # disabled: omitted entirely so LiteLLM's own default applies
    await run_turn(NativeAgentSpec(num_retries=0), list(msg), {}, None, _emit,
                   completion=completion)
    assert "num_retries" not in captured


def test_litellm_completion_sets_native_config():
    """The native runtime must not egress litellm's anonymized usage telemetry
    on every model call, must not spam stderr with its provider banner, and must
    drop params an arbitrary model doesn't support (instead of failing the turn).
    Resolving the real completion sets all three; it still returns acompletion."""
    import litellm
    from api.native.loop import _litellm_completion

    # simulate litellm's shipped defaults
    litellm.telemetry = True
    litellm.suppress_debug_info = False
    litellm.drop_params = False

    fn = _litellm_completion()
    assert fn is litellm.acompletion
    assert litellm.telemetry is False
    assert litellm.suppress_debug_info is True
    assert litellm.drop_params is True


def test_native_spec_clamps_misconfigured_loop_knobs():
    """A misconfigured native agent gets sane floors: a max_turns <= 0 would make
    range(max_turns) empty → a silent no-op turn (no model call), and a negative
    num_retries would be handed straight to LiteLLM."""
    f = NativeAgentSpec.from_config
    assert f(model=None, native={"max_turns": 0}).max_turns == 1
    assert f(model=None, native={"max_turns": -5}).max_turns == 1
    assert f(model=None, native={"num_retries": -3}).num_retries == 0
    # valid values pass through untouched
    s = f(model=None, native={"max_turns": 10, "num_retries": 4})
    assert s.max_turns == 10 and s.num_retries == 4


# ── interrupt-wedge: dangling assistant tool_calls healing ───────────────────
# An interrupt mid-tool-loop (CancelledError is a BaseException, so it bypasses
# _invoke_tool's `except Exception`) lands after the assistant tool_calls
# message is appended but before its tool results are. Providers 400 on that
# shape, so once checkpointed the session was durably wedged — every later
# prompt failed across hibernate/resume/restart.

def test_heal_dangling_tool_calls_inserts_stubs():
    from api.native.loop import heal_dangling_tool_calls

    # trailing dangling call (interrupt before any tool result)
    m = [
        {"role": "user", "content": "hi"},
        {"role": "assistant", "tool_calls": [
            {"id": "c1", "type": "function",
             "function": {"name": "bash", "arguments": "{}"}}]},
    ]
    heal_dangling_tool_calls(m)
    assert m[-1] == {"role": "tool", "tool_call_id": "c1",
                     "content": "error: interrupted"}

    # PARTIAL: first of two calls answered, second dangling, and the next user
    # turn already appended (a poisoned checkpoint that got prompted again) —
    # the stub must land after the existing results, BEFORE the user turn.
    m2 = [
        {"role": "assistant", "tool_calls": [
            {"id": "a", "type": "function",
             "function": {"name": "b", "arguments": "{}"}},
            {"id": "z", "type": "function",
             "function": {"name": "b", "arguments": "{}"}}]},
        {"role": "tool", "tool_call_id": "a", "content": "ok"},
        {"role": "user", "content": "next"},
    ]
    heal_dangling_tool_calls(m2)
    assert m2[2] == {"role": "tool", "tool_call_id": "z",
                     "content": "error: interrupted"}
    assert m2[3]["role"] == "user"

    # clean transcripts unchanged; healing is idempotent
    clean = [{"role": "user", "content": "x"},
             {"role": "assistant", "content": "done"}]
    snap = [dict(x) for x in clean]
    heal_dangling_tool_calls(clean)
    assert clean == snap
    heal_dangling_tool_calls(m)
    assert sum(1 for x in m if x["role"] == "tool") == 1


def _heal_forward_oracle(messages: list[dict]) -> None:
    """The ORIGINAL full forward scan, kept here as an equivalence oracle for
    the tail-bounded production implementation. If they ever diverge on a
    transcript this system can produce, test_heal_tail_matches_forward_oracle
    fails loudly. Do NOT 'optimize' this — it is the reference."""
    i = 0
    while i < len(messages):
        m = messages[i]
        if m.get("role") == "assistant" and m.get("tool_calls"):
            call_ids = [tc.get("id") for tc in m["tool_calls"] if tc.get("id")]
            j = i + 1
            answered: set = set()
            while j < len(messages) and messages[j].get("role") == "tool":
                answered.add(messages[j].get("tool_call_id"))
                j += 1
            missing = [cid for cid in call_ids if cid not in answered]
            if missing:
                stubs = [{"role": "tool", "tool_call_id": cid,
                          "content": "error: interrupted"} for cid in missing]
                messages[j:j] = stubs
                i = j + len(stubs)
                continue
        i += 1


def _gen_transcript(rng) -> list[dict]:
    """Build a transcript shaped like ones THIS loop actually produces: a run
    of fully-completed turns, then an optional dangling tail (interrupt
    mid-tool-loop), then an optional just-appended user message."""
    msgs: list[dict] = [{"role": "system", "content": "sys"}]
    cid = 0
    for _ in range(rng.randint(0, 6)):
        msgs.append({"role": "user", "content": "u"})
        # 0+ fully-answered tool rounds, then a final assistant text
        for _ in range(rng.randint(0, 3)):
            ids = [f"c{cid + k}" for k in range(rng.randint(1, 3))]
            cid += len(ids)
            msgs.append({"role": "assistant", "tool_calls": [
                {"id": i, "type": "function",
                 "function": {"name": "bash", "arguments": "{}"}} for i in ids]})
            for i in ids:  # every call answered (completed round)
                msgs.append({"role": "tool", "tool_call_id": i, "content": "ok"})
        msgs.append({"role": "assistant", "content": "done"})
    # optional dangling tail: assistant tool_calls with a SUBSET answered
    if rng.random() < 0.6:
        ids = [f"d{cid + k}" for k in range(rng.randint(1, 4))]
        cid += len(ids)
        msgs.append({"role": "assistant", "tool_calls": [
            {"id": i, "type": "function",
             "function": {"name": "bash", "arguments": "{}"}} for i in ids]})
        answered = [i for i in ids if rng.random() < 0.5]
        for i in answered:
            msgs.append({"role": "tool", "tool_call_id": i, "content": "ok"})
        if rng.random() < 0.5:   # a new prompt already appended (poisoned ckpt)
            msgs.append({"role": "user", "content": "next"})
    return msgs


def test_heal_tail_matches_forward_oracle():
    """The tail-bounded heal must produce byte-identical output to the original
    O(n) forward scan on every transcript this system can produce — that
    equivalence is what lets us bound the scan (killing the O(n²)-per-session
    cost) without weakening the durable-wedge safety net."""
    import random
    from api.native.loop import heal_dangling_tool_calls

    rng = random.Random(20260613)
    for _ in range(3000):
        msgs = _gen_transcript(rng)
        a = [dict(x) for x in msgs]
        b = [dict(x) for x in msgs]
        _heal_forward_oracle(a)
        heal_dangling_tool_calls(b)
        assert a == b, (msgs, a, b)
        # and the result is actually valid: every tool_call id is answered
        ans = {x.get("tool_call_id") for x in b if x.get("role") == "tool"}
        for x in b:
            if x.get("role") == "assistant" and x.get("tool_calls"):
                for tc in x["tool_calls"]:
                    assert tc["id"] in ans, (msgs, b)


@pytest.mark.asyncio
async def test_run_turn_self_heals_poisoned_prior_transcript():
    """A checkpoint persisted mid-tool-loop carries an assistant tool_calls
    with no tool results. run_turn must heal the array BEFORE the first model
    call so an already-poisoned session recovers on its next prompt instead of
    400ing forever."""
    spec = NativeAgentSpec()
    captured = []

    async def completion(**kwargs):
        captured.append([dict(m) for m in kwargs["messages"]])

        async def _it():
            yield _Chunk(_Delta(content="recovered"))
        return _it()

    msgs = [
        {"role": "assistant", "tool_calls": [
            {"id": "c9", "type": "function",
             "function": {"name": "bash", "arguments": "{}"}}]},
        {"role": "user", "content": "are you ok?"},
    ]
    events = []

    async def emit(ev):
        events.append(ev)

    result = await run_turn(spec, msgs, {}, None, emit, completion=completion)
    # the model never saw the dangling shape: stub inserted before the user turn
    sent = captured[0]
    assert sent[1] == {"role": "tool", "tool_call_id": "c9",
                       "content": "error: interrupted"}
    assert sent[2]["role"] == "user"
    assert result.stop_reason == "end_turn"
