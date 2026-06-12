"""P0-F gate: NativeSession honors the BaseSandboxSession contract.

Drives execute_prompt directly (bypassing start()'s DB) with a fake
transport and mocked completion, asserting:
- broadcast/yield parity (every yielded event re-parses from its broadcast
  block) — the property the SSE-log parity tests enforce in production;
- cancel mid-turn produces a done(cancelled) terminal + a checkpoint;
- always-alive liveness survives the pool's force-probe;
- the factory routes native states to NativeSession.
"""

from __future__ import annotations

import asyncio
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from api.native.loop import NativeAgentSpec  # noqa: E402
from api.native.session import NativeSession  # noqa: E402
from api.native.tools import build_toolset  # noqa: E402
from api.sandbox.state import NativeSandboxState  # noqa: E402
from api.sse import parse_acp_event  # noqa: E402


# ── fakes mirroring the loop test's ───────────────────────────────────────

class _Delta:
    def __init__(self, content=None, tool_calls=None):
        self.content = content
        self.reasoning_content = None
        self.tool_calls = tool_calls


class _Chunk:
    def __init__(self, delta=None, usage=None):
        self.choices = [type("C", (), {"delta": delta})()] if delta else []
        self.usage = usage


class _TCDelta:
    def __init__(self, index, id=None, name=None, arguments=None):
        self.index = index
        self.id = id
        self.function = type("F", (), {"name": name, "arguments": arguments})()


class _Usage:
    def __init__(self, p, c):
        self.prompt_tokens = p
        self.completion_tokens = c
        self.response_cost = 0.0


class _FakeTransport:
    async def exec(self, command, *, cwd=None, env=None, timeout_s=300):
        from api.native.transport import TransportExecResult
        await asyncio.sleep(0)
        return TransportExecResult(f"ran:{command}", "", 0, False)

    async def read_file(self, p, *, max_bytes=8 * 1024 * 1024):
        raise FileNotFoundError(p)

    async def write_file(self, p, d):
        pass

    async def destroy(self):
        pass


def _completion_factory(chunks_per_call):
    calls = {"n": 0}

    async def completion(**kwargs):
        i = calls["n"]
        calls["n"] += 1

        async def _it():
            for c in chunks_per_call[i]:
                yield c
        return _it()
    return completion


def _make_session(chunks_per_call, *, tools=None, transport=None,
                  capture_checkpoints=None):
    s = NativeSession(session_id="sess-native-f",
                      state=NativeSandboxState(provider="docker"))
    # bypass start()/DB
    s._started = True
    s._spec = NativeAgentSpec(instructions="sys", max_turns=5)
    s._tools = tools if tools is not None else {}
    s._transport = transport
    s._messages = [{"role": "system", "content": "sys"}]
    s._completion = _completion_factory(chunks_per_call)
    if capture_checkpoints is not None:
        async def _ckpt(usage):
            capture_checkpoints.append({"turn_seq": s._turn_seq,
                                        "messages": list(s._messages),
                                        "usage": usage})
        s._checkpoint = _ckpt  # type: ignore
    else:
        async def _noop(usage):
            return None
        s._checkpoint = _noop  # type: ignore
    return s


# ── tests ──────────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_broadcast_yield_parity_text_turn():
    s = _make_session([[
        _Chunk(_Delta(content="Hello")),
        _Chunk(usage=_Usage(5, 2)),
    ]])
    broadcasts = []
    s._broadcast = lambda item: broadcasts.append(item)

    yielded = [ev async for ev in s.execute_prompt("hi", rpc_id="rpc-1")]

    # every broadcast is (rpc_id, block); re-parsing each block reproduces
    # the matching yielded canonical event — parity by construction.
    assert len(broadcasts) == len(yielded)
    for (rpc, block), ev in zip(broadcasts, yielded):
        assert rpc == "rpc-1"
        assert parse_acp_event(block, "rpc-1") == ev
    assert [e["type"] for e in yielded] == ["text", "usage", "done"]
    assert yielded[-1]["stop_reason"] == "end_turn"


@pytest.mark.asyncio
async def test_tool_turn_through_session():
    ckpts = []
    s = _make_session(
        [
            [_Chunk(_Delta(tool_calls=[
                _TCDelta(0, id="c1", name="bash", arguments='{"command":"ls"}')]))],
            [_Chunk(_Delta(content="ok"))],
        ],
        tools=build_toolset(["bash"]), transport=_FakeTransport(),
        capture_checkpoints=ckpts)
    s._broadcast = lambda item: None

    types = [ev["type"] async for ev in s.execute_prompt("run", rpc_id="r2")]
    assert types == ["tool", "tool_result", "text", "done"]
    # one checkpoint written at turn end, turn_seq advanced
    assert len(ckpts) == 1 and ckpts[0]["turn_seq"] == 1
    roles = [m["role"] for m in ckpts[0]["messages"]]
    assert roles == ["system", "user", "assistant", "tool", "assistant"]


@pytest.mark.asyncio
async def test_cancel_produces_cancelled_terminal_and_checkpoint():
    # A model call that never ends until cancelled: an infinite chunk stream.
    async def _never(**kwargs):
        async def _it():
            while True:
                await asyncio.sleep(0.01)
                yield _Chunk(_Delta(content="."))
        return _it()

    ckpts = []
    s = _make_session([[]], capture_checkpoints=ckpts)
    s._completion = _never
    s._broadcast = lambda item: None

    seen = []

    async def _consume():
        async for ev in s.execute_prompt("loop forever", rpc_id="r3"):
            seen.append(ev)

    consumer = asyncio.create_task(_consume())
    # let the loop start streaming, then interrupt
    await asyncio.sleep(0.05)
    await s.cancel_active_prompt()
    await asyncio.wait_for(consumer, timeout=5)

    assert seen[-1] == {"type": "done", "stop_reason": "cancelled"}
    assert len(ckpts) == 1  # partial transcript persisted


@pytest.mark.asyncio
async def test_always_alive_liveness():
    s = NativeSession(session_id="x", state=NativeSandboxState(provider="docker"))
    assert await s.running() is True
    assert await s.running(force_probe=True) is True
    assert await s._liveness_probe() is True


def test_factory_routes_native():
    from api.sandbox import factory
    out = factory.make_session("s1", NativeSandboxState(provider="docker"))
    assert isinstance(out, NativeSession)


@pytest.mark.asyncio
async def test_recover_adopts_concurrent_replacement_no_double_create():
    """Concurrency-safety of recovery: two recoveries from the SAME dead
    transport must create exactly ONE replacement sandbox.

    A streaming turn's tool call and a /sandbox/exec on one session are NOT
    serialized above _provision_lock, so both can hit SandboxGoneError on the
    same dead transport. _ensure_sandbox(replace=dead) re-provisions only while
    `dead` is still cached; once a concurrent recovery has swapped in a fresh
    transport, a later recovery that still names the OLD dead one must ADOPT the
    fresh transport — not null it and create a SECOND sandbox (an orphan the
    reaper/boot-reconcile would have to reclaim, and a paid idle VM on
    daytona/modal until then)."""
    s = NativeSession(session_id="sess-recover-race",
                      state=NativeSandboxState(provider="docker"))
    s._started = True

    async def _noop_persist():   # pure unit test — no DB pool
        return None
    s._persist_state = _noop_persist  # type: ignore

    created: list = []

    class _T:
        def __init__(self, tag):
            self.ref = f"cid-{tag}"
            self.container_id = self.ref

        async def destroy(self):
            pass

    async def _factory():
        t = _T(len(created))
        created.append(t)
        return t

    s._transport_factory = _factory
    dead = _T("dead")
    s._transport = dead

    # first recovery: `dead` is still cached → provision exactly one fresh one
    t1 = await s._ensure_sandbox(replace=dead)
    assert len(created) == 1 and t1 is created[0]
    assert s._transport is t1

    # second recovery STILL naming the old dead transport: the fresh one is
    # cached now (≠ dead) → adopt it, do NOT double-create (no orphan/leak).
    t2 = await s._ensure_sandbox(replace=dead)
    assert t2 is t1
    assert len(created) == 1, (
        f"double-created a sandbox (orphan leak): {[c.ref for c in created]}")

    # and genuine concurrency: gather two recoveries from one fresh-dead
    # transport — the lock + replace-check still yields exactly one creation.
    dead2 = t1
    s._transport = dead2
    created.clear()
    g1, g2 = await asyncio.gather(
        s._ensure_sandbox(replace=dead2),
        s._ensure_sandbox(replace=dead2),
    )
    assert g1 is g2 and len(created) == 1, (
        f"concurrent recovery double-created: {[c.ref for c in created]}")


@pytest.mark.asyncio
async def test_secrets_split_keeps_llm_key_server_side():
    """start()'s split: AUTH_KEYS → server-side api_key; rest → sandbox env.
    Verified directly on the split logic without a DB round-trip."""
    from api.providers._shared import AUTH_KEYS

    s = NativeSession(session_id="x", state=NativeSandboxState(provider="docker"))
    s._spawn_env = {"OPENROUTER_API_KEY": "sk-secret",
                    "GITHUB_TOKEN": "ghp_x", "FOO": "bar"}
    # mimic the split start() performs
    s._llm_api_key = next((v for k, v in s._spawn_env.items()
                           if k in AUTH_KEYS), None)
    s._sandbox_env = {k: v for k, v in s._spawn_env.items()
                      if k not in AUTH_KEYS}
    assert s._llm_api_key == "sk-secret"
    assert "OPENROUTER_API_KEY" not in s._sandbox_env
    assert s._sandbox_env == {"GITHUB_TOKEN": "ghp_x", "FOO": "bar"}
