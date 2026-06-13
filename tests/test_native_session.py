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
async def test_model_failure_emits_error_terminal_and_session_recovers():
    """A model call that RAISES (litellm out of retries, a loop bug) must surface
    a clean ``error`` terminal AND end the stream — the sentinel in _drive's
    ``finally`` is what stops the client's iterator from hanging forever. And the
    failure must not wedge the session: the very next prompt must drive normally.
    Pins both (the no-hang via wait_for, the recovery via a second turn)."""
    base = _completion_factory([[_Chunk(_Delta(content="ok"))]])
    calls = {"n": 0}

    async def _boom_then_ok(**kwargs):
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("model exploded")
        return await base(**kwargs)

    s = _make_session([[]])
    s._completion = _boom_then_ok
    s._broadcast = lambda item: None

    async def _drain(rpc):
        return [ev async for ev in s.execute_prompt("hi", rpc_id=rpc)]

    # 1st prompt: the model raises → a single clean error terminal, no hang
    events = await asyncio.wait_for(_drain("r-err"), timeout=5)
    errs = [e for e in events if e.get("type") == "error"]
    assert len(errs) == 1
    assert errs[0]["kind"] == "RuntimeError"
    assert "model exploded" in errs[0]["text"]

    # 2nd prompt: the session is NOT wedged — a normal turn completes cleanly
    events2 = await asyncio.wait_for(_drain("r-ok"), timeout=5)
    assert [e["type"] for e in events2] == ["text", "done"]
    assert events2[-1]["stop_reason"] == "end_turn"


@pytest.mark.asyncio
async def test_always_alive_liveness():
    s = NativeSession(session_id="x", state=NativeSandboxState(provider="docker"))
    assert await s.running() is True
    assert await s.running() is True   # native is always-alive
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


def _pin_session(provider, cwd, subpath="agents/abc"):
    s = NativeSession(session_id="s",
                      state=NativeSandboxState(provider=provider))
    s._cwd = cwd
    s._subpath = subpath
    s._pin_modal_workspace_to_volume()
    return s._cwd


def test_modal_native_default_tmp_cwd_pinned_to_volume():
    """A default modal native session (cwd falls back to /tmp) must run on the
    /v Volume, not the ephemeral FS — else its workspace is lost on modal's
    routine terminate→recreate (the entrypoint won't symlink the critical /tmp)."""
    assert _pin_session("modal", "/tmp", "agents/abc") == "/v/agents/abc"
    assert _pin_session("modal", "/tmp/", "sessions/s1") == "/v/sessions/s1"


def test_modal_native_custom_cwd_left_for_symlink():
    """A non-critical custom cwd is symlinked onto the volume by the entrypoint,
    so the pin leaves it alone (no surprise cwd change)."""
    assert _pin_session("modal", "/workspace") == "/workspace"
    assert _pin_session("modal", "/home/agent") == "/home/agent"


def test_docker_native_tmp_cwd_not_redirected():
    """docker/daytona keep their FS across resume (and recreate is cold for
    docker anyway), so /tmp is fine there — the pin is modal-only."""
    assert _pin_session("docker", "/tmp") == "/tmp"
    assert _pin_session("daytona", "/tmp") == "/tmp"


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


@pytest.mark.asyncio
async def test_cancel_mid_tool_heals_checkpoint_no_dangling_tool_calls():
    """Interrupt while a tool EXECUTES (not while the model streams): the
    assistant tool_calls message is already in the array but its results are
    not. The cancel handler must NOT checkpoint that dangling shape verbatim —
    providers 400 on it, so the session would be durably wedged (every later
    prompt fails, surviving hibernate/resume and server restart)."""
    started = asyncio.Event()

    class _BlockingTransport(_FakeTransport):
        async def exec(self, command, *, cwd=None, env=None, timeout_s=300):
            started.set()
            await asyncio.sleep(3600)   # parked until cancelled

    ckpts = []
    s = _make_session(
        [[_Chunk(_Delta(tool_calls=[
            _TCDelta(0, id="c1", name="bash",
                     arguments='{"command":"sleep 60"}')]))]],
        tools=build_toolset(["bash"]), transport=_BlockingTransport(),
        capture_checkpoints=ckpts)
    s._broadcast = lambda item: None

    seen = []

    async def _consume():
        async for ev in s.execute_prompt("run", rpc_id="rX"):
            seen.append(ev)

    consumer = asyncio.create_task(_consume())
    await asyncio.wait_for(started.wait(), timeout=5)   # tool is executing
    await s.cancel_active_prompt()
    await asyncio.wait_for(consumer, timeout=5)

    assert seen[-1] == {"type": "done", "stop_reason": "cancelled"}
    assert len(ckpts) == 1
    msgs = ckpts[0]["messages"]
    for i, m in enumerate(msgs):
        if m.get("role") == "assistant" and m.get("tool_calls"):
            ids = {tc["id"] for tc in m["tool_calls"]}
            answered = {r.get("tool_call_id") for r in msgs[i + 1:]
                        if r.get("role") == "tool"}
            assert ids <= answered, (
                f"checkpoint kept dangling tool_calls {ids - answered} — "
                f"the next prompt would 400 (durably wedged session)")


@pytest.mark.asyncio
async def test_sandbox_exec_pins_in_flight_against_reaper():
    """/sandbox/exec runs no turn loop, so TurnRunner's observe_prompt bracket
    never fires for it. Without its own bracket, in_flight stays False for the
    whole exec — a session idle past its provider window could be hibernated
    (docker stop -t 0 / modal terminate) out from under a long-running command
    by the reaper's idle decision (pool._should_reap gates on in_flight)."""
    started = asyncio.Event()
    release = asyncio.Event()

    class _ParkedTransport(_FakeTransport):
        async def exec(self, command, *, cwd=None, env=None, timeout_s=300):
            started.set()
            await release.wait()
            return await super().exec(command, cwd=cwd, env=env,
                                      timeout_s=timeout_s)

    s = _make_session([[]], transport=_ParkedTransport())
    s._cwd = "/work"
    assert s.liveness.in_flight is False

    task = asyncio.create_task(s.sandbox_exec("sleep 200", timeout=300))
    await asyncio.wait_for(started.wait(), timeout=5)
    assert s.liveness.in_flight is True, (
        "long /sandbox/exec must pin in_flight so pool._should_reap returns "
        "prompt_in_flight instead of hibernating mid-command")
    release.set()
    res = await asyncio.wait_for(task, timeout=5)
    assert res["exit_code"] == 0
    assert s.liveness.in_flight is False

    # the bracket must release even when the exec path raises
    class _Boom(_FakeTransport):
        async def exec(self, command, *, cwd=None, env=None, timeout_s=300):
            raise RuntimeError("exec failed")

    s2 = _make_session([[]], transport=_Boom())
    s2._cwd = "/work"
    with pytest.raises(RuntimeError):
        await s2.sandbox_exec("true")
    assert s2.liveness.in_flight is False


@pytest.mark.asyncio
async def test_session_threads_sandbox_env_into_transports():
    """The loop's tools call transport.exec() with no env — the session's
    sandbox secrets must ride along as the transport's default env, or a
    native agent's own bash/git runs secret-less while /sandbox/exec (which
    passes env explicitly) gets them: an asymmetry vs the supervisor runtime,
    where the agent's shell inherits spawn_env."""
    s = NativeSession(session_id="s-env",
                      state=NativeSandboxState(provider="docker"))
    s._cwd = "/work"
    s._sandbox_env = {"GITHUB_TOKEN": "ghp_x", "CUSTOM": "1"}
    for provider, ref in (("docker", "cid-1"), ("daytona", "dt-1"),
                          ("modal", "sb-1")):
        t = s._reattach_transport(provider, ref)
        assert t.default_env == {"GITHUB_TOKEN": "ghp_x", "CUSTOM": "1"}, provider


@pytest.mark.asyncio
async def test_docker_exec_merges_default_env(monkeypatch):
    """DockerTransport injects its default env as an in-command `export`
    preamble on every call (NEVER `docker exec -e` — that applies before the
    OCI runtime resolves `sh`, so a PATH-class session env would 127 every
    exec); an explicit per-call env overrides key-by-key."""
    from api.native import transport as T

    seen: list[list] = []

    async def _fake_run_docker(*args, timeout=None):
        seen.append(list(args))
        return 0, b"ok", b""

    monkeypatch.setattr(T, "_run_docker", _fake_run_docker)
    t = T.DockerTransport(container_id="cid-x", workdir="/w",
                          env={"FOO": "bar", "TOK": "s3cr3t"})

    await t.exec("echo hi")
    cmd = seen[0][-1]                     # the wrapped `sh -c` payload
    assert "-e" not in seen[0]
    assert "export" in cmd and "FOO=bar" in cmd and "TOK=s3cr3t" in cmd

    await t.exec("echo hi", env={"FOO": "baz"})
    cmd = seen[1][-1]
    assert "FOO=baz" in cmd and "FOO=bar" not in cmd
    assert "TOK=s3cr3t" in cmd            # defaults persist under override


@pytest.mark.asyncio
async def test_docker_plumbing_is_env_immune(monkeypatch):
    """read_file routes through exec but must SKIP the default env — a
    session env named PATH would otherwise break `base64` lookup and with it
    every file op. The user-facing exec keeps the env (PATH altering the
    user's own command lookup is ordinary Unix semantics)."""
    import base64 as b64mod

    from api.native import transport as T

    seen: list[list] = []

    async def _fake_run_docker(*args, timeout=None):
        seen.append(list(args))
        return 0, b64mod.b64encode(b"data"), b""

    monkeypatch.setattr(T, "_run_docker", _fake_run_docker)
    t = T.DockerTransport(container_id="cid-x", workdir="/w",
                          env={"PATH": "/custom/bin"})

    assert await t.read_file("/f.txt") == b"data"
    assert "export" not in seen[0][-1], (
        "plumbing exec must not carry the session env — PATH would break "
        "the transport's own base64/mkdir machinery")
    assert "-e" not in seen[0]

    await t.exec("mytool --version")
    assert "export PATH=/custom/bin && mytool --version" in seen[1][-1]
    assert "-e" not in seen[1]            # never OCI-level env injection


# ── durable-wedge stress: repeated interrupt-mid-tool must never wedge ────────
def _first_dangling(messages):
    """Return the tool_call ids an assistant message left unanswered (the shape
    that 400s every future provider call = a durably wedged session), or None."""
    answered = {m.get("tool_call_id") for m in messages
                if m.get("role") == "tool"}
    for m in messages:
        if m.get("role") == "assistant" and m.get("tool_calls"):
            ids = {tc["id"] for tc in m["tool_calls"]}
            missing = ids - answered
            if missing:
                return missing
    return None


@pytest.mark.asyncio
async def test_repeated_cancel_mid_tool_never_wedges_concurrent():
    """Stress the durable-wedge guarantee: many sessions, each cycling
    interrupt-while-a-tool-executes -> recovery prompt, many times over. Every
    recovery must complete cleanly (the session is never wedged), no checkpoint
    may ever carry a dangling tool_calls, and no _active_task may leak. The
    single-shot heal is covered elsewhere; this drives the heal -> checkpoint ->
    next-prompt cycle REPEATEDLY and CONCURRENTLY to catch accumulation/state
    races a one-shot test can't."""
    K, M = 6, 8   # cycles per session, concurrent sessions

    async def run_session(sid):
        s = NativeSession(session_id=sid,
                          state=NativeSandboxState(provider="docker"))
        s._started = True
        s._spec = NativeAgentSpec(instructions="sys", max_turns=5)
        s._tools = build_toolset(["bash"])
        s._messages = [{"role": "system", "content": "sys"}]
        s._broadcast = lambda item: None

        async def _noop(usage):
            return None
        s._checkpoint = _noop  # type: ignore[method-assign]

        state = {"mode": "cancel", "cycle": 0, "started": asyncio.Event()}

        async def completion(**kwargs):
            if state["mode"] == "cancel":
                cid = f"c{state['cycle']}"

                async def _it():
                    yield _Chunk(_Delta(tool_calls=[_TCDelta(
                        0, id=cid, name="bash",
                        arguments='{"command":"sleep 60"}')]))
                return _it()

            async def _ok():
                yield _Chunk(_Delta(content="recovered"))
            return _ok()
        s._completion = completion

        class _BlockingTransport:
            async def exec(self, command, *, cwd=None, env=None, timeout_s=300):
                state["started"].set()
                await asyncio.sleep(3600)   # parked until the turn is cancelled

            async def read_file(self, p, *, max_bytes=8 * 1024 * 1024):
                raise FileNotFoundError(p)

            async def write_file(self, p, d):
                pass

            async def destroy(self):
                pass
        s._transport = _BlockingTransport()

        for k in range(K):
            # 1) interrupt while the tool is executing
            state["mode"], state["cycle"] = "cancel", k
            state["started"] = asyncio.Event()
            seen = []

            async def _consume(rpc):
                async for ev in s.execute_prompt("go", rpc_id=rpc):
                    seen.append(ev)

            consumer = asyncio.create_task(_consume(f"{sid}-c{k}"))
            await asyncio.wait_for(state["started"].wait(), timeout=5)
            await s.cancel_active_prompt()
            await asyncio.wait_for(consumer, timeout=5)
            assert seen[-1] == {"type": "done", "stop_reason": "cancelled"}, \
                (sid, k, seen[-1:])
            assert s._active_task is None, (sid, k, "leaked active task")
            assert _first_dangling(s._messages) is None, \
                (sid, k, "dangling after cancel -> would wedge")

            # 2) recovery prompt MUST succeed (not wedged)
            state["mode"] = "recover"
            seen2 = [ev async for ev
                     in s.execute_prompt("ok?", rpc_id=f"{sid}-r{k}")]
            assert seen2[-1] == {"type": "done", "stop_reason": "end_turn"}, \
                (sid, k, seen2[-1:])
            assert _first_dangling(s._messages) is None, (sid, k, "dangling")
        return s.session_id

    done = await asyncio.gather(*(run_session(f"sess-wedge-{i}")
                                  for i in range(M)))
    assert len(done) == M


@pytest.mark.asyncio
async def test_nway_concurrent_recovery_creates_exactly_one_no_orphans():
    """Scale the adopt-not-duplicate invariant: when a sandbox dies under load,
    MANY callers (every tool call in a turn + racing /sandbox/exec) can hit
    SandboxGoneError on the same dead transport at once. The _provision_lock +
    replace-check must still create EXACTLY ONE replacement per session — a
    second creation is an orphaned sandbox, i.e. a paid idle VM on daytona/modal
    leaking until the reaper reclaims it. Stresses N racing recoveries across M
    sessions; asserts one creation each and no deadlock."""
    N, M = 24, 6   # racing recoveries per session, concurrent sessions

    async def one_session(sid):
        s = NativeSession(session_id=sid,
                          state=NativeSandboxState(provider="docker"))
        s._started = True

        async def _noop_persist():
            return None
        s._persist_state = _noop_persist  # type: ignore[method-assign]

        created = []

        class _T:
            def __init__(self, tag):
                self.ref = f"{sid}-cid-{tag}"
                self.container_id = self.ref

            async def destroy(self):
                pass

        async def _factory():
            # a tiny await so concurrent callers genuinely interleave inside the
            # lock-acquire window rather than each running to completion first
            await asyncio.sleep(0)
            t = _T(len(created))
            created.append(t)
            return t

        s._transport_factory = _factory
        dead = _T("dead")
        s._transport = dead

        results = await asyncio.gather(
            *(s._ensure_sandbox(replace=dead) for _ in range(N)))
        # exactly one sandbox created, and every caller got that same one
        assert len(created) == 1, (
            sid, f"orphan leak: {[c.ref for c in created]}")
        assert all(r is created[0] for r in results), (
            sid, "a caller got a non-adopted transport")
        assert s._transport is created[0]
        return sid

    out = await asyncio.gather(*(one_session(f"sess-orphan-{i}")
                                 for i in range(M)))
    assert len(out) == M


@pytest.mark.asyncio
async def test_ensure_sandbox_records_native_op_telemetry(monkeypatch):
    """Native provisions compute lazily in _ensure_sandbox (not the pool's
    timed_op around start()), so it must record its own op_events — otherwise
    /admin/ops is blind to native sandbox create/resume latency + flakiness.
    A fresh provision records cold_create; a recreate over a dead ref records
    cold_recover + a recovery signal (the silent auto-heal the dashboard counts)."""
    import contextlib as _ctx
    import api.metrics

    recorded = []

    class _FakeMetrics:
        @_ctx.asynccontextmanager
        async def timed_op(self, *, provider, operation, session_id=None):
            try:
                yield
            finally:
                recorded.append(("op", provider, operation))

        async def record_recovery(self, kind, *, provider=None,
                                  session_id=None, **ctx):
            recorded.append(("recovery", provider, kind))

    monkeypatch.setattr(api.metrics, "get_metrics", lambda: _FakeMetrics())

    class _T:
        def __init__(self, ref="cid-x"):
            self.ref = ref
            self.container_id = ref

        async def destroy(self):
            pass

        async def status(self):
            return "missing"

    async def _noop_persist():
        return None

    async def _create(provider):
        return _T("cid-fresh")

    # 1) fresh provision (no ref, no replace) -> cold_create, no recovery
    s = NativeSession(session_id="s-cc",
                      state=NativeSandboxState(provider="docker"))
    s._started = True
    s._persist_state = _noop_persist  # type: ignore[method-assign]
    s._create_transport = _create     # type: ignore[method-assign]
    t = await s._ensure_sandbox()
    assert t.ref == "cid-fresh"
    assert ("op", "docker", "cold_create") in recorded
    assert not any(r[0] == "recovery" for r in recorded)

    # 2) recreate over a dead/missing ref -> cold_recover + recovery
    recorded.clear()
    s2 = NativeSession(session_id="s-cr", state=NativeSandboxState(
        provider="docker", sandbox_ref="old-dead"))
    s2._started = True
    s2._persist_state = _noop_persist       # type: ignore[method-assign]
    s2._create_transport = _create          # type: ignore[method-assign]
    s2._reattach_transport = lambda provider, ref: _T("old-dead")  # type: ignore
    await s2._ensure_sandbox()
    assert ("op", "docker", "cold_recover") in recorded
    assert ("recovery", "docker", "cold_recover") in recorded


@pytest.mark.asyncio
async def test_destroy_and_hibernate_failures_record_leaks(monkeypatch):
    """A failed native destroy ORPHANS a paid sandbox; a failed hibernate leaves
    compute RUNNING. Both are resource leaks — they must surface on /metrics via
    record_leak (counted like the supervisor path's reap-release leaks), not just
    a log line."""
    import api.metrics

    leaks = []

    class _FakeMetrics:
        async def record_leak(self, kind, *, provider=None, session_id=None, **ctx):
            leaks.append((kind, provider, session_id))

    monkeypatch.setattr(api.metrics, "get_metrics", lambda: _FakeMetrics())

    # destroy failure -> leak (orphaned sandbox)
    s = NativeSession(session_id="s-del", state=NativeSandboxState(
        provider="daytona", sandbox_ref="ref-x"))

    class _DeadDestroy:
        async def destroy(self):
            raise RuntimeError("delete refused")
    s._reattach_transport = lambda provider, ref: _DeadDestroy()  # type: ignore
    await s.destroy()
    assert ("native_destroy_failed", "daytona", "s-del") in leaks
    assert s._transport is None   # still tears down the in-memory handle

    # hibernate failure -> leak (compute not freed)
    leaks.clear()
    s2 = NativeSession(session_id="s-hib",
                       state=NativeSandboxState(provider="modal"))

    class _DeadHibernate:
        async def hibernate(self):
            raise RuntimeError("stop refused")
    s2._transport = _DeadHibernate()
    await s2.stop()
    assert ("native_hibernate_failed", "modal", "s-hib") in leaks


@pytest.mark.asyncio
async def test_checkpoint_retries_transient_db_failure(monkeypatch):
    """A transient DB failure on the checkpoint write would silently rewind the
    conversation on resume, so _checkpoint retries the idempotent upsert: it
    succeeds once the DB recovers, and gives up gracefully (never raises) if it
    can't — a checkpoint hiccup must not crash the turn loop."""
    import api.db

    captured = {}
    calls = {"n": 0}

    async def flaky_write(*, session_id, turn_seq, messages, usage):
        calls["n"] += 1
        if calls["n"] < 3:            # fail the first two attempts
            raise RuntimeError("connection reset")
        captured["ok"] = (session_id, turn_seq)

    monkeypatch.setattr(api.db, "write_native_checkpoint", flaky_write)

    s = NativeSession(session_id="s-ckpt",
                      state=NativeSandboxState(provider="docker"))
    s._turn_seq = 4
    s._messages = [{"role": "user", "content": "hi"}]

    await s._checkpoint({"inputTokens": 1})
    assert calls["n"] == 3                       # retried twice, then succeeded
    assert captured["ok"] == ("s-ckpt", 4)

    # exhausting all attempts must NOT raise (the turn loop must survive)
    calls["n"] = 0

    async def always_fail(**kw):
        calls["n"] += 1
        raise RuntimeError("db down")

    monkeypatch.setattr(api.db, "write_native_checkpoint", always_fail)
    await s._checkpoint({})                       # no exception escapes
    assert calls["n"] == NativeSession._CHECKPOINT_ATTEMPTS


@pytest.mark.asyncio
async def test_no_session_or_task_leak_across_lifecycle_churn():
    """A per-session leak — a lingering _drive task, a closure, or a registry
    ref pinning the session — would silently grow RAM under session churn and
    break scalability. After many create/drive/shutdown/del cycles, no
    NativeSession and no extra asyncio task may remain alive. Object counts are
    deterministic after gc.collect(), so this is a robust guard (no byte-level
    flakiness)."""
    import gc

    def live_native_sessions() -> int:
        gc.collect()
        return sum(1 for o in gc.get_objects() if isinstance(o, NativeSession))

    def live_tasks() -> int:
        return sum(1 for t in asyncio.all_tasks() if not t.done())

    async def one_cycle(i: int) -> None:
        s = _make_session([[
            _Chunk(_Delta(content="hi")),
            _Chunk(usage=_Usage(5, 2)),
        ]])
        s._broadcast = lambda item: None
        async for _ev in s.execute_prompt(f"go-{i}", rpc_id=f"r{i}"):
            pass
        await s.shutdown()
        # s drops out of scope on return -> eligible for GC

    # warmup so any one-time module caches are populated, then snapshot baseline
    for i in range(3):
        await one_cycle(i)
    base_sessions = live_native_sessions()
    base_tasks = live_tasks()

    for i in range(30):
        await one_cycle(1000 + i)

    leaked_sessions = live_native_sessions() - base_sessions
    leaked_tasks = live_tasks() - base_tasks
    assert leaked_sessions <= 0, \
        f"{leaked_sessions} NativeSession(s) leaked across 30 lifecycle cycles"
    assert leaked_tasks <= 0, \
        f"{leaked_tasks} asyncio task(s) leaked (lingering _drive?)"


@pytest.mark.asyncio
async def test_no_per_prompt_task_leak_in_a_long_live_session():
    """A LIVE session running many prompts back-to-back (no shutdown between)
    must not accumulate _drive tasks — only the conversation grows. The
    lifecycle-churn guard destroys+shuts-down each session, so shutdown's
    cancel would mask a per-prompt task leak; this drives ONE live session
    across many prompts to catch it directly."""
    import gc

    def live_tasks() -> int:
        gc.collect()
        return sum(1 for t in asyncio.all_tasks() if not t.done())

    s = NativeSession(session_id="long-live",
                      state=NativeSandboxState(provider="docker"))
    s._started = True
    s._spec = NativeAgentSpec(instructions="sys", max_turns=2)
    s._tools = {}
    s._messages = [{"role": "system", "content": "sys"}]
    s._broadcast = lambda item: None

    async def _noop_ckpt(usage):
        return None
    s._checkpoint = _noop_ckpt  # type: ignore[method-assign]

    async def completion(**kwargs):
        async def _it():
            yield _Chunk(_Delta(content="ok"))
            yield _Chunk(usage=_Usage(5, 2))
        return _it()
    s._completion = completion

    # warmup, then baseline; each prompt's _drive task must be done (GC'd) after
    for i in range(20):
        async for _ in s.execute_prompt(f"w{i}", rpc_id=f"w{i}"):
            pass
    base = live_tasks()

    for i in range(200):
        async for _ in s.execute_prompt(f"p{i}", rpc_id=f"p{i}"):
            pass

    assert live_tasks() <= base, "per-prompt _drive task leaked in a live session"
    # _active_task is cleared after each prompt completes
    assert s._active_task is None


@pytest.mark.asyncio
async def test_parallel_tools_concurrent_sandbox_recovery_one_replacement():
    """When a round's parallel tool calls all hit SandboxGoneError on the shared
    transport, they recover CONCURRENTLY — and must converge on exactly ONE
    replacement sandbox (no orphan leak), each retrying on it and returning a
    result, in order. Composition of the parallel-tool execution + the
    adopt-not-duplicate recovery."""
    from api.native.transport import SandboxGoneError, TransportExecResult

    class _Dead:
        ref = "dead"

        async def exec(self, *a, **k):
            raise SandboxGoneError("sandbox gone")

        async def destroy(self):
            pass

    class _Fresh:
        def __init__(self, tag):
            self.ref = f"fresh-{tag}"
            self.container_id = self.ref

        async def exec(self, command, *, cwd=None, env=None, timeout_s=300):
            return TransportExecResult(f"ran:{command}", "", 0, False)

        async def destroy(self):
            pass

    s = NativeSession(session_id="par-rec",
                      state=NativeSandboxState(provider="docker"))
    s._started = True
    s._spec = NativeAgentSpec(max_turns=4)
    s._tools = build_toolset(["bash"])
    s._messages = [{"role": "system", "content": "sys"}]
    s._broadcast = lambda item: None
    s._transport = _Dead()

    created = []

    async def _factory():
        # yield inside provisioning so the concurrent recoveries genuinely
        # interleave in the lock-acquire window (else scheduling can serialize
        # them and the pre-lock adopt-check masks an absent under-lock one).
        await asyncio.sleep(0)
        t = _Fresh(len(created))
        created.append(t)
        return t
    s._transport_factory = _factory

    async def _noop_persist():
        return None
    s._persist_state = _noop_persist  # type: ignore[method-assign]

    async def _noop_ckpt(usage):
        return None
    s._checkpoint = _noop_ckpt  # type: ignore[method-assign]

    n_calls = {"n": 0}

    async def completion(**kwargs):
        n_calls["n"] += 1
        if n_calls["n"] == 1:
            async def _it():
                yield _Chunk(_Delta(tool_calls=[
                    _TCDelta(i, id=f"c{i}", name="bash",
                             arguments='{"command":"x%d"}' % i)
                    for i in range(3)]))
            return _it()

        async def _done():
            yield _Chunk(_Delta(content="done"))
        return _done()
    s._completion = completion

    events = [ev async for ev in s.execute_prompt("go", rpc_id="r")]

    results = [e for e in events if e["type"] == "tool_result"]
    assert [e["tool_call_id"] for e in results] == ["c0", "c1", "c2"]
    assert all("ran:" in e["result"] for e in results), \
        "a parallel tool didn't recover"
    assert len(created) == 1, \
        f"concurrent recovery leaked sandboxes: {[c.ref for c in created]}"
    assert s._transport is created[0]
