"""Native runtime throughput + RAM — end to end, no LLM credits, no sandbox.

**Question:** how fast can the native runtime drive turns, how does it scale
with concurrent sessions, and how much RAM does an idle-but-active session
hold? This drives ``NativeSession.execute_prompt`` through its REAL path —
loop → ``emit`` → frame synthesis → ``_broadcast`` → internal queue → generator
yield → per-turn checkpoint — using the ``_completion`` test seam to stand in
for ``litellm.acompletion`` (a fake stream of text chunks). No network, no
provider, no API key, so it runs anywhere and costs nothing, while still
exercising the exact server-side machinery a real turn pays for.

It is the native counterpart to the supervisor-path ``load/`` benches: those
need a running uvicorn + a real agent; this isolates the in-process runtime
cost that multiplies across every concurrent session on a replica.

Views: single-session text-turn rate, **tool-heavy** (a tool-call/tool-result
loop — 2 model rounds + a tool exec per turn, exercising the tool/tool_result
frames and the streamed tool-arg accumulator), **parallel tool-calling** (a round
of N tool calls runs concurrently, bounded to a cap — turn time stays flat up to
the cap, then grows in waves, vs the N× sequential cost), concurrency scaling,
with-subscriber fan-out, session-length
scaling, and per-session RAM.

**Run:** ``.venv/bin/python benchmark/micro/bench_native_runtime.py``
Knobs (env): ``TURNS`` (per session, default 200), ``CHUNKS`` (text deltas per
turn, default 60), ``LEVELS`` (concurrency points, default ``1,2,4,8,16,32``).

**What good looks like:** turns/sec roughly flat (per-session cost stays
constant) as concurrency rises until the single event-loop thread saturates;
per-session RAM small and constant. Regressions here = a per-event allocation
or O(history) cost sneaking into the turn path.
"""

from __future__ import annotations

import asyncio
import gc
import json
import os
import time
import tracemalloc

from api.native.loop import NativeAgentSpec
from api.native.session import NativeSession
from api.native.tools import build_toolset
from api.sandbox.state import NativeSandboxState


# ── fake LLM stream (the _completion seam) ──────────────────────────────────
class _Delta:
    __slots__ = ("content", "reasoning_content", "tool_calls")

    def __init__(self, content=None):
        self.content = content
        self.reasoning_content = None
        self.tool_calls = None


class _Choice:
    __slots__ = ("delta",)

    def __init__(self, delta):
        self.delta = delta


class _Chunk:
    __slots__ = ("choices", "usage")

    def __init__(self, delta=None, usage=None):
        self.choices = [_Choice(delta)] if delta is not None else []
        self.usage = usage


class _Usage:
    __slots__ = ("prompt_tokens", "completion_tokens", "response_cost")

    def __init__(self, p, c):
        self.prompt_tokens = p
        self.completion_tokens = c
        self.response_cost = 0.0


_TOKENS = ["Hello", " world", ",", " this", " is", " a", " streamed",
           " token", "!", "\n", " 漢字", " 🚀"]


def _completion_for(chunks: int):
    """A _completion that streams ``chunks`` text deltas + a usage chunk on
    EVERY call (one call per turn — no tool calls, so the turn ends)."""
    async def completion(**kwargs):
        async def _it():
            for i in range(chunks):
                yield _Chunk(_Delta(content=_TOKENS[i % len(_TOKENS)]))
            yield _Chunk(usage=_Usage(100, chunks))
        return _it()
    return completion


# ── tool-heavy shape: a tool-call/tool-result loop (the realistic agent) ─────
class _TCDelta:
    __slots__ = ("index", "id", "function")

    def __init__(self, index, id=None, name=None, arguments=None):
        self.index = index
        self.id = id
        self.function = type("F", (), {"name": name, "arguments": arguments})()


class _ToolDelta:
    __slots__ = ("content", "reasoning_content", "tool_calls")

    def __init__(self, tool_calls):
        self.content = None
        self.reasoning_content = None
        self.tool_calls = tool_calls


class _ExecResult:
    __slots__ = ("stdout", "stderr", "exit_code", "timed_out")

    def __init__(self):
        self.stdout, self.stderr, self.exit_code, self.timed_out = "ok", "", 0, False


class _FakeTransport:
    async def exec(self, command, *, cwd=None, env=None, timeout_s=300):
        return _ExecResult()


def _tool_completion(arg_size: int):
    """Each tool-heavy turn = 2 model rounds: round 1 streams a bash tool_call
    (its argument split into 8-char fragments, exercising the tool-arg
    accumulator), round 2 returns text + usage → done. Drives the tool +
    tool_result frame templates and the loop's tool-execution path."""
    state = {"n": 0}

    async def completion(**kwargs):
        state["n"] += 1
        if state["n"] % 2 == 1:
            payload = json.dumps({"command": "echo " + "x" * arg_size})
            frags = [payload[i:i + 8] for i in range(0, len(payload), 8)]

            async def _it():
                yield _Chunk(_ToolDelta([_TCDelta(0, id=f"c{state['n']}",
                                                  name="bash")]))
                for f in frags:
                    yield _Chunk(_ToolDelta([_TCDelta(0, arguments=f)]))
            return _it()

        async def _done():
            yield _Chunk(_Delta(content="done"))
            yield _Chunk(usage=_Usage(100, 10))
        return _done()
    return completion


def _make_tool_session(sid: str, arg_size: int) -> NativeSession:
    s = NativeSession(session_id=sid,
                      state=NativeSandboxState(provider="docker"))
    s._started = True
    s._spec = NativeAgentSpec(instructions="sys", max_turns=4)
    s._tools = build_toolset(["bash"])
    s._transport = _FakeTransport()
    s._messages = [{"role": "system", "content": "sys"}]
    s._completion = _tool_completion(arg_size)

    async def _noop_ckpt(usage):
        return None
    s._checkpoint = _noop_ckpt  # type: ignore[method-assign]
    return s


# ── parallel tool-calling: N independent tool calls in ONE round ─────────────
class _LatencyTransport:
    """exec() sleeps a fixed latency, standing in for a real sandbox exec round
    trip — so concurrent vs sequential tool execution is visible."""

    def __init__(self, latency_s: float):
        self.latency_s = latency_s

    async def exec(self, command, *, cwd=None, env=None, timeout_s=300):
        await asyncio.sleep(self.latency_s)
        return _ExecResult()


def _parallel_tool_completion(n_tools: int):
    """Round 1 issues N bash tool_calls AT ONCE (parallel tool-calling); round 2
    returns text → done."""
    state = {"n": 0}

    async def completion(**kwargs):
        state["n"] += 1
        if state["n"] % 2 == 1:
            async def _it():
                yield _Chunk(_ToolDelta([
                    _TCDelta(i, id=f"c{state['n']}-{i}", name="bash",
                             arguments='{"command":"x"}')
                    for i in range(n_tools)]))
            return _it()

        async def _done():
            yield _Chunk(_Delta(content="done"))
            yield _Chunk(usage=_Usage(100, 10))
        return _done()
    return completion


def _make_parallel_tool_session(sid: str, n_tools: int,
                                latency_s: float) -> NativeSession:
    s = NativeSession(session_id=sid,
                      state=NativeSandboxState(provider="docker"))
    s._started = True
    s._spec = NativeAgentSpec(instructions="sys", max_turns=4)
    s._tools = build_toolset(["bash"])
    s._transport = _LatencyTransport(latency_s)
    s._messages = [{"role": "system", "content": "sys"}]
    s._completion = _parallel_tool_completion(n_tools)

    async def _noop_ckpt(usage):
        return None
    s._checkpoint = _noop_ckpt  # type: ignore[method-assign]
    return s


def _make_session(sid: str, chunks: int) -> NativeSession:
    s = NativeSession(session_id=sid,
                      state=NativeSandboxState(provider="docker"))
    s._started = True
    s._spec = NativeAgentSpec(instructions="sys", max_turns=2)
    s._tools = {}
    s._transport = None
    s._messages = [{"role": "system", "content": "sys"}]
    s._completion = _completion_for(chunks)

    async def _noop_ckpt(usage):
        return None
    s._checkpoint = _noop_ckpt  # type: ignore[method-assign]
    return s


async def _drive_turns(s: NativeSession, turns: int) -> int:
    """Run ``turns`` prompts, fully draining each. Returns total events."""
    events = 0
    for t in range(turns):
        async for _ev in s.execute_prompt("hi", rpc_id=f"rpc-{t}"):
            events += 1
    return events


# ── measurements ────────────────────────────────────────────────────────────
async def _single(turns: int, chunks: int) -> None:
    s = _make_session("sess-bench-single", chunks)
    # warmup
    await _drive_turns(s, 3)
    t0 = time.perf_counter()
    events = await _drive_turns(s, turns)
    dt = time.perf_counter() - t0
    print(f"single session: {turns} turns × {chunks} chunks")
    print(f"  {turns/dt:8.1f} turns/s   {events/dt/1e3:7.1f}k events/s   "
          f"{dt/turns*1e6:6.0f} µs/turn\n")


async def _parallel_tools(latency_ms: int = 20,
                          tool_counts: tuple[int, ...] = (1, 2, 4, 8, 16)) -> None:
    """Per-turn wall time as the model issues N tool calls in ONE round, each a
    ``latency_ms`` exec. The native loop runs them concurrently, BOUNDED to
    _MAX_CONCURRENT_TOOLS at a time: the turn stays ~flat (≈ one latency) up to
    the cap, then grows in waves of cap-size (so the resource ceiling is fixed).
    seq_est = N×latency, what fully-sequential execution would cost."""
    from api.native.loop import _MAX_CONCURRENT_TOOLS as cap

    L = latency_ms / 1000.0
    print(f"parallel tool-calling: a round of N bash calls, {latency_ms}ms each "
          f"(concurrency cap {cap})")
    print(f"  {'tools/round':>11} {'turn ms':>8} {'seq est ms':>11} {'speedup':>8}")
    for n in tool_counts:
        s = _make_parallel_tool_session(f"par-{n}", n, L)
        await _drive_turns(s, 2)
        turns = 20
        t0 = time.perf_counter()
        await _drive_turns(s, turns)
        dt = (time.perf_counter() - t0) / turns
        seq_est = n * L
        print(f"  {n:>11} {dt*1e3:>8.1f} {seq_est*1e3:>11.1f} {seq_est/dt:>7.1f}x")
    print(f"  (flat ≈ one latency up to N={cap}; beyond, waves of {cap} = a fixed "
          f"resource ceiling)\n")


async def _tool_heavy(turns: int, arg_sizes: tuple[int, ...] = (16, 4096)) -> None:
    """Throughput of the tool-call/tool-result loop (2 model rounds + a tool
    exec per turn). Exercises the tool/tool_result frame templates and the
    streamed tool-arg accumulator — the paths the text-only single() never
    touches. arg_size sweeps a small vs a large streamed tool argument."""
    print(f"tool-heavy turns (2 model rounds + 1 tool exec each): {turns} turns")
    print(f"  {'arg bytes':>10} {'turns/s':>10} {'ms/turn':>9}")
    for arg in arg_sizes:
        s = _make_tool_session("sess-tool", arg)
        await _drive_turns(s, 3)
        t0 = time.perf_counter()
        await _drive_turns(s, turns)
        dt = time.perf_counter() - t0
        print(f"  {arg:>10} {turns/dt:>10.1f} {dt/turns*1e3:>9.2f}")
    print()


async def _scaling(levels: list[int], turns: int, chunks: int) -> None:
    # The native runtime runs on ONE event-loop thread, so concurrency does not
    # add aggregate throughput — the right scalability question is whether
    # aggregate throughput HOLDS (no contention cliff) as sessions pile on. The
    # "retention" column is aggregate turns/s vs the 1-session baseline: ~100%
    # means clean sharing of the core (and clean horizontal scaling across
    # replicas); a drop means per-session contention crept into the turn path.
    print(f"concurrency scaling: {turns} turns × {chunks} chunks per session")
    print(f"  {'sessions':>8} {'agg turns/s':>12} {'agg events/s':>13} "
          f"{'ms/turn(p)':>11} {'retention':>10}")
    base = None
    for n in levels:
        sessions = [_make_session(f"sess-{n}-{i}", chunks) for i in range(n)]
        # warmup all
        await asyncio.gather(*(_drive_turns(s, 2) for s in sessions))
        gc.collect()
        t0 = time.perf_counter()
        results = await asyncio.gather(*(_drive_turns(s, turns) for s in sessions))
        dt = time.perf_counter() - t0
        total_turns = turns * n
        total_events = sum(results)
        tps = total_turns / dt
        if base is None:
            base = tps
        retention = tps / base  # ~1.0 = aggregate throughput held flat (good)
        # per-session wall per turn (each session shares the one core): dt is
        # the wall for `turns` turns experienced concurrently by every session.
        print(f"  {n:>8} {tps:>12.1f} {total_events/dt:>13.0f} "
              f"{dt/turns*1e3:>11.1f} {retention:>9.0%}")
    print()


async def _with_subscribers(turns: int, chunks: int,
                            sub_levels: tuple[int, ...] = (0, 1, 2, 4)) -> None:
    """Throughput with N concurrently-draining /events subscribers attached.

    Production /message turns ALWAYS have ≥1 streaming subscriber (the HTTP
    response drains broadcast blocks), so the 0-subscriber single() number is an
    underestimate — it skips the per-event ``put_nowait`` fan-out. Each level
    registers N subscribers, drains them in background tasks (the SSE consumer
    stand-in), and reports producer turns/s plus whether any event was DROPPED
    (the subscriber queue is bounded at _QUEUE_MAXSIZE; a too-slow drain loses
    events by design — worth seeing if it happens under this load)."""
    from api.sandbox.session import _HEARTBEAT

    print(f"with-subscriber fan-out: {turns} turns × {chunks} chunks")
    print(f"  {'subs':>5} {'turns/s':>10} {'events/s':>11} {'delivered/sub':>14} "
          f"{'drops':>7}")
    base = None
    for nsub in sub_levels:
        s = _make_session(f"sess-sub-{nsub}", chunks)
        await _drive_turns(s, 2)  # warmup
        counters = [0] * nsub
        drains = []
        for k in range(nsub):
            sid, q = s.register_subscriber()

            async def drain(sid=sid, q=q, k=k):
                async for item in s.iterate_subscriber(sid, q):
                    if item is not _HEARTBEAT:
                        counters[k] += 1
            drains.append(asyncio.create_task(drain()))

        gc.collect()
        t0 = time.perf_counter()
        events = await _drive_turns(s, turns)
        dt = time.perf_counter() - t0
        # let the drains fully catch up before measuring delivery
        for _ in range(50):
            await asyncio.sleep(0)
        for d in drains:
            d.cancel()
        tps = turns / dt
        if base is None and nsub == 0:
            base = tps
        delivered = (sum(counters) // nsub) if nsub else 0
        # each subscriber should see every broadcast event (events/turn×turns)
        drops = (events - delivered) if nsub else 0
        rel = f"  ({tps/base:.0%} of 0-sub)" if base else ""
        print(f"  {nsub:>5} {tps:>10.1f} {events/dt:>11.0f} {delivered:>14} "
              f"{drops:>7}{rel}")
    print("  (drops should be 0 at this load; >0 = bounded queue overflow)\n")


async def _session_growth(turns: int, chunks: int, buckets: int = 4) -> None:
    """Per-turn latency of the in-memory LOOP as ONE session's conversation
    deepens. With an O(1) per-turn cost the buckets stay flat; an O(n) per-turn
    step (e.g. re-scanning the whole transcript — the heal / tool-arg-accum bug
    class) shows up as later buckets getting slower, i.e. secretly O(n²). This is
    the canary for that LOOP bug class ONLY: the checkpoint is stubbed here
    (``_make_session`` installs ``_noop_ckpt``), so the known O(n²) write-volume
    cost is invisible to this view — ``_checkpoint_serialization`` measures that."""
    s = _make_session("sess-growth", chunks)
    await _drive_turns(s, 3)  # warmup
    per = max(1, turns // buckets)
    print(f"session-length scaling: per-turn cost as one session grows "
          f"({turns} turns × {chunks} chunks)")
    print(f"  {'turns so far':>13} {'µs/turn':>9} {'msgs':>7}")
    done = 0
    first = None
    for b in range(buckets):
        t0 = time.perf_counter()
        await _drive_turns(s, per)
        dt = time.perf_counter() - t0
        done += per
        us = dt / per * 1e6
        if first is None:
            first = us
        print(f"  {done:>13} {us:>9.0f} {len(s._messages):>7}  "
              f"{'×%.2f' % (us / first)}")
    print("  (flat = O(1) per turn; rising = a hidden O(n²) over the session)\n")


async def _checkpoint_serialization(turns: int, chunks: int, buckets: int = 5) -> None:
    """The per-turn checkpoint cost the OTHER views can't see. The real
    ``_checkpoint`` serializes the FULL ``_messages`` array to JSONB EVERY turn
    (``db.write_native_checkpoint`` via psycopg's ``Json`` adapter), so both its
    CPU and the bytes written to Postgres grow O(n) per turn → O(n²) over the
    session (docs/native_checkpoint_writevolume_design.md). Every other view
    stubs the checkpoint (``_noop_ckpt``, no DB), so ``_session_growth``'s
    "flat = O(1)" canary structurally CANNOT see this one. Here we measure
    ``json.dumps(_messages)`` — what the ``Json`` adapter does — per bucket:
    rising serialize-µs + payload-KB quantify whether the deferred
    delta-checkpoint migration is worth it at realistic conversation lengths.

    NOTE: synthetic messages here are small (a few chars), so the absolute µs is
    a LOWER BOUND — real turns carry tool args / file contents. The SHAPE (linear
    in msg count, flat µs/msg) is the signal, and it's payload-size-independent."""
    import json
    s = _make_session("sess-ckpt", chunks)
    await _drive_turns(s, 3)  # warmup
    per = max(1, turns // buckets)
    print(f"checkpoint serialization cost as one session grows "
          f"({turns} turns × {chunks} chunks)")
    print(f"  {'turns so far':>13} {'msgs':>6} {'serialize µs':>13} "
          f"{'payload KB':>11} {'µs/msg':>8}")
    done = 0
    for _b in range(buckets):
        await _drive_turns(s, per)
        done += per
        reps = 5
        t0 = time.perf_counter()
        for _ in range(reps):
            blob = json.dumps(s._messages)
        ser_us = (time.perf_counter() - t0) / reps * 1e6
        n = len(s._messages)
        print(f"  {done:>13} {n:>6} {ser_us:>13.0f} {len(blob)/1024:>11.1f} "
              f"{ser_us/n:>8.2f}")
    print("  (serialize µs + payload KB rise ~linearly with msgs → O(n²) write-"
          "volume over the session; µs/msg ~flat = it's array size, not per-msg)\n")


async def _ram(n: int, turns: int, chunks: int) -> None:
    """Per-session resident RAM after a conversation of ``turns`` turns."""
    gc.collect()
    tracemalloc.start()
    base = tracemalloc.take_snapshot()
    sessions = [_make_session(f"sess-ram-{i}", chunks) for i in range(n)]
    await asyncio.gather(*(_drive_turns(s, turns) for s in sessions))
    gc.collect()
    after = tracemalloc.take_snapshot()
    stats = after.compare_to(base, "filename")
    total = sum(s.size_diff for s in stats)
    tracemalloc.stop()
    msgs = len(sessions[0]._messages)
    print(f"RAM: {n} sessions × {turns} turns ({msgs} msgs each retained)")
    print(f"  {total/1024/1024:6.2f} MB total   "
          f"{total/n/1024:6.1f} KB/session   "
          f"(conversation held in _messages)\n")
    # keep sessions referenced until here so the snapshot sees them
    del sessions


async def main() -> None:
    turns = int(os.environ.get("TURNS", "200"))
    chunks = int(os.environ.get("CHUNKS", "60"))
    levels = [int(x) for x in os.environ.get("LEVELS", "1,2,4,8,16,32").split(",")]
    print(f"native runtime bench — turns={turns} chunks/turn={chunks}\n")
    await _single(turns, chunks)
    await _tool_heavy(turns)
    await _parallel_tools()
    await _scaling(levels, turns, chunks)
    await _with_subscribers(turns, chunks)
    await _session_growth(max(turns, 1200), chunks)
    await _checkpoint_serialization(max(turns, 1200), chunks)
    await _ram(max(levels), turns, chunks)


if __name__ == "__main__":
    asyncio.run(main())
