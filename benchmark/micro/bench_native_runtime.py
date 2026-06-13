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
import os
import time
import tracemalloc

from api.native.loop import NativeAgentSpec
from api.native.session import NativeSession
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
    await _scaling(levels, turns, chunks)
    await _ram(max(levels), turns, chunks)


if __name__ == "__main__":
    asyncio.run(main())
