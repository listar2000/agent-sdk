"""The native transport's base64 codec is loop-isolated for large payloads.

read_file/write_file move bytes as base64 over the exec channel (docker/daytona/
modal). base64 holds the GIL, so a multi-MB transfer encoded/decoded INLINE
freezes the event loop for every other session on the replica. The transport
offloads the codec to a worker thread above ``_B64_THREAD_THRESHOLD`` — not a
speedup (GIL), purely keeping the loop responsive (the win measured in
benchmark/micro/bench_b64.py). These tests pin both correctness and that
isolation property, backend-free (the helpers are pure).
"""

from __future__ import annotations

import asyncio
import base64

import pytest

from api.native import transport as tr


@pytest.mark.asyncio
async def test_b64_roundtrip_small_and_large():
    for n in (0, 16, tr._B64_THREAD_THRESHOLD - 1, tr._B64_THREAD_THRESHOLD,
              tr._B64_THREAD_THRESHOLD * 2 + 7):
        data = b"\x00\x01\x02hello\xff" * (max(1, n // 8))
        enc = await tr._b64encode(data)
        assert enc == base64.b64encode(data)
        assert await tr._b64decode(enc) == data
        # decode also accepts the str form the daytona/modal path produces
        assert await tr._b64decode(enc.decode()) == data


async def _ticks_during(coro):
    """Run ``coro`` while a cooperative ticker spins; return (result, ticks).
    A high tick count means the event loop stayed free during ``coro``."""
    ticks = [0]
    stop = [False]

    async def ticker():
        while not stop[0]:
            ticks[0] += 1
            await asyncio.sleep(0)

    t = asyncio.create_task(ticker())
    await asyncio.sleep(0)
    start = ticks[0]
    result = await coro
    during = ticks[0] - start
    stop[0] = True
    await t
    return result, during


@pytest.mark.asyncio
async def test_large_b64_encode_keeps_loop_free():
    """A >threshold encode runs off-thread, so a concurrent coroutine keeps
    making progress (loop not blocked). Inline (sub-threshold reference) the
    loop is frozen for the whole codec — proving the test actually discriminates."""
    big = b"x" * (8 * 1024 * 1024)        # 8MB > 4MB threshold -> to_thread
    enc, during_threaded = await _ticks_during(tr._b64encode(big))
    assert enc == base64.b64encode(big)
    assert during_threaded > 5, (
        f"loop blocked during large off-thread b64 ({during_threaded} ticks)")

    # Reference: force the SAME work inline (no thread) and confirm the loop is
    # frozen through it — i.e. removing the offload would regress to ~0 ticks.
    async def _inline():
        return base64.b64encode(big)
    _, during_inline = await _ticks_during(_inline())
    assert during_inline <= 1, (
        f"inline b64 should freeze the loop, saw {during_inline} ticks")
    assert during_threaded > during_inline * 5
