"""modal exec is async (no threadpool ceiling) + output is RAM-bounded.

exec_in_sandbox used `asyncio.to_thread(sync_exec)`, capping concurrent execs
at the shared threadpool (~min(32, cpu+4) workers). It now uses modal's async
API (`exec.aio` / `wait.aio` / async stream iteration) — verified ~1.4x more
throughput at 40 concurrent execs. Output is still bounded WHILE reading via
`_read_stream_capped_async`. This unit-tests the async bounded reader; the live
async-exec path is verified separately.
"""
from __future__ import annotations

import pytest

from api.providers.modal import _read_stream_capped_async
from api.providers._shared import _MAX_OUTPUT_BYTES

pytestmark = pytest.mark.asyncio


class _AsyncCountingStream:
    """Async-iterable stream tracking chunks pulled + whether it was aclosed."""

    def __init__(self, chunk, n_chunks: int):
        self._chunk = chunk
        self._n = n_chunks
        self.pulled = 0
        self.closed = False

    async def __aiter__(self):
        for _ in range(self._n):
            self.pulled += 1
            yield self._chunk

    async def aclose(self):
        self.closed = True


async def test_async_capped_read_stops_early_and_closes():
    stream = _AsyncCountingStream(b"x" * 1024, 10_000)  # ~10 MiB available
    text, truncated = await _read_stream_capped_async(stream, _MAX_OUTPUT_BYTES)
    assert truncated is True
    assert len(text) == _MAX_OUTPUT_BYTES
    assert stream.pulled < 10_000, "must stop reading once over the cap (bounded RAM)"
    assert stream.closed is True, "must aclose the stream after an early break"


async def test_async_capped_read_full_when_small():
    stream = _AsyncCountingStream("hi\n", 3)  # modal yields str chunks
    text, truncated = await _read_stream_capped_async(stream, _MAX_OUTPUT_BYTES)
    assert text == "hi\n" * 3 and truncated is False
    assert stream.pulled == 3 and stream.closed is True


async def test_async_capped_read_empty():
    stream = _AsyncCountingStream(b"", 0)
    text, truncated = await _read_stream_capped_async(stream, _MAX_OUTPUT_BYTES)
    assert text == "" and truncated is False
