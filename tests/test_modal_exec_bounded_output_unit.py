"""modal exec output read must be RAM-bounded, not read-all-then-truncate.

Modal's StreamReader.read() fetches the ENTIRE stream until EOF, so a
huge-output command (cat biglog, find /) buffered all of it in server RAM
before the 1 MiB truncation. _read_stream_capped iterates and STOPS once it
exceeds the cap — verified sync-iterable on a real sandbox — so peak RAM is
bounded regardless of how much the command emitted.

Pure-logic tests (mock stream); the live mechanism was verified separately.
"""
from __future__ import annotations

import pytest

from api.providers.modal import _read_stream_capped
from api.providers._shared import _MAX_OUTPUT_BYTES


class _CountingStream:
    """Iterable stream that records how many chunks were actually pulled —
    so a test can assert the reader STOPPED early (didn't drain it all)."""

    def __init__(self, chunk: bytes | str, n_chunks: int):
        self._chunk = chunk
        self._n = n_chunks
        self.pulled = 0

    def __iter__(self):
        for _ in range(self._n):
            self.pulled += 1
            yield self._chunk


def test_capped_read_stops_early_on_huge_output():
    # 10000 chunks of 1 KiB = ~10 MiB available; cap is 1 MiB.
    stream = _CountingStream(b"x" * 1024, 10_000)
    text, truncated = _read_stream_capped(stream, _MAX_OUTPUT_BYTES)

    assert truncated is True
    assert len(text) == _MAX_OUTPUT_BYTES, "output trimmed to exactly the cap"
    # The crux: it must NOT have pulled all 10000 chunks — peak RAM is bounded.
    assert stream.pulled < 10_000, "must stop reading once over the cap"
    assert stream.pulled <= (_MAX_OUTPUT_BYTES // 1024) + 1, "stops ~at the cap"


def test_capped_read_returns_all_when_under_cap():
    stream = _CountingStream(b"hello\n", 3)
    text, truncated = _read_stream_capped(stream, _MAX_OUTPUT_BYTES)
    assert text == "hello\n" * 3
    assert truncated is False
    assert stream.pulled == 3, "small output is read in full"


def test_capped_read_handles_str_chunks_and_empty():
    # modal yields str chunks (verified live)
    text, truncated = _read_stream_capped(_CountingStream("abc", 2), _MAX_OUTPUT_BYTES)
    assert text == "abcabc" and truncated is False
    # empty stream
    empty, trunc = _read_stream_capped(_CountingStream(b"", 0), _MAX_OUTPUT_BYTES)
    assert empty == "" and trunc is False
