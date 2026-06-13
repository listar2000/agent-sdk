"""Native tool-output cap — a conversation-RAM + prompt-cost guard.

A native tool result is appended to ``messages`` (the in-memory conversation)
and re-sent to the model on EVERY subsequent round of the turn. So a chatty
``bash`` command or a large ``read_file`` must be bounded by the tool BEFORE it
enters that array — otherwise one noisy command inflates the context (RAM + per
-turn token cost) for the rest of the session. ``_cap`` is that guard and was
previously untested; these pin its contract (the loop's RAM ask depends on it).

The transport's ``max_bytes`` bounds the RAW read (tested in
test_native_transport_b64.py); ``_cap`` is the SECOND, char-based bound on what
the model actually sees. Both matter: the first protects transfer/decode, the
second protects the conversation array.
"""

from __future__ import annotations

import pytest

from api.native.tools import _cap, _RESULT_CHAR_CAP, _bash, _read_file
from api.native.transport import TransportExecResult


# ── the pure guard ──────────────────────────────────────────────────────────

def test_cap_passes_through_under_limit_without_copy():
    s = "x" * (_RESULT_CHAR_CAP - 1)
    # under budget → returned as-is (no slice/alloc on the common path)
    assert _cap(s) is s


def test_cap_exactly_at_limit_passes_through():
    s = "x" * _RESULT_CHAR_CAP
    assert _cap(s) is s


def test_cap_truncates_over_limit_and_stays_bounded():
    over = _RESULT_CHAR_CAP + 50_000
    s = "x" * over
    out = _cap(s)
    # the whole point: the result the model sees is bounded regardless of how
    # large the underlying output was
    assert len(out) <= _RESULT_CHAR_CAP
    assert out.startswith("x" * 1000)            # head preserved
    assert "truncated" in out
    # the notice reports exactly how many chars were dropped
    dropped = over - (_RESULT_CHAR_CAP - 80)
    assert f"truncated {dropped} chars" in out


# ── through the tools (what actually lands in ``messages``) ──────────────────

class _StubExecTransport:
    def __init__(self, stdout: str):
        self._stdout = stdout

    async def exec(self, command, *, cwd=None, timeout_s=300):
        return TransportExecResult(stdout=self._stdout, stderr="",
                                   exit_code=0, timed_out=False)


class _StubFileTransport:
    def __init__(self, data: bytes):
        self._data = data

    async def read_file(self, path, *, max_bytes=8 * 1024 * 1024):
        return self._data


@pytest.mark.asyncio
async def test_bash_caps_chatty_output_before_conversation():
    """A command emitting far more than the cap yields a bounded tool result —
    the full stdout never reaches ``messages``."""
    t = _StubExecTransport("A" * (_RESULT_CHAR_CAP * 4))
    out = await _bash(t, {"command": "cat hugefile"})
    assert len(out) <= _RESULT_CHAR_CAP
    assert "truncated" in out


@pytest.mark.asyncio
async def test_read_file_caps_large_file_before_conversation():
    t = _StubFileTransport(("B" * (_RESULT_CHAR_CAP * 2)).encode())
    out = await _read_file(t, {"path": "/big.txt"})
    assert len(out) <= _RESULT_CHAR_CAP
    assert "truncated" in out


@pytest.mark.asyncio
async def test_read_file_non_utf8_returns_clean_error_not_mojibake():
    """A binary file must surface a clean error the model can act on, not a
    decoded blob — and certainly not crash the turn."""
    t = _StubFileTransport(b"\xff\xfe\x00\x01\x02 binary \x80\x81")
    out = await _read_file(t, {"path": "/b.bin"})
    assert out.startswith("error:") and "not UTF-8" in out
