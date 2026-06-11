"""`is_terminal_block` — confirmed-envelope terminal detection.

Regression for the content-truncation bug: the previous bare-substring check
(`"stopReason" in block`) matched inside streamed CONTENT, so an agent whose
text or tool output contained the characters ``stopReason`` terminated its
own /message+stream mid-turn. JSON escaping protects quoted patterns like
``'"error":'`` but cannot protect a bare word — only envelope confirmation
can.
"""

from __future__ import annotations

import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from api.sse import is_terminal_block  # noqa: E402
from api.native import frames  # noqa: E402

RPC = "rpc-term-test"
SID = "sess-term-test"


def test_real_done_envelopes_terminate():
    for stop in ("end_turn", "cancelled", "max_tokens", "max_turn_requests"):
        assert is_terminal_block(frames.done_block(RPC, stop), RPC)


def test_real_error_envelope_terminates():
    assert is_terminal_block(frames.error_block(RPC, "boom", "X"), RPC)


def test_content_containing_stopreason_does_not_terminate():
    """THE BUG: agent text mentioning stopReason must stream through."""
    b = frames.text_block(SID, "the parser checks result.stopReason here")
    assert not is_terminal_block(b, RPC)


def test_tool_result_with_adversarial_content_does_not_terminate():
    nasty = json.dumps({"error": "boom", "stopReason": "fake"}) + ' "error": raw'
    b = frames.tool_result_block(SID, "c1", "bash", nasty)
    assert not is_terminal_block(b, RPC)


def test_wrong_rpc_envelope_does_not_terminate():
    # The route's tag filter normally screens these out before the check,
    # but the function itself must also be rpc-precise (defense in depth).
    assert not is_terminal_block(frames.done_block("other-rpc", "end_turn"), RPC)
    assert not is_terminal_block(frames.error_block("other-rpc", "x"), RPC)


def test_result_without_stopreason_does_not_terminate():
    b = "data: " + json.dumps(
        {"jsonrpc": "2.0", "id": RPC, "result": {"something": "else"}},
        separators=(",", ":"))
    assert not is_terminal_block(b, RPC)


def test_non_json_and_plain_frames_do_not_terminate():
    assert not is_terminal_block("data: not json at all stopReason", RPC)
    assert not is_terminal_block(frames.text_block(SID, "hello"), RPC)
    assert not is_terminal_block(frames.usage_block(SID, 1, 2, 0.0), RPC)


def test_fast_path_skips_clean_content():
    # No candidate substring -> early False without JSON parsing.
    assert not is_terminal_block(frames.text_block(SID, "totally normal"), RPC)
