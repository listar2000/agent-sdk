"""Unit tests for the SSE frame helpers extracted in refactor slice 5:
``_sse_frame_for`` (filter-to-rpc + render) and ``_is_terminal_frame``
(turn-end detection). Pure functions, no server needed — these guard the
two-shape (ACP-block tuple vs error dict) dispatch that used to be inlined
in ``_execute_and_stream_sse_for``.
"""
from __future__ import annotations

import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from api.server import _is_terminal_frame, _sse_frame_for  # noqa: E402

RPC = "rpc-abc"
OTHER = "rpc-xyz"


# ── _sse_frame_for: filtering + rendering ──────────────────────────────────

def test_frame_for_acp_block_of_this_rpc():
    block = 'data: {"jsonrpc":"2.0"}'
    assert _sse_frame_for((RPC, block), RPC) == f"event: rpc:{RPC}\n{block}\n\n"


def test_frame_for_acp_block_other_rpc_is_skipped():
    assert _sse_frame_for((OTHER, "data: {}"), RPC) is None


def test_frame_for_error_dict_of_this_rpc_is_json_tagged():
    item = {"rpc_id": RPC, "type": "error", "error": {"code": -32603}}
    out = _sse_frame_for(item, RPC)
    assert out is not None
    assert out.startswith(f"event: rpc:{RPC}\ndata: ")
    # payload round-trips
    body = out.split("data: ", 1)[1].rstrip("\n")
    assert json.loads(body) == item


def test_frame_for_error_dict_other_rpc_is_skipped():
    assert _sse_frame_for({"rpc_id": OTHER, "type": "error"}, RPC) is None


def test_frame_for_unknown_shapes_skipped():
    for bad in (None, "string", 123, ("only-one",), (1, 2, 3), {}):
        assert _sse_frame_for(bad, RPC) is None


# ── _is_terminal_frame: turn-end detection ─────────────────────────────────

def test_terminal_on_stopreason_camelcase():
    assert _is_terminal_frame((RPC, 'x "stopReason":"end_turn" y'), RPC) is True


def test_terminal_on_done_marker():
    assert _is_terminal_frame((RPC, 'data: {"type":"done"}'), RPC) is True


def test_terminal_on_error_envelope():
    assert _is_terminal_frame((RPC, 'data: {"error":{"code":-32603}}'), RPC) is True


def test_not_terminal_on_plain_text_chunk():
    assert _is_terminal_frame((RPC, 'data: {"type":"text","text":"hi"}'), RPC) is False


def test_snake_case_stop_reason_is_not_terminal():
    # ACP wires camelCase; the old snake_case substring was a no-op bug.
    assert _is_terminal_frame((RPC, '"stop_reason":"end_turn"'), RPC) is False


def test_terminal_ignores_other_rpc_block():
    assert _is_terminal_frame((OTHER, '"stopReason":"end_turn"'), RPC) is False


def test_terminal_on_error_dict():
    assert _is_terminal_frame({"rpc_id": RPC, "type": "error"}, RPC) is True


def test_error_dict_other_rpc_not_terminal():
    assert _is_terminal_frame({"rpc_id": OTHER, "type": "error"}, RPC) is False


def test_non_error_dict_not_terminal():
    assert _is_terminal_frame({"rpc_id": RPC, "type": "text"}, RPC) is False
