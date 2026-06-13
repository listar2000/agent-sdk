"""Native frame-synthesis throughput — the per-token hot path.

**Question:** how much CPU does the native runtime burn turning one LLM stream
delta into a broadcast block? Every ``text``/``reasoning`` token the model
emits goes through ``frames`` once, so at high session concurrency this is a
real, shared-core cost (N tokens × C concurrent turns of pure JSON work on the
event loop thread).

**What it measures:** ns/event and M events/s for

  * the dict-dump path (the original implementation, recreated inline as a
    reference baseline so this bench stays meaningful even after the source
    changes),
  * the stateless fast templates (``frames.text_block`` — constant fragments +
    one ``json.dumps`` of the dynamic string),
  * the per-session ``frames.FrameEncoder`` (also caches ``json.dumps`` of the
    session id, the only other constant on the path).

All three are byte-identical (pinned by tests/test_native_frames.py); this only
measures the cost of producing those identical bytes.

**Run:** ``.venv/bin/python benchmark/micro/bench_native_frames.py``

**Findings (repeatable; absolute ns scale with the host):**

| path                        | ns/event | speedup |
|-----------------------------|---------:|--------:|
| dict-dump (original)        |   ~2200  |   1.0×  |
| stateless fast template     |    ~450  |   ~4.9× |
| FrameEncoder (cached prefix)|    ~260  |   ~8.5× |

The win is pure per-token CPU on the loop thread — it does not change wire
bytes, latency granularity, or memory. It matters precisely under the
"super scalable" case: many concurrent native turns sharing a core.
"""

from __future__ import annotations

import json
import time

from api.native import frames

_COMPACT = (",", ":")
SID = "sess-abc123def456"
RPC = "rpc-xyz789"

# Representative stream deltas — short tokens are the common case.
_TOKENS = ["Hello", " world", ",", " this", " is", " a", " token", "!",
           "\n", " More", " text", " here", " 漢字", " 🚀"]


def _dict_text_block(session_id: str, text: str) -> str:
    """The ORIGINAL dict-dump implementation, inline as the baseline."""
    return "data: " + json.dumps({
        "jsonrpc": "2.0", "method": "session/update",
        "params": {"sessionId": session_id, "update": {
            "sessionUpdate": "agent_message_chunk",
            "content": {"type": "text", "text": text},
        }},
    }, separators=_COMPACT)


def _timeit(label: str, fn, n: int) -> float:
    t0 = time.perf_counter()
    for i in range(n):
        fn(_TOKENS[i % len(_TOKENS)])
    dt = time.perf_counter() - t0
    print(f"{label:<30} {n/dt/1e6:6.3f} M ev/s   {dt/n*1e9:6.0f} ns/ev")
    return dt / n


def main() -> None:
    n = 300_000
    print(f"native frame synthesis — {n:,} events each\n")

    # byte-parity sanity before timing (the bench would be meaningless if the
    # three paths produced different bytes).
    enc = frames.FrameEncoder(SID)
    for tok in _TOKENS + ['"q"', "back\\slash", "}}}}", ""]:
        ref = _dict_text_block(SID, tok)
        assert frames.text_block(SID, tok) == ref, tok
        assert enc.block_for_event({"type": "text", "text": tok}, RPC) == ref, tok
    print("byte-parity: OK (dict == stateless == FrameEncoder)\n")

    base = _timeit("dict-dump (original)", lambda t: _dict_text_block(SID, t), n)
    fast = _timeit("stateless fast template",
                   lambda t: frames.text_block(SID, t), n)
    cached = _timeit("FrameEncoder (cached prefix)",
                     lambda t: enc.block_for_event({"type": "text", "text": t},
                                                   RPC), n)
    print(f"\nstateless speedup:  {base/fast:.2f}×")
    print(f"FrameEncoder speedup: {base/cached:.2f}×")


if __name__ == "__main__":
    main()
