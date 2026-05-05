# Baseline numbers for the expanded bench scenarios

Captured against `main` HEAD (commit `12b39ca` — the perf PR #82
already merged). These are the numbers future optimization PRs should
A/B against for the scenarios that didn't exist when PR #82 landed.

Provider: `unix_local`. Single host, server launched via
`scripts/launch_server_test.sh`. Anthropic = `haiku`. SKIP_RELEASE=1
on all runs (release/resume already covered by PR #82's RESULTS.md).

## Headline

| label | scenario | extras | wall | tput | err |
|---|---|---|---|---|---|
| baseline-default          | default                  | — | 11.85s | 0.42 sess/s | 0 |
| baseline-tool-heavy       | default                  | TOOL_HEAVY | 17.21s | 0.29 sess/s | 0 |
| baseline-long-chat        | long_chat (3×10 turns)   | — | 30.29s | 0.10 sess/s | 0 |
| baseline-bursty           | bursty (20 sessions)     | — | 14.89s | 1.34 sess/s | 0 |
| baseline-sse-concurrent   | default                  | SSE_SUBSCRIBER, CONCURRENT_PROMPTS=3 | 21.85s | 0.14 sess/s | 0 |
| baseline-volume-direct    | volume_direct (5×50 ops) | — | 0.30s | 16.59 sess/s | 0 |
| baseline-mixed            | mixed (8 heterogeneous)  | — | 12.87s | 0.62 sess/s | 0 |
| baseline-msa              | multi_session_per_agent  | MSA=4 | 33.93s | 0.09 sess/s | 0 |

## Per-op headlines worth tracking

### `tool_heavy_turn` (real production prompt shape)
3 Bash calls + summarization. **p50 = 6.1s, p99 = 10.6s** (5 samples).
This is the latency a user actually sees when claude-code does work,
vs the artificial 1.5s of "Reply OK" in the default bench.

### `lc_turn` (long-conversation tail)
30 turns across 3 long_chat sessions. **p50 = 1.57s, p99 = 5.23s**.
The p50→p99 spread is much wider than a 2-turn session — JSONL grows
turn over turn, so each subsequent `session/load` does more work.
Investigate if the spread is JSONL load time vs Anthropic's own context
growth cost.

### `burst_prompt` under cold-create pressure
20 sessions all created in the same gather window. **p50 = 2.21s,
p99 = 6.69s**. The 3× p50→p99 spread indicates contention somewhere —
probably Anthropic per-account rate-limit at burst-create time.

### `concurrent_prompts_x3` (per-session prompt_lock)
3 prompts fired in parallel on one session. **wall p50 = 4.91s, p99 = 12.15s**
(the wall = sum of serialized prompts since `_prompt_lock` serializes).
Equals ~3× single-prompt latency as expected.

### `sse_subscriber_chunks`
Parallel `/events` subscriber received 10 chunks during a 1.7s prompt
on the producer connection — confirms fan-out works, no chunks dropped.

### `volume_direct` ops (admin/inspector path)
`/volumes/{id}/files/*` is **6.2-6.5ms p50** (vs 30-45ms for the
session-scoped path). This is the cost of the SessionPool detour for
ops that don't need a sandbox — opportunity to skip the pool entirely
for read-only volume inspection.

## How to compare against this in future PRs

```bash
# Baseline (this commit's numbers should still match the above):
git worktree add /tmp/asdk-baseline 12b39ca
rm -f /tmp/workload_full.jsonl

# Pick one or more scenarios, run baseline + patched per the
# ab_harness pattern in RESULTS.md, then compare.py.
```

## What this exposed for the next round of optimization

1. **`burst_prompt` p99 = 3× p50** — under high concurrency, tail
   latency stretches. Likely Anthropic side, but worth confirming with
   a `bursty` run that uses a non-Anthropic agent (codex / opencode).
2. **`lc_turn` p99 = 3.3× p50** — long conversations have a per-turn
   cost that grows with history. If JSONL load is the culprit, a
   bounded-context mode (last N turns) could help.
3. **`volume_direct` is 5-10× faster than session-scoped file ops** —
   an architectural opportunity: read-only session-scoped file ops
   could route through the volume adapter directly when the session
   isn't already warm, skipping the SessionPool detour.
4. **`tool_heavy_turn` is 4-7× the simple-prompt cost** — out of our
   control mostly (each tool call is its own Anthropic round trip),
   but worth re-checking that we're not adding per-tool-call overhead
   on the agent-sdk side.

## What's been investigated and ruled out (don't repeat)

After PR #82 (executor / shared httpx / cached AcpClient / b64) and
PR #84 (shared probe pool) shipped, the following candidates were
profiled and either deemed sub-noise or out-of-scope:

### Pipeline `session_log` INSERTs via `psycopg.pipeline()`
**Result:** correct technique, **5ms saved per turn** (3.4× faster on
isolated micro-bench). Sub-noise vs the 1500ms Anthropic-bound prompt
turn. Did not ship — the win disappears in real workload variance.
Bench: `/tmp/asdk-bench/measure_log_event.py` (sequential 6.74ms vs
pipeline 1.97ms vs executemany 1.88ms for 5 INSERTs).

### Parallelize `set_*` ACP calls in `_forward_session_config`
**Result:** correct technique (3 independent calls should `gather()`),
**no measurable win** on `unix_local` single-tenant — supervisor.js
serializes set_config_option internally, and on the loopback network
there's no roundtrip latency to overlap. Predicted Daytona impact:
600-1500ms saved (3 × ~400ms set_*). Not yet measured. Diff preserved
on local `perf/parallel-acp-set` branch (not pushed) for when there's
Daytona signal.

### `session_create` phase profiling (the real cliff at N=20)
Captured under N=20 concurrent (`scaling_curve.py` showed
session_create p50 grows 3.2× from N=1 to N=20: 960ms → 3029ms):

| phase | typical p50 | dominant? |
|---|---|---|
| `bootstrap` (DB reads) | 10-260 ms | variable, pool warm-up |
| `create_sandbox` (node spawn) | 490-620 ms | constant |
| `wait_health` (poll /v1/health) | 180-290 ms | constant |
| **`attach_acp`** | **1350-1870 ms** | **dominant — 60% of total** |

The `attach_acp` cost is **supervisor.js spawning the claude-code-acp
child process** in response to `session/new`. At N=20 the supervisors
all spawn claude children simultaneously → CPU contention.

**This is supervisor-side, not Python-side.** Two paths to attack:

1. **Pre-spawn claude children in supervisor.js** — at supervisor boot,
   spawn the child immediately rather than lazily on `session/new`.
   Supervisor change, out of agent-sdk Python scope.

2. **Lazy attach in Python** — defer `_attach_acp` until first
   ACP-needing op (POST /message, POST /config). POST /sessions returns
   ~500ms faster but first POST /message gets the 1500ms cost. Big
   architectural change: affects test invariants, /admin/sessions readouts,
   `inner_session_id` availability timing. Not a clean iter PR.

Both deferred. Future iterations should target one of:
- The supervisor.js change (would have outsized impact)
- Volume-direct routing for read-only file ops (5-10× win on `/sessions/{id}/files/*`)
- Peek-mode `get_session()` for status/sandbox endpoints (prevents accidental cold-recovery on idle sessions)
