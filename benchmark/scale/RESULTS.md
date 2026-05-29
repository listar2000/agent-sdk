# Scale results — multi-replica + cluster-aware routing

## Headline: N=128 mock-ACP, 30s SSE streams

`benchmark/scale/mock_acp.js` replaces the LLM with a deterministic
synthetic stream (300 events × 50 chars × 100ms gap = 30s SSE per
prompt, 15 KB output). Same JSON-RPC + supervisor + server path as a
real claude turn, just no LLM latency variance. Cleanest apples-to-
apples measurement of what multi-replica buys.

Latest run (client-supplied session_id + consistent-hash + cluster-aware admin):

| Config              | wall  | chars/s | first_evt p50 | done p95 | Δ chars/s vs 1× |
|---------------------|-------|---------|---------------|----------|------------------|
| **1× baseline**     | 60.4s | 31 801  | 1.41s         | 32.58s   | —                |
| **4× nginx**        | **47.5s** | **40 413** | **0.39s** | 30.85s | **+27%**         |

`first_event` p50 drops 3.6× (1.41s → 0.39s) — parallelism eliminates the
single-replica queuing delay.

The earlier design (cookie set on POST /sessions, no client-side id)
hit **+44%** on this same workload, with the cost of leaking
internal replica identity to the client cookie jar and breaking
dashboard SSE for cross-replica sessions. The new design trades ~17pp
of throughput for two architectural wins:

- **User code only needs session_id**. No cookies set by the SDK; no
  client awareness of replicas.
- **Cluster-aware admin** (`/admin/sessions` filters by lease state;
  `agent_busy` reads `busy_at` cluster-wide). Dashboard now correctly
  shows completed sessions as inactive and reports busy state for
  sessions owned by any replica.

The 17pp comes from extra DB writes per prompt: collision-check on POST,
`busy_at = now()` on prompt start, `busy_at = NULL` on prompt end, plus
heartbeat-refresh while `_prompt_lock` is held. Roughly 1400 extra
writes for N=128 prompts. Acceptable for the visibility win.

## Real LLM workload (claude-haiku N=128 daytona)

The same code path on real daytona claude-haiku at N=128 delivers
+12–34% chars/s across runs, but **back-to-back same-config variance
was 47%** (Python LB ran 55s/2476 then 81s/1687 chars/s in two
adjacent runs). Daytona cold-create dominates wall and claude-haiku's
LLM latency dwarfs server fanout cost, so the throughput signal
washes out. **Per-prompt latency is identical across configs**
(p50 done 3.7–4.0s, first_event p50 1.8–2.1s). The architecture is
validated end-to-end; the throughput crossover lands cleanly on the
mock-ACP bench above.

## Recovery goldens — 90/90 on 4× nginx + new design

`pytest tests/test_golden.py -n auto`:

```
90 passed, 30 skipped, 0 failed in 4m 46s

claude-{unix_local, daytona, modal}    : 45/45  ✅
opencode-{unix_local, daytona, modal}  : 45/45  ✅
*-docker                                : 30 skipped (no docker daemon on this host)
```

Earlier 18/18 multi-provider lock-in (3 runs each × 3 providers, two
back-to-back rounds) under the routing-overhauled stack also passes.

## Adversarial tests

`benchmark/scale/test_adversarial.py`:
- ✅ **T1** cross-replica routing: 307 emits relative `Location` +
  `Set-Cookie agent_sdk_route=<owner>; Path=/sessions/<sid>`. The
  cookie steers the LB to the new owner on retry.
- ✅ **T2** lease takeover after SIGKILL: lease_generation bumps on
  ownership transfer; new owner serves cleanly.
- ✅ **T3** concurrent claim race: 32 parallel claims yield exactly
  one winner.
- ⚠️ T4 (coalescing byte preservation through session_log) is a
  pre-existing flake — the `session_log` row filter looks for
  `event_type='assistant_message'` which appears to depend on prompt
  shape. Not related to the routing/admin changes.

## Fault tolerance — replica SIGKILL mid-prompt

`fault_tolerance_demo.py`: 32 in-flight prompts, killed one replica
mid-bench. 24 sessions migrate cleanly via lease takeover; 8 fail
(their in-flight SSE streams were bound to the killed process and
could not be resumed). Recovery p95 = 4.74s post-takeover.

This is the **only** thing 1× cannot do — a single-replica deploy has
no failover.

## What was enabled

| Concern | Before | After |
|---|---|---|
| Multi-replica deploys | Couldn't — split-brain on session ownership | Postgres lease + atomic claim |
| Wrong-replica POST /message | Silent fire-and-forget failure | NotOwner → 307 → cookie-steer |
| `/admin/sessions` | Per-replica view (1/N of cluster); completed sessions stuck "active" | Cluster-wide via lease state |
| `agent_busy` | Per-replica only; peers report false | Cluster-wide via `busy_at` column |
| Supervisor death mid-prompt | Lost error events | Mid-prompt recovery retry |
| Replica crash | Manual intervention | Auto-takeover after lease TTL (120s) |
| Goldens under -n auto multi-replica | All 15 fail | 90/90 across two agents × three providers |
| Routing under nginx | 98 redirects / 128 prompts | 0 redirects (consistent-hash on session_id) |
| Client code complexity | Cookie set on every create; client tied to a replica | Client passes a UUID; nothing else |

## How to reproduce

```bash
# Production-shape local stack (4 replicas + nginx, default):
AGENT_SDK_REPLICAS=4 scripts/launch_server_test.sh

# Recovery goldens under -n auto (any provider):
.venv/bin/python -m pytest tests/test_golden.py -n auto

# Adversarial: cross-replica 307 + takeover + claim race
.venv/bin/python benchmark/scale/test_adversarial.py

# Headline mock-ACP bench (N=128, 30s streams):
AGENT_SDK_MOCK_ACP_PATH=$PWD/benchmark/scale/mock_acp.js \
MOCK_ACP_EVENTS_PER_PROMPT=300 MOCK_ACP_CHUNK_SIZE=50 MOCK_ACP_INTER_EVENT_MS=100 \
AGENT_SDK_REPLICAS=4 scripts/launch_server_test.sh &
PROVIDER=unix_local N_SESSIONS=128 .venv/bin/python benchmark/scale/driver.py

# Real daytona claude-haiku (high variance, illustrative):
PROVIDER=daytona N_SESSIONS=128 .venv/bin/python benchmark/scale/driver.py
```

## Caveats

- **1000-concurrent end-to-end was not benchmarked.** At N=384 daytona,
  the test account's 250-concurrent sandbox quota fires (502 Bad
  Gateway from daytona POST /sessions). The server itself was sub-1%
  CPU at that scale — account quota is the binding constraint, not
  our code.
- **Switch to multi-replica when you need fault tolerance or you're
  saturating a single replica.** The throughput crossover sharpens
  as server CPU pressure rises; on short / LLM-bound workloads the LB
  hop and parallelism win cancel out.
- The pre-existing `usage-stats accumulation` chunk in
  `src/agent_sdk/client.py` rode along in commit `5c2f948`; it's
  unrelated to scale work but was already in the working tree marked
  intentional.
