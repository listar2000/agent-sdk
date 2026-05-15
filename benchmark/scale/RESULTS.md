# Wave 1 / 2 / 3 — final results

## Headline: N=128 mock-ACP 30s streams, three configs back-to-back

`benchmark/scale/mock_acp.js` replaces the LLM with a deterministic
synthetic stream — 300 events × 50 chars × 100ms gap = 30s of SSE per
prompt, 15 KB output. Same JSON-RPC + supervisor + server path as a
real claude turn, just no LLM latency variance. This is the cleanest
apples-to-apples measurement of "what does multi-replica buy us."

| Config              | wall  | chars/s | first_evt p50 | done p95 | Δ chars/s vs 1× |
|---------------------|-------|---------|---------------|----------|------------------|
| **1× baseline**     | 59.7s | 32 161  | 1.18s         | 32.29s   | —                |
| **4× Python LB**    | 41.9s | 45 775  | 0.41s         | 30.75s   | **+42%**         |
| **4× nginx-sticky** | **41.5s** | **46 259** | **0.35s** | 30.78s | **+44%**         |

Per-config scaling factor as N grows from 64 → 128:

| Config | chars/s @ N=64 | chars/s @ N=128 | Δ      |
|--------|----------------|------------------|--------|
| 1×     | 20 890         | 32 161           | +54% (sub-linear; server saturating) |
| 4× nginx | 24 939       | 46 259           | **+86%** (close to linear; replicas have headroom) |

All configs: 128/128 success, 0 redirects (sticky cookie + lease lock-in
on every session-scoped route). nginx ≈ Python LB on raw throughput
(within 1%); nginx is the production default for ops reasons (no GIL,
no shared connection pool, standard).

### Real LLM workload (claude-haiku N=128 daytona)

The same code path on a real LLM workload at N=128 daytona claude-haiku
shows a smaller throughput delta (+12–34% across runs) because daytona
cold-create dominates wall and claude-haiku's LLM latency dwarfs the
server's fanout cost. Run-to-run variance was 47% for the same config —
55s/2476 vs 81s/1687 chars/s for back-to-back Python LB runs. The
mock-ACP bench above isolates server scaling cleanly.

Per-prompt latency on the daytona path was identical across configs
(p50 done 3.7–4.0s, first_event p50 1.8–2.1s).

## 18/18 multi-provider lock-in under `-n auto`

Two back-to-back 9-run lock-ins, 32-worker pytest, 4 replicas + nginx LB:

```
LOCK-IN #1 (clean)                        LOCK-IN #2 (back-to-back)
──────────────────────────────────────    ──────────────────────────────────────
[PASS] unix_local-1   74.7s  16 passed   [PASS] unix_local-1   72.3s  16 passed
[PASS] unix_local-2   71.3s  16 passed   [PASS] unix_local-2   70.3s  16 passed
[PASS] unix_local-3   71.7s  16 passed   [PASS] unix_local-3   72.3s  16 passed
[PASS] modal-1       121.9s  13 passed   [PASS] modal-1       119.3s  13 passed
[PASS] modal-2       123.6s  13 passed   [PASS] modal-2       124.8s  13 passed
[PASS] modal-3       120.9s  13 passed   [PASS] modal-3       119.3s  13 passed
[PASS] daytona-1     118.4s  16 passed   [PASS] daytona-1     122.2s  16 passed
[PASS] daytona-2     117.2s  16 passed   [PASS] daytona-2     108.9s  16 passed
[PASS] daytona-3     116.3s  16 passed   [PASS] daytona-3     121.4s  16 passed

ALL CLEAN: 9/9                            ALL CLEAN: 9/9
```

## Adversarial — 4/4 pass

`benchmark/scale/test_adversarial.py`:
- cross-replica 307 routing
- lease takeover after replica SIGKILL (generation bumps on transfer)
- 32-way concurrent claim race (exactly one winner)
- coalescing preserves end-to-end text bytes

## Wave 1 batching sweep (production defaults)

`tune_batching.sh` at N=8, 200-word prompts:

```
sup_ms  log_ms  chars/chunk  done_p95_s
0       0           85.0     5.64s    legacy
40      100         86.8     6.97s    ← production default
150     250        106.2     8.18s    biggest chunks
```

Coalescing lifts chars/chunk by **+25%** (85 → 106). `log_rows_total`
preserved (no row loss).

## Fault tolerance — replica SIGKILL mid-prompt

`fault_tolerance_demo.py`: 32 in-flight prompts, killed one replica
mid-bench. 24 sessions migrated cleanly via lease takeover; 8 failed
(their in-flight SSE streams were bound to the killed process and
could not be resumed). Recovery p95 = 4.74s post-takeover. A
single-replica deploy has no failover — this is the architectural
must-have, not a perf knob.

## What was enabled

| Concern | Before | After |
|---|---|---|
| Multi-replica deploys | Couldn't — split-brain on session ownership | Lease + 307 + heartbeat |
| `POST /message` against non-owner | Silent fire-and-forget failure | 307 to owner before 200 reply |
| `/admin/sessions` | Per-replica view (1/N of cluster) | DB-backed, cluster-wide |
| Supervisor death mid-prompt | Lost error events | Mid-prompt recovery retry |
| Replica crash | Manual intervention | Auto-takeover after lease TTL (120s) |
| Goldens under -n auto multi-replica | All 15 fail | 18/18 lock-in clean |
| Routing under nginx | 98 redirects / 128 prompts | 0 redirects (cookie-sticky) |

## How to reproduce

```bash
# Production-shape local stack (4 replicas + nginx):
AGENT_SDK_REPLICAS=4 AGENT_SDK_LB=nginx scripts/launch_server_test.sh

# Goldens under -n auto (any provider):
.venv/bin/python -m pytest tests/test_sandbox_stop_delete_recovery.py -n auto

# 9-run lock-in across unix_local + modal + daytona:
RUNS_PER=3 .venv/bin/python benchmark/scale/lockin.py

# Adversarial: cross-replica 307 + takeover + claim race + coalescing
.venv/bin/python benchmark/scale/test_adversarial.py

# Headline mock-ACP scaling bench (N=128, 30s streams):
AGENT_SDK_MOCK_ACP_PATH="$PWD/benchmark/scale/mock_acp.js" \
MOCK_ACP_EVENTS_PER_PROMPT=300 \
MOCK_ACP_CHUNK_SIZE=50 \
MOCK_ACP_INTER_EVENT_MS=100 \
AGENT_SDK_REPLICAS=4 AGENT_SDK_LB=nginx scripts/launch_server_test.sh &
PROVIDER=unix_local N_SESSIONS=128 .venv/bin/python benchmark/scale/driver.py

# Real daytona claude-haiku (high variance, not the headline):
PROVIDER=daytona N_SESSIONS=128 .venv/bin/python benchmark/scale/driver.py
```

## Files

```
M  Dockerfile                              # always single uvicorn worker
M  scripts/launch_server_test.sh           # AGENT_SDK_REPLICAS, AGENT_SDK_LB=nginx default
M  src/agent_sdk/api_client.py             # follow_redirects=True
M  src/agent_sdk/client.py                 # follow_redirects=True
M  src/api/db.py                           # lease columns + helpers
M  src/api/providers/daytona/__init__.py   # create timeout, 5xx retry, destroy-confirm
M  src/api/providers/unix_local/__init__.py# port-collision retry + AGENT_SDK_MOCK_ACP_PATH
M  src/api/sandbox/pool.py                 # NotOwner + heartbeat + lease wiring
M  src/api/server.py                       # route-level lease, DB /admin, sticky cookie
M  src/supervisor/supervisor.js            # SSE chunk coalescing
M  tests/test_sandbox_stop_delete_recovery.py
A  src/api/event_buffer.py                 # SessionLogBatcher
A  src/api/identity.py                     # owner_id/owner_addr/replica_id
A  benchmark/scale/nginx.conf              # production-default cookie-sticky LB
A  benchmark/scale/mock_acp.js             # zero-LLM bench harness (deterministic streams)
A  benchmark/scale/health_flood.py         # /health throughput harness
A  benchmark/scale/                        # multi-replica goldens + adversarial tests
```

## Caveats

- **1000-concurrent end-to-end was not benchmarked.** At N=384 daytona, the test account's 250-concurrent sandbox quota fires (`502 Bad Gateway` from daytona POST /sessions). The server itself was sub-1% CPU at that scale — account quota is the binding constraint, not our code.
- **Switch to multi-replica when you need fault tolerance or you're saturating a single replica.** The mock-ACP table above shows +44% at N=128 with 30s streams; on shorter / LLM-bound workloads the LB hop and parallelism win cancel out and gains are small.
- The pre-existing `usage-stats accumulation` chunk in `src/agent_sdk/client.py` rode along in commit `5c2f948`; it's unrelated to scale work but was already in the working tree marked intentional.
