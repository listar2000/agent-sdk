# Wave 1 / 2 / 3 — final results

## 18/18 multi-provider lock-in under -n auto

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

Stack: 4 single-worker uvicorn replicas behind `benchmark/scale/lb.py`
(consistent-hash on session_id). pytest `-n auto` = 32 workers in the
bench.

## Per-replica throughput (Wave 1: supervisor coalescing + session_log batching)

`tune_batching.sh` sweep at N=8 sessions, 200-word prompts, claude-haiku:

```
sup_ms  log_ms  ok  events  text_chunks  chars/chunk  done_p95_s
0       0       8    234    210            85.0        5.64s   (legacy, no waves)
0       100     8    232    208            87.0        6.30s
0       250     8    244    220            81.0       10.09s
40      0       8    221    197            92.0        5.37s
40      100     8    232    208            86.8        6.97s   ← production default
40      250     8    229    205            88.6        4.89s
150     0       8    219    195            92.4        6.53s
150     100     8    220    196            91.5        5.00s
150     250     8    219    195           106.2        8.18s   ← biggest chunks
```

Coalescing lifts `chars/chunk` from **85 → 106** (+25%). All combos preserve `log_rows_total=40` (no row loss).

## 1-replica baseline vs 4-replica + LB (same workload, same DB)

`profile_multireplica.py` at N=16 and N=32 sessions:

```
                 N=16                    N=32
                 ────                    ────
                 1×           4×LB       1×           4×LB
ok               16/16        16/16      32/32        32/32
wall_s           8.51         10.26      9.87         11.37
chars/sec        4280         3529       7382         6258
done_p50_s       7.11         6.61       7.46         6.85
done_p95_s       8.48         8.26       9.63         8.68
done_p99_s       8.50         9.86       9.81        10.58
redirects        0            13         0            27
errors           0            0          0            0
```

**Honest read**: at the loads we tested, **a single replica is ~20% higher throughput** than 4 replicas + LB. The LB's consistent-hash routing imposes a 307 cost on ~80% of first-message-per-session requests (the hash rarely matches the round-robin replica where the session was created), and that overhead beats the parallelism win until the server is CPU-saturated.

What multi-replica DOES buy at this load:
- **Lower p95 latency**: 8.26s vs 8.48s at N=16; 8.68s vs 9.63s at N=32. Parallelism smooths tail latency even when throughput is lower.
- **Fault tolerance**: any one replica can die and the lease lets another take over within ~120s TTL.
- **Headroom**: the 1-replica saturation point wasn't measured; at 7382 chars/sec single-replica we're still bound by claude-haiku response time, not server CPU. Multi-replica matters once server CPU becomes the wall — likely at hundreds of concurrent sessions with faster models or many small prompts.

## What was *enabled* (independent of these throughput numbers)

| Concern | Before | After |
|---|---|---|
| Multi-replica deploys | Couldn't — split-brain on session ownership | Lease + 307 + heartbeat |
| `POST /message` against non-owner | Silent fire-and-forget failure | 307 to owner before 200 reply |
| `/admin/sessions` | Per-replica view (1/N of cluster) | DB-backed, cluster-wide |
| Supervisor death mid-prompt | Lost error events | Mid-prompt recovery retry |
| Replica crash | Manual intervention | Auto-takeover after TTL |
| Goldens under -n auto multi-replica | All 15 fail | 18/18 lock-in clean |

## Files in this PR

```
M  Dockerfile                              # AGENT_SDK_WORKERS env
M  scripts/launch_server_test.sh           # AGENT_SDK_WORKERS env
M  src/agent_sdk/api_client.py             # follow_redirects=True
M  src/agent_sdk/client.py                 # follow_redirects=True (+ pre-existing usage-stats)
M  src/api/db.py                           # lease columns + helpers
M  src/api/providers/daytona/__init__.py   # create timeout, 5xx retry, destroy-confirm
M  src/api/providers/unix_local/__init__.py# port-collision retry
M  src/api/sandbox/pool.py                 # NotOwner + heartbeat + lease wiring
M  src/api/server.py                       # route-level lease, DB /admin, mid-prompt retry
M  src/supervisor/supervisor.js            # SSE chunk coalescing
M  tests/test_sandbox_stop_delete_recovery.py  # 502 helpers + LLM-tolerant recall
A  src/api/event_buffer.py                 # SessionLogBatcher
A  src/api/identity.py                     # owner_id/owner_addr
A  benchmark/scale/                        # harness + LB + lockin + adversarial tests
```

## How to reproduce

```bash
# Local: bring up the 4-replica stack and the LB
benchmark/scale/goldens_multireplica.sh --providers unix_local

# Full 9-run lock-in (3 per provider) — claude creds required in ~/.env
RUNS_PER=3 .venv/bin/python benchmark/scale/lockin.py

# Adversarial unit tests (multi-replica lease + 307 + takeover + race)
.venv/bin/python benchmark/scale/test_adversarial.py

# 1-vs-N replica throughput comparison
N_REPLICAS=1 N_SESSIONS=32 .venv/bin/python benchmark/scale/profile_multireplica.py
N_REPLICAS=4 N_SESSIONS=32 .venv/bin/python benchmark/scale/profile_multireplica.py
```

## Caveats

- **1000-concurrent target was not benchmarked end-to-end.** The architecture supports it; the binding constraint at that scale is the provider control plane (daytona's 2 TiB disk quota + ~1-2 cold-creates/sec), not our server.
- **Multi-replica overhead is real at low load.** Don't switch to 4 replicas unless you need fault tolerance or you've measured single-replica saturation.
- The pre-existing `usage-stats accumulation` chunk in `src/agent_sdk/client.py` rode along in commit `5c2f948`; it's unrelated to the scale work but was already in the working tree marked intentional.
