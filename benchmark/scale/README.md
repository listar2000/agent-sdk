# Scale benchmark — Wave 1/2/3 throughput harness

End-to-end driver + scenario orchestrator for measuring how the scaling
changes (supervisor.js coalescing, batched `session_log` writes,
multi-replica + Postgres lease + 307 redirect) compound under concurrent
prompt load.

See `RESULTS.md` for the headline numbers — the headline run is **N=128
daytona claude-haiku, three configs back-to-back**.

## What lives here

Driver / harness:
- `driver.py` — opens N concurrent sessions, drives one or more prompts
  through `POST /sessions/{id}/message+stream`, records per-prompt
  latencies + event counts, prints a one-line summary + optional
  JSON-lines row.
- `mock_acp.js` — zero-LLM ACP shim. Speaks the same JSON-RPC-over-stdio
  protocol as `@anthropic-ai/claude-agent-acp` / `opencode` but emits a
  configurable burst of `session/update` events. Drives high SSE rates
  at near-zero per-prompt latency, isolating server CPU + LB overhead
  from agent latency. Wire in via `AGENT_SDK_MOCK_ACP_PATH=...` on
  unix_local.
- `health_flood.py` — hammers `/health` to measure pure FastAPI + handler
  throughput, isolated from supervisor.js entirely.

Load balancer:
- `nginx.conf` — **production-default** LB. Cookie-sticky upstream
  (Set-Cookie `agent_sdk_route=<replica_id>` on POST /sessions →
  explicit cookie→backend map), with consistent-hash on `$session_id`
  as the fallback when the cookie is missing.
- `lb.py` — local-dev Python LB with affinity learning. Equivalent
  routing semantics; fallback when nginx isn't on PATH.

Orchestrators:
- `scenarios.sh` — A/Bs Wave 1 against legacy (baseline). Always single
  uvicorn worker.
- `goldens_multireplica.sh` — run the recovery goldens against a
  4-replica + LB stack.
- `lockin.py` — N×providers golden lock-in.

Profilers + adversarial tests:
- `profile_multireplica.py` — 1× vs 4×LB comparison with per-replica
  breakdown.
- `profile_realagent.py` — real-agent event profiler.
- `profile_with_pyspy.sh` — flame-graph the busiest replica during a
  4× run.
- `test_adversarial.py` — cross-replica 307, lease takeover (SIGKILL),
  concurrent-claim race, coalescing-byte-preservation.
- `fault_tolerance_demo.py` — kill-a-replica demo.

## Quick start

```bash
# Production-shape local stack (4 replicas + nginx + LB):
AGENT_SDK_REPLICAS=4 AGENT_SDK_LB=nginx scripts/launch_server_test.sh

# Headline daytona bench (claude/haiku, N=128, 1-turn):
PROVIDER=daytona N_SESSIONS=128 .venv/bin/python benchmark/scale/driver.py

# Server-saturation regime (mock ACP, N=200, no LLM cost):
AGENT_SDK_MOCK_ACP_PATH=benchmark/scale/mock_acp.js \
  PROVIDER=unix_local N_SESSIONS=200 \
  .venv/bin/python benchmark/scale/driver.py

# Wave-1 A/B against legacy:
benchmark/scale/scenarios.sh --only baseline
benchmark/scale/scenarios.sh --only wave1
```

The benchmark server lands on `BENCH_PORT` (default `7779`) so it doesn't
clobber a dev server on `7778`.

## Daytona and Modal

Both are real-cloud providers — running 100s of sessions has a cost and
quota implications. The harness reads `~/.env` so as long as
`DAYTONA_API_KEY` / `MODAL_TOKEN_ID` / `MODAL_TOKEN_SECRET` /
`CLAUDE_CODE_OAUTH_TOKEN` are set there, no per-run flag is needed.

```bash
PROVIDER=daytona N_SESSIONS=128 .venv/bin/python benchmark/scale/driver.py
PROVIDER=modal   N_SESSIONS=16  .venv/bin/python benchmark/scale/driver.py
```

**Important**: the supervisor.js coalescing change requires a fresh
runtime release for daytona/modal — the snapshot tags are pinned to a
specific commit (see project `CLAUDE.md`):

```bash
scripts/release.sh                    # rebuilds docker + daytona + modal
# commit .runtime-image-tag / .runtime-snapshot-tag / .modal-snapshot-tag
```

Without that step, daytona/modal sandboxes boot the OLD supervisor that
doesn't coalesce, so `wave1` reads no better than `baseline` for those
providers. unix_local is unaffected — it reads supervisor.js from the
source tree.

## Profiling

`py-spy` against a running uvicorn worker, sampled during a bench run:

```bash
benchmark/scale/profile_with_pyspy.sh
# -> benchmark/scale/flame-lb-replicas.svg
```

For multi-replica, py-spy attaches to the busiest replica's PID. Look
for `_broadcast`, `log_event`, and the per-event JSON parse — these were
the Wave-1 targets. If they still dominate after Wave 1, that's a signal
for a Wave-4 out-of-Python fanout (Centrifugo sidecar or similar).
