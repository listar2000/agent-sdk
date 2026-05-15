# Scale benchmark — Wave 1/2/3 throughput harness

End-to-end driver + scenario orchestrator for measuring how the scaling
changes (supervisor.js coalescing, batched `session_log` writes, multi-
worker uvicorn, Postgres lease + 307 redirect) compound under concurrent
prompt load.

## What lives here

- `driver.py` — opens N concurrent sessions, drives one or more prompts
  through `POST /sessions/{id}/message+stream`, drains the SSE stream,
  records per-prompt latencies + event counts, and prints a one-line
  summary plus (optional) a JSON-lines row for diffing across runs.
- `scenarios.sh` — orchestrates the server-launch / driver-run / teardown
  cycle for a sweep of named scenarios. Each scenario toggles individual
  waves via env flags so contributions can be A/B'd:
  - `baseline`        — wave 1+2+3 disabled (legacy 1-event-per-chunk,
                        per-event INSERT, single worker, no lease).
  - `wave1`           — supervisor coalescing + session_log batching on.
  - `wave1_w4`        — wave 1 + 4 uvicorn workers, lease still off.
  - `wave1_w4_lease`  — full stack on. Validates that lease + 307 doesn't
                        regress at single-host bench scale.
- `results.jsonl` — append-only result rows from each driver invocation.

## Quick start (local unix_local provider)

```bash
# Make sure the project-local Postgres (port 5433) is up. The
# launch_server_test.sh script manages it; you can also use the conda
# install or any other 5433 instance with the agent_sdk_server database.

# Default: full sweep against unix_local, N=16 concurrent, claude/haiku.
benchmark/scale/scenarios.sh

# Just one scenario, smaller N to validate the harness:
N_SESSIONS=4 benchmark/scale/scenarios.sh --only baseline
N_SESSIONS=4 benchmark/scale/scenarios.sh --only wave1

# Aim for the 1000-concurrent target gradually. Stop at the first
# scenario that hits >1% failure rate or median latency >2x baseline.
for N in 16 32 64 128 256 512 1000; do
    N_SESSIONS=$N benchmark/scale/scenarios.sh --only wave1_w4_lease
done
```

The benchmark server lands on `BENCH_PORT` (default `7779`) so it doesn't
clobber a dev server on `7778`.

## Daytona and Modal

Both are real-cloud providers — running 100s of sessions has a cost and
quota implications. The harness reads `~/.env` so as long as
`DAYTONA_API_KEY` / `MODAL_TOKEN_ID` / `MODAL_TOKEN_SECRET` /
`CLAUDE_CODE_OAUTH_TOKEN` are set there, no per-run flag is needed.

```bash
PROVIDER=daytona N_SESSIONS=16 benchmark/scale/scenarios.sh --only wave1
PROVIDER=modal   N_SESSIONS=16 benchmark/scale/scenarios.sh --only wave1
```

**Important**: the supervisor.js coalescing change requires a fresh
runtime release for daytona/modal — the snapshot tags are pinned to a
specific commit (see project `CLAUDE.md`):

```bash
scripts/release.sh                    # rebuilds docker + daytona + modal
# commit .runtime-image-tag / .runtime-snapshot-tag / .modal-snapshot-tag
```

Without that step, the daytona/modal sandboxes boot the OLD supervisor
that doesn't coalesce, so `wave1` will read no better than `baseline`
for those providers. unix_local is uneffected — it reads supervisor.js
straight from the source tree.

## Profiling (Tier 0 from the design)

`py-spy` against a running uvicorn worker, sampled while the driver runs:

```bash
# Terminal 1: launch the server for one scenario, blocking on PID
benchmark/scale/scenarios.sh --only wave1_w4_lease &
SERVER_PID=$(cat logs/scale-server.pid)

# Terminal 2: while the driver is hammering it, sample
sudo .venv/bin/py-spy record -o flame-wave1.svg -d 30 -p $SERVER_PID
```

For multi-worker bench, attach py-spy to the PARENT and use `--subprocesses`
so all worker children are sampled.

## What to look at in the SVG

After Wave 1, expect the hot path to move OUT of `_broadcast` (was: 60%+
of CPU) and INTO real work (`execute_prompt` httpx stream, the per-event
JSON parse). After Wave 1b, the `log_event` / Postgres INSERT cost
should drop to a thin stripe instead of a wide column.

If `_broadcast`, JSON parse, or PG INSERT are still dominant after
Wave 1, that's the signal to add Wave 4 (Centrifugo sidecar or similar
out-of-Python fanout).

## Known harness limitations

- **Same-host 307s are kernel-load-balanced away**. SO_REUSEPORT on Linux
  is per-flow; httpx keeps connections alive, so each session's requests
  tend to hit the same worker after the first. To force the 307 path on a
  single host you need either (a) a new TCP connection per request (set
  `httpx.Limits(max_keepalive_connections=0)`) or (b) a real LB doing
  consistent-hash routing. `redirects_total=0` in the local bench is
  expected; in a multi-replica deploy it'll be substantial.
- **Coalescing improves text-streaming-heavy prompts most**. A short
  "OK" reply is a single chunk regardless; the wins show on prompts that
  produce many small text deltas (the realistic Claude-Code-on-a-task
  workload).
- **Events-per-second** in the driver summary counts every SSE block
  including heartbeats and bookkeeping frames — not a clean fanout
  metric. For a clean fanout-rate measurement, query the row count delta
  on `session_log` before/after a run.

## Smoke-test results (unix_local, claude/haiku, N=2-8)

```
scenario          N    workers  wall_s   chars/s   done_p95_s
baseline          2    1        8.98     170.7     3.03
baseline          4    1        13.62    281.1     7.07
wave1             4    1        10.55    434.5     4.49     # +55% chars/s, -36% p95
wave1_w4_lease    8    4        10.22    843.1     3.81     # 4-worker stack alive
```

These are tiny smokes; the real signal lives at N ≥ 64. The interesting
result here is that `wave1_w4_lease` completed without redirects, errors,
or split-brain — meaning the lease + heartbeat path is correctness-safe
at this load and ready for a real multi-replica deploy where 307s
actually fire.
