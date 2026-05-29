# Scale benchmark — throughput + correctness harness

End-to-end driver + adversarial tests for the multi-replica deploy
shape (single uvicorn worker per replica, nginx LB with consistent-hash
on session_id, Postgres lease + cookie-on-failover).

See `RESULTS.md` for headline numbers — N=128 mock-ACP, 30s SSE streams.

## What lives here

Driver / harness:
- `driver.py` — opens N concurrent sessions, drives one or more prompts
  through `POST /sessions/{id}/message+stream`, records per-prompt
  latencies + event counts, prints a one-line summary + optional
  JSON-lines row. Mirrors the SDK's `X-Session-Id` header so the LB
  consistent-hashes POST and subsequent requests to the same replica.
- `mock_acp.js` — zero-LLM ACP shim. Speaks the same JSON-RPC-over-stdio
  protocol as `@anthropic-ai/claude-agent-acp` / `opencode` but emits a
  configurable burst of `session/update` events. Isolates server CPU +
  LB scaling from agent latency. Wire in via
  `AGENT_SDK_MOCK_ACP_PATH=...` on unix_local.

Load balancer:
- `nginx.conf` — production-default LB. Consistent-hash on session_id
  (URL on `/sessions/{id}/*`, `X-Session-Id` header on POST /sessions);
  cookie→backend map handles the rare failover override emitted by the
  server's NotOwner handler. `worker_processes 2` (auto on 32-core
  spawns 32 workers that fight uvicorn for CPU).
- `lb.py` — local-dev Python LB fallback when nginx isn't on PATH.

Test harnesses:
- `test_adversarial.py` — cross-replica 307 routing, lease takeover
  after SIGKILL, concurrent-claim race, end-to-end byte preservation.
- `goldens_multireplica.sh` — run the recovery goldens against a
  4-replica + LB stack.
- `lockin.py` — N×providers golden lock-in.
- `fault_tolerance_demo.py` — kill-a-replica demo for end-to-end
  failover behavior.

## Quick start

```bash
# Production-shape local stack (4 replicas + nginx LB, default):
AGENT_SDK_REPLICAS=4 scripts/launch_server_test.sh

# Headline daytona bench (claude/haiku, N=128):
PROVIDER=daytona N_SESSIONS=128 .venv/bin/python benchmark/scale/driver.py

# Server-saturation regime (mock ACP, N=128, no LLM cost):
AGENT_SDK_MOCK_ACP_PATH=$PWD/benchmark/scale/mock_acp.js \
  MOCK_ACP_EVENTS_PER_PROMPT=300 MOCK_ACP_CHUNK_SIZE=50 MOCK_ACP_INTER_EVENT_MS=100 \
  AGENT_SDK_REPLICAS=4 scripts/launch_server_test.sh &
PROVIDER=unix_local N_SESSIONS=128 .venv/bin/python benchmark/scale/driver.py

# Adversarial multi-replica correctness:
.venv/bin/python benchmark/scale/test_adversarial.py

# Goldens against 4× LB:
.venv/bin/python -m pytest tests/test_golden.py -n auto
```

## Daytona and Modal

Both are real-cloud providers — running 100s of sessions has a cost and
quota implications. The harness reads `~/.env` so as long as
`DAYTONA_API_KEY` / `MODAL_TOKEN_ID` / `MODAL_TOKEN_SECRET` /
`CLAUDE_CODE_OAUTH_TOKEN` are set there, no per-run flag is needed.

**Important**: if you touch `src/supervisor/supervisor.js` or the
Dockerfile, rebuild the runtime snapshots before re-running
daytona/modal goldens — the snapshot tags are pinned to a specific
commit (see project `CLAUDE.md`):

```bash
scripts/release.sh                    # rebuilds docker + daytona + modal
# commit .runtime-image-tag / .runtime-snapshot-tag / .modal-snapshot-tag
```

Without that step, daytona/modal sandboxes boot the OLD supervisor
that doesn't carry your changes. unix_local is unaffected — it reads
supervisor.js from the source tree.
