# agent-sdk benchmarks

Reusable benchmarks that surfaced real, repeatable wins (or no-wins). Pure
synthetic micro-benches whose results don't translate to the actual
workload were intentionally excluded.

## Layout

```
benchmark/
├── micro/    # No server needed. Standalone perf reference points.
└── load/     # Hit a running uvicorn / agent-sdk server with concurrent traffic.
```

All benchmarks use the project's ``.venv``. Run from repo root unless noted.

---

## micro/

### `bench_b64.py`
**Question:** does `base64.b64encode/decode` on the event loop block other
coroutines, and does `asyncio.to_thread` make it parallel?

**Run:** `.venv/bin/python benchmark/micro/bench_b64.py`

**Findings (repeatable on any host):**

| size | inline blocks loop for | to_thread blocks loop for |
|-----:|-----------------------:|---------------------------:|
| 10 MB | ~9 ms | 0 |
| 50 MB | ~47 ms | 0 |
| 100 MB | ~94 ms | 0 |

`to_thread` does NOT speed up the b64 work itself (Python's `binascii`
doesn't release the GIL), so concurrent throughput is essentially
identical. **The win is purely isolation** — the loop stays free to
serve other requests while a big upload is being decoded. Use it
gated on payload size; below ~1 MB the thread-dispatch overhead loses.

### `bench_httpx_pool.py`
**Question:** what's the cost of constructing a fresh `httpx.AsyncClient`
per request vs sharing one module-globally?

**Run:** `.venv/bin/python benchmark/micro/bench_httpx_pool.py` (spawns a
local FastAPI ping endpoint; talks to it via both patterns)

**Findings (repeatable):**

| concurrency | per-call RPS | shared RPS | speedup |
|------------:|-------------:|-----------:|--------:|
| 1 | 78 | 80 | ~same |
| 10 | 83 | 409 | **5×** |
| 50 | 80 | 599 | **7.5×** |
| 200 | 83 | 422 | **5×** |

per-call plateaus at ~80 RPS regardless of concurrency — that's the
TCP+TLS handshake floor (~12 ms per new client). Shared client reuses
the keepalive pool. **Always share `httpx.AsyncClient` for repeat calls
to the same backend.**

### `bench_uvloop_overhead.py`
**Question:** baseline overhead difference between asyncio default loop
and uvloop.

**Run:**
```
.venv/bin/python benchmark/micro/bench_uvloop_overhead.py            # default
.venv/bin/python benchmark/micro/bench_uvloop_overhead.py --uvloop   # uvloop
```

**Findings:** uvloop is ~25% faster on bare task creation/scheduling,
roughly tied on `asyncio.Queue` throughput. Real-world impact depends
on whether your server is CPU-bound on loop overhead — usually it isn't,
so this difference rarely shows up end-to-end. Keep this bench as a
reference for "how much could uvloop possibly help"; if the answer is
"not much," your bottleneck is elsewhere.

---

## load/

### `_minimal_app.py`
A tiny FastAPI app (`/ping`, `/work`) used as the workload for
`run_uvicorn_loop_combos.sh`. Not a benchmark itself.

### `load_uvicorn.py` + `run_uvicorn_loop_combos.sh`
**Question:** does `--loop uvloop --http httptools` actually move the
needle on a non-trivial uvicorn server?

**Run:** `bash benchmark/load/run_uvicorn_loop_combos.sh`

**Findings (multi-process loadgen so the SERVER is the bottleneck, not
the loadgen):** on a 32-CPU host, asyncio+h11 ≈ uvloop+httptools ≈ 3500
RPS at moderate concurrency. The default combo isn't materially slower
on this trivial endpoint — for the agent-sdk-shaped workload (lots of
SSE streaming, mostly waiting on remote HTTP), there's no need to
switch. **Single-process loadgens are misleading here** because the
client saturates first; this script intentionally uses N worker procs.

### `scaling_curve.py`
**Question:** at what concurrency does per-op latency start to degrade?
Reveals contention points (DB pool, executor, shared httpx client) that
single-N benches miss.

**Run:** `LEVELS=1,5,10,20,40 PROVIDER=unix_local .venv/bin/python benchmark/load/scaling_curve.py`

Hits a tiny workflow (create + 1 prompt + 3 file_tree calls + delete)
at each level, prints a degradation table. Useful for identifying
"the system is fine to N=20 but cliffs at N=40" type problems.

### `load_agent_sdk_unix.py`
**Question:** end-to-end concurrent throughput of the agent-sdk server
against the `unix_local` provider (no remote billing). Reads `N_SESSIONS`
env var (default 1).

**Setup:** start a server first via `scripts/launch_server_test.sh`.

**Run:** `N_SESSIONS=20 .venv/bin/python benchmark/load/load_agent_sdk_unix.py`

Useful for: validating that local provider concurrency works, sanity-
checking optimisations that don't depend on a real cloud provider.
Numbers will be very fast since unix_local boots in ~1s.

### `load_agent_sdk_daytona.py`
**Question:** raw cold-create throughput against Daytona. Provisions N
sandboxes, sends one prompt each, deletes. Useful for measuring
provisioning + first-prompt cost in isolation.

**Setup:** start a server first; needs `DAYTONA_API_KEY` and
`CLAUDE_CODE_OAUTH_TOKEN` in `~/.env`. Default model is haiku for cost.

**Run:** `N_SESSIONS=40 .venv/bin/python benchmark/load/load_agent_sdk_daytona.py`

**Caveat:** narrow workload — only exercises create + 1 prompt + delete.
For optimizations on the file-proxy / config / multi-turn paths, use
`workload_full.py` instead. Also: Daytona's control plane has
significant run-to-run variance; you need 2+ alternating iterations
per condition to read any A/B signal.

### `workload_full.py` + `ab_harness.sh` + `compare.py`  ★ **the head-to-head**

**Question:** does an optimization actually improve a realistic mixed
workload, end to end?

**`workload_full.py`** has two layers: top-level **scenario** + opt-in
**per-session extras**. Designed so the default A/B numbers stay
reproducible, while opt-ins exercise paths that aren't on the standard
session lifecycle.

### Default per-session workflow (`BENCH_SCENARIO=default`)

Each of N sessions independently runs:
- session create (eager, with config)
- status / sandbox introspection
- 3× ACP config calls (model / mode / thought_level)
- N multi-turn chat prompts (full SSE drain)
- N×4 session-scoped file ops (tree, upload small, read, upload large)
- sandbox exec
- optional release + resume + post-resume prompt

### Per-session opt-in extras (added to whatever scenario is running)

Each is gated by an env var; default off so existing A/B comparisons
stay valid:

| env var | what it adds |
|---|---|
| `BENCH_SSE_SUBSCRIBER=1` | Open a parallel `GET /events` while running a prompt. Tests subscriber fan-out + heartbeats. Records `sse_subscriber_prompt` (wall ms) and `sse_subscriber_chunks` (count delivered to the parallel subscriber). |
| `BENCH_CONCURRENT_PROMPTS=N` | Fire N prompts in parallel on the same session. Server's `_prompt_lock` serializes them. Records `concurrent_prompts_xN` (total wall) — meaningful test of the lock contention path. |
| `BENCH_LONG_TURNS=N` | N additional simple turns to test JSONL growth + supervisor stability over a sustained chat. |
| `BENCH_TOOL_HEAVY=1` | One prompt that forces the agent to run 3 Bash tool calls. Real production shape — not a single-Anthropic-roundtrip but a tool-call/tool-result loop. |
| `BENCH_CANCEL=1` | Submit a prompt via `/message`, sleep 200ms, send `POST /cancel`. Tests cancellation propagation through ACP. |
| `BENCH_INTERRUPT=1` | Submit a prompt, then submit another with `interrupt=true`. The "stop and resend" UI pattern. |

### Top-level scenarios (`BENCH_SCENARIO=...`)

Pick the shape of the load:

| scenario | what each "session" does |
|---|---|
| `default` | Full per-session workflow above (longest, exercises most code paths) |
| `bursty` | Minimal: create + 1 prompt + delete. Use with high N_SESSIONS to stress cold-create throughput |
| `long_chat` | One session, N_TURNS turns (use N_TURNS=20+). Stresses JSONL growth + supervisor memory |
| `mixed` | Per-session profile varies (default / bursty / long_chat / volume_direct cycling). Models real production where users do different things at the same time |
| `volume_direct` | `/volumes/{id}/files/*` — non-session-scoped file ops. Different code path used by the Volume Inspector UI |
| `multi_session_per_agent` | N sessions sharing one `agent_id` (`BENCH_MULTI_SESSION_PER_AGENT=N`). Tests sibling-session invariants. On Daytona, asserts the 409 sibling rejection contract |
| `gigantic_files` | One session per worker. Phase 1 uploads a `BENCH_GIGANTIC_MB`-sized file (default 0 — must set), Phase 2 reads it back, Phase 3 downloads it raw, Phase 4 search-replace edits a same-size text file. **Each phase pairs with a concurrent `/files/tree` probe on a separate connection** — the probe's p99 directly measures whether the big op blocks the event loop for other requests (the actual b64 `to_thread` win). Records `gf_<phase>_ms` (big op wall) + `gf_probe_<phase>` (per-probe latency p50/p99). |
| `stress` | Each of N sessions picks a profile (gigantic / tool_heavy / long_chat / files_only / bursty / volume_direct), all run in parallel. Production-shaped peak load — every workload type happening simultaneously. Per-op latencies under stress show isolation problems (e.g. file ops slow down when a gigantic upload is in flight on another session). Set N_SESSIONS to a multiple of 6 to get one of each. |

Knobs (env vars): `PROVIDER`, `N_SESSIONS`, `N_TURNS`, `N_FILE_OPS`,
`LARGE_MB`, `MODEL`, `SKIP_RELEASE`, `LABEL`, plus the BENCH_* knobs
above. Appends a JSON summary line per run to `/tmp/workload_full.jsonl`.

**`ab_harness.sh`** runs `workload_full.py` for ITERS iterations
against a server source path of your choice (`CHECKOUT_PATH`).
Restarts the server fresh between iterations. Use with `git worktree`
to compare two checkouts without touching either working tree:

```bash
# 1. Set up a baseline worktree at the comparison commit:
git worktree add /tmp/asdk-baseline HEAD

# 2. Run baseline (3 iters):
rm -f /tmp/workload_full.jsonl
CHECKOUT_PATH=/tmp/asdk-baseline LABEL=baseline \
  PROVIDER=unix_local N_SESSIONS=5 N_TURNS=2 N_FILE_OPS=5 LARGE_MB=4 \
  ITERS=3 bash benchmark/load/ab_harness.sh

# 3. Run patched (3 iters) on the current checkout:
LABEL=patched \
  PROVIDER=unix_local N_SESSIONS=5 N_TURNS=2 N_FILE_OPS=5 LARGE_MB=4 \
  ITERS=3 bash benchmark/load/ab_harness.sh

# 4. Compare:
.venv/bin/python benchmark/load/compare.py
```

**`compare.py`** groups by `LABEL` (stripping `_iterN` suffix), prints
per-op p50/p99 deltas with ↑↓≈ markers, and a wall + throughput summary.

**Reference numbers from the executor/httpx/AcpClient/b64 PR:**

| | Daytona (2 iters) | unix_local (3 iters) |
|---|---|---|
| wall | -12% | -15% |
| throughput | +14% | +17% |
| ACP config calls | -32 to -38% | -27 to -44% |
| File proxy ops | -33 to -39% | -35 to -43% |

**Caveat for single-tenant workloads:** the `to_thread` b64 wrap is for
loop *isolation* (keep the loop free during a big decode so other
concurrent requests aren't blocked). A single-tenant bench like this
can't measure that benefit because there's no competing traffic, so
you'll see the ~3ms thread-dispatch overhead as a wash or slight
regression on big uploads. That's expected; the real benefit only
shows up under multi-user load (which this bench doesn't simulate).

---

## What was excluded and why

These existed during the investigation but produced misleading or
noisy numbers; they're not in this directory:

- **Single-process httpx loadgen against local uvicorn** — the loadgen
  itself bottlenecks at ~900 RPS regardless of server config, hiding
  any real server-side delta. Multi-process is the only honest way.
- **Heartbeat-based "loop stall" measurement for b64** — the
  heartbeat coroutine had measurement noise from `wait_for(..., 0.001)`
  scheduling jitter that swamped the actual stall signal. Replaced
  with a counter-during-work approach (`bench_b64.py`).
- **Pure asyncio overhead microbenches divorced from a server** — the
  uvloop overhead bench is included as the one reference point, but
  expanding it didn't add information.
