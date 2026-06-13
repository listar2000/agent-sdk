# Native runtime scalability — consolidated results

A series of throughput / resource / reliability changes to the **native
runtime** (`src/api/native/` — the first-party server-side LiteLLM agent loop
with tools executing in a sandbox via a transport). Two changes
(`perf(streaming)`) land in shared `sandbox/session.py` and benefit **every**
provider's `/events` stream, not just native.

Branch: `perf/native-frame-encoder` (off `staging`). All changes are green and
each carries a regression test verified to fail on the unfixed code.

## How to reproduce

No cloud credentials, no LLM key, no running server — the benches drive the
runtime through its real path via the `_completion` / `_transport_factory` test
seams:

```bash
.venv/bin/python benchmark/micro/bench_native_frames.py      # frame synthesis
.venv/bin/python benchmark/micro/bench_native_runtime.py     # end-to-end runtime
.venv/bin/python benchmark/micro/bench_b64.py                # transport b64 isolation
```

Numbers below were captured on the dev host; absolute rates scale with the
machine, the ratios reproduce.

## End-to-end native turn throughput (`bench_native_runtime.py`)

Driving `NativeSession.execute_prompt` through loop → emit → frame synthesis →
`_broadcast` → internal queue → generator yield → per-turn checkpoint:

| metric | before (dict-dump frames) | after | delta |
|---|---:|---:|---:|
| single-session turns/s | ~3,650 | ~8,500 | **~2.3×** |
| with 1 live SSE subscriber | 47% of 0-sub | **74%** of 0-sub | +59% |
| with 4 live SSE subscribers | 21% of 0-sub | **50%** of 0-sub | +142% |
| concurrency retention (1→32 sessions) | ~100% | ~100% | flat (no cliff) |
| per-turn cost as a session grows to 2.4k msgs | O(n) (rising) | **O(1) (flat ×1.0)** | quadratic removed |

The runtime is CPU-bound on the single event-loop thread, so aggregate
throughput is *flat* across concurrent sessions (clean horizontal scaling
across replicas) — the lever is reducing per-turn CPU, which the changes do.

## What changed (with measured impact)

| area | change | impact |
|---|---|---|
| frame synthesis | `text`/`reasoning` (per token) → string-concat + per-session cached prefix (`FrameEncoder`) | **7.3×** per event (2150→295 ns) |
| frame synthesis | `tool`/`tool_result` → same fast path | **3.9×** per event (2700→690 ns) |
| dangling-heal | `heal_dangling_tool_calls` re-scanned the whole transcript every turn → bound to the tail | **O(n²)→O(n)** per session; 2000-turn session ~6,900→16,800 turns/s |
| streamed tool args | `acc.args += fragment` on an attribute (GIL defeats CPython's in-place opt) → list+join | **O(n²)→O(n)**; up to ~1187× at 50k fragments |
| **SSE drain (shared)** | `iterate_subscriber` armed an `asyncio.wait_for` timer per event → `get_nowait` hot path, timer only when idle | 1 subscriber **47%→74%**, 4 subs **21%→50%** of 0-sub; all providers |
| internal queue | unbounded SPSC handoff → `put_nowait` / `get_nowait`-first | ~124 coroutine allocs/turn eliminated (GC/resource); +3-5% streaming |
| transport b64 | large `read_file`/`write_file` codec → off-thread above 4 MB | partial loop isolation for multi-MB transfers (GIL-limited; see below) |
| checkpoint write | INSERT…ON CONFLICT + separate DELETE prune → one data-modifying-CTE statement | **2 DB round-trips/turn → 1** on the turn-completion path (remote-PG latency) |
| tool schemas | `Tool.schema` rebuilt per turn → precomputed once in `__post_init__` | **4.8×/turn** (485→102 ns); ~20 dict allocs/turn dropped |
| model-call kwargs | rebuilt every model round → hoisted, constant once per turn | per-round dict rebuild dropped on multi-round tool turns |

## Reliability

* **Transient model-call retries** — LiteLLM `num_retries` (default 2,
  configurable) retries the INITIAL completion call on rate-limit / 5xx /
  connection-reset before the stream starts, so a blip no longer fails the whole
  turn (no mid-stream double-emit).
* **Checkpoint-write retries** — the per-turn conversation checkpoint is the
  resume source of truth; a dropped write silently rewinds the conversation on
  resume. The idempotent upsert is now retried (3 attempts, backoff) on a
  transient DB failure, and never raises into the turn loop.
* **Telemetry parity with the supervisor path** — native provisions compute
  lazily in `_ensure_sandbox` (outside the pool's `timed_op`), so it was a blind
  spot on `/admin/ops` and `/metrics`. Now records op timing (`cold_create` /
  `cold_recover` / `resume`), a recovery signal on silent cold-recover, and
  resource leaks (`native_hibernate_failed` = compute not freed,
  `native_destroy_failed` = orphaned paid VM). Best-effort — never a new failure
  mode on the provisioning path.
* **Durable-wedge stress** — 8 sessions × 6 cycles of interrupt-mid-tool →
  recovery, asserting no dangling tool_calls (the provider-400 shape that bricks
  a session forever), no leaked task, recovery always succeeds.
* **N-way orphan-leak stress** — 24 racing sandbox recoveries × 6 sessions,
  asserting exactly one replacement sandbox per session (an orphan is a paid
  idle daytona/modal VM leaking until reclaimed).
* **De-flaked** the b64 isolation test (wall-clock → mechanism assertion).

Every change above carries a regression test verified to fail on the unfixed
code (the loop's discipline); the full native suite stays green.

## Findings worth keeping

* **`asyncio.to_thread` does NOT isolate the event loop for GIL-bound work.**
  Measured: `to_thread(time.sleep)` (releases the GIL) lets the loop tick 16,228×
  during the call; `to_thread(b64encode 8MB)` and `to_thread(json.dumps 2MB)`
  only ~7× — they hold the GIL, so offloading buys only *partial* isolation
  (periodic ~5 ms GIL-switch windows). The real lever for loop responsiveness is
  **reducing CPU**, not offloading it; true parallelism needs multiple processes.
* **Per-session RAM is conversation-bound** (~0.4 MB for a 600-message session):
  the in-memory `_messages` array (message dict ~232 B + content) is required and
  irreducible without context compaction (a product decision). Hibernated/reaped
  sessions are evicted from the pool and GC'd, so there is no idle-RAM win.

## Deliberately deferred (need human review)

* **Checkpoint write *volume*** — the per-turn round-trip is already halved (the
  CTE above) and a transient-failure retry added, but `write_native_checkpoint`
  still re-serializes the *full* transcript to JSONB every turn (O(n²) DB-write
  bytes / serialization CPU over a session, capped by the context-window
  ceiling). The bounded fix is an append-only-delta + periodic-snapshot storage
  redesign — a schema migration on a durability-critical table with
  retry-idempotency and crash-atomicity implications. Out of scope for an
  automated change; needs human review.
* **Context compaction** — the only lever left for per-session RAM and unbounded
  context growth, but it changes what the model sees (a product decision).
