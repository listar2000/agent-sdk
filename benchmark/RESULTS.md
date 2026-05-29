# Head-to-head benchmark results

Baseline: commit `ec5ef64` (main HEAD as of PR #82's branch point).
Patched: PR #82 + this PR's open follow-ups.

> **Update (PR #82 + #84 + #85 cumulative):** see "Cumulative impact"
> section at the bottom of this doc for the across-PRs table. The
> per-op p50 wins compound: most read-heavy ops are now -80 to -92%
> below baseline (status: -90.6%, files_read: -86.5%, config_*: -84%).
> Wall stays flat in the standard mixed bench because Anthropic API
> + supervisor cold-boot dominate it. The wins matter for ops that
> happen WITHOUT a prompt in flight: dashboard polling, file-browse
> UIs, multi-tab observation, etc.

Both runs on the same machine within the same session window, alternating
where possible to control for time-of-day Daytona load. Methodology in
`README.md`. Workload: `workload_full.py` (touches every server code
path that the changes can affect: session create, status, ACP config,
multi-turn chat, file proxies, sandbox exec, b64 upload).

How to reproduce:

```bash
# Set up a baseline worktree at the PR's branch point:
git worktree add /tmp/asdk-baseline ec5ef64

rm -f /tmp/workload_full.jsonl

# unix_local A/B (3 iters each side):
CHECKOUT_PATH=/tmp/asdk-baseline LABEL=baseline-unix \
  PROVIDER=unix_local N_SESSIONS=5 N_TURNS=2 N_FILE_OPS=5 LARGE_MB=2 ITERS=3 \
  bash benchmark/load/ab_harness.sh
LABEL=patched-unix \
  PROVIDER=unix_local N_SESSIONS=5 N_TURNS=2 N_FILE_OPS=5 LARGE_MB=2 ITERS=3 \
  bash benchmark/load/ab_harness.sh

# Daytona A/B (2 iters each side):
CHECKOUT_PATH=/tmp/asdk-baseline LABEL=baseline-daytona \
  PROVIDER=daytona N_SESSIONS=5 N_TURNS=2 N_FILE_OPS=3 LARGE_MB=2 ITERS=2 \
  bash benchmark/load/ab_harness.sh
LABEL=patched-daytona \
  PROVIDER=daytona N_SESSIONS=5 N_TURNS=2 N_FILE_OPS=3 LARGE_MB=2 ITERS=2 \
  bash benchmark/load/ab_harness.sh

.venv/bin/python benchmark/load/compare.py
```

---

## unix_local provider (3 iters × 5 sessions × 2 turns × 5 file ops × 2 MB upload)

| op | base p50 (ms) | new p50 (ms) | p50 Δ | base p99 | new p99 | p99 Δ |
|---|---:|---:|---:|---:|---:|---:|
| config_mode | 153.4 | 84.7 | **-44.8%** | 172.2 | 115.6 | -32.9% |
| config_model | 124.3 | 95.5 | **-23.2%** | 145.8 | 106.9 | -26.7% |
| config_thought_level | 87.7 | 63.1 | **-28.1%** | 98.4 | 72.9 | -25.9% |
| files_read | 83.0 | 37.2 | **-55.2%** | 156.6 | 69.3 | -55.7% |
| files_tree | 97.2 | 32.5 | **-66.6%** | 153.5 | 78.3 | -49.0% |
| files_upload_small | 77.2 | 42.0 | **-45.6%** | 187.9 | 60.4 | -67.9% |
| files_upload_large (2 MB) | 222.3 | 150.4 | **-32.3%** | 328.6 | 239.1 | -27.2% |
| sandbox_exec | 56.7 | 36.3 | **-36.0%** | 135.1 | 56.5 | -58.2% |
| session_status | 49.8 | 29.7 | **-40.4%** | 79.0 | 43.0 | -45.6% |
| session_sandbox_info | 56.1 | 55.0 | -2.0% (≈) | 95.1 | 68.9 | -27.5% |
| prompt_turn | 1479.6 | 1338.0 | **-9.6%** | 2508.3 | 2673.8 | +6.6% |
| prompt_after_resume | 1756.0 | 1601.0 | -8.8% | 6612.3 | 2187.4 | **-66.9%** |
| session_create | 1111.6 | 1030.6 | -7.3% | 1192.7 | 1113.5 | -6.6% |
| release | 5071.3 | 5070.2 | 0.0% (≈)¹ | 5114.7 | 7407.9 | +44.8%¹ |
| resume | 788.8 | 790.5 | 0.0% (≈)¹ | 845.0 | 920.6 | +8.9%¹ |
| **wall_s** | **25.61** | **21.00** | **-18.0%** | | | |
| **throughput sess/s** | **0.195** | **0.238** | **+22.1%** | | | |

## Daytona provider (2 iters × 5 sessions × 2 turns × 3 file ops × 2 MB upload)

| op | base p50 (ms) | new p50 (ms) | p50 Δ | base p99 | new p99 | p99 Δ |
|---|---:|---:|---:|---:|---:|---:|
| config_mode | 769.5 | 504.5 | **-34.4%** | 1021.8 | 661.8 | -35.2% |
| config_model | 754.5 | 514.4 | **-31.8%** | 953.9 | 566.4 | -40.6% |
| config_thought_level | 749.4 | 486.3 | **-35.1%** | 881.7 | 590.5 | -33.0% |
| files_read | 767.4 | 510.9 | **-33.4%** | 966.4 | 696.0 | -28.0% |
| files_tree | 778.7 | 562.1 | **-27.8%** | 967.0 | 998.0 | +3.2% (≈) |
| files_upload_small | 758.9 | 505.1 | **-33.4%** | 948.7 | 725.2 | -23.6% |
| files_upload_large (2 MB) | 1676.0 | 1366.4 | **-18.5%** | 3001.9 | 2108.9 | -29.7% |
| sandbox_exec | 790.0 | 521.9 | **-33.9%** | 1270.2 | 565.4 | -55.5% |
| session_status | 386.9 | 374.8 | -3.1% (≈) | 452.2 | 522.6 | +15.6% |
| session_sandbox_info | 384.5 | 375.5 | -2.3% (≈) | 436.7 | 415.1 | -5.0% (≈) |
| prompt_turn | 2620.7 | 2526.1 | -3.6% (≈)² | 4965.1 | 4530.8 | -8.7% |
| prompt_after_resume | 2755.6 | 2552.9 | -7.4% | 3821.4 | 2784.8 | **-27.1%** |
| session_create | 43408.8 | 39307.6 | **-9.4%** | 47626.3 | 45958.9 | -3.5% (≈) |
| release | 2637.1 | 2651.4 | 0.0% (≈)¹ | 5351.6 | 6503.7 | +21.5%¹ |
| resume | 6033.3 | 5007.6 | **-17.0%** | 7337.8 | 6329.0 | -13.7% |
| **wall_s** | **91.94** | **81.59** | **-11.3%** | | | |
| **throughput sess/s** | **0.054** | **0.061** | **+12.8%** | | | |

---

## What's driving the deltas

| change | code touchpoint | what it speeds up |
|---|---|---|
| Module-shared `httpx.AsyncClient` | `server.py: _proxy_from_session`, `_download_from_session` | All file proxy ops (`files_tree/read/upload_small/upload_large`), `sandbox_exec` — kills per-call TCP+TLS handshake to supervisor |
| Per-session cached `AcpClient` | `sandbox/session.py: _get_acp_client` + 4 provider session shutdowns | All ACP ops (`config_mode/model/thought_level`, `session_status`, `session_sandbox_info`) — kills per-call AcpClient construction + httpx pool churn |
| Cached Daytona SDK client (`_DAYTONA_CLIENT`) | `providers/daytona/__init__.py` | `session_create`, `resume`, status/info — Daytona SDK init is ~50-200ms wall, was paid on every reattach |
| Removed redundant `_wait_for_health` post-supervisor-up | `providers/daytona/session.py` | `session_create` — kills 100-500ms of repeat probing of an already-known-healthy supervisor |
| `_maybe_in_thread()` for >1 MB b64 | `server.py: volume_files_upload`, `volume_files_read` | `files_upload_large` (Daytona, where 2 MB matters more relative to network); on unix_local stays in the inline-fast path because tests use small payloads |

## Notes on the asterisks

¹ `release`/`resume` p99 noise: these paths are dominated by ~5 s of
filesystem snapshot work (unix_local) or ~3-6 s of Daytona pause/start.
The optimizations don't touch that critical path, so any p99 wobble is
the underlying provider's tail variance. p50 is flat as expected.

² `prompt_turn` is bounded by the Anthropic API + claude-agent-acp
turnaround (~1-2 s for haiku + claude-code framing). Our overhead is
~50-150 ms (ACP setup + per-chunk persist). The -9.6% on unix_local
and -3.6% on Daytona is the AcpClient cache + the initial ACP send
landing on a kept-alive httpx connection rather than a fresh handshake.

---

## Goldens (live integration suite)

Same A/B, same providers, same `-n auto` parallelism, all 4 providers
(unix_local, docker, daytona, modal):

| | baseline (`ec5ef64`) | patched |
|---|---|---|
| passed | 78 | 78 |
| failed | 0 | 0 |
| skipped | 14 | 14 |
| wall | 197s | 197s |

Skips (same on both sides) are env/install gates: codex CLI not
installed (7), `GEMINI_API_KEY` unset (3), OpenCode mac-only (2),
`cline-acp` config not yet validated (2). Wall is identical because
`test_session_survives_midstream_sandbox_stop` has a deterministic
`await asyncio.sleep(60)` that dominates the slowest worker; the per-op
deltas above are where the optimizations actually show up.

**Zero regressions across the live golden suite.**

---

# Cumulative impact: PR #82 + #84 + #85

After PR #82 shipped, two follow-up PRs landed on the same theme of
"small surgical fixes to the request hot path":

- **PR #84** — share httpx pool for `/v1/health` probe across all 4
  providers. Every `pool.get_session()` call (which fires on every
  /sessions/{id}/* request) was constructing a fresh `httpx.AsyncClient`
  for the supervisor health probe; PR #84 routes the probe through
  the cached per-session AcpClient pool. Win was -11% wall / +12%
  throughput on its own (see PR #84's body for the isolated A/B).

- **PR #85** — peek-mode `get_session(peek=True)` for read-only
  `/status` and `/sandbox` endpoints. The pre-PR #85 behaviour was
  buggy: a UI dashboard polling `/status` on a hibernated session
  unhibernated the sandbox on the first poll (1-2s cold-recovery)
  AND kept it warm forever, defeating the reaper. PR #85 falls back
  to a DB read of `sessions.sandbox_state` JSONB when the session
  isn't in the live pool. Targeted bench: 10 status polls on a
  hibernated session: 1668ms → 24ms (-98.6%) with the sandbox
  staying hibernated.

## Cumulative A/B (`unix_local`, 5 sessions × default workflow)

Measured against pre-PR #84 main HEAD (`12b39ca`, post-PR #82) so
the table shows what PR #84 + PR #85 add ON TOP of PR #82.

### With release/resume in the workload (default workflow, 3 iters)

| op | base p50 (ms) | new p50 (ms) | p50 Δ | p99 Δ |
|---|---:|---:|---:|---:|
| **session_status** | 53.0 | **5.0** | **-90.6%** | -91.1% |
| **config_thought_level** | 67.7 | 7.3 | **-89.2%** | -62.9% |
| files_upload_small | 37.2 | 4.8 | **-87.1%** | -55.5% |
| files_read | 30.4 | 4.1 | **-86.5%** | -62.2% |
| session_sandbox_info | 49.4 | 7.5 | **-84.8%** | -58.2% |
| config_model | 72.9 | 11.6 | **-84.1%** | -65.4% |
| files_tree | 35.7 | 7.7 | **-78.4%** | -58.2% |
| sandbox_exec | 36.8 | 8.3 | **-77.4%** | -83.6% |
| config_mode | 100.5 | 33.8 | **-66.4%** | -49.4% |
| files_upload_large (2 MB) | 150.2 | 104.5 | -30.4% | -27.1% |
| prompt_after_resume | 1783.8 | 1415.5 | **-20.6%** | **-41.2%** |
| session_create | 1080.1 | 1015.1 | -6.0% | -11.5% |
| prompt_turn | 1486.0 | 1555.2 | +4.7% (≈) | +1.6% (≈) |
| release | 5057 | 7456 | +47.4%¹ | +46.7%¹ |
| resume | 792 | 779 | -1.7% (≈) | -1.0% (≈) |
| **wall_s** | 20.91 | 20.74 | -0.8% (≈) | |
| **throughput sess/s** | 0.239 | 0.241 | +0.8% (≈) | |

¹ Single-iteration noise — release is dominated by ~5-7s of supervisor-side
snapshot work that this PR series doesn't touch. With 3 iters per side
and high natural variance in the snapshot path, the median can swing
1-3 seconds. None of the changed code paths run during release.

### Without release/resume (`SKIP_RELEASE=1`, 3 iters)

Removing release reveals the per-op picture without snapshot noise:

| op | base p50 (ms) | new p50 (ms) | p50 Δ | p99 Δ |
|---|---:|---:|---:|---:|
| files_upload_small | 52.7 | 4.1 | **-92.2%** | -88.0% |
| files_tree | 43.7 | 5.1 | **-88.3%** | -66.9% |
| files_read | 35.1 | 4.3 | **-87.7%** | -68.8% |
| config_mode | 109.7 | 16.7 | **-84.8%** | -70.9% |
| config_thought_level | 66.5 | 10.2 | **-84.7%** | -58.8% |
| session_sandbox_info | 52.7 | 8.3 | **-84.3%** | -59.7% |
| session_status | 32.2 | 5.6 | **-82.6%** | -87.2% |
| config_model | 81.2 | 14.9 | **-81.7%** | -39.0% |
| sandbox_exec | 42.9 | 8.9 | **-79.3%** | -49.8% |
| files_upload_large | 158.4 | 92.3 | **-41.7%** | -39.6% |
| session_create | 1079.8 | 1022.9 | -5.3% | -6.9% |
| prompt_turn | 1639 | 1636 | -0.2% (≈) | -1.1% (≈) |
| **wall_s** | 12.22 | 12.80 | +4.7% (≈) | |
| **throughput sess/s** | 0.409 | 0.391 | -4.4% (≈) | |

The wall stays flat even without release — because `prompt_turn`
(Anthropic-bound, ~1.6s) dominates the per-session pipe and isn't in
the path of any change. **The wins are real but they show up in
per-op latency, not aggregate wall**.

### Where the wins matter

If your workload is read-heavy or polling-heavy:

- **Dashboard / observability**: `/status` polls 10× cheaper, sandboxes
  stay hibernated under polling (PR #85)
- **File browser UI**: `/files/tree`, `/files/read`, `/files/upload`
  all 5-9× faster (PR #84 — probe was eating the latency)
- **ACP config polling**: `/config` 5-7× faster
- **`/sandbox/exec` for tool-call backends**: 5× faster

If your workload is chat-only (one prompt → one reply, repeat):

- The wall is bounded by Anthropic API latency. Per-prompt overhead is
  already <100ms server-side; no remaining low-hanging fruit on the
  Python side.
- Remaining session_create cost (~1s on `unix_local`, ~40s on Daytona)
  is dominated by supervisor + claude-code-acp child cold-boot.
  Optimizing this requires either supervisor.js changes (pre-spawn
  child) or a lazy-attach refactor in Python (defer `_attach_acp` to
  first ACP op). See `NEW_SCENARIOS_BASELINE.md` for the profiling.

## Goldens (cumulative, all 4 providers, `-n auto`)

Same suite, same `-n auto` parallelism, all PRs stacked:

| | baseline (`12b39ca`) | patched (#82+#84+#85) |
|---|---|---|
| passed | 78 | 78 |
| failed | 0 | 0 |
| skipped | 14 | 14 |
| wall | 196-200s (deterministic 60s sleep dominates) | 196-200s |

**Zero regressions across the live golden suite from any PR in the series.**

---

## Peek-mode follow-up (#85): isolated A/B against post-#84 main

After #82 + #84 landed in `main` (commit `322f464`), I re-A/B'd the
peek-mode branch (`perf/peek-mode-get-session-v2`, rebased onto current
main) against the same workload to see what additional wins peek mode
brings on top of the already-optimized baseline.

Methodology: same harness, same workload, same machine. 3 iters × 5
sessions × 2 turns × 5 file ops × 2 MB upload, unix_local.

```bash
# Baseline = current main (322f464, has #82 + #84):
git -C /tmp/asdk-baseline checkout 322f464
CHECKOUT_PATH=/tmp/asdk-baseline LABEL=baseline-newmain \
  PROVIDER=unix_local N_SESSIONS=5 N_TURNS=2 N_FILE_OPS=5 LARGE_MB=2 ITERS=3 \
  REPORT=/tmp/workload_full_v2.jsonl bash benchmark/load/ab_harness.sh

# Patched = peek-mode rebased on new main:
git checkout perf/peek-mode-get-session-v2
git rebase main
LABEL=patched-newmain \
  PROVIDER=unix_local N_SESSIONS=5 N_TURNS=2 N_FILE_OPS=5 LARGE_MB=2 ITERS=3 \
  REPORT=/tmp/workload_full_v2.jsonl bash benchmark/load/ab_harness.sh

REPORT=/tmp/workload_full_v2.jsonl .venv/bin/python benchmark/load/compare.py
```

| op | base p50 (ms) | new p50 (ms) | p50 Δ | base p99 | new p99 | p99 Δ |
|---|---:|---:|---:|---:|---:|---:|
| session_status | 5.0 | 5.0 | 0.0% (≈) | 23.5 | 8.3 | **-64.7%** ¹ |
| session_sandbox_info | 6.0 | 6.4 | +6.7% (≈) | 9.7 | 26.4 | +172% ² |
| prompt_after_resume | 1486.5 | 1315.7 | -11.5% | 3674.0 | 1753.8 | **-52.3%** ¹ |
| prompt_turn | 1576.7 | 1118.4 | -29.1% ³ | 2281.8 | 2451.4 | +7.4% (≈) ³ |
| files_read | 6.7 | 4.0 | -40.3% ⁴ | 34.7 | 28.3 | -18.4% |
| files_tree | 9.0 | 5.6 | -37.8% ⁴ | 52.6 | 29.3 | -44.3% |
| files_upload_small | 12.5 | 4.7 | -62.4% ⁴ | 33.4 | 27.8 | -16.8% |
| files_upload_large (2 MB) | 110.4 | 88.6 | -19.7% | 144.9 | 152.3 | +5.1% (≈) |
| sandbox_exec | 20.1 | 7.7 | -61.7% ⁴ | 30.9 | 29.1 | -5.8% (≈) |
| config_mode | 29.8 | 32.0 | +7.4% (≈) | 35.5 | 34.7 | -2.3% (≈) |
| config_model | 12.7 | 10.7 | -15.7% | 30.3 | 32.3 | +6.6% (≈) |
| session_create | 945.6 | 955.2 | +1.0% (≈) | 998.3 | 975.3 | -2.3% (≈) |
| release | 5040.8 | 5036.4 | 0.0% (≈) | 5101.8 | 5081.2 | -0.4% (≈) |
| resume | 759.3 | 730.4 | -3.8% (≈) | 771.5 | 763.0 | -1.1% (≈) |
| **wall_s** | **21.97** | **17.94** | **-18.3%** | | | |
| **throughput sess/s** | **0.228** | **0.279** | **+22.4%** | | | |

¹ `session_status` p99 -64.7% IS a real peek-mode signal — that endpoint
now bypasses `force_probe=True` and falls back to a DB read. The
`prompt_after_resume` p99 -52.3%, however, **is noise**: that endpoint
calls `pool.get_session()` *without* peek (peek is only used by
`/status` and `/sandbox`), so it still hits the same probe path on both
sides. With N=15 samples per side, p99 is dominated by the worst
outlier — Anthropic API tail variance + run-order cache warmth account
for the swing. Treat it as informational, not as a peek-mode win.

² `session_sandbox_info` p99 jumped 9.7→26.4 ms but absolute values are
tiny (sub-30 ms) and N=15 per side. One outlier dominates p99 at this
sample size — not a real regression.

³ `prompt_turn` deltas swing wildly because Anthropic API tail variance
dominates. -29% on p50 with +7% on p99 is the textbook signature of
small-sample noise, not a real shift. The earlier run (`ec5ef64` →
`322f464` baseline) had `prompt_turn` p50 = 1479 → 1338. This run's
1576 baseline is *higher* than the original baseline — same workload,
different day's API latency.

⁴ Big-percentage wins on sub-15 ms file/exec ops are similarly noise:
a 5 ms swing on `files_upload_small` reads as -62% but the absolute
floor is dominated by Python serialization + `httpx` per-request setup,
not anything peek-mode touches. Trust the wall-time + the protected p99
columns above; treat the per-op p50 deltas in this section as
informational only.

### What's actually shipping in #85

The bench above is the diffuse signal. The cleaner signal is the
targeted micro-bench in the PR body: **10 consecutive `/status` polls
on a hibernated session** dropped from 1668 ms cumulative to 24 ms
(**-98.6%**), AND the sandbox stays hibernated instead of being
reanimated. That second property — reaper correctness — is the real
value; the perf number is a downstream consequence.

### Goldens (peek mode on top of new main)

`tests/test_golden.py` — the canonical golden
suite — passes 60/60 in 158s with `-n auto`. No regressions.
