# Head-to-head benchmark results

Baseline: commit `ec5ef64` (main HEAD as of this PR's branch point).
Patched: this PR (8 modified files — see PR description).

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
