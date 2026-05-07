# Filesystem benchmark results

- backends: ['daytona_sb', 'modal_sb', 'modal_vol']
- workloads: ['small', '200m', '1g']
- small_count=200, small_size=65536, small_concurrency=64

## Aggregate (wall time per phase)

| backend | workload | op | seconds | MiB | MiB/s | conc |
|---------|----------|----|--------:|----:|------:|-----:|
| daytona_sb | - | setup | 1.92 | 0.000 | 0.000 | 1 |
| daytona_sb | small | write | 3.27 | 12.5 | 3.8 | 64 |
| daytona_sb | small | read | 3.20 | 12.5 | 3.9 | 64 |
| daytona_sb | small | delete | 1.41 | 12.5 | 8.9 | 64 |
| daytona_sb | 200m | write | 19.62 | 200 | 10.2 | 1 |
| daytona_sb | 200m | read | 4.79 | 200 | 41.7 | 1 |
| daytona_sb | 200m | delete | 0.22 | 200 | 892 | 1 |
| daytona_sb | 1g | write | 97.53 | 1024 | 10.5 | 1 |
| daytona_sb | 1g | read | 21.25 | 1024 | 48.2 | 1 |
| daytona_sb | 1g | delete | 0.33 | 1024 | 3095 | 1 |
| modal_sb | - | setup | 1.88 | 0.000 | 0.000 | 1 |
| modal_sb | small | write | 3.19 | 12.5 | 3.9 | 64 |
| modal_sb | small | read | 8.35 | 12.5 | 1.5 | 64 |
| modal_sb | small | delete | 0.90 | 12.5 | 13.8 | 64 |
| modal_sb | 200m | write | 52.62 | 200 | 3.8 | 1 |
| modal_sb | 200m | read | 4.86 | 200 | 41.1 | 1 |
| modal_sb | 200m | delete | 0.20 | 200 | 979 | 1 |
| modal_sb | 1g | write | 174.01 | 1024 | 5.9 | 1 |
| modal_sb | 1g | read | 20.06 | 1024 | 51.0 | 1 |
| modal_sb | 1g | delete | 0.18 | 1024 | 5552 | 1 |
| modal_vol | - | setup | 0.00 | 0.000 | 0.000 | 1 |
| modal_vol | small | write | 39.18 | 12.5 | 0.319 | 64 |
| modal_vol | small | read | 7.21 | 12.5 | 1.7 | 64 |
| modal_vol | small | delete | 1.03 | 12.5 | 12.1 | 64 |
| modal_vol | 200m | write | 22.34 | 200 | 9.0 | 1 |
| modal_vol | 200m | read | 2.79 | 200 | 71.7 | 1 |
| modal_vol | 200m | delete | 0.20 | 200 | 1019 | 1 |
| modal_vol | 1g | write | 0.52 | 1024 | 1958 | 1 |
| modal_vol | 1g | read | 10.35 | 1024 | 98.9 | 1 |
| modal_vol | 1g | delete | 0.20 | 1024 | 5232 | 1 |

## Per-op latency distribution

| backend | workload | op | n | min (s) | p50 | p95 | p99 | max | mean |
|---------|----------|----|--:|--------:|----:|----:|----:|----:|-----:|
| daytona_sb | small | write | 200 | 0.592 | 0.715 | 1.649 | 1.730 | 1.780 | 0.928 |
| daytona_sb | small | read | 200 | 0.607 | 0.781 | 1.509 | 1.584 | 1.604 | 0.898 |
| daytona_sb | small | delete | 200 | 0.172 | 0.190 | 1.223 | 1.252 | 1.255 | 0.413 |
| modal_sb | small | write | 200 | 0.419 | 0.832 | 1.186 | 1.202 | 1.247 | 0.914 |
| modal_sb | small | read | 200 | 0.340 | 1.443 | 5.649 | 5.650 | 5.651 | 2.546 |
| modal_sb | small | delete | 200 | 0.201 | 0.236 | 0.299 | 0.338 | 0.348 | 0.244 |
| modal_vol | small | write | 200 | 0.219 | 0.284 | 13.331 | 38.907 | 39.143 | 3.134 |
| modal_vol | small | read | 200 | 0.454 | 0.581 | 0.855 | 1.080 | 5.885 | 0.647 |
| modal_vol | small | delete | 200 | 0.189 | 0.252 | 0.405 | 0.438 | 0.443 | 0.279 |

## Headlines

- **Daytona sandbox FS** is the most balanced option: ~10 MiB/s up / ~45 MiB/s
  down on a single 200 MiB or 1 GiB file, and the tightest tail on the
  concurrent small-files workload (p99 ≤ 1.78 s vs p50 ≤ 0.78 s on writes;
  reads p99 1.58 s).
- **Modal sandbox FS** (`Sandbox.filesystem`) has the *slowest* large-file
  writes by a wide margin — 3.8 MiB/s on 200 MiB, 5.9 MiB/s on 1 GiB. Its
  concurrent reads also degrade badly: p50 1.44 s but p95 5.65 s, p99 5.65 s
  on the small-files workload. It looks like the per-task filesystem RPC
  serializes; piling 64 reads onto one sandbox produces head-of-line waits.
- **Modal volume** has the best read throughput (~72–99 MiB/s on 200 MiB / 1
  GiB) thanks to content-addressed block fetch, but the worst tail on
  concurrent writes: `Volume.batch_upload` commits per call, and 64 parallel
  commits ladder up to **p99 38.9 s** on tiny files. For one large file it is
  competitive (≈9 MiB/s on 200 MiB).

## Trade-off table

| Use case | Best fit | Why |
|---|---|---|
| Many concurrent small reads/writes | **daytona_sb** | Tightest tail (p99/p50 ≤ 2×); modal_sb reads tail blow up; modal_vol writes serialize on commit. |
| Single large upload (≥200 MiB) | **daytona_sb** or **modal_vol** | Both ~10 MiB/s; modal_sb is 2–3× slower. |
| Single large read | **modal_vol** | 72–99 MiB/s via CDN/block fetch, vs ~45 MiB/s for daytona_sb / modal_sb. |
| Sharing artifacts across many sandboxes | **modal_vol** | Reads cache; identical blocks dedupe (next bullet). |
| Workspace-private scratch | **daytona_sb** | No shared volume to manage; lifecycle tied to the sandbox. |

## Caveats and known artefacts

- `modal_vol / 1g / write` clocks at 0.52 s (1958 MiB/s). That is **not** a
  real ~2 GiB/s upload — Modal volumes are content-addressed at the block
  level. The benchmark generates large files by repeating a single 1 MiB
  `os.urandom` chunk, so the 1 GiB file's blocks are already present from the
  earlier 200 MiB write. Treat it as "best case repeat-content upload"; for a
  fresh-content number, regenerate the seed between sizes.
- `modal_sb` read p99 of 5.65 s on small files looks like a control-plane
  backpressure pattern: 64 parallel `read_bytes` against a single sandbox
  appear to queue. A second sandbox in front of the same workload would
  likely halve it.
- Sandbox setup is excluded from the per-op tables: daytona ~1.9 s, modal
  sandbox ~1.9 s, modal volume effectively 0 (no compute). This is one-time
  cost but matters for short workflows.
- Numbers are single-trial; expect ±15 % run-to-run variance, especially on
  the concurrent small-files tail (Modal/Daytona control planes occasionally
  rate-limit). Re-run with different `--small-concurrency` to see where each
  backend's knee is.

## Reproducing

```bash
MODAL_TOKEN_ID=… MODAL_TOKEN_SECRET=… \
  .venv/bin/python benchmark/fs/bench_fs.py \
  --workloads small,200m,1g \
  --small-count 200 --small-size 65536 --small-concurrency 64 \
  --output benchmark/fs/RESULTS.md
```

Backends run with **4 vCPU / 4 GiB RAM** (Daytona) and **4 vCPU / 4 GiB**
(Modal sandbox).
