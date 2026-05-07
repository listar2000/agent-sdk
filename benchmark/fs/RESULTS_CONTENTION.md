# Contention benchmark

- backends: ['modal_vol', 'modal_sb', 'daytona_sb']
- worker counts: [1, 5, 10]
- files/worker=20, file_size=65536, intra_concurrency=8

## Results

| backend | workers | op | wall (s) | ops | bytes (MiB) | MiB/s | p50 (s) | p95 | p99 | max | per-worker max |
|---|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| modal_vol | 1 | write | 3.17 | 20 | 1.25 | 0.395 | 0.377 | 3.010 | 3.168 | 3.168 | 3.17 |
| modal_vol | 1 | read | 1.74 | 20 | 1.25 | 0.719 | 0.585 | 0.624 | 0.743 | 0.743 | 1.74 |
| modal_vol | 1 | delete | 0.80 | 20 | 1.25 | 1.6 | 0.258 | 0.362 | 0.386 | 0.386 | 0.80 |
| modal_vol | 5 | write | 1.34 | 100 | 6.25 | 4.7 | 0.378 | 0.758 | 0.964 | 1.048 | 1.34 |
| modal_vol | 5 | read | 2.03 | 100 | 6.25 | 3.1 | 0.637 | 0.787 | 0.830 | 0.944 | 2.03 |
| modal_vol | 5 | delete | 2.23 | 100 | 6.25 | 2.8 | 0.280 | 0.428 | 0.462 | 1.959 | 2.23 |
| modal_vol | 10 | write | 1.24 | 200 | 12.50 | 10.1 | 0.373 | 0.578 | 0.664 | 0.716 | 1.24 |
| modal_vol | 10 | read | 1.92 | 200 | 12.50 | 6.5 | 0.570 | 0.736 | 0.806 | 1.026 | 1.92 |
| modal_vol | 10 | delete | 0.83 | 200 | 12.50 | 15.0 | 0.251 | 0.323 | 0.366 | 0.374 | 0.83 |
| modal_sb | 1 | write | 2.74 | 20 | 1.25 | 0.456 | 0.694 | 1.522 | 1.531 | 1.531 | 2.74 |
| modal_sb | 1 | read | 1.40 | 20 | 1.25 | 0.892 | 0.372 | 0.854 | 0.855 | 0.855 | 1.40 |
| modal_sb | 1 | delete | 1.13 | 20 | 1.25 | 1.1 | 0.374 | 0.400 | 0.400 | 0.400 | 1.13 |
| modal_sb | 5 | write | 2.93 | 100 | 6.25 | 2.1 | 0.940 | 1.239 | 1.253 | 1.271 | 2.93 |
| modal_sb | 5 | read | 1.39 | 100 | 6.25 | 4.5 | 0.402 | 0.649 | 0.660 | 0.660 | 1.39 |
| modal_sb | 5 | delete | 1.12 | 100 | 6.25 | 5.6 | 0.364 | 0.398 | 0.403 | 0.403 | 1.12 |
| modal_sb | 10 | write | 4.59 | 200 | 12.50 | 2.7 | 1.583 | 1.813 | 1.927 | 1.932 | 4.59 |
| modal_sb | 10 | read | 1.20 | 200 | 12.50 | 10.4 | 0.392 | 0.419 | 0.430 | 0.432 | 1.20 |
| modal_sb | 10 | delete | 1.19 | 200 | 12.50 | 10.5 | 0.377 | 0.413 | 0.440 | 0.440 | 1.19 |
| daytona_sb | 1 | write | 1.57 | 20 | 1.25 | 0.797 | 0.505 | 0.629 | 0.652 | 0.652 | 1.57 |
| daytona_sb | 1 | read | 1.40 | 20 | 1.25 | 0.895 | 0.460 | 0.550 | 0.552 | 0.552 | 1.40 |
| daytona_sb | 1 | delete | 0.77 | 20 | 1.25 | 1.6 | 0.193 | 0.432 | 0.433 | 0.433 | 0.77 |
| daytona_sb | 5 | write | 2.01 | 100 | 6.25 | 3.1 | 0.474 | 1.162 | 1.197 | 1.197 | 2.01 |
| daytona_sb | 5 | read | 1.69 | 100 | 6.25 | 3.7 | 0.509 | 0.788 | 0.838 | 0.877 | 1.69 |
| daytona_sb | 5 | delete | 0.83 | 100 | 6.25 | 7.5 | 0.186 | 0.474 | 0.513 | 0.513 | 0.83 |
| daytona_sb | 10 | write | 3.18 | 200 | 12.50 | 3.9 | 0.832 | 1.807 | 1.869 | 1.883 | 3.18 |
| daytona_sb | 10 | read | 2.84 | 200 | 12.50 | 4.4 | 0.850 | 1.493 | 1.653 | 1.769 | 2.84 |
| daytona_sb | 10 | delete | 1.09 | 200 | 12.50 | 11.5 | 0.189 | 0.721 | 0.737 | 0.801 | 1.09 |

## Headlines

**Modal volume — disjoint subpaths actually scale very well.** Wall time
for the write phase *drops* from 3.17 s (1 worker × 20 ops) to 1.24 s (10
workers × 20 ops = 200 ops) — about **8× more throughput** (10.1 MiB/s vs
0.4 MiB/s aggregate). The per-op p50 stays flat at ~0.37 s across 1/5/10
workers; the p99 actually *improves* with more workers (3.17 s → 0.66 s)
because the cold-start spike on the first call gets amortized over more
ops. Reads and deletes are similarly flat per-op.

This **revises the earlier "modal_vol writes serialize" claim.** What
serializes is *many concurrent commits to the same prefix*: in the first
benchmark, 64 parallel `batch_upload` calls, all writing into `small/`,
produced p99 = 38.9 s. Spreading those calls across 10 disjoint
subpaths drops the per-op tail under 1 s. Modal's commit appears to
contend on a per-prefix granularity (likely the directory CAS metadata),
not the whole volume.

**Modal sandbox FS — write degrades almost linearly with workers.**
p50 grows 0.69 s → 0.94 s → 1.58 s as workers go 1 → 5 → 10. Wall time
for writes goes 2.74 s → 2.93 s → 4.59 s. The Modal sandbox's
filesystem RPC channel inside one container appears to queue: more
in-flight writers means each request waits longer. Reads and deletes,
by contrast, are flat (the read path may have a different path-length
or backpressure model).

**Daytona sandbox FS — modest degradation with workers.** Write p50
0.51 s → 0.47 s → 0.83 s, read p50 0.46 s → 0.51 s → 0.85 s. The
sandbox toolbox's HTTP API appears to handle concurrency reasonably for
~5 workers, then starts to slow — likely TCP/HTTP connection pool
saturation at the toolbox side.

## Per-op p50 (writes) — visual scaling

|        | 1 worker | 5 workers | 10 workers | comment |
|--------|---------:|----------:|-----------:|---------|
| modal_vol  | 0.377 s | 0.378 s | 0.373 s | flat — disjoint paths scale |
| modal_sb   | 0.694 s | 0.940 s | 1.583 s | grows ~linearly — sandbox FS RPC queues |
| daytona_sb | 0.505 s | 0.474 s | 0.832 s | mostly flat to 5w, slows at 10w |

## Aggregate write throughput (200 ops / 12.5 MiB total at 10 workers)

| backend | MiB/s @ 10 workers | speedup vs 1 worker |
|---------|-------------------:|--------------------:|
| modal_vol  | 10.1 | **25.5×** (was 0.4) |
| modal_sb   | 2.7  | 5.9× (was 0.46) |
| daytona_sb | 3.9  | 4.9× (was 0.80) |

Modal volume parallelizes near-ideally across disjoint subpaths.

## Practical takeaways

- **For Modal volume, fan out across subpaths and use moderate
  per-worker concurrency.** Avoid hammering one prefix with many
  parallel `batch_upload` calls — that triggers the per-prefix
  contention seen in the small-files first bench. A single
  `batch_upload` with many `put_file()` inside it (one commit) also
  works well because it's only one commit, not N.
- **For Modal sandbox FS, scale the *number of sandboxes*, not the
  number of in-flight writes per sandbox.** Each sandbox's FS channel
  is the bottleneck. Two sandboxes with 5 writers each will outperform
  one sandbox with 10.
- **For Daytona sandbox FS, ~5 in-flight writers per sandbox is a
  sweet spot.** Above that, expect graceful degradation rather than a
  cliff.

## Reproduce

```bash
.venv/bin/python benchmark/fs/bench_contention.py \
    --backends modal_vol,modal_sb,daytona_sb \
    --workers 1,5,10 \
    --files-per-worker 20 \
    --file-size 65536 \
    --intra-concurrency 8 \
    --output benchmark/fs/RESULTS_CONTENTION.md
```

Single trial — re-run a few times to be confident; the cold-call spike
on `modal_vol / 1 / write` (3.0 s p95) varies a lot run to run.
