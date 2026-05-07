# Filesystem benchmark — Daytona / Modal

Compares three remote-FS backends:

| backend     | API                                                    |
|-------------|--------------------------------------------------------|
| `daytona_sb`| `daytona-sdk` `Sandbox.fs.{upload,download,delete}_file` |
| `modal_sb`  | `modal.Sandbox.filesystem.{copy_from_local,copy_to_local,remove}` |
| `modal_vol` | `modal.Volume.batch_upload` + `read_file` + `remove_file` |

Workloads:

- `small` — `--small-count` files of `--small-size` bytes, dispatched
  concurrently with `--small-concurrency` in-flight ops. Per-op latency
  distribution is reported (min / p50 / p95 / p99 / max / mean).
- `200m` — single 200 MiB file: write, read, delete (sequential).
- `1g` — single 1 GiB file.
- `2g` — single 2 GiB file (opt-in — large local disk + slow uploads).

Each provider sandbox is provisioned with **4 vCPU / 4 GiB RAM**
(`Resources(cpu=4, memory=4)` for Daytona, `cpu=4, memory=4096` for Modal).
The asyncio default thread pool is bumped to 128 workers so per-call
synchronous SDK methods can run truly in parallel.

## Run

```bash
MODAL_TOKEN_ID=…  MODAL_TOKEN_SECRET=…  DAYTONA_API_KEY=… \
  .venv/bin/python benchmark/fs/bench_fs.py \
  --workloads small,200m,1g \
  --small-count 200 --small-size 65536 --small-concurrency 64 \
  --output benchmark/fs/RESULTS.md
```

Or load creds from `~/.env`:

```bash
.venv/bin/python benchmark/fs/bench_fs.py --workloads small,200m,1g
```

Useful flags:

| flag | default | what it does |
|------|---------|---|
| `--backends` | all three | comma-list subset of `daytona_sb,modal_sb,modal_vol` |
| `--workloads` | `small,200m,1g` | subset of `small,200m,1g,2g` |
| `--small-count` | 200 | number of files in the small workload |
| `--small-size` | 65536 | bytes per small file |
| `--small-concurrency` | 64 | max in-flight ops |
| `--executor-workers` | 128 | size of the asyncio thread pool |
| `--output` | – | write the markdown report to this path |

See [`RESULTS.md`](RESULTS.md) for a full run from 2026-05-07 with
analysis and trade-offs.

## Caveats

- The bench generates large files by repeating a 1 MiB `os.urandom` chunk
  for speed. **Modal volumes content-address blocks**, so a 1 GiB upload
  whose blocks are already on the volume from a previous workload size
  will report unrealistically high "throughput". Look for a sub-second
  number on `modal_vol / 1g / write` after `modal_vol / 200m / write` —
  that's dedup, not 2 GiB/s.
- Single-trial. Real reporting should run 3+ trials and take medians;
  control-plane variance is highest on the concurrent small-files tail.
- The Modal sandbox is created with `sleep infinity`; its lifecycle is
  bound by `--executor-workers` (the bench teardowns terminate it
  unconditionally). Daytona sandboxes are tagged `agent_sdk_origin=test`
  and have `auto_delete_interval=15` minutes as a safety net.
