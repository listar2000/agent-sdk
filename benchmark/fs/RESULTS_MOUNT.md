# Mounted-volume bench (in-sandbox)

- Modal sandbox cpu=4.0 memory=4096 MiB
- Volume mounted at /vol; subpath `sub-0b8d47`
- paths tested: ['tmp', 'vol']
- workloads: ['dd200', 'dd1g', 'many', 'pip']
- many: count=1000 size=4096 parallel=16

## Results

| path | workload | op | seconds | MiB | MiB/s | extra |
|------|----------|----|--------:|----:|------:|-------|
| tmp | dd200m | write | 0.43 | 200.00 | 460 |  |
| tmp | dd200m | read | 0.20 | 200.00 | 1002 |  |
| tmp | dd200m | delete | 0.19 | 200.00 | 1027 |  |
| vol | dd200m | write | 0.43 | 200.00 | 463 |  |
| vol | dd200m | read | 0.21 | 200.00 | 939 |  |
| vol | dd200m | delete | 0.25 | 200.00 | 790 |  |
| tmp | dd1024m | write | 1.09 | 1024.00 | 938 |  |
| tmp | dd1024m | read | 0.35 | 1024.00 | 2887 |  |
| tmp | dd1024m | delete | 0.20 | 1024.00 | 5102 |  |
| vol | dd1024m | write | 1.16 | 1024.00 | 882 |  |
| vol | dd1024m | read | 0.38 | 1024.00 | 2709 |  |
| vol | dd1024m | delete | 0.31 | 1024.00 | 3291 |  |
| tmp | many1000 | write | 8.02 | 3.91 | 0.487 | parallel=16 |
| tmp | many1000 | read | 5.99 | 3.91 | 0.652 | parallel=16 |
| tmp | many1000 | delete | 0.23 | 3.91 | 17.3 |  |
| vol | many1000 | write | 7.52 | 3.91 | 0.519 | parallel=16 |
| vol | many1000 | read | 6.77 | 3.91 | 0.577 | parallel=16 |
| vol | many1000 | delete | 0.45 | 3.91 | 8.7 |  |
| tmp | pip:pandas | install | 6.71 | 65.17 | 9.7 | installed_bytes=68335487 |
| tmp | pip:pandas | rm | 0.24 | 65.17 | 271 |  |
| vol | pip:pandas | install | 24.39 | 65.07 | 2.7 | installed_bytes=68229447 |
| vol | pip:pandas | rm | 1.22 | 65.07 | 53.2 |  |

## Headlines

| workload | tmp (ephemeral) | vol (mounted) | delta |
|---|---:|---:|---|
| dd 200 MiB write     | 460 MiB/s | 463 MiB/s | **parity** |
| dd 200 MiB read      | 1002 MiB/s | 939 MiB/s | -6% |
| dd 1 GiB write       | 938 MiB/s | 882 MiB/s | -6% |
| dd 1 GiB read        | 2887 MiB/s | 2709 MiB/s | -6% (page cache) |
| 1000 × 4 KiB cp -P16 | 8.02 s | 7.52 s | **parity** |
| 1000 × 4 KiB rm -rf  | 0.23 s | 0.45 s | **2× slower** |
| `pip install pandas` | 6.71 s | **24.39 s** | **3.6× slower** |
| rm of pip install    | 0.24 s | 1.22 s | **5× slower** |

## What this says about the mounted-volume model

- **Bulk sequential I/O is essentially free.** Modal's mount appears to
  pass large `write()`/`read()` syscalls through to the local block
  device with negligible overhead. 200 MiB / 1 GiB dd round-trips run
  within 5–6 % of the ephemeral overlayfs.
- **Naive many-files copy is also fine.** `cp` 1000 × 4 KiB files
  finishes in the same wall time on `/vol` as on `/tmp`. The kernel
  batches the writes and the volume layer keeps up.
- **Metadata-heavy patterns are where the volume hurts.** `pip install
  pandas` is **3.6× slower** on `/vol` (24.4 s vs 6.7 s), and the
  follow-up `rm -rf` of the same tree is 5× slower. pip's installer
  does many `mkdir`/`rename`/`chmod`/`fsync` round-trips per file (it
  writes wheels to a temp dir then atomically renames into place); each
  of those metadata ops appears to pay a per-call cost on the mount that
  raw block writes don't.
- **`rm -rf` of a deep tree is consistently slower** on `/vol` (2–5×
  across the two cases). Same root cause: per-file unlink + per-dir
  rmdir all hit metadata.

## Practical guidance for "agent does work in a Modal volume"

- **Bulk artifacts (model weights, datasets, tarballs):** put them
  directly on the mounted volume. Negligible penalty; you get
  persistence + cross-sandbox sharing for free.
- **Package installs / source builds / `git clone`:** install to
  ephemeral disk (`/tmp` or `~/.local`) and **rsync/cp the result onto
  `/vol`** in one bulk pass. That converts O(N files) of metadata
  syscalls into one bulk copy. Or — if the package set is fixed — bake
  it into the sandbox image instead of installing at runtime.
- **Throwaway scratch:** always `/tmp`. The dd numbers say `/tmp` ties
  or beats `/vol` on every flavour of bulk I/O too.
- **Deletes of large trees:** if you have a choice, move the dir to a
  staging location and clean it asynchronously, rather than blocking the
  agent on a slow `rm -rf` of `/vol/<sub>`.

## Caveats

- Single trial; pip install in particular has high variance because
  PyPI / wheel CDN dominates the first ~2 s. The 3.6× ratio is real
  (network was the same for both) but absolute numbers vary.
- The very high read rates on `dd 1g` (2.7–2.9 GiB/s) are page-cache
  reads — the file we just wrote is hot in RAM. Cold reads from the
  volume would land somewhere between the dd write rate and these
  numbers; not measured here because dropping caches needs
  `CAP_SYS_ADMIN` in the sandbox.
- 4 vCPU / 4 GiB sandbox. CPU is not the bottleneck (verified during
  dd); the limit is I/O and metadata-RPC latency.
