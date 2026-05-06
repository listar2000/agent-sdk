# supervisor.js — snapshot compression & request body caps

Status doc for two related supervisor changes that landed in
`src/supervisor/supervisor.js` (uncommitted as of 2026-05-05). Both
exist to keep the supervisor stable under workloads that exercise its
weak spots: large workspaces in the snapshot path, large file uploads
in the `/v1/files/*` path. Filed for review / future merge alongside
the daytona snapshot bump and the Dockerfile change that ships zstd.

Companion runbook: [`runtime-image-rollout.md`](./runtime-image-rollout.md)
(image artifact freshness, modal rebuild blockers, mixed-version
compatibility).

## Issue 1 — snapshot tarballs are uncompressed

### Problem

`runSnapshotOnce` / `runSnapshotSync` / `runAgentMemorySnapshotOnce`
build a tarball of HOME (and `~/.claude/`) and stage it for upload. The
shell command is essentially `tar -cf snapshot.tar -C $HOME .`. For a
realistic workspace (claude code projects, codex cache, etc.) at ~1 GB
the artifact is ~1 GB on the wire and ~1 GB on the volume / S3.

That cost shows up in three places:

* sandbox release latency (the snapshot is a foreground step before
  the supervisor exits or the daytona pause completes),
* per-session storage on the volume,
* network egress to S3 / daytona-managed snapshot storage on every
  release.

### Choice — zstd-1 (not gzip)

Bench (1 vCPU sandbox, realistic workspace, 3 trials each):

| size  | uncompressed wall | gzip-1 wall | zstd-1 wall | zstd-1 size ratio |
|-------|-------------------|-------------|-------------|-------------------|
| 200 M | baseline          | similar     | ≈ baseline  | ~10×              |
| 1 G   | baseline          | **+95 %**   | ≈ baseline  | ~10×              |

zstd-1 trades CPU for bytes evenly enough that wall-clock is
unchanged on a 1-vCPU sandbox — the tar compression CPU is roughly
matched by the staging-copy bytes saved. gzip-1 is ~95 % slower on a
single core at the 1 GB point, so we don't fall back to it; the
choice is "zstd or uncompressed", nothing in between.

The ~10× size win is structure-dependent. Workspaces dominated by
already-compressed assets (PNG, mp4, .tar.gz) compress closer to 1×;
text-heavy claude/codex caches compress higher than 10×. Either way
the wall-clock is unchanged so there's no downside to enabling.

### Implementation

```js
const ZSTD_AVAILABLE = (() => {
  try { return spawnSync("zstd", ["--version"], { stdio: "ignore" }).status === 0; }
  catch { return false; }
})();
const TAR_COMPRESS_ARGS = ZSTD_AVAILABLE ? ["-I", "zstd -1"] : [];
```

Each tar invocation prepends `TAR_COMPRESS_ARGS`:

```js
const tarArgs = [...TAR_COMPRESS_ARGS, "-cf", stage, ...SNAPSHOT_EXCLUDES, "-C", args.root, "."];
```

Compression is **gated on the binary being available**, not on a
config flag. Runtime images that haven't been rebuilt with the new
Dockerfile (which adds `zstd` to apt) silently use the uncompressed
path. This is the safety story for mid-rollout fleets — no
flag-day coordination, no failed snapshots on stale images.

The Dockerfile bump is one line:

```dockerfile
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl nodejs npm git zstd && rm -rf /var/lib/apt/lists/*
```

### Compatibility

`tar -xf` autodetects compression by magic bytes, so old uncompressed
`snapshot.tar` files still on S3 / volumes stay readable through the
restore path. New writes produce zstd content at the same filename —
no rename, no migration script, no compatibility shim.

| Writer → reader               | Result                                         |
|-------------------------------|------------------------------------------------|
| old (no zstd) → old           | uncompressed read uncompressed. Works.         |
| old → new (with zstd)         | new tar autodetects uncompressed. Works.       |
| new → new                     | zstd read zstd. Works.                         |
| new → old (image rollback)    | old tar can't decompress zstd. **Fails.**      |

The fourth row is the one-way upgrade trap: once a session has
written a zstd snapshot, you cannot roll its image back to a build
without zstd or that session's restore will fail. Forward
migration is safe; rollback after zstd writes is not. This is also
called out in `runtime-image-rollout.md`.

A startup log line names which path is in effect:

```
snapshot compression: zstd-1 (artifact ~10× smaller; restore autodetects)
```

or, on a stale image without zstd:

```
snapshot compression: none — zstd not on PATH (safe fallback; bundle zstd in runtime image to enable)
```

## Issue 2 — request bodies aren't bounded before parse

### Problem

The four file-mutation routes (`/v1/files/edit`, `/v1/files/upload`,
`/v1/files/delete`, `/v1/files/rename`) all read the request body the
same way:

```js
let raw = "";
req.setEncoding("utf8");
for await (const chunk of req) raw += chunk;
// then check Buffer.byteLength(raw) > MAX_FILE_SIZE
```

The size check fires **after** the entire body has already been
buffered into V8 heap. A misbehaving (or malicious) client can stream
gigabytes into the supervisor process before the post-loop branch
sees it. At best the supervisor OOM-crashes; at worst it gets
SIGKILL'd by the cgroup limit and takes down the session it was
attached to.

The download route (`/v1/files/download`) had the matching foot-gun
on the response side: `fs.readFileSync(fullPath)` reads the whole
file into a Buffer before sending bytes, so a client that asked for a
multi-gigabyte file (within the path-allowlist but outside any sane
size budget) would also OOM the supervisor.

### Implementation

A single helper bounds the read at `max` bytes and aborts with 413
once exceeded:

```js
async function readBodyCapped(req, res, max) {
  let size = 0;
  const chunks = [];
  for await (const chunk of req) {
    size += chunk.length;
    if (size > max) {
      req.destroy();
      res.writeHead(413, { "content-type": "application/json" });
      res.end(JSON.stringify({ error: "payload too large" }));
      return null;
    }
    chunks.push(chunk);
  }
  return Buffer.concat(chunks).toString("utf8");
}
```

All four mutation routes call it identically:

```js
const raw = await readBodyCapped(req, res, MAX_FILE_SIZE);
if (raw === null) return;  // 413 already sent
```

The download route stops `readFileSync` and pipes via stream, with the
same 20 MB cap applied via `statSync` ahead of the response:

```js
let stat;
try { stat = fs.statSync(fullPath); } catch {
  res.writeHead(404, ...); res.end(...); return;
}
if (stat.isDirectory()) { res.writeHead(404, ...); return; }
if (stat.size > MAX_FILE_SIZE) {
  res.writeHead(413, ...);
  res.end(JSON.stringify({
    error: `file too large: ${stat.size} bytes (max ${MAX_FILE_SIZE})`,
  }));
  return;
}
// stream rather than buffer
res.writeHead(200, { ..., "content-length": stat.size });
fs.createReadStream(fullPath).pipe(res);
```

`MAX_FILE_SIZE` stays at 20 MB, matching what the API server's path
guard already advertises. The cap is a defense-in-depth bound, not a
new contract — well-behaved clients never see 413 because they
already self-limit.

### Compatibility

* Wire format is unchanged. Old clients continue to work.
* Response shapes are unchanged: 413 is the same status the
  download / upload routes already returned for over-cap requests
  (the difference is *when* the check fires).
* No new endpoints, no removed endpoints. Mid-rollout
  fleets running old supervisor + new server (or vice versa) work
  identically.

## Test coverage

Both changes are exercised through the live integration suite by the
existing `tests/test_sandbox_stop_delete_recovery.py` goldens — the
snapshot path runs every release/resume test, the file routes run
every test that uploads/downloads. Neither change has dedicated unit
tests yet because the supervisor's surface is JS-side and the
existing harness is Python; adding node unit tests would mean
introducing a JS test runner.

A practical verification once the changes ship to a runtime image:

```bash
# inside a sandbox built from the new image
which zstd && zstd --version | head -1            # → /usr/bin/zstd
grep -c readBodyCapped /opt/agent-sdk/runtime/supervisor.js  # → 5
grep -c TAR_COMPRESS_ARGS /opt/agent-sdk/runtime/supervisor.js  # → 4
grep -c ZSTD_AVAILABLE /opt/agent-sdk/runtime/supervisor.js     # → 3
tar -I 'zstd -1' -cf /tmp/x.tar -C /tmp data && head -c 4 /tmp/x.tar | od -An -tx1 -N4
# → 28 b5 2f fd  (zstd frame magic)
```

(Daytona snapshot `agent-sdk-8274e9f-dirty-1778038372` already
satisfies these. Modal snapshot is stale — see
`runtime-image-rollout.md`.)

## Open questions / follow-ups

1. **Should `MAX_FILE_SIZE` be configurable per deployment?** Currently
   hard-coded at 20 MB. Some hivespace flows might want a higher cap.
   Knock-on: making it configurable means plumbing a config flag from
   the API server through to supervisor spawn args. Defer until a real
   request lands.
2. **zstd level — is `-1` still the right call at 2+ vCPU?** The bench
   was done on 1 vCPU only. At higher core counts the wall-clock for
   `tar -I 'zstd -3'` may also be unchanged with even better
   compression. Worth re-benching if we ever bump sandbox sizing.
3. **What about `/v1/snapshot` endpoint?** It triggers
   `runSnapshotOnce` which now writes zstd. The endpoint contract
   is unchanged (POST returns 200 once tar finishes); no caller
   needs to know about compression.
