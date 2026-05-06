# Runtime image rollout — notes & open issues

Status snapshot, **not for merging yet**. Captures the operational state
of the runtime image / snapshot artifacts as of 2026-05-05 so the work
isn't lost between sessions.

## Background — what the tags are

Three repo-root files pin the runtime artifact each provider boots a
sandbox from:

| File                     | Provider | Read by                                                  |
|--------------------------|----------|----------------------------------------------------------|
| `.runtime-image-tag`     | docker   | `docker.create_sandbox` (also fallback for daytona)      |
| `.runtime-snapshot-tag`  | daytona  | `daytona.create_sandbox`                                 |
| `.modal-snapshot-tag`    | modal    | `modal._get_image()`                                     |

`unix_local` has no image — its supervisor is `<repo>/src/supervisor/`
or `/opt/agent-sdk/runtime/` from the Docker layer.

All four are produced by `scripts/release.sh` (docker + daytona) and
`scripts/release_modal_snapshot.py` (modal). The script docstrings
spell out the trigger conditions:

> Run after every change that affects the runtime image:
>   * `Dockerfile`
>   * `src/supervisor/package.json` / `supervisor.js`
>   * `src/api/providers/_shared.py:_ACP_NPM_SPECS`

## Current state (2026-05-05)

Two image-affecting in-flight changes are uncommitted on this checkout:

1. `Dockerfile` — adds `zstd` to the apt install line.
2. `src/supervisor/supervisor.js`
   * `ZSTD_AVAILABLE` / `TAR_COMPRESS_ARGS` — snapshot tarballs are
     written zstd-1 when zstd is on PATH; uncompressed fallback
     otherwise. `tar -xf` autodetects on restore so old uncompressed
     tars stay readable.
   * `readBodyCapped` helper — bounds `/v1/files/*` request bodies at
     20 MB to keep a misbehaving client from spilling gigabytes into
     V8 heap before the size check fires.
   * `handleFilesDownload` streams via `createReadStream` instead of
     reading the whole file into memory; adds the same 20 MB cap.

| Artifact                | State                                       |
|-------------------------|---------------------------------------------|
| `.runtime-snapshot-tag` | **fresh** — `agent-sdk-8274e9f-dirty-1778038372`. Verified inside a daytona sandbox: `zstd` v1.5.7 on PATH, `supervisor.js` contains `readBodyCapped`/`TAR_COMPRESS_ARGS`/`ZSTD_AVAILABLE`, end-to-end `tar -I 'zstd -1'` produces magic bytes `28 b5 2f fd`. |
| `.runtime-image-tag`    | unchanged — local-build only, refresh when next pushing the docker image. |
| `.modal-snapshot-tag`   | **stale** — `im-ZbhjkuIEhTyTeE74mOThZA`. Probed: zstd MISSING, `readBodyCapped`/`ZSTD_AVAILABLE` count 0. Rebuild attempted; failed with `modal.exception.ResourceExhaustedError: Workspace has exceeded its spend limit`. |

The stale modal snapshot is **not currently breaking goldens** —
`supervisor.js` gates compression behind `ZSTD_AVAILABLE`, so on the
old modal image (no zstd) tar invocations transparently use the
uncompressed path. The 20 MB body cap is missing, but no current
goldens POST anywhere near that size. So the modal goldens pass on
the old snapshot; the rebuild is hygiene, not a hot fix.

## Open issues

### 1. Modal rebuild blocked on workspace spend limit

```
$ python scripts/release_modal_snapshot.py
modal.exception.ResourceExhaustedError: Workspace has exceeded its spend limit
```

Resolution path:
* bump the modal workspace's spend limit, OR
* run the rebuild from a different modal workspace.

After resolving, run the script and commit `.modal-snapshot-tag`
alongside the Dockerfile/supervisor.js diff.

### 2. Modal `_get_image` is process-memoized

`src/api/providers/modal/__init__.py:_get_image` caches the resolved
`modal.Image` in a module-level `_image` global. Once the running
server has populated it from `.modal-snapshot-tag`, **changing the
file on disk has no effect for the lifetime of that process** — even
for new sandboxes. The server must be restarted for a refreshed
modal snapshot to take effect.

Daytona and docker re-read their tag files inside `create_sandbox`
on every call, so they're hot-swap. Only modal needs the restart.

If we end up doing modal-snapshot rotations more often, we should
either drop the memoization or invalidate it when the file mtime
changes. Not load-bearing today — keeping the memo is fine for
weekly-or-rarer cadence.

### 3. Image rollout safety — already-running sandboxes

Per-provider behavior when one of the tag files changes:

* **Already-running sandboxes never reload their image.** The tag
  files are read only inside `create_sandbox`. Reattach,
  `restart_supervisor`, `get_sandbox_status`, recovery — none of them
  re-resolve the image. Every running session keeps its baked-in
  supervisor.js for the rest of its life. No errors.
* **New sandboxes** post-tag-change use the new image (modal needs
  the server restart noted above).

Snapshot format compatibility under upgrade:

| Writer → reader               | Result                                            |
|-------------------------------|---------------------------------------------------|
| old supervisor → old supervisor | uncompressed read uncompressed. Works.          |
| old supervisor → new supervisor | new tar autodetects uncompressed. Works.        |
| new supervisor → new supervisor | zstd read zstd. Works (zstd installed).         |
| new supervisor → old supervisor | old tar can't decompress zstd; **fails**.       |

The fourth case is a **one-way upgrade trap**: once a session has
written a zstd-compressed snapshot, you can't roll the image back to
a version without zstd or that session's restore will fail. Forward
migration is safe; rollback after zstd writes is not.

Mid-rollout (some sandboxes on old, some on new) is safe in practice
because snapshots live per-session per-volume — they aren't shared
across sessions, so version skew can't cross-pollinate.

### 4. HTTP protocol surface unchanged

The supervisor.js diff only adds internal helpers (`readBodyCapped`,
`TAR_COMPRESS_ARGS`). No new endpoints, no removed endpoints, no
breaking response shapes. Server-side calls into either old or new
supervisor work identically. So a heterogeneous fleet during rollout
poses no protocol mismatch risk.

## Verification procedure (when the modal rebuild lands)

1. `python scripts/release_modal_snapshot.py` — writes new id to
   `.modal-snapshot-tag`.
2. **Restart the running agent-sdk server** so `modal._get_image()`
   memoization picks up the new id.
3. Probe a fresh modal sandbox built from the new snapshot:
   ```python
   sb = modal.Sandbox.create(image=modal.Image.from_id(<new_id>), app=app, timeout=120)
   for cmd in [
       "which zstd && zstd --version | head -1",
       "grep -c readBodyCapped /opt/agent-sdk/runtime/supervisor.js",
       "grep -c ZSTD_AVAILABLE /opt/agent-sdk/runtime/supervisor.js",
       "tar -I 'zstd -1' -cf /tmp/x.tar -C /tmp data && head -c 4 /tmp/x.tar | od -An -tx1 -N4",
   ]:
       p = sb.exec("bash", "-lc", cmd); print(p.stdout.read())
   sb.terminate()
   ```
   Expect: zstd v1.5.7+, both grep counts > 0, magic bytes `28 b5 2f fd`.
4. Run the modal subset of the goldens to confirm no protocol regression:
   ```bash
   .venv/bin/python -m pytest tests/test_sandbox_stop_delete_recovery.py \
     -n auto -k modal --tb=short
   ```
5. Commit `.modal-snapshot-tag` along with the Dockerfile + supervisor.js
   diff in a single PR — they're co-versioned.
