# Runtime image unification

Status: All phases shipped. The legacy `install_supervisor` flow is gone; the runtime ships in the agent-sdk Docker image at `/opt/agent-sdk/runtime/`.
Owner: TBD (multiple agents are touching adjacent code — coordinate before merging).
Replaces: per-volume `install_supervisor` flow on every provider.

## Implementation status (updated 2026-04-30)

| Phase | What | Status |
|---|---|---|
| A | Dockerfile bakes `/opt/agent-sdk/runtime/`; `_detect_runtime_path()` helper | Shipped |
| B | local provider behind feature flag | Shipped |
| C | docker, daytona, modal providers behind same flag; `_build_volume_mounts` flag-aware | Shipped |
| D | flag default flipped to ON via `_use_image_runtime()` helper | Shipped |
| E | delete `install_supervisor` × 4, drop `volumes.supervisor_agent_types`, delete obsolete tests | Shipped |
| F | `scripts/release.sh` + `scripts/cleanup_volume_supervisor_dirs.py` | Shipped |

Tests cover the unconditional image-runtime path on local, docker, and
daytona. The `_use_image_runtime()` flag and `AGENT_SDK_USE_IMAGE_RUNTIME=0`
opt-out were removed in Phase E along with the legacy code paths.

The DB column `volumes.supervisor_agent_types` is dropped via a forward-only
migration (`ALTER TABLE volumes DROP COLUMN IF EXISTS supervisor_agent_types`)
that runs on the next server start.

---

## TL;DR

Today the agent-sdk runtime (`supervisor.js` + per-agent-type ACP binaries) is
**installed onto the user's persistent volume at runtime**. The DB column
`volumes.supervisor_agent_types` caches "which agent_types are installed" but
the on-disk layout is single-`node_modules` / single-`deps.tar.gz` and
last-write-wins, so the cache and disk drift. A second agent_type's install
silently wipes the first's binary; the next session for the older agent_type
hits the cache, skips reinstall, and crashes with `HTTP 502 ACP binary
missing`.

The structural fix is to **stop installing onto volumes**. Bake the runtime
into a Docker image built from the repo's `Dockerfile`; volumes hold only
user data; every provider spawns the supervisor from a fixed in-image path.
Daytona snapshots become an *optional pre-warmed cache* derived from the
same image, not a separate source of truth.

Result: ~1300 LOC deleted, an entire bug class made impossible, daytona cold
start drops by the time of an `npm install` + tarball-extract round trip,
and the runtime version is mechanically pinned to a git commit.

---

## 1. Background — why the current design breaks

### 1.1 What lives where today

```
volume/                                    # per-user, persistent, mounted at runtime
├── shared/                                # user data
├── agents/<agent_id>/home/                # user data (Claude JSONLs, etc.)
└── system/supervisor/                     # ← agent-sdk runtime, leaked onto user storage
    ├── supervisor.js                      # copied from src/supervisor/
    ├── package.json                       # written by `npm init -y`
    ├── package-lock.json
    └── node_modules/
        └── .bin/
            ├── claude-agent-acp           # only present if claude was the most recent install
            └── codex-acp                  # only present if codex was the most recent install
```

### 1.2 The install path

`src/api/sandbox/session.py:138`:

```python
if agent_type not in volume.supervisor_agent_types:
    provider_mod.install_supervisor(volume.provider_ref, agent_type)
    await _db.add_supervisor_agent_type(volume.id, agent_type)
```

The skip-install fast path trusts `volumes.supervisor_agent_types` (a
cumulative JSONB list — `["claude", "codex"]`).

`install_supervisor` exists per-provider:

| Provider | File | Mechanism |
|----------|------|-----------|
| local    | `src/api/providers/local.py:114`    | `npm init -y` + `npm install <spec>` in a staging dir, atomic-rename to `system/supervisor/` |
| daytona  | `src/api/providers/daytona.py:868`  | spawn an ephemeral install-sandbox, `npm install <spec>` to local FS, tar to `system/supervisor/deps.tar.gz` on the volume, atomic swap |
| docker   | `src/api/providers/docker.py`       | analogous to local but inside a docker container |
| modal    | `src/api/providers/modal.py:201`    | analogous, modal volume |

Every provider's install creates a fresh dir/tarball containing **only the
new spec**. The atomic swap replaces the previous install. Older agent types'
binaries are gone.

### 1.3 The cache-vs-disk drift

| Sequence of events                     | DB cache `supervisor_agent_types` | On-disk `node_modules/.bin/` |
|----------------------------------------|-----------------------------------|------------------------------|
| Fresh volume                           | `[]`                              | (empty)                      |
| Session 1 starts (agent_type=claude)   | `["claude"]`                      | `claude-agent-acp`           |
| Session 2 starts (agent_type=codex)    | `["claude", "codex"]`             | `codex-acp` ← claude wiped   |
| Session 3 starts (agent_type=claude)   | `["claude", "codex"]` (unchanged) | `codex-acp` ← cache hit, no reinstall |

Session 3's `create_sandbox("claude", ...)` raises
`RuntimeError: ACP binary missing at .../node_modules/.bin/claude-agent-acp;
call install_supervisor('claude') first` and the request 502s.

This is not a hypothetical: it's the failure mode the current `default-local`
volume on the dev machine is in (codex installed last, claude tests crash).

### 1.4 What's been tried and why it isn't enough

- **Cumulative install** (PR landed for local provider, in `local.py:114`):
  when `final_dir/package.json` exists, copy it (and `node_modules`) into
  staging before `npm install <new-spec>` so the new spec is *added* rather
  than replacing. Works, but:
  - Doesn't fix daytona (single `deps.tar.gz`, no incremental concept).
  - Relies on `npm install <pkg>`'s "don't prune extraneous" behavior, which
    is correct *today* but is a non-load-bearing npm guarantee.
  - Adds complexity (seed step, JSON-merge, lock-file copy).
  - Doesn't address versioning at all — bumping a spec in
    `_ACP_NPM_SPECS` doesn't invalidate existing installs.
- **Verify-on-skip** (proposed but not implemented): make `session.py:138`
  also stat the binary on disk. Catches the symptom faster but doesn't
  prevent install-time clobbers and so doesn't *fix* the bug.

The deeper issue is that `volumes.supervisor_agent_types` and the on-disk
install dir are **two sources of truth for the same fact** ("is the runtime
for agent_type X available on this volume"), and they disagree by design.

---

## 2. Proposed design

### 2.1 The inversion

Move runtime artifacts off the volume and into the image:

```
ghcr.io/<org>/agent-sdk:<git-sha>            ← built from the repo's Dockerfile
└── /opt/agent-sdk/runtime/
    ├── supervisor.js
    └── node_modules/
        └── .bin/
            ├── claude-agent-acp             ← every supported agent_type, always
            ├── codex-acp
            ├── opencode
            ├── gemini
            ├── cline-acp
            └── deepagents-acp

volume/                                       ← per-user; data only
├── shared/
└── agents/<agent_id>/home/                   ← Claude JSONLs, user files
```

`system/supervisor/` on the volume is no longer written to or read from. The
supervisor is spawned from the in-image path. Volumes carry zero
agent-sdk code.

### 2.2 The new contract

A single env var defines where the runtime lives:

```
AGENT_SDK_RUNTIME_PATH=/opt/agent-sdk/runtime          # default in the image
                      =<repo>/src/supervisor           # default for local-source dev (see §4)
```

Provider responsibilities collapse to:

- `create_sandbox`: spawn `node $AGENT_SDK_RUNTIME_PATH/supervisor.js --acp $AGENT_SDK_RUNTIME_PATH/node_modules/.bin/<bin> ...`. No volume-side path resolution, no install precondition, no cache lookup.
- (Removed) `install_supervisor`: deleted.
- (Removed) `ensure_volume_supervisor` orchestration: deleted.

The DB column `volumes.supervisor_agent_types` is dropped. The model field
goes with it.

### 2.3 Daytona: snapshot becomes a cache, not a source

Daytona supports two sandbox-creation paths:

- `CreateSandboxFromSnapshotParams(snapshot=<name>, ...)` — pre-warmed, fast cold start, but the snapshot's contents must be built externally.
- `CreateSandboxFromImageParams(image=<registry-url>, ...)` — pulls the image, slower first time on a fresh daytona machine but the image is whatever the repo's Dockerfile produces.

The fork already exists in `daytona.py` (today only inside `install_supervisor`'s ephemeral install-sandbox path). Lift it to session-sandbox provisioning with this rule:

> If `DAYTONA_SNAPSHOT` is set, that snapshot **must** have been registered by
> `scripts/release.sh` from the same `Dockerfile`-built image. There is no
> hand-curated snapshot. Otherwise the image path is used.

This keeps the latency benefit without recreating the source-of-truth split.

### 2.4 `scripts/release.sh`

```bash
#!/usr/bin/env bash
set -euo pipefail

REGISTRY="${AGENT_SDK_REGISTRY:-ghcr.io/<org>}"
SHA="$(git rev-parse --short HEAD)"
TAG="${REGISTRY}/agent-sdk:${SHA}"

docker build -t "$TAG" .
docker push "$TAG"

# Pin the image tag for the runtime — this file is committed.
echo "$TAG" > .runtime-image-tag

# Optional: register the image as a daytona snapshot for warm cold-start.
# Skipped for dev machines without DAYTONA_API_KEY.
if [[ -n "${DAYTONA_API_KEY:-}" ]]; then
  daytona snapshot create --name "agent-sdk-${SHA}" --image "$TAG"
fi
```

`.runtime-image-tag` is committed back to the repo so that `docker compose
up` and `daytona`-based deploys work from a fresh checkout without any env
config.

---

## 3. Per-provider impact

### 3.1 `local`

Today:

- `create_volume` makes `system/supervisor/` (empty).
- First session on the volume runs `install_supervisor(ref, agent_type)` — npm install on the host, atomic swap into the volume.
- `create_sandbox` resolves bin from `<volume>/system/supervisor/node_modules/.bin/<bin>` and spawns `node <volume>/system/supervisor/supervisor.js`.

After:

- `create_volume` makes `shared/` only. No `system/` directory.
- `create_sandbox` spawns `node $AGENT_SDK_RUNTIME_PATH/supervisor.js --acp $AGENT_SDK_RUNTIME_PATH/node_modules/.bin/<bin>`.
- `install_supervisor` is deleted.

Local-dev mode (running `uvicorn api.server:app` against source, not in docker): `AGENT_SDK_RUNTIME_PATH` defaults to `<repo>/src/supervisor`. The repo's existing `Dockerfile` line 18 (`cd src/supervisor && npm install`) becomes a developer-facing setup step — also runnable as `npm --prefix src/supervisor install` from the README. See §4.

Performance: first session on a fresh volume is now as fast as the hundredth (no install penalty). Subsequent sessions are unchanged.

### 3.2 `docker`

Today: similar to local but install runs inside a container. The provisioned sandbox is a docker container that mounts the volume.

After: each sandbox container is started **from the same image that the server runs in** (ours) — bins are at `/opt/agent-sdk/runtime/`, fixed path. The provider spawns `docker run <image> node /opt/agent-sdk/runtime/supervisor.js --acp /opt/agent-sdk/runtime/node_modules/.bin/<bin> ...`. No install, no volume-side runtime files.

This also simplifies docker-provider's volume mount semantics: volumes are mounted purely for data, never for code.

### 3.3 `daytona`

Today (install path): spawn an ephemeral sandbox from `node:22-slim` (or the `hive-large` snapshot), `npm install`, tar → upload `deps.tar.gz` to volume, swap. Slow (~1–2 min).

Today (session path): create a sandbox from `hive-large` snapshot, mount the volume, supervisor extracts `deps.tar.gz` from the volume on startup. Volume-side extract is on slow `mountpoint-s3`.

After: create a sandbox from the image (or from a snapshot derived from the image). Volume is mounted for data only — no `deps.tar.gz` extract. `install_supervisor` is deleted.

Provisioning fork (`src/api/sandbox/providers/daytona.py` and the daytona session-sandbox creation path):

```python
snapshot = os.environ.get("DAYTONA_SNAPSHOT")
if snapshot:
    sb = daytona.create(CreateSandboxFromSnapshotParams(
        snapshot=snapshot, ...
    ))
else:
    image = os.environ.get("DAYTONA_IMAGE") or _read_runtime_image_tag()
    sb = daytona.create(CreateSandboxFromImageParams(
        image=image, ...
    ))
```

`_read_runtime_image_tag()` reads `.runtime-image-tag` from the repo root.

The default `DAYTONA_SNAPSHOT=hive-large` constant (currently inlined in `daytona.py:893`) is **removed**. There is no implicit snapshot. Operators who want pre-warmed snapshots opt in by running `scripts/release.sh` with `DAYTONA_API_KEY` set, which registers a snapshot under a deterministic name.

### 3.4 `modal`

Today: modal volumes hold the supervisor + bins, populated by an analogous `install_supervisor` (`modal.py:201`).

After: same shape as docker — modal sandboxes are launched from a modal image, image is built from the repo's Dockerfile. `install_supervisor` deleted.

(Modal's image API differs from docker's — concrete steps are in the implementation plan.)

---

## 4. Local-dev workflow

The most important non-regression: a developer running `uvicorn api.server:app` against source, with `provider="local"`, must still work without building a Docker image.

Plan:

- `AGENT_SDK_RUNTIME_PATH` defaults: `os.environ.get("AGENT_SDK_RUNTIME_PATH") or _detect_runtime_path()`, where `_detect_runtime_path()` returns:
  - `/opt/agent-sdk/runtime` if it exists (we're in the image)
  - `<repo>/src/supervisor` if `<repo>/src/supervisor/node_modules/.bin/claude-agent-acp` exists (we're in source)
  - else raise with a clear message: "Run `npm --prefix src/supervisor install` or set AGENT_SDK_RUNTIME_PATH"
- `src/supervisor/package.json` lists every spec in `_ACP_NPM_SPECS` so a single `npm install` in that directory pre-populates all bins. Today it only installs the supervisor's own deps; this needs extending.
- `scripts/launch_server_test.sh` and `launch_server_local.sh` run `npm --prefix src/supervisor install --silent` as a setup step (cheap re-run; npm is idempotent on already-installed deps).
- `docs/local-dev.md` is updated to call this out as a one-time setup step.

Existing local-dev users who are mid-session when this lands: their volume's `system/supervisor/` is **ignored** by the new code. No migration required for *them*; the local volume is just data going forward.

---

## 5. Migration

### 5.1 Existing volumes (production)

Volumes that already have `system/supervisor/` written by the old install path:

- New code never reads from `<volume>/system/supervisor/` → those files are dead weight, not a correctness problem.
- A one-shot cleanup migration removes `system/supervisor/` from existing volumes to reclaim disk.
  - `scripts/cleanup_volume_supervisor_dirs.py --provider <p> --yes`: lists every volume's provider_ref and deletes `system/supervisor/`.
  - For daytona: spawns a short-lived sandbox per volume, runs `rm -rf /work/system/supervisor`, exits.
  - For local: walks `~/.agent-sdk/volumes/*/system/` and removes `supervisor/`.
  - For docker / modal: analogous.
- Migration is **not** load-bearing — the new code works fine if it never runs. It's purely disk reclamation.

### 5.2 Database

- `volumes.supervisor_agent_types` column is dropped via a forward-only migration (`api/db.py`'s `_MIGRATIONS` list adds `ALTER TABLE volumes DROP COLUMN IF EXISTS supervisor_agent_types`).
- The model field `VolumeRecord.supervisor_agent_types` is removed.
- Helper `add_supervisor_agent_type` is removed.

### 5.3 In-flight sessions during deploy

- A session that's mid-prompt when the server restarts: existing session resumes via `pool.get_session` → `start()` → `create_sandbox`. The new `create_sandbox` resolves bin from in-image path; volume's stale `system/supervisor/` is irrelevant. No drop.
- A daytona sandbox that was provisioned from the old `hive-large` snapshot before deploy keeps running until it's stopped. New sandboxes after deploy come from the new image. Mixed-fleet for the duration of a rolling deploy is fine — both paths produce a sandbox that can spawn the supervisor from a known location, and the supervisor.js protocol is the same.

### 5.4 Rollback

If something goes wrong, `git revert` the implementation PR. Existing volumes' `system/supervisor/` directories are still present (the cleanup migration is a separate PR, not part of the same change), so the old `install_supervisor` flow is immediately functional again. The DB column drop is the riskiest single change — it should be the **last** migration step, after the new code has soaked.

---

## 6. Test strategy

### 6.1 Tests that are deleted

The whole class of "is the install correct on this volume" tests becomes nonsensical:

- `tests/test_install_chaos.py` — N concurrent installs, advisory-lock chaos, cache-correctness-on-failure invariants. The whole orchestration is gone.
- `tests/test_install_supervisor_fault.py` — fault injection on `install_supervisor`. Function is gone.
- `tests/test_session_survives_supervisor_dir_wiped_from_volume` (in `test_sandbox_stop_delete_recovery.py`) — wipes `system/supervisor/` on disk and verifies self-heal. New code never touches the path; nothing to self-heal.
- `tests/test_local_volume_integration.py::test_install_supervisor_*` — covered by image-build CI instead.

### 6.2 Tests that change

- `tests/test_local_volume_integration.py::test_install_supervisor_is_cumulative_across_agent_types` (added in the cumulative-install fix): becomes obsolete and is deleted.
- `tests/test_local_volume_integration.py::test_install_supervisor_populates_volume`: rewritten as `test_runtime_image_resolves_all_acp_bins` — given a runtime path with all bins installed, every supported agent_type's `create_sandbox` resolves and spawns successfully.
- Provider golden suites (`test_sandbox_stop_delete_recovery.py`, `test_pre_start_commands_*.py`): unchanged in behavior. They never asserted on install — they just used the implicit install-on-first-session.

### 6.3 New tests

- `tests/test_runtime_path_resolution.py`: pure unit tests of `_detect_runtime_path()` — image path present, source path present, neither (raises), both (image wins).
- `tests/test_dockerfile_builds_runtime.py` (CI-only, behind a marker like `pytest -m docker_build`): runs `docker build .`, runs `docker run <built-image> ls /opt/agent-sdk/runtime/node_modules/.bin/`, asserts every name in `_ACP_BIN_NAMES.values()` is present. Catches a missing-spec regression.
- `tests/test_release_image_tag_committed.py`: asserts `.runtime-image-tag` exists, parses to `<registry>/<image>:<sha>`, and the SHA is reachable in `git log`. Prevents committing a stale or missing tag.

### 6.4 Test-suite CI lanes

Today's `pytest -n auto` lane stays as-is. The image-build lane is a separate
CI job (slow, gated, runs on PRs touching `Dockerfile` or `_ACP_NPM_SPECS`).

---

## 7. Code that gets deleted

Concrete inventory (line counts approximate, subject to PR review):

| File / construct                                                   | LOC  | Status                |
|--------------------------------------------------------------------|------|-----------------------|
| `src/api/providers/local.py::install_supervisor`                   | ~110 | delete                |
| `src/api/providers/docker.py::install_supervisor`                  | ~80  | delete                |
| `src/api/providers/daytona.py::install_supervisor`                 | ~150 | delete                |
| `src/api/providers/modal.py::install_supervisor`                   | ~70  | delete                |
| `src/api/server.py::ensure_volume_supervisor` + advisory-lock glue | ~120 | delete                |
| `src/api/sandbox/session.py:138` install branch                    | ~15  | delete                |
| `src/api/db.py` migration adding `supervisor_agent_types`          | ~5   | new migration to drop |
| `src/api/db.py::add_supervisor_agent_type` + SQL                   | ~15  | delete                |
| `src/api/models.py::VolumeRecord.supervisor_agent_types`           | ~3   | delete                |
| `tests/test_install_chaos.py`                                       | ~330 | delete                |
| `tests/test_install_supervisor_fault.py`                            | ~250 | delete                |
| `test_session_survives_supervisor_dir_wiped_from_volume`            | ~50  | delete                |
| Cumulative-install fix in `local.py` (the recent PR)                | ~50  | delete (replaced)     |
| `DAYTONA_SNAPSHOT="hive-large"` defaults & references                | ~10  | delete                |
| `node:22-slim` image fallback in `install_supervisor`'s sandbox     | ~5   | delete                |
| Total                                                               | **~1260 LOC** | |

Code that grows:

| File / construct                                                   | LOC  | Status |
|--------------------------------------------------------------------|------|--------|
| `Dockerfile` extension (install all `_ACP_NPM_SPECS`, set runtime path env) | ~15 | new |
| `scripts/release.sh`                                                | ~20  | new |
| `_detect_runtime_path()` helper + import in providers                | ~20  | new |
| Provider `create_sandbox` updates (read runtime path)                | ~5×4 = 20 | modify |
| `src/supervisor/package.json` deps to cover all agent_types          | ~10  | modify |
| New tests (§6.3)                                                     | ~100 | new |
| Docs (`local-dev.md`, this design)                                   | ~100 | new |
| Total                                                               | **~285 LOC** | |

Net: **~975 LOC deleted**.

---

## 8. Risks and mitigations

| Risk | Mitigation |
|------|------------|
| Image pull is slow on a fresh daytona machine and adds cold-start latency for the first sandbox on that machine | `scripts/release.sh` registers a daytona snapshot from the image when `DAYTONA_API_KEY` is present. Operators with cold-start sensitivity opt in. |
| Image bloat (all ACP bins included even for users who only run claude) | ACP bins are small (~few MB each, ~30MB total). Image grows by tens of MB, not GB. One-time pull amortized. |
| Bumping a single ACP version is now a code release (no hot-patch) | Already true in spirit — `_ACP_NPM_SPECS` is in code. The runtime-install-on-volume path masked this with a runtime side-channel. We're aligning behavior with reality. |
| Local-dev developer forgets to run `npm --prefix src/supervisor install` and gets a confusing error | `_detect_runtime_path()` raises with the exact remediation command. `scripts/launch_server_test.sh` runs the install as a setup step. |
| Some volumes still have `system/supervisor/` and operators are confused | Cleanup migration is a separate PR with explicit `--dry-run` and `--yes` flags. New code doesn't *care* about those dirs; the cleanup is hygiene, not correctness. |
| Other agents are mid-merge on `local.py`, `daytona.py`, etc. | This refactor's PR list (see plan) is sequenced so the high-conflict files are touched once each, with clear commit boundaries. The first phase adds the new path *alongside* the old; the deletion phase comes last. |
| `_ACP_NPM_SPECS` and the image's installed deps drift if someone bumps one and not the other | Image-build CI lane diffs `_ACP_NPM_SPECS` against the bins actually present in the built image. PR fails if they disagree. |
| User who was relying on per-volume `node_modules/` being writable from inside the sandbox (custom tooling) breaks | Audit `tests/` and `examples/` for any reference. None found in initial scan, but call out in PR description for reviewers. |

---

## 9. Open questions

1. **Modal provider**: modal's image API is opinionated — you build a `modal.Image` programmatically rather than from a Dockerfile. Does it make sense to share the Dockerfile across docker/daytona but build the modal image via `modal.Image.from_dockerfile(...)`? (Probably yes — modal supports it.)
2. **Image registry**: GitHub Container Registry vs Docker Hub vs ECR. Decide before `release.sh` lands so the default in the script is sensible.
3. **`AGENT_SDK_RUNTIME_PATH` in tests**: do unit tests get a fixture-installed runtime, or do we expose a `monkeypatch.setenv` knob? (Probably both — fixture for golden tests, env-var for unit tests.)
4. **What about `cline` / `deepagents` / `gemini` / `goose` / `openhands`**: some of these have launch arg quirks (`_ACP_LAUNCH_ARGS`). Confirm they all work with the in-image path on docker (most likely fine; they're just CLI invocations) and call out any holdouts in the implementation plan.

---

## 10. References

- Bug repro: `tests/test_sandbox_stop_delete_recovery.py::test_session_resume_after_stop[local]` — failed with `HTTP 502: Provider 'local' failed: ACP binary missing`. Fixed temporarily by the cumulative-install PR; this design replaces that fix.
- Cumulative-install PR: `src/api/providers/local.py:114` — to be reverted as part of this refactor.
- Existing Dockerfile: `Dockerfile` (root) — already builds the server image; this refactor extends it, not replaces it.
- Provider boundaries: `src/api/providers/_shared.py:55-78` (the canonical `_ACP_BIN_NAMES`, `_ACP_NPM_SPECS`, `_ACP_LAUNCH_ARGS` tables).
- Install dispatch: `src/api/sandbox/session.py:138`.

---

## 10.5 Release workflow — when to run `scripts/release.sh`

`scripts/release.sh` rebuilds the runtime artifacts that providers
consume:

1. A **local Docker image** (`agent-sdk:<git-sha>`) — the docker provider
   spawns sandboxes from this image directly via the local docker daemon.
2. A **Daytona snapshot** (`agent-sdk-<git-sha>`) — the daytona provider
   creates sandboxes from this snapshot. Daytona builds the snapshot
   remotely from the repo's `Dockerfile` via `Image.from_dockerfile`; no
   registry push needed.
3. **`.runtime-image-tag` and `.runtime-snapshot-tag`** files committed
   to the repo — providers auto-resolve these so a fresh checkout works
   without per-environment env var setup.

### When you MUST re-run `release.sh`

Anything that changes the contents of `/opt/agent-sdk/runtime/` inside
the runtime artifact:

- **`src/supervisor/supervisor.js`** — the supervisor process binary
- **`src/supervisor/package.json`** — adds/removes/updates an npm dep
- **`src/api/providers/_shared.py:_ACP_NPM_SPECS`** — bumps an ACP
  binary version (the Dockerfile re-runs `npm install` against
  `src/supervisor` during build, picking up the new spec)
- **`Dockerfile`** itself

After landing a change in any of those, run `scripts/release.sh`,
commit the updated `.runtime-image-tag` and `.runtime-snapshot-tag`,
roll the server.

### When you DON'T need to re-run

- Python server code in `src/api/...` — runs on the server-host, not
  inside daytona sandboxes (those only run supervisor.js + the spawned
  ACP child)
- SDK code in `src/agent_sdk/...` — same; the SDK is a client library
- Tests — never affect the runtime image
- Docs — never affect the runtime image

### Usage

```
# Default: builds local docker image + daytona snapshot, no registry push
scripts/release.sh

# Iterating on the supervisor with uncommitted changes — adds a
# ``-dirty-<timestamp>`` suffix to the tags so dirty builds are
# distinguishable from committed ones
RELEASE_ALLOW_DIRTY=1 scripts/release.sh

# CI / multi-machine deploys: also push to a registry
RELEASE_PUSH=1 scripts/release.sh
AGENT_SDK_REGISTRY=ghcr.io/myorg RELEASE_PUSH=1 scripts/release.sh
```

The Daytona snapshot register step takes ~5 minutes (Daytona builds the
image remotely). Skip it by unsetting `DAYTONA_API_KEY`.


## 11. What this design does NOT do

- It does not change the **supervisor protocol** (HTTP + SSE between server and supervisor.js). That contract is unaffected.
- It does not change the **ACP wire protocol** between supervisor.js and the per-agent bins. Unchanged.
- It does not change the **session/volume/agent data model**. Volumes still exist, are still per-agent, still hold user data — they just don't hold runtime code.
- It does not change **where Claude's JSONLs live** (still `<volume>/agents/<agent_id>/home/.claude/projects/...`). Session resume, file APIs, conversation continuity: unaffected.
- It does not change **per-session pre_start_commands** behavior. Those run inside the sandbox as before.

The blast radius is intentionally narrow: only the question of *where the agent-sdk's own runtime code lives* changes.
