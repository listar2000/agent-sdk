# Runtime image unification — implementation plan

Status: ready-to-execute. Plan-only; no code in this artifact.
Companion to: `docs/runtime-image-unification.md` (read first).
Repository state assumed: `refactor/rename-serverclient-to-apiclient` branch
or descendants. Line numbers below are pinned to that branch's HEAD.

This plan is sequenced **parity-first**: the new image-baked path is added
alongside the old volume-install path, switched in behind a feature flag,
soaked, then the old path is deleted, and the DB column drop is the
**last** commit. Every phase has a clean rollback boundary.

The plan produces ~7 PRs (one per phase, except A which is a single
mechanical commit and F which is a small ops PR).

---

## Phase 0 — Pre-work, assumption-confirmation, open questions

Before writing any code, the implementer should:

### 0.1 Read

- `docs/runtime-image-unification.md` (the design)
- `Dockerfile` (root, currently 28 lines)
- `src/api/providers/_shared.py:55-78` (`_ACP_BIN_NAMES`, `_ACP_NPM_SPECS`, `_ACP_LAUNCH_ARGS`)
- `src/api/providers/local.py:114-222` (`install_supervisor`)
- `src/api/providers/docker.py:140-239` (`install_supervisor`, versioned-symlink scheme)
- `src/api/providers/daytona.py:104-262` (`start_supervisor_in_sandbox`, the volume-cache + tarball-extract path)
- `src/api/providers/daytona.py:330-436` (`provision_daytona_sandbox`, the snapshot-vs-image fork)
- `src/api/providers/daytona.py:868-1009` (`install_supervisor`)
- `src/api/providers/modal.py:124-139, 201-275, 282-316, 319-380` (`_get_image`, `install_supervisor`, entrypoint, `create_sandbox`)
- `src/api/sandbox/session.py:87-154` (`_bootstrap_session`, **the only call site of `install_supervisor`** — the design's claim that this lives in `server.py:ensure_volume_supervisor` is stale, see §0.4)
- `src/api/sandbox/providers/daytona.py:160-180` (the `_ensure_volume_supervisor` reference is a stale call site, see §0.4)
- `src/api/db.py:175-180, 528-595` (the `supervisor_agent_types` migration and `add_supervisor_agent_type` helper)
- `src/api/models.py:86-93` (`VolumeRecord.supervisor_agent_types`)
- `src/api/server.py:33-50` (the `add_supervisor_agent_type` import — currently unused; the symbol is imported but not called anywhere in `server.py`)
- `src/supervisor/package.json` (already lists every spec in `_ACP_NPM_SPECS`; design's "needs extending" claim is wrong, see §0.4)
- `tests/test_local_volume_integration.py:96-140` (the two install tests)
- `tests/test_install_chaos.py` (whole file, 341 LOC — references `srv.ensure_volume_supervisor` which no longer exists; this file is **already broken at import time** and may already be skipped/quarantined; verify before writing the deletion phase)
- `tests/test_install_supervisor_fault.py` (170 LOC)
- `tests/test_sandbox_stop_delete_recovery.py:1400-1452`
- `tests/test_provider_dispatch.py:80-150`
- `tests/test_api_consistency.py:155-205` (also references `api.server.ensure_volume_supervisor` which no longer exists — confirm before deletion)
- `scripts/launch_server_local.sh` (155 lines)
- `scripts/launch_server_test.sh` (16 lines)

### 0.2 Confirm assumptions

Run these commands and verify:

- `grep -rn "ensure_volume_supervisor\b" src/api/` returns **zero** definitions and one stale caller at `src/api/sandbox/providers/daytona.py:162`. (The design's claim that `server.py::ensure_volume_supervisor` is ~120 LOC is stale — that function was deleted in a previous refactor; the call site in `sandbox/providers/daytona.py` is now an `AttributeError` waiting to fire.)
- `grep -n "add_supervisor_agent_type" src/api/server.py` returns exactly one line (the import at line 35) and no usages — i.e. the symbol is dead in `server.py`.
- `wc -l src/api/providers/local.py` reports 780 (not the design's implied total LOC for the install — the install function alone is ~110).
- `wc -l tests/test_install_supervisor_fault.py` reports 170 (not 250 as the design claims).
- `wc -l tests/test_install_chaos.py` reports 341 (not 330; close).
- `cat src/supervisor/package.json` already declares all six `_ACP_NPM_SPECS` deps. (Design says "needs extending"; in fact the local-dev path's `npm install` already covers all bins. The Dockerfile currently does `cd src/supervisor && npm install --silent` at line 18 — i.e. the image build already pre-resolves every ACP CLI in `node_modules/.bin/`. The work in Phase A is to **expose the path** to providers, not to install packages.)
- `ls .runtime-image-tag` does NOT exist. `grep -rn "AGENT_SDK_RUNTIME_PATH" src/` returns zero hits.

### 0.3 Set up local dev runtime

Verify on the implementer's machine:

```
npm --prefix src/supervisor install
ls src/supervisor/node_modules/.bin/claude-agent-acp
ls src/supervisor/node_modules/.bin/codex-acp
```

If any bin is missing, the planned `_detect_runtime_path()` source-tree
fallback will not work in unit tests. This is the single hardest-to-debug
local-dev gotcha.

### 0.4 Open questions to resolve before starting

Each blocks Phase A or is a Phase F policy question:

1. **Image registry**: GHCR, Docker Hub, or a private registry? (Unblocks
   Phase F default in `scripts/release.sh`. Recommended: GHCR — the repo
   already lives on GitHub, no extra creds.)
2. **Modal image strategy**: `modal.Image.from_dockerfile(Dockerfile)`
   reads our root `Dockerfile` and produces a Modal-native image; alternative
   is to keep `_get_image()` building a programmatic image and copy/install
   the runtime separately. Recommended: `from_dockerfile`. Verify Modal
   supports `Image.from_dockerfile` at the version pinned in
   `pyproject.toml`. (Open in design §9 question 1.)
3. **Test fixture vs. env-var for `AGENT_SDK_RUNTIME_PATH` in unit tests**:
   default a fixture (`tmp_path`-based tarball/symlink) for golden tests;
   `monkeypatch.setenv` for unit tests. (Open in design §9 question 3.)
4. **`cline` / `deepagents` / `gemini` / `goose` / `openhands`** (design §9
   question 4): the runtime-path resolution is uniform — `node_modules/.bin/
   <_ACP_BIN_NAMES[agent_type]>` — but `goose` and `openhands` are NOT in
   `_ACP_NPM_SPECS` and resolve via PATH today (`shutil.which(bin_name)`
   in `local.py:278`). The plan retains this PATH-fallback for non-npm
   agents in Phase B; the in-image runtime contains npm bins only.
5. **Stale-test policy** (`test_install_chaos.py`, `test_api_consistency.py`
   refs to `ensure_volume_supervisor`): if these tests are already broken
   at collection time, are they currently skipped via a marker, or just
   silently failing? Confirm before Phase E so the deletion isn't
   dependent on a green CI signal that's already been red.
6. **Cumulative-install fix lifecycle**: the fix at
   `src/api/providers/local.py:114-222` adds a `_seed_from_existing()`
   step that copies `final_dir/package.json` and `node_modules` into
   staging before `npm install`. Confirm with the original PR author
   that reverting it as part of this refactor is acceptable rather than
   leaving it for a separate pre-cleanup commit. (Phase E reverts it.)
7. **Mid-flight production sessions**: an existing daytona sandbox that
   was provisioned before this refactor lands has `/opt/supervisor`
   bind-mounted from the volume's `system/supervisor/` and an extracted
   `deps.tar.gz` in `/tmp/sup-work-9100/`. After deploy, in-place restart
   (Type-1) re-runs `start_supervisor_in_sandbox`. The new path will not
   look at `/opt/supervisor`. Confirm via design §5.3 that the rolling
   deploy plan is "kill old sandboxes, let new ones come up from the
   image" rather than expecting hot-attach compatibility.

---

## Phase A — Add the runtime-path helper + extend Dockerfile (no provider wiring)

**Goal**: ship the shared infrastructure (env var, helper, Dockerfile change)
without changing any provider behavior. This commit, in isolation, must
leave every existing test green and every existing code path unchanged.

**PR scope**: 1 PR, ~80 LOC net.

### A.1 Files to change

| File                                           | Change                                          |
|------------------------------------------------|-------------------------------------------------|
| `Dockerfile` (root)                            | Add `ENV AGENT_SDK_RUNTIME_PATH=/opt/agent-sdk/runtime` and a step to materialize that directory from `src/supervisor/` (copy `supervisor.js` + `node_modules/`). The existing line 18 (`cd src/supervisor && npm install --silent`) stays. |
| `src/api/providers/_shared.py` (new helper)    | Add `_detect_runtime_path() -> str` and `_runtime_supervisor_js() / _runtime_acp_bin(agent_type)` accessors. |
| `tests/test_runtime_path_resolution.py` (new) | Unit tests for the helper. |

### A.2 Concrete code shape (sketch, not full diff)

#### A.2.a Dockerfile change

```dockerfile
# After: RUN cd src/supervisor && npm install --silent
RUN mkdir -p /opt/agent-sdk/runtime \
    && cp src/supervisor/supervisor.js /opt/agent-sdk/runtime/ \
    && cp -r src/supervisor/node_modules /opt/agent-sdk/runtime/node_modules
ENV AGENT_SDK_RUNTIME_PATH=/opt/agent-sdk/runtime
```

(Alternative: a single `cp -r src/supervisor/. /opt/agent-sdk/runtime/`
with `package.json` copied incidentally. Either works; the design's §2.1
diagram shows only `supervisor.js` + `node_modules/`. Keep `package.json`
for `npm`-tooling-friendliness inside the image.)

#### A.2.b `_detect_runtime_path()` in `_shared.py`

```python
# Resolution order (no I/O on the hot path; cached after first call):
#   1. $AGENT_SDK_RUNTIME_PATH explicitly set → return it (no existence check;
#      callers will fail loudly if invalid, matching dev expectations).
#   2. /opt/agent-sdk/runtime exists → return it (we're in the image).
#   3. <repo>/src/supervisor/node_modules/.bin/<some-bin> exists → return
#      <repo>/src/supervisor (we're in source tree).
#   4. Raise RuntimeError with remediation:
#      "Runtime not found at /opt/agent-sdk/runtime or <repo>/src/supervisor.
#       Run `npm --prefix src/supervisor install` or set AGENT_SDK_RUNTIME_PATH."
def _detect_runtime_path() -> str: ...

def _runtime_supervisor_js() -> str:
    return f"{_detect_runtime_path()}/supervisor.js"

def _runtime_acp_bin(agent_type: str) -> str:
    return f"{_detect_runtime_path()}/node_modules/.bin/{_acp_bin_name(agent_type)}"
```

`_REPO_ROOT` for fallback (3): use the same `Path(__file__).resolve().parents[3]`
pattern as `local.py:38`.

The "some-bin" sentinel for fallback (3): use `claude-agent-acp` (always
in `_ACP_NPM_SPECS`).

The function is module-level (not class), pure (no I/O after first call
caches the result, but the cache is **not** thread-locked because the
result is deterministic given filesystem state; concurrent callers may
re-stat but all converge to the same value). No memoization is required
for correctness — add a simple `functools.lru_cache(maxsize=1)` if the
stat overhead shows up in profiling, but not by default.

### A.3 Tests to add (`tests/test_runtime_path_resolution.py`)

Pure unit tests (no fixtures, no servers):

- `test_explicit_env_var_wins`: set `AGENT_SDK_RUNTIME_PATH=/some/path`,
  assert `_detect_runtime_path() == "/some/path"`.
- `test_image_path_when_present`: monkeypatch `os.path.exists` to claim
  `/opt/agent-sdk/runtime` exists; unset `AGENT_SDK_RUNTIME_PATH`; assert
  return value is `/opt/agent-sdk/runtime`.
- `test_source_tree_fallback`: unset env; monkeypatch `os.path.exists` to
  claim `/opt/agent-sdk/runtime` does NOT exist but
  `<repo>/src/supervisor/node_modules/.bin/claude-agent-acp` does; assert
  return ends with `src/supervisor`.
- `test_neither_present_raises`: unset env; both stat calls return False;
  assert `RuntimeError` whose message contains `"npm --prefix src/supervisor install"`
  AND `"AGENT_SDK_RUNTIME_PATH"`.
- `test_image_wins_over_source`: env unset; both image and source paths
  exist (i.e. running the test inside a built container that also has the
  repo mounted); assert image wins.
- `test_runtime_supervisor_js_format` and `test_runtime_acp_bin_format`:
  confirm path concatenation matches the contract callers will use.

### A.4 Tests to run before merging Phase A

```
.venv/bin/python -m pytest tests/test_runtime_path_resolution.py -n auto
.venv/bin/python -m pytest tests/test_local_volume_integration.py -n auto
.venv/bin/python -m pytest tests/test_provider_dispatch.py -n auto
.venv/bin/python -m pytest tests/test_sandbox_stop_delete_recovery.py -k local -n auto
```

The full daytona/docker golden suites are NOT required for this phase
(no provider behavior changed). The local volume + provider dispatch
suites are sufficient regression evidence.

### A.5 Acceptance criteria

- All new unit tests pass.
- The full repo's existing test suite passes — no behavior changed.
- `docker build .` succeeds.
- Inside a built image, `ls /opt/agent-sdk/runtime/node_modules/.bin/`
  shows every name in `_ACP_BIN_NAMES.values()` for which the agent_type
  is in `_ACP_NPM_SPECS` (i.e. claude, codex, opencode, gemini, cline,
  deepagents — six bins).
- `_detect_runtime_path()` returns `/opt/agent-sdk/runtime` inside the
  image and `<repo>/src/supervisor` from a developer checkout.

### A.6 Rollback

`git revert` the single commit. No DB, no provider, no test deletion;
zero risk.

---

## Phase B — Wire ONE provider (`local`) to read from `AGENT_SDK_RUNTIME_PATH`, behind a flag

**Goal**: prove the runtime-path-resolution design works against real code,
on the cheapest provider, without touching the install path. Both the old
and new code paths must be functional and selectable by env var, so a CI
matrix can run each.

**PR scope**: 1 PR, ~60 LOC net (additions; no deletions).

### B.1 Files to change

| File                                  | Change                                          |
|---------------------------------------|-------------------------------------------------|
| `src/api/providers/local.py`          | In `create_sandbox()` (around line 257-285), add a feature-flag branch: `if os.environ.get("AGENT_SDK_USE_IMAGE_RUNTIME") == "1": use _detect_runtime_path() else: use volume-side <ref>/system/supervisor/`. **Do NOT delete `install_supervisor`.** **Do NOT delete the cumulative-install fix.** |
| `src/api/providers/local.py`          | When the flag is on, the line-265 `if not supervisor_js.exists(): raise` and line-273 `if not acp_bin.exists(): raise` checks operate on the runtime-path values (which should always exist when the flag is set; their failure means the image/source dev setup is broken — message updated accordingly). |
| `tests/test_local_volume_integration.py` | Add `test_create_sandbox_uses_image_runtime_when_flag_set(tmp_path, monkeypatch)`: set `AGENT_SDK_USE_IMAGE_RUNTIME=1`, set `AGENT_SDK_RUNTIME_PATH=<a tmpdir with a stub supervisor.js + node_modules/.bin/claude-agent-acp executable>`, assert the spawned process's argv includes the runtime-path values. (Mock `subprocess.Popen` to capture argv without actually launching.) |

### B.2 Code shape (sketch)

```python
# in local.create_sandbox, replacing the block at lines 257-285:
use_image_runtime = os.environ.get("AGENT_SDK_USE_IMAGE_RUNTIME") == "1"

if use_image_runtime:
    supervisor_js = Path(_runtime_supervisor_js())
    if not supervisor_js.exists():
        raise RuntimeError(
            f"runtime supervisor.js missing at {supervisor_js}. "
            f"Set AGENT_SDK_RUNTIME_PATH or run `npm --prefix src/supervisor install`."
        )
    bin_name = _acp_bin_name(agent_type)
    if agent_type in _ACP_NPM_SPECS:
        acp_bin_str = _runtime_acp_bin(agent_type)
        if not Path(acp_bin_str).exists():
            raise RuntimeError(
                f"runtime ACP binary missing at {acp_bin_str}. "
                f"Rebuild image or re-run npm install in src/supervisor."
            )
    else:
        system_bin = shutil.which(bin_name)
        if not system_bin:
            raise RuntimeError(f"{bin_name} not found in PATH for agent_type={agent_type!r}")
        acp_bin_str = system_bin
else:
    # EXISTING volume-side resolution (unchanged) — keep all current code
    sup_dir = vol / "system" / "supervisor"
    supervisor_js = sup_dir / "supervisor.js"
    # ... rest of the original block ...
```

### B.3 Test changes/additions

- New: `test_create_sandbox_uses_image_runtime_when_flag_set` (above).
- New: `test_create_sandbox_uses_volume_runtime_by_default` (assert old
  path still works when flag unset).
- Existing tests in `test_local_volume_integration.py` (which call
  `install_supervisor` first) continue to pass with flag UNSET (they
  exercise the old path).
- The cumulative-install regression test
  (`test_install_supervisor_is_cumulative_across_agent_types`) **stays**
  through Phase D — it's a transitional safety net for the old path that
  is still active.

### B.4 Tests to run before merging Phase B

Run the **local** golden suite both ways:

```
# Flag off — exercises the old volume-install path
.venv/bin/python -m pytest tests/test_local_volume_integration.py tests/test_sandbox_stop_delete_recovery.py -k local -n auto

# Flag on — exercises the new image-path
AGENT_SDK_USE_IMAGE_RUNTIME=1 .venv/bin/python -m pytest tests/test_local_volume_integration.py tests/test_sandbox_stop_delete_recovery.py -k local -n auto
```

Both must pass. Daytona/docker/modal not yet touched, so their suites
are unaffected.

### B.5 Acceptance criteria

- Both flag positions (on/off) produce green local goldens.
- The new tests prove the supervisor argv is built from the runtime path
  when the flag is on, and from the volume path when it's off.
- The existing `test_install_supervisor_is_cumulative_across_agent_types`
  still passes (flag off path).
- No `daytona.py`, `docker.py`, `modal.py`, `server.py`, `db.py`, `models.py`,
  `session.py` files are touched — coordination with other in-flight work
  on those files is zero-conflict for this PR.

### B.6 Rollback

`git revert` the single commit. No data loss; volumes still have their
`system/supervisor/` from the old code path.

---

## Phase C — Wire remaining providers (`docker`, `daytona`, `modal`) to the runtime path, behind the same flag

**Goal**: extend the Phase B pattern to the three remaining providers. Each
sub-PR is independent; merge order chosen to minimise rebase pain with
in-flight conflicts (see "Coordination" section).

**PR scope**: 3 PRs (one per provider) OR 1 grouped PR. Recommend 3 PRs
because each touches a single high-conflict file. Each PR is ~50–80 LOC.

### C.1 Sub-phase C.1 — `docker` provider

#### C.1.1 Files to change

| File                                  | Change                                          |
|---------------------------------------|-------------------------------------------------|
| `src/api/providers/docker.py`         | In `create_sandbox` (line ~278+), behind `AGENT_SDK_USE_IMAGE_RUNTIME`: launch the sandbox container from `_NODE_IMAGE` as today **but** also bind-mount the host's runtime path into the container at `/opt/agent-sdk/runtime` (read-only), and resolve `supervisor_js`/`acp_path` from there instead of the volume-side `_SUPERVISOR_IN`. The cleaner long-term shape is "use the agent-sdk image as the sandbox image" (design §3.2), but Phase C keeps the bind-mount intermediate to avoid an image-pull dependency. Phase D (after soak) tightens to "sandbox image is the agent-sdk image". |
| `src/api/providers/docker.py`         | Add a comment: "TODO Phase D: replace `_NODE_IMAGE` with the agent-sdk runtime image so no host bind-mount is required." |
| `tests/test_docker_volume_integration.py` | Mirror the local provider's flag-on/flag-off tests. |

#### C.1.2 Code shape (sketch)

```python
# In docker.create_sandbox, replacing the supervisor/acp_path resolution block:
use_image_runtime = os.environ.get("AGENT_SDK_USE_IMAGE_RUNTIME") == "1"
runtime_in_container = "/opt/agent-sdk/runtime"

if use_image_runtime:
    host_runtime = _detect_runtime_path()
    extra_mounts = ["--mount", f"type=bind,source={host_runtime},target={runtime_in_container},readonly"]
    acp_path = f"{runtime_in_container}/node_modules/.bin/{bin_name}"
    supervisor_js_in = f"{runtime_in_container}/supervisor.js"
else:
    extra_mounts = ["--mount", f"type=volume,source={volume_ref},target={_SUPERVISOR_IN},volume-subpath=system/supervisor"]
    acp_path = f"{_SUPERVISOR_IN}/node_modules/.bin/{bin_name}"
    supervisor_js_in = f"{_SUPERVISOR_IN}/supervisor.js"
```

The shared mounts and HOME-mount stay identical. Volume is mounted for
data only when flag is on.

#### C.1.3 Acceptance / rollback

- Both flag positions produce green `tests/test_docker_volume_integration.py`.
- Docker golden suite passes both ways: `tests/test_sandbox_stop_delete_recovery.py -k docker`.
- Rollback: revert the single commit.

### C.2 Sub-phase C.2 — `daytona` provider

#### C.2.1 Files to change

| File                                  | Change                                          |
|---------------------------------------|-------------------------------------------------|
| `src/api/providers/daytona.py`        | In `start_supervisor_in_sandbox` (line 104), behind `AGENT_SDK_USE_IMAGE_RUNTIME`: skip the cache-visibility check (lines 184-213), skip the tar-extract block (lines 215-248), and skip the legacy fallback (lines 249-262); resolve `acp_bin = "/opt/agent-sdk/runtime/node_modules/.bin/<bin>"` and `sup_dir` from the in-image path baked into the sandbox. |
| `src/api/providers/daytona.py`        | In `provision_daytona_sandbox` (line 330), add an alternate provisioning fork: when the flag is on, EITHER use `DAYTONA_SNAPSHOT` (from `release.sh`'s registered snapshot, NOT the legacy `hive-large` default — the default-when-flag-on is `None`) OR `DAYTONA_IMAGE` (from `.runtime-image-tag` if present, else `os.environ["DAYTONA_IMAGE"]`). The flag-off path keeps `hive-large` for backwards compat through Phase D. |
| `src/api/providers/daytona.py`        | When flag on, the volume mounts list (`_build_volume_mounts` in `_shared.py:383`) drops the `system/supervisor` mount (it's the second `VolumeMount` in the existing list, line 425). Volume becomes data-only. |
| `src/api/sandbox/providers/daytona.py:162` | The stale `_ensure_volume_supervisor` call site is unconditionally bypassed when the flag is on (the function on `dt_provider` is `install_supervisor`-bound; with flag on, the runtime is already in the image, so the install dispatch in `session._bootstrap_session` never fires — guarded by Phase E's removal). For Phase C, gate this call so it's a no-op when the flag is on. |
| `tests/test_daytona_pre_start.py` and other daytona unit tests | Add flag-on coverage in unit/dispatch tests. The full daytona golden suite (`test_sandbox_stop_delete_recovery.py -k daytona`, `test_pre_start_*.py`) is the actual regression net. |

#### C.2.2 The provisioning fork

```python
# in provision_daytona_sandbox, replacing lines 364-393:
use_image_runtime = os.environ.get("AGENT_SDK_USE_IMAGE_RUNTIME") == "1"

if use_image_runtime:
    snapshot = os.environ.get("DAYTONA_SNAPSHOT", "").strip()  # NO hive-large default
    if snapshot:
        # Operator opted into a pre-warmed snapshot via release.sh
        sandbox = ... (CreateSandboxFromSnapshotParams(snapshot=snapshot, ...))
    else:
        # Image path. Read .runtime-image-tag if present, else env var, else fail.
        image = os.environ.get("DAYTONA_IMAGE") or _read_runtime_image_tag()
        if not image:
            raise RuntimeError("AGENT_SDK_USE_IMAGE_RUNTIME=1 requires DAYTONA_IMAGE or .runtime-image-tag")
        sandbox = ... (CreateSandboxFromImageParams(image=image, ...))
else:
    # EXISTING code from lines 364-393, unchanged.
```

`_read_runtime_image_tag()` is a small helper added to `_shared.py`:
```python
def _read_runtime_image_tag() -> str | None:
    """Read .runtime-image-tag from the repo root, or None if absent."""
    p = _REPO_ROOT / ".runtime-image-tag"
    if not p.exists():
        return None
    return p.read_text().strip() or None
```

(`_REPO_ROOT` resolved via the same `Path(__file__).resolve().parents[N]` pattern.)

#### C.2.3 Special note on snapshot semantics

The design (§3.3) says "the default `DAYTONA_SNAPSHOT=hive-large` constant
(currently inlined in `daytona.py:893`) is **removed**." `daytona.py` has
THREE such default sites: lines 364, 689, 893. Phase C keeps the legacy
default in the **flag-off** branch (so today's production keeps working);
Phase E removes them all when the flag-off branch is deleted.

#### C.2.4 Acceptance criteria

- Both flag positions produce green daytona golden suites:
  - `tests/test_sandbox_stop_delete_recovery.py -k daytona -n auto`
  - `tests/test_pre_start_commands_persist.py -k daytona -n auto`
  - `tests/test_skills_and_pre_start_survive_external_delete.py -k daytona -n auto`
  - `tests/test_pre_start_*.py -k daytona -n auto` (any others)
- Flag-on requires `.runtime-image-tag` to exist in the repo (Phase F
  produces it; for Phase C, hand-build a tag and commit it temporarily,
  or set `DAYTONA_IMAGE` in CI).
- Daytona quota guard: re-run `python scripts/cleanup_daytona_orphans.py
  --origin test --yes` between flag-on and flag-off runs to avoid the
  2000GiB ceiling.

#### C.2.5 Rollback

Revert the single commit. The flag-off path (still `hive-large` snapshot,
volume-cached deps.tar.gz) is unchanged and remains the production path.

### C.3 Sub-phase C.3 — `modal` provider

#### C.3.1 Files to change

| File                                  | Change                                          |
|---------------------------------------|-------------------------------------------------|
| `src/api/providers/modal.py`          | `_get_image()` (line 124): when `AGENT_SDK_USE_IMAGE_RUNTIME=1`, build `modal.Image.from_dockerfile(repo_root/"Dockerfile")` instead of the programmatic `debian_slim().apt_install(...)`. |
| `src/api/providers/modal.py`          | In `create_sandbox` and `_build_entrypoint_cmd` (lines 282-380): when flag on, the in-sandbox `_SUPERVISOR_IN` becomes `/opt/agent-sdk/runtime` (image-baked) instead of `/v/system/supervisor`. The symlink line `f"ln -s /v/system/supervisor {_SUPERVISOR_IN}"` (line 304) drops. |
| `src/api/providers/modal.py`          | Other `_get_image()` callers (line 673) inherit the same logic. |
| `tests/test_modal*` (if any)          | Verify, add flag-on coverage. |

#### C.3.2 Acceptance / rollback

- Modal golden suite passes both ways. (Verify whether modal goldens are
  in the repo's CI; if not, manual smoke is the bar.)
- Rollback: revert the single commit.

### C.4 Phase C overall acceptance

After all three sub-PRs land:

- The full provider matrix passes with flag on AND flag off:
  - `local`: tests/test_local_volume_integration.py + tests/test_sandbox_stop_delete_recovery.py -k local
  - `docker`: tests/test_docker_volume_integration.py + tests/test_sandbox_stop_delete_recovery.py -k docker
  - `daytona`: tests/test_sandbox_stop_delete_recovery.py -k daytona + pre-start suites
  - `modal`: any modal-specific suites
- The cumulative-install regression test still passes (flag-off only —
  with flag on, `install_supervisor` is never called, so the test's
  premise doesn't apply; gate it with `@pytest.mark.skipif(flag on)`).

---

## Phase D — Flip the flag default to ON; soak

**Goal**: flip `AGENT_SDK_USE_IMAGE_RUNTIME` default to `"1"` so production
defaults to the new path. Old path is kept reachable for emergency
rollback. Soak in production for at least 7 days before Phase E.

**PR scope**: 1 PR, ~10 LOC.

### D.1 Files to change

| File                                  | Change                                          |
|---------------------------------------|-------------------------------------------------|
| `src/api/providers/_shared.py` (or wherever the flag is read) | Replace `os.environ.get("AGENT_SDK_USE_IMAGE_RUNTIME") == "1"` with `os.environ.get("AGENT_SDK_USE_IMAGE_RUNTIME", "1") != "0"` — i.e. default-ON, opt-out via `=0`. |
| `docs/local-dev.md` (if it exists; else README) | Document: "If you're running against source and `npm --prefix src/supervisor install` hasn't been run, set `AGENT_SDK_USE_IMAGE_RUNTIME=0` to use the legacy install-on-volume path. (This will be removed in a follow-up.)" |
| `scripts/launch_server_local.sh` and `scripts/launch_server_test.sh` | Add `npm --prefix src/supervisor install --silent` as a setup step (cheap when already installed). Without this, a fresh checkout's local-dev path crashes with "ACP binary missing" because the default is now image-runtime. |

### D.2 Tests to run before merging Phase D

The full provider matrix, default flag (i.e. on):

```
.venv/bin/python -m pytest tests/ -n auto
```

Then with the legacy path explicitly opted in:

```
AGENT_SDK_USE_IMAGE_RUNTIME=0 .venv/bin/python -m pytest tests/ -n auto
```

Both must be green.

### D.3 Acceptance criteria

- Defaults work without setting any env var (image runtime).
- `=0` opt-out still works (legacy install-on-volume path).
- `launch_server_local.sh` adds `npm install` step that runs in <2s on a
  warm cache.
- Production has been soaked at least 7 days under default-on with no
  regressions before Phase E ships.

### D.4 Rollback

Revert the single commit (default goes back to off). Production reverts
to the volume-install path on the next deploy.

---

## Phase E — Delete the old path, the DB column, and the obsolete tests

**Goal**: remove ~975 LOC of the old install path, invariants, helpers,
and tests. The DB column drop is the **last commit of Phase E** and is the
only irreversible step in the entire refactor.

**PR scope**: 1 PR with multiple commits in the order below. Each commit
is independently revertable; only the final DB-drop commit is irreversible.

### E.1 Commits, in strict order

#### E.1.a Commit 1 — Delete `install_supervisor` from each provider

Files:
- `src/api/providers/local.py:114-222` — delete the entire `install_supervisor`
  function, including the cumulative-install fix at lines 169-184. Also
  remove `system/supervisor` directory creation in `create_volume` (line 93).
- `src/api/providers/docker.py:140-239` — delete `install_supervisor` and
  the versioned-symlink scheme. Remove `system/supervisor` initialization
  in `create_volume` if any.
- `src/api/providers/daytona.py:868-1009` — delete `install_supervisor`.
  Also remove the `system/supervisor` mkdir in `_init_volume_dirs`
  (line 716) — volume holds only `shared/`.
- `src/api/providers/modal.py:201-275` — delete `install_supervisor`.
- `src/api/providers/__init__.py:211` — remove `"install_supervisor"` from
  `_DISPATCH_FNS` frozenset.

Side cleanup in the same commit:
- `src/api/providers/local.py:38-39, 261, 265-266, 270-283` — drop the
  feature-flag branch added in Phase B; the new path is the only path now.
  `_SUPERVISOR_JS_SRC` constant is no longer used; delete.
- `src/api/providers/docker.py` — drop the flag branch from C.1; runtime
  is always image-baked. Replace `_NODE_IMAGE` with the agent-sdk image
  pulled from `.runtime-image-tag` (the design §3.2's "sandbox container
  is started from the same image that the server runs in"). Drop the bind-mount
  scaffolding from C.1; the runtime is at `/opt/agent-sdk/runtime` natively
  now.
- `src/api/providers/daytona.py` — drop the flag branch from C.2; remove
  the legacy-cached-deps-tarball block from `start_supervisor_in_sandbox`
  (lines 180-262). The supervisor command always uses
  `/opt/agent-sdk/runtime/...`.
- `src/api/providers/daytona.py:364, 689, 893` — replace each
  `os.environ.get("DAYTONA_SNAPSHOT", "hive-large")` with
  `os.environ.get("DAYTONA_SNAPSHOT")` (no default, see §C.2.3). Lines
  897-898 (the `volumes = [VolumeMount(...mount_path="/work", subpath="system")]`)
  in the deleted `install_supervisor` are gone. Lines 689-723
  (`_init_volume_dirs`) keep the snapshot-vs-image fork but use
  `_read_runtime_image_tag()` for the image-path default.
- `src/api/providers/modal.py:124-139` — drop the flag branch from C.3;
  `_get_image()` always returns `modal.Image.from_dockerfile(...)`.

#### E.1.b Commit 2 — Delete the install dispatch from `session._bootstrap_session`

File: `src/api/sandbox/session.py:137-152` — delete the entire `if agent_type
not in volume.supervisor_agent_types: ...` block (15 lines). Remove the
`from importlib import import_module` line if it becomes dead.

The function returns `self._volume_ref` directly after hydrating
`_subpath`, `_spawn_env`, etc.

#### E.1.c Commit 3 — Delete the stale `_ensure_volume_supervisor` call site

File: `src/api/sandbox/providers/daytona.py:160-162` — the call to
`self._ensure_volume_supervisor(dt_provider)` is dead (the method doesn't
exist anywhere; this was an `AttributeError` waiting to fire). Replace
with the line `volume_ref = await self._bootstrap_session()` directly.

#### E.1.d Commit 4 — Delete `add_supervisor_agent_type` and the import

Files:
- `src/api/db.py:585-595` — delete the `add_supervisor_agent_type` function
  and its SQL.
- `src/api/server.py:35` — remove the `add_supervisor_agent_type` from the
  `from .db import (...)` block (it's already unused).

#### E.1.e Commit 5 — Delete `VolumeRecord.supervisor_agent_types`

Files:
- `src/api/models.py:93` — delete the `supervisor_agent_types: list[str]
  = field(default_factory=list)` field.
- `src/api/db.py:531, 535, 537, 545` — drop the column from the INSERT
  / UPDATE / SELECT shaping. The migration drop comes in Commit 7.
- Any other references found by `grep -rn "supervisor_agent_types" src/`.

#### E.1.f Commit 6 — Delete obsolete tests

Files to delete entirely:
- `tests/test_install_chaos.py` (341 LOC; entire file — chaos tests for an
  orchestration that no longer exists).
- `tests/test_install_supervisor_fault.py` (170 LOC; entire file — fault
  injection on a function that no longer exists).
- `tests/test_local_volume_integration.py::test_install_supervisor_populates_volume`
  (lines 95-111).
- `tests/test_local_volume_integration.py::test_install_supervisor_is_cumulative_across_agent_types`
  (lines 114-140; the cumulative-install regression test, no longer
  meaningful since the cumulative-install fix is gone with the function).
- `tests/test_sandbox_stop_delete_recovery.py::test_session_survives_supervisor_dir_wiped_from_volume`
  (lines 1400-1452; nothing to self-heal because nothing is on the volume).

Files to update:
- `tests/test_provider_dispatch.py:80-150` — drop
  `test_install_supervisor_unknown_provider` and remove
  `"install_supervisor"` from the `for attr in (...)` loop at line 143.
- `tests/test_api_consistency.py:155-205` — drop the
  `patch("api.server.ensure_volume_supervisor", ...)` usages (already
  broken; verify in 0.4 question 5).
- `tests/test_docker_volume_integration.py:411` — fix the comment
  reference and any tests that called `install_supervisor` directly.

Files to add (new tests, design §6.3):
- `tests/test_runtime_image_resolves_all_acp_bins.py` (replaces the deleted
  `test_install_supervisor_populates_volume`): given a runtime path with
  every `_ACP_NPM_SPECS` bin installed (use the `tmp_path` fixture +
  symlink stubs), assert that `create_sandbox` for every supported
  agent_type successfully resolves and spawns. Mock the actual `Popen`
  step for speed.
- `tests/test_dockerfile_builds_runtime.py` (CI-only, behind
  `pytest -m docker_build`): runs `docker build .`, runs
  `docker run <image> ls /opt/agent-sdk/runtime/node_modules/.bin/`,
  asserts every name in `_ACP_BIN_NAMES` for npm-managed agents is present.
- `tests/test_release_image_tag_committed.py`: asserts `.runtime-image-tag`
  exists, parses to `<registry>/<image>:<sha>`, and the SHA is reachable
  in `git log`. Skipped when `AGENT_SDK_RUNTIME_IMAGE_TAG_SKIP=1` (so
  developer branches without a fresh release don't false-fail).

(Add `docker_build` marker to `pyproject.toml`'s `[tool.pytest.ini_options]
markers` section.)

#### E.1.g Commit 7 — Drop the DB column (IRREVERSIBLE)

File: `src/api/db.py`'s `_MIGRATIONS` list (line 67-onwards). Append a
new statement at the END of the list:

```sql
ALTER TABLE volumes DROP COLUMN IF EXISTS supervisor_agent_types
```

Do NOT remove the original ADD-COLUMN migration at line 177; migrations
are forward-only and cumulative. Existing databases run the new DROP at
startup; fresh databases run ADD then immediately DROP.

This commit lands LAST. After it merges, rolling back the entire refactor
requires a manual `ALTER TABLE volumes ADD COLUMN supervisor_agent_types
JSONB NOT NULL DEFAULT '[]'::jsonb` on production before the rollback
deploy can boot. **State this in the PR description.**

### E.2 Tests to run before merging Phase E

Full suite, default (image runtime):

```
.venv/bin/python -m pytest tests/ -n auto
```

The legacy-path opt-out is gone; there is no flag-off run. The
docker-build marker test runs separately:

```
.venv/bin/python -m pytest tests/test_dockerfile_builds_runtime.py -m docker_build -n auto
```

### E.3 Acceptance criteria

- Net LOC change in `src/` is approximately -700 (provider install paths,
  session install dispatch, db helper, model field).
- Net LOC change in `tests/` is approximately -700 (deletes outweigh new
  tests).
- All tests pass with no skip markers added (other than `docker_build`).
- The DB drop commit is the final commit on the branch.

### E.4 Rollback

- Commits 1-6: standard `git revert`.
- Commit 7 (DB drop): manual `ALTER TABLE volumes ADD COLUMN ...` on
  production followed by `git revert` of the migration commit. Document
  this as the only manual-intervention rollback path.

---

## Phase F — Release tooling, runtime tag, and cleanup migration

**Goal**: ship the operational tooling that makes the new path
self-sustaining: release script, image-tag commit machinery, and a
one-shot disk-reclamation migration for production volumes.

**PR scope**: 1 PR, ~150 LOC additions.

### F.1 Files to add

#### F.1.a `scripts/release.sh`

Per design §2.4. Concrete shape:

```bash
#!/usr/bin/env bash
set -euo pipefail
REGISTRY="${AGENT_SDK_REGISTRY:-ghcr.io/<org>}"  # See open question 1
SHA="$(git rev-parse --short HEAD)"
TAG="${REGISTRY}/agent-sdk:${SHA}"

docker build -t "$TAG" .
docker push "$TAG"

echo "$TAG" > .runtime-image-tag
git add .runtime-image-tag
# Caller is expected to commit (script does NOT auto-commit).

if [[ -n "${DAYTONA_API_KEY:-}" ]]; then
    daytona snapshot create --name "agent-sdk-${SHA}" --image "$TAG"
    echo "Daytona snapshot registered: agent-sdk-${SHA}"
fi
```

Make it executable (`chmod +x scripts/release.sh`).

The script does NOT auto-commit `.runtime-image-tag` — the caller commits
it as part of the release PR. This keeps the script idempotent and avoids
a script-driven git operation in CI.

#### F.1.b `.runtime-image-tag` (committed file)

A single line: `<registry>/<image>:<sha>`. Initially produced by the
first run of `scripts/release.sh`. Committed back to the repo.

#### F.1.c `scripts/cleanup_volume_supervisor_dirs.py`

Per design §5.1. Function shape:

```python
"""Reclaim disk on existing volumes by removing system/supervisor/.

Usage:
    python scripts/cleanup_volume_supervisor_dirs.py --provider local --dry-run
    python scripts/cleanup_volume_supervisor_dirs.py --provider local --yes
    python scripts/cleanup_volume_supervisor_dirs.py --provider daytona --yes
    ...

Required arg: --provider {local,docker,daytona,modal}
Required for write: --yes  (else dry-run regardless of --dry-run)

For each volume listed in `volumes` table for this provider:
  - local: shutil.rmtree(<provider_ref>/system/supervisor) if exists
  - docker: docker run --rm --mount type=volume,source=<ref>,target=/v
            alpine rm -rf /v/system/supervisor
  - daytona: spawn short-lived sandbox with whole-volume mount,
             rm -rf /v/system/supervisor
  - modal: modal.Function exec'ing rm -rf /v/system/supervisor on the volume

Logs every action; reports total volumes processed and errors per volume.
Idempotent: safe to re-run.
"""
```

Defaults to `--dry-run` unless `--yes` is passed. Logs every action.

#### F.1.d `.dockerignore` audit

Confirm `.dockerignore` doesn't exclude `src/supervisor/node_modules` (it
must NOT). If absent, add a `.dockerignore` that excludes `tests/`,
`.venv/`, `.postgres-*`, `.worktrees/`, but includes `src/supervisor/`.

#### F.1.e `pyproject.toml`

Add the `docker_build` marker to `[tool.pytest.ini_options]`:

```toml
markers = [
    "docker_build: tests that invoke docker build (slow, gated)",
]
```

### F.2 Tests to add

- `tests/test_release_script.py`: smoke-test that `scripts/release.sh`
  is executable, parses arguments correctly, and refuses to run without
  `docker` on PATH (mock `which` to return None).
- `tests/test_cleanup_volume_supervisor_dirs_dry_run.py`: against a
  fixture volume tree, assert `--dry-run` reports correctly without
  side effects.

### F.3 Acceptance criteria

- `scripts/release.sh` builds + pushes + writes `.runtime-image-tag` on
  a clean checkout.
- The committed `.runtime-image-tag` is parseable by `_read_runtime_image_tag()`.
- `scripts/cleanup_volume_supervisor_dirs.py --provider local --dry-run`
  on the dev machine reports the expected directories without removing
  them.
- Production runs the cleanup once per provider (separate ops PR / on-call
  runbook entry, NOT included in this code PR).

### F.4 Rollback

- Revert the PR. The cleanup script is operational tooling — its presence
  is harmless if nobody runs it. The `.runtime-image-tag` file removal
  causes daytona provisioning to require an explicit `DAYTONA_IMAGE` env
  var; if that's also unset, daytona session creation fails loudly,
  which is correct.

---

## Coordination & merge order (high-conflict file map)

Three other agents are concurrently touching `local.py`, `daytona.py`,
`server.py`. Recommended merge order:

1. **Phase A first.** Touches `Dockerfile`, `_shared.py` (helper), no
   provider files. Conflict-free with any in-flight work.
2. **Phase B second.** Touches `local.py` only. If a `local.py`
   conflict materializes (e.g. another agent is rewriting `create_sandbox`),
   reschedule Phase B first/last to land in a window between
   `local.py`-touching PRs. Keep the diff small and additive.
3. **Phase C in this order** to minimize blast radius:
   - **C.1 docker** first (lowest external usage; fewest other concurrent
     PRs touch it).
   - **C.3 modal** next (low traffic; modal-only paths).
   - **C.2 daytona LAST** because `daytona.py` (1438 LOC) is the
     hottest-conflict file and has three `DAYTONA_SNAPSHOT="hive-large"`
     defaults plus the `start_supervisor_in_sandbox` cache-extract path.
     Land C.2 in a quiet window. Coordinate explicitly with anyone touching
     daytona before merging.
4. **Phase D fourth.** Single-line default flip; minimal conflict.
5. **Phase E fifth.** Touches every provider file PLUS `db.py`,
   `models.py`, `server.py`, `session.py`, plus deletes test files.
   By design this should be the only PR open against those files for
   its merge window — coordinate heavily. Land in a quiet window with a
   24h soak after Phase D.
6. **Phase F last.** Operational tooling; conflict-free.

### Specific conflict-prone hunks to watch

- `src/api/providers/local.py:114-222` — Phase E deletes the cumulative-install
  fix landed in the most recent PR. If anyone bumps that fix between
  Phase B and Phase E, the rebase is mechanical (the entire function is
  going away) but the PR description must explicitly call out "yes, we
  are reverting <other-PR-sha> as part of this refactor."
- `src/api/providers/daytona.py:868-1009` — `install_supervisor` in
  daytona; nobody else should be modifying this in parallel because it's
  marked for deletion. Shadow-flag any incoming PR that touches it.
- `src/api/providers/daytona.py:104-262` — `start_supervisor_in_sandbox`
  is shared by the per-session path AND `restart_daytona_supervisor`.
  Phase C.2 alters its body materially. Any in-flight perf work on this
  function must coordinate.
- `src/api/server.py:35` — the unused `add_supervisor_agent_type` import.
  Phase E removes it; mechanical rebase if anyone else touches the import
  block.
- `src/api/sandbox/session.py:137-152` — the install dispatch. Delete
  in Phase E. If anyone modifies the surrounding `_bootstrap_session`
  body, rebase by hand.

---

## Definition of done

A flat checklist. Every item from the design's §7 deletion table appears,
plus every item from §6.3 new-tests, plus phase-level operational gates.

### Code deletions (design §7)

- [ ] `src/api/providers/local.py::install_supervisor` deleted (~110 LOC).
- [ ] `src/api/providers/docker.py::install_supervisor` deleted (~80 LOC).
- [ ] `src/api/providers/daytona.py::install_supervisor` deleted (~150 LOC).
- [ ] `src/api/providers/modal.py::install_supervisor` deleted (~70 LOC).
- [ ] `src/api/sandbox/session.py:137-152` install branch deleted (~15 LOC).
- [ ] `src/api/db.py::add_supervisor_agent_type` and its SQL deleted (~15 LOC).
- [ ] `src/api/db.py` migration appending `ALTER TABLE volumes DROP COLUMN
      IF EXISTS supervisor_agent_types` added (~5 LOC).
- [ ] `src/api/models.py::VolumeRecord.supervisor_agent_types` deleted (~3 LOC).
- [ ] `tests/test_install_chaos.py` deleted (~340 LOC).
- [ ] `tests/test_install_supervisor_fault.py` deleted (~170 LOC).
- [ ] `tests/test_sandbox_stop_delete_recovery.py::test_session_survives_supervisor_dir_wiped_from_volume`
      deleted (~50 LOC).
- [ ] Cumulative-install fix in `src/api/providers/local.py:114-222`
      reverted as part of the function deletion (~50 LOC).
- [ ] `DAYTONA_SNAPSHOT="hive-large"` defaults at `daytona.py:364, 689, 893`
      replaced with no-default reads (~10 LOC).
- [ ] `node:22-slim` fallback in `install_supervisor` (lines 911, 707) deleted.
- [ ] `tests/test_provider_dispatch.py` references to `install_supervisor`
      removed.
- [ ] `tests/test_api_consistency.py` references to `ensure_volume_supervisor`
      removed.
- [ ] `src/api/providers/__init__.py:211` — `"install_supervisor"` removed
      from `_DISPATCH_FNS`.
- [ ] `src/api/server.py:35` — `add_supervisor_agent_type` import removed.
- [ ] `src/api/sandbox/providers/daytona.py:162` — `_ensure_volume_supervisor`
      call site removed (was already an `AttributeError` waiting; doc this
      explicitly).

### Code additions (design §7 "Code that grows")

- [ ] `Dockerfile` extension: install all `_ACP_NPM_SPECS`, `cp` runtime
      to `/opt/agent-sdk/runtime`, set `ENV AGENT_SDK_RUNTIME_PATH=...` (~15 LOC).
- [ ] `scripts/release.sh` (~25 LOC).
- [ ] `_detect_runtime_path()` + `_runtime_supervisor_js()` +
      `_runtime_acp_bin()` helpers in `_shared.py` (~25 LOC).
- [ ] `_read_runtime_image_tag()` helper in `_shared.py` (~10 LOC).
- [ ] Per-provider `create_sandbox` updated to read from the runtime path
      (~5 × 4 = 20 LOC).
- [ ] `src/supervisor/package.json` — verified to include all
      `_ACP_NPM_SPECS` (already does as of plan-time; no change needed).
- [ ] `.runtime-image-tag` committed to repo root.
- [ ] `scripts/cleanup_volume_supervisor_dirs.py` (~80 LOC).
- [ ] `docs/local-dev.md` updated with `npm --prefix src/supervisor install`
      one-time setup step (or, if file doesn't exist, README updated).

### New tests (design §6.3)

- [ ] `tests/test_runtime_path_resolution.py` — pure unit tests for
      `_detect_runtime_path()` (image present, source present, neither,
      both → image wins, env var override).
- [ ] `tests/test_dockerfile_builds_runtime.py` (CI-only, behind
      `pytest -m docker_build`): `docker build .`, `docker run <image>
      ls /opt/agent-sdk/runtime/node_modules/.bin/`, assert every
      `_ACP_BIN_NAMES.values()` for `_ACP_NPM_SPECS` agent_types is present.
- [ ] `tests/test_release_image_tag_committed.py` — `.runtime-image-tag`
      exists, parses, SHA reachable in `git log`. Skip on dev branches via
      `AGENT_SDK_RUNTIME_IMAGE_TAG_SKIP=1`.

### Phase / process gates

- [ ] Phase A merged with full test suite green and `docker build .` working.
- [ ] Phase B merged with both flag-on and flag-off green for local goldens.
- [ ] Phase C merged (3 sub-PRs) with full daytona/docker/modal/local
      goldens green for both flag positions.
- [ ] Phase D merged: default flag flipped to ON; `launch_server_*.sh`
      install step added; soaked in production for ≥7 days with no
      regressions.
- [ ] Phase E merged: old path removed; DB column dropped; cumulative-install
      regression test deleted; PR description explicitly notes "DB drop is
      irreversible; manual `ALTER TABLE` required to roll back."
- [ ] Phase F merged: `scripts/release.sh` works end-to-end; cleanup script
      dry-runs cleanly per provider.
- [ ] Cleanup migration run once per provider in production (separate ops
      runbook task, NOT bundled with the code PR).

---

## Open questions to resolve before starting

These must be answered (and recorded in this doc or in the relevant PR
description) before Phase A starts. Repeated from §0.4 for top-of-doc
visibility:

1. **Image registry**: GHCR, Docker Hub, or private? Affects `release.sh`
   default. Recommended: GHCR.
2. **Modal image strategy**: `modal.Image.from_dockerfile()` vs. keep
   programmatic. Recommended: `from_dockerfile`. Verify Modal version
   compat.
3. **Test fixture vs env-var for `AGENT_SDK_RUNTIME_PATH`**: pick one
   convention so unit tests are uniform.
4. **`goose` / `openhands`**: stay PATH-resolved (not in `_ACP_NPM_SPECS`)?
   Confirm and document. Plan retains PATH fallback.
5. **Stale-test policy**: are `test_install_chaos.py` and
   `test_api_consistency.py`'s `ensure_volume_supervisor` references
   currently failing-at-collection? Confirm before Phase E so the
   deletion's CI signal isn't already-red-noise.
6. **Cumulative-install fix revert acceptance**: confirm with the original
   PR's author that bundling the revert into Phase E is acceptable.
7. **Mid-flight production sessions during deploy**: confirm the rolling
   deploy plan for Phase E is "kill old sandboxes; new ones come up from
   image" rather than expecting hot-attach compatibility (design §5.3
   says yes; reaffirm before deploy).

### Discrepancies between design doc and codebase (found during planning)

These are NOT planning errors — they're cases where the design doc's
prose drifted from the current code, and the implementer should be aware
so the LOC accounting is honest:

- **`server.py::ensure_volume_supervisor` does not exist.** The design's
  §7 table claims ~120 LOC there. That function was deleted in a prior
  refactor; the actual install dispatch is now 15 LOC in
  `src/api/sandbox/session.py:137-152` (already counted on its own row).
  Net: the design overstates total deletions by ~120 LOC. Real total is
  closer to ~1140 LOC (still significant; just not 1260).
- **`src/supervisor/package.json` already declares all six
  `_ACP_NPM_SPECS` deps.** Design §4 says it "needs extending"; in fact
  no change is required. The local-dev `npm install` already pre-resolves
  every npm-managed bin.
- **`tests/test_install_supervisor_fault.py` is 170 LOC, not 250.**
  Mechanically: still entirely deleted; LOC accounting just smaller.
- **`tests/test_install_chaos.py` and `tests/test_api_consistency.py`**
  reference `ensure_volume_supervisor` which doesn't exist; they're
  almost certainly already failing at import time. Expect "delete" not
  "modify" once verified.
- **`src/api/sandbox/providers/daytona.py:162`** has a stale call to
  `self._ensure_volume_supervisor(dt_provider)` against a method that
  was never defined on `DaytonaSandboxSession`. This is an
  `AttributeError`-on-cold-create waiting to fire. Phase E commit 3
  fixes it; if any cold-create golden test fails before Phase E with
  `AttributeError: '...' object has no attribute '_ensure_volume_supervisor'`,
  that's the cause and the fix can be hot-patched ahead of schedule.

---

End of plan.
