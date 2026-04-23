# Volumes on Docker and Local — Implementation Plan

> REQUIRED SUB-SKILL: superpowers:subagent-driven-development for task-by-task execution.

**Spec:** `docs/superpowers/specs/2026-04-22-volumes-docker-local-design.md`

**Goal:** Extend the volume/session decoupling to Docker and Local providers. Relocate supervisor install onto the volume for all three providers. Collapse provider-specific knowledge under `src/api/providers/`.

**Branch:** `volume-refactor` (off `env-secrets-split`).

---

## Parallelism strategy

Dependency DAG:

```
Phase 0 (split providers.py into package)
    ↓
Phase 1 (provider dispatch in ensure_*)
    ↓
Phase 2 (supervisor-on-volume for Daytona)
    ↓
    ├─→ Phase 3 (Docker)  ─┐
    └─→ Phase 4 (Local)   ─┤
                            ↓
                       Phase 5 (cleanup retired files)
```

**The only genuinely parallel work is Phase 3 ∥ Phase 4** — different files (`docker.py` vs `local.py`), zero shared state changes, independent test files.

Phases 0-1-2 and Phase 5 edit overlapping files, so serial within-phase.

**Max parallel throughput:** 2 concurrent subagents during Phases 3/4.

---

## Phase 0 — Package split (zero behavior change)

**Files:** Move `src/api/providers.py` → `src/api/providers/` package.

**Tasks (1 subagent dispatch, serial):**

| # | Task |
|---|---|
| 0.1 | Create `src/api/providers/{__init__.py, daytona.py, docker.py, local.py}`. Move existing Daytona-specific functions into `daytona.py`. Move shared helpers (`ProviderInstance`, `_ACP_*` constants, `_wait_for_health`, port allocator, env prefix builders) into `__init__.py`. `docker.py` and `local.py` start with `raise NotImplementedError` stubs for `create_volume`, `delete_volume`, `create_sandbox`, `install_supervisor`, `start_supervisor`, etc. |
| 0.2 | Add dispatch wrappers in `__init__.py`: `async def create_volume(provider, name)`, `async def get_sandbox_status(provider, ref)`, etc. Each wrapper does `{"daytona": daytona, "docker": docker, "local": local}[provider].<fn>(...)`. |
| 0.3 | Re-export every symbol `server.py` currently imports (`create_instance`, `destroy_instance`, `create_daytona_volume`, `get_daytona_sandbox_status`, …) so `from .providers import X` keeps working. |
| 0.4 | Run full suite + live e2e. Expect zero test changes needed. Commit. |

**Acceptance:** All existing tests pass unchanged. `git diff env-secrets-split` shows only moved code + new thin wrappers.

**Estimated:** 1 subagent dispatch (sonnet).

---

## Phase 1 — Provider dispatch in `ensure_sandbox` / `ensure_runtime`

**Files:** `src/api/server.py`, `src/api/providers/__init__.py`.

**Tasks (1 subagent dispatch, serial):**

| # | Task |
|---|---|
| 1.1 | In `ensure_sandbox` / `_ensure_sandbox_locked` / `_provision_new`: replace direct calls to `get_daytona_sandbox_status`, `start_daytona`, `destroy_daytona`, `provision_daytona_sandbox` with `providers.<op>(vol.provider, ...)`. Read `vol.provider` once near the top. |
| 1.2 | In `ensure_runtime` / `_ensure_runtime_locked`: remove the `from daytona_sdk import Daytona` import and the `daytona.get(sandbox_ref)` call. Replace with `providers.start_supervisor(vol.provider, inst, …)` returning the URL. Daytona's impl keeps the existing logic; Docker/Local's will be no-ops returning `inst.url`. |
| 1.3 | Full suite + live e2e — must be zero-behavior-change for Daytona. |

**Acceptance:** `grep -n "daytona_sdk\|_daytona" src/api/server.py` returns zero matches. All tests green.

**Estimated:** 1 subagent dispatch (sonnet).

**Micro-optimization (from spec review):** rename `start_supervisor` → `ensure_supervisor_url(inst, session_config)` to avoid the Docker/Local "no-op shim" smell. Let the Daytona impl do real work; Docker/Local return `inst.url` immediately and the name matches the intent.

---

## Phase 2 — Supervisor-on-volume for Daytona

**Files:** `src/api/providers/__init__.py` (ensure_volume_supervisor), `src/api/providers/daytona.py` (install, create_volume+sandbox changes), `src/api/db.py` (new column).

**Tasks (1 subagent dispatch, serial):**

| # | Task |
|---|---|
| 2.1 | **DB schema:** add column `volumes.supervisor_agent_types JSONB NOT NULL DEFAULT '[]'::jsonb`. Acts as a cache of "which agent_types have been installed on this volume" — avoids a 30s utility-sandbox probe on every Daytona boot. |
| 2.2 | **`ensure_volume_supervisor(volume_id, agent_type)`** in `providers/__init__.py`. Acquires `pg_try_advisory_lock(hash((volume_id, agent_type)))`. Checks the cache column first; if agent_type already listed, return. Else call provider-specific `install_supervisor(volume_ref, agent_type)`, then append to cache. |
| 2.3 | **`daytona.install_supervisor`:** spin a 1-shot Daytona sandbox with `VolumeMount(..., subpath="system/supervisor")`. Exec `npm init -y && npm install <_ACP_NPM_SPECS[agent_type]>`. Upload `supervisor.js`. Tear down. |
| 2.4 | **`daytona.create_volume`:** after the existing `volume.create` + poll-ready, run a utility sandbox to `mkdir -p /v/shared /v/system/supervisor`. |
| 2.5 | **`daytona.create_sandbox`:** update `_build_volume_mounts` to return all three mounts (home / shared / supervisor). Drop the inline `apt-get install libssl3` and `npm install` and `supervisor.js` upload — `ensure_volume_supervisor` handles them now. |
| 2.6 | **Hook `ensure_volume_supervisor` into `ensure_sandbox`**: call it right before `_provision_new` actually calls the provider. |
| 2.7 | **Integration test:** extend `test_session_volume_integration.py` — verify first session installs, second session (same volume, same agent) skips install entirely (0 utility sandboxes spun). |
| 2.8 | Live e2e (3x for stability). |

**Acceptance:** `test_sandbox_loss_resume_end_to_end` still passes. Second sandbox-reattach on same volume completes in <10s (no install). 3/3 live e2e green.

**Estimated:** 1 subagent dispatch (sonnet), possibly split if the install path gets thorny.

---

## Phase 3 — Docker provider (can run parallel to Phase 4)

**Files:** `src/api/providers/docker.py` (new), `tests/test_docker_volume_integration.py` (new).

**Tasks (1 subagent dispatch, covers entire phase):**

| # | Task |
|---|---|
| 3.1 | `docker.create_volume(name)` — `docker volume create <name>` + utility container to `mkdir -p /v/shared /v/system/supervisor`. Return `<name>`. |
| 3.2 | `docker.delete_volume(ref)` — `docker volume rm <ref>`, tolerate "not found", raise on "in use". |
| 3.3 | `docker.install_supervisor(ref, agent_type)` — `docker run --rm` with two mounts (volume-subpath=system/supervisor; bind-ro of host's `src/supervisor/supervisor.js`) to do `npm init/install + cp supervisor.js`. |
| 3.4 | `docker.create_sandbox(volume_ref, subpath, …)` — ensure subpath dir exists via a `mkdir` utility run (batched with first create on a fresh volume), then `docker run -d --rm -p <port>:9100` with three `--mount`s + supervisor cmd. Returns `ProviderInstance(url=f"http://localhost:{port}", …)`. |
| 3.5 | `docker.get_sandbox_status(ref)` — `docker inspect` → running/missing/error. Docker never has a "stopped" state for our usage (we use `--rm`); document this. |
| 3.6 | `docker.stop_sandbox` / `docker.destroy_sandbox` — both: `docker rm -f <id>`. Stop == destroy. |
| 3.7 | `docker.volume_{tree,read,write}` — per-call `docker run --rm --mount` + shell command (`find`, `cat`, `tee` via base64). |
| 3.8 | `docker.ensure_supervisor_url(inst, …)` — returns `inst.url` unchanged (supervisor started at create_sandbox). |
| 3.9 | `docker.start_sandbox(ref)` — raise `NotImplementedError` or no-op (see spec note: Docker never returns "stopped"). |
| 3.10 | **Integration test** `tests/test_docker_volume_integration.py`: skip if `docker` not in PATH. Covers: volume CRUD, sandbox create + message, destroy + recreate same subpath preserves transcript, volume file-ops. |

**Acceptance:** New test suite green. Existing tests still green. Pattern: `SKIP` if docker unavailable on CI machine.

**Estimated:** 1 subagent dispatch (sonnet). **Parallel with Phase 4.**

---

## Phase 4 — Local provider (can run parallel to Phase 3)

**Files:** `src/api/providers/local.py` (new), `tests/test_local_volume_integration.py` (new).

**Tasks (1 subagent dispatch, covers entire phase):**

| # | Task |
|---|---|
| 4.1 | `local.create_volume(name)` — `os.makedirs(<root>/<name>/{shared,system/supervisor})`. Root is `AGENT_SDK_LOCAL_VOL_ROOT` env var, default `~/.agent-sdk/volumes/`. Return `<root>/<name>`. |
| 4.2 | `local.delete_volume(ref)` — `shutil.rmtree(ref)`. |
| 4.3 | `local.install_supervisor(ref, agent_type)` — `subprocess.run(["npm","init","-y"], cwd=<ref>/system/supervisor)` + install + `shutil.copy supervisor.js`. |
| 4.4 | `local.create_sandbox(volume_ref, subpath, …)` — `os.makedirs` agent-home. Launch supervisor subprocess with `HOME=<vol>/<subpath>`, `AGENT_SHARED_DIR=<vol>/shared`, node on `<vol>/system/supervisor/supervisor.js`, `--acp` points at the on-volume binary. Returns `ProviderInstance(url=f"http://127.0.0.1:{port}", process=proc, …)`. |
| 4.5 | `local.get_sandbox_status` — `proc.poll()` is `None` → running, else → missing. |
| 4.6 | `local.stop_sandbox` / `destroy_sandbox` — `proc.terminate()` + wait + `proc.kill()`. |
| 4.7 | `local.volume_{tree,read,write}` — direct FS in-process with **realpath containment check** (prevent symlink escape from volume root). |
| 4.8 | `local.ensure_supervisor_url(inst, …)` — returns `inst.url` unchanged. |
| 4.9 | **Integration test** `tests/test_local_volume_integration.py`: uses `AGENT_SDK_LOCAL_VOL_ROOT=<tmp>` for isolation. Symlink-escape attempt must be rejected. Same matrix as Docker: CRUD, sandbox run, destroy-recreate, file ops. |

**Acceptance:** New test suite green. Path-safety test rejects symlink escapes.

**Estimated:** 1 subagent dispatch (sonnet). **Parallel with Phase 3.**

---

## Phase 5 — Retirements

**Files to delete:** `src/supervisor/Dockerfile`, `src/supervisor/node_modules/`, `src/supervisor/package-lock.json`. **Symbols to delete:** `_ensure_supervisor_docker_image`, `_supervisor_image_ready`, `_supervisor_image_lock`, `_ensure_local_supervisor_deps`, install-deps branch of `_bootstrap_supervisor_in_daytona_sandbox`.

**Tasks (1 subagent dispatch, serial):**

| # | Task |
|---|---|
| 5.1 | Grep for any remaining callers of the retired symbols. None expected — Phase 2 retired the install path on Daytona, Phase 3 retired the pre-baked docker image, Phase 4 retired the host-side install. |
| 5.2 | Delete the files. Drop the dead symbols. |
| 5.3 | Run full suite + live e2e 3x + docker e2e (if available) + local e2e. |
| 5.4 | Line count report: expect −300 to −500 lines net vs. Phase 0. |

**Acceptance:** All three provider test suites green. No orphaned imports.

**Estimated:** 1 subagent dispatch (haiku — simple deletions + test run).

---

## Dispatch schedule (wall-time optimized)

| Dispatch | Phase(s) | Model | Concurrency |
|---|---|---|---|
| D1 | 0 | sonnet | serial |
| D2 | 1 | sonnet | serial |
| D3 | 2 | sonnet | serial |
| D4a | 3 (Docker) | sonnet | **parallel with D4b** |
| D4b | 4 (Local) | sonnet | **parallel with D4a** |
| D5 | 5 | haiku | serial |

**6 total subagent dispatches; D4a ∥ D4b saves 1 dispatch worth of wall time.**

Estimated wall time: sonnet dispatches typically take 3-10 min each depending on scope; D1-D3 include live Daytona e2e runs (+80s each). Rough total: 30-60 min of orchestrated work.

---

## Testing gates between phases

| After phase | Must pass |
|---|---|
| 0 | Full mocked suite + live Daytona e2e. Zero test changes. |
| 1 | Same as 0. Verify `grep daytona_sdk src/api/server.py` returns nothing. |
| 2 | Mocked suite + Daytona e2e 3x (fresh DB each). New cache column populated after first install. |
| 3 | Docker integration suite (skip if no docker). |
| 4 | Local integration suite. |
| 5 | All three provider suites + live Daytona e2e once more. |

---

## Open questions (from spec review)

1. **Docker asymmetry (1 container = 1 supervisor):** means Docker sandbox density is lower than Daytona. Confirmed OK given Docker containers are cheap.
2. **Local native-binding hazard:** cross-OS volume reuse (install on macOS, use on Linux) will break native `.node` files. Mitigation: document "local volumes are host-OS-scoped"; optional `os.uname()` fingerprint check during install (defer to future).
3. **`/opt/supervisor` writable mounts:** after first install, nobody writes there in practice, but we don't enforce it. Daytona SDK 0.168 doesn't support `read_only`. Revisit if we upgrade the SDK.
4. **Scope creep?** Doc spans spec → plan. Plan is narrow — 6 phases, 6 dispatches, 8-10 test files touched. Adequate.
