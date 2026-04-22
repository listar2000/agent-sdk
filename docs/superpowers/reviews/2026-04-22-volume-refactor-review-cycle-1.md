# Cycle 1 External Review — `volume-refactor` branch

External agent reviewed 2026-04-22 after Phases 0-5 landed.

## Critical (breaks today)

- **C1** `src/api/server.py:1943-1981` — `_provision_new` always calls `provision_daytona_sandbox` and sets `provider="daytona"` / `root="/home/daytona"` regardless of `vol.provider`. Docker/Local sessions cannot provision via `/sessions/{id}/message`.
- **C2** `src/api/server.py:905-910`, `docker.py:299-365`, `local.py:305-385` — `sandbox_ref` stores the port via `_derive_sandbox_ref` for port-based providers, but provider APIs (`docker.get_sandbox_status`, `local._lookup_proc`) expect a container_id / pid. Re-provision loop.
- **C3** `src/api/server.py:2042-2058` — `ensure_runtime` constructs `ProviderInstance(url="")` and hands it to `ensure_supervisor_url`. For Docker/Local the returned URL is empty; `AcpClient("")` fails.
- **C4** `src/api/server.py:1273-1328` — `/sandboxes/provision` ignores `provider` in request body, always uses Daytona.
- **C5** `src/api/server.py:1105-1157` — `/volumes/{id}/files/*` calls `_run_in_volume_sandbox`, which raises 501 for non-Daytona. Docker/Local `volume_{tree,read,write}` are fully implemented and unit-tested but unreachable via REST.
- **C6** `src/api/server.py:1713-1784` — `_ensure_sandbox_alive` auto-restart drops `volume_id`/`subpath` when calling `create_instance`. Docker raises on empty subpath; Local launches outside the volume.
- **C7** `src/api/server.py:227-241` — `_shutdown_session_state` unconditionally imports `daytona_sdk` and calls `daytona_client.get(sandbox_id)`. Leaks per-session supervisor process on Docker/Local.

## Major (concurrency / edge cases)

- **M1** `src/api/server.py:1955` — `ensure_volume_supervisor` is only called inside `_provision_new` (Daytona-only via C1). `/sessions/quick` calls `create_instance` with no preceding install. Docker/Local fail with `"supervisor.js missing"`.
- **M2** `src/api/server.py:1853-1880` — docstring claims pg advisory locks but code uses in-process `asyncio.Lock()` only. Two server processes racing on same `(volume_id, agent_type)` will both install.
- **M3** Partial `install_supervisor` failure + sticky `package-lock.json` → re-entry behaves unpredictably. Consider staging-dir + atomic rename.
- **M4** `_provision_new` does `upsert_sandbox` then `set_session_current_sandbox` as two writes. Crash between → orphaned sandbox. Wrap in one transaction.
- **M5** Docker containers not `--rm` and no startup reconciliation. OOM-kill → zombie leak. Add `--label agent-sdk.sandbox-id=<id>` + startup scan.
- **M6** `_find_free_port` doesn't verify OS availability. Use `docker run -p 0:9100` + parse.
- **M7** `local.py:440-448` — TOCTOU between `_safe_join` and `open`. Use `openat` with `O_NOFOLLOW`.
- **M8** No early guard for unknown/mismatched providers.

## Minor

- **m1** `provision_daytona_sandbox` imported top-level; others local inside functions. Pick a style.
- **m2** `daytona.py:743-752` — `volume_tree/read/write` stubs raise `NotImplementedError`. Either implement or delete once server-side dispatch for Daytona files goes through `_run_in_volume_sandbox`.
- **m3** `daytona.py:166-170` — `if "yes" in check_result:` matches error messages too. Tighten to `strip() == "yes"`.
- **m4** `daytona.py:116, 202` — `asyncio.sleep(3)` before `_wait_for_health`. Redundant.
- **m5** Three near-identical path sanitizers (`_safe_path`, `_safe_rel`, `_safe_join`). Consolidate into `_shared.py`.
- **m6** `_init_volume_dirs` spins up a full sandbox to `mkdir`. Use `sb.fs.create_folder` instead.
- **m7** Docstring lies in `_provision_new` ("Create a fresh Daytona sandbox").
- **m8** `PORT_BASED_PROVIDERS` and `_derive_sandbox_ref` conflate "uses a port" and "`sandbox_ref` is a port".
- **m9** Root-level `demo.py` (+124 lines) not in plan. Move to `examples/` or drop.

## Test gaps

- **T1** No test drives `_provision_new` with a non-Daytona volume. Would have caught C1.
- **T2** No integration test for sandbox crash mid-message / SSE.
- **T3** No test for `delete_volume` cascade on non-Daytona provider.
- **T4** No test for concurrent `ensure_volume_supervisor` on new volume.
- **T5** No test for DB failure during `add_supervisor_agent_type` (install succeeds, cache update fails).
- **T6** No test for crash between `upsert_sandbox` and `set_session_current_sandbox` (M4).
- **T7** No test for corrupted `inner_session_id` on reattach.
- **T8** No test that `ensure_supervisor_url` for Docker/Local returns a non-empty URL. Would have caught C3.
- **T9** `_safe_join` TOCTOU not covered.
