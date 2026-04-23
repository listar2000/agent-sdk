# Cycle 4 External Review — `volume-refactor`

Fresh reviewer after cycles 1-3. Suite is 429 passed / 51 skipped. Cycle 1 criticals all closed.

## Major (new, not in cycle 1)

### MA1 — Docker label (M5) only set on `_provision_new` path

`providers/__init__.py:106-149::create_instance` has no `sandbox_id` kwarg. Call sites that don't pass it → unlabeled containers → reconcile can't find them → orphans on restart:
- `server.py:1184` (`POST /sandboxes`)
- `server.py:2599` (`POST /sessions/quick`)
- `server.py:1819-1827` (`_ensure_sandbox_alive` auto-restart)

Fix: thread `sandbox_id` through `create_instance` + pass it at all call sites.

### MA2 — Docker `install_supervisor` `rm -rf` deletes files from under live sandboxes

`docker.py:177`: `rm -rf /work/supervisor` on a volume subpath that currently-running containers have bind-mounted. Open inodes survive but new `dlopen`/`require` → ENOENT.

Fix options:
1. Gate installs on "no running sandboxes reference this volume"
2. Versioned subdir (`supervisor.v<n>`) + symlink flip
3. Document the constraint loudly

### MA3 — Docker reconcile conflates "resumable stopped" with "orphan"

`docker.py:486-489`: DB `status in ("stopped", "deleted")` + live container → force-removed. But `stop_sandbox_route` sets `status="stopped"` before calling `stop_instance`; partial crash leaves "stopped" in DB + alive container → reconcile nukes it.

Fix: reconcile must not force-remove a DB row in "stopped" state, OR change the ordering so stop_instance completes before status flip.

### MA4 — pg advisory lock holds pool connection across full Daytona install

`server.py:1947-1995`: one connection held for 60-300s while utility sandbox boots. Idle-in-transaction timeout → lock released silently → re-entrance → parallel install.

Fix: acquire a shorter-lived sentinel row; release advisory lock before the provider call. Or document the single-worker install guarantee.

### MA5 — Port bind-probe TOCTOU; no retry on docker run port collision

`_shared.py:197-205` narrowed but didn't eliminate the race. `docker run` can still fail with "port already allocated". No retry.

Fix: catch the error in `docker.py:302-306` and re-allocate once.

## Minor

- **MI1** `server.py:90` — dead `provision_daytona_sandbox` import.
- **MI2** `server.py:996-1003` — `POST /volumes` hardcodes daytona, 501s for docker/local; `_providers_mod.create_volume` works for all three.
- **MI3** `providers/__init__.py:152-169` — `create_instance`/`destroy_instance` use if/elif, no final else-raise. Destroy silently drops unknown providers.
- **MI4** `daytona.py:635-641` + `server.py:917` — docstring drift.
- **MI5** `server.py:1177-1182` — unreachable branch (`vol is not None` after 404 already raised).
- **MI6** `docker.py:246-247` — raises ValueError for empty subpath, but REST returns 502 rather than 400. Pre-check in REST layer.
- **MI7** `daytona.py:322-352::restart_daytona_supervisor` — uses legacy `_bootstrap_supervisor_in_daytona_sandbox` which installs to ephemeral `/tmp`, not the volume cache. Inconsistent with create path.
- **MI8** `ensure_supervisor_url` signature divergence — Daytona requires `agent_type`; Docker/Local silently accept **_kw.

## Test gaps

- **MT1** No test verifies Docker `agent-sdk.sandbox-id` label is set.
- **MT2** No test for `reconcile_on_startup` (would have caught MA3).
- **MT3** No test for install_supervisor mid-run crash → staging cleanup.
- **MT4** No test for `_find_free_port` bind-probe / OS-assigned fallback.
- **MT5** No test for control-char path rejection through consolidated `_safe_path`.
- **MT6** No test for install_supervisor while a sandbox on the same volume runs (ties to MA2).
- **MT7** No test for `docker stop` (non-destructive) vs reconcile (ties to MA3).
- **MT8** `test_cross_provider_volume_rejection_via_sandboxes_endpoint` still has pytest.xfail fallback despite the guard being in place — tighten to hard assert.

## Summary

Branch is in good shape; cycle-1 criticals are really fixed. Biggest remaining risk: MA1 (label propagation gap) — cycle-2's reconcile is half-wired and MT1 gap hides it. MA2 + MA3 are design-level semantic bugs that cycle-1's lens couldn't have seen.
