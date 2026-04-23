# Cycle 6 External Review — `volume-refactor`

Fresh reviewer auditing cycle-5 fixes. 441 passed / 59 skipped / 0 failed.

## Major (regression introduced by cycle-5)

### M1 — `pg_advisory_lock` handler leaks `autocommit=True` back into pool

`src/api/server.py:2036-2104` (`ensure_volume_supervisor`). Calls `await lock_conn.set_autocommit(True)` on a pooled connection; never restores `False`. `AsyncConnectionPool` in `db.py:221-233` has no `reset` callback, so connections return with whatever state the borrower left. Next borrower via `get_db()` runs queries in autocommit mode; manual `await conn.commit()` at `db.py:251` is a no-op (or errors, depending on version).

Fix: wrap `set_autocommit(True)` in try/finally that restores, or configure `AsyncConnectionPool(reset=...)` callback.

## Major (test quality)

### M2 — MT6 rubber-stamp for MA2

`tests/test_session_volume_integration.py:661-715` tests the **local** provider, which still uses `shutil.rmtree + os.rename` (NOT the docker symlink-flip pattern MA2 introduced). Test passes because the node process holds an open fd on supervisor.js. Revert MA2 in docker and the test still passes.

Fix: either add a docker-backed variant, or relabel to clarify it's a local-provider integration check.

### M3 — MA5 port-collision retry untested

`src/api/providers/docker.py:348-367`. No test covers the retry loop. `test_providers_shared.py` covers `_find_free_port` but not the `create_sandbox` retry.

Fix: unit test mocking `_run_docker` to return port-in-use rc on first call and success on second.

## Minor

- **Mi1** `server.py:1181-1185` + `1338-1342` — subpath guards added by MI6 are unreachable (earlier check at 1174/1322 already 400s on empty subpath).
- **Mi2** `server.py:1973` — MA4 docstring says "pg_advisory_xact_lock" but code uses session-scoped `pg_advisory_lock`.
- **Mi3** `daytona.py:681` — still has `**_kw` catch-all. Docker/local tightened, daytona wasn't. Docstring in docker/local claims "matches Daytona's exactly" — lie.
- **Mi4** `server.py:984-1025` — `/volumes POST` retains daytona special-case just to keep `test_volumes_api.py` patches working. Update tests, drop the branch.
- **Mi5** `server.py:1329-1334` — `provider != vol.provider` check is structurally dead unless body explicitly overrides provider.
- **Mi6** cosmetic asymmetry — `local.volume_tree(ref, subpath="")` vs others' `(ref, path)`; `local.volume_write` accepts str, others bytes; arg-ordering differs. Not bugs, but defeats load-time arg checking.
- **Mi7** `server.py:1408-1424::stop_sandbox_route` — between successful `stop_instance` and failed `upsert_sandbox`, `_INSTANCES` is popped, container exited, DB reads "running". Next `_ensure_sandbox_alive` provisions a new container with same label; reconcile on next restart finds two containers with same label. Low-probability, real.
- **Mi8** `server.py:1081-1092::DELETE /volumes/{id}` — only daytona branch calls provider-side delete. Docker/local volumes leak. **Pre-existing**, not cycle-5's bug, but visible now.

## Cross-check with cycle-4

Cleanly closed: MA1, MA3, MA5 (modulo M3 test gap), MT1, MT2, MT4, MT7, MT8, MI1, MI3, MI5, MI7.

Partially closed:
- **MA2** — fixed for docker; local still uses rm+rename (not in scope, but worth flagging).
- **MA4** — lock semantics correct, but introduces M1 autocommit leak.
- **MI4** — one docstring drift traded for another (Mi2 above).
- **MI6** — guards live but dead code (Mi1 above).
- **MI8** — docker/local aligned, daytona left behind (Mi3 above).

## Ship verdict

Conditional ship. M1 blocks production rollout (silently breaks transactions under traffic). M2+M3 are test-quality gaps for next cycle. Minors are acceptable debt.
