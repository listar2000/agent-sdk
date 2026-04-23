# Cycle 8 External Review — `volume-refactor`

Fresh reviewer after cycles 6+7. Verdict: **SHIP WITH FIXES** (all landed).

## Ma1 — autocommit restore failure poisons pool

`src/api/server.py::ensure_volume_supervisor` finally block. If `set_autocommit(False)` restore raises, the previous code only warned; the connection then returned to pool in autocommit=True — the exact cycle-6 M1 bug one level deeper.

**Fix:** on restore failure, `await lock_conn.close()` so the pool opens a fresh one. Log at ERROR.

## Ma2 — Mi8 swallows docker "volume in use" silently

`src/api/server.py:1084-1096` (delete_volume_route). Previous code caught every Exception. Docker's "volume in use" is a real conflict the user should see.

**Fix:** narrow to `"not found" / "no such" / "does not exist" / "404"` substrings. Propagate everything else as HTTP 409. Mirror the daytona branch's whitelist pattern.

Tests updated: `test_delete_volume_provider_not_found_swallowed` (204), `test_delete_volume_provider_error_surfaces_as_409` (409 + DB row preserved).

## Ma3 — set_autocommit(True) exception path silent

Previously swallowed both `AttributeError` (test-fake shim) and real psycopg rejections. Separated: `AttributeError` stays tolerated; other exceptions log at ERROR.

## Minor — unused imports removed

- `server.py`: `get_session_env`, `get_session_secrets`
- `daytona.py`: `allocate_sandbox_port`
- `db.py`: `typing.Any`
- `server.py`: kept `add_supervisor_agent_type` (test patches via `api.server.add_supervisor_agent_type`)

## Suite

449 passed / 60 skipped / 0 failed.

## Cross-cycle ship-readiness

All cycle-1→7 items closed. Remaining defensive hardening from cycle-8 now landed. Branch is mergeable.
