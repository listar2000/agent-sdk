# Cycle 1 Test Audit — `volume-refactor` branch

External agent reviewed 2026-04-22.

## Part A — Test pollution root causes (NOT all pollution)

### Root cause 1 — `sys.modules["api.db"]` stub leak (actual pollution)

`tests/test_adversarial.py:39-90` (and `106-134`) does `sys.modules["api.db"] = _stub_db` at **module import time**, with **no teardown**. Once collected, every later test that does `from api import db as dbmod` gets the no-op stub whose `get_db()` yields `None`.

**Affected (all pass in isolation, fail after adversarial):**
- `tests/test_volumes_db.py` — 6 tests
- `tests/test_ensure_helpers.py` — 6 tests
- `tests/test_session_volume_integration.py` — 8 tests
- `tests/test_volumes_api.py` — 4 tests
- `tests/test_session_volume_e2e.py::test_sandbox_loss_resume_end_to_end`

### Root cause 2 — stale tests reference removed symbols (NOT pollution)

- `tests/test_queue_interrupt.py:68` — `if "api.db" not in sys.modules` creates order-dependence. Own stub is incomplete, so file **only** imports cleanly when adversarial ran first.
- `tests/test_queue_interrupt.py::TestResumeRecovery::test_empty_reaped_session_starts_fresh_inner_session` + `tests/test_session_recovery.py::TestSessionRecoveryAfterReap::test_message_endpoint_recovers_reaped_session` both patch `api.server._do_resume`, which was removed in the ensure-refactor.

### Root cause 3 — pre-existing real failure (NOT pollution)

- `test_adversarial.py::test_concurrent_double_ensure_registered_only_calls_api_once` — fails alone with `ValueError: refusing to send credentials to 'http://fake' over plaintext HTTP`. `client.py:239` added this guard after the test was written.

### Fixes

1. Remove module-level `sys.modules["api.db"] = _stub_db` from `test_adversarial.py` (lines 39-90, 106-134) and `test_queue_interrupt.py` (lines 32-69). Convert to session-scoped fixtures using `patch.dict(sys.modules, ...)` per test.
2. Rewrite `test_session_recovery.py` and `test_queue_interrupt.py::TestResumeRecovery` against `ensure_sandbox`/`ensure_runtime` — `_do_resume` is gone.
3. Update `test_concurrent_double_ensure_registered_only_calls_api_once` — use `http://localhost` or drop the oauth token.

## Part B — Lifecycle / error-recovery coverage matrix

| # | Scenario | Status |
|---|----------|--------|
| 1 | Sandbox death mid-message | Missing |
| 2 | Sandbox reaped between messages | Covered |
| 3 | Concurrent `ensure_sandbox` on fresh session | Missing |
| 4 | Concurrent `ensure_volume_supervisor` on fresh volume | Partial — design claims pg advisory, impl uses asyncio.Lock |
| 5 | `ensure_volume_supervisor` partial failure (install OK, cache fails) | Missing |
| 6 | Volume delete with active session | Covered |
| 7 | FK ON DELETE SET NULL end-to-end | Partial |
| 8 | ACP inner_session_id reuse / FUSE-lag retry | Partial |
| 9 | Supervisor health-check timeout | Missing |
| 10 | `volume_read` on invalid path/volume | Partial |
| 11 | Provider dispatch on unknown provider | Partial — raises bare `KeyError` |
| 12 | Sandbox stop → start resume | Covered (Docker); partial Daytona |
| 13 | Cross-provider isolation | Missing — see C1 |
| 14 | DB connection loss during `ensure_sandbox` | Missing |
| 15 | Disk-full / network during `install_supervisor` | Missing |

## Prioritized test-writing plan

1. Concurrent `ensure_volume_supervisor` across-worker race (#4).
2. Concurrent `ensure_sandbox` on fresh session (#3).
3. `ensure_volume_supervisor` partial failure retry (#5).
4. Cross-provider isolation (#13) — paired with the C1 fix.
5. FK SET NULL end-to-end (#7).
6. Supervisor health-check timeout (#9).
7. Sandbox death mid-message (#1).
8. `volume_read` on unknown provider / missing volume (#10, #11).
9. `install_supervisor` fault injection (#15).
10. Rewrite session-recovery tests against ensure_* API.
