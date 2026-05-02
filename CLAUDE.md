# Testing

Run pytest with `-n auto` (pytest-xdist). Sequential daytona/docker
goldens are 8–15+ min; xdist parallel is mandatory. `-n auto` is fine
with `-k` filters — xdist negotiates worker count down.

    .venv/bin/python -m pytest tests/test_sandbox_stop_delete_recovery.py -n auto

For the golden suite, launch the dev server with
`scripts/launch_server_test.sh` (NOT `launch_server_local.sh` directly).
The wrapper sets `AGENT_SDK_ORIGIN=test`, so daytona sandboxes carry
`agent_sdk_origin=test` and `cleanup_orphans.py` can isolate them from
production.

## Sandbox cleanup across test runs

Tests that create real sandboxes (daytona / docker / local) are wrapped
by an autouse `_auto_cleanup_live_sessions` fixture in
`tests/conftest.py`. It tracks every session created via `Agent` or
`ApiClient.create_session` and fires `DELETE /sessions/{id}` at teardown
even on test failure.

For paused-on-release residue (daytona pauses; docker stops):

    python scripts/cleanup_orphans.py                       # dry run
    python scripts/cleanup_orphans.py --yes                 # reap origin=test
    python scripts/cleanup_orphans.py --provider daytona --yes
    python scripts/cleanup_orphans.py --provider docker --yes
    python scripts/cleanup_orphans.py --provider unix_local --yes   # orphan supervisor.js, ppid==1

CI opt-in for auto post-session cleanup: `AGENT_SDK_TEST_AUTO_CLEANUP=1`.
Off by default to avoid churn on local unit-test runs.

## Daytona quota errors

If the daytona golden suite fails with `Total disk limit exceeded.
Maximum allowed: 2000GiB`, that's orphaned sandboxes from a prior
failed run, NOT a code regression. Run
`cleanup_orphans.py --provider daytona --yes` (defaults to the `test`
origin; production is safe).
