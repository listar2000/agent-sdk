# Testing

Always run `pytest` with `-n auto` (pytest-xdist) so the full suite runs across
all available workers. Sequential runs of the daytona/docker golden suites take
8–15+ minutes and waste an enormous amount of iteration time. Example:

    .venv/bin/python -m pytest tests/test_sandbox_stop_delete_recovery.py -n auto

`-n auto` is fine even when filtering with `-k` — pytest-xdist negotiates worker
count down to the number of selected items.

Launch the dev server for the golden tests with `scripts/launch_server_test.sh`
(NOT `launch_server_local.sh` directly). The test wrapper sets
`AGENT_SDK_ORIGIN=test` so daytona sandboxes get labelled `agent_sdk_origin=test`
and stay isolatable from real production traffic — `cleanup_daytona_orphans.py`
greps that label.

## Sandbox cleanup across test runs

Tests that create real sandboxes (live daytona / docker / local provider)
are wrapped by an autouse `_auto_cleanup_live_sessions` fixture in
`tests/conftest.py`. It tracks every session created via `Agent` or
`ApiClient.create_session` and fires `DELETE /sessions/{id}` at teardown
even on test failure — so the session row + pool lease are always dropped.

For paused-not-deleted residue (daytona pauses on release; docker stops
on release; both leak across runs), use the unified cleanup script:

    # Dry run — see what'd be reaped:
    python scripts/cleanup_orphans.py

    # Reap everything labelled agent_sdk_origin=test:
    python scripts/cleanup_orphans.py --yes

    # Or one provider at a time:
    python scripts/cleanup_orphans.py --provider daytona --yes
    python scripts/cleanup_orphans.py --provider docker --yes
    python scripts/cleanup_orphans.py --provider unix_local --yes  # kills orphan supervisor.js whose ppid==1

CI can opt into automatic post-session cleanup with
`AGENT_SDK_TEST_AUTO_CLEANUP=1` (off by default for local dev to avoid
churn on every unit-test run).

## Daytona-specific notes

The daytona golden suite runs cleanly under `-n auto` as of #42 (POST /message
no longer blocks on cold-recovery; transition-aware probe handles
`starting`/`stopping`/etc. without spurious teardowns). If it suddenly starts
failing with `Total disk limit exceeded. Maximum allowed: 2000GiB`, that's
NOT a code regression — it's orphaned sandboxes from a previous failed run
accumulating on the Daytona side. Run `cleanup_orphans.py --provider daytona
--yes` (filter defaults to the `test` origin, so production is safe).
