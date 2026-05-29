# Testing

Run pytest with `-n auto` (pytest-xdist). Sequential daytona/docker
goldens are 8–15+ min; xdist parallel is mandatory. `-n auto` is fine
with `-k` filters — xdist negotiates worker count down.

    .venv/bin/python -m pytest tests/test_golden.py -n auto
    .venv/bin/python -m pytest tests/test_golden.py -n auto -k claude
    .venv/bin/python -m pytest tests/test_golden.py -n auto -k opencode

End-to-end tests are parametrized over `claude` + `opencode` via
`tests/_acp_runtimes.py`. Each runtime auto-skips on missing
credential (`CLAUDE_CODE_OAUTH_TOKEN` / `OPENROUTER_API_KEY`).

For local dev and the golden suite, launch the server with
`scripts/launch_server_test.sh` — the only local launcher. It defaults
`AGENT_SDK_ORIGIN=test` so daytona sandboxes carry `agent_sdk_origin=test`
and `cleanup_orphans.py` can isolate them from production. Override with
`AGENT_SDK_ORIGIN=production scripts/launch_server_test.sh` if you need a
production-tagged server locally.

## Sandbox cleanup across test runs

Tests that create real sandboxes (daytona / docker / local) are wrapped
by an autouse `_auto_cleanup_live_sessions` fixture in
`tests/conftest.py`. It tracks every session created via `Agent` or
`ApiClient.create_session` and fires `DELETE /sessions/{id}` at teardown
even on test failure.

For paused-on-release residue (daytona pauses; docker stops):

    python scripts/cleanup_orphans.py                       # dry run, all providers
    python scripts/cleanup_orphans.py --yes                 # reap origin=test across daytona + docker + unix_local
    python scripts/cleanup_orphans.py --provider daytona --yes   # one provider only

The script defaults to `--provider all` and `--origin $AGENT_SDK_ORIGIN`
(falls back to `test`), so the no-flag invocation is the right one to run
after a flaky golden suite. Each provider's section is skipped silently
if its dep is missing (`DAYTONA_API_KEY` unset, `docker` not on PATH).

CI opt-in for auto post-session cleanup: `AGENT_SDK_TEST_AUTO_CLEANUP=1`.
Off by default to avoid churn on local unit-test runs.

## Daytona quota errors

If the daytona golden suite fails with `Total disk limit exceeded.
Maximum allowed: 2000GiB`, that's orphaned sandboxes from a prior
failed run, NOT a code regression. Run
`cleanup_orphans.py --provider daytona --yes` (defaults to the `test`
origin; production is safe).

## Rebuilding runtime artifacts

After touching `src/supervisor/supervisor.js`, `Dockerfile`, or
`_ACP_NPM_SPECS`, rebuild before re-running daytona/modal goldens —
the snapshot tags are pinned to a specific commit:

    scripts/release.sh                       # docker + daytona + modal
    scripts/release.sh --provider daytona    # one provider only

Commit `.runtime-image-tag` / `.runtime-snapshot-tag` /
`.modal-snapshot-tag` alongside the runtime-affecting source change.
