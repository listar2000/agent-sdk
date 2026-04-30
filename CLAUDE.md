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

## Daytona-specific notes

The daytona golden suite runs cleanly under `-n auto` as of #42 (POST /message
no longer blocks on cold-recovery; transition-aware probe handles
`starting`/`stopping`/etc. without spurious teardowns). If it suddenly starts
failing with `Total disk limit exceeded. Maximum allowed: 2000GiB`, that's
NOT a code regression — it's orphaned sandboxes from a previous failed run
accumulating on the Daytona side. Run:

    python scripts/cleanup_daytona_orphans.py --origin test --yes

(default `--origin` is `production`, which only touches sandboxes from
non-test servers — the test wrapper sets `AGENT_SDK_ORIGIN=test`.)
