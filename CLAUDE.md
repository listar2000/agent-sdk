# Testing

Always run `pytest` with `-n auto` (pytest-xdist) so the full suite runs across
all available workers. Sequential runs of the daytona/docker golden suites take
8–15+ minutes and waste an enormous amount of iteration time. Example:

    .venv/bin/python -m pytest tests/test_sandbox_stop_delete_recovery.py -n auto

`-n auto` is fine even when filtering with `-k` — pytest-xdist negotiates worker
count down to the number of selected items.
