"""Regression tests for async/await correctness hazards.

Surviving coverage after the SessionPool refactor (which deleted the
``_spawn_bg`` / ``_BG_TASKS`` / ``_get_session_lock`` / ``_shutdown_session_state``
/ ``_maybe_auto_approve_permission`` / ``_cancel_task`` plumbing this file
originally pinned):

1. Local-provider ``create_sandbox`` rollback runs the blocking
   ``_kill_proc`` off the event loop — a hung supervisor health check
   can't stall every other coroutine for 10 seconds. Source-grep test.
"""
from __future__ import annotations

import asyncio
import os
import sys

import pytest

# ---------------------------------------------------------------------------
# 1. Local-provider rollback must not block the event loop
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_local_create_sandbox_rollback_uses_thread():
    """When supervisor health check fails, _kill_proc runs off the event loop.

    We don't exercise the full create_sandbox path — the fix swaps a raw
    ``_kill_proc(proc)`` call for ``await asyncio.to_thread(_kill_proc, proc)``.
    Verify by grepping the source so the regression is detected syntactically
    (integration runs of the local provider cover the behavior).
    """
    import inspect
    from api.providers import unix_local as lp

    src = inspect.getsource(lp.create_sandbox)
    # The rollback paths must NOT call _kill_proc synchronously — that
    # blocks the event loop for up to 10 seconds.
    assert "await asyncio.to_thread(_kill_proc," in src, (
        "local.create_sandbox rollback must offload _kill_proc to a thread"
    )
    # Sanity: at least the two rollback sites are updated.
    assert src.count("await asyncio.to_thread(_kill_proc,") >= 2


# ---------------------------------------------------------------------------
# 2. Port allocator atomicity under concurrent callers
# ---------------------------------------------------------------------------


