"""Bug A — ``pool.get_session`` must release compute when ``session.start()``
raises AFTER the underlying sandbox was already provisioned.

This is a POOL-LAYER integration test, not a golden (live-server) test, on
purpose:

  * Golden-style can't *deterministically* induce the failure. Bug A fires
    when ``create_sandbox`` succeeds (compute acquired) but ``_attach_acp``
    then raises — an agent-side ``session/new`` ``-32603``. There's no
    reliable HTTP-level way to make a real provider boot a sandbox+supervisor
    yet fail ``session/new`` on command, and the 502 the client gets back
    doesn't carry the leaked ``sandbox_ref`` to assert against.
  * The leak is provider-agnostic and lives in ``SessionPool.get_session``:
    ``await session.start()`` has no failure handling, so when start() raises
    after acquiring compute, the session never enters ``_active`` and nothing
    ever calls ``session.stop()`` → ``provider.destroy/stop`` → the SLURM job
    / daytona VM / container leaks until its own time limit.

So we drive the real ``SessionPool`` with a fake session whose ``start()``
mirrors the dangerous shape: acquire the sandbox, set ``state.sandbox_ref``,
THEN raise at the attach stage. The invariant: the pool must tear that
compute down (call ``stop()``) before the exception propagates.

Pre-fix this FAILS (``stop()`` never called — the leak). Post-fix (wrap
``await session.start()`` in get_session with stop()+shutdown on failure)
it PASSES.

Run::

    .venv/bin/python -m pytest tests/test_pool_releases_compute_on_start_failure.py -n auto
"""
from __future__ import annotations

import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from api.sandbox import (  # noqa: E402
    BaseSandboxSession,
    ModalSandboxState,
    Recipe,
)


class _LeakySession(BaseSandboxSession):
    """A session whose ``start()`` acquires compute then fails at attach.

    Mirrors every provider's ``start()`` outline (reattach → create_sandbox
    → set ``state.sandbox_ref`` → health probe → ``_attach_acp``) at the
    exact point Bug A bites: create_sandbox has returned (``acquired`` flips
    True, a sandbox_ref is recorded — real SLURM job / VM / container), and
    then attach raises. ``stop()`` records the teardown the pool is supposed
    to perform on the failure path.
    """

    volume_provider = "test"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.acquired = False
        self.stopped = False
        self.did_shutdown = False

    async def start(self) -> None:
        # create_sandbox succeeded: compute is now allocated + costing money.
        self.acquired = True
        self.state.sandbox_ref = "fake-sandbox-leaked-123"
        # _attach_acp fails — the agent-side session/new -32603 from the report.
        raise RuntimeError(
            "Failed to initialize session: session/new returned no sessionId "
            "and no existing sessions found"
        )

    async def running(self, *, force_probe: bool = False) -> bool:
        return False

    async def execute_prompt(self, *args, **kwargs):
        if False:  # pragma: no cover — make this an async generator
            yield

    async def stop(self) -> None:
        # Real sessions route stop() -> provider.destroy/stop -> compute freed.
        self.stopped = True

    async def shutdown(self) -> None:
        self.did_shutdown = True
        self._close_subscribers()


@pytest.mark.asyncio
async def test_get_session_releases_compute_when_start_fails(monkeypatch):
    from api import db as db_mod
    from api.sandbox.pool import SessionPool

    created: list[_LeakySession] = []

    def _factory(sid, state):
        s = _LeakySession(session_id=sid, state=state)
        created.append(s)
        return s

    pool = SessionPool(factory=_factory)

    async def _noop(*a, **k):
        return None

    # get_session would write state + publish after start(); start() raises
    # first so these aren't reached, but stub them so the test never touches
    # a real DB / broadcast bus even if the ordering changes.
    monkeypatch.setattr(pool, "_publish_state", _noop)
    monkeypatch.setattr(db_mod, "write_sandbox_state", _noop)

    # Pass initial_state so get_session skips the DB read path and goes
    # straight to factory() + start().
    state = ModalSandboxState(recipe=Recipe())

    with pytest.raises(RuntimeError, match="session/new returned no sessionId"):
        await pool.get_session("leaky-sess", initial_state=state)

    assert len(created) == 1, "factory should have constructed exactly one session"
    sess = created[0]

    # Sanity: the failure happened AFTER compute was acquired (otherwise this
    # test wouldn't be exercising the leak path at all).
    assert sess.acquired is True, (
        "test precondition broken: start() must acquire compute before raising"
    )

    # THE BUG: compute was acquired but the pool let start()'s exception
    # propagate without tearing the sandbox down.
    assert sess.stopped is True, (
        "BUG A — COMPUTE LEAK: session.start() raised after create_sandbox "
        "acquired the sandbox, but pool.get_session did not call "
        "session.stop(). Nothing frees the SLURM job / daytona VM / container "
        "— it leaks until its own time limit. Fix: wrap `await session.start()`"
        " in get_session with a try/except that calls session.stop() + "
        "_safe_shutdown(session) before re-raising."
    )

    # And it must not be left dangling in the live pool.
    assert "leaky-sess" not in pool._active, (
        "failed session must not remain in _active"
    )
