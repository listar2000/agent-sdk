"""Reproduces the orphan-provider-sandbox bug in the idle reaper.

Background (from production logs in this branch's investigation):
A session can land in ``lifecycle == "hibernated"`` while its underlying
provider sandbox (Daytona / Docker / Modal workspace) is still running.
Two ways this happens:

  1. ``_hibernate_session`` calls ``stop_sandbox`` and the provider raises.
     The code logs a WARNING but still flips the DB row to STOPPED, pops
     ``_INSTANCES``, and sets ``state.lifecycle = "hibernated"`` (server.py
     ~line 329-339). From the server's perspective, the sandbox is stopped.
     From Daytona/Docker's perspective, the workspace is still RUNNING.

  2. Out-of-band: anything that pops ``_INSTANCES`` and flips ``lifecycle``
     without going through ``_hibernate_session``'s stop call.

Either way, when the idle reaper later sees the now-hibernated state with
no subscribers, it takes the ``elif not state._session_subscribers``
branch (server.py ~line 456-467) and calls ``_shutdown_session_state``,
which only kills the supervisor process inside the sandbox — never
``stop_sandbox`` on the provider. Net result: an orphaned RUNNING sandbox
in the cloud provider that the server has zero memory of.

The test simulates condition (1): a hibernate where ``stop_sandbox`` failed
once on the provider. The test then runs one reaper tick and checks that
the eviction path attempts to stop the sandbox on the provider — i.e.
that orphan recovery is built into the eviction path, not relied on from
the prior (failed) hibernate.

This test FAILS on the current branch (demonstrating the bug). The
intended fix: in ``_reap_one_tick``'s eviction branch, fetch the sandbox
DB row and call ``stop_sandbox`` defensively before
``_shutdown_session_state``. Or push that call into
``_shutdown_session_state`` itself when force=True and the session has a
sandbox_id.
"""
from __future__ import annotations

import asyncio
import os
import sys
import time

import pytest
import pytest_asyncio
from unittest.mock import AsyncMock, patch

_SRC = os.path.join(os.path.dirname(__file__), "..", "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

_DB = os.environ.get("TEST_DATABASE_URL")
pytestmark = pytest.mark.skipif(_DB is None, reason="TEST_DATABASE_URL not set")
if _DB:
    os.environ["DATABASE_URL"] = _DB

from api import db as dbmod, server as srv  # noqa: E402
from api.models import (  # noqa: E402
    AgentConfig, AgentRecord, SandboxRecord, SessionState, VolumeRecord,
)
from api.providers import ProviderInstance  # noqa: E402


@pytest_asyncio.fixture
async def setup(clean_db):
    yield


async def _seed_session_with_running_provider_sandbox():
    """Insert agent/volume/sandbox/session rows + register an in-memory
    SessionState in SESSIONS and ProviderInstance in _INSTANCES, mirroring
    the post-resume state the server holds for a live session.
    """
    await dbmod.upsert_agent(AgentRecord(
        id="a-orphan", name="A", config=AgentConfig(agent_type="claude"),
    ))
    await dbmod.upsert_volume(VolumeRecord(
        id="v-orphan", name="v", provider="daytona", provider_ref="dt-vol-orphan",
    ))
    sb = SandboxRecord(
        id="sb-orphan", provider="daytona", sandbox_ref="dt-sandbox-orphan",
        status="running", root="/home/daytona",
        volume_id="v-orphan", subpath="agents/a-orphan",
    )
    await dbmod.upsert_sandbox(sb)
    async with dbmod.get_db() as conn:
        await conn.execute(
            "INSERT INTO sessions (id, agent_id, volume_id, current_sandbox_id) "
            "VALUES (%s,%s,%s,%s)",
            ("s-orphan", "a-orphan", "v-orphan", "sb-orphan"),
        )
    state = SessionState(
        session_id="s-orphan", agent_id="a-orphan",
        sandbox_id="sb-orphan", agent_type="claude",
        lifecycle="live",
    )
    srv.SESSIONS["s-orphan"] = state
    srv._INSTANCES["sb-orphan"] = ProviderInstance(
        provider="daytona", url="http://fake-supervisor",
        root="/home/daytona", sandbox_id="dt-sandbox-orphan",
    )
    return state


@pytest.mark.asyncio
async def test_eviction_branch_does_not_orphan_running_provider_sandbox(setup):
    """Reaper eviction branch must not leak a still-running provider sandbox.

    Scenario:
      1. Session is live; sandbox is running at provider.
      2. Hibernate is triggered. ``stop_sandbox`` raises on the provider
         (network blip / 502 from Daytona) — exactly the failure the
         existing ``hibernate: stop_sandbox failed`` warning catches.
      3. Hibernate continues anyway: pops _INSTANCES, sets
         ``lifecycle = hibernated``, flips DB row to STOPPED. The provider
         sandbox is STILL RUNNING.
      4. Reaper tick runs ~5 minutes later, sees no subscribers, takes the
         "evict hibernated" branch.

    Invariant: the eviction path must (re)attempt ``stop_sandbox`` on the
    provider before forgetting about the sandbox forever. Otherwise the
    sandbox runs indefinitely in the cloud provider.

    Without the fix this test fails — _shutdown_session_state never calls
    stop_sandbox, so step 2's failure is the last chance and the sandbox
    is leaked.
    """
    await _seed_session_with_running_provider_sandbox()

    stop_calls: list[str] = []
    fail_first = {"value": True}

    async def fake_stop_sandbox(instance):
        stop_calls.append(instance.sandbox_id)
        if fail_first["value"]:
            fail_first["value"] = False
            raise RuntimeError("daytona 502 — sandbox stop refused")

    state = srv.SESSIONS["s-orphan"]

    # Step 2-3: hibernate with provider stop failing once. The hibernate
    # logs a warning, sets lifecycle=hibernated, pops _INSTANCES — exactly
    # the production behaviour we already observed in the prod log.
    with patch("api.providers.daytona.stop_sandbox",
               new=AsyncMock(side_effect=fake_stop_sandbox)), \
         patch("api.server._cancel_task", new=AsyncMock()), \
         patch("api.server._request_supervisor_snapshot", new=AsyncMock()):
        await srv._hibernate_session(state)

    assert state.is_hibernated, "lifecycle should flip even when stop fails"
    assert "sb-orphan" not in srv._INSTANCES, "_INSTANCES popped per hibernate contract"
    assert stop_calls == ["dt-sandbox-orphan"], "first stop_sandbox attempted (and failed)"

    # Re-attach the SessionState since hibernate evicts it when there are
    # no subscribers (which is the path we want the reaper to take next).
    # In the bug scenario the state is already evicted at this point —
    # but we need a SessionState present in SESSIONS for the reaper tick
    # to be the thing under test. So re-register it.
    srv.SESSIONS["s-orphan"] = state
    state.shutdown.clear()
    # Push idle clock back so the reaper considers it long-idle.
    state.last_activity = time.time() - srv.IDLE_TIMEOUT_S - 60
    state.turn_completed_at = state.last_activity

    # Step 4: reaper tick. With the bug this calls _shutdown_session_state
    # only — no stop_sandbox. With the fix, eviction calls stop_sandbox on
    # the provider before forgetting the sandbox.
    with patch("api.providers.daytona.stop_sandbox",
               new=AsyncMock(side_effect=fake_stop_sandbox)), \
         patch("api.server._cancel_task", new=AsyncMock()), \
         patch("api.server._close_session_gracefully", new=AsyncMock()):
        await srv._reap_one_tick(time.time())

    # Eviction must have happened (state removed from SESSIONS).
    assert "s-orphan" not in srv.SESSIONS, "reaper should evict the hibernated state"

    # The orphan-prevention assertion. This FAILS on the current branch:
    # the eviction path calls _shutdown_session_state which does not call
    # stop_sandbox. After the fix, the eviction path retries stop_sandbox
    # on the provider before fully forgetting the sandbox.
    assert len(stop_calls) >= 2, (
        "BUG: eviction path leaked a running provider sandbox. The first "
        "stop_sandbox call (during hibernate) failed, and the eviction "
        "path never retried — so the workspace stays RUNNING in Daytona "
        "with no in-memory record. Calls observed: " + repr(stop_calls)
    )
