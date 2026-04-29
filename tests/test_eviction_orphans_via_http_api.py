"""HTTP-level reproduction of the orphan-provider-sandbox bug.

Companion to test_eviction_orphans_provider_sandbox.py — that file exercises
the bug via the internal _hibernate_session / _reap_one_tick helpers.
This file drives it through real FastAPI endpoints with ASGITransport,
proving the bug surfaces when an operator follows a normal API workflow:

    1. Create / restore a session bound to a Daytona sandbox.
    2. POST /sessions/{id}/hibernate while the provider is flaky
       (stop_sandbox returns 502 — exact symptom seen in the prod log:
       ``WARNING failed to stop daytona sandbox ... 502 Bad Gateway``).
    3. POST /admin/sessions/{id}/reap to fully tear down. The endpoint
       claims success but its own response body admits it never touched
       the provider — ``provider_stopped`` is null because _INSTANCES is
       already empty after the hibernate that "succeeded" server-side.

The response shape itself is the assertion: a passing test on a fixed
server would either return ``provider_stopped="daytona"`` (because admin
reap retried the stop) or expose a separate orphan-cleanup endpoint that
the client can drive. The current shape — claiming "reaped" while leaving
the workspace alive — is the bug.

Mock surface: only the leaf provider functions (stop_sandbox,
get_sandbox_status). Everything else is the real FastAPI app + real DB.
"""
from __future__ import annotations

import asyncio
import contextlib
import os
import sys
import time

import pytest
import pytest_asyncio
from unittest.mock import AsyncMock, patch
from httpx import ASGITransport, AsyncClient

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


async def _seed_live_session():
    """Seed DB rows + in-memory state representing a live session bound
    to a running Daytona sandbox. Mirrors the post-resume state the
    server holds after /sessions/{id}/message returns successfully.
    """
    await dbmod.upsert_agent(AgentRecord(
        id="a-http", name="A", config=AgentConfig(agent_type="claude"),
    ))
    await dbmod.upsert_volume(VolumeRecord(
        id="v-http", name="v", provider="daytona", provider_ref="dt-vol-http",
    ))
    sb = SandboxRecord(
        id="sb-http", provider="daytona", sandbox_ref="dt-sandbox-http",
        status="running", root="/home/daytona",
        volume_id="v-http", subpath="agents/a-http",
    )
    await dbmod.upsert_sandbox(sb)
    async with dbmod.get_db() as conn:
        await conn.execute(
            "INSERT INTO sessions (id, agent_id, volume_id, current_sandbox_id) "
            "VALUES (%s,%s,%s,%s)",
            ("s-http", "a-http", "v-http", "sb-http"),
        )
    state = SessionState(
        session_id="s-http", agent_id="a-http",
        sandbox_id="sb-http", agent_type="claude",
        lifecycle="live",
    )
    srv.SESSIONS["s-http"] = state
    srv._INSTANCES["sb-http"] = ProviderInstance(
        provider="daytona", url="http://fake-supervisor",
        root="/home/daytona", sandbox_id="dt-sandbox-http",
    )


@pytest.mark.asyncio
async def test_admin_reap_after_failed_hibernate_does_not_stop_provider(setup):
    """End-to-end via HTTP: hibernate→admin-reap leaves a Daytona orphan.

    Acts purely through public API endpoints — no internal helpers
    invoked by the test. Mocks only the provider's leaf stop_sandbox
    function (the same surface the prod code mocks Daytona with).

    After hibernate fails its provider stop, admin reap reports
    ``provider_stopped=null`` and the running provider sandbox stays
    orphaned. The test asserts the orphan condition the response body
    encodes — and asserts the underlying call counter to make the leak
    explicit.
    """
    await _seed_live_session()

    # Track every stop_sandbox call. First one fails (mirrors the 502
    # warning we see in the prod log); subsequent ones would succeed.
    stop_calls: list[str] = []
    fail_first = {"value": True}

    async def fake_stop_sandbox(instance):
        stop_calls.append(instance.sandbox_id)
        if fail_first["value"]:
            fail_first["value"] = False
            raise RuntimeError("daytona 502 — sandbox stop refused")

    transport = ASGITransport(app=srv.app)
    with contextlib.ExitStack() as stack:
        stack.enter_context(patch(
            "api.providers.daytona.stop_sandbox",
            new=AsyncMock(side_effect=fake_stop_sandbox),
        ))
        stack.enter_context(patch("api.server._cancel_task", new=AsyncMock()))
        stack.enter_context(patch("api.server._close_session_gracefully", new=AsyncMock()))
        stack.enter_context(patch("api.server._request_supervisor_snapshot", new=AsyncMock()))
        stack.enter_context(patch("api.server.kill_supervisor_in_sandbox", new=AsyncMock()))

        client = AsyncClient(transport=transport, base_url="http://test")
        await client.__aenter__()

        # Step 1: hibernate via HTTP. Mocked stop_sandbox raises once.
        # Server tolerates the failure (logs WARNING), flips DB to
        # STOPPED, and reports success. From the client's perspective
        # this looks fine.
        resp = await client.post("/sessions/s-http/hibernate")
        assert resp.status_code == 200, resp.text
        assert stop_calls == ["dt-sandbox-http"], (
            "hibernate should attempt stop exactly once"
        )

        # Re-register: hibernate auto-evicts if no subscribers attached.
        # In the bug scenario we want a SessionState present so admin
        # reap has something to reap. Pre-existing prod sessions with
        # subscribers would naturally still be in SESSIONS here.
        if "s-http" not in srv.SESSIONS:
            state = SessionState(
                session_id="s-http", agent_id="a-http",
                sandbox_id="sb-http", agent_type="claude",
                lifecycle="hibernated",
            )
            srv.SESSIONS["s-http"] = state

        # Step 2: admin reap via HTTP. This is the operator's "make
        # sure this is really gone" hammer. The response body is the
        # smoking gun.
        resp = await client.post("/admin/sessions/s-http/reap")
        assert resp.status_code == 200, resp.text
        body = resp.json()
        await client.aclose()

    # The assertion that captures the bug. After admin reap, the
    # response says "reaped" — but provider_stopped is null. That null
    # means: "I evicted the in-memory state but I never stopped (or
    # retried stopping) the actual provider sandbox." The Daytona
    # workspace is still RUNNING with no server-side memory of it.
    assert body["status"] == "reaped"
    assert body["sandbox_id"] == "sb-http"
    assert body["provider_stopped"] == "daytona", (
        "BUG via HTTP: POST /admin/sessions/.../reap returned "
        f"provider_stopped={body['provider_stopped']!r} — meaning the "
        "endpoint never retried stop_sandbox on the provider, even "
        "though hibernate's prior attempt failed (see preceding "
        "WARNING). Net effect: orphan sandbox in Daytona that the "
        f"server has zero record of. stop_calls={stop_calls!r}"
    )
    assert len(stop_calls) >= 2, (
        "Reap path should retry stop_sandbox after hibernate's failed "
        "attempt; observed only %d call(s): %r" % (len(stop_calls), stop_calls)
    )
