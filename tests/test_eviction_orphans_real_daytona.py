"""Live Daytona reproduction of the orphan-sandbox bug.

Most expensive of the three reproductions in this branch:
  - test_eviction_orphans_provider_sandbox.py   — internal helpers, mocked provider
  - test_eviction_orphans_via_http_api.py       — public HTTP, mocked provider
  - this file                                    — public HTTP, REAL Daytona sandbox

Skipped unless DAYTONA_API_KEY (and TEST_DATABASE_URL) are present.
Loads creds from ./.env or ~/.env so it runs locally without extra env wiring.

The test creates a real Daytona sandbox cheaply (alpine image, no volumes,
no supervisor), drives the hibernate→reap workflow that the orphan bug
exploits, and asks Daytona's own API at the end whether the sandbox is
still RUNNING. The final assertion is on Daytona's report, not on
internal server state — so a passing test means the cloud truly cleaned
up, and a failing test means an actual paid orphan exists in your
account.

Cleanup is in a try/finally to avoid leaving an orphan FROM THIS TEST
even when the assertion fires.
"""
from __future__ import annotations

import asyncio
import contextlib
import os
import sys

import pytest
import pytest_asyncio
from unittest.mock import AsyncMock, patch
from httpx import ASGITransport, AsyncClient

_SRC = os.path.join(os.path.dirname(__file__), "..", "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

from dotenv import load_dotenv  # noqa: E402

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))
load_dotenv(os.path.expanduser("~/.env"), override=False)

DAYTONA_API_KEY = os.environ.get("DAYTONA_API_KEY")
_DB = os.environ.get("TEST_DATABASE_URL")

pytestmark = pytest.mark.skipif(
    not (DAYTONA_API_KEY and _DB),
    reason="DAYTONA_API_KEY + TEST_DATABASE_URL required",
)
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


def _create_minimal_daytona_sandbox():
    """Spin up the cheapest possible Daytona sandbox synchronously.

    Alpine image, no volume mounts, no supervisor. Just enough that
    Daytona reports it as "started". Returns the live sandbox handle —
    the caller is responsible for tearing it down.
    """
    from daytona_sdk import (
        Daytona, DaytonaConfig, CreateSandboxFromImageParams,
    )
    daytona = Daytona(DaytonaConfig(api_key=DAYTONA_API_KEY))
    sandbox = daytona.create(
        CreateSandboxFromImageParams(
            image="alpine:latest",
            auto_stop_interval=0,
        ),
        timeout=120,
    )
    return daytona, sandbox


def _get_sandbox_state(daytona, sandbox_ref: str) -> str:
    """Return Daytona's authoritative state as a lowercase string.

    Daytona returns a ``SandboxState`` enum whose ``str()`` is
    ``"SandboxState.STARTED"`` — which is unhelpful for comparison.
    Pull ``.value`` if present, fall back to last enum component.
    """
    sb = daytona.get(sandbox_ref)
    state = getattr(sb, "state", "unknown")
    raw = getattr(state, "value", None) or str(state).rsplit(".", 1)[-1]
    return raw.lower()


@pytest.mark.asyncio
async def test_real_daytona_sandbox_orphaned_after_failed_hibernate_and_reap(setup):
    """End-to-end with REAL Daytona: bug leaves a billable orphan.

    Workflow:
      1. Create a real Daytona sandbox via the Daytona SDK (no server
         supervisor) and seed DB rows + in-memory state pointing at it.
      2. POST /sessions/{id}/hibernate via HTTP. We patch
         ``daytona.stop_sandbox`` to raise a 502 — the real Daytona
         API never receives the call, simulating the network blip we
         see in production logs.
      3. POST /admin/sessions/{id}/reap via HTTP.
      4. Ask Daytona directly: ``daytona.get(sandbox_ref).state``.
         If the bug is present, state is still "started".

    Cost: one short-lived alpine sandbox (~$0). Always cleaned up in
    finally, even on failure.
    """
    daytona, real_sandbox = await asyncio.get_running_loop().run_in_executor(
        None, _create_minimal_daytona_sandbox,
    )
    sandbox_ref = real_sandbox.id

    try:
        # Confirm Daytona reports it started before we touch anything.
        initial_state = await asyncio.get_running_loop().run_in_executor(
            None, _get_sandbox_state, daytona, sandbox_ref,
        )
        assert initial_state in {"started", "starting"}, (
            f"Daytona did not report sandbox as started: {initial_state!r}"
        )

        # Seed DB + in-memory state pointing at the real sandbox.
        await dbmod.upsert_agent(AgentRecord(
            id="a-real", name="A", config=AgentConfig(agent_type="claude"),
        ))
        await dbmod.upsert_volume(VolumeRecord(
            id="v-real", name="v", provider="daytona", provider_ref="vol-not-used",
        ))
        sb_row = SandboxRecord(
            id="sb-real", provider="daytona", sandbox_ref=sandbox_ref,
            status="running", root="/home/daytona",
            volume_id="v-real", subpath="agents/a-real",
        )
        await dbmod.upsert_sandbox(sb_row)
        async with dbmod.get_db() as conn:
            await conn.execute(
                "INSERT INTO sessions (id, agent_id, volume_id, current_sandbox_id) "
                "VALUES (%s,%s,%s,%s)",
                ("s-real", "a-real", "v-real", "sb-real"),
            )
        srv.SESSIONS["s-real"] = SessionState(
            session_id="s-real", agent_id="a-real",
            sandbox_id="sb-real", agent_type="claude",
            lifecycle="live",
        )
        srv._INSTANCES["sb-real"] = ProviderInstance(
            provider="daytona", url="http://fake-supervisor",
            root="/home/daytona", sandbox_id=sandbox_ref,
        )

        # Drive the bug through real HTTP endpoints. The first
        # stop_sandbox call simulates the prod 502 (raises). Subsequent
        # calls fall through to the REAL Daytona client so we can
        # observe whether the cloud actually got the stop. This tests
        # the full loop: failed first attempt → fix retries → real
        # Daytona stops the sandbox.
        from api.providers import daytona as _daytona_mod
        stop_calls: list[str] = []
        fail_first = {"value": True}
        real_stop_sandbox = _daytona_mod.stop_sandbox

        async def fake_stop_sandbox(instance):
            stop_calls.append(instance.sandbox_id)
            if fail_first["value"]:
                fail_first["value"] = False
                raise RuntimeError("daytona 502 — sandbox stop refused")
            return await real_stop_sandbox(instance)

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

            resp = await client.post("/sessions/s-real/hibernate")
            assert resp.status_code == 200, resp.text

            # Re-register the SessionState if hibernate auto-evicted it.
            if "s-real" not in srv.SESSIONS:
                srv.SESSIONS["s-real"] = SessionState(
                    session_id="s-real", agent_id="a-real",
                    sandbox_id="sb-real", agent_type="claude",
                    lifecycle="hibernated",
                )

            resp = await client.post("/admin/sessions/s-real/reap")
            assert resp.status_code == 200, resp.text
            reap_body = resp.json()
            await client.aclose()

        # Daytona is the source of truth. Ask the cloud directly.
        final_state_str = await asyncio.get_running_loop().run_in_executor(
            None, _get_sandbox_state, daytona, sandbox_ref,
        )

        assert final_state_str in {"stopped", "stopping", "destroyed", "deleted"}, (
            "BUG (live Daytona): server claimed reap succeeded but Daytona "
            f"still reports sandbox state={final_state_str!r}. The "
            "workspace is RUNNING with no in-memory record — paying for "
            "compute that nothing on the server side knows exists. "
            f"reap response: {reap_body!r}, stop_calls={stop_calls!r}"
        )

    finally:
        # Always destroy the real sandbox so the test doesn't itself
        # create an orphan when it fails.
        try:
            await asyncio.get_running_loop().run_in_executor(
                None, lambda: daytona.delete(real_sandbox),
            )
        except Exception:
            pass
