"""Tests for session auto-recovery after idle reaper cleanup.

The idle reaper removes sessions from the in-memory SESSIONS dict but leaves
the DB session row (and typically the sandbox) intact. Session endpoints must
transparently re-provision by walking the ``ensure_sandbox`` / ``ensure_runtime``
path (both composed by ``ensure_session_live``). These tests verify that the
POST /message and GET /events endpoints trigger that path, rather than
short-circuiting to 404.

History note: these tests used to patch ``api.server._do_resume``. That helper
was removed as part of the volume-refactor — the ensure_* API replaces it.
Each test now patches ``api.server.ensure_sandbox`` and
``api.server.ensure_runtime`` directly to verify the endpoint triggers the
new recovery path once per call.
"""

import asyncio
import os
import sys
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from httpx import ASGITransport, AsyncClient

os.environ.setdefault("DATABASE_URL", "sqlite:///test_recovery.db")

_SRC = os.path.join(os.path.dirname(__file__), "..", "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

from api.server import app, SESSIONS, _INSTANCES
from api.models import AgentConfig, AgentRecord, SessionState, SandboxRecord
from api.providers import ProviderInstance


@pytest.fixture(autouse=True)
def clean_state():
    """Clear in-memory state before and after each test."""
    SESSIONS.clear()
    _INSTANCES.clear()
    yield
    SESSIONS.clear()
    _INSTANCES.clear()


def _fake_session_state(session_id, agent_id, sandbox_id, inner_session_id="inner-1"):
    """Create a minimal SessionState for testing."""
    client = MagicMock()
    client.close_session = AsyncMock()
    client.aclose = AsyncMock()
    client.prompt = AsyncMock()
    return SessionState(
        session_id=session_id,
        agent_id=agent_id,
        sandbox_id=sandbox_id,
        acp_session_id="acp-1",
        inner_session_id=inner_session_id,
        client=client,
    )


def _fake_sandbox_record(sandbox_id: str) -> SandboxRecord:
    return SandboxRecord(
        id=sandbox_id,
        provider="local",
        sandbox_ref="9999",
        status="running",
        volume_id="vol-1",
        subpath="agents/a/home",
        listen_port=9999,
    )


class TestSessionRecoveryAfterReap:
    """Simulate the exact production failure: reaper removes session from
    SESSIONS, then a new request arrives with the old session_id.

    Expected behavior: the endpoint calls ``ensure_session_live`` which in
    turn calls ``ensure_sandbox`` (which re-provisions if ``current_sandbox_id``
    is absent or the sandbox row is gone) and ``ensure_runtime`` (which
    rebuilds in-memory state). We patch both and verify they were called.
    """

    @pytest.mark.asyncio
    async def test_message_endpoint_recovers_reaped_session(self):
        """POST /sessions/{id}/message must re-provision a reaped session.

        Under the new ensure_* API, POST /message calls ensure_session_live,
        which calls ensure_sandbox + ensure_runtime. Both must be invoked
        exactly once; the endpoint must return 200 instead of 404.
        """
        session_id = "sess-recovery-1"
        sandbox_id = "sandbox-recovery-1"
        agent_id = "agent-recovery-1"
        inner_session_id = "inner-recovery-1"

        # Session is NOT in SESSIONS (reaper removed it).
        assert SESSIONS.get(session_id) is None

        # DB still has the row (reaper doesn't delete from DB).
        db_record = {
            "id": session_id,
            "agent_id": agent_id,
            "current_sandbox_id": None,  # reaped; recovery re-provisions
            "inner_session_id": inner_session_id,
        }
        fake_sb = _fake_sandbox_record(sandbox_id)

        async def fake_ensure_runtime(session_row, sandbox):
            state = _fake_session_state(
                session_row["id"], agent_id, sandbox.id, inner_session_id
            )
            SESSIONS[session_row["id"]] = state
            return state

        mock_ensure_sandbox = AsyncMock(return_value=fake_sb)
        mock_ensure_runtime = AsyncMock(side_effect=fake_ensure_runtime)

        with patch("api.server.get_session", AsyncMock(return_value=db_record)), \
             patch("api.server.ensure_sandbox", mock_ensure_sandbox), \
             patch("api.server.ensure_runtime", mock_ensure_runtime), \
             patch("api.server.log_event", AsyncMock()):

            transport = ASGITransport(app=app)
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                resp = await client.post(
                    f"/sessions/{session_id}/message",
                    json={"message": "hello"},
                )

        assert resp.status_code == 200, (
            f"Expected 200 after auto-recovery; got {resp.status_code}: {resp.text}"
        )
        mock_ensure_sandbox.assert_awaited_once()
        mock_ensure_runtime.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_events_endpoint_recovers_reaped_session(self):
        """GET /sessions/{id}/events must trigger the ensure_* path too."""
        session_id = "sess-events-recovery"
        sandbox_id = "sandbox-events-recovery"

        db_record = {
            "id": session_id,
            "agent_id": "agent-1",
            "current_sandbox_id": None,
            "inner_session_id": "inner-1",
        }
        fake_sb = _fake_sandbox_record(sandbox_id)

        async def fake_ensure_runtime(session_row, sandbox):
            state = _fake_session_state(session_row["id"], "agent-1", sandbox.id, "inner-1")
            SESSIONS[session_row["id"]] = state
            return state

        mock_ensure_sandbox = AsyncMock(return_value=fake_sb)
        mock_ensure_runtime = AsyncMock(side_effect=fake_ensure_runtime)

        with patch("api.server.get_session", AsyncMock(return_value=db_record)), \
             patch("api.server.ensure_sandbox", mock_ensure_sandbox), \
             patch("api.server.ensure_runtime", mock_ensure_runtime):

            transport = ASGITransport(app=app)
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                # Stream so the server can return headers without us consuming the body.
                async with client.stream(
                    "GET", f"/sessions/{session_id}/events"
                ) as resp:
                    # Any non-404 status means the ensure_* path ran.
                    assert resp.status_code != 404, (
                        "Events endpoint returned 404 for a reaped session "
                        "instead of re-provisioning via ensure_sandbox."
                    )

        mock_ensure_sandbox.assert_awaited()


class TestReaperThenResumeIntegration:
    """End-to-end simulation of the reaper to new-message cycle."""

    @pytest.mark.asyncio
    async def test_full_reaper_cycle(self):
        """Active session -> reaper cleans up -> new message re-provisions.

        The exact sequence that breaks in production:
        1. Agent registers, gets session in SESSIONS
        2. Agent handles mention, goes idle
        3. 5 minutes pass, reaper removes session from SESSIONS
        4. New mention arrives, SDK sends message with old session_id
        5. Server must walk the ensure_* path and return 200
        """
        session_id = "sess-full-cycle"
        sandbox_id = "sandbox-full-cycle"
        agent_id = "agent-full-cycle"
        inner_session_id = "inner-full-cycle"

        # Step 1: Session was active.
        state = _fake_session_state(session_id, agent_id, sandbox_id, inner_session_id)
        SESSIONS[session_id] = state

        # Step 2: Reaper removes it.
        del SESSIONS[session_id]
        assert SESSIONS.get(session_id) is None

        # Step 3: New message arrives — should re-provision via ensure_*.
        db_record = {
            "id": session_id,
            "agent_id": agent_id,
            "current_sandbox_id": None,
            "inner_session_id": inner_session_id,
        }
        fresh_sb = _fake_sandbox_record(sandbox_id)

        async def fake_ensure_runtime(session_row, sandbox):
            new_state = _fake_session_state(
                session_row["id"], agent_id, sandbox.id, inner_session_id
            )
            SESSIONS[session_row["id"]] = new_state
            return new_state

        with patch("api.server.get_session", AsyncMock(return_value=db_record)), \
             patch("api.server.ensure_sandbox", AsyncMock(return_value=fresh_sb)), \
             patch("api.server.ensure_runtime", AsyncMock(side_effect=fake_ensure_runtime)), \
             patch("api.server.log_event", AsyncMock()):

            transport = ASGITransport(app=app)
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                resp = await client.post(
                    f"/sessions/{session_id}/message",
                    json={"message": "continue working"},
                )

        assert resp.status_code == 200, (
            f"Full reaper cycle failed: {resp.status_code}. After the idle "
            f"reaper removed the session from memory, the next message should "
            f"have re-provisioned via ensure_*. Response: {resp.text}"
        )
        assert session_id in SESSIONS, (
            "Session should be back in SESSIONS after ensure_runtime ran"
        )
        recovered = SESSIONS[session_id]
        assert recovered.sandbox_id == sandbox_id
        assert recovered.inner_session_id == inner_session_id


class TestConcurrentRecovery:
    """Concurrent requests for the same reaped session must not 404 or 502.

    Under the new API, the per-session lock inside ensure_sandbox+ensure_runtime
    prevents double-provisioning. All responses must be 200 or 409.
    """

    @pytest.mark.asyncio
    async def test_concurrent_requests_to_reaped_session_all_succeed(self):
        """5 concurrent POSTs to a reaped session: none should 404 or 502."""
        session_id = "sess-concurrent-1"
        sandbox_id = "sbx-concurrent-1"
        agent_id = "agt-concurrent-1"
        inner_session_id = "inner-concurrent-1"

        db_record = {
            "id": session_id,
            "agent_id": agent_id,
            "current_sandbox_id": None,
            "inner_session_id": inner_session_id,
        }
        fake_sb = _fake_sandbox_record(sandbox_id)

        async def fake_ensure_runtime(session_row, sandbox):
            # Yield to let other coroutines race in before we populate SESSIONS.
            await asyncio.sleep(0)
            state = SESSIONS.get(session_row["id"])
            if state is None:
                state = _fake_session_state(
                    session_row["id"], agent_id, sandbox.id, inner_session_id
                )
                SESSIONS[session_row["id"]] = state
            return state

        with patch("api.server.get_session", AsyncMock(return_value=db_record)), \
             patch("api.server.ensure_sandbox", AsyncMock(return_value=fake_sb)), \
             patch("api.server.ensure_runtime", AsyncMock(side_effect=fake_ensure_runtime)), \
             patch("api.server.log_event", AsyncMock()):

            transport = ASGITransport(app=app)
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                responses = await asyncio.gather(*[
                    client.post(
                        f"/sessions/{session_id}/message",
                        json={"message": f"concurrent-{i}"},
                    )
                    for i in range(5)
                ])

        status_codes = [r.status_code for r in responses]
        bad = [s for s in status_codes if s not in (200, 409)]
        assert not bad, (
            f"concurrent recovery produced unexpected status codes: {status_codes}"
        )


class TestSandboxResolution:
    """Sandbox resolution must not trust stale Daytona preview URLs.

    ``_resolve_sandbox_instance`` still exists post-refactor and is used by the
    volume / sandbox read endpoints. If the cached ProviderInstance is stale,
    it should be refreshed through ``_ensure_sandbox_alive``.
    """

    @pytest.mark.asyncio
    async def test_resolve_sandbox_instance_refreshes_stale_daytona_instance(self):
        from api.server import _resolve_sandbox_instance

        sandbox_id = "sandbox-stale-instance"
        stale = ProviderInstance(
            provider="daytona",
            url="https://old-daytona-url.example.com",
            sandbox_id="daytona-old",
        )
        fresh = ProviderInstance(
            provider="daytona",
            url="https://new-daytona-url.example.com",
            sandbox_id="daytona-new",
        )
        _INSTANCES[sandbox_id] = stale

        async def fake_ensure(*args, **kwargs):
            _INSTANCES[sandbox_id] = fresh
            return fresh.url, False

        with patch("api.server.get_sandbox", AsyncMock(return_value=SandboxRecord(
            id=sandbox_id, provider="daytona", sandbox_ref="daytona-sbx", status="running",
        ))), patch(
            "api.server._ensure_sandbox_alive", AsyncMock(side_effect=fake_ensure)
        ) as mock_ensure:
            resolved = await _resolve_sandbox_instance(sandbox_id)

        assert resolved is fresh
        mock_ensure.assert_awaited_once()


class TestNoRecoveryNeeded:
    """Cases where recovery should NOT be attempted."""

    @pytest.mark.asyncio
    async def test_active_session_skips_recovery(self):
        """If a healthy session is in SESSIONS, ensure_runtime reuses it.

        Under the new API we can't assert ``get_session`` is never called
        (ensure_session_live always reads the row first), so the invariant
        we enforce is: ensure_runtime returns the existing SESSIONS entry.
        """
        session_id = "sess-active"
        sandbox_id = "sandbox-active"
        agent_id = "agent-active"

        state = _fake_session_state(session_id, agent_id, sandbox_id)
        SESSIONS[session_id] = state

        db_record = {
            "id": session_id,
            "agent_id": agent_id,
            "current_sandbox_id": sandbox_id,
            "inner_session_id": "inner-1",
        }
        fake_sb = _fake_sandbox_record(sandbox_id)

        with patch("api.server.get_session", AsyncMock(return_value=db_record)), \
             patch("api.server.ensure_sandbox", AsyncMock(return_value=fake_sb)), \
             patch("api.server.ensure_runtime", AsyncMock(return_value=state)), \
             patch("api.server.log_event", AsyncMock()):

            transport = ASGITransport(app=app)
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                resp = await client.post(
                    f"/sessions/{session_id}/message",
                    json={"message": "hello"},
                )

        assert resp.status_code == 200
        assert SESSIONS[session_id] is state

    @pytest.mark.asyncio
    async def test_truly_gone_session_returns_404(self):
        """If session isn't in SESSIONS or DB, 404 is correct."""
        with patch("api.server.get_session", AsyncMock(return_value=None)):
            transport = ASGITransport(app=app)
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                resp = await client.post(
                    "/sessions/gone/message",
                    json={"message": "hello"},
                )
            assert resp.status_code == 404
