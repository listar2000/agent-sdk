"""Tests for session auto-recovery after idle reaper cleanup.

The idle reaper removes sessions from the in-memory SESSIONS dict but
leaves the DB record and (for Daytona) the sandbox intact. Session
endpoints must auto-recover by looking up the session in the DB and
calling _do_resume to rebuild in-memory state.

These tests FAIL against the buggy code (which just returns 404) and
PASS once the auto-recovery fix is applied.
"""

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from httpx import ASGITransport, AsyncClient

import os
os.environ.setdefault("DATABASE_URL", "sqlite:///test_recovery.db")

from api.server import app, SESSIONS, _INSTANCES
from api.models import SessionState


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
    return SessionState(
        session_id=session_id,
        agent_id=agent_id,
        sandbox_id=sandbox_id,
        acp_session_id="acp-1",
        inner_session_id=inner_session_id,
        client=client,
    )


class TestSessionRecoveryAfterReap:
    """Simulate the exact production failure: reaper removes session from
    SESSIONS, then a new request arrives with the old session_id.

    The correct behavior is auto-recovery (look up DB, restart sandbox,
    rebuild session). The buggy behavior is an immediate 404.
    """

    @pytest.mark.asyncio
    async def test_message_endpoint_recovers_reaped_session(self):
        """POST /sessions/{id}/message must auto-recover a reaped session.

        Reproduces the production bug: heartbeat dispatches agent, sandbox
        goes idle for 5min, reaper removes session from SESSIONS, next
        mention hits 404 instead of recovering.
        """
        session_id = "sess-recovery-1"
        sandbox_id = "sandbox-recovery-1"
        agent_id = "agent-recovery-1"
        inner_session_id = "inner-recovery-1"

        # Session is NOT in SESSIONS (reaper removed it)
        assert SESSIONS.get(session_id) is None

        # DB still has the record (reaper doesn't delete from DB)
        db_record = {
            "id": session_id,
            "agent_id": agent_id,
            "sandbox_id": sandbox_id,
            "inner_session_id": inner_session_id,
        }

        # _do_resume would restart sandbox and rebuild session state
        async def fake_do_resume(*, sandbox_id, agent_id, inner_session_id, client_session_id):
            state = _fake_session_state(client_session_id, agent_id, sandbox_id, inner_session_id)
            state.client.prompt = AsyncMock()
            SESSIONS[client_session_id] = state
            return {
                "session_id": client_session_id,
                "agent_id": agent_id,
                "sandbox_id": sandbox_id,
                "inner_session_id": inner_session_id,
                "status": "resumed",
            }

        with patch("api.server.get_session", AsyncMock(return_value=db_record)), \
             patch("api.server._do_resume", side_effect=fake_do_resume), \
             patch("api.server.log_event", AsyncMock()):

            transport = ASGITransport(app=app)
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                resp = await client.post(
                    f"/sessions/{session_id}/message",
                    json={"message": "hello"},
                )

            assert resp.status_code == 200, (
                f"BUG: Got {resp.status_code} instead of 200. "
                f"The message endpoint returned 404 for a reaped session instead of "
                f"auto-recovering from the DB. Response: {resp.text}"
            )

    @pytest.mark.asyncio
    async def test_events_endpoint_recovers_reaped_session(self):
        """GET /sessions/{id}/events must auto-recover a reaped session."""
        session_id = "sess-events-recovery"
        sandbox_id = "sandbox-events-recovery"

        db_record = {
            "id": session_id,
            "agent_id": "agent-1",
            "sandbox_id": sandbox_id,
            "inner_session_id": "inner-1",
        }

        async def fake_do_resume(*, sandbox_id, agent_id, inner_session_id, client_session_id):
            state = _fake_session_state(client_session_id, agent_id, sandbox_id, inner_session_id)
            SESSIONS[client_session_id] = state
            return {"session_id": client_session_id, "status": "resumed"}

        with patch("api.server.get_session", AsyncMock(return_value=db_record)), \
             patch("api.server._do_resume", side_effect=fake_do_resume):

            transport = ASGITransport(app=app)
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                resp = await client.get(
                    f"/sessions/{session_id}/events",
                )

            assert resp.status_code != 404, (
                "BUG: Events endpoint returned 404 for a reaped session instead of "
                "auto-recovering from the DB."
            )


class TestReaperThenResumeIntegration:
    """End-to-end simulation of the reaper → new message cycle."""

    @pytest.mark.asyncio
    async def test_full_reaper_cycle(self):
        """Active session → reaper cleans up → new message must auto-recover.

        This is the exact sequence that breaks in production:
        1. Agent registers, gets session in SESSIONS
        2. Agent handles mention, goes idle
        3. 5 minutes pass, reaper removes session from SESSIONS
        4. New mention arrives, SDK sends message with old session_id
        5. Server should recover, not 404
        """
        session_id = "sess-full-cycle"
        sandbox_id = "sandbox-full-cycle"
        agent_id = "agent-full-cycle"
        inner_session_id = "inner-full-cycle"

        # Step 1: Session is active
        state = _fake_session_state(session_id, agent_id, sandbox_id, inner_session_id)
        state.client.prompt = AsyncMock()
        SESSIONS[session_id] = state

        # Step 2: Reaper removes it
        del SESSIONS[session_id]
        assert SESSIONS.get(session_id) is None

        # Step 3: New message arrives — should auto-recover
        db_record = {
            "id": session_id,
            "agent_id": agent_id,
            "sandbox_id": sandbox_id,
            "inner_session_id": inner_session_id,
        }

        async def fake_do_resume(*, sandbox_id, agent_id, inner_session_id, client_session_id):
            new_state = _fake_session_state(client_session_id, agent_id, sandbox_id, inner_session_id)
            new_state.client.prompt = AsyncMock()
            SESSIONS[client_session_id] = new_state
            return {
                "session_id": client_session_id,
                "agent_id": agent_id,
                "sandbox_id": sandbox_id,
                "inner_session_id": inner_session_id,
                "status": "resumed",
            }

        with patch("api.server.get_session", AsyncMock(return_value=db_record)), \
             patch("api.server._do_resume", side_effect=fake_do_resume), \
             patch("api.server.log_event", AsyncMock()):

            transport = ASGITransport(app=app)
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                resp = await client.post(
                    f"/sessions/{session_id}/message",
                    json={"message": "continue working"},
                )

            assert resp.status_code == 200, (
                f"BUG: Full reaper cycle failed with {resp.status_code}. "
                f"After idle reaper removed the session from memory, the next message "
                f"should have auto-recovered from the DB record, but instead got 404. "
                f"Response: {resp.text}"
            )
            assert session_id in SESSIONS, "Session should be back in SESSIONS after recovery"
            recovered = SESSIONS[session_id]
            assert recovered.sandbox_id == sandbox_id
            assert recovered.inner_session_id == inner_session_id


class TestNoRecoveryNeeded:
    """Cases where recovery should NOT be attempted."""

    @pytest.mark.asyncio
    async def test_active_session_skips_recovery(self):
        """If session is already in SESSIONS, just use it directly."""
        session_id = "sess-active"
        sandbox_id = "sandbox-active"

        state = _fake_session_state(session_id, "agent-1", sandbox_id)
        state.client.prompt = AsyncMock()
        SESSIONS[session_id] = state

        mock_get_session = AsyncMock()

        with patch("api.server.get_session", mock_get_session), \
             patch("api.server.log_event", AsyncMock()):

            transport = ASGITransport(app=app)
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                resp = await client.post(
                    f"/sessions/{session_id}/message",
                    json={"message": "hello"},
                )

            assert resp.status_code == 200
            mock_get_session.assert_not_called()

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
