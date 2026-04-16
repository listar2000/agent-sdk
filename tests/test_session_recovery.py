"""Tests for session auto-recovery after idle reaper cleanup.

The idle reaper removes sessions from the in-memory SESSIONS dict but
leaves the DB record and (for Daytona) the sandbox intact. Session
endpoints must auto-recover by looking up the session in the DB and
calling _do_resume to rebuild in-memory state.

These tests FAIL against the buggy code (which just returns 404) and
PASS once the auto-recovery fix is applied.
"""

import asyncio
import json
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
        async def fake_do_resume(
            *,
            sandbox_id,
            agent_id,
            inner_session_id,
            client_session_id,
            force_replace_live_state=False,
        ):
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

        async def fake_do_resume(
            *,
            sandbox_id,
            agent_id,
            inner_session_id,
            client_session_id,
            force_replace_live_state=False,
        ):
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

    @pytest.mark.asyncio
    async def test_stale_live_session_forces_recovery(self):
        """A stale in-memory session must not bypass the recovery path."""
        from api.server import get_or_recover_session

        session_id = "sess-stale-live"
        sandbox_id = "sandbox-stale-live"
        agent_id = "agent-stale-live"
        inner_session_id = "inner-stale-live"

        stale_state = _fake_session_state(session_id, agent_id, sandbox_id, inner_session_id)
        stale_state.client.base_url = "https://old-daytona-url.example.com"
        stale_state._reader_task = asyncio.create_task(asyncio.sleep(3600))
        stale_state._scheduler_task = asyncio.create_task(asyncio.sleep(3600))
        stale_state._session_subscribers.append(asyncio.Queue())
        SESSIONS[session_id] = stale_state

        db_record = {
            "id": session_id,
            "agent_id": agent_id,
            "sandbox_id": sandbox_id,
            "inner_session_id": inner_session_id,
        }
        agent_record = AgentRecord(
            id=agent_id,
            name="agent",
            config=AgentConfig(agent_type="claude", cwd="/tmp"),
        )

        async def fake_initialize(client, config, acp_session_id, cwd):
            client.set_inner_session_id(acp_session_id, "inner-recovered")

        with patch("api.server.get_session", AsyncMock(return_value=db_record)), \
             patch("api.server.get_agent", AsyncMock(return_value=agent_record)), \
             patch("api.server.get_sandbox", AsyncMock(return_value=SandboxRecord(
                 id=sandbox_id, provider="daytona", sandbox_ref="daytona-sbx", status="running",
             ))), \
             patch("api.server._ensure_sandbox_alive", AsyncMock(return_value=(
                 "https://new-daytona-url.example.com", True,
             ))), \
             patch("api.server._apply_config_and_initialize", side_effect=fake_initialize), \
             patch("api.server.upsert_session", AsyncMock()), \
             patch("api.server._start_session_tasks"):
            recovered = await get_or_recover_session(session_id)

        assert recovered is SESSIONS[session_id]
        assert recovered is not stale_state
        assert recovered.client is not stale_state.client
        assert recovered.client.base_url == "https://new-daytona-url.example.com"
        assert recovered.inner_session_id == "inner-recovered"
        assert stale_state.shutdown.is_set()
        assert stale_state.client is None

    @pytest.mark.asyncio
    async def test_status_endpoint_recovers_stale_live_session(self):
        """GET /sessions/{id}/status must not trust a stale live Daytona session."""
        session_id = "sess-stale-status"
        sandbox_id = "sandbox-stale-status"
        agent_id = "agent-stale-status"
        inner_session_id = "inner-stale-status"

        stale_state = _fake_session_state(session_id, agent_id, sandbox_id, inner_session_id)
        stale_state.client.base_url = "https://old-daytona-url.example.com"
        SESSIONS[session_id] = stale_state

        db_record = {
            "id": session_id,
            "agent_id": agent_id,
            "sandbox_id": sandbox_id,
            "inner_session_id": inner_session_id,
        }

        async def fake_do_resume(
            *,
            sandbox_id,
            agent_id,
            inner_session_id,
            client_session_id,
            force_replace_live_state=False,
        ):
            new_state = _fake_session_state(
                client_session_id, agent_id, sandbox_id, inner_session_id
            )
            new_state.client.base_url = "https://new-daytona-url.example.com"
            SESSIONS[client_session_id] = new_state
            return {
                "session_id": client_session_id,
                "agent_id": agent_id,
                "sandbox_id": sandbox_id,
                "inner_session_id": inner_session_id,
                "status": "resumed",
            }

        with patch("api.server.get_session", AsyncMock(return_value=db_record)), \
             patch("api.server.get_sandbox", AsyncMock(return_value=SandboxRecord(
                 id=sandbox_id, provider="daytona", sandbox_ref="daytona-sbx", status="running",
             ))), \
             patch("api.server._ensure_sandbox_alive", AsyncMock(return_value=(
                 "https://new-daytona-url.example.com", False,
             ))), \
             patch("api.server._do_resume", side_effect=fake_do_resume) as mock_resume:
            transport = ASGITransport(app=app)
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                resp = await client.get(f"/sessions/{session_id}/status")

        assert resp.status_code == 200
        assert resp.json()["session_id"] == session_id
        mock_resume.assert_awaited_once()
        assert SESSIONS[session_id] is not stale_state


class TestSandboxResolution:
    """Sandbox resolution must not trust stale Daytona preview URLs."""

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
        ))), patch("api.server._ensure_sandbox_alive", AsyncMock(side_effect=fake_ensure)) as mock_ensure:
            resolved = await _resolve_sandbox_instance(sandbox_id)

        assert resolved is fresh
        mock_ensure.assert_awaited_once()


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

        async def fake_do_resume(
            *,
            sandbox_id,
            agent_id,
            inner_session_id,
            client_session_id,
            force_replace_live_state=False,
        ):
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


class TestConcurrentRecovery:
    """Two concurrent requests for the same reaped session must not 404 or 502,
    and must not double-resume — the per-session lock inside _do_resume guards this."""

    @pytest.mark.asyncio
    async def test_concurrent_requests_to_reaped_session_all_succeed(self):
        """5 concurrent POSTs to a reaped session: none should 404 or 502.

        The first request triggers _do_resume, adds session to SESSIONS.
        Subsequent concurrent requests may also enter _do_resume, but the
        lock+already_active guard ensures they return without double-work.
        All responses must be 200 or 409, never 404/502.
        """
        session_id = "sess-concurrent-1"
        sandbox_id = "sbx-concurrent-1"
        agent_id = "agt-concurrent-1"
        inner_session_id = "inner-concurrent-1"

        resume_invocations = []

        async def fake_do_resume(
            *,
            sandbox_id,
            agent_id,
            inner_session_id,
            client_session_id,
            force_replace_live_state=False,
        ):
            resume_invocations.append(client_session_id)
            # Yield to let other coroutines race in before we populate SESSIONS
            await asyncio.sleep(0)
            if client_session_id not in SESSIONS:
                new_state = _fake_session_state(client_session_id, agent_id, sandbox_id, inner_session_id)
                new_state.client.prompt = AsyncMock()
                SESSIONS[client_session_id] = new_state
            return {
                "session_id": client_session_id, "agent_id": agent_id,
                "sandbox_id": sandbox_id, "inner_session_id": inner_session_id,
                "status": "resumed",
            }

        db_record = {
            "id": session_id, "agent_id": agent_id,
            "sandbox_id": sandbox_id, "inner_session_id": inner_session_id,
        }

        with patch("api.server.get_session", AsyncMock(return_value=db_record)), \
             patch("api.server._do_resume", side_effect=fake_do_resume), \
             patch("api.server.log_event", AsyncMock()):

            transport = ASGITransport(app=app)
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                responses = await asyncio.gather(*[
                    client.post(f"/sessions/{session_id}/message",
                                json={"message": f"concurrent-{i}"})
                    for i in range(5)
                ])

        status_codes = [r.status_code for r in responses]
        bad = [s for s in status_codes if s not in (200, 409)]
        assert not bad, (
            f"concurrent recovery produced unexpected status codes: {status_codes}\n"
            f"_do_resume was called {len(resume_invocations)} times"
        )


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
