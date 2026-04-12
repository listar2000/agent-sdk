"""Tests for the sandbox layer: ensure_sandbox_running and get_sandbox_client.

These tests cover the two public functions extracted into src/api/sandbox.py
from the old _ensure_sandbox_alive in server.py.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from api.models import SandboxRecord
from api.sandbox import ensure_sandbox_running


# ---------------------------------------------------------------------------
# Test 1: local sandbox that is already healthy
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_returns_url_for_running_local_sandbox():
    """ensure_sandbox_running returns http://localhost:{port} for a healthy local sandbox."""
    port = "3001"
    record = SandboxRecord(id="sb-local", provider="local", sandbox_ref=port, status="running")

    with (
        patch("api.sandbox._get_sandbox", new=AsyncMock(return_value=record)),
        patch("api.sandbox._wait_for_health_fn", new=AsyncMock(return_value=True)),
    ):
        url = await ensure_sandbox_running("sb-local")

    assert url == f"http://localhost:{port}"


# ---------------------------------------------------------------------------
# Test 2: unknown sandbox raises RuntimeError
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_raises_for_unknown_sandbox():
    """ensure_sandbox_running raises RuntimeError when sandbox record is not found."""
    with patch("api.sandbox._get_sandbox", new=AsyncMock(return_value=None)):
        with pytest.raises(RuntimeError, match="not found"):
            await ensure_sandbox_running("nonexistent-id")


# ---------------------------------------------------------------------------
# Test 3: stopped daytona sandbox gets started and URL returned
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_starts_stopped_daytona_sandbox():
    """ensure_sandbox_running starts a stopped Daytona sandbox and returns signed URL."""
    daytona_ref = "daytona-abc123"
    record = SandboxRecord(
        id="sb-daytona", provider="daytona", sandbox_ref=daytona_ref, status="stopped"
    )

    # Build the mock sandbox object returned by daytona_client.get(...)
    signed = MagicMock()
    signed.url = "https://signed.example.com/preview"

    sandbox_obj = MagicMock()
    sandbox_obj.state = MagicMock()
    sandbox_obj.state.value = "stopped"  # triggers start()
    sandbox_obj.start = MagicMock()
    sandbox_obj.create_signed_preview_url = MagicMock(return_value=signed)

    daytona_client = MagicMock()
    daytona_client.get = MagicMock(return_value=sandbox_obj)

    with (
        patch("api.sandbox._get_sandbox", new=AsyncMock(return_value=record)),
        patch("api.sandbox._wait_for_health_fn", new=AsyncMock(return_value=True)),
        patch("api.providers._get_daytona_client", return_value=daytona_client),
    ):
        url = await ensure_sandbox_running("sb-daytona")

    assert signed.url in url
    sandbox_obj.start.assert_called_once()


# ---------------------------------------------------------------------------
# Tests for get_or_recover_session (Task 2)
# ---------------------------------------------------------------------------

from unittest.mock import patch, AsyncMock
from api.server import app, SESSIONS, _INSTANCES
from api.models import SessionState


def _fake_session_state(session_id, agent_id, sandbox_id, inner_session_id="inner-1"):
    from unittest.mock import MagicMock, AsyncMock
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


class TestGetOrRecoverSession:
    @pytest.fixture(autouse=True)
    def clean(self):
        SESSIONS.clear()
        yield
        SESSIONS.clear()

    @pytest.mark.asyncio
    async def test_returns_existing_session(self):
        from api.server import get_or_recover_session
        state = _fake_session_state("s1", "a1", "sb1")
        SESSIONS["s1"] = state
        result = await get_or_recover_session("s1")
        assert result is state

    @pytest.mark.asyncio
    async def test_recovers_from_db(self):
        from api.server import get_or_recover_session
        db_record = {"id": "s2", "agent_id": "a2", "sandbox_id": "sb2", "inner_session_id": "inner-2"}

        async def fake_do_resume(*, sandbox_id, agent_id, inner_session_id, client_session_id):
            state = _fake_session_state(client_session_id, agent_id, sandbox_id, inner_session_id)
            SESSIONS[client_session_id] = state
            return {"session_id": client_session_id, "status": "resumed"}

        with patch("api.server.get_session", AsyncMock(return_value=db_record)), \
             patch("api.server._do_resume", side_effect=fake_do_resume):
            result = await get_or_recover_session("s2")
            assert result.session_id == "s2"
            assert result.sandbox_id == "sb2"

    @pytest.mark.asyncio
    async def test_raises_for_unknown_session(self):
        from api.server import get_or_recover_session
        from fastapi import HTTPException
        with patch("api.server.get_session", AsyncMock(return_value=None)):
            with pytest.raises(HTTPException) as exc_info:
                await get_or_recover_session("nonexistent")
            assert exc_info.value.status_code == 404


# ---------------------------------------------------------------------------
# Tests for session endpoints (Task 3)
# ---------------------------------------------------------------------------

from httpx import ASGITransport, AsyncClient


class TestSessionEndpoints:
    @pytest.fixture(autouse=True)
    def clean(self):
        SESSIONS.clear()
        _INSTANCES.clear()
        yield
        SESSIONS.clear()
        _INSTANCES.clear()

    @pytest.mark.asyncio
    async def test_post_session_message(self):
        session_id = "sess-msg-1"
        state = _fake_session_state(session_id, "a1", "sb1")
        state.client.prompt = AsyncMock()
        SESSIONS[session_id] = state

        with patch("api.server.log_event", AsyncMock()):
            transport = ASGITransport(app=app)
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                resp = await client.post(
                    f"/sessions/{session_id}/message",
                    json={"message": "hello"},
                )
            assert resp.status_code == 200
            data = resp.json()
            assert data["status"] == "ok"
            assert "rpc_id" in data

    @pytest.mark.asyncio
    async def test_post_session_message_recovers_reaped(self):
        session_id = "sess-msg-recover"
        db_record = {
            "id": session_id, "agent_id": "a1",
            "sandbox_id": "sb1", "inner_session_id": "inner-1",
        }

        async def fake_do_resume(*, sandbox_id, agent_id, inner_session_id, client_session_id):
            state = _fake_session_state(client_session_id, agent_id, sandbox_id, inner_session_id)
            state.client.prompt = AsyncMock()
            SESSIONS[client_session_id] = state
            return {"session_id": client_session_id, "status": "resumed"}

        with patch("api.server.get_session", AsyncMock(return_value=db_record)), \
             patch("api.server._do_resume", side_effect=fake_do_resume), \
             patch("api.server.log_event", AsyncMock()):
            transport = ASGITransport(app=app)
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                resp = await client.post(
                    f"/sessions/{session_id}/message",
                    json={"message": "hello"},
                )
            assert resp.status_code == 200

    @pytest.mark.asyncio
    async def test_session_message_404_for_nonexistent(self):
        with patch("api.server.get_session", AsyncMock(return_value=None)):
            transport = ASGITransport(app=app)
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                resp = await client.post(
                    "/sessions/nonexistent/message",
                    json={"message": "hello"},
                )
            assert resp.status_code == 404

    @pytest.mark.asyncio
    async def test_post_session_cancel(self):
        session_id = "sess-cancel-1"
        state = _fake_session_state(session_id, "a1", "sb1")
        state.client.cancel_prompt = AsyncMock()
        SESSIONS[session_id] = state

        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.post(f"/sessions/{session_id}/cancel")
        assert resp.status_code == 200


# ---------------------------------------------------------------------------
# Tests for sandbox endpoints without session (Task 4)
# ---------------------------------------------------------------------------

class TestSandboxEndpointsNoSession:
    @pytest.fixture(autouse=True)
    def clean(self):
        SESSIONS.clear()
        _INSTANCES.clear()
        yield
        SESSIONS.clear()
        _INSTANCES.clear()

    @pytest.mark.asyncio
    async def test_fs_list_without_session(self):
        mock_client = MagicMock()
        mock_client.list_dir = AsyncMock(return_value=[{"name": "test.txt", "entryType": "file"}])

        with patch("api.server.get_sandbox_client", AsyncMock(return_value=mock_client)):
            transport = ASGITransport(app=app)
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                resp = await client.get("/sandboxes/sb-1/fs", params={"path": "/"})
            assert resp.status_code == 200

    @pytest.mark.asyncio
    async def test_exec_without_session(self):
        mock_client = MagicMock()
        mock_client.run_command = AsyncMock(return_value={"exitCode": 0, "stdout": "hello", "stderr": ""})

        with patch("api.server.get_sandbox_client", AsyncMock(return_value=mock_client)):
            transport = ASGITransport(app=app)
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                resp = await client.post("/sandboxes/sb-1/exec", json={"command": "echo", "args": ["hello"]})
            assert resp.status_code == 200

    @pytest.mark.asyncio
    async def test_health_without_session(self):
        mock_client = MagicMock()
        mock_client.health = AsyncMock(return_value={"status": "ok"})

        with patch("api.server.get_sandbox_client", AsyncMock(return_value=mock_client)):
            transport = ASGITransport(app=app)
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                resp = await client.get("/sandboxes/sb-1/health")
            assert resp.status_code == 200


class TestSandboxLifecycleEndpoints:

    @pytest.mark.asyncio
    async def test_stop_sandbox(self):
        record = SandboxRecord(id="sb-stop", provider="daytona", sandbox_ref="daytona-1", status="running")

        with (
            patch("api.server.get_sandbox", AsyncMock(return_value=record)),
            patch("api.server._get_sandbox_lock", MagicMock()),
            patch("api.server._INSTANCES", {"sb-stop": MagicMock()}),
            patch("api.server.stop_daytona", AsyncMock()),
            patch("api.server.upsert_sandbox", AsyncMock()),
        ):
            transport = ASGITransport(app=app)
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                resp = await client.post("/sandboxes/sb-stop/stop")
            assert resp.status_code == 200
            assert resp.json()["status"] == "stopped"

    @pytest.mark.asyncio
    async def test_start_sandbox(self):
        record = SandboxRecord(id="sb-start", provider="daytona", sandbox_ref="daytona-2", status="stopped")

        with (
            patch("api.server.get_sandbox", AsyncMock(return_value=record)),
            patch("api.server.ensure_sandbox_running", AsyncMock(return_value="https://example.com")),
            patch("api.server.upsert_sandbox", AsyncMock()),
        ):
            transport = ASGITransport(app=app)
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                resp = await client.post("/sandboxes/sb-start/start")
            assert resp.status_code == 200
            assert resp.json()["status"] == "running"
            assert resp.json()["url"] == "https://example.com"

    @pytest.mark.asyncio
    async def test_stop_nonexistent(self):
        with patch("api.server.get_sandbox", AsyncMock(return_value=None)):
            transport = ASGITransport(app=app)
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                resp = await client.post("/sandboxes/nonexistent/stop")
            assert resp.status_code == 404

    @pytest.mark.asyncio
    async def test_start_nonexistent(self):
        with patch("api.server.get_sandbox", AsyncMock(return_value=None)):
            transport = ASGITransport(app=app)
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                resp = await client.post("/sandboxes/nonexistent/start")
            assert resp.status_code == 404


class TestSDKClientEndpoints:
    def test_registration_uses_sessions_quick(self):
        import inspect
        from agent_sdk.client import Agent
        source = inspect.getsource(Agent._ensure_registered)
        assert "/sessions/quick" in source
        assert "/agents/quick" not in source

    def test_submit_message_uses_sessions(self):
        import inspect
        from agent_sdk.client import Agent
        source = inspect.getsource(Agent._submit_message)
        assert "/sessions/" in source
        assert "/sandboxes/" not in source

    def test_astream_uses_sessions_events(self):
        import inspect
        from agent_sdk.client import Agent
        # Check either astream or _sse_stream for the SSE URL
        for method_name in ['astream', '_sse_stream']:
            if hasattr(Agent, method_name):
                source = inspect.getsource(getattr(Agent, method_name))
                if "/events" in source:
                    assert "/sessions/" in source
                    break

