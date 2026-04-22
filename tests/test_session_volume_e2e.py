"""End-to-end test: sandbox-loss resume via the session's volume.

Requires both DAYTONA_API_KEY and CLAUDE_CODE_OAUTH_TOKEN. Makes real
calls; takes several minutes.
"""
from __future__ import annotations
import os, sys, asyncio, uuid
import pytest
from httpx import ASGITransport, AsyncClient

_SRC = os.path.join(os.path.dirname(__file__), "..", "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

_DB = os.environ.get("TEST_DATABASE_URL")
_HAS_KEYS = os.environ.get("DAYTONA_API_KEY") and os.environ.get("CLAUDE_CODE_OAUTH_TOKEN")

pytestmark = [
    pytest.mark.skipif(_DB is None, reason="TEST_DATABASE_URL not set"),
    pytest.mark.skipif(not _HAS_KEYS, reason="DAYTONA_API_KEY + CLAUDE_CODE_OAUTH_TOKEN required"),
    pytest.mark.integration,
]

if _DB:
    os.environ["DATABASE_URL"] = _DB

from api import db as dbmod, server as srv  # noqa: E402


async def _wait_for_prompt_reply(client, sid, rpc_id, timeout=120):
    """Poll /sessions/{id}/status until the prompt is done, then fetch logs."""
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        r = await client.get(f"/sessions/{sid}/status")
        if r.status_code == 200:
            st = r.json()
            if not st.get("agent_busy", True) and not st.get("pending_count"):
                # Check that we have an assistant message after our prompt.
                logr = await client.get(f"/sessions/{sid}/log?limit=200")
                if logr.status_code == 200:
                    events = logr.json()
                    # Look for an assistant_message after our submission.
                    return events
        await asyncio.sleep(2)
    raise TimeoutError(f"session {sid} didn't finish prompt {rpc_id} within {timeout}s")


@pytest.mark.asyncio
async def test_sandbox_loss_resume_end_to_end():
    """Send prompt, kill sandbox, send follow-up, verify CLI resumed."""
    from api.models import AgentConfig, AgentRecord

    dbmod.init_db()
    await dbmod.init_pool()
    # Clean slate.
    async with dbmod.get_db() as conn:
        await conn.execute("DELETE FROM session_log")
        await conn.execute("DELETE FROM sessions")
        await conn.execute("DELETE FROM sandboxes")
        await conn.execute("DELETE FROM volumes")
        await conn.execute("DELETE FROM agents")

    # Agent + volume setup.
    agent_id = f"agent-e2e-{uuid.uuid4().hex[:6]}"
    await dbmod.upsert_agent(AgentRecord(
        id=agent_id,
        name="E2E agent",
        config=AgentConfig(agent_type="claude",
                           prompt="You are a helpful assistant. Keep responses short."),
    ))

    transport = ASGITransport(app=srv.app)
    try:
        async with AsyncClient(transport=transport, base_url="http://test",
                               timeout=300.0) as client:
            vol_name = f"e2e-vol-{uuid.uuid4().hex[:6]}"
            r = await client.post("/volumes/provision",
                                  json={"name": vol_name, "provider": "daytona"})
            assert r.status_code == 200, f"volume provision: {r.text}"
            vol_id = r.json()["id"]

            # Create session — pass auth token as session secret so it lands
            # inside the Daytona sandbox's supervisor process env.
            oauth_token = os.environ.get("CLAUDE_CODE_OAUTH_TOKEN", "")
            r = await client.post("/sessions",
                                  json={"agent_id": agent_id, "volume_id": vol_id,
                                        "secrets": {"CLAUDE_CODE_OAUTH_TOKEN": oauth_token}})
            assert r.status_code == 200, f"session create: {r.text}"
            sid = r.json()["id"]

            # First prompt.
            r = await client.post(f"/sessions/{sid}/message",
                                  json={"message": "Please remember the secret word OSPREY. Just say OK."})
            assert r.status_code == 200, f"message 1: {r.text}"
            events1 = await _wait_for_prompt_reply(client, sid, r.json().get("rpc_id"), timeout=180)
            assert any(e["event_type"] == "assistant_message" for e in events1), \
                f"no assistant reply after first prompt; events: {events1[:10]}"

            # Capture the sandbox id, then kill the sandbox.
            sess = await dbmod.get_session(sid)
            old_sbid = sess["current_sandbox_id"]
            assert old_sbid is not None
            inner_sid_before = sess.get("inner_session_id")
            assert inner_sid_before is not None, \
                "inner_session_id must be set after first exchange"

            # Kill it.
            r = await client.delete(f"/sandboxes/{old_sbid}")
            assert r.status_code in (200, 204), f"delete sandbox: {r.text}"

            # Force recovery by clearing in-memory state (if any).
            if sid in srv.SESSIONS:
                state = srv.SESSIONS.pop(sid)
                state.shutdown.set()

            # Second prompt — should lazily reprovision + resume.
            r = await client.post(f"/sessions/{sid}/message",
                                  json={"message": "What is the secret word?"})
            assert r.status_code == 200, f"message 2: {r.text}"
            events2 = await _wait_for_prompt_reply(client, sid, r.json().get("rpc_id"), timeout=240)

            # Verify the CLI resumed (same inner_session_id OR volume path had the transcript).
            sess2 = await dbmod.get_session(sid)
            new_sbid = sess2["current_sandbox_id"]
            assert new_sbid != old_sbid, "should have new sandbox after loss"

            # Look for OSPREY in the assistant messages (case-insensitive).
            assistant_events = [e for e in events2 if e["event_type"] == "assistant_message"]
            joined = " ".join(str(e.get("payload", "")) for e in assistant_events).lower()
            assert "osprey" in joined, (
                f"CLI should have resumed transcript — expected 'osprey' in reply, "
                f"got assistant messages: {assistant_events[-5:]}"
            )
    finally:
        # Cleanup attempt.
        try:
            async with AsyncClient(transport=transport, base_url="http://test",
                                   timeout=60.0) as client:
                await client.delete(f"/volumes/{vol_name}?force=true")
        except Exception:
            pass
        await dbmod.close_pool()
