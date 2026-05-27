"""End-to-end behavior test for ``Agent.reload(mcp_servers=...)``.

Cold-create a session with NO MCP servers. Reload with the well-known
reference MCP server ``@modelcontextprotocol/server-everything`` (which
exposes an ``echo`` tool). Ask the agent to invoke that tool with a
known input. Assert the session log contains a ``tool_call`` event for
``echo`` carrying our input.

This validates TWO load-bearing pieces in one shot:
  1. ``POST /reload`` rewrote ``agents.config.mcp_servers``.
  2. The subsequent ``release`` + ``cold_recover`` cycle re-attached
     ACP with the new MCP set — via the dead-code fix at
     ``api/sandbox/session.py:_attach_acp`` that now reads
     ``agent.config.mcp_servers`` and threads it through ``client.attach``.

Without (2), the MCP would land in ``agents.config`` but the live ACP
session would never see it; the tool call would not occur.

Requires ``DAYTONA_API_KEY`` + ``CLAUDE_CODE_OAUTH_TOKEN``; opencode is
skipped — its MCP wiring goes through a different path on session/new
and is covered by opencode's own tests.

Run::

    .venv/bin/python -m pytest tests/test_mcp_hot_reload.py -n auto -v -s
"""
from __future__ import annotations

import asyncio
import os
import sys
import uuid

import httpx
import pytest

_SRC = os.path.join(os.path.dirname(__file__), "..", "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

from agent_sdk import ApiClient, Agent  # noqa: E402

DAYTONA_API_KEY = os.environ.get("DAYTONA_API_KEY")
OAUTH_TOKEN = os.environ.get("CLAUDE_CODE_OAUTH_TOKEN")
SERVER = os.environ.get("AGENT_SERVER_URL", "http://localhost:7778")

pytestmark = pytest.mark.skipif(
    not (DAYTONA_API_KEY and OAUTH_TOKEN),
    reason="DAYTONA_API_KEY + CLAUDE_CODE_OAUTH_TOKEN required",
)


async def _server_up() -> bool:
    try:
        async with httpx.AsyncClient(timeout=2) as c:
            r = await c.get(f"{SERVER}/health")
            return r.status_code == 200
    except Exception:
        return False


# The reference MCP server. ``server-everything`` ships an ``echo`` tool
# that returns its input verbatim — perfect for round-trip verification.
_MCP_EVERYTHING = {
    "type": "stdio",
    "command": "npx",
    "args": ["-y", "@modelcontextprotocol/server-everything"],
}


def _saw_echo_tool_call(events: list[dict], needle: str) -> bool:
    """Walk the session log looking for a tool_call carrying ``needle``.

    The agent may have called ``echo`` with the exact needle, or the
    tool might have surfaced under a vendor-prefixed name
    (``mcp__everything__echo`` etc.). Match on (a) the tool name
    containing ``echo`` AND (b) the args/payload containing ``needle``.
    Belt-and-suspenders: also accept a ``tool_result`` event with the
    needle in its content, since some ACP wrappers don't emit
    ``tool_call`` consistently.
    """
    for ev in events:
        et = ev.get("event_type") or ev.get("type") or ""
        payload = ev.get("payload") or {}
        serialized = repr(ev).lower()
        if et == "tool_call":
            name = (payload.get("tool") or "").lower()
            if "echo" in name and needle.lower() in serialized:
                return True
        if et == "tool_result" and needle.lower() in serialized:
            return True
    return False


@pytest.mark.asyncio
async def test_reload_hot_attaches_mcp_server_and_tool_is_callable():
    if not await _server_up():
        pytest.skip(f"no server at {SERVER}")

    needle = f"reload-mcp-{uuid.uuid4().hex[:8]}"

    agent = Agent(
        f"reload-mcp-{uuid.uuid4().hex[:8]}",
        provider="daytona",
        agent_type="claude",
        model="haiku",
        api_url=SERVER,
        # IMPORTANT: no mcp_servers at create time — the whole point is
        # that /reload hot-attaches them on the next session/load.
        oauth_token=OAUTH_TOKEN,
    )
    sdk = ApiClient(SERVER)
    try:
        await agent.configure(model="haiku")
        sid = agent.session_id
        assert sid and agent.sandbox_ref

        # ── Hot-attach MCP via /reload ──────────────────────────────────
        result = await asyncio.wait_for(
            agent.reload(mcp_servers={"everything": _MCP_EVERYTHING}),
            timeout=300,
        )
        assert result["status"] == "ok"
        assert result["mcp_servers"] == {"everything": _MCP_EVERYTHING}
        assert agent.mcp_servers == {"everything": _MCP_EVERYTHING}

        # ── Drive the agent to call the echo tool ───────────────────────
        # Two-step prompt: tell the agent EXACTLY what to do. Claude
        # under ``bypassPermissions`` should pick up the MCP-exposed
        # echo tool and invoke it without further prompting.
        prompt = (
            f"You have an MCP server called 'everything' attached. It "
            f"exposes a tool that echoes its input. Please call the "
            f"echo tool with the exact string {needle!r} and then "
            f"reply with whatever the tool returned. Use the tool — "
            f"do not just type the string back."
        )
        reply = await asyncio.wait_for(agent.arun(prompt), timeout=180)

        # The agent's text reply almost certainly contains the needle
        # (it would have to, to be useful). But the LOAD-BEARING
        # assertion is the tool_call event in the session log — that's
        # what proves the MCP attached for real.
        events = await sdk.get_session_log(sid, limit=500)
        assert _saw_echo_tool_call(events, needle), (
            f"MCP echo tool was NOT invoked. Either /reload didn't "
            f"persist mcp_servers, or _attach_acp didn't thread it "
            f"into client.attach (the dead-code fix). Agent reply:\n"
            f"{reply!r}\n\nSession log (last 10 events):\n"
            f"{events[-10:]}"
        )
    finally:
        try:
            if agent.session_id:
                await sdk.delete_session(agent.session_id)
        except Exception:
            pass
        await agent.aclose()
        await sdk.close()
