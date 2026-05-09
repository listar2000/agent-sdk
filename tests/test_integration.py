"""Integration tests — require live Docker stack (docker compose up).

Run: pytest tests/test_integration.py -v -m integration

These tests exercise the full stack: client SDK → FastAPI server → supervisor → claude-agent-acp.
They are slow (10-60s each) and require ANTHROPIC_API_KEY in the environment.
"""
import asyncio
import json
import os
import sys
import time

import httpx
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
from agent_sdk.client import Agent
from tests._acp_runtimes import acp_runtime_param

BASE_URL = "http://localhost:7778"


def _server_reachable() -> bool:
    try:
        import httpx as _httpx
        r = _httpx.get(f"{BASE_URL}/health", timeout=3)
        return r.status_code == 200
    except Exception:
        return False


pytestmark = pytest.mark.integration

skip_if_no_server = pytest.mark.skipif(
    not _server_reachable(),
    reason="Docker server not reachable at localhost:7778"
)


@skip_if_no_server
@acp_runtime_param
@pytest.mark.asyncio
async def test_basic_arun(acp_runtime):
    """Agent can complete a simple prompt end-to-end."""
    agent = Agent("test-basic", provider="unix_local", api_url=BASE_URL, **acp_runtime)
    resp = await asyncio.wait_for(
        agent.arun("Reply with exactly: HELLO_WORLD"),
        timeout=60,
    )
    await agent.aclose()
    assert "HELLO_WORLD" in resp


@skip_if_no_server
@acp_runtime_param
@pytest.mark.asyncio
async def test_session_resume_recalls_context(acp_runtime):
    """A resumed session recalls information from the previous turn.

    Both claude-agent-acp (JSONL under ``~/.claude/projects/``) and
    opencode (SQLite at ``~/.local/share/opencode/opencode.db``) survive
    sandbox respawn so long as ``AGENT_MEMORY_DIRS`` covers the right
    paths and the config-replay loop forwards ``agent_type`` to
    ``set_model`` (otherwise full opencode IDs get squashed to
    ``"haiku"`` and parsed as ``providerID=haiku``).
    """
    import random
    num = random.randint(100, 999)  # 3-digit to avoid false positives

    agent = Agent("test-resume", provider="unix_local", api_url=BASE_URL, **acp_runtime)
    resp1 = await asyncio.wait_for(
        agent.arun(f'Remember this number: {num}. Say only "OK {num}."'),
        timeout=90,
    )
    session_id = agent.session_id
    await agent.aclose()

    # Pass secrets again on resume — the SDK only auto-forwards Claude creds
    # from env; non-Claude runtimes (opencode + OPENROUTER_API_KEY) need an
    # explicit hand-off so the respawned supervisor can authenticate.
    agent2 = Agent(
        "test-resume-2",
        session_id=session_id,
        api_url=BASE_URL,
        secrets=acp_runtime["secrets"],
    )
    resp2 = await asyncio.wait_for(
        agent2.arun("What number did I ask you to remember?"),
        timeout=90,
    )
    await agent2.aclose()

    assert str(num) in resp2, f"Expected {num} in: {resp2!r}"


@skip_if_no_server
@acp_runtime_param
@pytest.mark.asyncio
async def test_astream_yields_text_and_done(acp_runtime):
    """astream yields at least one text event and a done event."""
    agent = Agent("test-events", provider="unix_local", api_url=BASE_URL, **acp_runtime)
    events = []

    async def _collect():
        async for ev in agent.astream("Say exactly: OK"):
            events.append(ev)

    await asyncio.wait_for(_collect(), timeout=60)
    await agent.aclose()

    types = {e["type"] for e in events}
    assert "text" in types
    assert "done" in types
    full_text = "".join(e["text"] for e in events if e["type"] == "text")
    assert "OK" in full_text


# ---------------------------------------------------------------------------
# ACP event-ordering scenarios — characterization tests for claude-code's
# prompt-queueing behavior and the server's FIFO attribution rule. Each
# scenario captures raw SSE envelopes from GET /events and asserts on the
# order, tags, and stub-vs-real pattern of terminal envelopes.
# ---------------------------------------------------------------------------

def _parse_tagged_block(block: str):
    """Extract (rpc_id_tag, payload) from a tagged SSE block.

    Tag comes from the `event: rpc:<id>` line the server stamps on each block.
    Payload is the JSON-RPC envelope from the `data:` line(s).
    """
    tag = None
    data_lines = []
    for line in block.split("\n"):
        if line.startswith("event: rpc:"):
            tag = line[len("event: rpc:"):].strip()
        elif line.startswith("data: "):
            data_lines.append(line[6:])
        elif line.startswith("data:"):
            data_lines.append(line[5:])
    payload = None
    if data_lines:
        try:
            payload = json.loads("\n".join(data_lines))
        except json.JSONDecodeError:
            pass
    return tag, payload


def _is_terminal(payload: dict) -> bool:
    if not isinstance(payload, dict):
        return False
    return (
        "id" in payload
        and "result" in payload
        and isinstance(payload["result"], dict)
        and "stopReason" in payload["result"]
    )


def _is_stub(payload: dict) -> bool:
    """A terminal envelope with all-zero usage totals."""
    if not _is_terminal(payload):
        return False
    usage = payload["result"].get("usage") or {}
    return not any(
        isinstance(v, (int, float)) and v > 0 for v in usage.values()
    )


async def _capture_envelopes(session_id: str, stop: asyncio.Event,
                              envelopes: list) -> None:
    """Subscribe to GET /events and append every tagged block to the list.

    Each entry is (t_from_start, tag, payload_dict).
    """
    t_start = time.monotonic()
    async with httpx.AsyncClient(base_url=BASE_URL, timeout=None) as client:
        async with client.stream(
            "GET", f"/sessions/{session_id}/events",
            headers={"Accept": "text/event-stream"},
        ) as resp:
            buf = ""
            async for chunk in resp.aiter_text():
                if stop.is_set():
                    return
                buf += chunk
                while "\n\n" in buf:
                    block, buf = buf.split("\n\n", 1)
                    tag, payload = _parse_tagged_block(block)
                    if payload is None:
                        continue
                    envelopes.append((time.monotonic() - t_start, tag, payload))


async def _drain_inflight(session_id: str, *, timeout: float = 120.0) -> None:
    async with httpx.AsyncClient(base_url=BASE_URL, timeout=30.0) as client:
        for _ in range(int(timeout * 2)):
            info = (await client.get(f"/sessions/{session_id}/status")).json()
            if info.get("inflight_count", 0) == 0:
                return
            await asyncio.sleep(0.5)


async def _run_scenario(steps: list, agent_name: str):
    """Shared harness: spawn an Agent, run a sequence of send/wait steps,
    capture all /events envelopes, return (rpc_ids, envelopes).
    """
    agent = Agent(agent_name, provider="unix_local", agent_type="claude", api_url=BASE_URL, model="haiku")
    await agent._ensure_registered()
    session_id = agent.session_id

    envelopes: list = []
    stop = asyncio.Event()
    capture_task = asyncio.create_task(_capture_envelopes(session_id, stop, envelopes))
    await asyncio.sleep(0.5)  # let /events subscription land

    rpc_ids: list[tuple[str, str]] = []
    try:
        for kind, arg in steps:
            if kind == "wait":
                await asyncio.sleep(arg)
            elif kind == "send":
                rpc = await agent.send(arg)
                rpc_ids.append((rpc, arg[:40]))
        await _drain_inflight(session_id, timeout=120.0)
        await asyncio.sleep(0.5)
    finally:
        stop.set()
        capture_task.cancel()
        try:
            await capture_task
        except (asyncio.CancelledError, Exception):
            pass
        await agent.aclose()

    return rpc_ids, envelopes

