"""Session-log persistence under the SessionPool flow.

After PR-D5 deleted the legacy persistent SSE-reader (which used to write
``session_log`` rows via _process_sse_block → _schedule_log → log_event),
persistence now lives in ``server._persist_prompt_events``: one row per
event yielded by ``SandboxSession.execute_prompt``.

These tests fire a real prompt that triggers a tool call and assert that
``GET /sessions/{id}/log`` returns rows for the event types the dashboard
and SDK depend on. Run against the live test server (``scripts/launch_server_test.sh``).
"""
from __future__ import annotations

import asyncio
import json
import os
import time

import httpx
import pytest

SERVER = os.environ.get("AGENT_SERVER_URL", "http://localhost:7778")

# Skip the whole module if no test server is reachable. Keeps the suite
# green in environments without a live local server (e.g. CI lanes that
# only run unit tests).
pytestmark = pytest.mark.asyncio


async def _server_up() -> bool:
    try:
        async with httpx.AsyncClient(timeout=2) as c:
            await c.get(f"{SERVER}/admin/sessions")
        return True
    except Exception:
        return False


async def _create_session(c: httpx.AsyncClient) -> str:
    r = await c.post(f"{SERVER}/sessions", json={
        "provider": "local", "agent_type": "claude", "config": {},
    })
    r.raise_for_status()
    return r.json()["session_id"]


async def _await_done_via_events(
    c: httpx.AsyncClient, sid: str, rpc_id: str, timeout: float = 90.0
) -> None:
    """Drain GET /events until the done envelope for ``rpc_id`` arrives."""
    deadline = time.time() + timeout
    async with c.stream(
        "GET", f"{SERVER}/sessions/{sid}/events",
        headers={"Accept": "text/event-stream"},
    ) as resp:
        resp.raise_for_status()
        buf = ""
        async for chunk in resp.aiter_text():
            buf += chunk
            while "\n\n" in buf:
                block, buf = buf.split("\n\n", 1)
                if rpc_id in block and ('"stopReason"' in block
                                         or '"type":"done"' in block):
                    return
            if time.time() > deadline:
                raise TimeoutError(f"timeout after {timeout}s")


async def _drain_message_stream(
    c: httpx.AsyncClient, sid: str, message: str, timeout: float = 90.0
) -> None:
    """POST /message+stream and drain until done."""
    deadline = time.time() + timeout
    async with c.stream(
        "POST", f"{SERVER}/sessions/{sid}/message+stream",
        json={"message": message},
        headers={"Accept": "text/event-stream"},
        timeout=timeout + 5,
    ) as resp:
        resp.raise_for_status()
        buf = ""
        async for chunk in resp.aiter_text():
            buf += chunk
            while "\n\n" in buf:
                block, buf = buf.split("\n\n", 1)
                if '"stopReason"' in block or '"type":"done"' in block:
                    return
            if time.time() > deadline:
                raise TimeoutError(f"timeout after {timeout}s")


def _count_types(rows: list[dict]) -> dict[str, int]:
    out: dict[str, int] = {}
    for row in rows:
        t = row.get("event_type", "?")
        out[t] = out.get(t, 0) + 1
    return out


_TOOL_PROMPT = "Run 'echo persistence-check' using the Bash tool. Reply with one short sentence."

# Event types that any tool-using turn must produce, regardless of which
# specific ACP update kinds the model emits along the way. The persister
# is a thin pass-through, so missing rows here means the loop dropped
# events between supervisor → execute_prompt → log_event.
_REQUIRED_TYPES = {"user_message", "tool_call", "tool_result", "turn_end"}


async def test_post_message_persists_session_log():
    """POST /sessions/{id}/message → events flow through execute_prompt
    in the background → all events for the prompt land in session_log."""
    if not await _server_up():
        pytest.skip(f"no server at {SERVER}")

    async with httpx.AsyncClient(timeout=120) as c:
        sid = await _create_session(c)
        r = await c.post(
            f"{SERVER}/sessions/{sid}/message", json={"message": _TOOL_PROMPT},
        )
        r.raise_for_status()
        rpc_id = r.json()["rpc_id"]
        await _await_done_via_events(c, sid, rpc_id)
        # Background persister runs slightly behind the SSE stream; give
        # it a beat to flush turn_end before we read the log.
        await asyncio.sleep(2.0)

        r = await c.get(f"{SERVER}/sessions/{sid}/log?limit=200")
        r.raise_for_status()
        rows = r.json()

    types = _count_types(rows)
    missing = _REQUIRED_TYPES - types.keys()
    assert not missing, (
        f"missing types={sorted(missing)} got={types} "
        f"rows={[(r['event_type'], list(r['payload'].keys())) for r in rows]}"
    )
    # Every row must carry the prompt_id so the UI can slice by turn.
    for row in rows:
        assert row["payload"].get("prompt_id") == rpc_id, row


async def test_post_message_stream_persists_session_log():
    """POST /sessions/{id}/message+stream returns the SSE in the response
    body. Same persister hooks → same set of session_log rows. The race
    where /message+stream returns before the persister flushes turn_end
    is exactly what the finally-block awaits guard."""
    if not await _server_up():
        pytest.skip(f"no server at {SERVER}")

    async with httpx.AsyncClient(timeout=120) as c:
        sid = await _create_session(c)
        await _drain_message_stream(c, sid, _TOOL_PROMPT)
        # See above — give the BG persister a moment to flush turn_end.
        await asyncio.sleep(2.0)

        r = await c.get(f"{SERVER}/sessions/{sid}/log?limit=200")
        r.raise_for_status()
        rows = r.json()

    types = _count_types(rows)
    missing = _REQUIRED_TYPES - types.keys()
    assert not missing, (
        f"missing types={sorted(missing)} got={types} "
        f"rows={[(r['event_type'], list(r['payload'].keys())) for r in rows]}"
    )
    # tool_call payload must surface SOMETHING the dashboard renderer
    # can read as the tool's name. Be permissive — the renderer tries
    # ``tool || name || tool_name || toolName || _meta.claudeCode.toolName``
    # — so this assertion mirrors that disjunction.
    tc = next(r for r in rows if r["event_type"] == "tool_call")
    p = tc["payload"]
    name = (
        p.get("tool") or p.get("name") or p.get("tool_name")
        or p.get("toolName")
        or p.get("_meta", {}).get("claudeCode", {}).get("toolName")
    )
    assert name, f"no recognizable tool name in tool_call payload: {p}"
