"""Session-log persistence under the SessionPool flow.

After PR-D5 deleted the legacy persistent SSE-reader (which used to write
``session_log`` rows via _process_sse_block → _schedule_log → log_event),
persistence now lives in ``server._persist_prompt_events``: one row per
event yielded by ``SandboxSession.execute_prompt``.

These tests fire a real prompt that triggers a tool call and assert that
``GET /sessions/{id}/log`` returns rows for the event types the dashboard
and SDK depend on. Run against the live test server (``scripts/launch_server_test.sh``).

Driven through ``agent_sdk.ApiClient`` rather than raw httpx — the wire
contract is pinned independently in ``test_api_client.py`` and the server
route tests, so behaviour-level integration tests like this one ride on
the SDK to avoid hand-rolling SSE framing.
"""
from __future__ import annotations

import asyncio
import os
import sys
import time

import httpx
import pytest

_SRC = os.path.join(os.path.dirname(__file__), "..", "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

from agent_sdk import ApiClient  # noqa: E402

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


async def _create_session(sdk: ApiClient) -> str:
    # Pin haiku to keep these tests under sonnet's exhausted weekly
    # quota when other suites have run on the same OAuth token recently.
    created = await sdk.create_session(
        provider="unix_local", agent_type="claude", config={}, model="haiku",
    )
    return created["session_id"]


async def _await_done(sdk: ApiClient, sid: str, rpc_id: str, timeout: float = 90.0) -> None:
    """Drain /events until the done envelope for ``rpc_id`` arrives."""
    deadline = time.time() + timeout
    buf = b""
    async for chunk in sdk.stream_events(sid):
        buf += chunk
        while b"\n\n" in buf:
            block, buf = buf.split(b"\n\n", 1)
            text = block.decode("utf-8", errors="replace")
            if rpc_id in text and ('"stopReason"' in text or '"type":"done"' in text):
                return
        if time.time() > deadline:
            raise TimeoutError(f"timeout after {timeout}s")


async def _drain_message_stream(
    sdk: ApiClient, sid: str, message: str, timeout: float = 90.0,
) -> None:
    """POST /message+stream and drain until done."""
    deadline = time.time() + timeout
    buf = b""
    async for chunk in sdk.send_message_stream(sid, message):
        buf += chunk
        while b"\n\n" in buf:
            block, buf = buf.split(b"\n\n", 1)
            text = block.decode("utf-8", errors="replace")
            if '"stopReason"' in text or '"type":"done"' in text:
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

    rows: list[dict] = []
    rpc_id: str | None = None
    async with ApiClient(SERVER, timeout=120.0) as sdk:
        sid: str | None = None
        try:
            sid = await _create_session(sdk)
            sent = await sdk.send_message(sid, _TOOL_PROMPT)
            rpc_id = sent["rpc_id"]
            await _await_done(sdk, sid, rpc_id)
            # Background persister runs slightly behind the SSE stream; give
            # it a beat to flush turn_end before we read the log.
            await asyncio.sleep(2.0)

            rows = await sdk.get_session_log(sid, limit=200)
        finally:
            # Always tear down the session so the local supervisor process
            # exits and (on daytona/docker/modal) the underlying sandbox is
            # released. Without this, repeated test runs accumulate live
            # supervisors and — on daytona — burn through the account's disk
            # quota. delete_session is idempotent (204 even when missing).
            if sid:
                try:
                    await sdk.delete_session(sid)
                except Exception as exc:
                    print(f"cleanup delete_session({sid}) raised: {exc}")

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

    rows: list[dict] = []
    async with ApiClient(SERVER, timeout=120.0) as sdk:
        sid: str | None = None
        try:
            sid = await _create_session(sdk)
            await _drain_message_stream(sdk, sid, _TOOL_PROMPT)
            # See above — give the BG persister a moment to flush turn_end.
            await asyncio.sleep(2.0)

            rows = await sdk.get_session_log(sid, limit=200)
        finally:
            if sid:
                try:
                    await sdk.delete_session(sid)
                except Exception as exc:
                    print(f"cleanup delete_session({sid}) raised: {exc}")

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
