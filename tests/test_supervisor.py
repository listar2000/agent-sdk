"""Supervisor-path tests (direct claude-agent-acp via POST+SSE, no sandbox-agent).

These spawn a real local supervisor subprocess and exercise the full
agent-sdk server pipeline (AcpClient → supervisor.js → claude-agent-acp).
They require `node` + `npm` on PATH plus an ANTHROPIC_API_KEY env var.
Run opt-in via `pytest -m supervisor`.
"""
from __future__ import annotations

import asyncio
import json
import os
import sys
import time
import uuid

import pytest

_SRC = os.path.join(os.path.dirname(__file__), "..", "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

import types
_stub_db = types.ModuleType("api.db")
async def _noop(*a, **kw): return None
async def _noop_list(*a, **kw): return []
async def _noop_false(*a, **kw): return False
from contextlib import asynccontextmanager as _asynccontextmanager
@_asynccontextmanager
async def _noop_get_db(*a, **kw):
    yield None
for name in [
    "init_db", "init_pool", "close_pool",
    "upsert_agent", "get_agent", "list_agents", "delete_agent",
    "upsert_sandbox", "get_sandbox", "list_sandboxes", "delete_sandbox",
    "upsert_session", "get_session", "log_event",
    "get_session_log", "get_agent_log", "session_has_log_entries",
]:
    setattr(_stub_db, name, _noop if "list" not in name and "log" not in name else _noop_list)
_stub_db.init_db = lambda: None
_stub_db.get_db = _noop_get_db
_stub_db.session_has_log_entries = _noop_false
sys.modules.setdefault("api.db", _stub_db)

from api.providers import create_instance, destroy_instance
from api.server import (
    _start_sse_reader, _apply_config_and_initialize,
    _submit_prompt, SESSIONS,
)
from api.acp_client import AcpClient
from api.models import SessionState, AgentConfig


pytestmark = pytest.mark.supervisor


def _need_api_key():
    if not os.environ.get("ANTHROPIC_API_KEY"):
        pytest.skip("ANTHROPIC_API_KEY not set")


async def _make_session(agent_type: str = "claude") -> tuple[SessionState, AcpClient, object]:
    _need_api_key()
    inst = await create_instance("local", agent_type=agent_type)
    assert inst.url.startswith("http://")
    session_id = str(uuid.uuid4())
    acp_session_id = str(uuid.uuid4())
    client = AcpClient(inst.url)
    await _apply_config_and_initialize(
        client, AgentConfig(agent_type=agent_type),
        acp_session_id, "/tmp",
    )
    state = SessionState(
        session_id=session_id,
        agent_id="test-agent",
        sandbox_id="test-sandbox",
        acp_session_id=acp_session_id,
        inner_session_id=client.get_inner_session_id(acp_session_id),
        agent_type=agent_type,
        client=client,
    )
    SESSIONS[session_id] = state
    _start_sse_reader(state)
    return state, client, inst


async def _teardown(state: SessionState, client, inst):
    state.shutdown.set()
    if state._reader_task:
        state._reader_task.cancel()
        try:
            await state._reader_task
        except (asyncio.CancelledError, Exception):
            pass
    if state._scheduler_task and not state._scheduler_task.done():
        state._scheduler_task.cancel()
    try:
        await client.aclose()
    except Exception:
        pass
    await destroy_instance(inst)
    SESSIONS.pop(state.session_id, None)


@pytest.mark.asyncio
async def test_supervisor_provider_returns_http_url():
    _need_api_key()
    inst = await create_instance("local", agent_type="claude")
    try:
        assert inst.provider == "local"
        assert inst.url.startswith("http://")
        assert inst.process is not None
    finally:
        await destroy_instance(inst)


@pytest.mark.asyncio
async def test_end_to_end_prompt_and_terminal_broadcast():
    state, client, inst = await _make_session()
    try:
        q = state.subscribe_session()
        # Mirror the real /message handler: _submit_prompt populates
        # _inflight_tasks synchronously so FIFO tagging attributes events
        # to the rpc before _run_prompt's await.
        rpc = str(uuid.uuid4())
        _submit_prompt(state, rpc, "Reply with exactly: HELLO_TEST")

        # Wait for the rpc task to finish (i.e., terminal received) by
        # polling _inflight_tasks.
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline and state.active_rpc_id == rpc:
            await asyncio.sleep(0.05)

        blocks = []
        for _ in range(200):
            try:
                item = await asyncio.wait_for(q.get(), timeout=0.5)
            except asyncio.TimeoutError:
                break
            if isinstance(item, tuple):
                blocks.append(item)
        full = "\n".join(b[1] for b in blocks)
        assert "HELLO_TEST" in full, f"no HELLO_TEST in {len(blocks)} blocks"
        terminal_blocks = [b for b in blocks if '"stopReason"' in b[1]]
        assert any(b[0] == rpc for b in terminal_blocks)
    finally:
        await _teardown(state, client, inst)


@pytest.mark.asyncio
async def test_resume_by_inner_session_id():
    """Reconnect to a running supervisor + claude-agent-acp with the same
    inner session id and confirm conversation history is preserved."""
    state, client, inst = await _make_session()
    try:
        num = 42
        _, resp1 = await client.prompt(
            state.acp_session_id,
            f'Remember this number: {num}. Reply with exactly: OK {num}.',
        )
        assert resp1.stop_reason == "end_turn"

        inner_sid = client.get_inner_session_id(state.acp_session_id)
        await client.aclose()

        client2 = AcpClient(inst.url)
        client2.set_inner_session_id(state.acp_session_id, inner_sid)
        _, resp2 = await client2.prompt(
            state.acp_session_id,
            "What number did I ask you to remember? Reply with exactly: NUMBER=<the number>.",
        )
        assert resp2.stop_reason == "end_turn"
        state.client = client2
    finally:
        await _teardown(state, state.client, inst)
