"""Invariant tests for SessionState.lifecycle.

The two writers that change a session's compute state must keep the
explicit lifecycle field in sync with _INSTANCES so callers don't need
to snoop the cache.
"""
from __future__ import annotations

import os
import sys
from types import SimpleNamespace

import pytest

_SRC = os.path.join(os.path.dirname(__file__), "..", "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

from api import server as srv  # noqa: E402
from api.models import SandboxRecord, SessionState  # noqa: E402


@pytest.fixture(autouse=True)
def _clean():
    srv.SESSIONS.clear()
    srv._INSTANCES.clear()
    yield
    srv.SESSIONS.clear()
    srv._INSTANCES.clear()


@pytest.mark.asyncio
async def test_hibernate_session_flips_lifecycle_to_hibernated(monkeypatch):
    """_hibernate_session must set state.lifecycle="hibernated" after popping _INSTANCES."""
    sandbox = SandboxRecord(
        id="sb-1", provider="local", sandbox_ref="ref",
        status="running", root="/tmp",
        volume_id="vol-1", subpath="agents/a1",
    )
    state = SessionState(
        session_id="sess-1", agent_id="agent-1", sandbox_id="sb-1",
        agent_type="claude",
    )
    srv._INSTANCES["sb-1"] = SimpleNamespace(provider="local", url="http://x")
    srv.SESSIONS["sess-1"] = state
    assert state.lifecycle == "live"

    async def fake_get_sandbox(sid):
        return sandbox

    async def fake_stop_sandbox(provider, instance):
        return None

    async def fake_upsert_sandbox(sb):
        return None

    async def fake_request_supervisor_snapshot(inst):
        return None

    async def fake_cancel_task(t):
        return None

    monkeypatch.setattr(srv, "get_sandbox", fake_get_sandbox)
    monkeypatch.setattr(srv._providers_mod, "stop_sandbox", fake_stop_sandbox)
    monkeypatch.setattr(srv, "upsert_sandbox", fake_upsert_sandbox)
    monkeypatch.setattr(srv, "_request_supervisor_snapshot", fake_request_supervisor_snapshot)
    monkeypatch.setattr(srv, "_cancel_task", fake_cancel_task)
    monkeypatch.setattr(srv, "_synthesize_instance", lambda sb: SimpleNamespace())

    await srv._hibernate_session(state)

    assert "sb-1" not in srv._INSTANCES
    assert state.lifecycle == "hibernated"
    assert state.is_hibernated is True


def test_lifecycle_default_is_live():
    state = SessionState(
        session_id="sess-1", agent_id="agent-1", sandbox_id="sb-1",
    )
    assert state.lifecycle == "live"
    assert state.is_hibernated is False
