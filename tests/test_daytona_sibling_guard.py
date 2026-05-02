"""Daytona-specific multi-session guard.

The S3-FUSE volume backing each Daytona Sandbox doesn't propagate
writes between sibling mounts coherently — see the discussion in
``docs/multi-session.md`` (or the Session/SessionPool refactor docs).
Until we have a shared-sandbox + multi-supervisor architecture for
Daytona, the server fails fast when a caller asks for a SECOND live
session under an existing ``agent_id`` while a first one is still in
the pool.

Contract verified here, NOT exercised:
  * ``pool.find_by_agent_id`` returns the right shape
  * ``_reject_daytona_sibling_when_active`` is a no-op for non-Daytona
    providers and for first-session creates (``agent_id`` is None).
  * It raises HTTPException(409) when called for daytona + a known
    live session.

These are unit tests; no DB, no real sandbox.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

_SRC = Path(__file__).resolve().parents[1] / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

from fastapi import HTTPException  # noqa: E402

from api.sandbox.pool import SessionPool  # noqa: E402
from api.server import _reject_daytona_sibling_when_active  # noqa: E402


# ---------------------------------------------------------------------------
# pool.find_by_agent_id
# ---------------------------------------------------------------------------

class _FakeSession:
    """Minimal stand-in for BaseSandboxSession.

    SessionPool.find_by_agent_id reads ``sess._agent_id`` directly —
    set during ``_bootstrap_session()`` on the real class. For unit
    tests we just attach the field manually.
    """

    def __init__(self, session_id: str, agent_id: str):
        self.session_id = session_id
        self._agent_id = agent_id


def test_find_by_agent_id_returns_matching_active_sessions():
    pool = SessionPool(factory=lambda *a, **k: None)  # factory unused
    s1 = _FakeSession("s1", "agent-A")
    s2 = _FakeSession("s2", "agent-A")
    s3 = _FakeSession("s3", "agent-B")
    pool._active = {"s1": s1, "s2": s2, "s3": s3}

    matches = pool.find_by_agent_id("agent-A")
    ids = sorted(m.session_id for m in matches)
    assert ids == ["s1", "s2"]


def test_find_by_agent_id_empty_when_no_active():
    pool = SessionPool(factory=lambda *a, **k: None)
    assert pool.find_by_agent_id("agent-X") == []


def test_find_by_agent_id_skips_sessions_with_unset_agent_id():
    """Defensive: if a session is mid-bootstrap and ``_agent_id``
    hasn't been set yet, it shouldn't false-positive against any
    real agent_id."""
    pool = SessionPool(factory=lambda *a, **k: None)
    s_orphan = _FakeSession("orphan", agent_id=None)  # type: ignore[arg-type]
    pool._active = {"orphan": s_orphan}
    assert pool.find_by_agent_id("agent-A") == []


# ---------------------------------------------------------------------------
# _reject_daytona_sibling_when_active
# ---------------------------------------------------------------------------

def test_reject_is_noop_when_agent_id_is_none(monkeypatch):
    """First-session creates pass agent_id=None; the guard must let
    them through (it's about siblings, not new agents)."""
    # No monkey-patching needed; the function returns early on agent_id=None.
    _reject_daytona_sibling_when_active(agent_id=None, provider="daytona")


def test_reject_is_noop_for_non_daytona_providers(monkeypatch):
    """docker / local / modal share their volume via POSIX kernel
    coherence — siblings are safe. Don't 409 on those."""
    # Even with a "live" session in the pool, we must let docker through.
    from api.sandbox import runtime as rt
    fake_pool = SessionPool(factory=lambda *a, **k: None)
    fake_pool._active = {"sX": _FakeSession("sX", "agent-A")}
    monkeypatch.setattr(rt, "_pool", fake_pool)

    # Should NOT raise.
    _reject_daytona_sibling_when_active(agent_id="agent-A", provider="docker")
    _reject_daytona_sibling_when_active(agent_id="agent-A", provider="unix_local")
    _reject_daytona_sibling_when_active(agent_id="agent-A", provider="modal")


def test_reject_409s_when_daytona_sibling_already_alive(monkeypatch):
    from api.sandbox import runtime as rt
    fake_pool = SessionPool(factory=lambda *a, **k: None)
    fake_pool._active = {"sX": _FakeSession("sX", "agent-A")}
    monkeypatch.setattr(rt, "_pool", fake_pool)

    with pytest.raises(HTTPException) as exc:
        _reject_daytona_sibling_when_active(agent_id="agent-A", provider="daytona")
    assert exc.value.status_code == 409
    # Error message names the existing session_id so the user can
    # release exactly the right one.
    assert "sX" in str(exc.value.detail)
    # And mentions Daytona to make the failure mode obvious.
    assert "Daytona" in str(exc.value.detail) or "daytona" in str(exc.value.detail).lower()


def test_reject_lets_daytona_through_when_no_sibling(monkeypatch):
    """Even on Daytona, the FIRST sibling-create against an existing
    agent_id is fine — only subsequent ones (where one is alive) 409."""
    from api.sandbox import runtime as rt
    fake_pool = SessionPool(factory=lambda *a, **k: None)
    fake_pool._active = {}  # no siblings live
    monkeypatch.setattr(rt, "_pool", fake_pool)

    # Should NOT raise.
    _reject_daytona_sibling_when_active(agent_id="agent-A", provider="daytona")
