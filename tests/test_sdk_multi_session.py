"""Lock the shape of the multi-session SDK surface.

Cheap contract tests — no server, no sandbox, no network. Verifies:

  * ``agent.create_session()`` returns a fresh ``Session`` bound to the
    same ``Agent`` (same ApiClient, same spec, distinct runtime state).
  * Legacy attributes on ``Agent`` (``session_id``, ``sandbox_ref``,
    ``inner_session_id``, ``usage``, ``sandbox``, ``_registered``)
    forward to the default session — backwards compat.
  * Two sessions of one Agent have distinct, independent runtime state.
  * On registration, a ``Session`` whose parent Agent already has an
    ``id`` includes ``agent_id`` in the ``POST /sessions`` payload so
    the server reuses the agent row instead of minting a new one.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

_SRC = Path(__file__).resolve().parents[1] / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

from agent_sdk.client import Agent, Session, Sandbox, UsageStats  # noqa: E402


# ---------------------------------------------------------------------------
# Factory + shape
# ---------------------------------------------------------------------------

def test_create_session_returns_a_session_bound_to_this_agent():
    a = Agent("x", provider="unix_local", api_url="http://localhost:7778")
    s = a.create_session()
    assert isinstance(s, Session)
    assert s._agent is a
    # Fresh session has no runtime state yet.
    assert s.session_id is None
    assert s.sandbox_ref is None
    assert s.inner_session_id is None
    assert s._registered is False


def test_default_session_is_lazy():
    """A fresh Agent has NO default session until something needs one.
    Pure attribute reads (``agent.session_id``) observe ``None`` rather
    than allocating a phantom session."""
    a = Agent("x", provider="unix_local", api_url="http://localhost:7778")
    assert a._default_session is None
    # Pure reads must NOT materialise the default session.
    _ = a.session_id
    _ = a.sandbox_ref
    _ = a.inner_session_id
    _ = a._registered
    assert a._default_session is None
    # An Agent that's only ever used through ``create_session()`` never
    # allocates its default slot.
    s = a.create_session()
    assert a._default_session is None
    assert s is not None


def test_default_session_seeded_when_resume_args_passed():
    """``Agent(session_id=..., sandbox_ref=...)`` is the resume case —
    seed the default session eagerly so the caller can read
    ``agent.session_id`` immediately after construction."""
    a = Agent("x", provider="unix_local", api_url="http://localhost:7778",
              session_id="seed-1", sandbox_ref="seed-sb")
    assert a._default_session is not None
    assert a._default_session.session_id == "seed-1"


def test_writing_session_id_materialises_default():
    """A direct write to ``agent.session_id`` (the legacy reset/seed
    path) must allocate the default session — otherwise the value would
    be lost."""
    a = Agent("x", provider="unix_local", api_url="http://localhost:7778")
    assert a._default_session is None
    a.session_id = "manual-1"
    assert a._default_session is not None
    assert a._default_session.session_id == "manual-1"


def test_reset_session_is_noop_when_no_default():
    """``reset_session()`` on a fresh Agent shouldn't crash or
    materialise an empty default."""
    a = Agent("x", provider="unix_local", api_url="http://localhost:7778")
    a.reset_session()
    assert a._default_session is None


def test_two_create_session_calls_return_distinct_sessions():
    a = Agent("x", provider="unix_local", api_url="http://localhost:7778")
    s1 = a.create_session()
    s2 = a.create_session()
    assert s1 is not s2
    # Each owns its own runtime state.
    assert s1.usage is not s2.usage
    assert s1._register_lock is not s2._register_lock
    assert s1._prompt_lock is not s2._prompt_lock
    assert s1.sandbox is not s2.sandbox
    # But they share the parent agent.
    assert s1._agent is s2._agent
    assert s1._agent is a


def test_default_session_distinct_from_explicit_create_session():
    """Calling ``create_session()`` does NOT return the default session
    — the default is reserved for legacy ``agent.arun()`` etc."""
    a = Agent("x", provider="unix_local", api_url="http://localhost:7778")
    s = a.create_session()
    # The default session was never materialised by create_session().
    assert a._default_session is None
    # And once we do touch agent.arun (via _ensure_default_session), the
    # materialised default is a different instance from the explicit one.
    default = a._ensure_default_session()
    assert s is not default


# ---------------------------------------------------------------------------
# Session resume — agent.session(session_id=...) and constructor seed
# ---------------------------------------------------------------------------

def test_agent_session_returns_session_seeded_with_id():
    """``agent.session(session_id=X)`` returns a fresh Session whose
    session_id is set; on first use it routes through the resume path
    (not the create path)."""
    a = Agent("x", provider="unix_local", api_url="http://localhost:7778")
    s = a.session(session_id="sess-123", sandbox_ref="sb-abc")
    assert isinstance(s, Session)
    assert s.session_id == "sess-123"
    assert s.sandbox_ref == "sb-abc"
    # Not yet registered — happens lazily on first use.
    assert s._registered is False
    # Resume route does NOT use lazy_provision (irrelevant on resume path).
    assert s._lazy_provision is False


def test_agent_constructor_with_session_id_seeds_default_session():
    """Backwards compat: ``Agent(session_id=...)`` puts the value on the
    default session, not on a free-floating attribute."""
    a = Agent("x", provider="unix_local", api_url="http://localhost:7778",
              session_id="seed-1", sandbox_ref="seed-sb")
    assert a.session_id == "seed-1"
    assert a.sandbox_ref == "seed-sb"
    assert a._default_session.session_id == "seed-1"
    assert a._default_session.sandbox_ref == "seed-sb"


# ---------------------------------------------------------------------------
# Backwards-compat: forwarded properties
# ---------------------------------------------------------------------------

def test_agent_session_id_forwards_to_default_session():
    a = Agent("x", provider="unix_local", api_url="http://localhost:7778")
    a.session_id = "sess-changed"  # writer materialises the default
    assert a._default_session.session_id == "sess-changed"
    assert a.session_id == "sess-changed"


def test_agent_sandbox_ref_forwards_to_default_session():
    a = Agent("x", provider="unix_local", api_url="http://localhost:7778")
    a.sandbox_ref = "sb-changed"
    assert a._default_session.sandbox_ref == "sb-changed"
    assert a.sandbox_ref == "sb-changed"


def test_agent_inner_session_id_forwards_to_default_session():
    a = Agent("x", provider="unix_local", api_url="http://localhost:7778")
    a.inner_session_id = "inner-1"
    assert a._default_session.inner_session_id == "inner-1"
    assert a.inner_session_id == "inner-1"


def test_agent_usage_forwards_to_default_session():
    """``agent.usage`` materialises the default session — the counter
    needs a stable home. A bump on the session is visible on the agent."""
    a = Agent("x", provider="unix_local", api_url="http://localhost:7778")
    assert isinstance(a.usage, UsageStats)
    assert a._default_session is not None  # materialised by .usage read
    a._default_session.usage.call_count += 1
    assert a.usage.call_count == 1


def test_agent_sandbox_forwards_to_default_session():
    a = Agent("x", provider="unix_local", api_url="http://localhost:7778")
    sb = a.sandbox
    assert isinstance(sb, Sandbox)
    assert sb is a._default_session.sandbox
    # Sandbox binds to the session, not the agent.
    assert sb._session is a._default_session


def test_sibling_session_has_its_own_sandbox():
    a = Agent("x", provider="unix_local", api_url="http://localhost:7778")
    s = a.create_session()
    assert s.sandbox._session is s
    # And is independent of any default session that legacy code might
    # later spawn.
    default_sandbox = a.sandbox  # materialises default
    assert s.sandbox is not default_sandbox


def test_agent_registered_flag_forwards_to_default_session():
    a = Agent("x", provider="unix_local", api_url="http://localhost:7778")
    assert a._registered is False  # phantom-safe: no session, no flag
    a._registered = True
    assert a._default_session is not None
    assert a._default_session._registered is True


def test_reset_session_resets_default_only_not_siblings():
    a = Agent("x", provider="unix_local", api_url="http://localhost:7778")
    # Use ``agent.session(session_id=...)`` to construct a sibling that
    # represents an existing server-side session — the resume verb.
    sibling = a.session(session_id="sibling-1", sandbox_ref="sb-sibling")
    sibling._registered = True
    # Materialise the default session and seed runtime state on it.
    a.session_id = "default-1"
    a._default_session._registered = True

    a.reset_session()

    # Default session was reset.
    assert a._default_session.session_id is None
    assert a._default_session._registered is False
    # Sibling session was NOT touched.
    assert sibling.session_id == "sibling-1"
    assert sibling._registered is True


# ---------------------------------------------------------------------------
# Registration payload: sibling sessions reuse agent_id
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_sibling_session_includes_agent_id_in_create_payload(monkeypatch):
    """When ``Agent.id`` is already set (the agent has been registered),
    a Session's first call MUST pass ``agent_id`` in the ``POST /sessions``
    payload so the server reuses the agent row. Without this, every
    sibling session would mint a new server-side agent and lose the
    shared volume subpath ``agents/<agent_id>/`` that gives them a
    common Claude JSONL history."""
    a = Agent("x", provider="unix_local", api_url="http://localhost:7778")
    a.id = "preexisting-agent-id"  # simulate already-registered

    captured: dict = {}

    async def fake_create_session(**body):
        captured.update(body)
        return {
            "agent_id": body["agent_id"],
            "session_id": "new-sess-id",
            "sandbox_ref": "new-sb-ref",
            "inner_session_id": "inner-id",
        }

    monkeypatch.setattr(a._api, "create_session", fake_create_session)

    sibling = a.create_session()
    await sibling._ensure_registered()

    assert captured.get("agent_id") == "preexisting-agent-id", (
        "sibling session must forward the existing agent_id"
    )
    # Sibling sessions are lazy-provisioned: server is told to mint the
    # session row only, no sandbox.
    assert captured.get("provision") is False, (
        "create_session() siblings must request lazy provisioning"
    )
    # Session captured the server's response.
    assert sibling.session_id == "new-sess-id"
    assert sibling.sandbox_ref == "new-sb-ref"
    assert sibling.inner_session_id == "inner-id"
    # Sibling registration MUST NOT mutate the agent_id.
    assert a.id == "preexisting-agent-id"


@pytest.mark.asyncio
async def test_first_session_omits_agent_id_and_captures_one_from_response(monkeypatch):
    """The very first session for an Agent doesn't know the agent_id
    yet — the server mints it and returns it in the response. Verify
    we don't accidentally send a stale ``agent_id`` and that we capture
    the server's value onto ``agent.id``."""
    a = Agent("x", provider="unix_local", api_url="http://localhost:7778")
    assert a.id is None

    captured: dict = {}

    async def fake_create_session(**body):
        captured.update(body)
        return {
            "agent_id": "minted-agent-id",
            "session_id": "first-sess-id",
            "sandbox_ref": "first-sb-ref",
            "inner_session_id": "inner-id-1",
        }

    monkeypatch.setattr(a._api, "create_session", fake_create_session)

    await a._ensure_default_session()._ensure_registered()

    assert "agent_id" not in captured, (
        "first session must NOT send agent_id; server mints it"
    )
    assert a.id == "minted-agent-id"
    assert a.session_id == "first-sess-id"


@pytest.mark.asyncio
async def test_two_concurrent_sibling_registrations_share_one_agent_row(monkeypatch):
    """Race test: two sessions register concurrently against an
    unregistered Agent. The agent-level register lock must serialise
    them so the SECOND session sees ``agent.id`` already set and
    forwards it — preventing two server-side agent rows for one client
    Agent."""
    import asyncio

    a = Agent("x", provider="unix_local", api_url="http://localhost:7778")
    assert a.id is None

    sent_agent_ids: list[str | None] = []
    minted_count = 0

    async def fake_create_session(**body):
        nonlocal minted_count
        # Hold each call briefly so the two coroutines really overlap.
        await asyncio.sleep(0.02)
        sent_agent_ids.append(body.get("agent_id"))
        agent_id = body.get("agent_id")
        if agent_id is None:
            minted_count += 1
            agent_id = f"minted-{minted_count}"
        return {
            "agent_id": agent_id,
            "session_id": f"sess-{len(sent_agent_ids)}",
            "sandbox_ref": f"sb-{len(sent_agent_ids)}",
            "inner_session_id": f"inner-{len(sent_agent_ids)}",
        }

    monkeypatch.setattr(a._api, "create_session", fake_create_session)

    s1 = a.create_session()
    s2 = a.create_session()
    await asyncio.gather(s1._ensure_registered(), s2._ensure_registered())

    # Exactly one of the two sent ``agent_id=None`` (the first through
    # the agent-level lock); the other forwarded the minted id.
    assert sent_agent_ids.count(None) == 1, (
        f"expected exactly one mint; got sent_agent_ids={sent_agent_ids!r}"
    )
    assert minted_count == 1, "server-side agent row should be minted exactly once"
    # Both sessions ended up under the same agent.
    assert s1.session_id != s2.session_id
    assert a.id == "minted-1"


# ---------------------------------------------------------------------------
# Lazy provisioning: create_session() vs. default session
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_default_session_does_not_request_lazy_provisioning(monkeypatch):
    """Backwards compat: ``agent.arun(...)`` (legacy entry point) must
    keep eager provisioning so callers reading ``agent.sandbox_ref``
    after first use still see a value."""
    a = Agent("x", provider="unix_local", api_url="http://localhost:7778")

    captured: dict = {}

    async def fake_create_session(**body):
        captured.update(body)
        return {
            "agent_id": "minted",
            "session_id": "sess-1",
            "sandbox_ref": "sb-1",
            "inner_session_id": "inner-1",
        }

    monkeypatch.setattr(a._api, "create_session", fake_create_session)

    await a._ensure_default_session()._ensure_registered()

    # Default session does NOT send provision: false. (Either omits it
    # entirely or explicitly true; today's code omits it so the server
    # default of True applies.)
    assert captured.get("provision", True) is True, (
        "default session must NOT request lazy provisioning; "
        f"got payload {captured!r}"
    )


# ---------------------------------------------------------------------------
# agent.session(session_id=...) routes through resume, not create
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_agent_session_routes_to_resume(monkeypatch):
    """``agent.session(session_id=X)._ensure_registered()`` calls
    ``POST /sessions/{id}/resume`` and NOT ``POST /sessions``."""
    a = Agent("x", provider="unix_local", api_url="http://localhost:7778")

    create_calls: list[dict] = []
    resume_calls: list[tuple[str, dict]] = []

    async def fake_create_session(**body):
        create_calls.append(body)
        return {}

    async def fake_resume_session(session_id, **body):
        resume_calls.append((session_id, body))
        return {
            "agent_id": "agent-from-server",
            "sandbox_ref": "sb-resumed",
            "inner_session_id": "inner-resumed",
        }

    monkeypatch.setattr(a._api, "create_session", fake_create_session)
    monkeypatch.setattr(a._api, "resume_session", fake_resume_session)

    s = a.session(session_id="existing-sess")
    await s._ensure_registered()

    assert create_calls == [], "session() must not call create_session"
    assert len(resume_calls) == 1
    assert resume_calls[0][0] == "existing-sess"
    # Server's response is captured onto the Session.
    assert s.sandbox_ref == "sb-resumed"
    assert s.inner_session_id == "inner-resumed"
    # And onto the Agent (so subsequent create_session() siblings
    # forward the correct agent_id).
    assert a.id == "agent-from-server"


@pytest.mark.asyncio
async def test_agent_with_provider_and_session_id_resumes(monkeypatch):
    """Multi-session support unblocks the constructor combo
    ``Agent(provider=..., session_id=X)``: today this used to mint a
    NEW session because the resume detection required ``provider is
    None``. Now resume is detected by ``session_id`` alone."""
    a = Agent("x", provider="unix_local", api_url="http://localhost:7778",
              session_id="existing")

    create_calls: list[dict] = []
    resume_calls: list[str] = []

    async def fake_create_session(**body):
        create_calls.append(body)
        return {}

    async def fake_resume_session(session_id, **body):
        resume_calls.append(session_id)
        return {"agent_id": "a", "sandbox_ref": "sb", "inner_session_id": "i"}

    monkeypatch.setattr(a._api, "create_session", fake_create_session)
    monkeypatch.setattr(a._api, "resume_session", fake_resume_session)

    await a._default_session._ensure_registered()
    assert resume_calls == ["existing"]
    assert create_calls == []
