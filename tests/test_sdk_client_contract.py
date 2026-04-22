"""Lock the shape of what ``agent_sdk.Agent`` sends to the server.

Regression guard
----------------
The SDK once shipped credentials as top-level ``oauth_token`` / ``api_key``
fields on the ``/sessions/quick`` payload.  The server silently ignored
them because it only consumes credentials through the ``secrets`` channel
(``_pop_env_and_secrets``).  Result: the supervisor spawned with no auth
and Claude errored with "Authentication required" on the first prompt.

The unit test suite didn't catch this because every test that exercised
``_registration_payload`` mocked the supervisor and never looked at
``secrets``.  These tests are cheap contract checks — no server, no
sandbox, just assertions on the payload dict.
"""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import pytest

_SRC = Path(__file__).resolve().parents[1] / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

from agent_sdk.client import Agent  # noqa: E402


def _payload(agent: Agent) -> dict:
    return agent._registration_payload()


# ---------------------------------------------------------------------------
# Credentials → secrets
# ---------------------------------------------------------------------------

@pytest.mark.timeout(5)
def test_registration_payload_puts_oauth_token_in_secrets():
    """OAuth token must ride through ``payload["secrets"]``, not as a
    top-level key.  The server only reads credentials from ``secrets``."""
    a = Agent("x", provider="local", api_url="http://localhost:7778",
              oauth_token="secret-oauth-123")
    p = _payload(a)
    assert "secrets" in p, "no secrets key on payload"
    assert p["secrets"].get("CLAUDE_CODE_OAUTH_TOKEN") == "secret-oauth-123"
    # Anti-regression: must NOT appear as a top-level field.
    assert "oauth_token" not in p
    assert "api_key" not in p


@pytest.mark.timeout(5)
def test_registration_payload_puts_api_key_in_secrets():
    """Same contract for ``api_key``."""
    a = Agent("x", provider="local", api_url="http://localhost:7778",
              api_key="sk-ant-secret")
    p = _payload(a)
    assert p["secrets"].get("ANTHROPIC_API_KEY") == "sk-ant-secret"
    assert "api_key" not in p


@pytest.mark.timeout(5)
def test_registration_payload_accepts_both_credentials():
    a = Agent("x", provider="local", api_url="http://localhost:7778",
              oauth_token="tok", api_key="sk")
    p = _payload(a)
    assert p["secrets"] == {
        "CLAUDE_CODE_OAUTH_TOKEN": "tok",
        "ANTHROPIC_API_KEY": "sk",
    }


@pytest.mark.timeout(5)
def test_registration_payload_omits_secrets_when_no_credentials(monkeypatch):
    """No creds → no ``secrets`` field at all.  Protects against a subtle
    regression where the SDK sent an empty dict and the server's PATCH
    semantics interpreted that as "wipe stored secrets"."""
    # Make sure inherited env-vars don't pollute the assertion.
    monkeypatch.delenv("CLAUDE_CODE_OAUTH_TOKEN", raising=False)
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    a = Agent("x", provider="local", api_url="http://localhost:7778")
    p = _payload(a)
    assert "secrets" not in p


# ---------------------------------------------------------------------------
# Volume defaults
# ---------------------------------------------------------------------------

@pytest.mark.timeout(5)
def test_registration_payload_omits_volume_id_by_default(monkeypatch):
    """When the caller doesn't specify a volume, the SDK must NOT invent
    one — the server auto-creates/looks up ``default-<provider>``.

    This was the root cause of ``Agent("x", provider="local")`` returning
    400 from ``/sessions/quick``: the SDK sent no volume_id, and at the
    time the server hadn't yet implemented ``_resolve_or_default_volume``.
    """
    monkeypatch.delenv("CLAUDE_CODE_OAUTH_TOKEN", raising=False)
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    a = Agent("x", provider="local", api_url="http://localhost:7778")
    p = _payload(a)
    assert "volume_id" not in p


# ---------------------------------------------------------------------------
# JSON-serializability — the payload goes over the wire as JSON
# ---------------------------------------------------------------------------

@pytest.mark.timeout(5)
def test_registration_payload_serializes_via_standard_json(monkeypatch):
    monkeypatch.delenv("CLAUDE_CODE_OAUTH_TOKEN", raising=False)
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    a = Agent(
        "x",
        provider="local",
        api_url="http://localhost:7778",
        oauth_token="t",
        model="claude-sonnet-4-5",
        cwd="/tmp",
        root="/tmp",
        prompt="be helpful",
        tools=["Read", "Write"],
        mcp_servers={"fs": {"command": "mcp-fs", "args": []}},
    )
    p = _payload(a)
    # Must not raise — validates every nested container is JSON-native.
    encoded = json.dumps(p)
    # Round-trip preserves the structure.
    assert json.loads(encoded) == p


# ---------------------------------------------------------------------------
# Top-level payload shape invariants
# ---------------------------------------------------------------------------

@pytest.mark.timeout(5)
def test_registration_payload_always_has_name_and_agent_type(monkeypatch):
    monkeypatch.delenv("CLAUDE_CODE_OAUTH_TOKEN", raising=False)
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    a = Agent("worker-42", provider="local", api_url="http://localhost:7778")
    p = _payload(a)
    assert p["name"] == "worker-42"
    assert p["agent_type"] == "claude"


@pytest.mark.timeout(5)
def test_registration_payload_forwards_provider(monkeypatch):
    monkeypatch.delenv("CLAUDE_CODE_OAUTH_TOKEN", raising=False)
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    for provider in ("local", "docker", "daytona"):
        a = Agent("x", provider=provider, api_url="http://localhost:7778")
        p = _payload(a)
        assert p.get("provider") == provider


@pytest.mark.timeout(5)
def test_registration_payload_drops_none_keys(monkeypatch):
    """Optional config (model, cwd, prompt, tools) is omitted when None.

    Sending explicit ``"cwd": null`` caused the server to prefer ``null``
    over its computed default in at least one cycle — the SDK just shouldn't
    put the key in.
    """
    monkeypatch.delenv("CLAUDE_CODE_OAUTH_TOKEN", raising=False)
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    a = Agent("x", provider="local", api_url="http://localhost:7778")
    p = _payload(a)
    for k in ("model", "prompt", "tools", "mcp_servers", "skills"):
        assert k not in p, f"{k!r} should be omitted when not set"
