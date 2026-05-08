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
from unittest.mock import AsyncMock

import httpx
import pytest

_SRC = Path(__file__).resolve().parents[1] / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

from agent_sdk.client import Agent  # noqa: E402


def _payload(agent: Agent) -> dict:
    return agent._registration_payload()


def _fake_response(status_code: int = 200, json_body: dict | None = None) -> httpx.Response:
    return httpx.Response(
        status_code,
        json=json_body or {},
        request=httpx.Request("POST", "http://test/"),
    )


# ---------------------------------------------------------------------------
# Credentials → secrets
# ---------------------------------------------------------------------------

@pytest.mark.timeout(5)
def test_registration_payload_puts_oauth_token_in_secrets():
    """OAuth token must ride through ``payload["secrets"]``, not as a
    top-level key.  The server only reads credentials from ``secrets``."""
    a = Agent("x", provider="unix_local", api_url="http://localhost:7778",
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
    a = Agent("x", provider="unix_local", api_url="http://localhost:7778",
              api_key="sk-ant-secret")
    p = _payload(a)
    assert p["secrets"].get("ANTHROPIC_API_KEY") == "sk-ant-secret"
    assert "api_key" not in p


@pytest.mark.asyncio
@pytest.mark.timeout(5)
async def test_resume_by_session_id_puts_credentials_in_secrets(monkeypatch):
    """Resume uses the same credential channel as session creation.

    The server's /sessions/{id}/resume handler only consumes ``env`` and
    ``secrets``. Top-level oauth_token/api_key fields are ignored, so a
    resume request must not send that legacy shape.
    """
    monkeypatch.delenv("CLAUDE_CODE_OAUTH_TOKEN", raising=False)
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)

    agent = Agent(
        "restored",
        session_id="sess-123",
        api_url="http://localhost:7778",
        oauth_token="resume-oauth",
        api_key="resume-api-key",
    )
    # Agent now layers ApiClient over an httpx.AsyncClient. ApiClient's
    # `_json` dispatches through `_http.request(method, path, ...)`, so we
    # must intercept `request` rather than `post`. (Not all paths pass
    # through `_http.post` directly.)
    request_mock = AsyncMock(return_value=_fake_response(
        json_body={
            "session_id": "sess-123",
            "agent_id": "agent-1",
            "sandbox_id": "sb-1",
            "inner_session_id": "inner-1",
        }
    ))
    monkeypatch.setattr(agent._api._http, "request", request_mock)

    await agent._ensure_registered()

    args, kwargs = request_mock.await_args
    # ApiClient.resume_session calls ``_http.request("POST", "/sessions/{id}/resume", json=...)``
    assert args[0] == "POST"
    assert args[1].endswith("/resume")
    assert kwargs["json"] == {
        "secrets": {
            "CLAUDE_CODE_OAUTH_TOKEN": "resume-oauth",
            "ANTHROPIC_API_KEY": "resume-api-key",
        }
    }
    assert "oauth_token" not in kwargs["json"]
    assert "api_key" not in kwargs["json"]
    await agent._api.close()


@pytest.mark.timeout(5)
def test_registration_payload_accepts_both_credentials():
    a = Agent("x", provider="unix_local", api_url="http://localhost:7778",
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
    a = Agent("x", provider="unix_local", api_url="http://localhost:7778")
    p = _payload(a)
    assert "secrets" not in p


# ---------------------------------------------------------------------------
# Volume defaults
# ---------------------------------------------------------------------------

@pytest.mark.timeout(5)
def test_registration_payload_omits_volume_id_by_default(monkeypatch):
    """When the caller doesn't specify a volume, the SDK must NOT invent
    one — the server auto-creates/looks up ``default-<provider>``.

    This was the root cause of ``Agent("x", provider="unix_local")`` returning
    400 from ``/sessions/quick``: the SDK sent no volume_id, and at the
    time the server hadn't yet implemented ``_resolve_or_default_volume``.
    """
    monkeypatch.delenv("CLAUDE_CODE_OAUTH_TOKEN", raising=False)
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    a = Agent("x", provider="unix_local", api_url="http://localhost:7778")
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
        provider="unix_local",
        api_url="http://localhost:7778",
        oauth_token="t",
        model="claude-sonnet-4-5",
        cwd="/tmp",
        root="/tmp",
        # Note: ``prompt`` and ``tools`` were removed from the Agent
        # constructor when ACP took over those concerns; the JSON-
        # serializability assertion is still meaningful via ``mcp_servers``
        # which is the largest nested-dict structure on the payload.
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
    a = Agent("worker-42", provider="unix_local", api_url="http://localhost:7778")
    p = _payload(a)
    assert p["name"] == "worker-42"
    assert p["agent_type"] == "opencode"


@pytest.mark.timeout(5)
def test_registration_payload_forwards_provider(monkeypatch):
    monkeypatch.delenv("CLAUDE_CODE_OAUTH_TOKEN", raising=False)
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    for provider in ("unix_local", "docker", "daytona"):
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
    a = Agent("x", provider="unix_local", api_url="http://localhost:7778")
    p = _payload(a)
    for k in ("model", "prompt", "tools", "mcp_servers", "skills"):
        assert k not in p, f"{k!r} should be omitted when not set"
