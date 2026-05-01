"""SDK/server field-name drift regressions (cycle-12 audit).

Cycle 11 caught one drift where the client read ``sandbox_id`` but the
server emitted ``current_sandbox_id`` from ``/sessions/quick``. These
tests lock in the sibling invariants we verified during the cycle-12
audit so the same class of bug can't regress silently:

* ``configure()`` / ``cancel()`` surface the server's ``{"error": "..."}``
  body instead of swallowing it into a bare ``HTTPStatusError: 400``.

(Volume forward-compat tests were dropped when the legacy ``Client``
class was removed; ``ApiClient`` returns raw dicts and is naturally
forward-compatible with new server fields.)
"""
from __future__ import annotations

import os
import sys
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

_SRC = os.path.join(os.path.dirname(__file__), "..", "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)


def _fake_response(status_code: int, json_body=None):
    r = MagicMock()
    r.status_code = status_code
    r.json = MagicMock(return_value=json_body)
    r.request = httpx.Request("POST", "http://fake/")
    r.text = ""
    return r


# ---------------------------------------------------------------------------
# Error surfacing: configure() / cancel() must include the server's
# ``{"error": "..."}`` body so callers can see what went wrong.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_configure_surfaces_server_error_message():
    from agent_sdk.client import Agent

    agent = Agent("err-cfg", api_url="http://localhost:7778")
    agent._registered = True
    agent.session_id = "s-1"
    agent.id = "err-cfg"

    resp = _fake_response(502, {"error": "supervisor unreachable: boom"})
    with patch.object(agent._api._http, "request", AsyncMock(return_value=resp)):
        with pytest.raises(httpx.HTTPStatusError) as exc_info:
            await agent.configure(mode="acceptEdits")
    # Cycle-11-style fix: error message must include server detail, not
    # just "HTTP 502".
    assert "supervisor unreachable: boom" in str(exc_info.value)
    await agent._api.close()


@pytest.mark.asyncio
async def test_cancel_surfaces_server_error_message():
    from agent_sdk.client import Agent

    agent = Agent("err-cancel", api_url="http://localhost:7778")
    agent._registered = True
    agent.session_id = "s-2"
    agent.id = "err-cancel"

    resp = _fake_response(504, {"error": "cancel timed out"})
    with patch.object(agent._api._http, "request", AsyncMock(return_value=resp)):
        with pytest.raises(httpx.HTTPStatusError) as exc_info:
            await agent.cancel()
    assert "cancel timed out" in str(exc_info.value)
    await agent._api.close()


@pytest.mark.asyncio
async def test_plain_register_surfaces_server_error_message():
    """``/agents`` registration (no provider, no session) used to call
    ``resp.raise_for_status()`` directly, hiding the server's error body.
    """
    from agent_sdk.client import Agent

    agent = Agent("err-reg", api_url="http://localhost:7778")

    resp = _fake_response(400, {"error": "name required"})
    with patch.object(agent._api._http, "request", AsyncMock(return_value=resp)):
        with pytest.raises(httpx.HTTPStatusError) as exc_info:
            await agent._ensure_registered()
    assert "name required" in str(exc_info.value)
    await agent._api.close()


# ---------------------------------------------------------------------------
# Server/client field-name parity: every key the SDK reads from a server
# response must actually appear in the matching handler's return dict.
# This is a static check — it greps the source so the audit stays honest
# even when the tests above are mocked.
# ---------------------------------------------------------------------------


def test_server_handlers_emit_every_field_the_sdk_reads():
    """Read the SDK + server source, collect the (endpoint, field) pairs
    the SDK reads, and verify each field appears in the matching handler's
    return shape. Keeps cycle-11-style regressions from slipping through."""
    import pathlib, re

    root = pathlib.Path(__file__).resolve().parent.parent
    server_src = (root / "src/api/server.py").read_text()

    # (endpoint_marker, required_fields_the_sdk_reads)
    # ``sandbox_id`` was renamed to ``sandbox_ref`` when the sandboxes
    # table was dropped (sandbox_state JSONB is now the single source of
    # truth) — the SDK reads ``sandbox_ref`` everywhere it used to read
    # ``sandbox_id``. See ``agent_sdk/client.py`` lines that call
    # ``data.get("sandbox_ref")``.
    expectations = [
        ("/sessions",
         ["agent_id", "sandbox_ref", "inner_session_id", "session_id"]),
        ("/sessions/{session_id}/resume",
         ["agent_id", "sandbox_ref", "inner_session_id"]),
        ("/sessions/{session_id}/message",
         ["rpc_id"]),
    ]

    for marker, required in expectations:
        # Grab the body of the handler by slicing from the decorator to
        # the next ``@app.`` boundary.
        idx = server_src.find(f'@app.post("{marker}")')
        assert idx != -1, f"handler {marker} not found in server.py"
        end = server_src.find("@app.", idx + 1)
        body = server_src[idx:end]
        for field in required:
            # Accept either ``"field"`` (dict literal key) or ``field=``
            # (kwarg) — either way the server emits it.
            pat = rf'["\']{field}["\']'
            assert re.search(pat, body), (
                f"{marker} handler does not emit '{field}' that the SDK reads. "
                f"This is the exact class of bug cycle 11 caught."
            )
