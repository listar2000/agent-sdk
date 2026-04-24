"""SDK/server field-name drift regressions (cycle-12 audit).

Cycle 11 caught one drift where the client read ``sandbox_id`` but the
server emitted ``current_sandbox_id`` from ``/sessions/quick``. These
tests lock in the sibling invariants we verified during the cycle-12
audit so the same class of bug can't regress silently:

* ``Volume(**server_payload)`` tolerates extra fields (the server keeps
  growing ``VolumeRecord`` — e.g. ``supervisor_agent_types``). A strict
  ``__init__`` would blow up on any new server column.
* ``configure()`` / ``cancel()`` surface the server's ``{"error": "..."}``
  body instead of swallowing it into a bare ``HTTPStatusError: 400``.
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
# Volume: tolerate forward-compatible server fields.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_volumes_create_ignores_unknown_server_fields():
    """Server's VolumeRecord has ``supervisor_agent_types`` (and may grow
    more fields). The SDK must not ``TypeError`` on ``Volume(**payload)``.
    """
    from agent_sdk.client import Client, Volume

    payload = {
        "id": "vol_x",
        "name": "p",
        "provider": "daytona",
        "provider_ref": "dt-x",
        "status": "ready",
        # Future/extra fields the server emits today or tomorrow:
        "supervisor_agent_types": ["claude", "codex"],
        "created_at": "2026-04-22T00:00:00Z",
    }

    c = Client(base_url="http://fake")
    with patch.object(c._http, "post",
                      AsyncMock(return_value=_fake_response(200, payload))):
        v = await c.volumes.create(name="p", provider="daytona")
    assert isinstance(v, Volume)
    assert v.id == "vol_x"
    # The extra field must not have been passed to __init__ (AttributeError
    # here is expected — the dataclass intentionally doesn't model it).
    assert not hasattr(v, "supervisor_agent_types")
    await c.close()


@pytest.mark.asyncio
async def test_volumes_list_ignores_unknown_server_fields():
    from agent_sdk.client import Client, Volume

    payload = [{
        "id": "vol_1",
        "name": "n",
        "provider": "docker",
        "provider_ref": "docker-1",
        "status": "ready",
        "supervisor_agent_types": [],
        "whatever_new_column": 42,
    }]

    c = Client(base_url="http://fake")
    with patch.object(c._http, "get",
                      AsyncMock(return_value=_fake_response(200, payload))):
        vs = await c.volumes.list()
    assert len(vs) == 1 and isinstance(vs[0], Volume)
    assert vs[0].name == "n"
    await c.close()


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
    with patch.object(agent._client, "post", AsyncMock(return_value=resp)):
        with pytest.raises(httpx.HTTPStatusError) as exc_info:
            await agent.configure(mode="acceptEdits")
    # Cycle-11-style fix: error message must include server detail, not
    # just "HTTP 502".
    assert "supervisor unreachable: boom" in str(exc_info.value)
    await agent._client.aclose()


@pytest.mark.asyncio
async def test_cancel_surfaces_server_error_message():
    from agent_sdk.client import Agent

    agent = Agent("err-cancel", api_url="http://localhost:7778")
    agent._registered = True
    agent.session_id = "s-2"
    agent.id = "err-cancel"

    resp = _fake_response(504, {"error": "cancel timed out"})
    with patch.object(agent._client, "post", AsyncMock(return_value=resp)):
        with pytest.raises(httpx.HTTPStatusError) as exc_info:
            await agent.cancel()
    assert "cancel timed out" in str(exc_info.value)
    await agent._client.aclose()


@pytest.mark.asyncio
async def test_plain_register_surfaces_server_error_message():
    """``/agents`` registration (no provider, no session) used to call
    ``resp.raise_for_status()`` directly, hiding the server's error body.
    """
    from agent_sdk.client import Agent

    agent = Agent("err-reg", api_url="http://localhost:7778")

    resp = _fake_response(400, {"error": "name required"})
    with patch.object(agent._client, "post", AsyncMock(return_value=resp)):
        with pytest.raises(httpx.HTTPStatusError) as exc_info:
            await agent._ensure_registered()
    assert "name required" in str(exc_info.value)
    await agent._client.aclose()


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
    expectations = [
        ("/sessions",
         ["agent_id", "sandbox_id", "inner_session_id", "session_id"]),
        ("/sessions/{session_id}/resume",
         ["agent_id", "sandbox_id", "inner_session_id"]),
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
