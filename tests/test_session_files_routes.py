"""Route-level tests for ``/sessions/{id}/files/*`` endpoints.

The session-scoped file API mirrors ``/sandboxes/{id}/files/*`` but
hides sandbox identity from callers. These tests prove each route:

1. Is registered on the FastAPI app.
2. Forwards through ``_proxy_from_session`` (which itself resolves
   the session's supervisor URL via the SessionPool).
3. Forwards the right verb/path/body to the sandbox's supervisor.

DB and provider calls are stubbed — ``_proxy_from_session`` and
``_download_from_session`` are monkeypatched, so no real sandbox
is provisioned.
"""
from __future__ import annotations

import json
import os
import sys
from dataclasses import dataclass
from typing import Any

import pytest
import pytest_asyncio
from fastapi import Response
from httpx import ASGITransport, AsyncClient

_SRC = os.path.join(os.path.dirname(__file__), "..", "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

from api import server as srv  # noqa: E402


@dataclass
class _FakeInstance:
    url: str = "http://sandbox.fake"
    provider: str = "test"


@pytest.fixture
def patched_helpers(monkeypatch):
    """Replace the two helpers so routes never touch DB or httpx.

    ``calls`` records every proxied request so assertions can inspect
    the exact verb + path + params + json the route tried to send to
    the supervisor, plus the session_id the route threaded through.
    """
    calls: list[dict[str, Any]] = []

    async def fake_proxy_from_session(session_id, method, path, *,
                                      params=None, json=None, timeout=30):
        calls.append({
            "kind": "proxy", "session_id": session_id,
            "method": method, "path": path,
            "params": params, "json": json, "timeout": timeout,
        })
        return Response(
            content=b'{"ok": true}', status_code=200, media_type="application/json",
        )

    async def fake_download_from_session(session_id, path):
        calls.append({"kind": "download", "session_id": session_id, "path": path})
        return Response(
            content=b"\x89PNG raw bytes",
            status_code=200,
            media_type="image/png",
            headers={"content-disposition": 'attachment; filename="img.png"'},
        )

    monkeypatch.setattr(srv, "_proxy_from_session", fake_proxy_from_session)
    monkeypatch.setattr(srv, "_download_from_session", fake_download_from_session)
    return calls


@pytest_asyncio.fixture
async def client():
    transport = ASGITransport(app=srv.app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


# ---------------------------------------------------------------------------
# Route registration
# ---------------------------------------------------------------------------


def test_session_file_routes_registered():
    """All seven session_file_* endpoints must be on the FastAPI app."""
    paths = {(r.path, tuple(sorted(r.methods or []))) for r in srv.app.routes}
    expected = {
        ("/sessions/{session_id}/files/tree", ("GET",)),
        ("/sessions/{session_id}/files/read", ("GET",)),
        ("/sessions/{session_id}/files/edit", ("POST",)),
        ("/sessions/{session_id}/files/upload", ("POST",)),
        ("/sessions/{session_id}/files/delete", ("POST",)),
        ("/sessions/{session_id}/files/rename", ("POST",)),
        ("/sessions/{session_id}/files/download", ("GET",)),
    }
    assert expected <= paths, f"missing: {expected - paths}"


# ---------------------------------------------------------------------------
# Proxy wiring (each route → supervisor path)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_tree_forwards_to_supervisor_tree(client, patched_helpers):
    r = await client.get("/sessions/s1/files/tree")
    assert r.status_code == 200
    assert patched_helpers[0]["kind"] == "proxy"
    assert patched_helpers[0]["session_id"] == "s1"
    assert patched_helpers[0]["method"] == "GET"
    assert patched_helpers[0]["path"] == "/v1/files/tree"
    assert patched_helpers[0]["params"] is None


@pytest.mark.asyncio
async def test_read_forwards_path_param(client, patched_helpers):
    r = await client.get("/sessions/s1/files/read", params={"path": "a.py"})
    assert r.status_code == 200
    assert patched_helpers[0]["method"] == "GET"
    assert patched_helpers[0]["path"] == "/v1/files/read"
    assert patched_helpers[0]["params"] == {"path": "a.py"}


@pytest.mark.asyncio
async def test_edit_forwards_json_body(client, patched_helpers):
    body = {"path": "a.py", "old_string": "x", "new_string": "y", "replace_all": True}
    r = await client.post("/sessions/s1/files/edit", json=body)
    assert r.status_code == 200
    assert patched_helpers[0]["method"] == "POST"
    assert patched_helpers[0]["path"] == "/v1/files/edit"
    assert patched_helpers[0]["json"] == body


@pytest.mark.asyncio
async def test_upload_forwards_base64_body_with_longer_timeout(client, patched_helpers):
    body = {"path": "CLAUDE.md", "content": "aGVsbG8="}
    r = await client.post("/sessions/s1/files/upload", json=body)
    assert r.status_code == 200
    assert patched_helpers[0]["method"] == "POST"
    assert patched_helpers[0]["path"] == "/v1/files/upload"
    assert patched_helpers[0]["json"] == body
    # Upload uses a larger timeout than the default 30s — matches the
    # sandbox route so big files don't time out.
    assert patched_helpers[0]["timeout"] == 60


@pytest.mark.asyncio
async def test_delete_forwards_path_body(client, patched_helpers):
    r = await client.post("/sessions/s1/files/delete", json={"path": "junk.txt"})
    assert r.status_code == 200
    assert patched_helpers[0]["method"] == "POST"
    assert patched_helpers[0]["path"] == "/v1/files/delete"
    assert patched_helpers[0]["json"] == {"path": "junk.txt"}


@pytest.mark.asyncio
async def test_rename_forwards_path_and_new_path(client, patched_helpers):
    body = {"path": "old.py", "new_path": "new.py"}
    r = await client.post("/sessions/s1/files/rename", json=body)
    assert r.status_code == 200
    assert patched_helpers[0]["method"] == "POST"
    assert patched_helpers[0]["path"] == "/v1/files/rename"
    assert patched_helpers[0]["json"] == body


@pytest.mark.asyncio
async def test_download_uses_dedicated_helper_and_returns_raw_bytes(client, patched_helpers):
    r = await client.get("/sessions/s1/files/download", params={"path": "img.png"})
    assert r.status_code == 200
    assert r.content == b"\x89PNG raw bytes"
    # Download has its own helper (forwards content-type + disposition),
    # so it does NOT go through _proxy_from_session.
    assert [c["kind"] for c in patched_helpers] == ["download"]
    assert patched_helpers[0] == {
        "kind": "download", "session_id": "s1", "path": "img.png",
    }


@pytest.mark.asyncio
async def test_session_id_is_threaded_to_resolver(client, patched_helpers):
    """The session_id in the URL must reach _proxy_from_session verbatim."""
    await client.get("/sessions/weird-id-123/files/tree")
    assert patched_helpers[0]["session_id"] == "weird-id-123"
