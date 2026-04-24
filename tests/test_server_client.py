"""Unit tests for ``agent_sdk.ServerClient``.

Uses ``httpx.MockTransport`` — no real server, no DB, no provider.
Asserts each method sends the right HTTP verb + path + body to the
agent-sdk REST API, and parses the response back correctly.

The point of these tests is to lock the wire contract between the
client and the server. If a method silently sends the wrong verb or
path, these tests catch it before the PR lands. They don't test the
server's behaviour — that's what the golden-suite in
test_sandbox_stop_delete_recovery.py is for.
"""
from __future__ import annotations

import json
import os
import sys
from typing import Any

import httpx
import pytest

_SRC = os.path.join(os.path.dirname(__file__), "..", "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

from agent_sdk.server_client import ServerClient  # noqa: E402


# ---------------------------------------------------------------------------
# Transport helper: record + respond
# ---------------------------------------------------------------------------


class _Recorder:
    """Capture the last request a method sent + return a canned response."""

    def __init__(self, response: dict | list | bytes | None = None, status: int = 200,
                 content_type: str = "application/json") -> None:
        self.response = response
        self.status = status
        self.content_type = content_type
        self.requests: list[httpx.Request] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        if self.response is None:
            return httpx.Response(self.status)
        if isinstance(self.response, (dict, list)):
            return httpx.Response(
                self.status,
                headers={"content-type": self.content_type},
                content=json.dumps(self.response).encode(),
            )
        return httpx.Response(
            self.status,
            headers={"content-type": self.content_type},
            content=self.response,
        )

    @property
    def last(self) -> httpx.Request:
        assert self.requests, "no request captured"
        return self.requests[-1]


def _make_client(recorder: _Recorder) -> ServerClient:
    """Build a ServerClient whose transport is the recorder."""
    http = httpx.AsyncClient(
        base_url="http://test",
        headers={"Accept": "application/json", "Authorization": "Bearer testtoken"},
        transport=httpx.MockTransport(recorder),
        timeout=httpx.Timeout(5.0, read=None),
    )
    return ServerClient("http://test", http_client=http)


# ---------------------------------------------------------------------------
# Volumes + volume files
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_create_volume():
    rec = _Recorder({"id": "v1"})
    async with _make_client(rec) as sc:
        await sc.create_volume(name="vol-a", provider="daytona")
    assert rec.last.method == "POST"
    assert rec.last.url.path == "/volumes"
    assert json.loads(rec.last.content) == {"name": "vol-a", "provider": "daytona"}


@pytest.mark.asyncio
async def test_list_volumes_forwards_provider_filter():
    rec = _Recorder([])
    async with _make_client(rec) as sc:
        await sc.list_volumes(provider="docker")
    assert rec.last.url.path == "/volumes"
    assert dict(rec.last.url.params) == {"provider": "docker"}


@pytest.mark.asyncio
async def test_get_volume():
    rec = _Recorder({"id": "v1"})
    async with _make_client(rec) as sc:
        await sc.get_volume("v1")
    assert rec.last.url.path == "/volumes/v1"


@pytest.mark.asyncio
async def test_delete_volume_force():
    rec = _Recorder(None)
    async with _make_client(rec) as sc:
        await sc.delete_volume("v1", force=True)
    assert rec.last.method == "DELETE"
    assert dict(rec.last.url.params) == {"force": "true"}


@pytest.mark.asyncio
async def test_volume_file_tree_optional_path():
    rec = _Recorder({"tree": []})
    async with _make_client(rec) as sc:
        await sc.volume_file_tree("v1", path="shared/42")
    assert rec.last.url.path == "/volumes/v1/files/tree"
    assert dict(rec.last.url.params) == {"path": "shared/42"}


@pytest.mark.asyncio
async def test_volume_file_read():
    rec = _Recorder({"content": "hello"})
    async with _make_client(rec) as sc:
        await sc.volume_file_read("v1", "a.txt")
    assert rec.last.url.path == "/volumes/v1/files/read"
    assert dict(rec.last.url.params) == {"path": "a.txt"}


@pytest.mark.asyncio
async def test_volume_file_write_posts_content():
    rec = _Recorder(None, status=204, content_type="")
    async with _make_client(rec) as sc:
        await sc.volume_file_write("v1", "a.txt", content="hello")
    assert rec.last.method == "POST"
    assert rec.last.url.path == "/volumes/v1/files/edit"
    assert json.loads(rec.last.content) == {"path": "a.txt", "content": "hello"}


@pytest.mark.asyncio
async def test_volume_file_edit_string_replace():
    rec = _Recorder(None, status=204, content_type="")
    async with _make_client(rec) as sc:
        await sc.volume_file_edit(
            "v1", "a.txt", old_string="foo", new_string="bar", replace_all=True,
        )
    body = json.loads(rec.last.content)
    assert body == {
        "path": "a.txt", "old_string": "foo", "new_string": "bar", "replace_all": True,
    }


# ---------------------------------------------------------------------------
# Session filesystem (session_id addresses the current sandbox)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_session_file_tree_gets_sessions_files_tree():
    rec = _Recorder([{"name": "a.py", "path": "a.py", "type": "file"}])
    async with _make_client(rec) as sc:
        out = await sc.session_file_tree("s1")
    assert out == [{"name": "a.py", "path": "a.py", "type": "file"}]
    assert rec.last.method == "GET"
    assert rec.last.url.path == "/sessions/s1/files/tree"


@pytest.mark.asyncio
async def test_session_file_read_passes_path_param():
    rec = _Recorder({"content": "hi", "path": "a.py"})
    async with _make_client(rec) as sc:
        await sc.session_file_read("s1", "a.py")
    assert rec.last.method == "GET"
    assert rec.last.url.path == "/sessions/s1/files/read"
    assert dict(rec.last.url.params) == {"path": "a.py"}


@pytest.mark.asyncio
async def test_session_file_edit_replace_all_flag_only_when_true():
    rec = _Recorder({"ok": True})
    async with _make_client(rec) as sc:
        await sc.session_file_edit("s1", "a.py", old_string="x", new_string="y")
    body = json.loads(rec.last.content)
    assert body == {"path": "a.py", "old_string": "x", "new_string": "y"}
    assert rec.last.method == "POST"
    assert rec.last.url.path == "/sessions/s1/files/edit"

    rec = _Recorder({"ok": True})
    async with _make_client(rec) as sc:
        await sc.session_file_edit("s1", "a.py", old_string="x", new_string="y", replace_all=True)
    assert json.loads(rec.last.content)["replace_all"] is True


@pytest.mark.asyncio
async def test_session_file_upload_carries_base64_body():
    rec = _Recorder({"ok": True})
    async with _make_client(rec) as sc:
        await sc.session_file_upload("s1", "CLAUDE.md", "aGVsbG8=")
    assert rec.last.method == "POST"
    assert rec.last.url.path == "/sessions/s1/files/upload"
    assert json.loads(rec.last.content) == {"path": "CLAUDE.md", "content": "aGVsbG8="}


@pytest.mark.asyncio
async def test_session_file_delete_posts_path_body():
    rec = _Recorder({"ok": True})
    async with _make_client(rec) as sc:
        await sc.session_file_delete("s1", "junk.txt")
    assert rec.last.method == "POST"
    assert rec.last.url.path == "/sessions/s1/files/delete"
    assert json.loads(rec.last.content) == {"path": "junk.txt"}


@pytest.mark.asyncio
async def test_session_file_rename_posts_path_and_new_path():
    rec = _Recorder({"ok": True})
    async with _make_client(rec) as sc:
        await sc.session_file_rename("s1", "old.py", "new.py")
    assert rec.last.method == "POST"
    assert rec.last.url.path == "/sessions/s1/files/rename"
    assert json.loads(rec.last.content) == {"path": "old.py", "new_path": "new.py"}


@pytest.mark.asyncio
async def test_session_file_download_returns_raw_bytes():
    raw = b"\x89PNG\r\n\x1a\n" + b"\x00" * 10
    rec = _Recorder(raw, content_type="application/octet-stream")
    async with _make_client(rec) as sc:
        out = await sc.session_file_download("s1", "img.png")
    assert out == raw
    assert rec.last.url.path == "/sessions/s1/files/download"
    assert dict(rec.last.url.params) == {"path": "img.png"}


# ---------------------------------------------------------------------------
# Sessions lifecycle
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_create_session_passthrough():
    rec = _Recorder({"session_id": "s1", "sandbox_id": "sb_1"})
    async with _make_client(rec) as sc:
        await sc.create_session(provider="daytona", provision=False, foo="bar")
    assert rec.last.method == "POST"
    assert rec.last.url.path == "/sessions"
    body = json.loads(rec.last.content)
    assert body == {"provider": "daytona", "provision": False, "foo": "bar"}


@pytest.mark.asyncio
async def test_get_session_log_limit_and_unwraps_events():
    rec = _Recorder({"events": [{"e": 1}, {"e": 2}]})
    async with _make_client(rec) as sc:
        evs = await sc.get_session_log("s1", limit=100)
    assert evs == [{"e": 1}, {"e": 2}]
    assert rec.last.url.path == "/sessions/s1/log"
    assert dict(rec.last.url.params) == {"limit": "100"}


# ---------------------------------------------------------------------------
# Sessions runtime
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_send_message_default_no_interrupt():
    rec = _Recorder({"rpc_id": "r1"})
    async with _make_client(rec) as sc:
        await sc.send_message("s1", "hello")
    body = json.loads(rec.last.content)
    assert body == {"message": "hello", "interrupt": False}


@pytest.mark.asyncio
async def test_send_message_interrupt_true():
    rec = _Recorder({"rpc_id": "r2"})
    async with _make_client(rec) as sc:
        await sc.send_message("s1", "stop", interrupt=True)
    assert json.loads(rec.last.content)["interrupt"] is True


@pytest.mark.asyncio
async def test_cancel_session():
    rec = _Recorder({"ok": True})
    async with _make_client(rec) as sc:
        await sc.cancel_session("s1")
    assert rec.last.method == "POST"
    assert rec.last.url.path == "/sessions/s1/cancel"


# ---------------------------------------------------------------------------
# Ghost endpoints must raise, not silently no-op
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_delete_session_raises_not_implemented():
    rec = _Recorder(None)
    async with _make_client(rec) as sc:
        with pytest.raises(NotImplementedError, match="DELETE /sessions"):
            await sc.delete_session("s1")
    # Critically: no HTTP call was made.
    assert rec.requests == []


# ---------------------------------------------------------------------------
# SSE: raw bytes streaming
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_stream_events_yields_raw_bytes():
    sse_bytes = b"event: rpc:r1\ndata: {\"a\":1}\n\n"

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/sessions/s1/events"
        assert request.headers.get("accept") == "text/event-stream"
        return httpx.Response(
            200,
            headers={"content-type": "text/event-stream"},
            content=sse_bytes,
        )

    http = httpx.AsyncClient(
        base_url="http://test",
        transport=httpx.MockTransport(handler),
        timeout=httpx.Timeout(5.0, read=None),
    )
    async with ServerClient("http://test", http_client=http) as sc:
        chunks = []
        async for chunk in sc.stream_events("s1"):
            chunks.append(chunk)
        assert b"".join(chunks) == sse_bytes


# ---------------------------------------------------------------------------
# Error mapping
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_http_error_surfaces_server_detail():
    rec = _Recorder({"error": "volume not found"}, status=404)
    async with _make_client(rec) as sc:
        with pytest.raises(httpx.HTTPStatusError) as excinfo:
            await sc.get_volume("vX")
    assert "404" in str(excinfo.value)
    assert "volume not found" in str(excinfo.value)


# ---------------------------------------------------------------------------
# Auth header — token baked at construction
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_token_attached_as_bearer_header():
    """Default constructor (no http_client override) attaches bearer."""
    rec = _Recorder({"ok": True})
    http = httpx.AsyncClient(
        base_url="http://test",
        headers={"Authorization": "Bearer s3cret"},
        transport=httpx.MockTransport(rec),
        timeout=httpx.Timeout(5.0, read=None),
    )
    async with ServerClient("http://test", http_client=http) as sc:
        await sc.list_sessions()
    assert rec.last.headers.get("authorization") == "Bearer s3cret"
