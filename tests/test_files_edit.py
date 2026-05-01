"""Tests for POST /v1/files/edit endpoint in supervisor.js.

These tests spawn a real supervisor process but use a dummy ACP binary
(just `cat` which sits idle on stdin). No API keys needed — the edit
endpoint is pure filesystem, independent of the ACP agent.

Run: pytest tests/test_files_edit.py -v
"""
from __future__ import annotations

import asyncio
import json
import os
import shutil
import tempfile

import httpx
import pytest
import pytest_asyncio

_SUPERVISOR_DIR = os.path.join(os.path.dirname(__file__), "..", "src", "supervisor")
_SUPERVISOR_JS = os.path.join(_SUPERVISOR_DIR, "supervisor.js")

# Use `cat` as a dummy ACP binary — it just sits reading stdin forever,
# which is all we need since edit doesn't touch ACP at all.
_DUMMY_ACP = shutil.which("cat")


def _free_port() -> int:
    """Allocate a fresh ephemeral port. Required for xdist parallelism —
    a hardcoded port collides between workers."""
    import socket
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    p = s.getsockname()[1]
    s.close()
    return p


@pytest_asyncio.fixture
async def supervisor(tmp_path):
    """Spawn a supervisor with a temp root dir. Yields (url, root_path)."""
    port = _free_port()
    root = str(tmp_path)
    proc = await asyncio.create_subprocess_exec(
        "node", _SUPERVISOR_JS,
        "--port", str(port),
        "--host", "127.0.0.1",
        "--root", root,
        "--acp", _DUMMY_ACP,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.DEVNULL,
    )
    url = f"http://127.0.0.1:{port}"
    # Wait for health
    async with httpx.AsyncClient(timeout=5) as client:
        for _ in range(20):
            try:
                r = await client.get(f"{url}/v1/health")
                if r.status_code == 200:
                    break
            except httpx.ConnectError:
                pass
            await asyncio.sleep(0.2)
        else:
            proc.kill()
            pytest.fail("supervisor failed to start")

    yield url, tmp_path

    proc.terminate()
    try:
        await asyncio.wait_for(proc.wait(), timeout=5)
    except asyncio.TimeoutError:
        proc.kill()


async def _edit(url: str, path: str, old_string: str, new_string: str,
                replace_all: bool = False) -> dict:
    body = {"path": path, "old_string": old_string, "new_string": new_string}
    if replace_all:
        body["replace_all"] = True
    async with httpx.AsyncClient(timeout=10) as client:
        r = await client.post(f"{url}/v1/files/edit", json=body)
        return {"status": r.status_code, **r.json()}


@pytest.mark.asyncio
async def test_create_file(supervisor):
    url, root = supervisor
    result = await _edit(url, "hello.txt", "", "Hello World\nLine 2\n")
    assert result["ok"] is True
    assert result["created"] is True
    assert (root / "hello.txt").read_text() == "Hello World\nLine 2\n"


@pytest.mark.asyncio
async def test_create_nested_dirs(supervisor):
    url, root = supervisor
    result = await _edit(url, "src/deep/nested/file.py", "", "print('hi')\n")
    assert result["ok"] is True
    assert (root / "src" / "deep" / "nested" / "file.py").read_text() == "print('hi')\n"


@pytest.mark.asyncio
async def test_replace_unique(supervisor):
    url, root = supervisor
    (root / "test.txt").write_text("foo bar baz")
    result = await _edit(url, "test.txt", "bar", "qux")
    assert result["ok"] is True
    assert result["replacements"] == 1
    assert (root / "test.txt").read_text() == "foo qux baz"


@pytest.mark.asyncio
async def test_non_unique_errors(supervisor):
    url, root = supervisor
    (root / "test.txt").write_text("aaa bbb aaa")
    result = await _edit(url, "test.txt", "aaa", "ccc")
    assert "error" in result
    assert result["matches"] == 2


@pytest.mark.asyncio
async def test_replace_all(supervisor):
    url, root = supervisor
    (root / "test.txt").write_text("aaa bbb aaa")
    result = await _edit(url, "test.txt", "aaa", "ccc", replace_all=True)
    assert result["ok"] is True
    assert result["replacements"] == 2
    assert (root / "test.txt").read_text() == "ccc bbb ccc"


@pytest.mark.asyncio
async def test_delete_text(supervisor):
    url, root = supervisor
    (root / "test.txt").write_text("keep this remove this")
    result = await _edit(url, "test.txt", " remove this", "")
    assert result["ok"] is True
    assert (root / "test.txt").read_text() == "keep this"


@pytest.mark.asyncio
async def test_path_traversal_denied(supervisor):
    url, root = supervisor
    result = await _edit(url, "../../etc/passwd", "", "pwned")
    assert result["status"] == 403
    assert "traversal" in result["error"]


@pytest.mark.asyncio
async def test_old_string_not_found(supervisor):
    url, root = supervisor
    (root / "test.txt").write_text("hello")
    result = await _edit(url, "test.txt", "nonexistent", "x")
    assert "error" in result
    assert "not found" in result["error"]


@pytest.mark.asyncio
async def test_identical_strings_error(supervisor):
    url, root = supervisor
    (root / "test.txt").write_text("hello")
    result = await _edit(url, "test.txt", "hello", "hello")
    assert "error" in result
    assert "identical" in result["error"]


@pytest.mark.asyncio
async def test_file_not_found_for_edit(supervisor):
    url, root = supervisor
    result = await _edit(url, "missing.txt", "foo", "bar")
    assert result["status"] == 404


@pytest.mark.asyncio
async def test_overwrite_existing_file(supervisor):
    url, root = supervisor
    (root / "test.txt").write_text("old content")
    result = await _edit(url, "test.txt", "", "new content")
    assert result["ok"] is True
    assert (root / "test.txt").read_text() == "new content"
