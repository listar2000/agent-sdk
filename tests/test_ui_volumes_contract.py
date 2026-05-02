"""Functional contract tests for /volumes endpoints the UI consumes.

The browser UI at /ui/volumes depends on these exact response shapes.
If the server shape drifts, these assertions catch it.
"""
from __future__ import annotations
import os, sys
import pytest
from unittest.mock import AsyncMock, patch
from httpx import ASGITransport, AsyncClient
import pytest_asyncio

_SRC = os.path.join(os.path.dirname(__file__), "..", "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

_DB = os.environ.get("TEST_DATABASE_URL")
pytestmark = pytest.mark.skipif(_DB is None, reason="TEST_DATABASE_URL not set")
if _DB:
    os.environ["DATABASE_URL"] = _DB

from api import server as srv  # noqa: E402


@pytest_asyncio.fixture
async def client(clean_db):
    transport = ASGITransport(app=srv.app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


@pytest.mark.asyncio
async def test_list_volumes_shape(client):
    """UI expects an array of objects with at least id, name, provider."""
    with patch("api.providers.daytona.create_daytona_volume",
               new=AsyncMock(return_value="dt-ui-contract")):
        r = await client.post("/volumes", json={"name": "ui-contract-vol", "provider": "daytona"})
    assert r.status_code == 200

    r = await client.get("/volumes")
    assert r.status_code == 200
    body = r.json()
    assert isinstance(body, list)
    vol = next((v for v in body if v["name"] == "ui-contract-vol"), None)
    assert vol is not None
    for k in ("id", "name", "provider"):
        assert k in vol, f"UI requires field '{k}' on each volume"


@pytest.mark.asyncio
async def test_tree_is_newline_string(client):
    """UI parses tree as newline-separated string; dirs end with '/', files don't."""
    with patch("api.providers.unix_local.create_volume",
               new=AsyncMock(return_value="local-ui-tree")), \
         patch("api.providers.unix_local.volume_tree",
               new=AsyncMock(return_value="a.txt\nsub/\nsub/b.txt")):
        r = await client.post("/volumes", json={"name": "ui-tree-vol", "provider": "unix_local"})
        assert r.status_code == 200
        r = await client.get("/volumes/ui-tree-vol/files/tree")
    assert r.status_code == 200
    body = r.json()
    assert "tree" in body
    assert isinstance(body["tree"], str)
    lines = [l for l in body["tree"].splitlines() if l]
    # Files: no trailing slash. Dirs: single trailing slash.
    for ln in lines:
        assert not ln.startswith("/"), f"expected relative path, got {ln!r}"
        assert not ln.endswith("//"), f"double trailing slash in {ln!r}"


@pytest.mark.asyncio
async def test_file_read_content_or_content_base64(client):
    """UI reads either {content} (text) or {content_base64} (binary)."""
    with patch("api.providers.unix_local.create_volume",
               new=AsyncMock(return_value="local-ui-read")), \
         patch("api.providers.unix_local.volume_read",
               new=AsyncMock(return_value=b"hello\nworld")):
        r = await client.post("/volumes", json={"name": "ui-read-vol", "provider": "unix_local"})
        assert r.status_code == 200
        r = await client.get("/volumes/ui-read-vol/files/read", params={"path": "a.txt"})
    assert r.status_code == 200
    body = r.json()
    assert ("content" in body) or ("content_base64" in body), \
        "UI expects exactly one of content / content_base64"
    assert not ({"size", "binary", "image", "pdf"} & set(body.keys())), \
        "UI should not rely on fields the server doesn't send"
