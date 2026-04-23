"""Smoke test: GET /ui/volumes returns 200 HTML."""
from __future__ import annotations
import os, sys
import pytest

_SRC = os.path.join(os.path.dirname(__file__), "..", "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

from httpx import ASGITransport, AsyncClient
from api import server as srv


@pytest.mark.asyncio
async def test_ui_volumes_route_returns_html():
    transport = ASGITransport(app=srv.app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        r = await c.get("/ui/volumes")
    assert r.status_code == 200
    assert "text/html" in r.headers["content-type"]
    assert "Volume Inspector" in r.text
