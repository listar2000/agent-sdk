"""Static UI routes: chat, validation dashboard, filesystem browser, volume
inspector. Self-contained (no ``api.server`` imports) — the first router
extracted out of the server monolith (refactor slice 6). Behavior identical:
same paths, same cached file serving.
"""
from pathlib import Path

from fastapi import APIRouter
from fastapi.responses import PlainTextResponse, Response

router = APIRouter()

# api/routers/ui.py -> parents[3] == repo root (where ``ui/`` lives). The
# former server.py copy used parents[2] because it sat one level shallower.
_UI_DIR = Path(__file__).parents[3] / "ui"
_UI_CACHE: dict[str, str] = {}


def _serve_ui_file(filename: str, label: str) -> Response:
    cached = _UI_CACHE.get(filename)
    if cached is None:
        try:
            cached = (_UI_DIR / filename).read_text()
        except FileNotFoundError:
            return PlainTextResponse(f"{label} not found", status_code=404)
        _UI_CACHE[filename] = cached
    return Response(content=cached, media_type="text/html")


@router.get("/ui")
async def serve_ui():
    """Serve the chat UI."""
    return _serve_ui_file("index.html", "UI")


@router.get("/ui/dashboard")
async def serve_dashboard():
    """Serve the validation dashboard."""
    return _serve_ui_file("dashboard.html", "Dashboard")


@router.get("/ui/files")
async def serve_files_ui():
    """Serve the filesystem browser UI."""
    return _serve_ui_file("fs.html", "Files UI")


@router.get("/ui/volumes")
async def serve_volumes_ui():
    """Serve the Volume Inspector UI."""
    return _serve_ui_file("volumes.html", "Volumes UI")
