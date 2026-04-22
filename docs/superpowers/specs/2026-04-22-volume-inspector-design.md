# Volume Inspector UI — Design Spec

**Date:** 2026-04-22  
**Status:** Approved

## Overview

A read-only browser-based UI for inspecting volumes managed by the agent-sdk server. Accessible at `GET /ui/volumes` on the running server (default `http://localhost:7778/ui/volumes`). Implemented as a single self-contained HTML file (`ui/volumes.html`) with no external dependencies, matching the dark theme of the existing `ui/fs.html` and `ui/index.html`.

## Layout

Three-panel layout within a full-height shell, all panels always visible:

```
┌─────────────────────────────────────────────────────────┐
│ Header: API URL | Provider selector | Refresh | Status  │
├──────────────┬──────────────────┬────────────────────────┤
│ Volumes list │  File tree       │  File viewer           │
│ (~200px)     │  (~240px)        │  (flex)                │
│              │                  │                        │
│ Volume cards │ Collapsible      │ Path + size header     │
│ name         │ folder/file tree │ Line-numbered content  │
│ provider tag │ for selected vol │                        │
│ short ID     │                  │                        │
│              │ Empty state      │ Empty state until      │
│              │ until vol select │ file selected          │
└──────────────┴──────────────────┴────────────────────────┘
```

Drag handles between panels 1–2 and 2–3 allow width adjustment.

## Header

- **API URL** text input — defaults to `http://localhost:7778`
- **Provider** dropdown — options: All, local, docker, daytona
- **Refresh** button — reloads volume list
- **Status pill** — shows count (e.g. "3 volumes") or error state

Provider change triggers an automatic volume list reload.

## Panel 1 — Volume List

- Calls `GET /volumes` (with `?provider=<p>` when not "All")
- Each volume shown as a card: name (bold), provider badge, truncated ID
- Clicking a volume selects it (highlighted) and loads its file tree
- Auto-refreshes list every 15 seconds (silent, no spinner)

## Panel 2 — File Tree

- Calls `GET /volumes/{id}/files/tree` when a volume is selected
- Same collapsible folder tree as `fs.html`: click folder to expand/collapse, click file to load
- File nodes show size
- Shows spinner while loading, error message on failure
- Auto-refreshes tree every 15 seconds (silent) when a volume is selected

## Panel 3 — File Viewer

- Calls `GET /volumes/{id}/files/read?path=<path>` when a file is clicked
- Header shows filename and formatted file size
- Content rendered as line-numbered code view (same style as `fs.html`)
- Handles text/code, markdown (rendered), images (displayed), binary (placeholder)
- No edit controls — read-only

## Server Change

Add one route to `src/api/server.py`:

```python
@app.get("/ui/volumes")
async def serve_volumes_ui():
    return _serve_ui_file("volumes.html", "Volumes UI")
```

## API Endpoints Used

| Method | Path | Purpose |
|--------|------|---------|
| GET | `/volumes?provider=<p>` | List volumes (provider optional) |
| GET | `/volumes/{id}/files/tree` | Load file tree |
| GET | `/volumes/{id}/files/read?path=<p>` | Read file content |

## Out of Scope

- Creating, deleting, or editing volumes/files
- Daytona-specific volume metadata beyond what `/volumes` returns
- Authentication/authorization
