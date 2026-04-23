# Volume Inspector UI — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a read-only three-panel volume inspector UI served at `GET /ui/volumes` on the running server (default `http://localhost:7778/ui/volumes`).

**Architecture:** A single self-contained `ui/volumes.html` file (no external dependencies) served via a new FastAPI route. Three panels — volumes list, file tree, file viewer — using the same dark-theme CSS variables as `ui/fs.html`.

**Tech Stack:** Vanilla HTML/CSS/JS, FastAPI (one new route), pytest + httpx for the route test.

---

## File Structure

| Action | Path | Responsibility |
|--------|------|----------------|
| Create | `ui/volumes.html` | Full three-panel Volume Inspector UI |
| Modify | `src/api/server.py:3338` | Add `GET /ui/volumes` route after existing `/ui/files` |
| Create | `tests/test_ui_volumes_route.py` | Verify route returns 200 HTML |

---

### Task 1: Add `/ui/volumes` route and test

**Files:**
- Modify: `src/api/server.py` (after line 3338)
- Create: `tests/test_ui_volumes_route.py`

- [ ] **Step 1: Write the failing test**

Create `tests/test_ui_volumes_route.py`:

```python
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
```

- [ ] **Step 2: Run test — verify it fails**

```bash
cd /Users/tianhaowu/mono/agent-sdk
pytest tests/test_ui_volumes_route.py -v
```

Expected: FAIL — route not found (404) or volumes.html not found (404 from `_serve_ui_file`).

- [ ] **Step 3: Add the route to server.py**

In `src/api/server.py`, after line 3338 (the `serve_files_ui` function body), add:

```python
@app.get("/ui/volumes")
async def serve_volumes_ui():
    """Serve the Volume Inspector UI."""
    return _serve_ui_file("volumes.html", "Volumes UI")
```

- [ ] **Step 4: Create a minimal stub `ui/volumes.html`** so the route returns 200

Create `ui/volumes.html` with just enough to pass the test:

```html
<!DOCTYPE html>
<html lang="en">
<head><meta charset="UTF-8"><title>Volume Inspector</title></head>
<body><h1>Volume Inspector</h1></body>
</html>
```

- [ ] **Step 5: Run test — verify it passes**

```bash
pytest tests/test_ui_volumes_route.py -v
```

Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add src/api/server.py tests/test_ui_volumes_route.py ui/volumes.html
git commit -m "feat(ui): add /ui/volumes route + smoke test"
```

---

### Task 2: Build the complete `ui/volumes.html`

**Files:**
- Replace: `ui/volumes.html`

- [ ] **Step 1: Replace the stub with the complete HTML file**

Overwrite `ui/volumes.html` with the full content below. This is the complete file — copy verbatim:

```html
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Volume Inspector</title>
<style>
  :root {
    --app-bg: #0d0f13;
    --chrome-bg: #12151b;
    --panel-bg: #171b22;
    --panel-elev: #1d222b;
    --panel-elev-2: #222834;
    --editor-bg: #101319;
    --border: #2a303a;
    --border-soft: #232934;
    --text: #d9dee7;
    --text-muted: #9ca5b4;
    --text-faint: #7b8492;
    --accent: #6b91ff;
    --accent-strong: #7ca6ff;
    --success: #4bb382;
    --warning: #d2a254;
    --danger: #d46b6b;
    --shadow: 0 22px 70px rgba(0, 0, 0, 0.45);
    --radius-xl: 18px;
    --radius-lg: 13px;
    --radius-md: 10px;
    --radius-sm: 8px;
  }

  * { box-sizing: border-box; margin: 0; padding: 0; }

  html, body { height: 100%; }

  body {
    font-family: "SF Pro Text", "Inter", "Segoe UI", sans-serif;
    background:
      radial-gradient(circle at top left, rgba(107, 145, 255, 0.12), transparent 28%),
      radial-gradient(circle at bottom right, rgba(74, 104, 163, 0.15), transparent 26%),
      linear-gradient(180deg, #0d1014 0%, #0a0c10 100%);
    color: var(--text);
    padding: 14px;
    overflow: hidden;
  }

  body::before {
    content: "";
    position: fixed;
    inset: 0;
    background-image:
      linear-gradient(rgba(255, 255, 255, 0.02) 1px, transparent 1px),
      linear-gradient(90deg, rgba(255, 255, 255, 0.02) 1px, transparent 1px);
    background-size: 24px 24px;
    pointer-events: none;
    opacity: 0.18;
  }

  #shell {
    position: relative;
    z-index: 1;
    width: 100%;
    height: calc(100vh - 28px);
    margin: 0 auto;
    display: flex;
    flex-direction: column;
    border: 1px solid var(--border);
    border-radius: var(--radius-xl);
    background:
      linear-gradient(180deg, rgba(255, 255, 255, 0.02), transparent 24%),
      var(--panel-bg);
    box-shadow: var(--shadow);
    overflow: hidden;
  }

  #shell::before {
    content: "";
    position: absolute;
    inset: 0;
    background:
      linear-gradient(180deg, rgba(255, 255, 255, 0.03), transparent 8%),
      radial-gradient(circle at top, rgba(124, 166, 255, 0.08), transparent 40%);
    pointer-events: none;
  }

  .topbar {
    position: relative;
    padding: 10px 12px 9px;
    border-bottom: 1px solid var(--border-soft);
    background: rgba(12, 14, 18, 0.78);
    backdrop-filter: blur(18px);
    z-index: 10;
  }

  .topbar-row {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 10px;
    margin-bottom: 9px;
  }

  .title-stack {
    display: flex;
    align-items: center;
    gap: 9px;
    min-width: 0;
  }

  .title-badge {
    width: 28px;
    height: 28px;
    display: grid;
    place-items: center;
    border-radius: 9px;
    border: 1px solid var(--border);
    background:
      linear-gradient(180deg, rgba(124, 166, 255, 0.18), rgba(124, 166, 255, 0.05)),
      var(--panel-elev);
    color: var(--accent-strong);
    font-size: 14px;
  }

  .title-copy h1 {
    font-size: 13px;
    font-weight: 600;
    letter-spacing: 0.01em;
  }

  .controls {
    display: grid;
    grid-template-columns: 3fr 1.4fr auto auto;
    gap: 8px;
    align-items: end;
  }

  .field {
    display: flex;
    flex-direction: column;
    gap: 4px;
  }

  .field-label {
    color: var(--text-faint);
    font-size: 10px;
    letter-spacing: 0.04em;
    text-transform: uppercase;
  }

  .field input, .field select {
    width: 100%;
    min-width: 0;
    border: 1px solid var(--border);
    border-radius: 9px;
    background: var(--editor-bg);
    color: var(--text);
    padding: 8px 9px;
    font-size: 12px;
    outline: none;
    font-family: inherit;
  }

  .field select option { background: var(--editor-bg); }

  .field input:focus, .field select:focus {
    border-color: #4560a6;
    box-shadow: 0 0 0 1px rgba(69, 96, 166, 0.35);
  }

  button {
    border: 1px solid var(--border);
    border-radius: 9px;
    background: var(--panel-elev);
    color: var(--text);
    cursor: pointer;
    transition: background 120ms ease, border-color 120ms ease, transform 120ms ease;
    padding: 8px 14px;
    font-size: 12px;
    font-family: inherit;
    white-space: nowrap;
  }

  button:hover { background: var(--panel-elev-2); border-color: #364050; }
  button:active { transform: translateY(1px); }

  button.primary {
    border-color: rgba(107, 145, 255, 0.35);
    background:
      linear-gradient(180deg, rgba(107, 145, 255, 0.18), rgba(107, 145, 255, 0.08)),
      var(--panel-elev);
  }

  button:disabled { opacity: 0.5; cursor: not-allowed; }

  .status-pill {
    padding: 7px 11px;
    border-radius: 9px;
    border: 1px solid var(--border);
    background: var(--editor-bg);
    font-size: 12px;
    color: var(--text-muted);
    white-space: nowrap;
  }

  .status-pill.ok { color: var(--success); border-color: rgba(75, 179, 130, 0.3); }
  .status-pill.err { color: var(--danger); border-color: rgba(212, 107, 107, 0.3); }

  #content {
    position: relative;
    flex: 1;
    display: flex;
    overflow: hidden;
  }

  /* Panel 1 — Volumes list */
  #volumes-panel {
    width: 200px;
    min-width: 120px;
    background: var(--editor-bg);
    border-right: 1px solid var(--border-soft);
    overflow-y: auto;
    padding: 8px 0;
    flex-shrink: 0;
  }

  #volumes-panel::-webkit-scrollbar { width: 10px; }
  #volumes-panel::-webkit-scrollbar-thumb {
    background: #252c36;
    border: 2px solid transparent;
    border-radius: 999px;
    background-clip: padding-box;
  }

  .vol-card {
    padding: 8px 12px;
    cursor: pointer;
    transition: background 80ms ease;
    border-bottom: 1px solid var(--border-soft);
  }

  .vol-card:last-child { border-bottom: none; }
  .vol-card:hover { background: rgba(124, 166, 255, 0.07); }

  .vol-card.selected {
    background: rgba(107, 145, 255, 0.14);
    border-left: 2px solid var(--accent);
    padding-left: 10px;
  }

  .vol-name {
    font-size: 12px;
    font-weight: 600;
    color: var(--text);
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    margin-bottom: 4px;
  }

  .vol-card.selected .vol-name { color: var(--accent-strong); }

  .vol-meta {
    display: flex;
    align-items: center;
    gap: 5px;
    flex-wrap: wrap;
  }

  .vol-badge {
    font-size: 10px;
    padding: 1px 6px;
    border-radius: 5px;
    font-weight: 500;
    letter-spacing: 0.02em;
  }

  .vol-badge.local { background: rgba(75, 179, 130, 0.18); color: #5ecf9a; border: 1px solid rgba(75, 179, 130, 0.3); }
  .vol-badge.docker { background: rgba(107, 145, 255, 0.18); color: var(--accent-strong); border: 1px solid rgba(107, 145, 255, 0.3); }
  .vol-badge.daytona { background: rgba(210, 162, 84, 0.18); color: #e8b96a; border: 1px solid rgba(210, 162, 84, 0.3); }
  .vol-badge.other { background: rgba(156, 165, 180, 0.15); color: var(--text-muted); border: 1px solid var(--border); }

  .vol-id {
    font-size: 10px;
    color: var(--text-faint);
    font-family: "SF Mono", "Fira Code", monospace;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    max-width: 100%;
  }

  /* Panel 2 — File tree */
  #tree-panel {
    width: 240px;
    min-width: 140px;
    background: var(--editor-bg);
    border-right: 1px solid var(--border-soft);
    overflow-y: auto;
    padding: 8px 0;
    flex-shrink: 0;
  }

  #tree-panel::-webkit-scrollbar { width: 10px; }
  #tree-panel::-webkit-scrollbar-thumb {
    background: #252c36;
    border: 2px solid transparent;
    border-radius: 999px;
    background-clip: padding-box;
  }

  .tree-node {
    display: flex;
    align-items: center;
    gap: 6px;
    padding: 5px 12px;
    cursor: pointer;
    user-select: none;
    transition: background 80ms ease;
    font-size: 12px;
    line-height: 1.4;
  }

  .tree-node:hover { background: rgba(124, 166, 255, 0.08); }
  .tree-node.selected { background: rgba(107, 145, 255, 0.15); color: var(--accent-strong); }
  .tree-node.directory { font-weight: 500; }
  .tree-icon { flex-shrink: 0; font-size: 14px; }
  .tree-name { flex: 1; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
  .tree-size { flex-shrink: 0; color: var(--text-faint); font-size: 11px; }
  .tree-children { padding-left: 16px; }

  /* Drag handles */
  .drag-handle {
    width: 4px;
    background: var(--border-soft);
    cursor: col-resize;
    flex-shrink: 0;
    position: relative;
    transition: background 120ms ease;
  }

  .drag-handle:hover, .drag-handle.dragging { background: var(--accent); }

  /* Panel 3 — Viewer */
  #viewer {
    flex: 1;
    background: var(--panel-bg);
    overflow: hidden;
    display: flex;
    flex-direction: column;
    min-width: 200px;
  }

  #viewer-header {
    padding: 10px 14px;
    border-bottom: 1px solid var(--border-soft);
    background: var(--editor-bg);
  }

  #viewer-header h2 { font-size: 12px; font-weight: 600; color: var(--text); margin-bottom: 3px; }
  #viewer-header p { font-size: 11px; color: var(--text-muted); }

  #viewer-content {
    flex: 1;
    overflow: auto;
    padding: 14px;
  }

  #viewer-content::-webkit-scrollbar { width: 10px; }
  #viewer-content::-webkit-scrollbar-thumb {
    background: #252c36;
    border: 2px solid transparent;
    border-radius: 999px;
    background-clip: padding-box;
  }

  .empty-state {
    height: 100%;
    display: grid;
    place-items: center;
    text-align: center;
    color: var(--text-muted);
    font-size: 13px;
  }

  .spinner {
    display: inline-block;
    width: 20px;
    height: 20px;
    border: 2px solid var(--border);
    border-top-color: var(--accent);
    border-radius: 50%;
    animation: spin 0.8s linear infinite;
  }

  @keyframes spin { to { transform: rotate(360deg); } }

  .code-block {
    position: relative;
    background: var(--editor-bg);
    border: 1px solid var(--border-soft);
    border-radius: var(--radius-md);
    overflow: hidden;
  }

  .code-content { overflow: auto; padding: 12px 0; }

  .code-content pre {
    margin: 0;
    padding: 0;
    font-family: "SF Mono", "Fira Code", "Consolas", monospace;
    font-size: 12px;
    line-height: 1.6;
    color: var(--text);
  }

  .code-line { display: flex; padding: 0 12px; }
  .code-line:hover { background: rgba(124, 166, 255, 0.05); }
  .line-number { min-width: 40px; text-align: right; color: var(--text-faint); user-select: none; padding-right: 12px; }
  .line-content { flex: 1; white-space: pre; }

  .markdown-content { color: var(--text); line-height: 1.6; font-size: 14px; }
  .markdown-content > :first-child { margin-top: 0; }
  .markdown-content h1, .markdown-content h2, .markdown-content h3 {
    margin: 1.2em 0 0.6em; color: var(--accent-strong); font-weight: 600;
  }
  .markdown-content h1 { font-size: 1.6rem; }
  .markdown-content h2 { font-size: 1.4rem; }
  .markdown-content h3 { font-size: 1.2rem; }
  .markdown-content p { margin: 0.85em 0; }
  .markdown-content ul, .markdown-content ol { margin: 0.85em 0; padding-left: 1.5rem; }
  .markdown-content li { margin: 0.3rem 0; }
  .markdown-content code {
    font-family: "SF Mono", "Fira Code", monospace;
    font-size: 12px; padding: 0.15rem 0.35rem;
    border-radius: 6px; background: rgba(124, 166, 255, 0.1); color: var(--accent-strong);
  }
  .markdown-content pre {
    margin: 0.85em 0; padding: 12px;
    border: 1px solid var(--border-soft); border-radius: var(--radius-md);
    background: var(--editor-bg); overflow-x: auto;
  }
  .markdown-content pre code { padding: 0; background: none; color: var(--text); }
  .markdown-content blockquote {
    margin: 0.85em 0; padding-left: 12px;
    border-left: 3px solid var(--accent); color: var(--text-muted);
  }
  .markdown-content a { color: var(--accent-strong); text-decoration: none; }
  .markdown-content a:hover { text-decoration: underline; }
  .markdown-content hr { margin: 1.5em 0; border: 0; border-top: 1px solid var(--border-soft); }
  .markdown-content img { max-width: 100%; border-radius: var(--radius-md); margin: 0.85em 0; }

  .binary-placeholder {
    padding: 40px 20px; text-align: center;
    border: 1px dashed var(--border); border-radius: var(--radius-md);
    color: var(--text-muted); background: var(--editor-bg);
  }
  .binary-placeholder h3 { font-size: 14px; font-weight: 600; margin-bottom: 8px; color: var(--text); }

  .image-viewer { display: flex; justify-content: center; align-items: center; padding: 20px; }
  .image-viewer img {
    max-width: 100%; max-height: calc(100vh - 200px);
    border-radius: var(--radius-md); box-shadow: 0 4px 20px rgba(0, 0, 0, 0.3);
  }

  @media (max-width: 768px) {
    body { padding: 0; }
    #shell { height: 100vh; border: none; border-radius: 0; }
    #volumes-panel { width: 140px; }
    #tree-panel { width: 180px; }
    .controls { grid-template-columns: 1fr 1fr; }
  }
</style>
</head>
<body>
<main id="shell">
  <header class="topbar">
    <div class="topbar-row">
      <div class="title-stack">
        <div class="title-badge">📦</div>
        <div class="title-copy">
          <h1>Volume Inspector</h1>
        </div>
      </div>
    </div>
    <div class="controls">
      <label class="field">
        <span class="field-label">API URL</span>
        <input id="api-url" type="text" placeholder="http://localhost:7778">
      </label>
      <label class="field">
        <span class="field-label">Provider</span>
        <select id="provider-select">
          <option value="all">All</option>
          <option value="local">local</option>
          <option value="docker">docker</option>
          <option value="daytona">daytona</option>
        </select>
      </label>
      <div class="field">
        <span class="field-label">&nbsp;</span>
        <button id="refresh-btn" class="primary">Refresh</button>
      </div>
      <div class="field">
        <span class="field-label">&nbsp;</span>
        <div id="status-pill" class="status-pill">—</div>
      </div>
    </div>
  </header>

  <section id="content">
    <aside id="volumes-panel">
      <div class="empty-state" style="padding:16px;font-size:12px;">Enter API URL<br>and click Refresh</div>
    </aside>

    <div class="drag-handle" id="drag-1"></div>

    <aside id="tree-panel">
      <div class="empty-state" style="padding:16px;font-size:12px;">Select a volume</div>
    </aside>

    <div class="drag-handle" id="drag-2"></div>

    <section id="viewer">
      <div id="viewer-header" style="display:none;">
        <h2 id="file-name"></h2>
        <p id="file-meta"></p>
      </div>
      <div id="viewer-content">
        <div class="empty-state">Select a file to view its contents</div>
      </div>
    </section>
  </section>
</main>

<script>
const $ = (id) => document.getElementById(id);

const state = {
  apiUrl: '',
  provider: 'all',
  volumes: [],
  selectedVolume: null,
  tree: null,
  expanded: new Set(),
  selectedPath: null,
  volRefreshTimer: null,
  treeRefreshTimer: null,
};

$('api-url').value = window.location.origin;

// ── Helpers ──────────────────────────────────────────────────────────────────

function formatSize(bytes) {
  if (!bytes || bytes === 0) return '';
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  if (bytes < 1024 * 1024 * 1024) return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
  return `${(bytes / (1024 * 1024 * 1024)).toFixed(1)} GB`;
}

function escapeHtml(text) {
  const div = document.createElement('div');
  div.textContent = text;
  return div.innerHTML;
}

function setStatus(text, type) {
  const pill = $('status-pill');
  pill.textContent = text;
  pill.className = 'status-pill' + (type ? ` ${type}` : '');
}

function providerBadgeClass(provider) {
  if (!provider) return 'other';
  const p = provider.toLowerCase();
  if (p === 'local') return 'local';
  if (p === 'docker') return 'docker';
  if (p === 'daytona') return 'daytona';
  return 'other';
}

function baseUrl() {
  return $('api-url').value.trim().replace(/\/+$/, '');
}

// ── Volume list ───────────────────────────────────────────────────────────────

async function loadVolumes() {
  state.apiUrl = baseUrl();
  state.provider = $('provider-select').value;

  if (!state.apiUrl) {
    setStatus('No URL', 'err');
    return;
  }

  const panel = $('volumes-panel');
  panel.innerHTML = '<div class="empty-state"><span class="spinner"></span></div>';
  setStatus('Loading…');

  try {
    const url = state.provider === 'all'
      ? `${state.apiUrl}/volumes`
      : `${state.apiUrl}/volumes?provider=${encodeURIComponent(state.provider)}`;
    const resp = await fetch(url);
    if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
    state.volumes = await resp.json();
    renderVolumes();
    setStatus(`${state.volumes.length} volume${state.volumes.length !== 1 ? 's' : ''}`, 'ok');

    clearInterval(state.volRefreshTimer);
    state.volRefreshTimer = setInterval(loadVolumesSilent, 15000);
  } catch (err) {
    panel.innerHTML = `<div class="empty-state" style="padding:12px;font-size:11px;color:var(--danger)">Error:<br>${escapeHtml(err.message)}</div>`;
    setStatus('Error', 'err');
  }
}

async function loadVolumesSilent() {
  if (!state.apiUrl) return;
  try {
    const url = state.provider === 'all'
      ? `${state.apiUrl}/volumes`
      : `${state.apiUrl}/volumes?provider=${encodeURIComponent(state.provider)}`;
    const resp = await fetch(url);
    if (!resp.ok) return;
    state.volumes = await resp.json();
    renderVolumes();
    setStatus(`${state.volumes.length} volume${state.volumes.length !== 1 ? 's' : ''}`, 'ok');
  } catch (_) {}
}

function renderVolumes() {
  const panel = $('volumes-panel');
  panel.innerHTML = '';

  if (!state.volumes.length) {
    panel.innerHTML = '<div class="empty-state" style="padding:16px;font-size:12px;">No volumes</div>';
    return;
  }

  state.volumes.forEach(vol => {
    const card = document.createElement('div');
    card.className = 'vol-card' + (state.selectedVolume && state.selectedVolume.id === vol.id ? ' selected' : '');

    const name = document.createElement('div');
    name.className = 'vol-name';
    name.textContent = vol.name || vol.id;
    card.appendChild(name);

    const meta = document.createElement('div');
    meta.className = 'vol-meta';

    if (vol.provider) {
      const badge = document.createElement('span');
      badge.className = `vol-badge ${providerBadgeClass(vol.provider)}`;
      badge.textContent = vol.provider;
      meta.appendChild(badge);
    }

    if (vol.id) {
      const idEl = document.createElement('div');
      idEl.className = 'vol-id';
      idEl.textContent = vol.id.length > 16 ? vol.id.slice(0, 16) + '…' : vol.id;
      idEl.title = vol.id;
      meta.appendChild(idEl);
    }

    card.appendChild(meta);
    card.addEventListener('click', () => selectVolume(vol));
    panel.appendChild(card);
  });
}

function selectVolume(vol) {
  state.selectedVolume = vol;
  state.tree = null;
  state.selectedPath = null;
  renderVolumes();
  resetViewer();
  loadTree();
}

// ── File tree ─────────────────────────────────────────────────────────────────

async function loadTree() {
  if (!state.selectedVolume) return;
  const volId = state.selectedVolume.id;
  const treePanel = $('tree-panel');
  treePanel.innerHTML = '<div class="empty-state"><span class="spinner"></span></div>';

  try {
    const resp = await fetch(`${state.apiUrl}/volumes/${encodeURIComponent(volId)}/files/tree`);
    if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
    state.tree = await resp.json();
    renderTree();

    clearInterval(state.treeRefreshTimer);
    state.treeRefreshTimer = setInterval(loadTreeSilent, 15000);
  } catch (err) {
    treePanel.innerHTML = `<div class="empty-state" style="padding:12px;font-size:11px;color:var(--danger)">Error:<br>${escapeHtml(err.message)}</div>`;
  }
}

async function loadTreeSilent() {
  if (!state.selectedVolume || !state.apiUrl) return;
  const volId = state.selectedVolume.id;
  try {
    const resp = await fetch(`${state.apiUrl}/volumes/${encodeURIComponent(volId)}/files/tree`);
    if (resp.ok) {
      state.tree = await resp.json();
      renderTree();
    }
  } catch (_) {}
}

function renderTree() {
  const treePanel = $('tree-panel');
  treePanel.innerHTML = '';

  if (!state.tree || !state.tree.length) {
    treePanel.innerHTML = '<div class="empty-state" style="padding:16px;font-size:12px;">Empty volume</div>';
    return;
  }

  state.tree.forEach(node => renderTreeNode(node, 0, treePanel));
}

function renderTreeNode(node, depth, container) {
  const isDir = node.type === 'directory';
  const isExpanded = state.expanded.has(node.path);
  const isSelected = state.selectedPath === node.path;

  const nodeEl = document.createElement('div');
  nodeEl.className = `tree-node ${isDir ? 'directory' : ''} ${isSelected ? 'selected' : ''}`;
  nodeEl.style.paddingLeft = `${12 + depth * 16}px`;

  const icon = document.createElement('span');
  icon.className = 'tree-icon';
  icon.textContent = isDir ? (isExpanded ? '📂' : '📁') : '📄';

  const name = document.createElement('span');
  name.className = 'tree-name';
  name.textContent = node.name;

  nodeEl.appendChild(icon);
  nodeEl.appendChild(name);

  if (!isDir && node.size) {
    const size = document.createElement('span');
    size.className = 'tree-size';
    size.textContent = formatSize(node.size);
    nodeEl.appendChild(size);
  }

  nodeEl.addEventListener('click', (e) => {
    e.stopPropagation();
    if (isDir) {
      if (isExpanded) state.expanded.delete(node.path);
      else state.expanded.add(node.path);
      renderTree();
    } else {
      loadFile(node.path, node.name, node.size);
    }
  });

  container.appendChild(nodeEl);

  if (isDir && isExpanded && node.children) {
    const childContainer = document.createElement('div');
    childContainer.className = 'tree-children';
    node.children.forEach(child => renderTreeNode(child, depth + 1, childContainer));
    container.appendChild(childContainer);
  }
}

// ── File viewer ───────────────────────────────────────────────────────────────

function resetViewer() {
  $('viewer-header').style.display = 'none';
  $('viewer-content').innerHTML = '<div class="empty-state">Select a file to view its contents</div>';
}

async function loadFile(path, name, size) {
  if (!state.selectedVolume) return;
  state.selectedPath = path;
  renderTree();

  const viewerContent = $('viewer-content');
  const viewerHeader = $('viewer-header');

  viewerHeader.style.display = 'block';
  $('file-name').textContent = name;
  $('file-meta').textContent = formatSize(size) || '';
  viewerContent.innerHTML = '<div class="empty-state"><span class="spinner"></span></div>';

  try {
    const volId = state.selectedVolume.id;
    const resp = await fetch(
      `${state.apiUrl}/volumes/${encodeURIComponent(volId)}/files/read?path=${encodeURIComponent(path)}`
    );
    if (!resp.ok) throw new Error(`HTTP ${resp.status}: ${resp.statusText}`);
    const data = await resp.json();
    $('file-meta').textContent = formatSize(data.size) || '';
    renderFileContent(data);
  } catch (err) {
    viewerContent.innerHTML = `<div class="empty-state">Error loading file:<br>${escapeHtml(err.message)}</div>`;
  }
}

function renderMarkdown(text) {
  let html = escapeHtml(text);
  html = html.replace(/```([a-z]*)\n([\s\S]*?)```/g, (_, lang, code) => `<pre><code>${code}</code></pre>`);
  html = html.replace(/^### (.*$)/gm, '<h3>$1</h3>');
  html = html.replace(/^## (.*$)/gm, '<h2>$1</h2>');
  html = html.replace(/^# (.*$)/gm, '<h1>$1</h1>');
  html = html.replace(/\*\*([^*]+)\*\*/g, '<strong>$1</strong>');
  html = html.replace(/\*([^*]+)\*/g, '<em>$1</em>');
  html = html.replace(/`([^`]+)`/g, '<code>$1</code>');
  html = html.replace(/\[([^\]]+)\]\(([^)]+)\)/g, '<a href="$2" target="_blank">$1</a>');
  html = html.replace(/^\* (.*$)/gm, '<li>$1</li>');
  html = html.replace(/(<li>.*<\/li>)/s, '<ul>$1</ul>');
  html = html.replace(/^---$/gm, '<hr>');
  html = html.split('\n\n').map(para => {
    if (para.startsWith('<') || para.trim() === '') return para;
    return `<p>${para}</p>`;
  }).join('\n');
  return html;
}

function renderFileContent(data) {
  const viewerContent = $('viewer-content');

  if (data.binary && !data.image && !data.pdf) {
    viewerContent.innerHTML = `<div class="binary-placeholder"><h3>Binary file</h3><p>${formatSize(data.size)}</p></div>`;
    return;
  }

  if (data.image) {
    viewerContent.innerHTML = `<div class="image-viewer"><img src="${data.content}" alt="${escapeHtml(data.name || '')}"></div>`;
    return;
  }

  if (data.pdf) {
    viewerContent.innerHTML = `<iframe style="width:100%;height:100%;border:none;" src="${data.content}"></iframe>`;
    return;
  }

  if ((data.name || '').endsWith('.md')) {
    viewerContent.innerHTML = `<div class="markdown-content">${renderMarkdown(data.content)}</div>`;
    return;
  }

  const lines = (data.content || '').split('\n');
  const codeHtml = lines.map((line, i) =>
    `<div class="code-line"><span class="line-number">${i + 1}</span><span class="line-content">${escapeHtml(line)}</span></div>`
  ).join('');
  viewerContent.innerHTML = `<div class="code-block"><div class="code-content"><pre>${codeHtml}</pre></div></div>`;
}

// ── Drag-to-resize ────────────────────────────────────────────────────────────

function makeDraggable(handleId, panelEl, minW, maxW) {
  const handle = $(handleId);
  let dragging = false;

  handle.addEventListener('mousedown', (e) => {
    dragging = true;
    handle.classList.add('dragging');
    e.preventDefault();
  });

  document.addEventListener('mousemove', (e) => {
    if (!dragging) return;
    const rect = panelEl.getBoundingClientRect();
    const newWidth = e.clientX - rect.left;
    if (newWidth >= minW && newWidth <= maxW) {
      panelEl.style.width = `${newWidth}px`;
    }
  });

  document.addEventListener('mouseup', () => {
    if (dragging) {
      dragging = false;
      handle.classList.remove('dragging');
    }
  });
}

makeDraggable('drag-1', $('volumes-panel'), 120, 360);
makeDraggable('drag-2', $('tree-panel'), 140, 480);

// ── Event listeners ───────────────────────────────────────────────────────────

$('refresh-btn').addEventListener('click', loadVolumes);

$('api-url').addEventListener('keypress', (e) => {
  if (e.key === 'Enter') loadVolumes();
});

$('provider-select').addEventListener('change', loadVolumes);

window.addEventListener('beforeunload', () => {
  clearInterval(state.volRefreshTimer);
  clearInterval(state.treeRefreshTimer);
});

// Auto-load on page open
loadVolumes();
</script>
</body>
</html>
```

- [ ] **Step 2: Clear the server's UI cache so the new file is served**

The server caches UI files in `_UI_CACHE`. Restart the server to pick up the new `volumes.html`:

```bash
# If running via scripts/launch_server_local.sh — Ctrl+C then re-run:
bash scripts/launch_server_local.sh
```

- [ ] **Step 3: Open the UI in a browser and verify**

Navigate to `http://localhost:7778/ui/volumes`.

Verify manually:
1. Page loads with dark theme, three visible panels, header controls
2. API URL defaults to the current origin
3. Clicking **Refresh** loads the volume list in Panel 1
4. Changing the Provider dropdown re-fetches volumes automatically
5. Clicking a volume card selects it (highlighted) and loads its file tree in Panel 2
6. Clicking a file in the tree displays read-only content in Panel 3
7. Line numbers appear on text/code files
8. Drag handles between Panel 1–2 and Panel 2–3 resize panels correctly
9. Status pill shows e.g. "3 volumes" (green) or "Error" (red) on failure
10. No edit controls visible anywhere

- [ ] **Step 4: Run the full test**

```bash
pytest tests/test_ui_volumes_route.py -v
```

Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add ui/volumes.html
git commit -m "feat(ui): build complete three-panel Volume Inspector UI"
```

---

## Self-Review Notes

- **Spec coverage:** Header (API URL, Provider, Refresh, status pill) ✓ — Volume list with cards (name, badge, short ID) ✓ — File tree with spinner + auto-refresh ✓ — File viewer read-only (code/markdown/image/binary) ✓ — Drag handles ✓ — Two server lines ✓ — API endpoints (`/volumes`, `/volumes/{id}/files/tree`, `/volumes/{id}/files/read`) ✓ — Auto-refresh 15s ✓
- **No edit controls** — `fs.html`'s Edit/Save/Cancel buttons are intentionally omitted from this file ✓
- **Provider change triggers reload** — `$('provider-select').addEventListener('change', loadVolumes)` ✓
- **Auto-load on open** — `loadVolumes()` called at script end ✓
- **Cache invalidation** — `_UI_CACHE` caches by filename; server restart needed after first creation ✓
