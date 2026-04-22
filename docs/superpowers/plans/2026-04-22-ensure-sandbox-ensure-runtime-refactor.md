# Ensure-Sandbox / Ensure-Runtime Refactor — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

<!-- NOTES (2026-04-22): Refactor complete on branch ensure-refactor.
  server.py net line reduction: 3317 → 3012 = -305 lines.
  Deleted: get_or_recover_session (~150 lines), _do_resume (~270 lines),
  _lazy_provision_sandbox_for_session + _locked (~75 lines).
  Kept: _ensure_sandbox_alive (~135 lines) — still used by /sandboxes/{id}/start
  and _resolve_sandbox_instance (file-browsing paths, not session-lifecycle).
  3/3 e2e runs pass (plus one transient cold-start flake = known pre-existing issue).
  demo.py: ✅ agent remembered OSPREY. -->

**Goal:** Replace the 4+ overlapping session-recovery helpers with 2 idempotent "ensure" primitives. Every session endpoint becomes ~5 lines of orchestration. Delete ~400 lines of branchy recovery code.

**Architecture:** Two idempotent helpers.
- `ensure_sandbox(session)` — returns a SandboxRecord that is currently live on the provider. Creates / restarts / reprovisions as needed. Internally holds the per-session lock. Emits `sandbox_reattach` event when replacement happens.
- `ensure_runtime(session, sandbox)` — returns a `SessionState` with a connected `AcpClient`, live supervisor URL, and an initialized/loaded ACP inner session. Rebuilds if the in-memory state is stale or missing.

Every endpoint (`/message`, `/events`, `/resume`, `/cancel`, `/sandbox/exec`, `/start-sandbox`, `/stop-sandbox`, `/reset-sandbox`) becomes:

```python
session = await require_session(session_id)
sandbox = await ensure_sandbox(session)
runtime = await ensure_runtime(session, sandbox)
# do endpoint-specific work with runtime
```

**Tech Stack:** Python 3.11+, FastAPI, psycopg v3, pytest. Builds on the current `session-volume-impl` branch.

**Reference:** the conversation context + `docs/superpowers/specs/2026-04-21-session-volume-decoupling-design.md` for the broader model.

---

## File Structure

| Path | Change |
|---|---|
| `src/api/server.py` | Add `ensure_sandbox`, `ensure_runtime`, `require_session` helpers. Rewire ~10 endpoints to use them. Delete `get_or_recover_session`, `_lazy_provision_sandbox_for_session`, `_lazy_provision_sandbox_for_session_locked`, `_ensure_sandbox_alive`, `_do_resume` (or shrink to thin compat shims if anything outside server.py imports them). |
| `tests/test_ensure_helpers.py` | **New** — unit tests for the two helpers with mocked providers. |
| `tests/test_session_volume_integration.py` | Update any tests that poked at internals of the deleted helpers (there shouldn't be many — most go through REST). |

No schema changes. No API changes. Pure refactor.

---

## Approach — Incremental Strangler

We build both helpers first, wire endpoints over one at a time, then delete the old code last. This lets us run the test suite after every task and catch regressions fast.

**Safety invariant throughout:** after every task, the full suite (mocked + live e2e) must pass. If a task leaves the code in an intermediate state where both paths exist, that's fine — but nothing can be broken.

---

## Phase 1 — Build the primitives

### Task 1: `ensure_sandbox(session_row)` — the sandbox idempotence function

**Files:**
- Modify: `src/api/server.py`
- Test: `tests/test_ensure_helpers.py` (new)

Behavior contract:

```
given: session_row (dict from get_session)
returns: SandboxRecord that is currently live on the provider
side effects: may create/start/delete provider sandbox; updates
              sessions.current_sandbox_id; appends sandbox_reattach
              event on replacement; holds _get_session_lock(session.id)
              for the entire operation
```

Internal logic:

```
current = session_row["current_sandbox_id"]
if current is None:
    create a new sandbox with session.volume + agents/<agent_id>/home subpath
    persist row, set session.current_sandbox_id, return it

sb = await get_sandbox(current)
if sb is None:
    # row was deleted (via DELETE /sandboxes, reaper, etc.)
    create replacement with same subpath
    emit sandbox_reattach event (old_id=current, new_id=replacement.id)
    return replacement

status = await provider.get_status(sb.sandbox_ref)
match status:
    "running" -> return sb
    "stopped" -> provider.start(sb.sandbox_ref); return sb
    "missing" -> delete sb row; create replacement; emit reattach; return
    "error"   -> destroy + create replacement; emit reattach; return
```

- [ ] **Step 1: Write failing tests** — create `tests/test_ensure_helpers.py`:

```python
"""Unit tests for ensure_sandbox / ensure_runtime helpers."""
from __future__ import annotations
import os, sys
import pytest
from unittest.mock import AsyncMock, patch
from httpx import ASGITransport, AsyncClient

_SRC = os.path.join(os.path.dirname(__file__), "..", "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

_DB = os.environ.get("TEST_DATABASE_URL")
pytestmark = pytest.mark.skipif(_DB is None, reason="TEST_DATABASE_URL not set")
if _DB:
    os.environ["DATABASE_URL"] = _DB

from api import db as dbmod, server as srv  # noqa: E402


@pytest.fixture
async def setup():
    dbmod.init_db()
    await dbmod.init_pool()
    async with dbmod.get_db() as conn:
        await conn.execute("DELETE FROM session_log")
        await conn.execute("DELETE FROM sessions")
        await conn.execute("DELETE FROM sandboxes")
        await conn.execute("DELETE FROM volumes")
        await conn.execute("DELETE FROM agents")
    yield
    await dbmod.close_pool()


async def _mk_fixtures():
    from api.models import AgentConfig, AgentRecord, VolumeRecord
    await dbmod.upsert_agent(AgentRecord(id="a1", name="A",
                                         config=AgentConfig(agent_type="claude")))
    await dbmod.upsert_volume(VolumeRecord(id="v1", name="v", provider="daytona",
                                           provider_ref="dt-v"))
    async with dbmod.get_db() as conn:
        await conn.execute(
            "INSERT INTO sessions (id, agent_id, volume_id) VALUES (%s,%s,%s)",
            ("s1", "a1", "v1"),
        )


@pytest.mark.asyncio
async def test_ensure_sandbox_creates_when_none(setup):
    await _mk_fixtures()
    from api.providers import ProviderInstance
    created = []

    async def fake_provision(**kw):
        created.append(kw)
        return ProviderInstance(provider="daytona", url="http://fake",
                                root="/home/daytona", sandbox_id=f"dt-{len(created)}")

    with patch("api.providers.provision_daytona_sandbox", new=AsyncMock(side_effect=fake_provision)):
        sess = await dbmod.get_session("s1")
        sb = await srv.ensure_sandbox(sess)

    assert sb is not None
    assert len(created) == 1
    assert created[0]["volume_id"] == "dt-v"
    assert created[0]["subpath"] == "agents/a1/home"


@pytest.mark.asyncio
async def test_ensure_sandbox_returns_existing_when_running(setup):
    await _mk_fixtures()
    from api.models import SandboxRecord
    sb = SandboxRecord(id="sb1", provider="daytona", sandbox_ref="dt-live",
                       status="running", root="/home/daytona",
                       volume_id="v1", subpath="agents/a1/home")
    await dbmod.upsert_sandbox(sb)
    await dbmod.set_session_current_sandbox("s1", "sb1")

    with patch("api.providers.get_daytona_sandbox_status",
               new=AsyncMock(return_value="running")), \
         patch("api.providers.provision_daytona_sandbox",
               new=AsyncMock(side_effect=AssertionError("should not provision"))):
        sess = await dbmod.get_session("s1")
        got = await srv.ensure_sandbox(sess)
    assert got.id == "sb1"


@pytest.mark.asyncio
async def test_ensure_sandbox_reprovisions_when_missing_emits_reattach(setup):
    await _mk_fixtures()
    # current_sandbox_id points to a row that's been deleted
    await dbmod.set_session_current_sandbox("s1", "sb_dead")
    # Directly set without creating the row first — represents a deleted sandbox
    async with dbmod.get_db() as conn:
        await conn.execute("UPDATE sessions SET current_sandbox_id='sb_dead' WHERE id='s1'")

    from api.providers import ProviderInstance
    async def fake_provision(**kw):
        return ProviderInstance(provider="daytona", url="http://new",
                                root="/home/daytona", sandbox_id="dt-new")

    with patch("api.providers.provision_daytona_sandbox", new=AsyncMock(side_effect=fake_provision)):
        sess = await dbmod.get_session("s1")
        sb = await srv.ensure_sandbox(sess)

    assert sb.sandbox_ref == "dt-new"
    async with dbmod.get_db() as conn:
        rows = await (await conn.execute(
            "SELECT event_type, payload FROM session_log WHERE session_id='s1'"
        )).fetchall()
    reattach = [r for r in rows if r["event_type"] == "sandbox_reattach"]
    assert len(reattach) == 1
    assert reattach[0]["payload"]["old_sandbox_id"] == "sb_dead"


@pytest.mark.asyncio
async def test_ensure_sandbox_starts_stopped(setup):
    await _mk_fixtures()
    from api.models import SandboxRecord
    sb = SandboxRecord(id="sb1", provider="daytona", sandbox_ref="dt-paused",
                       status="stopped", root="/home/daytona",
                       volume_id="v1", subpath="agents/a1/home")
    await dbmod.upsert_sandbox(sb)
    await dbmod.set_session_current_sandbox("s1", "sb1")

    start_calls = []
    async def fake_start(ref):
        start_calls.append(ref)

    with patch("api.providers.get_daytona_sandbox_status",
               new=AsyncMock(return_value="stopped")), \
         patch("api.providers.start_daytona",
               new=AsyncMock(side_effect=fake_start)), \
         patch("api.providers.provision_daytona_sandbox",
               new=AsyncMock(side_effect=AssertionError("should not provision"))):
        sess = await dbmod.get_session("s1")
        got = await srv.ensure_sandbox(sess)
    assert got.id == "sb1"
    assert start_calls == ["dt-paused"]
```

- [ ] **Step 2: Run tests — expect FAIL**

```bash
set -a; source .env; set +a
pytest tests/test_ensure_helpers.py -v
```
Expected: `AttributeError: module 'api.server' has no attribute 'ensure_sandbox'`.

- [ ] **Step 3: Implement `ensure_sandbox`**

Add to `src/api/server.py`, near `_lazy_provision_sandbox_for_session`:

```python
async def ensure_sandbox(session_row: dict) -> SandboxRecord:
    """Guarantees: returns a sandbox that is currently live on the provider.

    Idempotent: safe to call multiple times in a row. Creates/restarts/replaces
    as needed. Emits a ``sandbox_reattach`` event when the returned sandbox is
    a replacement for a previously-recorded one.

    Holds the per-session lock for the entire check-and-act sequence so
    concurrent callers don't double-provision.
    """
    session_id = session_row["id"]
    async with _get_session_lock(session_id):
        return await _ensure_sandbox_locked(session_row)


async def _ensure_sandbox_locked(session_row: dict) -> SandboxRecord:
    session_id = session_row["id"]
    # Re-read after acquiring lock in case a concurrent caller updated it.
    fresh = await get_session(session_id)
    if fresh is None:
        raise HTTPException(404, "Session not found")
    current_id = fresh.get("current_sandbox_id")

    # Case A: no sandbox yet — create one.
    if current_id is None:
        return await _provision_new(fresh, previous_id=None)

    sb = await get_sandbox(current_id)

    # Case B: row was deleted — replace and emit reattach.
    if sb is None:
        await set_session_current_sandbox(session_id, None)
        return await _provision_new(fresh, previous_id=current_id)

    # Case C-F: row exists — probe provider state.
    status = await _providers_mod.get_daytona_sandbox_status(sb.sandbox_ref)
    if status == "running":
        return sb
    if status == "stopped":
        await _providers_mod.start_daytona(sb.sandbox_ref)
        sb.status = STATUS_RUNNING
        await upsert_sandbox(sb)
        return sb
    if status in ("missing", "error"):
        if status == "error":
            try:
                from .providers import ProviderInstance
                inst = ProviderInstance(provider="daytona", url="",
                                        root=sb.root, sandbox_id=sb.sandbox_ref)
                await _providers_mod.destroy_daytona(inst)
            except Exception:
                pass
        await delete_sandbox(sb.id)
        await set_session_current_sandbox(session_id, None)
        return await _provision_new(fresh, previous_id=current_id)
    raise HTTPException(500, f"Unknown sandbox status: {status}")


async def _provision_new(session_row: dict, previous_id: str | None) -> SandboxRecord:
    """Create a fresh Daytona sandbox with session.volume + agents/<agent>/home."""
    vol = await get_volume(session_row["volume_id"])
    if vol is None:
        raise HTTPException(500, f"Session's volume {session_row['volume_id']} missing")
    agent_id = session_row["agent_id"]
    subpath = f"agents/{agent_id}/home"
    agent = await get_agent(agent_id)
    agent_type = (agent.config.agent_type if agent and agent.config else "claude")

    inst = await _providers_mod.provision_daytona_sandbox(
        agent_type=agent_type,
        volume_id=vol.provider_ref,
        subpath=subpath,
    )
    import uuid as _uuid
    sb = SandboxRecord(
        id=f"sb_{_uuid.uuid4().hex[:12]}",
        provider="daytona",
        sandbox_ref=inst.sandbox_id,
        status=STATUS_RUNNING,
        root="/home/daytona",
        volume_id=vol.id,
        subpath=subpath,
    )
    await upsert_sandbox(sb)
    await set_session_current_sandbox(session_row["id"], sb.id)
    if previous_id is not None:
        await log_event(
            session_id=session_row["id"],
            agent_id=agent_id,
            sandbox_id=sb.id,
            event_type="sandbox_reattach",
            payload={"old_sandbox_id": previous_id, "new_sandbox_id": sb.id},
        )
    return sb
```

Ensure `start_daytona` exists in `providers.py`. If it doesn't, add it:

```python
# in src/api/providers.py, alongside stop_daytona
async def start_daytona(sandbox_ref: str) -> None:
    client = _get_daytona_client()
    sb = await asyncio.to_thread(client.get, sandbox_ref)
    await asyncio.to_thread(sb.start)
```

(Adjust to match the actual Daytona SDK start API — may be `client.start(sb)`.)

- [ ] **Step 4: Run — expect PASS**

All 4 tests in test_ensure_helpers.py green.

- [ ] **Step 5: Run full regression**

```bash
pytest tests/test_volumes_db.py tests/test_volumes_api.py \
       tests/test_session_volume_integration.py tests/test_client_volumes.py \
       tests/test_ensure_helpers.py -v
```

All green.

- [ ] **Step 6: Commit**

```bash
git add src/api/server.py src/api/providers.py tests/test_ensure_helpers.py
git commit -m "feat(server): add ensure_sandbox idempotent helper"
```

---

### Task 2: `ensure_runtime(session_row, sandbox)` — the runtime idempotence function

**Files:**
- Modify: `src/api/server.py`
- Test: `tests/test_ensure_helpers.py` (append)

Behavior contract:

```
given:   session_row (dict), sandbox (SandboxRecord)
returns: SessionState with:
           - supervisor running in the sandbox
           - AcpClient connected
           - acp_session_id set
           - inner_session_id loaded/created
         SESSIONS[session_id] populated and ready to accept prompts.
side effects: may start supervisor, open network connection, call
              ACP session/new or session/load
```

Internal logic:

```
existing = SESSIONS.get(session_id)
if existing and existing.sandbox_id == sandbox.id and supervisor_healthy(existing):
    return existing

# Otherwise: tear down stale, build fresh
if existing:
    await _shutdown_session_state(existing, remove=True)

spawn_env = _build_spawn_env_from_row(session_row)
supervisor_url, port = await start_supervisor_in_sandbox(sandbox, spawn_env=spawn_env)
client = AcpClient(supervisor_url)
await client.handshake(acp_session_id=new_uuid, agent=agent_type)

inner_sid = session_row.get("inner_session_id")
if inner_sid:
    await client.load_session(acp_session_id, inner_sid, cwd, mcp_servers)  # with retry
else:
    inner_sid = await client.initialize(acp_session_id, agent, cwd, mcp_servers)  # has retry
    await set_session_inner_id(session_id, inner_sid)

state = SessionState(...)
SESSIONS[session_id] = state
_start_session_tasks(state)
return state
```

- [ ] **Step 1: Write failing tests** — append to `tests/test_ensure_helpers.py`:

```python
@pytest.mark.asyncio
async def test_ensure_runtime_reuses_healthy_state(setup):
    await _mk_fixtures()
    from api.models import SandboxRecord, SessionState
    sb = SandboxRecord(id="sb1", provider="daytona", sandbox_ref="dt-r",
                       status="running", root="/home/daytona",
                       volume_id="v1", subpath="agents/a1/home")
    await dbmod.upsert_sandbox(sb)
    await dbmod.set_session_current_sandbox("s1", "sb1")

    # Pre-populate in-memory state
    fake_client = AsyncMock()
    fake_client.base_url = "http://existing"
    state = SessionState(session_id="s1", agent_id="a1", sandbox_id="sb1",
                         acp_session_id="acp1", inner_session_id="inner1",
                         agent_type="claude", client=fake_client,
                         supervisor_url="http://existing")
    srv.SESSIONS["s1"] = state

    with patch("api.providers._wait_for_health",
               new=AsyncMock(return_value=True)):
        sess = await dbmod.get_session("s1")
        got = await srv.ensure_runtime(sess, sb)

    assert got is state  # identity — no rebuild


@pytest.mark.asyncio
async def test_ensure_runtime_rebuilds_when_missing(setup):
    await _mk_fixtures()
    from api.models import SandboxRecord
    sb = SandboxRecord(id="sb1", provider="daytona", sandbox_ref="dt-r",
                       status="running", root="/home/daytona",
                       volume_id="v1", subpath="agents/a1/home")
    await dbmod.upsert_sandbox(sb)
    await dbmod.set_session_current_sandbox("s1", "sb1")

    srv.SESSIONS.pop("s1", None)  # no in-memory state

    fake_client = AsyncMock()
    with patch("api.server.start_supervisor_in_sandbox",
               new=AsyncMock(return_value=("http://fresh", 9100))), \
         patch("api.server.AcpClient", return_value=fake_client), \
         patch("api.server._start_session_tasks"):
        fake_client.initialize = AsyncMock(return_value={"sessionId": "inner-new"})
        fake_client.get_inner_session_id = lambda *a: "inner-new"

        sess = await dbmod.get_session("s1")
        got = await srv.ensure_runtime(sess, sb)

    assert got.session_id == "s1"
    assert got.sandbox_id == "sb1"
    assert got.supervisor_url == "http://fresh"
```

- [ ] **Step 2: Run — FAIL** (ensure_runtime doesn't exist)

- [ ] **Step 3: Implement `ensure_runtime`**

Add alongside `ensure_sandbox`:

```python
async def ensure_runtime(session_row: dict, sandbox: SandboxRecord) -> SessionState:
    """Guarantees: returns a SessionState with a connected, initialized client.

    If the existing SESSIONS entry is healthy and attached to this sandbox,
    reuse it. Otherwise tear down any stale state and build fresh.
    """
    session_id = session_row["id"]
    async with _get_session_lock(session_id):
        return await _ensure_runtime_locked(session_row, sandbox)


async def _ensure_runtime_locked(session_row: dict, sandbox: SandboxRecord) -> SessionState:
    session_id = session_row["id"]
    existing = SESSIONS.get(session_id)

    # Reuse if the existing state is attached to this sandbox and the
    # supervisor is reachable.
    if (
        existing
        and not existing.shutdown.is_set()
        and existing.sandbox_id == sandbox.id
        and existing.supervisor_url
    ):
        try:
            from .providers import _wait_for_health
            ok = await _wait_for_health(existing.supervisor_url, max_retries=2, interval=1)
            if ok:
                return existing
        except Exception:
            pass
        # Supervisor dead — tear down and rebuild.
        await _shutdown_session_state(existing, remove=True)

    # Build fresh.
    agent_id = session_row["agent_id"]
    agent_record = await get_agent(agent_id)
    if agent_record is None:
        raise HTTPException(500, f"Agent {agent_id} missing")
    agent_type = agent_record.config.agent_type or "claude"

    spawn_env = await _build_spawn_env_from_row(session_row)
    supervisor_url, supervisor_port = await start_supervisor_in_sandbox(
        sandbox, agent_type=agent_type, spawn_env=spawn_env,
    )

    client = AcpClient(supervisor_url)
    acp_session_id = str(uuid.uuid4())
    inner_sid = session_row.get("inner_session_id")
    cwd = agent_record.config.cwd or "/home/daytona/workspace"
    mcp = agent_record.config.mcp_servers

    if inner_sid:
        # Reconnect to existing conversation on disk.
        await client.handshake(acp_session_id, agent_type)
        # session/load with built-in retry (from earlier tick)
        await client._send_rpc(acp_session_id, "session/load", {
            "sessionId": inner_sid, "cwd": cwd,
            "mcpServers": _mcp_dict_to_acp_array(mcp) if mcp else [],
        })
        client.set_inner_session_id(acp_session_id, inner_sid)
    else:
        # Fresh conversation.
        await client.initialize(acp_session_id, agent_type, cwd=cwd, mcp_servers=mcp)
        inner_sid = client.get_inner_session_id(acp_session_id)
        if inner_sid:
            await set_session_inner_id(session_id, inner_sid)

    state = SessionState(
        session_id=session_id, agent_id=agent_id, sandbox_id=sandbox.id,
        acp_session_id=acp_session_id, inner_session_id=inner_sid,
        agent_type=agent_type, client=client,
        supervisor_url=supervisor_url, supervisor_port=supervisor_port,
    )
    SESSIONS[session_id] = state
    _start_session_tasks(state)
    return state
```

- [ ] **Step 4: Run — expect PASS**

- [ ] **Step 5: Run full regression**

- [ ] **Step 6: Commit**

```bash
git add src/api/server.py tests/test_ensure_helpers.py
git commit -m "feat(server): add ensure_runtime idempotent helper"
```

---

### Task 3: `require_session(session_id)` + convenience wrapper

**Files:**
- Modify: `src/api/server.py`

- [ ] **Step 1: Add helpers**

```python
async def require_session(session_id: str) -> dict:
    """Fetch session row or 404."""
    rec = await get_session(session_id)
    if rec is None:
        raise HTTPException(404, f"Session {session_id} not found")
    return rec


async def ensure_session_live(session_id: str) -> tuple[dict, SandboxRecord, SessionState]:
    """One-shot: session → sandbox → runtime. Most endpoints use this."""
    session = await require_session(session_id)
    sandbox = await ensure_sandbox(session)
    runtime = await ensure_runtime(session, sandbox)
    return session, sandbox, runtime
```

- [ ] **Step 2: Commit (no tests needed — thin composition)**

```bash
git add src/api/server.py
git commit -m "feat(server): add ensure_session_live composite helper"
```

---

## Phase 2 — Switch endpoints one at a time

Each endpoint: replace the existing "recover session" block with a single `await ensure_session_live(session_id)` call. After each, run the full test suite.

### Task 4: Switch `POST /sessions/{id}/message`

- [ ] **Step 1: Locate the handler** (around line 2874 in server.py).

- [ ] **Step 2: Replace the setup preamble**

Before:
```python
try:
    state = await get_or_recover_session(session_id)
except HTTPException as exc:
    return JSONResponse({"error": exc.detail}, status_code=exc.status_code)

if not state.client or not state.acp_session_id:
    return JSONResponse({"error": "session not connected"}, status_code=409)
```

After:
```python
try:
    _, _, state = await ensure_session_live(session_id)
except HTTPException as exc:
    return JSONResponse({"error": exc.detail}, status_code=exc.status_code)
```

The "not connected" check is unnecessary — `ensure_runtime` guarantees a connected client.

- [ ] **Step 3: Run full regression + live e2e**

```bash
pytest tests/ --ignore=tests/test_session_volume_e2e.py -v
pytest tests/test_session_volume_e2e.py -v --timeout=300  # live e2e
```

All green.

- [ ] **Step 4: Commit**

```bash
git add src/api/server.py
git commit -m "refactor(server): /sessions/{id}/message uses ensure_session_live"
```

### Task 5: Switch `GET /sessions/{id}/events`

Same pattern. Find the handler (line ~2916). Replace the `get_or_recover_session` call.

- [ ] Run full regression after.
- [ ] Commit with matching message.

### Task 6: Switch `POST /sessions/{id}/cancel` + `POST /sessions/{id}/config`

Batch these — both are small handlers. Same replacement pattern.

- [ ] Run full regression.
- [ ] Commit.

### Task 7: Switch `POST /sessions/{id}/resume`

This one is trickier because `_do_resume` has extra logic (replay event stream) that `ensure_runtime` doesn't.

- [ ] **Step 1**: Check what's unique to `/resume` beyond what `ensure_session_live` provides.
- [ ] **Step 2**: If the only extra is `last_event_id` replay — wire that explicitly after the ensure call.
- [ ] **Step 3**: If something's deeply entangled, leave `_do_resume` as-is for this task and come back. Flag as DONE_WITH_CONCERNS.
- [ ] Commit.

### Task 8: Switch `POST /sessions/{id}/sandbox/exec`

- [ ] Replace the manual sandbox lookup at line ~3092 with `ensure_session_live`.
- [ ] Run regression.
- [ ] Commit.

### Task 9: Switch `POST /sessions/{id}/start-sandbox` / `/stop-sandbox` / `/reset-sandbox`

`start-sandbox` → `_, sb, _ = await ensure_session_live(...)`
`stop-sandbox` → keep most of the teardown logic; just use `require_session` for lookup
`reset-sandbox` → `stop_session_sandbox(...)` + `ensure_session_live(...)` (the `previous_sandbox_id` reattach event is now emitted by `ensure_sandbox` automatically on reprovision — verify the test still sees the event)

- [ ] Run regression (especially the reattach event test).
- [ ] Commit.

---

## Phase 3 — Delete the old code

At this point, no endpoint calls the old helpers. Verify with grep, then delete.

### Task 10: Delete `get_or_recover_session`

- [ ] **Step 1**: `grep -n "get_or_recover_session" src/ tests/` — should be zero refs outside its own definition.
- [ ] **Step 2**: Delete the function and its docstring.
- [ ] **Step 3**: Run full suite.
- [ ] **Step 4**: Commit: `chore(server): remove get_or_recover_session`.

### Task 11: Delete `_lazy_provision_sandbox_for_session` + `_locked` variant

Same pattern. Functionality is now in `_provision_new` (called by `ensure_sandbox`).

- [ ] Verify zero refs.
- [ ] Delete.
- [ ] Run suite.
- [ ] Commit.

### Task 12: Collapse `_ensure_sandbox_alive` and `_do_resume`

These two still exist if Task 7 didn't fully switch `/resume`. Either:
- (a) Finish the `/resume` switch now and delete both, or
- (b) Keep them if they still have unique functionality not covered by `ensure_*`.

- [ ] Audit what's still unique.
- [ ] Delete what's covered.
- [ ] Commit.

---

## Phase 4 — Final verification

### Task 13: End-to-end verification

- [ ] **Step 1**: Run mocked suite:
  ```bash
  pytest tests/test_volumes_db.py tests/test_volumes_api.py \
         tests/test_session_volume_integration.py tests/test_client_volumes.py \
         tests/test_ensure_helpers.py -v
  ```
  Expect: all green.

- [ ] **Step 2**: Run live e2e three times back-to-back:
  ```bash
  for i in 1 2 3; do
    dropdb agent_sdk_test; createdb agent_sdk_test
    pytest tests/test_session_volume_e2e.py -v --timeout=300
  done
  ```
  Expect: 3/3 pass. Any flake → fix before declaring done.

- [ ] **Step 3**: Run the demo:
  ```bash
  python demo.py
  ```
  Expect: ✅ remembered OSPREY.

- [ ] **Step 4**: Count lines removed. Commit message should cite the line reduction.

### Task 14: Documentation sweep

- [ ] Update `assets/data-model.html` if any endpoints changed semantics (they shouldn't — this is pure refactor).
- [ ] Update `docs/superpowers/specs/...` design doc if any documented behavior changed.
- [ ] Commit.

---

## Self-Review Checklist

Before declaring done, verify:

1. **Both helpers are truly idempotent** — calling `ensure_sandbox` 3x in a row makes the same number of provider calls as calling it once.
2. **Per-session lock is held throughout** — no concurrent callers can bypass the "check then act" sequence.
3. **All 5 states from the old `get_or_recover_session` are covered by one of the new helpers** (listed in the design discussion: in-memory healthy, in-memory stale, stale URL, lazy provision, reattach).
4. **Sandbox reattach event emission** — happens when `ensure_sandbox` decides to replace a previous sandbox. Verified by the `test_ensure_sandbox_reprovisions_when_missing_emits_reattach` test.
5. **Reaper still works** — it still walks `SESSIONS.values()`; no changes.
6. **Live e2e is stable** — 3/3 consecutive runs pass.
7. **Line count delta** — expect ~300-400 lines removed from server.py net, replaced by ~150 lines of helpers. Commit the savings proudly.

## Out of scope

- Schema changes (none).
- Dropping the sandboxes table (kept per explicit decision).
- Moving `last_activity` to DB (kept in memory per explicit decision).
- Multi-replica support (future work).
- Any new endpoints.
