# Snapshot-on-Stop Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace per-turn volume snapshotting with a single `snapshot_and_stop(sandbox)` helper that the server calls before any provider stop/delete. Remove the dirty-drain gating problem and cut per-turn latency.

**Architecture:** One trigger — the server's stop path — strictly serial: server → `POST /v1/snapshot` on the supervisor → await ack → call `stop_instance` / `destroy_instance`. Supervisor's per-turn snapshot in `handleAcpLine` goes away; SIGTERM `runSnapshotSync` is kept as a cheap safety net for external signals.

**Tech Stack:** Python 3.11+, FastAPI, psycopg v3, pytest. Node.js supervisor (`src/supervisor/supervisor.js`). Branch: `volume-refactor`.

**Reference:** `docs/volume-and-agent-architecture.md` (design doc). This plan implements the "Snapshot on sandbox stop" section only; one-sandbox-per-agent, per-session cwd, agent-scoped recovery, and agent-delete cleanup land in a follow-on plan.

---

## File Structure

| Path | Change |
|---|---|
| `src/supervisor/supervisor.js` | Add `POST /v1/snapshot` handler. Delete per-turn snapshot in `handleAcpLine`. Keep `runSnapshotSync` on SIGTERM. |
| `src/api/server.py` | Add `snapshot_supervisor(sandbox)` + `snapshot_and_stop(sandbox)` helpers. Wire reap loop, `/sandboxes/:id/stop`, `/sessions/:id/stop-sandbox` through `snapshot_and_stop`. Agent-delete currently does nothing on sandbox side (no wiring needed). |
| `tests/test_supervisor.py` | Existing file — add tests for `POST /v1/snapshot` endpoint behavior. Remove/update any test that asserts per-turn snapshot timing. |
| `tests/test_snapshot_on_stop.py` | **New** — end-to-end tests: concurrent turns do not race; stop-sandbox snapshots before provider call. |

No schema changes. No API-surface changes. Behavior change: turn-end no longer blocks on snapshot (~0.5–2s faster per turn on Daytona); sandbox stop blocks on snapshot instead.

---

## Invariants the plan must preserve

1. Server-initiated stop is durable: after `snapshot_and_stop(sandbox)` returns, `snapshot.tar` on the volume reflects `/home/daytona` at snapshot start.
2. Snapshot failure does **not** wedge the stop path. If snapshot returns non-200, log it and proceed to stop. The sandbox is dead either way; a transient S3 blip must not pin live resources.
3. The per-turn path no longer calls `runSnapshotOnce` — verify via absence of the tar/cp log lines on turn-end.

---

## Phase 1 — Supervisor endpoint

### Task 1: Add `POST /v1/snapshot` handler in supervisor.js

**Files:**
- Modify: `src/supervisor/supervisor.js:342-375` (the `handleSse` / route dispatch area — place the new handler alongside).
- Test: `tests/test_supervisor.py`

- [ ] **Step 1: Write the failing test**

Add to `tests/test_supervisor.py`:

```python
def test_post_v1_snapshot_triggers_tar_and_returns_200(tmp_path, supervisor):
    """POST /v1/snapshot runs the snapshot pipeline and returns 200 after
    the tarball is on disk."""
    snapshot_path = tmp_path / "snapshot.tar"
    sup = supervisor(snapshot_path=str(snapshot_path))
    # Write a marker file so we can prove the tarball captured it.
    (sup.root / "marker.txt").write_text("hello")

    r = httpx.post(f"{sup.url}/v1/snapshot", timeout=30)
    assert r.status_code == 200

    assert snapshot_path.exists(), "tarball should exist on volume"
    # Verify the marker is inside.
    import tarfile
    with tarfile.open(snapshot_path) as tf:
        names = tf.getnames()
    assert "./marker.txt" in names
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_supervisor.py::test_post_v1_snapshot_triggers_tar_and_returns_200 -v`
Expected: FAIL with 404 (endpoint does not exist) or similar.

- [ ] **Step 3: Add the handler in supervisor.js**

Inside the HTTP server's route dispatch (find the existing `handlePost` / `handleSse` routing — typically a `switch` on `req.method + req.url`). Add a branch for `POST /v1/snapshot`:

```javascript
async function handleSnapshot(req, res) {
  try {
    await runSnapshotOnce();
    res.writeHead(200, { "content-type": "application/json" });
    res.end(JSON.stringify({ ok: true }));
  } catch (e) {
    log(`snapshot endpoint error: ${e.message}`);
    res.writeHead(500, { "content-type": "application/json" });
    res.end(JSON.stringify({ error: String(e.message || e) }));
  }
}
```

Wire it in the router (same place that today routes `/v1/acp/:id` to `handleSse` / `handlePost`). Match on `req.method === "POST" && req.url === "/v1/snapshot"`.

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_supervisor.py::test_post_v1_snapshot_triggers_tar_and_returns_200 -v`
Expected: PASS.

- [ ] **Step 5: Add negative test for missing snapshot path**

When the supervisor is started without `--snapshot-path`, the endpoint should still succeed (no-op). `runSnapshotOnce` already returns immediately in that case (supervisor.js:153-156).

```python
def test_post_v1_snapshot_noop_when_no_snapshot_path(supervisor):
    sup = supervisor(snapshot_path=None)
    r = httpx.post(f"{sup.url}/v1/snapshot", timeout=5)
    assert r.status_code == 200
```

- [ ] **Step 6: Run both tests**

Run: `pytest tests/test_supervisor.py -k "snapshot" -v`
Expected: both PASS.

- [ ] **Step 7: Commit**

```bash
git add src/supervisor/supervisor.js tests/test_supervisor.py
git commit -m "feat(supervisor): add POST /v1/snapshot endpoint"
```

---

## Phase 2 — Server-side helpers

### Task 2: `snapshot_supervisor(sandbox)` — Python wrapper

**Files:**
- Modify: `src/api/server.py` (add near the other sandbox lifecycle helpers, e.g. after `_ensure_sandbox_alive` at server.py:1973).
- Test: `tests/test_snapshot_on_stop.py` (new).

Behavior contract:

```
given: SandboxRecord (provider, sandbox_ref, supervisor URL resolvable)
returns: None on success
raises: on unexpected exception — caller decides whether to propagate or swallow
side effects: calls POST /v1/snapshot on the sandbox's supervisor and awaits
              a 200. Logs and returns normally on non-2xx so callers can
              decide policy (typical: log + proceed to stop).
```

- [ ] **Step 1: Write the failing test**

Create `tests/test_snapshot_on_stop.py`:

```python
"""Tests for snapshot_on_stop behavior.

These tests assume a local/docker sandbox (no Daytona calls) and use the
real supervisor process. See tests/test_supervisor.py for the fixture setup.
"""

import asyncio
import pytest
from unittest.mock import AsyncMock, patch

from src.api.server import snapshot_supervisor
from src.api.models import SandboxRecord


@pytest.mark.asyncio
async def test_snapshot_supervisor_calls_endpoint(httpx_mock):
    sb = SandboxRecord(
        id="sb_test", provider="local", sandbox_ref="fake",
        status="running", root="/tmp", listen_port=12345,
    )
    httpx_mock.add_response(
        url="http://localhost:12345/v1/snapshot",
        method="POST", status_code=200, json={"ok": True},
    )
    # Patch ensure_supervisor_url if the helper calls through it; otherwise
    # snapshot_supervisor should read sandbox.derive_url() for port-based.
    await snapshot_supervisor(sb)
    # If we got here without raising, the call went through.
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_snapshot_on_stop.py::test_snapshot_supervisor_calls_endpoint -v`
Expected: FAIL with `ImportError: cannot import name 'snapshot_supervisor'`.

- [ ] **Step 3: Implement `snapshot_supervisor` in `src/api/server.py`**

Insert near `_ensure_sandbox_alive` (around server.py:1973):

```python
async def snapshot_supervisor(sandbox: SandboxRecord) -> None:
    """Call POST /v1/snapshot on the sandbox's supervisor and await the ack.

    Non-blocking w.r.t. stop policy: any non-200 response or transport error
    is logged, not raised. The caller (typically ``snapshot_and_stop``) then
    proceeds to stop the sandbox regardless — a transient volume error must
    not pin live resources, and the sandbox is about to die anyway.
    """
    vol = await get_volume(sandbox.volume_id) if sandbox.volume_id else None
    if vol is None:
        log.warning("snapshot_supervisor: sandbox %s has no volume; skipping",
                    sandbox.id)
        return

    # Resolve supervisor URL. Port-based providers (docker/local) derive from
    # the DB row; daytona uses the SDK-signed preview URL.
    from .providers import PORT_BASED_PROVIDERS
    try:
        if vol.provider in PORT_BASED_PROVIDERS:
            url = sandbox.derive_url()
        else:
            inst = ProviderInstance(
                provider=vol.provider, url="",
                root=sandbox.root, sandbox_id=sandbox.sandbox_ref,
            )
            url = await _providers_mod.ensure_supervisor_url(
                vol.provider, inst, agent_type="claude",
                root=sandbox.root, spawn_env={}, port=None,
            )
    except Exception as e:
        log.warning("snapshot_supervisor: cannot resolve URL for %s: %s",
                    sandbox.id, e)
        return

    async with httpx.AsyncClient(timeout=30) as client:
        try:
            r = await client.post(f"{url}/v1/snapshot")
            if r.status_code != 200:
                log.warning("snapshot_supervisor: %s returned %d: %s",
                            sandbox.id, r.status_code, r.text[:200])
        except Exception as e:
            log.warning("snapshot_supervisor: POST failed for %s: %s",
                        sandbox.id, e)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_snapshot_on_stop.py::test_snapshot_supervisor_calls_endpoint -v`
Expected: PASS.

- [ ] **Step 5: Add test for non-200 response (should log, not raise)**

```python
@pytest.mark.asyncio
async def test_snapshot_supervisor_swallows_500(httpx_mock, caplog):
    sb = SandboxRecord(
        id="sb_test", provider="local", sandbox_ref="fake",
        status="running", root="/tmp", listen_port=12345,
    )
    httpx_mock.add_response(
        url="http://localhost:12345/v1/snapshot",
        method="POST", status_code=500, text="boom",
    )
    # Should NOT raise.
    await snapshot_supervisor(sb)
    assert any("returned 500" in r.message for r in caplog.records)
```

- [ ] **Step 6: Run test**

Run: `pytest tests/test_snapshot_on_stop.py::test_snapshot_supervisor_swallows_500 -v`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add src/api/server.py tests/test_snapshot_on_stop.py
git commit -m "feat(server): add snapshot_supervisor helper"
```

### Task 3: `snapshot_and_stop(sandbox)` — the single stop entry point

**Files:**
- Modify: `src/api/server.py` (insert directly below `snapshot_supervisor`).
- Test: `tests/test_snapshot_on_stop.py`.

Behavior contract:

```
given: SandboxRecord (must be live at entry; caller checks)
returns: None
side effects: calls snapshot_supervisor(sandbox), awaits it, then calls
              stop_instance on the provider. Never raises from the snapshot
              step; may raise from stop_instance (caller logs and continues
              or propagates per existing policy).
```

- [ ] **Step 1: Write the failing test**

```python
@pytest.mark.asyncio
async def test_snapshot_and_stop_orders_snapshot_before_stop():
    """snapshot_supervisor must be called and awaited BEFORE stop_instance."""
    sb = SandboxRecord(
        id="sb_test", provider="local", sandbox_ref="fake",
        status="running", root="/tmp", listen_port=12345,
    )
    call_order = []

    async def fake_snapshot(s):
        call_order.append(("snapshot", s.id))
        await asyncio.sleep(0.01)

    async def fake_stop(inst):
        call_order.append(("stop", inst.sandbox_id))

    with patch("src.api.server.snapshot_supervisor", new=fake_snapshot), \
         patch("src.api.server.stop_instance", new=fake_stop):
        from src.api.server import snapshot_and_stop
        inst = ProviderInstance(
            provider="local", url="http://localhost:12345",
            root="/tmp", sandbox_id="fake",
        )
        await snapshot_and_stop(sb, inst)

    assert call_order == [("snapshot", "sb_test"), ("stop", "fake")]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_snapshot_on_stop.py::test_snapshot_and_stop_orders_snapshot_before_stop -v`
Expected: FAIL with `ImportError: cannot import name 'snapshot_and_stop'`.

- [ ] **Step 3: Implement `snapshot_and_stop`**

Add to `src/api/server.py` directly below `snapshot_supervisor`:

```python
async def snapshot_and_stop(
    sandbox: SandboxRecord, instance: ProviderInstance | None = None
) -> None:
    """Snapshot the sandbox's workspace, then stop it at the provider.

    This is the single entry point for every server-initiated sandbox stop:
    reap, manual /stop, sandbox-replacement during recovery, agent-delete.
    Ordering is strict — snapshot before stop, even if snapshot fails
    (we log the snapshot failure and still tear the sandbox down so a
    transient volume error can't wedge lifecycle).

    ``instance`` may be None if the caller doesn't have one handy; in that
    case we fall back to a synthesized ProviderInstance from the sandbox
    row, which is sufficient for stop_instance's needs.
    """
    try:
        await snapshot_supervisor(sandbox)
    except Exception as e:
        log.warning("snapshot_and_stop: snapshot failed for %s: %s; "
                    "proceeding to stop anyway", sandbox.id, e)

    inst = instance or _INSTANCES.get(sandbox.id)
    if inst is None:
        vol = await get_volume(sandbox.volume_id) if sandbox.volume_id else None
        provider = vol.provider if vol else sandbox.provider
        inst = ProviderInstance(
            provider=provider, url="",
            root=sandbox.root, sandbox_id=sandbox.sandbox_ref,
        )
    await stop_instance(inst)
```

- [ ] **Step 4: Run test**

Run: `pytest tests/test_snapshot_on_stop.py::test_snapshot_and_stop_orders_snapshot_before_stop -v`
Expected: PASS.

- [ ] **Step 5: Add test for snapshot-failure-does-not-block-stop**

```python
@pytest.mark.asyncio
async def test_snapshot_and_stop_proceeds_when_snapshot_raises():
    sb = SandboxRecord(
        id="sb_test", provider="local", sandbox_ref="fake",
        status="running", root="/tmp", listen_port=12345,
    )
    called = []

    async def boom(_s):
        raise RuntimeError("S3 is down")

    async def fake_stop(inst):
        called.append("stop")

    with patch("src.api.server.snapshot_supervisor", new=boom), \
         patch("src.api.server.stop_instance", new=fake_stop):
        from src.api.server import snapshot_and_stop
        inst = ProviderInstance(
            provider="local", url="http://localhost:12345",
            root="/tmp", sandbox_id="fake",
        )
        await snapshot_and_stop(sb, inst)

    assert called == ["stop"], "stop must still happen even when snapshot raises"
```

- [ ] **Step 6: Run both tests**

Run: `pytest tests/test_snapshot_on_stop.py -v`
Expected: all PASS.

- [ ] **Step 7: Commit**

```bash
git add src/api/server.py tests/test_snapshot_on_stop.py
git commit -m "feat(server): add snapshot_and_stop helper"
```

---

## Phase 3 — Wire stop paths through `snapshot_and_stop`

### Task 4: Reap loop uses `snapshot_and_stop`

**Files:**
- Modify: `src/api/server.py:307-326` (the reap loop body).
- Test: `tests/test_snapshot_on_stop.py`.

Current code at server.py:307-326 calls `stop_instance(instance)` directly. Replace with a `snapshot_and_stop` call.

- [ ] **Step 1: Write the failing test**

```python
@pytest.mark.asyncio
async def test_reap_calls_snapshot_then_stop(monkeypatch):
    """When the reaper decides a sandbox is idle, it must snapshot before
    stopping."""
    # Fixture: single idle session, reap loop runs one tick.
    from src.api import server as srv

    order = []
    async def fake_snap_and_stop(sb, inst=None):
        order.append(("snapshot_and_stop", sb.id))

    monkeypatch.setattr(srv, "snapshot_and_stop", fake_snap_and_stop)
    # Inject an idle SessionState backed by a fake sandbox; trigger one
    # iteration of the reaper body (factor it into a helper if needed).
    # See existing reaper test patterns in tests/test_sandbox_reaper.py.
    await srv._reap_one_tick(force_now=time.time() + srv.IDLE_TIMEOUT_S + 1)

    assert any(x[0] == "snapshot_and_stop" for x in order)
```

`_reap_one_tick` does not exist yet — it's a small refactor you do inside this task (Step 3 below) to make the reap body callable from a test without running the full 60-second ticker loop. See existing reap loop at server.py:274-326.

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_snapshot_on_stop.py::test_reap_calls_snapshot_then_stop -v`
Expected: FAIL — reaper still calls `stop_instance` directly.

- [ ] **Step 3: Refactor reap body into `_reap_one_tick(now)` helper**

Extract lines 287-326 of `_idle_reaper` into a standalone `async def _reap_one_tick(now: float) -> None`, then call it from `_idle_reaper`'s loop. This is a pure refactor — no behavior change.

- [ ] **Step 4: Replace direct `stop_instance` with `snapshot_and_stop`**

In `_reap_one_tick`, replace server.py:316-318:

```python
# BEFORE
await stop_instance(instance)
log.info("idle reaper: sandbox %s stopped", sandbox_id)

# AFTER
rec = await get_sandbox(sandbox_id)
if rec is not None:
    await snapshot_and_stop(rec, instance)
    log.info("idle reaper: sandbox %s snapshotted + stopped", sandbox_id)
else:
    # Row already gone (concurrent delete) — just stop the instance.
    await stop_instance(instance)
    log.info("idle reaper: sandbox %s (no row) stopped", sandbox_id)
```

- [ ] **Step 5: Run test**

Run: `pytest tests/test_snapshot_on_stop.py::test_reap_calls_snapshot_then_stop -v`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/api/server.py tests/test_snapshot_on_stop.py
git commit -m "refactor(reaper): route stop path through snapshot_and_stop"
```

### Task 5: `/sandboxes/{id}/stop` endpoint uses `snapshot_and_stop`

**Files:**
- Modify: `src/api/server.py:1622-1660` (`stop_sandbox_route`).

- [ ] **Step 1: Read the existing endpoint**

Confirm the current implementation calls `stop_instance` directly.

- [ ] **Step 2: Write the test**

```python
@pytest.mark.asyncio
async def test_stop_sandbox_route_snapshots_first(monkeypatch, test_client):
    """POST /sandboxes/:id/stop must trigger snapshot before stop."""
    from src.api import server as srv

    order = []
    async def fake_snap_and_stop(sb, inst=None):
        order.append(sb.id)

    monkeypatch.setattr(srv, "snapshot_and_stop", fake_snap_and_stop)
    # Fixture creates sandbox "sb_test" and registers it in _INSTANCES.
    r = await test_client.post("/sandboxes/sb_test/stop")
    assert r.status_code == 200
    assert order == ["sb_test"]
```

- [ ] **Step 3: Run test — expected FAIL**

- [ ] **Step 4: Update `stop_sandbox_route`**

Replace the direct `stop_instance` call with `snapshot_and_stop`. Preserve the existing DB-update + error-handling structure at server.py:1643-1660.

- [ ] **Step 5: Run test — expected PASS**

- [ ] **Step 6: Commit**

```bash
git add src/api/server.py tests/test_snapshot_on_stop.py
git commit -m "refactor(api): /sandboxes/:id/stop uses snapshot_and_stop"
```

### Task 6: `/sessions/{id}/stop-sandbox` snapshots before destroy

**Files:**
- Modify: `src/api/server.py:3228-3246` (`stop_session_sandbox`).
- Test: `tests/test_snapshot_on_stop.py`.

**Note:** this endpoint is misnamed — it calls `destroy_sandbox` (full delete, not `stop_instance`). We still want the snapshot to land before the destroy so the session can resume later on a replacement sandbox. Call `snapshot_supervisor(sb)` directly rather than `snapshot_and_stop` (the latter calls `stop_instance`, which is the wrong teardown here).

- [ ] **Step 1: Write the failing test**

```python
@pytest.mark.asyncio
async def test_stop_session_sandbox_snapshots_before_destroy(monkeypatch):
    from src.api import server as srv

    order = []
    async def fake_snap(sb):
        order.append(("snapshot", sb.id))
    async def fake_destroy(prov, inst):
        order.append(("destroy", inst.sandbox_id))

    monkeypatch.setattr(srv, "snapshot_supervisor", fake_snap)
    monkeypatch.setattr(srv._providers_mod, "destroy_sandbox", fake_destroy)
    # Use the same in-process FastAPI client pattern used in tests/test_*_route.py
    from fastapi.testclient import TestClient
    # ... fixture setup: create session row with current_sandbox_id=sb_test ...
    with TestClient(srv.app) as c:
        r = c.post("/sessions/sess_test/stop-sandbox")
    assert r.status_code == 204
    assert [x[0] for x in order] == ["snapshot", "destroy"]
```

- [ ] **Step 2: Run test — expected FAIL** (no snapshot call today).

- [ ] **Step 3: Update `stop_session_sandbox` at server.py:3228-3246**

Insert `await snapshot_supervisor(sb)` before `destroy_sandbox` (and swallow its failures — the sandbox is about to die):

```python
@app.post("/sessions/{session_id}/stop-sandbox", status_code=204)
async def stop_session_sandbox(session_id: str):
    sess = await _require_session_row(session_id)
    sbid = sess.get("current_sandbox_id")
    if sbid is None:
        return
    sb = await get_sandbox(sbid)
    if sb:
        try:
            await snapshot_supervisor(sb)
        except Exception as e:
            log.warning("stop_session_sandbox: snapshot failed for %s: %s; "
                        "proceeding to destroy", sb.id, e)
        inst = ProviderInstance(
            provider=sb.provider, url="",
            root=sb.root, sandbox_id=sb.sandbox_ref,
        )
        try:
            await _providers_mod.destroy_sandbox(sb.provider, inst)
        except Exception:
            pass  # best-effort
    await set_session_current_sandbox(session_id, None)
    await delete_sandbox(sbid)
```

- [ ] **Step 4: Run test — expected PASS**

- [ ] **Step 5: Commit**

```bash
git add src/api/server.py tests/test_snapshot_on_stop.py
git commit -m "refactor(api): /sessions/:id/stop-sandbox snapshots before destroy"
```

---

## Phase 4 — Remove per-turn snapshot from supervisor

### Task 7: Delete per-turn snapshot call in `handleAcpLine`

**Files:**
- Modify: `src/supervisor/supervisor.js:260-274` (the `if (isPromptResponse)` block).
- Test: `tests/test_supervisor.py`.

- [ ] **Step 1: Write the test — turn-end does NOT create a snapshot**

```python
def test_turn_end_does_not_snapshot(tmp_path, supervisor, acp_prompt):
    """After the per-turn snapshot removal, a prompt response must not
    touch the snapshot path."""
    snapshot_path = tmp_path / "snapshot.tar"
    sup = supervisor(snapshot_path=str(snapshot_path))
    acp_prompt(sup, "say hi")  # sends a prompt, awaits response
    # Snapshot should not exist yet — no stop call happened.
    assert not snapshot_path.exists(), (
        "per-turn snapshot was triggered; expected snapshot-on-stop only"
    )
```

- [ ] **Step 2: Run test — expected FAIL** (snapshot exists today)

Run: `pytest tests/test_supervisor.py::test_turn_end_does_not_snapshot -v`

- [ ] **Step 3: Remove the per-turn snapshot block**

In `src/supervisor/supervisor.js`, delete lines 259-274 (the `isPromptResponse` / `pendingPromptIds.delete(rid)` / `await runSnapshotOnce()` block). Also remove the `pendingPromptIds` Set declaration at line 124 and its population at line 321-323 — they're now unused.

After the edit, `handleAcpLine` looks roughly like:

```javascript
async function handleAcpLine(line) {
  broadcastSse(line);
  let msg = null;
  try { msg = JSON.parse(line); } catch { return; }
  if (
    msg && typeof msg === "object" && "id" in msg &&
    ("result" in msg || "error" in msg)
  ) {
    const rid = String(msg.id);
    const resolver = pendingResponses.get(rid);
    if (resolver) {
      pendingResponses.delete(rid);
      resolver(msg);
    }
  }
}
```

- [ ] **Step 4: Run test — expected PASS**

- [ ] **Step 5: Regression check — existing supervisor tests must still pass**

Run: `pytest tests/test_supervisor.py -v`
Expected: all PASS (no test should rely on per-turn snapshot timing; if any does, it was checking an implementation detail and should be deleted).

- [ ] **Step 6: Commit**

```bash
git add src/supervisor/supervisor.js tests/test_supervisor.py
git commit -m "refactor(supervisor): remove per-turn snapshot, rely on snapshot-on-stop"
```

### Task 8: Verify concurrent-turn race is gone

**Files:**
- Test: `tests/test_snapshot_on_stop.py`.

This is the regression test for the original bug: two concurrent ACP sessions each finishing a turn must not corrupt the snapshot tarball. With per-turn snapshotting removed, there's no tarball being written during turns, so the race is structurally impossible — but we verify it with an end-to-end test.

- [ ] **Step 1: Write the test**

```python
@pytest.mark.asyncio
async def test_concurrent_turn_ends_no_race(tmp_path, supervisor):
    """Two simultaneous ACP prompts finishing at ~the same time must not
    corrupt /tmp/agent-sdk-snapshot.tar (since we no longer snapshot on turn-end)."""
    sup = supervisor(snapshot_path=str(tmp_path / "snapshot.tar"))
    # Open two ACP sessions via the session/new RPC
    sid1 = await sup.new_session()
    sid2 = await sup.new_session()
    # Fire both prompts in parallel
    await asyncio.gather(
        sup.prompt(sid1, "echo a"),
        sup.prompt(sid2, "echo b"),
    )
    # Now explicitly ask for a snapshot; both sessions' state should be captured.
    r = await httpx.AsyncClient().post(f"{sup.url}/v1/snapshot")
    assert r.status_code == 200
    import tarfile
    with tarfile.open(tmp_path / "snapshot.tar") as tf:
        names = set(tf.getnames())
    # Both sessions' JSONLs should be present under .claude/projects/.
    assert any(".jsonl" in n for n in names), f"no JSONL in snapshot: {sorted(names)[:20]}"
```

- [ ] **Step 2: Run test — expected PASS**

Run: `pytest tests/test_snapshot_on_stop.py::test_concurrent_turn_ends_no_race -v`

- [ ] **Step 3: Commit**

```bash
git add tests/test_snapshot_on_stop.py
git commit -m "test: verify concurrent turns don't race under snapshot-on-stop"
```

---

## Phase 5 — Cleanup and verification

### Task 9: Audit for orphan references

- [ ] **Step 1: Grep for stale references**

Run: `grep -rn "pendingPromptIds\|runSnapshotOnce" src/supervisor/`
Expected output:
- `pendingPromptIds` — zero hits (removed).
- `runSnapshotOnce` — exactly 2 hits: its definition (~line 151) and its call from `handleSnapshot` (added in Task 1).

If there are stray references, remove them.

- [ ] **Step 2: Grep for direct `stop_instance` calls outside the helper**

Run: `grep -n "stop_instance(" src/api/server.py`
Expected: every call site either (a) lives inside `snapshot_and_stop`, or (b) has a deliberate comment explaining why snapshot is skipped. The recovery path (`_recover_missing_sandbox`) is the legitimate exception — the sandbox is already dead so snapshot is impossible.

- [ ] **Step 3: Run the full test suite**

Run: `pytest tests/ -v`
Expected: all PASS.

- [ ] **Step 4: Run the live demo (manual)**

```bash
./scripts/demo.py
```

Verify: prompt goes through, response returns noticeably faster than before (the ~0.5–2s per-turn snapshot latency is gone), and subsequent sessions resume conversation state correctly after a `/sandboxes/:id/stop` → new message.

- [ ] **Step 5: Commit if any cleanup changes were made**

```bash
git add -A
git commit -m "chore: remove orphan snapshot references"
```

---

## What's next

After this plan lands, the natural follow-on is the one-sandbox-per-agent refactor (design doc §"One live sandbox per agent" and onward). That plan will:

- Move `current_sandbox_id` from `sessions` to `agents`.
- Add `ensure_sandbox_for_agent(agent_id, volume_id)` and rewire `ensure_sandbox(session_row)` to route through it.
- Add per-session cwd at `/home/daytona/sessions/<session_id>/`.
- Add agent-scoped recovery lock.
- Wire `snapshot_and_stop` into the agent-delete path and the sandbox-replacement path.
- Drop the per-turn reap TTL to 3 minutes and add `AgentConfig.idle_ttl_seconds` override.

All of those depend on `snapshot_and_stop` existing, which is why it ships first.
