# Sandbox lifecycle unification

A refactor proposal. The goal is to collapse the sandbox-lifecycle surface
in `src/api/server.py` and the per-provider files onto a single
check-and-act function, a single lock, and one source of truth per concern.
Reviewable artifact — implementation happens in stages against a plan
derived from this doc.

## Motivation

A sandbox's state is scattered across four views that have to be kept in
sync by every lifecycle-touching call site:

| Layer | What it claims to know |
|---|---|
| `sandboxes` DB row | identity, provider, `sandbox_ref`, `status`, port |
| `_INSTANCES` dict (`server.py`) | live `ProviderInstance`, URL, cached Popen |
| provider registry (`_PROCESSES` on local / Daytona SDK / `docker inspect`) | the actual live process/container |
| `SESSIONS[session_id]` (`server.py`) | which sandbox a session is attached to, live `AcpClient` |

Because there are four views, every new lifecycle event requires four
reconciliations, and any drift is a class of bugs:

- Two "ensure" functions for the same concept — `_ensure_sandbox_locked`
  (session-locked, called from POST `/message`) and `_ensure_sandbox_alive`
  (sandbox-locked, called from the SSE-reader recovery branch).
- Two lock choices — session vs sandbox — chosen per-site.
- Three overlapping "is this alive?" checks — `process.poll()`,
  `provider.get_sandbox_status()`, `_wait_for_health(url)`.
- Stale-`_INSTANCES` bugs (fast-path returns a URL whose process has been
  reaped). This is the class of bug the PID probe + zombie-reap logic in
  `_ensure_sandbox_locked` was added to patch around
  (server.py:2420-2460).
- Compound lifecycle operations (`snapshot_and_stop`) that try to be
  atomic but can't reasonably hold a lock for the slow half (a multi-GB
  tar on Daytona is 30-60s).

`server.py` is 3764 lines; 33 call sites touch `ensure_*`/`_provision_new`.
Adding a new lifecycle event (pause, clone, suspend) currently requires
edits in 4+ places. That's the thing we're trying to fix.

## The rule

One line captures the entire reframing:

> **The provider is the truth for liveness. The DB row is the truth for
> identity. Everything else is a cache.**

Once that's accepted, most of the current complexity collapses.

- Liveness: `provider.get_sandbox_status(ref)`. Cheap by construction
  (local = dict + `poll()`, docker = `docker inspect` ~20ms, daytona =
  an in-memory SDK check). Called whenever we need to know; no caching.
- Identity: `sandboxes.<id>` row. The only source of `sandbox_ref`,
  `volume_id`, `subpath`.
- URL (derived state): a tiny memoization keyed by `sandbox_id`. For
  port-based providers it's `http://localhost:{port}`, always
  recomputable. For Daytona it's the signed preview URL, recomputable
  but non-trivial — cached but never authoritative.
- `_INSTANCES` stops carrying state. It's gone, or at most a URL memo.
- `SESSIONS[sid].client` stops caching against sandbox identity — it
  carries an `AcpClient`, invalidated on any sandbox lifecycle event.

## Target design

### Two layers, two functions

Session liveness and sandbox liveness are separate questions. Keeping them
in separate functions keeps each one simple.

- **Sandbox layer** answers *"given an existing sandbox, is its supervisor
  up?"* — one function, `ensure_live(sandbox_id)`.
- **Session layer** answers *"does this session even have a sandbox? if
  not, provision one; either way, hand the caller a live URL."* — a thin
  wrapper on top.

### The sandbox-layer primitive

```python
async def ensure_live(sandbox_id: str) -> tuple[SandboxRecord, str]:
    """Return (record, supervisor_url) with the supervisor confirmed alive.

    Idempotent. On entry the supervisor may be in any state; on exit it's
    running and reachable. Holds the per-sandbox lock for the check-and-act
    sequence so concurrent callers don't double-provision. Emits a
    ``sandbox_reattach`` event on replacement.
    """
    async with _sandbox_lock(sandbox_id):
        rec = await get_sandbox(sandbox_id)
        if rec is None:
            raise HTTPException(404, "sandbox missing")

        status = await provider(rec).get_sandbox_status(rec.sandbox_ref)

        if status == "running":
            return rec, _resolve_url(rec)

        if status == "stopped":
            await provider(rec).start_sandbox(rec.sandbox_ref)
            await _mark_status(rec, "running")
            return rec, _resolve_url(rec)

        # missing / error — replace in place (same sandbox_id, new ref)
        new_rec = await _replace(rec)
        return new_rec, _resolve_url(new_rec)
```

State transitions are explicit and total — every `get_status` return value
maps to exactly one action:

| `get_status(ref)` → | Action | Result |
|---|---|---|
| `running` | (none) | same `sandbox_ref`, same URL |
| `stopped` | `provider.start(ref)` | same `sandbox_ref`, supervisor revived |
| `missing` / `error` | `_replace(rec)` — new provider ref, same `sandboxes.id` row, same `volume_id` + `subpath` + `root` | same logical sandbox, new underlying instance |

The `sandboxes.id` row stays stable through every transition. Sessions
point at the DB id, not at `sandbox_ref` — so *sessions are never
disrupted by a replace.* The only case that surfaces as an error is
"row deleted out-of-band" (someone hit `DELETE /sandboxes/:id` during
the call), which propagates to the caller as a genuine 404.

### The session-layer wrapper

```python
async def ensure_session_alive(session_id: str) -> tuple[dict, str]:
    """Return (session_row, supervisor_url) ready to serve a request.

    Provisions a sandbox if the session doesn't have one yet, then ensures
    it's live. Session-scoped runtime state (the ACP client, SSE reader,
    prompt queue in ``SESSIONS[sid]``) is attached on top — invalidated
    when ``ensure_live`` returns a URL different from the one the session
    remembered.
    """
    session = await get_session(session_id)
    if session is None:
        raise HTTPException(404, "session missing")
    if session["current_sandbox_id"] is None:
        sandbox_id = await _provision_sandbox_for_session(session)
        await set_session_current_sandbox(session_id, sandbox_id)
        session["current_sandbox_id"] = sandbox_id
    rec, url = await ensure_live(session["current_sandbox_id"])
    return session, url
```

Every POST-path call-site becomes three lines:

```python
session, url = await ensure_session_alive(session_id)
runtime = await ensure_runtime(session, url)   # ACP client + SSE reader
# do endpoint-specific work with runtime
```

### Lifecycle verbs

Every other lifecycle operation is three lines, same shape, same lock:

```python
async def stop(sandbox_id: str) -> None:
    async with _sandbox_lock(sandbox_id):
        rec = await get_sandbox(sandbox_id)
        if rec is None: return
        await provider(rec).stop_sandbox(_instance_for(rec))
        await _mark_status(rec, "stopped")

async def destroy(sandbox_id: str) -> None:
    async with _sandbox_lock(sandbox_id):
        rec = await get_sandbox(sandbox_id)
        if rec is None: return
        await provider(rec).destroy_sandbox(_instance_for(rec))
        await delete_sandbox(sandbox_id)
```

No `snapshot_and_stop` compound. Durability lives entirely at turn-end in
the supervisor, as it already does (the supervisor writes `snapshot.tar`
synchronously before returning `turn-done` to the client). Stop is a
fast kill; the invariant is still "client sees turn-done ⇒ durable."

### Replacing state with memos

`_INSTANCES` is either deleted or demoted to:

```python
# Pure URL memo. Lost on server restart → rebuilt on next ensure_live.
_url_memo: dict[str, str] = {}
```

For port-based providers we don't even need the memo — the URL is
derivable from the DB row. Daytona's signed preview URL populates the
memo on first resolution.

`SESSIONS[sid]` still exists for session-scoped state (ACP client, SSE
reader, prompt queue) but stops trying to track sandbox liveness itself.
Any cached `client` is invalidated when `ensure_live` returns a different
URL than the one the session remembered.

## Consequences

What falls out for free once we have one function and one lock:

- **Stop-during-request race** (the question that motivated this doc):
  resolved. `stop` holds the sandbox lock; concurrent `ensure_live` waits
  briefly (kill is fast), wakes up, sees status=stopped, transitions
  through the "stopped" branch or "replace" branch cleanly.
- **SSE-reader vs `/message` double-provision race**: gone. Both paths
  call `ensure_live`; one wins the lock, the other waits and sees the
  completed transition.
- **`_INSTANCES` staleness**: impossible — nothing depends on it except
  the URL memo, which is cheap to rebuild on miss.
- **Fast-path liveness zombie-reaping**
  (server.py:2420-2460 and `os.kill(pid, 0)` gymnastics): gone. A single
  `provider.get_sandbox_status(ref)` is the truth.
- **Two "ensure" functions** (`_ensure_sandbox_locked` +
  `_ensure_sandbox_alive`) collapse into one. `_provision_new`,
  `_recover_missing_sandbox`, the SSE-reader's recovery block — all
  disappear into `ensure_live` + `_replace`.
- **Adding a new lifecycle event** (pause, snapshot, clone, suspend):
  one new verb with the same 3-line shape.

## Migration

Staged. Never big-bang — both the old and new paths coexist until every
call-site has moved over.

1. **Stage 0 — Skeleton.** Add `ensure_live`, `stop`, `destroy`,
   `_replace`, `_resolve_url`, `_mark_status` alongside the existing
   code. No call-site changes. Tests that exercise them directly. Ship
   as one PR.

2. **Stage 1 — Read paths.** Rewire every read path (POST `/message`,
   POST `/sessions/quick` follow-ups, `/cancel`, `/reset-sandbox`,
   `/exec`, `/files/*`) to go through `ensure_live`. The old
   `ensure_sandbox` + `ensure_runtime` stay — `ensure_live` returns the
   same shape. Ship per-endpoint, verify green tests between each.

3. **Stage 2 — Stop paths.** Replace `snapshot_and_stop` call sites
   (reap, `/sandboxes/:id/stop`, `/sessions/:id/stop-sandbox`,
   admin-reap, lifespan shutdown) with plain `stop`. Delete
   `snapshot_and_stop` and `snapshot_supervisor` from `server.py`.
   Supervisor `POST /v1/snapshot` endpoint stays (useful as an explicit
   tool).

4. **Stage 3 — SSE-reader recovery.** The reader's recovery branch
   (~100 lines in server.py:640-800) becomes `await ensure_live(...)`.
   Most of the block deletes.

5. **Stage 4 — Delete the old.** Remove `_ensure_sandbox_locked`,
   `_ensure_sandbox_alive`, `_ensure_runtime_locked`, `ensure_sandbox`,
   `ensure_runtime`, `_provision_new`, `_recover_missing_sandbox`, the
   `_INSTANCES` dict (or shrink to the URL memo). Expect ~500-line net
   reduction.

Each stage keeps the golden recovery tests + non-integration suite
green. The invariants tested by the golden tests don't change.

## What we give up

One real trade, plus one semantic change worth being upfront about.

### Real trade: "force a snapshot right now" stops being implicit

Today, calling `snapshot_and_stop` guarantees that *any* workspace change
since the last turn-end — including non-ACP writes like a background
process the agent spawned, a daemon, a stray file dropped outside an
ACP turn — is captured on the volume before SIGTERM. The stop path
always runs a fresh snapshot.

Under the new design, `stop` is just SIGTERM. Filesystem state captured
on the volume reflects the *most recent completed turn-end*. Anything
that happened between that turn-end and the stop call is not on the
volume — it lives only in the now-killed sandbox's ephemeral rootfs.

**For normal ACP agents this is a non-issue.** Claude Code's state
changes go through ACP turns; every turn-end already runs the
supervisor's per-turn snapshot; so by construction there's nothing
between-turn to lose.

**For unusual workloads** (a user starts a `sleep 60 && touch foo`, then
hits `/stop` at second 30), the between-turn write is lost. If that
matters, the caller can explicitly hit `POST /v1/snapshot` on the
supervisor before `/stop` — two lines client-side. The supervisor
endpoint stays in the design exactly so this knob is available; it just
stops being wired into every routine stop.

### Semantic change: `POST /sandboxes/:id/stop` returns fast

Client-observable latency goes from "seconds (tar + cp + SIGTERM)" to
"milliseconds (SIGTERM)". This is good, but integrations that
implicitly depend on "the stop endpoint returned, therefore durability
writes are flushed" need to be aware the flush isn't coupled to stop
anymore. The turn-end invariant is unchanged: **client sees turn-done ⇒
turn is durable on the volume**. That remains the load-bearing
guarantee.

## What stays

- Provider interface (`create_sandbox`, `start_sandbox`, `stop_sandbox`,
  `destroy_sandbox`, `get_sandbox_status`) — unchanged.
- Per-turn supervisor snapshot (the durability layer) — unchanged.
- Session-scoped state (`SESSIONS`, pending prompts, SSE readers) —
  unchanged; only loses its sandbox-liveness caching role.
- Golden recovery tests — unchanged, same invariants, should all
  continue to pass.

## Open questions (defer)

- **Per-provider `_resolve_url`:** for port-based providers the URL is
  trivial. For Daytona the SDK-signed URL is valid for a bounded
  lifetime — how long, and do we need to re-mint? Investigate before
  Stage 1.
- **`SandboxRecord` carrying provider:** right now we fetch the volume
  row to learn the provider (e.g.
  `await get_volume(sandbox.volume_id)`). If we denormalize provider
  onto the sandbox row, `provider(rec)` is a field access. Cheap win,
  orthogonal.
- **Sandbox-level recovery lock:** already held by `_sandbox_lock`;
  doesn't need to change. But the session-level lock (`_session_lock`)
  is still needed for `SESSIONS` mutations — keep it, just don't reuse
  it for sandbox lifecycle.

## Success criteria

- A new lifecycle event can be added with one new verb in one file.
- `server.py` drops ≥400 lines net.
- No path calls both `ensure_live` and `_ensure_*` — only one is used.
- Golden recovery tests pass on all three providers.
- A concurrent `/message` arriving during a stop waits ms, not seconds,
  and never returns 500 from the race.
