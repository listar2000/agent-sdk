# Session / Volume / Sandbox Decoupling

Date: 2026-04-21
Status: Draft (awaiting user review)

## Problem

Today, `agent-sdk` binds a session to a sandbox: `sessions.sandbox_id → sandboxes(id) ON DELETE CASCADE`. When the sandbox dies (idle timeout, crash, node loss), the session dies with it, taking its Claude Code CLI state (OAuth, project history, in-progress transcripts) with it.

We want to:

1. Treat sandboxes as ephemeral compute. A sandbox can die or be swapped without losing the session.
2. Introduce a first-class `Volume` as durable storage. The session's home directory and all CLI state live on the volume.
3. Scope volumes to a user-defined "sub-project". Multiple sessions/agents working on the same sub-project share a volume; each gets its own subpath so their HOMEs don't collide.

## Goals

- A session survives sandbox replacement. Its conversation continues where it left off.
- Users explicitly create/list/delete volumes; sessions opt into one by `volume_id` at creation.
- Concurrent sessions on the same volume run in parallel, each in an isolated HOME subpath.
- A shared read-only area exists on each volume for cross-session data sharing.
- Supported on Daytona first, then Docker, then local.

## Non-goals

- Hot-attaching a volume to a running sandbox. Daytona does not support it; we reprovision instead.
- Cross-volume data movement primitives. Users write to `shared/` out-of-band or via a future API.
- Per-session volumes as the default. Users can still opt in by creating a 1-session volume, but the norm is shared.

## Model

### Concepts

| | |
|---|---|
| **Volume** | User-scoped, persistent storage. Backed by a provider-native volume (Daytona volume, Docker named volume, host dir). Owns a namespace of per-session HOME subpaths + one shared area. |
| **Session** | Durable logical conversation. Anchored to exactly one volume. Has a stable id used as its subpath on the volume. |
| **Sandbox** | Ephemeral compute lease for a single session. Created lazily on first run; can be killed and replaced without touching session or volume state. |

### Relationships

- `Session → Volume`: required, immutable, `ON DELETE RESTRICT` (cannot delete a volume with live sessions except via `force=True`).
- `Session → current Sandbox`: nullable, swappable, `ON DELETE SET NULL` (sandbox row can disappear without killing the session).
- `Sandbox → Session`: every sandbox belongs to exactly one session (invariant, not a DB FK).
- `Sandbox → Volume`: inherited from its session at creation time. Not stored separately.

### Filesystem layout on a volume

```
<volume>/
├── sessions/
│   ├── <session-id-1>/
│   │   └── home/           ← mounted writable at /home/daytona
│   │       ├── .claude/    ← OAuth, projects/, transcripts, todos
│   │       ├── .claude.json
│   │       └── workspace/  ← session cwd
│   └── <session-id-2>/
│       └── home/
│           └── ...
└── shared/                 ← mounted read-only at /mnt/shared in every sandbox on this volume
```

### Per-sandbox mounts

Every sandbox on a volume mounts the volume twice:

```python
volumes=[
    VolumeMount(volume_id=v.provider_ref,
                mount_path="/home/daytona",
                subpath=f"sessions/{session.id}/home"),       # writable, private
    VolumeMount(volume_id=v.provider_ref,
                mount_path="/mnt/shared",
                subpath="shared",
                read_only=True),                               # read-only, cross-session
]
```

## Schema changes

New table:

```sql
CREATE TABLE IF NOT EXISTS volumes (
    id           TEXT PRIMARY KEY,
    name         TEXT NOT NULL UNIQUE,
    provider     TEXT NOT NULL,            -- "daytona" | "docker" | "local"
    provider_ref TEXT NOT NULL,            -- provider-native id
    status       TEXT NOT NULL,            -- "ready" | "error" | "deleting"
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_volumes_name ON volumes(name);
```

Sessions changes:

```sql
ALTER TABLE sessions ADD COLUMN IF NOT EXISTS volume_id TEXT REFERENCES volumes(id) ON DELETE RESTRICT;
ALTER TABLE sessions RENAME COLUMN sandbox_id TO current_sandbox_id;
ALTER TABLE sessions ALTER COLUMN current_sandbox_id DROP NOT NULL;
ALTER TABLE sessions DROP CONSTRAINT IF EXISTS sessions_sandbox_id_fkey;
ALTER TABLE sessions ADD CONSTRAINT sessions_current_sandbox_id_fkey
    FOREIGN KEY (current_sandbox_id) REFERENCES sandboxes(id) ON DELETE SET NULL;
```

`session_log.sandbox_id`: allow NULL, drop CASCADE (keep for audit, no longer lifecycle-coupled).

Backfill on first startup after migration: create a default "legacy" volume per provider actively in use (based on existing sandbox rows' providers), point all existing sessions without a `volume_id` at the matching legacy volume. Existing `current_sandbox_id` values stay valid until the sandbox dies, at which point the session rejoins the new world via the reprovision path.

After the backfill step completes successfully, a subsequent migration adds the `NOT NULL` constraint:

```sql
ALTER TABLE sessions ALTER COLUMN volume_id SET NOT NULL;
```

This must run after backfill; it lives at the bottom of `_MIGRATIONS` below the backfill logic.

## API surface

### Volumes (new, explicit CRUD)

SDK:

```python
client.volumes.create(name: str, provider: str) -> Volume
client.volumes.get(id_or_name: str) -> Volume
client.volumes.list(provider: str | None = None) -> list[Volume]
client.volumes.delete(id_or_name: str, force: bool = False) -> None
```

HTTP:

```
POST   /volumes                  {name, provider}
GET    /volumes/{id_or_name}
GET    /volumes
DELETE /volumes/{id_or_name}?force=false
```

`delete` errors if any session references the volume; `force=True` cascades to delete those sessions' subpaths and the sessions themselves.

### Sessions (modified)

SDK:

```python
sess = await client.sessions.create(agent_id="worker", volume_id=vol.id)
# No sandbox provisioned yet. Returns immediately.

async for ev in sess.run(prompt):
    ...

await sess.reset_sandbox()   # kill current AND immediately provision a new one (eager)
await sess.stop_sandbox()    # kill current; no new sandbox until next run() call (lazy)
await sess.delete()          # tears down sandbox + removes sessions/<sid>/ on volume
```

HTTP:

- `POST /sessions` body now requires `volume_id`.
- `sandbox_id` on session responses is renamed to `current_sandbox_id` and can be null between runs.
- New: `POST /sessions/{id}/start-sandbox` (pre-warm), `POST /sessions/{id}/reset-sandbox` (kill + provision new), `POST /sessions/{id}/stop-sandbox` (kill).

### Sandboxes (kept public, read + delete + pre-warm-via-session)

```
GET    /sandboxes                 # list — current active leases
GET    /sandboxes/{id}            # inspect — status, session_id, provider, created_at
DELETE /sandboxes/{id}            # kill — session survives with current_sandbox_id=NULL
```

No `POST /sandboxes`. Creation is always session-scoped (lazy on first run, or explicit via `POST /sessions/{id}/start-sandbox`). This preserves the invariant that every sandbox has exactly one owning session, which is what lets the volume mount path be unambiguous.

## Lifecycle

### Volume creation

```python
vol = await client.volumes.create(name="my-project", provider="daytona")
# Backend:
#   1. daytona.volume.create("my-project") -> provider_ref
#   2. INSERT INTO volumes (...)
#   3. return row
```

### Session creation (no compute yet)

```python
sess = await client.sessions.create(agent_id="worker", volume_id=vol.id)
# INSERT INTO sessions (id, agent_id, volume_id, current_sandbox_id=NULL, ...)
```

### First `run()` — lazy provisioning

1. Acquire `pg_try_advisory_lock(hash(session.id))`.
2. Probe `current_sandbox_id` (NULL -> skip).
3. Call `provider.create_sandbox(volumes=[...home subpath..., ...shared ro...])`.
4. Upsert `sandboxes` row. Set `sessions.current_sandbox_id`.
5. Ensure `~/workspace` exists inside the sandbox, `cd ~/workspace`.
6. Launch ACP agent (claude / codex / opencode) with `HOME=/home/daytona`. No `--resume` since first run.
7. Capture CLI-reported inner session id, store in `sessions.inner_session_id`.
8. Stream events.

### Subsequent `run()` (sandbox alive)

- Re-use `current_sandbox_id`. Send prompt via ACP. CLI auto-resumes from the transcript file on disk.

### Subsequent `run()` (sandbox dead, pre-run probe)

```
probe provider.get_sandbox(current_sandbox_id):
├─ running             → use as-is
├─ stopped / paused    → provider.start(id); reuse same sandbox row and id
├─ missing / deleted   → clear current_sandbox_id, go to "first run" path
└─ error               → destroy + go to "first run" path
```

The new sandbox gets the same `sessions/<session.id>/home` subpath, so the HOME content is identical to what the old sandbox had. CLI is re-launched with `--resume <inner_session_id>`.

### During-run sandbox loss

ACP client raises → abort the stream with a `{"type": "sandbox_lost", "sandbox_id": "..."}` event, clear `current_sandbox_id`. Caller decides whether to retry. No automatic mid-stream retry (would risk duplicating side effects).

### Transparent reattach event

On any pre-run reprovision, emit `{"type": "sandbox_reattach", "old_sandbox_id": "...", "new_sandbox_id": "..."}` on the stream before the first real event. Clients may surface "reconnecting…" UI but are not required to act on it.

### Volume deletion

`RESTRICT` by default. `force=True` path:

1. List sessions where `volume_id = X`.
2. For each: kill current sandbox, remove `sessions/<id>/home/` from the volume (optional; volume is about to be deleted), delete session row.
3. Delete provider volume.
4. Delete row.

### Session deletion

1. Kill current sandbox if any (via provider).
2. Remove `sessions/<id>/home/` subpath from the volume (best effort).
3. Delete session row.

## Concurrency

- **Per-session run lock:** Postgres advisory lock keyed on `hash(session.id)`. Prevents two concurrent `run()` calls on the same session from both provisioning a sandbox.
- **Cross-session on same volume:** allowed. Each session has a distinct `sessions/<id>/home` subpath, so writes don't collide. Reads from `/mnt/shared/` are concurrent-safe (read-only).
- **Shared-area writes:** out-of-band for now. If a session needs to write to `shared/`, we either add a dedicated write endpoint later or accept that users do it via direct volume access.

## Provider adapters

### Daytona (phase 1)

- `create_volume(name)` -> `daytona.volume.create(name).id`
- `delete_volume(ref)` -> `daytona.volume.delete(ref)`
- `create_sandbox(session_id, volume_ref)` -> `daytona.create(CreateSandboxFromSnapshotParams(..., volumes=[VolumeMount(volume_id=volume_ref, mount_path="/home/daytona", subpath=f"sessions/{session_id}/home"), VolumeMount(volume_id=volume_ref, mount_path="/mnt/shared", subpath="shared", read_only=True)]))`
- `start_sandbox(ref)`, `stop_sandbox(ref)`, `delete_sandbox(ref)`, `get_sandbox(ref)` as today.

### Docker (phase 2)

- `create_volume(name)` -> `docker volume create <name>`
- `delete_volume(ref)` -> `docker volume rm <ref>`
- `create_sandbox(...)` -> `docker run --mount type=volume,src=<ref>,dst=/home/daytona,volume-subpath=sessions/<sid>/home --mount type=volume,src=<ref>,dst=/mnt/shared,volume-subpath=shared,readonly ...`

Short `-v` form is not used — it doesn't support `volume-subpath`.

### Local (phase 3)

- `create_volume(name)` -> `mkdir -p ~/.agent-sdk/volumes/<name>`
- `delete_volume(ref)` -> `rm -rf ~/.agent-sdk/volumes/<ref>`
- `create_sandbox(...)` -> fork a process with `HOME=<vol>/sessions/<sid>/home` and `AGENT_SHARED_DIR=<vol>/shared` env vars. No mount namespace — soft isolation only. Acceptable for dev-only local path.

## Rollout

1. Schema + `Volume` DAO (migrations, `volumes.py` DB module). No behavior change.
2. Daytona adapter: `create_volume` / `delete_volume` / mounts baked into `create_sandbox`.
3. Volume CRUD endpoints + SDK methods. Standalone; sessions untouched.
4. Session changes on Daytona path: required `volume_id`; lazy sandbox; pre-run probe + reprovision; `--resume` wiring.
5. Docker adapter.
6. Local adapter.

Each step independently deployable. Steps 1-3 are additive no-ops for existing behavior. Step 4 is the flip point and needs the integration test below to pass before merging.

## Testing

- **Unit:** volume CRUD, FK constraints (`RESTRICT`, `SET NULL`), subpath computation.
- **Integration (Postgres + real Daytona):**
  - Volume lifecycle (create, list, get, delete, force-delete).
  - Session creation without sandbox, first run provisioning, second run reuse.
  - **Sandbox-loss resume test** (the key correctness test): create session, run prompt, kill sandbox out-of-band (`DELETE /sandboxes/{id}`), run another prompt, verify CLI resumed the same inner session and transcript continues.
  - Concurrent sessions on same volume, parallel runs, no subpath collision.
- **Docker and Local:** equivalent integration tests against those providers in phases 2 and 3.

## Open items

- Whether `/mnt/shared` write endpoint is needed in v1 (currently: no).
- Exact CLI resume mechanics for non-Claude ACP agents (Codex, OpenCode) — may need per-provider handling in step 4.
- Backfill behavior for multi-provider historical data — confirm during implementation that grouping by provider covers all existing rows.
