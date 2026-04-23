# Session / Volume / Sandbox Decoupling

Date: 2026-04-21
Status: Draft (awaiting user review)

## Problem

Today, `agent-sdk` binds a session to a sandbox: `sessions.sandbox_id → sandboxes(id) ON DELETE CASCADE`. When the sandbox dies (idle timeout, crash, node loss), the session dies with it, taking its Claude Code CLI state (OAuth, project history, in-progress transcripts) with it.

We want to:

1. Treat sandboxes as ephemeral compute. A sandbox can die or be swapped without losing the session.
2. Introduce a first-class `Volume` as durable storage. The agent's home directory and all CLI state live on the volume.
3. Scope volumes to a user-defined "sub-project". Multiple agents working on the same sub-project share a volume; each agent gets its own HOME subpath. Multiple sessions of the same agent share that agent's HOME and use Claude Code's own per-session transcript naming for isolation.

## Goals

- A session survives sandbox replacement. Its conversation continues where it left off.
- Users explicitly create/list/delete volumes; sessions opt into one by `volume_id` at creation.
- Concurrent sessions *of different agents* on the same volume run in parallel, each in its own agent HOME subpath.
- Concurrent sessions *of the same agent* on the same volume share one HOME — acceptable because Claude Code's per-session transcript naming gives per-conversation isolation, and cross-cutting files (`.claude.json`, statsig cache) rarely collide in practice.
- A shared read-only area exists on each volume for cross-agent data sharing.
- Sandboxes are public resources, always created with `(volume_id, subpath)`. No volumeless sandboxes, no full-volume mounts.
- Supported on Daytona first, then Docker, then local.

## Non-goals

- Hot-attaching a volume to a running sandbox. Daytona does not support it; we reprovision instead.
- Cross-volume data movement primitives. Users write to `shared/` out-of-band or via a future API.
- Per-session volumes as the default. Users can still opt in by creating a 1-session volume, but the norm is shared.

## Model

### Concepts

| | |
|---|---|
| **Volume** | User-scoped, persistent storage. Backed by a provider-native volume (Daytona volume, Docker named volume, host dir). Owns a namespace of per-agent HOME subpaths + one shared area. |
| **Session** | Durable logical conversation. Anchored to one volume (via its agent's volume_id) and one agent. Stable id; its transcripts live inside its agent's HOME. |
| **Sandbox** | Ephemeral compute. Created with a `(volume_id, subpath)` mount at creation time. Usually provisioned lazily when a session runs, with `subpath=agents/<agent_id>/home`. Can also be created standalone via the public API. Can be killed and replaced without touching session or volume state. |

### Relationships

- `Session → Volume`: required, immutable, `ON DELETE RESTRICT` (cannot delete a volume with live sessions except via `force=True`).
- `Session → Agent`: required, immutable (existing).
- `Session → current Sandbox`: nullable, swappable, `ON DELETE SET NULL` (sandbox row can disappear without killing the session).
- `Sandbox → Volume`: required at sandbox creation, stored on the sandbox row (via `volume_id` + `subpath` columns). A sandbox mounts exactly one "home" subpath of one volume (plus the implicit `shared` read-only mount on the same volume).
- `Sandbox → Session`: no hard FK. In practice a sandbox is provisioned for a session's agent; if `subpath = agents/<agent_id>/home`, the sandbox is bound to any session of that agent on that volume.

### Filesystem layout on a volume

```
<volume>/
├── agents/
│   ├── <agent-id-1>/
│   │   └── home/                        ← mounted writable at /home/daytona for all sessions of agent-1
│   │       ├── .claude.json             ← OAuth — shared across this agent's sessions
│   │       ├── .claude/
│   │       │   ├── projects/<cwd>/
│   │       │   │   ├── <inner-sess-A>.jsonl   ← per-session transcript (Claude CLI
│   │       │   │   ├── <inner-sess-B>.jsonl      names these by its own id)
│   │       │   │   └── ...
│   │       │   └── statsig/
│   │       └── workspace/               ← agent's cwd
│   └── <agent-id-2>/
│       └── home/
│           └── ...
└── shared/                              ← mounted read-only at /mnt/shared in every sandbox on this volume
```

### Per-sandbox mounts

Every sandbox on a volume mounts the volume twice:

```python
volumes=[
    VolumeMount(volume_id=v.provider_ref,
                mount_path="/home/daytona",
                subpath=f"agents/{session.agent_id}/home"),    # writable, per-agent
    VolumeMount(volume_id=v.provider_ref,
                mount_path="/mnt/shared",
                subpath="shared",
                read_only=True),                               # read-only, cross-agent
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

Sandboxes changes (volume-aware):

```sql
ALTER TABLE sandboxes ADD COLUMN IF NOT EXISTS volume_id TEXT REFERENCES volumes(id) ON DELETE RESTRICT;
ALTER TABLE sandboxes ADD COLUMN IF NOT EXISTS subpath  TEXT;
-- After backfill (see below), enforce required:
ALTER TABLE sandboxes ALTER COLUMN volume_id SET NOT NULL;
ALTER TABLE sandboxes ALTER COLUMN subpath   SET NOT NULL;
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

SDK (shape parallels `client.sandboxes.*`):

```python
client.volumes.create(name: str, provider: str) -> Volume
client.volumes.provision(name: str, provider: str) -> Volume        # blocks until status == "ready"
client.volumes.get(id_or_name: str) -> Volume
client.volumes.list(provider: str | None = None) -> list[Volume]
client.volumes.delete(id_or_name: str, force: bool = False) -> None

# File ops — mirror client.sandboxes.files.*
client.volumes.files.tree(id_or_name: str, path: str = "/") -> Tree
client.volumes.files.read(id_or_name: str, path: str) -> bytes
client.volumes.files.edit(id_or_name: str, path: str, content: bytes) -> None
```

HTTP (deliberately parallel to `/sandboxes`):

```
POST   /volumes                       # create — body: {name, provider}
POST   /volumes/provision             # create + wait for provider "ready" — mirrors /sandboxes/provision
GET    /volumes                       # list — all volumes (filter: ?provider=daytona)
GET    /volumes/{id_or_name}          # inspect — id, name, provider, provider_ref, status, created_at
DELETE /volumes/{id_or_name}?force=false    # delete — errors if sessions reference it, force=true cascades
GET    /volumes/{id_or_name}/files/tree?path=shared/
GET    /volumes/{id_or_name}/files/read?path=shared/datasets/foo.csv
POST   /volumes/{id_or_name}/files/edit     # body: {path, content} — write to any subpath (useful for seeding shared/)
```

Volume file-ops mirror the sandbox file-ops (`/sandboxes/{id}/files/{tree,read,edit}`). They let callers browse and populate a volume directly — critical for seeding the `shared/` area without spinning up a sandbox, and useful for debugging an agent's HOME contents.

Implementation note: the file-ops endpoints are backed by a tiny internal sandbox the server spins up on demand (or a pooled "volume-ops" sandbox), since the only way to access a Daytona volume's content is from inside a sandbox that has it mounted. This is an implementation detail; to callers, it's just a REST endpoint on the volume.

`DELETE` returns 409 if any session references the volume; `?force=true` cascades to those sessions (see "Volume deletion" below).

### Sessions (modified)

SDK:

```python
sess = await client.sessions.create(agent_id="worker", volume_id=vol.id)
# No sandbox provisioned yet. Returns immediately.

async for ev in sess.run(prompt):
    ...

await sess.reset_sandbox()   # kill current AND immediately provision a new one (eager)
await sess.stop_sandbox()    # kill current; no new sandbox until next run() call (lazy)
await sess.delete()          # tears down sandbox + deletes session row; HOME on volume is shared per-agent, so not removed
```

HTTP:

- `POST /sessions` body now requires `volume_id`.
- `sandbox_id` on session responses is renamed to `current_sandbox_id` and can be null between runs.
- New: `POST /sessions/{id}/start-sandbox` (pre-warm), `POST /sessions/{id}/reset-sandbox` (kill + provision new), `POST /sessions/{id}/stop-sandbox` (kill).

### Sandboxes (public, volume-aware)

```
POST   /sandboxes                 # create — body requires {provider, volume_id, subpath, ...compute params}
GET    /sandboxes                 # list
GET    /sandboxes/{id}            # inspect — status, provider, volume_id, subpath, created_at
DELETE /sandboxes/{id}            # kill — any session pointing at it has current_sandbox_id set to NULL
POST   /sandboxes/{id}/stop       # existing — stop the underlying provider compute
POST   /sandboxes/{id}/start      # existing — start a stopped sandbox
```

Existing file-ops endpoints (`/sandboxes/{id}/files/tree|read|edit`) keep working unchanged; they operate on whatever the sandbox sees, which under the new rules is always `/home/daytona` = `<vol>/agents/<agent_id>/home` (or whatever subpath the caller specified).

**Rule of thumb:** every sandbox is always `(volume_id, subpath)`-scoped at creation. No volumeless sandboxes, no full-volume mounts. Every sandbox also gets the implicit `/mnt/shared` read-only mount at subpath `"shared"` on the same volume.

Existing `POST /sandboxes/provision` (create + wait) stays, with the same new required fields.

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
3. Call `provider.create_sandbox(volume_id=session.volume_id, subpath=f"agents/{session.agent_id}/home", ...)`. The provider adds the implicit shared-ro mount internally.
4. Upsert `sandboxes` row with `volume_id` + `subpath` columns. Set `sessions.current_sandbox_id`.
5. Ensure `~/workspace` exists inside the sandbox, `cd ~/workspace`.
6. Launch ACP agent (claude / codex / opencode) with `HOME=/home/daytona`. No `--resume` since first run (or `--resume` from `sessions.inner_session_id` if this is a resumed session whose transcript is already on the volume).
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

The new sandbox gets the same `agents/<session.agent_id>/home` subpath, so the HOME content (including the session's transcript file at `~/.claude/projects/<cwd>/<inner_session_id>.jsonl`) is identical to what the old sandbox had. CLI is re-launched with `--resume <inner_session_id>`.

### During-run sandbox loss

ACP client raises → abort the stream with a `{"type": "sandbox_lost", "sandbox_id": "..."}` event, clear `current_sandbox_id`. Caller decides whether to retry. No automatic mid-stream retry (would risk duplicating side effects).

### Transparent reattach event

On any pre-run reprovision, emit `{"type": "sandbox_reattach", "old_sandbox_id": "...", "new_sandbox_id": "..."}` on the stream before the first real event. Clients may surface "reconnecting…" UI but are not required to act on it.

### Volume deletion

`RESTRICT` by default. `force=True` path:

1. List sandboxes where `volume_id = X`, delete each (kills compute).
2. List sessions where `volume_id = X`, delete each row.
3. Delete provider volume.
4. Delete `volumes` row.

### Session deletion

HOME is per-agent, not per-session, so session deletion does NOT remove the HOME subpath from the volume. Other sessions of the same agent may still use it.

1. If `current_sandbox_id` is set: kill the sandbox (the sandbox is per-agent-HOME, not per-session — so we only kill it if no other sessions of the same agent are likely to reuse it; simplest behavior: always kill on session delete, next session reprovisions).
2. Best-effort: delete the session's transcript file at `~/.claude/projects/<cwd>/<inner_session_id>.jsonl` by execing into a sandbox, OR defer cleanup (transcripts are small; leaving them is harmless).
3. Delete session row.

### Agent deletion

Extended semantic: deleting an agent removes its HOME on every volume.

1. Block if any session of this agent exists (standard FK), OR cascade-delete those sessions.
2. For each volume referenced by this agent's sessions: exec a cleanup step that removes `agents/<agent_id>/` from the volume.
3. Delete agent row.

(This is a new responsibility for the existing agent-delete path; previously there was no cross-volume state to clean up.)

## Concurrency

- **Per-session run lock:** Postgres advisory lock keyed on `hash(session.id)`. Prevents two concurrent `run()` calls on the same session from both provisioning a sandbox.
- **Cross-agent on same volume:** fully isolated — each agent has a distinct `agents/<agent_id>/home` subpath.
- **Same agent, different sessions, same volume:** share HOME. Accepted — Claude Code's per-session transcript filenames handle per-conversation isolation, and shared-file collisions (`.claude.json`, statsig cache) are rare and typically benign. No extra locking.
- **Shared-area writes:** out-of-band for now. If an agent needs to write to `shared/`, add a dedicated write endpoint later or do it via direct volume access.

## Provider adapters

### Daytona (phase 1)

- `create_volume(name)` -> `daytona.volume.create(name).id`
- `delete_volume(ref)` -> `daytona.volume.delete(ref)`
- `create_sandbox(volume_ref, subpath)` -> `daytona.create(CreateSandboxFromSnapshotParams(..., volumes=[VolumeMount(volume_id=volume_ref, mount_path="/home/daytona", subpath=subpath), VolumeMount(volume_id=volume_ref, mount_path="/mnt/shared", subpath="shared", read_only=True)]))`
- `start_sandbox(ref)`, `stop_sandbox(ref)`, `delete_sandbox(ref)`, `get_sandbox(ref)` as today.

### Docker (phase 2)

- `create_volume(name)` -> `docker volume create <name>`
- `delete_volume(ref)` -> `docker volume rm <ref>`
- `create_sandbox(volume_ref, subpath)` -> `docker run --mount type=volume,src=<ref>,dst=/home/daytona,volume-subpath=<subpath> --mount type=volume,src=<ref>,dst=/mnt/shared,volume-subpath=shared,readonly ...`

Short `-v` form is not used — it doesn't support `volume-subpath`.

### Local (phase 3)

- `create_volume(name)` -> `mkdir -p ~/.agent-sdk/volumes/<name>`
- `delete_volume(ref)` -> `rm -rf ~/.agent-sdk/volumes/<ref>`
- `create_sandbox(volume_ref, subpath)` -> fork a process with `HOME=<vol>/<subpath>` and `AGENT_SHARED_DIR=<vol>/shared` env vars. For session-driven creation, `subpath = agents/<agent_id>/home`. No mount namespace — soft isolation only. Acceptable for dev-only local path.

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
