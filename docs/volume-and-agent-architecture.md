# Volume + agent architecture

This document captures the target architecture for how agents, sessions, and
sandboxes map onto the Daytona volume (and the local/docker providers that
mirror the same layout). It is a reviewable artifact — implementation happens
against a plan derived from it, not against this doc directly.

## Motivation

Today a session is the unit of persistence: each session provisions its own
sandbox, restores `/vol/snapshot.tar` on boot, and writes it back on every
turn-end. The volume subpath was recently flipped to `agents/<agent_id>/home`
(server.py:2343), which means two sessions for the same agent now *share the
same snapshot file on disk* while still running in independent sandboxes. That
is a latent last-writer-wins bug waiting for any concurrent use.

More importantly, the "session = workspace" model is wrong for the product
direction: an agent is supposed to be a persistent machine with an evolving
home directory (installed deps, scratch files, accumulated JSONL history). A
conversation should load that home, not provision a blank `/home/daytona` from
scratch.

## Current state

Verified against the tree at commit `45eb054`:

- **Volume subpath is agent-scoped already.** `_provision_new` at
  server.py:2332-2410 passes `subpath=f"agents/{agent_id}/home"` to
  `_build_volume_mounts`. Every sandbox for agent *A* mounts the same
  `/vol` at the same S3 subpath.
- **Supervisor snapshots on every turn-end.** supervisor.js:267-273 runs
  `runSnapshotOnce()` after every `session/prompt` response. This was
  originally built to survive the `daytona.delete()` race
  (supervisor.js:139-141). Concurrent turn-ends in the same supervisor
  race on `/tmp/agent-sdk-snapshot.tar` and the single-file `cp` to
  `/vol/snapshot.tar`.
- **Each session owns its sandbox.** `sessions.current_sandbox_id` is the
  binding (db.py:98-114, server.py:2398); there is no
  `agents.current_sandbox_id`. Opening a second session for the same agent
  provisions a second sandbox, which then shares the volume subpath with
  the first → concurrent snapshot writes to the same S3 key.
- **All sessions share one cwd.** server.py:2519 uses
  `agent.config.cwd or "/tmp"` for every ACP `session/new` for the agent.
  Session-level workspace isolation does not exist.
- **Recovery is per-session.** server.py:684-747 runs the recovery branch
  (re-provision sandbox, re-attach ACP session) in each SSE reader
  independently. N readers observing the same supervisor death all try to
  provision replacements simultaneously.
- **Shared tree exists with no declaration surface.** `_build_volume_mounts`
  (src/api/providers/_shared.py:347-381) unconditionally mounts
  `shared/` at `/mnt/shared` for every session sandbox; there is no
  per-agent opt-in.

## Target design

### Volume layout (unchanged on disk)

```
<volume-root>/
├── agents/<agent_id>/
│   └── home/
│       └── snapshot.tar          # workspace tarball, written on sandbox stop
├── shared/
│   └── …                         # cross-agent shared tree; organized freely
└── system/supervisor/
    ├── deps.tar.gz
    └── …                         # prebuilt supervisor bundle
```

Mounts inside a per-agent sandbox:

| Mount           | Volume subpath        | Purpose                                        |
| --------------- | --------------------- | ---------------------------------------------- |
| `/vol`          | `agents/<agent_id>/home/` | snapshot tarball (only file ever PUT)      |
| `/mnt/shared`   | `shared/`             | cross-agent shared tree                        |
| `/opt/supervisor` | `system/supervisor/` | prebuilt supervisor bundle                     |

Utility (admin/init) sandboxes still mount the whole volume at `/v`.

No change to `_build_volume_mounts`'s signature — the subpath `_provision_new`
feeds in is still `agents/<agent_id>/home`. No share registry, no per-agent
config surface for shares; `shared/` is a global read/write tree that users
organize however they want.

### One live sandbox per agent

- **Schema.** Add `agents.current_sandbox_id` (TEXT, nullable,
  `REFERENCES sandboxes(id) ON DELETE SET NULL`). Drop
  `sessions.current_sandbox_id` in a later migration once session lookup is
  routed through the agent — not in the same migration as the add, so we can
  back out cleanly.
- **Provisioning.** `ensure_sandbox_for_agent(agent_id)` becomes the single
  entry point. If `agent.current_sandbox_id` points at a live sandbox, return
  it. Otherwise provision a fresh one against the agent subpath and UPDATE
  `agents.current_sandbox_id`.
- **Multiplexing.** Multiple sessions' ACP sessions live inside the same
  supervisor. `_attach_acp_session` already handles `session/new` vs
  `session/load` by inner_sid; no change needed there.
- **TTL / reap.** Sandbox activity is aggregated across all attached
  sessions. The sandbox is only considered idle when every session under the
  agent has gone idle past TTL.

### Per-session cwd

Workspace layout inside the sandbox:

```
/home/daytona/                           ← agent root, snapshotted as a unit
├── sessions/
│   ├── <session_id_1>/                  ← session 1's cwd
│   └── <session_id_2>/                  ← session 2's cwd
├── .claude/projects/<hash>/…            ← per-session JSONL history
└── …other agent-level files…
```

- `_attach_acp_session` receives `cwd=/home/daytona/sessions/<session_id>`.
- Supervisor `mkdir -p`'s the session dir before the first `session/new`.
- Session delete removes the session dir; the next stop-triggered snapshot
  picks up the deletion.
- Agent-level state (installed deps, `~/.claude/projects/`) stays rooted at
  `/home/daytona` and is shared by all sessions.
- `session/load` must use the same cwd that `session/new` used. This is
  stable per-session because it's derived from the session_id, which is
  immutable for the life of the session.

Isolation property: a `rm -rf .` inside session A's cwd can't nuke session
B's files or agent-level state.

### Snapshot on sandbox stop, not on turn-end

One trigger: the server's stop-sandbox code path. Sequence is strictly
serial — snapshot first, await completion, then call the provider's
`stop()` / `delete()`. Per-turn snapshotting goes away entirely.

In the one-sandbox-per-agent model this is the natural layer — the sandbox
is the agent's live workspace for all sessions simultaneously, so capturing
it on stop captures every session's contribution in a single atomic write.
No dirty-tracking, no draining, no concurrent-write races, no gating logic.

**The one helper:**

```python
async def snapshot_and_stop(sandbox: SandboxRecord) -> None:
    # 1. Tell the supervisor to snapshot; await the ack.
    await supervisor_snapshot(sandbox)
    # 2. Only then tear the sandbox down at the provider.
    await _providers_mod.stop_sandbox(...)
```

Every server-side stop path routes through this: reap (TTL), manual stop
via the API, sandbox-replacement when an agent needs a fresh one.
Agent-delete stops the sandbox first via the same helper; the snapshot
written by that call is then erased by the subtree rm, but the snapshot
cost is negligible and not worth a special-case skip.

**Supervisor surface.** A new `POST /v1/snapshot` endpoint wraps the
existing `runSnapshotOnce` — returns 200 after the tarball has landed on
the volume. Idempotent. Implementation is a 5-line handler; the tar/cp
machinery already exists. (Alternative: the server could exec the tar
command directly via the provider's process-exec API instead of HTTP —
both work; HTTP is slightly nicer because the supervisor already owns the
exclude list and staging path.)

**What disappears from the supervisor:**

- `pendingPromptIds`-tracked snapshot in `handleAcpLine`
  (supervisor.js:260-274) — removed.
- `runSnapshotSync` on SIGTERM/SIGINT — kept as a cheap safety net for
  external signals that skip the server (rare, but costs ~10 lines to
  retain).

**What the new durability contract says:**

> Workspace state is durable as of the most recent server-initiated
> sandbox stop. Ungraceful terminations (OOM, node failure, supervisor
> crash) lose every turn since the sandbox started.

This is weaker than the per-turn model's "client-sees-turn-done ⇒ durable"
invariant. Accepted because:

- Every expected lifecycle event routes through the server, which owns the
  snapshot-then-stop ordering. The `daytona.delete()` race that motivated
  per-turn snapshots is closed by that ordering.
- True crashes (kernel OOM, provider-side node eviction) are rare, and
  the client can't observe "turn done" for work lost in those cases
  anyway — so from the client's perspective the loss is indistinguishable
  from a connection error mid-turn, which clients must already tolerate.

**Tuning.** Effective snapshot cadence is now the idle TTL on the reap
loop — shorter TTL ⇒ more frequent snapshots ⇒ smaller crash-window.
The usual cost concern (cold start on resume) doesn't apply here: reap
stops the sandbox but doesn't delete it, so the next message hits the
provider's `start` fast-path on the preserved rootfs — a couple seconds,
no snapshot restore. The snapshot tarball is only consumed when the
provider evicts the stopped sandbox out of band, i.e. the safety-net
case. This makes 3 min a reasonable default; per-agent override via
`AgentConfig.idle_ttl_seconds` is still useful for agents that want to
stay warm longer (e.g. very interactive workflows).

### Agent-scoped recovery

Symptom today: one supervisor death ⇒ N independent recovery attempts (one
per session on that sandbox). Fix:

- Per-agent lock: `agent_recovery_locks[agent_id] = asyncio.Lock()`
  (in-memory dict, server-local).
- SSE reader observing an upstream disconnect takes the lock, re-checks
  `agent.current_sandbox_id` inside the lock, and either:
  - provisions + starts the replacement sandbox (first reader), or
  - no-ops because another reader already did it.
- After the lock releases, each reader independently re-attaches its own
  ACP session (`_attach_acp_session(inner_sid=state.inner_session_id,
  cwd=state.session_cwd)`) against the already-healed sandbox.

Result: each subscriber still sees its own reconnect, but the
provider-side thrashing collapses to one sandbox-create per agent.

This also closes the reader-subscribe race that motivated the original
question in the brainstorm — on recovery, the reader's HTTP GET happens
before the scheduler fires any new prompt, because the scheduler is gated on
`_prompt_ready` and no new prompt can be sent until the lock releases. The
first-attach race (fresh session, no recovery) is a separate fix that
`Last-Event-ID` will solve later (see "Deferred" below).

### Agent delete

`DELETE /agents/<agent_id>`:

1. Tear down `agent.current_sandbox_id` via `snapshot_and_stop` (same helper
   as reap; the snapshot it writes is about to be erased by step 2, but
   special-casing isn't worth it).
2. Spin a utility sandbox with the whole-volume mount and
   `rm -rf /v/agents/<agent_id>/`.
3. Delete the agents row (CASCADE wipes sessions).

Delete is idempotent: if the subtree is already gone, step 2 is a no-op; if
the sandbox is already dead, step 1 skips the snapshot and goes straight
to provider `delete()` (best-effort).

## Migration

- **Schema.** Two-step, to keep rollback clean:
  1. Add `agents.current_sandbox_id`, keep `sessions.current_sandbox_id`.
     Backfill: for each agent, pick the most-recent active session and copy
     its sandbox_id up. Wire `ensure_sandbox_for_agent` to prefer the agent
     column.
  2. After a release cycle, drop `sessions.current_sandbox_id`.
- **Volume.** No data move required — the subpath is already
  `agents/<agent_id>/home`. The only cleanup is old per-session snapshot
  files under deprecated subpaths (if any exist in legacy tenants), which
  a one-shot utility sandbox migration handles.
- **Code.** Touched files: server.py (agent-scoped provisioning +
  `snapshot_and_stop` helper + agent-scoped recovery + per-session cwd),
  models.py (`AgentRecord.current_sandbox_id`, `SessionState.session_cwd`),
  db.py (schema + backfill), supervisor.js (delete per-turn snapshot, add
  `/v1/snapshot` endpoint, mkdir per-session cwd), tests/* for all three.

## Deferred

- **First-attach SSE race.** Brief window between POST `/message` and the
  reader's upstream GET subscribing where chunks can be dropped. Proper fix
  is supervisor-side event IDs + ring buffer + `Last-Event-ID` replay
  (client already sends the header at server.py:622). Not part of this
  refactor — symptom only bites the fresh-start path, not the recovery
  path that matters here.
- **Read-only shared mount.** Daytona SDK 0.168 lacks `read_only` on
  `VolumeMount`. When the SDK adds it, `/mnt/shared` becomes read-only by
  default and writes route through an explicit admin/write endpoint.
- **Shared subfolder ACLs.** The `shared/` tree has no access control
  today. If that becomes needed, it's an app-layer concern
  (path sanitizer + per-agent allowlist), not a mount-layer one.

## Invariants the design preserves

- "Server-initiated sandbox stop is durable before the provider call
  lands" (snapshot-then-stop ordering).
- "A session's cwd is isolated from sibling sessions' cwds" (new).
- "One supervisor death ⇒ one replacement provision" (new).

## Invariant the design gives up

- "Client sees turn-done ⇒ turn is durable on S3." Now: turn is in the
  sandbox's live `/home/daytona` but not on the volume until the next
  server-initiated stop. Ungraceful termination between two stops loses
  everything in that window. Acceptable for agent workloads; the
  simplification and the I/O savings are worth the residual risk.
