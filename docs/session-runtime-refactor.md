# Session runtime model

This document describes the current session runtime in the API server.

## Core model

There are two layers:

1. **The session row** (`sessions` table) — durable. Holds `agent_id`,
   `volume_id`, `cwd`, `env`, `secrets`, `inner_session_id`,
   `pre_start_commands`, and the `sandbox_state` JSONB blob. Survives
   server restart, sandbox death, and hibernation.

2. **The active SandboxSession** (in-process, owned by the SessionPool)
   — ephemeral. Holds the supervisor URL, the live ACP child, the
   subscriber fan-out queues, and the liveness oracle. Created lazily
   on first use and torn down on idle / explicit release.

`sandbox_state` JSONB is the contract between the two: a Pydantic
discriminated union (`daytona | docker | unix_local | modal | unknown`)
carrying `sandbox_ref`, `listen_port`, `snapshot_path`,
`snapshot_version`, and the `Recipe` (provisioning fingerprint:
`agent_type`, `dockerfile`, `shared_mounts`, `root`,
`pre_start_commands`).

The recipe lives on the session row, never on the compute itself, so a
deleted sandbox can't lose it. This is the architectural fix for the
older bug class where a Type 2 recovery would silently re-provision
without the original `pre_start_commands`.

## SessionPool

```python
pool = api.sandbox.get_pool()
session = await pool.get_session(session_id)
```

`SessionPool` is the entire recovery surface. The single `get_session`
method handles every scenario the legacy recovery chain
(`_ensure_sandbox_alive` / `_type1_recover` / `_type2_recover` /
`_rebind_state`) used to handle:

- session was hibernated → start fresh (resume from snapshot)
- cached session is dead → tear down + start fresh
- cached session is alive → return immediately (warm path, ~10ms)
- server just restarted → no cached → load state from DB → start

No "Type 1 vs Type 2" decision lives in the pool — that's internal to
`SandboxSession.start()`.

The pool holds at-most-one active `SandboxSession` per session_id.
Concurrent `get_session` calls on the same id serialise on
`_locks[session_id]`, so we never end up with two leases for the same
session.

Cold-create has a dedicated entry point:

```python
session = await pool.cold_create(session_id, provider="daytona", recipe=recipe)
```

`cold_create` constructs the per-provider initial `SandboxState` from
the recipe and delegates to `get_session(initial_state=...)` so the
same lock + cache logic runs. For recovery (server restart, hibernated
session resume) call `get_session(session_id)` directly — that reads
state from the DB.

`pool.release(session_id)` snapshots the volume and drops the compute
lease (idempotent). The reaper invokes the same path on idle sessions
(default 180 s; tune via `AGENT_SDK_REAPER_IDLE_S`).

## Per-prompt execution

`POST /sessions/{id}/message` and `POST /sessions/{id}/message+stream`
share a single execution path (`_execute_and_stream_sse`):

1. yield an immediate SSE heartbeat (so the client knows the request
   is alive while cold-recovery runs)
2. `pool.get_session(session_id)` — cold-recovers the SandboxSession
   if needed (30–60 s on Daytona under contended control plane)
3. log the `user_message` event
4. register a subscriber on the SandboxSession
5. spawn `_persist_prompt_events` as a drive task — it calls
   `session.execute_prompt(message, rpc_id)` and writes one
   `session_log` row per yielded event
6. iterate the subscriber, yielding SSE blocks scoped to this `rpc_id`
   to the response body. Done blocks (`stop_reason` / `"type":"done"`)
   end the stream.

`POST /sessions/{id}/message` runs the same generator inside a
background task with the response body discarded — fire-and-forget,
events go to `session_log` and are broadcast to any open `/events`
subscriber.

There is no per-session prompt queue, no scheduler task, no
`active_rpc_id` bookkeeping. Each prompt is its own SSE stream from
the SandboxSession's fan-out, and concurrency is bounded by the ACP
adapter's behaviour (see [`acp-boundary-problem.md`](acp-boundary-problem.md)).

## Subscribers and fan-out

`SandboxSession.subscribe()` and `register_subscriber()` /
`iterate_subscriber()` give a per-session fan-out queue. Multiple
concurrent subscribers receive every event broadcast by
`execute_prompt` — used by:

- `GET /sessions/{id}/events` — the long-lived multi-subscriber stream
- `POST /sessions/{id}/message+stream` — a per-prompt scoped subset
  (filtered to the rpc_id in the route handler)

A `_HEARTBEAT` sentinel surfaces during idle so SSE intermediaries
(nginx, cloudflare, browser EventSource) don't close the connection
between prompts. A short replay buffer flushes recent events when a
new subscriber attaches, so a UI that reconnects after a transient
disconnect doesn't miss events that fired during the gap.

## Cancellation

`POST /sessions/{id}/cancel` sends `session/cancel` (a JSON-RPC
notification) to the supervisor's ACP child via the live SandboxSession.
The ACP child aborts the turn; the `done` event with
`stopReason: "cancelled"` arrives on the same SSE subscribers. No
active lease → `{"status": "ok", "detail": "no active lease"}`.

`Agent.send(message, interrupt=True)` is a thin wrapper: cancel via
`/cancel`, then submit the new message. The client side does the
ordering; the server has no separate "interrupt" path.

## Why this model exists

The previous runtime threaded prompt state through `SessionState` (a
per-session in-memory record with `pending_prompts`, `_prompt_ready`,
`_prompt_done`, `active_rpc_id`, `_scheduler_task`) plus four parallel
recovery functions (`_ensure_sandbox_alive`, `_type1_recover`,
`_type2_recover`, `_rebind_state`) plus an in-memory `_INSTANCES`
registry plus a `_session_locks` dict plus an `is_hibernated` flag.

That sprawled the same invariant ("at most one active sandbox per
session") across many places, and "what happens when X breaks" was
hard to reason about because each path made its own decision. The
SessionPool collapses all of it into one method, with the
discriminated `sandbox_state` JSONB as the single durable contract.

The vestigial `SessionState` dataclass in `src/api/models.py` is no
longer wired into the runtime; it survives only for response-shape
back-compat fields the dashboard reads (`agent_busy`, `active_rpc_id`,
`pending_count`) — which are constants now.
