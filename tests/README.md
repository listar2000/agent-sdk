# agent-sdk tests

Most tests here are unit/integration tests that run against mocks or an
in-process app. A small set of E2E tests drive a real server on
`localhost:7778` with one or more providers (`local`, `docker`, `daytona`) and
are skipped automatically when the required dependencies aren't present.

## Golden recovery tests — `test_sandbox_stop_delete_recovery.py`

These tests describe the real-world crash/kill surface users hit in
production. They are load-bearing: they must pass for every supported
provider before the server is considered safe to ship. Don't weaken the
assertions to land a green build — if a test fails, fix the server, not the
test.

**Count.** 14 tests parameterized over `{local, docker, daytona}` +
2 daytona/local-only tests + 1 local-only stale-cache test. Most tests
simulate an out-of-band provider event by bypassing the server's HTTP
API (`daytona.delete`, `docker rm -f`, `kill -9` on the local supervisor
PID, or `pkill supervisor.js` inside a daytona sandbox). The next
client request must then succeed without human intervention.

**LLM-prose-independent assertions.** Several earlier revisions of these
tests used "agent recalls a ticket ID" as the invariant, which flaked
whenever Claude's safety guardrails refused to echo the token. The
current tests use deterministic server-side invariants (`inner_session_id`
preserved across recovery, non-empty reply, matching `sandbox_ref`) and
only fall back to behavioural checks where the thing under test genuinely
involves LLM state.

### 1. `test_stop_sandbox_same_sandbox_after_restart`

**Scenario:** provider stops the sandbox while a session is live
(supervisor crash, host reboot, manual `daytona.stop`).

**Invariant:** the next request restarts the *same* sandbox —
`sandbox_ref` before == `sandbox_ref` after. A changed ref means the
server incorrectly provisioned a replacement instead of waking the
original.

### 2. `test_server_delete_persists_workspace`

**Scenario:** user hits the server's `DELETE /sandboxes/{id}` endpoint.
Arbitrary HOME files the prior sandbox wrote must survive on the
replacement (because the server triggers a cold snapshot before
tear-down — the durability boundary).

**Invariant:** a marker file written in turn 1 is readable in turn 2
on the replacement sandbox; `sandbox_ref` after != `sandbox_ref`
before. Proves the server-driven snapshot fires before compute is
destroyed and the replacement is provisioned on the same volume.

### 3. `test_external_delete_preserves_agent_memory`

**Scenario:** out-of-band delete (`daytona.delete()`, `docker rm -f`,
etc.) bypasses the server, so no server-triggered cold snapshot runs.

**Invariant:** `inner_session_id` is unchanged across the delete —
the per-turn ``agent_memory.tar`` (supervisor-side snapshot of
`.claude/sessions`, `.codex/sessions`, etc.) preserves enough
state for `session/load` to resume. We deliberately do NOT assert
anything about arbitrary workspace files: whether they survive depends
on provider-specific graceful-shutdown handling (daytona's SIGTERM
handler flushes a full HOME tar on external delete; SIGKILL wouldn't)
and that's implementation detail — agent_memory is the contract.

### 4. `test_session_resume_after_stop`

**Scenario:** the sandbox is stopped mid-conversation; user reconnects
via a fresh httpx client and sends another message.

**Invariants:**
- turn 2 returns a non-empty reply
- `inner_session_id` is unchanged across the stop → resume (proves
  `session/load` ran, not `session/new`)

### 5. `test_session_resume_after_delete`

**Scenario:** combination of #3 and #4 — sandbox *deleted* between turns;
new sandbox is a different provider-level instance.

**Invariants:** same pair as #4, but across a delete — so session
continuity is restored via the volume-persisted JSONL rather than the
original sandbox's filesystem.

### 6. `test_session_survives_midstream_sandbox_stop`

**Scenario:** sandbox stopped externally; user does NOT send a new
message. Server's SSE reader must detect the upstream EOF, rebind to a
fresh supervisor, and preserve the prior conversation (no silent
`session/new`).

**Invariants:**
- turn 2 returns a non-empty reply
- `inner_session_id` on the in-memory `SessionState` is unchanged

Exercises `_recover_after_disconnect` (reader-driven recovery),
distinct from the `/message`-driven recovery in #4/#5.

### 7. `test_message_immediately_after_stop`

**Scenario:** stop the sandbox and POST `/message` with **no delay**.
The scheduler picks up the prompt before the SSE reader observes the
upstream disconnect — `_reader_connected` is still true and the
dispatch races a dead supervisor.

**Invariants:** turn 2 non-empty reply + `inner_session_id` stable.
The `ConnectError` retry in `_execute_one_prompt` clears
`_reader_connected` and forces rebind before giving up.

### 8. `test_message_after_stop_with_delay`

**Scenario:** stop the sandbox, **wait 4 seconds**, then POST
`/message`. Reader has observed the disconnect and `_INSTANCES` holds a
stale entry — the reusable-state check must tear down cleanly rather
than submitting to the dead URL.

**Invariants:** turn 2 non-empty reply + `inner_session_id` stable.
Distinct from #7 because this exercises the "confidently dead URL" path
rather than the race.

### 9. `test_persistent_sse_stop_then_message`

**Scenario:** the UI flow — one `/events` connection held open across
turn 1 / external stop / 4 s wait / turn 2, exactly as a browser does
with a long-lived SSE stream.

**Invariant:** turn 2's events must reach the held-open stream. This is
the path where the subscriber-kick-on-state-rebuild bug lived. The fix
is in-place rebind (`_rebind_state` mutates the existing `SessionState`
instead of replacing it).

### 10. `test_persistent_sse_external_delete_then_message`

**Scenario:** same persistent-SSE UI flow as #9, but the sandbox is
deleted **out-of-band** (daytona dashboard, `docker rm`, `kill -9`) —
the server only learns from the SSE reader observing upstream
disconnect. After the reader exhausts its retry ladder, the server
must provision a REPLACEMENT daytona sandbox and start a supervisor on
it (daytona is 2-phase: `create_sandbox` returns `url=""` and
`ensure_supervisor_url` must be called separately).

**Invariant:** turn 2 returns a non-empty reply on the SAME persistent
/events stream.

**Specific failure this catches:** if
`_type2_recover`'s daytona replacement branch forgets to
call `ensure_supervisor_url` on the freshly-created sandbox, the
rebuild path builds `AcpClient("")` and httpx raises
`UnsupportedProtocol: Request URL is missing an 'http://' or
'https://' protocol.` The reader then triggers
`_on_sse_reader_death` → full session teardown → another rebuild
→ port 9101 (9100 still held by the prior attempt) → cascading
rebuilds that lose turn 2's events.

### 11. `test_persistent_sse_delete_sandbox_then_message`

**Scenario:** same UI flow as #9/#10, but the user hits `DELETE
/sandboxes/{id}` on the server API directly — exactly what the UI does
when the user clicks a "delete sandbox" button.

**Invariant:** turn 2 returns a non-empty reply on the persistent
stream. This catches the zombie-state bug: without `force=True` on
`_shutdown_session_state`, an open /events subscriber made the shutdown
a no-op, leaving a SessionState pointing at the deleted sandbox and
the UI's queue orphaned. Fixed by (a) forcing the shutdown in
`delete_sandbox_route` and (b) kicking all subscribers in
`_shutdown_session_state` itself so the /events handler wakes and the
UI's `_PersistentSse` helper reconnects onto the replacement state.

### 12. `test_persistent_sse_supervisor_killed_then_message` *(daytona+local only)*

**Scenario:** supervisor process dies in-place (the prod "502 Bad
Gateway from daytona proxy" / supervisor OOM scenario) — the sandbox
itself stays alive. Test `pkill supervisor.js` inside the daytona
sandbox; for local, `kill -9` the supervisor PID. Test waits 6 s so
the server's SSE reader can observe the upstream disconnect and
rebind.

**Invariant:** turn 2 returns a non-empty reply on the SAME persistent
/events stream. Distinct from #10 because the sandbox row in the
server's DB is untouched — only the compute inside the sandbox died —
so recovery must go through `restart_daytona_supervisor` (or
respawn-in-place for local) rather than full replacement.

Docker excluded: `docker exec pkill supervisor.js` takes down container
PID 1 and the container exits — that's a different failure mode,
already covered by #10.

### 13. `test_persistent_sse_supervisor_killed_immediate_message` *(daytona+local only)*

**Scenario:** same as #12 but **no delay** before turn 2's POST —
the message fires in the ~100 ms window where the server still
thinks the cached supervisor URL is alive (`_reader_connected=True`
hasn't flipped yet). On daytona, the POST to `/v1/acp/...` returns
`502 Bad Gateway`.

**Invariant:** turn 2 non-empty reply. Exercises the
`_execute_one_prompt` retry path — on
`ConnectError`/`RemoteProtocolError`/`ReadError`, clear
`_reader_connected` and retry once through the rebind path.

### 14. `test_ui_reconnect_gap_loses_replies_and_blocks_followups` *(daytona+local only)*

**Scenario:** the *exact* prod UI trace — supervisor dies →
server's SSE reader kicks the UI's `/events` subscriber → browser
EventSource retry timer is still running when the user types the
next message → the POST lands with ZERO subscribers attached → the
scheduler dispatches reply events to empty subscriber lists →
events silently dropped → UI reconnects `/events` shortly after
but sees "Queued for agent" forever. Replayed here with a
persistent stream, `pkill supervisor.js`, `async with` exit to
stop auto-reconnect, two POSTs in the gap, then a fresh `/events`.

**Invariant:** both follow-up rpcs get a reply on the reconnected
stream. On unfixed baseline, `_collect_reply` times out on both.

**Fix this pins:** `SessionState.dispatch` appends items to a bounded
`_pending_broadcasts` deque when BOTH `_rpc_subscribers` and
`_session_subscribers` are empty; `subscribe_session` drains the
deque onto the new queue. See the **companion unit test** in
`test_async_correctness.py` that pins the mechanism directly.

### 15. `test_session_survives_supervisor_dir_wiped_from_volume` *(local-only)*

**Scenario:** `volumes.supervisor_agent_types = ['claude']` in Postgres
says the supervisor is installed, but the on-disk
`<vol>/system/supervisor/` directory has been wiped out-of-band
(container restart with an ephemeral volume, deploy reset, etc.).

**Invariant:** the next turn succeeds because the server's provision
path detects the stale-cache marker ("supervisor.js missing", "ACP
binary missing"), clears `supervisor_agent_types`, re-installs, and
retries. Without this, the 500 surfaces on the user's first POST
after a volume-wipe deploy.

## Companion unit tests — `test_async_correctness.py`

Fast, deterministic tests (no real sandbox, no sleeps) that pin
specific server mechanisms. Two of them back the golden suite:

- `test_dispatch_with_no_subscribers_buffers_for_next_subscribe` —
  UI reconnect-gap bug's *mechanism*. Dispatches 3 events to an empty
  `SessionState`, then calls `subscribe_session`, and asserts all 3
  arrive on the new queue. Fails on unfixed baseline with
  `got 0 items, want 3`. Complements integration test #14 above.
- `test_dispatch_with_active_subscriber_skips_buffer` — the buffer
  must only be used when there's no live subscriber, so a late
  second subscriber doesn't see events already delivered to the first.
- `test_buffer_bounded_under_flood` — the no-subscribers buffer is
  bounded (maxlen=2000) so a never-reconnecting client can't grow
  memory forever.

These run in <0.1 s and should stay green in the unit-only CI pass
before the golden suite is run against real providers.

## Why these are load-bearing

Production incidents that fall in this quadrant — sandbox went away, user
came back, got a 500 or a hung stream — are the hardest to debug after
the fact: provider state is gone, logs are in the deleted container,
user is frustrated. These tests pin down the server's contract for that
quadrant so regressions are caught in CI, not by customers.

If you touch any of:

- `_rebind_state` / `_ensure_state_live` / `ensure_runtime_locked` /
  `ensure_session_live`
- `_recover_after_disconnect` (SSE-reader recovery path)
- `_shutdown_session_state` (specifically the subscriber-kick logic —
  skipping it was the bug that broke #11)
- `delete_sandbox_route` (the `force=True` + `current_sandbox_id=NULL`
  invariant)
- `_type2_recover` — especially the daytona replacement
  branch that must call `ensure_supervisor_url` on the fresh sandbox
  (the bug that broke #10)
- `ensure_supervisor_url` or `restart_daytona_supervisor` in any provider
- Anything that sets/reads `HOME` in the spawn_env for a supervisor
- `_provision_new` (how a replacement sandbox gets its root + mounts)
- The volume-mount layout (`_build_volume_mounts`)
- The `_reader_connected` flag or subscriber-dispatch
  (`SessionState.broadcast` / `dispatch` / `subscribe_session`)
- `_pending_broadcasts` replay buffer (UI reconnect-gap fix, #14)
- `_execute_one_prompt`'s error handling (kill-then-send retry, #7, #13)

…run this file against `daytona` (live `DAYTONA_API_KEY` +
`CLAUDE_CODE_OAUTH_TOKEN`) before merging. The other providers are
cheaper to run and worth running too, but `daytona` has historically
been the one that surfaces mount/HOME bugs because its sandbox ↔ volume
separation is strictest.

## Golden — `test_attach_recovers_with_no_volume_journal.py`

Companion to `test_session_resume_after_delete` (#5). Where #5 covers
the *happy* `session/load` path — sandbox replaced after a successful
turn, the volume's `snapshot.tar` carries the JSONL forward, the new
sandbox's HOME gets the JSONL restored, `session/load` succeeds — this
test covers the inverse: **the JSONL is missing from the volume** when
the new sandbox boots, so `session/load` returns
`-32603 Internal error` and the agent must fall back to `session/new`
(via `acp_client.attach`'s catch-and-retry) instead of wedging.

**Scenario.** Cold-create the session (which mints `inner_session_id`
during `_attach_acp`); do **not** run a turn (so `snapshot.tar` is
never written to the volume — the supervisor only writes it after a
successful turn-end); external-delete the sandbox; then send a
prompt. The new sandbox boots with an empty HOME because the volume
had no snapshot to restore from.

**Invariants:**
- prompt returns a non-empty reply (agent recovered)
- `inner_session_id` on the in-memory `SessionState` is **different**
  from the pre-delete value (proving the recovery went through
  `session/new`, not `session/load`)

**Provider coverage.** Parameterized over `{daytona, docker, modal}`
— excluded from `local` because that provider's `_external_delete`
deliberately preserves HOME, so the JSONL never disappears and the
failure mode can't be exercised. The fix in `acp_client.attach` is
provider-agnostic.

**Production correspondence.** Reproduced 1:1 the wooden-marmot
incident on 2026-05-01: a stale `inner_session_id` from a previous
sandbox (which had been deleted as part of cleaning up
`hive-large`-snapshot zombies) was passed back to the freshly
provisioned sandbox; `session/load` failed; every subsequent revival
hit the same load failure; `pool.get_session`'s `start()` raised
*after* the new sandbox was provisioned but *before*
`db.write_sandbox_state` ran, so the DB stayed pointing at the old
ref — and the next request created **another** new sandbox, leaking
~30 orphans across 8 minutes. This test exercises the per-attach
fallback. The state-persistence half (don't leak the new sandbox if
attach fails) is a separate hardening — see Open Issues.

## Open issues / follow-ups

**Partial-failure leak in `pool.get_session`.** Lines 101-102:

```python
await session.start()                                          # raises
await db.write_sandbox_state(session_id, serialize(session.state))   # never runs
```

If `start()` raises after a fresh sandbox is provisioned (any reason —
ACP-attach failure, supervisor health timeout, …), the new
`sandbox_ref` is never persisted and the new sandbox is never
released. Every subsequent `get_session` reads the stale (or empty)
ref from the DB and provisions yet another sandbox. Independent of
the wooden-marmot fix; would have prevented the orphan-storm scaling
even with the old `attach` semantics. Fix shape: persist the partial
state immediately after `_resolve_or_create_sandbox` returns, *before*
`_attach_acp`, and on `start()` failure either tear down the
just-created sandbox or persist its ref so the next attempt reuses
it instead of creating yet another.

### Running

```sh
# Prerequisites: server on localhost:7778, relevant provider deps.
scripts/launch_server_local.sh &     # or launch_server_docker.sh
# Daytona: export DAYTONA_API_KEY=... CLAUDE_CODE_OAUTH_TOKEN=...
# Docker:  make sure `docker info` works.

.venv/bin/pytest tests/test_sandbox_stop_delete_recovery.py -v -s
.venv/bin/pytest tests/test_attach_recovers_with_no_volume_journal.py -v -s
# Or scoped to a single provider:
.venv/bin/pytest tests/test_sandbox_stop_delete_recovery.py -k daytona -v -s
# Or the fast mechanism-only pass (no real server needed):
.venv/bin/pytest tests/test_async_correctness.py -v
```

Timing on a warm server: ~3 min for the full suite on local; ~10–12
min on Daytona (provisioning dominates). Docker is skipped when no
daemon is reachable. Companion unit tests in `test_async_correctness.py`
run in <1 s.
