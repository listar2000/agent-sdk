# agent-sdk tests

Most tests here are unit/integration tests that run against mocks or an
in-process app. A small set of E2E tests drive a real server on
`localhost:7778` with one or more providers (`local`, `docker`, `daytona`) and
are skipped automatically when the required dependencies aren't present.

## Golden recovery tests — `test_sandbox_stop_delete_recovery.py`

These **ten** tests describe the real-world crash/kill surface users hit in
production. They are load-bearing: they must pass for every supported
provider before the server is considered safe to ship. Don't weaken the
assertions to land a green build — if a test fails, fix the server, not the
test.

Each test is parameterized over `{local, docker, daytona}` and simulates an
out-of-band provider event by bypassing the server's HTTP API (`daytona.stop`,
`docker rm -f`, `kill -9` on the local supervisor PID). The next client
request must then succeed without human intervention.

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

### 2. `test_delete_sandbox_volume_persistence`

**Scenario:** provider deletes the sandbox (quota cleanup, manual
`daytona.delete`). The attached **volume** is untouched.

**Invariant:** the next request provisions a *new* sandbox on the *same*
volume, and files the previous sandbox wrote to `~/` survive. The test
writes a marker file (`~/recovery-test-marker.txt`) via the agent, deletes
the sandbox, then asks the agent to `cat` it. A new `sandbox_ref` +
surviving marker together prove delete-then-reattach works.

This is the canary for the volume-refactor: if HOME isn't on the volume,
the marker is gone and the test fails loudly.

### 3. `test_session_resume_after_stop`

**Scenario:** the sandbox is stopped mid-conversation; user reconnects
via a fresh httpx client and sends another message.

**Invariants:**
- turn 2 returns a non-empty reply
- `inner_session_id` is unchanged across the stop → resume (proves
  `session/load` ran, not `session/new`)

### 4. `test_session_resume_after_delete`

**Scenario:** combination of #2 and #3 — sandbox *deleted* between turns;
new sandbox is a different provider-level instance.

**Invariants:** same pair as #3, but across a delete — so session
continuity is restored via the volume-persisted JSONL rather than the
original sandbox's filesystem.

### 5. `test_session_survives_midstream_sandbox_stop`

**Scenario:** sandbox stopped externally; user does NOT send a new
message. Server's SSE reader must detect the upstream EOF, rebind to a
fresh supervisor, and preserve the prior conversation (no silent
`session/new`).

**Invariants:**
- turn 2 returns a non-empty reply
- `inner_session_id` on the in-memory `SessionState` is unchanged

Exercises `_recover_after_disconnect` (reader-driven recovery),
distinct from the `/message`-driven recovery in #3/#4.

### 6. `test_message_immediately_after_stop`

**Scenario:** stop the sandbox and POST `/message` with **no delay**.
The scheduler picks up the prompt before the SSE reader observes the
upstream disconnect — `_reader_connected` is still true and the
dispatch races a dead supervisor.

**Invariants:** turn 2 non-empty reply + `inner_session_id` stable.
The `ConnectError` retry in `_execute_one_prompt` clears
`_reader_connected` and forces rebind before giving up.

### 7. `test_message_after_stop_with_delay`

**Scenario:** stop the sandbox, **wait 4 seconds**, then POST
`/message`. Reader has observed the disconnect and `_INSTANCES` holds a
stale entry — the reusable-state check must tear down cleanly rather
than submitting to the dead URL.

**Invariants:** turn 2 non-empty reply + `inner_session_id` stable.
Distinct from #6 because this exercises the "confidently dead URL" path
rather than the race.

### 8. `test_persistent_sse_stop_then_message`

**Scenario:** the UI flow — one `/events` connection held open across
turn 1 / external stop / 4 s wait / turn 2, exactly as a browser does
with a long-lived SSE stream.

**Invariant:** turn 2's events must reach the held-open stream. This is
the path where the subscriber-kick-on-state-rebuild bug lived. The fix
is in-place rebind (`_rebind_state` mutates the existing `SessionState`
instead of replacing it).

### 9. `test_persistent_sse_external_delete_then_message`

**Scenario:** same persistent-SSE UI flow as #8, but the sandbox is
deleted **out-of-band** (daytona dashboard, `docker rm`, `kill -9`) —
the server only learns from the SSE reader observing upstream
disconnect.

**Invariant:** turn 2 returns a non-empty reply on the SAME persistent
/events stream. Covers the path where the reader-initiated recovery
must either preserve subscribers (rebind) or hand off cleanly to a
replacement session state without dropping events.

### 10. `test_persistent_sse_delete_sandbox_then_message`

**Scenario:** same UI flow as #8/#9, but the user hits `DELETE
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

## Why these are load-bearing

Production incidents that fall in this quadrant — sandbox went away, user
came back, got a 500 or a hung stream — are the hardest to debug after
the fact: provider state is gone, logs are in the deleted container,
user is frustrated. These tests pin down the server's contract for that
quadrant so regressions are caught in CI, not by customers.

If you touch any of:

- `_rebind_state` / `_ensure_state_live` / `_ensure_runtime_locked` /
  `ensure_session_live`
- `_recover_after_disconnect` (SSE-reader recovery path)
- `_shutdown_session_state` (specifically the subscriber-kick logic —
  skipping it was the bug that broke #10)
- `delete_sandbox_route` (the `force=True` + `current_sandbox_id=NULL`
  invariant)
- `ensure_supervisor_url` or `restart_daytona_supervisor` in any provider
- Anything that sets/reads `HOME` in the spawn_env for a supervisor
- `_provision_new` (how a replacement sandbox gets its root + mounts)
- The volume-mount layout (`_build_volume_mounts`)
- The `_reader_connected` flag or subscriber-dispatch
  (`SessionState.broadcast` / `dispatch`)
- `_execute_one_prompt`'s error handling

…run this file against `daytona` (live `DAYTONA_API_KEY` +
`CLAUDE_CODE_OAUTH_TOKEN`) before merging. The other providers are
cheaper to run and worth running too, but `daytona` has historically
been the one that surfaces mount/HOME bugs because its sandbox ↔ volume
separation is strictest.

### Running

```sh
# Prerequisites: server on localhost:7778, relevant provider deps.
scripts/launch_server_local.sh &     # or launch_server_docker.sh
# Daytona: export DAYTONA_API_KEY=... CLAUDE_CODE_OAUTH_TOKEN=...
# Docker:  make sure `docker info` works.

.venv/bin/pytest tests/test_sandbox_stop_delete_recovery.py -v -s
# Or scoped to a single provider:
.venv/bin/pytest tests/test_sandbox_stop_delete_recovery.py -k daytona -v -s
```

Timing on a warm server: ~2:30 for 10 tests on local; ~9–10 min on
Daytona (provisioning dominates). Docker is skipped when no daemon is
reachable.
