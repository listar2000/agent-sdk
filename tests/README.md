# agent-sdk tests

Unit and integration tests run against mocks or an in-process app. E2E
tests drive a real server on `localhost:7778` against `unix_local`,
`docker`, `daytona`, or `modal` and skip when their deps aren't present.

Run pytest with `-n auto` (mandatory — sequential is 8–15+ min on the
golden suites). `-n auto` is fine with `-k` filters; xdist negotiates
worker count down.

## Golden recovery — `test_sandbox_stop_delete_recovery.py`

15 tests, mostly parametrized over `{daytona, docker, unix_local, modal}`.
Each simulates an out-of-band provider event (`daytona.delete`,
`docker rm -f`, `kill -9`, `pkill supervisor.js`) bypassing the server's
HTTP API; the next client request must succeed without intervention.

Invariants are deterministic server-side (`inner_session_id`,
`sandbox_ref`, non-empty reply) — not "agent recalls X", which flaked on
LLM safety guardrails.

### 1. `test_stop_sandbox_same_sandbox_after_restart`
Provider stops the sandbox while a session is live. Next request must
restart the *same* sandbox: `sandbox_ref` before == after.

### 2. `test_server_delete_persists_workspace`
`POST /sessions/{id}/release` (hibernation) must snapshot to volume
before dropping compute. Marker file written turn 1 readable turn 2.
`sandbox_ref` may change (docker/local/modal provision fresh) or not
(daytona pauses in place).

### 3. `test_delete_session_destroys_sandbox`
`DELETE /sessions/{id}` must destroy compute, not pause it — the session
row is gone, nothing can resume. Within ~20 s post-DELETE the provider
shows the sandbox `destroyed`/`archived` (daytona), unknown to `docker
inspect`, or PID + marker gone (unix_local). Pins the leak that left 6
paused-with-no-row daytona sandboxes against the 2000 GiB quota in a
single day. Excluded from modal (terminate is already destructive).

### 4. `test_external_delete_preserves_agent_memory`
Out-of-band delete bypasses the server-side cold snapshot.
`inner_session_id` must be unchanged across the delete — the per-turn
`agent_memory.tar` (`.claude/sessions`, `.codex/sessions`) gives
`session/load` enough to resume. We don't assert workspace-file survival
— that's provider-specific shutdown handling, not the contract.

### 5. `test_session_resume_after_stop`
Sandbox stopped mid-conversation, fresh client sends turn 2. Non-empty
reply + `inner_session_id` unchanged (proves `session/load`, not `new`).

### 6. `test_session_resume_after_delete`
#4 + #5: sandbox *deleted* between turns; new sandbox is a different
provider instance. Continuity restored from the volume-persisted JSONL.

### 7. `test_session_survives_midstream_sandbox_stop`
Sandbox stopped externally; user does NOT send a new message. The SSE
reader must detect upstream EOF, rebind, and preserve the conversation
(no silent `session/new`). Exercises reader-driven recovery, distinct
from `/message`-driven recovery in #5/#6.

### 8. `test_message_immediately_after_stop`
Stop, then POST `/message` with no delay — scheduler picks up the prompt
before the SSE reader sees EOF. Exercises the `ConnectError` retry that
clears `_reader_connected` and forces rebind.

### 9. `test_message_after_stop_with_delay`
Stop, wait 4 s, POST. Reader has observed the disconnect and a stale
state entry exists — the reusable-state check must tear down cleanly
rather than POST to a dead URL. Distinct from #8: "confidently dead"
path, not the race.

### 10. `test_persistent_sse_stop_then_message`
UI flow: one `/events` connection held open across turn 1 / stop / wait
/ turn 2. Turn 2's events must reach the held-open stream. Pins
in-place rebind (`_rebind_state` mutates the existing state instead of
replacing it, so subscribers don't get kicked).

### 11. `test_persistent_sse_external_delete_then_message`
#10 with out-of-band delete. After reader retries exhaust, the server
must provision a replacement daytona sandbox AND call
`ensure_supervisor_url` on it (daytona is 2-phase: `create_sandbox`
returns `url=""`). Without that call, `AcpClient("")` raises
`UnsupportedProtocol` and recovery cascades into port-collision rebuilds
that drop turn 2.

### 12. `test_persistent_sse_delete_sandbox_then_message`
#10 with `DELETE /sessions/{id}` (UI's "delete session" button). Turn
2 must reach the persistent stream. Pins the zombie-state fix:
`force=True` on shutdown + kicking subscribers so the /events handler
wakes and reconnects onto the replacement state.

### 13. `test_persistent_sse_supervisor_killed_then_message` *(no docker)*
Supervisor dies in place, sandbox stays alive (the prod 502 / OOM
scenario). `pkill supervisor.js` (daytona) or `kill -9` (local). Turn
2 reaches the same stream via `restart_daytona_supervisor` /
respawn-in-place — sandbox row in DB is untouched, so this is recovery
not replacement. Docker excluded: `pkill` takes container PID 1 down,
covered by #11.

### 14. `test_persistent_sse_supervisor_killed_immediate_message`
#13 with no delay — POST fires in the ~100 ms window where
`_reader_connected=True` is still stale; daytona returns 502.
Exercises `_execute_one_prompt`'s retry on
`ConnectError`/`RemoteProtocolError`/`ReadError`.

### 15. `test_ui_reconnect_gap_persists_replies_to_session_log`
Supervisor dies → /events subscribers are kicked → user sends two
POSTs while no subscriber is attached → each turn must still land
`user_message` + `turn_end` rows in `session_log`. /events is
live-only; durable history lives in /log. The UI cold-loads /log on
mount (and on reconnect) so the gap-submitted prompts surface there
even though no subscriber saw them stream live.

Earlier this test pinned a per-session in-memory replay buffer that
re-delivered missed events through /events on reconnect; the buffer
was removed because it double-delivered everything a cold-loading UI
had just fetched from /log.

## Companion unit tests — `test_async_correctness.py`

Deterministic mechanism tests, no real sandbox. Three back the buffer fix:

- `test_dispatch_with_no_subscribers_buffers_for_next_subscribe` — the
  mechanism for #15. Dispatch 3 events to an empty session, then
  `subscribe_session`, assert all 3 arrive.
- `test_dispatch_with_active_subscriber_skips_buffer` — buffer is for
  no-live-subscriber only; a late second subscriber must not see events
  already delivered to the first.
- `test_buffer_bounded_under_flood` — `maxlen=2000` so a never-reconnecting
  client can't grow memory unbounded.

Run in <0.1 s; should be green before the golden suite runs.

## When to run the golden suite

If you touch:
- `SessionPool.get_session` / `cold_create` / `release`
- `SandboxSession.start`, `_recover_after_disconnect`, or
  `_shutdown_session_state`
- The session-delete route (pinned by #3)
- The daytona replacement branch (pinned by #11), or
  `ensure_supervisor_url` / `restart_daytona_supervisor`
- `HOME` in the supervisor spawn_env
- `sandbox_state.recipe` or `_build_volume_mounts`
- Subscriber dispatch (`broadcast` / `dispatch` / `subscribe_session`)
- session_log persistence in `_persist_prompt_events` (#15)
- `_execute_one_prompt`'s error handling (#8, #14)

Run against daytona (`DAYTONA_API_KEY` + `CLAUDE_CODE_OAUTH_TOKEN`) — its
sandbox↔volume separation surfaces mount/HOME bugs the others don't.

## Golden — `test_attach_recovers_with_no_volume_journal.py`

Inverse of #6. #6 covers happy `session/load` (volume's `snapshot.tar`
restored, JSONL present, load succeeds). This covers JSONL **missing**
from the volume on the new sandbox: `session/load` returns
`-32603 Internal error` and `acp_client.attach` must fall back to
`session/new` instead of wedging.

**Setup:** cold-create (mints `inner_session_id`), do NOT run a turn
(so `snapshot.tar` is never written), external-delete, send a prompt.
The new sandbox boots with empty HOME.

**Invariants:** non-empty reply + `inner_session_id` *different* from
pre-delete (proves `session/new`, not `session/load`).

**Coverage:** `{daytona, docker, modal}`. `unix_local`'s `_external_delete`
preserves HOME, so the failure mode can't be reproduced.

## Open issues

**Partial-failure leak in `pool.get_session`.** If `start()` raises
after a sandbox is provisioned (attach failure, health timeout), the
new `sandbox_ref` is never persisted and the sandbox isn't released.
Every subsequent `get_session` reads the stale ref and provisions
*another* sandbox. Fix: persist immediately after
`_resolve_or_create_sandbox` returns, before `_attach_acp`; on
`start()` failure either tear down or persist so the next attempt
reuses it.

## Running

```sh
# Server on localhost:7778; provider deps on PATH.
scripts/launch_server_test.sh &     # sets AGENT_SDK_ORIGIN=test
# Daytona: DAYTONA_API_KEY + CLAUDE_CODE_OAUTH_TOKEN
# Modal:   `modal setup` + CLAUDE_CODE_OAUTH_TOKEN

.venv/bin/pytest tests/test_sandbox_stop_delete_recovery.py -n auto -v
.venv/bin/pytest tests/test_attach_recovers_with_no_volume_journal.py -n auto -v
.venv/bin/pytest tests/test_sandbox_stop_delete_recovery.py -k daytona -n auto -v
.venv/bin/pytest tests/test_async_correctness.py -n auto -v   # mechanism-only, no server
```

Warm-server timing under `-n auto`: ~1 min on unix_local, ~3–5 min on
daytona. `test_async_correctness.py` is <1 s.
