# agent-sdk tests

Unit tests run against mocks; goldens drive a real server on `localhost:7778` against `unix_local`, `docker`, `daytona`, or `modal` and skip when their deps are absent.

`-n auto` is mandatory (sequential is 8–15 min). Fine with `-k`; xdist negotiates worker count down.

## Running

```sh
scripts/launch_server_test.sh &     # defaults AGENT_SDK_ORIGIN=test
# Source whichever creds you have set so the tests can forward them:
#   Claude  : CLAUDE_CODE_OAUTH_TOKEN
#   OpenCode: OPENROUTER_API_KEY
#   Daytona : DAYTONA_API_KEY
#   Modal   : `modal setup` (writes ~/.modal.toml)

.venv/bin/pytest tests/test_golden.py -n auto -v
.venv/bin/pytest tests/test_attach_recovers_with_no_volume_journal.py -n auto -v
.venv/bin/pytest tests/test_async_correctness.py -n auto -v   # mechanism-only, no server
```

Warm-server timing under `-n auto`: ~1 min unix_local, ~3–5 min daytona. `test_async_correctness.py` <1 s.

End-to-end tests are parametrized over `claude` + `opencode` via `tests/_acp_runtimes.py`. Each runtime auto-skips when its credential env var (`CLAUDE_CODE_OAUTH_TOKEN` / `OPENROUTER_API_KEY`) is unset. Filter with `-k claude` / `-k opencode` to run one runtime.

## Golden recovery — `test_golden.py`

16 tests × `provider × agent_type` = **120 cases**. Each simulates an out-of-band provider event (`daytona.delete`, `docker rm -f`, `kill -9`, `pkill supervisor.js`) that bypasses the server's HTTP API; the next client request must succeed without intervention. Invariants are deterministic server-side (`inner_session_id`, `sandbox_ref`, non-empty reply) — never "agent recalls X" (LLM guardrails flake).

| # | What it pins |
|---|---|
| 1 | Provider stop → next request restarts the *same* sandbox (`sandbox_ref` unchanged). |
| 2 | `/release` snapshots to volume *before* dropping compute (turn-1 marker readable turn 2). |
| 3 | `DELETE /sessions/{id}` destroys compute (not pause) — pinned the leak that left 6 paused-with-no-row daytona sandboxes against the 2000 GiB quota in a day. Modal excluded. |
| 4 | Out-of-band delete bypasses cold snapshot; per-turn `agent_memory.tar` (`.claude/sessions`, `.codex/sessions`, `.local/share/opencode`) gives `session/load` enough to resume. `inner_session_id` unchanged. |
| 5 | Stop mid-conversation → fresh client turn 2: non-empty reply + `inner_session_id` unchanged (proves `session/load`, not `session/new`). |
| 6 | #4 + #5: sandbox *deleted* between turns; new instance, continuity restored from volume JSONL / SQLite DB. |
| 7 | Sandbox stopped externally, no new message — SSE reader detects EOF, rebinds, preserves the conversation. Reader-driven recovery (#5/#6 are message-driven). |
| 8 | Stop, POST `/message` with no delay — scheduler picks up before reader sees EOF. Exercises `ConnectError` retry that clears `_reader_connected` and forces rebind. |
| 9 | Stop + 4 s wait + POST. Reader has seen the disconnect; reusable-state check must tear down cleanly rather than POST to a dead URL. "Confidently dead" path. |
| 10 | UI flow: one `/events` connection held across turn 1 / stop / turn 2. Pins in-place rebind (`_rebind_state` mutates state instead of replacing it, so subscribers don't get kicked). |
| 11 | #10 with out-of-band delete. Server provisions replacement daytona AND calls `ensure_supervisor_url` (daytona is 2-phase: `create_sandbox` returns `url=""`); without it `AcpClient("")` raises `UnsupportedProtocol`. |
| 12 | #10 with `DELETE /sessions/{id}`. Pins zombie-state fix: `force=True` shutdown + kicking subscribers so `/events` wakes onto the replacement. |
| 13 | Supervisor dies in place, sandbox alive (prod 502 / OOM). `pkill supervisor.js` (daytona) / `kill -9` (local). Recovery in place — DB row untouched. No docker (`pkill` takes container PID 1 down, covered by #11). |
| 14 | #13 with no delay — POST in the ~100 ms `_reader_connected=True` stale window; daytona 502. Exercises `_execute_one_prompt`'s retry on `ConnectError`/`RemoteProtocolError`/`ReadError`. |
| 15 | Supervisor dies, subscribers kicked, two POSTs without subscribers — each turn must still land `user_message` + `turn_end` in `session_log`. `/events` is live-only; durable history is `/log` (UI cold-loads it on mount/reconnect). |
| 16 | Concurrent stop + message: external stop fires while a fresh prompt is in flight. The pool's race-handling must reach a single consistent state — neither silent-failure nor double-recovery. |

The earlier per-session in-memory replay buffer (re-delivering missed events through `/events`) was removed — it double-delivered everything a cold-loading UI had just fetched from `/log`.

## Companion mechanism tests — `test_async_correctness.py`

No real sandbox. Three back the #15 buffer fix:

- `test_dispatch_with_no_subscribers_buffers_for_next_subscribe` — dispatch to empty session, then `subscribe_session`, all events arrive.
- `test_dispatch_with_active_subscriber_skips_buffer` — buffer is no-live-subscriber-only; a late subscriber must not see events already delivered.
- `test_buffer_bounded_under_flood` — `maxlen=2000` so a never-reconnecting client can't grow memory unbounded.

## Golden — `test_attach_recovers_with_no_volume_journal.py`

Inverse of #6. #6 = happy `session/load` (snapshot.tar + JSONL present). This = JSONL **missing** on the new sandbox: `session/load` returns `-32603` and `acp_client.attach` must fall back to `session/new` instead of wedging.

Setup: cold-create (mints `inner_session_id`), no turn (so `snapshot.tar` never written), external-delete, prompt. New sandbox boots with empty HOME. Invariants: non-empty reply + `inner_session_id` *different* (proves `session/new`). Covers `{daytona, docker, modal}` — `unix_local`'s `_external_delete` preserves HOME, so the failure mode can't be reproduced.

## When to run the golden suite

You touched:
- `SessionPool.get_session` / `cold_create` / `release`
- `SandboxSession.start`, `_recover_after_disconnect`, `_shutdown_session_state`
- The session-delete route (#3)
- The daytona replacement branch (#11), `ensure_supervisor_url`, `restart_daytona_supervisor`
- `HOME` in the supervisor spawn_env or the snapshot tarball (`AGENT_MEMORY_DIRS`)
- `sandbox_state.recipe` or `_build_volume_mounts`
- Subscriber dispatch (`broadcast` / `dispatch` / `subscribe_session`)
- `_persist_prompt_events` (#15)
- `_execute_one_prompt`'s error handling (#8, #14)
- The ACP config-replay loop in `_attach_acp` (set_model / set_mode / set_thought_level)

Run against daytona — its sandbox↔volume separation surfaces mount/HOME bugs the others don't. Run with `-k opencode` after touching opencode-specific paths (terminal/permission handlers, fs delegate, XDG storage layout).

## Rebuilding runtime artifacts

After supervisor.js, Dockerfile, or `_ACP_NPM_SPECS` changes, rebuild whichever cloud runtime your tests will exercise — otherwise daytona/modal sandboxes will boot the previous tag's supervisor:

```sh
scripts/release.sh                          # all three (docker + daytona + modal), each skipping if its prereq is absent
scripts/release.sh --provider daytona       # only daytona snapshot
scripts/release.sh --provider modal         # only modal snapshot
scripts/release.sh --provider docker        # only local docker image
```

The script writes the new tags to `.runtime-image-tag`, `.runtime-snapshot-tag`, and `.modal-snapshot-tag`; commit those alongside the runtime-affecting source change so other developers / CI pick up the right artifacts.

## Open issues

**Partial-failure leak in `pool.get_session`.** If `start()` raises after sandbox provisioning (attach failure, health timeout), the new `sandbox_ref` is never persisted and the sandbox isn't released — every subsequent `get_session` reads the stale ref and provisions *another* sandbox. Fix: persist immediately after `_resolve_or_create_sandbox` returns, before `_attach_acp`; on `start()` failure either tear down or persist so the next attempt reuses it.
