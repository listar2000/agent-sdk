# agent-sdk tests

Most tests here are unit/integration tests that run against mocks or an
in-process app. A small set of E2E tests drive a real server on
`localhost:7778` with one or more providers (`local`, `docker`, `daytona`) and
are skipped automatically when the required dependencies aren't present.

## Golden recovery tests — `test_sandbox_stop_delete_recovery.py`

These four tests describe the **real-world crash/kill surface** users hit in
production. They are load-bearing: they must pass for every supported
provider before the server is considered safe to ship. Don't weaken the
assertions to land a green build — if a test fails, fix the server (or the
test prompt, if the LLM is actually misbehaving in a reproducible way), not
the contract.

Each test is parameterized over `{local, docker, daytona}` and simulates an
out-of-band provider event by bypassing the server's HTTP API (`daytona.stop`,
`docker rm -f`, `kill -9` on the local supervisor PID). The next client
request must then succeed without human intervention.

### 1. `test_stop_sandbox_same_sandbox_after_restart`

**Scenario:** provider stops the sandbox while a session is live
(supervisor crash, host reboot, manual `daytona.stop`).

**Invariant:** the next request to the same session restarts the *same*
sandbox — verified by comparing `sandbox_ref` from the server's
`/sandboxes/{id}` endpoint before and after the stop. A changed
`sandbox_ref` means the server incorrectly provisioned a replacement instead
of waking the original, which would waste resources and orphan in-flight
state.

The test also asks the agent for `hostname` after the restart as a liveness
check — the sandbox must answer. The primary assertion is on `sandbox_ref`,
not hostname, because container UUIDs aren't a reliable invariant across
Daytona stop/start.

### 2. `test_delete_sandbox_volume_persistence`

**Scenario:** provider deletes the sandbox (quota cleanup, manual
`daytona.delete`). The attached **volume** is untouched — that's the whole
point of separating sandbox (ephemeral) from volume (durable).

**Invariant:** the next request provisions a *new* sandbox on the *same*
volume, and files the previous sandbox wrote to `~/` survive. The test
writes a marker file (`~/recovery-test-marker.txt`) via the agent before the
delete and asks the agent to `cat` it after. A new `sandbox_ref` + surviving
marker together prove the delete-then-reattach path works.

This is the canary for the volume-refactor: if HOME isn't on the volume
(e.g. Claude Code writes to `/root/.claude` because HOME inherits from the
VM default), the marker is gone and the test fails loudly.

### 3. `test_session_resume_after_stop`

**Scenario:** the sandbox is stopped mid-conversation and the user
reconnects.

**Invariant:** conversation history is restored on the restart. We tell the
agent a ticket ID in turn 1, stop the sandbox externally, then ask in turn 2
(via a fresh httpx client, simulating a UI reconnect) what the ticket ID was.
The agent must recall it — which only works if (a) the server calls
`session/load` with the original `inner_session_id`, (b) the CLI finds the
session JSONL on disk, and (c) that disk path is on the volume (HOME must be
`/home/daytona`, not `/root`).

The ticket-ID framing (`TKT-78901`) is deliberate: Claude refuses to "remember
a secret code" on safety grounds, even when instructed, which produced
false negatives before.

### 4. `test_session_resume_after_delete`

**Scenario:** combination of #2 and #3 — the sandbox is *deleted* between
turns and the new sandbox is a different provider-level instance, but the
session/conversation continuity must hold via the volume.

**Invariant:** same as #3 but across a `delete` rather than a `stop`.
Everything the server needs to keep the conversation alive must live on the
volume (the sandbox row in Postgres, the CLI's session JSONL, the
supervisor install under `/opt/supervisor`, etc.). A new container, a new
supervisor URL, but the user's conversation picks up where it left off.

## Why these are load-bearing

Production incidents that fall in this quadrant — sandbox went away, user
came back, got a 500 — are the hardest to debug after the fact: the provider
state is gone, the logs are in the deleted container, and the user is
frustrated. These four tests pin down the server's contract for that
quadrant so regressions are caught in CI, not by customers.

If you touch any of:

  - `_ensure_sandbox_locked` / `_ensure_runtime_locked` / `ensure_session_live`
  - `ensure_supervisor_url` or `restart_daytona_supervisor` in any provider
  - Anything that sets/reads `HOME` in the spawn_env for a supervisor
  - `_provision_new` (how a replacement sandbox gets its root + mounts)
  - The volume-mount layout (`_build_volume_mounts`)

…run this file against `daytona` (live `DAYTONA_API_KEY` + `CLAUDE_CODE_OAUTH_TOKEN`)
before merging. The other providers are cheaper to run and worth running
too, but `daytona` has historically been the one that surfaces mount/HOME
bugs because its sandbox ↔ volume separation is strictest.

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

Each test takes ~30–90 s per provider (Daytona provisioning is the
expensive step). All four over all three providers is ~10 minutes.
