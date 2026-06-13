# Native Runtime — P0 implementation plan

Companion to `native_runtime_design.md` (v2, stress-tested + frame-validated).
Each step lands as its own commit and must pass its verification gate before
the next step starts. Target: PR into `staging`, draft until P0-H is green.

## Steps and gates

| # | Step | Deliverable | Verification gate |
|---|------|-------------|-------------------|
| A | Frame synthesis | `src/api/native/frames.py` (templates from design §2) + `tests/test_native_frames.py` (promoted 42-assertion spike: byte round-trip through `parse_acp_event`, terminal-rule cases, adversarial content) | `pytest tests/test_native_frames.py` green |
| B | Precise terminal detection | `/message+stream` terminal check: substring prefilter + JSON-confirmed envelope (`id==rpc_id` ∧ (`result.stopReason` ∨ top-level `error`)). Fixes the latent ACP bug where agent content containing `stopReason` truncates the stream (design §2 F2-amended) | New unit test (content containing `stopReason` does NOT terminate; real envelopes do) + `tests/test_client_streaming.py`, `tests/test_acp_invariants.py`, `tests/test_acp_error_terminates_turn.py` green |
| C | Dispatch + storage wiring | `NativeSandboxState` in `state.py` union + factory route; `AgentConfig.native` passthrough field; `db.py` `native_transcripts` DDL + `write_native_checkpoint` / `read_native_checkpoint` (keep-last-2) | New unit tests: state round-trip serialize/deserialize, factory dispatch, config passthrough, checkpoint write/read/prune |
| D | Docker transport | `src/api/native/transport.py`: `DockerTransport` — native-flavor create (`env {prefix} sleep infinity` PID-1, no port publish, `exec true` readiness), exec with in-container `timeout`/cwd/env, stdin-pipe file write (>96KiB safe), read via exec | Transport integration test against local docker daemon (create → exec → write 200KiB file → read back → destroy) |
| E | Loop + tools | `src/api/native/loop.py` (LiteLLM streaming, tool-call accumulation, max_turns, usage) + `tools.py` (bash/read/write/edit schemas + dispatch) | Loop unit test with monkeypatched `litellm.acompletion` (scripted tool-call sequence) + fake transport; asserts emitted event sequence + message growth |
| F | NativeSession | `src/api/native/session.py`: internal-task+queue `execute_prompt`, `cancel_active_prompt` override (→ `done("cancelled")`), always-alive liveness, `start` (load checkpoint) / `stop` (write checkpoint), secrets split (AUTH_KEYS server-side) | Session unit tests: event pairing (broadcast block == parse(yield)), cancel mid-turn produces cancelled terminal + checkpoint, liveness survives pool force-probe pattern |
| G | Server wiring | Create paths: force-lazy + initial `NativeSandboxState` persist when `agent_type=="native"`; native branch for `/sandbox/exec` + `/files/*` (transport-backed; 409 pre-compute); reload: installs via transport, `mcp_servers` → 400 | Route unit tests via ASGI transport (create native session → state row shape; exec route 409 pre-compute) |
| H | End-to-end + goldens | Live server (launch_server_test.sh), SDK `Agent(agent_type="native", model="openrouter/...", provider="docker")` real turn; add `native` to `agent_type` params for `test_tool_effects_matrix` + interrupt-cancel golden (docker scope) | Those goldens green on docker; existing claude/opencode goldens unaffected (spot: `-k "tool_effects and docker"` all runtimes) |

## Sequencing notes

- B is independently valuable (production bugfix) and lands second so its
  test rides the new frames test helpers.
- C before D/E/F because every later step imports the state/config types.
- Per-step commits; push after each gate; PR stays draft until H. After every
  push: `gh pr view --json mergeable,mergeStateStatus` until CLEAN (branch
  hygiene rule).
- P1 (daytona/modal transports, hibernate/resume, secrets-split full
  enforcement at provider create) and P2 (workspace tar, background bash,
  compaction) are follow-up PRs — out of scope here.

## Risks watched during implementation

- Frame drift vs parsers → gate A pins byte-level parity; any sse.py change
  breaks the test loudly.
- TurnRunner interplay (cancel path) → gate F asserts the `turn_end` row and
  `done("cancelled")` ordering the interrupt goldens require.
- Pool force-probe semantics → gate F includes the cached-session probe
  pattern from pool.py:152-185.
- Golden cost: H runs docker-only for native; daytona/modal native goldens
  arrive with P1.
