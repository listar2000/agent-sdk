# Native Runtime — an agent loop we own

Status: DRAFT v2 — v1 stress-tested by a 4-dimension adversarial code review
(server touchpoints, event-stream wire contract, provider transports,
state/secrets/lifecycle). All findings below cite real file:line and are
folded in. v1's core claim held; its "zero changes above execute_prompt"
did not — the true integration surface is enumerated in §8.

## Goal

A first-party agent runtime (`agent_type="native"`): an OpenAI-Agents-SDK-style
loop — raw LLM calls via LiteLLM, tool dispatch, repeat — running as a **thin
Python coroutine inside the API server**, with **tools executing in the
sandbox** (docker / daytona / modal). No supervisor, no ACP child process.
Agent runtime and sandbox compute are fully separate layers.

Non-goals (v1): MCP, handoffs/multi-agent, guardrails framework, voice,
unix_local provider, replacing claude/opencode agent_types.

## Verdict from the stress review

- **The load-bearing decision held**: `TurnRunner` and the subscriber fan-out
  consume only canonical event dicts + `(rpc_id, block)` broadcasts; a native
  `execute_prompt` slots in cleanly (verified at turn.py:34-42,215-238;
  session.py:98,686-791).
- **Byte-compatible synthesized ACP frames are MANDATORY, not optional**:
  `/message+stream` terminates by substring-matching raw blocks
  (server.py:2300-2330) and every consumer re-parses blocks via
  `parse_acp_event`; canonical-dict broadcasts would hang every stream
  (v1 open question 3 is resolved: must synthesize).
- The rest of the review produced ~10 BREAKS, all with small, concrete
  resolutions — captured per-section below and summarized in §8.

## 1. The loop (`src/api/native/loop.py`) — OpenAI-shaped, minimal

```python
@dataclass
class NativeAgentSpec:            # parsed from AgentConfig
    instructions: str
    model: str                    # any LiteLLM id
    max_turns: int = 25
    max_tokens: int | None = None
    temperature: float | None = None
    tool_names: list[str] | None = None

async def run_turn(spec, messages, tools, emit) -> None:
    for _ in range(spec.max_turns):
        resp = await litellm.acompletion(model=spec.model, messages=messages,
                                         tools=[t.schema for t in tools],
                                         stream=True, api_key=..., timeout=...)
        # stream deltas -> emit text/reasoning; accumulate tool_calls
        if not tool_calls:
            emit(usage); emit(done("end_turn")); return
        for tc in tool_calls:
            emit(tool(tc))
            result = await tools[tc.name].invoke(tc.args)   # sandbox RPC
            emit(tool_result(tc, result))
            messages.append(...)
    emit(done("max_turns"))
```

LiteLLM normalizes all providers, streaming included. LLM credentials are
passed per-call (`api_key=`), held in memory only — see secrets split §6.

**CONFIG (B3)**: `AgentConfig.from_dict` filters unknown fields
(models.py:48-53), so `instructions`/`max_turns`/etc. would be silently
dropped today. Resolution: add a `native: dict` passthrough field to
`_AGENT_CONFIG_FIELDS` carrying the spec verbatim.

**`set_model`/`configure` (S2)**: the base `_acp_call` no-ops silently with
no supervisor (session.py:647-666) but the server still persists the value to
`agents.config` (server.py:2042-2061). NativeSession overrides `set_model`/
`set_mode` to mutate the in-memory spec; the loop additionally re-reads spec
from config at each `execute_prompt` so recovery and config converge.

## 2. The wire contract (`src/api/native/frames.py`) — synthesized ACP frames

`execute_prompt` must, per event: `self._broadcast((rpc_id, block))` THEN
`yield canonical_dict` (same pairing as session.py:503-504). `block` is one
line, `"data: " + compact_json` (separators `(",",":")`), no trailing
delimiter (routes add framing, server.py:2182-2184).

Templates (verified against sse.py parsers + test_acp_error_terminates_turn):

| event | block payload |
|---|---|
| text | `{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":SID,"update":{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":…}}}}` |
| reasoning | same, `"agent_thought_chunk"` |
| tool | `…"update":{"sessionUpdate":"tool_call","toolCallId":tc.id,"toolName":name,"rawInput":{…},"status":"pending"}` |
| tool_result | `…"update":{"sessionUpdate":"tool_call_update","toolCallId":tc.id,"toolName":name,"status":"completed","content":[{"type":"content","content":{"type":"text","text":stringified}}]}` |
| usage | `…"update":{"sessionUpdate":"usage_update","cost":{"inputTokens":N,"outputTokens":M,"totalCostUsd":C}}` |
| done | `{"jsonrpc":"2.0","id":RPC,"result":{"stopReason":"end_turn"\|"cancelled"\|"max_turns"}}` |
| error | `{"jsonrpc":"2.0","id":RPC,"error":{"code":-32603,"message":…,"data":{"kind":…}}}` (never -32601 — filtered, sse.py:93-94) |

**Templates VALIDATED empirically** (spike, 42/42 assertions): every template
round-trips byte-for-byte through the real `parse_acp_event`, including
adversarial content; see `/tmp/frames_spike.py` (promote to
`tests/test_native_frames.py` in P0).

Hard constraints found by the review:

- **F2 (amended after spike) — terminal detection must be fixed, not worked
  around.** `/message+stream` matches bare substrings on raw blocks
  (server.py:2325-2329), and the spike proved JSON escaping protects
  `'"error":'` but NOT bare `stopReason`: **this is a latent production bug
  TODAY** — any ACP agent whose streamed text or tool output contains the
  character sequence `stopReason` (e.g. an agent reading this repo's own
  sse.py/server.py) terminates its own `/message+stream` mid-turn.
  Resolution (validated in the spike): keep the substring as a cheap
  prefilter, then JSON-parse the candidate block and confirm a true terminal
  envelope (`payload["id"] == rpc_id` and `result.stopReason` present, or
  top-level `error`). Fixes ACP today and gives native a sound contract.
  Tool results stay stringified-into-text regardless (required by F7's
  result-slot rule).
- **F5** — `args` is parsed exclusively from `rawInput` (sse.py:280): always
  fabricate it on `tool_call`.
- **F6** — done/error envelope `id` must byte-equal the rpc_id
  (sse.py:85,90) or the terminal frame is silently dropped → golden timeouts.
- **F7** — every `tool_call_update` must carry a non-None result slot and
  repeat `toolName`/`toolCallId` (sse.py:174-189, 284-293).
- **F8** — usage wrapped in `cost`, emitted BEFORE done (post-done frames are
  unreachable on `/message+stream`, server.py:2330).
- Heartbeats: not native's job — `iterate_subscriber` + routes produce them
  (session.py:729-737).

One `_emit(event)` helper produces block+dict from the same source object so
broadcast/persist parity holds by construction, and calls
`self.liveness.observe_chunk()` (S6/3.6 — feeds the reaper's compute clock,
pool.py:366-380).

## 3. The session class (`src/api/native/session.py`)

`NativeSession(BaseSandboxSession)` — reuses `_bootstrap_session` (DB-only;
gives `_agent_id`, cwd, volume wiring; volume_provider="" skips the provider
check, session.py:181), subscriber fan-out, `_prompt_lock`.

- **execute_prompt**: runs `run_turn` in an **internal asyncio.Task** feeding
  an asyncio.Queue the generator drains (S1/F3). Why: TurnRunner pulls the
  generator inside its own task (turn.py:215); cancelling THAT task would
  kill the persister and skip the `turn_end` row. `cancel_active_prompt()`
  override cancels the internal task; its CancelledError handler enqueues
  done(`"cancelled"` — exact string asserted by interrupt goldens) so
  TurnRunner sees a normal terminal. CancelledError must never escape into
  TurnRunner (`_drive_one` catches only Exception, turn.py:237).
- **Liveness (B5/3.3 — load-bearing, wrong polarity in v1)**: the pool
  force-probes every cached hit (pool.py:153-156) and the base probe reports
  dead when `_supervisor_url is None` (session.py:678-681) → v1's design
  would tear down and rebuild the native session on every message, losing
  in-memory state. Override `_liveness_probe`/`running()`: **the session
  object itself is the runtime — report alive unconditionally** (sandbox
  health only matters lazily, at tool-call time; must return well under the
  2s probe budget, liveness.py:163).
- **start()**: load latest checkpoint (or none) — no compute. The pool's
  start-failure cleanup is guarded on `sandbox_ref` so no-compute sessions
  skip destroy (pool.py:255-256).
- **Lifecycle (implemented) — hibernate ≠ destroy, so compute is reclaimed
  without leaking and resume is cheap.** The pool's `release()` (reaper or
  explicit) calls `stop()` then `shutdown()`:
  - **`stop()` = hibernate**: `docker stop` the container — frees CPU/RAM,
    KEEPS the container + its writable layer (workspace files survive).
    `sandbox_ref` is preserved on `state` (pool persists it after).
  - **`shutdown()` = in-memory teardown only**: cancels the loop task,
    drops the transport handle — does NOT touch compute (putting destroy
    here would make every reap a hard delete; the P0 bug this fixes).
  - **runtime hibernate is automatic**: `release()` pops the NativeSession
    from `pool._active`, so the in-memory `_messages` are GC'd; the next
    prompt cold-paths a fresh NativeSession whose `start()` reloads the
    checkpoint.
  - **resume**: `_ensure_sandbox` is reattach-or-create — if `sandbox_ref`
    is set and the container still exists, `docker start` it (workspace
    intact, fast); a stale ref (container gone) falls through to a fresh
    create.
  - **destroy()** (DELETE /sessions): `docker rm -f` by ref — the only path
    that actually removes the container. `_destroy_session_compute` routes
    native through `state.provider` (docker) since `state.type=="native"`
    has no provider module. Verified end-to-end: turn → release (container
    `exited`) → next turn reattaches the SAME container with the file
    intact AND recalls a checkpointed fact → delete removes it.
- **Lazy compute (S7)**: first tool call provisions via the provider's
  native-flavor create (§4), **then immediately `db.write_sandbox_state`**
  with the new sandbox_ref and the standard origin labels — otherwise a crash
  strands a live sandbox that boot-reconcile will destroy as an orphan
  (daytona/__init__.py:1418-1460). Provision under the pool's session lock.
- **Sandbox-exec / files routes (B2)**: `/sessions/{id}/sandbox/exec` and
  `/files/*` currently 502 for any supervisor-less session
  (server.py:2765-2795). Native implementations route through the same
  transport as the toolset; with no sandbox yet → 409 with a clear error
  (UI affordance) — not auto-provision (a file browse shouldn't cost a VM).
- **Reload (S4)**: skills/cli_tools installs exec via transport instead of
  supervisor `/v1/exec`; `mcp_servers` on native → 400 unsupported.
- **Credential refresh (S3/3.5)**: the pool's writer POSTs supervisor
  `/v1/exec` and silently skips when there's no URL (pool.py:552-591) —
  hivespace-style recipes would never land credentials. Add a
  `session.write_files(contents)` hook backed by the transport; pool calls
  it when the session class provides it.

## 4. Provider transports (`src/api/native/transport.py`)

Uniform `SandboxTransport` per provider: `exec(cmd, *, cwd, env, timeout)`,
`read_file`, `write_file`, `ensure_sandbox()` (lazy create). Verified
primitives + required shims:

| | exec today | shims required |
|---|---|---|
| docker | `docker exec` subprocess (docker/__init__.py:399-410), ~10-50ms | **D3**: timeout kills the docker *client*, not the in-container process — wrap as `timeout {t}s sh -c …` in-container. **D2**: plumb cwd/env (`-w`/`-e`). **D4**: base64-over-exec caps at ~96KiB (MAX_ARG_STRLEN) — file writes use `docker exec -i … 'base64 -d > f'` with stdin pipe (or `docker cp`). |
| daytona | `sandbox.process.exec` toolbox HTTPS (daytona/__init__.py:883-900), ~200-600ms | **Y1**: SDK supports `cwd=`/`env=` natively — plumb through. **Y2**: no stderr field exists — append `2>&1`, cap at source (`\| head -c 1048576`). **Y3**: map the SDK's raised server-side timeout to `timed_out` results. **Y7**: cache the `AsyncSandbox` handle per session (halves RTTs). Files: `fs.download_file`/`upload_file` work on session sandboxes as-is (Y4). |
| modal | `sb.exec` via thread (modal/__init__.py:611-646), ~200-800ms | **M2**: pass `timeout=`/`workdir=`/`env=` to `sb.exec` (SDK supports; repo doesn't plumb); kill = pid-capture + `kill` exec (ContainerProcess has no kill — verified). **M3**: files via base64-over-exec (gRPC args, chunk >1MiB) or the unused `sb.open()` API. **M6**: cache the Sandbox handle. |

**Native-flavor sandbox creation (D5, M4 — BREAKS as-is):** docker and modal
both run *the supervisor as PID-1* and gate creation on its health
(docker/__init__.py:251-299,333-345; modal/__init__.py:310,401-454). Add a
create flavor: PID-1 = `env {prefix} sleep infinity` (env prefix preserved —
D6/Y6: it's the only way session env + AUTH_KEYS-unset hygiene reach the
sandbox), no port publish / no tunnel, readiness = one `exec true`. Modal
keeps its mount-prelude (mkdir/symlink, modal/__init__.py:290-303). Daytona
create is already clean (provision returns url="", daytona/__init__.py:449-455)
but must `mkdir -p /home/daytona` at first tool call (supervisor.js does it
today) and pass `env_vars=_get_sandbox_env_vars(spawn_env)` at create (Y6).

**Modal lifetime (M5 — BREAKS the v1 "nothing to do" claim):** sandboxes have
a hard 1h ceiling + 2100s idle timeout whose only activity signal without a
tunnel is exec traffic (modal/__init__.py:70-81); stop is destructive and
start always raises (modal/__init__.py:532-558). Workspace files survive on
the v2 volume, so: the transport treats `SandboxMissingError` mid-turn as
**recreate on same volume+subpath, retry the tool call once**. Sandboxes are
cattle; the volume is truth.

## 5. State & checkpoints — dedicated table (v1 open question 1: RESOLVED)

The review killed the session_log option (Q1): the batcher is lossy **by
contract** (event_buffer.py:16-19,115-116 — silent drops on flush failure;
a dropped checkpoint silently rewinds the conversation), its memory model
assumes ~1KB rows, redaction is impossible to get right both ways (redacted
checkpoints feed `[REDACTED]` back to the model on resume; unredacted rows
leak secrets into the `/log` UI route, db.py:667-690), and checkpoint blobs
would pollute the 500-row `/log` tail.

**`native_transcripts` table**: `(session_id FK CASCADE, turn_seq, messages
JSONB, usage JSONB, created_at)`, written **synchronously** at turn end
(direct INSERT, not the batcher — checkpoint durability is correctness),
keep-last-N (N=2) per session via delete-on-insert, never served by `/log`,
never redacted (it round-trips to the model, not to humans; the `/log` rows
TurnRunner writes remain the redacted human-facing record). Resume = newest
row. Compaction (v2): summarize-and-truncate when the array exceeds a token
budget.

**Workspace files (daytona)**: unchanged from v1 — v1 rides daytona
pause/resume (parity with today's hibernate-while-VM-lives); v1.5 adds
server-side tar persist (exec tar → fs.download → volume), run only in
`release`/reap, never in `shutdown_all`'s 10s budget (S5).

## 6. Secrets split (S8/2.2 — must be explicit)

`sessions.secrets` is plaintext JSONB, loadable server-side at prompt time
(db.py:41-51,464-471) — FINE for per-call LiteLLM keys. But
`_bootstrap_session` merges env+secrets wholesale into `_spawn_env`
(session.py:209-212), which provider creates inject into the sandbox — v1's
"LLM keys never enter the sandbox" requires an explicit split, and silently
dropping ALL secrets from the sandbox would break non-LLM secrets
(GITHUB_TOKEN etc.) that today's semantics deliver:

- `secrets ∩ AUTH_KEYS` (_shared.py:43-69) → server-side LiteLLM only; never
  in `_spawn_env`, never in transport exec env.
- `secrets ∖ AUTH_KEYS` → sandbox env at native-flavor create (env prefix).

## 7. Session create & recovery dispatch (B1/3.1/3.8 — the biggest v1 gap)

- **State variant**: add `NativeSandboxState` to the discriminated union
  (state.py:174-199) carrying `provider` (target for lazy create),
  `sandbox_ref|None`, `recipe` (pre_start_commands, credential_refresh_url,
  mounts all apply once compute exists), `snapshot_path`, `last_turn_seq`.
  Keeping `_BaseSandboxState` fields makes `live_sandbox_refs`/orphan
  reconcile work unchanged (3.2).
- **Factory**: `make_session` currently dispatches on provider only and
  defaults unknown→daytona (factory.py:45-57,87). Register `"native"` →
  `NativeSession`.
- **Create paths**: `_sessions_create_lazy` persists NO sandbox_state
  (server.py:1810-1820) → recovery would deserialize `UnknownSandboxState`
  with `Recipe(agent_type="opencode")` and cold-create a daytona supervisor
  (!). Fix: at create, when `agent_type=="native"`, ALWAYS persist an initial
  `NativeSandboxState` (provider, no ref). And `_sessions_create_eager`
  (`provision=true` is the server AND SDK default — server.py:1758,
  client.py:212) must branch to the lazy path for native (3.8): "SDK: zero
  changes" survives only because the server reroutes.

## 8. The honest integration surface (replaces v1's "untouched")

New code: `src/api/native/{loop,frames,session,tools,transport,checkpoints}.py`.
Touched existing code (all small, none in the ACP/CLI path):

1. `models.py` — `native` config passthrough field (B3)
2. `state.py` + `factory.py` — NativeSandboxState + dispatch (B1)
3. `server.py` create paths — force-lazy + initial state persist for native (3.8)
4. `server.py` sandbox-exec/files routes — native branch over transport (B2)
5. `server.py` reload route — transport exec / 400 for unsupported (S4)
6. `pool.py` credential-refresh — `write_files` hook (S3)
7. providers docker/modal/daytona — native-flavor create + cwd/env/timeout
   plumbing on exec/file primitives (§4)
8. `db.py` — `native_transcripts` DDL + accessors (§5)
9. `server.py` `/message+stream` terminal detection — precise envelope check
   replacing bare substrings (F2-amended; also fixes the latent ACP
   `stopReason`-in-content truncation bug, independently shippable)

Explicitly NOT touched: TurnRunner, SSE parsing, subscriber fan-out, SDK,
supervisor.js, the claude/opencode paths, goldens (they gain a parametrize
entry).

## 9. What stays radically simpler (unchanged from v1, now verified)

per-session runtime = coroutine + messages list; chat-only turns = no
compute (but NOT zero provisioning: `sessions.volume_id` is NOT NULL — a
default volume row still resolves at create, 3.7); interrupt = internal-task
cancel; conversation durability = one synchronous Postgres row per turn —
no tar machinery, no S3 visibility waits, no wedged-child class (LiteLLM
calls carry real timeouts).

## 10. Phasing

- **P0**: loop + frames + NativeSession + docker transport (native-flavor
  create) + 4 tools (`bash`, `read_file`, `write_file` str-replace `edit`),
  checkpoint table, dispatch wiring. Gate: tool-effects-matrix golden +
  interrupt parity goldens green with `agent_type="native"` on docker.
- **P1**: daytona + modal transports (incl. M5 recreate-on-missing), lazy
  provision under lock, hibernate/resume via checkpoints, secrets split.
  Gate: recovery goldens green on all three providers.
- **P2**: daytona workspace tar persist; `bash_background` (daytona process
  sessions); compaction; parallel tool calls; custom user tools.

## Open questions (v2)

1. ~~checkpoint storage~~ → RESOLVED: dedicated table.
2. Edit tool: str-replace (v1 lean) vs V4A patch — still open, default lean.
3. ~~byte-compat frames~~ → RESOLVED: mandatory.
4. NEW: `/sandbox/exec`-before-compute UX — 409 (current design) vs
   auto-provision on first exec from the UI?
