# Plan: `extra_options` — ACP vendor-options pass-through

## Goal

Let callers of `agent_sdk.Agent` pass vendor-specific options that get
forwarded to the underlying ACP wrapper through `session/new`'s `params._meta`.
Concrete first user: the automle quality-gate grader wants Claude Code's
`tools`, `extraArgs` (for `--json-schema`), and `maxThinkingTokens` — which
claude-agent-acp reads from `params._meta.claudeCode.options`
(see `claude-agent-acp/dist/acp-agent.js:1037`).

## Why one field, vendor-namespaced at the edge

claude-agent-acp puts options under `_meta.claudeCode.options`. Other
ACP wrappers (codex-acp, opencode) will pick their own top-level namespace
key. The cleanest API is:

- **One generic kwarg** on `Agent`: `extra_options: dict | None`.
- **One translation site** at the protocol edge (`acp_client.initialize`)
  that wraps the dict as `_meta.<vendor>.options` based on `agent_type`.
- A tiny **const map** of `agent_type → vendor namespace` keeps the magic
  in one place. Unknown agent types log a warning and drop the options.

This matches the existing precedent: `mcp_servers` is a flat dict on `Agent`,
converted to the ACP array form at the same edge by `_mcp_dict_to_acp_array`.

## Scope guardrails

- **Session-level**, not per-call. claude-agent-acp only reads `_meta` in
  its `session/new` handler (`acp-agent.js:272 prompt()` does NOT look at
  `params._meta`). So we don't pretend we can change tools mid-session.
- **Persisted on the session row**, not the agent row. Mirrors claude-acp's
  scoping; lets a future Session-level override land naturally.
- **Pass-through, no validation in agent-sdk.** We don't know what claude
  / codex / opencode will accept in their options dicts — they will tell
  the user if they reject something. We document examples per known vendor.
- **No per-prompt override** in v1. If callers want different options for
  different prompts of the same agent, they make a new session.

## Vendor namespace map

```python
# api/acp_client.py
_VENDOR_META_NAMESPACE: dict[str, str] = {
    "claude": "claudeCode",
    # TODO once confirmed from the corresponding ACP wrapper source:
    # "codex": "<from @zed-industries/codex-acp source>",
    # "opencode": "<from opencode source>",
    # "cline": "<from cline-acp source>",
}
```

Add entries only after reading the wrapper's actual source (or a test
session that confirms the namespace is what's expected). For an
agent_type missing from this map, agent-sdk logs a warning and sends
`session/new` without `_meta`.

## Files to change

### 1. `src/api/db.py`

Add an `extra_options` JSONB column to `sessions`, following the
`workspace` column precedent (line 141):

```python
# already-present pattern
"ALTER TABLE sessions ADD COLUMN IF NOT EXISTS workspace TEXT",
# ──── add ────
"ALTER TABLE sessions ADD COLUMN IF NOT EXISTS extra_options JSONB",
```

Update `upsert_session()` and `update_session_env()` precedent to optionally
write the column:

```python
async def upsert_session(..., extra_options: dict | None = None) -> None:
    # add ("extra_options", extra_options, Jsonb) to the optional-columns
    # tuple list around line 366 (same shape as workspace).
```

Update `get_session()` to round-trip the column. Add an
`update_session_extra_options()` helper if we want server-side mutation,
but v1 sets it once at session create.

### 2. `src/api/models.py`

Add the field to whichever Pydantic-style model represents a session
record / `POST /sessions` body. Looking at the existing
`mode: str | None = None  # "default" | "plan" | "bypassPermissions" | ...`
field on line 48, the location pattern is clear — just add:

```python
extra_options: dict[str, Any] | None = None
```

to:
- the session-create request body model
- the session-row response model

### 3. `src/api/server.py`

`POST /sessions` accepts `extra_options` in the body, passes it to
`upsert_session()` and to the ACP-initialize path. The session-list
queries (server.py:1005, 1066) don't need to know about it — they just
echo whatever JSON the DB returns.

The acp_client call site (search for `acp.initialize(`) needs the new
kwarg threaded through:

```python
await acp.initialize(
    session_id,
    agent_type,
    cwd=cwd,
    mcp_servers=mcp_servers,
    extra_options=extra_options,   # ← new
)
```

### 4. `src/api/acp_client.py`

**The translation point.** Add the kwarg + namespace lookup:

```python
_VENDOR_META_NAMESPACE: dict[str, str] = {
    "claude": "claudeCode",
}

async def initialize(self, session_id: str, agent: str, cwd: str = "/tmp",
                     mcp_servers: dict | None = None,
                     extra_options: dict | None = None) -> dict:
    result = await self.handshake(session_id, agent)
    try:
        mcp_array = _mcp_dict_to_acp_array(mcp_servers) if mcp_servers else []
        params: dict = {"cwd": cwd, "mcpServers": mcp_array}

        if extra_options:
            ns = _VENDOR_META_NAMESPACE.get(agent)
            if ns:
                params["_meta"] = {ns: {"options": dict(extra_options)}}
            else:
                log.warning(
                    "agent_type=%r has no _meta namespace mapping; "
                    "extra_options will be ignored by the ACP wrapper",
                    agent,
                )
        # … rest of session/new retry logic unchanged …
        await self._send_rpc(session_id, "session/new", params)
```

Same treatment for `resume_session()` (line 270 area) where it falls
back to `initialize` on a missing inner session: forward `extra_options`
on that path too.

### 5. `src/agent_sdk/client.py`

Two additions on the `Agent` class:

a. Constructor kwarg (around line 614 with `mcp_servers`):

```python
extra_options: dict[str, Any] | None = None,
```

Stored on `self.extra_options = extra_options`.

b. Add `"extra_options"` to `_CLONABLE_FIELDS` (line 600) so
`Agent.clone()` and the registration-payload builder include it.

c. Wherever the session-create body is assembled (the place that
already sends `mcp_servers`), add `"extra_options"` next to it.

### 6. `src/agent_sdk/api_client.py`

The flat-method-per-route layer. Mirror the new field on
`create_session()` and `resume_session()` signatures — same as
`mcp_servers` is mirrored today.

### 7. Tests

The goal is **behavioral round-trip tests** — prove the option actually
changed what the agent does, not just that the wire format didn't break.
"The HTTP call succeeded" is necessary but not sufficient; the value
itself has to be honored downstream by claude code.

#### 7a. Unit — wire format snapshot (`tests/test_extra_options_wire.py`)

No live server. Patch `acp_client._send_rpc` to record params. Cases:

- `Agent(agent_type="claude", extra_options={"tools": [...]})` →
  payload's `_meta.claudeCode.options == {"tools": [...]}`.
- `Agent(agent_type="claude", extra_options=None)` →
  no `_meta` key in payload (wire byte-identical to today).
- `Agent(agent_type="opencode", extra_options={"x": 1})` →
  no `_meta`, warning logged (asserted via `caplog`).
- `Agent.clone()` preserves `extra_options`. Mutation of the
  passed-in dict after construction does NOT bleed through (the
  field is `dict(extra_options)` defensively copied at translation).

These run in milliseconds and catch namespace-mapping regressions.

#### 7b. Integration — **behavioral** roundtrip on a live test server

`tests/test_extra_options_behavior.py`, parametrized over claude in
unix_local + docker. Each test asserts the agent's *actual behavior*
changed, mirroring the pattern the user proposed.

**Test 1 — `disallowedTools` actually blocks the tool.**

```python
async def test_disallowed_tools_blocks_write(tmp_path):
    agent = Agent(
        "test-disallow",
        agent_type="claude",
        provider="unix_local",
        model="haiku",
        cwd=str(tmp_path),
        extra_options={"disallowedTools": ["Edit", "Write"]},
    )
    out = await agent.arun(
        "Create a file at /tmp/extra_opts_check.txt with the contents 'hello'. "
        "Do not ask, just do it.")

    # Behavior expectation: file is NOT created (the tool is gone)
    # AND the response acknowledges the restriction.
    assert not (tmp_path / "extra_opts_check.txt").exists()
    text = out.lower()
    refusal_markers = ["cannot", "can't", "unable", "not allowed",
                       "disallowed", "restricted", "no write"]
    assert any(m in text for m in refusal_markers), (
        f"agent did not signal the restriction; got: {out[:400]}")

    # Inverse control: same prompt without the restriction → file created.
    agent2 = Agent("test-allow", agent_type="claude", provider="unix_local",
                   model="haiku", cwd=str(tmp_path))
    await agent2.arun(
        "Create a file at /tmp/extra_opts_check_ok.txt with 'hello'.")
    assert (tmp_path / "extra_opts_check_ok.txt").exists()
```

The "and the agent says it can't" half is what makes this a *behavioral*
test rather than just an absence-of-side-effect test. The inverse
control proves the test isn't trivially passing because Claude refused
for unrelated reasons (auth, prompt confusion, etc.).

**Test 2 — `tools` whitelist limits which tools the agent has.**

```python
async def test_tools_whitelist_excludes_bash():
    agent = Agent(..., extra_options={
        "tools": [{"type": "builtin", "name": "Read"},
                  {"type": "builtin", "name": "Glob"}],
    })
    out = await agent.arun(
        "Run the shell command 'echo hello > /tmp/whitelist.txt'. "
        "If you can't, say why.")
    assert not Path("/tmp/whitelist.txt").exists()
    assert any(m in out.lower()
               for m in ["no shell", "cannot", "bash", "not available"])
```

**Test 3 — `extraArgs` with a JSON schema gets structured output.**

```python
async def test_extra_args_json_schema():
    schema = {
        "type": "object",
        "properties": {"answer": {"type": "integer"}},
        "required": ["answer"],
    }
    agent = Agent(..., extra_options={
        "extraArgs": {"json-schema": json.dumps(schema)},
    })
    # arun returns concatenated text by default; we need the structured
    # output. Use astream() or expose a new field on the result.
    structured = None
    async for ev in agent.astream("What is 2+2?"):
        if ev.get("type") == "result":
            structured = ev.get("structured_output")
    assert structured is not None
    assert isinstance(structured.get("answer"), int)
    assert structured["answer"] == 4
```

If `astream` doesn't currently surface `structured_output`, that's a
pre-requisite of this PR (a small follow-up in the SSE/event mapping
layer — claude-agent-acp emits it on the ResultMessage).

**Test 4 — `maxThinkingTokens` is observed.**

Hard to assert thinking count directly. The minimal observable is that
the option doesn't crash the prompt. A stronger check: capture the
ACP-emitted thinking-block events and assert the cumulative thinking
content respects the cap roughly. This is brittle, so mark it as
`@pytest.mark.smoke` rather than a strict assertion. Acceptable v1:
just smoke-run the prompt and confirm completion.

**Test 5 — option is session-scoped.**

```python
async def test_extra_options_immutable_in_session():
    agent = Agent(..., extra_options={"disallowedTools": ["Edit", "Write"]})
    s1 = await agent.create_session()
    # Reading the option back from the server should match what we set.
    row = await ApiClient(...).get_session(s1.session_id)
    assert row["extra_options"] == {"disallowedTools": ["Edit", "Write"]}
    # A second session can pass a different value (or fall back to the
    # agent's value if agent-level defaults are added).
```

**Test 6 — unknown agent_type drops the option with a warning.**

```python
async def test_unknown_agent_type_drops_extra_options(caplog):
    agent = Agent(name="t", agent_type="opencode",  # not in map
                  provider="unix_local",
                  extra_options={"some": "thing"})
    with caplog.at_level("WARNING"):
        await agent.arun("hi")
    assert "no _meta namespace mapping" in caplog.text
    # And the prompt still succeeds — option drop is non-fatal.
```

#### 7c. Regression — existing goldens

Run the daytona / docker / unix_local end-to-end goldens with
`extra_options=None` everywhere. Wire payload for `session/new` should
be byte-identical to today (no spurious `_meta` field). If anything
regresses, the `if extra_options:` branch is wrong and we have a leak.

#### 7d. Manual smoke before merging

`python examples/demo.py --test` with one extra line:

```python
agent = Agent(..., extra_options={"disallowedTools": ["Bash"]})
# Then prompt "run uname" — agent should say it can't.
```

Eyeball the streamed events to confirm `extra_options` was honored.

#### Why this set covers it

| Concern | Test |
|---|---|
| Wire format correct | 7a |
| Option actually applied by claude-agent-acp | 7b/1, 7b/2 |
| Option threads through to claude code's CLI flags | 7b/3 |
| Option survives DB round-trip | 7b/5 |
| Unknown agent_type doesn't crash | 7b/6 |
| Existing flows untouched | 7c |

Pre-merge: 7a (unit, fast), 7b/1 + 7b/3 (the two highest-value behavioral
tests), 7b/6 (warning path), 7c (regression). 7b/2 + 7b/4 + 7b/5 nice-to-have.

### 8. Docs

Add a short section to README under "Use the SDK":

```markdown
### Vendor-specific options (`extra_options`)

Pass through to the ACP wrapper's `_meta.<vendor>.options` channel.
agent-sdk wraps the dict based on `agent_type`.

For Claude Code (agent_type="claude"), the namespace is `claudeCode`
and the dict shape follows `@agentclientprotocol/claude-agent-acp`'s
`userProvidedOptions` (see its source for the full list). Common keys:

| Key | What |
|---|---|
| `tools` | Built-in tool preset or whitelist (e.g. `[{"type":"builtin","name":"Read"}, ...]`) |
| `disallowedTools` | List of tool names to deny (merged with ACP defaults like `AskUserQuestion`) |
| `maxThinkingTokens` | Thinking-token budget |
| `extraArgs` | Raw flags forwarded to the claude CLI — e.g. `{"json-schema": "<schema-json>"}` |
| `mcpServers` | Additional MCP servers beyond agent-sdk's `mcp_servers` (merged) |

Example: schema-enforced JSON output + restricted tools, mirroring
harbor's quality-check setup:

```python
agent = Agent(
    "qg",
    agent_type="claude",
    provider="unix_local",
    model="sonnet",
    extra_options={
        "tools": [{"type":"builtin","name":"Read"},
                  {"type":"builtin","name":"Glob"},
                  {"type":"builtin","name":"Grep"}],
        "maxThinkingTokens": 10000,
        "extraArgs": {"json-schema": json.dumps(my_pydantic_schema)},
    },
)
```

`extra_options` is set at session creation time and is immutable for the
lifetime of that session. Different options → make a new session.
For agent types not yet in agent-sdk's vendor map, the field is dropped
with a warning until the appropriate namespace is added.
```

## Implementation order

1. **db.py** column + `upsert_session` — smallest change, easiest to verify.
2. **acp_client.py** `_VENDOR_META_NAMESPACE` + `initialize` — the actual
   translation. Add a single unit test (route-the-call-and-snapshot-payload).
3. **server.py** body field + thread to acp_client. Daytona/docker goldens
   should stay green here.
4. **models.py** Pydantic shapes.
5. **client.py** / **api_client.py** — top-of-stack ergonomic field on
   `Agent` + `ApiClient.create_session`.
6. README section + example.
7. Once landed: swap automle's `quality_gate/grader.py` to set
   `extra_options={...}` instead of relying on prompt-only enforcement,
   re-run the discriminator analysis, and confirm AUC moves closer to
   claude_agent_sdk's number (head-to-head was ~0.69 vs 0.70 on the
   44-task overlap, so the expectation is "tightens, not transforms").

## Success criteria

- `pytest tests/ -n auto` passes unchanged (only the new tests added).
- Manual smoke: `python examples/demo.py --test` with `extra_options=
  {"tools": [{"type":"builtin","name":"Read"}]}` confirms claude won't
  use Bash for `"create hello.py"` (it has to refuse or use a tool it's
  allowed to).
- For the automle QG path: discriminator AUC measured on the same task
  pool moves from ~0.74 toward claude_agent_sdk's ~0.79 baseline. Even a
  3pp lift validates the change is doing something.

## Failure criteria

- Any existing daytona/docker golden regresses → revert and add a
  scoped flag (`enable_extra_options=False` default).
- claude-agent-acp rejects the `_meta.claudeCode.options` shape we
  construct (server returns an ACP error during session/new) → debug
  by capturing the on-wire payload from a real claude_agent_sdk call
  and diffing.

## Open questions for upstream

- **codex / opencode namespaces**: have to read the actual source.
  zed-industries/codex-acp is on GitHub; opencode publishes only a
  binary. May need to grep the binary or check upstream docs to learn
  what they accept.
- **Validation strictness**: should agent-sdk reject unknown top-level
  keys in `extra_options` for the active agent_type? Cleaner ergonomics
  vs. forward-compat tension. Recommend leaving lenient (pass-through)
  in v1, tighten if it causes silent failures in practice.
- **Conflict with `mode`**: agent-sdk's existing `Session.set_mode` /
  `mode` parameter (line 48 of models.py) sets permission mode. claude-
  agent-acp also reads permission mode from `settingsManager` (line 1034)
  — not from `_meta`. So permission_mode is independently handled and
  doesn't go through `extra_options`. Document this so users don't try
  to set `extra_options={"permissionMode": ...}` and wonder why it's
  ignored.
