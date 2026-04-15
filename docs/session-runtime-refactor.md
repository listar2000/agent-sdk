# Session runtime model

This document describes the current session runtime in the API server.

## Core model

Per session, the runtime is deliberately single-threaded upstream:

- one prompt is active at a time
- later prompts sit in a FIFO queue
- one long-lived SSE reader ingests upstream events
- downstream clients subscribe to the server fan-out

The explicit state lives on `SessionState`:

- `active_rpc_id: str | None`
- `pending_prompts: deque[PendingPrompt]`
- `_prompt_ready: asyncio.Event`
- `_prompt_done: asyncio.Event`
- `_scheduler_task: asyncio.Task | None`

`agent_busy` is just `active_rpc_id is not None`.

## Scheduling

`POST /sessions/{id}/message` does not talk to ACP directly. It:

1. validates the request
2. allocates a new `rpc_id`
3. appends a `PendingPrompt`
4. sets `_prompt_ready`
5. returns immediately

The scheduler loop owns execution:

1. wait for `_prompt_ready`
2. pop the next `PendingPrompt`
3. assign its `rpc_id` to `active_rpc_id`
4. call `state.client.prompt(...)`
5. on terminal response or terminal error:
   - clear `active_rpc_id`
   - mark the turn finished
   - set `_prompt_done`
   - continue to the next queued item

This keeps queue state explicit and prompt ownership in one place.

## Subscribers and fan-out

The server maintains one upstream SSE connection per live session. The
reader parses each ACP block once, updates runtime state, logs it, and
dispatches it downstream.

There are two subscriber classes:

- session-scoped subscribers via `subscribe_session()`
- RPC-scoped subscribers via `subscribe_rpc(rpc_id)`

Session subscribers receive everything. RPC subscribers only receive
events tagged for their prompt. This is how `Agent.events()` and
`Agent.astream()` can coexist with debug UIs or other observers without
opening multiple upstream ACP streams.

## Interrupt semantics

`interrupt=True` is explicit queue policy:

- if a prompt is active, send `session/cancel`
- wait for `_prompt_done`
- preserve existing queued prompts
- append the new prompt at the tail of the queue

So interrupt means "cancel the running turn, then continue in queue
order." It is not "jump this prompt to the front" and it does not drop
queued work.

The standalone `POST /sessions/{id}/cancel` endpoint performs the same
cancel-and-wait step without enqueuing anything new.

## Why this model exists

The server used to infer prompt state indirectly from in-flight task
tracking. The current runtime keeps the same single-threaded upstream
execution model but makes it explicit:

- queue state is inspectable
- busy state is unambiguous
- prompt lifecycle transitions happen in one scheduler loop
- SSE ingestion and downstream fan-out stay centralized

That keeps the runtime predictable while matching the constraints of the
underlying ACP session.
