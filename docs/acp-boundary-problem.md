# ACP prompt-boundary constraints

This document explains why the server runs at most one prompt per
session through the upstream ACP adapter at a time, and why
`interrupt=True` on the SDK is implemented as cancel-then-submit
client-side rather than a true mid-turn handoff on the wire.

## Desired properties

In the abstract, the ideal behavior would provide all three:

1. prompt B can take over while prompt A is still mid-turn
2. the server keeps using the standard ACP POST + SSE transport
3. events tagged with `rpc_id=A` contain only A's work, and likewise for B

With the current upstream ACP adapters, you can reliably get any two of
these at once, but not all three together.

## What upstream ACP does

When a second `session/prompt` arrives during an active turn, the ACP
adapters can hand control over at a natural boundary. In practice that
means "after the current tool result or model step," not necessarily
"after the whole task is finished."

That sounds like queue-on-the-wire behavior, but there is a catch:
intermediate chunks are emitted under the lifetime of whichever upstream
prompt loop is currently active. The adapters do not currently populate a
stable per-chunk message identifier that would let the server separate
"work about A" from "work about B" once those two have overlapped.

So if the server simply allowed multiple prompt RPCs in flight:

- B may start sooner
- but B's tagged event stream can contain reasoning, tool work, or text
  that is still semantically about A

That makes clean per-prompt attribution unreliable.

## Current server behaviour

The server runs at most one `session/prompt` against the ACP child at
a time per session. `POST /sessions/{id}/message+stream` and `POST
/sessions/{id}/message` share one canonical execution path
(`_execute_and_stream_sse`); both go through the SandboxSession's
`execute_prompt`, which holds the upstream prompt lock until the
`done` block surfaces.

Concurrent callers see whichever serialisation the SandboxSession
imposes — there is no explicit queue layer in front. The ACP child is
the choke point.

## How interrupt is implemented

`interrupt=True` on `Agent.send()` / `Agent.arun()` / `Agent.astream()`
is purely a client-side ordering:

1. `await agent.cancel()` — calls `POST /sessions/{id}/cancel`, which
   sends `session/cancel` (a JSON-RPC notification) to the supervisor's
   ACP child
2. wait for the cancelled prompt to reach a terminal block
   (`stopReason: "cancelled"`)
3. submit the new message via `POST /message` or `/message+stream`

The server's `POST /message` accepts an `interrupt` boolean for
historical wire compatibility, but on the per-prompt SSE path it is a
no-op — the client must drive cancel-then-submit.

`POST /sessions/{id}/cancel` is the single primitive: it routes
through the SessionPool so the cancel reaches the live ACP child even
if compute had to be cold-recovered first, and returns 200 (with
`detail: "no active lease"` when there is nothing to cancel).

## Why not do true concurrent prompt submission

The blocking issue is upstream attribution, not local queueing. The
server could stop serializing prompt RPCs, but then the tagged event
stream would no longer mean "everything here belongs only to this user
message."

Unless upstream ACP starts emitting a stable per-chunk message id, or we
replace the upstream adapter with something we own end-to-end, true
mid-turn queueing and clean per-prompt event attribution are in tension.

## Practical takeaway

What the server can explain clearly today:

- normal submission: serialise behind the current upstream prompt
- interrupt submission (client-side): cancel via `/cancel`, then submit
- `POST /sessions/{id}/cancel`: best-effort abort of the in-flight turn

Anything stronger than that requires either upstream protocol support
for semantic chunk attribution or a different agent runtime that we own.
