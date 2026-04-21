# ACP prompt-boundary constraints

This document explains why the server uses an explicit queue plus
optional `interrupt=True` instead of forwarding multiple `session/prompt`
requests concurrently to the same ACP session.

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

So if the server simply allows multiple prompt RPCs in flight:

- B may start sooner
- but B's tagged event stream can contain reasoning, tool work, or text
  that is still semantically about A

That makes clean per-prompt attribution unreliable.

## Current server decision

The server therefore keeps execution single-threaded per session:

- one active prompt upstream
- later prompts queued in `pending_prompts`
- one long-lived upstream SSE reader
- downstream fan-out from the server, not directly from ACP

If the caller wants to take over quickly, they use `interrupt=True`:

1. cancel the active turn
2. wait for that turn to reach a terminal state
3. enqueue the new prompt behind any already-queued work

This preserves semantic cleanliness for prompt tagging at the cost of
true mid-turn handoff.

## Why not do true concurrent prompt submission

The blocking issue is upstream attribution, not local queueing. The
server could stop serializing prompt RPCs, but then the tagged event
stream would no longer mean "everything here belongs only to this user
message."

Unless upstream ACP starts emitting a stable per-chunk message id, or we
replace the upstream adapter with something we own end-to-end, true
mid-turn queueing and clean per-prompt event attribution are in tension.

## Practical takeaway

Queueing and interrupt are the public semantics the server can explain
clearly today:

- normal submission: append to the queue
- interrupt submission: cancel active turn, then continue in queue order

Anything stronger than that requires either upstream protocol support
for semantic chunk attribution or a different agent runtime that we own.
