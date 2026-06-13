# Native checkpoint write-volume — design proposal (needs human review)

**Status:** proposal only. The native-scalability loop deliberately did NOT
implement this — it's a schema migration on a durability-critical table and the
cost/benefit is a judgment call. This doc exists so that review has something
concrete to evaluate. The orthogonal round-trip and retry wins are already
shipped (see below).

## Problem

`write_native_checkpoint` (db.py) persists the **full** message array on every
turn — an idempotent upsert into `native_transcripts (session_id, turn_seq,
messages JSONB, usage)` with a keep-last-2 prune, folded into one statement.

Per turn that serializes + writes O(transcript size). Over a session that's
**O(n²)** in cumulative serialization CPU, JSON bytes on the wire, and WAL.
It is bounded — a session can't grow `_messages` past the model's context
window, so `n` is capped (~hundreds of messages / ~1 MB JSON at a 200k-token
window) — but a long session at high context still re-ships its whole transcript
every turn (~100× write amplification for a 200-turn session).

### Measured cost (benchmark/micro/bench_native_runtime.py `_checkpoint_serialization`)

Per-turn `json.dumps(_messages)` — exactly what psycopg's `Json` adapter pays on
every `write_native_checkpoint` — as one session deepens (synthetic SMALL
messages, so these are a **lower bound**; real turns carry tool args / file
contents):

| msgs | serialize µs/turn | payload KB/turn | µs/msg |
|---:|---:|---:|---:|
| 807 | 351 | 159 | 0.43 |
| 1607 | 591 | 316 | 0.37 |
| 2407 | 927 | 474 | 0.39 |
| 3207 | 1302 | 631 | 0.41 |
| 4007 | 1478 | 788 | 0.37 |
| 4807 | 1857 | 946 | 0.39 |

`µs/msg` is flat → strictly O(n) per turn (O(n²) cumulative). The takeaway that
reframes the whole loop: the **loop** per-turn cost is ~118 µs and FLAT
(`_session_growth`), so by ~4800 messages the checkpoint serialize (~1.86 ms) is
**~16× the entire loop** and still climbing — it is the dominant per-turn cost
for deep sessions, and it's GIL-holding on the event-loop thread (psycopg
serializes the param inline), so it stalls every other session on the replica
for that window. Plus ~0.95 MB/turn of WAL+network at that depth.

**Update — the CPU half is now mitigated (orjson).** `write_native_checkpoint`
now serializes via `db._fast_dumps` (orjson, stdlib fallback), ~1.8× faster on
these tiny synthetic messages and **~4.4× on realistic content-heavy
transcripts** (2.39 → 0.55 ms at 4800 msgs / ~1.2 MB), so the loop-thread-
blocking serialize is cut to ~0.5 ms. That removes the *CPU* stall but does
**not** touch the per-turn payload bytes — the **O(n²) WAL + network write
volume** is unchanged and is now the *sole* remaining concern this migration
targets. So the lever has narrowed from "CPU + bytes" to "bytes," but it's still
the single biggest deep-session scalability item, and still needs human sign-off
(durability-critical schema migration).

### Already shipped (orthogonal, keep regardless)
- **1 round-trip/turn** — upsert + prune folded into one data-modifying CTE.
- **Transient-failure retry** — idempotent upsert retried 3× with backoff; a DB
  blip no longer silently rewinds the conversation on resume.

These reduce *round-trips* and improve *durability*; they do NOT reduce the
per-turn write *volume*. This proposal targets the volume.

## Design: append-only deltas + periodic snapshots

Keep `native_transcripts` as the **snapshot** table (full transcript, written
every `K` turns) and add a **delta** table for per-turn appends:

```sql
CREATE TABLE native_transcript_deltas (
    session_id   TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
    turn_seq     INTEGER NOT NULL,
    new_messages JSONB NOT NULL,   -- ONLY the messages appended this turn
    usage        JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (session_id, turn_seq)
);
```

**Write per turn** (the common path — one small INSERT):
- Append a delta row carrying only the messages appended since the last
  checkpoint (the session already knows this boundary — it appends to
  `_messages` and can track a `_persisted_upto` index).
- Every `K` turns (e.g. 20) OR when the delta backlog exceeds a byte budget:
  write a full snapshot into `native_transcripts` and prune deltas with
  `turn_seq <= snapshot_turn` — one CTE, atomic.

Per-turn write volume drops to **O(new-messages-this-turn)**; the snapshot cost
amortizes to **O(n²/K)**. With K=20 that's a ~20× reduction in cumulative bytes,
tunable.

**Resume** (`read_native_checkpoint`): read the latest snapshot, then all delta
rows with `turn_seq > snapshot_turn` ordered by `turn_seq`, and concatenate:
`messages = snapshot.messages + concat(deltas.new_messages)`. One query with a
CTE/join; deltas are small.

## Correctness

- **Idempotent retry:** delta PK `(session_id, turn_seq)` → `ON CONFLICT DO
  UPDATE` dedups a retried turn. Snapshots keyed the same way. (Matches today's
  upsert contract.)
- **Crash atomicity:** a delta is one INSERT (atomic). A snapshot+prune is one
  CTE (atomic, same pattern as today). A crash between a delta and the next
  snapshot just leaves extra deltas — resume still reconstructs correctly.
- **Delta-tracking boundary:** the session tracks `_persisted_upto` (count of
  messages already in a delta/snapshot). On the heal/interrupt path the healed
  suffix must be re-derived against `_persisted_upto` so a stub inserted before
  a not-yet-persisted user message isn't double-counted. **This is the subtle
  part** and the main reason it needs careful review + tests.
- **Gap detection:** if delta `turn_seq` has a hole (a write was lost despite
  retry), resume must detect it (contiguity check) and fall back — better to
  resume from the snapshot + error than to silently replay a torn transcript.

## Cost / benefit — honest recommendation

**Benefit:** ~K× less cumulative checkpoint write volume (CPU + WAL + network)
for LONG, HIGH-CONTEXT sessions. Negligible for the common case (short/low-
context sessions already write a small transcript).

**Cost:** a two-table model, a resume-reconstruction query, session-side
delta-boundary tracking that must interact correctly with the dangling-tool-call
heal, gap detection, and a schema migration on a durability-critical table.

**Recommendation:** implement **only if** production telemetry shows native
sessions routinely running long at high context (the `op_events` + the
conversation-length distribution will tell — now visible thanks to the native
telemetry added in this branch). For today's workloads the shipped round-trip +
retry wins likely capture the practical value at a fraction of the risk. If
adopted, gate it behind a flag and dual-write for one release to validate resume
reconstruction against the snapshot path before cutting over.
