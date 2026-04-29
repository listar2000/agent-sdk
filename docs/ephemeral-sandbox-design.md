# Ephemeral sandbox design

Status: **proposal**, not yet implemented. Branch: `refactor/ephemeral-sandbox-session`.

This document is the canonical reference for the upcoming refactor that
collapses sandbox lifecycle, recovery logic, and streaming machinery into
a small set of deep modules. It supersedes `session-runtime-refactor.md`
once landed.

---

## 1. Goals

The user contract in one paragraph: a session is the agent's stable
handle; messages get streaming responses; conversation persists across
turns. **Sandbox is implementation detail and never appears in the
user-facing API.**

Reduce the complexity of the sandbox + recovery + streaming surface
without changing this contract. Criteria and targets are pinned in §2.

---

## 2. Success criteria & complexity targets

A change qualifies as "done" only if every row below is met. These are
the contractual asks; LoC is just one column among many.

### 2.1 Functional criteria (no regressions)

| Criterion | How to verify |
|-----------|---------------|
| `POST /sessions`, `POST /message`, `GET /events` semantics unchanged | Existing integration tests in `tests/test_sandbox_stop_delete_recovery.py` (14 cases × 4 providers) pass without modification |
| Multi-subscriber `GET /events` works | New test: two concurrent `GET /events` clients on the same session, both receive every event |
| Session resumes across server restart | New test: kill server mid-conversation, restart, send next message, assert reply + `inner_session_id` preserved |
| Session resumes across external sandbox stop | Existing `test_session_resume_after_stop[daytona]` passes |
| Session resumes across external sandbox delete | Existing `test_session_resume_after_delete[daytona]` passes |
| Conversation context preserved across recovery | Existing tests asserting `inner_session_id` unchanged across recovery still pass |
| `shared_mounts`/`dockerfile` survive any kind of recovery | New unit test: simulate every recovery path, assert recipe round-trips |

### 2.2 Non-functional criteria

| Criterion | Target | How to verify |
|-----------|--------|---------------|
| Warm-path message latency | ≤ 1.5× today's median | Benchmark: 100 sequential messages on warm session, p50 latency ≤ 1.5× baseline |
| Cold-path message latency (rare) | ≤ today's Type 2 latency (~30–60 s for daytona) | Benchmark: kill compute, send message, measure full turn time |
| 2× concurrent test suite | All 14 daytona tests pass in both runs (matches today's PR-#20 baseline) | Run `tests/test_sandbox_stop_delete_recovery.py -k daytona` twice in parallel |
| Test 7 race fixable | Either: passes both 2× runs, OR test asserts the contract directly and is unflakable by construction | Run test 7 ten times in a row under 2× load; zero flakes |
| Adding a new provider | One file (~150–200 LoC), one method-class implementation, one factory line | Tracked qualitatively per PR; reviewer enforces |

### 2.3 Bug-class elimination criteria

These are bugs that must become **structurally inexpressible** (cannot
be reintroduced without breaking the schema or a class invariant):

| Bug class | Why it becomes inexpressible |
|-----------|------------------------------|
| Hivespace `/mnt/<name>` data loss after sandbox replacement | Recipe lives on `sessions.sandbox_state.recipe`; never deleted by recovery |
| `_reader_connected` lying about supervisor liveness (test 7) | No persistent server↔supervisor connection; liveness is observed only when a prompt actually needs it |
| Daytona transitional-state classifier triggering destroy (PR #20) | Provider state classification is internal to `SandboxSession.start`; nothing else branches on it |
| Sandbox FK `ON DELETE SET NULL` race (Case B in `_ensure_sandbox_locked`) | No FK; state is JSONB on session row |
| `_INSTANCES` dict drifting from DB | No DB row for compute; pool is the only source of truth |

Verification: each bug class gets a regression test that exercises the
old failing pattern; tests must pass after the refactor and continue
passing without explicit defensive code paths.

### 2.4 Complexity targets (concrete numbers)

The LoC reduction targets, with provenance — these are the **commitments**
the refactor must hit, not aspirational guesses:

| Surface | Current LoC | Target LoC | Reduction | Threshold to pass |
|---------|-------------|------------|-----------|-------------------|
| `server.py` recovery functions (`_ensure_*`, `_type*_recover`, `_rebind_state`) | ~510 | ~80 (only `pool.get_session` + `pool.release`) | **−85%** | must be ≥−75% |
| `server.py` streaming + scheduler stack (`_sse_reader_*`, `_scheduler_loop`, subscriber multiplex) | ~410 | ~70 (per-prompt stream + fan-out) | **−83%** | must be ≥−70% |
| `server.py` total | ~4,750 | ≤ 2,400 | **−50%** | must be ≥−40% |
| `db.py` (sandboxes CRUD removal) | ~665 | ~580 (drop ~85 LoC of sandbox code) | −13% | must be ≥−10% |
| Per-provider total (`providers/{daytona,docker,local,modal}.py`) | ~2,700 | ~750 (one class per provider × 5 methods) | **−72%** | must be ≥−60% |
| New abstraction code (`sandbox/{__init__,pool,session,state,factory}.py` + per-provider `Session` impls) | 0 | ~900 | n/a (greenfield) | must be ≤1,000 |
| **Net change across all sandbox + recovery + streaming + provider code** | **~8,500** | **~3,300** | **−61%** | **must be ≥−50%** |

The 50% net-reduction floor is the contractual ask. If the refactor
lands and we're below 50% net reduction, we've added complexity for too
little benefit and should reconsider scope.

### 2.5 Mental-model criterion

A new engineer should be able to read the recovery surface in **under 30
minutes** and understand:

- Where the source of truth for "is there compute" lives (the pool)
- How a message gets a reply (POST /message → pool.get_session → execute_prompt)
- How recovery works (pool.get_session calls SandboxSession.start, which decides reattach vs create)
- How to add a new provider (subclass `BaseSandboxSession`, register in factory)

Today's recovery surface requires reading 4 functions across 800 LoC and
mentally simulating 5 layers of cached state. The 30-minute criterion is
the qualitative simplification target.

---

## 3. The mental model

Today: "session has-a sandbox; sandbox can break; recovery picks the
right repair."

After: "the user has a session. The system runs compute when the user is
talking, releases compute when they're not."

Compute is **ephemeral and fungible**. The only durable things are the
session row (which holds the recipe + snapshot pointer) and the snapshot
itself (on the volume). Sandboxes are leases — created on demand,
released when idle, never persisted.

---

## 4. Data model

### Durable (Postgres)

```sql
sessions                           -- user-facing handle, durable
  id                  uuid PK
  agent_id            uuid FK
  volume_id           uuid FK
  inner_session_id    text         -- ACP conversation pointer
  cwd                 text
  env                 jsonb        -- non-secret env
  secrets             jsonb        -- secrets (separate, redacted)
  sandbox_state       jsonb        -- *** the new column ***
  last_active_at      timestamptz

agents                             -- unchanged
volumes                            -- unchanged
session_log                        -- drop sandbox_id column

-- DROPPED tables/columns:
--   sandboxes (entire table)
--   sessions.current_sandbox_id (FK column)
--   session_log.sandbox_id (column)
```

### `sandbox_state` JSONB shape

The schema is owned by the SandboxSession code (pydantic), discriminated
by `type`. The DB doesn't know what's inside.

```python
class BaseSandboxState(BaseModel):
    type: str                         # discriminator: "daytona", "docker", "unix_local", "modal"
    snapshot_path: str | None = None  # path on the volume to last good agent_memory.tar
    snapshot_version: int = 0         # monotonic, bumped per snapshot write
    recipe: Recipe                    # provisioning identity

class Recipe(BaseModel):
    dockerfile: str | None = None
    shared_mounts: list[str] = []
    root: str = "/home/daytona"
    agent_type: str = "claude"
    pre_start_commands: list[str] = []

class DaytonaSandboxState(BaseSandboxState):
    type: Literal["daytona"] = "daytona"
    sandbox_id: str | None = None     # daytona's id; None until first start; mutated on cold path
    listen_port: int = 9100

# Similarly: DockerSandboxState, UnixLocalSandboxState, ModalSandboxState
```

### In-memory (transient, per-process)

```python
class SandboxSession:                 # one running compute, per session
    session_id: str
    state: BaseSandboxState           # mutable; mirrored to DB on change
    
    # Provider-side handles (set after start())
    supervisor_url: str | None = None
    acp_client: AcpClient | None = None
    acp_session_id: str | None = None
    
    # Single liveness oracle
    liveness: Liveness                # writer: SSE drain in execute_prompt
    
    # Multi-subscriber fan-out
    subscribers: dict[str, asyncio.Queue]
    
    # Per-session lock for serialised prompts
    # (held during execute_prompt; pool also takes it during get_session)

class SessionPool:
    _active: dict[str, SandboxSession]      # ONE per session_id
    _locks: dict[str, asyncio.Lock]         # ONE per session_id
```

That's it. All in-memory state. Server restart wipes it; DB is the truth
for what should be running.

---

## 5. Lifecycle methods

`SandboxSession` has exactly five methods:

```python
class BaseSandboxSession(abc.ABC):
    @abc.abstractmethod
    async def start(self) -> None:
        """Bring compute up, restore from snapshot if `state.snapshot_path`,
        attach ACP. Mutates state in place (e.g. fills sandbox_id on
        cold-create). Idempotent if already started.
        
        Decision tree (provider-internal, not exposed):
          - state has reusable id → daytona.get → if alive use; if stopped
            daytona.start; if missing → fall through to create
          - else → daytona.create + state.id = new
          - then → mount, supervisor boot, snapshot extract, ACP attach
        """
    
    @abc.abstractmethod
    async def running(self) -> bool:
        """Single liveness oracle. Probes via `liveness` first; if state is
        unknown, makes a cheap supervisor /health call (bounded ~2 s)."""
    
    @abc.abstractmethod
    async def execute_prompt(self, message: str) -> AsyncIterator[Event]:
        """Open an SSE stream from the supervisor for this one prompt;
        drain it; close it; broadcast each event to subscribers AND yield
        to the caller. Errors propagate as exceptions."""
    
    @abc.abstractmethod
    async def stop(self) -> None:
        """Write FULL filesystem snapshot to volume (update
        state.snapshot_path and state.snapshot_version), then call
        `daytona.stop()` (pause). Never deletes the sandbox; permanent
        delete is only triggered from `DELETE /sessions/{id}` or admin.
        Persists state to DB."""
    
    @abc.abstractmethod
    async def shutdown(self) -> None:
        """Final teardown of in-memory tasks. Doesn't touch the daytona
        side. Idempotent."""
```

`stop()` and `shutdown()` are split because:
- `stop()` is the data-preserving operation (snapshot + pause the compute)
- `shutdown()` is the in-memory cleanup (cancel tasks, drop subscribers)
- The pool calls `stop()` then `shutdown()` in sequence on `release()`
- A truly-dead session (compute crashed) gets `shutdown()` but skips `stop()`

---

## 6. The pool

The entire recovery surface, in one method:

```python
class SessionPool:
    async def get_session(self, session_id: str) -> SandboxSession:
        async with self._locks.setdefault(session_id, asyncio.Lock()):
            cached = self._active.get(session_id)
            if cached and await cached.running():
                return cached                              # warm path, ~10 ms
            
            if cached:
                # Stale entry; tear down runtime in background
                asyncio.create_task(cached.shutdown())
            
            state = await db.load_sandbox_state(session_id)
            session_cls = SESSION_CLASSES[state.type]
            session = session_cls(session_id=session_id, state=state)
            await session.start()                          # cold path, 5–60 s
            await db.save_sandbox_state(session_id, session.state)
            self._active[session_id] = session
            return session
    
    async def release(self, session_id: str) -> None:
        """Hibernate: snapshot + drop compute. Idempotent."""
        async with self._locks.setdefault(session_id, asyncio.Lock()):
            session = self._active.pop(session_id, None)
            if session is None:
                return
            try:
                await session.stop()
                await db.save_sandbox_state(session_id, session.state)
            finally:
                await session.shutdown()
```

That replaces:
- `_ensure_sandbox_alive` (~78 LoC)
- `_ensure_sandbox_locked` (~67 LoC)
- `_ensure_state_live` (~30 LoC)
- `_rebind_state` (~50 LoC)
- `_type1_recover` (~106 LoC)
- `_type2_recover` (~77 LoC)
- All `_INSTANCES` mutations
- `is_hibernated` flag plumbing

---

## 7. Streaming

### Two streams with different lifetimes

| Stream | Direction | Lifetime | Held by |
|--------|-----------|----------|---------|
| Supervisor → Server | upstream SSE | **per-prompt** (open at message, close at stopReason) | `execute_prompt` |
| Server → Client | downstream SSE | **persistent** (lives as long as the client holds it) | `GET /events` handler |

The server↔supervisor stream is what was historically persistent and the
source of most race conditions. Making it per-prompt eliminates:
- Stale URL races (today's test 7)
- Subscriber-on-dead-supervisor races
- `_reader_connected` flag and 5 reader sites
- SSE reader reconnect/backoff logic

### `execute_prompt` does the fan-out

```python
async def execute_prompt(self, message: str) -> AsyncIterator[Event]:
    async with self._supervisor_stream(message) as upstream:
        async for event in upstream:
            self.liveness.observe_chunk()
            self._broadcast(event)        # fan out to subscribers
            yield event                   # AND to caller
```

### Subscriber fan-out (kept for multi-subscriber)

```python
def subscribe(self) -> AsyncIterator[Event]:
    sid = str(uuid.uuid4())
    q = asyncio.Queue(maxsize=1024)
    self.subscribers[sid] = q
    try:
        while True:
            event = await q.get()
            if event is _SENTINEL_END:
                break
            yield event
    finally:
        self.subscribers.pop(sid, None)

def _broadcast(self, event):
    for q in list(self.subscribers.values()):
        try: q.put_nowait(event)
        except asyncio.QueueFull: pass    # slow subscriber, drop
```

Slow-subscriber-drops policy: if a subscriber's queue fills, drop the
event. Don't block the source supervisor stream on slow consumers. Fast
subscribers and the POST /message caller see all events.

### REST handlers

```python
@app.post("/sessions/{id}/message")
async def post_message(session_id: str, body: dict):
    async with _session_locks[session_id]:
        session = await _pool.get_session(session_id)
        async def gen():
            async for event in session.execute_prompt(body["message"]):
                yield f"data: {json.dumps(event)}\n\n"
        return StreamingResponse(gen(), media_type="text/event-stream")

@app.get("/sessions/{id}/events")
async def get_events(session_id: str):
    session = await _pool.get_session(session_id)   # may cold-start
    async def gen():
        async for event in session.subscribe():
            yield f"data: {json.dumps(event)}\n\n"
    return StreamingResponse(gen(), media_type="text/event-stream")
```

That's the entire HTTP layer for messages + events. ~15 LoC total.

### Per-session prompt serialisation

`session_locks[session_id]` around `pool.get_session + execute_prompt`
means concurrent POSTs to the same session block. This replaces today's
explicit scheduler queue + `pending_prompts` deque + `_prompt_done`
event signalling — same semantic, simpler implementation.

---

## 8. REST API surface

### User-facing

```
POST   /sessions                      create session row only (no compute provisioned)
GET    /sessions/{id}                 read state (incl. lifecycle: active/hibernated)
DELETE /sessions/{id}                 permanently delete
POST   /sessions/{id}/release         hibernate (snapshot + drop compute)
POST   /sessions/{id}/message         submit prompt → streams events
GET    /sessions/{id}/events          persistent multi-subscriber stream
POST   /agents                        agent CRUD
GET    /agents/{id}
POST   /volumes                       volume CRUD
GET    /volumes/{id}
GET    /health
```

### Admin / debug (separate namespace)

```
GET    /admin/sessions                pool inventory + per-session liveness
POST   /admin/sessions/{id}/wake      explicit pre-warm
GET    /admin/sandboxes               compute-level pool inventory (debug)
DELETE /admin/sandboxes/{ref}         force-kill by daytona ref (operator escape hatch)
```

### Removed / moved

| Endpoint today | Disposition |
|----------------|-------------|
| `POST /sandboxes` | gone (sandboxes are server-internal) |
| `GET /sandboxes/{id}` | moved to `/admin/sandboxes/{id}` |
| `DELETE /sandboxes/{id}` | moved to admin; user-facing replacement is `POST /sessions/{id}/release` |
| `POST /sessions/{id}/reset-sandbox` | gone (recovery is automatic via `pool.get_session`) |
| `POST /sessions/{id}/stop-sandbox` | replaced by `POST /sessions/{id}/release` |
| `POST /sessions/{id}/start-sandbox` | gone (first POST /message triggers it implicitly) |

### Response shape changes

| Field | Today | After |
|-------|-------|-------|
| `POST /sessions` response | `{session_id, agent_id, sandbox_id, current_sandbox_id, inner_session_id, connected}` | `{session_id, agent_id, lifecycle, inner_session_id, snapshot_version}` — no sandbox_id |
| `GET /sessions/{id}` | row data + sandbox info | `{id, lifecycle, last_active_at, snapshot_version, recipe}` |

`lifecycle ∈ {"active", "hibernated", "missing"}` — derived from
`pool._active.get(id)` and the session row.

---

## 9. Lifecycle walkthroughs

### A. New session, first message

1. `POST /sessions` → row created, `sandbox_state.sandbox_id = None`, no compute yet
2. `POST /message`:
   - `pool.get_session` → no cached → load state → no `sandbox_id` →
     `daytona.create` (30–60 s)
   - state mutated with new `sandbox_id`, persisted to DB
   - `execute_prompt` opens supervisor SSE → drains → closes
   - response streams to client

### B. Second message (warm path) — 99% of cases

1. `POST /message`:
   - `pool.get_session` → cached, `running() == True` → return (~10 ms)
   - `execute_prompt` opens supervisor SSE → drains → closes
   - response streams

**Per-message latency unchanged from today.**

### C. External stop (admin pauses sandbox during idle)

1. Outside actor calls `daytona.stop(sandbox)`. Server doesn't know yet.
2. (No active stream from server to supervisor, so no immediate observation.)
3. User sends next message:
   - `pool.get_session` → cached, but `running() → False` (probe times out)
   - Background `asyncio.create_task(cached.shutdown())`
   - Load state from DB → `daytona.get(sandbox_id)` → returns paused sandbox
   - `daytona.start(sandbox)` → resume from pause (5–15 s)
   - re-mint supervisor URL, attach ACP via session/load
   - `execute_prompt` → reply

**Recovery latency: 5–15 s.** User sees a brief delay then normal response.

### D. External delete

1. Outside actor calls `daytona.delete(sandbox)`. Server doesn't know.
2. User sends message:
   - `pool.get_session` → cached, `running() → False`
   - Load state → `daytona.get(sandbox_id)` → 404
   - Fall through: `daytona.create(...)` (30–60 s)
   - Extract snapshot from `state.snapshot_path` (5–15 s)
   - Mutate `state.sandbox_id = new_id`, persist
   - Attach ACP via session/load on the restored JSONL
   - `execute_prompt` → reply

**Recovery latency: 35–75 s.** Conversation context preserved via snapshot.

### E. Server restart mid-conversation

1. Server crashes. In-memory pool wiped. DB intact. Daytona sandbox keeps
   running (Daytona doesn't care about our server).
2. Server restarts; pool empty.
3. User sends message:
   - `pool.get_session` → no cached
   - Load state → `daytona.get(sandbox_id)` → exists, started
   - Re-mint supervisor URL (signed URL needs re-mint)
   - Attach ACP via session/load
   - `execute_prompt` → reply

**Recovery latency: ~3–5 s. Server restart is invisible.**

### F. Concurrent prompts on same session

1. `POST /message` #1 acquires `_session_locks[id]`, starts execute_prompt.
2. `POST /message` #2 arrives, blocks on the same lock.
3. #1 streams to completion, lock released.
4. #2 acquires lock, runs.

**Same serialisation guarantee as today's scheduler queue.**

### G. Hibernation (reaper or explicit `POST /release`)

1. `pool.release(session_id)` → take lock
2. `session.stop()`:
   - Write FULL filesystem snapshot to `/vol/snapshot.tar` (3–10 s)
   - Update `state.snapshot_path`, bump `snapshot_version`
   - `daytona.stop()` (pause — never deletes here)
3. Persist state to DB
4. `session.shutdown()` (cancel tasks, drop subscribers)
5. Pool entry removed

Next `POST /message` → cold path C (resume from pause, ~5-15 s).

### H. New message arrives during release (reaper races user)

1. Reaper takes `_session_locks[id]`, starts release.
2. User's POST arrives, blocks on the lock.
3. Release completes (snapshot done, `state.snapshot_path` updated).
4. POST acquires lock, calls `pool.get_session`:
   - Loads fresh state (with new snapshot_path)
   - Cold-starts session (daytona create or resume)
   - Snapshot just written is what gets restored
5. `execute_prompt` → reply

**No data loss.** Lock serialises release-then-acquire.

---

## 10. Error matrix

| Failure | Behavior |
|---------|----------|
| daytona API timeout in `start()` | `start()` raises; pool doesn't cache; next message retries from scratch |
| Snapshot extract fails (corrupt tarball) | `start()` boots fresh (no restore); inner_session_id is reset; conversation context lost (rare) |
| supervisor.js dies mid-prompt | Stream errors out; caller sees error event; subsequent message triggers fresh `start()` |
| Network blip mid-execute_prompt | Client must retry the prompt (we don't preserve in-flight state across server-side reconnects) |
| Daytona quota exceeded on create | `start()` raises; user gets HTTP 503 |
| Concurrent get + release race | Lock serialises; release wins; get re-runs after |
| Server crash mid-snapshot | Snapshot incomplete on /vol; previous snapshot still there. Next start uses previous (loses one turn) |
| Client connection drop on `GET /events` | Subscriber popped from session; other subscribers unaffected |
| Slow client subscriber | Events dropped on its queue; other subscribers and source unaffected |

---

## 11. What stays from today

- `supervisor.js` + ACP child (essential for streaming)
- Two-tier snapshot scheme (see §15.4): per-turn `agent_memory.tar` written by supervisor.js after every successful prompt; full FS `snapshot.tar` written on `release()`
- S3-FUSE volume mount as the non-POSIX bridge for snapshot durability
- Volume mount + `shared_mounts` on container start
- `acp_client.py` — JSON-RPC over HTTP/SSE
- `sse.py` — SSE parsing
- `db.py` — sandboxes CRUD removed (~85 LoC); rest unchanged
- `POST /sessions`, `POST /message`, `GET /events` external contract — zero user-facing change

---

## 12. What disappears

| Removed | LoC |
|---------|-----|
| `_type1_recover` | 106 |
| `_type2_recover` | 77 |
| `_ensure_sandbox_alive` | 78 |
| `_ensure_sandbox_locked` | 67 |
| `_ensure_state_live` + `_rebind_state` | 130 |
| `_INSTANCES` dict + 12 mutation sites | 50 |
| `_sandbox_locks` (only `_session_locks` remains) | 20 |
| `is_hibernated` flag plumbing | 30 |
| `_classify_prompt_error` sandbox-specific cases | 20 |
| `delete_sandbox_route` (replaced by 5-line release endpoint) | 25 |
| `sandboxes` table CRUD in `db.py` | 80 |
| `current_sandbox_id` FK plumbing in `server.py` | 40 |
| Persistent SSE reader task (`_sse_reader_*`, `_on_sse_reader_death`) | 175 |
| `_reader_connected` flag + 5 read sites | 30 |
| Scheduler task + `pending_prompts` + `_prompt_done` signalling | 110 |
| Last-Event-ID coordination | 20 |
| Per-provider recovery quirks across docker/local/modal/daytona | ~2,150 |
| **Total deletion** | **~3,108 LoC** |

| Added | LoC |
|-------|-----|
| `BaseSandboxSession` abstract + 5 method docs | 50 |
| `BaseSandboxState` + `Recipe` + per-provider state subclasses | 100 |
| `SessionPool` | 80 |
| `Liveness` | 50 |
| `DaytonaSandboxSession` | 200 |
| `DockerSandboxSession` | 120 |
| `UnixLocalSandboxSession` | 80 |
| `ModalSandboxSession` | 150 |
| Subscriber fan-out (in `BaseSandboxSession`) | 40 |
| Factory dispatch (`SESSION_CLASSES`) | 30 |
| **Total addition** | **~900 LoC** |

**Net: ~−2,200 LoC.** Roughly half of `server.py` deleted.

---

## 13. Bug classes that become structurally inexpressible

| Bug class today | Why it's gone after |
|-----------------|---------------------|
| `_type2_recover` and `_ensure_sandbox_locked` divergence on shared_mounts (hivespace `/mnt/7`) | Recipe lives on session, not on a row that gets deleted. There's no separate "Type 2" code path to forget to keep in sync. |
| `_reader_connected` lying about supervisor liveness (test 7 race) | No persistent server↔supervisor connection to be stale. `running()` probes at the moment a prompt needs to talk. |
| Daytona transitional-state classifier mapping `stopping` → `error` → destroy (PR #20 fix) | `daytona.stop()` is internal to `SandboxSession.stop()`. Nothing outside the session classifies provider state. |
| `_INSTANCES` cache vs DB row drift | No DB row. Pool is the only source of truth for "is there compute". |
| Sandbox FK `ON DELETE SET NULL` race with `_ensure_sandbox_locked` Case B | No FK to set null. State is JSONB on session row; either present or not. |
| Subscriber on dead supervisor URL | No subscriber-to-supervisor binding. Subscribers see events from `_broadcast` which only fires when `execute_prompt` is actually streaming from a live supervisor. |

---

## 14. Migration plan

Three phases, each independently shippable, none breaking the user contract.

### Phase 1 — Schema move (~1 day, 0 behavior change)

```sql
ALTER TABLE sessions ADD COLUMN sandbox_state JSONB;

-- Backfill from existing sandboxes:
UPDATE sessions s SET sandbox_state = jsonb_build_object(
  'type', (SELECT provider FROM sandboxes WHERE id = s.current_sandbox_id),
  'sandbox_id', (SELECT sandbox_ref FROM sandboxes WHERE id = s.current_sandbox_id),
  'recipe', jsonb_build_object(
      'dockerfile', (SELECT dockerfile FROM sandboxes WHERE id = s.current_sandbox_id),
      'shared_mounts', (SELECT shared_mounts FROM sandboxes WHERE id = s.current_sandbox_id),
      'root', (SELECT root FROM sandboxes WHERE id = s.current_sandbox_id),
      'agent_type', (SELECT a.config->>'agent_type' FROM agents a WHERE a.id = s.agent_id)
  )
);
```

Code: dual-write to both `sandbox_state` and `sandboxes` row. No reads change.

**Risk: zero.** Mergeable as a small PR.

### Phase 2 — Single recovery entry point (~3–5 days)

- Add `BaseSandboxSession`, per-provider concrete classes
- Add `SessionPool`, `Liveness`
- Rewrite `_ensure_state_live`, `_ensure_sandbox_alive`, `_ensure_sandbox_locked`,
  `_rebind_state` to be one-line wrappers around `pool.get_session()`
- `_type1_recover` and `_type2_recover` become two strategies inside
  `DaytonaSandboxSession.start()`
- Add per-prompt supervisor SSE in `execute_prompt`; remove `_sse_reader_task`
- Replace scheduler queue with session_locks + inline streaming

**Critical commit in this phase**: liveness oracle replaces `_reader_connected`
in the fast-path. Closes test 7's race class.

**Risk: medium.** Many internal moving parts; tests should pass unchanged
because the API surface is identical. Keep dual-write to `sandboxes` row
as a safety net during this phase.

### Phase 3 — Drop the table (~1 day, mostly deletion)

```sql
ALTER TABLE sessions DROP COLUMN current_sandbox_id;
ALTER TABLE session_log DROP COLUMN sandbox_id;
DROP TABLE sandboxes;
```

- Delete `upsert_sandbox`, `get_sandbox`, `delete_sandbox`, `_row_to_sandbox`
- Delete `_INSTANCES` plumbing
- Delete `is_hibernated` flag plumbing
- Delete the now-unused `_ensure_sandbox_locked` shim
- Move `/sandboxes` endpoints to `/admin` namespace
- Update API responses to drop `sandbox_id` field

**Risk: low.** Mechanical. The compatibility shim during phase 2 (the
`sandboxes` row dual-write) means external consumers reading the
`sandbox_id` response field continue to work through phase 2; they break
on phase 3. Phase 3 must be paired with an SDK release that updates
client code to use `session_id` only.

---

## 15. Resolved decisions

All design questions are answered. No open trades.

### 15.1 Architecture

| Decision | Choice |
|----------|--------|
| Single active session per session_id, no parallel prompts within a session | YES — serialised by `_session_locks[id]`. Replaces today's scheduler queue. |
| Leases are in-memory only; server restart wipes pool; DB state is the truth | YES — restart is invisible (walkthrough §9 case E). |
| Drop Type 1 (warm-restart same container) as a separate code path | YES — `start()` is the one path; reattach-when-possible falls out naturally inside `start()` without explicit branching. |
| Per-prompt supervisor SSE (no persistent connection between prompts) | YES — opens at message, closes at stopReason. Closes test 7 race class. |
| Multi-subscriber `GET /events` preserved | YES — in-memory fan-out (~40 LoC) inside `SandboxSession`. |
| Drop user-facing `/sandboxes` endpoints; sandboxes are implementation detail | YES — admin-only namespace retains them for operator use. |
| Sensitive secrets stay on `sessions.secrets` column, not in `sandbox_state` JSONB | YES — separation of concerns (logging redaction). |
| `GET /events` late joiners only see events from subscribe-time onward | YES — past events come from `session_log`, separate concern. |

### 15.2 Multi-process safety (Q1)

**Decision: row-level DB lock.** `pool.get_session` reads `sandbox_state`
under `SELECT ... FOR UPDATE` so two processes serving the same session
serialise on the row.

```sql
-- inside get_session, replacing db.load_sandbox_state:
BEGIN;
SELECT sandbox_state FROM sessions WHERE id = $1 FOR UPDATE;
-- ... start session, mutate state ...
UPDATE sessions SET sandbox_state = $2 WHERE id = $1;
COMMIT;
```

Cost: ~20 LoC, one extra round-trip per cold path (<10 ms). Worth it
for correctness in multi-process deployments.

### 15.3 Compute release policy (Q2)

**Decision: always pause.** `SandboxSession.stop()` always calls
`daytona.stop()`, never `daytona.delete()`. Resume on next message is
fast (5–15 s); idle-period cost is paid as Daytona's pause-storage rate
(cheap relative to running compute).

Implication: `state.sandbox_id` persists across pause/resume cycles. We
only call `daytona.delete()` from `DELETE /sessions/{id}` (explicit
permanent delete) or from operator action via `/admin/sandboxes`.

### 15.4 Snapshot durability (Q3)

**Decision: two-tier snapshot scheme.**

| Tier | Trigger | Contents | Cost |
|------|---------|----------|------|
| **Memory snapshot** | per-turn, in supervisor.js, after every successful `session/prompt` | ACP conversation JSONL only (small, KB) | ~100 ms tar+write, in background after stopReason — invisible to user |
| **Filesystem snapshot** | on `release()` only | full workspace tarball (potentially MB) | ~3–10 s; release is non-interactive so latency is fine |

Cold-path restore order:
1. `start()` extracts the latest **filesystem** snapshot (full state as of last release).
2. Then overlays the latest **memory** snapshot (catches up to the most
   recent turn).

Loss bound: at most one in-flight turn (the one mid-prompt when the
supervisor crashed).

This is more sophisticated than a single-tier scheme: we get
turn-granularity conversation recovery without paying full-FS-tar cost
every turn.

### 15.5 Subscriber backpressure (Q4)

**Decision: drop events on `QueueFull`** — keep today's behaviour.
Slow subscriber loses some events; other subscribers and source stream
unaffected.

Code:
```python
def _broadcast(self, event):
    for q in list(self.subscribers.values()):
        try: q.put_nowait(event)
        except asyncio.QueueFull: pass
```

Event-order preservation is explicitly not a guarantee. Subscribers
that need it must use a different mechanism (e.g. polling the
`session_log` table by sequence number).

---

## 16. Glossary (for the implementation)

Following the project's `LANGUAGE.md` conventions:

- **Module** — the `SandboxSession` class is one module; `SessionPool` is another;
  the per-provider concrete classes are modules at the seam.
- **Interface** — `SandboxSession`'s 5 methods + their invariants (idempotency,
  what `state` looks like before/after).
- **Seam** — the abstract `BaseSandboxSession` is the seam; concrete provider
  classes are adapters at it.
- **Adapter** — `DaytonaSandboxSession`, `DockerSandboxSession`, etc.
- **Depth** — the pool is deep (small interface: `get_session(id) → Session`,
  large implementation: cache + locking + cold-start + snapshot restore).
- **Locality** — recovery, lifecycle, and snapshot policy all concentrate
  inside `BaseSandboxSession.start/stop` and `SessionPool.get_session/release`.
  No more scattering across `_ensure_*`, `_type*_recover`, `_INSTANCES`,
  `_reader_connected`.
