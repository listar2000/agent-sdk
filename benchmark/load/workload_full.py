"""End-to-end workload that exercises the full agent-sdk server surface.

What this hits (each "iteration" runs the whole thing per session):

  Session lifecycle:
    POST /sessions (eager, with config)
    GET  /sessions/{id}/status
    GET  /sessions/{id}/sandbox

  Multi-turn conversation:
    POST /sessions/{id}/message+stream  (N_TURNS times, drained as SSE)

  Session-scoped file ops (every one of these is a /v1/* proxy through
  the supervisor — the hot path for the httpx-share win):
    GET  /sessions/{id}/files/tree
    POST /sessions/{id}/files/edit
    GET  /sessions/{id}/files/read
    POST /sessions/{id}/files/upload  (small + large payloads)
    GET  /sessions/{id}/files/download

  Sandbox exec:
    POST /sessions/{id}/sandbox/exec  ('uname -a', tiny)

  ACP config calls (per-session AcpClient cache should kick in):
    POST /sessions/{id}/config  (set model)
    POST /sessions/{id}/config  (set mode)
    POST /sessions/{id}/config  (set thought_level)

  Hibernate + resume cycle:
    POST /sessions/{id}/release
    POST /sessions/{id}/resume

  Cleanup:
    DELETE /sessions/{id}

Knobs (env vars):
  API           default http://localhost:7778
  PROVIDER      unix_local | daytona  (default unix_local)
  N_SESSIONS    concurrent sessions (default 5)
  N_TURNS       chat turns per session (default 2)
  N_FILE_OPS    repetitions of the file-op block per session (default 5)
  PROMPT        prompt text (default short)
  LARGE_MB      size of large upload in MB (default 2 — exercises b64 to_thread)
  MODEL         model alias (default haiku)
  SKIP_RELEASE  set to 1 to skip the release/resume cycle (Daytona snapshot is slow)
  REPORT        path to write JSON line summary (default /tmp/workload_full.jsonl)

Opt-in scenarios (default off so baseline A/B numbers stay reproducible):
  BENCH_SSE_SUBSCRIBER=1        open a parallel GET /events during a prompt;
                                tests subscriber fan-out + heartbeats
  BENCH_CONCURRENT_PROMPTS=N    after the sequential turns, fire N prompts
                                concurrently on the same session;
                                tests _prompt_lock queueing
  BENCH_LONG_TURNS=N            do N additional simple turns to test JSONL
                                growth + supervisor stability
  BENCH_TOOL_HEAVY=1            one prompt that forces multiple Bash tool
                                calls (3 commands); measures tool-use shape
  BENCH_CANCEL=1                fire a prompt, send POST /cancel mid-stream;
                                tests cancellation propagation through ACP
  BENCH_INTERRUPT=1             fire a prompt, then a second with
                                interrupt=true; tests in-flight supersede
  BENCH_MULTI_SESSION_PER_AGENT=N
                                used by `multi_session_per_agent` scenario:
                                create N sessions sharing one agent_id;
                                exercises shared HOME/JSONL invariants
                                (Daytona enforces a 409 sibling-rejection
                                contract; the test asserts that)
  BENCH_GIGANTIC_MB=N           used by `gigantic_files` scenario: payload
                                size in MB for the big upload/read/edit probe
  BENCH_SCENARIO=name           top-level scenario shape (see SCENARIOS dict
                                below for the full list — default, bursty,
                                long_chat, mixed, volume_direct,
                                multi_session_per_agent, gigantic_files,
                                stress)

Reports per-op median + p99 latency, aggregate throughput, and error count.
Designed for head-to-head A/B: run twice (baseline vs patched server),
diff the outputs.
"""
from __future__ import annotations

import asyncio
import base64
import json
import os
import statistics
import sys
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass, field

import httpx

API = os.environ.get("API", "http://localhost:7778")
PROVIDER = os.environ.get("PROVIDER", "unix_local")
N_SESSIONS = int(os.environ.get("N_SESSIONS", "5"))
N_TURNS = int(os.environ.get("N_TURNS", "2"))
N_FILE_OPS = int(os.environ.get("N_FILE_OPS", "5"))
PROMPT = os.environ.get("PROMPT", "Reply with exactly the single word: OK")
LARGE_MB = int(os.environ.get("LARGE_MB", "2"))
MODEL = os.environ.get("MODEL", "haiku")
SKIP_RELEASE = os.environ.get("SKIP_RELEASE", "0") == "1"
REPORT = os.environ.get("REPORT", "/tmp/workload_full.jsonl")
LABEL = os.environ.get("LABEL", "run")

# Opt-in scenario knobs (default 0/off so baseline A/B stays comparable).
BENCH_SSE_SUBSCRIBER = os.environ.get("BENCH_SSE_SUBSCRIBER", "0") == "1"
BENCH_CONCURRENT_PROMPTS = int(os.environ.get("BENCH_CONCURRENT_PROMPTS", "0"))
BENCH_LONG_TURNS = int(os.environ.get("BENCH_LONG_TURNS", "0"))
BENCH_TOOL_HEAVY = os.environ.get("BENCH_TOOL_HEAVY", "0") == "1"
BENCH_CANCEL = os.environ.get("BENCH_CANCEL", "0") == "1"
BENCH_INTERRUPT = os.environ.get("BENCH_INTERRUPT", "0") == "1"
BENCH_MULTI_SESSION_PER_AGENT = int(
    os.environ.get("BENCH_MULTI_SESSION_PER_AGENT", "0")
)
BENCH_SCENARIO = os.environ.get("BENCH_SCENARIO", "default")
# Gigantic-upload scenario: upload a single big file via the session-scoped
# proxy while a SECOND HTTP connection hammers /files/tree on the same
# session. The probe's latency p99 directly measures the cost of
# inline b64 (loop-blocked) vs to_thread b64 (loop-free).
BENCH_GIGANTIC_MB = int(os.environ.get("BENCH_GIGANTIC_MB", "0"))

TOOL_HEAVY_PROMPT = (
    "Run these three shell commands in order and report each command's "
    "output verbatim:\n"
    "1. uname -a\n"
    "2. pwd\n"
    "3. echo TOOL_HEAVY_DONE\n"
    "Use the Bash tool. Reply with the three outputs."
)


@dataclass
class Op:
    """One observed operation: name + duration ms + ok flag."""
    name: str
    ms: float
    ok: bool = True


@dataclass
class SessionResult:
    session_id: str = ""
    ops: list[Op] = field(default_factory=list)
    err: str | None = None


def now() -> float:
    return time.perf_counter()


@asynccontextmanager
async def timed(results: list[Op], name: str):
    """Context manager: time an op, append to results."""
    t0 = now()
    ok = True
    try:
        yield
    except Exception:
        ok = False
        raise
    finally:
        results.append(Op(name=name, ms=(now() - t0) * 1000, ok=ok))


async def drain_sse(c: httpx.AsyncClient, url: str, json_body: dict, timeout: float = 120) -> int:
    """Drain an SSE stream from POST /message+stream until done/error.
    Returns chunk count."""
    chunks = 0
    async with c.stream("POST", url, json=json_body, timeout=timeout) as r:
        r.raise_for_status()
        async for line in r.aiter_lines():
            if line.startswith("data:") or line.startswith("event:"):
                chunks += 1
    return chunks


async def drain_events_until(
    c: httpx.AsyncClient, sid: str, deadline: float, timeout: float = 120,
) -> tuple[int, int]:
    """Subscribe to GET /sessions/{id}/events until ``deadline`` (perf_counter).
    Returns (chunks, heartbeats). Counts chunk lines and ': heartbeat' lines.
    Used to measure SSE subscriber fan-out cost while a producer is running.
    """
    chunks = 0
    heartbeats = 0
    try:
        async with c.stream("GET", f"{API}/sessions/{sid}/events", timeout=timeout) as r:
            r.raise_for_status()
            async for line in r.aiter_lines():
                if now() > deadline:
                    return chunks, heartbeats
                if line.startswith(": heartbeat"):
                    heartbeats += 1
                elif line.startswith("data:") or line.startswith("event:"):
                    chunks += 1
    except (httpx.ReadError, httpx.RemoteProtocolError, asyncio.CancelledError):
        pass
    return chunks, heartbeats


async def fire_message_async(
    c: httpx.AsyncClient, sid: str, message: str, *, interrupt: bool = False,
    timeout: float = 120,
) -> str:
    """Submit a prompt via POST /message (the fire-and-forget endpoint).
    Returns rpc_id. Used by concurrent / interrupt / cancel scenarios where
    we need to fire a prompt without draining its SSE inline."""
    body: dict = {"message": message}
    if interrupt:
        body["interrupt"] = True
    r = await c.post(f"{API}/sessions/{sid}/message", json=body, timeout=timeout)
    r.raise_for_status()
    return r.json()["rpc_id"]


def _session_body(name: str, *, agent_id: str | None = None, with_secrets: bool = True) -> dict:
    body: dict = {
        "name": name,
        "provider": PROVIDER,
        "config": {"agent_type": "claude", "model": MODEL},
    }
    if with_secrets:
        body["secrets"] = {
            "CLAUDE_CODE_OAUTH_TOKEN": os.environ.get("CLAUDE_CODE_OAUTH_TOKEN", ""),
        }
    if agent_id:
        body["agent_id"] = agent_id
    return body


async def create_session(
    c: httpx.AsyncClient, name: str, *, agent_id: str | None = None,
    with_secrets: bool = True, timeout: float = 120,
) -> dict:
    """POST /sessions, return JSON body. Raises on non-2xx."""
    r = await c.post(
        f"{API}/sessions",
        json=_session_body(name, agent_id=agent_id, with_secrets=with_secrets),
        timeout=timeout,
    )
    r.raise_for_status()
    return r.json()


async def safe_delete_session(c: httpx.AsyncClient, sid: str, timeout: float = 120) -> None:
    """DELETE /sessions/{id} swallowing exceptions — for `finally` cleanup."""
    try:
        await c.delete(f"{API}/sessions/{sid}", timeout=timeout)
    except Exception:
        pass


async def run_session(c: httpx.AsyncClient, idx: int) -> SessionResult:
    """Full per-session workflow."""
    res = SessionResult()
    try:
        # ---- create ----
        async with timed(res.ops, "session_create"):
            sid = (await create_session(c, f"bench-{idx}"))["session_id"]
            res.session_id = sid

        # ---- status / sandbox metadata reads ----
        async with timed(res.ops, "session_status"):
            r = await c.get(f"{API}/sessions/{sid}/status", timeout=30)
            r.raise_for_status()
        async with timed(res.ops, "session_sandbox_info"):
            r = await c.get(f"{API}/sessions/{sid}/sandbox", timeout=30)
            r.raise_for_status()

        # ---- ACP config (3 calls — exercises cached AcpClient) ----
        for cfg, val in (("model", MODEL), ("mode", "bypassPermissions"), ("thought_level", "low")):
            async with timed(res.ops, f"config_{cfg}"):
                r = await c.post(f"{API}/sessions/{sid}/config",
                                 json={cfg: val}, timeout=30)
                # 200 expected; some agents may reject thought_level/mode
                # — non-fatal for the bench
                if r.status_code >= 500:
                    r.raise_for_status()

        # ---- multi-turn chat ----
        for _ in range(N_TURNS):
            async with timed(res.ops, "prompt_turn"):
                await drain_sse(
                    c, f"{API}/sessions/{sid}/message+stream",
                    {"message": PROMPT}, timeout=120,
                )

        # ---- session-scoped file ops (the httpx-share hot path) ----
        # Repeat several times so connection-reuse benefit shows up.
        small_b64 = base64.b64encode(b"hello world\n" * 32).decode()
        large_b64 = base64.b64encode(os.urandom(LARGE_MB * 1024 * 1024)).decode() if LARGE_MB > 0 else None

        for k in range(N_FILE_OPS):
            async with timed(res.ops, "files_tree"):
                r = await c.get(f"{API}/sessions/{sid}/files/tree", timeout=30)
                if r.status_code >= 500:
                    r.raise_for_status()

            async with timed(res.ops, "files_upload_small"):
                r = await c.post(f"{API}/sessions/{sid}/files/upload",
                                 json={"path": f"bench/small_{k}.txt", "content": small_b64},
                                 timeout=30)
                if r.status_code >= 500:
                    r.raise_for_status()

            async with timed(res.ops, "files_read"):
                r = await c.get(f"{API}/sessions/{sid}/files/read",
                                params={"path": f"bench/small_{k}.txt"}, timeout=30)
                if r.status_code >= 500:
                    r.raise_for_status()

            if large_b64:
                async with timed(res.ops, "files_upload_large"):
                    r = await c.post(f"{API}/sessions/{sid}/files/upload",
                                     json={"path": f"bench/large_{k}.bin", "content": large_b64},
                                     timeout=120)
                    if r.status_code >= 500:
                        r.raise_for_status()

        # ---- sandbox exec ----
        async with timed(res.ops, "sandbox_exec"):
            r = await c.post(f"{API}/sessions/{sid}/sandbox/exec",
                             json={"command": "uname -a", "timeout": 10}, timeout=30)
            if r.status_code >= 500:
                r.raise_for_status()

        # ---- (opt-in) SSE subscriber: POST /message AND parallel GET /events
        # measures the subscriber fan-out cost from a second connection while
        # a producer is in flight. Times: prompt total, /events chunks observed
        # by the parallel subscriber, /events heartbeat count during idle.
        if BENCH_SSE_SUBSCRIBER:
            async with timed(res.ops, "sse_subscriber_prompt"):
                deadline = now() + 60
                # Start subscriber first so it doesn't miss the prompt's chunks
                sub_task = asyncio.create_task(
                    drain_events_until(c, sid, deadline)
                )
                await asyncio.sleep(0.05)
                await drain_sse(
                    c, f"{API}/sessions/{sid}/message+stream",
                    {"message": PROMPT}, timeout=120,
                )
                # drain_events_until catches CancelledError internally and
                # returns its accumulated counts, so cancel() + await yields
                # the real observation (not the 0-fallback).
                sub_task.cancel()
                try:
                    sub_chunks, sub_hbs = await sub_task
                except Exception:
                    sub_chunks, sub_hbs = 0, 0
            res.ops.append(Op(name="sse_subscriber_chunks", ms=float(sub_chunks)))
            res.ops.append(Op(name="sse_subscriber_heartbeats", ms=float(sub_hbs)))

        # ---- (opt-in) Concurrent prompts on the same session ----
        # Fire N prompts in parallel; the server's per-session _prompt_lock
        # serializes them. Time the WHOLE batch. With N=3 and ~1.3s/prompt,
        # expect ~4s total wall (vs <2s if they ran in parallel).
        if BENCH_CONCURRENT_PROMPTS > 0:
            n = BENCH_CONCURRENT_PROMPTS
            async with timed(res.ops, f"concurrent_prompts_x{n}"):
                results_ = await asyncio.gather(
                    *[
                        drain_sse(
                            c, f"{API}/sessions/{sid}/message+stream",
                            {"message": PROMPT}, timeout=120,
                        ) for _ in range(n)
                    ],
                    return_exceptions=True,
                )
                ok = sum(1 for x in results_ if not isinstance(x, Exception))
            res.ops.append(Op(name="concurrent_prompts_ok",
                              ms=float(ok), ok=(ok == n)))

        # ---- (opt-in) Cancel mid-prompt ----
        # Submit a real prompt via /message, wait briefly, then POST /cancel.
        # The cancellation should land before the response completes.
        if BENCH_CANCEL:
            async with timed(res.ops, "cancel_mid_prompt"):
                await fire_message_async(c, sid, PROMPT)
                await asyncio.sleep(0.2)
                r = await c.post(f"{API}/sessions/{sid}/cancel", timeout=30)
                r.raise_for_status()

        # ---- (opt-in) Interrupt: prompt + supersede ----
        # /message+stream's `interrupt` flag is a no-op (server.py post_session_message_stream
        # docstring), so we use the fire-and-forget /message endpoint twice
        # then drain a third /message+stream that has to queue behind the
        # supersede. Wall time captures: submit #1 → submit #2 with interrupt
        # → cancel of #1 + run of #2 + ack of #3.
        if BENCH_INTERRUPT:
            async with timed(res.ops, "interrupt_supersede"):
                await fire_message_async(c, sid, PROMPT)
                await asyncio.sleep(0.1)
                await fire_message_async(c, sid, PROMPT, interrupt=True)
                await drain_sse(
                    c, f"{API}/sessions/{sid}/message+stream",
                    {"message": PROMPT}, timeout=120,
                )

        # ---- (opt-in) Tool-heavy turn ----
        # Real claude-code shape: agent uses Bash 3 times, then summarizes.
        # Latency is dominated by 3 round-trips through the tool-call /
        # tool-result loop, not just one Anthropic round trip.
        if BENCH_TOOL_HEAVY:
            async with timed(res.ops, "tool_heavy_turn"):
                await drain_sse(
                    c, f"{API}/sessions/{sid}/message+stream",
                    {"message": TOOL_HEAVY_PROMPT}, timeout=180,
                )

        # ---- (opt-in) Long conversation ----
        # N additional simple turns to exercise JSONL growth, supervisor
        # memory, and per-session prompt_lock under sustained load.
        for _ in range(BENCH_LONG_TURNS):
            async with timed(res.ops, "long_turn"):
                await drain_sse(
                    c, f"{API}/sessions/{sid}/message+stream",
                    {"message": PROMPT}, timeout=120,
                )

        # ---- hibernate + resume ----
        if not SKIP_RELEASE:
            async with timed(res.ops, "release"):
                r = await c.post(f"{API}/sessions/{sid}/release", timeout=120)
                r.raise_for_status()
            async with timed(res.ops, "resume"):
                r = await c.post(f"{API}/sessions/{sid}/resume", timeout=120)
                r.raise_for_status()
            # one more prompt so we actually use the resumed session
            async with timed(res.ops, "prompt_after_resume"):
                await drain_sse(
                    c, f"{API}/sessions/{sid}/message+stream",
                    {"message": PROMPT}, timeout=120,
                )

    except Exception as e:
        res.err = f"{type(e).__name__}: {e}"
    finally:
        if res.session_id:
            await safe_delete_session(c, res.session_id)
    return res


async def run_volume_direct_ops(c: httpx.AsyncClient, idx: int) -> SessionResult:
    """Exercise /volumes/{id}/files/* — a separate code path from the
    session-scoped file proxy. Used by the Volume Inspector UI when no
    session is active. No sandbox is provisioned; cheap & fast.
    """
    res = SessionResult()
    name = f"bench-vol-{idx}-{int(now() * 1000)}"
    vol_id: str | None = None
    try:
        async with timed(res.ops, "volume_create"):
            r = await c.post(f"{API}/volumes",
                             json={"name": name, "provider": PROVIDER}, timeout=60)
            r.raise_for_status()
            vol_id = r.json()["id"]
        res.session_id = vol_id

        async with timed(res.ops, "volume_get"):
            r = await c.get(f"{API}/volumes/{vol_id}", timeout=10)
            r.raise_for_status()

        async with timed(res.ops, "volume_list"):
            r = await c.get(f"{API}/volumes", timeout=10)
            r.raise_for_status()

        small = base64.b64encode(b"vol-direct-bench\n" * 4).decode()
        for k in range(N_FILE_OPS):
            async with timed(res.ops, "vol_files_upload"):
                r = await c.post(
                    f"{API}/volumes/{vol_id}/files/upload",
                    json={"path": f"vd/{k}.txt", "content": small}, timeout=30,
                )
                if r.status_code >= 500:
                    r.raise_for_status()
            async with timed(res.ops, "vol_files_exists"):
                r = await c.get(
                    f"{API}/volumes/{vol_id}/files/exists",
                    params={"path": f"vd/{k}.txt"}, timeout=10,
                )
                if r.status_code >= 500:
                    r.raise_for_status()
            async with timed(res.ops, "vol_files_read"):
                r = await c.get(
                    f"{API}/volumes/{vol_id}/files/read",
                    params={"path": f"vd/{k}.txt"}, timeout=10,
                )
                if r.status_code >= 500:
                    r.raise_for_status()
            async with timed(res.ops, "vol_files_tree"):
                r = await c.get(
                    f"{API}/volumes/{vol_id}/files/tree",
                    params={"path": "vd"}, timeout=10,
                )
                if r.status_code >= 500:
                    r.raise_for_status()
    except Exception as e:
        res.err = f"{type(e).__name__}: {e}"
    finally:
        if vol_id:
            try:
                await c.delete(f"{API}/volumes/{vol_id}", params={"force": "true"}, timeout=60)
            except Exception:
                pass
    return res


async def run_multi_session_per_agent(c: httpx.AsyncClient, idx: int) -> SessionResult:
    """N sessions sharing one agent_id. Exercises the multi-session-per-
    agent invariants (shared HOME, JSONL store, rejection on Daytona).
    Each session does a minimal handshake + 1 turn so we can compare
    cost vs a fresh-agent session.
    """
    res = SessionResult()
    n = max(1, BENCH_MULTI_SESSION_PER_AGENT)
    agent_id: str | None = None
    sids: list[str] = []
    try:
        # First session creates the agent (server mints one when not provided)
        async with timed(res.ops, "msa_session1_create"):
            data = await create_session(c, f"msa-{idx}-1")
            sid = data["session_id"]
            agent_id = data["agent_id"]
            sids.append(sid)
            res.session_id = sid

        # Daytona rejects sibling sessions; record the 409 and stop
        if PROVIDER == "daytona":
            async with timed(res.ops, "msa_sibling_409"):
                r = await c.post(
                    f"{API}/sessions",
                    json=_session_body(f"msa-{idx}-2", agent_id=agent_id, with_secrets=False),
                    timeout=60,
                )
                if r.status_code != 409:
                    raise RuntimeError(f"expected 409 on Daytona sibling, got {r.status_code}")
            return res

        # Other providers: create N-1 more siblings concurrently to actually
        # stress the cold-attach race on the shared agent supervisor.
        async def _create_sibling(i: int) -> str:
            async with timed(res.ops, "msa_sibling_create"):
                d = await create_session(c, f"msa-{idx}-{i}", agent_id=agent_id)
                return d["session_id"]

        sibling_ids = await asyncio.gather(
            *[_create_sibling(i) for i in range(2, n + 1)]
        )
        sids.extend(sibling_ids)

        # Prompts in parallel: exercises the shared-JSONL write race
        # (per-session _prompt_lock is per-session, not per-agent).
        async def _msa_prompt(s: str) -> None:
            async with timed(res.ops, "msa_prompt"):
                await drain_sse(
                    c, f"{API}/sessions/{s}/message+stream",
                    {"message": PROMPT}, timeout=120,
                )

        await asyncio.gather(*[_msa_prompt(s) for s in sids])
    except Exception as e:
        res.err = f"{type(e).__name__}: {e}"
    finally:
        await asyncio.gather(*[safe_delete_session(c, s) for s in sids])
    return res


async def run_gigantic_files(c: httpx.AsyncClient, idx: int) -> SessionResult:
    """Exercise EVERY big-payload file path against a configurable size
    (default 100 MB via BENCH_GIGANTIC_MB), each phase paired with a
    concurrent latency probe so we can SEE whether the big op blocks
    other requests on the event loop.

    Phases (each phase emits 2 ops: ``<phase>_ms`` and ``probe_during_<phase>_ms``):
      1. upload    — POST /sessions/{id}/files/upload (b64 decode path)
      2. read      — GET  /sessions/{id}/files/read   (b64 encode path)
      3. download  — GET  /sessions/{id}/files/download (raw stream)
      4. edit      — POST /sessions/{id}/files/edit (search/replace, reads + writes)

    The probe runs on a SECOND httpx.AsyncClient (separate connection
    pool) so it competes with the big op only for server-side event-loop
    time, not for client-side connection slots. probe_during_<phase>_ms
    p99 is the headline isolation metric.
    """
    res = SessionResult()
    if BENCH_GIGANTIC_MB <= 0:
        res.err = "BENCH_GIGANTIC_MB must be > 0"
        return res

    size = BENCH_GIGANTIC_MB * 1024 * 1024
    # Generate raw payload + b64 ONCE (CPU we don't want timed inside the request)
    raw = os.urandom(size)
    payload_b64 = base64.b64encode(raw).decode()
    payload_path = f"giant/{idx}.bin"

    sid: str | None = None
    try:
        async with timed(res.ops, "gf_session_create"):
            sid = (await create_session(c, f"gigantic-{idx}", timeout=300))["session_id"]
            res.session_id = sid

        # ------ phase 1: upload ------
        await _gigantic_phase(
            res, sid, "upload",
            big_op=_giant_upload(c, sid, payload_path, payload_b64),
        )

        # ------ phase 2: read (server b64-encodes the binary back) ------
        await _gigantic_phase(
            res, sid, "read",
            big_op=_giant_read(c, sid, payload_path),
        )

        # ------ phase 3: download (raw bytes, no b64 — control case) ------
        await _gigantic_phase(
            res, sid, "download",
            big_op=_giant_download(c, sid, payload_path),
        )

        # ------ phase 4: edit (search-replace; reads then writes) ------
        # Use a chunk that's actually in the random data is unlikely; instead,
        # write a known marker file first, then search-replace within it.
        marker_path = f"giant/{idx}.txt"
        marker_size = max(1, BENCH_GIGANTIC_MB) * 1024 * 1024  # MB of "AAAA..." text
        marker_text = ("A" * 1024 + "\n") * (marker_size // 1025)
        marker_b64 = base64.b64encode(marker_text.encode()).decode()
        async with timed(res.ops, "gf_edit_setup"):
            r = await c.post(f"{API}/sessions/{sid}/files/upload",
                             json={"path": marker_path, "content": marker_b64},
                             timeout=300)
            r.raise_for_status()
        await _gigantic_phase(
            res, sid, "edit",
            big_op=_giant_edit(c, sid, marker_path),
        )
    except Exception as e:
        res.err = f"{type(e).__name__}: {e}"
    finally:
        if sid:
            await safe_delete_session(c, sid)
    return res


async def _giant_upload(c, sid, path, b64):
    r = await c.post(f"{API}/sessions/{sid}/files/upload",
                     json={"path": path, "content": b64}, timeout=300)
    r.raise_for_status()


async def _giant_read(c, sid, path):
    r = await c.get(f"{API}/sessions/{sid}/files/read",
                    params={"path": path}, timeout=300)
    r.raise_for_status()
    # consume body
    _ = r.content


async def _giant_download(c, sid, path):
    r = await c.get(f"{API}/sessions/{sid}/files/download",
                    params={"path": path}, timeout=300)
    r.raise_for_status()
    _ = r.content


async def _giant_edit(c, sid, path):
    # search-replace some "A"s with "B"s; small textual change forces full
    # read + full write through the volume adapter
    r = await c.post(f"{API}/sessions/{sid}/files/edit",
                     json={"path": path, "old_string": "AAAA", "new_string": "BBBB",
                           "replace_all": True},
                     timeout=300)
    r.raise_for_status()


async def _gigantic_phase(
    res: SessionResult, sid: str, phase: str, big_op,
):
    """Run ``big_op`` while a parallel probe loop hammers /files/tree on
    a separate connection. Records ``gf_<phase>_ms`` (the big op wall)
    and per-probe latencies as ``gf_probe_<phase>_ms``.
    """
    probe_lats: list[float] = []
    stop = asyncio.Event()

    async def probe():
        # Separate client → separate keep-alive pool, so we measure SERVER
        # event-loop blocking, not client-side queueing.
        async with httpx.AsyncClient(timeout=30) as pc:
            while not stop.is_set():
                t0 = now()
                try:
                    r = await pc.get(f"{API}/sessions/{sid}/files/tree", timeout=30)
                    if r.status_code < 500:
                        probe_lats.append((now() - t0) * 1000)
                except Exception:
                    pass

    probe_task = asyncio.create_task(probe())
    try:
        async with timed(res.ops, f"gf_{phase}"):
            await big_op
    finally:
        stop.set()
        try:
            await asyncio.wait_for(probe_task, timeout=2)
        except (asyncio.TimeoutError, Exception):
            probe_task.cancel()

    # Record EACH probe latency as a separate Op so aggregate picks up p50/p99.
    for ms in probe_lats:
        res.ops.append(Op(name=f"gf_probe_{phase}", ms=ms))
    # Also record probe count so we know how dense the sampling was.
    res.ops.append(Op(name=f"gf_probe_count_{phase}", ms=float(len(probe_lats))))


async def run_bursty_session(c: httpx.AsyncClient, idx: int) -> SessionResult:
    """Minimal-work session: create + 1 prompt + delete. Used by the
    bursty scenario to exercise cold-create throughput under a tight
    arrival window without diluting the signal with file ops, etc."""
    res = SessionResult()
    try:
        async with timed(res.ops, "burst_create"):
            sid = (await create_session(c, f"burst-{idx}"))["session_id"]
            res.session_id = sid
        async with timed(res.ops, "burst_prompt"):
            await drain_sse(
                c, f"{API}/sessions/{sid}/message+stream",
                {"message": PROMPT}, timeout=120,
            )
    except Exception as e:
        res.err = f"{type(e).__name__}: {e}"
    finally:
        if res.session_id:
            await safe_delete_session(c, res.session_id, timeout=60)
    return res


async def run_long_chat_session(c: httpx.AsyncClient, idx: int) -> SessionResult:
    """One session, N_TURNS turns. Exercises JSONL growth, supervisor
    memory, per-session prompt_lock under sustained load. Use N_TURNS=20+
    to stress the conversation tail."""
    res = SessionResult()
    try:
        async with timed(res.ops, "lc_create"):
            sid = (await create_session(c, f"long-chat-{idx}"))["session_id"]
            res.session_id = sid

        for _ in range(N_TURNS):
            async with timed(res.ops, "lc_turn"):
                await drain_sse(
                    c, f"{API}/sessions/{sid}/message+stream",
                    {"message": PROMPT}, timeout=120,
                )
        # Read /log to measure how big the conversation persistence is
        async with timed(res.ops, "lc_log_read"):
            r = await c.get(f"{API}/sessions/{sid}/log",
                            params={"limit": N_TURNS * 10}, timeout=30)
            r.raise_for_status()
            res.ops.append(Op(name="lc_log_rows", ms=float(len(r.json()))))
    except Exception as e:
        res.err = f"{type(e).__name__}: {e}"
    finally:
        if res.session_id:
            await safe_delete_session(c, res.session_id)
    return res


async def run_stress_profile(c: httpx.AsyncClient, idx: int) -> SessionResult:
    """Stress mix: each "session" picks a profile from a heterogeneous menu
    and runs it. The N sessions all run in parallel via main()'s gather,
    so we get gigantic uploads, tool-heavy turns, long chats, and quick
    file ops all happening simultaneously.

    The tagged ops keep their original names (so `tool_heavy_turn` still
    shows up under stress), letting us compare each op's latency under
    stress vs in isolation.

    Profiles cycle over idx: gigantic / tool_heavy / long_chat /
    files_only / bursty / volume_direct, repeating.
    """
    profiles = [
        "gigantic", "tool_heavy", "long_chat", "files_only", "bursty", "volume_direct",
    ]
    profile = profiles[idx % len(profiles)]

    if profile == "gigantic":
        if BENCH_GIGANTIC_MB <= 0:
            # Default to 25 MB under stress so we don't accidentally OOM
            # a developer machine running stress without setting the knob.
            globals()["BENCH_GIGANTIC_MB"] = 25
        return await run_gigantic_files(c, idx)
    if profile == "tool_heavy":
        # Single tool-heavy turn (no other phases) — what a real "user
        # asks the agent to do something" looks like.
        res = SessionResult()
        try:
            async with timed(res.ops, "stress_th_create"):
                sid = (await create_session(c, f"stress-th-{idx}"))["session_id"]
                res.session_id = sid
            async with timed(res.ops, "tool_heavy_turn"):
                await drain_sse(
                    c, f"{API}/sessions/{sid}/message+stream",
                    {"message": TOOL_HEAVY_PROMPT}, timeout=240,
                )
        except Exception as e:
            res.err = f"{type(e).__name__}: {e}"
        finally:
            if res.session_id:
                await safe_delete_session(c, res.session_id)
        return res
    if profile == "long_chat":
        return await run_long_chat_session(c, idx)
    if profile == "files_only":
        # Many file ops, no chat — what an editor/IDE integration does.
        res = SessionResult()
        try:
            async with timed(res.ops, "stress_fo_create"):
                sid = (await create_session(c, f"stress-fo-{idx}"))["session_id"]
                res.session_id = sid
            small_b64 = base64.b64encode(b"stress files-only\n" * 32).decode()
            for k in range(N_FILE_OPS * 4):
                async with timed(res.ops, "files_tree"):
                    r = await c.get(f"{API}/sessions/{sid}/files/tree", timeout=30)
                    if r.status_code >= 500: r.raise_for_status()
                async with timed(res.ops, "files_upload_small"):
                    r = await c.post(f"{API}/sessions/{sid}/files/upload",
                                     json={"path": f"stress/{k}.txt", "content": small_b64},
                                     timeout=30)
                    if r.status_code >= 500: r.raise_for_status()
                async with timed(res.ops, "files_read"):
                    r = await c.get(f"{API}/sessions/{sid}/files/read",
                                    params={"path": f"stress/{k}.txt"}, timeout=30)
                    if r.status_code >= 500: r.raise_for_status()
        except Exception as e:
            res.err = f"{type(e).__name__}: {e}"
        finally:
            if res.session_id:
                await safe_delete_session(c, res.session_id)
        return res
    if profile == "bursty":
        return await run_bursty_session(c, idx)
    return await run_volume_direct_ops(c, idx)


_MIXED_PROFILES = (
    run_session,            # full workload
    run_bursty_session,     # quick-in-out
    run_long_chat_session,  # sustained chat
    run_volume_direct_ops,  # admin/inspector traffic
)


async def run_mixed(c: httpx.AsyncClient, idx: int) -> SessionResult:
    """Per-session profile rotates through _MIXED_PROFILES — models real
    production where users do different things at the same time."""
    return await _MIXED_PROFILES[idx % len(_MIXED_PROFILES)](c, idx)


SCENARIOS = {
    "default": run_session,
    "bursty": run_bursty_session,
    "long_chat": run_long_chat_session,
    "mixed": run_mixed,
    "volume_direct": run_volume_direct_ops,
    "multi_session_per_agent": run_multi_session_per_agent,
    "gigantic_files": run_gigantic_files,
    "stress": run_stress_profile,
}


def aggregate(rs: list[SessionResult]) -> dict:
    by_op: dict[str, list[float]] = {}
    err_count = sum(1 for r in rs if r.err)
    for r in rs:
        for o in r.ops:
            if o.ok:
                by_op.setdefault(o.name, []).append(o.ms)
    summary: dict = {}
    for name, lats in sorted(by_op.items()):
        summary[name] = {
            "n": len(lats),
            "p50_ms": round(statistics.median(lats), 1),
            "p99_ms": round(sorted(lats)[min(int(len(lats) * 0.99), len(lats) - 1)], 1),
        }
    return {"sessions": len(rs), "errors": err_count, "ops": summary}


_FLAG_EXTRAS = (
    (BENCH_SSE_SUBSCRIBER, "SSE_SUBSCRIBER"),
    (BENCH_TOOL_HEAVY, "TOOL_HEAVY"),
    (BENCH_CANCEL, "CANCEL"),
    (BENCH_INTERRUPT, "INTERRUPT"),
)
_NUMERIC_EXTRAS = (
    (BENCH_CONCURRENT_PROMPTS, "CONCURRENT_PROMPTS"),
    (BENCH_LONG_TURNS, "LONG_TURNS"),
    (BENCH_MULTI_SESSION_PER_AGENT, "MSA"),
)


async def main() -> None:
    if BENCH_SCENARIO not in SCENARIOS:
        print(f"ERROR: unknown BENCH_SCENARIO={BENCH_SCENARIO!r}; valid: {list(SCENARIOS)}",
              file=sys.stderr)
        sys.exit(2)
    scenario_fn = SCENARIOS[BENCH_SCENARIO]
    extras = [name for flag, name in _FLAG_EXTRAS if flag]
    extras += [f"{name}={v}" for v, name in _NUMERIC_EXTRAS if v]
    print(f"API={API}  PROVIDER={PROVIDER}  N_SESSIONS={N_SESSIONS}  N_TURNS={N_TURNS}  "
          f"N_FILE_OPS={N_FILE_OPS}  LARGE_MB={LARGE_MB}  MODEL={MODEL}  LABEL={LABEL}")
    print(f"SCENARIO={BENCH_SCENARIO} ({scenario_fn.__name__})  extras={extras or '-'}")
    async with httpx.AsyncClient(
        timeout=120,
        limits=httpx.Limits(max_connections=N_SESSIONS * 4, max_keepalive_connections=N_SESSIONS * 2),
    ) as c:
        try:
            r = await c.get(f"{API}/health", timeout=10)
            print("Health:", r.json())
        except Exception as e:
            print("ERROR: server not reachable:", e); sys.exit(2)

        t0 = now()
        results = await asyncio.gather(*[scenario_fn(c, i) for i in range(N_SESSIONS)])
        wall = now() - t0

    summary = aggregate(results)
    summary["wall_s"] = round(wall, 2)
    summary["throughput_sessions_per_s"] = round(len(results) / wall, 3)
    summary["label"] = LABEL
    summary["provider"] = PROVIDER
    summary["n_sessions"] = N_SESSIONS
    summary["scenario"] = BENCH_SCENARIO
    summary["extras"] = extras

    # human report
    print(f"\n=== {LABEL} ({PROVIDER}, {N_SESSIONS} sessions) ===")
    print(f"wall={wall:.2f}s  errors={summary['errors']}  "
          f"throughput={summary['throughput_sessions_per_s']:.2f} sess/s")
    print(f"{'op':>22} {'n':>5} {'p50_ms':>10} {'p99_ms':>10}")
    for name, stats in summary["ops"].items():
        print(f"{name:>22} {stats['n']:>5} {stats['p50_ms']:>10.1f} {stats['p99_ms']:>10.1f}")

    # JSONL append for cross-run comparison
    with open(REPORT, "a") as f:
        f.write(json.dumps(summary) + "\n")
    print(f"\nappended to {REPORT}")


if __name__ == "__main__":
    asyncio.run(main())
