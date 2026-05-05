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
            # crude terminator detection: stopReason or top-level error
            if "stopReason" in line or '"type":"done"' in line or '"error":' in line:
                # let the rest of the block flush
                pass
    return chunks


async def run_session(c: httpx.AsyncClient, idx: int) -> SessionResult:
    """Full per-session workflow."""
    res = SessionResult()
    try:
        # ---- create ----
        async with timed(res.ops, "session_create"):
            r = await c.post(f"{API}/sessions", json={
                "name": f"bench-{idx}",
                "provider": PROVIDER,
                "config": {"agent_type": "claude", "model": MODEL},
                "secrets": {
                    "CLAUDE_CODE_OAUTH_TOKEN": os.environ.get("CLAUDE_CODE_OAUTH_TOKEN", ""),
                },
            }, timeout=120)
            r.raise_for_status()
            sid = r.json()["session_id"]
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
        for turn in range(N_TURNS):
            async with timed(res.ops, "prompt_turn"):
                _ = await drain_sse(
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
                _ = await drain_sse(
                    c, f"{API}/sessions/{sid}/message+stream",
                    {"message": PROMPT}, timeout=120,
                )

    except Exception as e:
        res.err = f"{type(e).__name__}: {e}"
    finally:
        if res.session_id:
            try:
                await c.delete(f"{API}/sessions/{res.session_id}", timeout=120)
            except Exception:
                pass
    return res


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


async def main() -> None:
    print(f"API={API}  PROVIDER={PROVIDER}  N_SESSIONS={N_SESSIONS}  N_TURNS={N_TURNS}  "
          f"N_FILE_OPS={N_FILE_OPS}  LARGE_MB={LARGE_MB}  MODEL={MODEL}  LABEL={LABEL}")
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
        results = await asyncio.gather(*[run_session(c, i) for i in range(N_SESSIONS)])
        wall = now() - t0

    summary = aggregate(results)
    summary["wall_s"] = round(wall, 2)
    summary["throughput_sessions_per_s"] = round(len(results) / wall, 3)
    summary["label"] = LABEL
    summary["provider"] = PROVIDER
    summary["n_sessions"] = N_SESSIONS

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
