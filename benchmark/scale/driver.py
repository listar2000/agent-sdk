"""Scale driver for the agent-sdk server.

Spawns N concurrent prompt streams against an already-running server,
measures per-prompt latencies and aggregate throughput, and prints a
single-line summary suitable for diffing across scenarios.

Run while a server is up (see ``scenarios.sh`` for the orchestration that
launches the server with the right flags and runs this back-to-back).

Env knobs:
  API           default http://localhost:7778
  PROVIDER      unix_local | daytona | modal   (default unix_local)
  AGENT_TYPE    claude | opencode  (default claude)
  MODEL         passed through to /sessions    (default haiku)
  N_SESSIONS    concurrent sessions/prompts    (default 16)
  N_TURNS       prompts per session             (default 1)
  PROMPT        text sent each turn             (default: short scripted)
  WARMUP        N_SESSIONS warmup pass before measurement (default 0)
  TIMEOUT_S     hard cap per prompt             (default 180)
  SCENARIO      label included in the result line for grep-friendliness
  TAG           free-form tag for the JSON results row
  RESULT_PATH   optional path to append a JSON-lines row per run

Outputs to stdout. Optional JSON-lines append for diffing.
"""
from __future__ import annotations

import asyncio
import json
import os
import random
import statistics
import sys
import time
from dataclasses import dataclass, field

import httpx


API = os.environ.get("API", "http://localhost:7778")
PROVIDER = os.environ.get("PROVIDER", "unix_local")
AGENT_TYPE = os.environ.get("AGENT_TYPE", "claude")
MODEL = os.environ.get("MODEL", "haiku")
N_SESSIONS = int(os.environ.get("N_SESSIONS", "16"))
N_TURNS = int(os.environ.get("N_TURNS", "1"))
# Spread session creates over CREATE_STAGGER_S so the provider's
# control plane isn't thunder-herded. Daytona returns 502 at >~50
# simultaneous creates; staggering keeps the rate sane.
CREATE_STAGGER_S = float(os.environ.get("CREATE_STAGGER_S", "0"))
PROMPT = os.environ.get("PROMPT", "Reply with exactly five short sentences about the weather.")
WARMUP = int(os.environ.get("WARMUP", "0"))
TIMEOUT_S = float(os.environ.get("TIMEOUT_S", "180"))
SCENARIO = os.environ.get("SCENARIO", "default")
TAG = os.environ.get("TAG", "")
RESULT_PATH = os.environ.get("RESULT_PATH", "")


@dataclass
class PromptResult:
    idx: int
    session_id: str = ""
    create_s: float | None = None
    first_event_s: float | None = None
    done_s: float | None = None
    events: int = 0
    text_blocks: int = 0
    total_text_chars: int = 0
    redirect_count: int = 0
    error: str | None = None


async def _drive_prompt(client: httpx.AsyncClient, session_id: str, idx: int) -> PromptResult:
    """Drive one /message+stream against an existing session and tally events."""
    r = PromptResult(idx=idx, session_id=session_id)
    t_start = time.perf_counter()
    try:
        # ``message+stream`` returns SSE blocks; follow_redirects is set on
        # the client so a 307 from a non-owner replica transparently retries
        # at the right URL. The 307 doesn't reset our perf-counter start;
        # we measure end-to-end from submit-to-first-byte.
        async with client.stream(
            "POST",
            f"{API}/sessions/{session_id}/message+stream",
            json={"message": PROMPT},
            timeout=TIMEOUT_S,
        ) as resp:
            r.redirect_count = sum(1 for h in resp.history if h.status_code in (307, 308))
            resp.raise_for_status()
            buf = ""
            async for chunk in resp.aiter_text():
                buf += chunk
                while "\n\n" in buf:
                    block, buf = buf.split("\n\n", 1)
                    if r.first_event_s is None:
                        r.first_event_s = time.perf_counter() - t_start
                    r.events += 1
                    if "data:" in block:
                        try:
                            data_line = next(
                                ln[5:].lstrip() for ln in block.split("\n") if ln.startswith("data:")
                            )
                            payload = json.loads(data_line)
                            update = (
                                payload.get("params", {}).get("update")
                                if isinstance(payload, dict) else None
                            )
                            if update:
                                content = update.get("content") or {}
                                text = content.get("text") or content.get("thinking") or ""
                                if text:
                                    r.text_blocks += 1
                                    r.total_text_chars += len(text)
                            if (
                                isinstance(payload, dict)
                                and "result" in payload
                                and isinstance(payload["result"], dict)
                                and "stopReason" in payload["result"]
                            ):
                                r.done_s = time.perf_counter() - t_start
                                return r
                            if isinstance(payload, dict) and "error" in payload:
                                r.error = str(payload["error"])[:200]
                                r.done_s = time.perf_counter() - t_start
                                return r
                        except (StopIteration, json.JSONDecodeError):
                            continue
    except Exception as e:
        r.error = f"{type(e).__name__}: {e}"[:200]
        r.done_s = time.perf_counter() - t_start
    return r


async def _create_session(client: httpx.AsyncClient, idx: int) -> str:
    """POST /sessions and return session_id. Eager (provisions sandbox).

    Mirrors the SDK's client-side UUID + ``X-Session-Id`` pattern so the
    LB can consistent-hash POST + subsequent requests to the same
    replica (zero-redirect steady state). Without the header the LB
    would round-robin POST and then consistent-hash subsequent
    requests; the resulting mismatch pays one 307 + Set-Cookie hop per
    session.

    Forwards CLAUDE_CODE_OAUTH_TOKEN (or ANTHROPIC_API_KEY) as a session
    secret so non-host providers can launch claude inside the sandbox.
    """
    import uuid as _uuid
    session_id = str(_uuid.uuid4())
    secrets: dict[str, str] = {}
    for k in ("CLAUDE_CODE_OAUTH_TOKEN", "ANTHROPIC_API_KEY", "OPENROUTER_API_KEY"):
        v = os.environ.get(k)
        if v:
            secrets[k] = v
    body = {
        "id": session_id,
        "name": f"bench-{idx}",
        "provider": PROVIDER,
        "agent_type": AGENT_TYPE,
        "model": MODEL,
    }
    if secrets:
        body["secrets"] = secrets
    r = await client.post(
        f"{API}/sessions",
        json=body,
        headers={"X-Session-Id": session_id},
        timeout=300,
    )
    r.raise_for_status()
    return session_id


async def _delete_session(client: httpx.AsyncClient, sid: str) -> None:
    try:
        await client.delete(f"{API}/sessions/{sid}", timeout=30)
    except Exception:
        pass


async def _run_session_lifecycle(_shared: httpx.AsyncClient, idx: int) -> list[PromptResult]:
    """Create → run N_TURNS prompts → delete. Returns one result per turn.

    Uses a per-session ``AsyncClient`` so each session has its own cookie
    jar — the sticky-route cookie that nginx hashes on must not be
    shared across sessions, or all sessions converge on whichever
    replica handled the most recent POST /sessions. This mirrors how
    real clients work (one Client per user, not one shared globally).
    """
    sid = None
    out: list[PromptResult] = []
    t0 = time.perf_counter()
    limits = httpx.Limits(max_keepalive_connections=4, max_connections=8)
    async with httpx.AsyncClient(limits=limits, follow_redirects=True) as client:
        try:
            sid = await _create_session(client, idx)
            create_s = time.perf_counter() - t0
            for turn in range(N_TURNS):
                r = await _drive_prompt(client, sid, idx)
                r.create_s = create_s if turn == 0 else None
                out.append(r)
        except Exception as e:
            out.append(PromptResult(idx=idx, error=f"create: {e}"[:200]))
        finally:
            if sid:
                await _delete_session(client, sid)
    return out


async def _gather_session_log_counts(initial: int | None = None) -> int:
    """Query Postgres (via the server's admin endpoint surrogate) for the
    total ``session_log`` row count. We don't have a direct count endpoint,
    so we approximate via summing each pool-active session's log read —
    if that's empty, we return 0 and rely on per-prompt event counters.
    """
    # No public /admin/db endpoint; the harness counts events per-prompt
    # instead. Stub kept for symmetry with future direct DB reads.
    return 0


def _summary(results: list[PromptResult], wall_s: float) -> dict:
    done = [r for r in results if r.done_s is not None and r.error is None]
    failed = [r for r in results if r.error]
    first_evt = [r.first_event_s for r in done if r.first_event_s is not None]
    done_s = [r.done_s for r in done if r.done_s is not None]
    total_events = sum(r.events for r in results)
    total_chars = sum(r.total_text_chars for r in results)

    def pct(xs, q):
        if not xs:
            return None
        return statistics.quantiles(xs, n=100, method="inclusive")[q - 1] if len(xs) >= 2 else xs[0]

    return {
        "scenario": SCENARIO,
        "tag": TAG,
        "provider": PROVIDER,
        "agent_type": AGENT_TYPE,
        "model": MODEL,
        "n_sessions": N_SESSIONS,
        "n_turns": N_TURNS,
        "wall_s": round(wall_s, 3),
        "ok": len(done),
        "fail": len(failed),
        "events_total": total_events,
        "events_per_sec": round(total_events / wall_s, 2) if wall_s > 0 else None,
        "chars_total": total_chars,
        "chars_per_sec": round(total_chars / wall_s, 1) if wall_s > 0 else None,
        "first_event_p50_s": round(pct(first_evt, 50), 3) if first_evt else None,
        "first_event_p95_s": round(pct(first_evt, 95), 3) if first_evt else None,
        "done_p50_s": round(pct(done_s, 50), 3) if done_s else None,
        "done_p95_s": round(pct(done_s, 95), 3) if done_s else None,
        "done_p99_s": round(pct(done_s, 99), 3) if done_s else None,
        "redirects_total": sum(r.redirect_count for r in results),
        "errors_sample": [r.error for r in failed[:3]],
    }


async def main() -> None:
    limits = httpx.Limits(max_keepalive_connections=N_SESSIONS * 2,
                          max_connections=N_SESSIONS * 4)
    async with httpx.AsyncClient(limits=limits, follow_redirects=True) as client:
        # Warmup pass — drives a small number of prompts so steady-state
        # things (httpx pool, supervisor.js boot for unix_local) don't
        # contaminate the measurement window.
        if WARMUP > 0:
            print(f"[warmup] running {WARMUP} sessions ...", flush=True)
            await asyncio.gather(
                *[_run_session_lifecycle(client, -i - 1) for i in range(WARMUP)],
                return_exceptions=True,
            )

        print(f"[main] launching {N_SESSIONS} concurrent sessions × {N_TURNS} turn(s) ...",
              flush=True)
        t0 = time.perf_counter()
        # Stagger session-creates to keep the provider's control plane
        # happy. CREATE_STAGGER_S (env) spreads N sessions over that many
        # seconds — daytona's create API returns 502 above ~50 simultaneous
        # cold-creates, so for N=512 use CREATE_STAGGER_S=60+. Default 0
        # preserves the old behavior (small randomised jitter only).
        async def _staggered(i: int):
            if CREATE_STAGGER_S > 0:
                # Even-rate stagger: session i starts at i / N * STAGGER.
                await asyncio.sleep(CREATE_STAGGER_S * i / max(N_SESSIONS, 1))
            else:
                await asyncio.sleep(random.uniform(0, min(2.0, N_SESSIONS * 0.02)))
            return await _run_session_lifecycle(client, i)

        per_session = await asyncio.gather(
            *[_staggered(i) for i in range(N_SESSIONS)],
            return_exceptions=True,
        )
        wall = time.perf_counter() - t0
        results: list[PromptResult] = []
        for r in per_session:
            if isinstance(r, BaseException):
                results.append(PromptResult(idx=-1, error=f"{type(r).__name__}: {r}"[:200]))
            else:
                results.extend(r)

    summary = _summary(results, wall)
    print("\n=== SUMMARY ===")
    for k, v in summary.items():
        print(f"  {k:24} {v}")

    if RESULT_PATH:
        line = json.dumps({"ts": time.time(), **summary}, sort_keys=True)
        with open(RESULT_PATH, "a") as f:
            f.write(line + "\n")
        print(f"\nappended to {RESULT_PATH}")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        sys.exit(130)
