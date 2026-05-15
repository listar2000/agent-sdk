"""Real-agent event profiler.

Drives N concurrent prompts that produce non-trivial streaming output and
records the metrics that matter for tuning Wave 1a/1b:

  • raw_events       — total SSE blocks the driver received (after coalesce)
  • text_chunks      — count of agent_message_delta/chunk blocks with text
  • text_chars       — total text bytes delivered
  • coalesced_ratio  — average chars/text_chunk (higher ⇒ better coalescing)
  • log_rows         — count of session_log rows written for these prompts
                       (sampled from Postgres post-run)
  • first_event_p*   — time-to-first-event distribution
  • done_p*          — turn-end latency distribution

Knobs (env vars):
  API           default http://localhost:7779
  N_SESSIONS    default 8
  PROMPT        default a paragraph-shaped prompt that produces ~200-400 chars
  TAG           label written into results.jsonl
  RESULT_PATH   jsonl path to append to
  DB_URL        for direct session_log row-count queries

Typical use is via ``tune_batching.sh`` which sweeps server-side
AGENT_SDK_SUPERVISOR_FLUSH_MS × AGENT_SDK_LOG_FLUSH_MS combinations and
captures the results so you can pick a sweet spot.
"""
from __future__ import annotations

import asyncio
import json
import os
import statistics
import time
import uuid

import httpx


API = os.environ.get("API", "http://localhost:7779")
N_SESSIONS = int(os.environ.get("N_SESSIONS", "8"))
N_TURNS = int(os.environ.get("N_TURNS", "1"))
PROMPT = os.environ.get(
    "PROMPT",
    # A longer prompt produces denser streaming, which is where Wave 1a's
    # coalescing actually shows up. Asking for ~200 words gives us a few
    # hundred text chunks per turn.
    "Write a 200-word essay on how distributed consensus protocols handle "
    "network partitions. Cover Raft, Paxos, and at least one practical "
    "system. Use concrete examples.",
)
TAG = os.environ.get("TAG", "")
RESULT_PATH = os.environ.get("RESULT_PATH", "")
DB_URL = os.environ.get(
    "DB_URL", "postgresql://postgres@localhost:5433/agent_sdk_test_scale"
)


async def _count_log_rows(sids: list[str]) -> dict[str, int]:
    """Query Postgres for per-event-type log row counts for our sessions."""
    if not sids:
        return {}
    try:
        from psycopg.rows import dict_row
        import psycopg
    except Exception:
        return {}
    async with await psycopg.AsyncConnection.connect(DB_URL, row_factory=dict_row) as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT event_type, COUNT(*) AS n FROM session_log"
                " WHERE session_id = ANY(%s) GROUP BY event_type",
                (sids,),
            )
            return {r["event_type"]: r["n"] for r in await cur.fetchall()}


async def _one_session(client: httpx.AsyncClient, idx: int) -> dict:
    r = await client.post(
        f"{API}/sessions",
        json={"name": f"prof-{idx}", "provider": "unix_local",
              "agent_type": "claude", "model": "haiku"},
        timeout=120,
    )
    r.raise_for_status()
    sid = r.json()["session_id"]
    res = {
        "sid": sid, "events": 0, "text_chunks": 0, "text_chars": 0,
        "first_evt": None, "done": None, "redirects": 0, "error": None,
    }
    t0 = time.perf_counter()
    try:
        async with client.stream(
            "POST", f"{API}/sessions/{sid}/message+stream",
            json={"message": PROMPT}, timeout=180,
        ) as resp:
            res["redirects"] = sum(1 for h in resp.history
                                   if h.status_code in (307, 308))
            resp.raise_for_status()
            buf = ""
            async for chunk in resp.aiter_text():
                buf += chunk
                while "\n\n" in buf:
                    block, buf = buf.split("\n\n", 1)
                    if res["first_evt"] is None:
                        res["first_evt"] = time.perf_counter() - t0
                    res["events"] += 1
                    for line in block.split("\n"):
                        if not line.startswith("data:"):
                            continue
                        try:
                            msg = json.loads(line[5:].lstrip())
                        except Exception:
                            continue
                        update = msg.get("params", {}).get("update") \
                            if isinstance(msg, dict) else None
                        if update:
                            c = update.get("content") or {}
                            if isinstance(c, dict) and c.get("text"):
                                res["text_chunks"] += 1
                                res["text_chars"] += len(c["text"])
                        if (
                            isinstance(msg, dict) and "result" in msg
                            and isinstance(msg["result"], dict)
                            and "stopReason" in msg["result"]
                        ):
                            res["done"] = time.perf_counter() - t0
                            return res
    except Exception as e:
        res["error"] = f"{type(e).__name__}: {e}"[:200]
    return res


async def _delete_sessions(client: httpx.AsyncClient, sids: list[str]) -> None:
    """Delete every session we created, *after* we've sampled session_log."""
    await asyncio.gather(*[
        client.delete(f"{API}/sessions/{s}", timeout=30) for s in sids
    ], return_exceptions=True)


async def main() -> None:
    print(f"[profile] API={API} N={N_SESSIONS} prompt={PROMPT[:60]!r}")
    t_start = time.perf_counter()
    async with httpx.AsyncClient(follow_redirects=True) as c:
        results = await asyncio.gather(
            *[_one_session(c, i) for i in range(N_SESSIONS)],
            return_exceptions=True,
        )
        wall = time.perf_counter() - t_start
        ok = [r for r in results if isinstance(r, dict) and r.get("done") is not None]
        fail = [r for r in results if not isinstance(r, dict) or r.get("error")]
        # Give the server-side batcher a beat to flush before we query log
        # rows. AGENT_SDK_LOG_FLUSH_MS up to 250 means a single 0.5s wait
        # covers any pending batch.
        await asyncio.sleep(0.5)
        sids_to_clean = [r["sid"] for r in ok if r.get("sid")]
        log_rows = await _count_log_rows(sids_to_clean)
        # Now delete — after we've counted.
        await _delete_sessions(c, sids_to_clean)

    def pct(xs, q):
        if not xs: return None
        if len(xs) < 2: return xs[0]
        return statistics.quantiles(xs, n=100, method="inclusive")[q - 1]

    total_text_chunks = sum(r["text_chunks"] for r in ok)
    total_text_chars = sum(r["text_chars"] for r in ok)
    total_events = sum(r["events"] for r in ok)
    coalesced_ratio = (total_text_chars / total_text_chunks) if total_text_chunks else 0.0
    summary = {
        "tag": TAG,
        "n_sessions": N_SESSIONS,
        "n_turns": N_TURNS,
        "wall_s": round(wall, 3),
        "ok": len(ok),
        "fail": len(fail),
        "events_total": total_events,
        "events_per_sec": round(total_events / wall, 2),
        "text_chunks_total": total_text_chunks,
        "text_chars_total": total_text_chars,
        "chars_per_chunk_avg": round(coalesced_ratio, 1),
        "chars_per_sec": round(total_text_chars / wall, 1),
        "first_event_p50_s": round(pct([r["first_evt"] for r in ok if r["first_evt"]], 50) or 0, 3),
        "first_event_p95_s": round(pct([r["first_evt"] for r in ok if r["first_evt"]], 95) or 0, 3),
        "done_p50_s": round(pct([r["done"] for r in ok], 50) or 0, 3),
        "done_p95_s": round(pct([r["done"] for r in ok], 95) or 0, 3),
        "done_p99_s": round(pct([r["done"] for r in ok], 99) or 0, 3),
        "redirects_total": sum(r["redirects"] for r in ok),
        "log_rows_by_type": log_rows,
        "log_rows_total": sum(log_rows.values()),
    }
    print("\n=== PROFILE RESULT ===")
    for k, v in summary.items():
        print(f"  {k:24} {v}")

    if RESULT_PATH:
        with open(RESULT_PATH, "a") as f:
            f.write(json.dumps({"ts": time.time(), **summary}, sort_keys=True) + "\n")
        print(f"\nappended to {RESULT_PATH}")


if __name__ == "__main__":
    asyncio.run(main())
