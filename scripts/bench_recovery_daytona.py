#!/usr/bin/env python3
"""Validate the daytona snapshot fix AND report per-phase timing for the
three boot paths, end-to-end, with no test-file changes.

Sequence (mirrors test_session_resume_after_delete + _after_stop):
  1. cold-create + turn 1                          → COLD CREATE breakdown
  2. external daytona STOP (pause) + turn 2         → RESUME (same VM) breakdown
  3. external daytona DELETE + turn 3               → RECOVERY (restore) breakdown

Correctness asserts (the invariant my first fix broke): inner_session_id is
unchanged across both stop and delete — i.e. session/load restored the
volume-backed JSONL, proving per-turn snapshot writes still happen on a
--skip-restore first-create.

Per-phase numbers come from the server's ``[BENCH] daytona.cold_create``
lines, grouped per start() (one group per boot, terminated by TOTAL).

Env: AGENT_SERVER_URL (default :7778), ANTHROPIC_API_KEY, DAYTONA_API_KEY,
SERVER_LOG_PATH (default /tmp/agent-sdk-server.log).
"""
from __future__ import annotations

import asyncio
import os
import re
import time

import httpx

SERVER = os.environ.get("AGENT_SERVER_URL", "http://localhost:7778")
API_KEY = os.environ.get("ANTHROPIC_API_KEY")
DAYTONA_API_KEY = os.environ.get("DAYTONA_API_KEY")
LOG_PATH = os.environ.get("SERVER_LOG_PATH", "/tmp/agent-sdk-server.log")

_BENCH_RE = re.compile(
    r"\[BENCH\] daytona\.cold_create session=(?P<sid>\S+) "
    r"phase=(?P<phase>\S+) s=(?P<dur>[\d.]+)"
)
_ORDER = ["resolve_or_create_sandbox", "start_supervisor", "acp_attach", "TOTAL"]
_TEXT_RE = re.compile(r'"text":"((?:[^"\\]|\\.)*)"')


def _bench_groups(sid8: str) -> list[dict]:
    """All start() phase-groups for a session, in order (1 per boot)."""
    groups: list[dict] = []
    cur: dict = {}
    if not os.path.exists(LOG_PATH):
        return groups
    with open(LOG_PATH, errors="ignore") as fh:
        for line in fh:
            m = _BENCH_RE.search(line)
            if not m or m.group("sid") != sid8:
                continue
            cur[m.group("phase")] = float(m.group("dur"))
            if m.group("phase") == "TOTAL":
                groups.append(cur)
                cur = {}
    return groups


async def _ask(client: httpx.AsyncClient, sid: str, msg: str) -> tuple[bool, str]:
    texts: list[str] = []
    ok = False
    async with client.stream(
        "POST", f"{SERVER}/sessions/{sid}/message+stream",
        json={"message": msg}, timeout=httpx.Timeout(connect=10, read=120, write=10, pool=10),
    ) as resp:
        if resp.status_code != 200:
            return False, f"<http {resp.status_code}>"
        async for block in resp.aiter_text():
            for m in _TEXT_RE.finditer(block):
                texts.append(m.group(1))
            if '"error":' in block:
                break
            if "stopReason" in block or '"type":"done"' in block:
                ok = True
                break
    reply = " ".join(t for t in texts if msg[:10] not in t).strip()
    return ok, reply


async def _inner_sid(client: httpx.AsyncClient, sid: str) -> str | None:
    r = await client.get(f"{SERVER}/admin/sessions", timeout=10)
    r.raise_for_status()
    hit = next((s for s in r.json().get("sessions", []) if s["session_id"] == sid), None)
    return hit.get("inner_session_id") if hit else None


def _daytona_op(op: str, ref: str) -> None:
    """op ∈ {stop, delete} on a daytona sandbox, out-of-band."""
    from daytona_sdk import Daytona, DaytonaConfig
    dt = Daytona(DaytonaConfig(api_key=DAYTONA_API_KEY))
    sb = dt.get(ref)
    if op == "stop":
        sb.stop()
    else:
        dt.delete(sb)


def _show(label: str, g: dict | None) -> None:
    if not g:
        print(f"  {label:26s} (no BENCH group)")
        return
    parts = "  ".join(f"{p}={g.get(p, 0):.2f}s" for p in _ORDER)
    print(f"  {label:26s} {parts}")


async def main() -> None:
    assert API_KEY, "ANTHROPIC_API_KEY required"
    assert DAYTONA_API_KEY, "DAYTONA_API_KEY required"
    body = {"provider": "daytona", "agent_type": "claude",
            "secrets": {"ANTHROPIC_API_KEY": API_KEY}}
    async with httpx.AsyncClient() as c:
        print("1) cold-create + turn 1")
        r = await c.post(f"{SERVER}/sessions", json=body, timeout=300)
        r.raise_for_status()
        sid = r.json()["session_id"]
        sid8 = sid[:8]
        ref1 = (await c.get(f"{SERVER}/sessions/{sid}/sandbox", timeout=15)).json().get("sandbox_ref")
        ok1, rep1 = await _ask(c, sid, "Reply with one word: READY.")
        inner0 = await _inner_sid(c, sid)
        print(f"   session={sid8} ok={ok1} reply={rep1[:20]!r} inner={inner0}")

        print("2) external STOP (pause same VM) + turn 2")
        await asyncio.to_thread(_daytona_op, "stop", ref1)
        await asyncio.sleep(3)
        ok2, rep2 = await _ask(c, sid, "Reply with one word: AGAIN.")
        inner1 = await _inner_sid(c, sid)
        print(f"   ok={ok2} reply={rep2[:20]!r} inner={inner1}")

        print("3) external DELETE (new VM, restore) + turn 3")
        ref2 = (await c.get(f"{SERVER}/sessions/{sid}/sandbox", timeout=15)).json().get("sandbox_ref")
        await asyncio.to_thread(_daytona_op, "delete", ref2)
        await asyncio.sleep(3)
        ok3, rep3 = await _ask(c, sid, "Reply with one word: BACK.")
        inner2 = await _inner_sid(c, sid)
        print(f"   ok={ok3} reply={rep3[:20]!r} inner={inner2}")

        # cleanup
        try:
            await c.delete(f"{SERVER}/sessions/{sid}", timeout=60)
        except Exception:
            pass

        await asyncio.sleep(0.5)
        groups = _bench_groups(sid8)
        print("\n=== per-phase timing (server [BENCH]) ===")
        _show("COLD CREATE (first)", groups[0] if len(groups) > 0 else None)
        _show("RESUME after stop", groups[1] if len(groups) > 1 else None)
        _show("RECOVERY after delete", groups[2] if len(groups) > 2 else None)

        print("\n=== correctness ===")
        turns_ok = ok1 and ok2 and ok3
        stop_keep = inner1 == inner0
        del_keep = inner2 == inner0
        print(f"  all turns replied:           {turns_ok}")
        print(f"  inner preserved across STOP: {stop_keep}  ({inner0} -> {inner1})")
        print(f"  inner preserved across DEL:  {del_keep}  ({inner0} -> {inner2})")
        ok = turns_ok and stop_keep and del_keep
        print(f"\n{'PASS ✓ recovery still restores' if ok else 'FAIL ✗ regression'}")
        raise SystemExit(0 if ok else 1)


if __name__ == "__main__":
    asyncio.run(main())
