#!/usr/bin/env python3
"""Reproduce the prod opencode "Unexpected error" on the slim daytona snapshot.

WHY: prod sessions on snapshot ``agent-sdk-c3229ee`` fail with
  RuntimeError: supervisor on port 9100 ... failed health check
  [acp-stderr] Error: Unexpected error, check log file at
     /home/daytona/.local/share/opencode/log/<ts>.log
The real cause is in that in-sandbox log, which the supervisor health-check
dump does NOT capture. This script reads it.

It discriminates the two hypotheses:
  H1  fresh opencode WORKS  -> the crash is from RESTORED old-session state
                              (supervisor.js restores .local/share/opencode etc.)
  H2  fresh opencode CRASHES -> the deployed snapshot's opencode is itself broken
                              (deployed snapshot != the Jun-6 golden-verified build)

Run where DAYTONA_API_KEY is set (e.g. a Railway shell or your daytona env):
    DAYTONA_API_KEY=... .venv/bin/python scripts/repro_opencode_crash.py
Optional: SNAPSHOT=agent-sdk-c3229ee (default below).
Creates ONE origin=test sandbox and deletes it in finally.
"""
import asyncio
import os
import sys

SNAPSHOT = os.environ.get("SNAPSHOT", "agent-sdk-c3229ee")
OPENCODE_BIN = "/opt/agent-sdk/runtime/node_modules/.bin/opencode"

# Minimal startup probes — each exercises opencode init the same way the
# supervisor's ``opencode acp`` launch does, then we read whatever log it wrote.
PROBES = [
    ("node --version", "node --version 2>&1"),
    ("opencode bin?", f"ls -la {OPENCODE_BIN} 2>&1; readlink -f {OPENCODE_BIN} 2>&1"),
    ("opencode binary variants present", "ls -1 /app/src/supervisor/node_modules | grep -i opencode 2>&1"),
    ("cpu avx2?", "grep -o 'avx2' /proc/cpuinfo | head -1 || echo NO_AVX2"),
    ("opencode --version", f"HOME=/home/daytona {OPENCODE_BIN} --version 2>&1; echo rc=$?"),
    ("opencode acp (startup)", f"HOME=/home/daytona timeout 20 {OPENCODE_BIN} acp </dev/null 2>&1; echo rc=$?"),
    ("=> opencode LOG (the real error)",
     "ls -t /home/daytona/.local/share/opencode/log/*.log 2>/dev/null | head -1 | xargs cat 2>&1 | tail -60 || echo NO_LOG"),
]


async def main() -> int:
    if not os.environ.get("DAYTONA_API_KEY"):
        print("FATAL: DAYTONA_API_KEY not set", file=sys.stderr)
        return 2
    from daytona_sdk import AsyncDaytona, DaytonaConfig, CreateSandboxFromSnapshotParams

    d = AsyncDaytona(DaytonaConfig(api_key=os.environ["DAYTONA_API_KEY"]))
    sb = None
    try:
        print(f"[repro] creating FRESH sandbox from snapshot {SNAPSHOT} (origin=test)...")
        sb = await d.create(CreateSandboxFromSnapshotParams(
            snapshot=SNAPSHOT, auto_stop_interval=0,
            labels={"agent_sdk_origin": "test"},
        ), timeout=240)
        print(f"[repro] sandbox {sb.id} state={getattr(sb,'state',None)}")
        for label, cmd in PROBES:
            r = await sb.process.exec(cmd, timeout=60)
            out = (getattr(r, "result", None) or getattr(r, "stdout", "") or "").strip()
            print(f"\n=== {label} (exit={getattr(r,'exit_code',None)}) ===")
            print(out[:4000] if out else "(no output)")
    finally:
        if sb is not None:
            try:
                await d.delete(sb)
                print(f"\n[cleanup] deleted {sb.id}")
            except Exception as e:
                print(f"\n[cleanup] delete failed: {e} -- reap with cleanup_orphans.py --origin test")
    print("\nINTERPRET: 'opencode --version'/'acp' rc!=0 with 'Unexpected error' => H2 (snapshot broken).")
    print("           both rc==0 (fresh works) => H1 (crash is from restored old-session state).")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
