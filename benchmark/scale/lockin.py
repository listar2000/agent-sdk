"""Run goldens N times per provider, report failures explicitly.

Foreground orchestrator — replaces the brittle bash version. Loops:
  unix_local × RUNS, modal × RUNS, daytona × RUNS,
runs pytest on each, writes per-run log, prints a single line per run,
exits 0 only when EVERY run reported zero failures.

Assumes:
  • Postgres at localhost:5433
  • Multi-replica stack already running on 7791..7794 + LB on 7790
  • ~/.env carries provider credentials
"""
from __future__ import annotations

import os
import subprocess
import sys
import time
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
VENV_PY = str(REPO / ".venv" / "bin" / "python")
DB = "postgresql://postgres@localhost:5433/agent_sdk_test_scale"
LB = "http://127.0.0.1:7790"
LOG_DIR = REPO / "logs" / "lockin"
LOG_DIR.mkdir(parents=True, exist_ok=True)

RUNS = int(os.environ.get("RUNS_PER", "3"))


def _load_dotenv() -> None:
    for path in (Path.home() / ".env", REPO / ".env"):
        if not path.is_file():
            continue
        for raw in path.read_text().splitlines():
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, _, v = line.partition("=")
            os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


def _wipe_db() -> None:
    subprocess.run(
        ["psql", "-h", "localhost", "-p", "5433", "-U", "postgres",
         "-d", "agent_sdk_test_scale", "-c",
         "DELETE FROM session_log; DELETE FROM sessions;"
         " DELETE FROM agents; DELETE FROM volumes;"],
        env={**os.environ, "PGPASSWORD": "postgres"},
        capture_output=True,
    )


def _run_one(provider: str, run: int, filter_: str, timeout: int) -> tuple[bool, str]:
    _wipe_db()
    log_path = LOG_DIR / f"{provider}-{run}.log"
    env = {
        **os.environ,
        "DATABASE_URL": DB,
        "TEST_DATABASE_URL": DB,
        "PYTHONPATH": str(REPO / "src"),
        "AGENT_API_URL": LB,
        "AGENT_SERVER_URL": LB,
    }
    cmd = [
        VENV_PY, "-m", "pytest",
        str(REPO / "tests" / "test_golden.py"),
        "-n", "auto", "-k", filter_, "--tb=long",
    ]
    t0 = time.time()
    with log_path.open("wb") as f:
        try:
            rc = subprocess.run(cmd, env=env, stdout=f, stderr=subprocess.STDOUT,
                                timeout=timeout).returncode
        except subprocess.TimeoutExpired:
            rc = 124
    dt = time.time() - t0
    content = log_path.read_text(errors="replace")
    # pytest prints "X failed" or "X passed" in the summary.
    has_fail = (" failed" in content) or (rc != 0)
    # Last non-empty line.
    summary = ""
    for line in reversed(content.splitlines()):
        if line.strip():
            summary = line.strip()
            break
    return (not has_fail, f"rc={rc} wall={dt:.1f}s {summary}")


def main() -> int:
    _load_dotenv()
    sweeps = [
        ("unix_local", "unix_local", 600),
        ("modal", "modal and claude", 900),
        ("daytona", "daytona and claude", 900),
    ]
    failures: list[str] = []
    for provider, filter_, tmo in sweeps:
        for run in range(1, RUNS + 1):
            ok, msg = _run_one(provider, run, filter_, tmo)
            tag = "[PASS]" if ok else "[FAIL]"
            print(f"{tag} {provider}-{run}  {msg}", flush=True)
            if not ok:
                failures.append(f"{provider}-{run}")
    print()
    if failures:
        print(f"=== {len(failures)} failure(s) across {RUNS * len(sweeps)} runs ===")
        for f in failures:
            print(f"  - {f}  → logs/lockin/{f}.log")
        return 1
    print(f"=== ALL CLEAN: {RUNS * len(sweeps)} runs passed under -n auto ===")
    return 0


if __name__ == "__main__":
    sys.exit(main())
