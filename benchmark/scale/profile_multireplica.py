"""Multi-replica load profile.

Production deployment shape: N single-worker uvicorn replicas behind an
L7 LB doing consistent-hash routing on the ``/sessions/{id}/...`` path.
The benchmark client only knows about the LB; routing is the LB's job,
ownership safety is the lease's job.

This script:

  1. Spawns N uvicorn replicas (each 1 worker, distinct port, shared
     Postgres).
  2. Spawns ``benchmark/scale/lb.py`` in front (consistent-hash on
     session_id).
  3. Drives N concurrent prompts at the LB. Measures throughput, p50/p95/p99,
     and how many requests the lease bounced via 307 (the redirect chain
     is observable because the LB rewrites Location headers).
  4. Reads ``session_log`` row counts post-run to confirm batching
     didn't drop rows.

Env knobs:
  N_REPLICAS              default 4
  WORKERS_PER_REPLICA     default 1  (recommended for production)
  N_SESSIONS              default 16
  PORT_BASE               default 7791 (uvicorn replicas land on
                          PORT_BASE..PORT_BASE+N_REPLICAS-1)
  LB_PORT                 default 7790 (the bench targets THIS port only)
  PROMPT                  agent prompt
  DATABASE_URL            shared Postgres for all replicas
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import random
import signal
import statistics
import subprocess
import sys
import time
from pathlib import Path

import httpx

REPO_ROOT = Path(__file__).resolve().parents[2]


def _load_dotenv() -> None:
    """Source the user's ~/.env so CLAUDE_CODE_OAUTH_TOKEN (the user's own
    OAuth key) is in the environment for spawned uvicorn workers — they
    pass it through to the sandbox supervisors. Also picks up
    DAYTONA_API_KEY / MODAL_TOKEN_* if you switch PROVIDER."""
    for path in (Path.home() / ".env", REPO_ROOT / ".env"):
        if not path.is_file():
            continue
        for raw in path.read_text().splitlines():
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, _, v = line.partition("=")
            k = k.strip()
            v = v.strip().strip('"').strip("'")
            os.environ.setdefault(k, v)


_load_dotenv()

VENV_PY = str(REPO_ROOT / ".venv" / "bin" / "python")
DB_URL = os.environ.get(
    "DATABASE_URL", "postgresql://postgres@localhost:5433/agent_sdk_test_scale"
)
N_REPLICAS = int(os.environ.get("N_REPLICAS", "4"))
PORT_BASE = int(os.environ.get("PORT_BASE", "7791"))
LB_PORT = int(os.environ.get("LB_PORT", "7790"))
PROVIDER = os.environ.get("PROVIDER", "unix_local")
# Deployment shape:
#   "lb"          — N single-worker replicas behind benchmark/scale/lb.py.
#                   Consistent-hash routing by session_id.
#   "workers"     — One uvicorn with --workers N. No LB.
#                   Kernel SO_REUSEPORT does the routing; the 503-retry
#                   path handles peer-worker misses.
DEPLOY_MODE = os.environ.get("DEPLOY_MODE", "lb")
N_SESSIONS = int(os.environ.get("N_SESSIONS", "16"))
PROMPT = os.environ.get(
    "PROMPT",
    "Write a 200-word essay on how distributed consensus protocols handle "
    "network partitions. Cover Raft, Paxos, and at least one practical system. "
    "Use concrete examples.",
)
SERVER_LOG_DIR = REPO_ROOT / "logs"
SERVER_LOG_DIR.mkdir(exist_ok=True)


class Replica:
    def __init__(self, name: str, port: int) -> None:
        self.name = name
        self.port = port
        self.proc: subprocess.Popen | None = None
        self.log_path = SERVER_LOG_DIR / f"mr-{name}.log"

    def url(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    def __init_workers__(self, workers: int) -> None:
        self.workers = workers

    def start(self) -> None:
        workers = getattr(self, "workers", 1)
        env = os.environ.copy()
        env["DATABASE_URL"] = DB_URL
        env["PYTHONPATH"] = str(REPO_ROOT / "src") + os.pathsep + env.get("PYTHONPATH", "")
        env["AGENT_SDK_PORT"] = str(self.port)
        env["AGENT_SDK_REPLICA_ID"] = self.name
        env["AGENT_SDK_INTERNAL_HOST"] = "127.0.0.1"
        env["AGENT_SDK_LEASE_TTL_S"] = "30"
        env["AGENT_SDK_LEASE_HEARTBEAT_S"] = "10"
        # Production-target Wave 1 settings
        env["AGENT_SDK_SUPERVISOR_FLUSH_MS"] = "40"
        env["AGENT_SDK_LOG_FLUSH_MS"] = "100"
        env["AGENT_SDK_ORIGIN"] = "test"
        env["AGENT_SDK_WORKERS"] = str(workers)
        self.log_path.write_text("")
        log_f = open(self.log_path, "ab")
        self.proc = subprocess.Popen(
            [VENV_PY, "-m", "uvicorn", "api.server:app",
             "--host", "127.0.0.1", "--port", str(self.port),
             "--workers", str(workers)],
            env=env, cwd=str(REPO_ROOT), stdout=log_f, stderr=log_f,
        )

    def wait_ready(self, timeout_s: float = 30) -> None:
        deadline = time.time() + timeout_s
        with httpx.Client(timeout=1.0) as c:
            while time.time() < deadline:
                try:
                    if c.get(f"{self.url()}/health").status_code == 200:
                        return
                except Exception:
                    pass
                time.sleep(0.2)
        raise RuntimeError(f"replica {self.name} did not become ready")

    def stop(self) -> None:
        if self.proc and self.proc.poll() is None:
            try:
                self.proc.terminate()
                self.proc.wait(timeout=8)
            except subprocess.TimeoutExpired:
                try:
                    os.kill(self.proc.pid, signal.SIGKILL)
                except Exception:
                    pass
                self.proc.wait(timeout=5)


async def _create_session(c: httpx.AsyncClient, base_url: str, name: str) -> str:
    """Create a session via the LB. The LB round-robins POST /sessions
    (no session_id in the path yet), so claims spread across replicas."""
    r = await c.post(
        f"{base_url}/sessions",
        json={"name": name, "provider": PROVIDER,
              "agent_type": "claude", "model": "haiku"},
        timeout=300,  # daytona cold-create can take ~30-60s under load
    )
    r.raise_for_status()
    return r.json()["session_id"]


async def _drive_prompt(c: httpx.AsyncClient, base_url: str, sid: str) -> dict:
    """Drive one /message+stream through the LB.

    The LB routes via consistent hash. The lease handles ownership safety
    via 307. We use follow_redirects=False so the redirect is observable
    in the metric — the LB rewrites the Location to point back at itself,
    so a single redirect re-routes to the right backend on retry.
    """
    res = {"sid": sid, "base_url": base_url, "redirects": 0, "intra_retries": 0,
           "events": 0, "text_chars": 0, "first_evt": None, "done": None, "error": None,
           "backends": []}
    t0 = time.perf_counter()
    url = f"{base_url}/sessions/{sid}/message+stream"
    body = {"message": PROMPT}
    max_total_retries = int(os.environ.get("RETRY_BUDGET", "64"))

    for attempt in range(max_total_retries):
        try:
            req = c.build_request("POST", url, json=body)
            resp = await c.send(req, stream=True, follow_redirects=False)
        except Exception as e:
            res["error"] = f"{type(e).__name__}: {e}"[:200]
            return res

        try:
            backend = resp.headers.get("X-Backend")
            if backend and (not res["backends"] or res["backends"][-1] != backend):
                res["backends"].append(backend)
            if resp.status_code == 307:
                loc = resp.headers.get("Location")
                if not loc:
                    res["error"] = "307 without Location header"
                    return res
                res["redirects"] += 1
                url = loc
                continue
            if resp.status_code == 503 and resp.headers.get("X-Intra-Replica-Miss"):
                res["intra_retries"] += 1
                await asyncio.sleep(0.005)
                continue
            if resp.status_code != 200:
                body_preview = ""
                try:
                    async for chunk in resp.aiter_bytes():
                        body_preview += chunk.decode(errors="replace")
                        if len(body_preview) > 300:
                            break
                except Exception:
                    pass
                res["error"] = f"HTTP {resp.status_code}: {body_preview[:200]}"
                return res

            # Stream the body and look for stopReason.
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
                        u = (msg.get("params", {}).get("update")
                             if isinstance(msg, dict) else None)
                        if u and isinstance(u.get("content"), dict) \
                                and u["content"].get("text"):
                            res["text_chars"] += len(u["content"]["text"])
                        if (isinstance(msg, dict) and "result" in msg
                                and isinstance(msg["result"], dict)
                                and "stopReason" in msg["result"]):
                            res["done"] = time.perf_counter() - t0
                            return res
            # Stream ended without stopReason — record and exit.
            res["error"] = "stream ended without stopReason"
            return res
        finally:
            try:
                await resp.aclose()
            except Exception:
                pass

    res["error"] = f"exceeded {max_total_retries} retries"
    return res


async def _count_log_rows(sids: list[str]) -> dict:
    if not sids:
        return {}
    from psycopg.rows import dict_row
    import psycopg
    async with await psycopg.AsyncConnection.connect(DB_URL, row_factory=dict_row) as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT event_type, COUNT(*) AS n FROM session_log"
                " WHERE session_id = ANY(%s) GROUP BY event_type",
                (sids,),
            )
            return {r["event_type"]: r["n"] for r in await cur.fetchall()}


def _pct(xs, q):
    xs = [x for x in xs if x is not None]
    if not xs:
        return None
    if len(xs) < 2:
        return xs[0]
    return round(statistics.quantiles(xs, n=100, method="inclusive")[q - 1], 3)


async def _scenario(label: str, sids: list[str], base_url: str,
                    c: httpx.AsyncClient) -> dict:
    """Drive prompts at ``base_url`` (the LB)."""
    print(f"\n[{label}] driving {len(sids)} prompts at {base_url} ...", flush=True)
    t0 = time.perf_counter()
    results = await asyncio.gather(
        *[_drive_prompt(c, base_url, sid) for sid in sids],
        return_exceptions=True,
    )
    wall = time.perf_counter() - t0
    ok = [r for r in results if isinstance(r, dict) and r.get("done") is not None]
    fail = [r for r in results if not isinstance(r, dict) or r.get("error")]
    redirects = sum(r["redirects"] for r in ok)
    intra_retries = sum(r.get("intra_retries", 0) for r in ok)
    return {
        "label": label,
        "n": len(sids),
        "wall_s": round(wall, 3),
        "ok": len(ok),
        "fail": len(fail),
        "events_total": sum(r["events"] for r in ok),
        "text_chars_total": sum(r["text_chars"] for r in ok),
        "chars_per_sec": round(sum(r["text_chars"] for r in ok) / wall, 1),
        "first_event_p50_s": _pct([r["first_evt"] for r in ok], 50),
        "first_event_p95_s": _pct([r["first_evt"] for r in ok], 95),
        "done_p50_s": _pct([r["done"] for r in ok], 50),
        "done_p95_s": _pct([r["done"] for r in ok], 95),
        "done_p99_s": _pct([r["done"] for r in ok], 99),
        "redirects_total": redirects,
        "redirect_rate": round(redirects / max(len(ok), 1), 2),
        "intra_replica_retries": intra_retries,
        "errors_sample": [r.get("error") for r in fail][:3],
    }


async def main(do_pyspy: bool) -> int:
    os.environ["DATABASE_URL"] = DB_URL
    sys.path.insert(0, str(REPO_ROOT / "src"))
    from api.db import init_db
    init_db()

    workers_per_replica = int(os.environ.get("WORKERS_PER_REPLICA", "1"))
    replicas: list[Replica] = []
    lb_proc = None

    if DEPLOY_MODE == "workers":
        # One uvicorn, N workers, one port. No LB — the bench hits the
        # uvicorn port directly and the kernel SO_REUSEPORT-balances
        # across workers. Cheapest deployment, accepts higher 503 retry.
        workers = N_REPLICAS  # treat N_REPLICAS as the worker count
        r = Replica("worker_pool", PORT_BASE)
        r.__init_workers__(workers)
        replicas.append(r)
        r.start()
        lb_url = f"http://127.0.0.1:{PORT_BASE}"
        print(f"[mr] DEPLOY_MODE=workers; one uvicorn with {workers} workers "
              f"on :{PORT_BASE}; no LB. DB={DB_URL}")
    else:
        # N single-worker uvicorn replicas behind benchmark/scale/lb.py.
        for i in range(N_REPLICAS):
            r = Replica(f"r{i}", PORT_BASE + i)
            r.__init_workers__(workers_per_replica)
            replicas.append(r)
        print(f"[mr] DEPLOY_MODE=lb; {N_REPLICAS} replicas on ports "
              f"{[r.port for r in replicas]} "
              f"(workers/replica={workers_per_replica}), DB={DB_URL}")
        for r in replicas:
            r.start()
        # LB sidecar.
        lb_env = os.environ.copy()
        lb_env["BACKENDS"] = ",".join(r.url() for r in replicas)
        lb_env["PORT"] = str(LB_PORT)
        lb_env["PYTHONPATH"] = str(REPO_ROOT / "src") + os.pathsep + lb_env.get("PYTHONPATH", "")
        lb_log = open(SERVER_LOG_DIR / "mr-lb.log", "wb")
        lb_proc = subprocess.Popen(
            [VENV_PY, str(REPO_ROOT / "benchmark" / "scale" / "lb.py")],
            env=lb_env, cwd=str(REPO_ROOT), stdout=lb_log, stderr=lb_log,
        )
        lb_url = f"http://127.0.0.1:{LB_PORT}"
        for _ in range(60):
            try:
                if httpx.get(f"{lb_url}/health", timeout=1).status_code == 200:
                    break
            except Exception:
                pass
            time.sleep(0.2)
        print(f"[mr] LB ready at {lb_url} → backends {lb_env['BACKENDS']}")
    pyspy_procs: list[subprocess.Popen] = []
    try:
        for r in replicas:
            r.wait_ready()
        print(f"[mr] {len(replicas)} replicas healthy")

        if do_pyspy:
            for r in (a, b):
                svg = SERVER_LOG_DIR / f"mr-flame-{r.name}.svg"
                pyspy_procs.append(subprocess.Popen([
                    "sudo", "-n", f"{REPO_ROOT}/.venv/bin/py-spy", "record",
                    "-o", str(svg), "-d", "30", "--subprocesses", "-p",
                    str(r.proc.pid),
                ], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL))
                print(f"[mr] py-spy recording {r.name} → {svg}")



        # Create N sessions via the LB. The LB round-robins POST /sessions
        # (no session_id in the URL yet), so claims spread across replicas.
        sids: list[str] = []
        async with httpx.AsyncClient(follow_redirects=False) as c:
            print(f"[mr] creating {N_SESSIONS} sessions via LB ...")
            raw = await asyncio.gather(
                *[_create_session(c, lb_url, f"mr-{i}") for i in range(N_SESSIONS)],
                return_exceptions=True,
            )
        for i, sid in enumerate(raw):
            if isinstance(sid, Exception):
                print(f"   skipping idx={i}: {sid}")
                continue
            sids.append(sid)
        print(f"[mr] {len(sids)} sessions created and lease-claimed")

        results = []
        # Bump connection pool so 50-100 concurrent streams don't stall on
        # pool acquisition (the default httpx limit is 100 keepalive / 100
        # total — fine for N≤32 but the failure mode at higher N is silent
        # task hangs because aiter_text waits on a never-acquired conn).
        limits = httpx.Limits(max_keepalive_connections=N_SESSIONS * 3,
                              max_connections=N_SESSIONS * 6)
        async with httpx.AsyncClient(follow_redirects=False, timeout=180,
                                     limits=limits) as c:
            # The only scenario we need: drive prompts through the LB.
            # The LB does consistent-hash routing on session_id; the lease
            # validates ownership; 307 fires only when ownership moved.
            res = await _scenario("lb_routed", sids, lb_url, c)
            results.append(res)
            # Sample log rows post-batch-flush.
            await asyncio.sleep(0.5)
            log_rows = await _count_log_rows(sids)

        print("\n=== MULTI-REPLICA PROFILE ===")
        for r in results:
            print(f"\n  -- {r['label']} --")
            for k, v in r.items():
                if k == "label":
                    continue
                print(f"     {k:24} {v}")
        print(f"\n  log_rows_by_type   {log_rows}")
        print(f"  log_rows_total     {sum(log_rows.values())}")
        print(f"  per_session_avg    {sum(log_rows.values()) / max(len(sids), 1):.1f}")

        # Cleanup via the LB.
        async with httpx.AsyncClient(timeout=30, follow_redirects=True) as c:
            await asyncio.gather(*[
                c.delete(f"{lb_url}/sessions/{s}") for s in sids
            ], return_exceptions=True)

        return 0
    finally:
        for p in pyspy_procs:
            try: p.wait(timeout=35)
            except Exception:
                try: p.kill()
                except Exception: pass
        if lb_proc is not None:
            try:
                lb_proc.terminate()
                lb_proc.wait(timeout=5)
            except Exception:
                try: lb_proc.kill()
                except Exception: pass
        for r in replicas:
            r.stop()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--py-spy", action="store_true",
                    help="capture flame graphs via py-spy (requires sudo -n)")
    args = ap.parse_args()
    sys.exit(asyncio.run(main(args.py_spy)))
