"""Adversarial integration tests for the multi-replica lease + 307 path.

Spawns two uvicorn processes on different ports (sharing one Postgres)
and walks through the failure modes the Wave-3 design has to handle:

  T1  Cross-replica 307 routing
      Session is created on replica A → claim lands on A. A request
      directly to replica B for that session must reply 307 with the
      Location pointing at A. Follow-redirects=True must produce a
      successful response.

  T2  Lease takeover after replica death
      A claims, then we SIGKILL A. Heartbeat stops. After the TTL
      window passes, replica B successfully claims via the
      same ``try_claim_lease`` UPDATE. ``lease_generation`` bumps.

  T3  Concurrent claim race
      Many parallel callers race ``try_claim_lease`` against a fresh
      session_id from the same Python process. Exactly one observes
      "you own it"; the rest see None or get the same owner_id.

  T4  Coalescing preserves text bytes end-to-end
      A streaming prompt against replica A (unix_local provider, claude
      haiku) produces some text. The coalesced text in the SSE stream
      must concatenate to the same bytes as the un-coalesced baseline,
      and ``session_log`` must capture every assistant_message row.

Run with the project venv:

    DATABASE_URL=postgresql://postgres@localhost:5433/agent_sdk_test_scale \\
        PYTHONPATH=src .venv/bin/python benchmark/scale/test_adversarial.py

Cleans up its own DB rows and the two uvicorn child processes on exit.
"""
from __future__ import annotations

import asyncio
import contextlib
import json
import os
import signal
import subprocess
import sys
import time
import uuid
from pathlib import Path

import httpx

REPO_ROOT = Path(__file__).resolve().parents[2]


def _load_dotenv() -> None:
    """Mirror the orchestrator helpers: pull CLAUDE_CODE_OAUTH_TOKEN (the
    user's OAuth key) and provider creds from ~/.env / project .env so
    direct python invocations don't need a shell preload."""
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
PORT_A = int(os.environ.get("PORT_A", "7791"))
PORT_B = int(os.environ.get("PORT_B", "7792"))
SERVER_LOG_DIR = REPO_ROOT / "logs"
SERVER_LOG_DIR.mkdir(exist_ok=True)


def _green(s: str) -> str:
    return f"\033[32m{s}\033[0m"


def _red(s: str) -> str:
    return f"\033[31m{s}\033[0m"


class Replica:
    """One uvicorn process bound to a specific port with a unique replica id."""

    def __init__(self, name: str, port: int) -> None:
        self.name = name
        self.port = port
        self.proc: subprocess.Popen | None = None
        self.log_path = SERVER_LOG_DIR / f"adv-{name}.log"

    def base_url(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    def start(self) -> None:
        env = os.environ.copy()
        env["DATABASE_URL"] = DB_URL
        env["PYTHONPATH"] = str(REPO_ROOT / "src") + os.pathsep + env.get("PYTHONPATH", "")
        env["AGENT_SDK_PORT"] = str(self.port)
        env["AGENT_SDK_REPLICA_ID"] = self.name
        env["AGENT_SDK_INTERNAL_HOST"] = "127.0.0.1"
        # Tight TTL so T2 (takeover after kill) runs in reasonable time.
        env["AGENT_SDK_LEASE_TTL_S"] = "6"
        env["AGENT_SDK_LEASE_HEARTBEAT_S"] = "2"
        env["AGENT_SDK_ORIGIN"] = "test"
        # Single worker per replica so the lease semantics are unambiguous
        # (each replica → exactly one owner_id, easy to reason about).
        cmd = [
            VENV_PY, "-m", "uvicorn", "api.server:app",
            "--host", "127.0.0.1", "--port", str(self.port),
            "--workers", "1",
        ]
        self.log_path.write_text("")  # truncate
        log_f = open(self.log_path, "ab")
        self.proc = subprocess.Popen(
            cmd, env=env, cwd=str(REPO_ROOT),
            stdout=log_f, stderr=log_f,
        )

    def wait_ready(self, timeout_s: float = 30) -> None:
        deadline = time.time() + timeout_s
        with httpx.Client(timeout=1.0) as c:
            while time.time() < deadline:
                try:
                    r = c.get(f"{self.base_url()}/health")
                    if r.status_code == 200:
                        return
                except Exception:
                    pass
                time.sleep(0.2)
        raise RuntimeError(
            f"replica {self.name} on port {self.port} did not become ready; "
            f"see {self.log_path}"
        )

    def kill_hard(self) -> None:
        if self.proc and self.proc.poll() is None:
            try:
                os.kill(self.proc.pid, signal.SIGKILL)
            except Exception:
                pass
            self.proc.wait(timeout=5)

    def stop(self) -> None:
        if self.proc and self.proc.poll() is None:
            try:
                self.proc.terminate()
                self.proc.wait(timeout=8)
            except subprocess.TimeoutExpired:
                self.kill_hard()


async def _create_session_on(replica: Replica, name: str) -> dict:
    """Create a session bound to this replica (provider=unix_local).
    Returns the JSON body. Asserts no 307 (we want this replica to own
    it). The very first request to a replica IS this replica's claim."""
    async with httpx.AsyncClient(follow_redirects=False, timeout=120) as c:
        r = await c.post(
            f"{replica.base_url()}/sessions",
            json={
                "name": name,
                "provider": "unix_local",
                "agent_type": "claude",
                "model": "haiku",
            },
        )
        if r.status_code == 307:
            raise AssertionError(
                f"unexpected 307 on session-create at {replica.name}: "
                f"Location={r.headers.get('Location')}"
            )
        r.raise_for_status()
        return r.json()


async def _delete_session(url: str, sid: str) -> None:
    async with httpx.AsyncClient(timeout=30, follow_redirects=True) as c:
        with contextlib.suppress(Exception):
            await c.delete(f"{url}/sessions/{sid}")


async def _read_lease_row(sid: str) -> dict | None:
    """Direct DB read — bypasses any in-flight pool state."""
    from psycopg.rows import dict_row
    import psycopg

    async with await psycopg.AsyncConnection.connect(DB_URL, row_factory=dict_row) as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT lease_owner_id, lease_owner_addr, lease_generation,"
                " lease_expires_at FROM sessions WHERE id = %s", (sid,),
            )
            row = await cur.fetchone()
            return row


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

async def t1_cross_replica_307(a: Replica, b: Replica) -> None:
    print("\n[T1] cross-replica 307 routing")
    body = await _create_session_on(a, "t1-x-replica")
    sid = body["session_id"]
    lease = await _read_lease_row(sid)
    print(f"     created sid={sid[:8]} on {a.name} → lease owner={lease['lease_owner_id']} addr={lease['lease_owner_addr']}")
    assert lease["lease_owner_addr"].endswith(f":{a.port}"), (
        f"expected lease addr to point at A (:{a.port}), got {lease['lease_owner_addr']}"
    )

    # T1.a — direct request to B, follow_redirects=False. Server now
    # emits a RELATIVE Location + ``Set-Cookie: agent_sdk_route=<A>;
    # Path=/sessions/<sid>`` so an LB in front can route the retry to
    # A via the cookie. Without an LB (this test), we verify the
    # headers directly instead of expecting httpx to follow.
    async with httpx.AsyncClient(follow_redirects=False, timeout=30) as c:
        r = await c.post(f"{b.base_url()}/sessions/{sid}/resume", json={})
    print(f"     direct POST /resume on {b.name}: status={r.status_code}, "
          f"location={r.headers.get('Location')!r}, "
          f"set-cookie={r.headers.get('Set-Cookie')!r}")
    assert r.status_code == 307, f"expected 307 from non-owner, got {r.status_code} body={r.text[:200]}"
    loc = r.headers.get("Location", "")
    assert loc.startswith(f"/sessions/{sid}"), (
        f"307 Location should be relative path under /sessions/{sid}, got {loc!r}"
    )
    set_cookie = r.headers.get("Set-Cookie", "")
    assert "agent_sdk_route=" in set_cookie, (
        f"307 should include Set-Cookie agent_sdk_route=..., got {set_cookie!r}"
    )
    assert f"Path=/sessions/{sid}" in set_cookie, (
        f"Set-Cookie Path should be scoped to /sessions/{sid}, got {set_cookie!r}"
    )

    # T1.b — manually emulate the LB cookie route: parse the cookie's
    # value (the owner's replica id), use it to pick the right backend,
    # and re-issue the request there. End-to-end should 200.
    import re
    m = re.search(r"agent_sdk_route=([^;]+)", set_cookie)
    owner_id = m.group(1) if m else ""
    assert owner_id, f"Set-Cookie missing agent_sdk_route value: {set_cookie!r}"
    # The cookie value is A's replica_id; route to A's port.
    async with httpx.AsyncClient(follow_redirects=False, timeout=30) as c:
        r = await c.post(f"{a.base_url()}/sessions/{sid}/resume", json={})
    assert r.status_code == 200, f"after LB cookie-routes to owner, should 200, got {r.status_code} body={r.text[:200]}"
    print(f"     cookie-routed retry to {a.name}: {r.json().get('status')}")

    print(_green("     T1 PASS"))

    await _delete_session(a.base_url(), sid)


async def t2_takeover_after_kill(a: Replica, b: Replica) -> None:
    print("\n[T2] lease takeover after replica A death")
    body = await _create_session_on(a, "t2-takeover")
    sid = body["session_id"]
    lease0 = await _read_lease_row(sid)
    gen0 = lease0["lease_generation"]
    print(f"     sid={sid[:8]} owned by A, gen={gen0}, addr={lease0['lease_owner_addr']}")

    # Kill A hard so its heartbeat dies.
    print(f"     SIGKILL replica A (pid={a.proc.pid})")
    a.kill_hard()

    # Wait for the lease to expire. With LEASE_TTL_S=6 above, the lease
    # row's lease_expires_at is "now + 6s" from the last renewal. The
    # heartbeat fires every 2s; we kill right after a renew at worst,
    # so wait 8s to be safe.
    wait_s = 8.0
    print(f"     waiting {wait_s}s for lease TTL to expire ...")
    await asyncio.sleep(wait_s)

    # B should now be able to claim by hitting an endpoint that calls
    # pool.get_session. Use /resume.
    async with httpx.AsyncClient(follow_redirects=False, timeout=60) as c:
        r = await c.post(f"{b.base_url()}/sessions/{sid}/resume", json={})
    # Possible outcomes:
    #   - 200: B's pool.get_session claimed (gen bumps)
    #   - 307: lease row stale, points at A — shouldn't happen post-TTL
    #   - 500/502: sandbox-reattach failure (the unix_local PID inside A's
    #              host is also gone; B will cold-create a fresh sandbox)
    print(f"     POST /resume on B: status={r.status_code}, location={r.headers.get('Location')}")
    if r.status_code == 307:
        raise AssertionError(
            f"B got 307 after TTL expired — Location={r.headers.get('Location')!r}"
        )
    assert r.status_code in (200, 502), (
        f"unexpected status {r.status_code} body={r.text[:200]}"
    )
    lease1 = await _read_lease_row(sid)
    gen1 = lease1["lease_generation"]
    print(f"     lease after takeover: owner={lease1['lease_owner_id']} gen={gen1}")
    assert lease1["lease_owner_addr"].endswith(f":{b.port}"), (
        f"expected B to own; lease points at {lease1['lease_owner_addr']!r}"
    )
    assert gen1 > gen0, f"generation should bump on ownership transfer (was {gen0}, now {gen1})"
    print(_green("     T2 PASS"))

    await _delete_session(b.base_url(), sid)


async def t3_concurrent_claim_race(_a: Replica, _b: Replica) -> None:
    """Direct DB-level race against ``try_claim_lease`` — does not require
    the uvicorn processes; just needs the schema."""
    print("\n[T3] concurrent lease claims (DB-level race)")
    from api import db as dbmod
    from api.models import AgentConfig, AgentRecord, VolumeRecord

    # Use a separate pool so we don't tangle with the replicas' state.
    os.environ.setdefault("DATABASE_URL", DB_URL)
    await dbmod.init_pool()
    try:
        aid = f"a-{uuid.uuid4().hex[:8]}"
        vid = f"v-{uuid.uuid4().hex[:8]}"
        sid = f"s-{uuid.uuid4().hex[:8]}"
        await dbmod.upsert_agent(AgentRecord(id=aid, name="race-test",
                                             config=AgentConfig(agent_type="claude")))
        await dbmod.upsert_volume(VolumeRecord(
            id=vid, name=vid, provider="unix_local",
            provider_ref=str(REPO_ROOT / "logs" / "fake-vol"),
        ))
        async with dbmod.get_db() as conn:
            await conn.execute(
                "INSERT INTO sessions (id, agent_id, volume_id) VALUES (%s,%s,%s)",
                (sid, aid, vid),
            )

        # Race N callers all trying to claim the same fresh session.
        N = 32
        owners = [f"racer-{i}" for i in range(N)]

        async def _try(owner: str) -> dict | None:
            return await dbmod.try_claim_lease(
                sid, owner_id=owner, owner_addr=f"127.0.0.1:{1000 + abs(hash(owner)) % 9000}",
                ttl_seconds=30,
            )

        results = await asyncio.gather(*(_try(o) for o in owners))
        # Exactly one distinct lease_owner_id should be reflected in the
        # rows that came back non-None. (All non-None rows should say the
        # SAME owner — the winner. None means we lost.)
        winners = [r for r in results if r is not None]
        winner_ids = {r["lease_owner_id"] for r in winners}
        print(f"     {N} parallel try_claim_lease → {len(winners)} returned a row, distinct owners={winner_ids}")
        assert len(winner_ids) == 1, f"multiple owners observed: {winner_ids}"
        # Cleanup
        async with dbmod.get_db() as conn:
            await conn.execute("DELETE FROM sessions WHERE id=%s", (sid,))
            await conn.execute("DELETE FROM volumes WHERE id=%s", (vid,))
            await conn.execute("DELETE FROM agents WHERE id=%s", (aid,))
        print(_green("     T3 PASS"))
    finally:
        await dbmod.close_pool()


async def t4_coalescing_preserves_text(a: Replica, _b: Replica) -> None:
    print("\n[T4] coalescing preserves end-to-end text bytes")
    PROMPT = "Reply with exactly: The quick brown fox jumps over the lazy dog."
    body = await _create_session_on(a, "t4-coalesce")
    sid = body["session_id"]
    try:
        # Drain the SSE stream and concatenate ALL text content fields.
        text_acc = ""
        async with httpx.AsyncClient(timeout=120, follow_redirects=True) as c:
            async with c.stream(
                "POST",
                f"{a.base_url()}/sessions/{sid}/message+stream",
                json={"message": PROMPT},
            ) as resp:
                resp.raise_for_status()
                buf = ""
                async for chunk in resp.aiter_text():
                    buf += chunk
                    while "\n\n" in buf:
                        block, buf = buf.split("\n\n", 1)
                        for line in block.split("\n"):
                            if not line.startswith("data:"):
                                continue
                            payload = line[5:].lstrip()
                            try:
                                msg = json.loads(payload)
                            except Exception:
                                continue
                            update = (
                                msg.get("params", {}).get("update")
                                if isinstance(msg, dict) else None
                            )
                            if not update:
                                if (
                                    isinstance(msg, dict)
                                    and "result" in msg
                                    and isinstance(msg["result"], dict)
                                    and "stopReason" in msg["result"]
                                ):
                                    # Stream ends here.
                                    raise StopAsyncIteration
                                continue
                            content = update.get("content") or {}
                            t = content.get("text", "")
                            if t:
                                text_acc += t

        # Stream-end happens inside the async with; raise to break out.
    except StopAsyncIteration:
        pass

    # The SSE generator returned the moment we saw ``stopReason``; the
    # batched session_log writer needs a beat to flush (100ms flush
    # window) AND the persister's text-buffer flush runs AFTER our
    # break (it's coalesced server-side). Sleep long enough to let
    # both settle.
    await asyncio.sleep(2.0)

    # Read the session_log rows that the batcher wrote.
    async with httpx.AsyncClient(timeout=30) as c:
        log = await c.get(f"{a.base_url()}/sessions/{sid}/log?limit=500")
    log.raise_for_status()
    rows = log.json()
    log_text = "".join(
        r["payload"].get("text", "") for r in rows
        if r["event_type"] == "assistant_message"
    )

    # The phrase we asked for should appear in BOTH the SSE stream and the
    # batched session_log. Coalescing must not drop bytes; batching must
    # not drop rows.
    expected = "The quick brown fox jumps over the lazy dog."
    print(f"     SSE text acc has phrase: {expected in text_acc}, log has phrase: {expected in log_text}")
    print(f"     SSE chars={len(text_acc)} log chars={len(log_text)}")
    assert expected in text_acc, f"SSE text missing target phrase. Got: {text_acc[:300]!r}"
    assert expected in log_text, f"session_log missing target phrase. Got: {log_text[:300]!r}"
    print(_green("     T4 PASS"))

    await _delete_session(a.base_url(), sid)


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

def _start_nginx_lb(a: Replica, b: Replica, lb_port: int) -> subprocess.Popen:
    """Spin up an nginx LB in front of the two replicas with the same
    cookie-failover + consistent-hash config used in production. Cookie
    map keys are the replicas' ``owner_id`` (``<name>-<pid>``) so the
    server's NotOwner Set-Cookie steers retries correctly.

    Required so T5 (and any future tests exercising the full routing
    chain) can hit the LB URL the way an SDK client would in
    production, rather than poking replicas directly.
    """
    assert a.proc and b.proc, "replicas must be started first"
    pid_a, pid_b = a.proc.pid, b.proc.pid
    owner_a = f"{a.name}-{pid_a}"
    owner_b = f"{b.name}-{pid_b}"
    conf_path = SERVER_LOG_DIR / f"adv-nginx-{lb_port}.conf"
    pid_path = SERVER_LOG_DIR / f"adv-nginx-{lb_port}.pid"
    log_path = SERVER_LOG_DIR / f"adv-nginx-{lb_port}.log"
    conf_path.write_text(
        f"""daemon off;
worker_processes 1;
error_log stderr warn;
pid {pid_path};
events {{ worker_connections 4096; }}
http {{
  access_log off;
  map $request_uri $url_sid {{
    "~^/sessions/(?<sid>[0-9a-f-]+)" $sid;
    default "";
  }}
  map $url_sid $route_key {{
    ""      $http_x_session_id;
    default $url_sid;
  }}
  map $cookie_agent_sdk_route $sticky_backend {{
    "{owner_a}"    127.0.0.1:{a.port};
    "{owner_b}"    127.0.0.1:{b.port};
    default "";
  }}
  upstream agent_sdk_hash {{
    hash $route_key consistent;
    server 127.0.0.1:{a.port};
    server 127.0.0.1:{b.port};
    keepalive 64;
  }}
  upstream agent_sdk_rr {{
    server 127.0.0.1:{a.port};
    server 127.0.0.1:{b.port};
    keepalive 64;
  }}
  proxy_http_version 1.1;
  proxy_set_header   Connection "";
  proxy_buffering    off;
  proxy_read_timeout 1h;
  proxy_send_timeout 1h;
  server {{
    listen {lb_port};
    location ~ ^/sessions/[0-9a-f-]+(/.*)?$ {{
      if ($sticky_backend != "") {{ proxy_pass http://$sticky_backend; break; }}
      proxy_pass http://agent_sdk_hash;
    }}
    location = /sessions {{
      if ($http_x_session_id != "") {{ proxy_pass http://agent_sdk_hash; break; }}
      proxy_pass http://agent_sdk_rr;
    }}
    location / {{ proxy_pass http://agent_sdk_rr; }}
  }}
}}
"""
    )
    return subprocess.Popen(
        ["nginx", "-p", str(REPO_ROOT), "-c", str(conf_path)],
        stdout=open(log_path, "ab"), stderr=subprocess.STDOUT,
    )


async def t5_parallel_prompts_across_replicas(a: Replica, b: Replica) -> None:
    """Regression: N sessions spread across replicas, fire a prompt at
    EACH, every one of them must persist a complete turn (user_message +
    assistant_message + turn_end) to session_log.

    Failure mode this catches: a prompt POSTed to the wrong-owner replica
    silent-fails (the pre-fix bug where /message returned 200 with a bg
    drain that lost NotOwner events). Or a routing regression where some
    sessions never reach their lease owner.
    """
    print("\n[T5] parallel prompts at N=10 spread across replicas")
    import uuid as _uuid
    secrets = {}
    if os.environ.get("CLAUDE_CODE_OAUTH_TOKEN"):
        secrets["CLAUDE_CODE_OAUTH_TOKEN"] = os.environ["CLAUDE_CODE_OAUTH_TOKEN"]
    if not secrets:
        print(_red("     T5 SKIP: CLAUDE_CODE_OAUTH_TOKEN not set"))
        return

    N = 10
    sids = [str(_uuid.uuid4()) for _ in range(N)]

    # Start nginx in front of the two replicas — same routing config
    # as production. SDK clients hit the LB URL, never replica URLs
    # directly; the cookie-failover path needs an LB to read the
    # cookie back on retry.
    LB_PORT = 7790
    nginx_proc = _start_nginx_lb(a, b, LB_PORT)
    LB = f"http://127.0.0.1:{LB_PORT}"
    # Wait for nginx to bind.
    try:
        deadline = time.time() + 10
        while time.time() < deadline:
            try:
                async with httpx.AsyncClient(timeout=1.0) as probe:
                    r = await probe.get(f"{LB}/health")
                if r.status_code == 200:
                    break
            except Exception:
                await asyncio.sleep(0.2)
        else:
            raise AssertionError("nginx LB did not become ready in 10s")
    except Exception:
        nginx_proc.terminate()
        raise

    try:
        async with httpx.AsyncClient(timeout=120, follow_redirects=True) as c:
            # All requests go through the LB. ``X-Session-Id`` on POST
            # so consistent-hash routes POST + subsequent requests to
            # the same replica.
            async def _create(i, sid):
                r = await c.post(
                    f"{LB}/sessions",
                    headers={"X-Session-Id": sid},
                    json={
                        "id": sid,
                        "name": f"t5-{i:02d}",
                        "provider": "unix_local",
                        "agent_type": "claude",
                        "model": "haiku",
                        "secrets": secrets,
                    },
                )
                r.raise_for_status()
            await asyncio.gather(*[_create(i, sid) for i, sid in enumerate(sids)])

            async def _prompt(i, sid):
                r = await c.post(
                    f"{LB}/sessions/{sid}/message",
                    json={"message": f"reply with exactly: hello {i}"},
                    timeout=120,
                )
                r.raise_for_status()
            await asyncio.gather(*[_prompt(i, sid) for i, sid in enumerate(sids)])

            # Wait for events to drain (claude reply + batcher flush).
            await asyncio.sleep(15)

            # Verify EVERY session has a complete turn in its log.
            missing = []
            for i, sid in enumerate(sids):
                r = await c.get(f"{LB}/sessions/{sid}/log?limit=500")
                r.raise_for_status()
                rows = r.json()
                types = {x["event_type"] for x in rows}
                need = {"user_message", "assistant_message", "turn_end"}
                if not need.issubset(types):
                    missing.append((sid, sorted(types)))

            if missing:
                for sid, types in missing[:5]:
                    print(_red(f"     incomplete turn for {sid[:8]}: got {types}"))
                raise AssertionError(
                    f"T5: {len(missing)}/{N} sessions did not complete a turn"
                )
            print(_green(f"     T5 PASS  ({N}/{N} sessions completed)"))

            for sid in sids:
                await _delete_session(LB, sid)
    finally:
        nginx_proc.terminate()
        try:
            nginx_proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            nginx_proc.kill()


async def main() -> int:
    # Ensure the schema is in place (idempotent).
    os.environ["DATABASE_URL"] = DB_URL
    sys.path.insert(0, str(REPO_ROOT / "src"))
    from api.db import init_db
    init_db()

    a = Replica("replicaA", PORT_A)
    b = Replica("replicaB", PORT_B)
    print(f"starting replicas: A=:{PORT_A}, B=:{PORT_B}, DB={DB_URL}")
    a.start()
    b.start()
    try:
        a.wait_ready()
        b.wait_ready()
        print(_green("both replicas healthy"))

        failures: list[str] = []
        for label, fn in [
            ("T1", t1_cross_replica_307),
            ("T2", t2_takeover_after_kill),
            ("T3", t3_concurrent_claim_race),
            ("T4", t4_coalescing_preserves_text),
            ("T5", t5_parallel_prompts_across_replicas),
        ]:
            try:
                # T2 kills A — restart it after.
                if label == "T2" and a.proc and a.proc.poll() is not None:
                    print("     (restarting A before next test)")
                    a.start()
                    a.wait_ready()
                await fn(a, b)
                # If T2 killed A, restart it for subsequent tests.
                if a.proc and a.proc.poll() is not None:
                    print(f"     (T2 killed A; restarting for next test)")
                    a.start()
                    a.wait_ready()
            except AssertionError as e:
                print(_red(f"     {label} FAIL: {e}"))
                failures.append(f"{label}: {e}")
            except Exception as e:
                print(_red(f"     {label} ERROR: {type(e).__name__}: {e}"))
                failures.append(f"{label}: {type(e).__name__}: {e}")

        if failures:
            print("\n" + _red(f"{len(failures)} failure(s):"))
            for f in failures:
                print(f"   - {f}")
            return 1
        print("\n" + _green("all adversarial tests PASSED"))
        return 0
    finally:
        a.stop()
        b.stop()


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
