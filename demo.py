"""Live demo: session survives sandbox deletion via persistent volume.

Run:
    set -a; source .env; set +a
    python demo.py
"""
from __future__ import annotations
import asyncio
import os
import sys
import time
import uuid

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))

from httpx import ASGITransport, AsyncClient  # noqa: E402

if "TEST_DATABASE_URL" in os.environ:
    os.environ.setdefault("DATABASE_URL", os.environ["TEST_DATABASE_URL"])
elif "DATABASE_URL" not in os.environ:
    sys.exit("Set TEST_DATABASE_URL or DATABASE_URL (e.g. `set -a; source .env; set +a`).")
from api import db as dbmod, server as srv  # noqa: E402
from api.models import AgentConfig, AgentRecord  # noqa: E402


def log(step: str, msg: str = "") -> None:
    t = time.strftime("%H:%M:%S")
    print(f"\033[90m{t}\033[0m  \033[1;36m{step:22}\033[0m  {msg}")


async def wait_for_reply(client, sid, timeout: int = 180) -> list[dict]:
    deadline = time.time() + timeout
    while time.time() < deadline:
        r = await client.get(f"/sessions/{sid}/status")
        if r.status_code == 200:
            st = r.json()
            if not st.get("agent_busy", True) and not st.get("pending_count"):
                logr = await client.get(f"/sessions/{sid}/log?limit=200")
                if logr.status_code == 200:
                    return logr.json()
        await asyncio.sleep(2)
    raise TimeoutError("timed out waiting for reply")


async def main() -> int:
    # Reset test DB.
    dbmod.init_db()
    await dbmod.init_pool()
    async with dbmod.get_db() as conn:
        await conn.execute("DELETE FROM session_log")
        await conn.execute("DELETE FROM sessions")
        await conn.execute("DELETE FROM sandboxes")
        await conn.execute("DELETE FROM volumes")
        await conn.execute("DELETE FROM agents")

    agent_id = f"demo-{uuid.uuid4().hex[:6]}"
    await dbmod.upsert_agent(AgentRecord(
        id=agent_id, name="Demo",
        config=AgentConfig(agent_type="claude",
                           prompt="Helpful assistant. Keep responses short."),
    ))

    async with AsyncClient(transport=ASGITransport(app=srv.app),
                           base_url="http://test", timeout=300.0) as c:

        log("1  PROVISION VOLUME", "Creating a Daytona volume (persistent storage)...")
        r = await c.post("/volumes",
                         json={"name": f"demo-vol-{uuid.uuid4().hex[:6]}",
                               "provider": "daytona"})
        vol = r.json()
        log("   →", f"vol id={vol['id'][:24]}  status={vol['status']}  provider_ref={vol['provider_ref'][:12]}...")

        log("2  CREATE SESSION", "Binds to volume; NO sandbox yet (lazy)...")
        r = await c.post("/sessions", json={
            "agent_id": agent_id, "volume_id": vol["id"],
            "secrets": {"CLAUDE_CODE_OAUTH_TOKEN": os.environ["CLAUDE_CODE_OAUTH_TOKEN"]},
        })
        sess = r.json()
        sid = sess["id"]
        log("   →", f"session={sid[:24]}  current_sandbox_id={sess.get('current_sandbox_id')}  (lazy!)")

        log("3  FIRST PROMPT", "Triggers lazy sandbox provision + mounts volume...")
        await c.post(f"/sessions/{sid}/message",
                     json={"message": "Please remember the secret word OSPREY. Just say OK."})
        await wait_for_reply(c, sid)
        sess = await dbmod.get_session(sid)
        old_sbid = sess["current_sandbox_id"]
        log("   →", f"sandbox now provisioned: {old_sbid[:24]}  inner_session_id={sess['inner_session_id'][:24]}...")

        log("4  KILL SANDBOX", f"DELETE /sandboxes/{old_sbid[:24]}...")
        await c.delete(f"/sandboxes/{old_sbid}")
        if sid in srv.SESSIONS:
            st = srv.SESSIONS.pop(sid)
            st.shutdown.set()
        log("   →", "sandbox gone — session row still alive, volume data preserved")

        log("5  SECOND PROMPT", "Should reprovision + CLI resumes transcript from volume...")
        t0 = time.time()
        await c.post(f"/sessions/{sid}/message",
                     json={"message": "What is the secret word?"})
        events2 = await wait_for_reply(c, sid)
        elapsed = time.time() - t0
        sess2 = await dbmod.get_session(sid)
        new_sbid = sess2["current_sandbox_id"]
        log("   →", f"new sandbox={new_sbid[:24]}  (elapsed {elapsed:.0f}s)")

        assistant = [e for e in events2
                     if e["event_type"] == "assistant_message"][-3:]
        text = " ".join(str(e.get("payload", "")) for e in assistant).lower()

        reattach = [e for e in events2 if e["event_type"] == "sandbox_reattach"]
        if reattach:
            log("   →", f"sandbox_reattach event: old={reattach[0]['payload'].get('old_sandbox_id','?')[:18]} → new={reattach[0]['payload'].get('new_sandbox_id','?')[:18]}")

        ok = "osprey" in text
        mark = "✅" if ok else "❌"
        log("6  RESULT", f"{mark}  {'agent remembered OSPREY across sandbox death' if ok else 'agent did NOT remember OSPREY'}")

        log("7  CLEANUP", "deleting volume (tolerates 403)...")
        await c.delete(f"/volumes/{vol['id']}?force=true")

    await dbmod.close_pool()
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
