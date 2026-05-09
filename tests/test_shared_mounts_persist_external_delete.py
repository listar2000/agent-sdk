"""End-to-end: shared_mounts data persists across external sandbox delete.

Real test, real Daytona, no mocks. The user has reported this bug class
before — sandbox delete WIPES the shared mount, then cold-recovery sees
an empty /mnt/<name>. The architectural promise is the opposite: the
shared mount is volume-backed (lives at <volume>/shared/<name>/), and
the volume is decoupled from sandbox lifecycle, so data must survive.

Workflow:

  1. Create daytona session with shared_mounts=["e2etest"]
  2. Write a marker file to /mnt/e2etest/marker via sandbox/exec
  3. Read it back to confirm the mount works
  4. EXTERNALLY delete the daytona sandbox (bypasses our server)
  5. Send a follow-up message → cold-recovery provisions a new sandbox
     with the SAME shared_mounts
  6. Read /mnt/e2etest/marker on the NEW sandbox — must be present
     with the SAME content

Skipped unless DAYTONA_API_KEY + CLAUDE_CODE_OAUTH_TOKEN are set and
the dev server is running on localhost:7778.
"""
from __future__ import annotations

import asyncio
import os
import sys
import time
import uuid

import httpx
import pytest

_SRC = os.path.join(os.path.dirname(__file__), "..", "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

from agent_sdk import Agent, ApiClient  # noqa: E402

DAYTONA_API_KEY = os.environ.get("DAYTONA_API_KEY")
OAUTH_TOKEN = os.environ.get("CLAUDE_CODE_OAUTH_TOKEN")
SERVER = os.environ.get("AGENT_SERVER_URL", "http://localhost:7778")

pytestmark = pytest.mark.skipif(
    not (DAYTONA_API_KEY and OAUTH_TOKEN),
    reason="DAYTONA_API_KEY + CLAUDE_CODE_OAUTH_TOKEN required",
)


async def _server_up() -> bool:
    try:
        async with httpx.AsyncClient(timeout=2) as c:
            r = await c.get(f"{SERVER}/health")
            return r.status_code == 200
    except Exception:
        return False


async def _external_delete_daytona(sandbox_ref: str) -> None:
    from daytona_sdk import Daytona, DaytonaConfig
    daytona = Daytona(DaytonaConfig(api_key=DAYTONA_API_KEY))
    loop = asyncio.get_event_loop()
    sb = await loop.run_in_executor(None, lambda: daytona.get(sandbox_ref))
    await loop.run_in_executor(None, lambda: daytona.delete(sb))
    deadline = loop.time() + 15
    while loop.time() < deadline:
        try:
            await loop.run_in_executor(None, lambda: daytona.get(sandbox_ref))
            await asyncio.sleep(0.5)
        except Exception:
            return


@pytest.mark.asyncio
async def test_shared_mounts_persist_across_external_sandbox_delete():
    """Shared-mount data must outlive the sandbox.

    Asserts:
      A. shared_mount /mnt/e2etest is mounted on first provision and
         is writable by the agent
      B. After external Daytona sandbox delete + cold-recovery, the
         marker written before the delete is STILL readable on the
         new sandbox via the SAME shared_mount path

    What this test would catch:
      * shared_mounts being passed only on first create, then dropped
        from the recipe — would fail at step B (mount not present)
      * shared_mounts pointing at sandbox-local storage instead of
        the volume — would fail at step B (file gone with sandbox)
      * Cold-recovery using a different mount name / path
    """
    if not await _server_up():
        pytest.skip(f"no server at {SERVER}")

    mount_name = "e2etest"
    marker_content = f"persisted-across-delete-{uuid.uuid4().hex[:12]}"
    marker_path = f"/mnt/{mount_name}/marker.txt"

    agent = Agent(
        f"shared-mount-survive-{uuid.uuid4().hex[:8]}",
        provider="daytona",
        agent_type="claude",
        model="haiku",
        api_url=SERVER,
        shared_mounts=[mount_name],
        oauth_token=OAUTH_TOKEN,
    )
    sdk = ApiClient(SERVER)
    try:
        # ── Phase 1: create + write marker to the shared mount ─────────
        print(f"\n[phase 1] creating daytona session with shared_mounts=[{mount_name!r}]...")
        await agent.configure(model="haiku")  # forces _ensure_registered
        sid = agent.session_id
        sandbox_ref_1 = agent.sandbox_ref
        assert sid and sandbox_ref_1, f"agent not registered: {agent.__dict__}"
        print(f"   session_id={sid[:8]}  sandbox_ref={sandbox_ref_1[:16]}")

        # Verify the mount exists + is writable.
        ex = await sdk.session_sandbox_exec(
            sid,
            f"mkdir -p /mnt/{mount_name} && "
            f"echo {marker_content!r} > {marker_path} && "
            f"cat {marker_path}",
            timeout=20,
        )
        assert ex["exit_code"] == 0, (
            f"[phase 1] could not write to /mnt/{mount_name}: "
            f"stdout={ex.get('stdout')!r} stderr={ex.get('stderr')!r}"
        )
        assert marker_content in ex["stdout"], (
            f"[phase 1] marker readback mismatch: got {ex['stdout']!r}"
        )
        print(f"   ✓ wrote + read back marker on /mnt/{mount_name}")

        # Send one prompt so claude-agent-acp persists a conversation
        # JSONL (and the supervisor.js writes a snapshot tarball at
        # turn-end). Without this, cold-recovery in Phase 3 would call
        # ACP ``session/load`` on a session_id that was never written to
        # disk; claude-agent-acp returns -32603 ``Internal error`` and the
        # follow-up message hangs the SSE stream. The contract is "turn
        # completed → snapshot/JSONL persisted, recoverable"; this test's
        # original Phase 1 (configure + sandbox_exec, no prompt) hit
        # exactly that corner case.
        warmup_reply = await asyncio.wait_for(
            agent.arun("Reply with the single word READY and nothing else."),
            timeout=120,
        )
        assert "ready" in warmup_reply.lower(), (
            f"warmup prompt didn't get a normal reply: {warmup_reply!r}"
        )
        print("   ✓ warmup prompt completed (JSONL + snapshot persisted)")

        # ── Phase 2: external Daytona delete ───────────────────────────
        print(f"\n[phase 2] externally deleting daytona sandbox {sandbox_ref_1[:16]}...")
        await _external_delete_daytona(sandbox_ref_1)
        print("   ✓ daytona sandbox gone")
        await asyncio.sleep(2)

        # ── Phase 3: cold-recovery via follow-up agent message ─────────
        print("\n[phase 3] sending follow-up message to trigger cold-recovery...")
        # arun goes through /message+stream; the response body waits for
        # the cold-recovery + ACP attach + reply to complete.
        reply = await asyncio.wait_for(
            agent.arun("Reply with the single word ACK and nothing else."),
            timeout=300,
        )
        assert "ack" in reply.lower() or "ACK" in reply, (
            f"agent didn't reply ACK after recovery: {reply!r}"
        )

        sb = await sdk.get_session_sandbox(sid)
        sandbox_ref_2 = sb.get("sandbox_ref")
        assert sandbox_ref_2 and sandbox_ref_2 != sandbox_ref_1, (
            f"sandbox_ref did NOT change — cold-recovery did not provision "
            f"a new sandbox. old={sandbox_ref_1[:16]} new={sandbox_ref_2}"
        )
        print(f"   ✓ new daytona sandbox: {sandbox_ref_2[:16]}")

        # ── Phase 4: verify the marker survived ────────────────────────
        print(f"\n[phase 4] reading {marker_path} on the NEW sandbox...")
        ex2 = await sdk.session_sandbox_exec(
            sid,
            # ls the mount first so we can diagnose if it isn't even there
            f"ls -la /mnt/{mount_name}/ && cat {marker_path}",
            timeout=20,
        )
        assert ex2["exit_code"] == 0, (
            f"[phase 4] /mnt/{mount_name} NOT mounted on REPLACEMENT sandbox: "
            f"stdout={ex2.get('stdout')!r} stderr={ex2.get('stderr')!r}"
        )
        assert marker_content in ex2["stdout"], (
            f"[phase 4] marker content LOST across sandbox delete — "
            f"shared_mount data did NOT persist. Expected {marker_content!r} "
            f"in: {ex2['stdout']!r}"
        )
        print(f"   ✓ marker still readable: {ex2['stdout'].strip().splitlines()[-1]}")
        print("\n✅ shared_mounts data survives external sandbox delete")

    finally:
        try:
            if agent.session_id:
                await sdk.delete_session(agent.session_id)
        except Exception:
            pass
        await agent.aclose()
        await sdk.close()
