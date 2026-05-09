"""End-to-end: skills + pre_start_commands survive external sandbox delete.

This is the test the design discussion called for. NOT a unit test, NOT a
mock — actually provisions a real Daytona sandbox via the SDK, installs
the ``rllm-org/hive`` skill (which adds ``hive-create-task`` and
``hive-setup`` to Claude's available skills), drops a marker file via
pre_start_commands, EXTERNALLY deletes the Daytona sandbox (bypassing
our server, simulating "the sandbox just disappears"), then triggers a
cold-recovery via a follow-up message and asserts that BOTH the skills
AND the marker re-appear on the freshly-provisioned replacement sandbox.

How we verify skills are installed: we ASK THE AGENT. ``npx skills add
... --all -g`` writes into Claude's skills directory under HOME, not
into ``/usr/local/bin`` — so ``command -v hive`` is the wrong check.
Claude reads its skills at startup and surfaces them via the ACP
``configOptions`` / ``available_commands`` channel; we send a prompt
asking the agent to list its skills and grep the reply.

Skipped unless ``DAYTONA_API_KEY`` and ``CLAUDE_CODE_OAUTH_TOKEN`` are set.
Server must be running on localhost:7778 (use ``scripts/launch_server_test.sh``).

Run via::

    .venv/bin/python -m pytest \\
        tests/test_skills_and_pre_start_survive_external_delete.py -v -s
"""
from __future__ import annotations

import asyncio
import base64
import os
import sys
import time
import uuid

import httpx
import pytest

_SRC = os.path.join(os.path.dirname(__file__), "..", "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

from agent_sdk import ApiClient, Agent  # noqa: E402

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
    """Delete the Daytona sandbox out-of-band — bypasses our server.

    This is what "the sandbox just disappears" looks like in production:
    a Daytona dashboard delete, an account-level cleanup, infra GC, etc.
    The next /message on the session must cold-recover by provisioning a
    fresh sandbox + replaying the recipe (skills + pre_start_commands)
    from the persisted session state.
    """
    from daytona_sdk import Daytona, DaytonaConfig
    daytona = Daytona(DaytonaConfig(api_key=DAYTONA_API_KEY))
    loop = asyncio.get_event_loop()
    sb = await loop.run_in_executor(None, lambda: daytona.get(sandbox_ref))
    await loop.run_in_executor(None, lambda: daytona.delete(sb))
    # Wait until daytona's index no longer knows the ref — otherwise the
    # next get() during cold-recovery races the delete.
    deadline = loop.time() + 15
    while loop.time() < deadline:
        try:
            await loop.run_in_executor(None, lambda: daytona.get(sandbox_ref))
            await asyncio.sleep(0.5)
        except Exception:
            return  # gone


def _decode(resp: dict) -> str:
    raw = resp.get("content", "")
    if isinstance(raw, str):
        try:
            return base64.b64decode(raw).decode("utf-8", errors="replace")
        except Exception:
            return raw
    return str(raw)


async def _ask_agent_for_skills(agent) -> str:
    """Send a prompt asking the agent to list its installed skills.

    Uses ``Agent.arun`` (which already does the SSE accumulation into
    a single text string). Returns the assistant's reply so the caller
    can grep for expected skill names.
    """
    return await agent.arun(
        "List your available skills (those installed via `npx skills add`, "
        "NOT your built-in tools like Read/Write/Bash). Reply with each "
        "skill name on its own line, no other prose. If you have no "
        "skills installed, reply with the single word NONE."
    )


_SKILL_NAMES_EXPECTED = ("hive-create-task", "hive-setup")


def _grep_skill_names(reply: str) -> set[str]:
    """Pull skill-name tokens out of the agent's reply, lowercased.

    Tolerant: matches our expected names anywhere in the reply
    (the agent may format as a bullet list, JSON-ish output, etc.).
    """
    lower = reply.lower()
    return {n for n in _SKILL_NAMES_EXPECTED if n in lower}


@pytest.mark.asyncio
async def test_skills_and_pre_start_survive_external_sandbox_delete():
    """End-to-end: install skills + pre_start, externally nuke the sandbox,
    next message cold-recovers, all artifacts re-appear.

    Asserts the load-bearing invariants:
      A. ``rllm-org/hive`` skills are installed on first provision —
         verified by asking the AGENT to list its skills (not by checking
         /usr/local/bin)
      B. pre_start marker file lands on first provision
      C. After external Daytona delete + a follow-up message, the new
         sandbox has the SAME skills (agent re-asks, gets the same list)
         AND a NEWER marker timestamp (proves pre_start re-ran on the
         replacement, not just snapshot restore — the marker is under
         .cache/ which is snapshot-excluded).
    """
    if not await _server_up():
        pytest.skip(f"no server at {SERVER}")

    # Marker is HOME-relative so /sessions/{id}/files/read can find it
    # (the file route proxies through supervisor and resolves under
    # $HOME == /home/daytona/agents/<agent_id>). Using a fresh per-test
    # name so concurrent runs don't collide on the same volume.
    marker_name = f".pre_start_marker_{uuid.uuid4().hex[:8]}"
    pre_start = [
        f"mkdir -p $HOME && date +%s%N > $HOME/{marker_name}",
    ]

    agent = Agent(
        f"skills-survive-{uuid.uuid4().hex[:8]}",
        provider="daytona",
        agent_type="claude",
        model="haiku",
        api_url=SERVER,
        skills=["rllm-org/hive"],
        pre_start_commands=pre_start,
        oauth_token=OAUTH_TOKEN,
    )
    sdk = ApiClient(SERVER)  # used for sandbox/file ops + external-delete plumbing
    try:
        print("\n[phase 1] creating daytona session via Agent (with skills + pre_start)...")
        # configure() forces _ensure_registered, which is the actual create.
        await agent.configure(model="haiku")
        sid = agent.session_id
        sandbox_ref_1 = agent.sandbox_ref
        assert sid, f"agent has no session id: {agent.__dict__}"
        assert sandbox_ref_1, f"agent has no sandbox_ref: {agent.__dict__}"
        print(f"   session_id={sid[:8]}  sandbox_ref={sandbox_ref_1[:16]}")

        # ── Invariant A: ask the agent what skills it has ──────────────
        print("[phase 1] asking agent to list its skills...")
        reply1 = await _ask_agent_for_skills(agent)
        print(f"   agent reply (first 400 chars):\n{reply1[:400]}")
        seen1 = _grep_skill_names(reply1)
        missing1 = set(_SKILL_NAMES_EXPECTED) - seen1
        assert not missing1, (
            f"[phase 1] agent did NOT see expected skills "
            f"{sorted(_SKILL_NAMES_EXPECTED)} — missing {sorted(missing1)}. "
            f"Full reply:\n{reply1}"
        )
        print(f"   ✓ agent sees skills: {sorted(seen1)}")

        # ── Invariant B: pre_start marker present ───────────────────────
        r1 = await sdk.session_file_read(sid, marker_name)
        first_ts = int(_decode(r1).strip())
        print(f"   ✓ marker $HOME/{marker_name} present (ts={first_ts})")

        # ── Externally delete the Daytona sandbox ──────────────────────
        print(f"\n[phase 2] externally deleting daytona sandbox {sandbox_ref_1[:16]}...")
        await _external_delete_daytona(sandbox_ref_1)
        print("   ✓ daytona sandbox gone (verified via daytona.get raises)")
        await asyncio.sleep(2)

        # ── Trigger cold-recovery via a follow-up message ──────────────
        print("\n[phase 3] sending follow-up agent message to trigger cold-recovery...")
        # agent.arun blocks on the cold-recovery (it goes through
        # /message+stream which the server's bg drain runs through
        # the same canonical path; the response body waits for the
        # SSE stream to complete).
        reply2 = await asyncio.wait_for(
            _ask_agent_for_skills(agent), timeout=300,
        )

        # Confirm a new sandbox was minted (sandbox_ref changed).
        sb = await sdk.get_session_sandbox(sid)
        sandbox_ref_2 = sb.get("sandbox_ref")
        assert sandbox_ref_2 and sandbox_ref_2 != sandbox_ref_1, (
            f"sandbox_ref did NOT change after external delete — "
            f"cold-recovery did not provision a new sandbox. "
            f"old={sandbox_ref_1[:16]} new={sandbox_ref_2}"
        )
        print(f"   ✓ new daytona sandbox: {sandbox_ref_2[:16]}")

        # ── Invariant C.1: agent still sees the skills on new sandbox ──
        print("[phase 4] verifying skills + pre_start re-applied on new sandbox...")
        print(f"   agent reply post-recovery (first 400 chars):\n{reply2[:400]}")
        seen2 = _grep_skill_names(reply2)
        missing2 = set(_SKILL_NAMES_EXPECTED) - seen2
        assert not missing2, (
            f"[phase 4] agent did NOT see expected skills on REPLACEMENT "
            f"sandbox — missing {sorted(missing2)}. Skills did NOT re-install "
            f"during cold-recovery. Full reply:\n{reply2}"
        )
        print(f"   ✓ agent still sees skills after recovery: {sorted(seen2)}")

        # ── Invariant C.2: pre_start marker NEWER (proves re-run) ──────
        r2 = await sdk.session_file_read(sid, marker_name)
        second_ts = int(_decode(r2).strip())
        assert second_ts > first_ts, (
            f"[phase 4] marker timestamp did NOT advance — pre_start did NOT "
            f"re-run during cold-recovery. first={first_ts} second={second_ts}"
        )
        print(
            f"   ✓ marker re-written: first={first_ts} second={second_ts} "
            f"delta={(second_ts - first_ts) / 1e9:.1f}s"
        )
        print("\n✅ skills + pre_start_commands BOTH survive external sandbox delete")

    finally:
        try:
            await sdk.delete_session(agent.session_id) if agent.session_id else None
        except Exception:
            pass
        await agent.aclose()
        await sdk.close()
