"""End-to-end behavior tests for ``Agent.reload(skills=...)``.

Two scenarios, both run against a real Daytona sandbox:

1. **Hot install + conversation continuity** (both ``claude`` and ``opencode``):
   Cold-create a session with NO skills. Plant a fact in turn 1
   ("my secret word is ``pineapple-NNN``"). Reload with a real skill
   bundle. Ask the agent (a) the secret word from turn 1 (must still
   remember after the supervisor restart — proves ACP ``session/load``
   restored history), (b) to list its skills (must include the newly
   installed ones — proves the install actually surfaced to the LLM).

2. **Persistence across Type-2 cold-recovery** (claude only — Daytona-
   specific external-delete path): Hot-install via ``/reload``, then
   externally delete the Daytona sandbox out-of-band, then send a
   follow-up message which forces a fresh provision. Assert the new
   sandbox has the skills — proves ``recipe.pre_start_commands`` was
   correctly rewritten so Type-2 re-runs the new install set.

Skipped unless the corresponding credentials are present:
  * Test 1 ``claude`` parametrization: ``CLAUDE_CODE_OAUTH_TOKEN``
  * Test 1 ``opencode`` parametrization: ``OPENROUTER_API_KEY``
  * Test 2: ``DAYTONA_API_KEY`` + ``CLAUDE_CODE_OAUTH_TOKEN``

All require ``DAYTONA_API_KEY`` + a running server on
``$AGENT_SERVER_URL`` (default ``http://localhost:7778``).

Run::

    .venv/bin/python -m pytest tests/test_skills_hot_reload.py -n auto -v -s
"""
from __future__ import annotations

import asyncio
import os
import sys
import uuid

import httpx
import pytest

_SRC = os.path.join(os.path.dirname(__file__), "..", "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

from agent_sdk import ApiClient, Agent  # noqa: E402
from tests._acp_runtimes import acp_runtime_param  # noqa: E402

DAYTONA_API_KEY = os.environ.get("DAYTONA_API_KEY")
OAUTH_TOKEN = os.environ.get("CLAUDE_CODE_OAUTH_TOKEN")
SERVER = os.environ.get("AGENT_SERVER_URL", "http://localhost:7778")


async def _server_up() -> bool:
    try:
        async with httpx.AsyncClient(timeout=2) as c:
            r = await c.get(f"{SERVER}/health")
            return r.status_code == 200
    except Exception:
        return False


# ── Skills + the assertion helpers ──────────────────────────────────────────
#
# ``rllm-org/hive`` installs two skills the agent surfaces by name:
# ``hive-create-task`` and ``hive-setup``. We match these tokens (lowercased)
# anywhere in the agent's reply rather than parsing structure, since claude
# and opencode format the listing differently.
_EXPECTED_SKILLS = ("hive-create-task", "hive-setup")


async def _list_skills_on_disk(sdk: ApiClient, session_id: str) -> set[str]:
    """Read ``~/.claude/skills/`` directly via ``POST /sandbox/exec``.

    This is the ground-truth check. We previously asked the agent to
    enumerate its skills via a text prompt, but haiku occasionally
    HALLUCINATED plausible Anthropic skill names ("update-config",
    "keybindings-help", "claude-api" — names it has seen in training
    data) rather than actually checking. The supervisor's exec runs
    in the same HOME the ACP child sees (after the supervisor.js
    HOME=args.root fix), so this returns what Claude / OpenCode
    would scan at boot.
    """
    r = await sdk._json(
        "POST", f"/sessions/{session_id}/sandbox/exec",
        json={
            "command": "ls -1 $HOME/.claude/skills/ 2>/dev/null || true",
            "timeout": 10,
        },
    )
    out = (r.get("stdout") or "").strip()
    return {line.strip() for line in out.splitlines() if line.strip()}


# ────────────────────────────────────────────────────────────────────────────
# Test 1 (parametrized claude+opencode): hot install + conversation continuity
# ────────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
@acp_runtime_param
async def test_reload_hot_installs_skill_preserves_conversation(acp_runtime):
    """Hot-install via ``Agent.reload`` must:
      A. Land the skill on disk so the LLM sees it in the same session.
      B. Preserve conversation history across the supervisor restart
         (release + cold_recover) — ACP ``session/load`` round-trip.
    """
    if acp_runtime is None:
        pytest.skip("acp_runtime credential missing")
    if not DAYTONA_API_KEY:
        pytest.skip("DAYTONA_API_KEY required")
    if not await _server_up():
        pytest.skip(f"no server at {SERVER}")

    # ``opencode`` haiku refuses to repeat a "secret word" citing safety
    # ("I am designed to protect sensitive information…"), so frame this
    # as a neutral marker. The recall assertion only cares that the
    # exact token survives the supervisor restart.
    marker = f"pineapple-{uuid.uuid4().hex[:6]}"

    agent = Agent(
        f"reload-skill-{uuid.uuid4().hex[:8]}",
        provider="daytona",
        api_url=SERVER,
        # ``acp_runtime`` carries agent_type + model + secrets for either
        # claude (oauth) or opencode (openrouter).
        **acp_runtime,
    )
    sdk = ApiClient(SERVER)
    try:
        # ── Cold-create with NO skills ──────────────────────────────────
        await agent.configure(model=acp_runtime["model"])
        assert agent.session_id and agent.sandbox_ref

        # Baseline: hive skills NOT yet on disk.
        baseline = await _list_skills_on_disk(sdk, agent.session_id)
        assert not (set(_EXPECTED_SKILLS) & baseline), (
            f"hive skills present BEFORE install: {sorted(baseline)}"
        )

        # ── Plant a marker for the conversation-continuity check ────────
        await asyncio.wait_for(
            agent.arun(
                f"I am testing message persistence across a restart. "
                f"The marker for this test is the literal string "
                f"{marker!r}. Please acknowledge by repeating the marker."
            ),
            timeout=180,
        )

        # ── Hot install via /reload ─────────────────────────────────────
        result = await asyncio.wait_for(
            agent.reload(skills=["rllm-org/hive"]),
            timeout=300,
        )
        assert result["status"] == "ok"
        assert result["skills"] == ["rllm-org/hive"]
        merged = result.get("pre_start_commands") or []
        assert any("rllm-org/hive" in c and "npx -y skills add" in c
                   for c in merged), (
            f"merged pre_start lacks the install line; got {merged!r}"
        )
        # The SDK should have mirrored the new value onto Agent.skills.
        assert agent.skills == ["rllm-org/hive"]

        # ── A. Skills landed in the agent's HOME/.claude/skills/ ────────
        # Ground truth: scan the directory directly via /sandbox/exec.
        # This also confirms the supervisor.js HOME=args.root fix landed
        # in the snapshot — without it, the install goes to /root and
        # this set comes back empty.
        on_disk = await _list_skills_on_disk(sdk, agent.session_id)
        missing = set(_EXPECTED_SKILLS) - on_disk
        assert not missing, (
            f"skills did NOT land in $HOME/.claude/skills/ after hot "
            f"reload — missing {sorted(missing)} from {sorted(on_disk)}. "
            f"Either the install exec failed or supervisor.js still "
            f"runs /v1/exec with the wrong HOME."
        )

        # ── B. Conversation history preserved across the restart ────────
        recall = await asyncio.wait_for(
            agent.arun(
                "What was the marker string I asked you to acknowledge "
                "earlier? Reply with just the marker, nothing else."
            ),
            timeout=180,
        )
        assert marker.lower() in recall.lower(), (
            f"conversation history LOST across reload — agent did not "
            f"recall {marker!r}. Reply: {recall!r}"
        )
    finally:
        try:
            if agent.session_id:
                await sdk.delete_session(agent.session_id)
        except Exception:
            pass
        await agent.aclose()
        await sdk.close()


# ────────────────────────────────────────────────────────────────────────────
# Test 2 (claude only — Daytona external-delete pattern): persistence proof
# ────────────────────────────────────────────────────────────────────────────


async def _external_delete_daytona(sandbox_ref: str) -> None:
    """Delete the Daytona sandbox bypassing the server — simulates an
    external GC / dashboard delete. The next request on the session is
    forced through Type-2 cold-recovery, which re-runs
    ``recipe.pre_start_commands``."""
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
            return  # gone


@pytest.mark.asyncio
@pytest.mark.skipif(
    not (DAYTONA_API_KEY and OAUTH_TOKEN),
    reason="DAYTONA_API_KEY + CLAUDE_CODE_OAUTH_TOKEN required",
)
async def test_reload_skills_survive_type2_cold_recovery():
    """Hot-installed skills must survive a sandbox replacement.

    Proves the ``/reload`` handler correctly rewrote
    ``sandbox_state.recipe.pre_start_commands`` — without that write,
    the new sandbox would replay the OLD install list (empty) and the
    agent would lose the skill.
    """
    if not await _server_up():
        pytest.skip(f"no server at {SERVER}")

    agent = Agent(
        f"reload-survive-{uuid.uuid4().hex[:8]}",
        provider="daytona",
        agent_type="claude",
        model="haiku",
        api_url=SERVER,
        oauth_token=OAUTH_TOKEN,
    )
    sdk = ApiClient(SERVER)
    try:
        await agent.configure(model="haiku")
        sid = agent.session_id
        sandbox_ref_1 = agent.sandbox_ref
        assert sid and sandbox_ref_1

        # Hot install.
        result = await asyncio.wait_for(
            agent.reload(skills=["rllm-org/hive"]),
            timeout=300,
        )
        assert result["status"] == "ok"

        # Sanity: install landed on the current sandbox right after reload.
        on_disk_pre = await _list_skills_on_disk(sdk, sid)
        assert not (set(_EXPECTED_SKILLS) - on_disk_pre), (
            f"baseline post-reload missing skills on disk: {sorted(on_disk_pre)}"
        )

        # ── Externally nuke the Daytona sandbox ─────────────────────────
        await _external_delete_daytona(sandbox_ref_1)
        await asyncio.sleep(2)

        # ── Force cold-recovery via a follow-up turn ────────────────────
        # Any session-touching call wakes the pool; arun goes through
        # the canonical recovery path. We don't care about the LLM's
        # reply — the assertion is on the post-recovery filesystem.
        await asyncio.wait_for(
            agent.arun("Reply with the single word OK."),
            timeout=300,
        )

        # Sanity: a brand-new sandbox was actually provisioned.
        sb = await sdk.get_session_sandbox(sid)
        sandbox_ref_2 = sb.get("sandbox_ref")
        assert sandbox_ref_2 and sandbox_ref_2 != sandbox_ref_1, (
            f"sandbox_ref unchanged after external delete: "
            f"old={sandbox_ref_1[:16]} new={sandbox_ref_2}"
        )

        on_disk_post = await _list_skills_on_disk(sdk, sid)
        missing = set(_EXPECTED_SKILLS) - on_disk_post
        assert not missing, (
            f"skills did NOT survive Type-2 cold-recovery — missing "
            f"{sorted(missing)} from {sorted(on_disk_post)}. Recipe "
            f"update from /reload likely didn't take."
        )
    finally:
        try:
            if agent.session_id:
                await sdk.delete_session(agent.session_id)
        except Exception:
            pass
        await agent.aclose()
        await sdk.close()
