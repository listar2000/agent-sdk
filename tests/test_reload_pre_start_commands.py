"""End-to-end behavior tests for the pre_start_commands reload path.

Four scenarios on a real unix_local sandbox (same provider choice as
``test_reload_cli_tools_and_secrets.py`` — fast iteration, no snapshot
rebuild required):

1. **Hot-exec**: cold-create with no user pre_start. Reload with
   ``pre_start_commands=["echo HELLO > $HOME/marker.txt"]``. The
   marker file must exist on the live sandbox immediately AND survive
   a release+resume (Type-1) — proves the command actually ran on
   the live sandbox before the lease released, and that the resulting
   file is on the volume (not in ephemeral process memory).

2. **Non-idempotency / freshly-supplied only**: pre_start is run on
   reload ONLY when the field is in the body. A follow-up reload that
   does NOT pass ``pre_start_commands`` must NOT re-execute the
   previously-stored user commands. Asserted via an append-style
   counter file: 2 reloads, only the one that supplied the command
   appends a line.

3. **Clearing with []**: ``pre_start_commands=[]`` wipes the stored
   user portion (response confirms via ``user_pre_start_commands``)
   and produces a merged list with only cli/skill installs (none here,
   so empty).

4. **Validation**: non-list and non-string-element bodies are rejected
   with HTTP 400 before any state mutation.

We use **filesystem + shell-exec assertions**, not LLM prompts,
because pre_start effects are best observed deterministically via
``/sandbox/exec``.

Skipped unless ``CLAUDE_CODE_OAUTH_TOKEN`` is set and the test server
is up.

Run::

    .venv/bin/python -m pytest tests/test_reload_pre_start_commands.py -n auto -v -s
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

from agent_sdk import Agent, ApiClient  # noqa: E402

OAUTH_TOKEN = os.environ.get("CLAUDE_CODE_OAUTH_TOKEN")
SERVER = os.environ.get("AGENT_SERVER_URL", "http://localhost:7778")

pytestmark = pytest.mark.skipif(
    not OAUTH_TOKEN,
    reason="CLAUDE_CODE_OAUTH_TOKEN required",
)


async def _server_up() -> bool:
    try:
        async with httpx.AsyncClient(timeout=2) as c:
            r = await c.get(f"{SERVER}/health")
            return r.status_code == 200
    except Exception:
        return False


async def _exec(sdk: ApiClient, session_id: str, command: str, timeout: int = 30) -> dict:
    return await sdk._json(
        "POST", f"/sessions/{session_id}/sandbox/exec",
        json={"command": command, "timeout": timeout},
    )


def _new_agent(label: str) -> Agent:
    """Standard agent setup matching the cli_tools/secrets test pattern."""
    return Agent(
        f"{label}-{uuid.uuid4().hex[:8]}",
        provider="unix_local",
        agent_type="claude",
        model="haiku",
        api_url=SERVER,
        oauth_token=OAUTH_TOKEN,
    )


# ────────────────────────────────────────────────────────────────────────────
# 1. Hot-exec on live sandbox + persistence across release+resume
# ────────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_reload_hot_execs_pre_start_commands():
    """``Agent.reload(pre_start_commands=[...])`` runs the commands on the
    live sandbox immediately, and their filesystem effects survive a
    release+resume cycle (Type-1 recovery doesn't re-run pre_start, but
    files written to $HOME live on the persistent volume).
    """
    if not await _server_up():
        pytest.skip(f"no server at {SERVER}")

    sentinel = f"hello-{uuid.uuid4().hex[:10]}"
    marker_cmd = f"echo {sentinel} > $HOME/marker.txt"

    agent = _new_agent("reload-prestart-hot")
    sdk = ApiClient(SERVER)
    try:
        await agent.configure(model="haiku")
        sid = agent.session_id
        assert sid

        # Baseline: marker absent.
        r = await _exec(sdk, sid, "cat $HOME/marker.txt 2>/dev/null; echo __end__", timeout=5)
        assert sentinel not in (r.get("stdout") or ""), (
            f"marker.txt somehow has sentinel before reload: {r}"
        )

        # ── Hot-exec via reload ────────────────────────────────────────
        result = await asyncio.wait_for(
            agent.reload(pre_start_commands=[marker_cmd]),
            timeout=300,
        )
        assert result["status"] == "ok"
        # Raw user portion echoed back exactly.
        assert result.get("user_pre_start_commands") == [marker_cmd], (
            f"user_pre_start_commands mismatch in response: {result}"
        )
        # Merged list contains it too (no cli/skills here, so it's
        # alone — but assert containment rather than equality to stay
        # robust to future implicit additions).
        merged = result.get("pre_start_commands") or []
        assert marker_cmd in merged, (
            f"merged pre_start lacks the user cmd: {merged!r}"
        )

        # ── File exists on live sandbox right now ──────────────────────
        r = await _exec(sdk, sid, "cat $HOME/marker.txt", timeout=5)
        assert sentinel in (r.get("stdout") or ""), (
            f"marker.txt not created on live sandbox after reload: {r}"
        )

        # ── Persistence: release + resume, file still there ────────────
        await sdk.release_session(sid)
        await sdk.resume_session(sid)
        r = await _exec(sdk, sid, "cat $HOME/marker.txt", timeout=5)
        assert sentinel in (r.get("stdout") or ""), (
            f"marker.txt disappeared after release+resume — pre_start "
            f"didn't actually write to the volume: {r}"
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
# 2. Non-idempotency: pre_start re-runs only when freshly supplied
# ────────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_reload_does_not_rerun_pre_start_when_not_supplied():
    """Critical contract: ``live_cmds`` only appends user pre_start when
    the caller passed ``pre_start_commands`` in this request. A reload
    that updates only ``secrets`` must NOT re-execute the previously-
    stored user commands — caller code isn't assumed idempotent.

    Test pattern: an append-only counter file. After:

        reload(pre_start_commands=["echo X >> counter.txt"])   # ➀ runs
        reload(secrets={...})                                  # ➁ MUST NOT run ➀ again
        reload(pre_start_commands=["echo X >> counter.txt"])   # ➂ runs again

    the file should contain exactly 2 X lines (➀ and ➂), not 3.
    """
    if not await _server_up():
        pytest.skip(f"no server at {SERVER}")

    cmd = "echo X >> $HOME/counter.txt"

    agent = _new_agent("reload-prestart-noidem")
    sdk = ApiClient(SERVER)
    try:
        await agent.configure(model="haiku")
        sid = agent.session_id
        assert sid

        # Ensure clean slate.
        await _exec(sdk, sid, "rm -f $HOME/counter.txt", timeout=5)

        # ➀ First reload WITH pre_start — should append one line.
        await asyncio.wait_for(
            agent.reload(pre_start_commands=[cmd]),
            timeout=300,
        )
        r = await _exec(sdk, sid, "wc -l < $HOME/counter.txt", timeout=5)
        n_lines_1 = int((r.get("stdout") or "0").strip() or "0")
        assert n_lines_1 == 1, (
            f"first pre_start reload didn't produce 1 line: got {n_lines_1}, raw={r}"
        )

        # ➁ Second reload WITHOUT pre_start — line count must stay at 1.
        await asyncio.wait_for(
            agent.reload(secrets={"CLAUDE_CODE_OAUTH_TOKEN": OAUTH_TOKEN}),
            timeout=300,
        )
        r = await _exec(sdk, sid, "wc -l < $HOME/counter.txt", timeout=5)
        n_lines_2 = int((r.get("stdout") or "0").strip() or "0")
        assert n_lines_2 == 1, (
            f"reload without pre_start re-ran the stored user pre_start "
            f"(expected 1 line, got {n_lines_2}). live_cmds is leaking "
            f"the stored user portion across non-pre_start reloads."
        )

        # ➂ Third reload, pre_start freshly supplied again — line count → 2.
        await asyncio.wait_for(
            agent.reload(pre_start_commands=[cmd]),
            timeout=300,
        )
        r = await _exec(sdk, sid, "wc -l < $HOME/counter.txt", timeout=5)
        n_lines_3 = int((r.get("stdout") or "0").strip() or "0")
        assert n_lines_3 == 2, (
            f"freshly-supplied pre_start didn't re-run (expected 2 lines, "
            f"got {n_lines_3}). Hot-exec is gated incorrectly."
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
# 3. Clearing the user portion with []
# ────────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_reload_clears_pre_start_with_empty_list():
    """``pre_start_commands=[]`` wipes the stored user portion.

    After clearing, the response's ``user_pre_start_commands`` is ``[]``
    and the merged list contains only cli/skill installs (here: none,
    since neither was configured on the agent).
    """
    if not await _server_up():
        pytest.skip(f"no server at {SERVER}")

    sentinel_cmd = "echo set > /dev/null"

    agent = _new_agent("reload-prestart-clear")
    sdk = ApiClient(SERVER)
    try:
        await agent.configure(model="haiku")
        sid = agent.session_id
        assert sid

        # Set a non-trivial user pre_start so we can observe it being cleared.
        result_set = await asyncio.wait_for(
            agent.reload(pre_start_commands=[sentinel_cmd]),
            timeout=300,
        )
        assert result_set.get("user_pre_start_commands") == [sentinel_cmd]

        # Now clear it.
        result_clear = await asyncio.wait_for(
            agent.reload(pre_start_commands=[]),
            timeout=300,
        )
        assert result_clear["status"] == "ok"
        assert result_clear.get("user_pre_start_commands") == [], (
            f"empty-list reload didn't clear user pre_start: {result_clear}"
        )
        # No cli_tools / skills configured → merged is also empty.
        assert result_clear.get("pre_start_commands") == [], (
            f"merged pre_start nonempty after clear with no cli/skills: "
            f"{result_clear}"
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
# 4. Body validation
# ────────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_reload_validates_pre_start_commands_type():
    """Server rejects non-list and non-string-element bodies with 400
    BEFORE mutating any state.

    Two bad shapes:
      - ``"a string"``  → not a list at all
      - ``["ok", 123]`` → list, but contains a non-string element
    """
    if not await _server_up():
        pytest.skip(f"no server at {SERVER}")

    agent = _new_agent("reload-prestart-validate")
    sdk = ApiClient(SERVER)
    try:
        await agent.configure(model="haiku")
        sid = agent.session_id
        assert sid

        for bad_body, why in [
            ({"pre_start_commands": "echo hi"}, "string, not list"),
            ({"pre_start_commands": ["ok", 123]}, "non-string element"),
            ({"pre_start_commands": [None]}, "None element"),
        ]:
            with pytest.raises(httpx.HTTPStatusError) as ei:
                await sdk._json(
                    "POST", f"/sessions/{sid}/reload", json=bad_body,
                    timeout=httpx.Timeout(15.0, read=15.0),
                )
            assert ei.value.response.status_code == 400, (
                f"expected 400 for {why}, got "
                f"{ei.value.response.status_code}: {ei.value.response.text}"
            )
    finally:
        try:
            if agent.session_id:
                await sdk.delete_session(agent.session_id)
        except Exception:
            pass
        await agent.aclose()
        await sdk.close()
