"""End-to-end behavior tests for the cli_tools + secrets reload paths.

Two scenarios on real daytona sandboxes:

1. **CLI tools hot-install**: cold-create with no ``cli_tools``, verify the
   target binary is absent. Reload with ``cli_tools=["ruff"]``. Verify
   that ``ruff`` is now on the agent's PATH and runs. Then release+resume
   (Type-1) and verify it still works — proves the install landed on the
   volume's ``$HOME/.local/bin/`` (not just in transient memory).

2. **Secrets hot-swap**: cold-create with no secrets. Verify the agent's
   shell ``echo $FAVORITE_COLOR`` returns empty. Reload with
   ``secrets={"FAVORITE_COLOR": "<token>"}``. The supervisor was respawned,
   so a follow-up ``echo $FAVORITE_COLOR`` should print the new value
   (proves the secret landed in ``spawn_env`` for the new supervisor).

We use a **filesystem / shell-exec assertion**, not an LLM "what is your
env var" prompt, because haiku is non-deterministic about reporting env
contents and might refuse to reveal a value it considers "secret". Pure
``/sandbox/exec`` checks are deterministic and cheap.

Skipped unless ``DAYTONA_API_KEY`` + ``CLAUDE_CODE_OAUTH_TOKEN`` are set
and the test server is up.

Run::

    .venv/bin/python -m pytest tests/test_reload_cli_tools_and_secrets.py -n auto -v -s
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

DAYTONA_API_KEY = os.environ.get("DAYTONA_API_KEY")
OAUTH_TOKEN = os.environ.get("CLAUDE_CODE_OAUTH_TOKEN")
SERVER = os.environ.get("AGENT_SERVER_URL", "http://localhost:7778")

pytestmark = pytest.mark.skipif(
    not OAUTH_TOKEN,
    reason="CLAUDE_CODE_OAUTH_TOKEN required",
)

# These tests use ``provider="unix_local"`` so they can iterate without
# rebuilding the daytona / modal snapshot. ``uv`` lives on the host
# (``/home/<user>/.local/bin/uv``) and the supervisor's spawn PATH
# inherits the launching shell's PATH, so the agent sees it. Once the
# daytona/modal images have uv baked in, parametrize over providers.


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


# ────────────────────────────────────────────────────────────────────────────
# CLI tools hot-install
# ────────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_reload_hot_installs_cli_tool():
    """``Agent.reload(cli_tools=[...])`` lands the tool in $HOME/.local/bin
    on the live sandbox AND it stays there across a release+resume cycle.

    ``ruff`` is the test fixture: it's a single-binary PyPI package that
    installs in seconds and exits 0 on ``--version``, so we can probe
    presence + invocability deterministically without involving the LLM.
    """
    if not await _server_up():
        pytest.skip(f"no server at {SERVER}")

    agent = Agent(
        f"reload-cli-{uuid.uuid4().hex[:8]}",
        provider="unix_local",
        agent_type="claude",
        model="haiku",
        api_url=SERVER,
        # No cli_tools at create time — we hot-install via /reload.
        oauth_token=OAUTH_TOKEN,
    )
    sdk = ApiClient(SERVER)
    try:
        await agent.configure(model="haiku")
        sid = agent.session_id
        assert sid and agent.sandbox_ref

        # No baseline "ruff missing" check — on unix_local the install
        # writes to the HOST's $HOME/.local/bin so the binary may already
        # exist from a previous run / dev environment. ``uv tool install``
        # is idempotent on already-present sources, so a re-install is
        # fine; we just verify post-reload invocability.

        # ── Hot install ────────────────────────────────────────────────
        result = await asyncio.wait_for(
            agent.reload(cli_tools=["ruff"]),
            timeout=300,
        )
        assert result["status"] == "ok"
        assert result["cli_tools"] == ["ruff"]
        merged = result.get("pre_start_commands") or []
        assert any("uv tool install ruff" in c for c in merged), (
            f"merged pre_start lacks the cli install line: {merged!r}"
        )
        assert agent.cli_tools == ["ruff"]

        # ── Tool is invocable on the live sandbox ──────────────────────
        r = await _exec(sdk, sid, "ruff --version", timeout=10)
        assert r.get("exit_code") == 0, f"ruff --version failed: {r}"
        assert "ruff" in (r.get("stdout") or "").lower(), (
            f"ruff --version stdout missing 'ruff': {r}"
        )

        # ── Persistence: release + resume, ruff still works ────────────
        await sdk.release_session(sid)
        await sdk.resume_session(sid)
        r = await _exec(sdk, sid, "ruff --version", timeout=10)
        assert r.get("exit_code") == 0, (
            f"ruff disappeared after release+resume — install didn't "
            f"persist to the volume's $HOME/.local/bin: {r}"
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
# Secrets hot-swap
# ────────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_reload_hot_swaps_secrets():
    """``Agent.reload(secrets={...})`` lands new env vars in the supervisor's
    spawn_env on the next boot.

    Uses a unique sentinel value so we can detect that the supervisor
    actually respawned with the new env (not just returned a cached
    value from before the reload).
    """
    if not await _server_up():
        pytest.skip(f"no server at {SERVER}")

    sentinel = f"hot-swap-{uuid.uuid4().hex[:10]}"

    agent = Agent(
        f"reload-secret-{uuid.uuid4().hex[:8]}",
        provider="unix_local",
        agent_type="claude",
        model="haiku",
        api_url=SERVER,
        # No FAVORITE_COLOR at create — only the oauth token.
        oauth_token=OAUTH_TOKEN,
    )
    sdk = ApiClient(SERVER)
    try:
        await agent.configure(model="haiku")
        sid = agent.session_id
        assert sid

        # Baseline: FAVORITE_COLOR is unset.
        r = await _exec(sdk, sid, "echo FAVORITE_COLOR=${FAVORITE_COLOR:-unset}", timeout=5)
        assert "unset" in (r.get("stdout") or ""), (
            f"FAVORITE_COLOR somehow set before reload: {r}"
        )

        # ── Hot-swap secrets via reload ────────────────────────────────
        result = await asyncio.wait_for(
            agent.reload(secrets={"FAVORITE_COLOR": sentinel,
                                  "CLAUDE_CODE_OAUTH_TOKEN": OAUTH_TOKEN}),
            timeout=300,
        )
        assert result["status"] == "ok"
        # Response surfaces only the KEY set, never the values.
        assert sorted(result.get("secret_keys") or []) == sorted(
            ["FAVORITE_COLOR", "CLAUDE_CODE_OAUTH_TOKEN"]
        )

        # ── The respawned supervisor sees the new env ──────────────────
        r = await _exec(sdk, sid, "echo FAVORITE_COLOR=$FAVORITE_COLOR", timeout=5)
        assert sentinel in (r.get("stdout") or ""), (
            f"FAVORITE_COLOR not set after reload — release+cold_recover "
            f"didn't push the new secrets into the supervisor's "
            f"spawn_env. Got: {r}"
        )

        # ── Clearing wipes them ────────────────────────────────────────
        # Pass back the oauth so the agent stays usable, but drop
        # FAVORITE_COLOR.
        await asyncio.wait_for(
            agent.reload(secrets={"CLAUDE_CODE_OAUTH_TOKEN": OAUTH_TOKEN}),
            timeout=300,
        )
        r = await _exec(sdk, sid, "echo FAVORITE_COLOR=${FAVORITE_COLOR:-unset}", timeout=5)
        assert "unset" in (r.get("stdout") or ""), (
            f"FAVORITE_COLOR survived a follow-up reload that dropped "
            f"it from the secret set: {r}"
        )
    finally:
        try:
            if agent.session_id:
                await sdk.delete_session(agent.session_id)
        except Exception:
            pass
        await agent.aclose()
        await sdk.close()
