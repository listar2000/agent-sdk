"""Opt-in live checks for `/goal` through the public Agent stream.

Run against a local server after installing the pinned supervisor runtimes::

    RUN_ACP_GOAL_TESTS=1 uv run --group dev pytest \
        tests/test_acp_goal_integration.py -n 0 -v

The test makes real model calls and is skipped by default.
"""

from __future__ import annotations

import os

import httpx
import pytest

from agent_sdk.client import Agent
from api.providers._shared import _ACP_LOCAL_LOGIN_FILES


BASE_URL = os.environ.get("ACP_TEST_URL", "http://localhost:7778")


def _server_reachable() -> bool:
    try:
        return httpx.get(f"{BASE_URL}/health", timeout=3).status_code == 200
    except Exception:
        return False


def _claude_runtime_case():
    oauth = os.environ.get("CLAUDE_CODE_OAUTH_TOKEN")
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if oauth:
        return {"secrets": {"CLAUDE_CODE_OAUTH_TOKEN": oauth}, "model": "haiku"}
    if api_key:
        return {"secrets": {"ANTHROPIC_API_KEY": api_key}, "model": "haiku"}
    return None


def _local_login_runtime_case():
    login_file = _ACP_LOCAL_LOGIN_FILES["codex"]
    if os.path.isfile(os.path.expanduser(f"~/{login_file}")):
        return {}
    return None


_RUNTIME_CASES = {
    "claude": (_claude_runtime_case, None),
    "codex": (_local_login_runtime_case, "codex"),
}


pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.environ.get("RUN_ACP_GOAL_TESTS") != "1",
        reason="set RUN_ACP_GOAL_TESTS=1 to make live model calls",
    ),
    pytest.mark.skipif(not _server_reachable(), reason=f"server unavailable at {BASE_URL}"),
]


@pytest.mark.asyncio
@pytest.mark.parametrize("agent_type", ["claude", "codex"])
async def test_goal_command_is_advertised_and_completes(agent_type, tmp_path):
    runtime_factory, goal_meta_namespace = _RUNTIME_CASES[agent_type]
    runtime = runtime_factory()
    if runtime is None:
        pytest.skip(f"credentials for {agent_type} are not configured")

    objective = (
        f"Write the exact text GOAL_OK to {tmp_path.name}-goal-smoke.txt, "
        "verify the file, then mark this goal complete."
    )
    agent = Agent(
        f"goal-smoke-{agent_type}",
        agent_type=agent_type,
        provider="unix_local",
        api_url=BASE_URL,
        **runtime,
    )
    events: list[dict] = []
    try:
        async for event in agent.astream(f"/goal {objective}"):
            events.append(dict(event))
        command_names = {
            command.get("name")
            for event in events if event.get("type") == "commands"
            for command in event.get("commands", [])
        }
        assert "goal" in command_names
        assert any(event.get("type") == "done" for event in events)

        if goal_meta_namespace:
            goal_events = [
                event for event in events
                if event.get("type") == "session_info"
                and isinstance(
                    event.get("raw", {}).get("_meta", {}).get(
                        goal_meta_namespace
                    ),
                    dict,
                )
                and "goal" in event["raw"]["_meta"][goal_meta_namespace]
            ]
            assert goal_events, "goal state was not exposed over session_info_update"
            assert any(
                isinstance(
                    event["raw"]["_meta"][goal_meta_namespace].get("goal"),
                    dict,
                )
                and objective
                in event["raw"]["_meta"][goal_meta_namespace]["goal"].get(
                    "objective", "",
                )
                for event in goal_events
            )
    finally:
        if agent.session_id:
            try:
                await agent._api.delete_session(agent.session_id)
            except Exception:
                pass
        await agent.aclose()
