"""Shared ACP runtime parametrization for end-to-end tests.

Tests that drive a real ``Agent`` end-to-end need to cover both ``claude``
(claude-agent-acp + Anthropic OAuth) and ``opencode`` (opencode CLI +
OpenRouter API key). The choice of runtime + model + cred-env-var is
coupled, so this module exposes a single parametrize decorator that
yields valid (agent_type, model, secrets) triples for whichever
runtimes have credentials configured locally.

Usage::

    from tests._acp_runtimes import acp_runtime_param

    @acp_runtime_param
    async def test_basic_arun(acp_runtime):
        agent = Agent(
            "test-basic", provider="unix_local", api_url=BASE_URL,
            **acp_runtime,
        )
        ...

A test is auto-skipped for any runtime whose credential env var is
unset, so unconfigured environments don't surface as failures.
"""
from __future__ import annotations

import os

import pytest


_PARAMS: list = []

_oauth = os.environ.get("CLAUDE_CODE_OAUTH_TOKEN")
if _oauth:
    _PARAMS.append(
        pytest.param(
            {
                "agent_type": "claude",
                "model": "haiku",
                "secrets": {"CLAUDE_CODE_OAUTH_TOKEN": _oauth},
            },
            id="claude",
        )
    )
else:
    _PARAMS.append(
        pytest.param(
            None,
            id="claude",
            marks=pytest.mark.skip(reason="CLAUDE_CODE_OAUTH_TOKEN not set"),
        )
    )

_openrouter = os.environ.get("OPENROUTER_API_KEY")
if _openrouter:
    _PARAMS.append(
        pytest.param(
            {
                "agent_type": "opencode",
                "model": "openrouter/anthropic/claude-3.5-haiku",
                "secrets": {"OPENROUTER_API_KEY": _openrouter},
            },
            id="opencode",
        )
    )
else:
    _PARAMS.append(
        pytest.param(
            None,
            id="opencode",
            marks=pytest.mark.skip(reason="OPENROUTER_API_KEY not set"),
        )
    )


acp_runtime_param = pytest.mark.parametrize("acp_runtime", _PARAMS)


# Lightweight variant that yields just the ``agent_type`` string for tests
# (e.g. ``test_golden``) that build their own body
# via a helper rather than spreading ``acp_runtime`` into ``Agent(...)``.
# Same skip-on-missing-cred behaviour as ``acp_runtime_param``.
_AT_PARAMS: list = []
_AT_PARAMS.append(
    pytest.param(
        "claude",
        id="claude",
        marks=([] if _oauth else [pytest.mark.skip(reason="CLAUDE_CODE_OAUTH_TOKEN not set")]),
    )
)
_AT_PARAMS.append(
    pytest.param(
        "opencode",
        id="opencode",
        marks=([] if _openrouter else [pytest.mark.skip(reason="OPENROUTER_API_KEY not set")]),
    )
)

agent_type_param = pytest.mark.parametrize("agent_type", _AT_PARAMS)
