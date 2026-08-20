"""Shared ACP runtime parametrization for end-to-end tests.

Tests that drive a real ``Agent`` end-to-end cover ``claude``
(claude-agent-acp + Anthropic OAuth), ``opencode`` (OpenRouter), and ``codex``
(the host's ``codex login`` session on unix_local). The choice of runtime,
model, and authentication is coupled, so this module exposes parametrizers
that yield valid triples for whichever runtimes are configured locally.

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
from api.providers._shared import _ACP_LOCAL_LOGIN_FILES


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

_codex_login = os.path.isfile(os.path.expanduser(
    f"~/{_ACP_LOCAL_LOGIN_FILES['codex']}"
))
if _codex_login:
    _PARAMS.append(
        pytest.param(
            {
                "agent_type": "codex",
                "model": None,
            },
            id="codex",
        )
    )
else:
    _PARAMS.append(
        pytest.param(
            None,
            id="codex",
            marks=pytest.mark.skip(reason="local codex login not found"),
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
_AT_PARAMS.append(
    pytest.param(
        "codex",
        id="codex",
        marks=(
            []
            if _codex_login
            else [pytest.mark.skip(reason="local codex login not found")]
        ),
    )
)

agent_type_param = pytest.mark.parametrize("agent_type", _AT_PARAMS)


# Native-inclusive variant for the lifecycle/recovery goldens that DON'T
# depend on a supervisor (hibernate/resume/delete/reap/workspace). Native is
# the first-party in-server loop — it has no ACP child, so it's deliberately
# absent from supervisor-specific goldens (wedged-container, supervisor-
# killed, agent-memory-tar). Live-verified on docker + daytona + modal; the
# golden's _require_provider skips only native×unix_local (transport not
# built yet).
_AT_PARAMS_NATIVE = list(_AT_PARAMS)
_AT_PARAMS_NATIVE.append(
    pytest.param(
        "native",
        id="native",
        marks=([] if _openrouter else [pytest.mark.skip(reason="OPENROUTER_API_KEY not set")]),
    )
)

agent_type_param_with_native = pytest.mark.parametrize("agent_type", _AT_PARAMS_NATIVE)
