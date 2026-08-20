"""Unit tests for the ACP model-name normalisation in
``api.acp_client._normalize_acp_model``.

Background: the pinned claude-agent-acp exposes semantic model aliases —
``default``, ``sonnet``, ``opus``, and ``haiku`` — and
``setSessionConfigOption`` rejects anything outside that set with
``-32603 Invalid value for config option model: <value>``. Hivespace
stores public-API IDs (``claude-sonnet-4-6``,
``claude-haiku-4-5-20251001``, ``claude-opus-4-6``) so without
normalisation every set_model call is noise. See the data-research /
Task Builder repro from 2026-05-04.

The mapping is a substring rule: the public-API ID always embeds exactly one
of ``sonnet`` / ``opus`` / ``haiku``. Keep the semantic choice explicit;
``default`` can move independently as the adapter updates its recommendation.
"""
from unittest.mock import AsyncMock

import pytest


from api.acp_client import AcpClient, _normalize_acp_model


def _norm_claude(model: str) -> str:
    return _normalize_acp_model(model, agent_type="claude")


def _norm_opencode(model: str) -> str:
    return _normalize_acp_model(model, agent_type="opencode")


class TestKnownAcpAliasesPassThrough:
    def test_default_passthrough(self):
        assert _norm_claude("default") == "default"

    def test_opus_passthrough(self):
        assert _norm_claude("opus") == "opus"

    def test_haiku_passthrough(self):
        assert _norm_claude("haiku") == "haiku"

    def test_uppercase_passthrough(self):
        assert _norm_claude("DEFAULT") == "default"
        assert _norm_claude("Opus") == "opus"


class TestPublicApiIdsCollapse:
    """Hivespace's ``_SUPPORTED_MODEL_IDS`` set as of 2026-05-04."""

    def test_claude_sonnet_4_6_to_sonnet(self):
        # Production agent.model value for the Task Builder repro.
        assert _norm_claude("claude-sonnet-4-6") == "sonnet"

    def test_claude_opus_4_6_to_opus(self):
        assert _norm_claude("claude-opus-4-6") == "opus"

    def test_claude_haiku_4_5_dated_to_haiku(self):
        assert _norm_claude("claude-haiku-4-5-20251001") == "haiku"


class TestOlderSnapshotsAndAliases:
    def test_claude_sonnet_4_5_dated(self):
        assert _norm_claude("claude-sonnet-4-5-20250929") == "sonnet"

    def test_bare_sonnet_alias(self):
        assert _norm_claude("sonnet") == "sonnet"

    def test_claude_3_haiku_dated(self):
        assert _norm_claude("claude-3-haiku-20240307") == "haiku"

    def test_anthropic_provider_prefix(self):
        # langchain-style "anthropic:claude-..." prefixed IDs.
        assert _norm_claude("anthropic:claude-sonnet-4-5") == "sonnet"
        assert _norm_claude("anthropic:claude-opus-4-6") == "opus"


class TestEdgeCases:
    def test_empty_falls_back_to_default(self):
        assert _norm_claude("") == "default"

    def test_unknown_falls_back_to_default(self):
        # ACP would reject anything else with -32603; "default" is the only
        # safe choice that lets the agent actually run.
        assert _norm_claude("gpt-4o") == "default"

    def test_whitespace_trimmed(self):
        assert _norm_claude("  claude-sonnet-4-6  ") == "sonnet"

    def test_haiku_priority_when_both_match(self):
        # Synthetic — no real Anthropic model contains two slot names,
        # but pin the precedence (sonnet > opus > haiku in our cascade)
        # so a future ID like "claude-sonnet-haiku-experimental" lands
        # deterministically.
        assert _norm_claude("claude-sonnet-haiku-x") == "sonnet"


class TestOpenCodePassThrough:
    def test_provider_model_id_is_unchanged(self):
        assert _norm_opencode("openai/gpt-5.5") == "openai/gpt-5.5"

    def test_whitespace_is_trimmed(self):
        assert _norm_opencode("  anthropic/claude-sonnet-4-6  ") == "anthropic/claude-sonnet-4-6"

    def test_empty_model_remains_empty(self):
        assert _norm_opencode("") == ""


@pytest.mark.asyncio
async def test_thought_level_uses_advertised_option():
    client = AcpClient("http://localhost:1")
    client.call = AsyncMock(return_value={})
    client._session_config_options["outer"] = [{
        "id": "runtime-defined-id",
        "name": "Reasoning effort",
        "description": "Controls thinking depth",
    }]
    try:
        await client.set_thought_level("outer", "high")
        client.call.assert_awaited_once_with(
            "outer",
            "session/set_config_option",
            {"configId": "runtime-defined-id", "value": "high"},
        )
    finally:
        await client.aclose()


@pytest.mark.asyncio
async def test_thought_level_uses_legacy_fallback_without_options():
    client = AcpClient("http://localhost:1")
    client.call = AsyncMock(return_value={})
    try:
        await client.set_thought_level("outer", "medium")
        client.call.assert_awaited_once_with(
            "outer",
            "session/set_config_option",
            {"configId": "effort", "value": "medium"},
        )
    finally:
        await client.aclose()


@pytest.mark.asyncio
async def test_model_verifies_applied_value_and_accepts_claude_alias_resolution():
    client = AcpClient("http://localhost:1")
    client.call = AsyncMock(return_value={
        "configOptions": [{
            "id": "model",
            "currentValue": "claude-sonnet-4-6",
        }],
    })
    try:
        await client.set_model("outer", "claude-sonnet-4-6", "claude")
    finally:
        await client.aclose()


@pytest.mark.asyncio
async def test_model_and_thought_level_raise_on_applied_mismatch():
    client = AcpClient("http://localhost:1")
    try:
        client.call = AsyncMock(return_value={"currentValue": "gpt-5.5"})
        with pytest.raises(RuntimeError, match="applied 'gpt-5.5'"):
            await client.set_model("outer", "gpt-5.6-sol", "codex")

        client.call = AsyncMock(return_value={"currentValue": "low"})
        with pytest.raises(RuntimeError, match="applied 'low'"):
            await client.set_thought_level("outer", "high", "codex")
    finally:
        await client.aclose()
