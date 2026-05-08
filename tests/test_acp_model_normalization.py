"""Unit tests for the ACP model-name normalisation in
``api.acp_client._normalize_acp_model``.

Background: claude-agent-acp's ``getAvailableModels`` exposes only
three model IDs under OAuth — ``default``, ``opus``, ``haiku`` — and
``setSessionConfigOption`` rejects anything outside that set with
``-32603 Invalid value for config option model: <value>``. Hivespace
stores public-API IDs (``claude-sonnet-4-6``,
``claude-haiku-4-5-20251001``, ``claude-opus-4-6``) so without
normalisation every set_model call is noise. See the data-research /
Task Builder repro from 2026-05-04.

The mapping is a substring rule: the public-API ID always embeds
exactly one of ``sonnet`` / ``opus`` / ``haiku`` and ACP's ``default``
slot is the current Sonnet, so the mapping is semantically correct.
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from api.acp_client import _normalize_acp_model


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

    def test_claude_sonnet_4_6_to_default(self):
        # Production agent.model value for the Task Builder repro.
        assert _norm_claude("claude-sonnet-4-6") == "default"

    def test_claude_opus_4_6_to_opus(self):
        assert _norm_claude("claude-opus-4-6") == "opus"

    def test_claude_haiku_4_5_dated_to_haiku(self):
        assert _norm_claude("claude-haiku-4-5-20251001") == "haiku"


class TestOlderSnapshotsAndAliases:
    def test_claude_sonnet_4_5_dated(self):
        assert _norm_claude("claude-sonnet-4-5-20250929") == "default"

    def test_bare_sonnet_alias(self):
        assert _norm_claude("sonnet") == "default"

    def test_claude_3_haiku_dated(self):
        assert _norm_claude("claude-3-haiku-20240307") == "haiku"

    def test_anthropic_provider_prefix(self):
        # langchain-style "anthropic:claude-..." prefixed IDs.
        assert _norm_claude("anthropic:claude-sonnet-4-5") == "default"
        assert _norm_claude("anthropic:claude-opus-4-6") == "opus"


class TestEdgeCases:
    def test_empty_falls_back_to_default(self):
        assert _norm_claude("") == "default"

    def test_unknown_falls_back_to_default(self):
        # ACP would reject anything else with -32603; "default" is the only
        # safe choice that lets the agent actually run.
        assert _norm_claude("gpt-4o") == "default"

    def test_whitespace_trimmed(self):
        assert _norm_claude("  claude-sonnet-4-6  ") == "default"

    def test_haiku_priority_when_both_match(self):
        # Synthetic — no real Anthropic model contains two slot names,
        # but pin the precedence (sonnet > opus > haiku in our cascade)
        # so a future ID like "claude-sonnet-haiku-experimental" lands
        # deterministically.
        assert _norm_claude("claude-sonnet-haiku-x") == "default"


class TestOpenCodePassThrough:
    def test_provider_model_id_is_unchanged(self):
        assert _norm_opencode("openai/gpt-5.5") == "openai/gpt-5.5"

    def test_whitespace_is_trimmed(self):
        assert _norm_opencode("  anthropic/claude-sonnet-4-6  ") == "anthropic/claude-sonnet-4-6"

    def test_empty_model_remains_empty(self):
        assert _norm_opencode("") == ""
