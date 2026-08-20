"""Unit coverage for the codex ACP harness support (no network, no sandbox).

Covers the pieces added to run task-generation agents on codex (GPT-5.5) instead
of claude: the npm spec / bin resolution, the bypass-mode + auth-required
heuristic, thought_level config-id routing, done-frame usage extraction, and the
client's env-based codex-credential auto-forward.
"""
from __future__ import annotations

import asyncio
import json

from agent_sdk.client import Agent, UsageStats
from api.acp_client import _BYPASS_MODE, AcpClient, _is_auth_required
from api.providers._shared import AUTH_KEYS, _ACP_NPM_SPECS, _acp_bin_name, _spec_package_name
from api.sse import _extract_done_usage, parse_acp_event


def test_codex_acp_spec_bin_and_auth_keys():
    assert _ACP_NPM_SPECS["codex"].startswith("@agentclientprotocol/codex-acp@")
    assert _spec_package_name(_ACP_NPM_SPECS["codex"]) == "@agentclientprotocol/codex-acp"
    assert _acp_bin_name("codex") == "codex-acp"
    assert {"CODEX_ACCESS_TOKEN", "CODEX_API_KEY"} <= AUTH_KEYS  # stripped from ambient unless forwarded


def test_bypass_mode_and_auth_required_heuristic():
    assert _BYPASS_MODE["codex"] == "agent-full-access" and _BYPASS_MODE["claude"] == "bypassPermissions"
    assert _is_auth_required(RuntimeError("ACP error [-32000]: authenticate required"))
    assert _is_auth_required(RuntimeError("not authenticated"))
    assert not _is_auth_required(RuntimeError("ACP error [-32603]: Internal error"))


def test_set_thought_level_config_id_routes_by_agent_type():
    client = AcpClient("http://localhost:1")
    captured: list[tuple[str, dict]] = []

    async def fake_call(session_id, method, params=None, *, notify=False):
        captured.append((method, params or {}))
        return {}

    client.call = fake_call  # type: ignore[assignment]

    async def go():
        await client.set_thought_level("s", "high", "codex")
        await client.set_thought_level("s", "high", "claude")
        await client.aclose()

    asyncio.run(go())
    assert captured[0][1]["configId"] == "reasoning_effort" and captured[0][1]["value"] == "high"  # codex
    assert captured[1][1]["configId"] == "effort"  # pinned claude-agent-acp


def test_extract_done_usage_probes_common_locations():
    assert _extract_done_usage({"stopReason": "end_turn", "usage": {"input_tokens": 10}}) == {"input_tokens": 10}
    assert _extract_done_usage({"stopReason": "end_turn", "_meta": {"usage": {"output_tokens": 5}}}) == {"output_tokens": 5}
    # codex-acp also mirrors usage under _meta.quota.token_count (snake_case) — cheap forward-compat probe
    assert _extract_done_usage({"stopReason": "end_turn", "_meta": {"quota": {"token_count": {"input_tokens": 7}}}}) == {"input_tokens": 7}
    assert _extract_done_usage({"stopReason": "end_turn"}) is None  # no usage on the frame -> None (claude path)


def test_codex_done_frame_usage_lands_in_usage_stats():
    # The REAL codex camelCase PromptResponse shape end-to-end: a done frame -> parse_acp_event ->
    # UsageStats. inputTokens is already net-of-cache; all FIVE numbers must land (this closes the
    # fixture-shape gap: previously cachedReadTokens + thoughtTokens were dropped).
    usage = {"totalTokens": 1500, "inputTokens": 1000, "cachedReadTokens": 200, "outputTokens": 500, "thoughtTokens": 300}
    block = "data: " + json.dumps({"id": "1", "result": {"stopReason": "end_turn", "usage": usage}})
    ev = parse_acp_event(block, rpc_id=None)
    assert ev["type"] == "done" and ev["usage"] == usage

    stats = UsageStats()
    stats.update(ev["usage"])
    assert stats.input_tokens == 1000 and stats.output_tokens == 500 and stats.total_tokens == 1500
    assert stats.cached_input_tokens == 200 and stats.thought_tokens == 300


def test_usage_stats_accumulates_cache_and_thought_across_calls_snake_and_camel():
    stats = UsageStats()
    stats.update({"inputTokens": 100, "outputTokens": 50, "cachedReadTokens": 20, "thoughtTokens": 10})  # codex camelCase
    stats.update({"input_tokens": 200, "output_tokens": 30, "cache_read_input_tokens": 40, "thought_tokens": 5})  # claude/snake
    assert stats.input_tokens == 300 and stats.output_tokens == 80 and stats.total_tokens == 380
    assert stats.cached_input_tokens == 60 and stats.thought_tokens == 15  # additive across both shapes


def test_codex_credentials_auto_forwarded_only_for_codex(monkeypatch):
    monkeypatch.setenv("CODEX_ACCESS_TOKEN", "pat-xyz")
    monkeypatch.delenv("CODEX_API_KEY", raising=False)
    # localhost api_url keeps the plaintext-credential guard happy
    codex = Agent("t", agent_type="codex", provider="daytona", api_url="http://localhost:7811")
    assert codex._secrets_payload().get("CODEX_ACCESS_TOKEN") == "pat-xyz"
    claude = Agent("t", agent_type="claude", provider="daytona", api_url="http://localhost:7811")
    assert "CODEX_ACCESS_TOKEN" not in claude._secrets_payload()  # not forwarded for non-codex harnesses


def test_thought_level_rides_config_data(monkeypatch):
    monkeypatch.delenv("CODEX_ACCESS_TOKEN", raising=False)
    assert "thought_level" in Agent._CLONABLE_FIELDS
    a = Agent("t", agent_type="codex", provider="daytona", api_url="http://localhost:7811", thought_level="high")
    assert a._registration_payload()["thought_level"] == "high"
    assert a.clone().thought_level == "high"  # survives clone
