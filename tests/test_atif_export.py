from __future__ import annotations

import json

import pytest

from api.atif import build_atif_trajectory
from api.models import AgentConfig, AgentRecord, LogEntry


SESSION = {
    "id": "session-1",
    "agent_id": "agent-1",
    "sandbox_state": {
        "type": "modal",
        "recipe": {"agent_type": "codex"},
    },
}
AGENT = AgentRecord(
    id="agent-1",
    name="test agent",
    config=AgentConfig(agent_type="claude"),
)


def _entry(row_id: int, event_type: str, payload: dict) -> LogEntry:
    return LogEntry(
        id=row_id,
        session_id="session-1",
        agent_id="agent-1",
        event_type=event_type,
        payload=payload,
        created_at=1_700_000_000 + row_id,
    )


def test_build_atif_maps_complete_turn_and_deep_redacts():
    secret = "sk-" + "a" * 24
    opaque_secret = "dtn_" + "c" * 64
    trajectory = build_atif_trajectory(
        session=SESSION,
        agent=AGENT,
        entries=[
            _entry(1, "user_message", {
                "prompt_id": "p1", "text": "inspect",
                "attachments": [{"metadata": {"token": opaque_secret}}],
            }),
            _entry(2, "reasoning", {"prompt_id": "p1", "text": "think"}),
            _entry(3, "tool_call", {
                "prompt_id": "p1", "tool_call_id": "call-1",
                "tool_name": "shell",
                "args": {"nested": {"key": secret}},
            }),
            _entry(4, "tool_result", {
                "prompt_id": "p1", "tool_call_id": "call-1",
                "result": {"nested": secret}, "status": "completed",
            }),
            _entry(5, "usage", {"prompt_id": "p1", "usage": {
                "inputTokens": 10, "outputTokens": 4, "totalCostUsd": 0.01,
            }}),
            _entry(6, "assistant_message", {"prompt_id": "p1", "text": "done"}),
            _entry(7, "turn_end", {"prompt_id": "p1", "stop_reason": "end_turn"}),
        ],
    )

    assert trajectory["schema_version"] == "ATIF-v1.7"
    assert trajectory["agent"]["name"] == "codex"
    assert trajectory["agent"]["version"] == "unknown"
    assert [step["step_id"] for step in trajectory["steps"]] == [1, 2]

    user, turn = trajectory["steps"]
    assert user["source"] == "user"
    assert user["extra"]["attachments"][0]["metadata"]["token"] == "[REDACTED]"
    assert turn["source"] == "agent"
    assert turn["message"] == "done"
    assert turn["reasoning_content"] == "think"
    assert turn["tool_calls"][0]["arguments"] == {
        "nested": {"key": "[REDACTED]"},
    }
    assert turn["observation"]["results"][0]["source_call_id"] == "call-1"
    assert turn["observation"]["results"][0]["content"] == (
        '{"nested":"[REDACTED]"}'
    )
    assert turn["metrics"] == {
        "prompt_tokens": 10,
        "completion_tokens": 4,
        "cost_usd": 0.01,
    }
    assert trajectory["final_metrics"]["total_prompt_tokens"] == 10
    assert secret not in json.dumps(trajectory)
    assert opaque_secret not in json.dumps(trajectory)


def test_build_atif_does_not_guess_cumulative_metrics_or_orphan_links():
    secret = "sk-" + "b" * 24
    trajectory = build_atif_trajectory(
        session=SESSION,
        agent=AGENT,
        entries=[
            _entry(1, "user_message", {"prompt_id": "p1", "text": "go"}),
            _entry(2, "tool_result", {
                "prompt_id": "p1", "tool_call_id": "missing", "result": "output",
            }),
            _entry(3, "usage", {
                "prompt_id": "p1", "usage": {"amount": 0.2, "currency": "USD"},
            }),
            _entry(4, "error", {"prompt_id": "p1", "message": secret}),
        ],
    )

    turn = trajectory["steps"][1]
    result = turn["observation"]["results"][0]
    assert "source_call_id" not in result
    assert "metrics" not in turn
    assert turn["extra"]["usage"] == {"amount": 0.2, "currency": "USD"}
    assert turn["extra"]["errors"][0]["message"] == "[REDACTED]"
    assert trajectory["final_metrics"] == {"total_steps": 2}


def test_build_atif_reads_codex_usage_from_terminal_done_frame():
    trajectory = build_atif_trajectory(
        session=SESSION,
        agent=AGENT,
        entries=[
            _entry(1, "user_message", {"prompt_id": "p1", "text": "go"}),
            _entry(2, "assistant_message", {"prompt_id": "p1", "text": "done"}),
            _entry(3, "turn_end", {
                "prompt_id": "p1",
                "stop_reason": "end_turn",
                "usage": {
                    "inputTokens": 100,
                    "cachedReadTokens": 25,
                    "outputTokens": 40,
                    "thoughtTokens": 10,
                },
            }),
        ],
    )

    turn = trajectory["steps"][1]
    assert turn["metrics"] == {
        "prompt_tokens": 100,
        "cached_tokens": 25,
        "completion_tokens": 40,
        "reasoning_tokens": 10,
    }
    assert trajectory["final_metrics"]["total_reasoning_tokens"] == 10


def test_build_atif_rejects_empty_session():
    with pytest.raises(ValueError, match="no trajectory steps"):
        build_atif_trajectory(session=SESSION, agent=AGENT, entries=[])


@pytest.mark.asyncio
async def test_get_session_log_without_limit_uses_full_ordered_query(monkeypatch):
    from api import db

    captured: list[tuple[str, tuple]] = []

    async def all_rows(sql: str, params: tuple):
        captured.append((sql, params))
        return []

    monkeypatch.setattr(db, "_all", all_rows)
    assert await db.get_session_log("session-1", limit=None) == []
    sql, params = captured[0]
    assert "LIMIT" not in sql
    assert "ORDER BY id ASC" in sql
    assert params == ("session-1",)
