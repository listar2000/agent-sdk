"""Best-effort ATIF v1.7 export from the persisted session log."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from .models import AgentRecord, LogEntry
from .redact import redact_secrets


ATIF_VERSION = "ATIF-v1.7"
_SECRET_KEYS = {
    "api_key", "authorization", "credential", "credentials", "password",
    "secret", "token", "token_id", "token_secret",
}


def _deep_redact(value: Any) -> Any:
    if isinstance(value, str):
        return redact_secrets(value)
    if isinstance(value, list):
        return [_deep_redact(item) for item in value]
    if isinstance(value, dict):
        result = {}
        for key, item in value.items():
            safe_key = redact_secrets(str(key))
            normalized = safe_key.lower().replace("-", "_")
            sensitive = normalized in _SECRET_KEYS or normalized.endswith(
                ("_api_key", "_password", "_secret", "_token", "_token_id")
            )
            result[safe_key] = "[REDACTED]" if sensitive else _deep_redact(item)
        return result
    return value


def _timestamp(value: float) -> str:
    return (
        datetime.fromtimestamp(value, tz=timezone.utc)
        .isoformat()
        .replace("+00:00", "Z")
    )


def _text(value: str | None) -> str:
    return redact_secrets(value or "")


def _result_content(value: Any) -> str:
    value = _deep_redact(value)
    if isinstance(value, str):
        return value
    if value is None:
        return ""
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _usage(entries: list[LogEntry]) -> dict | None:
    for entry in reversed(entries):
        usage = entry.payload.get("usage")
        if isinstance(usage, dict) and entry.event_type in {"usage", "turn_end"}:
            return usage
    return None


def _metrics(usage: dict | None) -> dict | None:
    if not usage:
        return None
    metrics = {
        target: usage[source]
        for source, target in (
            ("inputTokens", "prompt_tokens"),
            ("outputTokens", "completion_tokens"),
            ("cachedReadTokens", "cached_tokens"),
            ("thoughtTokens", "reasoning_tokens"),
            ("totalCostUsd", "cost_usd"),
        )
        if source in usage
    }
    return metrics or None


def _agent_step(entries: list[LogEntry]) -> dict:
    text = "".join(_text(e.payload.get("text")) for e in entries
                   if e.event_type == "assistant_message")
    reasoning = "".join(_text(e.payload.get("text")) for e in entries
                        if e.event_type == "reasoning")

    calls: list[dict] = []
    call_ids: dict[str, str] = {}
    for entry in entries:
        if entry.event_type != "tool_call":
            continue
        source_id = entry.payload.get("tool_call_id")
        call_id = _text(source_id) if source_id else f"log-call-{entry.id}"
        if source_id:
            call_ids[source_id] = call_id
        calls.append({
            "tool_call_id": call_id,
            "function_name": _text(entry.payload.get("tool_name")) or "unknown",
            "arguments": _deep_redact(entry.payload.get("args") or {}),
            "extra": {"source_log_row_id": entry.id},
        })

    results: list[dict] = []
    for entry in entries:
        if entry.event_type != "tool_result":
            continue
        source_id = entry.payload.get("tool_call_id")
        result: dict[str, Any] = {
            "content": _result_content(entry.payload.get("result")),
            "extra": {"source_log_row_id": entry.id},
        }
        if source_id in call_ids:
            result["source_call_id"] = call_ids[source_id]
        results.append(result)

    prompt_id = entries[0].payload.get("prompt_id")
    extra: dict[str, Any] = {"source_log_row_ids": [e.id for e in entries]}
    if prompt_id:
        extra["prompt_id"] = _text(prompt_id)

    terminal = next(
        (e for e in reversed(entries) if e.event_type == "turn_end"),
        None,
    )
    errors = [e for e in entries if e.event_type == "error"]
    info = [e for e in entries if e.event_type == "session_info"]
    if terminal:
        stop_reason = terminal.payload.get("stop_reason")
        if stop_reason is not None:
            extra["stop_reason"] = _deep_redact(stop_reason)
    if errors:
        extra["errors"] = [_deep_redact(e.payload) for e in errors]
    if info:
        extra["session_info"] = [_deep_redact(e.payload) for e in info]

    usage = _usage(entries)
    metrics = _metrics(usage)
    if usage and not metrics:
        extra["usage"] = _deep_redact(usage)

    step: dict[str, Any] = {
        "timestamp": _timestamp(entries[0].created_at),
        "source": "agent",
        "message": text,
        "extra": extra,
    }
    if reasoning:
        step["reasoning_content"] = reasoning
    if calls:
        step["tool_calls"] = calls
    if results:
        step["observation"] = {"results": results}
    if metrics:
        step["metrics"] = metrics
    return step


def _final_metrics(steps: list[dict]) -> dict:
    final: dict[str, Any] = {"total_steps": len(steps)}
    agent_steps = [step for step in steps if step["source"] == "agent"]
    if not agent_steps or any("metrics" not in step for step in agent_steps):
        return final
    for source, target in (
        ("prompt_tokens", "total_prompt_tokens"),
        ("completion_tokens", "total_completion_tokens"),
        ("cached_tokens", "total_cached_tokens"),
        ("reasoning_tokens", "total_reasoning_tokens"),
        ("cost_usd", "total_cost_usd"),
    ):
        if all(source in step["metrics"] for step in agent_steps):
            final[target] = sum(step["metrics"][source] for step in agent_steps)
    return final


def build_atif_trajectory(
    *, session: dict, agent: AgentRecord, entries: list[LogEntry],
) -> dict:
    """Convert a complete, chronologically ordered session log to ATIF v1.7."""
    groups: dict[str, list[LogEntry]] = {}
    for entry in entries:
        prompt_id = entry.payload.get("prompt_id")
        key = f"prompt:{prompt_id}" if prompt_id else f"unscoped:{entry.id}"
        groups.setdefault(key, []).append(entry)

    steps: list[dict] = []
    for group in groups.values():
        for entry in group:
            if entry.event_type != "user_message":
                continue
            extra: dict[str, Any] = {"source_log_row_ids": [entry.id]}
            prompt_id = entry.payload.get("prompt_id")
            if prompt_id:
                extra["prompt_id"] = _text(prompt_id)
            if "attachments" in entry.payload:
                extra["attachments"] = _deep_redact(entry.payload["attachments"])
            steps.append({
                "timestamp": _timestamp(entry.created_at),
                "source": "user",
                "message": _text(entry.payload.get("text")),
                "extra": extra,
            })
        agent_entries = [e for e in group if e.event_type != "user_message"]
        if agent_entries:
            steps.append(_agent_step(agent_entries))

    if not steps:
        raise ValueError("session has no trajectory steps")
    for index, step in enumerate(steps, start=1):
        step["step_id"] = index

    state = session.get("sandbox_state") or {}
    recipe = state.get("recipe") or {}
    agent_type = recipe.get("agent_type") or agent.config.agent_type
    agent_extra: dict[str, Any] = {
        "agent_id": agent.id,
        "version_source": "unavailable",
    }
    if state.get("type"):
        agent_extra["provider"] = _deep_redact(state["type"])

    session_id = str(session["id"])
    return {
        "schema_version": ATIF_VERSION,
        "session_id": session_id,
        "trajectory_id": f"agent-sdk:{session_id}",
        "agent": {
            "name": _text(str(agent_type)),
            "version": "unknown",
            "extra": agent_extra,
        },
        "steps": steps,
        "notes": (
            "Reconstructed from agent-sdk session_log. The log does not capture "
            "agent version or per-LLM-call boundaries."
        ),
        "final_metrics": _final_metrics(steps),
        "extra": {
            "producer": "agent-sdk",
            "fidelity": "reconstructed",
            "source": "session_log",
            "source_log": {
                "event_count": len(entries),
                "first_id": entries[0].id,
                "last_id": entries[-1].id,
            },
            "redaction": "deep",
        },
    }
