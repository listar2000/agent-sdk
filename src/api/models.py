"""Data models for the API server: agents, volumes, and runtime session state.

Sandbox identity is not modelled here — it lives in ``sessions.sandbox_state``
JSONB and is owned by ``api.sandbox.SessionPool`` (see
``api.sandbox.state.SandboxState`` for the discriminated union, and
 for the model)."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Literal


# ── Closed enums (Literal aliases) ──
# Closed set: narrowing the literal lets type-checkers catch silent-drop
# bugs (a dict literal that maps "daytona"+"docker" but forgets
# "unix_local"+"modal").
Provider = Literal["unix_local", "docker", "daytona", "modal"]


@dataclass
class AgentConfig:
    """Pure agent identity. No per-invocation or provisioning knobs — those
    live on the session (cwd, env, secrets) or sandbox (dockerfile,
    shared_mounts, root) rows.
    """
    agent_type: str = "opencode"
    model: str | None = None
    mcp_servers: dict | None = None
    skills: list | dict | None = None  # npx skills sources
    cli_tools: list | dict | None = None  # uv tool install sources
    # ACP dynamic config that gets re-applied on every fresh attach so
    # cold-recovery (Type-2) doesn't silently revert a caller's
    # set_mode / set_thought_level. Keep model on its own field above
    # for back-compat (it predates this group).
    mode: str | None = None              # "default" | "plan" | "bypassPermissions" | "acceptEdits" | ...
    thought_level: str | None = None     # "low" | "medium" | "high" — Claude's "thinking" config_id

    # Vendor-specific ACP ``extra_options`` is NOT here — it's session-scoped
    # (claude-agent-acp only reads ``_meta.<vendor>.options`` on session/new,
    # see ``acp_client._VENDOR_META_NAMESPACE``). Stored on the ``sessions``
    # row and threaded through ``acp_client.attach`` at session creation
    # time. Mirrors the ``workspace`` pattern (also session-scoped, not on
    # AgentConfig).

    def to_dict(self) -> dict[str, Any]:
        return {k: v for k, v in asdict(self).items() if v is not None}

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> AgentConfig:
        return cls(**{k: v for k, v in data.items() if k in _AGENT_CONFIG_FIELDS})


_AGENT_CONFIG_FIELDS = frozenset(AgentConfig.__dataclass_fields__)

# ── Session event type constants ──
EVT_USER_MESSAGE = "user_message"
EVT_ASSISTANT_MESSAGE = "assistant_message"
EVT_REASONING = "reasoning"
EVT_TOOL_CALL = "tool_call"
EVT_TOOL_RESULT = "tool_result"
EVT_USAGE = "usage"
EVT_ERROR = "error"

# ── Sandbox status constants ──
STATUS_RUNNING = "running"
STATUS_STOPPED = "stopped"
STATUS_ERROR = "error"
STATUS_CREATING = "creating"


@dataclass
class AgentRecord:
    id: str
    name: str | None = None
    config: AgentConfig = field(default_factory=AgentConfig)


# SandboxRecord removed: the sandboxes table is gone. Sandbox identity
# lives in ``sessions.sandbox_state`` JSONB owned by the SessionPool.


@dataclass
class VolumeRecord:
    id: str
    name: str
    provider: Provider
    provider_ref: str
    status: str = "ready"
    # ``supervisor_agent_types`` field deleted in Phase E of
    # the runtime-image-unification refactor. The DB column stays (now unused)
    # until the column-drop migration ships.


@dataclass
class LogEntry:
    id: int
    session_id: str
    agent_id: str
    event_type: str      # "user_message" | "assistant_message" | "tool_call" | "tool_result" | "usage" | "error"
    payload: dict        # JSON, structure varies by event_type
    created_at: float
