"""Data models for the AFE server: agents, sandboxes, and runtime session state."""

from __future__ import annotations

import time
from dataclasses import asdict, dataclass, field
from typing import Any, Literal


# ── Closed enums (Literal aliases) ──
# Provider names and sandbox statuses are closed sets — narrowing them
# lets type-checkers catch the silent-drop bug class (e.g. a dict literal
# that maps "daytona"+"docker" but forgets "local"+"modal").
Provider = Literal["local", "docker", "daytona", "modal"]
SandboxStatus = Literal["running", "stopped", "error", "creating", "missing"]



@dataclass
class AgentConfig:
    """Pure agent identity. No per-invocation or provisioning knobs — those
    live on the session (cwd, env, secrets) or sandbox (dockerfile,
    shared_mounts, root) rows.
    """
    agent_type: str = "claude"
    model: str | None = None
    prompt: str | None = None
    tools: list[str] | None = None
    mcp_servers: dict | None = None
    skills: list | dict | None = None  # npx skills sources

    def to_dict(self) -> dict[str, Any]:
        return {k: v for k, v in asdict(self).items() if v is not None}

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> AgentConfig:
        return cls(**{k: v for k, v in data.items() if k in _AGENT_CONFIG_FIELDS})


_AGENT_CONFIG_FIELDS = frozenset(AgentConfig.__dataclass_fields__)

# Sentinel pushed to slow subscribers when they are kicked off for being full
_KICK_SENTINEL = object()

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


@dataclass
class SandboxRecord:
    id: str
    provider: Provider
    sandbox_ref: str
    status: SandboxStatus = "stopped"
    root: str = "/tmp"
    volume_id: str | None = None
    subpath: str | None = None
    # Host-side port the supervisor listens on. Populated for docker/local;
    # always NULL for Daytona (URL comes from the SDK-signed preview API).
    listen_port: int | None = None
    # Provisioning-time identity — frozen for the sandbox's lifetime. A
    # sandbox replacement (e.g. daytona unrecoverable error) reads these
    # from the row to rebuild an equivalent instance, so editing the agent
    # afterwards doesn't change how an existing sandbox is restarted.
    dockerfile: str | None = None
    shared_mounts: list[str] = field(default_factory=list)

    def derive_url(self) -> str:
        """Reconstruct the supervisor URL from the DB row.

        Used on cold-start when the in-memory ``_INSTANCES`` cache is empty.
        Requires ``listen_port`` to be set (docker/local). Daytona has no
        port-based URL; callers must consult the provider's SDK instead.
        """
        if self.listen_port is not None:
            return f"http://localhost:{self.listen_port}"
        raise NotImplementedError(
            f"URL derivation for provider '{self.provider}' requires external resolver"
        )


@dataclass
class VolumeRecord:
    id: str
    name: str
    provider: Provider
    provider_ref: str
    status: str = "ready"
    supervisor_agent_types: list[str] = field(default_factory=list)


class LogEntry:
    id: int
    session_id: str
    agent_id: str
    sandbox_id: str
    event_type: str      # "user_message" | "assistant_message" | "tool_call" | "tool_result" | "usage" | "error"
    payload: dict        # JSON, structure varies by event_type
    created_at: float
