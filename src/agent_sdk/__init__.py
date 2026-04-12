"""Generic async agent client SDK.

Works with any server implementing the agent orchestration REST API.
"""

__version__ = "0.5.0"

from .client import (
    Agent, UsageStats,
    CLAUDE, CODEX, OPENCODE, AMP, PI, CURSOR, MOCK, AGENT_TYPES,
    LOCAL, DOCKER, DAYTONA, PROVIDERS,
)
from .errors import (
    AgentSDKError, AgentConnectionError, AgentNotRegisteredError,
    SandboxError, AgentBusyError, AgentTimeoutError, PromptError, StreamError,
)
from .persist import SessionRecord, SqliteSessionDriver

__all__ = [
    "__version__",
    "Agent",
    "UsageStats",
    "CLAUDE", "CODEX", "OPENCODE", "AMP", "PI", "CURSOR", "MOCK", "AGENT_TYPES",
    "LOCAL", "DOCKER", "DAYTONA", "PROVIDERS",
    "AgentSDKError", "AgentConnectionError", "AgentNotRegisteredError",
    "SandboxError", "AgentBusyError", "AgentTimeoutError", "PromptError", "StreamError",
    "SessionRecord",
    "SqliteSessionDriver",
]
