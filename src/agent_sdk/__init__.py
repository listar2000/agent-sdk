"""Generic async agent client SDK.

Works with any server implementing the agent orchestration REST API.

Two entry points:
  * ``Agent`` — user persona, single-session UX (``arun``, ``astream``,
    ``send``, ``events``, ``cancel``, ``configure``).
  * ``ApiClient`` — operator persona, flat one-method-per-route wrapper
    over the REST surface. Use from services that manage other people's
    sessions.
"""

__version__ = "0.5.0"

from .client import (
    Agent, Event, Sandbox, UsageStats,
    CLAUDE, CODEX, OPENCODE, GEMINI, CLINE, DEEPAGENTS, OPENHANDS, GOOSE, AGENT_TYPES,
    LOCAL, DOCKER, DAYTONA, PROVIDERS,
)
from .errors import (
    AgentSDKError, AgentConnectionError, AgentNotRegisteredError,
    SandboxError, VolumeFileExistsError, AgentBusyError, AgentTimeoutError,
    PromptError, StreamError,
)
from .persist import SessionRecord, SqliteSessionDriver
from .api_client import ApiClient

__all__ = [
    "__version__",
    "Agent",
    "Event",
    "Sandbox",
    "UsageStats",
    "CLAUDE", "CODEX", "OPENCODE", "GEMINI", "CLINE", "DEEPAGENTS", "OPENHANDS", "GOOSE", "AGENT_TYPES",
    "LOCAL", "DOCKER", "DAYTONA", "PROVIDERS",
    "AgentSDKError", "AgentConnectionError", "AgentNotRegisteredError",
    "SandboxError", "VolumeFileExistsError", "AgentBusyError", "AgentTimeoutError", "PromptError", "StreamError",
    "SessionRecord",
    "SqliteSessionDriver",
    "ApiClient",
]
