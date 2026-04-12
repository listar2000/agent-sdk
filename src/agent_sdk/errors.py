"""Structured error types for the Agent SDK."""


class AgentSDKError(Exception):
    """Base error for all Agent SDK errors."""


class AgentConnectionError(AgentSDKError):
    """Failed to connect to the API server or sandbox."""


class AgentNotRegisteredError(AgentSDKError):
    """Agent has not been registered yet."""


class SandboxError(AgentSDKError):
    """Error from the sandbox or sandbox-agent process."""


class AgentBusyError(AgentSDKError):
    """Agent is already processing a message."""


class AgentTimeoutError(AgentSDKError):
    """Agent operation timed out."""


class PromptError(AgentSDKError):
    """Error while processing a prompt."""


class StreamError(AgentSDKError):
    """Error in SSE stream (connection lost, parse failure)."""
