"""Structured error types for the Agent SDK."""


class AgentSDKError(Exception):
    """Base error for all Agent SDK errors."""


class AgentConnectionError(AgentSDKError):
    """Failed to connect to the API server or sandbox."""


class AgentNotRegisteredError(AgentSDKError):
    """Agent has not been registered yet."""


class SandboxError(AgentSDKError):
    """Error from the sandbox provider."""


class VolumeFileExistsError(AgentSDKError):
    """Destination already exists during a no-overwrite volume rename."""

    def __init__(self, path: str | None = None, message: str | None = None):
        self.path = path
        super().__init__(message or (f"destination exists: {path}" if path else "destination exists"))


class AgentBusyError(AgentSDKError):
    """Agent is already processing a message."""


class AgentTimeoutError(AgentSDKError):
    """Agent operation timed out."""


class PromptError(AgentSDKError):
    """Error while processing a prompt.

    Carries an optional ``kind`` discriminator and structured ``data`` dict
    forwarded from the server's JSON-RPC error frame so callers can branch
    on the failure mode (e.g. ``sandbox_process_died``, ``timeout``).
    """

    def __init__(self, message: str, *, kind: str | None = None,
                 data: dict | None = None):
        super().__init__(message)
        self.kind = kind
        self.data = data or {}


class StreamError(AgentSDKError):
    """Error in SSE stream (connection lost, parse failure)."""
