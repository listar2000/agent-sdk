"""UnixLocalSandboxSession — the default rows of the supervisor-session
template (no container, no remote URL: a local supervisor.js subprocess).

See ``api.sandbox.supervisor_session`` for the shared start()/stop()
algorithm; unix_local needs no hook overrides.
"""
from __future__ import annotations

from api.sandbox.state import SandboxState, UnixLocalSandboxState
from api.sandbox.supervisor_session import SupervisorSandboxSession


class UnixLocalSandboxSession(SupervisorSandboxSession):
    """One running local supervisor.js + ACP child subprocess."""

    volume_provider = "unix_local"
    state: UnixLocalSandboxState

    _provider_mod = "unix_local"
    _snapshot_path = "/tmp/agentsdk-snapshot.tar"
    _health_fail_msg = "Local supervisor not responding at {url}"

    def __init__(self, *, session_id: str, state: SandboxState) -> None:
        if not isinstance(state, UnixLocalSandboxState):
            state = UnixLocalSandboxState(recipe=state.recipe)
        super().__init__(session_id=session_id, state=state)
