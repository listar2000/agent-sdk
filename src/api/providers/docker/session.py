"""DockerSandboxSession — docker rows of the supervisor-session template.

See ``api.sandbox.supervisor_session`` for the shared start()/stop()
algorithm; this class declares only docker's genuine deltas, each pinned in
tests/test_supervisor_session_template.py.
"""
from __future__ import annotations

import logging

from api.sandbox.state import DockerSandboxState, SandboxState
from api.sandbox.supervisor_session import SupervisorSandboxSession

log = logging.getLogger(__name__)


class DockerSandboxSession(SupervisorSandboxSession):
    """One running Docker container + supervisor + ACP child."""

    volume_provider = "docker"
    _default_root = "/home/agent"
    state: DockerSandboxState

    _provider_mod = "docker"
    _health_fail_msg = "Supervisor not responding at {url} after create_sandbox"

    def __init__(self, *, session_id: str, state: SandboxState) -> None:
        if not isinstance(state, DockerSandboxState):
            state = DockerSandboxState(recipe=state.recipe)
        super().__init__(session_id=session_id, state=state)
        self._container_id: str | None = None
        self._cwd = "/home/agent"

    def _create_kwargs(self) -> dict:
        # sandbox_ref=session_id becomes the agent-sdk.sandbox-id container
        # label that reconcile_on_startup filters on — orphan reconciliation
        # silently breaks without it.
        return {
            "resources": self.state.recipe.resources,
            "sandbox_ref": self.session_id,
        }

    def _on_instance_resolved(self, instance) -> None:
        self._container_id = instance.sandbox_ref

    async def _on_wedged_reattach(self, mod, instance) -> None:
        # DESTROY the wedged container so the NEXT get_session sees it as
        # missing and cold-creates. Merely nulling the in-memory ref is not
        # enough: the DB row still carries the old ref, so the next recovery
        # would reattach to the same wedged container and loop forever. The
        # volume (and its per-turn snapshot) survive the rm.
        try:
            await mod.destroy_sandbox(instance)
        except Exception:
            log.exception(
                "failed to destroy wedged container %s; next recovery "
                "may reattach to it", (self._container_id or "")[:16],
            )
        self.state.sandbox_ref = None

    def _stop_ready(self) -> bool:
        # Guard on the in-memory handle (set only by a start() in THIS
        # process): a reloaded-but-never-started session skips snapshot+stop.
        return self._container_id is not None

    async def shutdown(self) -> None:
        self._container_id = None  # provider-specific handle; rest is base
        await super().shutdown()
