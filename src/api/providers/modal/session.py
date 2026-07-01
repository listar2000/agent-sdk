"""ModalSandboxSession — modal rows of the supervisor-session template.

See ``api.sandbox.supervisor_session`` for the shared start()/stop()
algorithm. Modal's deltas: tunnel URLs are minted at create time (the
reattach URL must be resolved from the provider, never derived from the
ref), terminate is destructive (no revive of "stopped"), create
health-waits internally (the template's health gate runs only on
reattach), ACP attach retries with health diagnostics, and a
freshly-created sandbox is torn down when attach fails (it isn't
pool-visible yet — it would leak as created-but-unregistered).
"""
from __future__ import annotations

import asyncio
import logging

import httpx

from api.sandbox.state import ModalSandboxState, SandboxState
from api.sandbox.supervisor_session import SupervisorSandboxSession

log = logging.getLogger(__name__)

_ATTACH_RETRY_ATTEMPTS = 6
_ATTACH_RETRY_DELAY_S = 1.0


class ModalSandboxSession(SupervisorSandboxSession):
    """One running Modal sandbox + supervisor + ACP child."""

    volume_provider = "modal"
    _default_root = "/v"
    state: ModalSandboxState

    _provider_mod = "modal"
    _health_retries = 15
    _health_interval = 0.5
    _health_on_fresh_create = False
    _revive_stopped = False
    _cleanup_fresh_on_attach_failure = True
    _health_fail_msg = "Modal supervisor not responding at {url}"

    def __init__(self, *, session_id: str, state: SandboxState) -> None:
        if not isinstance(state, ModalSandboxState):
            state = ModalSandboxState(recipe=state.recipe)
        super().__init__(session_id=session_id, state=state)
        self._cwd = "/v"

    def _create_kwargs(self) -> dict:
        # Thread the per-session custom image / dockerfile from the recipe so
        # the modal provider can boot a non-default image (e.g. LocalStack +
        # the agent-sdk runtime) on the SUPERVISOR path. Both default to None
        # (the baked agent-sdk runtime image) when unset.
        return {
            "resources": self.state.recipe.resources,
            "image": self.state.recipe.image,
            "dockerfile": self.state.recipe.dockerfile,
        }

    async def _reattach_url(self, mod, status: str) -> str | None:
        if status != "running":
            return None
        # Fetch the REAL HTTPS tunnel URL — Modal allocates it at
        # sandbox-create time and it is NOT derivable from sandbox_ref.
        return await mod.resolve_supervisor_url(self.state.sandbox_ref)

    async def _on_wedged_reattach(self, mod, instance) -> None:
        self.state.sandbox_ref = None
        self.state.listen_port = None

    async def _attach(self) -> None:
        await self._attach_with_retry()

    async def _attach_with_retry(self) -> None:
        last_error: Exception | None = None
        for attempt in range(1, _ATTACH_RETRY_ATTEMPTS + 1):
            try:
                await self._attach_acp()
                return
            except Exception as exc:
                last_error = exc
                if attempt >= _ATTACH_RETRY_ATTEMPTS:
                    break
                # Diagnostic: probe /v1/health to distinguish supervisor-dead
                # (health 0/5xx) from POST-handler-broken (health 200, POST fails).
                health_status = "unknown"
                try:
                    async with httpx.AsyncClient(timeout=3.0) as probe:
                        r = await probe.get(f"{self._supervisor_url}/v1/health")
                        health_status = str(r.status_code)
                except Exception as probe_exc:
                    health_status = f"err:{type(probe_exc).__name__}"
                log.warning(
                    "Modal ACP attach failed (attempt %s/%s) for session %s: "
                    "%s: %r [health=%s]",
                    attempt, _ATTACH_RETRY_ATTEMPTS, self.session_id,
                    type(exc).__name__, exc, health_status,
                )
                await self._aclose_acp_client()
                await asyncio.sleep(_ATTACH_RETRY_DELAY_S * attempt)
        assert last_error is not None
        raise last_error
