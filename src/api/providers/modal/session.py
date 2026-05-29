"""ModalSandboxSession — concrete SandboxSession for the modal provider.

Wraps existing primitives in ``src/api/providers/modal.py``. Modal's
shape sits between docker (no native pause) and daytona (remote
provider with managed compute lifecycle).
"""
from __future__ import annotations

import asyncio
import logging
from uuid import uuid4

import httpx

from api.sandbox.session import BaseSandboxSession
from api.sandbox.state import ModalSandboxState, SandboxState

log = logging.getLogger(__name__)

_ATTACH_RETRY_ATTEMPTS = 6
_ATTACH_RETRY_DELAY_S = 1.0


class ModalSandboxSession(BaseSandboxSession):
    """One running Modal sandbox + supervisor + ACP child."""

    volume_provider = "modal"
    state: ModalSandboxState

    def __init__(self, *, session_id: str, state: SandboxState) -> None:
        if not isinstance(state, ModalSandboxState):
            state = ModalSandboxState(recipe=state.recipe)
        super().__init__(session_id=session_id, state=state)
        self._cwd = "/v"

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

    async def start(self) -> None:
        if self._supervisor_url is not None and await self.running():
            return

        from api.providers import modal as md_provider
        from api.providers._shared import _wait_for_health

        volume_ref = await self._bootstrap_session()

        instance = None
        reattached = False
        created_fresh = False
        if self.state.sandbox_ref:
            try:
                status = await md_provider.get_sandbox_status(self.state.sandbox_ref)
                if status == "running":
                    # Fetch the REAL HTTPS tunnel URL — Modal allocates it at
                    # sandbox-create time and it is NOT derivable from
                    # sandbox_ref. The previous code constructed
                    # "http://<ref>.modal.host:<port>" which never routes.
                    url = await md_provider.resolve_supervisor_url(
                        self.state.sandbox_ref
                    )
                    if url:
                        from api.providers import ProviderInstance
                        instance = ProviderInstance(
                            provider="modal",
                            url=url,
                            root=self.state.recipe.root or "/v",
                            sandbox_ref=self.state.sandbox_ref,
                            port=self.state.listen_port,
                        )
                        reattached = True
            except Exception:
                pass

        if instance is None:
            instance = await md_provider.create_sandbox(
                volume_ref=volume_ref,
                subpath=self._subpath or f"sessions/{self.session_id}",
                agent_type=self.state.recipe.agent_type,
                root=self.state.recipe.root,
                spawn_env=self._spawn_env,
                pre_start_commands=self.state.recipe.pre_start_commands or None,
                shared_mounts=self.state.recipe.shared_mounts or None,
                resources=self.state.recipe.resources,
            )
            self.state.sandbox_ref = instance.sandbox_ref
            self.state.listen_port = instance.port
            created_fresh = True

        self._supervisor_url = instance.url

        if reattached:
            ok = await _wait_for_health(instance.url, max_retries=15, interval=0.5)
            if not ok:
                # Reattached to an existing modal sandbox but its supervisor
                # is unreachable. Abandon the ref so the next get_session
                # cold-creates fresh instead of looping on the wedged one.
                self.state.sandbox_ref = None
                self.state.listen_port = None
                raise RuntimeError(
                    f"Modal supervisor not responding at {instance.url}"
                )

        try:
            self.liveness.observe_chunk()
            if self._acp_session_id is None:
                self._acp_session_id = str(uuid4())
            await self._attach_with_retry()
        except Exception:
            # Freshly-created Modal sandboxes are not visible to the SessionPool
            # until start() returns and state is persisted; tear down on attach
            # failure so we do not leak "created but unregistered" sandboxes.
            if created_fresh and self.state.sandbox_ref:
                try:
                    from api.providers import ProviderInstance
                    await md_provider.stop_sandbox(ProviderInstance(
                        provider="modal",
                        url=self._supervisor_url or "",
                        root=self.state.recipe.root or "/v",
                        sandbox_ref=self.state.sandbox_ref,
                        port=self.state.listen_port,
                    ))
                except Exception:
                    log.exception(
                        "modal cleanup after attach failure failed: session=%s sandbox=%s",
                        self.session_id, self.state.sandbox_ref,
                    )
            if created_fresh or reattached:
                self.state.sandbox_ref = None
                self.state.listen_port = None
            self._supervisor_url = None
            raise

        log.info(
            "ModalSandboxSession started: session=%s sandbox=%s url=%s",
            self.session_id, (self.state.sandbox_ref or "")[:16], instance.url,
        )

    async def running(self, *, force_probe: bool = False) -> bool:
        return await self.liveness.is_alive(force_probe=force_probe)

    async def _liveness_probe(self) -> bool:
        if self._supervisor_url is None:
            return False
        ok, _ = await self._get_acp_client().health_probe()
        return ok

    async def stop(self) -> None:
        if self.state.sandbox_ref is None:
            return
        if self._supervisor_url is not None:
            try:
                async with httpx.AsyncClient(timeout=60.0) as client:
                    resp = await client.post(f"{self._supervisor_url}/v1/snapshot",
                                             json={"path": "/v/snapshot.tar"})
                    if resp.status_code == 200:
                        self.state.snapshot_path = "/v/snapshot.tar"
                        self.state.snapshot_version += 1
            except Exception:
                log.exception("snapshot request failed for session %s", self.session_id)

        # Modal: terminate is destructive (no pause). Per docs §15.3 we
        # still call stop_sandbox; the persisted snapshot lets the next
        # start() restore from it.
        from api.providers import modal as md_provider
        from api.providers import ProviderInstance
        try:
            await md_provider.stop_sandbox(ProviderInstance(
                provider="modal", url=self._supervisor_url or "",
                root=self.state.recipe.root or "/v",
                sandbox_ref=self.state.sandbox_ref or "",
                port=self.state.listen_port,
            ))
        except Exception:
            log.exception("modal.stop_sandbox failed for session %s", self.session_id)
        # Modal sandbox is gone; clear sandbox_id so next start cold-creates.
        self.state.sandbox_ref = None
        self.state.listen_port = None

    async def shutdown(self) -> None:
        self._supervisor_url = None
        self._close_subscribers()
        await self._aclose_acp_client()
