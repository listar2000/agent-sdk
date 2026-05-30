"""DockerSandboxSession — concrete SandboxSession for the docker provider.

Wraps existing primitives in ``src/api/providers/docker.py`` into the
five-method ``BaseSandboxSession`` contract — adding a provider is one
file + one factory line in ``api/sandbox/factory.py``.

Docker is structurally simpler than daytona:
  * No S3-FUSE bridge — local volume mounts are POSIX
  * No signed-URL minting — supervisor URL is stable for the container's
    lifetime
  * No pause/resume — ``stop`` removes the container; ``start`` creates
    a fresh one against the same volume subpath
"""
from __future__ import annotations

import logging
from uuid import uuid4

import httpx

from api.sandbox.session import BaseSandboxSession
from api.sandbox.state import DockerSandboxState, SandboxState

log = logging.getLogger(__name__)


class DockerSandboxSession(BaseSandboxSession):
    """One running Docker container + supervisor + ACP child."""

    volume_provider = "docker"
    state: DockerSandboxState

    def __init__(self, *, session_id: str, state: SandboxState) -> None:
        if not isinstance(state, DockerSandboxState):
            state = DockerSandboxState(recipe=state.recipe)
        super().__init__(session_id=session_id, state=state)
        self._container_id: str | None = None
        self._cwd = "/home/agent"

    # ------------------------------------------------------------------ #
    # start: reattach-or-create + supervisor                              #
    # ------------------------------------------------------------------ #

    async def start(self) -> None:
        if self._supervisor_url is not None and await self.running():
            return

        from api.providers import docker as dk_provider
        from api.providers._shared import _wait_for_health

        volume_ref = await self._bootstrap_session()

        # If state has a container id, try to keep it (test invariant:
        # external stop must restart same container, not provision new).
        instance = None
        reattached = False
        if self.state.sandbox_ref:
            try:
                status = await dk_provider.get_sandbox_status(self.state.sandbox_ref)
                from api.providers import ProviderInstance
                if status == "running":
                    instance = ProviderInstance(
                        provider="docker",
                        url=f"http://127.0.0.1:{self.state.listen_port}",
                        root=self.state.recipe.root or "/home/agent",
                        sandbox_ref=self.state.sandbox_ref,
                        port=self.state.listen_port,
                    )
                    reattached = True
                elif status == "stopped":
                    # Container exists but stopped (`docker stop` w/o --rm).
                    # `docker start` revives it on the same image+volume.
                    await dk_provider.start_sandbox(self.state.sandbox_ref)
                    instance = ProviderInstance(
                        provider="docker",
                        url=f"http://127.0.0.1:{self.state.listen_port}",
                        root=self.state.recipe.root or "/home/agent",
                        sandbox_ref=self.state.sandbox_ref,
                        port=self.state.listen_port,
                    )
                    reattached = True
                # missing/error → fall through to create.
            except Exception:
                pass

        if instance is None:
            instance = await dk_provider.create_sandbox(
                volume_ref=volume_ref,
                subpath=self._subpath or f"sessions/{self.session_id}",
                agent_type=self.state.recipe.agent_type,
                root=self.state.recipe.root,
                spawn_env=self._spawn_env,
                pre_start_commands=self.state.recipe.pre_start_commands or None,
                shared_mounts=self.state.recipe.shared_mounts or None,
                resources=self.state.recipe.resources,
                sandbox_ref=self.session_id,
            )
            self.state.sandbox_ref = instance.sandbox_ref
            self.state.listen_port = instance.port

        self._container_id = instance.sandbox_ref
        self._supervisor_url = instance.url

        ok = await _wait_for_health(instance.url, max_retries=10, interval=0.3)
        if not ok:
            if reattached:
                # We reattached to an existing container but its supervisor
                # is unreachable. Abandon the ref so the next get_session
                # cold-creates a fresh container instead of looping on the
                # same wedged one.
                self.state.sandbox_ref = None
            raise RuntimeError(
                f"Supervisor not responding at {instance.url} after create_sandbox"
            )

        self.liveness.observe_chunk()

        if self._acp_session_id is None:
            self._acp_session_id = str(uuid4())
        await self._attach_acp()

        log.info(
            "DockerSandboxSession started: session=%s container=%s url=%s",
            self.session_id, (self._container_id or "")[:16], instance.url,
        )

    # ------------------------------------------------------------------ #
    # running: liveness oracle (probe via /v1/health)                     #
    # ------------------------------------------------------------------ #

    async def running(self, *, force_probe: bool = False) -> bool:
        return await self.liveness.is_alive(force_probe=force_probe)

    async def _liveness_probe(self) -> bool:
        if self._supervisor_url is None:
            return False
        ok, _ = await self._get_acp_client().health_probe()
        return ok

    # ------------------------------------------------------------------ #
    # stop: snapshot then container stop                                  #
    # ------------------------------------------------------------------ #

    async def stop(self) -> None:
        if self._container_id is None:
            return
        # Snapshot via supervisor's /v1/snapshot endpoint (same shape as
        # daytona). Local volume FS is POSIX so this is fast.
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

        # Docker doesn't have a "pause" — stop_sandbox removes the container.
        # Per docs §15.3 we still want pause-like semantics; on docker that
        # means: stop, but keep volume; next start creates a fresh container
        # against the same volume subpath, restoring from snapshot.
        from api.providers import docker as dk_provider
        from api.providers import ProviderInstance
        try:
            await dk_provider.stop_sandbox(ProviderInstance(
                provider="docker", url=self._supervisor_url or "",
                root=self.state.recipe.root or "/home/agent",
                sandbox_ref=self.state.sandbox_ref or "",
                port=self.state.listen_port,
            ))
        except Exception:
            log.exception("docker.stop_sandbox failed for session %s", self.session_id)
        # Container is gone; clear sandbox_id so next start cold-creates.
        self.state.sandbox_ref = None
        self.state.listen_port = None

    # ------------------------------------------------------------------ #
    # shutdown: in-memory cleanup                                         #
    # ------------------------------------------------------------------ #

    async def shutdown(self) -> None:
        self._container_id = None
        self._supervisor_url = None
        self._close_subscribers()
        await self._aclose_acp_client()
