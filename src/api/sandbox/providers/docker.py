"""DockerSandboxSession — concrete SandboxSession for the docker provider.

Wraps existing primitives in ``src/api/providers/docker.py`` into the
five-method ``BaseSandboxSession`` contract. Per
``docs/ephemeral-sandbox-design.md`` §11 — adding a provider is one
file + one factory line.

Docker is structurally simpler than daytona:
  * No S3-FUSE bridge — local volume mounts are POSIX
  * No signed-URL minting — supervisor URL is stable for the container's
    lifetime
  * No pause/resume — ``stop`` removes the container; ``start`` creates
    a fresh one against the same volume subpath
"""
from __future__ import annotations

import json
import logging
from collections.abc import AsyncIterator
from typing import Any
from uuid import uuid4

import httpx

from ..session import BaseSandboxSession
from ..state import DockerSandboxState, SandboxState

log = logging.getLogger(__name__)

# Per-prompt SSE drain budget; matches DaytonaSandboxSession.
_SSE_READ_TIMEOUT_S = 60.0


class DockerSandboxSession(BaseSandboxSession):
    """One running Docker container + supervisor + ACP child."""

    state: DockerSandboxState

    def __init__(self, *, session_id: str, state: SandboxState) -> None:
        if not isinstance(state, DockerSandboxState):
            state = DockerSandboxState(recipe=state.recipe)
        super().__init__(session_id=session_id, state=state)
        self._container_id: str | None = None
        self._supervisor_url: str | None = None
        self._acp_session_id: str | None = None
        self._inner_session_id: str | None = None
        self._spawn_env: dict[str, str] = {}

    # ------------------------------------------------------------------ #
    # start: reattach-or-create + supervisor                              #
    # ------------------------------------------------------------------ #

    async def start(self) -> None:
        if self._supervisor_url is not None and await self.running():
            return

        from api.providers import docker as dk_provider
        from api.providers._shared import _wait_for_health

        # Docker doesn't support pause/resume. If state has a container
        # id, check if it's still alive; otherwise create fresh.
        instance = None
        if self.state.sandbox_id:
            try:
                status = await dk_provider.get_sandbox_status(self.state.sandbox_id)
                if status == "running":
                    # Container alive — reuse. The existing ensure_supervisor_url
                    # is a no-op when a supervisor is already running.
                    from api.providers import ProviderInstance
                    instance = ProviderInstance(
                        provider="docker",
                        url=f"http://127.0.0.1:{self.state.listen_port}",
                        root=self.state.recipe.root or "/home/agent",
                        sandbox_id=self.state.sandbox_id,
                        port=self.state.listen_port,
                    )
                # Other statuses (stopped/missing/error) → fall through to create.
            except Exception:
                # Status probe failed — fall through to create.
                pass

        if instance is None:
            instance = await dk_provider.create_sandbox(
                volume_ref=self.state.recipe.root or "agentsdk-default",
                subpath=f"sessions/{self.session_id}",
                agent_type=self.state.recipe.agent_type,
                root=self.state.recipe.root,
                spawn_env=self._spawn_env,
                pre_start_commands=self.state.recipe.pre_start_commands or None,
                shared_mounts=self.state.recipe.shared_mounts or None,
            )
            self.state.sandbox_id = instance.sandbox_id
            self.state.listen_port = instance.port

        self._container_id = instance.sandbox_id
        self._supervisor_url = instance.url

        ok = await _wait_for_health(instance.url, max_retries=10, interval=0.3)
        if not ok:
            raise RuntimeError(
                f"Supervisor not responding at {instance.url} after create_sandbox"
            )

        self.liveness.observe_chunk()

        if self._acp_session_id is None:
            self._acp_session_id = str(uuid4())

        log.info(
            "DockerSandboxSession started: session=%s container=%s url=%s",
            self.session_id, (self._container_id or "")[:16], instance.url,
        )

    # ------------------------------------------------------------------ #
    # running: liveness oracle (probe via /v1/health)                     #
    # ------------------------------------------------------------------ #

    async def running(self) -> bool:
        return await self.liveness.is_alive()

    async def _liveness_probe(self) -> bool:
        if self._supervisor_url is None:
            return False
        try:
            async with httpx.AsyncClient(timeout=2.0) as client:
                resp = await client.get(f"{self._supervisor_url}/v1/health")
                return resp.status_code == 200
        except Exception:
            return False

    # ------------------------------------------------------------------ #
    # execute_prompt: per-prompt supervisor SSE stream                    #
    # ------------------------------------------------------------------ #

    async def execute_prompt(self, message: str) -> AsyncIterator[Any]:
        if self._supervisor_url is None or self._acp_session_id is None:
            raise RuntimeError("DockerSandboxSession.execute_prompt called before start()")

        # SSE pipe is identical to daytona's — supervisor.js exposes the
        # same /v1/acp/{id} endpoint regardless of which container it
        # runs in. Reuse the daytona module's parser.
        from .daytona import _parse_sse_block
        import asyncio

        rpc_id = str(uuid4())
        prompt_payload = {
            "jsonrpc": "2.0",
            "id": rpc_id,
            "method": "session/prompt",
            "params": {
                "sessionId": self._inner_session_id,
                "prompt": [{"type": "text", "text": message}],
            },
        }

        async with httpx.AsyncClient(
            base_url=self._supervisor_url,
            timeout=httpx.Timeout(connect=10, read=_SSE_READ_TIMEOUT_S, write=10, pool=10),
        ) as http:
            async with http.stream("GET", f"/v1/acp/{self._acp_session_id}",
                                    headers={"Accept": "text/event-stream"}) as sse:
                sse.raise_for_status()

                async def _send_prompt() -> None:
                    try:
                        await http.post(f"/v1/acp/{self._acp_session_id}",
                                        json=prompt_payload)
                    except Exception:
                        log.exception("prompt POST failed for session %s", self.session_id)

                send_task = asyncio.create_task(_send_prompt())

                buf = ""
                try:
                    async for chunk in sse.aiter_text():
                        self.liveness.observe_chunk()
                        buf += chunk
                        while "\n\n" in buf:
                            block, buf = buf.split("\n\n", 1)
                            event = _parse_sse_block(block, rpc_id)
                            if event is None:
                                continue
                            self._broadcast(event)
                            yield event
                            if event.get("type") == "done":
                                return
                finally:
                    if not send_task.done():
                        send_task.cancel()
                        try:
                            await send_task
                        except (asyncio.CancelledError, Exception):
                            pass
                    self.liveness.observe_close()

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
                sandbox_id=self.state.sandbox_id or "",
                port=self.state.listen_port,
            ))
        except Exception:
            log.exception("docker.stop_sandbox failed for session %s", self.session_id)
        # Container is gone; clear sandbox_id so next start cold-creates.
        self.state.sandbox_id = None
        self.state.listen_port = None

    # ------------------------------------------------------------------ #
    # shutdown: in-memory cleanup                                         #
    # ------------------------------------------------------------------ #

    async def shutdown(self) -> None:
        self._container_id = None
        self._supervisor_url = None
        self._close_subscribers()
