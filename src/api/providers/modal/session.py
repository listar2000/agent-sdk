"""ModalSandboxSession — concrete SandboxSession for the modal provider.

Wraps existing primitives in ``src/api/providers/modal.py``. Modal's
shape sits between docker (no native pause) and daytona (remote
provider with managed compute lifecycle).
"""
from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator
from typing import Any
from uuid import uuid4

import httpx

from api.sandbox.session import BaseSandboxSession
from api.sandbox.state import ModalSandboxState, SandboxState

log = logging.getLogger(__name__)

_SSE_READ_TIMEOUT_S = 60.0


class ModalSandboxSession(BaseSandboxSession):
    """One running Modal sandbox + supervisor + ACP child."""

    volume_provider = "modal"
    state: ModalSandboxState

    def __init__(self, *, session_id: str, state: SandboxState) -> None:
        if not isinstance(state, ModalSandboxState):
            state = ModalSandboxState(recipe=state.recipe)
        super().__init__(session_id=session_id, state=state)
        self._cwd = "/v"

    async def start(self) -> None:
        if self._supervisor_url is not None and await self.running():
            return

        from api.providers import modal as md_provider
        from api.providers._shared import _wait_for_health

        volume_ref = await self._bootstrap_session()

        instance = None
        reattached = False
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

        self._supervisor_url = instance.url

        ok = await _wait_for_health(instance.url, max_retries=15, interval=0.5)
        if not ok:
            if reattached:
                # Reattached to an existing modal sandbox but its supervisor
                # is unreachable. Abandon the ref so the next get_session
                # cold-creates fresh instead of looping on the wedged one.
                self.state.sandbox_ref = None
            raise RuntimeError(
                f"Modal supervisor not responding at {instance.url}"
            )

        self.liveness.observe_chunk()
        if self._acp_session_id is None:
            self._acp_session_id = str(uuid4())
        await self._attach_acp()

        log.info(
            "ModalSandboxSession started: session=%s sandbox=%s url=%s",
            self.session_id, (self.state.sandbox_ref or "")[:16], instance.url,
        )

    async def running(self, *, force_probe: bool = False) -> bool:
        return await self.liveness.is_alive(force_probe=force_probe)

    async def _liveness_probe(self) -> bool:
        if self._supervisor_url is None:
            return False
        try:
            async with httpx.AsyncClient(timeout=2.0) as client:
                resp = await client.get(f"{self._supervisor_url}/v1/health")
                return resp.status_code == 200
        except Exception:
            return False

    async def execute_prompt(
        self, message: str, *, rpc_id: str | None = None,
    ) -> AsyncIterator[Any]:
        if self._supervisor_url is None or self._acp_session_id is None:
            raise RuntimeError("ModalSandboxSession.execute_prompt called before start()")

        from api.providers.daytona.session import _parse_sse_block

        if rpc_id is None:
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

        sse_client = httpx.AsyncClient(
            base_url=self._supervisor_url,
            timeout=httpx.Timeout(connect=10, read=None, write=10, pool=10),
        )
        post_client = httpx.AsyncClient(
            base_url=self._supervisor_url,
            timeout=httpx.Timeout(connect=10, read=_SSE_READ_TIMEOUT_S, write=10, pool=10),
        )
        try:
            async with sse_client.stream(
                "GET", f"/v1/acp/{self._acp_session_id}",
                headers={"Accept": "text/event-stream"},
            ) as sse:
                sse.raise_for_status()

                async def _send_prompt() -> None:
                    try:
                        await post_client.post(
                            f"/v1/acp/{self._acp_session_id}", json=prompt_payload,
                        )
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
                            self._broadcast((rpc_id, block))
                            yield event
                            if event.get("type") in ("done", "error"):
                                return
                finally:
                    if not send_task.done():
                        send_task.cancel()
                        try:
                            await send_task
                        except (asyncio.CancelledError, Exception):
                            pass
                    self.liveness.observe_close()
        finally:
            await sse_client.aclose()
            await post_client.aclose()

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
