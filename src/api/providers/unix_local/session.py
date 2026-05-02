"""UnixLocalSandboxSession — concrete SandboxSession for the local provider.

Wraps existing primitives in ``src/api/providers/local.py`` into the
five-method ``BaseSandboxSession`` contract. The simplest provider
shape: just a local subprocess running supervisor.js + ACP, no
container, no remote URL.
"""
from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator
from typing import Any
from uuid import uuid4

import httpx

from api.sandbox.session import BaseSandboxSession
from api.sandbox.state import SandboxState, UnixLocalSandboxState

log = logging.getLogger(__name__)

_SSE_READ_TIMEOUT_S = 60.0


class UnixLocalSandboxSession(BaseSandboxSession):
    """One running local supervisor.js + ACP child subprocess."""

    volume_provider = "unix_local"
    state: UnixLocalSandboxState

    def __init__(self, *, session_id: str, state: SandboxState) -> None:
        if not isinstance(state, UnixLocalSandboxState):
            state = UnixLocalSandboxState(recipe=state.recipe)
        super().__init__(session_id=session_id, state=state)

    async def start(self) -> None:
        if self._supervisor_url is not None and await self.running():
            return

        from api.providers import unix_local as lc_provider
        from api.providers._shared import _wait_for_health

        volume_ref = await self._bootstrap_session()

        instance = None
        # sandbox_id here is the local provider's stable ref (local-XXXX).
        # On second start() we try to restart the SAME sandbox in place so
        # the test_stop_sandbox_same_sandbox_after_restart invariant holds.
        if self.state.sandbox_ref:
            try:
                status = await lc_provider.get_sandbox_status(self.state.sandbox_ref)
                from api.providers import ProviderInstance
                if status == "running":
                    instance = ProviderInstance(
                        provider="unix_local",
                        url=f"http://127.0.0.1:{self.state.listen_port}",
                        root=self.state.recipe.root or "/tmp",
                        sandbox_ref=self.state.sandbox_ref,
                        port=self.state.listen_port,
                    )
                elif status == "stopped":
                    # Process died but spawn plan + alive marker intact;
                    # respawn at the same ref (same volume subpath, same
                    # pre_start commands) — preserves the contract that the
                    # sandbox identity survives external stops.
                    await lc_provider.start_sandbox(self.state.sandbox_ref)
                    instance = ProviderInstance(
                        provider="unix_local",
                        url=f"http://127.0.0.1:{self.state.listen_port}",
                        root=self.state.recipe.root or "/tmp",
                        sandbox_ref=self.state.sandbox_ref,
                        port=self.state.listen_port,
                    )
                # status == "missing" → fall through to create.
            except Exception:
                pass

        if instance is None:
            instance = await lc_provider.create_sandbox(
                volume_ref=volume_ref,
                subpath=self._subpath or f"sessions/{self.session_id}",
                agent_type=self.state.recipe.agent_type,
                root=self.state.recipe.root,
                spawn_env=self._spawn_env,
                pre_start_commands=self.state.recipe.pre_start_commands or None,
                shared_mounts=self.state.recipe.shared_mounts or None,
            )
            self.state.sandbox_ref = instance.sandbox_ref
            self.state.listen_port = instance.port

        self._supervisor_url = instance.url

        ok = await _wait_for_health(instance.url, max_retries=10, interval=0.3)
        if not ok:
            raise RuntimeError(
                f"Local supervisor not responding at {instance.url}"
            )

        self.liveness.observe_chunk()
        if self._acp_session_id is None:
            self._acp_session_id = str(uuid4())
        await self._attach_acp()

        log.info(
            "UnixLocalSandboxSession started: session=%s pid=%s url=%s",
            self.session_id, self.state.sandbox_ref, instance.url,
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
            raise RuntimeError("UnixLocalSandboxSession.execute_prompt called before start()")

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

        # Use SEPARATE httpx.AsyncClient instances for the SSE GET and the
        # session/prompt POST. With a shared client the GET stream's keep-alive
        # connection serializes pipeline behaviour with the concurrent POST,
        # which on httpx 0.27+ closes the SSE stream prematurely (~1.5s).
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
                        log.info("execute_prompt POST starting for %s rpc=%s",
                                 self.session_id, rpc_id)
                        resp = await post_client.post(
                            f"/v1/acp/{self._acp_session_id}", json=prompt_payload,
                        )
                        log.info("execute_prompt POST done for %s rpc=%s status=%s",
                                 self.session_id, rpc_id, resp.status_code)
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
                            # Broadcast the rpc-tagged raw block + the parsed
                            # event. /events subscribers consume the raw block
                            # (with ``event: rpc:<id>`` tag); internal callers
                            # of execute_prompt see the parsed dict.
                            self._broadcast((rpc_id, block))
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
                                             json={"path": "/tmp/agentsdk-snapshot.tar"})
                    if resp.status_code == 200:
                        self.state.snapshot_path = "/tmp/agentsdk-snapshot.tar"
                        self.state.snapshot_version += 1
            except Exception:
                log.exception("snapshot request failed for session %s", self.session_id)

        from api.providers import unix_local as lc_provider
        from api.providers import ProviderInstance
        try:
            await lc_provider.stop_sandbox(ProviderInstance(
                provider="unix_local", url=self._supervisor_url or "",
                root=self.state.recipe.root or "/tmp",
                sandbox_ref=self.state.sandbox_ref or "",
                port=self.state.listen_port,
            ))
        except Exception:
            log.exception("local.stop_sandbox failed for session %s", self.session_id)
        # Process is gone; clear sandbox_id so next start cold-creates.
        self.state.sandbox_ref = None
        self.state.listen_port = None

    async def shutdown(self) -> None:
        self._supervisor_url = None
        self._close_subscribers()
