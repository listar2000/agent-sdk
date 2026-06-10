"""UnixLocalSandboxSession — concrete SandboxSession for the local provider.

Wraps existing primitives in ``src/api/providers/unix_local/__init__.py`` into
the three-method (``start``/``running``/``stop``) ``BaseSandboxSession``
contract. The simplest provider
shape: just a local subprocess running supervisor.js + ACP, no
container, no remote URL.
"""
from __future__ import annotations

import logging
from uuid import uuid4

from api.sandbox.session import BaseSandboxSession
from api.sandbox.state import SandboxState, UnixLocalSandboxState

log = logging.getLogger(__name__)


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
        reattached = False
        # sandbox_ref here is the local provider's stable ref (local-XXXX).
        # On second start() we try to restart the SAME sandbox in place so
        # the test_stop_sandbox_same_sandbox_after_restart invariant holds.
        if self.state.sandbox_ref:
            try:
                status = await lc_provider.get_sandbox_status(self.state.sandbox_ref)
                if status == "running":
                    instance = self._provider_instance(
                        url=f"http://127.0.0.1:{self.state.listen_port}",
                        sandbox_ref=self.state.sandbox_ref,
                        port=self.state.listen_port,
                    )
                    reattached = True
                elif status == "stopped":
                    # Process died but spawn plan + alive marker intact;
                    # respawn at the same ref (same volume subpath, same
                    # pre_start commands) — preserves the contract that the
                    # sandbox identity survives external stops.
                    await lc_provider.start_sandbox(self.state.sandbox_ref)
                    instance = self._provider_instance(
                        url=f"http://127.0.0.1:{self.state.listen_port}",
                        sandbox_ref=self.state.sandbox_ref,
                        port=self.state.listen_port,
                    )
                    reattached = True
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
            if reattached:
                # Reattached to an existing supervisor PID but it's
                # unreachable. Abandon the ref so the next get_session
                # cold-creates fresh instead of looping on the wedged one.
                self.state.sandbox_ref = None
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

    # running() and _liveness_probe() inherited from BaseSandboxSession.

    async def stop(self) -> None:
        if self.state.sandbox_ref is None:
            return
        await self._write_snapshot("/tmp/agentsdk-snapshot.tar")

        from api.providers import unix_local as lc_provider
        try:
            await lc_provider.stop_sandbox(self._provider_instance(
                url=self._supervisor_url or "",
                sandbox_ref=self.state.sandbox_ref or "",
                port=self.state.listen_port,
            ))
        except Exception:
            log.exception("local.stop_sandbox failed for session %s", self.session_id)
        # Process is gone; clear sandbox_ref so next start cold-creates.
        self.state.sandbox_ref = None
        self.state.listen_port = None

    # shutdown() inherited from BaseSandboxSession (no provider-specific
    # handles to null beyond the base's _supervisor_url).
