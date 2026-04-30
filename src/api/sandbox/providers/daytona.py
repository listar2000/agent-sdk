"""DaytonaSandboxSession — concrete SandboxSession for the daytona provider.

Wraps existing primitives in ``src/api/providers/daytona.py`` into the
five-method ``BaseSandboxSession`` contract. Per
``docs/ephemeral-sandbox-design.md`` §5.

Lifecycle decisions live inside ``start()``:
  * ``state.sandbox_id`` set, sandbox alive on Daytona  → reattach (cheapest)
  * ``state.sandbox_id`` set, sandbox stopped/paused    → ``daytona.start()`` (resume)
  * ``state.sandbox_id`` missing or sandbox not found   → fresh ``daytona.create()``

No "Type 1 vs Type 2" branching outside this class — recovery just calls
``start()``; the class picks the cheapest path internally.

NOT yet wired into server.py — phase 2 sub-task 3 is the wiring commit.
"""
from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import AsyncIterator
from typing import Any
from uuid import uuid4

import httpx

from ..session import BaseSandboxSession
from ..state import DaytonaSandboxState, SandboxState

log = logging.getLogger(__name__)

# Supervisor inside every Daytona sandbox listens on this fixed port; the
# Daytona signed preview URL maps host URL to container port. Matches
# ``_SUPERVISOR_REMOTE_PORT`` in src/api/providers/daytona.py.
_SUPERVISOR_PORT = 9100

# Per-prompt SSE drain budget. supervisor.js sends a ``: heartbeat\n\n``
# every 25 s, so any 60 s gap means the supervisor (or the proxy path
# to it) is gone. Same value as src/api/server.py ``_SSE_READ_TIMEOUT_S``.
_SSE_READ_TIMEOUT_S = 60.0


class DaytonaSandboxSession(BaseSandboxSession):
    """One running Daytona sandbox + the supervisor + ACP child inside it."""

    volume_provider = "daytona"
    state: DaytonaSandboxState  # narrow the base's SandboxState union

    def __init__(self, *, session_id: str, state: SandboxState) -> None:
        if not isinstance(state, DaytonaSandboxState):
            # Coerce UnknownSandboxState → DaytonaSandboxState (fresh).
            state = DaytonaSandboxState(recipe=state.recipe)
        super().__init__(session_id=session_id, state=state)
        # Filled in by start(); cleared by shutdown().
        self._daytona_sandbox: Any | None = None
        self._cwd = "/home/daytona"  # provider-specific default

    # ------------------------------------------------------------------ #
    # start: reattach-or-create + supervisor + ACP                        #
    # ------------------------------------------------------------------ #

    async def start(self) -> None:
        """Bring the daytona sandbox up to "ready to receive prompts".

        Idempotent: calling on an already-started session probes liveness
        and returns early if alive.
        """
        if self._supervisor_url is not None and await self.running():
            return

        # Lazy imports — keeps test imports cheap and avoids hauling in the
        # daytona SDK at module-load time.
        from api.providers import daytona as dt_provider
        from api.providers._shared import _wait_for_health

        await self._bootstrap_session()

        # Resolve the daytona sandbox handle: reattach, restart, or create.
        sandbox = await self._resolve_or_create_sandbox(dt_provider)
        self._daytona_sandbox = sandbox
        self.state.sandbox_id = sandbox.id

        # Bring the supervisor up. ``start_supervisor_in_sandbox`` is
        # idempotent (skips re-spawning when one is already healthy on
        # this port); reused across reattach / restart / cold-create.
        url = await dt_provider.start_supervisor_in_sandbox(
            sandbox,
            self.state.recipe.agent_type,
            _SUPERVISOR_PORT,
            root=self.state.recipe.root or "/home/daytona",
            spawn_env=self._spawn_env,
        )
        self._supervisor_url = url
        self.state.listen_port = _SUPERVISOR_PORT

        # Verify the supervisor answers /v1/health before declaring
        # ourselves started. Bounded short wait — start_supervisor already
        # did its own readiness poll, this is just a sanity check.
        ok = await _wait_for_health(url, max_retries=3, interval=0.5)
        if not ok:
            raise RuntimeError(
                f"Supervisor not responding at {url} after start_supervisor_in_sandbox"
            )

        # Mark liveness alive — start_supervisor_in_sandbox just probed
        # /v1/health successfully, so we have direct evidence.
        self.liveness.observe_chunk()

        # ACP attach happens on first execute_prompt; we pre-allocate the
        # acp_session_id so multiple subscribers + multiple prompts share
        # one ACP child.
        if self._acp_session_id is None:
            self._acp_session_id = str(uuid4())
        await self._attach_acp()

        log.info(
            "DaytonaSandboxSession started: session=%s sandbox=%s url=%s",
            self.session_id, sandbox.id[:16], url,
        )

    async def _resolve_or_create_sandbox(self, dt_provider) -> Any:
        """The internal Type-1-vs-Type-2 decision tree, hidden from callers."""
        if self.state.sandbox_id:
            try:
                # Try reattach + resume from pause if needed. The existing
                # restart_daytona_supervisor handles "stopping/starting"
                # transitional states via _wait_for_stable_daytona_state
                # internally, so it's safe under load.
                instance = await dt_provider.restart_daytona_supervisor(
                    self.state.sandbox_id,
                    agent_type=self.state.recipe.agent_type,
                    root=self.state.recipe.root or "/home/daytona",
                    spawn_env=self._spawn_env,
                )
                # restart_daytona_supervisor returned an instance with .url
                # set; we also need the daytona sandbox handle. Get it
                # explicitly so subsequent stop()/exec() calls have it.
                from daytona_sdk import Daytona, DaytonaConfig
                import os as _os
                client = Daytona(DaytonaConfig(api_key=_os.environ["DAYTONA_API_KEY"]))
                loop = asyncio.get_running_loop()
                sandbox = await loop.run_in_executor(
                    None, lambda: client.get(self.state.sandbox_id)
                )
                self._supervisor_url = instance.url
                return sandbox
            except Exception as e:
                msg = str(e).lower()
                if "not found" in msg or "404" in msg:
                    log.info(
                        "DaytonaSandboxSession: sandbox %s not found, will create fresh",
                        (self.state.sandbox_id or "")[:16],
                    )
                    self.state.sandbox_id = None
                else:
                    # Hard error during reattach — propagate, don't silently recreate.
                    raise

        volume_ref = self._volume_ref
        if volume_ref is None:
            volume_ref = await self._ensure_volume_supervisor(dt_provider)

        # Cold create. create_sandbox passes the session volume/subpath so
        # /opt/supervisor is mounted for start_supervisor_in_sandbox().
        instance = await dt_provider.create_sandbox(
            volume_ref=volume_ref,
            subpath=self._subpath or f"sessions/{self.session_id}",
            agent_type=self.state.recipe.agent_type,
            dockerfile=self.state.recipe.dockerfile,
            pre_start_commands=self.state.recipe.pre_start_commands or None,
            root=self.state.recipe.root or "/home/daytona",
            shared_mounts=self.state.recipe.shared_mounts or None,
        )
        from daytona_sdk import Daytona, DaytonaConfig
        import os as _os
        client = Daytona(DaytonaConfig(api_key=_os.environ["DAYTONA_API_KEY"]))
        loop = asyncio.get_running_loop()
        sandbox = await loop.run_in_executor(None, lambda: client.get(instance.sandbox_id))
        return sandbox

    # ------------------------------------------------------------------ #
    # running: liveness oracle                                            #
    # ------------------------------------------------------------------ #

    async def running(self, *, force_probe: bool = False) -> bool:
        """The single liveness oracle. Probes /v1/health when state is
        ``unknown``, otherwise returns last-observed."""
        return await self.liveness.is_alive(force_probe=force_probe)

    async def _liveness_probe(self) -> bool:
        """Cheap GET /v1/health against the supervisor URL."""
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

    async def execute_prompt(
        self, message: str, *, rpc_id: str | None = None,
    ) -> AsyncIterator[Any]:
        """Open an SSE stream for THIS prompt; drain it; close it.

        Per docs §7. No persistent server↔supervisor connection — opens
        on demand, closes at stopReason. Each event is broadcast to all
        subscribers and yielded to the caller.

        For the wiring commit (sub-task 3) this is what replaces the
        persistent _sse_reader_task in server.py.
        """
        if self._supervisor_url is None or self._acp_session_id is None:
            raise RuntimeError("DaytonaSandboxSession.execute_prompt called before start()")

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

        async with httpx.AsyncClient(
            base_url=self._supervisor_url,
            timeout=httpx.Timeout(connect=10, read=_SSE_READ_TIMEOUT_S, write=10, pool=10),
        ) as http:
            # Open SSE stream first so we don't miss early events.
            async with http.stream("GET", f"/v1/acp/{self._acp_session_id}",
                                    headers={"Accept": "text/event-stream"}) as sse:
                sse.raise_for_status()

                # Send the prompt as a fire-and-forget POST. Its response
                # is the JSON-RPC stopReason; we ignore it because the
                # done event arrives via SSE too.
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
    # stop: snapshot + pause                                              #
    # ------------------------------------------------------------------ #

    async def stop(self) -> None:
        """Write FULL filesystem snapshot to volume; then pause the
        sandbox. Per docs §15.3 (always pause, never delete here)."""
        if self._daytona_sandbox is None:
            return
        # Trigger supervisor to write snapshot. The existing
        # supervisor.js exposes ``POST /v1/snapshot`` for this — we just
        # call it; supervisor handles the tarball + write.
        if self._supervisor_url is not None:
            try:
                async with httpx.AsyncClient(timeout=60.0) as client:
                    resp = await client.post(f"{self._supervisor_url}/v1/snapshot",
                                             json={"path": "/vol/snapshot.tar"})
                    if resp.status_code == 200:
                        self.state.snapshot_path = "/vol/snapshot.tar"
                        self.state.snapshot_version += 1
            except Exception:
                log.exception("snapshot request failed for session %s", self.session_id)

        # Always-pause policy (docs §15.3).
        from api.providers import daytona as dt_provider
        from api.providers import ProviderInstance
        try:
            await dt_provider.stop_daytona(ProviderInstance(
                provider="daytona", url=self._supervisor_url or "",
                root=self.state.recipe.root or "/home/daytona",
                sandbox_id=self.state.sandbox_id or "",
            ))
        except Exception:
            log.exception("daytona.stop failed for session %s", self.session_id)

    # ------------------------------------------------------------------ #
    # shutdown: in-memory cleanup                                         #
    # ------------------------------------------------------------------ #

    async def shutdown(self) -> None:
        """Final teardown of in-memory state. Idempotent."""
        self._daytona_sandbox = None
        self._supervisor_url = None
        self._close_subscribers()


# ---------------------------------------------------------------------------
# SSE parsing helper
# ---------------------------------------------------------------------------


def _parse_sse_block(block: str, rpc_id: str) -> dict[str, Any] | None:
    """Parse one ``data: <json>\\n`` block into a structured event dict.

    Filters by rpc_id — events for OTHER prompts (concurrent ACP traffic)
    are skipped. Returns None on malformed blocks or non-event lines
    (e.g. ``: heartbeat``).
    """
    data_lines = [
        line[5:].lstrip() for line in block.splitlines()
        if line.startswith("data:")
    ]
    if not data_lines:
        return None
    raw = "\n".join(data_lines)
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        return None
    # Match by rpc_id (id field on the JSON-RPC envelope).
    if isinstance(payload, dict):
        msg_id = payload.get("id")
        if msg_id is not None and msg_id != rpc_id:
            return None
        if "result" in payload and isinstance(payload["result"], dict):
            if "stopReason" in payload["result"]:
                return {"type": "done", "stop_reason": payload["result"]["stopReason"]}
        if "error" in payload:
            return {"type": "error", "error": payload["error"]}
        # session/update notifications carry the actual text/tool events.
        if payload.get("method") == "session/update":
            update = payload.get("params", {}).get("update", {})
            update_type = update.get("sessionUpdate")
            if update_type == "agent_message_chunk":
                content = update.get("content", {})
                if content.get("type") == "text":
                    return {"type": "text", "text": content.get("text", "")}
            return {"type": update_type or "unknown", "raw": update}
    return None
