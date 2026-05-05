"""DaytonaSandboxSession — concrete SandboxSession for the daytona provider.

Wraps existing primitives in ``src/api/providers/daytona/__init__.py``
into the five-method ``BaseSandboxSession`` contract.

Lifecycle decisions live inside ``start()``:
  * ``state.sandbox_ref`` set, sandbox alive on Daytona  → reattach (cheapest)
  * ``state.sandbox_ref`` set, sandbox stopped/paused    → ``daytona.start()`` (resume)
  * ``state.sandbox_ref`` missing or sandbox not found   → fresh ``daytona.create()``

No "Type 1 vs Type 2" branching outside this class — recovery just calls
``start()``; the class picks the cheapest path internally.

"""
from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import AsyncIterator
from typing import Any
from uuid import uuid4

import httpx

from api.sandbox.session import BaseSandboxSession
from api.sandbox.state import DaytonaSandboxState, SandboxState

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
        self.state.sandbox_ref = sandbox.id

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
        if self.state.sandbox_ref:
            try:
                # Try reattach + resume from pause if needed. The existing
                # restart_daytona_supervisor handles "stopping/starting"
                # transitional states via _wait_for_stable_daytona_state
                # internally, so it's safe under load.
                instance = await dt_provider.restart_daytona_supervisor(
                    self.state.sandbox_ref,
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
                    None, lambda: client.get(self.state.sandbox_ref)
                )
                self._supervisor_url = instance.url
                return sandbox
            except Exception as e:
                # Whether the sandbox is genuinely missing (404) or alive
                # but unreachable for any other reason — Daytona 5xx,
                # supervisor wedged, port held, disk full, OOM — the
                # answer is the same: abandon the ref and cold-create
                # a fresh sandbox. The previously-attempted sandbox
                # stays labelled ``agent_sdk_origin`` in Daytona for
                # ``cleanup_orphans.py`` to reap. Without this
                # fall-through, a wedged sandbox locks the session
                # forever — every retry hits the same dead reattach.
                log.warning(
                    "DaytonaSandboxSession: reattach to %s failed (%s); abandoning + cold-creating",
                    (self.state.sandbox_ref or "")[:16], e,
                )
                self.state.sandbox_ref = None

        # ``_bootstrap_session`` (in ``BaseSandboxSession``) ran from
        # ``start()`` before this method was called and unconditionally
        # set ``_volume_ref`` from the session's volume row. Assert the
        # invariant so a future refactor that decouples the two methods
        # fails loudly here instead of falling through to a phantom
        # ``_ensure_volume_supervisor`` (which never existed on this class).
        assert self._volume_ref is not None, (
            "_bootstrap_session must run before _resolve_or_create_sandbox"
        )
        volume_ref = self._volume_ref

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
            resources=self.state.recipe.resources,
        )
        from daytona_sdk import Daytona, DaytonaConfig
        import os as _os
        client = Daytona(DaytonaConfig(api_key=_os.environ["DAYTONA_API_KEY"]))
        loop = asyncio.get_running_loop()
        sandbox = await loop.run_in_executor(None, lambda: client.get(instance.sandbox_ref))
        return sandbox

    # ------------------------------------------------------------------ #
    # running: liveness oracle                                            #
    # ------------------------------------------------------------------ #

    async def running(self, *, force_probe: bool = False) -> bool:
        """The single liveness oracle. Probes /v1/health when state is
        ``unknown``, otherwise returns last-observed."""
        return await self.liveness.is_alive(force_probe=force_probe)

    async def _liveness_probe(self) -> bool:
        """Liveness probe layered for Daytona's actual semantics.

        Layer 1 — fast path: GET /v1/health against the signed URL.
        Returns True on 200; returns False on 4xx (supervisor up but
        said no); falls through on 5xx / connection errors.

        Layer 2 — transition-aware: on layer-1 failure, consult the
        Daytona control plane for sandbox state. If the sandbox is in
        a transitional state (starting / stopping / pulling_image /
        resizing / archiving / destroying), poll for stable state up
        to 10s then retry the probe — the URL was failing because the
        sandbox was mid-transition, not because the supervisor died.
        If the sandbox is in a stable non-started state (stopped,
        paused, error, archived, destroyed), return False — caller
        cold-recovers via restart_daytona_supervisor (which itself
        does the longer 45s _wait_for_stable). If the sandbox IS
        started but the URL still fails after one retry, the supervisor
        process inside is dead — return False.

        Why not unconditionally call the control plane: every layer-1
        success path stays a single HTTP RTT against the signed URL.
        Only the failure path pays the extra ~100ms Daytona API call.
        """
        if self._supervisor_url is None or self._daytona_sandbox is None:
            return False

        async def _probe_url() -> tuple[bool, int | None]:
            """Returns (alive, status_code or None on connection error)."""
            try:
                async with httpx.AsyncClient(timeout=2.0) as client:
                    resp = await client.get(f"{self._supervisor_url}/v1/health")
                return resp.status_code == 200, resp.status_code
            except Exception:
                return False, None

        ok, status = await _probe_url()
        if ok:
            return True
        # 4xx: supervisor is up but said no — don't retry, don't query state.
        if status is not None and 400 <= status < 500:
            return False

        # Layer 2: consult sandbox state. Transitional → wait + retry.
        sandbox_state = await self._daytona_sandbox_state()
        TRANSITIONAL = {
            "starting", "stopping", "pulling_image", "resizing",
            "archiving", "destroying", "creating",
        }
        if sandbox_state in TRANSITIONAL:
            # Wait briefly for the sandbox to leave the transitional
            # state. We poll state rather than re-probing in a loop
            # because the URL won't recover before state stabilises.
            import asyncio as _asyncio
            for _ in range(20):  # 20 * 0.5s = 10s
                await _asyncio.sleep(0.5)
                sandbox_state = await self._daytona_sandbox_state()
                if sandbox_state not in TRANSITIONAL:
                    break
            # Stable now — give the URL one more shot.
            ok, _ = await _probe_url()
            return ok

        # Stable non-started or supervisor-dead-inside-live-sandbox.
        # Either way the caller's cold-recovery is the right move.
        return False

    async def _daytona_sandbox_state(self) -> str:
        """Fetch current Daytona sandbox state string. Empty on error
        — caller treats unknown state as non-transitional."""
        try:
            from daytona_sdk import Daytona, DaytonaConfig
            import os as _os
            client = Daytona(DaytonaConfig(api_key=_os.environ["DAYTONA_API_KEY"]))
            loop = asyncio.get_running_loop()
            sb = await loop.run_in_executor(
                None, lambda: client.get(self._daytona_sandbox.id),
            )
            raw = sb.state
            return (raw.value if hasattr(raw, "value") else str(raw)).lower()
        except Exception:
            return ""

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

        # Use SEPARATE httpx clients for the SSE GET and the session/prompt
        # POST. Sharing one client serialises both requests on the same
        # keep-alive connection and prematurely closes the SSE stream
        # (~1.5s after the POST lands). See unix_local for matching fix.
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
                            # rpc-tagged tuple so /events emits ``event: rpc:<id>``
                            # and the legacy test/UI ``extract_sse_tag`` can
                            # correlate per-prompt streams.
                            self._broadcast((rpc_id, block))
                            yield event
                            # Both ``done`` (clean stopReason — end_turn /
                            # cancelled / max_tokens / max_turn_requests) and
                            # ``error`` (top-level JSON-RPC error envelope —
                            # auth failure, internal error, process death)
                            # signal that ACP is finished with this rpc_id and
                            # will write nothing else for it. Stop iterating
                            # so the SSE stream closes promptly. Tool failures
                            # are ``session/update`` notifications and surface
                            # as ``tool_result`` / ``update`` events — they
                            # never become ``type=="error"``, so this check
                            # cannot accidentally end a turn the LLM is still
                            # recovering from.
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
                sandbox_ref=self.state.sandbox_ref or "",
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

    Single source of truth: delegates to ``api.sse.parse_acp_event`` so
    every consumer (SDK ``astream``, server ``_persist_prompt_events``,
    /events SSE) sees the same event taxonomy. Returns ``None`` for
    heartbeats, non-event meta updates (e.g. ``available_commands_update``),
    empty-text chunks, or events whose JSON-RPC ``id`` doesn't match
    ``rpc_id`` (concurrent ACP traffic on the same supervisor).
    """
    from api.sse import parse_acp_event
    return parse_acp_event(block, rpc_id)
