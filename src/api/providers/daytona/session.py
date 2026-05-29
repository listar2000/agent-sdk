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

import logging
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

        # ``start_supervisor_in_sandbox`` returned only after its own
        # ``_wait_for_health`` saw a 200 — the supervisor IS healthy as
        # of microseconds ago. We used to do another 3-attempt poll here
        # as a "sanity check" but it never caught anything that ``ACP
        # attach`` (which fires next, also via HTTP to the same URL)
        # wouldn't catch on the same round trip; it just added 100-500ms
        # to every session_create. Trust the upstream signal and let ACP
        # attach be the next probe.
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
                # set; we also need the daytona sandbox handle for later
                # stop()/exec() calls. Use the process-shared async client
                # — the handle is an ``AsyncSandbox`` which threads through
                # ``start_supervisor_in_sandbox`` and the snapshot path.
                client = await dt_provider._get_async_daytona_client()
                sandbox = await client.get(self.state.sandbox_ref)
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
        # Async-singleton path: returns an ``AsyncSandbox`` handle that
        # threads through ``start_supervisor_in_sandbox`` and the snapshot
        # path without any further sync boundary.
        client = await dt_provider._get_async_daytona_client()
        sandbox = await client.get(instance.sandbox_ref)
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

        # Layer 1 uses the cached AcpClient's pooled httpx connection so
        # we don't pay TLS handshake to the Daytona signed URL on every
        # probe. PR #82 fixed this for the message proxy paths; same
        # win applies to the probe.
        client = self._get_acp_client()

        async def _probe_url() -> tuple[bool, int | None]:
            return await client.health_probe()

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
        — caller treats unknown state as non-transitional. Uses the
        process-shared async Daytona client so we don't pay SDK init
        on every probe layer-2 fallback."""
        try:
            from . import _get_async_daytona_client
            client = await _get_async_daytona_client()
            sb = await client.get(self._daytona_sandbox.id)
            raw = sb.state
            return (raw.value if hasattr(raw, "value") else str(raw)).lower()
        except Exception:
            return ""

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
        await self._aclose_acp_client()
