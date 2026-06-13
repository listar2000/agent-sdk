"""SupervisorSandboxSession — the one reattach-or-create start()/stop() template.

docker, unix_local, and modal sessions ran line-for-line copies of the same
algorithm: probe the persisted sandbox_ref, reattach (optionally reviving a
stopped sandbox), else cold-create; health-gate; attach ACP; on stop,
snapshot then stop the sandbox and clear the ref. The copies differed only
in declared, behavior-bearing details — which this template encodes as
class attributes and four small hooks, pinned one-by-one in
tests/test_supervisor_session_template.py BEFORE the collapse:

  * docker passes ``sandbox_ref=session_id`` into create (it becomes the
    ``agent-sdk.sandbox-id`` container label that orphan reconciliation
    filters on) and DESTROYS a wedged container on reattach-health-failure.
  * modal resolves its reattach URL from the provider (tunnel URLs are not
    derivable from the ref), never revives "stopped" (terminate is
    destructive), health-gates only on reattach (create health-waits
    internally), retries ACP attach, and tears down a freshly-created
    sandbox when attach fails (it isn't pool-visible yet — would leak).
  * unix_local is the default row.

Daytona deliberately does NOT inherit this: its two-phase start
(create → start_supervisor_in_sandbox, signed URLs, S3-FUSE settle) is a
genuinely different algorithm and keeps its own class.
"""

from __future__ import annotations

import importlib
import logging
from uuid import uuid4

from .session import BaseSandboxSession

log = logging.getLogger(__name__)


class SupervisorSandboxSession(BaseSandboxSession):
    """Template start()/stop() for supervisor-runtime sessions.

    Subclasses set ``_provider_mod`` plus the knobs below, and override the
    hooks where their provider genuinely diverges. The provider module is
    resolved at call time so test monkeypatches on module attributes
    (``create_sandbox``, ``get_sandbox_status``, ...) keep working.
    """

    #: module name under ``api.providers`` (resolved lazily per call)
    _provider_mod: str = ""
    #: where the supervisor writes the per-release snapshot tar
    _snapshot_path: str = "/v/snapshot.tar"
    #: health-gate budget after the instance is resolved
    _health_retries: int = 10
    _health_interval: float = 0.3
    #: modal's create health-waits internally — it only gates on reattach
    _health_on_fresh_create: bool = True
    #: docker/local revive a "stopped" sandbox in place (same-ref invariant)
    _revive_stopped: bool = True
    #: modal tears down a fresh, not-yet-pool-visible sandbox on attach failure
    _cleanup_fresh_on_attach_failure: bool = False
    #: health-failure message (``{url}`` formatted in)
    _health_fail_msg: str = "Supervisor not responding at {url}"

    # ── hooks ────────────────────────────────────────────────────────────────

    def _mod(self):
        return importlib.import_module(f"api.providers.{self._provider_mod}")

    async def _reattach_url(self, mod, status: str) -> str | None:
        """URL to reattach with, or None to fall through to cold-create.
        Default: port-template URL; revives a stopped sandbox in place."""
        if status == "running":
            return f"http://127.0.0.1:{self.state.listen_port}"
        if status == "stopped" and self._revive_stopped:
            await mod.start_sandbox(self.state.sandbox_ref)
            return f"http://127.0.0.1:{self.state.listen_port}"
        return None

    def _create_kwargs(self) -> dict:
        """Extra per-provider kwargs for ``create_sandbox`` (docker: the
        reconcile label ref + resources; modal: resources)."""
        return {}

    def _on_instance_resolved(self, instance) -> None:
        """Called once the instance is known, before the health gate
        (docker records its container handle here)."""

    async def _on_wedged_reattach(self, mod, instance) -> None:
        """Reattached but unhealthy: abandon the ref so the next recovery
        cold-creates instead of looping on the wedged sandbox."""
        self.state.sandbox_ref = None

    async def _attach(self) -> None:
        await self._attach_acp()

    def _stop_ready(self) -> bool:
        return self.state.sandbox_ref is not None

    # ── template ─────────────────────────────────────────────────────────────

    async def start(self) -> None:
        # Idempotence + self-heal: a second start() on a live session is a
        # no-op; on a session whose supervisor died it falls through to the
        # full reattach-or-create flow (running() probes — no cached verdict).
        if self._supervisor_url is not None and await self.running():
            return

        from api.providers import _shared
        mod = self._mod()

        volume_ref = await self._bootstrap_session()

        instance = None
        reattached = False
        created_fresh = False
        # Keep the persisted sandbox if we can — the same-sandbox-after-
        # external-stop invariant (test_stop_sandbox_same_sandbox_after_restart).
        if self.state.sandbox_ref:
            try:
                status = await mod.get_sandbox_status(self.state.sandbox_ref)
                url = await self._reattach_url(mod, status)
                if url:
                    instance = self._provider_instance(
                        url=url,
                        sandbox_ref=self.state.sandbox_ref,
                        port=self.state.listen_port,
                    )
                    self._on_instance_resolved(instance)
                    # Gate reattach on health HERE, not after committing below.
                    # A WEDGED target — supervisor dead/suspended while the
                    # provider still reports the sandbox reattachable (a killed
                    # PID-1 supervisor whose control plane lags, a SIGSTOPped
                    # one, an OOM'd ACP child) — must RECOVER by cold-creating
                    # below, never raise. Raising here escapes get_session as a
                    # 500 on POST /message and strands the caller's turn; the
                    # volume carries the workspace, so a fresh sandbox resumes
                    # transparently. The provider hook abandons the wedged ref
                    # (docker DESTROYS the container — else the DB row's stale
                    # ref reattaches to it and loops forever).
                    if await _shared._wait_for_health(
                        url,
                        max_retries=self._health_retries,
                        interval=self._health_interval,
                    ):
                        reattached = True
                    else:
                        await self._on_wedged_reattach(mod, instance)
                        instance = None
            except Exception:
                instance = None
                reattached = False

        if instance is None:
            instance = await mod.create_sandbox(
                volume_ref=volume_ref,
                subpath=self._subpath or f"sessions/{self.session_id}",
                agent_type=self.state.recipe.agent_type,
                root=self.state.recipe.root,
                spawn_env=self._spawn_env,
                pre_start_commands=self.state.recipe.pre_start_commands or None,
                shared_mounts=self.state.recipe.shared_mounts or None,
                **self._create_kwargs(),
            )
            self.state.sandbox_ref = instance.sandbox_ref
            self.state.listen_port = instance.port
            created_fresh = True
            self._on_instance_resolved(instance)

        self._supervisor_url = instance.url

        # A FRESHLY-created sandbox that won't come healthy cannot be recovered
        # by creating yet another one (it would loop), so this gate still
        # raises. Reattach health is gated above, where failure recovers
        # instead. (Providers whose create() health-waits internally set
        # ``_health_on_fresh_create = False`` and skip this entirely.)
        if created_fresh and self._health_on_fresh_create:
            ok = await _shared._wait_for_health(
                instance.url,
                max_retries=self._health_retries,
                interval=self._health_interval,
            )
            if not ok:
                raise RuntimeError(self._health_fail_msg.format(url=instance.url))

        try:
            self.liveness.observe_chunk()
            if self._acp_session_id is None:
                self._acp_session_id = str(uuid4())
            await self._attach()
        except Exception:
            if self._cleanup_fresh_on_attach_failure:
                # A freshly-created sandbox is not pool-visible until start()
                # returns and state persists; tear it down or it leaks as
                # "created but unregistered".
                if created_fresh and self.state.sandbox_ref:
                    try:
                        await mod.stop_sandbox(self._provider_instance(
                            url=self._supervisor_url or "",
                            sandbox_ref=self.state.sandbox_ref,
                            port=self.state.listen_port,
                        ))
                    except Exception:
                        log.exception(
                            "cleanup after attach failure failed: session=%s sandbox=%s",
                            self.session_id, self.state.sandbox_ref,
                        )
                if created_fresh or reattached:
                    self.state.sandbox_ref = None
                    self.state.listen_port = None
                self._supervisor_url = None
            raise

        log.info(
            "%s started: session=%s sandbox=%s url=%s",
            type(self).__name__, self.session_id,
            (self.state.sandbox_ref or "")[:16], instance.url,
        )

    async def stop(self) -> None:
        if not self._stop_ready():
            return
        await self._write_snapshot(self._snapshot_path)

        mod = self._mod()
        try:
            await mod.stop_sandbox(self._provider_instance(
                url=self._supervisor_url or "",
                sandbox_ref=self.state.sandbox_ref or "",
                port=self.state.listen_port,
            ))
        except Exception:
            log.exception(
                "%s.stop_sandbox failed for session %s",
                self._provider_mod, self.session_id,
            )
        # Compute is stopped/terminated; clear the ref so the next start
        # cold-creates against the same volume subpath (snapshot restores).
        self.state.sandbox_ref = None
        self.state.listen_port = None
