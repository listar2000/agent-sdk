"""Daytona provider — create/destroy/exec in Daytona sandboxes.

Package layout:
- ``__init__.py`` (this file) — volume + sandbox primitives (functional,
  stateless). Loaded by ``api.providers.__init__``'s dispatch table.
- ``session.py`` — ``DaytonaSandboxSession``, the per-session lifecycle
  class. Loaded directly via ``from api.providers.daytona.session
  import DaytonaSandboxSession`` (avoids a circular import with
  ``api.sandbox.session`` which session.py depends on).
"""

import asyncio
import logging
import os
import shlex
import time
import uuid
from pathlib import Path
from typing import NamedTuple

from ... import load_dotenv

log = logging.getLogger(__name__)

load_dotenv()


class _ExecResult(NamedTuple):
    stdout: str
    stderr: str
    exit_code: int | None

    @property
    def ok(self) -> bool:
        # exit_code None = SDK didn't report it → treat as OK, log warning elsewhere
        return self.exit_code in (None, 0)


def _run_sandbox_exec(sandbox, cmd: str, timeout: int = 120) -> "_ExecResult":
    """Run ``cmd`` in ``sandbox`` and return stdout, stderr, and exit_code.

    Defensive against SDK versions that may not expose all fields.
    Callers that want tolerant behaviour for a command that may fail should
    wrap their command with ``|| true`` so the shell always exits 0.
    """
    r = sandbox.process.exec(cmd, timeout=timeout)
    return _ExecResult(
        stdout=getattr(r, "result", "") or "",
        stderr=getattr(r, "stderr", "") or "",
        exit_code=getattr(r, "exit_code", None),
    )


# Re-import shared helpers from __init__ to avoid circular imports.
# These are defined here inline or imported lazily.
from .._shared import (
    _acp_bin_name,
    _acp_launch_args,
    _ACP_NPM_SPECS,
    _build_env_prefix,
    _build_volume_mounts,
    _get_sandbox_env_vars,
    _read_runtime_image_tag,
    _read_runtime_snapshot_tag,
    _safe_path,
    _wait_for_health,
    build_supervisor_argv,
    ProviderInstance,
    VolumeFileExistsError,
    normalize_find_output,
)

# the runtime-image-unification refactor: ``_SUPERVISOR_DIR``,
# ``_SUPERVISOR_REMOTE_DIR``, and ``_SUPERVISOR_VOLUME_DIR`` were deleted
# along with the install/cache helpers that used them. The supervisor now
# lives at ``/opt/agent-sdk/runtime/`` inside the daytona sandbox image.
_SUPERVISOR_REMOTE_PORT = 9100

# The agent's HOME inside a Daytona sandbox — a LOCAL ext4 directory the
# supervisor creates on boot. It is populated either from the snapshot
# tarball at _SNAPSHOT_PATH (see below) or left empty for a brand-new agent.
# Critically this is NOT a volume mount: mountpoint-s3 can't handle
# append-only writes (session JSONLs) or POSIX rename semantics, so we
# keep the hot filesystem local and only round-trip a single tarball to
# the volume.
_DAYTONA_AGENT_HOME = "/home/daytona"

# Per-session volume subpath mount point. Daytona-only. supervisor.js
# writes the rolling workspace snapshot to `{_DAYTONA_VOLUME_MOUNT}/snapshot.tar`
# after every turn and restores from it on boot.
_DAYTONA_VOLUME_MOUNT = "/vol"
_SNAPSHOT_PATH = f"{_DAYTONA_VOLUME_MOUNT}/snapshot.tar"


# Single label so test orphans can be identified and bulk-deleted via
# ``daytona.list(labels={"agent_sdk_origin": "test"})``. Production
# sandboxes default to ``"production"`` so the same query never touches
# them. Set ``AGENT_SDK_ORIGIN=test`` in the test process before launching
# the server.
_LABEL_ORIGIN = "agent_sdk_origin"


def _sandbox_labels() -> dict[str, str]:
    return {_LABEL_ORIGIN: os.environ.get("AGENT_SDK_ORIGIN", "production")}


def _get_daytona_client():
    """Get a Daytona SDK client. Raises ImportError or RuntimeError on failure."""
    from daytona_sdk import Daytona, DaytonaConfig
    api_key = os.environ.get("DAYTONA_API_KEY")
    if not api_key:
        raise RuntimeError("DAYTONA_API_KEY not set")
    return Daytona(DaytonaConfig(api_key=api_key))


async def start_supervisor_in_sandbox(
    sandbox, agent_type: str, port: int, root: str = "/tmp",
    spawn_env: dict[str, str] | None = None,
) -> str:
    """Start a NEW supervisor on a specific port inside an existing sandbox.

    If the volume has a cached deps.tar.gz (Phase 2+), extract it to a local
    ephemeral directory and run supervisor from there.  Extraction from a
    single archive read is fast; writing thousands of node_modules to the
    network volume at install time is avoided entirely.

    Falls back to the legacy /tmp install path when no volume cache exists.
    Returns the signed preview URL for this supervisor.

    Emits ``[BENCH] daytona.start_supervisor phase=<name> s=<seconds>`` log
    lines for each critical-path phase; grep-friendly for recovery-time
    benchmarks (scripts/bench_recovery.py).
    """
    bin_name = _acp_bin_name(agent_type)
    loop = asyncio.get_running_loop()
    sid8 = sandbox.id[:8] if sandbox.id else "?"
    total_t0 = time.monotonic()
    phases: list[tuple[str, float]] = []

    def _bench(phase: str, t0: float) -> None:
        dt = time.monotonic() - t0
        phases.append((phase, dt))
        log.info("[BENCH] daytona.start_supervisor sandbox=%s phase=%s s=%.3f",
                 sid8, phase, dt)

    def _exec(cmd: str, timeout: int = 120) -> str:
        return _run_sandbox_exec(sandbox, cmd, timeout=timeout).stdout

    # (0) Idempotency fast-path: if a supervisor is already healthy on
    # this port (left over from a prior call in the same sandbox), skip
    # the 2-3 s extract+spawn dance and just mint a fresh signed URL
    # pointing at it. Saves redundant respawns when concurrent recovery
    # paths (Type 1 retries, pool resume + a parallel cancel) both land
    # here under separate sandbox locks.
    t0 = time.monotonic()
    try:
        existing = await loop.run_in_executor(None, lambda: _exec(
            # `-m 2` request-timeout, `-o /dev/null -w '%{http_code}'`
            # prints just the status line so we can string-match cheaply.
            # NOTE: supervisor exposes /v1/health (matches _wait_for_health
            # in providers/_shared.py), not /healthz.
            f"curl -s -m 2 -o /dev/null -w '%{{http_code}}' "
            f"http://127.0.0.1:{port}/v1/health 2>/dev/null || echo 000",
            10,
        ))
    except Exception as e:
        # Not fatal — fall through to the normal spawn path.
        existing = "000"
        log.debug("idempotency probe raised for sandbox=%s port=%d: %s",
                  sid8, port, e)
    _bench("idempotency_probe", t0)
    if existing.strip() == "200":
        t0 = time.monotonic()
        signed = await loop.run_in_executor(
            None, lambda: sandbox.create_signed_preview_url(port, 24 * 3600)
        )
        _bench("mint_url_only", t0)
        url = signed.url.rstrip("/")
        total_dt = time.monotonic() - total_t0
        log.info(
            "[BENCH] daytona.start_supervisor sandbox=%s TOTAL s=%.3f "
            "(reused-existing %s)",
            sid8, total_dt,
            ", ".join(f"{p}={d:.2f}" for p, d in phases),
        )
        log.info(
            "supervisor on port %d already running, reusing: %s "
            "(sandbox %s)", port, url[:60], sandbox.id[:16],
        )
        return url

    # the runtime-image-unification refactor: the daytona sandbox boots
    # from an image whose ``/opt/agent-sdk/runtime/`` already contains
    # supervisor.js + every ACP bin. No volume-side cache check, no
    # deps.tar.gz extract, no legacy /tmp install — all gone with the image.
    # The bin path is resolved via ``package.json#bin`` (not
    # ``node_modules/.bin/``) because daytona's image-build flattens
    # symlinks; the underlying scripts survive but the symlinks don't.
    from .._shared import _runtime_acp_bin_relative
    sup_dir = "/opt/agent-sdk/runtime"
    acp_bin = f"{sup_dir}/{_runtime_acp_bin_relative(agent_type)}"
    log.info(
        "start_supervisor_in_sandbox: using image runtime %s "
        "(port %d, sandbox %s)",
        sup_dir, port, sandbox.id[:16],
    )

    env_prefix = _build_env_prefix(spawn_env)
    log_file = f"{sup_dir}/sup-{port}.log"
    supervisor_argv = build_supervisor_argv(
        supervisor_js="supervisor.js", acp_bin=acp_bin,
        acp_launch_args=_acp_launch_args(agent_type),
        port=port, root=root,
        snapshot_path=_SNAPSHOT_PATH, quote_paths=False,
    )
    # Ensure the agent's HOME (``root``) exists inside the sandbox before
    # the supervisor spawns the ACP child with ``cwd=root``. If the volume
    # subpath hasn't been pre-created, node.spawn() fails with a
    # misleading ENOENT pointing at the binary rather than the cwd.
    inner = (
        f"mkdir -p {shlex.quote(root)} && "
        f"cd {sup_dir} && "
        f"setsid env {env_prefix} {supervisor_argv} "
        f"> {log_file} 2>&1 </dev/null & echo started"
    )
    start_cmd = f"sh -c {shlex.quote(inner)}"

    # (3) Parallelize the detached-spawn exec with the signed-URL mint.
    # The URL doesn't depend on whether node has booted yet, and the
    # spawn exec returns as soon as setsid forks — both are in-flight
    # network calls that we don't need to serialize.
    t0 = time.monotonic()
    _, signed = await asyncio.gather(
        loop.run_in_executor(None, lambda: _exec(start_cmd, timeout=10)),
        loop.run_in_executor(
            None, lambda: sandbox.create_signed_preview_url(port, 24 * 3600)
        ),
    )
    _bench("spawn+mint_url", t0)
    url = signed.url.rstrip("/")

    t0 = time.monotonic()
    # Budget covers worst-case Type 2 boot inside supervisor.js: cold
    # snapshot poll (15 s) + agent_memory poll (15 s) + ACP child spawn
    # (~2 s) + slack. Type 1 (warm restart, sentinel present) finishes
    # in <2 s and exits this poll on the first probe.
    healthy = await _wait_for_health(url, max_retries=45, interval=1)
    _bench("health_wait", t0)
    if not healthy:
        log_out = await loop.run_in_executor(None, lambda: _exec(f"tail -40 {log_file} 2>&1"))
        raise RuntimeError(
            f"supervisor on port {port} in sandbox {sandbox.id} failed health check; log:\n{log_out[:800]}"
        )

    total_dt = time.monotonic() - total_t0
    log.info("[BENCH] daytona.start_supervisor sandbox=%s TOTAL s=%.3f (%s)",
             sid8, total_dt, ", ".join(f"{p}={d:.2f}" for p, d in phases))
    log.info("supervisor on port %d ready: %s (sandbox %s, dir %s)", port, url[:60], sandbox.id[:16], sup_dir)
    return url


# ``_resolve_legacy_volume_supervisor`` was deleted in Phase E of
# the runtime-image-unification refactor — all its volume-cache + tar-extract +
# legacy-fallback work is obsolete now that the runtime is in the image.


async def kill_supervisor_in_sandbox(sandbox, port: int) -> None:
    """Kill a supervisor process by port inside a Daytona sandbox."""
    loop = asyncio.get_running_loop()
    try:
        await loop.run_in_executor(
            None,
            lambda: _run_sandbox_exec(sandbox, f"fuser -k {port}/tcp 2>/dev/null || true", timeout=10),
        )
    except Exception as e:
        log.warning("kill_supervisor_in_sandbox port=%d failed: %s", port, e)


async def provision_daytona_sandbox(
    agent_type: str = "claude",
    dockerfile: str | None = None,
    pre_start_commands: list[str] | None = None,
    root: str = "/tmp",
    volume_id: str | None = None,
    subpath: str | None = None,
    shared_mounts: list[str] | None = None,
) -> ProviderInstance:
    """Create a Daytona sandbox with 3 volume mounts, but do NOT install deps
    or start a supervisor (those are handled by ensure_volume_supervisor and
    ensure_supervisor_url respectively).

    Returns a ProviderInstance with sandbox_ref but no usable supervisor URL.
    Supervisors are started per-session via start_supervisor_in_sandbox().
    The supervisor binary + ACP package are expected to already be installed on
    the volume at system/supervisor/ (mounted at /opt/supervisor).
    """
    try:
        from daytona_sdk import (
            Daytona, DaytonaConfig, CreateSandboxFromImageParams,
            CreateSandboxFromSnapshotParams,
        )
    except ImportError:
        raise RuntimeError("daytona-sdk not installed. Run: pip install daytona-sdk")

    api_key = os.environ.get("DAYTONA_API_KEY")
    if not api_key:
        raise RuntimeError("DAYTONA_API_KEY not set")

    env_vars = _get_sandbox_env_vars()
    loop = asyncio.get_running_loop()
    daytona = Daytona(DaytonaConfig(api_key=api_key))

    # the runtime-image-unification refactor: the runtime is baked
    # into the agent-sdk Docker image. Provisioning needs either a
    # snapshot (faster cold-start, registered by ``scripts/release.sh``
    # via Daytona's ``Image.from_dockerfile``) or an image reference. The
    # legacy "hive-large" default is gone — it predates the runtime
    # baking and would silently skip it.
    #
    # Snapshot precedence: ``DAYTONA_SNAPSHOT`` env > ``.runtime-snapshot-tag``
    # repo file. Image precedence: ``DAYTONA_IMAGE`` > ``AGENT_SDK_IMAGE``
    # > ``.runtime-image-tag``. The repo files are written by release.sh
    # so a fresh checkout's daytona path "just works" without any env-var
    # plumbing in launch scripts.
    snapshot = (
        os.environ.get("DAYTONA_SNAPSHOT", "").strip()
        or (_read_runtime_snapshot_tag() or "")
    )
    use_snapshot = dockerfile is None and snapshot.lower() not in {"", "0", "false", "image"}

    if dockerfile is not None:
        if not Path(dockerfile).exists():
            raise FileNotFoundError(f"Dockerfile not found: {dockerfile}")
        from daytona_sdk import Image
        image = Image.from_dockerfile(dockerfile)
    elif not use_snapshot:
        # Same precedence as docker.create_sandbox: per-provider override >
        # cross-provider override > committed pin.
        image = (
            os.environ.get("DAYTONA_IMAGE")
            or os.environ.get("AGENT_SDK_IMAGE")
            or _read_runtime_image_tag()
        )
        if not image:
            raise RuntimeError(
                "Daytona provisioning requires DAYTONA_IMAGE / "
                "AGENT_SDK_IMAGE / .runtime-image-tag (produced by "
                "scripts/release.sh)."
            )

    create_timeout = 300 if dockerfile else 60

    volumes = _build_volume_mounts(volume_id, subpath, shared_mounts)
    labels = _sandbox_labels()

    if use_snapshot:
        sandbox = await loop.run_in_executor(None, lambda: daytona.create(
            CreateSandboxFromSnapshotParams(
                snapshot=snapshot, auto_stop_interval=0, env_vars=env_vars,
                volumes=volumes, labels=labels,
            ), timeout=create_timeout,
        ))
    else:
        sandbox = await loop.run_in_executor(None, lambda: daytona.create(
            CreateSandboxFromImageParams(
                image=image, auto_stop_interval=0, env_vars=env_vars,
                volumes=volumes, labels=labels,
            ), timeout=create_timeout,
        ))

    try:
        # Run pre-start commands (skills, CLI install, etc.).
        # On non-zero exit the command raises so provisioning fails loudly.
        # Callers that want tolerant behaviour should wrap their command with
        # ``|| true`` so the shell always exits 0.
        if pre_start_commands:
            for cmd in pre_start_commands:
                log.info("provision pre-start: %s", cmd)
                # Pin HOME to the daytona agent home so user commands
                # (e.g. ``npx skills add ... -g``) write into the same
                # ``$HOME/.claude/skills/`` directory the supervisor
                # later spawns Claude under (cwd=/home/daytona). Without
                # this the shell's default HOME is /root and skills end
                # up in /root/.claude/skills/, invisible to Claude.
                wrapped = (
                    f"export HOME={_DAYTONA_AGENT_HOME} && "
                    f"mkdir -p {_DAYTONA_AGENT_HOME} && {cmd}"
                )
                result = await loop.run_in_executor(
                    None, lambda c=wrapped: _run_sandbox_exec(sandbox, c, timeout=120),
                )
                if result.exit_code is None:
                    log.warning(
                        "pre-start command ran but Daytona SDK returned no exit_code "
                        "— can't confirm success: %s", cmd,
                    )
                elif result.exit_code != 0:
                    snippet = (result.stderr or result.stdout or "")[-500:]
                    log.error(
                        "pre-start command failed (exit=%s): %s\n---stderr---\n%s",
                        result.exit_code, cmd, snippet,
                    )
                    raise RuntimeError(
                        f"pre_start_commands failed on Daytona sandbox "
                        f"(exit={result.exit_code}): {cmd!r}\n{snippet}"
                    )
                else:
                    log.info("provision pre-start OK (exit=0): %s", cmd)

        log.info("sandbox provisioned: %s (no supervisor yet)", sandbox.id[:16])
        return ProviderInstance(
            provider="daytona",
            url="",  # no supervisor URL yet
            root=root,
            sandbox_ref=sandbox.id,
        )
    except BaseException:
        try:
            await loop.run_in_executor(None, lambda: daytona.delete(sandbox))
        except Exception:
            pass
        raise


async def restart_daytona_supervisor(
    daytona_sandbox_id: str, agent_type: str = "claude", root: str = "/tmp",
    spawn_env: dict[str, str] | None = None,
) -> ProviderInstance:
    """Re-attach to an existing daytona sandbox and respawn the supervisor
    inside it. Used by the resume path after the sandbox was stopped (or
    after the supervisor process was killed in place). Preserves the
    sandbox filesystem, so claude-agent-acp's persisted session state is
    available for session/load.

    Routes through ``start_supervisor_in_sandbox`` which reads from the
    per-volume deps.tar.gz cache installed by ``install_supervisor``. Any
    post-volume-refactor sandbox has that cache; sandboxes old enough to
    lack it are no longer supported (pre-2026-04).
    """
    try:
        from daytona_sdk import Daytona, DaytonaConfig
    except ImportError:
        raise RuntimeError("daytona-sdk not installed. Run: pip install daytona-sdk")
    api_key = os.environ.get("DAYTONA_API_KEY")
    if not api_key:
        raise RuntimeError("DAYTONA_API_KEY not set")

    loop = asyncio.get_running_loop()
    daytona = Daytona(DaytonaConfig(api_key=api_key))
    # Wait for a stable (non-transitional) state before attempting start.
    # Without this, an external stop that's still in progress makes
    # `sandbox.start()` reject with "Sandbox state change in progress",
    # which kills the fast recovery path that preserves /events
    # subscribers. Bumped from 15 s → 45 s after observing 2x concurrent
    # load take ~30 s for Daytona's stopping→stopped transition to land.
    sandbox, state_str = await _wait_for_stable_daytona_state(
        daytona, daytona_sandbox_id, max_wait_s=45.0,
    )
    if state_str not in ("started", "running"):
        log.info("starting stopped daytona sandbox %s (state=%s)",
                 daytona_sandbox_id, state_str)
        await loop.run_in_executor(None, sandbox.start)
        await _wait_for_daytona_sandbox_ready(daytona, daytona_sandbox_id)
        sandbox = await loop.run_in_executor(None, lambda: daytona.get(daytona_sandbox_id))

    # Volume-cached path. Uses the fixed supervisor port so the signed URL
    # is stable across restarts for an already-issued session (Daytona maps
    # preview URLs by port). HOME is set to root by supervisor.js when it
    # spawns ACP — no need to force it here.
    url = await start_supervisor_in_sandbox(
        sandbox, agent_type, _SUPERVISOR_REMOTE_PORT,
        root=_DAYTONA_AGENT_HOME, spawn_env=spawn_env,
    )
    return ProviderInstance(
        provider="daytona",
        url=url,
        root=root,
        sandbox_ref=sandbox.id,
    )


async def _daytona_sandbox_op(instance: ProviderInstance, op: str) -> None:
    """Shared logic for destroy/stop Daytona sandbox."""
    if not instance.sandbox_ref:
        return
    try:
        daytona = _get_daytona_client()
    except (ImportError, RuntimeError) as e:
        log.warning("cannot %s daytona sandbox %s: %s", op, instance.sandbox_ref, e)
        return

    loop = asyncio.get_running_loop()
    try:
        sandbox = await loop.run_in_executor(None, lambda: daytona.get(instance.sandbox_ref))
        if op == "delete":
            await loop.run_in_executor(None, lambda: daytona.delete(sandbox))
        else:
            await loop.run_in_executor(None, sandbox.stop)
        log.info("daytona sandbox %sd: %s", op, instance.sandbox_ref)
    except Exception as e:
        log.warning("failed to %s daytona sandbox %s: %s", op, instance.sandbox_ref, e)


async def destroy_daytona(instance: ProviderInstance) -> None:
    """Delete a Daytona sandbox."""
    await _daytona_sandbox_op(instance, "delete")


async def stop_daytona(instance: ProviderInstance) -> None:
    """Stop (not delete) a Daytona sandbox so it can be resumed later."""
    await _daytona_sandbox_op(instance, "stop")


async def _wait_for_stable_daytona_state(
    daytona, sandbox_ref: str, max_wait_s: float = 15.0,
) -> tuple[object, str]:
    """Poll until the sandbox is NOT in a transitional state. Returns
    (sandbox_obj, state_string). Used by the restart/start paths so a
    `sandbox.start()` call doesn't race an external stop still in
    progress (which rejects with "state change in progress").

    Terminal states: started, running, stopped, paused, error, archived.
    Transitional: starting, stopping, pulling_image, resizing, ...
    """
    loop = asyncio.get_running_loop()
    deadline = loop.time() + max_wait_s
    STABLE = {"started", "running", "stopped", "paused", "error",
              "archived", "destroyed"}
    sandbox = None
    state_str = ""
    while True:
        sandbox = await loop.run_in_executor(None, lambda: daytona.get(sandbox_ref))
        raw = sandbox.state
        state_str = (raw.value if hasattr(raw, "value") else str(raw)).lower()
        if state_str in STABLE or loop.time() > deadline:
            return sandbox, state_str
        await asyncio.sleep(0.5)


async def _wait_for_daytona_sandbox_ready(daytona, sandbox_ref: str, sandbox=None) -> None:
    """Poll until the sandbox is "started" AND an exec succeeds (IP allocated).

    sandbox.start() returns as soon as Daytona accepts the request, before the
    container network is configured.  Subsequent exec calls fail with
    "failed to resolve container IP" until the network stack is up.

    Check FIRST, then sleep — the old "sleep 2s up-front" burned ~2s on
    every restart even when the sandbox was already ready. 0.5s cadence
    also surfaces readiness ~4x faster than the old 2s polls.
    """
    loop = asyncio.get_running_loop()
    for attempt in range(30):  # 30 * 0.5s = 15s max
        if attempt > 0:
            await asyncio.sleep(0.5)
        try:
            sandbox = await loop.run_in_executor(None, lambda: daytona.get(sandbox_ref))
            raw_state = sandbox.state
            state_str = raw_state.value if hasattr(raw_state, "value") else str(raw_state)
            if state_str != "started":
                continue
            r = await loop.run_in_executor(
                None, lambda: sandbox.process.exec("echo ready", timeout=5)
            )
            result = (r.result if hasattr(r, "result") else str(r)) or ""
            if "ready" in result:
                log.info("daytona sandbox %s is ready", sandbox_ref[:16])
                return
        except Exception:
            pass
    raise RuntimeError(
        f"Daytona sandbox {sandbox_ref} did not become network-ready after start"
    )


async def start_daytona(sandbox_ref: str) -> None:
    """Start a stopped Daytona sandbox and wait for it to be network-ready.

    Retries `sandbox.start()` while Daytona reports the sandbox is in the
    middle of a state change ("state change in progress"). This race
    happens when /message arrives a few seconds after an external stop:
    the sandbox is still transitioning from started→stopped, and
    `sandbox.start()` rejects until the transition completes. Polling
    for a stable state first (or retrying on the error) is required;
    otherwise the fast-recovery path fails and we fall back to a full
    state rebuild, losing any persistent /events subscribers.
    """
    try:
        daytona = _get_daytona_client()
    except (ImportError, RuntimeError) as e:
        log.warning("cannot start daytona sandbox %s: %s", sandbox_ref, e)
        return

    loop = asyncio.get_running_loop()
    sandbox, state_str = await _wait_for_stable_daytona_state(daytona, sandbox_ref)
    if state_str in ("started", "running"):
        log.info("daytona sandbox %s already started", sandbox_ref)
        await _wait_for_daytona_sandbox_ready(daytona, sandbox_ref, sandbox=sandbox)
        return
    try:
        await loop.run_in_executor(None, sandbox.start)
        log.info("daytona sandbox started: %s", sandbox_ref)
        await _wait_for_daytona_sandbox_ready(daytona, sandbox_ref)
    except Exception as e:
        log.warning("failed to start daytona sandbox %s: %s", sandbox_ref, e)
        raise


async def create_daytona_volume(name: str, wait_ready_timeout: int = 120) -> str:
    """Create a Daytona volume and return its provider-native id.

    Polls until the volume reaches the 'ready' state before returning so that
    callers can immediately attach the volume to a new sandbox. If polling
    fails (timeout or terminal error state), the just-created volume is
    best-effort deleted so callers don't end up with an orphaned resource
    they can't identify later.

    After the volume is ready, a utility sandbox is spun up to pre-create
    the directory structure: shared/ and system/supervisor/.
    """
    from daytona_api_client import VolumesApi
    from daytona_api_client.models import VolumeState

    client = _get_daytona_client()
    # Idempotent: volume.get(name, create=True) returns the existing volume
    # if one already has this name, else creates a new one. Lets
    # `_get_or_create_default_volume` re-enter safely across server restarts
    # and on multi-worker deploys where the DB row was lost but the Daytona
    # volume still exists.
    vol = await asyncio.to_thread(client.volume.get, name, True)
    vol_id = vol.id

    volumes_api = VolumesApi(client._api_client)
    try:
        deadline = asyncio.get_running_loop().time() + wait_ready_timeout
        while True:
            dto = await asyncio.to_thread(volumes_api.get_volume, vol_id)
            state = dto.state
            state_val = state.value if hasattr(state, "value") else str(state)
            if state_val == VolumeState.READY:
                break
            if state_val in {VolumeState.ERROR, VolumeState.DELETED, VolumeState.DELETING}:
                raise RuntimeError(f"Daytona volume {vol_id} entered unexpected state: {state_val}")
            if asyncio.get_running_loop().time() > deadline:
                raise TimeoutError(f"Daytona volume {vol_id} did not become ready within {wait_ready_timeout}s (last state: {state_val})")
            await asyncio.sleep(3)
    except Exception:
        try:
            await asyncio.to_thread(volumes_api.delete_volume, vol_id)
        except Exception as cleanup_err:
            log.warning(
                "orphaned daytona volume %s: cleanup delete failed: %s",
                vol_id, cleanup_err,
            )
        raise

    # Pre-create the standard directory layout on the volume.
    await _init_volume_dirs(vol_id)

    return vol_id


async def _init_volume_dirs(volume_ref: str) -> None:
    """Spin a 1-shot sandbox to mkdir -p shared/ system/supervisor/ on the volume."""
    from daytona_sdk import (
        Daytona, DaytonaConfig, CreateSandboxFromSnapshotParams,
        CreateSandboxFromImageParams, VolumeMount,
    )

    api_key = os.environ.get("DAYTONA_API_KEY")
    if not api_key:
        raise RuntimeError("DAYTONA_API_KEY not set")
    daytona = Daytona(DaytonaConfig(api_key=api_key))
    loop = asyncio.get_running_loop()

    # _init_volume_dirs only runs `mkdir` on a brand-new volume — any image
    # with a POSIX shell works. Phase E: no implicit hive-large default;
    # operators opt into a snapshot via DAYTONA_SNAPSHOT, otherwise the
    # ``node:22-slim`` fallback is used (volume-init does not need the
    # agent-sdk runtime).
    snapshot = os.environ.get("DAYTONA_SNAPSHOT", "").strip()
    use_snapshot = snapshot.lower() not in {"", "0", "false", "image"}

    # Mount the whole volume at /v (no subpath) so we can create dirs.
    volumes = [VolumeMount(volume_id=volume_ref, mount_path="/v")]
    init_labels = _sandbox_labels()

    if use_snapshot:
        sb = await loop.run_in_executor(None, lambda: daytona.create(
            CreateSandboxFromSnapshotParams(
                snapshot=snapshot, auto_stop_interval=0,
                env_vars=_get_sandbox_env_vars(), volumes=volumes,
                labels=init_labels,
            ), timeout=120,
        ))
    else:
        sb = await loop.run_in_executor(None, lambda: daytona.create(
            CreateSandboxFromImageParams(
                image="node:22-slim", auto_stop_interval=0,
                env_vars=_get_sandbox_env_vars(), volumes=volumes,
                labels=init_labels,
            ), timeout=120,
        ))

    try:
        await loop.run_in_executor(
            None,
            lambda: _run_sandbox_exec(sb, "mkdir -p /v/shared /v/system/supervisor", timeout=30),
        )
        log.info("volume %s: initialized shared/ and system/supervisor/ dirs", volume_ref)
    finally:
        try:
            await loop.run_in_executor(None, lambda: daytona.delete(sb))
        except Exception:
            pass


async def delete_daytona_volume(provider_ref: str) -> None:
    """Delete a Daytona volume by provider-native id (UUID)."""
    from daytona_api_client import VolumesApi
    client = _get_daytona_client()
    volumes_api = VolumesApi(client._api_client)
    await asyncio.to_thread(volumes_api.delete_volume, provider_ref)


async def get_daytona_sandbox_status(sandbox_ref: str) -> str:
    """Return one of: 'running' | 'stopped' | 'missing' | 'error'.

    Callers (the SessionPool's recovery path) treat ``error`` as
    unrecoverable and provision a brand-new replacement. Transitional
    states like ``stopping`` / ``starting`` (5–30 s under load) MUST
    classify by their target state, not as ``error`` — otherwise an
    in-flight stop or boot that races a POST /message destroys the
    live sandbox the caller is trying to recover.

    Default for an unrecognized state is ``running`` rather than
    ``error`` for the same reason: a future Daytona state name we
    haven't seen yet should fall through to ``_wait_for_health`` /
    ``start_sandbox``, not to destroy + replace.
    """
    try:
        client = _get_daytona_client()
        sb = await asyncio.to_thread(client.get, sandbox_ref)
    except Exception as e:
        msg = str(e).lower()
        if "not found" in msg or "404" in msg:
            return "missing"
        return "error"
    state = (getattr(sb, "state", None) or "")
    state_str = (state.value if hasattr(state, "value") else str(state)).lower()
    if state_str in ("started", "running", "starting",
                     "pulling_image", "creating", "resizing"):
        return "running"
    if state_str in ("stopped", "paused", "stopping"):
        return "stopped"
    if state_str in ("destroyed", "destroying", "archived"):
        return "missing"
    if state_str == "error":
        return "error"
    return "running"


# ---------------------------------------------------------------------------
# Uniform API — each provider module exposes these names. Daytona's internals
# already have the right shape, so dispatch via thin wrappers that re-resolve
# the underlying name on each call. Module-level aliases (``foo = bar``) bind
# once at import and break ``unittest.mock.patch("api.providers.daytona.bar")``
# silently — the alias keeps pointing at the original. The wrappers below
# look the name up at call time so patching either the wrapper or the
# underlying name works as expected.
# ---------------------------------------------------------------------------


async def create_volume(*args, **kwargs):
    return await create_daytona_volume(*args, **kwargs)


async def delete_volume(*args, **kwargs):
    return await delete_daytona_volume(*args, **kwargs)


async def get_sandbox_status(*args, **kwargs):
    return await get_daytona_sandbox_status(*args, **kwargs)


async def start_sandbox(*args, **kwargs):
    return await start_daytona(*args, **kwargs)


async def destroy_sandbox(*args, **kwargs):
    return await destroy_daytona(*args, **kwargs)


async def stop_sandbox(*args, **kwargs):
    return await stop_daytona(*args, **kwargs)


async def ensure_supervisor_url(inst: ProviderInstance, *, agent_type: str,
                                root: str = "/tmp",
                                spawn_env: dict | None = None,
                                port: int | None = None) -> str:
    """Daytona: start a supervisor in the sandbox referenced by ``inst`` and
    return its URL.

    Daytona follows a 2-phase "create the sandbox, then start the
    supervisor" model — ``create_sandbox`` returns an instance with
    ``url=""``, and the server calls this helper later to spawn the
    supervisor process and mint a signed preview URL.

    Docker and local providers collapse these two steps: their
    ``create_sandbox`` already has ``url`` set when it returns, so the
    corresponding ``ensure_supervisor_url`` is effectively a no-op that
    just echoes ``inst.url``.  The dispatcher in
    ``providers.__init__.ensure_supervisor_url`` routes transparently to
    whichever provider the instance belongs to.

    Mi3: the ``**_kw`` catch-all was removed so a mis-spelled kwarg
    surfaces as TypeError instead of being silently swallowed — matching
    the docker + local signatures."""
    from daytona_sdk import Daytona, DaytonaConfig
    import os as _os
    api_key = _os.environ.get("DAYTONA_API_KEY")
    if not api_key:
        raise RuntimeError("DAYTONA_API_KEY not set")
    daytona_client = Daytona(DaytonaConfig(api_key=api_key))
    loop = asyncio.get_running_loop()
    try:
        sandbox = await loop.run_in_executor(
            None, lambda: daytona_client.get(inst.sandbox_ref)
        )
    except Exception as e:
        # Daytona raises a plain Exception with "not found" in the message when
        # the sandbox has been deleted out-of-band. Surface this as a typed
        # error so the server can re-provision on the same volume.
        if "not found" in str(e).lower():
            from .._shared import SandboxMissingError
            raise SandboxMissingError(
                f"Daytona sandbox {inst.sandbox_ref} not found (deleted externally)"
            ) from e
        raise
    # If the sandbox was stopped externally (e.g. daytona.stop()), start it
    # and wait for the container network to be ready before exec-ing.
    raw_state = sandbox.state
    state_str = raw_state.value if hasattr(raw_state, "value") else str(raw_state)
    if state_str != "started":
        log.info("ensure_supervisor_url: sandbox %s is %s; starting", inst.sandbox_ref[:16], state_str)
        await loop.run_in_executor(None, sandbox.start)
        await _wait_for_daytona_sandbox_ready(daytona_client, inst.sandbox_ref)
        sandbox = await loop.run_in_executor(None, lambda: daytona_client.get(inst.sandbox_ref))
    # The agent's HOME is /home/daytona — a local ext4 dir the supervisor
    # creates and populates from the volume snapshot on boot. supervisor.js
    # sets HOME=root when spawning the ACP child so Claude Code's session
    # JSONLs land in the restored workspace. No env-level HOME override
    # needed here anymore.
    return await start_supervisor_in_sandbox(
        sandbox, agent_type, port, root=_DAYTONA_AGENT_HOME, spawn_env=spawn_env,
    )


# ``install_supervisor`` was deleted in Phase E of
# the runtime-image-unification refactor. The daytona sandbox now boots from an
# image whose /opt/agent-sdk/runtime/ contains supervisor.js + every ACP
# bin; ``provision_daytona_sandbox`` reads ``DAYTONA_IMAGE`` /
# ``.runtime-image-tag`` for that image.


async def create_sandbox(
    *,
    volume_ref: str,
    subpath: str,
    agent_type: str = "claude",
    spawn_env: dict[str, str] | None = None,
    port: int | None = None,
    root: str | None = None,
    dockerfile: str | None = None,
    pre_start_commands: list[str] | None = None,
    sandbox_ref: str | None = None,  # accepted for parity; unused here
    shared_mounts: list[str] | None = None,
) -> ProviderInstance:
    """Uniform ``create_sandbox`` for the Daytona provider.

    Delegates to ``provision_daytona_sandbox`` which creates the sandbox with
    the volume mounts (per-agent subpath + supervisor cache + any opt-in
    shared mounts) but does NOT start a supervisor; the caller must run
    ``ensure_supervisor_url`` before talking to the supervisor.

    ``spawn_env`` / ``port`` / `sandbox_ref` are accepted for parity with
    docker/local but are unused here — the supervisor is started later with
    its own env + port, and Daytona doesn't take a name on create.
    """
    # Per-session sandboxes always root at /home/daytona — the supervisor
    # will mkdir it, restore from the volume snapshot, and use it as HOME.
    # The volume itself is mounted at /vol (see _build_volume_mounts) and
    # the agent never sees it directly.
    effective_root = _DAYTONA_AGENT_HOME if subpath else (root or _DAYTONA_AGENT_HOME)
    return await provision_daytona_sandbox(
        agent_type=agent_type,
        dockerfile=dockerfile,
        pre_start_commands=pre_start_commands,
        root=effective_root,
        volume_id=volume_ref,
        subpath=subpath,
        shared_mounts=shared_mounts,
    )


# ---------------------------------------------------------------------------
# Volume file ops — dispatched from server.py's /volumes/{id}/files/*
# ---------------------------------------------------------------------------
# Every GET /volumes/{id}/files/tree / files/read / files/edit needs a
# sandbox with the volume mounted at /v. Provisioning fresh on each call
# meant ~10s+ per request on daytona and a new sandbox every UI poll. We
# keep one utility sandbox per volume with a short TTL; reuse bypasses
# the whole provision/destroy round-trip on back-to-back calls.
# ---------------------------------------------------------------------------

_UTILITY_TTL_S = 300.0  # 5 minutes idle before the reaper tears it down
_UTILITY_REAPER_TICK_S = 30.0
_utility_cache: dict[str, tuple["ProviderInstance", float]] = {}  # ref -> (inst, last_used)
_utility_cache_lock = asyncio.Lock()
_utility_reaper_started = False
_conditional_create_support_cache: dict[str, bool] = {}
_conditional_create_probe_lock = asyncio.Lock()


async def _get_or_create_utility(ref: str) -> "ProviderInstance":
    """Return a ready utility sandbox for ``ref`` — cached per volume.

    First call per volume: provisions + caches. Subsequent calls within
    ``_UTILITY_TTL_S`` of the last use: returns the cached instance. The
    reaper tears down idle entries; a torn-down entry is transparently
    re-provisioned on the next call.
    """
    import time as _time
    async with _utility_cache_lock:
        cached = _utility_cache.get(ref)
        if cached is not None:
            inst, _last = cached
            _utility_cache[ref] = (inst, _time.monotonic())
            _ensure_utility_reaper()
            return inst
        # Provision outside the lock? No — provisioning is 5-15s and we
        # want the lock held so a burst of concurrent file-ops on the
        # same volume doesn't create N sandboxes. Readers wait; winner
        # populates cache; other readers then hit the fast path above.
        log.info("daytona utility sandbox: provisioning for volume %s", ref[:16])
        inst = await provision_daytona_sandbox(
            agent_type="claude", volume_id=ref, subpath=None,
        )
        _utility_cache[ref] = (inst, _time.monotonic())
        _ensure_utility_reaper()
        return inst


async def _drop_utility(ref: str) -> None:
    """Remove a cached utility sandbox and destroy it. No-op if absent."""
    async with _utility_cache_lock:
        entry = _utility_cache.pop(ref, None)
    if entry is None:
        return
    inst, _ = entry
    try:
        await destroy_daytona(inst)
    except Exception as e:  # pragma: no cover
        log.warning("utility sandbox destroy failed for %s: %s", ref[:16], e)


def _ensure_utility_reaper() -> None:
    """Lazily start the background reaper on first cache entry."""
    global _utility_reaper_started
    if _utility_reaper_started:
        return
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return  # not in an event loop (e.g. unit tests) — caller manages cleanup
    loop.create_task(_utility_reaper_loop())
    _utility_reaper_started = True


async def _utility_reaper_loop() -> None:
    """Destroy utility sandboxes that have been idle for > _UTILITY_TTL_S."""
    import time as _time
    while True:
        await asyncio.sleep(_UTILITY_REAPER_TICK_S)
        now = _time.monotonic()
        stale: list[tuple[str, "ProviderInstance"]] = []
        async with _utility_cache_lock:
            for ref, (inst, last) in list(_utility_cache.items()):
                if now - last > _UTILITY_TTL_S:
                    stale.append((ref, inst))
                    _utility_cache.pop(ref, None)
        for ref, inst in stale:
            log.info("daytona utility sandbox: reaping idle volume %s", ref[:16])
            try:
                await destroy_daytona(inst)
            except Exception as e:  # pragma: no cover
                log.warning("utility reaper: destroy failed for %s: %s", ref[:16], e)


async def _run_in_utility_sandbox(ref: str, cmd: str, timeout: int = 30):
    """Run ``cmd`` in the utility sandbox for ``ref``. Cached + TTL-reaped.

    Transient errors (sandbox died on the provider side between the cache
    entry's freshness check and the exec call) trigger one retry after
    dropping the cache entry, so callers don't see a single stale-cache
    hit bubble up as a 500.
    """
    from ... import providers as _prov  # local import for cycle
    inst = await _get_or_create_utility(ref)
    try:
        return await _prov.exec_in_instance(inst, cmd, timeout=timeout)
    except Exception as e:
        log.warning("utility exec failed on cached sandbox for %s (%s); retrying with fresh sandbox", ref[:16], e)
        await _drop_utility(ref)
        inst = await _get_or_create_utility(ref)
        return await _prov.exec_in_instance(inst, cmd, timeout=timeout)


async def volume_tree(ref: str, path: str) -> str:
    """Tree listing of ``<volume>/<path>`` in the unified format (max depth 3)."""
    rel = _safe_path(None, path or "")
    target = "/v/" + rel if rel else "/v"
    res = await _run_in_utility_sandbox(
        ref,
        f"find {shlex.quote(target)} -mindepth 1 -maxdepth 3 -printf '%y %P\\n' 2>/dev/null"
    )
    normalized = normalize_find_output(res.stdout)
    if not rel or not normalized:
        return normalized
    lines = [f"{rel.rstrip('/')}/{ln}" for ln in normalized.splitlines()]
    return "\n".join(sorted(lines))


async def volume_read(ref: str, path: str) -> bytes:
    """Read ``<volume>/<path>`` bytes via a short-lived utility sandbox."""
    rel = _safe_path(None, path or "")
    if not rel:
        raise ValueError("volume_read: path required")
    target = "/v/" + rel
    # base64 so binary survives the exec response.
    res = await _run_in_utility_sandbox(
        ref,
        f"if [ ! -f {shlex.quote(target)} ]; then echo __MISSING__; exit 2; fi; "
        f"base64 -w0 {shlex.quote(target)} 2>/dev/null || base64 {shlex.quote(target)}",
    )
    if res.exit_code != 0:
        if "__MISSING__" in (res.stdout or ""):
            raise FileNotFoundError(f"{path} not found on volume {ref}")
        raise RuntimeError(f"volume_read failed: {res.stderr[:400]}")
    import base64 as _b64
    try:
        return _b64.b64decode((res.stdout or "").strip())
    except Exception as exc:
        raise RuntimeError(f"volume_read: malformed base64: {exc}") from exc


async def volume_download(ref: str, path: str) -> bytes:
    """Read raw bytes from ``<volume>/<path>`` via Daytona's filesystem API.

    This bypasses ``volume_read``'s exec/stdout path by using Daytona's
    dedicated file-download endpoint through the SDK.
    """
    rel = _safe_path(None, path or "")
    if not rel:
        raise ValueError("volume_download: path required")
    target = "/v/" + rel
    inst = await _get_or_create_utility(ref)
    if not inst.sandbox_ref:
        raise RuntimeError("volume_download: utility sandbox_ref missing")

    loop = asyncio.get_running_loop()
    daytona_client = _get_daytona_client()
    try:
        sandbox = await loop.run_in_executor(
            None, lambda: daytona_client.get(inst.sandbox_ref)
        )
    except Exception as e:
        raise RuntimeError(f"volume_download: get sandbox failed: {e}") from e

    try:
        return await loop.run_in_executor(
            None, lambda: sandbox.fs.download_file(target)
        )
    except Exception as e:
        msg = str(e)
        if "not found" in msg.lower() or "404" in msg:
            raise FileNotFoundError(f"{path} not found on volume {ref}") from e
        raise RuntimeError(f"volume_download failed: {msg}") from e


async def _conditional_upload_if_absent(ref: str, abs_path: str, content: bytes) -> str:
    """Attempt atomic create-if-absent upload through Daytona toolbox.

    Returns:
      - "created" when destination was created
      - "exists" when backend rejected due to precondition
    Raises RuntimeError on transport/protocol failures.
    """
    inst = await _get_or_create_utility(ref)
    if not inst.sandbox_ref:
        raise RuntimeError("conditional upload: utility sandbox_ref missing")
    loop = asyncio.get_running_loop()
    daytona_client = _get_daytona_client()
    try:
        sandbox = await loop.run_in_executor(
            None, lambda: daytona_client.get(inst.sandbox_ref)
        )
    except Exception as e:
        raise RuntimeError(f"conditional upload: get sandbox failed: {e}") from e
    try:
        await loop.run_in_executor(
            None,
            lambda: sandbox.fs._api_client.upload_file(  # pyright: ignore[reportPrivateUsage]
                path=abs_path,
                file=content,
                _headers={"If-None-Match": "*"},
            ),
        )
        return "created"
    except Exception as e:
        msg = str(e).lower()
        if "412" in msg or "precondition" in msg:
            return "exists"
        raise RuntimeError(f"conditional upload failed: {e}") from e


async def _daytona_supports_conditional_create(ref: str) -> bool:
    """Detect once per volume whether toolbox upload honors If-None-Match: *."""
    mode = (os.environ.get("DAYTONA_CONDITIONAL_CREATE_MODE", "auto") or "auto").strip().lower()
    if mode in {"on", "true", "1", "force", "force_on"}:
        return True
    if mode in {"off", "false", "0", "disable", "force_off"}:
        return False
    cached = _conditional_create_support_cache.get(ref)
    if cached is not None:
        return cached
    async with _conditional_create_probe_lock:
        cached = _conditional_create_support_cache.get(ref)
        if cached is not None:
            return cached
        probe_rel = f"system/.conditional-create-probe-{uuid.uuid4().hex}.txt"
        probe_abs = "/v/" + probe_rel
        try:
            await volume_write(ref, probe_rel, b"probe-a")
            result = await _conditional_upload_if_absent(ref, probe_abs, b"probe-b")
            if result != "exists":
                _conditional_create_support_cache[ref] = False
                return False
            current = await volume_download(ref, probe_rel)
            supported = current == b"probe-a"
            _conditional_create_support_cache[ref] = supported
            return supported
        except Exception:
            _conditional_create_support_cache[ref] = False
            return False
        finally:
            try:
                await volume_delete(ref, probe_rel)
            except Exception:
                pass


async def volume_exists(ref: str, path: str) -> bool:
    """Return whether ``<volume>/<path>`` exists."""
    rel = _safe_path(None, path or "")
    target = "/v/" + rel if rel else "/v"
    res = await _run_in_utility_sandbox(ref, f"test -e {shlex.quote(target)}")
    if res.exit_code == 0:
        return True
    if res.exit_code == 1:
        return False
    raise RuntimeError(f"volume_exists failed: {res.stderr[:400]}")


async def volume_write(ref: str, path: str, content: bytes) -> None:
    """Write ``content`` to ``<volume>/<path>`` via a short-lived utility sandbox."""
    rel = _safe_path(None, path or "")
    if not rel:
        raise ValueError("volume_write: path required")
    target = "/v/" + rel
    parent = "/v/" + "/".join(rel.split("/")[:-1])
    import base64 as _b64
    b64 = _b64.b64encode(content).decode()
    cmd = (
        f"mkdir -p {shlex.quote(parent)} && "
        f"printf %s {shlex.quote(b64)} | base64 -d > {shlex.quote(target)}"
    )
    res = await _run_in_utility_sandbox(ref, cmd)
    if res.exit_code != 0:
        raise RuntimeError(f"volume_write failed: {res.stderr[:400]}")


async def volume_upload(ref: str, path: str, content: bytes) -> None:
    """Upload bytes to ``<volume>/<path>``."""
    await volume_write(ref, path, content)


async def volume_mkdir(ref: str, path: str) -> None:
    """Create a directory at ``<volume>/<path>``."""
    rel = _safe_path(None, path or "")
    if not rel:
        raise ValueError("volume_mkdir: path required")
    target = "/v/" + rel
    res = await _run_in_utility_sandbox(ref, f"mkdir -p {shlex.quote(target)}")
    if res.exit_code != 0:
        raise RuntimeError(f"volume_mkdir failed: {res.stderr[:400]}")


async def volume_delete(ref: str, path: str) -> None:
    """Delete a file or directory at ``<volume>/<path>``."""
    rel = _safe_path(None, path or "")
    if not rel:
        raise ValueError("volume_delete: path required")
    target = "/v/" + rel
    cmd = (
        f"if [ ! -e {shlex.quote(target)} ]; then echo __MISSING__; exit 2; fi; "
        f"rm -rf -- {shlex.quote(target)}"
    )
    res = await _run_in_utility_sandbox(ref, cmd)
    if res.exit_code != 0:
        if "__MISSING__" in (res.stdout or ""):
            raise FileNotFoundError(f"{path} not found on volume {ref}")
        raise RuntimeError(f"volume_delete failed: {res.stderr[:400]}")


async def volume_rename(ref: str, path: str, new_path: str, *, overwrite: bool = True) -> None:
    """Rename or move ``<volume>/<path>`` to ``<volume>/<new_path>``."""
    src_rel = _safe_path(None, path or "")
    dst_rel = _safe_path(None, new_path or "")
    if not src_rel or not dst_rel:
        raise ValueError("volume_rename: path and new_path required")
    src = "/v/" + src_rel
    dst = "/v/" + dst_rel
    dst_parent = "/v/" + "/".join(dst_rel.split("/")[:-1])
    # mountpoint-backed volumes can lag after rename/link+unlink. Do not report
    # success until dst is visible and src is gone in the utility sandbox view.
    settle_check = (
        f"for _i in 1 2 3 4 5 6 7 8 9 10; do "
        f"if [ -e {shlex.quote(dst)} ] && [ ! -e {shlex.quote(src)} ]; then exit 0; fi; "
        f"sleep 0.1; "
        f"done; "
        f"echo __RENAME_NOT_VISIBLE__; exit 98"
    )
    if overwrite:
        cmd = (
            f"if [ ! -e {shlex.quote(src)} ]; then echo __MISSING__; exit 2; fi; "
            f"mkdir -p {shlex.quote(dst_parent)} && "
            f"mv -- {shlex.quote(src)} {shlex.quote(dst)} && "
            f"{settle_check}"
        )
    else:
        # Daytona volumes are object-store backed; hardlinks are not reliable.
        # Require a real create-if-absent primitive instead of race-prone emulation.
        if not await _daytona_supports_conditional_create(ref):
            raise NotImplementedError(
                "atomic no-overwrite rename is not supported on this Daytona volume backend"
            )
        res = await _run_in_utility_sandbox(
            ref,
            (
                f"if [ ! -e {shlex.quote(src)} ]; then echo __MISSING__; exit 2; fi; "
                f"mkdir -p {shlex.quote(dst_parent)} || exit $?; "
                f"if [ -d {shlex.quote(src)} ]; then echo __UNSUPPORTED_DIR__; exit 95; fi"
            ),
        )
        if res.exit_code != 0:
            if "__MISSING__" in (res.stdout or ""):
                raise FileNotFoundError(f"{path} not found on volume {ref}")
            if "__UNSUPPORTED_DIR__" in (res.stdout or ""):
                raise NotImplementedError("atomic no-overwrite directory rename is not supported")
            raise RuntimeError(f"volume_rename failed: {res.stderr[:400]}")
        src_bytes = await volume_download(ref, src_rel)
        outcome = await _conditional_upload_if_absent(ref, dst, src_bytes)
        if outcome == "exists":
            raise VolumeFileExistsError(new_path)
        if outcome != "created":
            raise RuntimeError("volume_rename failed: conditional destination claim returned unknown result")
        await volume_delete(ref, src_rel)
        verify = await _run_in_utility_sandbox(ref, settle_check)
        if verify.exit_code != 0:
            raise RuntimeError("volume_rename postcondition failed: destination not visible")
        return
    res = await _run_in_utility_sandbox(ref, cmd)
    if res.exit_code != 0:
        if "__MISSING__" in (res.stdout or ""):
            raise FileNotFoundError(f"{path} not found on volume {ref}")
        if "__EXISTS__" in (res.stdout or ""):
            raise VolumeFileExistsError(new_path)
        if "__UNSUPPORTED_DIR__" in (res.stdout or ""):
            raise NotImplementedError("atomic no-overwrite directory rename is not supported")
        if "__RENAME_NOT_VISIBLE__" in (res.stdout or ""):
            raise RuntimeError("volume_rename postcondition failed: destination not visible")
        raise RuntimeError(f"volume_rename failed: {res.stderr[:400]}")


# ---------------------------------------------------------------------------
# Reconciliation — delete orphaned daytona sandboxes on startup
# ---------------------------------------------------------------------------

async def reconcile_on_startup() -> None:
    """Delete daytona sandboxes whose id is not in any live session row.

    Lists by ``agent_sdk_origin=<AGENT_SDK_ORIGIN>`` so dev/test/prod
    deploys sharing one daytona account never cross-reap. Mirrors what
    ``scripts/cleanup_orphans.py`` does, automated at boot.

    Failures are logged and swallowed.
    """
    try:
        from ... import db as dbmod
    except Exception as e:
        log.warning("daytona reconcile: cannot import api.db: %s", e)
        return

    try:
        daytona = _get_daytona_client()
    except Exception as e:
        log.warning("daytona reconcile: client unavailable: %s", e)
        return

    labels = _sandbox_labels()
    try:
        page = await asyncio.to_thread(lambda: daytona.list(labels=labels))
        items = list(getattr(page, "items", None) or page)
    except Exception as e:
        log.warning("daytona reconcile: list failed: %s", e)
        return

    try:
        live_refs = await dbmod.live_sandbox_refs()
    except Exception as e:
        log.warning("daytona reconcile: live-session query failed: %s", e)
        return

    for sb in items:
        sid = getattr(sb, "id", None)
        if not sid or sid in live_refs:
            continue
        log.info("daytona reconcile: deleting orphan %s", sid[:16])
        try:
            await asyncio.to_thread(lambda s=sb: daytona.delete(s))
        except Exception as e:
            log.warning("daytona reconcile: delete %s: %s", sid[:16], e)
