"""Daytona provider — create/destroy/exec in Daytona sandboxes."""

import asyncio
import logging
import os
import shlex
import uuid
from pathlib import Path

from .. import load_dotenv

log = logging.getLogger(__name__)

load_dotenv()

# Re-import shared helpers from __init__ to avoid circular imports.
# These are defined here inline or imported lazily.
from ._shared import (
    _acp_bin_name,
    _acp_launch_args,
    _ACP_NPM_SPECS,
    _build_env_prefix,
    _get_sandbox_env_vars,
    _safe_path,
    _wait_for_health,
    build_supervisor_argv,
    ProviderInstance,
    _build_volume_mounts,
    normalize_find_output,
)

_SUPERVISOR_DIR = Path(__file__).resolve().parent.parent.parent / "supervisor"
_SUPERVISOR_REMOTE_DIR = "/tmp/agent-sdk-sup"  # legacy path (pre-volume era)
_SUPERVISOR_VOLUME_DIR = "/opt/supervisor"      # volume-mounted path (Phase 2+)
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


def _get_daytona_client():
    """Get a Daytona SDK client. Raises ImportError or RuntimeError on failure."""
    from daytona_sdk import Daytona, DaytonaConfig
    api_key = os.environ.get("DAYTONA_API_KEY")
    if not api_key:
        raise RuntimeError("DAYTONA_API_KEY not set")
    return Daytona(DaytonaConfig(api_key=api_key))


async def _bootstrap_supervisor_in_daytona_sandbox(
    sandbox, agent_type: str, *,
    root: str = "/tmp",
    spawn_env: dict[str, str] | None = None,
) -> ProviderInstance:
    """Start the supervisor inside an existing daytona sandbox.

    Recovery path only: deps are already on disk from the previous run
    (either the original /tmp install or a volume-cached deps.tar.gz), so
    we just exec the supervisor binary against the existing node_modules.
    """
    bin_name = _acp_bin_name(agent_type)
    loop = asyncio.get_running_loop()

    def _exec(cmd: str, timeout: int = 120) -> str:
        r = sandbox.process.exec(cmd, timeout=timeout)
        return (r.result if hasattr(r, "result") else str(r)) or ""

    acp_bin = f"{_SUPERVISOR_REMOTE_DIR}/node_modules/.bin/{bin_name}"
    env_prefix = _build_env_prefix(spawn_env)
    supervisor_argv = build_supervisor_argv(
        supervisor_js="supervisor.js", acp_bin=acp_bin,
        acp_launch_args=_acp_launch_args(agent_type),
        port=_SUPERVISOR_REMOTE_PORT, root=root,
        snapshot_path=_SNAPSHOT_PATH, quote_paths=False,
    )
    inner = (
        f"cd {_SUPERVISOR_REMOTE_DIR} && "
        f"setsid env {env_prefix} {supervisor_argv} "
        f"> {_SUPERVISOR_REMOTE_DIR}/sup.log 2>&1 </dev/null & echo started"
    )
    start_cmd = f"sh -c {shlex.quote(inner)}"
    await loop.run_in_executor(None, lambda: _exec(start_cmd, timeout=10))
    # _wait_for_health already polls with backoff; no redundant pre-sleep.

    signed = await loop.run_in_executor(
        None, lambda: sandbox.create_signed_preview_url(_SUPERVISOR_REMOTE_PORT, 24 * 3600)
    )
    url = signed.url.rstrip("/")

    if not await _wait_for_health(url, max_retries=20, interval=1):
        log_out = await loop.run_in_executor(None, lambda: _exec(f"tail -40 {_SUPERVISOR_REMOTE_DIR}/sup.log 2>&1"))
        raise RuntimeError(
            f"supervisor in Daytona sandbox {sandbox.id} failed health check; log:\n{log_out[:800]}"
        )

    log.info("daytona supervisor ready: %s (sandbox %s)", url[:60], sandbox.id[:16])
    return ProviderInstance(
        provider="daytona",
        url=url,
        root=root,
        sandbox_id=sandbox.id,
    )


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
    """
    bin_name = _acp_bin_name(agent_type)
    loop = asyncio.get_running_loop()

    def _exec(cmd: str, timeout: int = 120) -> str:
        r = sandbox.process.exec(cmd, timeout=timeout)
        return (r.result if hasattr(r, "result") else str(r)) or ""

    # Check for Phase-2 volume cache: a single deps.tar.gz written by
    # install_supervisor.  If present, extract to a per-port local dir so
    # we never write node_modules to the slow network volume.
    vol_tarball = f"{_SUPERVISOR_VOLUME_DIR}/deps.tar.gz"
    vol_supervisor = f"{_SUPERVISOR_VOLUME_DIR}/supervisor.js"
    local_work = f"/tmp/sup-work-{port}"

    # Daytona S3-backed volumes have FUSE write-to-read visibility lag
    # (seconds), so a session sandbox created ~immediately after
    # install_supervisor may not yet see /opt/supervisor/deps.tar.gz.
    # Retry a handful of times before falling back to the legacy /tmp
    # install path.
    check_result = "no"
    for _attempt in range(10):
        check_result = await loop.run_in_executor(
            None, lambda: _exec(f"test -f {vol_tarball} && echo yes || echo no")
        )
        if check_result.strip() == "yes":
            break
        await asyncio.sleep(1)

    if check_result.strip() != "yes":
        diag = await loop.run_in_executor(
            None, lambda: _exec(
                f"ls -la {_SUPERVISOR_VOLUME_DIR} 2>&1 | head -10; "
                f"echo '---'; mount | grep -i supervisor"
            )
        )
        log.warning(
            "start_supervisor_in_sandbox: volume cache not visible after retries "
            "(sandbox %s); /opt/supervisor contents:\n%s",
            sandbox.id[:16], diag,
        )

    if check_result.strip() == "yes":
        # Volume-cached mode: extract deps tarball to local ephemeral dir.
        # Reading one archive from the volume is fast; we never write
        # node_modules there.
        extract_out = await loop.run_in_executor(None, lambda: _exec(
            f"set -e && "
            f"mkdir -p {local_work} && "
            # Copy the tarball off the slow volume first; S3-backed FUSE
            # reads block the tar streaming decode if a chunk hasn't been
            # fetched yet and show up as silent tar data corruption.
            f"cp {vol_tarball} /tmp/deps-{port}.tar.gz && "
            f"ls -l /tmp/deps-{port}.tar.gz && "
            f"echo '--- tarball content sample ---' && "
            f"tar -tzf /tmp/deps-{port}.tar.gz | grep -c 'node_modules/.bin' && "
            f"tar -C {local_work} -xzf /tmp/deps-{port}.tar.gz && "
            f"cp {vol_supervisor} {local_work}/supervisor.js && "
            f"rm -f /tmp/deps-{port}.tar.gz && "
            f"echo '--- extracted .bin/ ---' && "
            f"ls -la {local_work}/node_modules/.bin/{bin_name} 2>&1 && "
            f"target=$(readlink -f {local_work}/node_modules/.bin/{bin_name}) && "
            f"echo \"target=$target\" && "
            f"ls -la \"$target\" && "
            f"head -1 \"$target\" && "
            # npm install should set +x on bin entries — but tar sometimes
            # strips it when packing + extracting across hosts. Re-apply.
            f"chmod +x \"$target\" && "
            f"test -x \"$target\" && echo 'bin executable' || echo 'bin NOT executable'",
            120,
        ))
        log.info("start_supervisor_in_sandbox: volume cache extract for sandbox %s:\n%s",
                 sandbox.id[:16], extract_out)
        sup_dir = local_work
        log.info("start_supervisor_in_sandbox: using volume cache → %s (port %d, sandbox %s)",
                 local_work, port, sandbox.id[:16])
    else:
        # Legacy path: deps are installed directly in the sandbox.
        sup_dir = _SUPERVISOR_REMOTE_DIR
        log.info("start_supervisor_in_sandbox: using legacy path %s (port %d, sandbox %s)",
                 _SUPERVISOR_REMOTE_DIR, port, sandbox.id[:16])

    # Resolve the symlink target explicitly. node.spawn() on a symlinked
    # script occasionally surfaces ENOENT on the symlink path even when
    # the target resolves fine — the node runtime's execve loop doesn't
    # always follow symlinks for script-with-shebang reliably. Passing
    # the concrete index.js target sidesteps the class of bugs.
    acp_bin_resolved = await loop.run_in_executor(None, lambda: _exec(
        f"readlink -f {sup_dir}/node_modules/.bin/{bin_name}"
    ))
    acp_bin = (acp_bin_resolved.strip()
               or f"{sup_dir}/node_modules/.bin/{bin_name}")
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
    await loop.run_in_executor(None, lambda: _exec(start_cmd, timeout=10))
    # _wait_for_health already polls with backoff; no redundant pre-sleep.

    signed = await loop.run_in_executor(
        None, lambda: sandbox.create_signed_preview_url(port, 24 * 3600)
    )
    url = signed.url.rstrip("/")

    if not await _wait_for_health(url, max_retries=20, interval=1):
        log_out = await loop.run_in_executor(None, lambda: _exec(f"tail -40 {log_file} 2>&1"))
        raise RuntimeError(
            f"supervisor on port {port} in sandbox {sandbox.id} failed health check; log:\n{log_out[:800]}"
        )

    log.info("supervisor on port %d ready: %s (sandbox %s, dir %s)", port, url[:60], sandbox.id[:16], sup_dir)
    return url


async def kill_supervisor_in_sandbox(sandbox, port: int) -> None:
    """Kill a supervisor process by port inside a Daytona sandbox."""
    loop = asyncio.get_running_loop()
    try:
        def _exec(cmd: str) -> str:
            r = sandbox.process.exec(cmd, timeout=10)
            return (r.result if hasattr(r, "result") else str(r)) or ""
        await loop.run_in_executor(None, lambda: _exec(f"fuser -k {port}/tcp 2>/dev/null || true"))
    except Exception as e:
        log.warning("kill_supervisor_in_sandbox port=%d failed: %s", port, e)


async def provision_daytona_sandbox(
    agent_type: str = "claude",
    dockerfile: str | None = None,
    pre_start_commands: list[str] | None = None,
    root: str = "/tmp",
    volume_id: str | None = None,
    subpath: str | None = None,
) -> ProviderInstance:
    """Create a Daytona sandbox with 3 volume mounts, but do NOT install deps
    or start a supervisor (those are handled by ensure_volume_supervisor and
    ensure_supervisor_url respectively).

    Returns a ProviderInstance with sandbox_id but no usable supervisor URL.
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

    snapshot = os.environ.get("DAYTONA_SNAPSHOT", "hive-large").strip()
    use_snapshot = dockerfile is None and snapshot.lower() not in {"", "0", "false", "image"}

    if dockerfile is not None:
        if not Path(dockerfile).exists():
            raise FileNotFoundError(f"Dockerfile not found: {dockerfile}")
        from daytona_sdk import Image
        image = Image.from_dockerfile(dockerfile)
    elif not use_snapshot:
        image = "node:22-slim"

    create_timeout = 300 if dockerfile else 60

    volumes = _build_volume_mounts(volume_id, subpath)

    if use_snapshot:
        sandbox = await loop.run_in_executor(None, lambda: daytona.create(
            CreateSandboxFromSnapshotParams(
                snapshot=snapshot, auto_stop_interval=0, env_vars=env_vars,
                volumes=volumes,
            ), timeout=create_timeout,
        ))
    else:
        sandbox = await loop.run_in_executor(None, lambda: daytona.create(
            CreateSandboxFromImageParams(
                image=image, auto_stop_interval=0, env_vars=env_vars,
                volumes=volumes,
            ), timeout=create_timeout,
        ))

    try:
        def _exec(cmd: str, timeout: int = 120) -> str:
            r = sandbox.process.exec(cmd, timeout=timeout)
            return (r.result if hasattr(r, "result") else str(r)) or ""

        # Run pre-start commands (skills, CLI install, etc.)
        if pre_start_commands:
            for cmd in pre_start_commands:
                log.info("provision pre-start: %s", cmd)
                await loop.run_in_executor(None, lambda c=cmd: _exec(c, timeout=120))

        log.info("sandbox provisioned: %s (no supervisor yet)", sandbox.id[:16])
        return ProviderInstance(
            provider="daytona",
            url="",  # no supervisor URL yet
            root=root,
            sandbox_id=sandbox.id,
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

    Prefers the volume-cache install path
    (``start_supervisor_in_sandbox``) so the restart and fresh-provision
    paths agree on where the ACP binary lives.  Falls back to the legacy
    ``_bootstrap_supervisor_in_daytona_sandbox`` branch only when the
    volume cache is absent — typically because the sandbox was created
    before the volume-refactor landed and has no
    ``/opt/supervisor`` mount.
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
    # subscribers. Max ~15s wait — matches Daytona's typical stop latency.
    sandbox, state_str = await _wait_for_stable_daytona_state(daytona, daytona_sandbox_id)
    if state_str not in ("started", "running"):
        log.info("starting stopped daytona sandbox %s (state=%s)",
                 daytona_sandbox_id, state_str)
        await loop.run_in_executor(None, sandbox.start)
        await _wait_for_daytona_sandbox_ready(daytona, daytona_sandbox_id)
        sandbox = await loop.run_in_executor(None, lambda: daytona.get(daytona_sandbox_id))

    # Probe for the volume-cached deps tarball. If present, route through
    # start_supervisor_in_sandbox which extracts from the cache; otherwise
    # fall back to the legacy /tmp install path.
    def _exec(cmd: str, timeout: int = 10) -> str:
        r = sandbox.process.exec(cmd, timeout=timeout)
        return (r.result if hasattr(r, "result") else str(r)) or ""

    cache_check = await loop.run_in_executor(
        None,
        lambda: _exec(
            f"test -f {_SUPERVISOR_VOLUME_DIR}/deps.tar.gz "
            f"&& test -f {_SUPERVISOR_VOLUME_DIR}/supervisor.js "
            "&& echo yes || echo no"
        ),
    )
    if cache_check.strip() == "yes":
        # Volume-cached path. Uses the fixed supervisor port so the
        # signed URL is stable across restarts for an already-issued
        # session (Daytona maps preview URLs by port). HOME is set to
        # root by supervisor.js when it spawns ACP — no need to force it
        # here.
        url = await start_supervisor_in_sandbox(
            sandbox, agent_type, _SUPERVISOR_REMOTE_PORT,
            root=_DAYTONA_AGENT_HOME, spawn_env=spawn_env,
        )
        return ProviderInstance(
            provider="daytona",
            url=url,
            root=root,
            sandbox_id=sandbox.id,
        )

    # Legacy fallback — /tmp install produced by an older fresh-provision.
    log.info(
        "restart_daytona_supervisor: volume cache missing for sandbox %s; "
        "using legacy /tmp bootstrap path",
        daytona_sandbox_id[:16],
    )
    return await _bootstrap_supervisor_in_daytona_sandbox(
        sandbox, agent_type, root=root, spawn_env=spawn_env,
    )


async def _daytona_sandbox_op(instance: ProviderInstance, op: str) -> None:
    """Shared logic for destroy/stop Daytona sandbox."""
    if not instance.sandbox_id:
        return
    try:
        daytona = _get_daytona_client()
    except (ImportError, RuntimeError) as e:
        log.warning("cannot %s daytona sandbox %s: %s", op, instance.sandbox_id, e)
        return

    loop = asyncio.get_running_loop()
    try:
        sandbox = await loop.run_in_executor(None, lambda: daytona.get(instance.sandbox_id))
        if op == "delete":
            await loop.run_in_executor(None, lambda: daytona.delete(sandbox))
        else:
            await loop.run_in_executor(None, sandbox.stop)
        log.info("daytona sandbox %sd: %s", op, instance.sandbox_id)
    except Exception as e:
        log.warning("failed to %s daytona sandbox %s: %s", op, instance.sandbox_id, e)


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
    """
    loop = asyncio.get_running_loop()
    for _attempt in range(30):
        await asyncio.sleep(2)
        sandbox = await loop.run_in_executor(None, lambda: daytona.get(sandbox_ref))
        raw_state = sandbox.state
        state_str = raw_state.value if hasattr(raw_state, "value") else str(raw_state)
        if state_str != "started":
            continue
        try:
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

    snapshot = os.environ.get("DAYTONA_SNAPSHOT", "hive-large").strip()
    use_snapshot = snapshot.lower() not in {"", "0", "false", "image"}

    # Mount the whole volume at /v (no subpath) so we can create dirs.
    volumes = [VolumeMount(volume_id=volume_ref, mount_path="/v")]

    if use_snapshot:
        sb = await loop.run_in_executor(None, lambda: daytona.create(
            CreateSandboxFromSnapshotParams(
                snapshot=snapshot, auto_stop_interval=0,
                env_vars=_get_sandbox_env_vars(), volumes=volumes,
            ), timeout=120,
        ))
    else:
        sb = await loop.run_in_executor(None, lambda: daytona.create(
            CreateSandboxFromImageParams(
                image="node:22-slim", auto_stop_interval=0,
                env_vars=_get_sandbox_env_vars(), volumes=volumes,
            ), timeout=120,
        ))

    try:
        def _exec(cmd: str, timeout: int = 30) -> str:
            r = sb.process.exec(cmd, timeout=timeout)
            return (r.result if hasattr(r, "result") else str(r)) or ""

        await loop.run_in_executor(None, lambda: _exec("mkdir -p /v/shared /v/system/supervisor"))
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
    """Return one of: 'running' | 'stopped' | 'missing' | 'error'."""
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
    if state_str in ("started", "running"):
        return "running"
    if state_str in ("stopped", "paused"):
        return "stopped"
    return "error"


# ---------------------------------------------------------------------------
# Uniform API — each provider module exposes these names.
# ---------------------------------------------------------------------------

async def create_volume(name: str) -> str:
    return await create_daytona_volume(name)


async def delete_volume(ref: str) -> None:
    return await delete_daytona_volume(ref)


async def get_sandbox_status(ref: str) -> str:
    return await get_daytona_sandbox_status(ref)


async def start_sandbox(ref: str) -> None:
    return await start_daytona(ref)


async def destroy_sandbox(inst: ProviderInstance) -> None:
    return await destroy_daytona(inst)


async def stop_sandbox(inst: ProviderInstance) -> None:
    return await stop_daytona(inst)


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
            None, lambda: daytona_client.get(inst.sandbox_id)
        )
    except Exception as e:
        # Daytona raises a plain Exception with "not found" in the message when
        # the sandbox has been deleted out-of-band. Surface this as a typed
        # error so the server can re-provision on the same volume.
        if "not found" in str(e).lower():
            from ._shared import SandboxMissingError
            raise SandboxMissingError(
                f"Daytona sandbox {inst.sandbox_id} not found (deleted externally)"
            ) from e
        raise
    # If the sandbox was stopped externally (e.g. daytona.stop()), start it
    # and wait for the container network to be ready before exec-ing.
    raw_state = sandbox.state
    state_str = raw_state.value if hasattr(raw_state, "value") else str(raw_state)
    if state_str != "started":
        log.info("ensure_supervisor_url: sandbox %s is %s; starting", inst.sandbox_id[:16], state_str)
        await loop.run_in_executor(None, sandbox.start)
        await _wait_for_daytona_sandbox_ready(daytona_client, inst.sandbox_id)
        sandbox = await loop.run_in_executor(None, lambda: daytona_client.get(inst.sandbox_id))
    # The agent's HOME is /home/daytona — a local ext4 dir the supervisor
    # creates and populates from the volume snapshot on boot. supervisor.js
    # sets HOME=root when spawning the ACP child so Claude Code's session
    # JSONLs land in the restored workspace. No env-level HOME override
    # needed here anymore.
    return await start_supervisor_in_sandbox(
        sandbox, agent_type, port, root=_DAYTONA_AGENT_HOME, spawn_env=spawn_env,
    )


async def install_supervisor(volume_ref: str, agent_type: str) -> None:
    """Atomically install supervisor.js + ACP deps on this volume.

    Writes the artifacts (``deps.tar.gz`` + ``supervisor.js``) into a sibling
    staging dir (``system/supervisor.tmp.<uuid>/``) first, verifies both
    sentinels are present, then atomically renames the staging dir over
    ``system/supervisor`` via ``mv``. If any step fails the staging dir is
    removed, leaving any previous install untouched — a retried install
    can never end up with a half-written ``deps.tar.gz`` racing against a
    live reader.

    Mounts ``system/`` (not ``system/supervisor``) so staging and final
    share a single mount point and ``mv`` is a rename-within-volume.
    """
    from daytona_sdk import (
        Daytona, DaytonaConfig, CreateSandboxFromSnapshotParams,
        CreateSandboxFromImageParams, VolumeMount,
    )

    api_key = os.environ.get("DAYTONA_API_KEY")
    if not api_key:
        raise RuntimeError("DAYTONA_API_KEY not set")
    daytona = Daytona(DaytonaConfig(api_key=api_key))
    loop = asyncio.get_running_loop()

    snapshot = os.environ.get("DAYTONA_SNAPSHOT", "hive-large").strip()
    use_snapshot = snapshot.lower() not in {"", "0", "false", "image"}

    # Mount whole system/ so staging + final share the mount and mv can rename.
    volumes = [VolumeMount(volume_id=volume_ref, mount_path="/work", subpath="system")]

    if use_snapshot:
        sb = await loop.run_in_executor(None, lambda: daytona.create(
            CreateSandboxFromSnapshotParams(
                snapshot=snapshot, auto_stop_interval=0,
                env_vars=_get_sandbox_env_vars(), volumes=volumes,
            ), timeout=120,
        ))
    else:
        sb = await loop.run_in_executor(None, lambda: daytona.create(
            CreateSandboxFromImageParams(
                image="node:22-slim", auto_stop_interval=0,
                env_vars=_get_sandbox_env_vars(), volumes=volumes,
            ), timeout=120,
        ))

    staging_name = f"supervisor.tmp.{uuid.uuid4().hex[:8]}"
    staging_on_volume = f"/work/{staging_name}"
    final_on_volume = "/work/supervisor"

    try:
        npm_spec = _ACP_NPM_SPECS[agent_type]

        def _exec(cmd: str, timeout: int | None = 60) -> str:
            r = sb.process.exec(cmd, timeout=timeout)
            return (r.result if hasattr(r, "result") else str(r)) or ""

        # npm install to local ephemeral FS (fast SSD), pack to a tarball,
        # place it on the volume under the staging dir.  Every shell step
        # uses ``set -e`` so a silent npm failure can't produce an empty
        # node_modules that then tarballs without the acp bin.
        local_dir = "/tmp/sup-install"
        bin_name = _acp_bin_name(agent_type)
        await loop.run_in_executor(None, lambda: _exec(
            f"set -e && rm -rf {local_dir} && mkdir -p {local_dir} && "
            f"cd {local_dir} && npm init -y >/dev/null 2>&1"
        ))
        install_out = await loop.run_in_executor(None, lambda: _exec(
            f"set -e && cd {local_dir} && "
            f"npm install --omit=optional {npm_spec} 2>&1",
            240,
        ))
        # Sentinel: the ACP bin must exist after npm install.  If it
        # doesn't, surface the npm output so the caller knows why.
        verify = await loop.run_in_executor(None, lambda: _exec(
            f"test -f {local_dir}/node_modules/.bin/{bin_name} && echo ok || echo missing"
        ))
        if verify.strip() != "ok":
            raise RuntimeError(
                f"npm install produced no {bin_name} at "
                f"{local_dir}/node_modules/.bin/ — npm output:\n"
                f"{install_out[-2000:]}"
            )

        # Build the staging dir on the volume and drop the tarball there.
        await loop.run_in_executor(None, lambda: _exec(
            f"set -e && mkdir -p {staging_on_volume} && "
            f"tar -C {local_dir} -czf /tmp/deps.tar.gz . && "
            # Sanity-check the tarball contains the acp bin before we ship
            # it onto the slow network volume.  Cheap sanity gate.
            f"tar -tzf /tmp/deps.tar.gz | grep -q 'node_modules/.bin/{bin_name}' && "
            f"cp /tmp/deps.tar.gz {staging_on_volume}/deps.tar.gz && "
            f"rm -f /tmp/deps.tar.gz && rm -rf {local_dir}",
            120,
        ))

        # Upload supervisor.js into the staging dir via the Daytona fs API.
        with open(_SUPERVISOR_DIR / "supervisor.js", "rb") as f:
            sup_js_bytes = f.read()
        await loop.run_in_executor(
            None, lambda: sb.fs.upload_file(
                sup_js_bytes, f"{staging_on_volume}/supervisor.js"
            )
        )

        # Sentinel checks + atomic swap. Using shell ``test`` so a single
        # missing file aborts before we touch the existing supervisor dir.
        # mountpoint-s3 doesn't support rename of non-empty dirs, so we
        # create final/ and copy contents in (idempotent via rm -rf first).
        await loop.run_in_executor(None, lambda: _exec(
            f"set -e && "
            f"test -f {staging_on_volume}/deps.tar.gz && "
            f"test -f {staging_on_volume}/supervisor.js && "
            f"rm -rf {final_on_volume} && "
            f"mkdir -p {final_on_volume} && "
            f"cp -f {staging_on_volume}/deps.tar.gz {final_on_volume}/deps.tar.gz && "
            f"cp -f {staging_on_volume}/supervisor.js {final_on_volume}/supervisor.js && "
            f"rm -rf {staging_on_volume} && "
            f"test -f {final_on_volume}/deps.tar.gz && "
            f"test -f {final_on_volume}/supervisor.js && "
            f"sync",
            120,
        ))

        log.info("supervisor installed on volume %s for %s", volume_ref, agent_type)
    except BaseException:
        # Best-effort staging-dir cleanup; don't mask the original error.
        try:
            await loop.run_in_executor(None, lambda: sb.process.exec(
                f"rm -rf {staging_on_volume}", timeout=30,
            ))
        except Exception as cleanup_err:  # pragma: no cover
            log.warning("daytona install_supervisor staging cleanup failed: %s", cleanup_err)
        raise
    finally:
        try:
            await loop.run_in_executor(None, lambda: daytona.delete(sb))
        except Exception:
            pass


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
    sandbox_id: str | None = None,  # accepted for parity; daytona has no labels
) -> ProviderInstance:
    """Uniform ``create_sandbox`` for the Daytona provider.

    Delegates to ``provision_daytona_sandbox`` which creates the sandbox with
    the three volume mounts but does NOT start a supervisor; the caller must
    run ``ensure_supervisor_url`` before talking to the supervisor.

    ``spawn_env`` / ``port`` / ``sandbox_id`` are accepted for parity with
    docker/local but are unused here — the supervisor is started later with
    its own env + port, and Daytona has no container-label concept.
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
    )


# ---------------------------------------------------------------------------
# Volume file ops — dispatched from server.py's /volumes/{id}/files/*
# ---------------------------------------------------------------------------

async def _run_in_utility_sandbox(ref: str, cmd: str, timeout: int = 30):
    """Spin a short-lived sandbox with ``ref`` mounted at /v, run cmd, tear down."""
    from ._shared import _exec_subprocess  # noqa: F401
    from .. import providers as _prov  # pragma: no cover — local import for cycle
    inst = await provision_daytona_sandbox(
        agent_type="claude",
        volume_id=ref,
        subpath=None,
    )
    try:
        return await _prov.exec_in_instance(inst, cmd, timeout=timeout)
    finally:
        try:
            await destroy_daytona(inst)
        except Exception as cleanup_err:  # pragma: no cover
            log.warning("utility sandbox cleanup failed: %s", cleanup_err)


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
