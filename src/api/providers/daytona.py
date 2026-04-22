"""Daytona provider — create/destroy/exec in Daytona sandboxes."""

import asyncio
import logging
import os
import shlex
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
    _wait_for_health,
    ProviderInstance,
    _build_volume_mounts,
    allocate_sandbox_port,
)

_SUPERVISOR_DIR = Path(__file__).resolve().parent.parent.parent / "supervisor"
_SUPERVISOR_REMOTE_DIR = "/tmp/agent-sdk-sup"  # legacy path (pre-volume era)
_SUPERVISOR_VOLUME_DIR = "/opt/supervisor"      # volume-mounted path (Phase 2+)
_SUPERVISOR_REMOTE_PORT = 9100


def _get_daytona_client():
    """Get a Daytona SDK client. Raises ImportError or RuntimeError on failure."""
    from daytona_sdk import Daytona, DaytonaConfig
    api_key = os.environ.get("DAYTONA_API_KEY")
    if not api_key:
        raise RuntimeError("DAYTONA_API_KEY not set")
    return Daytona(DaytonaConfig(api_key=api_key))


async def _bootstrap_supervisor_in_daytona_sandbox(
    sandbox, agent_type: str, *, install_deps: bool,
    pre_start_commands: list[str] | None = None,
    root: str = "/tmp",
    spawn_env: dict[str, str] | None = None,
) -> ProviderInstance:
    """Install (optionally) + start the supervisor inside an existing daytona
    sandbox object. Returns a fresh ProviderInstance with a new signed URL.

    install_deps=False is the recovery path: deps are already on disk from
    the previous run, so we skip apt + npm install + supervisor.js upload
    and just exec the supervisor binary.
    """
    bin_name = _acp_bin_name(agent_type)
    npm_spec = _ACP_NPM_SPECS[agent_type]
    loop = asyncio.get_running_loop()

    def _exec(cmd: str, timeout: int = 120) -> str:
        r = sandbox.process.exec(cmd, timeout=timeout)
        return (r.result if hasattr(r, "result") else str(r)) or ""

    if install_deps:
        await loop.run_in_executor(None, lambda: _exec(
            "apt-get update >/dev/null 2>&1 && "
            "apt-get install -y --no-install-recommends libssl3 ca-certificates >/dev/null 2>&1 || true",
            timeout=120,
        ))
        await loop.run_in_executor(None, lambda: _exec(
            f"mkdir -p {_SUPERVISOR_REMOTE_DIR} && cd {_SUPERVISOR_REMOTE_DIR} && "
            "npm init -y >/dev/null 2>&1"
        ))
        await loop.run_in_executor(None, lambda: _exec(
            f"cd {_SUPERVISOR_REMOTE_DIR} && "
            f"npm install --silent {npm_spec} 2>&1 | tail -5",
            timeout=240,
        ))

        import base64 as _b64
        with open(_SUPERVISOR_DIR / "supervisor.js", "rb") as f:
            b64 = _b64.b64encode(f.read()).decode()

        def _upload():
            _exec(f"rm -f {_SUPERVISOR_REMOTE_DIR}/supervisor.js {_SUPERVISOR_REMOTE_DIR}/supervisor.js.b64")
            chunk = 4096
            for i in range(0, len(b64), chunk):
                seg = b64[i:i + chunk]
                _exec(f"printf '%s' '{seg}' >> {_SUPERVISOR_REMOTE_DIR}/supervisor.js.b64")
            _exec(
                f"base64 -d {_SUPERVISOR_REMOTE_DIR}/supervisor.js.b64 > "
                f"{_SUPERVISOR_REMOTE_DIR}/supervisor.js && "
                f"rm {_SUPERVISOR_REMOTE_DIR}/supervisor.js.b64"
            )
        await loop.run_in_executor(None, _upload)

    # Run pre-start commands (e.g. skill installation) before supervisor
    if pre_start_commands:
        for cmd in pre_start_commands:
            log.info("daytona pre-start: %s", cmd)
            await loop.run_in_executor(None, lambda c=cmd: _exec(c, timeout=120))

    acp_bin = f"{_SUPERVISOR_REMOTE_DIR}/node_modules/.bin/{bin_name}"
    launch_args = _acp_launch_args(agent_type)
    acp_arg_flags = "".join(f" --acp-arg {shlex.quote(a)}" for a in launch_args)
    env_prefix = _build_env_prefix(spawn_env)
    inner = (
        f"cd {_SUPERVISOR_REMOTE_DIR} && "
        f"setsid env {env_prefix} node supervisor.js --host 0.0.0.0 --port {_SUPERVISOR_REMOTE_PORT} "
        f"--acp {acp_bin}{acp_arg_flags} --root {root} "
        f"> {_SUPERVISOR_REMOTE_DIR}/sup.log 2>&1 </dev/null & echo started"
    )
    start_cmd = f"sh -c {shlex.quote(inner)}"
    await loop.run_in_executor(None, lambda: _exec(start_cmd, timeout=10))
    await asyncio.sleep(3)

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

    check_result = await loop.run_in_executor(
        None, lambda: _exec(f"test -f {vol_tarball} && echo yes || echo no")
    )

    if "yes" in check_result:
        # Volume-cached mode: extract deps tarball to local ephemeral dir.
        # Reading one archive from the volume is fast; we never write
        # node_modules there.
        await loop.run_in_executor(None, lambda: _exec(
            f"mkdir -p {local_work} && "
            f"tar -C {local_work} -xzf {vol_tarball} && "
            f"cp {vol_supervisor} {local_work}/supervisor.js",
            120,
        ))
        sup_dir = local_work
        log.info("start_supervisor_in_sandbox: using volume cache → %s (port %d, sandbox %s)",
                 local_work, port, sandbox.id[:16])
    else:
        # Legacy path: deps are installed directly in the sandbox.
        sup_dir = _SUPERVISOR_REMOTE_DIR
        log.info("start_supervisor_in_sandbox: using legacy path %s (port %d, sandbox %s)",
                 _SUPERVISOR_REMOTE_DIR, port, sandbox.id[:16])

    acp_bin = f"{sup_dir}/node_modules/.bin/{bin_name}"
    launch_args = _acp_launch_args(agent_type)
    acp_arg_flags = "".join(f" --acp-arg {shlex.quote(a)}" for a in launch_args)
    env_prefix = _build_env_prefix(spawn_env)
    log_file = f"{sup_dir}/sup-{port}.log"
    inner = (
        f"cd {sup_dir} && "
        f"setsid env {env_prefix} node supervisor.js --host 0.0.0.0 --port {port} "
        f"--acp {acp_bin}{acp_arg_flags} --root {root} "
        f"> {log_file} 2>&1 </dev/null & echo started"
    )
    start_cmd = f"sh -c {shlex.quote(inner)}"
    await loop.run_in_executor(None, lambda: _exec(start_cmd, timeout=10))
    await asyncio.sleep(3)

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
    sandbox = await loop.run_in_executor(None, lambda: daytona.get(daytona_sandbox_id))

    raw_state = sandbox.state
    state_str = raw_state.value if hasattr(raw_state, "value") else str(raw_state)
    if state_str != "started":
        log.info("starting stopped daytona sandbox %s", daytona_sandbox_id)
        await loop.run_in_executor(None, sandbox.start)

    return await _bootstrap_supervisor_in_daytona_sandbox(
        sandbox, agent_type, install_deps=False, root=root, spawn_env=spawn_env,
    )


async def create_daytona(
    agent_type: str = "claude",
    dockerfile: str | None = None,
    pre_start_commands: list[str] | None = None,
    root: str = "/tmp",
    spawn_env: dict[str, str] | None = None,
    volume_id: str | None = None,
    subpath: str | None = None,
) -> ProviderInstance:
    """Create a fresh Daytona sandbox, install + start a supervisor inside it."""
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

    env_vars = _get_sandbox_env_vars(spawn_env)
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
        # node:22-slim ships node + npm preinstalled. libssl3 is added at
        # runtime in the bootstrap helper because codex-acp's native binary
        # dynamically links against it.
        image = "node:22-slim"

    create_timeout = 300 if dockerfile else 60

    volumes = _build_volume_mounts(volume_id, subpath)

    if use_snapshot:
        sandbox = await loop.run_in_executor(None, lambda: daytona.create(
            CreateSandboxFromSnapshotParams(
                snapshot=snapshot,
                auto_stop_interval=0,
                env_vars=env_vars,
                volumes=volumes,
            ),
            timeout=create_timeout,
        ))
    else:
        sandbox = await loop.run_in_executor(None, lambda: daytona.create(
            CreateSandboxFromImageParams(
                image=image,
                auto_stop_interval=0,
                env_vars=env_vars,
                volumes=volumes,
            ),
            timeout=create_timeout,
        ))

    try:
        return await _bootstrap_supervisor_in_daytona_sandbox(
            sandbox, agent_type, install_deps=True, pre_start_commands=pre_start_commands,
            root=root, spawn_env=spawn_env,
        )
    except BaseException:
        try:
            await loop.run_in_executor(None, lambda: daytona.delete(sandbox))
        except Exception:
            pass
        raise


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


async def start_daytona(sandbox_ref: str) -> None:
    """Start a stopped Daytona sandbox by its provider ref."""
    try:
        daytona = _get_daytona_client()
    except (ImportError, RuntimeError) as e:
        log.warning("cannot start daytona sandbox %s: %s", sandbox_ref, e)
        return

    loop = asyncio.get_running_loop()
    try:
        sandbox = await loop.run_in_executor(None, lambda: daytona.get(sandbox_ref))
        await loop.run_in_executor(None, sandbox.start)
        log.info("daytona sandbox started: %s", sandbox_ref)
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
    vol = await asyncio.to_thread(client.volume.create, name)
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


async def destroy_sandbox(inst) -> None:
    return await destroy_daytona(inst)


async def stop_sandbox(inst) -> None:
    return await stop_daytona(inst)


async def ensure_supervisor_url(inst, *, agent_type: str, root: str = "/tmp",
                                spawn_env: dict | None = None,
                                port: int | None = None, **_kw) -> str:
    """Daytona: start a supervisor in the sandbox referenced by inst and
    return its URL. Implements the 2-phase 'create, then start-supervisor'
    model (docker/local start the supervisor at create time and just return
    inst.url)."""
    from daytona_sdk import Daytona, DaytonaConfig
    import os as _os
    api_key = _os.environ.get("DAYTONA_API_KEY")
    if not api_key:
        raise RuntimeError("DAYTONA_API_KEY not set")
    daytona_client = Daytona(DaytonaConfig(api_key=api_key))
    loop = asyncio.get_running_loop()
    sandbox = await loop.run_in_executor(
        None, lambda: daytona_client.get(inst.sandbox_id)
    )
    return await start_supervisor_in_sandbox(
        sandbox, agent_type, port, root=root, spawn_env=spawn_env,
    )


async def install_supervisor(volume_ref: str, agent_type: str) -> None:
    """Install supervisor.js + ACP binary on this volume at system/supervisor/.

    Spins a 1-shot Daytona sandbox with the volume mounted
    (subpath=system/supervisor at /work), runs npm install + uploads
    supervisor.js, then tears down the sandbox.
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

    volumes = [VolumeMount(volume_id=volume_ref, mount_path="/work", subpath="system/supervisor")]

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
        npm_spec = _ACP_NPM_SPECS[agent_type]

        def _exec(cmd: str, timeout: int | None = 60) -> str:
            r = sb.process.exec(cmd, timeout=timeout)
            return (r.result if hasattr(r, "result") else str(r)) or ""

        # Install to local ephemeral FS (fast SSD), create a single archive,
        # then copy ONLY that archive to the volume.  Extracting thousands of
        # node_modules files directly to a network volume is very slow and
        # times out; writing one ~20 MB .tar.gz is fast.
        # At start time, start_supervisor_in_sandbox extracts the archive
        # to local ephemeral FS inside the session sandbox.
        local_dir = "/tmp/sup-install"
        await loop.run_in_executor(None, lambda: _exec(
            f"mkdir -p {local_dir} && cd {local_dir} && "
            "(test -f package.json || npm init -y >/dev/null 2>&1)"
        ))
        await loop.run_in_executor(None, lambda: _exec(
            f"cd {local_dir} && npm install --omit=optional {npm_spec} 2>&1 | tail -5",
            240,
        ))

        # Pack to a single archive on local FS, then copy to volume as one file.
        # cp of a single ~20 MB file is fast (a few seconds); tar-extracting
        # thousands of files is slow.
        await loop.run_in_executor(None, lambda: _exec(
            f"tar -C {local_dir} -czf /tmp/deps.tar.gz . && "
            f"cp /tmp/deps.tar.gz /work/deps.tar.gz && "
            f"rm -f /tmp/deps.tar.gz && rm -rf {local_dir}",
            120,
        ))

        # Upload supervisor.js using the Daytona filesystem API — avoids the
        # shell command length / printf append reliability issues.
        with open(_SUPERVISOR_DIR / "supervisor.js", "rb") as f:
            sup_js_bytes = f.read()

        await loop.run_in_executor(
            None, lambda: sb.fs.upload_file(sup_js_bytes, "/work/supervisor.js")
        )

        log.info("supervisor installed on volume %s for %s", volume_ref, agent_type)
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
    **_kw,
) -> ProviderInstance:
    """Uniform ``create_sandbox`` for the Daytona provider.

    Delegates to ``provision_daytona_sandbox`` which creates the sandbox with
    the three volume mounts but does NOT start a supervisor; the caller must
    run ``ensure_supervisor_url`` before talking to the supervisor.

    ``spawn_env`` / ``port`` are accepted for parity with docker/local but are
    unused here — the supervisor is started later with its own env + port.
    """
    return await provision_daytona_sandbox(
        agent_type=agent_type,
        dockerfile=dockerfile,
        pre_start_commands=pre_start_commands,
        root=root or "/home/daytona",
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
    """Tree listing of ``<volume>/<path>``.

    Spins a utility sandbox with the whole volume at /v, lists files.
    Paths are relative to the volume root (e.g. ``"shared"``, ``""`` for all).
    """
    rel = (path or "").lstrip("/")
    if ".." in rel.split("/") or any(c in rel for c in "\x00\n\r"):
        raise ValueError("invalid path")
    target = "/v/" + rel if rel else "/v"
    res = await _run_in_utility_sandbox(
        ref, f"find {shlex.quote(target)} -maxdepth 3 -printf '%y %p\\n' 2>/dev/null"
    )
    return res.stdout


async def volume_read(ref: str, path: str) -> bytes:
    """Read ``<volume>/<path>`` bytes via a short-lived utility sandbox."""
    rel = (path or "").lstrip("/")
    if not rel or ".." in rel.split("/") or any(c in rel for c in "\x00\n\r"):
        raise ValueError("invalid path")
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
    rel = (path or "").lstrip("/")
    if not rel or ".." in rel.split("/") or any(c in rel for c in "\x00\n\r"):
        raise ValueError("invalid path")
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
