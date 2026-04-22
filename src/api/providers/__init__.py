"""ACP supervisor provider management — local, docker, daytona.

This package splits provider-specific code into sub-modules:
  - daytona.py   — Daytona sandbox management
  - docker.py    — Docker container management
  - local.py     — Local subprocess management
  - _shared.py   — Shared types, constants, helpers

providers/__init__.py:
  - Re-exports everything from _shared for backward compatibility
  - Re-exports Daytona-specific symbols for server.py compatibility
  - Provides universal dispatch wrappers (create_instance, destroy_instance, etc.)
  - Provides new uniform-API dispatch helpers for Phase 1+ use
"""

import asyncio
import logging
import shutil

from .. import load_dotenv

load_dotenv()

# Re-export all shared symbols so existing imports still work
from ._shared import (
    PORT_BASED_PROVIDERS,
    AUTH_KEYS,
    ProviderInstance,
    ExecResult,
    _ACP_BIN_NAMES,
    _ACP_NPM_SPECS,
    _ACP_LAUNCH_ARGS,
    _acp_bin_name,
    _acp_launch_args,
    _get_sandbox_env_vars,
    _auth_vars_to_unset,
    _build_env_prefix,
    _wait_for_health,
    _recycle_port,
    _find_free_port,
    allocate_sandbox_port,
    free_sandbox_port,
    _build_volume_mounts,
    _MAX_OUTPUT_BYTES,
    _truncate,
    _exec_subprocess,
    _port_lock,
    _freed_ports,
    _sandbox_port_counters,
    _sandbox_freed_ports,
)

# Re-export Daytona-specific symbols for server.py compatibility
from .daytona import (
    create_daytona,
    destroy_daytona,
    stop_daytona,
    create_daytona_volume,
    delete_daytona_volume,
    get_daytona_sandbox_status,
    provision_daytona_sandbox,
    restart_daytona_supervisor,
    start_supervisor_in_sandbox,
    kill_supervisor_in_sandbox,
    start_daytona,
    _get_daytona_client,
    _bootstrap_supervisor_in_daytona_sandbox,
    _SUPERVISOR_REMOTE_DIR,
    _SUPERVISOR_REMOTE_PORT,
    _daytona_sandbox_op,
)

# Provider module dispatch table
from . import daytona as _daytona_mod
from . import docker as _docker_mod
from . import local as _local_mod

_PROVIDER_MODS = {
    "daytona": _daytona_mod,
    "docker": _docker_mod,
    "local": _local_mod,
}

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Universal dispatch
# ---------------------------------------------------------------------------

async def create_instance(
    provider: str,
    agent_type: str = "claude",
    dockerfile: str | None = None,
    pre_start_commands: list[str] | None = None,
    root: str = "/tmp",
    spawn_env: dict[str, str] | None = None,
    volume_id: str | None = None,
    subpath: str | None = None,
) -> ProviderInstance:
    """Create an ACP supervisor instance using the specified provider.

    pre_start_commands are shell commands to run inside the sandbox BEFORE
    the supervisor process starts (used for skill installation).

    spawn_env is the merged env that should land in the supervisor process:
    {IS_SANDBOX:1} ∪ agent.env ∪ session.env ∪ secrets. The server never
    injects its own API keys — if spawn_env is empty, the supervisor runs
    with no credentials.

    volume_id + subpath are forwarded to providers that support volume
    mounts.
    """
    if agent_type not in _ACP_BIN_NAMES:
        raise ValueError(f"unsupported agent_type: {agent_type!r}. Supported: {sorted(_ACP_BIN_NAMES)}")
    if provider == "local":
        return await _local_mod.create_sandbox(
            volume_ref=volume_id, subpath=subpath or "",
            agent_type=agent_type, root=root, spawn_env=spawn_env,
        )
    if provider == "docker":
        return await _docker_mod.create_sandbox(
            volume_ref=volume_id, subpath=subpath or "",
            agent_type=agent_type, dockerfile=dockerfile,
            pre_start_commands=pre_start_commands,
            root=root, spawn_env=spawn_env,
        )
    if provider == "daytona":
        return await create_daytona(
            agent_type, dockerfile=dockerfile, pre_start_commands=pre_start_commands,
            root=root, spawn_env=spawn_env,
            volume_id=volume_id, subpath=subpath,
        )
    raise ValueError(f"Unknown provider: {provider!r}. Use 'local', 'docker', or 'daytona'.")


async def destroy_instance(instance: ProviderInstance) -> None:
    """Destroy a supervisor instance."""
    if instance.provider == "local":
        await _local_mod.destroy_sandbox(instance)
    elif instance.provider == "daytona":
        await destroy_daytona(instance)
    elif instance.provider == "docker":
        await _docker_mod.destroy_sandbox(instance)


async def stop_instance(instance: ProviderInstance) -> None:
    """Stop a supervisor instance (resumable). For local/docker, same as destroy."""
    if instance.provider == "local":
        await _local_mod.destroy_sandbox(instance)
    elif instance.provider == "daytona":
        await stop_daytona(instance)
    elif instance.provider == "docker":
        await _docker_mod.destroy_sandbox(instance)


async def exec_in_instance(instance: ProviderInstance, cmd: str, timeout: int = 30) -> ExecResult:
    """Run a shell command in the sandbox environment."""
    if instance.provider == "local":
        proc = await asyncio.create_subprocess_shell(
            cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        return await _exec_subprocess(proc, timeout)

    elif instance.provider == "docker":
        docker = shutil.which("docker")
        if not docker or not instance.container_id:
            raise RuntimeError("docker not available or no container_id")
        proc = await asyncio.create_subprocess_exec(
            docker, "exec", instance.container_id, "sh", "-c", cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        return await _exec_subprocess(proc, timeout)

    elif instance.provider == "daytona":
        if not instance.sandbox_id:
            raise RuntimeError("no sandbox_id for daytona exec")
        daytona = _get_daytona_client()
        loop = asyncio.get_running_loop()
        sandbox = await loop.run_in_executor(None, lambda: daytona.get(instance.sandbox_id))
        try:
            r = await asyncio.wait_for(
                loop.run_in_executor(
                    None, lambda: sandbox.process.exec(cmd, timeout=timeout)
                ),
                timeout=timeout + 5,
            )
            out = (r.result if hasattr(r, "result") else str(r)) or ""
            out, trunc = _truncate(out.encode(), _MAX_OUTPUT_BYTES)
            return ExecResult(stdout=out, stderr="", exit_code=0, stdout_truncated=trunc)
        except asyncio.TimeoutError:
            return ExecResult(stdout="", stderr="", exit_code=-1, timed_out=True)

    else:
        raise ValueError(f"exec_in_instance: unsupported provider {instance.provider!r}")


# ---------------------------------------------------------------------------
# New uniform-API dispatch helpers (for Phase 1+ use by server.py)
# ---------------------------------------------------------------------------

async def create_volume(provider: str, name: str) -> str:
    return await _PROVIDER_MODS[provider].create_volume(name)


async def delete_volume(provider: str, ref: str) -> None:
    return await _PROVIDER_MODS[provider].delete_volume(ref)


async def get_sandbox_status(provider: str, ref: str) -> str:
    return await _PROVIDER_MODS[provider].get_sandbox_status(ref)


async def start_sandbox(provider: str, ref: str) -> None:
    return await _PROVIDER_MODS[provider].start_sandbox(ref)


async def destroy_sandbox(provider: str, inst) -> None:
    return await _PROVIDER_MODS[provider].destroy_sandbox(inst)


async def stop_sandbox(provider: str, inst) -> None:
    return await _PROVIDER_MODS[provider].stop_sandbox(inst)


async def ensure_supervisor_url(provider: str, inst, **kw) -> str:
    return await _PROVIDER_MODS[provider].ensure_supervisor_url(inst, **kw)


async def install_supervisor(provider: str, volume_ref: str, agent_type: str) -> None:
    return await _PROVIDER_MODS[provider].install_supervisor(volume_ref, agent_type)


async def volume_tree(provider: str, ref: str, path: str) -> str:
    return await _PROVIDER_MODS[provider].volume_tree(ref, path)


async def volume_read(provider: str, ref: str, path: str) -> bytes:
    return await _PROVIDER_MODS[provider].volume_read(ref, path)


async def volume_write(provider: str, ref: str, path: str, content: bytes) -> None:
    return await _PROVIDER_MODS[provider].volume_write(ref, path, content)
