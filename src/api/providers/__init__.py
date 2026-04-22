"""ACP supervisor provider management — local, docker, daytona.

This package splits provider-specific code into sub-modules:
  - daytona.py   — Daytona sandbox management
  - docker.py    — Docker container management (NotImplementedError stubs)
  - local.py     — Local subprocess management (NotImplementedError stubs)
  - _shared.py   — Shared types, constants, helpers

providers/__init__.py:
  - Re-exports everything from _shared for backward compatibility
  - Re-exports Daytona-specific symbols for server.py compatibility
  - Implements local/docker provider logic (create/destroy/exec)
  - Provides universal dispatch wrappers (create_instance, destroy_instance, etc.)
  - Provides new uniform-API dispatch helpers for Phase 1+ use
"""

import asyncio
import logging
import os
import shlex
import shutil
import signal
from pathlib import Path

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
# Local provider implementation (kept in __init__.py for now)
# ---------------------------------------------------------------------------

_SUPERVISOR_DIR = Path(__file__).resolve().parent.parent.parent / "supervisor"
_supervisor_deps_lock = asyncio.Lock()
_supervisor_deps_ready = False


async def _ensure_local_supervisor_deps(agent_type: str) -> str:
    """Ensure the ACP binary for *agent_type* is available.

    For npm-based agents: installs from src/supervisor/package.json.
    For system-installed agents (openhands, etc.): looks up in PATH.
    Returns the absolute path to the binary."""
    global _supervisor_deps_ready
    bin_name = _acp_bin_name(agent_type)

    # Non-npm agents: look up in PATH
    if agent_type not in _ACP_NPM_SPECS:
        system_bin = shutil.which(bin_name)
        if not system_bin:
            raise RuntimeError(
                f"{bin_name} not found in PATH. Install it first "
                f"(e.g. 'uv tool install {bin_name}' or check the agent's docs)."
            )
        return system_bin

    acp_bin = _SUPERVISOR_DIR / "node_modules" / ".bin" / bin_name
    async with _supervisor_deps_lock:
        if _supervisor_deps_ready and acp_bin.exists():
            return str(acp_bin)
        if not acp_bin.exists():
            log.info("installing supervisor deps in %s", _SUPERVISOR_DIR)
            npm = shutil.which("npm")
            if not npm:
                raise RuntimeError("npm not found; install Node.js >=18")
            proc = await asyncio.create_subprocess_exec(
                npm, "install", "--silent",
                cwd=str(_SUPERVISOR_DIR),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            _, stderr = await proc.communicate()
            if proc.returncode != 0:
                raise RuntimeError(f"npm install failed: {stderr.decode()[:500]}")
        if not acp_bin.exists():
            raise RuntimeError(f"supervisor dep install succeeded but {bin_name} missing at {acp_bin}")
        _supervisor_deps_ready = True
        return str(acp_bin)


async def create_local(
    agent_type: str = "claude",
    root: str = "/tmp",
    spawn_env: dict[str, str] | None = None,
) -> ProviderInstance:
    """Spawn a local supervisor.js subprocess that bridges stdio ↔ HTTP
    (POST + SSE) for the given agent's ACP binary."""
    node = shutil.which("node")
    if not node:
        raise RuntimeError("node binary not found; install Node.js >=18")
    acp_bin = await _ensure_local_supervisor_deps(agent_type)
    launch_args = _acp_launch_args(agent_type)

    port = await _find_free_port()
    # Start from os.environ (for PATH, HOME, LANG, etc.) but strip every
    # credential key — the server never leaks its own API keys/tokens into
    # a sandbox supervisor. Then overlay IS_SANDBOX + spawn_env.
    env = {k: v for k, v in os.environ.items() if k not in AUTH_KEYS}
    env.update(_get_sandbox_env_vars(spawn_env))
    extra: list[str] = []
    for arg in launch_args:
        extra += ["--acp-arg", arg]
    try:
        proc = await asyncio.create_subprocess_exec(
            node, str(_SUPERVISOR_DIR / "supervisor.js"),
            "--host", "127.0.0.1",
            "--port", str(port),
            "--acp", acp_bin,
            *extra,
            "--root", root,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.PIPE,
            env=env,
        )
    except Exception:
        async with _port_lock:
            _freed_ports.append(port)
        raise

    url = f"http://127.0.0.1:{port}"
    try:
        if not await _wait_for_health(url):
            raise RuntimeError(f"supervisor failed to start on port {port}")
    except BaseException:
        proc.kill()
        await proc.wait()
        async with _port_lock:
            _freed_ports.append(port)
        raise

    log.info("local supervisor started on port %d (pid %d)", port, proc.pid)
    return ProviderInstance(
        provider="local",
        url=url,
        root=root,
        process=proc,
        port=port,
    )


def _get_child_pids(ppid: int) -> list[int]:
    """Return all descendant PIDs of *ppid* (best-effort, non-blocking)."""
    try:
        children = []
        for entry in os.listdir("/proc"):
            if not entry.isdigit():
                continue
            try:
                with open(f"/proc/{entry}/stat") as f:
                    parts = f.read().split()
                    if int(parts[3]) == ppid:
                        child = int(entry)
                        children.append(child)
                        children.extend(_get_child_pids(child))
            except (OSError, IndexError, ValueError):
                continue
        return children
    except Exception:
        return []


async def destroy_local(instance: ProviderInstance) -> None:
    """Kill a local supervisor subprocess and its ACP children."""
    pid = instance.process.pid if instance.process else None

    children = _get_child_pids(pid) if pid else []

    if instance.process and instance.process.returncode is None:
        instance.process.terminate()
        try:
            await asyncio.wait_for(instance.process.wait(), timeout=5)
        except asyncio.TimeoutError:
            instance.process.kill()
            await instance.process.wait()

    for cpid in children:
        try:
            os.kill(cpid, signal.SIGINT)
            log.warning("sent SIGINT to child pid %d of supervisor %s", cpid, pid)
        except OSError:
            pass

    port = instance.port
    await _recycle_port(instance)
    if port is not None:
        log.info("local supervisor stopped (port %d)", port)


# ---------------------------------------------------------------------------
# Docker provider implementation (kept in __init__.py for now)
# ---------------------------------------------------------------------------

_SUPERVISOR_DOCKER_IMAGE = "agent-sdk-acp-supervisor:latest"
_SUPERVISOR_DOCKER_PORT = 9100
_supervisor_image_lock = asyncio.Lock()
_supervisor_image_ready = False


async def _ensure_supervisor_docker_image() -> str:
    """Build the supervisor Docker image once per process, cache the tag."""
    global _supervisor_image_ready
    docker = shutil.which("docker")
    if not docker:
        raise RuntimeError("docker binary not found. Install Docker: https://docs.docker.com/get-docker/")
    async with _supervisor_image_lock:
        if _supervisor_image_ready:
            return _SUPERVISOR_DOCKER_IMAGE
        inspect = await asyncio.create_subprocess_exec(
            docker, "image", "inspect", _SUPERVISOR_DOCKER_IMAGE,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
        )
        await inspect.wait()
        if inspect.returncode == 0:
            _supervisor_image_ready = True
            return _SUPERVISOR_DOCKER_IMAGE
        log.info("building supervisor docker image %s", _SUPERVISOR_DOCKER_IMAGE)
        build = await asyncio.create_subprocess_exec(
            docker, "build", "-t", _SUPERVISOR_DOCKER_IMAGE, str(_SUPERVISOR_DIR),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        _, stderr = await build.communicate()
        if build.returncode != 0:
            raise RuntimeError(f"supervisor docker build failed:\n{stderr.decode()[:800]}")
        _supervisor_image_ready = True
        return _SUPERVISOR_DOCKER_IMAGE


async def create_docker(
    agent_type: str = "claude",
    dockerfile: str | None = None,
    pre_start_commands: list[str] | None = None,
    root: str = "/tmp",
    spawn_env: dict[str, str] | None = None,
) -> ProviderInstance:
    """Run the supervisor Docker image as a per-session container."""
    docker = shutil.which("docker")
    if not docker:
        raise RuntimeError("docker binary not found. Install Docker: https://docs.docker.com/get-docker/")

    image = await _ensure_supervisor_docker_image()
    port = await _find_free_port()

    bin_name = _acp_bin_name(agent_type)
    acp_path = f"/app/node_modules/.bin/{bin_name}"
    launch_args = _acp_launch_args(agent_type)

    env_prefix = _build_env_prefix(spawn_env)
    acp_arg_flags = "".join(f" --acp-arg {shlex.quote(a)}" for a in launch_args)
    supervisor_cmd = (
        f"env {env_prefix} node /app/supervisor.js --host 0.0.0.0 "
        f"--port {_SUPERVISOR_DOCKER_PORT} --root {shlex.quote(root)} "
        f"--acp {shlex.quote(acp_path)}{acp_arg_flags}"
    )
    if pre_start_commands:
        setup = " && ".join(pre_start_commands)
        shell_cmd = f"{setup} && {supervisor_cmd}"
    else:
        shell_cmd = supervisor_cmd

    cmd = [
        docker, "run", "-d", "--rm",
        "-p", f"{port}:{_SUPERVISOR_DOCKER_PORT}",
        "--entrypoint", "sh",
        image,
        "-c", shell_cmd,
    ]
    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await proc.communicate()
        if proc.returncode != 0:
            raise RuntimeError(f"docker run failed: {stderr.decode().strip()}")
        container_id = stdout.decode().strip()

        url = f"http://localhost:{port}"
        if not await _wait_for_health(url):
            rm_proc = await asyncio.create_subprocess_exec(
                docker, "rm", "-f", container_id,
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.DEVNULL,
            )
            await rm_proc.wait()
            raise RuntimeError(f"supervisor container failed to start on port {port}")

        log.info("docker supervisor started on port %d (container %s)", port, container_id[:12])
        return ProviderInstance(
            provider="docker",
            url=url,
            root=root,
            port=port,
            container_id=container_id,
        )
    except BaseException:
        async with _port_lock:
            _freed_ports.append(port)
        raise


async def destroy_docker(instance: ProviderInstance) -> None:
    """Stop and remove a Docker container running a supervisor."""
    if not instance.container_id:
        return

    docker = shutil.which("docker")
    if not docker:
        log.warning("docker not found, cannot remove container %s", instance.container_id[:12])
        return
    proc = await asyncio.create_subprocess_exec(
        docker, "rm", "-f", instance.container_id,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.DEVNULL,
    )
    await proc.communicate()
    port = instance.port
    cid = instance.container_id
    if proc.returncode == 0:
        await _recycle_port(instance)
        instance.container_id = None
        if port is not None:
            log.info("docker container stopped (port %d, container %s)", port, (cid or "?")[:12])
    else:
        log.warning("docker rm -f failed (rc=%d) for container %s — port %d NOT freed",
                     proc.returncode, (cid or "?")[:12], port or 0)


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
    mounts. Only Daytona uses them today.
    """
    if agent_type not in _ACP_BIN_NAMES:
        raise ValueError(f"unsupported agent_type: {agent_type!r}. Supported: {sorted(_ACP_BIN_NAMES)}")
    if provider == "local":
        return await create_local(agent_type, root=root, spawn_env=spawn_env)
    if provider == "docker":
        return await create_docker(
            agent_type, dockerfile=dockerfile, pre_start_commands=pre_start_commands,
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
        await destroy_local(instance)
    elif instance.provider == "daytona":
        await destroy_daytona(instance)
    elif instance.provider == "docker":
        await destroy_docker(instance)


async def stop_instance(instance: ProviderInstance) -> None:
    """Stop a supervisor instance (resumable). For local/docker, same as destroy."""
    if instance.provider == "local":
        await destroy_local(instance)
    elif instance.provider == "daytona":
        await stop_daytona(instance)
    elif instance.provider == "docker":
        await destroy_docker(instance)


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
