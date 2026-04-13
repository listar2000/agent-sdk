"""Sandbox-agent provider management.

Handles the lifecycle of sandbox-agent instances:
- local: spawn as subprocess on the host
- daytona: create Daytona sandbox, install sandbox-agent, start server
"""

import asyncio
import logging
import os
import signal
import shutil
import uuid
from dataclasses import dataclass
from pathlib import Path

import httpx

from . import load_dotenv

log = logging.getLogger(__name__)

SANDBOX_AGENT_IMAGE = "rivetdev/sandbox-agent:0.4.2-full"
DEFAULT_DAYTONA_SNAPSHOT = "hive-large"
SANDBOX_AGENT_PORT = 3000
SANDBOX_AGENT_INSTALL_CMDS = [
    "apt-get update && apt-get install -y --no-install-recommends curl nodejs npm && rm -rf /var/lib/apt/lists/*",
    "curl -fsSL https://releases.rivet.dev/sandbox-agent/0.4.x/install.sh | sh",
]
PORT_BASED_PROVIDERS = frozenset({"local", "docker"})


async def _wait_for_health(url: str, max_retries: int = 30, interval: float = 0.5) -> bool:
    """Poll /v1/health until 200 or retries exhausted."""
    async with httpx.AsyncClient(timeout=5) as client:
        for _ in range(max_retries):
            try:
                r = await client.get(f"{url}/v1/health")
                if r.status_code == 200:
                    return True
            except Exception:
                pass
            await asyncio.sleep(interval)
    return False


load_dotenv()


def _get_sandbox_env_vars() -> dict[str, str]:
    """Collect API keys and sandbox config from environment."""
    env: dict[str, str] = {"IS_SANDBOX": "1"}
    for var in ("ANTHROPIC_API_KEY", "OPENAI_API_KEY"):
        val = os.environ.get(var)
        if val:
            env[var] = val
    return env


@dataclass
class ProviderInstance:
    """A running sandbox-agent instance."""
    provider: str              # "local", "daytona", or "docker"
    url: str                   # base URL for the sandbox-agent server
    sandbox_id: str | None = None  # Daytona sandbox ID (if daytona)
    process: asyncio.subprocess.Process | None = None  # local subprocess
    port: int | None = 0       # local port (if local or docker)
    container_id: str | None = None  # Docker container ID (if docker)


# ── Local provider ──

_next_local_port = 2469
_freed_ports: list[int] = []
_port_lock = asyncio.Lock()


async def _recycle_port(instance) -> None:
    """Return instance's port to the free pool (idempotent)."""
    port = instance.port
    if port is None:
        return
    instance.port = None
    async with _port_lock:
        _freed_ports.append(port)


async def _find_free_port() -> int:
    global _next_local_port
    async with _port_lock:
        if _freed_ports:
            return _freed_ports.pop()
        port = _next_local_port
        _next_local_port += 1
        return port


async def create_local(agent_type: str = "claude") -> ProviderInstance:
    """Spawn a local sandbox-agent server as a subprocess."""
    binary = shutil.which("sandbox-agent")
    if not binary:
        raise RuntimeError("sandbox-agent binary not found. Install: curl -fsSL https://releases.rivet.dev/sandbox-agent/0.4.x/install.sh | sh")

    port = await _find_free_port()

    env = {**os.environ, **_get_sandbox_env_vars()}
    try:
        proc = await asyncio.create_subprocess_exec(
            binary, "server", "--no-token", "--port", str(port),
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
            env=env,
        )
    except Exception:
        async with _port_lock:
            _freed_ports.append(port)
        raise

    url = f"http://localhost:{port}"
    try:
        if not await _wait_for_health(url):
            raise RuntimeError(f"sandbox-agent failed to start on port {port}")
    except BaseException:
        # Health-check failure, CancelledError, or other — kill and recycle port.
        # All error paths funnel here to prevent double-free.
        proc.kill()
        await proc.wait()
        async with _port_lock:
            _freed_ports.append(port)
        raise

    log.info("local sandbox-agent started on port %d (pid %d)", port, proc.pid)
    return ProviderInstance(provider="local", url=url, process=proc, port=port)


def _get_child_pids(ppid: int) -> list[int]:
    """Return all descendant PIDs of *ppid* (best-effort, non-blocking).

    Uses /proc to avoid dependency on pgrep (not in slim images).
    """
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
    """Kill a local sandbox-agent subprocess and its ACP children."""
    pid = instance.process.pid if instance.process else None

    # Snapshot child PIDs before killing the parent so they don't get
    # re-parented to init before we can signal them.
    children = _get_child_pids(pid) if pid else []

    if instance.process and instance.process.returncode is None:
        instance.process.terminate()
        try:
            await asyncio.wait_for(instance.process.wait(), timeout=5)
        except asyncio.TimeoutError:
            instance.process.kill()
            await instance.process.wait()

    # ACP node processes ignore SIGTERM but exit on SIGINT.
    for cpid in children:
        try:
            os.kill(cpid, signal.SIGINT)
            log.warning("sent SIGINT to child pid %d of sandbox-agent %s", cpid, pid)
        except OSError:
            pass  # already gone

    port = instance.port
    await _recycle_port(instance)
    if port is not None:
        log.info("local sandbox-agent stopped (port %d)", port)


# ── Daytona provider ──

def _build_daytona_image(dockerfile: str | None):
    """Build a Daytona image spec from an optional Dockerfile path.

    Returns the default sandbox-agent image string when no dockerfile is given,
    or a Daytona Image object with sandbox-agent auto-injected otherwise.
    """
    if dockerfile is None:
        return SANDBOX_AGENT_IMAGE

    if not Path(dockerfile).exists():
        raise FileNotFoundError(f"Dockerfile not found: {dockerfile}")

    from daytona_sdk import Image
    return Image.from_dockerfile(dockerfile).run_commands(*SANDBOX_AGENT_INSTALL_CMDS)


def _get_daytona_snapshot() -> str | None:
    """Return the Daytona snapshot name for new sandboxes.

    Defaults to DEFAULT_DAYTONA_SNAPSHOT. Set DAYTONA_SNAPSHOT to override, or to
    empty /0 / false / image to use the legacy image/Dockerfile path instead.
    """
    raw = os.environ.get("DAYTONA_SNAPSHOT")
    if raw is None:
        return DEFAULT_DAYTONA_SNAPSHOT
    s = raw.strip()
    if not s or s.lower() in ("0", "false", "image"):
        return None
    return s


async def create_daytona(agent_type: str = "claude", dockerfile: str | None = None) -> ProviderInstance:
    """Create a Daytona sandbox with sandbox-agent pre-installed."""
    try:
        from daytona_sdk import (
            Daytona,
            DaytonaConfig,
            CreateSandboxFromImageParams,
            CreateSandboxFromSnapshotParams,
        )
    except ImportError:
        raise RuntimeError("daytona-sdk not installed. Run: pip install daytona-sdk")

    api_key = os.environ.get("DAYTONA_API_KEY")
    if not api_key:
        raise RuntimeError("DAYTONA_API_KEY not set")

    # Collect credentials and config to inject into sandbox
    env_vars = _get_sandbox_env_vars()

    loop = asyncio.get_running_loop()
    daytona = Daytona(DaytonaConfig(api_key=api_key))
    snapshot = _get_daytona_snapshot()
    if snapshot:
        if dockerfile is not None:
            log.info("using Daytona snapshot %s; ignoring dockerfile %s", snapshot, dockerfile)
        create_params = CreateSandboxFromSnapshotParams(
            snapshot=snapshot,
            auto_stop_interval=0,
            env_vars=env_vars,
        )
        create_timeout = 60
    else:
        image = _build_daytona_image(dockerfile)
        create_params = CreateSandboxFromImageParams(
            image=image,
            auto_stop_interval=0,
            env_vars=env_vars,
        )
        # Custom images need longer timeout for build + agent install.
        create_timeout = 300 if dockerfile else 60

    sandbox = await loop.run_in_executor(None, lambda: daytona.create(
        create_params,
        timeout=create_timeout,
    ))

    # For custom images, install agent processes at runtime (CDN unreachable during build).
    # Snapshot-based sandboxes may also need the agent installed even if the snapshot
    # already contains sandbox-agent itself.
    if dockerfile is not None or snapshot is not None:
        await loop.run_in_executor(None, lambda: sandbox.process.exec(
            f"sandbox-agent install-agent {agent_type}"
        ))

    # Start sandbox-agent server inside
    await loop.run_in_executor(None, lambda: sandbox.process.exec(
        f"nohup sandbox-agent server --no-token --host 0.0.0.0 --port {SANDBOX_AGENT_PORT} >/dev/null 2>&1 &"
    ))

    # Wait for server to be ready
    await asyncio.sleep(3)

    # Get signed preview URL (max 24h per Daytona API limit)
    signed = await loop.run_in_executor(None, lambda: sandbox.create_signed_preview_url(SANDBOX_AGENT_PORT, 24 * 3600))
    url = signed.url

    if not await _wait_for_health(url, max_retries=20, interval=1):
        raise RuntimeError(f"sandbox-agent in Daytona sandbox {sandbox.id} failed health check")

    log.info("daytona sandbox-agent ready: %s (sandbox %s)", url[:50], sandbox.id[:16])
    return ProviderInstance(provider="daytona", url=url, sandbox_id=sandbox.id)


def _get_daytona_client():
    """Get a Daytona SDK client. Raises ImportError or RuntimeError on failure."""
    from daytona_sdk import Daytona, DaytonaConfig
    api_key = os.environ.get("DAYTONA_API_KEY")
    if not api_key:
        raise RuntimeError("DAYTONA_API_KEY not set")
    return Daytona(DaytonaConfig(api_key=api_key))


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


# ── Docker provider ──

_DOCKER_INTERNAL_PORT = 2468  # matches the default image's exposed port


async def _build_docker_image(dockerfile: str, tag: str, docker: str = "docker") -> str:
    """Build a Docker image from a Dockerfile, injecting sandbox-agent install commands."""
    if not Path(dockerfile).exists():
        raise FileNotFoundError(f"Dockerfile not found: {dockerfile}")

    context_dir = str(Path(dockerfile).parent)
    original = Path(dockerfile).read_text()

    # Append sandbox-agent install commands
    injected_lines = [original.rstrip()]
    for cmd in SANDBOX_AGENT_INSTALL_CMDS:
        injected_lines.append(f'RUN {cmd}')
    augmented = "\n".join(injected_lines) + "\n"

    # Write to temp file, build, clean up
    tmp_path = Path(dockerfile).parent / f".sandbox_agent_tmp_{uuid.uuid4().hex}.dockerfile"
    tmp_path.write_text(augmented)
    try:
        proc = await asyncio.create_subprocess_exec(
            docker, "build", "-t", tag, "-f", str(tmp_path), context_dir,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await proc.communicate()
        if proc.returncode != 0:
            raise RuntimeError(f"docker build failed:\n{stderr.decode()[:500]}")
    finally:
        tmp_path.unlink(missing_ok=True)

    return tag


async def create_docker(agent_type: str = "claude", dockerfile: str | None = None) -> ProviderInstance:
    """Run a local Docker container with sandbox-agent."""
    docker = shutil.which("docker")
    if not docker:
        raise RuntimeError("docker binary not found. Install Docker: https://docs.docker.com/get-docker/")

    port = await _find_free_port()

    try:
        if dockerfile is not None:
            image = f"sandbox-agent-custom:{uuid.uuid4().hex[:8]}"
            image = await _build_docker_image(dockerfile, image, docker=docker)
        else:
            image = SANDBOX_AGENT_IMAGE

        # Collect env vars to inject
        env_vars = _get_sandbox_env_vars()
        env_args: list[str] = []
        for k, v in env_vars.items():
            env_args += ["-e", f"{k}={v}"]

        cmd = [
            docker, "run", "-d", "--rm",
            "-p", f"{port}:{_DOCKER_INTERNAL_PORT}",
            *env_args,
            image,
            "server", "--no-token", "--host", "0.0.0.0", "--port", str(_DOCKER_INTERNAL_PORT),
        ]

        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await proc.communicate()
        if proc.returncode != 0:
            raise RuntimeError(f"docker run failed: {stderr.decode().strip()}")

        container_id = stdout.decode().strip()

        if dockerfile is not None:
            # Install agent processes at runtime
            install_proc = await asyncio.create_subprocess_exec(
                docker, "exec", container_id, "sandbox-agent", "install-agent", agent_type,
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.PIPE,
            )
            _, install_err = await install_proc.communicate()
            if install_proc.returncode != 0:
                log.warning("agent install in docker failed: %s", install_err.decode().strip())

        url = f"http://localhost:{port}"
        if not await _wait_for_health(url):
            rm_proc = await asyncio.create_subprocess_exec(
                docker, "rm", "-f", container_id,
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.DEVNULL,
            )
            await rm_proc.wait()
            raise RuntimeError(f"sandbox-agent in Docker container failed to start on port {port}")

        log.info("docker sandbox-agent started on port %d (container %s)", port, container_id[:12])
        return ProviderInstance(provider="docker", url=url, port=port, container_id=container_id)

    except BaseException:
        async with _port_lock:
            _freed_ports.append(port)
        raise


async def destroy_docker(instance: ProviderInstance) -> None:
    """Stop and remove a Docker container running sandbox-agent."""
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


# ── Dispatch ──

async def create_instance(provider: str, agent_type: str = "claude", dockerfile: str | None = None) -> ProviderInstance:
    """Create a sandbox-agent instance using the specified provider."""
    if provider == "local":
        return await create_local(agent_type)
    elif provider == "daytona":
        return await create_daytona(agent_type, dockerfile=dockerfile)
    elif provider == "docker":
        return await create_docker(agent_type, dockerfile=dockerfile)
    else:
        raise ValueError(f"Unknown provider: {provider!r}. Use 'local', 'docker', or 'daytona'.")


async def destroy_instance(instance: ProviderInstance) -> None:
    """Destroy a sandbox-agent instance."""
    if instance.provider == "local":
        await destroy_local(instance)
    elif instance.provider == "daytona":
        await destroy_daytona(instance)
    elif instance.provider == "docker":
        await destroy_docker(instance)
    else:
        log.warning("unknown provider %r, skipping cleanup", instance.provider)


async def stop_instance(instance: ProviderInstance) -> None:
    """Stop a sandbox-agent instance (resumable). For local/docker, same as destroy."""
    if instance.provider == "local":
        await destroy_local(instance)
    elif instance.provider == "daytona":
        await stop_daytona(instance)
    elif instance.provider == "docker":
        await destroy_docker(instance)
    else:
        log.warning("unknown provider %r, skipping stop", instance.provider)
