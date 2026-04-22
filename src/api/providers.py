"""ACP supervisor provider management — local, docker, daytona."""

import asyncio
import logging
import os
import shlex
import signal
import shutil
import uuid
from dataclasses import dataclass
from pathlib import Path

import httpx

from . import load_dotenv

log = logging.getLogger(__name__)

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


# Auth/credential env vars that the server MUST NOT leak into sandboxes via
# its own environment. When a sandbox spawns a supervisor, any of these keys
# not explicitly provided by the caller are stripped or unset — no fallback
# to ambient server credentials.
AUTH_KEYS = frozenset({
    "ANTHROPIC_API_KEY",
    "CLAUDE_CODE_OAUTH_TOKEN",
    "OPENAI_API_KEY",
    "CLAUDE_CODE_USE_VERTEX",
    "ANTHROPIC_VERTEX_BASE_URL",
    "ANTHROPIC_VERTEX_PROJECT_ID",
    "CLAUDE_CODE_SKIP_VERTEX_AUTH",
    "GOOGLE_APPLICATION_CREDENTIALS",
    "GOOGLE_API_KEY",
})


def _get_sandbox_env_vars(spawn_env: dict[str, str] | None = None) -> dict[str, str]:
    """Return the env to inject into a sandbox/supervisor.

    No server-ambient fallback: the server never injects its own API keys or
    auth config. Only IS_SANDBOX=1 plus whatever the caller supplied in
    ``spawn_env`` (which comes from merging agent.env + session.env + secrets).
    """
    env: dict[str, str] = {"IS_SANDBOX": "1"}
    if spawn_env:
        env.update(spawn_env)
    return env


def _auth_vars_to_unset(spawn_env: dict[str, str] | None) -> list[str]:
    """Auth-related env vars that must be explicitly unset in the spawned
    supervisor's env. Covers the case where a Daytona snapshot or Docker image
    has credentials baked in at build time — even though the server doesn't
    inject ambient creds, the sandbox itself might already have them.
    We unset every known auth key the caller didn't explicitly provide."""
    provided = set(spawn_env.keys()) if spawn_env else set()
    return [k for k in AUTH_KEYS if k not in provided]


def _build_env_prefix(spawn_env: dict[str, str] | None) -> str:
    """Argv for ``env`` that unsets baked-in auth keys and sets spawn vars.

    Returns a shlex-quoted string like ``-u K1 -u K2 X=v Y=w`` suitable for
    appending after ``env`` (or ``setsid env``) in a shell command.
    """
    env_vars = _get_sandbox_env_vars(spawn_env)
    unset = " ".join(f"-u {shlex.quote(v)}" for v in _auth_vars_to_unset(spawn_env))
    setv = " ".join(f"{k}={shlex.quote(v)}" for k, v in env_vars.items())
    return f"{unset} {setv}".strip()


@dataclass
class ProviderInstance:
    """A running ACP supervisor instance."""
    provider: str              # "local", "daytona", or "docker"
    url: str                   # http:// base URL
    root: str = "/tmp"         # filesystem root for the sandbox
    sandbox_id: str | None = None  # Daytona sandbox ID (if daytona)
    process: asyncio.subprocess.Process | None = None  # local subprocess
    port: int | None = 0       # local port (if local or docker)
    container_id: str | None = None  # Docker container ID (if docker)


# ── Supervisor provider (direct stdio to {claude,codex}-agent-acp) ──

_SUPERVISOR_DIR = Path(__file__).resolve().parent.parent / "supervisor"
_supervisor_deps_lock = asyncio.Lock()
_supervisor_deps_ready = False

_ACP_BIN_NAMES = {
    "claude": "claude-agent-acp",
    "codex": "codex-acp",
    "opencode": "opencode",
    "gemini": "gemini",
    "cline": "cline-acp",
    "deepagents": "deepagents-acp",
    "openhands": "openhands",
    "goose": "goose",
}
_ACP_NPM_SPECS = {
    "claude": "@agentclientprotocol/claude-agent-acp@^0.27.0",
    "codex": "@zed-industries/codex-acp@^0.11.1",
    "opencode": "opencode-ai@^1.4.3",
    "gemini": "@google/gemini-cli@^0.37.2",
    "cline": "cline-acp@^0.1.6",
    "deepagents": "deepagents-acp@^0.1.8",
}
_ACP_LAUNCH_ARGS: dict[str, list[str]] = {
    "opencode": ["acp"],
    "gemini": ["--acp"],
    "openhands": ["acp"],
    "goose": ["acp"],
}


def _acp_bin_name(agent_type: str) -> str:
    try:
        return _ACP_BIN_NAMES[agent_type]
    except KeyError:
        raise ValueError(f"unsupported agent_type for supervisor path: {agent_type!r}")


def _acp_launch_args(agent_type: str) -> list[str]:
    return list(_ACP_LAUNCH_ARGS.get(agent_type, []))


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


# ---------------------------------------------------------------------------
# Per-sandbox port allocation (for multiple supervisors in one sandbox)
# ---------------------------------------------------------------------------
_sandbox_port_counters: dict[str, int] = {}
_sandbox_freed_ports: dict[str, list[int]] = {}

def allocate_sandbox_port(sandbox_id: str) -> int:
    """Allocate a port for a new supervisor inside an existing sandbox."""
    freed = _sandbox_freed_ports.get(sandbox_id)
    if freed:
        return freed.pop()
    port = _sandbox_port_counters.get(sandbox_id, _SUPERVISOR_REMOTE_PORT)
    _sandbox_port_counters[sandbox_id] = port + 1
    return port

def free_sandbox_port(sandbox_id: str, port: int) -> None:
    """Return a port to the pool when a supervisor is shut down."""
    _sandbox_freed_ports.setdefault(sandbox_id, []).append(port)


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


# ── Daytona provider ──

_SUPERVISOR_REMOTE_DIR = "/tmp/agent-sdk-sup"
_SUPERVISOR_REMOTE_PORT = 9100


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

    Deps (node, npm, ACP binary) must already be installed.
    Returns the signed preview URL for this supervisor.
    """
    bin_name = _acp_bin_name(agent_type)
    loop = asyncio.get_running_loop()

    def _exec(cmd: str, timeout: int = 120) -> str:
        r = sandbox.process.exec(cmd, timeout=timeout)
        return (r.result if hasattr(r, "result") else str(r)) or ""

    acp_bin = f"{_SUPERVISOR_REMOTE_DIR}/node_modules/.bin/{bin_name}"
    launch_args = _acp_launch_args(agent_type)
    acp_arg_flags = "".join(f" --acp-arg {shlex.quote(a)}" for a in launch_args)
    env_prefix = _build_env_prefix(spawn_env)
    log_file = f"{_SUPERVISOR_REMOTE_DIR}/sup-{port}.log"
    inner = (
        f"cd {_SUPERVISOR_REMOTE_DIR} && "
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

    log.info("supervisor on port %d ready: %s (sandbox %s)", port, url[:60], sandbox.id[:16])
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
    """Create a Daytona sandbox and install deps, but do NOT start a supervisor.

    Returns a ProviderInstance with sandbox_id but no usable supervisor URL.
    Supervisors are started per-session via start_supervisor_in_sandbox().
    """
    try:
        from daytona_sdk import (
            Daytona, DaytonaConfig, CreateSandboxFromImageParams,
            CreateSandboxFromSnapshotParams, VolumeMount,
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

    # Build volumes list if volume_id + subpath are provided.
    # NOTE: Daytona SDK 0.168 does not support read_only on VolumeMount, so the
    # spec's /mnt/shared read-only mount is deferred until the SDK adds that field.
    volumes = None
    if volume_id and subpath:
        volumes = [VolumeMount(
            volume_id=volume_id,
            mount_path="/home/daytona",
            subpath=subpath,
        )]

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
        # Install deps only (no supervisor start)
        bin_name = _acp_bin_name(agent_type)
        npm_spec = _ACP_NPM_SPECS[agent_type]

        def _exec(cmd: str, timeout: int = 120) -> str:
            r = sandbox.process.exec(cmd, timeout=timeout)
            return (r.result if hasattr(r, "result") else str(r)) or ""

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

        # Upload supervisor.js
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
            CreateSandboxFromSnapshotParams, VolumeMount,
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

    # Build volumes list if volume_id + subpath are provided.
    # NOTE: Daytona SDK 0.168 does not support read_only on VolumeMount, so the
    # spec's /mnt/shared read-only mount is deferred until the SDK adds that field.
    volumes = None
    if volume_id and subpath:
        volumes = [VolumeMount(
            volume_id=volume_id,
            mount_path="/home/daytona",
            subpath=subpath,
        )]

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


def _get_daytona_client():
    """Get a Daytona SDK client. Raises ImportError or RuntimeError on failure."""
    from daytona_sdk import Daytona, DaytonaConfig
    api_key = os.environ.get("DAYTONA_API_KEY")
    if not api_key:
        raise RuntimeError("DAYTONA_API_KEY not set")
    return Daytona(DaytonaConfig(api_key=api_key))


async def create_daytona_volume(name: str, wait_ready_timeout: int = 120) -> str:
    """Create a Daytona volume and return its provider-native id.

    Polls until the volume reaches the 'ready' state before returning so that
    callers can immediately attach the volume to a new sandbox.
    """
    from daytona_api_client import VolumesApi
    from daytona_api_client.models import VolumeState

    client = _get_daytona_client()
    vol = await asyncio.to_thread(client.volume.create, name)
    vol_id = vol.id

    # Poll until ready (volume creation is async on the Daytona backend).
    volumes_api = VolumesApi(client._api_client)
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

    return vol_id


async def delete_daytona_volume(provider_ref: str) -> None:
    """Delete a Daytona volume by provider-native id (UUID)."""
    from daytona_api_client import VolumesApi
    client = _get_daytona_client()
    volumes_api = VolumesApi(client._api_client)
    await asyncio.to_thread(volumes_api.delete_volume, provider_ref)


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


# ── Dispatch ──

async def create_instance(
    provider: str,
    agent_type: str = "claude",
    dockerfile: str | None = None,
    pre_start_commands: list[str] | None = None,
    root: str = "/tmp",
    spawn_env: dict[str, str] | None = None,
) -> ProviderInstance:
    """Create an ACP supervisor instance using the specified provider.

    pre_start_commands are shell commands to run inside the sandbox BEFORE
    the supervisor process starts (used for skill installation).

    spawn_env is the merged env that should land in the supervisor process:
    {IS_SANDBOX:1} ∪ agent.env ∪ session.env ∪ secrets. The server never
    injects its own API keys — if spawn_env is empty, the supervisor runs
    with no credentials.
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


_MAX_OUTPUT_BYTES = 1_048_576  # 1 MB stdout/stderr cap


@dataclass
class ExecResult:
    stdout: str
    stderr: str
    exit_code: int
    stdout_truncated: bool = False
    stderr_truncated: bool = False
    timed_out: bool = False

    def to_dict(self) -> dict:
        from dataclasses import asdict
        return asdict(self)


def _truncate(data: bytes, limit: int) -> tuple[str, bool]:
    if len(data) > limit:
        return data[:limit].decode(errors="replace"), True
    return data.decode(errors="replace"), False


async def _exec_subprocess(proc, timeout: int) -> ExecResult:
    timed_out = False
    try:
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=timeout)
    except asyncio.TimeoutError:
        proc.kill()
        stdout, stderr = await proc.communicate()
        timed_out = True
    out, out_trunc = _truncate(stdout or b"", _MAX_OUTPUT_BYTES)
    err, err_trunc = _truncate(stderr or b"", _MAX_OUTPUT_BYTES)
    return ExecResult(
        stdout=out, stderr=err,
        exit_code=proc.returncode if proc.returncode is not None else -1,
        stdout_truncated=out_trunc, stderr_truncated=err_trunc,
        timed_out=timed_out,
    )


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
