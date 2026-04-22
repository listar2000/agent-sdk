"""Shared constants, types, and helpers used by all provider modules.

This module exists to break the potential circular import between
providers/__init__.py and providers/daytona.py: __init__.py re-exports
everything from here, and daytona.py imports from here directly.
"""

import asyncio
import logging
import shlex
from dataclasses import dataclass

import httpx

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Provider constants
# ---------------------------------------------------------------------------

PORT_BASED_PROVIDERS = frozenset({"local", "docker"})

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

# Remote supervisor constants (also used by daytona.py)
_SUPERVISOR_REMOTE_PORT = 9100


# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# ACP helpers
# ---------------------------------------------------------------------------

def _acp_bin_name(agent_type: str) -> str:
    try:
        return _ACP_BIN_NAMES[agent_type]
    except KeyError:
        raise ValueError(f"unsupported agent_type for supervisor path: {agent_type!r}")


def _acp_launch_args(agent_type: str) -> list[str]:
    return list(_ACP_LAUNCH_ARGS.get(agent_type, []))


# ---------------------------------------------------------------------------
# Env helpers
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Health check
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Port allocator
# ---------------------------------------------------------------------------

_next_local_port = 2469
_freed_ports: list[int] = []
_port_lock = asyncio.Lock()

_sandbox_port_counters: dict[str, int] = {}
_sandbox_freed_ports: dict[str, list[int]] = {}


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


# ---------------------------------------------------------------------------
# Volume mounts
# ---------------------------------------------------------------------------

def _build_volume_mounts(volume_id: str | None, subpath: str | None):
    """Build the VolumeMount list for a Daytona sandbox. Returns None if no volume.

    Per-session sandboxes (subpath is a non-empty string) get three mounts:
      - /home/daytona  → volume subpath (agent's home directory)
      - /mnt/shared    → volume shared/ (cross-session shared data)
      - /opt/supervisor → volume system/supervisor/ (pre-installed supervisor)

    Utility sandboxes (subpath is None or empty string) get a single whole-volume
    mount at /v. This avoids the supervisor mount failing before system/supervisor/
    has been created.

    NOTE: Daytona SDK 0.168 does not support read_only on VolumeMount, so the
    spec's /mnt/shared read-only mount is deferred until the SDK adds that field.
    """
    if not volume_id:
        return None
    from daytona_sdk import VolumeMount
    if not subpath:
        # Utility sandbox: whole-volume mount so we can inspect/create any dir.
        return [VolumeMount(volume_id=volume_id, mount_path="/v")]
    # Regular per-session sandbox: three named mounts.
    return [
        VolumeMount(volume_id=volume_id, mount_path="/home/daytona", subpath=subpath),
        VolumeMount(volume_id=volume_id, mount_path="/mnt/shared", subpath="shared"),
        VolumeMount(volume_id=volume_id, mount_path="/opt/supervisor", subpath="system/supervisor"),
    ]


# ---------------------------------------------------------------------------
# Exec helpers
# ---------------------------------------------------------------------------

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
