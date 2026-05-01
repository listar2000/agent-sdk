"""Shared constants, types, and helpers used by all provider modules.

This module exists to break the potential circular import between
providers/__init__.py and providers/daytona.py: __init__.py re-exports
everything from here, and daytona.py imports from here directly.
"""

import asyncio
import logging
import os
import re
import shlex
import socket
from dataclasses import dataclass
from typing import TYPE_CHECKING

import httpx

if TYPE_CHECKING:
    from ..models import Provider

log = logging.getLogger(__name__)

# POSIX env var name: letter/underscore followed by letters/digits/underscores.
# Validated at _build_env_prefix() to prevent shell injection via spawn_env keys
# when providers (daytona, docker) interpolate them into ``sh -c`` commands.
_ENV_KEY_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

# ---------------------------------------------------------------------------
# Provider constants
# ---------------------------------------------------------------------------

# Providers whose recovery model is "reprovision via provision_sandbox" rather
# than "restart supervisor inside an existing sandbox" (daytona's model).
# local/docker reach the supervisor on localhost:<port>; modal reaches it via
# an HTTPS tunnel; all three are recreated from scratch on miss.
PORT_BASED_PROVIDERS = frozenset({"local", "docker", "modal"})

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


# Per-provider "where the agent's persistent HOME lives". For docker this
# is a volume mount (POSIX-real, append-safe). For daytona this is a local
# ext4 directory inside the sandbox; the supervisor restores it from the
# volume snapshot at startup and writes back after each turn, so the hot
# filesystem never touches mountpoint-s3. /tmp is explicitly NOT the default
# for local — the host filesystem is persistent anyway and tests expect
# the volume path.
_PROVIDER_VOLUME_HOME: dict[str, str] = {
    "daytona": "/home/daytona",
    "docker": "/home/agent",
    # Modal mounts the whole volume at /v and the sandbox's pre-start shell
    # symlinks /home/agent -> /v/agents/<subpath>, mirroring Docker's layout
    # so downstream code can treat the two providers identically.
    "modal": "/home/agent",
}


def default_cwd_for_provider(provider: str) -> str:
    """Persistent default cwd / HOME for the given provider.

    Returns the volume mount point where ~/.claude etc. will live for
    daytona/docker. Falls back to "/tmp" for local (the host filesystem is
    not ephemeral in the container sense) and for unknown providers.
    """
    return _PROVIDER_VOLUME_HOME.get(provider, "/tmp")


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------

class SandboxMissingError(Exception):
    """Raised when a provider cannot find the sandbox (deleted out-of-band).

    Distinct from "stopped" (recoverable by start). Callers should treat this
    as "the sandbox record is stale; provision a new sandbox on the same
    volume" rather than retry.
    """


class VolumeFileExistsError(FileExistsError):
    """Raised when an atomic no-overwrite volume rename hits an existing dst."""

    def __init__(self, path: str):
        super().__init__(path)
        self.path = path


# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------

@dataclass
class ProviderInstance:
    """A running ACP supervisor instance."""
    provider: "Provider"       # "local" | "docker" | "daytona" | "modal"
    url: str                   # http:// base URL
    root: str = "/tmp"         # filesystem root for the sandbox
    sandbox_ref: str | None = None  # provider's opaque ref (Daytona id, docker container id, local "local-<hex>")
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


def build_supervisor_argv(
    *,
    supervisor_js: str,
    acp_bin: str,
    acp_launch_args: list[str],
    port: int,
    root: str,
    host: str = "0.0.0.0",
    snapshot_path: str | None = None,
    quote_paths: bool = True,
) -> str:
    """Return the ``node supervisor.js ...`` argv string shared by every
    provider. Callers wrap with their own env prefix, backgrounding, and
    I/O redirection — Daytona prepends ``setsid env`` and appends ``&``,
    Docker uses ``exec`` as the container PID 1.

    ``snapshot_path`` is Daytona-only: it's the path inside the sandbox
    where the workspace tarball is restored from on boot and written to
    after each turn-end. Docker and local providers leave this unset —
    their volumes are POSIX-real and don't need the snapshot round-trip.

    ``quote_paths=False`` is for Daytona, whose paths are constants
    controlled by this package (no shell-metacharacter risk) and which
    built its command without quoting before the helper existed.
    """
    q = shlex.quote if quote_paths else (lambda s: s)
    acp_flags = "".join(f" --acp-arg {shlex.quote(a)}" for a in acp_launch_args)
    snapshot_flag = f" --snapshot-path {q(snapshot_path)}" if snapshot_path else ""
    return (
        f"node {q(supervisor_js)} "
        f"--host {host} --port {port} "
        f"--acp {q(acp_bin)}{acp_flags} "
        f"--root {q(root)}{snapshot_flag}"
    )


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

    SECURITY: env *values* are shlex-quoted, but env *keys* are interpolated
    unquoted on the ``K=V`` side — the ``=`` is syntactic and splitting on it
    would break the shell form. We therefore reject any key that isn't a
    POSIX env var name (``[A-Za-z_][A-Za-z0-9_]*``). Without this guard,
    a key like ``FOO;rm -rf /;BAR`` would escape the ``env`` builtin's
    argument list and execute arbitrary commands inside the sandbox.
    Server-side ingress (``_pop_env_and_secrets``) also applies this filter
    so the ValueError here is defence-in-depth only.
    """
    env_vars = _get_sandbox_env_vars(spawn_env)
    for k in env_vars:
        if not _ENV_KEY_RE.match(k):
            raise ValueError(
                f"invalid env var name {k!r}: must match [A-Za-z_][A-Za-z0-9_]*"
            )
    unset = " ".join(f"-u {shlex.quote(v)}" for v in _auth_vars_to_unset(spawn_env))
    setv = " ".join(f"{k}={shlex.quote(v)}" for k, v in env_vars.items())
    return f"{unset} {setv}".strip()


# ---------------------------------------------------------------------------
# Health check
# ---------------------------------------------------------------------------

async def _wait_for_health(url: str, max_retries: int = 150, interval: float = 0.1) -> bool:
    """Poll /v1/health until 200 or retries exhausted.

    Tight 100ms interval (was 500ms) with proportionally more attempts — node
    supervisors typically come up in 100-300ms, and the old 500ms cadence
    wasted ~400ms per recovery on "just-missed" polling windows. Total
    budget ~15s stays the same.
    """
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


def _port_is_bindable(port: int) -> bool:
    """Return True if ``port`` is currently free to bind on 127.0.0.1.

    Rejects port 0 even though ``bind(("127.0.0.1", 0))`` technically
    succeeds — that's OS-assigned allocation, not "this port is free," and
    callers treat the return value as the port they'll listen on. Letting
    0 through here meant a spurious 0 entry in ``_freed_ports`` would get
    recycled and crash supervisor startup with "health check failed on
    port 0".
    """
    if port <= 0:
        return False
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            s.bind(("127.0.0.1", port))
        except OSError:
            return False
    return True


async def _find_free_port() -> int:
    """Allocate a host port that is currently free at the OS level.

    Keeps the monotonic counter + freed-port recycling for backward compat,
    but each candidate is verified by a bind-and-close probe before return.
    If no candidate binds cleanly within a bounded loop, falls through to
    ``bind(0)`` and lets the OS pick.
    """
    global _next_local_port
    async with _port_lock:
        # Try up to N candidates (recycled + counter) before falling back
        # to OS-assigned. Bounded so we can't spin forever.
        for _ in range(64):
            if _freed_ports:
                candidate = _freed_ports.pop()
            else:
                candidate = _next_local_port
                _next_local_port += 1
            if _port_is_bindable(candidate):
                return candidate
            # Port in use at OS level — drop it, try another.
        # Fallback: let the OS pick any free port.
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind(("127.0.0.1", 0))
            return s.getsockname()[1]


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

def _build_volume_mounts(
    volume_id: str | None,
    subpath: str | None,
    shared_mounts: list[str] | None = None,
):
    """Build the VolumeMount list for a Daytona sandbox. Returns None if no volume.

    Per-session sandbox layout (subpath is a non-empty string like ``agents/<id>``):
      - /vol              → volume subpath (S3-backed; snapshot tarball lives here)
      - /opt/supervisor   → volume system/supervisor/ (pre-installed supervisor)
      - /mnt/<name>       → volume shared/<name>/ (one per entry in shared_mounts)

    The agent's HOME is ``/home/daytona`` — a local ext4 directory created
    by the supervisor at boot — NOT the volume mount. The supervisor
    restores that directory from ``/vol/snapshot.tar`` on startup and
    writes a fresh snapshot after every turn-end. mountpoint-s3 can't
    handle append-only writes (session JSONLs) or POSIX rename, so the
    volume only ever sees single-file full-overwrite PUTs of the
    snapshot tarball.

    Shared mounts are OPT-IN per agent. An agent with ``shared_mounts=[]``
    (the default) sees no /mnt/<name> directories. An agent with
    ``shared_mounts=["projects", "datasets"]`` gets /mnt/projects and
    /mnt/datasets mounted read-write from <volume>/shared/projects and
    <volume>/shared/datasets respectively.

    Utility sandboxes (subpath is None or empty string) get a single
    whole-volume mount at /v. This avoids the supervisor mount failing
    before system/supervisor/ has been created.

    NOTE: Daytona SDK 0.168 does not support read_only on VolumeMount, so
    every shared mount is read-write today. Scope per-mount permissions
    when the SDK exposes that field.
    """
    if not volume_id:
        return None
    from daytona_sdk import VolumeMount
    if not subpath:
        # Utility sandbox: whole-volume mount so we can inspect/create any dir.
        return [VolumeMount(volume_id=volume_id, mount_path="/v")]
    mounts = [
        VolumeMount(volume_id=volume_id, mount_path="/vol", subpath=subpath),
        VolumeMount(volume_id=volume_id, mount_path="/opt/supervisor", subpath="system/supervisor"),
    ]
    for name in (shared_mounts or []):
        # Defense-in-depth: the agent-config API accepts arbitrary strings,
        # so strip separators to prevent an agent from mounting
        # "../agents/<other-id>" at /mnt/anything.
        clean = name.strip("/").replace("..", "").replace("/", "-")
        if not clean:
            continue
        mounts.append(VolumeMount(
            volume_id=volume_id,
            mount_path=f"/mnt/{clean}",
            subpath=f"shared/{clean}",
        ))
    return mounts


# ---------------------------------------------------------------------------
# Exec helpers
# ---------------------------------------------------------------------------

def _truncate(data: bytes, limit: int) -> tuple[str, bool]:
    if len(data) > limit:
        return data[:limit].decode(errors="replace"), True
    return data.decode(errors="replace"), False


# ---------------------------------------------------------------------------
# Path sanitizer (shared across server + providers)
# ---------------------------------------------------------------------------

def _safe_path(ref: str | None, rel_path: str) -> str:
    """Normalize + validate a volume-relative path.

    Strips a leading ``/`` so it never anchors to host root, rejects ``..``
    traversal and NUL/CR/LF control chars. If ``ref`` is provided (local
    provider), realpath-validates that the resolved target stays inside
    ``ref`` — catches symlink escapes.

    Returns the normalized relative path (no leading slash). Raises
    ``ValueError`` on any violation; callers that need HTTP semantics should
    translate to 400.
    """
    p = (rel_path or "").lstrip("/")
    if "\x00" in p or "\n" in p or "\r" in p:
        raise ValueError("invalid control characters in path")
    parts = [seg for seg in p.split("/") if seg not in ("", ".")]
    for seg in parts:
        if seg == "..":
            # Unified message: traversal and realpath-escape both report as
            # "escapes volume root" so callers/tests can match one phrase.
            raise ValueError("path escapes volume root")
    normalized = "/".join(parts)
    if ref is not None and normalized:
        candidate = os.path.realpath(os.path.join(ref, normalized))
        root_real = os.path.realpath(ref)
        if candidate != root_real and not candidate.startswith(root_real + os.sep):
            raise ValueError("path escapes volume root")
    return normalized


def normalize_find_output(raw: str) -> str:
    """Normalize the output of ``find -printf '%y %P\\n'`` to the unified tree format.

    Each non-empty line of *raw* must be ``"<type> <relpath>"`` where type is
    one of ``d``/``f``/``l``. ``%P`` gives the path relative to the find root,
    so no ``/v/`` stripping is needed. Output: one path per line, directories
    end with ``/``, files do not, sorted.
    """
    entries: set[str] = set()
    for line in (raw or "").splitlines():
        line = line.rstrip("\r")
        if not line:
            continue
        parts = line.split(" ", 1)
        if len(parts) != 2:
            continue
        type_char, path = parts
        path = path.strip().lstrip("/")
        if not path:
            continue  # the find root itself — skip
        if type_char == "d":
            entries.add(path.rstrip("/") + "/")
        elif type_char in ("f", "l"):
            entries.add(path.rstrip("/"))
        # other types (c, b, p, s) ignored
    return "\n".join(sorted(entries))


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
