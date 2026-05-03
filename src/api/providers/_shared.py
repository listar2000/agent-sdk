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
PORT_BASED_PROVIDERS = frozenset({"unix_local", "docker", "modal"})

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
    provider: "Provider"       # "unix_local" | "docker" | "daytona" | "modal"
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


# ---------------------------------------------------------------------------
# Runtime path resolution
#
# The agent-sdk runtime (``supervisor.js`` + per-agent-type ACP binaries) is
# baked into the Docker image at ``/opt/agent-sdk/runtime/`` (see ``Dockerfile``)
# so providers no longer install it onto user volumes at runtime. ``Volumes``
# carry only user data; the runtime is pinned to the agent-sdk version.
#
# When running the server from source (no image), the helper falls back to
# ``<repo>/src/supervisor`` which the developer pre-populates with
# ``npm --prefix src/supervisor install``. ``scripts/launch_server_local.sh``
# does this automatically.
#
# Resolution order:
#   1. ``$AGENT_SDK_RUNTIME_PATH`` set → use it as-is (no existence check;
#      callers fail loudly with a clear error if the contents are wrong).
#   2. ``/opt/agent-sdk/runtime`` exists on disk → we're in the image.
#   3. ``<repo>/src/supervisor/node_modules/.bin/claude-agent-acp`` exists
#      → we're running from source.
#   4. Raise ``RuntimeError`` with the remediation command.
# ---------------------------------------------------------------------------

# Repo root: providers/ → api/ → src/ → repo (mirrors local.py:38)
_REPO_ROOT_FROM_SHARED = os.path.realpath(
    os.path.join(os.path.dirname(__file__), "..", "..", "..")
)
_IMAGE_RUNTIME_PATH = "/opt/agent-sdk/runtime"
_SOURCE_RUNTIME_PATH = os.path.join(_REPO_ROOT_FROM_SHARED, "src", "supervisor")
_SOURCE_RUNTIME_SENTINEL = os.path.join(
    _SOURCE_RUNTIME_PATH, "node_modules", ".bin", "claude-agent-acp",
)


def _detect_runtime_path() -> str:
    """Return the absolute path to the agent-sdk runtime directory.

    The directory must contain ``supervisor.js`` and a ``node_modules/.bin/``
    populated with every ACP binary in ``_ACP_NPM_SPECS``. Existence of those
    contents is NOT validated here — callers (each provider's
    ``create_sandbox``) raise with a precise file path when something is
    missing, which is more actionable than a generic "runtime not found".

    Pure: no I/O after the env-var fast path except up to two ``os.path.exists``
    calls, both on local FS.
    """
    explicit = os.environ.get("AGENT_SDK_RUNTIME_PATH")
    if explicit:
        return explicit
    if os.path.isdir(_IMAGE_RUNTIME_PATH):
        return _IMAGE_RUNTIME_PATH
    if os.path.exists(_SOURCE_RUNTIME_SENTINEL):
        return _SOURCE_RUNTIME_PATH
    raise RuntimeError(
        f"agent-sdk runtime not found. Looked at "
        f"$AGENT_SDK_RUNTIME_PATH (unset), {_IMAGE_RUNTIME_PATH} (missing), "
        f"and source-tree fallback {_SOURCE_RUNTIME_PATH} (missing "
        f"sentinel {_SOURCE_RUNTIME_SENTINEL}). "
        f"Run `npm --prefix src/supervisor install` for source-tree dev "
        f"or set AGENT_SDK_RUNTIME_PATH to a populated runtime directory."
    )


def _runtime_supervisor_js() -> str:
    """Path to ``supervisor.js`` inside the resolved runtime directory."""
    return os.path.join(_detect_runtime_path(), "supervisor.js")


def _runtime_acp_bin_relative(agent_type: str) -> str:
    """Path to the per-agent-type ACP binary RELATIVE to the runtime root.

    Resolves through ``node_modules/<pkg>/package.json#bin`` rather than
    through ``node_modules/.bin/<name>`` because Daytona's snapshot
    image-build flattens symlinks under ``node_modules/.bin/`` to 0-byte
    regular files (the underlying scripts stay intact). Going via
    package.json gives us the real path either way.

    Reads the SERVER's local runtime ``package.json`` to determine the
    relative path. This is correct as long as the server's runtime layout
    matches the sandbox's runtime layout — which it does, because both
    come from the same Docker image (or both run from ``<repo>/src/supervisor``
    in source-tree dev). The returned path is a relative POSIX path the
    caller prepends to its own sandbox-side runtime root.

    Only valid for agent_types in ``_ACP_NPM_SPECS``; non-npm agents
    (``goose``, ``openhands``) resolve via PATH and should not call this
    helper.
    """
    runtime = _detect_runtime_path()
    bin_name = _acp_bin_name(agent_type)
    spec = _ACP_NPM_SPECS[agent_type]
    pkg_name = _spec_package_name(spec)
    pkg_dir = os.path.join(runtime, "node_modules", pkg_name)
    pkg_json_path = os.path.join(pkg_dir, "package.json")
    if not os.path.exists(pkg_json_path):
        # Fall back to the legacy .bin/<name> path so callers' existence
        # checks raise a useful error rather than a confusing
        # FileNotFoundError from the package.json read.
        return os.path.join("node_modules", ".bin", bin_name)
    import json
    with open(pkg_json_path) as f:
        pkg = json.load(f)
    bin_field = pkg.get("bin")
    if isinstance(bin_field, str):
        rel = bin_field
    elif isinstance(bin_field, dict):
        rel = bin_field.get(bin_name)
        if rel is None:
            if len(bin_field) == 1:
                rel = next(iter(bin_field.values()))
            else:
                raise RuntimeError(
                    f"runtime ACP package {pkg_name!r} has bin entries "
                    f"{list(bin_field)} but none matches {bin_name!r}"
                )
    else:
        raise RuntimeError(
            f"runtime ACP package {pkg_name!r} has no bin field in package.json"
        )
    return os.path.join("node_modules", pkg_name, rel)


def _runtime_acp_bin(agent_type: str) -> str:
    """Absolute path to the per-agent-type ACP binary on the SERVER's
    filesystem (i.e. resolved against the server-host's runtime path).
    Used by the local provider where server == sandbox host."""
    return os.path.join(_detect_runtime_path(), _runtime_acp_bin_relative(agent_type))


def _spec_package_name(spec: str) -> str:
    """Strip the ``@<version>`` suffix from an npm spec, preserving any
    leading ``@scope/``. E.g.::

        @agentclientprotocol/claude-agent-acp@^0.27.0 → @agentclientprotocol/claude-agent-acp
        opencode-ai@^1.4.3                            → opencode-ai
    """
    # Find the LAST ``@`` whose position is > 0 (so the leading ``@`` of a
    # scoped package isn't mistaken for the version separator).
    if "@" not in spec[1:]:
        return spec
    at_idx = spec.rfind("@")
    if at_idx <= 0:
        return spec
    return spec[:at_idx]


# ``_use_image_runtime()`` was deleted in Phase E of
# the runtime-image-unification refactor. The image-runtime path is now
# unconditional (the only path); ``AGENT_SDK_USE_IMAGE_RUNTIME=0`` is no
# longer honoured.


def _read_runtime_image_tag() -> str | None:
    """Read ``.runtime-image-tag`` from the repo root.

    Written by ``scripts/release.sh`` after each successful image build.
    Used by the daytona / docker / modal providers as the default container
    image when no per-environment ``*_IMAGE`` env var is set. Returns
    ``None`` if the file is absent or empty (typical for fresh dev
    checkouts that haven't released yet).
    """
    return _read_repo_tag(".runtime-image-tag")


def _read_runtime_snapshot_tag() -> str | None:
    """Read ``.runtime-snapshot-tag`` from the repo root.

    Written by ``scripts/release.sh`` after a successful Daytona snapshot
    register. Lets ``provision_daytona_sandbox`` default ``DAYTONA_SNAPSHOT``
    without requiring per-environment env-var setup, and lets launch
    scripts pick up a known-good snapshot for free.
    """
    return _read_repo_tag(".runtime-snapshot-tag")


def _read_repo_tag(filename: str) -> str | None:
    path = os.path.join(_REPO_ROOT_FROM_SHARED, filename)
    if not os.path.exists(path):
        return None
    with open(path) as f:
        tag = f.read().strip()
    return tag or None


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


async def _supervisor_health_status(
    url: str, *, timeout: float = 2.0,
) -> tuple[bool, int | None]:
    """Probe ``GET {url}/v1/health`` and report whether the supervisor's
    inner ACP child is alive — not just whether the supervisor itself is
    reachable.

    Returns ``(alive, status_code_or_None)``:
      * ``(True, 200)``  — 200 with ``acp_alive`` true (or absent for older
        supervisors that don't yet emit the field).
      * ``(False, 200)`` — 200 with ``acp_alive: false``. This is the
        "ghost supervisor" case: HTTP up, child dead, messages pile up
        unread. Caller MUST treat as dead and cold-recover.
      * ``(False, <status>)`` — any non-200 status.
      * ``(False, None)`` — connection error / timeout.

    The signature mirrors what daytona's two-layer probe needs (status
    code distinguishes 4xx-stable from 5xx/connection layer-2 retry).
    Other providers ignore the status code and just take the bool.
    """
    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            resp = await client.get(f"{url}/v1/health")
    except Exception:
        return False, None
    if resp.status_code != 200:
        return False, resp.status_code
    # 200: trust the body's acp_alive verdict. Absent / non-bool → trust the
    # 200 (legacy supervisor or malformed body — supervisor is reachable).
    try:
        body = resp.json()
    except Exception:
        return True, 200
    acp_alive = body.get("acp_alive") if isinstance(body, dict) else None
    if acp_alive is False:
        return False, 200
    return True, 200


# ---------------------------------------------------------------------------
# Port allocator
# ---------------------------------------------------------------------------

# ``_freed_ports`` was a recycle pool when ports came from a per-process
# monotonic counter. It's now vestigial — the OS allocator never re-issues
# the same ephemeral port back-to-back, so recycling has no value. Kept as
# a still-existing-but-unread list so existing call sites that append on
# error paths don't need rewiring; entries are never consumed.
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
    """Allocate a host port that is currently free at the OS level.

    Always asks the OS via ``bind(("127.0.0.1", 0))`` rather than walking
    a process-local counter. The monotonic-counter approach was unsafe
    under pytest-xdist: each worker is its own Python process, so two
    workers would both pick e.g. ``2469``, both bind-probe successfully
    (the probe doesn't reserve), and both try to bind for real — one
    won, the other got "address already in use". The OS allocator hands
    out ports from the ephemeral range (32768+) and won't double-issue
    across processes.

    There's a tiny TOCTOU window between this function's
    ``getsockname()`` and the caller's actual ``bind`` (since we close
    the socket before returning), but two callers in the same process
    are serialised by ``_port_lock`` and across processes the kernel
    won't hand out the same ephemeral twice in close succession.
    Callers that hit a "port in use" race retry once in
    ``docker.create_sandbox``.
    """
    async with _port_lock:
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
    # the runtime-image-unification refactor: the supervisor lives
    # in the image at /opt/agent-sdk/runtime, never on the volume. Volume
    # is data-only — only /vol is mounted (plus any opt-in shared mounts).
    mounts = [
        VolumeMount(volume_id=volume_id, mount_path="/vol", subpath=subpath),
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
