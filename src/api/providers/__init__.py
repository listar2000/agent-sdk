"""ACP supervisor provider management — daytona, docker, unix_local, modal.

This package splits provider-specific code into sub-modules:
  - daytona.py     — Daytona sandbox management
  - docker.py      — Docker container management
  - unix_local.py  — Host-subprocess management
  - modal.py       — Modal sandbox management
  - _shared.py     — Shared types, constants, helpers

providers/__init__.py:
  - Re-exports the ``_shared`` and ``.daytona`` symbols that server.py
    (and tests) import from ``api.providers``.
  - Provides universal dispatch wrappers (create_instance, destroy_instance,
    exec_in_instance).
  - Provides uniform-API dispatch helpers (create_volume, delete_volume,
    provision_sandbox, reconcile_sandboxes, ensure_supervisor_url, etc.).
"""

import asyncio
import logging
import shutil

from .. import load_dotenv

load_dotenv()

# Re-export shared symbols used by server.py + tests. Internal-only helpers
# (``_build_env_prefix``, ``_port_lock``, ``_find_free_port``, etc.) live in
# ``._shared`` and are imported by provider modules directly — no need to
# expose them at the package level too.
from ._shared import (
    PORT_BASED_PROVIDERS,
    AUTH_KEYS,
    ProviderInstance,
    ExecResult,
    SandboxMissingError,
    VolumeFileExistsError,
    default_cwd_for_provider,
    _ACP_BIN_NAMES,
    _ACP_NPM_SPECS,
    _acp_bin_name,
    _acp_launch_args,
    _get_sandbox_env_vars,
    _wait_for_health,
    allocate_sandbox_port,
    free_sandbox_port,
    _MAX_OUTPUT_BYTES,
    _truncate,
    _exec_subprocess,
    _normalize_workspace,
)

# Re-export Daytona-specific symbols for server.py + tests.
from .daytona import (
    destroy_daytona,
    stop_daytona,
    create_daytona_volume,
    delete_daytona_volume,
    provision_daytona_sandbox,
    restart_daytona_supervisor,
    kill_supervisor_in_sandbox,
    _get_async_daytona_client,
    _daytona_sandbox_op,
)

# Provider module dispatch table
from . import daytona as _daytona_mod
from . import docker as _docker_mod
from . import unix_local as _unix_local_mod
from . import modal as _modal_mod

_PROVIDER_MODS = {
    "daytona": _daytona_mod,
    "docker": _docker_mod,
    "unix_local": _unix_local_mod,
    "modal": _modal_mod,
}


def _dispatch_mod(provider: str):
    """Look up a provider module or raise with a clear error.

    The unix subprocess provider is canonically ``"unix_local"``; the
    legacy ``"local"`` spelling is no longer accepted anywhere.

    Avoids bare ``KeyError('foobar')`` from ``_PROVIDER_MODS[provider]`` in
    a long stack trace — the server's exception handler turns this into a
    500 with a readable message that names the valid providers.
    """
    if provider not in _PROVIDER_MODS:
        raise ValueError(
            f"unknown provider {provider!r}; valid: {sorted(_PROVIDER_MODS)}"
        )
    return _PROVIDER_MODS[provider]


log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Universal dispatch
# ---------------------------------------------------------------------------

async def create_instance(
    provider: str,
    agent_type: str = "opencode",
    dockerfile: str | None = None,
    pre_start_commands: list[str] | None = None,
    root: str = "/tmp",
    spawn_env: dict[str, str] | None = None,
    volume_id: str | None = None,
    subpath: str | None = None,
    sandbox_ref: str | None = None,
    shared_mounts: list[str] | None = None,
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

    sandbox_ref, when provided, is attached as a label/tag on providers
    that support it (Docker label, Modal tag) so ``reconcile_on_startup``
    can cross-reference live containers with the live SessionPool's
    ``sandbox_state.sandbox_ref`` set. Daytona and local ignore it.
    """
    if agent_type not in _ACP_BIN_NAMES:
        raise ValueError(f"unsupported agent_type: {agent_type!r}. Supported: {sorted(_ACP_BIN_NAMES)}")
    # All three provider modules accept the same kwargs (they ignore what
    # they don't use — local/docker/daytona all declare ``dockerfile``,
    # ``pre_start_commands``, ``shared_mounts`` for parity). Dispatch is a
    # single call; _dispatch_mod raises a readable error for unknown providers.
    return await _dispatch_mod(provider).create_sandbox(
        volume_ref=volume_id, subpath=subpath or "",
        agent_type=agent_type, root=root, spawn_env=spawn_env,
        dockerfile=dockerfile, pre_start_commands=pre_start_commands,
        sandbox_ref=sandbox_ref, shared_mounts=shared_mounts,
    )


async def destroy_instance(instance: ProviderInstance) -> None:
    """Destroy a supervisor instance."""
    await _dispatch_mod(instance.provider).destroy_sandbox(instance)


async def exec_in_instance(instance: ProviderInstance, cmd: str, timeout: int = 30) -> ExecResult:
    """Run a shell command in the sandbox environment."""
    if instance.provider == "unix_local":
        proc = await asyncio.create_subprocess_shell(
            cmd,
            cwd=instance.root,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        return await _exec_subprocess(proc, timeout)

    elif instance.provider == "docker":
        docker = shutil.which("docker")
        container_id = instance.container_id or instance.sandbox_ref
        if not docker or not container_id:
            raise RuntimeError("docker not available or no container_id")
        proc = await asyncio.create_subprocess_exec(
            docker, "exec", container_id, "sh", "-c", cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        return await _exec_subprocess(proc, timeout)

    elif instance.provider == "daytona":
        if not instance.sandbox_ref:
            raise RuntimeError("no sandbox_ref for daytona exec")
        from .daytona import _get_async_daytona_client
        daytona = await _get_async_daytona_client()
        sandbox = await daytona.get(instance.sandbox_ref)
        try:
            r = await asyncio.wait_for(
                sandbox.process.exec(cmd, timeout=timeout),
                timeout=timeout + 5,
            )
            out = (r.result if hasattr(r, "result") else str(r)) or ""
            err = (r.stderr if hasattr(r, "stderr") else "") or ""
            code = r.exit_code if hasattr(r, "exit_code") else None
            out, trunc = _truncate(out.encode(), _MAX_OUTPUT_BYTES)
            return ExecResult(
                stdout=out,
                stderr=err,
                exit_code=code,
                stdout_truncated=trunc,
            )
        except asyncio.TimeoutError:
            return ExecResult(stdout="", stderr="", exit_code=-1, timed_out=True)

    elif instance.provider == "modal":
        return await _modal_mod.exec_in_sandbox(instance, cmd, timeout=timeout)

    else:
        raise ValueError(f"exec_in_instance: unsupported provider {instance.provider!r}")


# ---------------------------------------------------------------------------
# Uniform-API dispatch helpers — each forwards to the per-provider function
# of the same name. Per-volume file ops moved to ``BaseVolumeAdapter``
# (use ``get_volume_adapter(provider, ref)``).
# ---------------------------------------------------------------------------

async def create_volume(provider: str, *args, **kwargs):
    return await _dispatch_mod(provider).create_volume(*args, **kwargs)

async def delete_volume(provider: str, *args, **kwargs):
    return await _dispatch_mod(provider).delete_volume(*args, **kwargs)

async def ensure_supervisor_url(provider: str, *args, **kwargs):
    return await _dispatch_mod(provider).ensure_supervisor_url(*args, **kwargs)


# ---------------------------------------------------------------------------
# Volume adapter dispatch — per-provider ``BaseVolumeAdapter`` instances.
# Replaces the ``__getattr__`` magic dispatch for per-volume file ops.
# Lifecycle ops (create_volume / delete_volume) stay on the legacy dispatch.
# ---------------------------------------------------------------------------

from ._volume import BaseVolumeAdapter  # noqa: E402

_VOLUME_ADAPTERS: dict[str, type[BaseVolumeAdapter]] = {}


def _register_volume_adapters() -> None:
    """Lazy-load each provider's volume adapter class. Same lazy pattern
    as ``api.sandbox.factory._register_default_providers`` — first call
    populates the table; subsequent calls are no-ops."""
    if _VOLUME_ADAPTERS:
        return
    from .daytona.volumes import DaytonaVolumeAdapter
    from .docker.volumes import DockerVolumeAdapter
    from .modal.volumes import ModalVolumeAdapter
    from .unix_local.volumes import UnixLocalVolumeAdapter
    _VOLUME_ADAPTERS["daytona"] = DaytonaVolumeAdapter
    _VOLUME_ADAPTERS["docker"] = DockerVolumeAdapter
    _VOLUME_ADAPTERS["modal"] = ModalVolumeAdapter
    _VOLUME_ADAPTERS["unix_local"] = UnixLocalVolumeAdapter


def get_volume_adapter(provider: str, provider_ref: str) -> BaseVolumeAdapter:
    """Construct a per-volume adapter bound to ``provider_ref``.

    Raises ``ValueError`` for unknown providers (same shape as
    ``_dispatch_mod``). During Phase 2 rollout, only providers with a
    registered adapter are wired here; others still go through the
    legacy ``_providers_mod.volume_*`` dispatch.
    """
    _register_volume_adapters()
    cls = _VOLUME_ADAPTERS.get(provider)
    if cls is None:
        raise ValueError(
            f"no volume adapter registered for provider {provider!r}; "
            f"available: {sorted(_VOLUME_ADAPTERS)}"
        )
    return cls(provider_ref)


async def reconcile_sandboxes(provider: str) -> None:
    """Reconcile in-process sandbox state with live provider state on startup.

    Only the Docker provider needs this today: its containers survive
    server restarts and would accumulate as orphans without a scan.
    Daytona sandboxes are managed by the Daytona control plane; local
    sandboxes (subprocess-backed) die with the server process.
    """
    mod = _PROVIDER_MODS.get(provider)
    fn = getattr(mod, "reconcile_on_startup", None)
    if fn is None:
        return
    await fn()


async def provision_sandbox(
    provider: str,
    *,
    volume_ref: str,
    subpath: str,
    agent_type: str = "opencode",
    spawn_env: dict | None = None,
    port: int | None = None,
    root: str | None = None,
    dockerfile: str | None = None,
    pre_start_commands: list[str] | None = None,
    shared_mounts: list[str] | None = None,
    **kwargs,
) -> ProviderInstance:
    """Uniform sandbox provisioning across providers.

    Each provider exposes ``create_sandbox(volume_ref, subpath, agent_type, ...)``
    and returns a fresh ``ProviderInstance``. For Daytona, the returned instance
    has ``url=""`` (no supervisor yet) and the caller must invoke
    ``ensure_supervisor_url`` before talking to the supervisor. For Docker/Local
    the supervisor is already started at create-time and ``inst.url`` is live.
    """
    mod = _dispatch_mod(provider)
    kw: dict = dict(kwargs)
    if root is not None:
        kw["root"] = root
    if dockerfile is not None:
        kw["dockerfile"] = dockerfile
    if pre_start_commands is not None:
        kw["pre_start_commands"] = pre_start_commands
    return await mod.create_sandbox(
        volume_ref=volume_ref,
        subpath=subpath,
        agent_type=agent_type,
        spawn_env=spawn_env,
        port=port,
        shared_mounts=shared_mounts,
        **kw,
    )
