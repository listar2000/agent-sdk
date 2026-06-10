"""ACP supervisor provider management — daytona, docker, unix_local, modal.

This package splits provider-specific code into sub-modules:
  - daytona/       — Daytona sandbox management
  - docker/        — Docker container management
  - unix_local/    — Host-subprocess management
  - modal/         — Modal sandbox management
  - _shared.py     — Shared types, constants, helpers

providers/__init__.py:
  - Re-exports the ``_shared`` and ``.daytona`` symbols that server.py
    (and tests) import from ``api.providers``.
  - Provides universal dispatch wrappers (destroy_instance, exec_in_instance,
    create_volume, delete_volume, reconcile_sandboxes, ensure_supervisor_url)
    that route to the per-provider module of the same name.
"""

import asyncio
import logging

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
    _exec_subprocess,
    _normalize_workspace,
)

# Re-export Daytona-specific symbols for server.py + tests.
from .daytona import (
    destroy_daytona,
    create_daytona_volume,
    delete_daytona_volume,
    provision_daytona_sandbox,
    restart_daytona_supervisor,
    _get_async_daytona_client,
)

# Provider module dispatch table
from . import daytona as _daytona_mod
from . import docker as _docker_mod
from . import fake as _fake_mod
from . import unix_local as _unix_local_mod
from . import modal as _modal_mod

_PROVIDER_MODS = {
    "daytona": _daytona_mod,
    "docker": _docker_mod,
    "fake": _fake_mod,
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

async def destroy_instance(instance: ProviderInstance) -> None:
    """Destroy a supervisor instance."""
    await _dispatch_mod(instance.provider).destroy_sandbox(instance)


async def exec_in_instance(instance: ProviderInstance, cmd: str, timeout: int = 30) -> ExecResult:
    """Run a shell command in the sandbox environment. Dispatches to the
    provider module's ``exec_in_sandbox`` — provider-specific exec (host
    subprocess / ``docker exec`` / daytona SDK / modal tunnel) lives WITH the
    provider, not inline here."""
    return await _dispatch_mod(instance.provider).exec_in_sandbox(
        instance, cmd, timeout=timeout,
    )


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
    ``_dispatch_mod``). All four providers have a registered adapter, so
    this is the single path for per-volume file ops.
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


