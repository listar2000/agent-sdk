"""Fake (in-memory) sandbox provider — for unit-testing the pool lifecycle.

No real compute. All operations are in-memory no-ops.

Only module-level functions actually called by the pool or dispatch layer
are implemented here. The dispatch table in ``api.providers.__init__``
calls ``destroy_sandbox`` and ``exec_in_sandbox`` via the universal
wrappers; ``create_volume`` / ``delete_volume`` / ``ensure_supervisor_url``
are also dispatched if the pool's volume/reconcile path needs them. For
the fake provider, every call is a no-op (unit tests bypass real
provisioning entirely).
"""
from __future__ import annotations

from api.providers._shared import ExecResult, ProviderInstance


async def destroy_sandbox(instance: ProviderInstance) -> None:
    """No-op: fake sandboxes have no real compute to destroy."""


async def exec_in_sandbox(
    instance: ProviderInstance, cmd: str, *, timeout: int = 30,
) -> ExecResult:
    """No-op exec — returns empty stdout/stderr with exit code 0."""
    return ExecResult(stdout="", stderr="", returncode=0)


async def create_volume(*args, **kwargs) -> str:
    """No-op volume creation — returns a fake ref."""
    return "fake-volume-ref"


async def delete_volume(*args, **kwargs) -> None:
    """No-op volume deletion."""


async def ensure_supervisor_url(*args, **kwargs) -> str:
    """No-op — fake supervisor URL is fixed at start() time."""
    return "http://fake.local"
