"""Factory: ``state.type`` discriminator → concrete SandboxSession class.

One dict, one method call. No "if provider == X" branches in shared code.

Adding a new provider means: implement ``XxxSandboxSession`` and add one
line here.
"""
from __future__ import annotations

from typing import Callable

from .session import BaseSandboxSession
from .state import (
    DaytonaSandboxState,
    DockerSandboxState,
    ModalSandboxState,
    SandboxState,
    UnixLocalSandboxState,
    UnknownSandboxState,
)

# Map from state-type discriminator → concrete session class. Phase 2
# sub-task 2 only landed daytona; docker/local/modal added in sub-tasks
# 4-6. The "unknown" entry routes to daytona by default — coerced via
# the DaytonaSandboxSession constructor — so an unwired provider doesn't
# 500 the server.
_REGISTRY: dict[str, Callable[[str, SandboxState], BaseSandboxSession]] = {}
_REGISTRY_INITIALIZED = False


def register(type_value: str, factory: Callable[[str, SandboxState], BaseSandboxSession]) -> None:
    """Register a concrete session class for a state.type discriminator.
    Idempotent (last write wins) so test setup can swap implementations."""
    _REGISTRY[type_value] = factory


def make_session(session_id: str, state: SandboxState) -> BaseSandboxSession:
    """Dispatch a state into its concrete SandboxSession.

    Concrete classes use keyword-only ``__init__(*, session_id, state)``
    so this dispatcher invokes them with kwargs. Adapters in the
    registry are functions taking ``(session_id, state)`` positionally
    and returning a constructed session.

    Raises KeyError for an unregistered ``state.type`` — caller should
    pre-register all providers it intends to use.
    """
    global _REGISTRY_INITIALIZED
    if not _REGISTRY_INITIALIZED:
        _register_default_providers()
        _REGISTRY_INITIALIZED = True
    type_value = getattr(state, "type", "unknown")
    factory = _REGISTRY.get(type_value)
    if factory is None:
        # UnknownSandboxState (no compute provisioned yet) routes to the
        # default — currently daytona. This matches today's behavior:
        # POST /sessions defaults to daytona unless provider is given.
        factory = _REGISTRY.get("daytona")
        if factory is None:
            raise KeyError(
                f"No SandboxSession registered for type={type_value!r} "
                f"and no daytona default registered. Call register() at startup."
            )
    return factory(session_id, state)


def _adapt(cls):
    """Wrap a SandboxSession class so it can be registered as a factory.
    Bridges the positional ``(session_id, state)`` factory contract to
    the class's keyword-only ``__init__(*, session_id, state)``."""
    def _build(session_id: str, state: SandboxState) -> BaseSandboxSession:
        return cls(session_id=session_id, state=state)
    _build.__qualname__ = f"_adapt({cls.__qualname__})"
    return _build


def _register_default_providers() -> None:
    """Register all available concrete SandboxSession classes.

    Called lazily on the first ``make_session`` invocation so module-load
    cycles (provider session.py needs ``BaseSandboxSession`` from
    ``api.sandbox.session``, which imports this factory) are avoided.
    Tests that need a registered provider before constructing a state
    can call ``make_session`` once with any state to trigger registration.
    """
    from api.providers.daytona.session import DaytonaSandboxSession
    from api.providers.docker.session import DockerSandboxSession
    from api.providers.modal.session import ModalSandboxSession
    from api.providers.unix_local.session import UnixLocalSandboxSession
    register("daytona", _adapt(DaytonaSandboxSession))
    register("docker", _adapt(DockerSandboxSession))
    register("modal", _adapt(ModalSandboxSession))
    register("unix_local", _adapt(UnixLocalSandboxSession))
    register("unknown", _adapt(DaytonaSandboxSession))  # default-to-daytona
