"""Ephemeral sandbox / per-prompt streaming model.

See ``docs/ephemeral-sandbox-design.md`` for the full design.

Phase 2 of the refactor: this subpackage exists alongside the existing
recovery code. The classes here are not yet wired into the REST handlers
in ``src/api/server.py`` — that wiring lands in subsequent phase-2
commits.
"""
from .liveness import Liveness, LivenessState
from .pool import SessionPool
from .session import BaseSandboxSession
from .state import (
    DaytonaSandboxState,
    DockerSandboxState,
    ModalSandboxState,
    Recipe,
    SandboxState,
    UnixLocalSandboxState,
    UnknownSandboxState,
    deserialize,
    serialize,
)

__all__ = [
    "BaseSandboxSession",
    "DaytonaSandboxState",
    "DockerSandboxState",
    "Liveness",
    "LivenessState",
    "ModalSandboxState",
    "Recipe",
    "SandboxState",
    "SessionPool",
    "UnixLocalSandboxState",
    "UnknownSandboxState",
    "deserialize",
    "serialize",
]
