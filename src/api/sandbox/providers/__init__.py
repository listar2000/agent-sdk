"""Concrete SandboxSession implementations per provider.

One class per provider, each wrapping the existing primitives in
``src/api/providers/<name>.py`` into the
``BaseSandboxSession`` 5-method contract.

Phase 2 sub-task 2 starts with daytona because it's the most complex;
docker/local/modal follow.
"""
from .daytona import DaytonaSandboxSession
from .docker import DockerSandboxSession
from .modal import ModalSandboxSession
from .unix_local import UnixLocalSandboxSession

__all__ = [
    "DaytonaSandboxSession",
    "DockerSandboxSession",
    "ModalSandboxSession",
    "UnixLocalSandboxSession",
]
