"""Ephemeral SandboxSession + per-provider concrete classes.

Per ``docs/ephemeral-sandbox-design.md``. This module ships the
classes; **wiring into the server's recovery path is a separate PR**.

What's here:
  * ``BaseSandboxSession`` — abstract: start / running / execute_prompt
    / stop / shutdown, plus shared subscriber fan-out + bootstrap.
  * ``DaytonaSandboxSession`` / ``DockerSandboxSession`` /
    ``UnixLocalSandboxSession`` / ``ModalSandboxSession`` — concrete
    impls, one file per provider.
  * ``Liveness`` — single per-session liveness oracle (replaces
    today's scattered ``_reader_alive`` / ``_reader_connected`` /
    ``_instance_process_alive``).
  * ``BaseSandboxState`` + per-provider state subclasses — Pydantic
    discriminated union backing ``sessions.sandbox_state`` JSONB.
  * ``make_session`` factory — discriminates by ``state.type``.

What's NOT here yet (intentionally — follow-up PR):
  * ``SessionPool`` / runtime singleton
  * DB bindings (``load_sandbox_state`` / ``save_sandbox_state``)
  * Cutover from ``_ensure_*`` recovery in ``server.py`` to the pool

The classes can be exercised in isolation by tests (state round-trip,
factory dispatch, liveness state machine) so they're not dead code —
they just don't run on the request path yet.
"""
from .factory import make_session, register
from .liveness import Liveness, LivenessState
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
    "UnixLocalSandboxState",
    "UnknownSandboxState",
    "deserialize",
    "make_session",
    "register",
    "serialize",
]
