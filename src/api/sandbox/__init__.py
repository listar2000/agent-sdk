"""Ephemeral SandboxSession + per-provider concrete classes.

See ````.

What's here:
  * ``BaseSandboxSession`` — abstract lifecycle methods start / running
    / stop; concrete execute_prompt / destroy / shutdown, plus shared
    subscriber fan-out + bootstrap.
  * ``DaytonaSandboxSession`` / ``DockerSandboxSession`` /
    ``UnixLocalSandboxSession`` / ``ModalSandboxSession`` — concrete
    impls, one file per provider.
  * ``Liveness`` — single per-session liveness oracle (replaces
    today's scattered ``_reader_alive`` / ``_reader_connected`` /
    ``_instance_process_alive``).
  * ``BaseSandboxState`` + per-provider state subclasses — Pydantic
    discriminated union backing ``sessions.sandbox_state`` JSONB.
  * ``make_session`` factory — discriminates by ``state.type``.
  * ``SessionPool`` + ``get_pool()`` runtime singleton — at-most-one
    active SandboxSession per session_id; reads/writes
    ``sessions.sandbox_state`` directly via ``api.db``.
"""
from .factory import make_session, register
from .liveness import Liveness
from .orphan_monitor import start_orphan_monitor, stop_orphan_monitor
from .pool import SessionNotFoundError, SessionPool
from .runtime import get_pool, shutdown_pool, start_reaper, start_worker_heartbeat
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
    state_for_provider,
)

__all__ = [
    "BaseSandboxSession",
    "DaytonaSandboxState",
    "DockerSandboxState",
    "Liveness",
    "ModalSandboxState",
    "Recipe",
    "SandboxState",
    "SessionNotFoundError",
    "SessionPool",
    "UnixLocalSandboxState",
    "UnknownSandboxState",
    "deserialize",
    "get_pool",
    "make_session",
    "register",
    "serialize",
    "shutdown_pool",
    "start_orphan_monitor",
    "start_reaper",
    "start_worker_heartbeat",
    "state_for_provider",
    "stop_orphan_monitor",
]
