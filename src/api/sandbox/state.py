"""Pydantic models for ``sessions.sandbox_state`` JSONB.

This is the single source of truth for "what compute should this session
have, and what's it currently bound to". Per ``docs/ephemeral-sandbox-design.md``
§4 — recipe lives on the session row, never on the compute itself, so
recovery cannot lose it.

Discriminated by ``type`` so the JSONB roundtrips through
``SandboxStateAdapter.deserialize`` into the correct subclass.
"""
from __future__ import annotations

from typing import Annotated, Any, Literal, Union

from pydantic import BaseModel, Field, TypeAdapter


class Recipe(BaseModel):
    """The provisioning identity of a session — what to spin up.

    Lives on the session, never on the compute, so deleting a sandbox
    can't lose it (the architectural fix for the hivespace ``/mnt/<name>``
    bug class).
    """

    dockerfile: str | None = None
    shared_mounts: list[str] = Field(default_factory=list)
    root: str | None = None
    agent_type: str = "claude"
    pre_start_commands: list[str] = Field(default_factory=list)


class _BaseSandboxState(BaseModel):
    """Common fields for every provider's sandbox state.

    Subclasses MUST set ``type`` to a unique discriminator literal.
    """

    snapshot_path: str | None = None
    snapshot_version: int = 0
    recipe: Recipe = Field(default_factory=Recipe)


class UnknownSandboxState(_BaseSandboxState):
    """Placeholder state when no compute has been provisioned yet (or the
    referenced sandbox row was deleted out-of-band). Never directly
    started — ``SessionPool.get_session`` resolves the recipe + an
    appropriate concrete state when this is observed."""

    type: Literal["unknown"] = "unknown"
    sandbox_ref: None = None
    listen_port: None = None


class DaytonaSandboxState(_BaseSandboxState):
    type: Literal["daytona"] = "daytona"
    sandbox_ref: str | None = None
    listen_port: int | None = None


class DockerSandboxState(_BaseSandboxState):
    type: Literal["docker"] = "docker"
    sandbox_ref: str | None = None  # container id
    listen_port: int | None = None


class UnixLocalSandboxState(_BaseSandboxState):
    type: Literal["unix_local", "local"] = "unix_local"
    sandbox_ref: str | None = None  # pid as string
    listen_port: int | None = None


class ModalSandboxState(_BaseSandboxState):
    type: Literal["modal"] = "modal"
    sandbox_ref: str | None = None
    listen_port: int | None = None


SandboxState = Annotated[
    Union[
        DaytonaSandboxState,
        DockerSandboxState,
        UnixLocalSandboxState,
        ModalSandboxState,
        UnknownSandboxState,
    ],
    Field(discriminator="type"),
]


_ADAPTER: TypeAdapter[SandboxState] = TypeAdapter(SandboxState)


_KNOWN_TYPES = {"daytona", "docker", "unix_local", "local", "modal", "unknown"}


# Maps API-level provider names (the ``provider`` field on POST /sessions
# and POST /sandboxes) to their concrete SandboxState class. ``"local"``
# is an accepted alias for ``"unix_local"``.
_PROVIDER_STATE_CLASS: dict[str, type[_BaseSandboxState]] = {
    "daytona": DaytonaSandboxState,
    "docker": DockerSandboxState,
    "local": UnixLocalSandboxState,
    "unix_local": UnixLocalSandboxState,
    "modal": ModalSandboxState,
}


def state_for_provider(provider: str, recipe: Recipe) -> SandboxState:
    """Construct the per-provider initial SandboxState for a fresh cold-create.

    ``provider`` is the API-level provider name (matches the ``provider``
    field on POST /sessions and POST /sandboxes). Raises ``ValueError``
    for an unknown name — caller should map that to HTTP 400.
    """
    cls = _PROVIDER_STATE_CLASS.get(provider)
    if cls is None:
        raise ValueError(f"unsupported provider: {provider!r}")
    return cls(recipe=recipe)


def deserialize(payload: dict[str, Any] | None) -> SandboxState:
    """JSONB blob → typed state. NULL, missing ``type``, or an
    unrecognised ``type`` value all collapse to UnknownSandboxState — be
    lenient on read so a forward-compat schema bump doesn't 500 the
    server."""
    if payload is None:
        return UnknownSandboxState()
    # Tolerate legacy "local" alias by routing to unix_local.
    if payload.get("type") == "local":
        payload = {**payload, "type": "unix_local"}
    # Tolerate legacy "sandbox_id" key — pre-d5 JSONB blobs used that
    # name; the field was renamed to "sandbox_ref" to better reflect
    # its meaning (opaque provider reference, not a DB row PK).
    if "sandbox_id" in payload and "sandbox_ref" not in payload:
        payload = {**payload, "sandbox_ref": payload["sandbox_id"]}
    if payload.get("type") not in _KNOWN_TYPES:
        return UnknownSandboxState()
    return _ADAPTER.validate_python(payload)


def serialize(state: SandboxState) -> dict[str, Any]:
    """Typed state → JSONB-ready dict."""
    return _ADAPTER.dump_python(state, mode="json")
