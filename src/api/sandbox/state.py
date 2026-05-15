"""Pydantic models for ``sessions.sandbox_state`` JSONB.

This is the single source of truth for "what compute should this session
have, and what's it currently bound to". See ````
— recipe lives on the session row, never on the compute itself, so
recovery cannot lose it.

Discriminated by ``type`` so the JSONB roundtrips through
``SandboxStateAdapter.deserialize`` into the correct subclass.
"""
from __future__ import annotations

from typing import Annotated, Any, Literal, Union

from pydantic import BaseModel, Field, TypeAdapter


class Resources(BaseModel):
    """Per-session compute request. Each provider applies the subset it
    supports and silently ignores the rest — see ``providers/<P>/__init__.py``
    for the per-provider mapping.

    ``gpu`` follows Modal's convention: ``"TYPE"`` (count=1), ``"TYPE:COUNT"``,
    or a bare integer string for count-only. Type-aware providers (modal)
    require a type and silently skip count-only requests; count-aware
    providers (daytona, docker) extract the count and ignore the type.
    ``disk_gib`` is only honoured by daytona.
    """

    cpu: float | None = None
    memory_mib: int | None = None
    gpu: str | None = None
    disk_gib: int | None = None


def parse_gpu(s: str | None) -> tuple[str | None, int | None]:
    """Parse a Modal-style gpu string into ``(type, count)``.

    Returns ``(None, None)`` for empty input. ``"T4:2"`` → ``("T4", 2)``;
    ``"T4"`` → ``("T4", 1)``; ``"2"`` → ``(None, 2)``.
    """
    if not s:
        return None, None
    if ":" in s:
        t, c = s.split(":", 1)
        return (t.upper() if t else None), int(c)
    if s.isdigit():
        return None, int(s)
    return s.upper(), 1


def validate_resources_for_provider(provider: str, req: Resources | None) -> None:
    """Reject resources requests with fields the provider can't honour.

    Per-provider support matrix:
      * unix_local: rejects any non-None ``resources`` (subprocess on host —
        no isolation primitive).
      * daytona: rejects ``gpu`` with a type component (daytona accepts
        count only).
      * modal: rejects ``disk_gib`` (no per-sandbox storage knob) and
        count-only ``gpu`` (modal requires a type).
      * docker: rejects ``gpu`` with a type component and ``disk_gib``.

    Raises ``ValueError`` with a human-readable message; callers should
    convert to HTTP 400.
    """
    if req is None:
        return
    if provider == "unix_local":
        raise ValueError(
            "unix_local does not support per-session resources "
            "(it runs as a host subprocess with no isolation primitive)"
        )
    gpu_type, gpu_count = parse_gpu(req.gpu)
    if provider == "daytona":
        if gpu_type is not None:
            raise ValueError(
                f"daytona does not support gpu type selection (got {req.gpu!r}); "
                "use a count-only string like \"1\" or \"2\""
            )
    elif provider == "modal":
        if req.disk_gib is not None:
            raise ValueError("modal does not support disk_gib (no per-sandbox storage knob)")
        if gpu_type is None and gpu_count is not None:
            raise ValueError(
                f"modal requires a gpu type (got count-only {req.gpu!r}); "
                "use \"T4\", \"A100:2\", etc."
            )
    elif provider == "docker":
        if gpu_type is not None:
            raise ValueError(
                f"docker does not support gpu type selection (got {req.gpu!r}); "
                "use a count-only string like \"1\" or \"2\""
            )
        if req.disk_gib is not None:
            raise ValueError("docker does not support disk_gib (no per-container storage quota)")


class Recipe(BaseModel):
    """The provisioning identity of a session — what to spin up.

    Lives on the session, never on the compute, so deleting a sandbox
    can't lose it (the architectural fix for the hivespace ``/mnt/<name>``
    bug class).
    """

    dockerfile: str | None = None
    shared_mounts: list[str] = Field(default_factory=list)
    root: str | None = None
    agent_type: str = "opencode"
    pre_start_commands: list[str] = Field(default_factory=list)
    resources: Resources | None = None
    # Optional credential-refresh hook. When set, ``SessionPool`` spawns
    # one background task per active session that POSTs to
    # ``credential_refresh_url`` with ``credential_refresh_token`` as
    # bearer, expecting JSON:
    #     {"contents": {"<abs-path>": "<base64-content>", ...},
    #      "next_refresh_at": <unix-ts>}
    # Each file is written atomically into the sandbox; the task sleeps
    # until ``next_refresh_at`` and polls again. Lifecycle is tied to
    # the active session: started on every wake (cold create + resume),
    # cancelled on hibernation/release. Designed for short-TTL tokens
    # (GitHub installation tokens, etc.) without an in-sandbox daemon.
    credential_refresh_url: str | None = None
    credential_refresh_token: str | None = None


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
    type: Literal["unix_local"] = "unix_local"
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


_KNOWN_TYPES = {"daytona", "docker", "unix_local", "modal", "unknown"}


# Maps API-level provider names (the ``provider`` field on POST /sessions
# and POST /sandboxes) to their concrete SandboxState class.
_PROVIDER_STATE_CLASS: dict[str, type[_BaseSandboxState]] = {
    "daytona": DaytonaSandboxState,
    "docker": DockerSandboxState,
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
