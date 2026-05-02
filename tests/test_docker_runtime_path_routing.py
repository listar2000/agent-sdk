"""Phase C coverage: docker provider's image-runtime feature flag.

Mirrors ``tests/test_local_volume_integration.py``'s flag-on/flag-off
coverage, but for the docker provider. Doesn't require a running docker
daemon — ``_run_docker`` (the only path through which docker.py invokes
``docker run ...``) is replaced with a recorder so we can assert on the
argv shape (bind-mount of the runtime path, no system/supervisor volume
mount) before any real container starts.

When Phase E deletes the legacy volume-install path, the by-default test
collapses to a single happy-path check.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

_SRC = os.path.join(os.path.dirname(__file__), "..", "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

from api.providers import docker as dprov  # noqa: E402


def _make_recorder():
    """Return an AsyncMock that records every (rc, out, err) call to
    ``_run_docker`` and returns a docker-run-shaped success.

    The first ``run`` invocation produces a 64-hex container id (what
    ``docker create_sandbox`` parses out of stdout). Subsequent calls
    succeed with empty output.
    """
    captured: list[tuple] = []
    container_id = "a" * 64

    async def fake(*args, **kwargs):
        captured.append(args)
        # Match _run_docker's tuple shape: (rc, stdout, stderr).
        # The "run" subcommand returns the container id on stdout.
        if args and args[0] == "run":
            return (0, (container_id + "\n").encode(), b"")
        if args and args[0] == "rm":
            return (0, b"", b"")
        return (0, b"", b"")

    return AsyncMock(side_effect=fake), captured


def _docker_run_args(captured: list[tuple]) -> tuple:
    """Find the create_sandbox docker-run args.

    docker.py invokes ``_run_docker`` twice during create_sandbox:
      1. ``_ensure_subpath_dir`` runs ``docker run --rm ... mkdir -p ...``
         to pre-create the agent's HOME on the volume.
      2. ``create_sandbox`` runs ``docker run -d -p ...`` to launch the
         actual sandbox container.
    We want #2 — the detached run with the supervisor flags.
    """
    for args in captured:
        if args and args[0] == "run" and "-d" in args:
            return args
    raise AssertionError(
        f"no detached docker-run captured among {len(captured)} calls: "
        f"{[a[:3] for a in captured]}"
    )


@pytest.mark.asyncio
async def test_docker_create_sandbox_uses_runtime_image(monkeypatch):
    """Docker provider's ``create_sandbox`` uses the agent-sdk runtime
    image as the container base (symmetric with daytona's image-based
    provisioning), no host bind-mount of the runtime path. Resolved via
    DOCKER_IMAGE / AGENT_SDK_IMAGE / .runtime-image-tag (in that order)."""
    monkeypatch.setenv("DOCKER_IMAGE", "ghcr.io/example/agent-sdk:test-tag")

    recorder, captured = _make_recorder()
    monkeypatch.setattr(dprov, "_run_docker", recorder)
    monkeypatch.setattr(dprov, "_run_docker_checked", recorder)
    async def _fail_health(*_a, **_kw):
        return False
    monkeypatch.setattr(dprov, "_wait_for_health", _fail_health)

    with pytest.raises(Exception):
        await dprov.create_sandbox(
            volume_ref="vol-fake",
            subpath="agents/a1/home",
            agent_type="claude",
            port=33333,
        )

    args = _docker_run_args(captured)
    argv_str = " ".join(args)

    # The image arg in the docker-run is the resolved runtime image —
    # daytona-symmetric (the image's filesystem already contains
    # ``/opt/agent-sdk/runtime/``, no bind-mount needed).
    assert "ghcr.io/example/agent-sdk:test-tag" in args, (
        f"docker run argv missing runtime image. Got argv: {argv_str!r}"
    )

    # No host bind-mount of the runtime path: the image already has it baked.
    assert "type=bind,source=" not in argv_str or "/opt/agent-sdk/runtime" not in argv_str, (
        f"docker run argv should not bind-mount runtime path; image bakes it. "
        f"Got argv: {argv_str!r}"
    )

    # Legacy volume-subpath=system/supervisor is gone (Phase E removed it).
    assert "volume-subpath=system/supervisor" not in argv_str


@pytest.mark.asyncio
async def test_docker_create_sandbox_raises_without_runtime_image(monkeypatch):
    """Without DOCKER_IMAGE / AGENT_SDK_IMAGE / .runtime-image-tag, the
    docker provider raises a clear remediation error rather than silently
    falling back to a stock node image (the symmetry-breaking it used to do
    by bind-mounting the host runtime)."""
    monkeypatch.delenv("DOCKER_IMAGE", raising=False)
    monkeypatch.delenv("AGENT_SDK_IMAGE", raising=False)
    monkeypatch.setattr(dprov, "_read_runtime_image_tag", lambda: None)

    with pytest.raises(RuntimeError) as excinfo:
        await dprov.create_sandbox(
            volume_ref="vol-fake",
            subpath="agents/a1/home",
            agent_type="claude",
            port=55555,
        )

    msg = str(excinfo.value)
    assert "DOCKER_IMAGE" in msg or "AGENT_SDK_IMAGE" in msg
    assert ".runtime-image-tag" in msg


# ``test_docker_create_sandbox_image_runtime_flag_off`` was deleted in
# the runtime-image-unification refactor — the flag-off path no
# longer exists, so there's nothing to pin.
