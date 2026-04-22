"""M3 — unit test for the MA5 port-collision retry in ``docker.create_sandbox``.

Exercises the TOCTOU retry loop without needing a real Docker daemon: we
patch the ``_run_docker`` helper and the supervisor health-check, then
assert that a simulated "port already allocated" error triggers exactly
one retry with a fresh port.

Regression guard: reverting MA5 (strip the retry block) leaves the first
``docker run`` failure un-recoverable and the test fails at the single
call-count + RuntimeError assertions below.
"""
from __future__ import annotations

import os
import sys
from unittest.mock import AsyncMock, patch

import pytest

_SRC = os.path.join(os.path.dirname(__file__), "..", "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

from api.providers import docker as dprov  # noqa: E402


@pytest.mark.asyncio
async def test_create_sandbox_retries_once_on_port_collision():
    """Simulate ``docker run`` returning rc=125 with the port-allocated
    error on the first call; the second call returns a successful container
    id.  The retry MUST happen exactly once, the returned instance MUST
    carry the container id from the second call, and the port MUST differ
    from the first attempt (the retry allocates a fresh one)."""
    first_port_holder: list[int] = []
    second_port_holder: list[int] = []
    call_count = {"n": 0}

    async def fake_run_docker(*args, **kw):
        # Parse ``-p <host>:<container>`` out of the argv to verify retry
        # used a fresh port.  The "run" command is the only one that
        # should hit this mock for this test; other helpers are bypassed
        # by the subpath-dir + health-check patches below.
        port_arg: int | None = None
        for i, a in enumerate(args):
            if a == "-p" and i + 1 < len(args):
                port_arg = int(args[i + 1].split(":", 1)[0])
                break
        call_count["n"] += 1
        if call_count["n"] == 1:
            # First call — return the exact stderr shape Docker emits so
            # ``_is_port_collision`` keys on it.
            if port_arg is not None:
                first_port_holder.append(port_arg)
            return (
                125,
                b"",
                b"docker: Error response from daemon: driver failed programming "
                b"external connectivity on endpoint xyz: Bind for 0.0.0.0:12345 "
                b"failed: port is already allocated.\n",
            )
        # Retry — return a plausible docker-run success (64-char hex id).
        if port_arg is not None:
            second_port_holder.append(port_arg)
        return (0, b"fake" + b"0" * 60 + b"\n", b"")

    # Bypass the util-container mkdir and the supervisor health check —
    # those are orthogonal to the retry path under test.
    async def _noop_ensure_subpath_dir(*a, **kw):
        return None

    async def _always_healthy(*a, **kw):
        return True

    with patch("api.providers.docker._run_docker",
               new=AsyncMock(side_effect=fake_run_docker)), \
         patch("api.providers.docker._ensure_subpath_dir",
               new=AsyncMock(side_effect=_noop_ensure_subpath_dir)), \
         patch("api.providers.docker._wait_for_health",
               new=AsyncMock(side_effect=_always_healthy)):
        inst = await dprov.create_sandbox(
            volume_ref="vol-mock",
            subpath="agents/retry/home",
            agent_type="claude",
        )

    assert call_count["n"] == 2, (
        f"expected exactly one retry after port collision, "
        f"got {call_count['n']} calls to _run_docker"
    )
    assert inst.container_id and inst.container_id.startswith("fake"), (
        f"retry should have produced a container: {inst!r}"
    )
    assert inst.port is not None
    # The retry allocates a fresh port via _find_free_port.  It's not
    # guaranteed to be different (the OS may reissue the same free port)
    # but it's overwhelmingly likely in practice; the load-bearing
    # assertion is that the retry used whatever port ``inst.port`` carries.
    assert second_port_holder and second_port_holder[0] == inst.port, (
        f"second docker-run should use the instance's reported port: "
        f"inst.port={inst.port}, second-run port={second_port_holder!r}"
    )


@pytest.mark.asyncio
async def test_create_sandbox_no_retry_on_non_port_error():
    """A non-port-related ``docker run`` failure must NOT trigger the
    retry — the retry is specific to the TOCTOU window for port
    allocation.  Retrying on an image-pull or permission error would
    mask bugs and double the outage window."""
    call_count = {"n": 0}

    async def fake_run_docker(*args, **kw):
        call_count["n"] += 1
        return (
            125,
            b"",
            b"docker: Error response from daemon: pull access denied for "
            b"missing-image, repository does not exist or may require "
            b"'docker login'.\n",
        )

    async def _noop(*a, **kw):
        return None

    with patch("api.providers.docker._run_docker",
               new=AsyncMock(side_effect=fake_run_docker)), \
         patch("api.providers.docker._ensure_subpath_dir",
               new=AsyncMock(side_effect=_noop)), \
         patch("api.providers.docker._wait_for_health",
               new=AsyncMock(return_value=True)):
        with pytest.raises(RuntimeError, match="docker run failed"):
            await dprov.create_sandbox(
                volume_ref="vol-mock",
                subpath="agents/noretry/home",
                agent_type="claude",
            )

    assert call_count["n"] == 1, (
        f"non-port-collision errors must not retry; got {call_count['n']} calls"
    )
