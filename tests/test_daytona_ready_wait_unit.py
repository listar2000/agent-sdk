"""Unit test: ``_wait_for_daytona_sandbox_ready`` returns the ready handle.

The restart/resume path (``restart_daytona_supervisor``) used to call
``daytona.get()`` AGAIN immediately after the ready-poll, just to refresh the
sandbox handle — a redundant control-plane round-trip (~100 ms) on every
cold-recovery, since the poll loop had already fetched a fresh, network-ready
handle and thrown it away (returned ``None``). The helper now RETURNS that
handle so the caller reuses it. Pin both the return value and that it costs
exactly one ``get()`` when the sandbox is ready on the first probe.
"""
from __future__ import annotations

from types import SimpleNamespace

import pytest


def _ready_client(get_calls: list):
    """Fake AsyncDaytona whose ``get()`` returns a started, exec-ready sandbox
    and records each call."""
    async def _exec(cmd, timeout=5):
        return SimpleNamespace(result="ready\n")

    async def _get(ref):
        get_calls.append(ref)
        return SimpleNamespace(id=ref, state="started",
                               process=SimpleNamespace(exec=_exec))

    return SimpleNamespace(get=_get)


@pytest.mark.asyncio
async def test_ready_wait_returns_handle_without_extra_get():
    from api.providers import daytona

    calls: list = []
    client = _ready_client(calls)
    sb = await daytona._wait_for_daytona_sandbox_ready(client, "sb-123")

    # Returns the fresh, network-ready handle (the old code returned None, so the
    # caller had to issue a second get() — that redundant RTT is now gone).
    assert sb is not None, "ready-wait must return the sandbox handle, not None"
    assert sb.id == "sb-123"
    assert daytona._enum_str(sb.state) == "started"
    # Exactly one control-plane round-trip when ready on the first probe.
    assert len(calls) == 1, f"expected 1 get(), got {len(calls)}"


@pytest.mark.asyncio
async def test_ready_wait_retries_until_started_then_returns_handle():
    """If the sandbox reports a non-'started' state first, the poll keeps going
    and returns the handle once it's started + exec-ready."""
    from api.providers import daytona

    states = iter(["starting", "starting", "started"])

    async def _exec(cmd, timeout=5):
        return SimpleNamespace(result="ready")

    async def _get(ref):
        return SimpleNamespace(id=ref, state=next(states),
                               process=SimpleNamespace(exec=_exec))

    client = SimpleNamespace(get=_get)
    sb = await daytona._wait_for_daytona_sandbox_ready(client, "sb-xyz")
    assert sb is not None and sb.id == "sb-xyz"
    assert daytona._enum_str(sb.state) == "started"
