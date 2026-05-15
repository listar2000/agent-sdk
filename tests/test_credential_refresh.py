"""Unit tests for the credential-refresh loop in ``api.sandbox.pool``.

Verifies the contract Hivespace (and any future caller) relies on:
- Loop POSTs to the configured URL with the bearer token.
- ``contents`` payload is written into the sandbox atomically via the
  supervisor's ``/v1/exec`` channel.
- Loop sleeps to ``next_refresh_at``.
- Empty ``contents`` is a no-op (still sleeps, still alive).
- Recipe round-trips the new fields through serialize / deserialize so
  hibernation+recovery doesn't lose them.

These tests stub the network with ``respx`` rather than spinning up a
real supervisor — the loop's job is just to translate the JSON contract
into supervisor /v1/exec calls.
"""
from __future__ import annotations

import asyncio
import base64
import time

import httpx
import pytest
import respx

from api.sandbox.pool import (
    _credential_refresh_loop,
    _write_credentials_via_supervisor,
)
from api.sandbox.state import Recipe, deserialize, serialize


# ────────────────────────── Recipe persistence ──────────────────────────


def test_recipe_round_trips_new_fields():
    """Credential refresh hooks must survive serialize → JSONB → deserialize
    so a server restart can resume the loop on the next get_session()."""
    from api.sandbox.state import DaytonaSandboxState
    state = DaytonaSandboxState(
        recipe=Recipe(
            credential_refresh_url="https://hive/api/refresh",
            credential_refresh_token="bearer-xyz",
        ),
    )
    blob = serialize(state)
    rt = deserialize(blob)
    assert rt.recipe.credential_refresh_url == "https://hive/api/refresh"
    assert rt.recipe.credential_refresh_token == "bearer-xyz"


# ────────────────────────── file writer ──────────────────────────


@pytest.mark.asyncio
async def test_write_credentials_via_supervisor_emits_atomic_pipeline():
    """Each (path, b64) pair becomes one /v1/exec call with a
    mktemp + base64 -d + chmod 600 + mv pipeline. The atomic rename
    keeps in-flight git readers from ever seeing a partial file."""
    captured: list[dict] = []
    async with respx.mock:
        respx.post("http://sup/v1/exec").mock(side_effect=lambda req: httpx.Response(
            200, json={"stdout": "", "stderr": "", "exit_code": 0},
        ))
        b64 = base64.b64encode(b"https://x-access-token:T@github.com/o/r\n").decode()
        await _write_credentials_via_supervisor(
            "http://sup", {"/home/daytona/.git-credentials": b64},
        )
        for call in respx.calls:
            captured.append(call.request.read().decode())

    assert len(captured) == 1
    cmd = captured[0]
    assert "mktemp" in cmd
    assert "base64 -d" in cmd
    assert "chmod 600" in cmd
    assert "/home/daytona/.git-credentials" in cmd


@pytest.mark.asyncio
async def test_write_credentials_via_supervisor_quotes_unsafe_path():
    """Path quoting matters: a path with shell metachars must not break
    the pipeline or allow command injection through the exec channel."""
    async with respx.mock:
        route = respx.post("http://sup/v1/exec").mock(return_value=httpx.Response(
            200, json={"stdout": "", "stderr": "", "exit_code": 0},
        ))
        await _write_credentials_via_supervisor(
            "http://sup", {"/tmp/$(rm -rf /)/creds": "YWJj"},
        )
        cmd = route.calls[0].request.read().decode()
    # The shell-special chars must be inside single quotes; an unquoted
    # $(rm -rf /) would expand. shlex.quote should bury it.
    assert "'/tmp/$(rm -rf /)/creds'" in cmd


# ────────────────────────── full loop ──────────────────────────


@pytest.mark.asyncio
async def test_loop_writes_then_sleeps_to_next_refresh_at(monkeypatch):
    """One full tick: fetch → write → sleep until next_refresh_at. We
    intercept asyncio.sleep so the test doesn't actually wait."""
    sleeps: list[float] = []

    async def fake_sleep(delay):
        sleeps.append(delay)
        if len(sleeps) >= 2:  # let the loop fetch twice, then bail
            raise asyncio.CancelledError

    monkeypatch.setattr(asyncio, "sleep", fake_sleep)

    next_at = time.time() + 1800  # 30 min ahead
    b64 = base64.b64encode(b"creds-line\n").decode()
    async with respx.mock:
        respx.post("http://hive/refresh").mock(return_value=httpx.Response(
            200, json={
                "contents": {"/home/daytona/.git-credentials": b64},
                "next_refresh_at": next_at,
            },
        ))
        exec_route = respx.post("http://sup/v1/exec").mock(return_value=httpx.Response(
            200, json={"stdout": "", "stderr": "", "exit_code": 0},
        ))
        with pytest.raises(asyncio.CancelledError):
            await _credential_refresh_loop(
                "sess-1",
                url="http://hive/refresh",
                bearer="b-1",
                get_supervisor_url=lambda: "http://sup",
            )

    # Wrote credentials at least once.
    assert exec_route.called
    # First sleep was ~1800s (clamped to [60, 3600]).
    assert 1700 < sleeps[0] < 1900


@pytest.mark.asyncio
async def test_loop_tolerates_fetch_failure_and_retries(monkeypatch):
    """If the refresh endpoint 500s, the loop logs + sleeps 60s + retries.
    Never propagates the error — a flaky upstream must not tear down the
    session lease."""
    sleeps: list[float] = []

    async def fake_sleep(delay):
        sleeps.append(delay)
        if len(sleeps) >= 1:
            raise asyncio.CancelledError

    monkeypatch.setattr(asyncio, "sleep", fake_sleep)

    async with respx.mock:
        respx.post("http://hive/refresh").mock(return_value=httpx.Response(500))
        with pytest.raises(asyncio.CancelledError):
            await _credential_refresh_loop(
                "sess-2",
                url="http://hive/refresh",
                bearer="b-2",
                get_supervisor_url=lambda: "http://sup",
            )

    # First sleep is the retry-on-failure short sleep (60s), not the
    # next_refresh_at-based delay.
    assert sleeps[0] == 60


@pytest.mark.asyncio
async def test_loop_handles_empty_contents_payload(monkeypatch):
    """Agents without grants will hit a hivespace endpoint that returns
    ``{contents: {}, next_refresh_at: ...}``. The loop must accept that
    as a no-op tick and not try to exec anything."""
    sleeps: list[float] = []

    async def fake_sleep(delay):
        sleeps.append(delay)
        raise asyncio.CancelledError

    monkeypatch.setattr(asyncio, "sleep", fake_sleep)

    async with respx.mock:
        respx.post("http://hive/refresh").mock(return_value=httpx.Response(
            200, json={"contents": {}, "next_refresh_at": time.time() + 900},
        ))
        exec_route = respx.post("http://sup/v1/exec").mock(return_value=httpx.Response(
            200, json={"stdout": "", "stderr": "", "exit_code": 0},
        ))
        with pytest.raises(asyncio.CancelledError):
            await _credential_refresh_loop(
                "sess-3",
                url="http://hive/refresh",
                bearer="b-3",
                get_supervisor_url=lambda: "http://sup",
            )

    assert not exec_route.called
    # Still slept (loop is alive, just no work to do).
    assert sleeps and 60 <= sleeps[0] <= 3600
