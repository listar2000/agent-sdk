"""Unit tests for ``get_daytona_sandbox_status`` state classification.

The status classifier is the input to ``_ensure_sandbox_locked``'s
recovery-vs-replacement decision: ``stopped`` triggers ``start_sandbox``
(Type 1 revive), ``error`` triggers ``destroy_sandbox`` + ``delete_sandbox``
+ provision-new (Type 2 replacement). Misclassifying a transitional
state as ``error`` therefore destroys live work.

These tests pin the mapping for every Daytona state we know about.
"""
from __future__ import annotations

import os
import sys
from types import SimpleNamespace

import pytest

_SRC = os.path.join(os.path.dirname(__file__), "..", "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)


def _patch_client_to_return_state(monkeypatch, state_value: str):
    """Make ``await daytona._get_async_daytona_client(); await client.get(ref)``
    return a sandbox whose ``state`` is ``state_value``."""
    from api.providers import daytona
    from unittest.mock import AsyncMock

    fake_sandbox = SimpleNamespace(state=state_value)
    fake_client = SimpleNamespace(get=AsyncMock(return_value=fake_sandbox))
    monkeypatch.setattr(
        daytona,
        "_get_async_daytona_client",
        AsyncMock(return_value=fake_client),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("state,expected", [
    # Stable terminal states — reference points.
    ("started", "running"),
    ("running", "running"),
    ("stopped", "stopped"),
    ("paused", "stopped"),
    # Transitional states — these are the regression target. Under load
    # Daytona's stop / start can sit in "stopping" / "starting" for 5-30s,
    # and the classifier must lean toward the target state, NOT "error".
    # Misclassifying as "error" makes _ensure_sandbox_locked destroy the
    # sandbox + provision a replacement (Type 2), which breaks any test
    # asserting same-sandbox recovery and any UI session that expected
    # in-place restart.
    ("stopping", "stopped"),
    ("starting", "running"),
    ("pulling_image", "running"),
    ("creating", "running"),
    ("resizing", "running"),
    # Truly gone — fine to classify as missing (caller will provision new).
    ("destroyed", "missing"),
    ("destroying", "missing"),
    ("archived", "missing"),
    # Daytona's own error signal stays mapped to "error".
    ("error", "error"),
])
async def test_status_mapping_for_each_daytona_state(monkeypatch, state, expected):
    from api.providers import daytona

    _patch_client_to_return_state(monkeypatch, state)
    result = await daytona.get_daytona_sandbox_status("any-ref")
    assert result == expected, (
        f"daytona state={state!r} classified as {result!r}; expected {expected!r}. "
        f"Mapping {state!r} → 'error' would trigger destroy+Type 2 in "
        f"_ensure_sandbox_locked, killing in-place recovery."
    )


@pytest.mark.asyncio
async def test_status_unknown_state_does_not_destroy_sandbox(monkeypatch):
    """A Daytona SDK upgrade could introduce a new transitional state name.

    The classifier MUST NOT map an unrecognized state to ``error`` — the
    cost of a false ``error`` (destroy + Type 2) is high; the cost of a
    false ``running`` is just one extra wait-for-health round-trip.
    """
    from api.providers import daytona

    _patch_client_to_return_state(monkeypatch, "future_unknown_state")
    result = await daytona.get_daytona_sandbox_status("any-ref")
    assert result != "error", (
        "unknown state classified as 'error' — would trigger destroy + "
        "Type 2 replacement on any future Daytona state name we haven't "
        "seen yet. Default to a non-destructive classification (e.g. 'running')."
    )


@pytest.mark.asyncio
async def test_status_not_found_maps_to_missing(monkeypatch):
    from api.providers import daytona
    from unittest.mock import AsyncMock

    async def raise_not_found(_ref):
        raise Exception("Sandbox with ID or name foo not found")

    fake_client = SimpleNamespace(get=raise_not_found)
    monkeypatch.setattr(
        daytona, "_get_async_daytona_client",
        AsyncMock(return_value=fake_client),
    )

    result = await daytona.get_daytona_sandbox_status("any-ref")
    assert result == "missing"


@pytest.mark.asyncio
async def test_status_other_exception_maps_to_error(monkeypatch):
    """API failures other than 404 surface as 'error' (caller may retry)."""
    from api.providers import daytona
    from unittest.mock import AsyncMock

    async def raise_other(_ref):
        raise Exception("internal server error 500")

    fake_client = SimpleNamespace(get=raise_other)
    monkeypatch.setattr(
        daytona, "_get_async_daytona_client",
        AsyncMock(return_value=fake_client),
    )

    result = await daytona.get_daytona_sandbox_status("any-ref")
    assert result == "error"
