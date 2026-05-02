"""Provider dispatch raises a typed, readable error on unknown providers.

Pins the contract that bare ``KeyError('foobar')`` from
``_PROVIDER_MODS[provider]`` never reaches the server's exception
handler — it must be a ``ValueError`` whose message names the bogus
input and lists the valid providers.
"""
from __future__ import annotations

import os
import sys

import pytest

_SRC = os.path.join(os.path.dirname(__file__), "..", "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

import api.providers as providers  # noqa: E402


_BOGUS = "totally-unknown-provider-xyz"


@pytest.mark.asyncio
async def test_unknown_provider_raises_typed_error_with_allowlist():
    """One dispatch call is enough — _dispatch_mod is the choke point for
    every uniform-API helper. The shape of the error is what matters, not
    which helper triggered it."""
    with pytest.raises(ValueError) as excinfo:
        await providers.create_volume(_BOGUS, "vol1")
    msg = str(excinfo.value)
    assert "unknown provider" in msg.lower()
    assert _BOGUS in msg
    for p in ("unix_local", "docker", "daytona", "modal"):
        assert p in msg, f"valid-provider allowlist omits {p!r}: {msg}"


@pytest.mark.parametrize("provider", ["unix_local", "docker", "daytona", "modal"])
def test_registered_providers_are_dispatchable(provider):
    """Sanity check: each registered provider exposes the uniform API."""
    assert provider in providers._PROVIDER_MODS
    mod = providers._PROVIDER_MODS[provider]
    for attr in ("create_volume", "create_sandbox"):
        assert hasattr(mod, attr), f"{provider}.{attr} missing"
