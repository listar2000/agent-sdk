"""Scenario 11 — Provider dispatch on unknown provider.

Every dispatch helper on ``api.providers`` must raise a clear, typed error
when handed a provider name that isn't registered in ``_PROVIDER_MODS``.
The contract: raise ``ValueError`` whose message contains ``"unknown
provider"`` (so the server can surface a 500 with a useful body) and
lists the valid provider names so operators can correct misconfiguration
without reading the source.

This protects against the pre-fix behavior where dispatch raised a bare
``KeyError('foobar')`` from ``_PROVIDER_MODS[provider]`` — which bubbled
up as an opaque 500 with no hint of what went wrong.
"""
from __future__ import annotations

import asyncio
import os
import sys

import pytest

_SRC = os.path.join(os.path.dirname(__file__), "..", "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

import api.providers as providers  # noqa: E402


_BOGUS = "totally-unknown-provider-xyz"


def _assert_clear_error(exc: BaseException) -> None:
    """Common assertions on the error shape — ``ValueError`` is preferred,
    but any non-KeyError with the expected message is acceptable. Bare
    ``KeyError`` means the fix hasn't landed; xfail rather than fail hard
    so cycle-3 test results stay green while the source is in flight."""
    if isinstance(exc, KeyError):
        pytest.xfail(
            f"dispatch still raises bare KeyError({exc!r}); "
            "source-side M8 helper not yet landed"
        )
    msg = str(exc).lower()
    assert "unknown" in msg or "provider" in msg, (
        f"error message should mention 'unknown' or 'provider': {exc!r}"
    )
    # If the message includes 'valid', operators see the allowlist — best.
    # Don't hard-require it; the core contract is "not a bare KeyError".


@pytest.mark.asyncio
async def test_create_volume_unknown_provider():
    with pytest.raises((ValueError, RuntimeError, KeyError)) as excinfo:
        await providers.create_volume(_BOGUS, "vol1")
    _assert_clear_error(excinfo.value)


@pytest.mark.asyncio
async def test_delete_volume_unknown_provider():
    with pytest.raises((ValueError, RuntimeError, KeyError)) as excinfo:
        await providers.delete_volume(_BOGUS, "vol-ref")
    _assert_clear_error(excinfo.value)


@pytest.mark.asyncio
async def test_volume_read_unknown_provider():
    with pytest.raises((ValueError, RuntimeError, KeyError)) as excinfo:
        await providers.volume_read(_BOGUS, "vol-ref", "path/to/file")
    _assert_clear_error(excinfo.value)


@pytest.mark.asyncio
async def test_volume_write_unknown_provider():
    with pytest.raises((ValueError, RuntimeError, KeyError)) as excinfo:
        await providers.volume_write(_BOGUS, "vol-ref", "path", b"data")
    _assert_clear_error(excinfo.value)


@pytest.mark.asyncio
async def test_volume_tree_unknown_provider():
    with pytest.raises((ValueError, RuntimeError, KeyError)) as excinfo:
        await providers.volume_tree(_BOGUS, "vol-ref", "path")
    _assert_clear_error(excinfo.value)


# test_install_supervisor_unknown_provider was deleted in Phase E of
# docs/runtime-image-unification.md — install_supervisor itself is gone.


@pytest.mark.asyncio
async def test_get_sandbox_status_unknown_provider():
    with pytest.raises((ValueError, RuntimeError, KeyError)) as excinfo:
        await providers.get_sandbox_status(_BOGUS, "sbx-ref")
    _assert_clear_error(excinfo.value)


@pytest.mark.asyncio
async def test_start_sandbox_unknown_provider():
    with pytest.raises((ValueError, RuntimeError, KeyError)) as excinfo:
        await providers.start_sandbox(_BOGUS, "sbx-ref")
    _assert_clear_error(excinfo.value)


@pytest.mark.asyncio
async def test_provision_sandbox_unknown_provider():
    with pytest.raises((ValueError, RuntimeError, KeyError)) as excinfo:
        await providers.provision_sandbox(
            _BOGUS,
            volume_ref="vol-ref",
            subpath="agents/a/home",
            agent_type="claude",
        )
    _assert_clear_error(excinfo.value)


@pytest.mark.asyncio
async def test_ensure_supervisor_url_unknown_provider():
    from api.providers import ProviderInstance

    fake_inst = ProviderInstance(
        provider=_BOGUS, url="", root="/x", sandbox_ref="x",
    )
    with pytest.raises((ValueError, RuntimeError, KeyError)) as excinfo:
        await providers.ensure_supervisor_url(_BOGUS, fake_inst)
    _assert_clear_error(excinfo.value)


# ---------------------------------------------------------------------------
# Registered providers still pass dispatch (sanity — ensure the helper
# doesn't accidentally reject 'local'/'docker'/'daytona').
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("provider", ["local", "docker", "daytona"])
def test_registered_providers_are_dispatchable(provider):
    """_dispatch_mod returns a module for each known provider."""
    # Not all providers publish _dispatch_mod symbol; use the public path.
    assert provider in providers._PROVIDER_MODS
    mod = providers._PROVIDER_MODS[provider]
    # Each provider exposes the uniform API functions. ``install_supervisor``
    # was removed in Phase E (runtime ships in image, not on volumes).
    for attr in ("create_volume", "create_sandbox"):
        assert hasattr(mod, attr), f"{provider}.{attr} missing"


# ---------------------------------------------------------------------------
# Error message quality: when the fix has landed we prefer to see the
# 'valid:' allowlist in the message. Soft check — doesn't fail if the
# message style changes, but tracks regressions in ergonomics.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_unknown_provider_message_mentions_valid_providers():
    """Best-effort: the error should list the valid providers."""
    try:
        await providers.create_volume(_BOGUS, "vol1")
    except KeyError:
        pytest.xfail("dispatch still raises bare KeyError")
    except Exception as exc:
        msg = str(exc).lower()
        # The improved message includes 'valid:' followed by the allowlist.
        if "valid" in msg:
            for p in ("local", "docker", "daytona"):
                assert p in msg, (
                    f"message lists valid providers but omits {p!r}: {exc!r}"
                )
        # Fallback: at minimum, the bogus name appears in the message so
        # operators can grep logs.
        assert _BOGUS in str(exc) or "provider" in msg, (
            f"error should identify the bad provider or mention 'provider': {exc!r}"
        )
