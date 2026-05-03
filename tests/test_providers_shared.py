"""MT4 — ``_find_free_port`` bind-probe + OS-assigned fallback.

``api.providers._shared._find_free_port`` must:

1. Never return a port that is currently bound at the OS level, even if
   its internal monotonic counter happens to land on it.
2. Fall through to ``bind(0)`` (OS-assigned) when every candidate in a
   bounded loop is occupied.

These tests bind real sockets on 127.0.0.1 to simulate OS-level port
occupancy, then assert the allocator skips them.
"""
from __future__ import annotations

import asyncio
import contextlib
import os
import socket
import sys

import pytest


_SRC = os.path.join(os.path.dirname(__file__), "..", "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)


@contextlib.contextmanager
def _bind_port(port: int):
    """Occupy ``port`` on 127.0.0.1 with a listening socket."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind(("127.0.0.1", port))
    s.listen(1)
    try:
        yield s
    finally:
        s.close()


@pytest.mark.asyncio
async def test_find_free_port_skips_occupied_port():
    """A port occupied at the OS level must never be returned by the
    allocator. The implementation now uses ``bind(("127.0.0.1", 0))``
    which delegates to the kernel — it won't hand out a port that's
    already bound by another socket.
    """
    from api.providers import _shared as sh

    # Hold a port via bind+listen, then call the allocator. The kernel
    # picks an ephemeral; verify it's never the held port across many
    # calls.
    with _bind_port(0) as held:
        held_port = held.getsockname()[1]
        for _ in range(8):
            port = await sh._find_free_port()
            assert port != held_port, (
                f"allocator returned occupied port {held_port}"
            )


@pytest.mark.asyncio
async def test_find_free_port_loop_never_returns_bound_port():
    """Loop: bind a known port, call _find_free_port N times, assert none
    of the returned ports equals the bound one.  (Realistic scenario —
    another process on the box holds the port.)"""
    from api.providers import _shared as sh

    with _bind_port(0) as bound:
        hot = bound.getsockname()[1]
        seen: list[int] = []
        for _ in range(8):
            p = await sh._find_free_port()
            seen.append(p)
        assert hot not in seen, (
            f"allocator returned the bound port {hot}: {seen}"
        )


@pytest.mark.asyncio
async def test_find_free_port_returns_bindable_ports():
    """Every port the allocator returns must be bindable at OS level.

    The implementation just delegates to ``bind(("127.0.0.1", 0))``, so
    the kernel guarantees an unused ephemeral. This test pins that
    contract — a future change that introduces caching or recycling
    must keep it true.
    """
    from api.providers import _shared as sh

    seen: set[int] = set()
    for _ in range(16):
        port = await sh._find_free_port()
        assert port > 0
        # Verify bindable. We have to close the allocator's socket
        # (it already did), so we can rebind here.
        test = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        test.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            test.bind(("127.0.0.1", port))
        finally:
            test.close()
        # The allocator should not double-issue within a single test
        # process even though we don't reserve.
        seen.add(port)
    assert len(seen) >= 8, (
        f"expected mostly-unique ports across 16 calls, got {len(seen)} "
        "distinct values — kernel may be re-issuing aggressively"
    )


# ---------------------------------------------------------------------------
# Security — shell-injection via spawn_env keys (Cycle 13)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "bad_key",
    [
        "FOO;echo PWNED;BAR",          # classic command separator
        "FOO=val BAR",                 # space inside the key
        "FOO\nBAR",                    # newline
        "FOO`id`",                     # backtick command sub
        "FOO$(id)",                    # $() command sub
        "FOO&whoami",                  # background + second cmd
        "FOO|cat",                     # pipe
        "FOO>&2",                      # redir
        "1STARTS_WITH_DIGIT",          # POSIX rejects
        "",                            # empty
        "FOO BAR",                     # plain space
        "-u FOO",                      # mimic env --unset flag
    ],
)
def test_build_env_prefix_rejects_shell_metacharacter_keys(bad_key):
    """``_build_env_prefix`` must refuse to interpolate a non-POSIX env var name.

    Values are ``shlex.quote``-wrapped, but keys are emitted raw on the
    left of ``K=V`` — a key like ``FOO;echo PWNED;BAR`` would break out of
    ``env``'s arglist and run arbitrary commands inside the sandbox.
    """
    from api.providers._shared import _build_env_prefix

    with pytest.raises(ValueError, match="invalid env var name"):
        _build_env_prefix({bad_key: "safe-value"})


def test_build_env_prefix_accepts_valid_keys():
    """Sanity: legitimate POSIX names pass and the value is shlex-quoted."""
    from api.providers._shared import _build_env_prefix

    prefix = _build_env_prefix({"FOO": "bar", "_UNDERSCORE": "x", "A1": "y"})
    # Values quoted, keys raw, no stray shell metas in the rendered output.
    assert "FOO=bar" in prefix
    assert "_UNDERSCORE=x" in prefix
    assert "A1=y" in prefix
    for meta in (";", "`", "$(", "&", "|"):
        assert meta not in prefix, f"shell metacharacter {meta!r} leaked: {prefix}"


def test_build_env_prefix_rejects_injection_attempt_end_to_end():
    """Regression: exact payload that triggered the finding — ensure a ``;``
    in a key can't land in the final rendered shell command."""
    from api.providers._shared import _build_env_prefix

    payload = {"FOO;touch /tmp/pwned;BAR": "v"}
    with pytest.raises(ValueError):
        _build_env_prefix(payload)


# ---------------------------------------------------------------------------
# Security — HTTP ingress rejects shell-metachar env keys + bad volume/subpath
# ---------------------------------------------------------------------------


def test_pop_env_and_secrets_rejects_shell_metachar_keys():
    """``_pop_env_and_secrets`` (used by /sessions/new, /sessions/{id}/resume,
    /sandboxes) must 400 on any non-POSIX env or secrets key.
    Defence-in-depth: ``_build_env_prefix`` rejects too, but we want the
    error surfaced at the HTTP layer so clients get a clean 400."""
    from fastapi import HTTPException

    from api.server import _pop_env_and_secrets

    for bad in ("FOO;evil", "FOO BAR", "FOO\nBAR", "-u EVIL", ""):
        with pytest.raises(HTTPException) as exc:
            _pop_env_and_secrets({"env": {bad: "v"}})
        assert exc.value.status_code == 400
        with pytest.raises(HTTPException) as exc:
            _pop_env_and_secrets({"secrets": {bad: "v"}})
        assert exc.value.status_code == 400


def test_pop_env_and_secrets_rejects_auth_key_smuggling():
    """``_pop_env_and_secrets`` must 400 when a known credential key (e.g.
    ANTHROPIC_API_KEY) is passed in ``env``.  Credentials must go in
    ``secrets`` instead, because ``env`` is stored plain and returned by GET."""
    from fastapi import HTTPException

    from api.server import _pop_env_and_secrets

    with pytest.raises(HTTPException) as exc:
        _pop_env_and_secrets({"env": {"ANTHROPIC_API_KEY": "x"}})
    assert exc.value.status_code == 400


def test_validate_subpath_rejects_docker_mount_injection():
    """Subpath flows into docker ``--mount ...,volume-subpath=<subpath>``.
    A value like ``foo,readonly`` would inject a second mount flag."""
    from fastapi import HTTPException

    from api.server import _validate_subpath

    # Valid paths pass.
    _validate_subpath("agents/abc/home")
    _validate_subpath("shared")

    # Injection / traversal must 400.
    for bad in (
        "foo,readonly",     # docker mount kv injection
        "foo readonly",     # whitespace
        "foo\nbar",         # newline
        "../etc/passwd",    # traversal
        "foo/../bar",       # mid-path traversal
        "",                 # empty
        "/absolute",        # leading slash (regex rejects)
        "foo;bar",          # shell-metachar (defence-in-depth)
    ):
        with pytest.raises(HTTPException) as exc:
            _validate_subpath(bad)
        assert exc.value.status_code == 400, f"should 400 on {bad!r}"


def test_validate_volume_name_rejects_path_escape():
    """Volume name flows into local provider filesystem layout AND docker
    argv. A ``../`` or ``/``-containing name could escape the volume root."""
    from fastapi import HTTPException

    from api.server import _validate_volume_name

    _validate_volume_name("my-vol_1")
    _validate_volume_name("abc")

    for bad in (
        "../etc",
        "foo/bar",
        "foo;rm",
        ".hidden",        # leading dot — excluded by regex
        "",
        "a" * 200,        # too long
        "foo bar",
    ):
        with pytest.raises(HTTPException) as exc:
            _validate_volume_name(bad)
        assert exc.value.status_code == 400, f"should 400 on {bad!r}"
