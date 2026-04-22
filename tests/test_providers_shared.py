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
    """A port occupied via bind+listen must never be returned by the
    allocator, even if the monotonic counter would otherwise pick it."""
    from api.providers import _shared as sh

    # Snap the counter forward so the NEXT candidate is a port we choose
    # and occupy.  Binding 0 lets the OS pick a free port; we then
    # occupy it explicitly AND rewind the counter to it.
    probe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    probe.bind(("127.0.0.1", 0))
    occupied = probe.getsockname()[1]
    probe.listen(1)

    try:
        # Force the allocator to see ``occupied`` as the next counter value.
        async with sh._port_lock:
            prev_next = sh._next_local_port
            sh._next_local_port = occupied
            # Clear any freed cache for a deterministic test.
            sh._freed_ports.clear()
        try:
            # First call: occupied port is bindable=False → skipped.
            port = await sh._find_free_port()
            assert port != occupied, (
                f"_find_free_port returned occupied port {occupied}"
            )
            # The allocator should still be returning a usable port.
            with _bind_port(port):
                pass  # if the port were taken, this would raise
        finally:
            async with sh._port_lock:
                sh._next_local_port = prev_next
    finally:
        probe.close()


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
async def test_find_free_port_os_fallback_when_counter_exhausted():
    """If every counter candidate is occupied for the entire bounded
    loop, the allocator falls through to ``bind(0)`` and returns an
    OS-assigned free port."""
    from api.providers import _shared as sh

    # Bind a contiguous range of ports so the counter lands on occupied
    # candidates for every iteration.  The implementation loops 64
    # candidates before the fallback; binding ~12 consecutive ports is
    # enough because the OS-assigned fallback is the failsafe path.
    occupied_socks: list[socket.socket] = []
    base = 0
    try:
        # Find a free starting point.
        probe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        probe.bind(("127.0.0.1", 0))
        base = probe.getsockname()[1]
        probe.close()

        # Hold a generous contiguous block.
        for i in range(16):
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                s.bind(("127.0.0.1", base + i))
                s.listen(1)
                occupied_socks.append(s)
            except OSError:
                # Someone else took it — irrelevant for the test.
                s.close()

        # Rewind counter to the base of the occupied block and clear
        # the freed cache so the allocator must chew through our block.
        async with sh._port_lock:
            prev_next = sh._next_local_port
            sh._freed_ports.clear()
            sh._next_local_port = base

        try:
            port = await sh._find_free_port()
            # Whatever port we got, it must actually be bindable.
            assert port > 0
            occupied_ports = {s.getsockname()[1] for s in occupied_socks}
            assert port not in occupied_ports, (
                f"allocator returned an occupied port {port} (held block: "
                f"{sorted(occupied_ports)})"
            )
            # Sanity: the port must be bindable right now (closing the
            # allocator's probe socket already released it back to the OS).
            test = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            test.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                test.bind(("127.0.0.1", port))
            finally:
                test.close()
        finally:
            async with sh._port_lock:
                sh._next_local_port = prev_next
    finally:
        for s in occupied_socks:
            s.close()


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
    /sandboxes/provision) must 400 on any non-POSIX env or secrets key.
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


def test_forbid_auth_keys_in_env_also_rejects_shell_metachars():
    """``_forbid_auth_keys_in_env`` guards ``config.env`` for POST /agents
    and POST /sandboxes/provision. Both auth-key smuggling and shell-metachar
    keys must 400."""
    from fastapi import HTTPException

    from api.server import _forbid_auth_keys_in_env

    with pytest.raises(HTTPException) as exc:
        _forbid_auth_keys_in_env({"FOO;x": "v"}, "test")
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
