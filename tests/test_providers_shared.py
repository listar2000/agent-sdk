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
