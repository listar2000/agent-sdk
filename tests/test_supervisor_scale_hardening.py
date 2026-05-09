"""Targeted tests for the supervisor.js scale-hardening fixes.

Exercises the failure modes introduced by missing client-disconnect
cleanup paths. Run against the local supervisor.js with a dummy ACP
(`/bin/cat`) — `/bin/cat` echoes every line of stdin back on stdout,
which is enough to drive supervisor.js's stdout-line parser without
needing the real claude-agent-acp binary.

Two scenarios:

1. ``pendingResponses`` cleanup on client disconnect — POST with an
   ``id`` registers a resolver that's only normally cleared when ACP
   echoes back a ``result``/``error`` envelope. ``/bin/cat`` mirrors
   the request line verbatim, which has neither, so the resolver
   would leak forever without ``req.on('close')`` cleanup.

2. ``handleExec`` kills the bash child on client disconnect — a 30 s
   sleep that's cancelled mid-flight at the HTTP layer must not keep
   running inside the sandbox.
"""
from __future__ import annotations

import os
import shutil
import socket
import subprocess
import time

import httpx
import pytest


_SUP_JS = os.path.join(
    os.path.dirname(__file__), "..", "src", "supervisor", "supervisor.js"
)


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _wait_for_health(url: str, timeout: float = 5.0) -> None:
    deadline = time.time() + timeout
    last_err: Exception | None = None
    while time.time() < deadline:
        try:
            r = httpx.get(f"{url}/v1/health", timeout=0.5)
            if r.status_code == 200:
                return
        except Exception as e:  # noqa: BLE001
            last_err = e
        time.sleep(0.05)
    raise RuntimeError(f"supervisor didn't come up on {url}: {last_err}")


@pytest.fixture
def supervisor_proc(tmp_path):
    if shutil.which("node") is None:
        pytest.skip("node not on PATH")

    port = _free_port()
    root = tmp_path / "root"
    root.mkdir()

    proc = subprocess.Popen(
        ["node", _SUP_JS,
         "--acp", "/bin/cat",
         "--root", str(root),
         "--port", str(port)],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    )
    url = f"http://127.0.0.1:{port}"
    try:
        _wait_for_health(url)
        yield url, root, port
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=3)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=2)


def _pending_responses(url: str) -> int:
    return httpx.get(f"{url}/v1/health", timeout=2).json()["pending_responses"]


def test_pending_responses_cleanup_on_client_disconnect(supervisor_proc):
    """Without req.on('close') cleanup in handlePost, a request whose ACP
    response never arrives leaves a resolver in pendingResponses forever.
    /bin/cat echoes the request line verbatim — same JSON. We deliberately
    omit ``method`` so the echoed line matches NEITHER ``handleAcpLine``
    branch: not a client-initiated ACP request (no method) and not a
    response (no result/error). The resolver therefore never fires; only
    the ``res.on('close')`` cleanup path can drain it.
    The fix must clear the entry when the upstream client disconnects.
    """
    url, _root, port = supervisor_proc

    assert _pending_responses(url) == 0

    # Send a request with an id (but no method/result/error), then close
    # the socket before the (never-arriving) response. Use a raw socket
    # so we can drop it cleanly mid-await without the request library
    # trying to read the response.
    body = b'{"jsonrpc":"2.0","id":42}'
    request = (
        b"POST /v1/acp/sess-1 HTTP/1.1\r\n"
        b"Host: 127.0.0.1\r\n"
        b"Content-Type: application/json\r\n"
        b"Content-Length: " + str(len(body)).encode() + b"\r\n"
        b"Connection: close\r\n"
        b"\r\n"
        + body
    )
    s = socket.create_connection(("127.0.0.1", port), timeout=2)
    s.sendall(request)

    # Give the supervisor a moment to register the resolver.
    deadline = time.time() + 2
    while time.time() < deadline:
        if _pending_responses(url) == 1:
            break
        time.sleep(0.05)
    assert _pending_responses(url) == 1, "resolver should be registered"

    # Drop the connection. With the fix, req.on('close') fires and the
    # entry is removed; without the fix, it stays in the Map forever.
    s.close()

    deadline = time.time() + 3
    last = None
    while time.time() < deadline:
        last = _pending_responses(url)
        if last == 0:
            return
        time.sleep(0.05)
    pytest.fail(
        f"pendingResponses didn't drain after client disconnect (still {last})"
    )


def test_handle_exec_kills_child_on_client_disconnect(supervisor_proc):
    """handleExec spawns bash; without req.on('close'), a client that
    cancels mid-exec strands the bash subprocess for up to the configured
    timeout (≤300s). The fix must SIGKILL the child as soon as the
    upstream socket closes.

    Strategy: command writes ``started`` immediately, then sleeps, then
    writes ``finished``. We disconnect during the sleep. With the fix,
    only ``started`` lands; without the fix, ``finished`` would also
    appear once bash naturally completes.
    """
    url, root, _port = supervisor_proc
    marker = root / "exec_marker.txt"
    cmd = (
        f"echo started > {marker} && "
        f"sleep 5 && "
        f"echo finished >> {marker}"
    )

    # httpx with a tight timeout — the supervisor's response only lands
    # after the bash child closes its pipes, which is after the sleep.
    # We want to cut the connection while the sleep is still in flight.
    body = {"command": cmd, "timeout": 30}
    with pytest.raises((httpx.ReadTimeout, httpx.RemoteProtocolError, httpx.TimeoutException)):
        httpx.post(f"{url}/v1/exec", json=body, timeout=0.5)

    # Give the supervisor's req.on('close') handler time to fire and
    # SIGKILL bash. Then wait an interval longer than the sleep duration
    # to confirm the post-sleep echo never lands.
    time.sleep(7)

    assert marker.exists(), "marker should exist (started stage ran)"
    contents = marker.read_text()
    assert "started" in contents, f"first echo missing: {contents!r}"
    assert "finished" not in contents, (
        f"bash child wasn't killed on client disconnect: {contents!r}"
    )

    # And the supervisor itself must still be healthy.
    r = httpx.get(f"{url}/v1/health", timeout=2)
    assert r.status_code == 200, f"supervisor unhealthy after disconnect: {r.text}"
