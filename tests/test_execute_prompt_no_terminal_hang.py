"""Regression repro for the silent ~1h cold-recovery hang (Task #86 RCA).

WHAT THE BUG IS
---------------
The trigger is a HUNG-BUT-NOT-EXITED ACP child (deadlocked / stuck on a
syscall / OOM-thrashing but not killed) — NOT a pause/resume and NOT a clean
crash:

  * pause/resume re-attaches: start() -> _attach_acp() -> session/load
    restores the conversation onto the respawned child.
  * a clean child crash is caught: supervisor.js process.exit()s with the
    child, so /v1/health goes unreachable, running() returns False, and the
    pool cold-recovers a fresh session (re-attach + session/load).

A *wedged* child is the gap. supervisor.js reports ``acp_alive:
acp.exitCode === null`` — i.e. merely "the process still exists" — so a
deadlocked child still returns ``200 + acp_alive:true``. The pool's liveness
probe is supervisor-only (``GET /v1/health``); it never round-trips the ACP
session, so it can't tell a wedged child from a healthy one. ``running()``
says alive, the cached session is reused, and the prompt is driven against
the wedged child.

supervisor.js ignores the session id in the URL and just fans out the single
ACP child's stdout, so ``GET /v1/acp/{id}`` returns 200 and streams only 25s
heartbeats — and a wedged child emits no ``done``/``error`` terminal frame.
``BaseSandboxSession.execute_prompt`` opens that stream with ``read=None``
(api/sse.py: the SSE GET has no read timeout) and loops forever waiting for a
terminal event that never comes. From the client this is "send_message
returned 200, agent never responded" until its own hour-long ``wait_for``
fires. Restarting the agent spawns a fresh child, which is why "I have to
restart it to keep chatting" works around it.

WHY THIS TEST EXISTS
--------------------
``tests/test_acp_error_terminates_turn.py`` already pins termination for
every case where a terminal frame *does* arrive (done / error / tool
failure). The hang is the one case nothing covers: the stream NEVER
produces a terminal frame. This drives the real ``execute_prompt`` against
a fake supervisor that mimics the forgotten-session wire behaviour and
asserts the contract a healthy system must hold:

    a prompt drive must surface a terminal event within a bounded
    deadline even when the agent never emits one.

Today it does not, so the bug test is ``xfail(strict=True)``: it documents
the defect without reddening CI, and flips to a hard failure (XPASS) the
moment a fix makes ``execute_prompt`` bound its wait — forcing the marker
to be removed.

NOTE ON FIX SCOPE: ``fix/probe-honors-acp-alive`` does NOT make this pass —
it evicts sessions whose *process* is dead (``acp_alive: false``). A wedged
child still reports ``acp_alive: true`` (exitCode is null), so the only thing
that closes this is a first/terminal-event deadline in the drive itself (or
a real ACP responsiveness ping, not a process-exists check).
"""
from __future__ import annotations

import asyncio
import contextlib
import json
import os
import socket
import sys
import threading
import time
from typing import Iterator

import pytest

from api.sandbox import BaseSandboxSession, Recipe, UnixLocalSandboxState


# ---------------------------------------------------------------------------
# Fake supervisor — mimics supervisor.js's HTTP contract for one session.
# ---------------------------------------------------------------------------
#
# Two GET modes:
#   "silent" — heartbeats forever, no terminal frame (the forgotten-session
#              bug: child is alive, never answers for this rpc_id).
#   "done"   — a real ``done`` envelope after a beat (control: proves the
#              harness can reach termination, so a timeout in the bug test
#              is the defect, not the rig).
#
# POST /v1/acp/{id}  -> 200 (supervisor accepts the prompt write; the
#                       forgotten session means the child silently drops it).
# GET  /v1/health    -> 200 {acp_alive: true}  (the masking signal: the
#                       probe that today's pool trusts says "alive").

_DONE_ENVELOPE = json.dumps(
    {"jsonrpc": "2.0", "id": "r1", "result": {"stopReason": "end_turn"}}
)


def _make_app(mode: str):
    from starlette.applications import Starlette
    from starlette.responses import JSONResponse, StreamingResponse
    from starlette.routing import Route

    async def acp_get(request):
        async def stream():
            # First heartbeat immediately so the stream is established.
            yield b": heartbeat\n\n"
            if mode == "done":
                await asyncio.sleep(0.05)
                yield f"data: {_DONE_ENVELOPE}\n\n".encode()
                return
            # mode == "silent": beat forever, never a terminal frame.
            while True:
                await asyncio.sleep(0.05)
                yield b": heartbeat\n\n"

        return StreamingResponse(stream(), media_type="text/event-stream")

    async def acp_post(request):
        # Read+discard the prompt body; supervisor would write it to the ACP
        # child's stdin and return 200 regardless of whether the child knows
        # the session.
        await request.body()
        return JSONResponse({"ok": True})

    async def health(request):
        return JSONResponse({"status": "ok", "acp_pid": 1, "acp_alive": True})

    return Starlette(routes=[
        Route("/v1/acp/{sid}", acp_get, methods=["GET"]),
        Route("/v1/acp/{sid}", acp_post, methods=["POST"]),
        Route("/v1/health", health, methods=["GET"]),
    ])


def _free_port() -> int:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


@contextlib.contextmanager
def _fake_supervisor(mode: str) -> Iterator[str]:
    import uvicorn

    port = _free_port()
    config = uvicorn.Config(
        _make_app(mode), host="127.0.0.1", port=port,
        log_level="error", loop="asyncio",
    )
    server = uvicorn.Server(config)
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    try:
        deadline = time.time() + 10
        while not server.started and time.time() < deadline:
            time.sleep(0.02)
        if not server.started:
            raise RuntimeError("fake supervisor failed to start")
        yield f"http://127.0.0.1:{port}"
    finally:
        server.should_exit = True
        thread.join(timeout=5)


# ---------------------------------------------------------------------------
# Minimal concrete session — inherits the REAL base ``execute_prompt``
# (base-only, not overridden by any provider), which is the code under test.
# ---------------------------------------------------------------------------

class _StubSession(BaseSandboxSession):
    volume_provider = "unix_local"

    async def start(self) -> None:  # pragma: no cover - not exercised
        pass

    async def running(self, *, force_probe: bool = False) -> bool:
        return True

    async def _liveness_probe(self) -> bool:
        return True

    async def stop(self) -> None:  # pragma: no cover - not exercised
        pass

    async def shutdown(self) -> None:  # pragma: no cover - not exercised
        pass


def _make_session(supervisor_url: str) -> _StubSession:
    sess = _StubSession(
        session_id="sess-stale-acp",
        state=UnixLocalSandboxState(recipe=Recipe()),
    )
    # Post-resume state: supervisor reachable, but these ids point at a
    # session the respawned ACP child no longer has.
    sess._supervisor_url = supervisor_url
    sess._acp_session_id = "stale-acp-session-id"
    sess._inner_session_id = "stale-inner-session-id"
    return sess


async def _drive(sess: _StubSession) -> list:
    return [ev async for ev in sess.execute_prompt("ping", rpc_id="r1")]


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_terminal_event_completes_promptly():
    """Control: a terminal ``done`` frame ends the real drive promptly.

    If this passes and the sibling test times out, the timeout is the bug,
    not the test harness."""
    with _fake_supervisor("done") as base_url:
        sess = _make_session(base_url)
        events = await asyncio.wait_for(_drive(sess), timeout=10.0)
    assert events, "expected at least the terminal event"
    assert events[-1]["type"] == "done"
    assert events[-1]["stop_reason"] == "end_turn"


@pytest.mark.xfail(
    strict=True,
    reason=(
        "RCA Task #86: execute_prompt has no terminal-event deadline. A "
        "wedged-but-alive ACP child (health 200 + acp_alive:true, but emits "
        "no done/error) + read=None on the SSE GET hangs the drive "
        "indefinitely. No fix on main closes the drive itself "
        "(probe-honors-acp-alive can't see a wedged child). Remove this "
        "marker when execute_prompt bounds the wait for the first/terminal "
        "event."
    ),
)
@pytest.mark.asyncio
async def test_heartbeat_only_stream_must_not_hang():
    """THE BUG. Supervisor is up (``acp_alive: true`` — the child process
    exists) and accepts the prompt POST (200), but the child is WEDGED, so
    the stream is heartbeats forever with no terminal frame. The
    supervisor-only health probe can't tell a wedged child from a healthy one.

    Contract: the drive must surface a terminal event within a bounded
    deadline even when the agent never emits one. Today it hangs, so
    ``wait_for`` raises ``TimeoutError`` and this xfails."""
    with _fake_supervisor("silent") as base_url:
        sess = _make_session(base_url)
        events = await asyncio.wait_for(_drive(sess), timeout=6.0)
    assert events and events[-1]["type"] in ("done", "error"), (
        "execute_prompt must terminate with a done/error event, not hang "
        "on a heartbeat-only stream"
    )
