"""Deterministic BEHAVIORAL coverage for the terminal-less-recovery bug.

The unit tests in ``test_persist_parser_parity.py`` mock the session's
``execute_prompt`` outright. This file instead drives the REAL
``BaseSandboxSession.execute_prompt`` SSE-read loop against a fake supervisor
that EOFs the ``/v1/acp`` stream cleanly WITHOUT a done/error event — the exact
condition a dying daytona supervisor produces (its proxy closes the connection
gracefully, no terminal). That is the wire-level shape the live golden
``test_midprompt_recovery_does_not_leak_subscriber[*-daytona]`` only hits
non-deterministically (it depends on daytona's proxy timing); here it is
forced every run, no live infra.

Pre-fix: ``_drive_one`` returned ``(True, None)`` on the clean EOF, the run
reported success, no terminal was persisted, and the client polling /log waited
forever — a silently DROPPED prompt. Post-fix: the terminal-less end is
reported as failure, recovery re-drives on the pool replacement, and the real
turn_end is delivered (or an error terminal when no replacement exists).
"""
from __future__ import annotations

import asyncio
import json

import pytest
from aiohttp import web

from api.sandbox.session import BaseSandboxSession


class _NoopLiveness:
    _last_compute_at = None

    def observe_chunk(self) -> None: ...
    def observe_prompt_start(self) -> None: ...
    def observe_prompt_end(self) -> None: ...


class _RealExecPromptSession:
    """Duck-typed session that runs the REAL execute_prompt against a fake
    supervisor URL — so the actual SSE-read / EOF-without-terminal code runs,
    not a stand-in generator."""

    # Bind the real async-generator method; called as self.execute_prompt(...).
    execute_prompt = BaseSandboxSession.execute_prompt

    def __init__(self, supervisor_url: str) -> None:
        self._supervisor_url = supervisor_url
        self._acp_session_id = "acp-1"
        self._inner_session_id = "inner-1"
        self._agent_id = "agent-x"
        self.session_id = "sess-eof"
        self._prompt_lock = asyncio.Lock()
        self.liveness = _NoopLiveness()
        self.broadcasts: list = []

    def _broadcast(self, item) -> None:
        self.broadcasts.append(item)


class _ReplacementSession:
    """The cold-recovered replacement: yields a real done so the retry's
    turn_end can be delivered."""

    def __init__(self) -> None:
        self.session_id = "sess-eof"
        self._agent_id = "agent-x"
        self._prompt_lock = asyncio.Lock()
        self.liveness = _NoopLiveness()

    async def execute_prompt(self, message: str, *, rpc_id: str):
        yield {"type": "text", "text": "recovered reply"}
        yield {"type": "done", "stop_reason": "end_turn"}

    def _broadcast(self, _item) -> None:
        pass


async def _start_fake_supervisor(*, send_text: bool = True):
    """A fake supervisor whose GET /v1/acp/{id} streams (optionally) one text
    event then closes the SSE response WITHOUT a done/error — a clean EOF, no
    terminal. POST /v1/acp/{id} accepts the prompt."""
    routes = web.RouteTableDef()

    @routes.get("/v1/acp/{sid}")
    async def _sse(request: web.Request) -> web.StreamResponse:
        resp = web.StreamResponse(
            headers={"Content-Type": "text/event-stream",
                     "Cache-Control": "no-cache"})
        await resp.prepare(request)
        if send_text:
            block = json.dumps({
                "jsonrpc": "2.0", "method": "session/update",
                "params": {"sessionUpdate": "agent_message_chunk",
                           "content": {"type": "text", "text": "partial reply"}},
            })
            await resp.write(f"data: {block}\n\n".encode())
        # Close WITHOUT a done/error — the terminal-less clean EOF.
        await resp.write_eof()
        return resp

    @routes.post("/v1/acp/{sid}")
    async def _post(_request: web.Request) -> web.Response:
        return web.json_response({"ok": True})

    app = web.Application()
    app.add_routes(routes)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    port = site._server.sockets[0].getsockname()[1]
    return runner, f"http://127.0.0.1:{port}"


def _capture_persist(monkeypatch) -> list:
    rows: list = []

    async def _fake_log_event(*, session_id, agent_id, event_type, payload):
        rows.append((event_type, payload))

    import api.turn as _turn
    monkeypatch.setattr(_turn, "log_event", _fake_log_event)
    return rows


@pytest.mark.asyncio
async def test_real_execute_prompt_terminalless_eof_recovers_turn_end(monkeypatch):
    """REAL execute_prompt hits a clean SSE EOF with no terminal; with a
    recovered replacement available the turn MUST deliver the real turn_end."""
    from api.turn import TurnRunner

    rows = _capture_persist(monkeypatch)
    runner, url = await _start_fake_supervisor()
    try:
        sess = _RealExecPromptSession(url)
        replacement = _ReplacementSession()

        class _RecoveredPool:
            _active = {sess.session_id: sess}

            async def get_session(self, _sid):
                return replacement

        import api.sandbox as _sb
        monkeypatch.setattr(_sb, "get_pool", lambda: _RecoveredPool())

        await TurnRunner(sess, "do a thing", "rpc-eof").run()
    finally:
        await runner.cleanup()

    etypes = [et for et, _ in rows]
    assert "turn_end" in etypes, (
        "the real execute_prompt SSE stream EOF'd without a terminal and the "
        "turn was NOT recovered — the prompt is silently dropped. "
        f"persisted: {etypes}"
    )


@pytest.mark.asyncio
async def test_real_execute_prompt_terminalless_eof_writes_error_terminal(monkeypatch):
    """Same clean EOF, but NO replacement available — the run must still write
    an `error` terminal so the client unblocks, never claim silent success."""
    from api.turn import TurnRunner

    rows = _capture_persist(monkeypatch)
    runner, url = await _start_fake_supervisor(send_text=False)
    try:
        sess = _RealExecPromptSession(url)

        class _NoReplacementPool:
            _active = {sess.session_id: sess}

            async def get_session(self, _sid):
                raise RuntimeError("no live session")

        import api.sandbox as _sb
        monkeypatch.setattr(_sb, "get_pool", lambda: _NoReplacementPool())

        await TurnRunner(sess, "do a thing", "rpc-eof2").run()
    finally:
        await runner.cleanup()

    etypes = [et for et, _ in rows]
    assert any(et in ("turn_end", "error") for et in etypes), (
        "no terminal persisted for a real terminal-less SSE EOF with no "
        f"replacement — prompt dropped. persisted: {etypes}"
    )
