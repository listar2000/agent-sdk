"""Minimal direct-stdio ACP client for claude-agent-acp.

Spawns `node claude-agent-acp/dist/index.js` as a subprocess, talks ACP
(JSON-RPC 2.0 over newline-delimited JSON) over stdin/stdout. No HTTP,
no sandbox-agent. Bare minimum to drive session/prompt + session/cancel
with full streaming session/update notifications, and the push-based
queue that lets a second prompt slip into the current turn via
session.input.push.

Usage:
    client = AcpDirectClient(cwd="/tmp", acp_bin="/tmp/acp_direct/node_modules/.bin/claude-agent-acp")
    await client.start()
    await client.initialize()
    session_id = await client.new_session()
    # subscribe to notifications for this session
    async for ev in client.stream(session_id):
        print(ev)
    # send a prompt (concurrent-safe — upstream queues)
    rpc_id, task = await client.prompt(session_id, "say hi")
    result = await task
"""
from __future__ import annotations

import asyncio
import json
import os
import sys
import uuid
from dataclasses import dataclass, field
from typing import Any, AsyncIterator, Optional


@dataclass
class _Pending:
    future: asyncio.Future


class AcpDirectClient:
    def __init__(self, *, cwd: str = "/tmp", acp_bin: Optional[str] = None,
                 env: Optional[dict] = None):
        self.cwd = cwd
        self.acp_bin = acp_bin or os.environ.get(
            "ACP_BIN",
            "/tmp/acp_direct/node_modules/.bin/claude-agent-acp",
        )
        self.env = env or os.environ.copy()
        self.proc: Optional[asyncio.subprocess.Process] = None
        self._pending: dict[str, _Pending] = {}  # rpc_id -> Pending (for requests we sent)
        self._reader_task: Optional[asyncio.Task] = None
        self._stderr_task: Optional[asyncio.Task] = None
        # session_id -> asyncio.Queue for session/update notifications tagged
        # to that session. Queues are stamped by sessionId in params.
        self._session_queues: dict[str, asyncio.Queue] = {}
        # Handler for requests FROM the agent (e.g. fs/read_text_file for MCP
        # file capability). We're a headless client so we mostly reject these.
        self._closed = asyncio.Event()

    async def start(self) -> None:
        """Spawn the acp subprocess and start the reader."""
        self.proc = await asyncio.create_subprocess_exec(
            self.acp_bin,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=self.env,
            cwd=self.cwd,
        )
        self._reader_task = asyncio.create_task(self._read_loop(), name="acp-reader")
        self._stderr_task = asyncio.create_task(self._stderr_loop(), name="acp-stderr")

    async def _stderr_loop(self) -> None:
        """Forward stderr to our own stderr for visibility."""
        assert self.proc and self.proc.stderr
        async for line in self.proc.stderr:
            sys.stderr.write(f"[acp-stderr] {line.decode(errors='replace')}")
            sys.stderr.flush()

    async def _read_loop(self) -> None:
        """Read ndjson frames from the subprocess stdout and dispatch.

        - Response frames (have "id" and "result"/"error"): resolve the matching
          _pending future.
        - Notification frames (have "method" but no "id"): route by method.
          session/update gets fanned out to _session_queues by sessionId.
          Other notifications are currently ignored.
        - Request frames from the agent (have both "method" and "id"): reply
          with method-not-found since we're a bare-bones client.
        """
        assert self.proc and self.proc.stdout
        try:
            async for raw_line in self.proc.stdout:
                line = raw_line.decode(errors="replace").strip()
                if not line:
                    continue
                try:
                    msg = json.loads(line)
                except Exception as e:
                    sys.stderr.write(f"[acp-reader] parse error: {e} line={line!r}\n")
                    continue
                # dispatch
                if isinstance(msg, dict):
                    if "id" in msg and ("result" in msg or "error" in msg):
                        # response to a request we sent
                        rid = str(msg["id"])
                        pending = self._pending.pop(rid, None)
                        if pending and not pending.future.done():
                            if "error" in msg:
                                pending.future.set_exception(
                                    RuntimeError(f"ACP error: {msg['error']}")
                                )
                            else:
                                pending.future.set_result(msg.get("result"))
                        continue
                    if "method" in msg and "id" not in msg:
                        # notification from agent
                        method = msg.get("method")
                        params = msg.get("params") or {}
                        if method == "session/update":
                            sid = params.get("sessionId")
                            q = self._session_queues.get(sid)
                            if q is not None:
                                q.put_nowait(params)
                        # otherwise ignore
                        continue
                    if "method" in msg and "id" in msg:
                        # request from the agent — reject with method-not-found
                        rid = msg.get("id")
                        await self._send_raw({
                            "jsonrpc": "2.0",
                            "id": rid,
                            "error": {"code": -32601, "message": "Method not found"},
                        })
                        continue
        except Exception as e:
            sys.stderr.write(f"[acp-reader] loop error: {e!r}\n")
        finally:
            self._closed.set()

    async def _send_raw(self, payload: dict) -> None:
        assert self.proc and self.proc.stdin
        data = (json.dumps(payload) + "\n").encode()
        self.proc.stdin.write(data)
        await self.proc.stdin.drain()

    async def _request(self, method: str, params: dict) -> dict:
        rid = str(uuid.uuid4())
        fut: asyncio.Future = asyncio.get_running_loop().create_future()
        self._pending[rid] = _Pending(future=fut)
        await self._send_raw({
            "jsonrpc": "2.0",
            "id": rid,
            "method": method,
            "params": params,
        })
        return await fut

    async def _notify(self, method: str, params: dict) -> None:
        await self._send_raw({"jsonrpc": "2.0", "method": method, "params": params})

    # ── Public API ─────────────────────────────────────────────────

    async def initialize(self) -> dict:
        return await self._request("initialize", {"protocolVersion": 1})

    async def new_session(self, *, cwd: Optional[str] = None,
                          mcp_servers: Optional[list] = None) -> str:
        result = await self._request("session/new", {
            "cwd": cwd or self.cwd,
            "mcpServers": mcp_servers or [],
        })
        sid = result["sessionId"]
        self._session_queues[sid] = asyncio.Queue()
        return sid

    async def set_mode(self, session_id: str, mode_id: str) -> None:
        await self._request("session/set_mode", {
            "sessionId": session_id, "modeId": mode_id,
        })

    async def prompt(self, session_id: str, message: str,
                      rpc_id: Optional[str] = None) -> tuple[str, asyncio.Future]:
        """Submit a session/prompt request. Returns (rpc_id, future).
        The future resolves with the {stopReason, usage} result when the
        turn ends. Multiple concurrent prompts on the same session are
        allowed: they go into the upstream pendingMessages queue and run
        in submission order.
        """
        if rpc_id is None:
            rpc_id = str(uuid.uuid4())
        fut: asyncio.Future = asyncio.get_running_loop().create_future()
        self._pending[rpc_id] = _Pending(future=fut)
        await self._send_raw({
            "jsonrpc": "2.0",
            "id": rpc_id,
            "method": "session/prompt",
            "params": {
                "sessionId": session_id,
                "prompt": [{"type": "text", "text": message}],
            },
        })
        return rpc_id, fut

    async def cancel(self, session_id: str) -> None:
        """Send session/cancel notification (no id, fire-and-forget)."""
        await self._notify("session/cancel", {"sessionId": session_id})

    async def stream(self, session_id: str) -> AsyncIterator[dict]:
        """Async iterator of session/update params for this session."""
        q = self._session_queues.get(session_id)
        if q is None:
            raise RuntimeError(f"unknown session_id {session_id}")
        while True:
            item = await q.get()
            yield item

    async def close(self) -> None:
        if self.proc and self.proc.stdin and not self.proc.stdin.is_closing():
            try:
                self.proc.stdin.close()
            except Exception:
                pass
        if self.proc:
            try:
                await asyncio.wait_for(self.proc.wait(), timeout=5)
            except asyncio.TimeoutError:
                self.proc.kill()
                await self.proc.wait()
        for t in (self._reader_task, self._stderr_task):
            if t and not t.done():
                t.cancel()
                try:
                    await t
                except (asyncio.CancelledError, Exception):
                    pass


# ── Smoke test ─────────────────────────────────────────────────

async def _smoke() -> None:
    client = AcpDirectClient(cwd="/tmp")
    await client.start()
    print("[smoke] spawned acp subprocess", flush=True)

    init = await client.initialize()
    print(f"[smoke] initialize → capabilities={list(init.get('agentCapabilities') or init.get('capabilities') or {})}", flush=True)

    sid = await client.new_session()
    print(f"[smoke] session/new → {sid}", flush=True)

    try:
        await client.set_mode(sid, "bypassPermissions")
        print("[smoke] set_mode bypassPermissions OK", flush=True)
    except Exception as e:
        print(f"[smoke] set_mode failed: {e}", flush=True)

    # Listen in the background
    events = []

    async def listener():
        async for ev in client.stream(sid):
            events.append(ev)
            upd = ev.get("update") or {}
            ut = upd.get("sessionUpdate", "?")
            preview = ""
            if ut in ("agent_message_chunk", "agent_thought_chunk"):
                preview = str(upd.get("content", {}).get("text", ""))[:60]
            print(f"[smoke] update {ut} {preview}", flush=True)

    lt = asyncio.create_task(listener())

    # Single prompt
    rpc_a, task_a = await client.prompt(sid, "Reply with exactly: SMOKE-OK")
    print(f"[smoke] submitted rpc_a={rpc_a[:8]}", flush=True)
    result_a = await asyncio.wait_for(task_a, timeout=60)
    print(f"[smoke] terminal A: {result_a}", flush=True)

    lt.cancel()
    try:
        await lt
    except asyncio.CancelledError:
        pass

    await client.close()
    print("[smoke] done", flush=True)


if __name__ == "__main__":
    asyncio.run(_smoke())
