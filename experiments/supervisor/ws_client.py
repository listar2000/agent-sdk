"""WebSocket ACP client — connects to a supervisor at ws://host:port/acp,
then drives a full ACP session (initialize, session/new, set_mode, prompt).
"""
from __future__ import annotations

import asyncio
import json
import sys
import time
import uuid

import httpx
import websockets


class AcpWsClient:
    def __init__(self, ws_url: str, cwd: str = "/tmp"):
        self.ws_url = ws_url
        self.cwd = cwd
        self.ws = None
        self._pending: dict[str, asyncio.Future] = {}
        self._sessions: dict[str, asyncio.Queue] = {}
        self._reader_task = None

    async def connect(self):
        self.ws = await websockets.connect(self.ws_url)
        self._reader_task = asyncio.create_task(self._read_loop())

    async def _read_loop(self):
        try:
            async for raw in self.ws:
                line = raw.strip() if isinstance(raw, str) else raw.decode(errors="replace").strip()
                if not line:
                    continue
                try:
                    msg = json.loads(line)
                except Exception:
                    print(f"[client] parse fail: {line[:120]!r}", file=sys.stderr, flush=True)
                    continue
                if "id" in msg and ("result" in msg or "error" in msg):
                    rid = str(msg["id"])
                    f = self._pending.pop(rid, None)
                    if f and not f.done():
                        if "error" in msg:
                            f.set_exception(RuntimeError(f"acp error: {msg['error']}"))
                        else:
                            f.set_result(msg.get("result"))
                    continue
                if "method" in msg and "id" not in msg:
                    m = msg["method"]
                    p = msg.get("params") or {}
                    if m == "session/update":
                        sid = p.get("sessionId")
                        q = self._sessions.get(sid)
                        if q:
                            q.put_nowait(p)
                    continue
                if "method" in msg and "id" in msg:
                    # request from agent — reject
                    await self._send({
                        "jsonrpc": "2.0",
                        "id": msg["id"],
                        "error": {"code": -32601, "message": "Method not found"},
                    })
        except Exception as e:
            print(f"[client] read loop err: {e!r}", file=sys.stderr, flush=True)

    async def _send(self, payload: dict):
        await self.ws.send(json.dumps(payload))

    async def _request(self, method: str, params: dict) -> dict:
        rid = str(uuid.uuid4())
        fut = asyncio.get_running_loop().create_future()
        self._pending[rid] = fut
        await self._send({"jsonrpc": "2.0", "id": rid, "method": method, "params": params})
        return await fut

    async def _notify(self, method: str, params: dict):
        await self._send({"jsonrpc": "2.0", "method": method, "params": params})

    async def initialize(self):
        return await self._request("initialize", {"protocolVersion": 1})

    async def new_session(self) -> str:
        r = await self._request("session/new", {"cwd": self.cwd, "mcpServers": []})
        sid = r["sessionId"]
        self._sessions[sid] = asyncio.Queue()
        return sid

    async def set_mode(self, sid: str, mode: str):
        await self._request("session/set_mode", {"sessionId": sid, "modeId": mode})

    async def prompt(self, sid: str, text: str):
        rid = str(uuid.uuid4())
        fut = asyncio.get_running_loop().create_future()
        self._pending[rid] = fut
        await self._send({
            "jsonrpc": "2.0", "id": rid, "method": "session/prompt",
            "params": {"sessionId": sid, "prompt": [{"type": "text", "text": text}]},
        })
        return rid, fut

    async def cancel(self, sid: str):
        await self._notify("session/cancel", {"sessionId": sid})

    async def stream(self, sid: str):
        q = self._sessions[sid]
        while True:
            yield await q.get()

    async def close(self):
        if self._reader_task:
            self._reader_task.cancel()
            try:
                await self._reader_task
            except (asyncio.CancelledError, Exception):
                pass
        if self.ws:
            await self.ws.close()


async def run_session_test(base_http: str, ws_url: str, label: str):
    print(f"\n=== {label} ===", flush=True)
    print(f"[test] health: {base_http}/health", flush=True)
    async with httpx.AsyncClient(timeout=10) as h:
        hr = await h.get(f"{base_http}/health")
    print(f"[test] health = {hr.json()}", flush=True)

    client = AcpWsClient(ws_url)
    await client.connect()
    print("[test] ws connected", flush=True)

    init = await client.initialize()
    print(f"[test] initialize ok, capabilities_keys={list(init.get('agentCapabilities') or {})}", flush=True)

    sid = await client.new_session()
    print(f"[test] session = {sid}", flush=True)

    await client.set_mode(sid, "bypassPermissions")
    print("[test] set_mode bypassPermissions ok", flush=True)

    # Background listener
    text_buf = []
    tool_events = []

    async def listen():
        async for ev in client.stream(sid):
            upd = ev.get("update") or {}
            ut = upd.get("sessionUpdate")
            if ut == "agent_message_chunk":
                text_buf.append(upd.get("content", {}).get("text", ""))
            elif ut == "tool_call":
                tool_events.append(("call", upd.get("title")))
            elif ut == "tool_call_update":
                tool_events.append(("update", upd.get("status")))

    lt = asyncio.create_task(listen())

    # Send a real prompt that exercises tools
    t0 = time.monotonic()
    rpc, fut = await client.prompt(sid,
        "Use Bash to run `echo REMOTE-OK && date`. Then say: FINAL-LINE <the echo output>.")
    print(f"[test] submitted rpc={rpc[:8]}", flush=True)
    result = await asyncio.wait_for(fut, timeout=120)
    t1 = time.monotonic()
    print(f"[test] terminal t={t1-t0:.1f}s stop={result.get('stopReason')} usage_out={result.get('usage',{}).get('outputTokens')}", flush=True)
    text = "".join(text_buf)
    print(f"[test] final text: {text[:200]}", flush=True)
    print(f"[test] tool events: {tool_events[:6]}", flush=True)
    assert "REMOTE-OK" in text or "FINAL-LINE" in text, f"no expected marker in text={text!r}"
    print(f"[test] PASS", flush=True)

    lt.cancel()
    try:
        await lt
    except asyncio.CancelledError:
        pass
    await client.close()


async def main():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", required=True, help="http[s]://host:port")
    ap.add_argument("--label", default="test")
    args = ap.parse_args()
    base = args.base.rstrip("/")
    ws_url = base.replace("http://", "ws://").replace("https://", "wss://") + "/acp"
    await run_session_test(base, ws_url, args.label)

if __name__ == "__main__":
    asyncio.run(main())
