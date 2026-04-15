"""Minimal ACP supervisor — spawns claude-agent-acp, bridges stdio ↔ WebSocket.

One subprocess per supervisor instance. One WebSocket client at a time (this
is a spike, not production). Client sends JSON-RPC frames as WS text; we
write them to subprocess stdin. Subprocess stdout lines are streamed back
to the client as WS text. stderr goes to our own stderr.

Run:
    python supervisor.py --port 9000 --acp /path/to/claude-agent-acp

WebSocket endpoint: ws://host:9000/acp
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse
import uvicorn

log = logging.getLogger("supervisor")

# Global state (spike — one subprocess per supervisor)
_state: dict = {
    "proc": None,
    "ws": None,
    "reader_task": None,
    "stderr_task": None,
    "acp_bin": None,
}


async def _start_acp() -> asyncio.subprocess.Process:
    bin_path = _state["acp_bin"]
    if not bin_path:
        raise RuntimeError("acp binary path not configured")
    env = os.environ.copy()
    proc = await asyncio.create_subprocess_exec(
        bin_path,
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        env=env,
        cwd="/tmp",
    )
    log.info("spawned claude-agent-acp pid=%d", proc.pid)
    return proc


async def _forward_stdout_to_ws(proc: asyncio.subprocess.Process) -> None:
    """Read ndjson lines from subprocess stdout, forward to WS."""
    try:
        while True:
            line = await proc.stdout.readline()
            if not line:
                break
            ws = _state["ws"]
            if ws is None:
                log.warning("no ws client; dropping line: %s", line[:80])
                continue
            try:
                await ws.send_text(line.decode(errors="replace"))
            except Exception as e:
                log.warning("ws send failed: %s", e)
                break
    except Exception as e:
        log.exception("stdout forward error: %s", e)


async def _forward_stderr(proc: asyncio.subprocess.Process) -> None:
    try:
        while True:
            line = await proc.stderr.readline()
            if not line:
                break
            sys.stderr.write(f"[acp-stderr] {line.decode(errors='replace')}")
            sys.stderr.flush()
    except Exception as e:
        log.exception("stderr forward error: %s", e)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Start acp process at supervisor startup, keep it alive for the whole
    # lifetime of the supervisor. One supervisor = one ACP connection.
    proc = await _start_acp()
    _state["proc"] = proc
    _state["reader_task"] = asyncio.create_task(
        _forward_stdout_to_ws(proc), name="acp-stdout-forward"
    )
    _state["stderr_task"] = asyncio.create_task(
        _forward_stderr(proc), name="acp-stderr-forward"
    )
    yield
    # Shutdown
    for t in (_state["reader_task"], _state["stderr_task"]):
        if t and not t.done():
            t.cancel()
            try:
                await t
            except (asyncio.CancelledError, Exception):
                pass
    if proc.returncode is None:
        try:
            proc.stdin.close()
            await asyncio.wait_for(proc.wait(), timeout=5)
        except Exception:
            proc.kill()
            await proc.wait()


app = FastAPI(lifespan=lifespan)


@app.get("/health")
async def health():
    proc = _state["proc"]
    return {
        "status": "ok",
        "acp_pid": proc.pid if proc else None,
        "acp_alive": (proc is not None and proc.returncode is None),
        "ws_connected": _state["ws"] is not None,
    }


@app.websocket("/acp")
async def acp_ws(ws: WebSocket):
    await ws.accept()
    if _state["ws"] is not None:
        await ws.close(code=4000, reason="another client already connected")
        return
    _state["ws"] = ws
    proc = _state["proc"]
    if not proc or proc.returncode is not None:
        await ws.close(code=4001, reason="acp subprocess not alive")
        _state["ws"] = None
        return
    log.info("ws client connected")
    try:
        while True:
            msg = await ws.receive_text()
            line = (msg.rstrip("\n") + "\n").encode()
            proc.stdin.write(line)
            await proc.stdin.drain()
    except WebSocketDisconnect:
        log.info("ws client disconnected")
    except Exception as e:
        log.exception("ws loop error: %s", e)
    finally:
        if _state["ws"] is ws:
            _state["ws"] = None


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=9000)
    parser.add_argument("--acp", required=True, help="path to claude-agent-acp binary or node entry")
    args = parser.parse_args()

    _state["acp_bin"] = args.acp

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        stream=sys.stderr,
    )
    log.info("supervisor starting on %s:%d acp=%s", args.host, args.port, args.acp)
    uvicorn.run(app, host=args.host, port=args.port, log_level="warning")


if __name__ == "__main__":
    main()
