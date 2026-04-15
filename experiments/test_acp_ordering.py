"""Empirically test whether claude-agent-acp strictly preserves ordering
across multiple concurrent prompts on one session.

Spawns a local supervisor.js subprocess, opens the raw /v1/acp/{id} SSE
stream, fires prompts back-to-back, and captures every ACP frame with
wallclock timestamps. For each scenario it prints the frame timeline
plus an analysis verdict.

Usage:
  python experiments/test_acp_ordering.py [scenario_name]
  python experiments/test_acp_ordering.py           # run all scenarios
"""

from __future__ import annotations

import asyncio
import json
import os
import subprocess
import sys
import time
import uuid
from pathlib import Path

import httpx

REPO = Path(__file__).resolve().parent.parent
SUPERVISOR_DIR = REPO / "src" / "supervisor"
ACP_BIN_CLAUDE = SUPERVISOR_DIR / "node_modules" / ".bin" / "claude-agent-acp"
ACP_BIN_CODEX = SUPERVISOR_DIR / "node_modules" / ".bin" / "codex-acp"
ACP_BIN = ACP_BIN_CLAUDE
PORT = 9500
URL = f"http://127.0.0.1:{PORT}"


async def post_rpc(sid: str, method: str, params: dict,
                    rpc_id: str | int | None = None) -> dict | None:
    body: dict = {"jsonrpc": "2.0", "method": method, "params": params}
    if rpc_id is not None:
        body["id"] = rpc_id
    async with httpx.AsyncClient(timeout=120) as c:
        r = await c.post(f"{URL}/v1/acp/{sid}", json=body)
        try:
            return r.json()
        except Exception:
            return None


async def stream_reader(acp_sid: str, frames: list, stop_evt: asyncio.Event, t0: float):
    async with httpx.AsyncClient(timeout=None) as c:
        async with c.stream("GET", f"{URL}/v1/acp/{acp_sid}") as resp:
            buf = ""
            async for chunk in resp.aiter_text():
                if stop_evt.is_set():
                    return
                buf += chunk
                while "\n\n" in buf:
                    block, buf = buf.split("\n\n", 1)
                    for line in block.split("\n"):
                        if line.startswith("data: "):
                            try:
                                payload = json.loads(line[6:])
                            except Exception:
                                continue
                            frames.append((round(time.monotonic() - t0, 4), payload))


async def wait_health(timeout: float = 10.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            async with httpx.AsyncClient(timeout=2) as c:
                r = await c.get(f"{URL}/v1/health")
                if r.status_code == 200:
                    return True
        except Exception:
            pass
        await asyncio.sleep(0.15)
    return False


def frame_summary(payload: dict) -> str:
    if "id" in payload and isinstance(payload.get("result"), dict) and "stopReason" in payload["result"]:
        return f"TERMINAL id={payload['id']} stop={payload['result']['stopReason']}"
    if "id" in payload and "result" in payload:
        return f"response id={payload['id']} (non-terminal)"
    if "method" not in payload:
        return f"unknown: {json.dumps(payload)[:100]}"
    method = payload["method"]
    params = payload.get("params", {}) or {}
    update = params.get("update") if isinstance(params, dict) else None
    if method == "session/update" and isinstance(update, dict):
        su = update.get("sessionUpdate", "")
        if su == "agent_message_chunk":
            content = update.get("content", {}) or {}
            text = content.get("text", "")
            return f"agent_message_chunk {text[:50]!r}"
        if su == "agent_thought_chunk":
            content = update.get("content", {}) or {}
            return f"agent_thought_chunk {(content.get('text','') or '')[:40]!r}"
        if su == "tool_call":
            return f"tool_call name={update.get('name') or update.get('toolName')!r} id={update.get('toolCallId','')[:12]}"
        if su == "tool_call_update":
            tcu = update.get("toolCallId", "")[:12]
            status = update.get("status", "")
            return f"tool_call_update id={tcu} status={status!r}"
        if su == "usage_update":
            return f"usage used={update.get('used')} size={update.get('size')}"
        return f"session/update {su}"
    return method


async def run_scenario(name: str, prompts: list[tuple[str, str, float]],
                        cancel_before: int | None = None,
                        post_all_at_once: bool = True,
                        acp_bin: Path = ACP_BIN_CLAUDE) -> None:
    print(f"\n{'='*72}\n{name}\n{'='*72}")
    for rpc_id, text, delay in prompts:
        print(f"  plan: id={rpc_id} delay={delay}s text={text[:60]!r}")

    env = {**os.environ}
    supervisor = subprocess.Popen(
        [
            "node", str(SUPERVISOR_DIR / "supervisor.js"),
            "--host", "127.0.0.1",
            "--port", str(PORT),
            "--acp", str(acp_bin),
            "--cwd", "/tmp",
        ],
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
    )
    frames: list = []
    stop_evt = asyncio.Event()
    reader_task: asyncio.Task | None = None
    try:
        if not await wait_health():
            raise RuntimeError("supervisor did not come up in time")

        acp_sid = str(uuid.uuid4())
        await post_rpc(acp_sid, "initialize", {"protocolVersion": 1}, rpc_id="init-1")
        r = await post_rpc(acp_sid, "session/new", {"cwd": "/tmp", "mcpServers": []}, rpc_id="new-1")
        assert r and "result" in r, f"session/new failed: {r}"
        inner_sid = r["result"]["sessionId"]

        t0 = time.monotonic()
        reader_task = asyncio.create_task(stream_reader(acp_sid, frames, stop_evt, t0))
        await asyncio.sleep(0.15)

        try:
            await post_rpc(acp_sid, "session/set_mode",
                            {"sessionId": inner_sid, "modeId": "bypassPermissions"},
                            rpc_id="mode-1")
        except Exception:
            pass

        post_times: dict[str, float] = {}
        prompt_tasks: list[asyncio.Task] = []

        async def _fire(rpc_id: str, text: str):
            post_times[rpc_id] = round(time.monotonic() - t0, 4)
            print(f"  [{post_times[rpc_id]:>7.4f}] POST session/prompt id={rpc_id}")
            result = await post_rpc(acp_sid, "session/prompt", {
                "sessionId": inner_sid,
                "prompt": [{"type": "text", "text": text}],
            }, rpc_id=rpc_id)
            ack_time = round(time.monotonic() - t0, 4)
            stop = None
            if isinstance(result, dict) and isinstance(result.get("result"), dict):
                stop = result["result"].get("stopReason")
            print(f"  [{ack_time:>7.4f}] <-- session/prompt id={rpc_id} returned stop={stop}")

        for idx, (rpc_id, text, delay) in enumerate(prompts):
            if delay > 0:
                await asyncio.sleep(delay)
            if cancel_before is not None and idx == cancel_before:
                t = round(time.monotonic() - t0, 4)
                print(f"  [{t:>7.4f}] NOTIFY session/cancel")
                await post_rpc(acp_sid, "session/cancel", {"sessionId": inner_sid})
            if post_all_at_once:
                prompt_tasks.append(asyncio.create_task(_fire(rpc_id, text)))
            else:
                await _fire(rpc_id, text)

        expected = {rpc_id for rpc_id, _, _ in prompts}
        deadline = time.monotonic() + 120
        while time.monotonic() < deadline:
            terminals = {
                p["id"] for _, p in frames
                if "id" in p and isinstance(p.get("result"), dict)
                and "stopReason" in p["result"] and p["id"] in expected
            }
            if terminals >= expected:
                break
            await asyncio.sleep(0.1)

        for t in prompt_tasks:
            try:
                await asyncio.wait_for(t, timeout=3)
            except Exception:
                pass

        await asyncio.sleep(0.3)
        stop_evt.set()

        print("\nFrame timeline:")
        for t, p in frames:
            print(f"  [{t:>7.4f}] {frame_summary(p)}")

        # Check whether any frame carries a messageId on its content chunk
        has_message_id = False
        message_ids: set[str] = set()
        for _, p in frames:
            if isinstance(p.get("params"), dict):
                upd = p["params"].get("update") or {}
                mid = upd.get("messageId") if isinstance(upd, dict) else None
                if mid:
                    has_message_id = True
                    message_ids.add(mid)
        print(f"\nChunk messageId populated: {has_message_id}")
        if has_message_id:
            print(f"Distinct messageIds: {sorted(message_ids)}")

        print("\nAnalysis:")
        term_order: list[tuple[str, float]] = []
        for t, p in frames:
            if "id" in p and isinstance(p.get("result"), dict) and "stopReason" in p["result"]:
                if p["id"] in expected:
                    term_order.append((p["id"], t))
        submission = [rpc_id for rpc_id, _, _ in prompts]
        term_ids = [rid for rid, _ in term_order]
        print(f"  submission order: {submission}")
        print(f"  terminal order:   {term_ids}")
        print(f"  terminals match submission: {submission == term_ids}")

        # For each pair (A, B) with A submitted before B, check whether any
        # non-terminal notification falls between A's POST and A's terminal
        # where that notification clearly belongs to B (by text content).
        # Since non-terminals carry no rpc id, we use terminal boundaries.
        if len(term_order) >= 2:
            idx_terminals = []
            for i, (t, p) in enumerate(frames):
                if "id" in p and isinstance(p.get("result"), dict) and "stopReason" in p["result"] and p["id"] in expected:
                    idx_terminals.append((i, p["id"]))
            prev_end = -1
            for i, rid in idx_terminals:
                between = frames[prev_end + 1:i + 1]
                texts = []
                for t, pp in between:
                    if (pp.get("method") == "session/update"
                            and isinstance(pp.get("params"), dict)):
                        upd = pp["params"].get("update") or {}
                        if upd.get("sessionUpdate") == "agent_message_chunk":
                            txt = (upd.get("content") or {}).get("text", "")
                            if txt:
                                texts.append(txt)
                joined = "".join(texts)[:120]
                print(f"  slice for terminal {rid}: {joined!r}")
                prev_end = i

    finally:
        if reader_task is not None:
            stop_evt.set()
            reader_task.cancel()
            try:
                await reader_task
            except BaseException:
                pass
        supervisor.terminate()
        try:
            supervisor.wait(timeout=5)
        except Exception:
            supervisor.kill()


async def main():
    scenarios: dict[str, tuple[list, dict]] = {
        "1_fast_fast": (
            [
                ("A", "Reply with EXACTLY this single word in all caps: ALPHA", 0.0),
                ("B", "Reply with EXACTLY this single word in all caps: BETA", 0.0),
            ],
            {},
        ),
        "2_slow_fast": (
            [
                ("A", "Count slowly from 1 to 8 with a one-sentence description of each number. End with: DONE-A", 0.0),
                ("B", "Reply with EXACTLY this single word in all caps: BETA", 0.0),
            ],
            {},
        ),
        "3_bash_fast": (
            [
                ("A", "Use the Bash tool to run: sleep 3 && echo ALPHA_DONE. Then reply with: DONE-A", 0.0),
                ("B", "Reply with EXACTLY this single word: BETA", 0.0),
            ],
            {},
        ),
        "4_triple_fast": (
            [
                ("A", "Reply with EXACTLY: ONE", 0.0),
                ("B", "Reply with EXACTLY: TWO", 0.0),
                ("C", "Reply with EXACTLY: THREE", 0.0),
            ],
            {},
        ),
        "5_cancel_mid_turn": (
            [
                ("A", "Count slowly from 1 to 30 with a sentence per number. End with: DONE-A", 0.0),
                ("B", "Reply with EXACTLY: BETA_AFTER_CANCEL", 1.5),
            ],
            {"cancel_before": 1},
        ),
        "6_sequential_await": (
            [
                ("A", "Reply with EXACTLY this single word in all caps: ALPHA", 0.0),
                ("B", "Reply with EXACTLY this single word in all caps: BETA", 0.0),
                ("C", "Reply with EXACTLY this single word in all caps: GAMMA", 0.0),
            ],
            {"post_all_at_once": False},
        ),
        # The critical scenario: A runs a real tool-bearing task. Mid-task,
        # B arrives. We want to see whether claude-agent-acp's pendingMessages
        # handoff fires cleanly at the next natural boundary (after A's current
        # tool call, before a new model round for A).
        "7_tool_then_mid_submit": (
            [
                ("A", "Use the Bash tool to run: sleep 4 && echo TOOL_A_DONE. After you see the output, reply with exactly: DONE-A", 0.0),
                ("B", "Reply with EXACTLY this single word: BETA", 1.0),
            ],
            {},
        ),
        # Longer A with multiple sequential tool calls. B submitted after
        # the first tool call. Does handoff fire after the first tool result
        # or wait for A to run all its tool calls?
        "8_multi_tool_mid_submit": (
            [
                ("A", "Use the Bash tool to run 3 sequential commands: first 'echo STEP1', then 'sleep 2 && echo STEP2', then 'echo STEP3'. Run them one at a time and wait for each to finish. Then reply with: DONE-A", 0.0),
                ("B", "Reply with EXACTLY: BETA", 2.5),
            ],
            {},
        ),
    }

    if len(sys.argv) > 1:
        names = [sys.argv[1]]
    else:
        names = list(scenarios.keys())

    acp_bin = ACP_BIN_CLAUDE
    if "--codex" in sys.argv:
        acp_bin = ACP_BIN_CODEX

    for name in names:
        if name.startswith("--"):
            continue
        if name not in scenarios:
            print(f"unknown scenario: {name}")
            continue
        prompts, kwargs = scenarios[name]
        try:
            await run_scenario(name, prompts, acp_bin=acp_bin, **kwargs)
        except Exception as e:
            print(f"!!! scenario {name} crashed: {type(e).__name__}: {e}")


if __name__ == "__main__":
    asyncio.run(main())
