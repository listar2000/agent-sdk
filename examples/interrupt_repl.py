"""Interactive REPL for exercising the interrupt path with clear tag + event logs.

A background SSE listener prints every event with its [rpc_id] tag so you
can see exactly which prompt each chunk belongs to. A foreground input
loop lets you fire normal prompts and interrupt injections concurrently.

Prereqs:
  Ensure the API server is reachable.
  curl https://agent-sdk-server-production.up.railway.app/health

Usage:
  python examples/interrupt_repl.py
  python examples/interrupt_repl.py --test
  python examples/interrupt_repl.py --session-id <uuid>   # resume an existing session

Commands at the >>> prompt:
  <text>        send <text> as a normal prompt (subject to agent_busy gate)
  /s <text>     inject <text> as an interrupt (bypasses agent_busy)
  /c            cancel the active turn
  /q            quit
"""

import argparse
import asyncio
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from agent_sdk.client import Agent
from api.sse import extract_sse_tag, iter_sse_blocks, parse_acp_event

RAILWAY_API_URL = "https://agent-sdk-server-production.up.railway.app"
LOCAL_TEST_API_URL = "http://localhost:7778"


def short(s: str, n: int = 100) -> str:
    s = str(s).replace("\n", "\\n")
    return s if len(s) <= n else s[: n - 1] + "…"


def tag_label(tag: str | None, seen: dict[str, str]) -> str:
    if not tag:
        return "  -  "
    if tag not in seen:
        seen[tag] = chr(ord("A") + len(seen) % 26)
    return f"{seen[tag]}:{tag[-6:]}"


async def listener(agent: Agent, stop: asyncio.Event) -> None:
    seen: dict[str, str] = {}
    async with agent._open_sse() as sse:
        async for block in iter_sse_blocks(sse):
            if stop.is_set():
                return
            tag = extract_sse_tag(block)
            label = tag_label(tag, seen)
            ev = parse_acp_event(block, None)
            if ev is None:
                continue
            t = ev["type"]
            if t == "text":
                line = f"[{label}] text   {short(ev['text'])}"
            elif t == "reasoning":
                line = f"[{label}] think  {short(ev['text'])}"
            elif t == "tool":
                line = f"[{label}] tool   {ev.get('tool_name')} {short(ev.get('args'))}"
            elif t == "tool_result":
                line = f"[{label}] result {ev.get('tool_name')} {short(ev.get('result'))}"
            elif t == "usage":
                u = ev.get("usage", {}) or {}
                in_t = u.get("inputTokens", u.get("input_tokens", 0))
                out_t = u.get("outputTokens", u.get("output_tokens", 0))
                line = f"[{label}] usage  in={in_t} out={out_t}"
            elif t == "done":
                line = f"[{label}] DONE   stop={ev.get('stop_reason')}"
            elif t == "error":
                line = f"[{label}] ERROR  kind={ev.get('kind')} {short(ev.get('text'))}"
            else:
                line = f"[{label}] {t}"
            print(f"\n{line}", flush=True)


async def read_line(prompt: str) -> str:
    return await asyncio.get_event_loop().run_in_executor(None, input, prompt)


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--session-id", default=None)
    parser.add_argument("--provider", default="local")
    parser.add_argument("--model", default="claude-sonnet-4-6")
    parser.add_argument("--test", action="store_true", help="Use local http://localhost:7778 instead of Railway.")
    args = parser.parse_args()
    api_url = LOCAL_TEST_API_URL if args.test else RAILWAY_API_URL

    if args.session_id:
        agent = Agent("interrupt-repl", session_id=args.session_id, api_url=api_url)
    else:
        agent = Agent("interrupt-repl", provider=args.provider, cwd="/tmp",
                      model=args.model, api_url=api_url)

    await agent._ensure_registered()
    print(f"session: {agent.session_id}")
    print(f"sandbox: {agent.sandbox_id}")
    print("commands: <text> | /s <text> (interrupt) | /c (cancel) | /q (quit)")

    stop = asyncio.Event()
    listener_task = asyncio.create_task(listener(agent, stop))

    try:
        while True:
            try:
                line = await read_line("\n>>> ")
            except (EOFError, KeyboardInterrupt):
                break
            line = line.strip()
            if not line:
                continue
            if line in ("/q", "/quit"):
                break
            if line == "/c":
                try:
                    await agent.cancel()
                    print("[CANCEL sent]")
                except Exception as e:
                    print(f"[CANCEL failed: {e}]")
                continue
            if line.startswith("/s "):
                msg = line[3:]
                try:
                    rpc_id = await agent.send(msg, interrupt=True)
                    print(f"[INTERRUPT sent rpc={rpc_id}]")
                except Exception as e:
                    print(f"[INTERRUPT failed: {e}]")
                continue
            try:
                rpc_id = await agent._post_message(line)
                print(f"[PROMPT sent rpc={rpc_id}]")
            except Exception as e:
                print(f"[PROMPT failed: {e}]")
    finally:
        stop.set()
        listener_task.cancel()
        try:
            await listener_task
        except (asyncio.CancelledError, Exception):
            pass
        await agent.aclose()


if __name__ == "__main__":
    asyncio.run(main())
