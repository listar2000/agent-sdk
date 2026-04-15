"""Demo: inject a message into an active agent turn with interrupt=True.

The agent is given a long open-ended task (write an essay on AI).
Once the first streaming chunk arrives (confirming the agent is busy),
we wait 2 seconds then inject an interrupting message via agent.send().
We observe that the output pivots to reflect the interrupting message.

Prerequisites:
  Ensure the API server is reachable.
  curl https://agent-sdk-server-production.up.railway.app/health

Usage:
  python examples/interrupt_demo.py
  python examples/interrupt_demo.py --test
"""

import argparse
import asyncio
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
from agent_sdk.client import Agent


DIVIDER = "─" * 60

RAILWAY_API_URL = "https://agent-sdk-server-production.up.railway.app"
LOCAL_TEST_API_URL = "http://localhost:7778"


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--test", action="store_true", help="Use local http://localhost:7778 instead of Railway.")
    args = parser.parse_args()
    api_url = LOCAL_TEST_API_URL if args.test else RAILWAY_API_URL

    print(DIVIDER)
    print("INTERRUPT DEMO")
    print("Ask agent to write a long essay, then interrupt it mid-turn")
    print(f"API: {api_url}")
    print(DIVIDER)

    agent = Agent(
        "interrupt-demo",
        provider="local",
        model="claude-haiku-4-5-20251001",
        api_url=api_url,
    )

    # Event that fires once we've received the first streaming chunk,
    # confirming agent_busy=True on the server side.
    first_chunk_event = asyncio.Event()

    async def interrupt_after_first_chunk():
        """Wait for streaming to begin, then interrupt 2 seconds later."""
        await first_chunk_event.wait()
        await asyncio.sleep(2)
        print()
        print(DIVIDER)
        print("[INTERRUPT] Injecting interrupting message via agent.send()...")
        print(DIVIDER)
        try:
            rpc_id = await agent.send(
                "IMPORTANT REDIRECT: Stop writing about history. "
                "Immediately pivot to discuss ONLY the ethical implications of AI. "
                "Start a new section titled 'Ethical Implications' and explicitly "
                "acknowledge that you received this mid-turn interrupting message.",
                interrupt=True,
            )
            print(f"[INTERRUPT] Message delivered (rpc_id={rpc_id}) — watch for pivot in the output below.\n")
        except Exception as e:
            print(f"[INTERRUPT] send() raised: {e}\n")

    prompt = (
        "Write a comprehensive, multi-section essay about the history of artificial "
        "intelligence. Cover the Turing test, early expert systems, the AI winters, "
        "the rise of neural networks, and modern large language models. Be very "
        "thorough — write at least 1000 words across multiple detailed paragraphs. "
        "Take your time with each section."
    )

    print(f"\n[PROMPT] {prompt[:90]}...\n")
    print(DIVIDER)
    print("AGENT OUTPUT (streaming):")
    print(DIVIDER + "\n")

    interrupt_task = asyncio.create_task(interrupt_after_first_chunk())
    output_chars = 0
    first_chunk_seen = False

    try:
        async for ev in agent.astream(prompt):
            print(ev, end="", flush=True)
            output_chars += len(str(ev))
            if not first_chunk_seen and ev.get("type") == "text":
                first_chunk_seen = True
                first_chunk_event.set()  # Signal: agent is now actively streaming
    finally:
        interrupt_task.cancel()
        try:
            await interrupt_task
        except asyncio.CancelledError:
            pass

    print()
    print(DIVIDER)
    print(f"\n[DONE] Total characters streamed: {output_chars}")
    print(f"[DONE] Session ID: {agent.session_id}")

    await agent.aclose()


if __name__ == "__main__":
    asyncio.run(main())
