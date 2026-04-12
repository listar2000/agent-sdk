"""Demo: trigger an in-sandbox crash and observe what the client sees.

This is a deliberate OOM test for the agent-sdk error path. The agent is
asked (with explicit context that this is a sandboxed test) to allocate a
huge bytearray in Python, which the kernel OOM-killer will SIGKILL almost
immediately. That kills the `claude` Node.js process inside the sandbox
while a `session/prompt` is in flight — the same path a real OOM hits.

Usage:
  PYTHONPATH=src python examples/crash_demo.py local
  PYTHONPATH=src python examples/crash_demo.py daytona
"""

import asyncio
import os
import sys
import traceback

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
from agent_sdk import Agent
from agent_sdk.errors import PromptError, StreamError

API_URL = os.environ.get("AGENT_API_URL", "http://localhost:7778")

OOM_PROMPT = (
    "Please run this exact "
    "Bash command for testing:\n\n"
    "    python3 -c 'b = bytearray(200 * 1024 * 1024 * 1024)'\n\n"
)


async def crash(provider: str, cwd: str = "/tmp") -> None:
    print(f"=== {provider} — asking agent to OOM itself ===\n")
    agent = Agent(
        f"crash-{provider}",
        provider=provider,
        cwd=cwd,
        model="haiku",
        api_url=API_URL,
    )

    try:
        async for chunk in agent.astream(OOM_PROMPT):
            print(chunk, end="", flush=True)
        print("\n[stream ended cleanly — no error]\n")
    except PromptError as e:
        print(f"\n\n[PromptError] {e}\n")
    except StreamError as e:
        print(f"\n\n[StreamError] {e}\n")
    except Exception as e:
        print(f"\n\n[{type(e).__name__}] {e}")
        traceback.print_exc()

    print("--- second prompt on the same Agent (does it recover?) ---")
    try:
        reply = await agent.arun("Say 'still alive' in 3 words.")
        print(f"[recovered] {reply}\n")
    except Exception as e:
        print(f"[{type(e).__name__}] {e}\n")
        traceback.print_exc()
    finally:
        try:
            await agent.aclose()
        except Exception:
            pass


async def main() -> None:
    provider = sys.argv[1] if len(sys.argv) > 1 else "local"
    cwd = "/home/sandbox" if provider == "daytona" else "/tmp"
    await crash(provider, cwd=cwd)


if __name__ == "__main__":
    asyncio.run(main())
