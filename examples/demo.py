"""Demo: run agents using the SDK against the docker server.

Prerequisites:
  1. docker compose up --build -d
     (requires .env file with ANTHROPIC_API_KEY=sk-ant-...)
  2. curl http://localhost:7778/health    # should return {"status":"ok"}

Usage:
  python examples/demo.py
  python examples/demo.py daytona         # optional — use remote daytona sandbox
"""

import asyncio
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
from agent_sdk import Agent

API_URL = "http://localhost:7778"


async def run_demo(provider: str, cwd: str = "/tmp"):
    print(f"=== {provider.capitalize()} agent ===\n")
    agent = Agent(
        f"demo-{provider}", provider=provider, cwd=cwd,
        model="haiku", api_url=API_URL,
    )
    async for chunk in agent.astream("Say hello in 5 words, and then create a hello_world.py."):
        print(chunk, end="", flush=True)
    print("\n")
    await agent.aclose()


async def main():
    mode = sys.argv[1] if len(sys.argv) > 1 else "local"
    if mode == "daytona":
        await run_demo("daytona", cwd="/home/sandbox")
    else:
        await run_demo("local")


if __name__ == "__main__":
    asyncio.run(main())
