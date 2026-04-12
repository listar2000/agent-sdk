"""Demo: run agents using the SDK with different providers.

Prerequisites:
  1. Start the server:  docker compose up --build -d
     (requires .env file with ANTHROPIC_API_KEY=sk-ant-...)
  2. Verify:            curl http://localhost:7778/health

Usage:
  PYTHONPATH=src python examples/demo.py local
  PYTHONPATH=src python examples/demo.py docker
  PYTHONPATH=src python examples/demo.py daytona
  PYTHONPATH=src python examples/demo.py all
"""

import asyncio
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
from agent_sdk import Agent

API_URL = os.environ.get("AGENT_API_URL", "http://localhost:7778")


async def run_demo(provider: str, cwd: str = "/tmp"):
    print(f"=== {provider.capitalize()} agent ===\n")
    agent = Agent(f"demo-{provider}", provider=provider, cwd=cwd, model="haiku", api_url=API_URL)
    async for chunk in agent.astream("Say hello in 5 words, and then create a hello_world.py."):
        print(chunk, end="", flush=True)
    print("\n")
    await agent.aclose()


async def main():
    mode = sys.argv[1] if len(sys.argv) > 1 else "local"

    if mode in ("local", "all"):
        await run_demo("local")

    if mode in ("docker", "all"):
        await run_demo("docker")

    if mode in ("daytona", "all"):
        await run_demo("daytona", cwd="/home/sandbox")


if __name__ == "__main__":
    asyncio.run(main())
