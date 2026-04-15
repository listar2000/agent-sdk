"""Demo: run agents using the SDK against the hosted server.

Prerequisites:
  1. Ensure the API server is reachable.
  2. curl https://agent-sdk-server-production.up.railway.app/health

Usage:
  python examples/demo.py
  python examples/demo.py --test
  python examples/demo.py daytona         # optional — use remote daytona sandbox
  python examples/demo.py daytona --test
"""

import argparse
import asyncio
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
from agent_sdk import Agent

RAILWAY_API_URL = "https://agent-sdk-server-production.up.railway.app"
LOCAL_TEST_API_URL = "http://localhost:7778"


async def run_demo(provider: str, api_url: str, cwd: str = "/tmp"):
    print(f"=== {provider.capitalize()} agent ===\n")
    agent = Agent(
        f"demo-{provider}", provider=provider, cwd=cwd,
        model="haiku", api_url=api_url,
    )
    async for chunk in agent.astream("Say hello in 5 words, and then create a hello_world.py."):
        print(chunk, end="", flush=True)
    print("\n")
    await agent.aclose()


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("provider", nargs="?", default="local", choices=["local", "daytona"])
    parser.add_argument("--test", action="store_true", help="Use local http://localhost:7778 instead of Railway.")
    args = parser.parse_args()

    api_url = LOCAL_TEST_API_URL if args.test else RAILWAY_API_URL
    if args.provider == "daytona":
        await run_demo("daytona", api_url=api_url, cwd="/home/sandbox")
    else:
        await run_demo("local", api_url=api_url)


if __name__ == "__main__":
    asyncio.run(main())
