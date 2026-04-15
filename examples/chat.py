"""Demo: one-shot streaming chat against the docker server.

Creates a fresh session and streams a single prompt's response. Useful for
quickly verifying the docker stack is working end-to-end.

Prerequisites:
  docker compose up --build -d
  curl http://localhost:7778/health

Usage:
  python examples/chat.py -p "hello, who are you?"
"""

import argparse
import asyncio
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
from agent_sdk.client import Agent

API_URL = "http://localhost:7778"

async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-p", required=True, help="Prompt to send to the agent")
    args = parser.parse_args()

    agent = Agent(
        "chat-demo", provider="local", cwd="/tmp",
        model="haiku", api_url=API_URL,
    )
    async for resp in agent.astream(args.p):
        print(resp, end="", flush=True)
    print()
    await agent.aclose()

if __name__ == "__main__":
    asyncio.run(main())
