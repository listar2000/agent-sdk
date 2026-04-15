"""Demo: one-shot streaming chat against the hosted server.

Creates a fresh session and streams a single prompt's response. Useful for
quickly verifying the API is working end-to-end.

Prerequisites:
  Ensure the API server is reachable.
  curl https://agent-sdk-server-production.up.railway.app/health

Usage:
  python examples/chat.py -p "hello, who are you?"
  python examples/chat.py --test -p "hello, who are you?"
"""

import argparse
import asyncio
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
from agent_sdk.client import Agent

RAILWAY_API_URL = "https://agent-sdk-server-production.up.railway.app"
LOCAL_TEST_API_URL = "http://localhost:7778"

async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-p", required=True, help="Prompt to send to the agent")
    parser.add_argument("--test", action="store_true", help="Use local http://localhost:7778 instead of Railway.")
    args = parser.parse_args()
    api_url = LOCAL_TEST_API_URL if args.test else RAILWAY_API_URL

    agent = Agent(
        "chat-demo", provider="local", cwd="/tmp",
        model="haiku", api_url=api_url,
    )
    async for resp in agent.astream(args.p):
        print(resp, end="", flush=True)
    print()
    await agent.aclose()

if __name__ == "__main__":
    asyncio.run(main())
