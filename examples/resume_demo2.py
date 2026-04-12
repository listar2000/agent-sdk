"""Demo: session resume using native session/load.

Shows that conversation history survives a full disconnect/reconnect.
The user only needs session_id to resume — the server looks up everything.

Prerequisites:
  - sandbox-agent binary installed
  - Server running: uvicorn src.api.server:app --port 7778
  - For Docker: Docker daemon running

Usage:
  python examples/resume_demo.py [local|docker]
"""

import argparse
import asyncio
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
from agent_sdk.client import Agent
import random

async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-p", required=True, help="Prompt to send to the agent")
    args = parser.parse_args()

    agent = Agent("different-name", session_id="29d6ec31-7fd1-4ddc-bba3-a6824c9c72ee")

    async for resp in agent.astream(args.p):
        print(resp)

    await agent.aclose()

if __name__ == "__main__":
    asyncio.run(main())
