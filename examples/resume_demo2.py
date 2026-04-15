"""Demo: reattach to an existing session by session_id and send a prompt.

Shows that you can reconstruct a session client from just the session_id —
the server looks up agent/sandbox from the DB and rebuilds state if needed.

Prerequisites:
  Ensure the API server is reachable.
  curl https://agent-sdk-server-production.up.railway.app/health

Usage:
  python examples/resume_demo2.py --session-id <uuid> -p "your prompt"
  python examples/resume_demo2.py --test --session-id <uuid> -p "your prompt"
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
    parser.add_argument("--session-id", required=True, help="Existing session UUID")
    parser.add_argument("-p", required=True, help="Prompt to send to the agent")
    parser.add_argument("--test", action="store_true", help="Use local http://localhost:7778 instead of Railway.")
    args = parser.parse_args()
    api_url = LOCAL_TEST_API_URL if args.test else RAILWAY_API_URL

    agent = Agent("resume-demo2", session_id=args.session_id, api_url=api_url)
    async for resp in agent.astream(args.p):
        print(resp, end="", flush=True)
    print()
    await agent.aclose()

if __name__ == "__main__":
    asyncio.run(main())
