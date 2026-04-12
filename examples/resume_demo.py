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

import asyncio
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
from agent_sdk.client import Agent
import random

async def main():
    provider = sys.argv[1] if len(sys.argv) > 1 else "local"

    # Step 1: Create agent, tell it a secret number
    print(f"=== Step 1: Tell agent a secret number (provider={provider}) ===\n")
    agent = Agent("resume-demo", provider=provider, cwd="/tmp", model="haiku")
    num = random.randint(0, 100)

    resp = await agent.arun(
        f'Remember this secret number: {num}. '
        f'Just say "OK, I will remember {num}." Nothing else.'
    )
    print(f"Response: {resp}")
    print(f"Session ID: {agent.session_id}")

    saved_session = agent.session_id

    # Step 2: Kill the connection
    print("\n=== Step 2: Close agent (simulates crash/restart) ===\n")
    await agent.aclose()
    print("Agent closed. ACP session is gone.")

    # Step 3: Resume with just session_id
    print("\n=== Step 3: Resume via session_id and ask for the number ===\n")
    agent2 = Agent("different-name", session_id=saved_session)

    resp2 = await agent2.arun("What secret number did I tell you to remember?")
    print(f"\nResponse: {resp2}")

    await agent2.aclose()

    # Verdict
    if f"{num}" in resp2:
        print(f"\n=== SUCCESS: Agent remembered {num} via session/load! ===")
    else:
        print(f"\n=== FAILED: Agent did not recall {num} ===")


if __name__ == "__main__":
    asyncio.run(main())
