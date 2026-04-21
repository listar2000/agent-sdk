"""Demo: session resume across a real supervisor restart.

Proves that conversation history survives the supervisor process being
killed and respawned. The flow:

  1. Create a session, tell the agent a secret number.
  2. Force-reap the session via POST /admin/sessions/{id}/reap.
     This kills the supervisor + claude-agent-acp child (local/docker) or
     stops the daytona workspace, and drops the in-memory session state.
  3. Reconnect with just session_id. The server's recovery path spawns a
     fresh supervisor + ACP child and runs session/load against the stored
     inner_session_id to restore conversation history.
  4. Ask the agent for the secret number — proves session/load worked.

Note: inner_session_id is the *conversation id*, not the process id, so
it stays the same across the restart — that's the whole point of
session/load. The proof of supervisor restart is that the server's reap
endpoint reports `provider_stopped` and the agent still remembers the
secret afterwards.

Prerequisites:
  Ensure the API server is reachable.
  curl https://agent-sdk-server-production.up.railway.app/health

Usage:
  python examples/resume_demo.py [local|docker|daytona]
  python examples/resume_demo.py [local|docker|daytona] --test
"""

import argparse
import asyncio
import os
import random
import sys

import httpx

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
from agent_sdk.client import Agent

RAILWAY_API_URL = "https://agent-sdk-server-production.up.railway.app"
LOCAL_TEST_API_URL = "http://localhost:7778"


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("provider", nargs="?", default="local", choices=["local", "docker", "daytona"])
    parser.add_argument("--test", action="store_true", help="Use local http://localhost:7778 instead of Railway.")
    args = parser.parse_args()
    provider = args.provider
    api_url = LOCAL_TEST_API_URL if args.test else RAILWAY_API_URL

    print(f"=== Step 1: Tell agent a secret number (provider={provider}) ===\n")
    agent = Agent(
        "resume-demo", provider=provider, cwd="/tmp",
        model="haiku", api_url=api_url,
    )
    num = random.randint(0, 100)

    resp = await agent.arun(
        f'Remember this secret number: {num}. '
        f'Just say "OK, I will remember {num}." Nothing else.'
    )
    print(f"Response: {resp}")
    print(f"Session ID: {agent.session_id}")
    print(f"Inner session: {agent.inner_session_id}")

    saved_session = agent.session_id
    await agent._client.aclose()  # close HTTP client without reaping

    print("\n=== Step 2: Force-reap server-side (kills supervisor + ACP child) ===\n")
    async with httpx.AsyncClient(base_url=api_url, timeout=30.0) as adm:
        r = await adm.post(f"/admin/sessions/{saved_session}/reap")
        r.raise_for_status()
        print(f"Reap response: {r.json()}")

    print("\n=== Step 3: Resume via session_id and ask for the number ===\n")
    agent2 = Agent("different-name", session_id=saved_session, api_url=api_url)

    resp2 = await agent2.arun("What secret number did I tell you to remember?")
    print(f"Response: {resp2}")
    print(f"Inner session after resume: {agent2.inner_session_id} "
          f"(unchanged — session/load preserves conversation id)")

    await agent2.aclose()

    if f"{num}" in resp2:
        print(f"\n=== SUCCESS: Agent remembered {num} via session/load across "
              f"a real supervisor restart! ===")
    else:
        print(f"\n=== FAILED: Agent did not recall {num} ===")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
