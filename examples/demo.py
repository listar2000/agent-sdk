"""Demo: run agents using the SDK against the hosted server.

Prerequisites:
  1. Ensure the API server is reachable.
  2. curl https://agent-sdk-server-production.up.railway.app/health

  For Claude (default):  set CLAUDE_CODE_OAUTH_TOKEN in env / ~/.env
  For OpenCode:          set OPENROUTER_API_KEY in env / ~/.env

Usage:
  python examples/demo.py                          # claude + unix_local
  python examples/demo.py --test                   # claude + unix_local + localhost server
  python examples/demo.py daytona --test           # claude + remote daytona
  python examples/demo.py --agent opencode         # opencode + unix_local + openrouter
  python examples/demo.py --agent opencode --model openrouter/openai/gpt-4o
"""

import argparse
import asyncio
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

# Optional dotenv: tests + interactive runs both want ~/.env auto-loaded.
try:
    from dotenv import load_dotenv  # type: ignore
    load_dotenv(os.path.expanduser("~/.env"))
except ImportError:
    pass

from agent_sdk import Agent

RAILWAY_API_URL = "https://agent-sdk-server-production.up.railway.app"
LOCAL_TEST_API_URL = "http://localhost:7778"

# Per-agent defaults: model + the env var that carries credentials.
AGENT_DEFAULTS = {
    "claude": {
        "model": "haiku",
        "secret_env": "CLAUDE_CODE_OAUTH_TOKEN",
    },
    "opencode": {
        "model": "openrouter/anthropic/claude-3.5-haiku",
        "secret_env": "OPENROUTER_API_KEY",
    },
}


async def run_demo(provider: str, api_url: str, agent_type: str, model: str) -> None:
    print(f"=== {agent_type} agent on {provider} (model={model}) ===\n")
    secret_env = AGENT_DEFAULTS[agent_type]["secret_env"]
    secret_val = os.environ.get(secret_env)
    if not secret_val:
        sys.exit(f"ERROR: {secret_env} not set (required for agent_type={agent_type!r})")

    agent = Agent(
        f"demo-{agent_type}-{provider}",
        agent_type=agent_type,
        provider=provider,
        model=model,
        api_url=api_url,
        secrets={secret_env: secret_val},
    )
    try:
        async for chunk in agent.astream(
            "Say hello in 5 words, and then create a hello_world.py "
            "with a simple print statement."
        ):
            print(chunk, end="", flush=True)
        print("\n")
    finally:
        await agent.aclose()


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("provider", nargs="?", default="unix_local",
                        choices=["unix_local", "daytona", "modal"])
    parser.add_argument("--test", action="store_true",
                        help="Use http://localhost:7778 instead of Railway.")
    parser.add_argument("--agent", default="claude",
                        choices=sorted(AGENT_DEFAULTS),
                        help="ACP agent runtime (default: claude).")
    parser.add_argument("--model", default=None,
                        help="Override the per-agent default model.")
    args = parser.parse_args()

    api_url = LOCAL_TEST_API_URL if args.test else RAILWAY_API_URL
    model = args.model or AGENT_DEFAULTS[args.agent]["model"]
    await run_demo(args.provider, api_url=api_url,
                   agent_type=args.agent, model=model)


if __name__ == "__main__":
    asyncio.run(main())
