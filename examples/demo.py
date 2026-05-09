"""Demo: run agents using the SDK against the hosted server.

Prerequisites:
  1. Ensure the API server is reachable.
  2. curl https://agent-sdk-server-production.up.railway.app/health

Usage:
  python examples/demo.py
  python examples/demo.py --test
  python examples/demo.py daytona                                  # remote daytona sandbox
  python examples/demo.py daytona --test
  python examples/demo.py daytona --test --agent-type opencode --model openrouter/anthropic/claude-3.5-haiku
"""

import argparse
import asyncio
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
from agent_sdk import Agent

RAILWAY_API_URL = "https://agent-sdk-server-production.up.railway.app"
LOCAL_TEST_API_URL = "http://localhost:7778"

# Per-agent-type sensible default model when --model isn't passed.
# Claude-agent-acp accepts the bare alias ("haiku"); other ACP runtimes
# (opencode, etc.) want a fully-qualified provider/model id.
DEFAULT_MODEL_BY_AGENT_TYPE = {
    "claude": "haiku",
    "opencode": "openrouter/anthropic/claude-3.5-haiku",
    "codex": "gpt-5",
    "gemini": "gemini-2.5-flash",
}


def _collect_secrets() -> dict[str, str]:
    """Forward whichever LLM creds are present in the local env into the sandbox.

    The SDK only auto-forwards CLAUDE_CODE_OAUTH_TOKEN / ANTHROPIC_API_KEY;
    everything else (OPENROUTER, OPENAI, etc.) has to ride the explicit
    ``secrets`` channel or the sandbox supervisor sees no credentials.
    """
    keys = ("CLAUDE_CODE_OAUTH_TOKEN", "OPENROUTER_API_KEY")
    return {k: os.environ[k] for k in keys if os.environ.get(k)}


async def run_demo(
    provider: str,
    api_url: str,
    *,
    agent_type: str,
    model: str,
):
    print(f"=== {provider.capitalize()} agent ({agent_type}, {model}) ===\n")
    agent = Agent(
        f"demo-{provider}",
        provider=provider,
        agent_type=agent_type,
        model=model,
        api_url=api_url,
        secrets=_collect_secrets() or None,
    )
    async for chunk in agent.astream("Say hello in 5 words, and then create a hello_world.py."):
        print(chunk, end="", flush=True)
    print("\n")
    await agent.aclose()


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("provider", nargs="?", default="local", choices=["local", "daytona"])
    parser.add_argument("--test", action="store_true", help="Use local http://localhost:7778 instead of Railway.")
    parser.add_argument(
        "--agent-type",
        default="claude",
        choices=sorted(DEFAULT_MODEL_BY_AGENT_TYPE.keys()),
        help="ACP runtime to use (default: claude).",
    )
    parser.add_argument(
        "--model",
        default=None,
        help="Model id. Defaults per agent-type; for opencode, use a fully-qualified provider/model id.",
    )
    args = parser.parse_args()

    api_url = LOCAL_TEST_API_URL if args.test else RAILWAY_API_URL
    model = args.model or DEFAULT_MODEL_BY_AGENT_TYPE[args.agent_type]
    provider = "unix_local" if args.provider == "local" else args.provider
    await run_demo(provider, api_url=api_url, agent_type=args.agent_type, model=model)


if __name__ == "__main__":
    asyncio.run(main())
