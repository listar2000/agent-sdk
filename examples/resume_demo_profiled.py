"""Profiled version of resume_demo.py — times every sub-operation."""

import argparse
import asyncio
import os
import random
import sys
import time
from contextlib import asynccontextmanager

import httpx

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
from agent_sdk.client import Agent

RAILWAY_API_URL = "https://agent-sdk-server-production.up.railway.app"
LOCAL_TEST_API_URL = "http://localhost:7778"


class Timer:
    def __init__(self, label):
        self.label = label
        self.start = None
        self.elapsed = None

    def __enter__(self):
        self.start = time.monotonic()
        return self

    def __exit__(self, *args):
        self.elapsed = time.monotonic() - self.start
        print(f"  ⏱  {self.label}: {self.elapsed:.2f}s")


async def timed_request(client, method, url, **kwargs):
    label = f"{method} {url}"
    t0 = time.monotonic()
    if method == "POST":
        resp = await client.post(url, **kwargs)
    elif method == "GET":
        resp = await client.get(url, **kwargs)
    else:
        resp = await client.request(method, url, **kwargs)
    dt = time.monotonic() - t0
    print(f"  ⏱  HTTP {method} {url} → {resp.status_code} in {dt:.2f}s")
    return resp


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("provider", nargs="?", default="local", choices=["local", "docker", "daytona"])
    parser.add_argument("--test", action="store_true", help="Use local http://localhost:7778 instead of Railway.")
    args = parser.parse_args()
    provider = args.provider
    api_url = LOCAL_TEST_API_URL if args.test else RAILWAY_API_URL
    num = random.randint(0, 100)

    print(f"\n{'='*60}")
    print(f"PROFILED RESUME DEMO (provider={provider}, secret={num})")
    print(f"{'='*60}\n")

    # ── Step 1: Create session ──
    print("═══ Step 1: Create session + send first message ═══\n")

    overall_t0 = time.monotonic()

    with Timer("Agent.__init__"):
        agent = Agent(
            "resume-demo", provider=provider, cwd="/tmp",
            model="haiku", api_url=api_url,
        )

    # Break down _ensure_registered (sessions/quick)
    with Timer("sessions/quick (register)"):
        await agent._ensure_registered()

    print(f"  → session_id: {agent.session_id}")
    print(f"  → inner_session_id: {agent.inner_session_id}")

    with Timer("arun (first prompt)"):
        resp = await agent.arun(
            f'Remember this secret number: {num}. '
            f'Just say "OK, I will remember {num}." Nothing else.'
        )

    print(f"  → Response: {resp}")

    saved_session = agent.session_id

    with Timer("agent.aclose"):
        await agent.aclose()

    step1_total = time.monotonic() - overall_t0
    print(f"\n  ══ Step 1 total: {step1_total:.2f}s ══\n")

    # ── Step 2: Reap ──
    print("═══ Step 2: Force-reap session ═══\n")

    step2_t0 = time.monotonic()

    async with httpx.AsyncClient(base_url=api_url, timeout=30.0) as adm:
        with Timer("POST /admin/sessions/{id}/reap"):
            r = await adm.post(f"/admin/sessions/{saved_session}/reap")
            r.raise_for_status()
            print(f"  → {r.json()}")

    step2_total = time.monotonic() - step2_t0
    print(f"\n  ══ Step 2 total: {step2_total:.2f}s ══\n")

    # ── Step 3: Resume + ask ──
    print("═══ Step 3: Resume session + ask for number ═══\n")

    step3_t0 = time.monotonic()

    with Timer("Agent.__init__ (resume)"):
        agent2 = Agent("different-name", session_id=saved_session, api_url=api_url)

    # Break down _ensure_registered (resume path)
    with Timer("sessions/{id}/resume (register)"):
        await agent2._ensure_registered()

    print(f"  → inner_session_id: {agent2.inner_session_id}")

    with Timer("arun (second prompt)"):
        resp2 = await agent2.arun("What secret number did I tell you to remember?")

    print(f"  → Response: {resp2}")

    with Timer("agent2.aclose"):
        await agent2.aclose()

    step3_total = time.monotonic() - step3_t0
    print(f"\n  ══ Step 3 total: {step3_total:.2f}s ══\n")

    # ── Summary ──
    total = time.monotonic() - overall_t0
    print(f"{'='*60}")
    print(f"TIMING SUMMARY")
    print(f"{'='*60}")
    print(f"  Step 1 (create + prompt): {step1_total:.2f}s")
    print(f"  Step 2 (reap):            {step2_total:.2f}s")
    print(f"  Step 3 (resume + prompt):  {step3_total:.2f}s")
    print(f"  TOTAL:                     {total:.2f}s")
    print(f"{'='*60}\n")

    if f"{num}" in resp2:
        print(f"✅ SUCCESS: Agent remembered {num}")
    else:
        print(f"❌ FAILED: Agent did not recall {num}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
