"""Golden: SessionPool partial-failure recovery (real Daytona).

Pins the fix for "session locks up when its sandbox can't be reattached
to anymore." The provider's reattach branch used to ``raise`` on any
non-404 reattach failure, which preserved transient retries but
created an infinite-loop bug — every subsequent ``/message`` hit the
same dead reattach forever, until someone manually ``DELETE``d the
session. Meanwhile the wedged sandbox burned Daytona quota.

Fix: any reattach failure clears ``sandbox_ref`` and falls through to
cold-create, same as the 404 branch already did. The wedged sandbox
stays labelled in Daytona for ``cleanup_orphans.py`` to reap.

Test path (no mocks):
  * Real ``ApiClient`` against ``localhost:7778``.
  * Real Daytona sandbox.
  * Engineered permanent wedge: kill ``supervisor.js`` AND bind the
    supervisor port (``9100``) inside the sandbox so any respawn fails
    with ``EADDRINUSE``. Sandbox stays ``started``; Daytona returns
    success on every API call; the failure is in
    ``start_supervisor_in_sandbox`` waiting for /v1/health.

Today (RED): post-wedge ``/message`` returns an error — the reattach
branch raises and the session never recovers. Test fails.

Post-fix (GREEN): post-wedge ``/message`` succeeds — reattach failure
is now handled by clearing ``sandbox_ref`` and cold-creating a fresh
sandbox. ``sandbox_ref`` on the session changes to the new one.
"""
from __future__ import annotations

import asyncio
import os
import sys
import time

import httpx
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from dotenv import load_dotenv  # noqa: E402
load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))
load_dotenv(os.path.expanduser("~/.env"), override=False)

from agent_sdk import ApiClient  # noqa: E402

SERVER = os.environ.get("AGENT_SERVER_URL", "http://localhost:7778")
DAYTONA_API_KEY = os.environ.get("DAYTONA_API_KEY")
OAUTH_TOKEN = os.environ.get("CLAUDE_CODE_OAUTH_TOKEN")
SUPERVISOR_PORT = 9100   # _SUPERVISOR_REMOTE_PORT in src/api/providers/daytona/__init__.py


def _has_server() -> bool:
    try:
        with httpx.Client() as c:
            return c.get(f"{SERVER}/health", timeout=3).status_code == 200
    except httpx.HTTPError:
        return False


pytestmark = pytest.mark.skipif(
    not (DAYTONA_API_KEY and OAUTH_TOKEN and _has_server()),
    reason="needs DAYTONA_API_KEY + CLAUDE_CODE_OAUTH_TOKEN + live server on localhost:7778",
)


async def _wedge_supervisor(sandbox_ref: str) -> None:
    """Wedge the supervisor inside a live Daytona sandbox. Kill the
    running supervisor.js, then bind its port with ``nc`` so any
    respawn hits ``EADDRINUSE``. Sandbox stays ``started``; Daytona
    API calls all succeed; only ``start_supervisor_in_sandbox``'s
    health probe will fail."""
    from daytona_sdk import Daytona, DaytonaConfig
    daytona = Daytona(DaytonaConfig(api_key=DAYTONA_API_KEY))
    loop = asyncio.get_running_loop()
    sb = await loop.run_in_executor(None, lambda: daytona.get(sandbox_ref))
    await loop.run_in_executor(
        None,
        lambda: sb.process.exec("pkill -9 -f supervisor.js || true", timeout=15),
    )
    await loop.run_in_executor(
        None,
        lambda: sb.process.exec(
            f"nohup nc -kl {SUPERVISOR_PORT} > /dev/null 2>&1 &",
            timeout=15,
        ),
    )


async def _send_and_classify(sdk: ApiClient, sid: str, msg: str) -> str:
    """Returns ``"ok"`` if the turn completed, ``"error"`` on an error
    event or 5xx, ``"timeout"`` otherwise."""
    try:
        rpc_resp = await sdk.send_message(sid, msg)
    except httpx.HTTPStatusError:
        return "error"
    rpc = rpc_resp["rpc_id"]
    deadline = time.time() + 60
    async for chunk in sdk.stream_events(sid):
        if rpc.encode() not in chunk:
            if time.time() > deadline:
                return "timeout"
            continue
        if b'"error"' in chunk or b'"-32000"' in chunk:
            return "error"
        if b"stopReason" in chunk:
            return "ok"
        if time.time() > deadline:
            return "timeout"
    return "timeout"


@pytest.mark.asyncio
async def test_pool_recovers_from_wedged_sandbox():
    """Wedge a real Daytona sandbox; the next ``/message`` must succeed
    on a freshly cold-created sandbox. Today: returns an error and
    sandbox_ref never changes (the bug). After the fix: cold-create
    kicks in transparently, message lands, sandbox_ref differs.
    """
    async with ApiClient(base_url=SERVER) as sdk:
        # 1. Create + warm up a daytona session.
        sess = await sdk.create_session(
            provider="daytona", agent_type="claude", model="haiku",
            secrets={"CLAUDE_CODE_OAUTH_TOKEN": OAUTH_TOKEN},
        )
        sid = sess["session_id"]
        warmup = await _send_and_classify(sdk, sid, "say 'ready'")
        assert warmup == "ok", f"warmup must succeed; got {warmup!r}"
        original_ref = (await sdk.get_session_sandbox(sid))["sandbox_ref"]

        # 2. Wedge the supervisor permanently.
        await _wedge_supervisor(original_ref)

        # 3. Drop the warm pool entry so the next /message hits the
        #    full reattach path (where the bug lives).
        await sdk.release_session(sid)

        # 4. The next message MUST succeed on a fresh sandbox.
        outcome = await _send_and_classify(sdk, sid, "are you back?")
        assert outcome == "ok", (
            f"post-wedge message should cold-create a fresh sandbox "
            f"and succeed; got {outcome!r}"
        )
        new_ref = (await sdk.get_session_sandbox(sid))["sandbox_ref"]
        assert new_ref != original_ref, (
            "post-wedge sandbox_ref must point at a freshly cold-created "
            "sandbox; the wedged one is left labelled for cleanup_orphans.py"
        )

        # 5. Tear down the recovered session. The wedged ``original_ref``
        #    is intentionally NOT deleted here — that's the contract:
        #    the pool's job is to break the loop, not clean up. Cleanup
        #    runs out-of-band via cleanup_orphans.py.
        await sdk.delete_session(sid)
