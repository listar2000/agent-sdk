"""Golden: SessionPool partial-failure recovery, parametrised over providers.

Pins the fix for "session locks up when its sandbox can't be reattached
to anymore." Two related shapes of the same root cause:

  * **daytona**: ``_resolve_or_create_sandbox`` used to ``raise`` on any
    non-404 reattach failure. Fix: any reattach failure clears
    ``sandbox_ref`` and falls through to cold-create.

  * **docker, unix_local, modal**: reattach succeeded but the post-reattach
    health probe failed → exception propagated with ``sandbox_ref`` still
    pointing at the wedged sandbox. Fix: clear ``sandbox_ref`` if we
    reattached AND health failed, so the next call cold-creates fresh.

Either way, the wedged sandbox is left for ``cleanup_orphans.py`` to reap
by label. The pool's only job here is to break the loop so the session
itself recovers.

No mocks. Real ``ApiClient`` against ``localhost:7778``, real provider
sandboxes, real LLM round-trips. Each provider has its own engineered
wedge that survives whatever the provider's "release" path does:

  * **daytona**: kill ``supervisor.js`` + ``rm /opt/agent-sdk/runtime/supervisor.js``.
    Deletion lands in the writable overlay → survives pause/resume.
    Cold-create gets a fresh container so the new sandbox is clean.

  * **docker**: same idea — ``docker exec`` to remove the supervisor
    binary inside the container; preserved across ``docker stop``/``docker start``.

Providers excluded from the parametrisation:
  * **unix_local**: no sandbox-vs-host isolation — the supervisor binary
    is a host-shared resource. Any wedge that breaks the binary also
    breaks cold-create (the recovery path uses the same binary). When
    a unix_local supervisor "dies", the PID is gone, ``get_sandbox_status``
    returns ``"missing"``, and the existing 404 path cold-creates
    naturally — the wedge-mode failure shape doesn't apply.
  * **modal**: modal sandboxes terminate on stop. Reattach naturally
    falls through to cold-create after any release, so there's no
    persistent ``sandbox_ref`` to wedge.

The provider-side fix is still applied to all four providers as
defense-in-depth (cheap and consistent), even though only daytona and
docker exercise it under this test.

Skipped per-provider when its deps aren't present (no docker daemon,
no DAYTONA_API_KEY + CLAUDE_CODE_OAUTH_TOKEN, no live server).
"""
from __future__ import annotations

import asyncio
import os
import shutil
import subprocess
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


def _has_server() -> bool:
    try:
        with httpx.Client() as c:
            return c.get(f"{SERVER}/health", timeout=3).status_code == 200
    except httpx.HTTPError:
        return False


def _has_docker() -> bool:
    if shutil.which("docker") is None:
        return False
    try:
        return subprocess.run(
            ["docker", "info"], capture_output=True, timeout=5
        ).returncode == 0
    except (subprocess.TimeoutExpired, OSError):
        return False


pytestmark = pytest.mark.skipif(
    not _has_server(), reason="needs live server on localhost:7778",
)


# ---------------------------------------------------------------------------
# Provider-specific wedges. Each must produce a sandbox that:
#   1. Reports as "running" / alive to the provider's get_sandbox_status
#      (so reattach is attempted, not skipped).
#   2. Has a supervisor that can't be reached on /v1/health (so the
#      provider's reattach-then-health-probe path raises).
#   3. Stays wedged across whatever the provider's release/stop does
#      (so the next /message lands on the same wedged ref).
# ---------------------------------------------------------------------------

async def _wedge_daytona(sandbox_ref: str) -> None:
    """Kill supervisor + delete supervisor.js from the image overlay.
    Deletion persists across daytona pause/resume; cold-create gets a
    fresh container with intact /opt."""
    from daytona_sdk import Daytona, DaytonaConfig
    daytona = Daytona(DaytonaConfig(api_key=DAYTONA_API_KEY))
    loop = asyncio.get_running_loop()
    sb = await loop.run_in_executor(None, lambda: daytona.get(sandbox_ref))
    await loop.run_in_executor(
        None,
        lambda: sb.process.exec(
            "pkill -9 -f supervisor.js || true; "
            "rm -f /opt/agent-sdk/runtime/supervisor.js",
            timeout=15,
        ),
    )


async def _wedge_docker(sandbox_ref: str) -> None:
    """``docker exec`` into the container to kill the supervisor and
    remove its binary. The modification lands in the container's
    writable layer and survives ``docker stop`` / ``docker start``."""
    proc = await asyncio.create_subprocess_exec(
        "docker", "exec", sandbox_ref, "sh", "-c",
        "pkill -9 -f supervisor.js || true; "
        "rm -f /opt/agent-sdk/runtime/supervisor.js",
        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    )
    await asyncio.wait_for(proc.wait(), timeout=15)


async def _send_and_classify(sdk: ApiClient, sid: str, msg: str) -> str:
    """Returns ``"ok"`` if the turn completed, ``"error"`` on an error
    event or 5xx, ``"timeout"`` otherwise. Generous deadline because
    cold-recovery from a wedged sandbox is a real provisioning round-trip."""
    try:
        rpc_resp = await sdk.send_message(sid, msg)
    except httpx.HTTPStatusError:
        return "error"
    rpc = rpc_resp["rpc_id"]
    deadline = time.time() + 240
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


# ---------------------------------------------------------------------------
# Parametrised over the providers where the wedge mode is reproducible.
# Modal is excluded (sandboxes are ephemeral; reattach naturally falls
# through after any release).
# ---------------------------------------------------------------------------


_PROVIDERS = [
    pytest.param(
        "daytona", _wedge_daytona,
        marks=pytest.mark.skipif(
            not (DAYTONA_API_KEY and OAUTH_TOKEN),
            reason="needs DAYTONA_API_KEY + CLAUDE_CODE_OAUTH_TOKEN",
        ),
        id="daytona",
    ),
    pytest.param(
        "docker", _wedge_docker,
        marks=pytest.mark.skipif(not _has_docker(), reason="needs docker"),
        id="docker",
    ),
]


@pytest.mark.asyncio
@pytest.mark.parametrize("provider, wedge_fn", _PROVIDERS)
async def test_pool_recovers_from_wedged_sandbox(provider: str, wedge_fn) -> None:
    """Wedge a real sandbox; the next ``/message`` must succeed on a
    freshly cold-created sandbox; ``sandbox_ref`` must change."""
    body = {"provider": provider, "agent_type": "claude", "model": "haiku"}
    if OAUTH_TOKEN:
        body["secrets"] = {"CLAUDE_CODE_OAUTH_TOKEN": OAUTH_TOKEN}

    async with ApiClient(base_url=SERVER) as sdk:
        sess = await sdk.create_session(**body)
        sid = sess["session_id"]
        try:
            warmup = await _send_and_classify(sdk, sid, "say 'ready'")
            assert warmup == "ok", f"warmup must succeed; got {warmup!r}"
            original_ref = (await sdk.get_session_sandbox(sid))["sandbox_ref"]

            await wedge_fn(original_ref)

            # Force the next /message through the cold-recovery path
            # by dropping the warm pool entry.
            await sdk.release_session(sid)

            outcome = await _send_and_classify(sdk, sid, "are you back?")
            assert outcome == "ok", (
                f"[{provider}] post-wedge message should cold-create a fresh "
                f"sandbox and succeed; got {outcome!r}"
            )
            new_ref = (await sdk.get_session_sandbox(sid))["sandbox_ref"]
            assert new_ref != original_ref, (
                f"[{provider}] post-wedge sandbox_ref must point at a freshly "
                "cold-created sandbox; the wedged one is left labelled for "
                "cleanup_orphans.py to reap"
            )
        finally:
            try:
                await sdk.delete_session(sid)
            except Exception:
                pass
