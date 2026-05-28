"""Behaviour (golden-style) reproduction of the zombie-subscriber leak.

Same shape as ``test_sandbox_stop_delete_recovery.py`` — drives a REAL
server on ``localhost:7778`` against a parametrized provider × agent_type,
auto-skipping any combo whose credential / dep is missing. Reuses that
module's provider-aware helpers (``_quick_session``, ``_get_sandbox``,
``_external_stop``, ``_require_provider``) so the kill mechanism isn't
hard-coded to one provider.

The bug: when a sandbox dies *mid-prompt*, ``_persist_prompt_events``
recovers by calling ``pool.get_session``, which hands the in-flight SSE
subscriber queue off from the dead session object (A) to a freshly
provisioned replacement (B). But the consumer's cleanup
(``iterate_subscriber``'s ``finally: self._subscribers.pop(sid)``) is
closure-bound to A, so it pops A's already-cleared dict and leaves a
permanent ZOMBIE entry in B's ``_subscribers``. Symptom: the session is
pinned in the pool's in-memory ``_active`` forever (``reap_idle`` skips
any session whose ``_subscribers`` is non-empty), so the dashboard shows
it leased/busy forever and the backing sandbox leaks until its own time
limit.

Observable contract (HTTP only, no white-box pool access):
  * ``POST /sessions/{id}/message`` runs the SAME
    ``_execute_and_stream_sse_for`` generator in a background drain, so it
    registers a subscriber on session A even with no client on /events.
  * ``GET /sessions/{id}/status`` carries the session id in the path, so
    the LB consistent-hashes it to the OWNING replica, and returns
    ``session_subscriber_count`` in ``peek`` mode (no cold-recovery side
    effect). With no client connected, that count MUST settle back to 0
    once the prompt's rpc terminates. A stuck non-zero count is the zombie.

Pre-fix this FAILS (count stuck >= 1); post-fix it PASSES. Verified by
toggling ``iterate_subscriber``'s finally between ``self`` and ``owner``.
"""
from __future__ import annotations

import asyncio
import os
import sys
import time

import httpx
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from agent_sdk import ApiClient  # noqa: E402
from tests._acp_runtimes import agent_type_param  # noqa: E402

# Reuse the golden recovery suite's provider-aware helpers so the kill
# path, session creation, and credential/dep skips stay identical.
from tests.test_sandbox_stop_delete_recovery import (  # noqa: E402
    SERVER,
    _CREATED_SESSIONS,
    _external_stop,
    _get_sandbox,
    _quick_session,
    _require_provider,
    _send_message,
)

# Same provider matrix as the golden recovery tests.
_PROVIDERS = ["daytona", "docker", "unix_local", "modal"]

# How many mid-prompt-death cycles to try. The hand-off window is the
# whole duration of execute_prompt, but a too-fast turn can finish before
# the stop lands; a few attempts make the repro reliable on buggy code.
_ATTEMPTS = 3
# Delay after POST returns rpc_id before stopping — long enough for the
# background drain to register its subscriber and start execute_prompt.
_KILL_DELAY_S = 0.6
_TERMINAL_TIMEOUT_S = 180.0
_SETTLE_TIMEOUT_S = 15.0
# A turn long enough to still be running when the stop lands.
_LONG_PROMPT = "Count from 1 to 40, one number per line. Do not stop early."


@pytest.fixture(autouse=True)
def _auto_destroy_test_sandboxes():
    """Mirror the golden suite's teardown: DELETE every session that
    ``_quick_session`` registered, even if the test body raised."""
    _CREATED_SESSIONS.clear()
    yield
    sessions = list(_CREATED_SESSIONS)
    _CREATED_SESSIONS.clear()
    with httpx.Client() as c:
        for sid in sessions:
            try:
                c.delete(f"{SERVER}/sessions/{sid}", timeout=30)
            except Exception:
                pass


# --- leak-specific helpers (HTTP-only observables) -------------------------

async def _wait_terminal(sdk: ApiClient, sid: str, rpc: str, timeout: float) -> str | None:
    """Poll /log until a turn_end / error row for ``rpc`` appears."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        resp = await sdk._http.get(f"/sessions/{sid}/log?limit=300", timeout=10)
        if resp.status_code == 200:
            for e in resp.json():
                if (e.get("payload") or {}).get("prompt_id") == rpc and \
                        e.get("event_type") in ("turn_end", "error"):
                    return e["event_type"]
        await asyncio.sleep(1.0)
    return None


async def _subscriber_count(sdk: ApiClient, sid: str) -> int:
    resp = await sdk._http.get(f"/sessions/{sid}/status", timeout=10)
    resp.raise_for_status()
    return int(resp.json().get("session_subscriber_count") or 0)


async def _wait_count(sdk: ApiClient, sid: str, target: int, timeout: float) -> bool:
    """True if the subscriber count reaches ``target`` within ``timeout``."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        if await _subscriber_count(sdk, sid) == target:
            return True
        await asyncio.sleep(0.5)
    return False


async def _post_and_wait(sdk: ApiClient, sid: str, msg: str) -> None:
    rpc = await _send_message(sdk, sid, msg)
    term = await _wait_terminal(sdk, sid, rpc, _TERMINAL_TIMEOUT_S)
    assert term is not None, f"warm turn never terminated (rpc={rpc[:8]})"


# --- the reproduction ------------------------------------------------------

@pytest.mark.parametrize("provider", _PROVIDERS)
@agent_type_param
@pytest.mark.asyncio
@pytest.mark.timeout(900)
async def test_midprompt_recovery_does_not_leak_subscriber(provider, agent_type):
    """A mid-prompt sandbox death triggers the recovery hand-off; once the
    prompt's rpc terminates and no client is connected, the session must
    have ZERO subscribers. A stuck count is the zombie-subscriber leak
    that pins the session against the idle reaper (the sandbox leak)."""
    _require_provider(provider)

    async with ApiClient(SERVER) as sdk:
        sess = await _quick_session(sdk, provider, agent_type=agent_type)
        sid = sess["session_id"]

        # Warm turn: supervisor up + a finished turn (session/load
        # contract) so recovery resumes rather than restarts cold.
        await _post_and_wait(sdk, sid, "Reply with the single word: ready.")
        assert await _wait_count(sdk, sid, 0, 15.0), (
            "baseline broken: an idle session with no client should have 0 "
            f"subscribers, got {await _subscriber_count(sdk, sid)}"
        )

        leaked_at: int | None = None
        for attempt in range(_ATTEMPTS):
            sandbox = await _get_sandbox(sdk, sid)

            # Fire a multi-second turn, then kill the sandbox while
            # execute_prompt is in flight — the recovery hand-off window.
            rpc = await _send_message(sdk, sid, _LONG_PROMPT)
            await asyncio.sleep(_KILL_DELAY_S)
            stop_task = asyncio.create_task(_external_stop(sandbox))
            try:
                # Recovery + retry runs in the background drain; wait for
                # the rpc to terminate (turn_end on success, error on
                # give-up).
                term = await _wait_terminal(sdk, sid, rpc, _TERMINAL_TIMEOUT_S)
            finally:
                try:
                    await stop_task
                except Exception:
                    pass
            assert term is not None, (
                f"recovery never produced a terminal for rpc={rpc[:8]} "
                f"(provider={provider} attempt={attempt}) — prompt dropped"
            )

            # rpc done + no client connected => count MUST return to 0.
            # If it stays >=1, the handed-off subscriber leaked onto the
            # replacement session = the zombie.
            if not await _wait_count(sdk, sid, 0, _SETTLE_TIMEOUT_S):
                leaked_at = attempt
                break

            # Clean recovery — re-warm and try again to hit the window.
            await _post_and_wait(sdk, sid, "Reply with the single word: ok.")

        assert leaked_at is None, (
            "ZOMBIE SUBSCRIBER LEAK reproduced "
            f"(provider={provider} agent_type={agent_type} attempt={leaked_at}): "
            f"session_subscriber_count is stuck at {await _subscriber_count(sdk, sid)} "
            "with no client connected. The handed-off SSE queue's cleanup popped "
            "the OLD session, leaving a permanent entry on the replacement — "
            "reap_idle now skips this session forever and the sandbox leaks."
        )
