"""E2E: sandbox stop/delete recovery and session resume.

All tests require a live server on localhost:7778. Three test groups:

  1. stop  — external sandbox stop → server restarts same sandbox → same hostname,
             files at /tmp survive (same sandbox, /tmp is not volume but same process)

  2. delete — external sandbox delete → server provisions new sandbox on same volume
              → different hostname, files in the VOLUME working dir survive

  3. resume — session persists across ensure_session_live re-entrancy (simulates
              server restart by clearing SESSIONS in-process)

Each parametrized over provider ("local", "docker", "daytona") and skipped when
the provider is unavailable.
"""
from __future__ import annotations

import asyncio
import json
import os
import re as _re
import shutil
import subprocess
import sys
import time

import httpx
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

# Load env in order: project .env first (wins), then ~/.env as fallback
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))
load_dotenv(os.path.expanduser("~/.env"), override=False)

from api.sse import extract_sse_tag, parse_acp_event  # noqa: E402

SERVER = os.environ.get("AGENT_SERVER_URL", "http://localhost:7778")
DAYTONA_API_KEY = os.environ.get("DAYTONA_API_KEY")
OAUTH_TOKEN = os.environ.get("CLAUDE_CODE_OAUTH_TOKEN")
PROMPT_TIMEOUT = 180


# ---------------------------------------------------------------------------
# Provider availability guards
# ---------------------------------------------------------------------------

def _has_docker() -> bool:
    return shutil.which("docker") is not None and subprocess.run(
        ["docker", "info"], capture_output=True, timeout=5
    ).returncode == 0


def _has_daytona() -> bool:
    return bool(DAYTONA_API_KEY and OAUTH_TOKEN)


def _has_server() -> bool:
    try:
        return httpx.get(f"{SERVER}/health", timeout=3).status_code == 200
    except Exception:
        return False


def _require_provider(provider: str) -> None:
    """Call pytest.skip() if the provider isn't available. Call at test start."""
    if not _has_server():
        pytest.skip("server not running on localhost:7778")
    if provider == "daytona" and not _has_daytona():
        pytest.skip("DAYTONA_API_KEY + CLAUDE_CODE_OAUTH_TOKEN required")
    if provider == "docker" and not _has_docker():
        pytest.skip("docker not available")


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------

async def _quick_session(client: httpx.AsyncClient, provider: str) -> dict:
    body: dict = {"provider": provider, "agent_type": "claude"}
    if OAUTH_TOKEN:
        body["secrets"] = {"CLAUDE_CODE_OAUTH_TOKEN": OAUTH_TOKEN}
    resp = await client.post(f"{SERVER}/sessions/quick", json=body, timeout=180)
    assert resp.status_code == 200, f"quick session failed ({provider}): {resp.text}"
    return resp.json()


async def _get_sandbox(client: httpx.AsyncClient, session_id: str) -> dict:
    sess = await client.get(f"{SERVER}/sessions/{session_id}", timeout=10)
    sb_id = sess.json().get("current_sandbox_id") or sess.json().get("sandbox_id")
    sb = await client.get(f"{SERVER}/sandboxes/{sb_id}", timeout=10)
    return sb.json()


async def _send_message(client: httpx.AsyncClient, session_id: str, message: str) -> str:
    resp = await client.post(
        f"{SERVER}/sessions/{session_id}/message",
        json={"message": message},
        timeout=30,
    )
    assert resp.status_code == 200, f"message post failed: {resp.text}"
    return resp.json()["rpc_id"]


async def _collect_reply(client: httpx.AsyncClient, session_id: str, rpc_id: str) -> str:
    """Stream /events until stopReason arrives for rpc_id; return full text."""
    parts: list[str] = []
    deadline = time.time() + PROMPT_TIMEOUT
    async with client.stream(
        "GET", f"{SERVER}/sessions/{session_id}/events",
        timeout=PROMPT_TIMEOUT + 10,
    ) as stream:
        buf = ""
        async for chunk in stream.aiter_text():
            buf += chunk
            while "\n\n" in buf:
                block, buf = buf.split("\n\n", 1)
                tag = extract_sse_tag(block)
                if tag != rpc_id:
                    continue
                evt = parse_acp_event(block, rpc_id)
                if evt is None:
                    continue
                if evt["type"] == "text":
                    parts.append(evt["text"])
                elif evt["type"] == "done":
                    return "".join(parts)
                elif evt["type"] == "error":
                    raise RuntimeError(f"agent error: {evt['text']}")
            if time.time() > deadline:
                raise TimeoutError(f"no reply within {PROMPT_TIMEOUT}s for rpc {rpc_id}")
    return "".join(parts)


async def _ask(client: httpx.AsyncClient, session_id: str, message: str) -> str:
    rpc_id = await _send_message(client, session_id, message)
    return await _collect_reply(client, session_id, rpc_id)


# ---------------------------------------------------------------------------
# Provider-specific external stop/delete (simulate crash/kill, bypass server API)
# ---------------------------------------------------------------------------

async def _external_stop(sandbox: dict) -> None:
    provider = sandbox["provider"]
    ref = sandbox.get("sandbox_ref") or sandbox.get("provider_ref", "")
    loop = asyncio.get_event_loop()

    if provider == "daytona":
        from daytona_sdk import Daytona, DaytonaConfig
        daytona = Daytona(DaytonaConfig(api_key=DAYTONA_API_KEY))
        sb = await loop.run_in_executor(None, lambda: daytona.get(ref))
        await loop.run_in_executor(None, sb.stop)

    elif provider == "docker":
        await loop.run_in_executor(None, lambda: subprocess.run(
            ["docker", "stop", ref], capture_output=True, timeout=30
        ))

    elif provider == "local":
        # ref is the PID of the supervisor process
        try:
            pid = int(ref)
            os.kill(pid, 9)
        except (ValueError, ProcessLookupError):
            pass

    print(f"\n[test] externally stopped {provider} sandbox {ref[:20]}")


async def _external_delete(sandbox: dict) -> None:
    provider = sandbox["provider"]
    ref = sandbox.get("sandbox_ref") or sandbox.get("provider_ref", "")
    loop = asyncio.get_event_loop()

    if provider == "daytona":
        from daytona_sdk import Daytona, DaytonaConfig
        daytona = Daytona(DaytonaConfig(api_key=DAYTONA_API_KEY))
        sb = await loop.run_in_executor(None, lambda: daytona.get(ref))
        await loop.run_in_executor(None, lambda: daytona.delete(sb))

    elif provider == "docker":
        await loop.run_in_executor(None, lambda: subprocess.run(
            ["docker", "rm", "-f", ref], capture_output=True, timeout=30
        ))

    elif provider == "local":
        # Kill the process and remove the sandbox directory from the volume
        try:
            pid = int(ref)
            os.kill(pid, 9)
        except (ValueError, ProcessLookupError):
            pass

    print(f"\n[test] externally deleted {provider} sandbox {ref[:20]}")


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------

def _extract_kv(text: str, key: str) -> str | None:
    """Extract a KEY=value pair from agent output."""
    m = _re.search(rf"{key}=(\S+)", text)
    return m.group(1).strip("`\"'") if m else None


@pytest.mark.parametrize("provider", ["daytona", "docker", "local"])
@pytest.mark.asyncio
async def test_stop_sandbox_same_sandbox_after_restart(provider):
    """Stop sandbox externally → server restarts it → same sandbox_ref, sandbox responds."""
    _require_provider(provider)

    async with httpx.AsyncClient() as client:
        sess = await _quick_session(client, provider)
        session_id = sess["session_id"]
        print(f"\n[test:{provider}] session={session_id[:8]}")

        # Baseline: verify sandbox is live and record sandbox_ref from the API
        reply1 = await _ask(
            client, session_id,
            "Please run the shell command `hostname` and reply with a single line "
            "formatted as `HOSTNAME=<value>` so I can parse it.",
        )
        hostname_before = _extract_kv(reply1, "HOSTNAME")
        print(f"[test:{provider}] before stop — reply: {reply1[:200]!r}, hostname: {hostname_before}")

        sandbox_before = await _get_sandbox(client, session_id)
        sandbox_ref_before = sandbox_before.get("sandbox_ref") or sandbox_before.get("provider_ref", "")
        assert sandbox_ref_before, f"could not get sandbox_ref: {sandbox_before}"
        print(f"[test:{provider}] sandbox_ref before stop: {sandbox_ref_before[:20]}")

        # External stop
        await _external_stop(sandbox_before)
        await asyncio.sleep(3)

        # Followup — server must restart the SAME sandbox (not provision a new one)
        reply2 = await _ask(
            client, session_id,
            "Please run the shell command `hostname` again and reply with a single line "
            "formatted as `HOSTNAME=<value>`.",
        )
        hostname_after = _extract_kv(reply2, "HOSTNAME")
        print(f"[test:{provider}] after restart — reply: {reply2[:200]!r}, hostname: {hostname_after}")

        sandbox_after = await _get_sandbox(client, session_id)
        sandbox_ref_after = sandbox_after.get("sandbox_ref") or sandbox_after.get("provider_ref", "")
        print(f"[test:{provider}] sandbox_ref after restart: {sandbox_ref_after[:20]}")

        # Primary invariant: server must reuse the same sandbox (not provision a replacement)
        assert sandbox_ref_after == sandbox_ref_before, (
            f"sandbox_ref changed after stop/restart — server provisioned a NEW sandbox "
            f"instead of restarting the existing one: {sandbox_ref_before!r} → {sandbox_ref_after!r}"
        )
        # Secondary: agent is actually responding (not just asserting the API call worked)
        assert hostname_after, f"agent did not return a hostname after restart: {reply2}"


# ---------------------------------------------------------------------------
# Tests: delete recovery + volume persistence
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("provider", ["daytona", "docker", "local"])
@pytest.mark.asyncio
async def test_delete_sandbox_volume_persistence(provider):
    """Delete sandbox externally → new sandbox provisioned on same volume → files survive."""
    _require_provider(provider)

    marker = "recovery-test-marker.txt"

    async with httpx.AsyncClient() as client:
        sess = await _quick_session(client, provider)
        session_id = sess["session_id"]
        print(f"\n[test:{provider}] session={session_id[:8]}")

        # Baseline: create a marker file on the persistent home directory,
        # then read it back — combining the write+read in one turn forces
        # textual output (pure tool-use turns return empty SSE text).
        reply1 = await _ask(
            client, session_id,
            f"Please run this shell pipeline and tell me the output:\n"
            f"  echo 'volume-test' > ~/{marker} && cat ~/{marker}",
        )
        print(f"[test:{provider}] setup reply: {reply1[:300]!r}")
        assert "volume-test" in reply1, f"marker setup failed: {reply1}"

        # Capture sandbox_ref before delete
        sandbox = await _get_sandbox(client, session_id)
        sandbox_ref_before = sandbox.get("sandbox_ref") or sandbox.get("provider_ref", "")
        print(f"[test:{provider}] sandbox_ref before delete: {sandbox_ref_before[:20]}")

        # External delete
        await _external_delete(sandbox)
        await asyncio.sleep(3)

        # Followup — server must provision a NEW sandbox on same volume.
        # Ask the agent to read the marker and tell me what's in it.
        reply2 = await _ask(
            client, session_id,
            f"Please run this shell command and tell me the output:\n"
            f"  cat ~/{marker} || echo NOT_FOUND",
        )
        print(f"[test:{provider}] after delete reply: {reply2[:400]!r}")

        assert "NOT_FOUND" not in reply2, (
            f"marker file lost after sandbox delete — volume not persisted!\n{reply2}"
        )
        assert "volume-test" in reply2, (
            f"marker file content not found after delete:\n{reply2}"
        )

        sandbox_after = await _get_sandbox(client, session_id)
        sandbox_ref_after = sandbox_after.get("sandbox_ref") or sandbox_after.get("provider_ref", "")
        print(f"[test:{provider}] sandbox_ref after replacement: {sandbox_ref_after[:20]}")

        # After a delete the server must provision a DIFFERENT sandbox (new sandbox_ref)
        if sandbox_ref_before:
            assert sandbox_ref_after != sandbox_ref_before, (
                f"sandbox_ref unchanged after delete — old sandbox was restarted instead "
                f"of a new one being provisioned: {sandbox_ref_before!r}"
            )


# ---------------------------------------------------------------------------
# Tests: resume (session persists across re-connection)
# ---------------------------------------------------------------------------

# Neutral ticket-ID framing avoids Claude's "secret code = social engineering"
# guardrail. The agent will freely echo/recall TKT-<digits> tokens.

@pytest.mark.parametrize("provider", ["daytona", "docker", "local"])
@pytest.mark.asyncio
async def test_session_resume_after_stop(provider):
    """Full session resume: stop sandbox between turns, reconnect, context preserved."""
    _require_provider(provider)

    async with httpx.AsyncClient() as client:
        sess = await _quick_session(client, provider)
        session_id = sess["session_id"]
        print(f"\n[test:{provider}] session={session_id[:8]}")

        ticket = "TKT-78901"
        reply1 = await _ask(
            client, session_id,
            f"I'm tracking work under ticket ID {ticket}. Please acknowledge by echoing "
            f"the ticket ID back to me so I know you have it."
        )
        print(f"[test:{provider}] turn1: {reply1[:200]}")
        assert ticket in reply1, f"agent didn't echo the ticket ID: {reply1}"

        # Stop sandbox externally
        sandbox = await _get_sandbox(client, session_id)
        await _external_stop(sandbox)
        await asyncio.sleep(3)

        # Turn 2: open a FRESH httpx connection (simulates UI reconnect)
        async with httpx.AsyncClient() as client2:
            reply2 = await _ask(
                client2, session_id,
                "What was the ticket ID I mentioned earlier in this conversation? "
                "Reply with only the ticket ID."
            )
        print(f"[test:{provider}] turn2 (after stop+reconnect): {reply2[:200]}")
        assert ticket in reply2, (
            f"agent lost conversation context after stop/resume: {reply2}"
        )


@pytest.mark.parametrize("provider", ["daytona", "docker", "local"])
@pytest.mark.asyncio
async def test_session_resume_after_delete(provider):
    """Delete sandbox between turns → new sandbox → session context preserved via volume."""
    _require_provider(provider)

    async with httpx.AsyncClient() as client:
        sess = await _quick_session(client, provider)
        session_id = sess["session_id"]
        print(f"\n[test:{provider}] session={session_id[:8]}")

        ticket = "TKT-99501"
        reply1 = await _ask(
            client, session_id,
            f"I'm tracking work under ticket ID {ticket}. Please acknowledge by echoing "
            f"the ticket ID back to me so I know you have it."
        )
        print(f"[test:{provider}] turn1: {reply1[:200]}")
        assert ticket in reply1, f"agent didn't echo the ticket ID: {reply1}"

        # Delete sandbox externally
        sandbox = await _get_sandbox(client, session_id)
        await _external_delete(sandbox)
        await asyncio.sleep(3)

        async with httpx.AsyncClient() as client2:
            reply2 = await _ask(
                client2, session_id,
                "What was the ticket ID I mentioned earlier in this conversation? "
                "Reply with only the ticket ID."
            )
        print(f"[test:{provider}] turn2 (after delete+reconnect): {reply2[:200]}")
        assert ticket in reply2, (
            f"session context lost after sandbox delete — volume-backed conversation "
            f"state not restored: {reply2}"
        )


# ---------------------------------------------------------------------------
# Test: midstream sandbox stop (UI-flow reproduction)
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("provider", ["daytona", "docker", "local"])
@pytest.mark.asyncio
async def test_session_survives_midstream_sandbox_stop(provider):
    """SSE upstream death triggers sandbox recovery — MUST resume, not reset.

    Reproduces the exact failure mode from the UI flow: user chats, then
    the sandbox is stopped out-of-band (Daytona dashboard button, docker
    stop, kill -9 on local supervisor). The user doesn't POST a message
    right away — the server's own SSE reader detects the upstream death
    through failed reconnects and enters its background recovery path.

    Distinct from ``test_session_resume_after_stop`` which immediately sends
    a new POST /message and thereby exercises the ``_ensure_runtime_locked``
    path. The bug this test catches lives in the SSE-reader's own recovery
    block, which used to ALWAYS create a fresh session via ``session/new``
    regardless of whether the existing conversation was resumable — so the
    agent would silently start over with no memory of prior turns, and the
    DB's ``inner_session_id`` would be overwritten before the user's next
    message ever arrived.

    Regression guards:

      A. ``inner_session_id`` on the session row MUST NOT change across the
         recovery. If it changes, the server silently created a new
         conversation when it could have resumed — the exact bug.

      B. The agent recalls a product the USER never typed (only the agent
         replied with it in turn 1). Rules out the "agent echoes what's in
         the user-turn JSONL line" false-positive recall pattern.
    """
    _require_provider(provider)

    async with httpx.AsyncClient() as client:
        sess = await _quick_session(client, provider)
        session_id = sess["session_id"]
        inner_sid_before = sess["inner_session_id"]
        print(f"\n[test:{provider}] session={session_id[:8]} inner_sid={inner_sid_before}")

        # Turn 1: agent computes a value that was NOT in the prompt.
        # 317 * 419 = 132823. The user's message contains 317 and 419 but
        # not the product, so a later recall of 132823 proves the agent's
        # own reply was persisted and restored, not just echoed from the
        # user-turn line of the JSONL.
        reply1 = await _ask(
            client, session_id,
            "Please compute 317 * 419 (use `echo $((317*419))` in a shell if "
            "it helps). Reply on a single line as `PRODUCT=<value>` so I can "
            "parse it.",
        )
        print(f"[test:{provider}] turn1 reply: {reply1[:200]!r}")
        product = _extract_kv(reply1, "PRODUCT")
        assert product == "132823", (
            f"agent didn't compute the product correctly, cannot proceed: {reply1!r}"
        )

        # External stop — the exact UI scenario the user reproduced manually.
        sandbox = await _get_sandbox(client, session_id)
        print(f"[test:{provider}] stopping sandbox {sandbox.get('sandbox_ref', '?')[:20]} externally")
        await _external_stop(sandbox)

        # Wait for the SSE reader's retries to exhaust and the recovery
        # path to complete. Reader backoff is 1,2,4,8,10s over 5 retries
        # (~25–35s), plus ~10s to start the sandbox + attach the ACP
        # session. Budget 60s so we comfortably clear that window.
        print(f"[test:{provider}] waiting 60s for SSE-reader recovery to fire")
        await asyncio.sleep(60)

        # INVARIANT A — deterministic: inner_session_id in the live SessionState
        # must survive recovery. We read from /admin/sessions (in-memory), not
        # GET /sessions/{id} (DB) — the buggy SSE-reader recovery path mutates
        # state.inner_session_id in memory but doesn't upsert_session, so the
        # DB stays stale and the DB-backed check would silently pass.
        admin = (await client.get(f"{SERVER}/admin/sessions", timeout=10)).json()
        in_mem = next(
            (s for s in admin.get("sessions", []) if s["session_id"] == session_id),
            None,
        )
        assert in_mem is not None, f"session {session_id[:8]} missing from in-memory SESSIONS"
        inner_sid_after = in_mem.get("inner_session_id")
        print(f"[test:{provider}] in-memory inner_sid after recovery: {inner_sid_after}")
        assert inner_sid_after == inner_sid_before, (
            f"inner_session_id changed across SSE-reader recovery — server "
            f"silently created a new conversation instead of resuming the "
            f"existing one: {inner_sid_before!r} → {inner_sid_after!r}"
        )

        # INVARIANT B — behavioral recall. The number 132823 is the agent's
        # own computation from turn 1. Recalling it requires the assistant
        # turn to have landed on disk AND session/load to have actually
        # resumed it after recovery.
        reply2 = await _ask(
            client, session_id,
            "What was the product you computed earlier in this conversation? "
            "Reply with just the number.",
        )
        print(f"[test:{provider}] turn2 reply (after recovery): {reply2[:200]!r}")
        # Strip thousands separators and whitespace so "132,823" / "132 823" /
        # "132823" all count as a match. What we care about is that the
        # digits appear somewhere in the reply; the agent choosing to format
        # with commas is LLM flavor, not a recovery-path failure.
        normalized = _re.sub(r"[,\s_]", "", reply2)
        assert "132823" in normalized, (
            f"agent lost conversation context across SSE recovery — cannot "
            f"recall its own prior reply: {reply2!r}"
        )
