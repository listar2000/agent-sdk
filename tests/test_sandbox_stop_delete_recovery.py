"""E2E: sandbox stop/delete recovery and session resume.

All tests require a live server on localhost:7778. Four test groups
(8 test functions total, each parametrized over local/docker/daytona):

  1. stop  — external sandbox stop → server restarts same sandbox → same hostname,
             files at /tmp survive (same sandbox, /tmp is not volume but same process)

  2. delete — external sandbox delete → server provisions new sandbox on same volume
              → different hostname, files in the VOLUME working dir survive

  3. resume — session persists across ensure_session_live re-entrancy, including
              a midstream variant that exercises the SSE-reader's own recovery
              path (reader observes upstream EOF, rebinds, resumes)

  4. message-after-stop — POST /message races external stop. Three scenarios
              increasing in subtlety: no-delay (scheduler race), short-delay
              (reader has observed disconnect but still retrying), and
              persistent-SSE (UI holds /events open across turns — the path
              where the subscriber-kick-on-rebuild bug lived)

Skipped when the provider is unavailable (no docker daemon, no DAYTONA_API_KEY
+ CLAUDE_CODE_OAUTH_TOKEN, or no server on localhost:7778).
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
        # Kill the supervisor PID. Read it from the ``pid`` field if the
        # server exposes one (sandbox_ref may be a stable UUID, not the
        # PID, once the local provider supports restart-same-ref). Fall
        # back to treating ref itself as the PID for older server shapes.
        pid_str = sandbox.get("pid") or ref
        try:
            os.kill(int(pid_str), 9)
        except (ValueError, ProcessLookupError, TypeError):
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
        # "delete" = kill the supervisor AND remove the sandbox-alive marker
        # file. HOME stays intact (that's the volume data the test expects
        # to persist across delete). The marker is the signal local's
        # get_sandbox_status uses to distinguish delete (marker gone →
        # "missing" → reprovision, new ref) from stop (marker intact →
        # "stopped" → restart in place, same ref).
        try:
            pid_str = sandbox.get("pid") or ref
            os.kill(int(pid_str), 9)
        except (ValueError, ProcessLookupError, TypeError):
            pass
        marker = sandbox.get("marker_path")
        if marker:
            try:
                os.remove(marker)
            except FileNotFoundError:
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

async def _inner_sid_in_memory(client: httpx.AsyncClient, session_id: str) -> str | None:
    admin = (await client.get(f"{SERVER}/admin/sessions", timeout=10)).json()
    row = next((s for s in admin.get("sessions", []) if s["session_id"] == session_id), None)
    return row.get("inner_session_id") if row else None


@pytest.mark.parametrize("provider", ["daytona", "docker", "local"])
@pytest.mark.asyncio
async def test_session_resume_after_stop(provider):
    """Full session resume: stop sandbox between turns, reconnect, session is
    LOADED (not recreated).

    Deterministic invariants (no LLM-prose dependency):
      A. Turn 2 returns a non-empty reply.
      B. ``inner_session_id`` on the in-memory SessionState is unchanged
         across stop+resume — proves the server did ``session/load``, not
         ``session/new``.
    """
    _require_provider(provider)

    async with httpx.AsyncClient() as client:
        sess = await _quick_session(client, provider)
        session_id = sess["session_id"]
        inner_before = sess["inner_session_id"]
        print(f"\n[test:{provider}] session={session_id[:8]} inner_sid={inner_before}")

        reply1 = await _ask(client, session_id, "Reply with a single short word.")
        assert reply1.strip(), f"turn 1 empty: {reply1!r}"

        # Stop sandbox externally
        sandbox = await _get_sandbox(client, session_id)
        await _external_stop(sandbox)
        await asyncio.sleep(3)

        # Turn 2: open a FRESH httpx connection (simulates UI reconnect)
        async with httpx.AsyncClient() as client2:
            reply2 = await _ask(client2, session_id, "Reply with a single short word.")
            inner_after = await _inner_sid_in_memory(client2, session_id)

        assert reply2.strip(), f"turn 2 empty after stop+resume: {reply2!r}"
        assert inner_after == inner_before, (
            f"session/load did not run — conversation restarted from scratch: "
            f"{inner_before!r} → {inner_after!r}"
        )


@pytest.mark.parametrize("provider", ["daytona", "docker", "local"])
@pytest.mark.asyncio
async def test_session_resume_after_delete(provider):
    """Delete sandbox between turns → NEW sandbox provisioned → session/load
    restores conversation via the persistent volume.

    Deterministic invariants:
      A. Turn 2 returns a non-empty reply.
      B. ``inner_session_id`` unchanged — session/load succeeded against
         the volume-persisted JSONL on the replacement sandbox.
    """
    _require_provider(provider)

    async with httpx.AsyncClient() as client:
        sess = await _quick_session(client, provider)
        session_id = sess["session_id"]
        inner_before = sess["inner_session_id"]
        print(f"\n[test:{provider}] session={session_id[:8]} inner_sid={inner_before}")

        reply1 = await _ask(client, session_id, "Reply with a single short word.")
        assert reply1.strip(), f"turn 1 empty: {reply1!r}"

        sandbox = await _get_sandbox(client, session_id)
        await _external_delete(sandbox)
        await asyncio.sleep(3)

        async with httpx.AsyncClient() as client2:
            reply2 = await _ask(client2, session_id, "Reply with a single short word.")
            inner_after = await _inner_sid_in_memory(client2, session_id)

        assert reply2.strip(), f"turn 2 empty after delete+resume: {reply2!r}"
        assert inner_after == inner_before, (
            f"session context lost after sandbox delete — session/load did "
            f"not restore the volume-backed JSONL: "
            f"{inner_before!r} → {inner_after!r}"
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


@pytest.mark.parametrize("provider", ["daytona", "docker", "local"])
@pytest.mark.asyncio
async def test_message_immediately_after_stop(provider):
    """Turn 1 → stop sandbox → turn 2 with NO sleep. Reproduces the race
    the user flagged: the POST /message arrives before the server's SSE
    reader has observed the upstream disconnect, so ensure_session_live's
    liveness check may still trust a supervisor that's about to die (or
    just died). The prompt is submitted; the response is 'missed' because
    the supervisor never acks / the SSE stream never delivers the events.

    The invariants we assert are deterministic (LLM-prose-independent):

      A. Turn 2 returns a non-empty reply within the timeout — the server
         didn't silently swallow the prompt.
      B. ``inner_session_id`` is unchanged across the recovery — the
         server did ``session/load`` on the replacement sandbox instead
         of ``session/new``, preserving conversation context.

    Invariant B catches the exact bug the test docstring calls out
    (supervisor "never acks / SSE never delivers") without depending on
    the agent's cooperation to echo a ticket.

    This is a SEPARATE test from test_session_resume_after_stop because
    that one has an explicit ``await asyncio.sleep(3)`` between stop and
    turn 2, which masks the race. The whole point here is no sleep.
    """
    _require_provider(provider)

    async with httpx.AsyncClient() as client:
        sess = await _quick_session(client, provider)
        session_id = sess["session_id"]
        inner_sid_before = sess["inner_session_id"]
        print(f"\n[test:{provider}] session={session_id[:8]} inner_sid={inner_sid_before}")

        # Turn 1 — confirm the agent is live.
        reply1 = await _ask(client, session_id, "Reply with a single short word.")
        assert reply1.strip(), f"turn 1 empty reply: {reply1!r}"
        print(f"[test:{provider}] turn1 ok")

        # External stop — NO sleep. This is the race we're after.
        sandbox = await _get_sandbox(client, session_id)
        await _external_stop(sandbox)
        print(f"[test:{provider}] sandbox stopped; immediately sending turn 2")

        # Turn 2 — the server must recover and deliver a reply.
        reply2 = await _ask(client, session_id, "Reply with a single short word.")
        print(f"[test:{provider}] turn2 reply len: {len(reply2)}")
        assert reply2.strip(), (
            f"turn 2 lost: server accepted the prompt but no reply came back "
            f"(probable race: SSE reader hadn't yet detected the dead "
            f"supervisor when ensure_session_live returned): {reply2!r}"
        )

        # Invariant B — inner_session_id must survive recovery; if it
        # changed, the server did session/new (new conversation) instead
        # of session/load. Read from /admin/sessions (in-memory) — the
        # buggy path updates state.inner_session_id without upsert_session.
        admin = (await client.get(f"{SERVER}/admin/sessions", timeout=10)).json()
        in_mem = next(
            (s for s in admin.get("sessions", []) if s["session_id"] == session_id),
            None,
        )
        assert in_mem is not None, f"session missing from /admin/sessions"
        assert in_mem.get("inner_session_id") == inner_sid_before, (
            f"inner_session_id changed across recovery — server silently "
            f"started a new conversation: {inner_sid_before!r} → "
            f"{in_mem.get('inner_session_id')!r}"
        )


@pytest.mark.parametrize("provider", ["daytona", "docker", "local"])
@pytest.mark.asyncio
async def test_message_after_stop_with_delay(provider):
    """Turn 1 → stop sandbox → wait ~4s → turn 2. User-reported repro.

    Different from test_message_immediately_after_stop: by the time we
    POST /message, the SSE reader has definitely observed the upstream
    disconnect, the reader task has exited, and _INSTANCES holds a stale
    entry whose supervisor URL no longer answers. The cache entry is
    "confidently dead" rather than "racing with the kill".

    User's exact words: "wait for just a few sec after the sandbox
    stopped. And then send a new msg, and no reply received."

    Invariants (deterministic — no LLM-prose dependency):
      A. Turn 2 returns a non-empty reply.
      B. ``inner_session_id`` is unchanged across recovery.
    """
    _require_provider(provider)

    async with httpx.AsyncClient() as client:
        sess = await _quick_session(client, provider)
        session_id = sess["session_id"]
        inner_sid_before = sess["inner_session_id"]
        print(f"\n[test:{provider}] session={session_id[:8]} inner_sid={inner_sid_before}")

        reply1 = await _ask(client, session_id, "Reply with a single short word.")
        assert reply1.strip(), f"turn 1 empty: {reply1!r}"
        print(f"[test:{provider}] turn1 ok")

        sandbox = await _get_sandbox(client, session_id)
        await _external_stop(sandbox)
        # Give the server's SSE reader time to observe the upstream
        # disconnect and tear down. This is the state the UI is in when
        # the user clicks send.
        await asyncio.sleep(4)
        print(f"[test:{provider}] stopped + waited 4s; sending turn 2")

        reply2 = await _ask(client, session_id, "Reply with a single short word.")
        print(f"[test:{provider}] turn2 reply len: {len(reply2)}")
        assert reply2.strip(), (
            f"turn 2 lost after 4s stop-delay: server did not recover "
            f"the dead sandbox before dispatching the prompt: {reply2!r}"
        )

        admin = (await client.get(f"{SERVER}/admin/sessions", timeout=10)).json()
        in_mem = next(
            (s for s in admin.get("sessions", []) if s["session_id"] == session_id),
            None,
        )
        assert in_mem is not None, f"session missing from /admin/sessions"
        assert in_mem.get("inner_session_id") == inner_sid_before, (
            f"inner_session_id changed across recovery — server silently "
            f"started a new conversation: {inner_sid_before!r} → "
            f"{in_mem.get('inner_session_id')!r}"
        )


# ---------------------------------------------------------------------------
# UI-path reproduction: persistent SSE across turn-1 / stop / turn-2
# ---------------------------------------------------------------------------

class _PersistentSse:
    """Matches the UI: one long-lived /events connection that reconnects on
    error, demuxing events by rpc_id into per-rpc queues. Events that arrive
    while no reader is reading them stay queued.

    The previous per-ask helper (_ask) opens a FRESH /events stream each
    time — that path always starts with a live subscriber before the prompt
    is dispatched, so the "new state has no subscribers" window is invisible
    to it. The UI doesn't; holding a persistent connection is the actual
    repro.
    """

    def __init__(self, client: httpx.AsyncClient, session_id: str) -> None:
        self._client = client
        self._session_id = session_id
        self._queues: dict[str, asyncio.Queue] = {}
        self._alive = True
        self._reader_task: asyncio.Task | None = None
        self._reconnected = asyncio.Event()  # flipped whenever a new stream opens

    async def __aenter__(self) -> "_PersistentSse":
        self._reader_task = asyncio.create_task(self._reader())
        # Wait for first connection so the subscriber is live before the
        # caller posts anything — matches the UI opening /events at session
        # load, then posting messages later.
        await asyncio.wait_for(self._reconnected.wait(), timeout=30)
        return self

    async def __aexit__(self, *_exc) -> None:
        self._alive = False
        if self._reader_task:
            self._reader_task.cancel()
            try:
                await self._reader_task
            except (asyncio.CancelledError, Exception):
                pass

    def get_queue(self, rpc_id: str) -> asyncio.Queue:
        return self._queues.setdefault(rpc_id, asyncio.Queue())

    async def _reader(self) -> None:
        attempt = 0
        while self._alive:
            try:
                async with self._client.stream(
                    "GET",
                    f"{SERVER}/sessions/{self._session_id}/events",
                    timeout=None,
                    headers={"Accept": "text/event-stream"},
                ) as stream:
                    if stream.status_code != 200:
                        raise RuntimeError(f"/events HTTP {stream.status_code}")
                    attempt = 0
                    self._reconnected.set()
                    buf = ""
                    async for chunk in stream.aiter_text():
                        if not self._alive:
                            return
                        buf += chunk
                        while "\n\n" in buf:
                            block, buf = buf.split("\n\n", 1)
                            tag = extract_sse_tag(block)
                            if tag is None:
                                continue
                            evt = parse_acp_event(block, tag)
                            if evt is None:
                                continue
                            await self._queues.setdefault(
                                tag, asyncio.Queue()
                            ).put(evt)
            except asyncio.CancelledError:
                return
            except Exception as e:
                if not self._alive:
                    return
                attempt += 1
                delay = min(1.0 * (2 ** (attempt - 1)), 10.0)
                print(f"[persistent-sse] stream error ({e!r}); "
                      f"reconnecting in {delay:.1f}s (attempt={attempt})")
                await asyncio.sleep(delay)


async def _ask_on_stream(
    client: httpx.AsyncClient, session_id: str, stream: _PersistentSse,
    message: str,
) -> str:
    """POST /message then drain the rpc's events off the persistent stream.

    Unlike `_ask`, this does NOT open a new /events connection — it uses
    the already-open one, matching the UI flow.
    """
    rpc_id = await _send_message(client, session_id, message)
    q = stream.get_queue(rpc_id)
    parts: list[str] = []
    deadline = time.time() + PROMPT_TIMEOUT
    while True:
        try:
            evt = await asyncio.wait_for(q.get(), timeout=max(1.0, deadline - time.time()))
        except asyncio.TimeoutError:
            raise TimeoutError(f"no reply within {PROMPT_TIMEOUT}s for rpc {rpc_id}")
        if evt["type"] == "text":
            parts.append(evt["text"])
        elif evt["type"] == "done":
            return "".join(parts)
        elif evt["type"] == "error":
            raise RuntimeError(f"agent error: {evt['text']}")


@pytest.mark.parametrize("provider", ["daytona", "docker", "local"])
@pytest.mark.asyncio
async def test_persistent_sse_stop_then_message(provider):
    """UI-shape repro: one persistent /events connection spans turn1 →
    external stop → a few seconds wait → turn 2.

    The UI keeps /events open the whole session. When the sandbox dies
    server-side, _ensure_runtime_locked's reusable-check health probe
    fails → it tears down the OLD state (kicking the UI subscriber) and
    builds a FRESH state with zero subscribers. The prompt dispatches to
    the new supervisor, events flow into the new state's subscriber list
    — which is empty until the UI reconnects. The UI reconnects with
    backoff (starts at 1s). Events that arrive before the reconnect land
    are lost.

    User's words: "I can reproduce this bug pretty consistently in the UI."
    """
    _require_provider(provider)

    async with httpx.AsyncClient() as client:
        sess = await _quick_session(client, provider)
        session_id = sess["session_id"]
        print(f"\n[test:{provider}] session={session_id[:8]}")

        async with _PersistentSse(client, session_id) as sse:
            ticket = "TKT-55501"
            reply1 = await _ask_on_stream(
                client, session_id, sse,
                f"I'm tracking work under ticket ID {ticket}. Please acknowledge by "
                f"echoing the ticket ID back to me so I know you have it.",
            )
            assert ticket in reply1, f"turn 1 didn't echo ticket: {reply1!r}"
            print(f"[test:{provider}] turn1 ok (persistent SSE held open)")

            sandbox = await _get_sandbox(client, session_id)
            await _external_stop(sandbox)
            await asyncio.sleep(4)
            print(f"[test:{provider}] stopped + waited 4s; sending turn 2 "
                  f"on the SAME persistent /events stream")

            reply2 = await _ask_on_stream(
                client, session_id, sse,
                "What was the ticket ID I mentioned earlier in this conversation? "
                "Reply with only the ticket ID.",
            )
            print(f"[test:{provider}] turn2 reply: {reply2[:200]!r}")
            assert ticket in reply2, (
                f"turn 2 lost on persistent SSE after stop+delay: the "
                f"server's state rebuild dropped the UI's subscribers and "
                f"events for the new prompt went nowhere: {reply2!r}"
            )


@pytest.mark.parametrize("provider", ["daytona", "docker", "local"])
@pytest.mark.asyncio
async def test_persistent_sse_external_delete_then_message(provider):
    """UI repro for an OUT-OF-BAND sandbox delete (Daytona dashboard, ``docker rm``,
    ``kill -9``) with a persistent /events stream held open.

    Different from test_persistent_sse_delete_sandbox_then_message, this
    one does NOT go through the server's DELETE endpoint — the server
    only learns the sandbox is gone when its SSE reader observes
    upstream disconnect. The reader-initiated recovery (``_rebind_state``
    or fresh provision) must keep the UI's subscriber list intact so
    the next /message's events reach the persistent stream.

    Invariant: turn 2 returns a non-empty reply on the SAME persistent
    /events stream the UI opened before the external delete.
    """
    _require_provider(provider)

    async with httpx.AsyncClient() as client:
        sess = await _quick_session(client, provider)
        session_id = sess["session_id"]
        print(f"\n[test:{provider}] session={session_id[:8]}")

        async with _PersistentSse(client, session_id) as sse:
            reply1 = await _ask_on_stream(
                client, session_id, sse, "Reply with a single short word.",
            )
            assert reply1.strip(), f"turn 1 empty: {reply1!r}"

            # Out-of-band delete — server finds out via SSE disconnect.
            sandbox = await _get_sandbox(client, session_id)
            await _external_delete(sandbox)
            await asyncio.sleep(4)

            reply2 = await _ask_on_stream(
                client, session_id, sse, "Reply with a single short word.",
            )
            assert reply2.strip(), (
                f"turn 2 lost on persistent SSE after external delete+delay: "
                f"the SSE reader's recovery path dropped the UI's subscribers. "
                f"Reply was: {reply2!r}"
            )


@pytest.mark.parametrize("provider", ["daytona", "docker", "local"])
@pytest.mark.asyncio
async def test_persistent_sse_delete_sandbox_then_message(provider):
    """UI repro for the `DELETE /sandboxes/{id}` + persistent /events flow.

    User-reported: open UI (which holds /events), send msg, wait for reply,
    delete the sandbox via the API, wait a few seconds, send another msg
    — agent never replies.

    Invariant (deterministic, no LLM-prose dependency):
      A. Turn 2 returns a non-empty reply on the PERSISTENT stream.

    The bug this catches is in ``delete_sandbox_route``: without
    ``force=True`` on _shutdown_session_state, the presence of the UI's
    /events subscriber makes the shutdown a no-op, leaving a zombie
    SessionState whose SSE reader is still retrying the dead URL. Fresh
    /message builds new state; events for the new prompt land on the
    fresh state's (empty) subscriber list. The UI's reconnect to /events
    lands on yet another state. Events lost; UI sees no reply.
    """
    _require_provider(provider)

    async with httpx.AsyncClient() as client:
        sess = await _quick_session(client, provider)
        session_id = sess["session_id"]
        inner_before = sess["inner_session_id"]
        print(f"\n[test:{provider}] session={session_id[:8]} inner_sid={inner_before}")

        async with _PersistentSse(client, session_id) as sse:
            reply1 = await _ask_on_stream(
                client, session_id, sse, "Reply with a single short word.",
            )
            assert reply1.strip(), f"turn 1 empty: {reply1!r}"

            # DELETE the sandbox via the server API (the exact UI path).
            sess_row = (await client.get(f"{SERVER}/sessions/{session_id}", timeout=10)).json()
            sbid = sess_row.get("current_sandbox_id") or sess_row.get("sandbox_id")
            assert sbid, f"no current sandbox on session: {sess_row}"
            r = await client.delete(f"{SERVER}/sandboxes/{sbid}", timeout=30)
            assert r.status_code in (200, 204), f"delete sandbox failed: {r.text}"

            # Wait — the UI's SSE stream may observe stream-end here; the
            # _PersistentSse helper reconnects automatically.
            await asyncio.sleep(4)

            reply2 = await _ask_on_stream(
                client, session_id, sse, "Reply with a single short word.",
            )
            assert reply2.strip(), (
                f"turn 2 lost on persistent SSE after delete+delay: the "
                f"server's zombie-state path dropped events for the new "
                f"sandbox. Reply was: {reply2!r}"
            )


@pytest.mark.asyncio
async def test_session_survives_supervisor_dir_wiped_from_volume():
    """Stale ``volumes.supervisor_agent_types`` cache + missing supervisor.js
    on disk: server must detect, clear the cache, reinstall, and retry.

    Reproduces the production stack trace the user hit:
      RuntimeError: supervisor.js missing at <volume>/system/supervisor/
      supervisor.js; call install_supervisor first

    The volumes table had ``supervisor_agent_types = ['claude']`` so
    ``ensure_volume_supervisor`` took the fast-path skip, but the volume
    dir had been wiped out-of-band (container restart with a wiped
    ephemeral volume, a deploy that reset the bind-mount, etc.) — so
    ``local.create_sandbox`` crashed with 500 on the NEXT /events call.

    Local-only. The cache-invalidation + retry path is in ``_provision_new``;
    daytona/docker behave the same but the local provider is the cheapest
    way to exercise the disk-wipe.
    """
    provider = "local"
    _require_provider(provider)

    async with httpx.AsyncClient() as client:
        sess = await _quick_session(client, provider)
        session_id = sess["session_id"]
        print(f"\n[test:{provider}] session={session_id[:8]}")

        reply1 = await _ask(client, session_id, "Reply with a single short word.")
        assert reply1.strip(), f"turn 1 empty: {reply1!r}"

        # Destroy the current sandbox so the next /message goes through
        # _provision_new. We need that path to hit ``create_sandbox``.
        sandbox = await _get_sandbox(client, session_id)
        await _external_delete(sandbox)
        await asyncio.sleep(2)

        # Out-of-band wipe of the supervisor dir: the volume still has its
        # "claude installed" cache entry in Postgres, but the files are gone.
        vol_ref = os.path.expanduser("~/.agent-sdk/volumes/default-local")
        sup_dir = os.path.join(vol_ref, "system", "supervisor")
        if os.path.isdir(sup_dir):
            shutil.rmtree(sup_dir)
            print(f"[test:{provider}] wiped {sup_dir}")

        # Next turn: _provision_new → ensure_volume_supervisor (cache hit,
        # skip install) → create_sandbox raises RuntimeError (supervisor.js
        # missing). Server's retry path clears the cache + reinstalls + retries.
        reply2 = await _ask(client, session_id, "Reply with a single short word.")
        assert reply2.strip(), (
            f"server did not recover from stale supervisor cache — "
            f"the retry path in _provision_new should clear the cache and "
            f"reinstall. Reply was: {reply2!r}"
        )
