"""E2E: sandbox stop/delete recovery and session resume. THE golden suite.

This is the canonical golden file — run it ALONE with ``-n auto`` (see
CLAUDE.md). All tests require a live server on localhost:7778, most
parameterized over ``{daytona, docker, unix_local, modal}`` (a few omit
a provider where the failure mode doesn't apply). See tests/README.md
for per-test invariants. Six thematic groups:

  1. stop — external sandbox stop → server restarts same sandbox →
            same ``sandbox_ref``, files at /tmp survive.

  2. delete — external sandbox delete (or server-driven
              ``DELETE /sessions/{id}``) → server provisions new sandbox
              on same volume → different ``sandbox_ref``, files in the
              VOLUME working dir survive.

  3. resume — session persists across pool-mediated reattach,
              including a midstream variant that exercises the
              SSE-reader's own recovery path.

  4. message-after-stop — POST /message races external stop. Three
              scenarios increasing in subtlety: no-delay (scheduler
              race), short-delay (reader observed disconnect but still
              retrying), and persistent-SSE (UI holds /events open
              across turns).

  5. persistent-SSE + delete — variants matching real UI flow:
              server-side ``DELETE /sessions/{id}`` vs out-of-band
              delete (daytona dashboard / docker rm / kill -9), plus
              supervisor-killed-in-place and reconnect-gap replay.

  6. subscriber-leak — mid-prompt sandbox death triggers the recovery
              hand-off; once the rpc terminates with no client attached,
              the session's subscriber count MUST settle to 0. A stuck
              count is the zombie-subscriber leak that pins the session
              against the idle reaper (merged from the former
              ``test_subscriber_leak_recovery.py``).

  7. reaper-subscriber-decouple — an idle session (no prompt in flight)
              with a LEGITIMATELY open /events consumer MUST still
              hibernate when the reaper decision runs. A refusal means
              subscriber-presence is being counted as compute activity
              (the Bug B pin). Drives POST /sessions/{id}/reap?idle_s=0.

Skipped when the provider is unavailable (no docker daemon, no
DAYTONA_API_KEY + CLAUDE_CODE_OAUTH_TOKEN, no modal profile, or no
server on localhost:7778).
"""
from __future__ import annotations

import asyncio
import contextlib
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

from agent_sdk import ApiClient  # noqa: E402
from api.sse import extract_sse_tag, parse_acp_event  # noqa: E402
from tests._acp_runtimes import agent_type_param  # noqa: E402

SERVER = os.environ.get("AGENT_SERVER_URL", "http://localhost:7778")
DAYTONA_API_KEY = os.environ.get("DAYTONA_API_KEY")
OAUTH_TOKEN = os.environ.get("CLAUDE_CODE_OAUTH_TOKEN")
# Modal terminate is destructive: stop=>recreate a fresh container. With a
# warm image this is still ~3-4 min in practice (container schedule + tunnel
# setup + supervisor boot), so the recovery budget for modal must dominate
# the daytona/docker/local pause-resume ~10s. Daytona/docker/local successful
# paths complete in <30s so the larger ceiling only stretches their failure
# diagnosis time, not their happy-path runtime.
PROMPT_TIMEOUT = 360


# ---------------------------------------------------------------------------
# Provider availability guards
# ---------------------------------------------------------------------------

def _has_docker() -> bool:
    return shutil.which("docker") is not None and subprocess.run(
        ["docker", "info"], capture_output=True, timeout=5
    ).returncode == 0


def _has_daytona() -> bool:
    return bool(DAYTONA_API_KEY and OAUTH_TOKEN)


def _has_modal() -> bool:
    """Modal is available if the ``modal`` SDK imports and a profile is set.

    ``modal setup`` writes ~/.modal.toml with workspace credentials; we rely
    on that rather than an env var, matching how the provider itself looks
    up the SDK handle.
    """
    try:
        import modal  # noqa: F401
    except ImportError:
        return False
    modal_toml = os.path.expanduser("~/.modal.toml")
    return bool(OAUTH_TOKEN) and os.path.exists(modal_toml)


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
    if provider == "modal" and not _has_modal():
        pytest.skip("modal SDK + ~/.modal.toml + CLAUDE_CODE_OAUTH_TOKEN required")


# ---------------------------------------------------------------------------
# HTTP helpers (driven through agent_sdk.ApiClient — wire contract is
# pinned in test_api_client.py + server route tests)
# ---------------------------------------------------------------------------

_OPENROUTER_KEY = os.environ.get("OPENROUTER_API_KEY")

# Per-runtime defaults so a test can pass ``agent_type="opencode"`` (or
# leave the default ``"claude"``) and ``_quick_session`` picks the right
# model + secret env var. Pinned to the cheapest available models — the
# recovery suite issues tiny tool-use prompts and would burn budget on
# anything bigger.
_RUNTIME_DEFAULTS: dict[str, dict] = {
    "claude": {"model": "haiku", "secret_env": "CLAUDE_CODE_OAUTH_TOKEN"},
    "opencode": {"model": "openrouter/anthropic/claude-3.5-haiku",
                 "secret_env": "OPENROUTER_API_KEY"},
}


async def _quick_session(
    sdk: ApiClient, provider: str, *, agent_type: str = "claude",
) -> dict:
    # Pin haiku-class models for the recovery suite — sonnet's per-key weekly
    # quota trips before this suite finishes when other suites have run on
    # the same OAuth token recently. Haiku is on a separate quota
    # bucket and answers the tiny tool-use prompts these tests exercise
    # just as reliably. See server.py _sessions_create_eager — the
    # ``model`` body field is forwarded via ``set_model`` after the
    # SandboxSession is up.
    defaults = _RUNTIME_DEFAULTS[agent_type]
    body: dict = {
        "provider": provider,
        "agent_type": agent_type,
        "model": defaults["model"],
    }
    secret_val = os.environ.get(defaults["secret_env"])
    if secret_val:
        body["secrets"] = {defaults["secret_env"]: secret_val}
    sess = await sdk.create_session(**body)
    # Register for autouse-fixture teardown so the daytona/docker/local
    # sandbox provisioned by this session is destroyed even if the test
    # body raises. Defence-in-depth alongside the ``agent_sdk_origin``
    # label on every daytona create — if the test crashes after this
    # call but before its own cleanup, the fixture finalizer still runs.
    _CREATED_SESSIONS.append(sess["session_id"])
    return sess


# Sessions registered by `_quick_session` during the current test.
# Cleared and acted on by the ``_auto_destroy_test_sandboxes`` autouse
# fixture defined further down. Module-level rather than fixture-local so
# `_quick_session` callers don't need to plumb an extra param.
_CREATED_SESSIONS: list[str] = []


@pytest.fixture(autouse=True)
def _auto_destroy_test_sandboxes():
    """For every test that calls ``_quick_session``, destroy the sandbox(es)
    it created at teardown — even if the test body or its own ``finally``
    block didn't get there. Routes through ``DELETE /sessions/{id}``: a
    single round-trip releases the pool lease (snapshot + pause), drops
    the back-compat sandboxes row, and deletes the session row.
    Idempotent — missing sessions return 204, not 404, so this is safe
    after a test that already deleted its own sandbox.

    Errors are swallowed (best-effort cleanup) and the underlying daytona
    sandbox still has the ``agent_sdk_origin`` label as a backup so a
    crashed-mid-cleanup orphan can be picked up by
    ``scripts/cleanup_orphans.py``.
    """
    _CREATED_SESSIONS.clear()
    yield
    sessions = list(_CREATED_SESSIONS)
    _CREATED_SESSIONS.clear()
    if not sessions:
        return
    with httpx.Client() as c:
        for sid in sessions:
            try:
                c.delete(f"{SERVER}/sessions/{sid}", timeout=30)
            except Exception:
                # Best-effort — label-based cleanup catches the rest.
                pass


async def _get_sandbox(sdk: ApiClient, session_id: str) -> dict:
    """Read sandbox metadata via the session-scoped route — single
    round-trip, no sandboxes-table dependency. Returns the same shape
    as the legacy ``GET /sandboxes/{id}`` (provider, sandbox_ref,
    status, root, url, marker_path) so the ``_external_*`` helpers
    don't need to change."""
    return await sdk.get_session_sandbox(session_id)


async def _send_message(sdk: ApiClient, session_id: str, message: str) -> str:
    resp = await sdk.send_message(session_id, message)
    return resp["rpc_id"]


async def _collect_reply(sdk: ApiClient, session_id: str, rpc_id: str) -> str:
    """Stream /events until stopReason arrives for rpc_id; return full text."""
    parts: list[str] = []
    deadline = time.time() + PROMPT_TIMEOUT
    buf = b""
    async for chunk in sdk.stream_events(session_id):
        buf += chunk
        while b"\n\n" in buf:
            raw, buf = buf.split(b"\n\n", 1)
            block = raw.decode("utf-8", errors="replace")
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


async def _ask(sdk: ApiClient, session_id: str, message: str) -> str:
    rpc_id = await _send_message(sdk, session_id, message)
    return await _collect_reply(sdk, session_id, rpc_id)


async def _admin_session_row(sdk: ApiClient, session_id: str) -> dict | None:
    """Read the in-memory session row from /admin/sessions.

    Goes through ``sdk._http`` because /admin/sessions is an operator
    debug route, not part of ApiClient's first-class surface.
    """
    resp = await sdk._http.get("/admin/sessions", timeout=10)
    resp.raise_for_status()
    admin = resp.json()
    sessions = admin.get("sessions", [])
    hit = next(
        (s for s in sessions if s["session_id"] == session_id),
        None,
    )
    if hit is None:
        # Diagnostic — emit on misses so the goldens' under-load failure
        # mode is observable in pytest captured output.
        ids = [s.get("session_id", "?")[:8] for s in sessions]
        print(
            f"[_admin_session_row MISS] session={session_id[:8]} "
            f"admin returned {len(sessions)} rows; "
            f"this_replica={admin.get('this_replica')!r}; "
            f"first 10 ids={ids[:10]}"
        )
    return hit


async def _admin_inner_sid(sdk: ApiClient, session_id: str) -> str | None:
    row = await _admin_session_row(sdk, session_id)
    return row.get("inner_session_id") if row else None


# ---------------------------------------------------------------------------
# Provider-specific external stop/delete (simulate crash/kill, bypass server API)
# ---------------------------------------------------------------------------

def _local_supervisor_pid(sandbox: dict) -> int | None:
    """Find the supervisor PID for a local-provider sandbox.

    The supervisor process under the pool flow isn't surfaced via ``pid`` on
    ``GET /sandboxes/{id}`` (only the legacy ``_INSTANCES`` path populated
    that field). Discover it from properties that ARE always exposed: the
    URL's port (port-based providers always expose ``url``) — the supervisor
    is the only process bound to that port. Tries ``lsof`` first (most
    portable), falls back to ``pgrep -f`` matching the supervisor's
    ``--port <port>`` cmdline arg in case lsof is unavailable.
    """
    pid = sandbox.get("pid")
    if pid is not None:
        try:
            return int(pid)
        except (TypeError, ValueError):
            return None
    url = sandbox.get("url") or ""
    try:
        port = int(url.rsplit(":", 1)[-1].split("/", 1)[0])
    except (ValueError, IndexError):
        return None

    for argv in (
        ["lsof", "-t", f"-iTCP:{port}", "-sTCP:LISTEN"],
        ["pgrep", "-f", rf"supervisor\.js .*--port {port}\b"],
    ):
        try:
            result = subprocess.run(argv, capture_output=True, text=True, timeout=5)
        except (FileNotFoundError, subprocess.SubprocessError):
            continue
        for line in result.stdout.strip().splitlines():
            try:
                return int(line.strip())
            except ValueError:
                continue
    return None


async def _external_stop(sandbox: dict) -> None:
    provider = sandbox["provider"]
    ref = sandbox.get("sandbox_ref") or sandbox.get("provider_ref", "")
    loop = asyncio.get_event_loop()

    if provider == "daytona":
        from daytona_sdk import Daytona, DaytonaConfig
        daytona = Daytona(DaytonaConfig(api_key=DAYTONA_API_KEY))
        sb = await loop.run_in_executor(None, lambda: daytona.get(ref))
        # Daytona's ``sb.stop()`` issues the stop request AND then polls
        # ``GET /sandboxes/{id}`` until state stabilises. Under 32-way
        # parallel test load the control plane occasionally returns 502
        # on those polls (their CDN/edge layer, not the sandbox itself).
        # The stop HAS been accepted; the sandbox is transitioning to
        # ``stopped``. Verify by polling the state ourselves and treat
        # final state ∈ {stopped, paused, archived} as success.
        try:
            await loop.run_in_executor(None, sb.stop)
        except Exception as e:
            msg = str(e)
            if "502" not in msg and "Bad Gateway" not in msg:
                raise
            # Confirm the stop actually took effect.
            STABLE_STOPPED = {"stopped", "paused", "archived"}
            deadline = loop.time() + 30.0
            final_state = ""
            while loop.time() < deadline:
                try:
                    sb_now = await loop.run_in_executor(None, lambda: daytona.get(ref))
                    raw = sb_now.state
                    final_state = (raw.value if hasattr(raw, "value") else str(raw)).lower()
                    if final_state in STABLE_STOPPED:
                        break
                except Exception:
                    pass  # transient again; loop and retry the state read
                await asyncio.sleep(1.0)
            if final_state not in STABLE_STOPPED:
                raise RuntimeError(
                    f"daytona sandbox {ref[:16]} did not reach stopped "
                    f"state after 502 on sb.stop(); final_state={final_state!r}"
                )

    elif provider == "docker":
        await loop.run_in_executor(None, lambda: subprocess.run(
            ["docker", "stop", ref], capture_output=True, timeout=30
        ))

    elif provider == "unix_local":
        pid = await loop.run_in_executor(None, _local_supervisor_pid, sandbox)
        if pid is not None:
            try:
                os.kill(pid, 9)
            except (ProcessLookupError, PermissionError):
                pass

    elif provider == "modal":
        # Modal has no stop==pause: terminate is destructive. The server
        # recovers via the SandboxMissingError path, same as external delete.
        import modal
        sb = await loop.run_in_executor(None, lambda: modal.Sandbox.from_id(ref))
        await loop.run_in_executor(None, sb.terminate)

    print(f"\n[test] externally stopped {provider} sandbox {ref[:20]}")


async def _kill_supervisor_in_sandbox(sandbox: dict) -> None:
    """Kill ONLY the supervisor.js process inside the sandbox — the sandbox
    itself stays alive. Mimics prod's '502 Bad Gateway' scenario where the
    daytona proxy forwards to port 9100 but no process listens there
    (supervisor OOM'd, crashed, or was killed by the runtime). The server's
    DB still thinks the sandbox is fine; only the supervisor is gone.
    """
    provider = sandbox["provider"]
    ref = sandbox.get("sandbox_ref") or sandbox.get("provider_ref", "")
    loop = asyncio.get_event_loop()

    if provider == "daytona":
        from daytona_sdk import Daytona, DaytonaConfig
        daytona = Daytona(DaytonaConfig(api_key=DAYTONA_API_KEY))
        sb = await loop.run_in_executor(None, lambda: daytona.get(ref))
        await loop.run_in_executor(
            None,
            lambda: sb.process.exec(
                "pkill -9 -f supervisor.js || pkill -9 -f 'node.*supervisor'",
                timeout=10,
            ),
        )

    elif provider == "unix_local":
        # Local has no separate "supervisor inside sandbox" — the supervisor
        # IS the sandbox process. Same kill path as _external_stop.
        pid = await loop.run_in_executor(None, _local_supervisor_pid, sandbox)
        if pid is not None:
            try:
                os.kill(pid, 9)
            except (ProcessLookupError, PermissionError):
                pass

    elif provider == "docker":
        # docker exec into the container and kill the supervisor PID 1.
        # Without pid 1 the container exits; use pkill within the container.
        await loop.run_in_executor(None, lambda: subprocess.run(
            ["docker", "exec", ref, "pkill", "-9", "-f", "supervisor.js"],
            capture_output=True, timeout=10,
        ))

    elif provider == "modal":
        # ``sb.exec`` runs a command in the live modal sandbox. We pkill the
        # supervisor.js so the sandbox object stays alive but its tunnel
        # target stops responding — same scenario as docker/daytona above.
        import modal
        sb = await loop.run_in_executor(None, lambda: modal.Sandbox.from_id(ref))
        proc = await loop.run_in_executor(
            None,
            lambda: sb.exec(
                "bash", "-c",
                "pkill -9 -f supervisor.js || pkill -9 -f 'node.*supervisor'",
            ),
        )
        await loop.run_in_executor(None, proc.wait)

    print(f"[test] killed supervisor inside {provider} sandbox {ref[:20]}")


async def _external_delete(sandbox: dict) -> None:
    provider = sandbox["provider"]
    ref = sandbox.get("sandbox_ref") or sandbox.get("provider_ref", "")
    loop = asyncio.get_event_loop()

    if provider == "daytona":
        from daytona_sdk import Daytona, DaytonaConfig
        daytona = Daytona(DaytonaConfig(api_key=DAYTONA_API_KEY))
        sb = await loop.run_in_executor(None, lambda: daytona.get(ref))
        # Same 502 robustness as ``_external_stop``: the delete may be
        # initiated successfully but the polling for completion can 502
        # under load. Verify final state via direct ``get()`` rather than
        # trusting the SDK's internal poll.
        try:
            await loop.run_in_executor(None, lambda: daytona.delete(sb))
        except Exception as e:
            msg = str(e)
            if "502" not in msg and "Bad Gateway" not in msg:
                raise
            # Fall through to the poll below — it confirms the delete by
            # observing get(ref) raising (sandbox gone).
        # Wait for daytona's internal state to settle. Without this, the
        # NEXT test's daytona.create can race the delete's cleanup and
        # get "An unexpected error occurred" from the API. Poll until
        # get(ref) raises (sandbox is gone), with a bounded timeout.
        deadline = asyncio.get_event_loop().time() + 30.0
        deleted = False
        while asyncio.get_event_loop().time() < deadline:
            try:
                await loop.run_in_executor(None, lambda: daytona.get(ref))
                await asyncio.sleep(0.5)
            except Exception:
                deleted = True
                break  # get raised → sandbox is gone from daytona's index
        if not deleted:
            raise RuntimeError(
                f"daytona sandbox {ref[:16]} not confirmed deleted within 30s"
            )

    elif provider == "docker":
        await loop.run_in_executor(None, lambda: subprocess.run(
            ["docker", "rm", "-f", ref], capture_output=True, timeout=30
        ))

    elif provider == "unix_local":
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

    elif provider == "modal":
        import modal
        sb = await loop.run_in_executor(None, lambda: modal.Sandbox.from_id(ref))
        await loop.run_in_executor(None, sb.terminate)

    print(f"\n[test] externally deleted {provider} sandbox {ref[:20]}")


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------

def _extract_kv(text: str, key: str) -> str | None:
    """Extract a KEY=value pair from agent output."""
    m = _re.search(rf"{key}=(\S+)", text)
    return m.group(1).strip("`\"'") if m else None


# Modal is omitted here: its "stop" terminates the sandbox (no pause state),
# so recovery always yields a fresh Modal object_id — the stable-ref invariant
# simply doesn't apply. Session continuity is covered by
# ``test_session_resume_after_stop[modal]`` instead.
@pytest.mark.parametrize("provider", ["daytona", "docker", "unix_local"])
@agent_type_param
@pytest.mark.asyncio
async def test_stop_sandbox_same_sandbox_after_restart(provider, agent_type):
    """Stop sandbox externally → server restarts it → same sandbox_ref, sandbox responds.

    Parameterised over ``agent_type`` because the recovery path goes
    through ACP ``session/load`` and the per-runtime ``set_model``
    replay — both of which had opencode-specific bugs that this test
    catches when run with ``agent_type="opencode"``.
    """
    _require_provider(provider)

    async with ApiClient(SERVER) as sdk:
        sess = await _quick_session(sdk, provider, agent_type=agent_type)
        session_id = sess["session_id"]
        print(f"\n[test:{provider}] session={session_id[:8]}")

        # Baseline: verify sandbox is live and record sandbox_ref from the API
        reply1 = await _ask(
            sdk, session_id,
            "Please run the shell command `hostname` and reply with a single line "
            "formatted as `HOSTNAME=<value>` so I can parse it.",
        )
        hostname_before = _extract_kv(reply1, "HOSTNAME")
        print(f"[test:{provider}] before stop — reply: {reply1[:200]!r}, hostname: {hostname_before}")

        sandbox_before = await _get_sandbox(sdk, session_id)
        sandbox_ref_before = sandbox_before.get("sandbox_ref") or sandbox_before.get("provider_ref", "")
        assert sandbox_ref_before, f"could not get sandbox_ref: {sandbox_before}"
        print(f"[test:{provider}] sandbox_ref before stop: {sandbox_ref_before[:20]}")

        # External stop
        await _external_stop(sandbox_before)
        await asyncio.sleep(3)

        # Followup — server must restart the SAME sandbox (not provision a new one)
        reply2 = await _ask(
            sdk, session_id,
            "Please run the shell command `hostname` again and reply with a single line "
            "formatted as `HOSTNAME=<value>`.",
        )
        hostname_after = _extract_kv(reply2, "HOSTNAME")
        print(f"[test:{provider}] after restart — reply: {reply2[:200]!r}, hostname: {hostname_after}")

        sandbox_after = await _get_sandbox(sdk, session_id)
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

async def _write_marker_and_read(sdk, session_id, marker):
    """Turn-1: have the agent write marker and echo content back so
    stdout is non-empty (pure tool-use turns return no SSE text)."""
    return await _ask(
        sdk, session_id,
        f"Please run this shell pipeline and tell me the output:\n"
        f"  echo 'volume-test' > ~/{marker} && cat ~/{marker}",
    )


async def _read_marker(sdk, session_id, marker):
    # Send a prompt — this triggers cold-recovery on the released session
    # and runs ``cat`` via the agent's bash tool. We harvest the *tool
    # result* events from the SSE stream (deterministic shell stdout),
    # not the LLM's prose reply: opencode + verbose claude variants
    # frequently paraphrase ("the file was read successfully") without
    # echoing the bytes, and the test would false-fail on a perfectly-
    # restored sandbox.
    rpc_id = await _send_message(
        sdk,
        session_id,
        f"Please run this shell command and tell me the output:\n"
        f"  cat ~/{marker} || echo __MARKER_ABSENT__",
    )
    text_parts: list[str] = []
    tool_outputs: list[str] = []
    deadline = time.time() + PROMPT_TIMEOUT
    buf = b""
    async for chunk in sdk.stream_events(session_id):
        buf += chunk
        while b"\n\n" in buf:
            raw, buf = buf.split(b"\n\n", 1)
            block = raw.decode("utf-8", errors="replace")
            tag = extract_sse_tag(block)
            if tag is not None and tag != rpc_id:
                continue
            ev = parse_acp_event(block, rpc_id)
            if ev is None:
                continue
            if ev.get("type") == "text":
                text_parts.append(ev.get("text", ""))
            elif ev.get("type") == "tool_result":
                result = ev.get("result")
                if isinstance(result, dict):
                    out = result.get("stdout") or result.get("output")
                    if isinstance(out, str):
                        tool_outputs.append(out)
                elif isinstance(result, str):
                    tool_outputs.append(result)
            elif ev.get("type") == "done":
                # Tool stdout wins because it's the real bytes; LLM prose
                # is the fallback if a runtime doesn't surface
                # tool_result content (some adapters omit ``stdout`` from
                # the result envelope).
                return "\n".join(tool_outputs) if tool_outputs else "".join(text_parts)
            elif ev.get("type") == "error":
                raise AssertionError(f"prompt errored: {ev}")
        if time.time() > deadline:
            raise TimeoutError(f"no reply within {PROMPT_TIMEOUT}s for rpc {rpc_id}")
    return "\n".join(tool_outputs) if tool_outputs else "".join(text_parts)


@pytest.mark.parametrize("provider", ["daytona", "docker", "unix_local", "modal"])
@agent_type_param
@pytest.mark.asyncio
async def test_server_delete_persists_workspace(provider, agent_type):
    """Server-mediated DELETE triggers a cold snapshot (filesystem_cache)
    before teardown, so arbitrary HOME files survive onto the replacement
    sandbox. Conversation continuity is also preserved (agent_memory).
    """
    _require_provider(provider)
    marker = "server-delete-marker.txt"

    async with ApiClient(SERVER) as sdk:
        sess = await _quick_session(sdk, provider, agent_type=agent_type)
        session_id = sess["session_id"]
        print(f"\n[test:{provider}] session={session_id[:8]}")

        reply1 = await _write_marker_and_read(sdk, session_id, marker)
        assert "volume-test" in reply1, f"marker setup failed: {reply1}"

        sandbox = await _get_sandbox(sdk, session_id)
        sandbox_ref_before = sandbox.get("sandbox_ref") or sandbox.get("provider_ref", "")
        # POST /sessions/{id}/release is the server-API-driven equivalent
        # of the legacy DELETE /sandboxes/{id}: snapshot to volume +
        # drop the pool's compute lease. Next prompt cold-recovers,
        # which is what "delete the sandbox" meant operationally.
        await sdk.release_session(session_id)
        await asyncio.sleep(3)

        reply2 = await _read_marker(sdk, session_id, marker)
        print(f"[test:{provider}] after server-delete reply: {reply2[:400]!r}")
        # Single positive check — marker content present means the file
        # survived the snapshot/teardown round-trip. Negative-presence on
        # the sentinel was redundant and false-positives on verbose models
        # (opencode explains the ``||`` fallback in prose).
        assert "volume-test" in reply2, (
            f"marker content missing after server DELETE — cold snapshot didn't run "
            f"before teardown: {reply2}"
        )

        # Note: sandbox_ref may or may not change after release(), depending
        # on the provider. Daytona pauses the sandbox in-place (same ref);
        # docker/local drop compute and the next prompt provisions fresh
        # (new ref). The contract this test pins is volume persistence —
        # the sandbox-ref-changed assertion was an implementation detail
        # of the legacy DELETE /sandboxes/{id} that always destroyed.
        await _get_sandbox(sdk, session_id)  # smoke-check the route


@pytest.mark.parametrize("provider", ["daytona", "docker", "unix_local"])
@agent_type_param
@pytest.mark.asyncio
async def test_delete_session_destroys_sandbox(provider, agent_type):
    """``DELETE /sessions/{id}`` must destroy the underlying sandbox, not
    just pause it. Hibernation (``stop_daytona`` / ``docker stop``) is
    correct for the idle reaper and ``POST /sessions/{id}/release`` —
    those paths leave the session row in place so a future prompt can
    resume. ``DELETE`` drops the session row, so the sandbox has nothing
    to resume to: leaving it paused leaks compute against the provider
    quota with no automatic cleanup (label-based reapers default to
    ``--origin test`` so production orphans require manual reaping).

    Reproduces the leak observed in production after hivespace's
    ``DELETE /api/agents/{N}`` cascade: 9 deleted agents in a single day
    left 6 paused-with-no-session-row daytona sandboxes against the
    2000 GiB account quota.
    """
    _require_provider(provider)

    async with ApiClient(SERVER) as sdk:
        sess = await _quick_session(sdk, provider, agent_type=agent_type)
        session_id = sess["session_id"]
        sandbox = await _get_sandbox(sdk, session_id)
        sandbox_ref = sandbox.get("sandbox_ref") or sandbox.get("provider_ref", "")
        assert sandbox_ref, f"fresh session has no sandbox_ref: {sandbox}"
        print(f"\n[test:{provider}] session={session_id[:8]} sandbox={sandbox_ref[:24]}")

        # Skip the autouse cleanup fixture's DELETE — we're calling
        # DELETE explicitly so we can assert on its post-condition.
        if session_id in _CREATED_SESSIONS:
            _CREATED_SESSIONS.remove(session_id)

        async with httpx.AsyncClient() as c:
            resp = await c.delete(f"{SERVER}/sessions/{session_id}", timeout=30)
        assert resp.status_code == 204, f"DELETE returned {resp.status_code}: {resp.text}"

        try:
            await _assert_sandbox_gone(provider, sandbox, timeout_s=20.0)
        except AssertionError:
            # Best-effort cleanup so we don't leak from this test itself.
            try:
                await _external_delete(sandbox)
            except Exception:
                pass
            raise


async def _assert_sandbox_gone(provider: str, sandbox: dict, *, timeout_s: float) -> None:
    """Poll the provider until the sandbox is no longer findable / running.
    Raises AssertionError on timeout — meaning DELETE leaked compute.
    """
    ref = sandbox.get("sandbox_ref") or sandbox.get("provider_ref", "")
    loop = asyncio.get_event_loop()
    deadline = loop.time() + timeout_s
    last_state: str | None = None

    while loop.time() < deadline:
        if provider == "daytona":
            from daytona_sdk import Daytona, DaytonaConfig
            daytona = Daytona(DaytonaConfig(api_key=DAYTONA_API_KEY))
            try:
                sb = await loop.run_in_executor(None, lambda: daytona.get(ref))
            except Exception:
                return  # gone from daytona's index — the post-condition
            state = getattr(sb, "state", "?")
            last_state = state.value if hasattr(state, "value") else str(state)
            if last_state in ("destroyed", "archived"):
                return

        elif provider == "docker":
            result = await loop.run_in_executor(None, lambda: subprocess.run(
                ["docker", "inspect", ref], capture_output=True, timeout=10,
            ))
            if result.returncode != 0:
                return  # docker no longer knows about it
            try:
                meta = json.loads(result.stdout)
                last_state = meta[0]["State"]["Status"] if meta else "?"
            except (json.JSONDecodeError, KeyError, IndexError):
                last_state = "?"

        elif provider == "unix_local":
            # Local "delete" = supervisor process gone AND the sandbox
            # marker file gone. Both are observable without server help:
            # the supervisor PID was on the sandbox row; the marker path
            # too. If either survives, DELETE leaked.
            pid = _local_supervisor_pid(sandbox)
            marker = sandbox.get("marker_path")
            marker_alive = bool(marker and os.path.exists(marker))
            if pid is None and not marker_alive:
                return
            last_state = f"pid={pid} marker_alive={marker_alive}"

        await asyncio.sleep(0.5)

    raise AssertionError(
        f"DELETE /sessions/{{id}} did not destroy {provider} sandbox "
        f"{ref!r}; last observed state: {last_state}. "
        f"Sandbox is leaked: session row is gone (DELETE /sessions cascades "
        f"to delete_session), so nothing can resume it; provider compute "
        f"sits paused against quota until a manual cleanup script runs."
    )


@pytest.mark.parametrize("provider", ["daytona", "docker", "unix_local", "modal"])
@agent_type_param
@pytest.mark.asyncio
async def test_external_delete_preserves_agent_memory(provider, agent_type):
    """Out-of-band delete (daytona dashboard / docker rm) bypasses the
    server, so no server-driven cold snapshot runs. The invariant we
    require is agent_memory preservation — conversation continues on
    the replacement sandbox because the supervisor tarred per-turn
    session state (or HOME lives on the volume for local/docker).

    We deliberately do NOT assert anything about arbitrary workspace
    files: daytona's SIGTERM handler happens to flush a full snapshot
    on graceful external delete, but that's implementation detail —
    the agent_memory invariant is the contract.
    """
    _require_provider(provider)

    async with ApiClient(SERVER) as sdk:
        sess = await _quick_session(sdk, provider, agent_type=agent_type)
        session_id = sess["session_id"]
        inner_before = sess["inner_session_id"]
        print(f"\n[test:{provider}] session={session_id[:8]} inner={inner_before}")

        reply1 = await _ask(sdk, session_id, "Reply with a single short word.")
        assert reply1.strip(), f"turn 1 empty: {reply1!r}"

        sandbox = await _get_sandbox(sdk, session_id)
        await _external_delete(sandbox)
        await asyncio.sleep(3)

        # agent_memory preserved → session/load succeeds → inner_sid
        # unchanged across the delete.
        inner_after = await _admin_inner_sid(sdk, session_id)
        assert inner_after == inner_before, (
            f"agent_memory not preserved across external delete — session/load "
            f"didn't restore: {inner_before!r} → {inner_after!r}"
        )


# ---------------------------------------------------------------------------
# Tests: resume (session persists across re-connection)
# ---------------------------------------------------------------------------

# Neutral ticket-ID framing avoids Claude's "secret code = social engineering"
# guardrail. The agent will freely echo/recall TKT-<digits> tokens.



@pytest.mark.parametrize("provider", ["daytona", "docker", "unix_local", "modal"])
@agent_type_param
@pytest.mark.asyncio
async def test_session_resume_after_stop(provider, agent_type):
    """Full session resume: stop sandbox between turns, reconnect, session is
    LOADED (not recreated).

    Deterministic invariants (no LLM-prose dependency):
      A. Turn 2 returns a non-empty reply.
      B. ``inner_session_id`` on the in-memory SessionState is unchanged
         across stop+resume — proves the server did ``session/load``, not
         ``session/new``.
    Parameterised over ``agent_type`` because the resume path differs per
    runtime (claude-agent-acp reads ``~/.claude/projects/…``; opencode
    reads ``~/.local/share/opencode/opencode.db``).
    """
    _require_provider(provider)

    async with ApiClient(SERVER) as sdk:
        sess = await _quick_session(sdk, provider, agent_type=agent_type)
        session_id = sess["session_id"]
        inner_before = sess["inner_session_id"]
        print(f"\n[test:{provider}] session={session_id[:8]} inner_sid={inner_before}")

        reply1 = await _ask(sdk, session_id, "Reply with a single short word.")
        assert reply1.strip(), f"turn 1 empty: {reply1!r}"

        # Stop sandbox externally
        sandbox = await _get_sandbox(sdk, session_id)
        await _external_stop(sandbox)
        await asyncio.sleep(3)

        # Turn 2: open a FRESH httpx connection (simulates UI reconnect)
        async with ApiClient(SERVER) as sdk2:
            reply2 = await _ask(sdk2, session_id, "Reply with a single short word.")
            inner_after = await _admin_inner_sid(sdk2, session_id)

        assert reply2.strip(), f"turn 2 empty after stop+resume: {reply2!r}"
        assert inner_after == inner_before, (
            f"session/load did not run — conversation restarted from scratch: "
            f"{inner_before!r} → {inner_after!r}"
        )


@pytest.mark.parametrize("provider", ["daytona", "docker", "unix_local", "modal"])
@agent_type_param
@pytest.mark.asyncio
async def test_session_resume_after_delete(provider, agent_type):
    """Delete sandbox between turns → NEW sandbox provisioned → session/load
    restores conversation via the persistent volume.

    Deterministic invariants:
      A. Turn 2 returns a non-empty reply.
      B. ``inner_session_id`` unchanged — session/load succeeded against
         the volume-persisted JSONL on the replacement sandbox.
    Parameterised over ``agent_type`` to verify the snapshot/restore
    machinery covers both runtimes' on-disk session formats.
    """
    _require_provider(provider)

    async with ApiClient(SERVER) as sdk:
        sess = await _quick_session(sdk, provider, agent_type=agent_type)
        session_id = sess["session_id"]
        inner_before = sess["inner_session_id"]
        print(f"\n[test:{provider}] session={session_id[:8]} inner_sid={inner_before}")

        reply1 = await _ask(sdk, session_id, "Reply with a single short word.")
        assert reply1.strip(), f"turn 1 empty: {reply1!r}"

        sandbox = await _get_sandbox(sdk, session_id)
        await _external_delete(sandbox)
        await asyncio.sleep(3)

        async with ApiClient(SERVER) as sdk2:
            reply2 = await _ask(sdk2, session_id, "Reply with a single short word.")
            inner_after = await _admin_inner_sid(sdk2, session_id)

        assert reply2.strip(), f"turn 2 empty after delete+resume: {reply2!r}"
        assert inner_after == inner_before, (
            f"session context lost after sandbox delete — session/load did "
            f"not restore the volume-backed JSONL: "
            f"{inner_before!r} → {inner_after!r}"
        )


# ---------------------------------------------------------------------------
# Test: midstream sandbox stop (UI-flow reproduction)
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("provider", ["daytona", "docker", "unix_local", "modal"])
@agent_type_param
@pytest.mark.asyncio
async def test_session_survives_midstream_sandbox_stop(provider, agent_type):
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

    async with ApiClient(SERVER) as sdk:
        sess = await _quick_session(sdk, provider, agent_type=agent_type)
        session_id = sess["session_id"]
        inner_sid_before = sess["inner_session_id"]
        print(f"\n[test:{provider}] session={session_id[:8]} inner_sid={inner_sid_before}")

        # Turn 1: agent computes a value that was NOT in the prompt.
        # 317 * 419 = 132823. The user's message contains 317 and 419 but
        # not the product, so a later recall of WHATEVER digits the agent
        # named here proves the assistant turn was persisted and restored,
        # not just echoed from the user-turn line of the JSONL.
        #
        # Don't assert the math is correct — claude-haiku occasionally
        # hallucinates a digit under -n auto load and the recovery
        # invariant we care about (does session/load restore the prior
        # assistant turn?) is orthogonal to whether 317*419 came out to
        # 132823 or 133023. We pin the AGENT'S OWN ANSWER and assert
        # the recall matches it, which is what the test always intended.
        reply1 = await _ask(
            sdk, session_id,
            "Please compute 317 * 419 (use `echo $((317*419))` in a shell if "
            "it helps). Reply on a single line as `PRODUCT=<value>` so I can "
            "parse it.",
        )
        print(f"[test:{provider}] turn1 reply: {reply1[:200]!r}")
        product = _extract_kv(reply1, "PRODUCT")
        assert product and product.isdigit(), (
            f"agent didn't reply in the PRODUCT=<digits> shape, cannot proceed: {reply1!r}"
        )

        # External stop — the exact UI scenario the user reproduced manually.
        sandbox = await _get_sandbox(sdk, session_id)
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
        in_mem = await _admin_session_row(sdk, session_id)
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
            sdk, session_id,
            "What was the product you computed earlier in this conversation? "
            "Reply with just the number.",
        )
        print(f"[test:{provider}] turn2 reply (after recovery): {reply2[:200]!r}")
        # Strip thousands separators and whitespace so "132,823" / "132 823" /
        # "132823" all count as a match. What we care about is that the
        # digits the AGENT named in turn 1 (saved as ``product``) appear
        # somewhere in the reply; the agent choosing to format with commas
        # is LLM flavor, not a recovery-path failure.
        normalized = _re.sub(r"[,\s_]", "", reply2)
        assert product in normalized, (
            f"agent lost conversation context across SSE recovery — cannot "
            f"recall its own prior reply (expected {product!r}, got {reply2!r})"
        )


@pytest.mark.parametrize("provider", ["daytona", "docker", "unix_local", "modal"])
@agent_type_param
@pytest.mark.asyncio
async def test_message_immediately_after_stop(provider, agent_type):
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

    async with ApiClient(SERVER) as sdk:
        sess = await _quick_session(sdk, provider, agent_type=agent_type)
        session_id = sess["session_id"]
        inner_sid_before = sess["inner_session_id"]
        print(f"\n[test:{provider}] session={session_id[:8]} inner_sid={inner_sid_before}")

        # Turn 1 — confirm the agent is live.
        reply1 = await _ask(sdk, session_id, "Reply with a single short word.")
        assert reply1.strip(), f"turn 1 empty reply: {reply1!r}"
        print(f"[test:{provider}] turn1 ok")

        # External stop — NO sleep. This is the race we're after.
        sandbox = await _get_sandbox(sdk, session_id)
        await _external_stop(sandbox)
        print(f"[test:{provider}] sandbox stopped; immediately sending turn 2")

        # Turn 2 — the server must recover and deliver a reply.
        reply2 = await _ask(sdk, session_id, "Reply with a single short word.")
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
        in_mem = await _admin_session_row(sdk, session_id)
        assert in_mem is not None, f"session missing from /admin/sessions"
        assert in_mem.get("inner_session_id") == inner_sid_before, (
            f"inner_session_id changed across recovery — server silently "
            f"started a new conversation: {inner_sid_before!r} → "
            f"{in_mem.get('inner_session_id')!r}"
        )


@pytest.mark.parametrize("provider", ["daytona", "docker", "unix_local", "modal"])
@agent_type_param
@pytest.mark.asyncio
async def test_message_after_stop_with_delay(provider, agent_type):
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

    async with ApiClient(SERVER) as sdk:
        sess = await _quick_session(sdk, provider, agent_type=agent_type)
        session_id = sess["session_id"]
        inner_sid_before = sess["inner_session_id"]
        print(f"\n[test:{provider}] session={session_id[:8]} inner_sid={inner_sid_before}")

        reply1 = await _ask(sdk, session_id, "Reply with a single short word.")
        assert reply1.strip(), f"turn 1 empty: {reply1!r}"
        print(f"[test:{provider}] turn1 ok")

        sandbox = await _get_sandbox(sdk, session_id)
        await _external_stop(sandbox)
        # Give the server's SSE reader time to observe the upstream
        # disconnect and tear down. This is the state the UI is in when
        # the user clicks send.
        await asyncio.sleep(4)
        print(f"[test:{provider}] stopped + waited 4s; sending turn 2")

        reply2 = await _ask(sdk, session_id, "Reply with a single short word.")
        print(f"[test:{provider}] turn2 reply len: {len(reply2)}")
        assert reply2.strip(), (
            f"turn 2 lost after 4s stop-delay: server did not recover "
            f"the dead sandbox before dispatching the prompt: {reply2!r}"
        )

        in_mem = await _admin_session_row(sdk, session_id)
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

    def __init__(self, sdk: ApiClient, session_id: str) -> None:
        self._sdk = sdk
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
                buf = b""
                # Mark "stream open" on the first chunk we receive — that's
                # equivalent to seeing the response status_code on the
                # underlying httpx stream.
                first = True
                async for chunk in self._sdk.stream_events(self._session_id):
                    if not self._alive:
                        return
                    if first:
                        attempt = 0
                        self._reconnected.set()
                        first = False
                    buf += chunk
                    while b"\n\n" in buf:
                        raw, buf = buf.split(b"\n\n", 1)
                        block = raw.decode("utf-8", errors="replace")
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
    sdk: ApiClient, session_id: str, stream: _PersistentSse,
    message: str,
) -> str:
    """POST /message then drain the rpc's events off the persistent stream.

    Unlike `_ask`, this does NOT open a new /events connection — it uses
    the already-open one, matching the UI flow.
    """
    rpc_id = await _send_message(sdk, session_id, message)
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


@pytest.mark.parametrize("provider", ["daytona", "docker", "unix_local", "modal"])
@agent_type_param
@pytest.mark.asyncio
async def test_persistent_sse_stop_then_message(provider, agent_type):
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

    async with ApiClient(SERVER) as sdk:
        sess = await _quick_session(sdk, provider, agent_type=agent_type)
        session_id = sess["session_id"]
        print(f"\n[test:{provider}] session={session_id[:8]}")

        async with _PersistentSse(sdk, session_id) as sse:
            ticket = "TKT-55501"
            reply1 = await _ask_on_stream(
                sdk, session_id, sse,
                f"I'm tracking work under ticket ID {ticket}. Please acknowledge by "
                f"echoing the ticket ID back to me so I know you have it.",
            )
            assert ticket in reply1, f"turn 1 didn't echo ticket: {reply1!r}"
            print(f"[test:{provider}] turn1 ok (persistent SSE held open)")

            sandbox = await _get_sandbox(sdk, session_id)
            await _external_stop(sandbox)
            await asyncio.sleep(4)
            print(f"[test:{provider}] stopped + waited 4s; sending turn 2 "
                  f"on the SAME persistent /events stream")

            reply2 = await _ask_on_stream(
                sdk, session_id, sse,
                "What was the ticket ID I mentioned earlier in this conversation? "
                "Reply with only the ticket ID.",
            )
            print(f"[test:{provider}] turn2 reply: {reply2[:200]!r}")
            assert ticket in reply2, (
                f"turn 2 lost on persistent SSE after stop+delay: the "
                f"server's state rebuild dropped the UI's subscribers and "
                f"events for the new prompt went nowhere: {reply2!r}"
            )


@pytest.mark.parametrize("provider", ["daytona", "docker", "unix_local", "modal"])
@agent_type_param
@pytest.mark.asyncio
@pytest.mark.timeout(240)
async def test_persistent_sse_external_delete_then_message(provider, agent_type):
    """UI repro for an OUT-OF-BAND sandbox delete (Daytona dashboard, ``docker rm``,
    ``kill -9``) with a persistent /events stream held open.

    Individual timeout bumped to 240 s. The daytona path does a full
    provision-replacement-sandbox + start-supervisor dance on turn 2
    (see ``_type2_recover`` + ``ensure_supervisor_url``), and
    with daytona-side latency variance the critical path (turn 1 LLM +
    external-delete poll + SSE retry ladder + fresh provisioning +
    session/load + turn 2 LLM) can hit ~100 s on a slow day. 120 s was
    tight; 240 s matches the suite-wide ``--timeout``.

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

    async with ApiClient(SERVER) as sdk:
        sess = await _quick_session(sdk, provider, agent_type=agent_type)
        session_id = sess["session_id"]
        print(f"\n[test:{provider}] session={session_id[:8]}")

        async with _PersistentSse(sdk, session_id) as sse:
            reply1 = await _ask_on_stream(
                sdk, session_id, sse, "Reply with a single short word.",
            )
            assert reply1.strip(), f"turn 1 empty: {reply1!r}"

            # Out-of-band delete — server finds out via SSE disconnect.
            sandbox = await _get_sandbox(sdk, session_id)
            await _external_delete(sandbox)
            await asyncio.sleep(4)

            reply2 = await _ask_on_stream(
                sdk, session_id, sse, "Reply with a single short word.",
            )
            assert reply2.strip(), (
                f"turn 2 lost on persistent SSE after external delete+delay: "
                f"the SSE reader's recovery path dropped the UI's subscribers. "
                f"Reply was: {reply2!r}"
            )


@pytest.mark.parametrize("provider", ["daytona", "docker", "unix_local", "modal"])
@agent_type_param
@pytest.mark.asyncio
async def test_persistent_sse_delete_sandbox_then_message(provider, agent_type):
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

    async with ApiClient(SERVER) as sdk:
        sess = await _quick_session(sdk, provider, agent_type=agent_type)
        session_id = sess["session_id"]
        inner_before = sess["inner_session_id"]
        print(f"\n[test:{provider}] session={session_id[:8]} inner_sid={inner_before}")

        async with _PersistentSse(sdk, session_id) as sse:
            reply1 = await _ask_on_stream(
                sdk, session_id, sse, "Reply with a single short word.",
            )
            assert reply1.strip(), f"turn 1 empty: {reply1!r}"

            # POST /sessions/{id}/release replaces the legacy DELETE
            # /sandboxes/{id}: snapshot + drop the pool lease so the
            # next prompt cold-recovers (the "delete sandbox" semantics
            # the UI exercised).
            await sdk.release_session(session_id)

            # Wait — the UI's SSE stream may observe stream-end here; the
            # _PersistentSse helper reconnects automatically.
            await asyncio.sleep(4)

            reply2 = await _ask_on_stream(
                sdk, session_id, sse, "Reply with a single short word.",
            )
            assert reply2.strip(), (
                f"turn 2 lost on persistent SSE after delete+delay: the "
                f"server's zombie-state path dropped events for the new "
                f"sandbox. Reply was: {reply2!r}"
            )


@pytest.mark.parametrize("provider", ["daytona", "unix_local", "modal"])
@agent_type_param
@pytest.mark.asyncio
async def test_persistent_sse_supervisor_killed_then_message(provider, agent_type):
    """Prod UI repro: supervisor process dies, sandbox stays alive.

    Exact trace the user reported against prod daytona:
      - multiple turns work
      - one POST /v1/acp returns 502 Bad Gateway (proxy forwards to port
        9100, nothing listens — supervisor crashed/OOM'd/restarted)
      - stream errors, UI reconnects
      - subsequent /message calls stick at 'Queued for agent' with no
        reply ever arriving

    Differs from the external-delete tests: the sandbox itself is NOT
    removed. The server's cached ``_INSTANCES[sandbox_id]`` still points
    at the old URL, and the DB row is untouched. Recovery must detect
    the dead supervisor, rebind to a freshly-started one (daytona
    supervisor lazy-start), and deliver turn 2's events to the
    subscribers held on the persistent /events stream.

    Docker intentionally excluded — ``docker exec ... pkill supervisor.js``
    kills the container's PID 1 and the container exits, which is
    covered by test_persistent_sse_external_delete_then_message.
    """
    _require_provider(provider)

    async with ApiClient(SERVER) as sdk:
        sess = await _quick_session(sdk, provider, agent_type=agent_type)
        session_id = sess["session_id"]
        print(f"\n[test:{provider}] session={session_id[:8]}")

        async with _PersistentSse(sdk, session_id) as sse:
            reply1 = await _ask_on_stream(
                sdk, session_id, sse, "Reply with a single short word.",
            )
            assert reply1.strip(), f"turn 1 empty: {reply1!r}"

            sandbox = await _get_sandbox(sdk, session_id)
            await _kill_supervisor_in_sandbox(sandbox)
            # Give the server's SSE reader time to observe the upstream
            # disconnect and flip into recovery. No sandbox-delete event
            # will ever arrive from the provider; the server only knows
            # the supervisor died via this disconnect.
            await asyncio.sleep(6)

            reply2 = await _ask_on_stream(
                sdk, session_id, sse, "Reply with a single short word.",
            )
            assert reply2.strip(), (
                f"turn 2 lost on persistent SSE after supervisor kill: the "
                f"server didn't recover from 'supervisor dead, sandbox alive' "
                f"and events for the new prompt never reached subscribers. "
                f"Reply was: {reply2!r}"
            )


@pytest.mark.parametrize("provider", ["daytona", "docker", "unix_local", "modal"])
@agent_type_param
@pytest.mark.asyncio
async def test_persistent_sse_supervisor_killed_immediate_message(provider, agent_type):
    """Prod UI race: kill supervisor, then POST /message BEFORE the server
    has observed the upstream disconnect.

    Different from test_persistent_sse_supervisor_killed_then_message —
    that test sleeps 6s so the SSE reader has time to flip _reader_connected
    to False and enter rebind. Here we fire the message in the ~100ms
    window where the server still thinks the cached supervisor URL is alive.

    On daytona, the dead supervisor → proxy returns ``502 Bad Gateway``
    from ``httpx.raise_for_status`` on the POST to ``/v1/acp/...``. The
    previous retry path only caught ConnectError/RemoteProtocolError/ReadError,
    so the 502 fell through to the generic ``except`` which just logged
    and dispatched an error event — no rebind, no retry. UI sees
    'Queued for agent' forever if it's not listening for the error
    event on the rpc it just submitted (typical EventSource reconnect
    loses rpc_id subscription).

    Docker variant: ``pkill supervisor.js`` takes down PID 1 and the
    container exits, but the server's cached URL still points at the
    dead container — same stale-cache race the test pins.

    Invariant: turn 2 returns a non-empty reply. Either the retry path
    rebinds and succeeds, or the error event reaches the persistent SSE.
    """
    _require_provider(provider)

    async with ApiClient(SERVER) as sdk:
        sess = await _quick_session(sdk, provider, agent_type=agent_type)
        session_id = sess["session_id"]
        print(f"\n[test:{provider}] session={session_id[:8]}")

        async with _PersistentSse(sdk, session_id) as sse:
            reply1 = await _ask_on_stream(
                sdk, session_id, sse, "Reply with a single short word.",
            )
            assert reply1.strip(), f"turn 1 empty: {reply1!r}"

            sandbox = await _get_sandbox(sdk, session_id)
            await _kill_supervisor_in_sandbox(sandbox)
            # NO sleep — fire the message while the server still thinks
            # the cached supervisor URL is alive. This is the exact race
            # the UI's "Stream error. Reconnecting... <message>" trace
            # reproduces: the user hit Send before the server's SSE
            # reader observed the upstream disconnect.

            reply2 = await _ask_on_stream(
                sdk, session_id, sse, "Reply with a single short word.",
            )
            assert reply2.strip(), (
                f"turn 2 lost on persistent SSE after supervisor kill (no delay): "
                f"the 502/connection error on POST /v1/acp was not retried. "
                f"Reply was: {reply2!r}"
            )



@pytest.mark.parametrize("provider", ["daytona", "docker", "unix_local", "modal"])
@agent_type_param
@pytest.mark.asyncio
async def test_ui_reconnect_gap_persists_replies_to_session_log(provider, agent_type):
    """End-to-end wiring: prompts submitted while no /events subscriber is
    attached must still land in ``session_log`` so a reconnecting UI can
    cold-load them via ``GET /sessions/{id}/log``.

    History recovery is now the /log endpoint's job, not a per-session
    replay buffer on /events. (Earlier the server kept a bounded replay
    buffer that seeded each new subscriber; that double-delivered every
    event a cold-loading UI just fetched from /log, so the buffer was
    removed.) /events is live-only; durable history lives in
    ``session_log`` which is persisted in-line by ``_persist_prompt_events``
    regardless of subscriber state.

    Flow in-order:
      1. Open persistent /events (UI's EventSource on page load).
      2. Turn 1 via POST /message over the persistent stream — succeeds.
      3. Kill supervisor inside the sandbox (the prod 502 / OOM trigger).
      4. Exit the persistent-SSE context — no subscribers attached.
      5. POST msg2 and msg3 during the gap.
      6. Wait for the server scheduler to drive both turns; with no
         subscriber the events are still persisted to ``session_log``.
      7. GET /sessions/{id}/log and assert both rpc ids landed
         ``user_message`` + ``turn_end`` rows. Replays the cold-load
         path the UI takes on EventSource reconnect.
    """
    _require_provider(provider)

    async with ApiClient(SERVER) as sdk:
        sess = await _quick_session(sdk, provider, agent_type=agent_type)
        session_id = sess["session_id"]
        print(f"\n[test:{provider}] session={session_id[:8]}")

        async with _PersistentSse(sdk, session_id) as sse:
            reply1 = await _ask_on_stream(
                sdk, session_id, sse, "Reply with a single short word.",
            )
            assert reply1.strip(), f"turn 1 empty: {reply1!r}"

            sandbox = await _get_sandbox(sdk, session_id)
            await _kill_supervisor_in_sandbox(sandbox)
            # Let the server's upstream SSE reader observe the death and
            # kick the UI subscriber.
            await asyncio.sleep(3)

        # No subscribers attached. The follow-up POSTs land in this gap.
        rpc2 = await _send_message(
            sdk, session_id, "Reply with a single short word.",
        )
        rpc3 = await _send_message(
            sdk, session_id, "Reply with a single short word.",
        )

        # Wait for the scheduler to drive both turns to completion. The
        # /log endpoint reads ``session_log`` rows that
        # ``_persist_prompt_events`` writes inline as ACP events arrive,
        # independent of any subscriber.
        deadline = time.time() + 90
        seen_done: set[str] = set()
        while seen_done < {rpc2, rpc3} and time.time() < deadline:
            await asyncio.sleep(2)
            async with httpx.AsyncClient(timeout=10) as http:
                resp = await http.get(
                    f"{SERVER}/sessions/{session_id}/log",
                    params={"limit": 500},
                )
            resp.raise_for_status()
            for entry in resp.json():
                pid = (entry.get("payload") or {}).get("prompt_id")
                if pid in (rpc2, rpc3) and entry["event_type"] == "turn_end":
                    seen_done.add(pid)

        missing = [rpc for rpc in (rpc2, rpc3) if rpc not in seen_done]
        assert not missing, (
            f"{len(missing)}/2 follow-up messages never landed a turn_end "
            f"row in session_log. Events submitted during a no-subscriber "
            f"gap must still persist so /log can serve them on UI "
            f"reconnect. Missing rpc_ids = {missing}; "
            f"seen_done = {sorted(seen_done)}"
        )


# ``test_session_survives_supervisor_dir_wiped_from_volume`` was deleted in
# the runtime-image-unification refactor. The test exercised the
# ``volumes.supervisor_agent_types`` cache-vs-disk-drift recovery path; that
# path no longer exists because the supervisor + ACP bins now ship in the
# image (``/opt/agent-sdk/runtime/``) instead of being installed onto each
# volume's ``system/supervisor/`` dir. Wiping a non-existent volume
# directory on a path the runtime never reads is a no-op.


# ---------------------------------------------------------------------------
# Silent-failure regression: data-research / Task Builder bug 2026-05-04
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("provider", ["daytona", "docker", "unix_local"])
@agent_type_param
@pytest.mark.asyncio
async def test_no_silent_failure_concurrent_stop_and_message(provider, agent_type):
    """Repro of the data-research / Task Builder silent-failure bug
    (2026-05-04).

    Production scenario:
      * The hivespace UI keeps a long-lived ``GET /events`` SSE
        connection open from chat-panel mount onwards.
      * The user sits idle long enough for Daytona's idle reaper to
        initiate auto-pause asynchronously.
      * The user sends a message during the pause transition: the
        supervisor URL still answers ``/v1/health`` 200 (proxy cache)
        but ``/v1/acp/<inner_sid>`` returns ``400`` because the ACP
        child was already torn down.
      * The pool either (a) trusts the cached probe and forwards
        directly to a dead supervisor, or (b) detects the stale state,
        re-provisions a new session, and shuts the old one down. In
        case (b), ``shutdown()`` calls ``_close_subscribers()`` which
        sends ``_END`` to every queue — including the UI's persistent
        /events subscriber — and the user's chat stream silently dies.
      * Result: no reply, no error toast, just silence. User refreshes
        the page (which re-establishes /events on the new session).

    To reproduce we fire ``daytona.stop()`` (or the per-provider
    equivalent) and ``POST /message`` concurrently while a persistent
    /events stream is already open. The race is the same as prod's
    auto-pause: pool sees a transitional state, re-provisions, the
    fresh-session broadcasts must reach the existing subscriber.

    Invariant: every accepted ``POST /message`` produces SOMETHING on
    the live wire (text+done or error) within
    ``silent_timeout_s`` for a UI that was already subscribed before
    the message went out. Pure silence is the only failure mode.

    Subscriber-handoff fix lives in ``api.sandbox.pool.SessionPool.
    get_session`` — when the pool detects a stale entry it transfers
    the prior session's ``_subscribers`` dict onto the replacement
    session before kicking off ``shutdown()`` on the old one, so
    ``_close_subscribers()`` doesn't tear down /events streams that
    belong to the live session.

    Modal is omitted: ``modal.terminate`` is destructive (no pause
    state), so the in-place "stop while signed URL is still serving"
    race the test pins doesn't apply — modal recovery goes through a
    different SandboxMissingError path covered by
    ``test_session_resume_after_delete[modal]``.
    """
    _require_provider(provider)

    silent_timeout_s = 90.0

    async with ApiClient(SERVER) as sdk:
        sess = await _quick_session(sdk, provider, agent_type=agent_type)
        session_id = sess["session_id"]
        print(f"\n[test:{provider}] session={session_id[:8]}")

        reply1 = await _ask(sdk, session_id, "Reply with a single short word.")
        assert reply1.strip(), f"turn 1 empty reply: {reply1!r}"

        sandbox = await _get_sandbox(sdk, session_id)

        # Subscribe FIRST — matches the production UI flow (chat panel
        # opens /events on mount, user sends messages later). Without
        # subscribe-first ordering, broadcasts that fire before the
        # subscriber attaches are lost as a matter of architectural
        # contract (server.py:1773 — /events is live-only). The bug
        # under test is "the persistent subscriber dies during recovery
        # so it MISSES live broadcasts", not "the test forgot to
        # subscribe in time".
        async with _PersistentSse(sdk, session_id) as sse:
            # Race: kick off the stop, then immediately submit turn 2.
            stop_task = asyncio.create_task(_external_stop(sandbox))
            try:
                rpc2 = await _send_message(
                    sdk, session_id, "Reply with a single short word.",
                )
            finally:
                await stop_task
            print(f"[test:{provider}] stop+turn2 raced; rpc2={rpc2}")

            # _PersistentSse stores per-rpc events. We only need to know
            # if SOMETHING terminal arrives for rpc2 within the budget.
            q = sse.get_queue(rpc2)
            terminal_kind: str | None = None
            seen_text = False
            deadline = time.time() + silent_timeout_s

            while time.time() < deadline and terminal_kind is None:
                try:
                    evt = await asyncio.wait_for(
                        q.get(), timeout=max(1.0, deadline - time.time()),
                    )
                except asyncio.TimeoutError:
                    break
                if evt["type"] == "text":
                    seen_text = True
                elif evt["type"] == "done":
                    terminal_kind = "text+done" if seen_text else "done"
                elif evt["type"] == "error":
                    terminal_kind = "error"

        if terminal_kind is None:
            async with httpx.AsyncClient() as c:
                resp = await c.get(
                    f"{SERVER}/sessions/{session_id}/log?limit=200",
                    timeout=10,
                )
            log_entries = resp.json() if resp.status_code == 200 else []
            log_for_rpc2 = [
                e for e in log_entries
                if (e.get("payload") or {}).get("prompt_id") == rpc2
            ]
            assert log_for_rpc2, (
                f"silent failure: turn 2 (rpc={rpc2}) produced NO event "
                f"on /events within {silent_timeout_s}s AND no row in "
                f"session_log. The server accepted POST /message and "
                f"dropped the prompt on the floor — this is the data-"
                f"research / Task Builder repro."
            )
            kinds = sorted({e["event_type"] for e in log_for_rpc2})
            raise AssertionError(
                f"broadcast leak: turn 2 (rpc={rpc2}) was processed by "
                f"the server (session_log shows {kinds}) but NOTHING "
                f"reached the persistent /events stream within "
                f"{silent_timeout_s}s. Pool re-provisioned the session "
                f"during the stop+message race and the prior session's "
                f"shutdown() closed the UI's subscriber via "
                f"_close_subscribers() — the recovery's broadcasts "
                f"landed on the new session whose subscriber dict was "
                f"empty. Fix: hand off subscribers from stale → fresh "
                f"session in pool.get_session."
            )

        print(f"[test:{provider}] turn 2 terminal_kind={terminal_kind}")
        assert terminal_kind in ("text+done", "done", "error"), terminal_kind


# ===========================================================================
# Zombie-subscriber leak after mid-prompt recovery (merged from the former
# test_subscriber_leak_recovery.py — same golden harness, HTTP-only).
# ===========================================================================
#
# The bug: when a sandbox dies *mid-prompt*, ``_persist_prompt_events``
# recovers by calling ``pool.get_session``, which hands the in-flight SSE
# subscriber queue off from the dead session object (A) to a freshly
# provisioned replacement (B). But the consumer's cleanup
# (``iterate_subscriber``'s ``finally: self._subscribers.pop(sid)``) is
# closure-bound to A, so it pops A's already-cleared dict and leaves a
# permanent ZOMBIE entry in B's ``_subscribers``. Symptom: the session is
# pinned in the pool's in-memory ``_active`` forever (``reap_idle`` skips
# any session whose ``_subscribers`` is non-empty), so the dashboard shows
# it leased/busy forever and the backing sandbox leaks until its own time
# limit.
#
# Observable contract (HTTP only, no white-box pool access):
#   * ``POST /sessions/{id}/message`` runs the SAME
#     ``_execute_and_stream_sse_for`` generator in a background drain, so it
#     registers a subscriber on session A even with no client on /events.
#   * ``GET /sessions/{id}/status`` carries the session id in the path, so
#     the LB consistent-hashes it to the OWNING replica, and returns
#     ``session_subscriber_count`` in ``peek`` mode (no cold-recovery side
#     effect). With no client connected, that count MUST settle back to 0
#     once the prompt's rpc terminates. A stuck non-zero count is the zombie.
#
# Pre-fix this FAILS (count stuck >= 1); post-fix it PASSES. Verified by
# toggling ``iterate_subscriber``'s finally between ``self`` and ``owner``.

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


@pytest.mark.parametrize("provider", ["daytona", "docker", "unix_local", "modal"])
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


# ===========================================================================
# Reaper / subscriber de-conflation (Bug B — merged from the former
# test_golden_reaper_subscriber.py once green across providers).
# ===========================================================================
#
# Distinct from the zombie leak above: there the subscriber count is stuck
# >0 with NO client (a leak); here a real client IS connected and the
# question is whether idle compute still reaps despite it. Before the fix
# (``pool._should_reap``/``reap_idle`` treating any non-empty ``_subscribers``
# as compute activity) an open /events consumer — a dashboard, a monitor, an
# idle chat UI — pinned the sandbox indefinitely past the idle window.
#
# Observable contract (HTTP only): finish a turn so compute is idle, open a
# persistent /events consumer, confirm ``session_subscriber_count == 1``,
# then ``POST /sessions/{id}/reap?idle_s=0`` (the SAME decision the
# background reaper uses, run per-session so it's deterministic + -n-auto
# safe — the global reaper's timing can't be exercised per session). With no
# prompt in flight it MUST hibernate. The fix keys the decision off
# compute-only activity (``_last_compute_at``) + an in-flight gate, not
# ``_subscribers`` membership.


@pytest.mark.parametrize("provider", ["daytona", "docker", "unix_local", "modal"])
@agent_type_param
@pytest.mark.asyncio
@pytest.mark.timeout(900)
async def test_idle_session_with_open_subscriber_is_reaped(provider, agent_type):
    """An idle session (no prompt in flight) with an open /events consumer
    MUST hibernate when the reaper decision runs. A refusal means
    subscriber-presence is being counted as compute activity — the pin."""
    _require_provider(provider)

    async with ApiClient(SERVER) as sdk:
        sess = await _quick_session(sdk, provider, agent_type=agent_type)
        sid = sess["session_id"]

        # Finish a turn so the sandbox is up, the compute clock is set, and
        # no prompt is in flight. With no client attached the count is 0.
        await _post_and_wait(sdk, sid, "Reply with the single word: ready.")
        assert await _wait_count(sdk, sid, 0, 15.0), (
            "baseline broken: an idle session with no client should have 0 "
            f"subscribers, got {await _subscriber_count(sdk, sid)}"
        )

        # Open a PERSISTENT /events consumer and keep it draining in the
        # background — this is the dashboard/monitor that used to pin compute.
        ended = asyncio.Event()

        async def _drain() -> None:
            try:
                async for _chunk in sdk.stream_events(sid):
                    pass
            except Exception:
                pass
            finally:
                ended.set()

        drain = asyncio.create_task(_drain())
        try:
            assert await _wait_count(sdk, sid, 1, 15.0), (
                "open GET /events did not register a subscriber on the session"
            )

            # Run the reaper's decision for THIS session with an explicit
            # idle_s=0 (authoritative — overrides per-provider windows incl.
            # modal's 30 min). The only thing keeping it 'busy' is the open
            # subscriber, so it MUST still hibernate.
            r = await sdk._http.post(
                f"/sessions/{sid}/reap", params={"idle_s": 0}, timeout=30,
            )
            r.raise_for_status()
            body = r.json()
            assert body.get("hibernated") is True, (
                "BUG B — COMPUTE PINNED BY SUBSCRIBER "
                f"(provider={provider} agent_type={agent_type}): an idle "
                "session with no prompt in flight but an open /events consumer "
                f"was NOT hibernated (reason={body.get('reason')!r}). The reap "
                "decision must key off compute-only activity (+ an in-flight "
                "gate), not _subscribers membership."
            )
        finally:
            drain.cancel()
            with contextlib.suppress(BaseException):
                await drain

        # Continuity: the session was hibernated, not destroyed. The next
        # turn cold-resumes the sandbox and answers — conversation preserved
        # via session/load.
        reply = await _ask(sdk, sid, "Reply with the single word: again.")
        assert "again" in reply.lower(), (
            f"session unusable after reap (cold-resume failed): {reply!r}"
        )
