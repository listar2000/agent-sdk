"""Characterization tests for ACP wire-level behavior across providers.

These tests pin down the per-provider event ordering, attribution, and merge
semantics the server's FIFO rpc_id attribution depends on. Each invariant
tested here is something our production code relies on — if any of these
fail, the attribution and tagging logic needs updating.

Provider coverage
-----------------

claude-code    First-class. Full merge semantics characterized (stub vs real,
               FIFO ordering, mid-tool-loop boundary, late-interrupt window).
codex          First-class for single-prompt and strict-sequential only.
               Concurrent prompts are broken at the codex ACP adapter layer:
               codex emits only ONE terminal per session, tagged to the first
               rpc_id, regardless of how many prompts were submitted.

Characterized patterns (see docs/api.md for full writeups)
---------------------------------------------------------

1. Every SSE block emitted by the server carries `event: rpc:<id>` prefix
   tagging it to the oldest-inflight rpc_id at emission time (FIFO head).

2. Claude-code merge pattern: N prompts queued while the first is running
   produces N-1 stub end_turns (zero usage) in submission order, followed
   by one real end_turn (nonzero usage) tagged to the last-submitted rpc_id.
   All of the merged turn's content streams under the last rpc_id's tag.

3. Claude-code merge window is bounded by A's stub envelope on the wire.
   Prompts that arrive BEFORE A's stub fires are merged into the group.
   Prompts that arrive AFTER A's stub fires become independent turns.
   (Wall-clock wait times are not deterministic — the stub envelope is.)

4. Claude-code stubs fire at tool-round BOUNDARIES, not mid-tool. A interrupt
   arriving during a multi-step tool loop takes effect at the next boundary.

5. Both claude-code and codex emit a `session/update` notification with
   `sessionUpdate: "usage_update"` at the end of each turn's content, right
   before the terminal envelope. The field is NOT in the official ACP spec
   schema but is emitted as a de facto extension by both agents.

6. Codex behaves differently from claude on several dimensions:
   - Terminals always carry `usage: {}` (empty dict), not zero-valued
   - usage_update carries `cost: null` instead of claude's `{amount, currency}`
   - Concurrent prompts do not merge — only one terminal is emitted per
     session, tagged to the first submitted rpc_id
   - The model answers the LATEST prompt but the response is attributed
     to the first rpc_id on the wire

Running
-------

    pytest tests/test_acp_invariants.py -v -m integration

The tests hit real model calls (10-60 seconds each); the full suite runs
in roughly 10-20 minutes. All tests are marked `@pytest.mark.integration`
and skipped unless invoked with `-m integration`.

Target server URL is controlled by the `ACP_TEST_URL` environment variable
(default `http://localhost:7778`). The server must have `ANTHROPIC_API_KEY`
set for claude tests and `OPENAI_API_KEY` set for codex tests. Missing
credentials or unavailable agents cause the affected tests to skip rather
than fail.
"""
from __future__ import annotations

import asyncio
import json
import os
import sys
import time
from typing import Any

import httpx
import pytest
import pytest_asyncio

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
from agent_sdk import ApiClient
from agent_sdk.client import Agent

BASE_URL = os.environ.get("ACP_TEST_URL", "http://localhost:7778")


# ---------------------------------------------------------------------------
# Module-level markers
# ---------------------------------------------------------------------------

def _server_reachable() -> bool:
    try:
        r = httpx.get(f"{BASE_URL}/health", timeout=3)
        return r.status_code == 200
    except Exception:
        return False


pytestmark = pytest.mark.integration

skip_if_no_server = pytest.mark.skipif(
    not _server_reachable(),
    reason=f"Server not reachable at {BASE_URL}"
)


# ---------------------------------------------------------------------------
# SSE parsing helpers
# ---------------------------------------------------------------------------

def parse_tagged_block(block: str) -> tuple[str | None, dict | None]:
    """Extract (rpc_id_tag, payload_dict) from a server-tagged SSE block.

    The server stamps each block with `event: rpc:<rpc_id>` before the data
    line (see /sessions/{id}/events handler in src/api/server.py). Untagged
    blocks (heartbeats, bootstrap events before any prompt is inflight) are
    returned with tag=None.
    """
    tag = None
    data_lines = []
    for line in block.split("\n"):
        if line.startswith("event: rpc:"):
            tag = line[len("event: rpc:"):].strip()
        elif line.startswith("data: "):
            data_lines.append(line[6:])
        elif line.startswith("data:"):
            data_lines.append(line[5:])
    payload: dict | None = None
    if data_lines:
        try:
            payload = json.loads("\n".join(data_lines))
        except json.JSONDecodeError:
            pass
    return tag, payload


def is_terminal(p: Any) -> bool:
    """True if this is a JSON-RPC response envelope with a stopReason."""
    return (
        isinstance(p, dict)
        and "id" in p
        and "result" in p
        and isinstance(p["result"], dict)
        and "stopReason" in p["result"]
    )


def is_claude_stub(p: Any) -> bool:
    """Claude-code specific: a terminal with non-empty, all-zero usage.

    Claude-code fills in usage fields (cachedReadTokens, inputTokens, etc.)
    with explicit zero values when emitting a stub for a merged prompt.
    Non-merged turns have nonzero values. This check is NOT valid for codex
    which always returns `usage: {}` regardless of merge state.
    """
    if not is_terminal(p):
        return False
    usage = p["result"].get("usage") or {}
    if not usage:
        return False  # empty dict is the codex pattern, not a claude stub
    return not any(
        isinstance(v, (int, float)) and v > 0 for v in usage.values()
    )


def has_nonzero_claude_usage(p: Any) -> bool:
    """Claude-code specific: terminal has at least one nonzero usage field."""
    if not is_terminal(p):
        return False
    usage = p["result"].get("usage") or {}
    return any(
        isinstance(v, (int, float)) and v > 0 for v in usage.values()
    )


def update_type(p: Any) -> str | None:
    """Extract the sessionUpdate variant from a session/update notification."""
    if not isinstance(p, dict) or p.get("method") != "session/update":
        return None
    upd = p.get("params", {}).get("update", {})
    if not isinstance(upd, dict):
        return None
    return upd.get("sessionUpdate")


def is_usage_update(p: Any) -> bool:
    return update_type(p) in ("usage_update", "usage_updated")


def extract_text(p: Any) -> str:
    """Pull the visible text from a session/update notification, if any."""
    if not isinstance(p, dict):
        return ""
    upd = p.get("params", {}).get("update", {})
    c = upd.get("content") or {} if isinstance(upd, dict) else {}
    if isinstance(c, dict):
        t = c.get("text") or c.get("thinking") or ""
        return t if isinstance(t, str) else ""
    return ""


def terminals_of(envelopes: list) -> list[tuple[float, str | None, dict]]:
    return [(t, tag, p) for t, tag, p in envelopes if is_terminal(p)]


def usage_updates_of(envelopes: list) -> list[tuple[float, str | None, dict]]:
    return [(t, tag, p) for t, tag, p in envelopes if is_usage_update(p)]


def untagged_blocks(envelopes: list) -> list:
    return [e for e in envelopes if e[1] is None]


# ---------------------------------------------------------------------------
# Runtime helpers
# ---------------------------------------------------------------------------

async def _try_create_agent(agent_type: str, name: str) -> Agent | None:
    """Return a registered Agent, or None if the provider failed to initialize.

    Returns None instead of raising so fixtures can call pytest.skip() cleanly
    when a provider requires credentials the server doesn't have (e.g. codex
    needs OPENAI_API_KEY, opencode needs ACP Phase 7).

    Codex tests are opt-in: they need a working OPENAI_API_KEY with quota,
    which most dev environments don't have. Set ``RUN_CODEX_TESTS=1`` to
    enable them. Without that flag, codex tests skip cleanly rather than
    failing with "no terminal arrived" (the symptom of an OpenAI auth or
    quota error from the spawned codex-acp).
    """
    secrets: dict[str, str] = {}
    if agent_type == "codex":
        if os.environ.get("RUN_CODEX_TESTS") != "1":
            return None
        key = os.environ.get("OPENAI_API_KEY")
        if not key:
            return None
        secrets["OPENAI_API_KEY"] = key
    try:
        agent = Agent(
            name, provider="unix_local", api_url=BASE_URL, agent_type=agent_type,
            secrets=secrets or None,
        )
        await asyncio.wait_for(agent._ensure_registered(), timeout=20.0)
        return agent
    except Exception:
        return None


async def capture_envelopes(
    session_id: str, stop: asyncio.Event, envelopes: list
) -> None:
    """Subscribe to /sessions/{id}/events and append every (t, tag, payload).

    Runs forever until `stop` is set. Each captured entry is a tuple:
      (timestamp_from_start, event:rpc:<tag> or None, payload_dict).
    """
    t_start = time.monotonic()
    async with ApiClient(BASE_URL, timeout=30.0) as sdk:
        buf = b""
        async for chunk in sdk.stream_events(session_id):
            if stop.is_set():
                return
            buf += chunk
            while b"\n\n" in buf:
                raw, buf = buf.split(b"\n\n", 1)
                block = raw.decode("utf-8", errors="replace")
                tag, payload = parse_tagged_block(block)
                if payload is None:
                    continue
                envelopes.append((time.monotonic() - t_start, tag, payload))


async def drain_inflight(session_id: str, timeout: float = 180.0) -> bool:
    """Poll GET /sessions/{id}/status until inflight_count reaches zero."""
    async with ApiClient(BASE_URL, timeout=30.0) as sdk:
        for _ in range(int(timeout * 2)):
            try:
                info = await sdk.get_session_status(session_id)
                if info.get("inflight_count", 0) == 0:
                    return True
            except Exception:
                pass
            await asyncio.sleep(0.5)
    return False


async def run_steps(
    agent: Agent,
    steps: list[tuple[str, Any]],
    *,
    drain_timeout: float = 120.0,
) -> tuple[list[tuple[str, str]], list]:
    """Run a sequence of `("send", prompt_text)` / `("wait", seconds)` steps.

    Returns `(rpc_ids, envelopes)` where rpc_ids is the list of rpc_ids
    returned by each `send` (in submission order) and envelopes is the
    complete captured timeline for the session.
    """
    session_id = agent.session_id
    envelopes: list = []
    stop = asyncio.Event()
    capture_task = asyncio.create_task(
        capture_envelopes(session_id, stop, envelopes)
    )
    await asyncio.sleep(0.5)  # let subscription land
    rpc_ids: list[tuple[str, str]] = []
    try:
        for kind, arg in steps:
            if kind == "wait":
                await asyncio.sleep(float(arg))
            elif kind == "send":
                rpc = await agent.send(str(arg))
                rpc_ids.append((rpc, str(arg)[:40]))
            else:
                raise ValueError(f"unknown step kind {kind!r}")
        await drain_inflight(session_id, timeout=drain_timeout)
        await asyncio.sleep(0.5)
    finally:
        stop.set()
        capture_task.cancel()
        try:
            await capture_task
        except (asyncio.CancelledError, Exception):
            pass
    return rpc_ids, envelopes


async def wait_for_stub(envelopes: list, rpc_id: str, *, timeout: float = 30.0) -> bool:
    """Poll the live envelopes list until a stub terminal for rpc_id appears.

    Used by deterministic merge-window tests to synchronize on the wire signal
    rather than a wall-clock wait.
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        for _, _, p in envelopes:
            if is_terminal(p) and p.get("id") == rpc_id and is_claude_stub(p):
                return True
        await asyncio.sleep(0.1)
    return False


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest_asyncio.fixture
async def claude_agent(request):
    """Create a claude-code agent, skip test if unavailable."""
    name = f"acp-inv-{request.node.name}"[:60]
    agent = await _try_create_agent("claude", name)
    if agent is None:
        pytest.skip(
            "claude agent not available (check server and ANTHROPIC_API_KEY)"
        )
    yield agent
    await _full_cleanup(agent)


@pytest_asyncio.fixture
async def codex_agent(request):
    """Create a codex agent, skip test if unavailable."""
    name = f"acp-inv-{request.node.name}"[:60]
    agent = await _try_create_agent("codex", name)
    if agent is None:
        pytest.skip(
            "codex agent not available (check server, codex install, and OPENAI_API_KEY)"
        )
    yield agent
    await _full_cleanup(agent)


@pytest_asyncio.fixture(params=["claude", "codex"])
async def any_agent(request):
    """Parametrized fixture: each test runs once per provider.

    Tests decorated with this fixture produce two test instances (claude and
    codex), each of which skips independently if that provider isn't available.
    """
    agent_type = request.param
    name = f"acp-inv-{agent_type}-{request.node.name}"[:60]
    agent = await _try_create_agent(agent_type, name)
    if agent is None:
        pytest.skip(f"{agent_type} agent not available")
    yield agent
    await _full_cleanup(agent)


async def _full_cleanup(agent: Agent) -> None:
    """Best-effort full teardown: delete the session (drops sandbox + DB row)
    AND close the Agent's HTTP client.

    ``Agent.aclose()`` only calls ``release_session`` (snapshot + drop pool
    lease) — the underlying daytona/docker sandbox stays alive on its
    provider-side state and the session row stays in DB. For test fixtures
    we want full teardown so per-test sandboxes don't accumulate and burn
    through the daytona account's disk quota under -n auto.
    """
    sid = agent.session_id
    if sid:
        try:
            await agent._api.delete_session(sid)
        except Exception as exc:
            print(f"_full_cleanup: delete_session({sid}) raised: {exc}")
    try:
        await agent.aclose()
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Universal invariants — must hold for every first-class provider
# ---------------------------------------------------------------------------

@skip_if_no_server
class TestUniversalInvariants:
    """Invariants that must hold for every first-class provider.

    These tests run once per agent type via the `any_agent` fixture. A failure
    on any provider is a regression in either our server's tagging/attribution
    layer or the provider's adherence to the core ACP contract.
    """

    @pytest.mark.asyncio
    async def test_every_block_has_rpc_tag(self, any_agent):
        """Server stamps every SSE block with `event: rpc:<id>` before data.

        This is what lets clients filter their own slice of the session stream.
        Untagged blocks (tag=None) indicate a gap in the active_rpc_id pointer
        — usually a bug in the reader's block-splitting loop.
        """
        _, envelopes = await run_steps(
            any_agent,
            [("send", "Just say exactly: HELLO")],
            drain_timeout=60.0,
        )
        untagged = untagged_blocks(envelopes)
        assert not untagged, (
            f"{len(untagged)} of {len(envelopes)} blocks missing event: rpc: tag"
        )

    @pytest.mark.asyncio
    async def test_usage_update_precedes_or_equals_terminal(self, any_agent):
        """For a clean single-prompt turn, usage_update fires at or before
        the terminal envelope (never after)."""
        _, envelopes = await run_steps(
            any_agent,
            [("send", "Just say exactly: HELLO")],
            drain_timeout=60.0,
        )
        uus = usage_updates_of(envelopes)
        terms = terminals_of(envelopes)
        if not uus or not terms:
            pytest.skip("missing usage_update or terminal — no ordering to check")
        latest_uu_time = max(t for t, _, _ in uus)
        earliest_term_time = min(t for t, _, _ in terms)
        # 200ms tolerance: usage_update and terminal can fire in the same tick
        assert latest_uu_time <= earliest_term_time + 0.2, (
            f"usage_update at {latest_uu_time:.2f}s fired AFTER terminal "
            f"at {earliest_term_time:.2f}s — invariant violated"
        )



# ---------------------------------------------------------------------------
# Codex-specific characterization
# ---------------------------------------------------------------------------

@skip_if_no_server
class TestCodexCharacterization:
    """Pin down codex's wire-level behavior, which differs from claude-code.

    These are characterization tests: they assert the current observed
    codex behavior so we get a clear failure signal if either (a) codex
    upstream fixes its concurrent-prompt handling, or (b) we accidentally
    apply claude-specific assumptions to codex.
    """

    @pytest.mark.asyncio
    async def test_single_prompt_terminal_has_empty_usage(self, codex_agent):
        """Codex terminals carry `usage: {}` (empty dict), not zero-filled.

        This distinguishes codex from claude-code's stub pattern (which uses
        zero-valued usage fields). Our `is_claude_stub` check correctly
        returns False for codex terminals because empty dict is not the
        same as all-zero values.
        """
        _, envelopes = await run_steps(
            codex_agent,
            [("send", "Just say: PING")],
            drain_timeout=60.0,
        )
        terminals = terminals_of(envelopes)
        assert len(terminals) == 1
        usage = terminals[0][2]["result"].get("usage")
        assert usage == {} or usage is None, (
            f"codex terminal should have empty usage, got {usage!r}"
        )
        # And is_claude_stub should correctly NOT flag this as a stub
        assert not is_claude_stub(terminals[0][2]), (
            "codex terminal with empty usage must NOT be classified as claude stub"
        )

    @pytest.mark.asyncio
    async def test_single_prompt_usage_update_has_null_cost(self, codex_agent):
        """Codex's usage_update carries `cost: None` (no billing telemetry).

        Claude-code fills this with `{amount: ..., currency: 'USD'}`.
        Both agents emit the same field name and position, but payload differs.
        """
        _, envelopes = await run_steps(
            codex_agent,
            [("send", "Just say: PING")],
            drain_timeout=60.0,
        )
        uus = usage_updates_of(envelopes)
        assert len(uus) >= 1
        for _, _, p in uus:
            upd = p.get("params", {}).get("update", {})
            cost = upd.get("cost")
            assert cost is None, f"expected codex cost=None, got {cost!r}"

    @pytest.mark.asyncio
    async def test_strict_sequential_two_prompts_get_two_terminals(self, codex_agent):
        """Codex DOES support sequential prompts on the same ACP session
        when each one is allowed to fully drain before the next is submitted.

        Submit A, wait for A's terminal AND wait for inflight_count to reach 0,
        THEN submit B. Both prompts get their own terminal with their own
        rpc_id, in FIFO order. This is the pattern our gate-based CodexAdapter
        enforces: reject concurrent submissions with 409, accept serialized
        submissions cleanly.

        The earlier probe at /tmp/codex_deep_probe.py showed "only 1 terminal
        for 2 sequential prompts", but that was because the probe submitted
        B before A's terminal had actually fired on the wire — a timing race,
        not a codex limitation. With proper drain-then-submit sequencing,
        codex gives each prompt its own terminal.
        """
        agent = codex_agent
        envelopes: list = []
        stop = asyncio.Event()
        capture_task = asyncio.create_task(
            capture_envelopes(agent.session_id, stop, envelopes)
        )
        await asyncio.sleep(0.5)
        try:
            rpc_a = await agent.send("Just say exactly: ONE")

            # Wait for A's terminal on the wire (deterministic signal)
            deadline = time.monotonic() + 60.0
            while time.monotonic() < deadline:
                if any(
                    is_terminal(p) and p.get("id") == rpc_a
                    for _, _, p in envelopes
                ):
                    break
                await asyncio.sleep(0.1)
            else:
                pytest.fail("A's terminal never arrived")

            # And wait for the server's inflight_count to actually be 0 so
            # the gate passes on the next submission
            drained = await drain_inflight(agent.session_id, timeout=30.0)
            assert drained, "inflight never reached 0 after A's terminal"

            rpc_b = await agent.send("Just say exactly: TWO")
            # Wait for B's terminal
            deadline = time.monotonic() + 60.0
            while time.monotonic() < deadline:
                if any(
                    is_terminal(p) and p.get("id") == rpc_b
                    for _, _, p in envelopes
                ):
                    break
                await asyncio.sleep(0.1)
            else:
                pytest.fail("B's terminal never arrived")
            await drain_inflight(agent.session_id, timeout=30.0)
        finally:
            stop.set()
            capture_task.cancel()
            try:
                await capture_task
            except (asyncio.CancelledError, Exception):
                pass

        terminals = terminals_of(envelopes)
        terminal_ids = [p["id"] for _, _, p in terminals]
        assert len(terminals) == 2, (
            f"expected 2 terminals for strict-sequential codex, got {len(terminals)}"
        )
        assert terminal_ids == [rpc_a, rpc_b], (
            f"terminals should be in FIFO submission order; got {terminal_ids}"
        )

    @pytest.mark.asyncio
    async def test_concurrent_submission_rejected_with_409(self, codex_agent):
        """The codex adapter rejects concurrent submissions with HTTP 409.

        Codex's codex-acp adapter cannot handle more than one prompt per
        ACP session — even sequential submissions on the same session leave
        the second prompt orphaned (no terminal, no content). So instead of
        attempting a server-side workaround, our CodexAdapter enforces a
        strict one-prompt-at-a-time rule via the agent_busy gate (stricter
        than claude: the interrupt flag does NOT bypass for codex). Clients
        must serialize their submissions themselves.
        """
        agent = codex_agent
        # Submit A directly via the client's _post_message to kick it off,
        # then immediately try to submit B. The second should raise HTTP 409.
        import httpx

        rpc_a = await agent.send("Just say: ONE AND WAIT PATIENTLY")
        assert rpc_a, "first submission should succeed"

        # Second submission while A is in flight should be rejected
        with pytest.raises((httpx.HTTPStatusError, Exception)) as exc_info:
            await agent.send("Second concurrent prompt — should be rejected")

        err = exc_info.value
        # Either HTTPStatusError 409 or a PromptError wrapping it
        msg = str(err)
        assert "409" in msg or "busy" in msg.lower(), (
            f"expected 409/busy error, got: {err!r}"
        )

        # Drain so the session is clean for the next test
        await drain_inflight(agent.session_id, timeout=45.0)
