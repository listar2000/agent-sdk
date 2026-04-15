"""Integration tests — require live Docker stack (docker compose up).

Run: pytest tests/test_integration.py -v -m integration

These tests exercise the full stack: client SDK → FastAPI server → supervisor → claude-agent-acp.
They are slow (10-60s each) and require ANTHROPIC_API_KEY in the environment.
"""
import asyncio
import json
import os
import sys
import time

import httpx
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
from agent_sdk.client import Agent

BASE_URL = "http://localhost:7778"


def _server_reachable() -> bool:
    try:
        import httpx as _httpx
        r = _httpx.get(f"{BASE_URL}/health", timeout=3)
        return r.status_code == 200
    except Exception:
        return False


pytestmark = pytest.mark.integration

skip_if_no_server = pytest.mark.skipif(
    not _server_reachable(),
    reason="Docker server not reachable at localhost:7778"
)


@skip_if_no_server
@pytest.mark.asyncio
async def test_basic_arun():
    """Agent can complete a simple prompt end-to-end."""
    agent = Agent("test-basic", provider="local", api_url=BASE_URL, model="haiku")
    resp = await asyncio.wait_for(
        agent.arun("Reply with exactly: HELLO_WORLD"),
        timeout=60,
    )
    await agent.aclose()
    assert "HELLO_WORLD" in resp


@skip_if_no_server
@pytest.mark.asyncio
async def test_session_resume_recalls_context():
    """A resumed session recalls information from the previous turn."""
    import random
    num = random.randint(100, 999)  # 3-digit to avoid false positives

    agent = Agent("test-resume", provider="local", api_url=BASE_URL, model="haiku")
    resp1 = await asyncio.wait_for(
        agent.arun(f'Remember this number: {num}. Say only "OK {num}."'),
        timeout=90,
    )
    session_id = agent.session_id
    await agent.aclose()

    agent2 = Agent("test-resume-2", session_id=session_id, api_url=BASE_URL)
    resp2 = await asyncio.wait_for(
        agent2.arun("What number did I ask you to remember?"),
        timeout=90,
    )
    await agent2.aclose()

    assert str(num) in resp2, f"Expected {num} in: {resp2!r}"


@skip_if_no_server
@pytest.mark.asyncio
async def test_astream_yields_text_and_done():
    """astream yields at least one text event and a done event."""
    agent = Agent("test-events", provider="local", api_url=BASE_URL, model="haiku")
    events = []

    async def _collect():
        async for ev in agent.astream("Say exactly: OK"):
            events.append(ev)

    await asyncio.wait_for(_collect(), timeout=60)
    await agent.aclose()

    types = {e["type"] for e in events}
    assert "text" in types
    assert "done" in types
    full_text = "".join(e["text"] for e in events if e["type"] == "text")
    assert "OK" in full_text


# ---------------------------------------------------------------------------
# ACP event-ordering scenarios — characterization tests for claude-code's
# prompt-queueing behavior and the server's FIFO attribution rule. Each
# scenario captures raw SSE envelopes from GET /events and asserts on the
# order, tags, and stub-vs-real pattern of terminal envelopes.
# ---------------------------------------------------------------------------

def _parse_tagged_block(block: str):
    """Extract (rpc_id_tag, payload) from a tagged SSE block.

    Tag comes from the `event: rpc:<id>` line the server stamps on each block.
    Payload is the JSON-RPC envelope from the `data:` line(s).
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
    payload = None
    if data_lines:
        try:
            payload = json.loads("\n".join(data_lines))
        except json.JSONDecodeError:
            pass
    return tag, payload


def _is_terminal(payload: dict) -> bool:
    if not isinstance(payload, dict):
        return False
    return (
        "id" in payload
        and "result" in payload
        and isinstance(payload["result"], dict)
        and "stopReason" in payload["result"]
    )


def _is_stub(payload: dict) -> bool:
    """A terminal envelope with all-zero usage totals."""
    if not _is_terminal(payload):
        return False
    usage = payload["result"].get("usage") or {}
    return not any(
        isinstance(v, (int, float)) and v > 0 for v in usage.values()
    )


async def _capture_envelopes(session_id: str, stop: asyncio.Event,
                              envelopes: list) -> None:
    """Subscribe to GET /events and append every tagged block to the list.

    Each entry is (t_from_start, tag, payload_dict).
    """
    t_start = time.monotonic()
    async with httpx.AsyncClient(base_url=BASE_URL, timeout=None) as client:
        async with client.stream(
            "GET", f"/sessions/{session_id}/events",
            headers={"Accept": "text/event-stream"},
        ) as resp:
            buf = ""
            async for chunk in resp.aiter_text():
                if stop.is_set():
                    return
                buf += chunk
                while "\n\n" in buf:
                    block, buf = buf.split("\n\n", 1)
                    tag, payload = _parse_tagged_block(block)
                    if payload is None:
                        continue
                    envelopes.append((time.monotonic() - t_start, tag, payload))


async def _drain_inflight(session_id: str, *, timeout: float = 120.0) -> None:
    async with httpx.AsyncClient(base_url=BASE_URL, timeout=30.0) as client:
        for _ in range(int(timeout * 2)):
            info = (await client.get(f"/sessions/{session_id}/status")).json()
            if info.get("inflight_count", 0) == 0:
                return
            await asyncio.sleep(0.5)


async def _run_scenario(steps: list, agent_name: str):
    """Shared harness: spawn an Agent, run a sequence of send/wait steps,
    capture all /events envelopes, return (rpc_ids, envelopes).
    """
    agent = Agent(agent_name, provider="local", api_url=BASE_URL, model="haiku")
    await agent._ensure_registered()
    session_id = agent.session_id

    envelopes: list = []
    stop = asyncio.Event()
    capture_task = asyncio.create_task(_capture_envelopes(session_id, stop, envelopes))
    await asyncio.sleep(0.5)  # let /events subscription land

    rpc_ids: list[tuple[str, str]] = []
    try:
        for kind, arg in steps:
            if kind == "wait":
                await asyncio.sleep(arg)
            elif kind == "send":
                rpc = await agent.send(arg)
                rpc_ids.append((rpc, arg[:40]))
        await _drain_inflight(session_id, timeout=120.0)
        await asyncio.sleep(0.5)
    finally:
        stop.set()
        capture_task.cancel()
        try:
            await capture_task
        except (asyncio.CancelledError, Exception):
            pass
        await agent.aclose()

    return rpc_ids, envelopes


class TestACPEventOrdering:
    """Characterization tests for claude-code's prompt-queueing wire contract.

    These tests drive a live docker server through four scenarios covering:
      1. Multiple rapid interrupts during a long-running prompt → merge pattern
      2. Late interrupt arriving after the merged response starts → bounded merge window
      3. Rapid interrupts after a clean completion → sequential (non-merged) turns
      4. Mid-tool-loop interrupt → stub fires at the next tool boundary

    Each asserts on terminal envelope ordering, stub/real usage totals, and
    that every block carries a server-side `event: rpc:<id>` tag.
    """

    @skip_if_no_server
    @pytest.mark.asyncio
    async def test_scenario_1_rapid_interrupts_each_get_their_own_terminal(self):
        """A long bash + 3 rapid interrupts: interrupt+drain serializes
        prompts, so each completes (cancelled or end_turn) before the next
        starts. Every rpc gets its own terminal in FIFO order; the last
        runs to completion as end_turn.
        """
        rpc_ids, envelopes = await _run_scenario(
            [
                ("send", "Use the Bash tool to run `for i in 1 2 3 4 5 6; do echo line-$i; sleep 1; done`. After it finishes, summarize in one sentence."),
                ("wait", 2.0),
                ("send", "STOP and say exactly: B-INTERRUPTED"),
                ("wait", 0.5),
                ("send", "Also say exactly: C-INTERRUPTED"),
                ("wait", 0.5),
                ("send", "And finally say: D-INTERRUPTED"),
            ],
            agent_name="acp-scenario-1",
        )
        assert len(rpc_ids) == 4

        terminals = [(t, tag, p) for t, tag, p in envelopes if _is_terminal(p)]
        assert len(terminals) == 4, f"expected 4 terminals, got {len(terminals)}"

        terminal_ids = [p["id"] for _, _, p in terminals]
        expected_ids = [r for r, _ in rpc_ids]
        assert terminal_ids == expected_ids, (
            f"terminals out of FIFO order: got {terminal_ids}"
        )

        # Last terminal must be end_turn with usage.
        assert terminals[-1][2]["result"]["stopReason"] == "end_turn", (
            f"final terminal should be end_turn, got {terminals[-1][2]['result']['stopReason']}"
        )
        assert not _is_stub(terminals[-1][2]), "final end_turn should carry usage"

        # All intermediate terminals are either cancelled or end_turn
        # (depends on timing — interrupt may arrive after natural completion).
        for _, _, p in terminals[:-1]:
            assert p["result"]["stopReason"] in ("cancelled", "end_turn")

        untagged = [e for e in envelopes if e[1] is None]
        assert not untagged, f"{len(untagged)} blocks missing event: rpc: tag"

    @skip_if_no_server
    @pytest.mark.asyncio
    async def test_scenario_3_rapid_sequential_sends(self):
        """Three sequential agent.send() calls with interrupt+drain.
        PHASE-1 completes during the 4s wait. PHASE-2 may be cancelled
        or complete naturally (depends on timing). PHASE-3 completes."""
        rpc_ids, envelopes = await _run_scenario(
            [
                ("send", "Just say exactly: PHASE-1"),
                ("wait", 4.0),
                ("send", "Just say exactly: PHASE-2"),
                ("wait", 0.3),
                ("send", "Just say exactly: PHASE-3"),
            ],
            agent_name="acp-scenario-3",
        )
        assert len(rpc_ids) == 3

        terminals = [(t, tag, p) for t, tag, p in envelopes if _is_terminal(p)]
        assert len(terminals) == 3, f"expected 3 terminals, got {len(terminals)}"

        terminal_ids = [p["id"] for _, _, p in terminals]
        expected_ids = [r for r, _ in rpc_ids]
        assert terminal_ids == expected_ids, (
            f"terminals out of FIFO order: got {terminal_ids}"
        )

        # PHASE-1 completes cleanly (no one interrupted it).
        assert terminals[0][2]["result"]["stopReason"] == "end_turn"
        assert not _is_stub(terminals[0][2]), "PHASE-1 should carry usage"
        # PHASE-3 completes cleanly.
        assert terminals[2][2]["result"]["stopReason"] == "end_turn"
        assert not _is_stub(terminals[2][2]), "PHASE-3 should carry usage"
        # PHASE-2: either cancelled or end_turn depending on timing.
        assert terminals[1][2]["result"]["stopReason"] in ("cancelled", "end_turn")

        untagged = [e for e in envelopes if e[1] is None]
        assert not untagged, f"{len(untagged)} blocks missing event: rpc: tag"

    @skip_if_no_server
    @pytest.mark.asyncio
    async def test_scenario_4_mid_tool_interrupt_cancels_in_progress_tool_loop(self):
        """A interrupt arriving mid-tool-loop fires session/cancel which
        interrupts the in-flight prompt. A's terminal stops with cancelled;
        B runs fresh and gets its own end_turn terminal."""
        rpc_ids, envelopes = await _run_scenario(
            [
                ("send",
                 "Run these bash commands in sequence using the Bash tool, ONE PER ROUND, "
                 "waiting for each to finish before the next:\n"
                 "1. `echo step-1 && sleep 1`\n"
                 "2. `echo step-2 && sleep 1`\n"
                 "3. `echo step-3 && sleep 1`\n"
                 "4. `echo step-4 && sleep 1`\n"
                 "Then write a 1-line summary."),
                ("wait", 3.5),
                ("send", "STOP the task. Just say exactly: INTERRUPTED"),
            ],
            agent_name="acp-scenario-4",
        )
        assert len(rpc_ids) == 2
        rpc_a, rpc_b = [r for r, _ in rpc_ids]

        terminals = [(t, tag, p) for t, tag, p in envelopes if _is_terminal(p)]
        assert len(terminals) == 2, f"expected 2 terminals, got {len(terminals)}"

        terminal_ids = [p["id"] for _, _, p in terminals]
        assert terminal_ids == [rpc_a, rpc_b], (
            f"terminals out of FIFO order: got {terminal_ids}"
        )

        # A is either cancelled (interrupt landed mid-tool) or end_turn
        # (tool loop finished before cancel propagated).
        assert terminals[0][2]["result"]["stopReason"] in ("cancelled", "end_turn"), (
            f"A should be cancelled or end_turn, got {terminals[0][2]['result']['stopReason']}"
        )
        assert terminals[1][2]["result"]["stopReason"] == "end_turn", (
            "B should complete cleanly"
        )
        assert not _is_stub(terminals[1][2]), "B end_turn should carry usage"

        # Confirm A actually started executing tools before cancellation.
        a_term_time = terminals[0][0]
        tool_events_before_a_term = [
            (t, tag, p) for t, tag, p in envelopes
            if t < a_term_time
            and isinstance(p, dict)
            and p.get("method") == "session/update"
            and p.get("params", {}).get("update", {}).get("sessionUpdate")
                in ("tool_call", "tool_call_update")
        ]
        assert len(tool_events_before_a_term) >= 2, (
            f"expected tool_call activity before A's terminal; got "
            f"{len(tool_events_before_a_term)} tool events"
        )

        untagged = [e for e in envelopes if e[1] is None]
        assert not untagged, f"{len(untagged)} blocks missing event: rpc: tag"
