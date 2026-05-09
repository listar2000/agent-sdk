"""Integration test: interrupt flow against a real claude-agent-acp with Haiku.

Requires:
  - Server running at localhost:7778 (docker compose up -d --build)
  - ANTHROPIC_API_KEY set (or in ~/.env)

Run:
  .venv/bin/pytest tests/test_interrupt_integration.py -v -s
"""

import asyncio
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from agent_sdk.client import Agent

# Load API keys from ~/.env if not already set
_KEY_PREFIXES = (
    "ANTHROPIC_API_KEY=",
    "GEMINI_API_KEY=",
    "GOOGLE_API_KEY=",
    "OPENROUTER_API_KEY=",
    "CLAUDE_CODE_OAUTH_TOKEN=",
)
env_path = os.path.expanduser("~/.env")
if os.path.exists(env_path):
    with open(env_path) as f:
        for line in f:
            line = line.strip()
            for prefix in _KEY_PREFIXES:
                if line.startswith(prefix):
                    key, val = line.split("=", 1)
                    if not os.environ.get(key):
                        os.environ[key] = val

API_URL = "http://localhost:7778"
MODEL = "claude-haiku-4-5-20251001"


def _server_reachable() -> bool:
    import httpx
    try:
        r = httpx.get(f"{API_URL}/health", timeout=3)
        return r.status_code == 200
    except Exception:
        return False


skip_no_server = pytest.mark.skipif(
    not _server_reachable(),
    reason="Server not running at localhost:7778",
)


@skip_no_server
class TestInterruptIntegration:

    @pytest.mark.asyncio
    async def test_basic_prompt(self):
        """Sanity check: a single prompt round-trips correctly."""
        agent = Agent(
            "test-basic",
            agent_type="claude",
            provider="unix_local",
            model=MODEL,
            cwd="/tmp",
            api_url=API_URL,
        )
        try:
            result = await agent.arun("Reply with exactly: HELLO")
            assert "HELLO" in result
        finally:
            await agent.aclose()

    @pytest.mark.asyncio
    async def test_interrupt_cancels_running_prompt(self):
        """interrupt=True cancels a running prompt, then the new prompt runs.

        1. Submit a slow prompt (ask for a long story)
        2. Wait for streaming to start
        3. Send a short prompt with interrupt=True
        4. Verify the first prompt was cancelled and the second completed
        """
        agent = Agent(
            "test-interrupt",
            agent_type="claude",
            provider="unix_local",
            model=MODEL,
            cwd="/tmp",
            api_url=API_URL,
        )
        try:
            await agent._ensure_registered()

            events_a = []
            events_b = []
            stop_reasons = []

            # Start SSE listener
            sse_started = asyncio.Event()
            sse_stop = asyncio.Event()

            async def listen():
                async with agent.events() as stream:
                    sse_started.set()
                    async for ev in stream:
                        if sse_stop.is_set():
                            return
                        if ev["type"] == "done":
                            stop_reasons.append(ev.get("stop_reason"))
                            if len(stop_reasons) >= 2:
                                sse_stop.set()

            listener = asyncio.create_task(listen())
            await sse_started.wait()

            # 1. Submit slow prompt (long response)
            rpc_a = await agent._post_message(
                "Write a 2000-word essay about the history of mathematics. "
                "Start immediately with content, no preamble."
            )
            print(f"\n[A] submitted rpc={rpc_a}")

            # 2. Wait for agent to start streaming
            await asyncio.sleep(3)

            # 3. Interrupt with a short prompt
            rpc_b = await agent._post_message(
                "Reply with exactly one word: INTERRUPTED",
                interrupt=True,
            )
            print(f"[B] submitted with interrupt=True rpc={rpc_b}")

            # 4. Wait for both to complete
            try:
                await asyncio.wait_for(sse_stop.wait(), timeout=30)
            except asyncio.TimeoutError:
                print(f"[TIMEOUT] got stop_reasons={stop_reasons}")

            print(f"[RESULT] stop_reasons={stop_reasons}")

            # First prompt should have been cancelled
            assert len(stop_reasons) >= 1, f"Expected at least 1 stop reason, got {stop_reasons}"
            assert "cancelled" in stop_reasons, (
                f"Expected 'cancelled' in stop_reasons, got {stop_reasons}"
            )

            listener.cancel()
            try:
                await listener
            except (asyncio.CancelledError, Exception):
                pass
        finally:
            await agent.aclose()

    @pytest.mark.asyncio
    async def test_interrupt_idle_no_cancel(self):
        """interrupt=True on an idle agent just submits normally (no cancel)."""
        agent = Agent(
            "test-int-idle",
            agent_type="claude",
            provider="unix_local",
            model=MODEL,
            cwd="/tmp",
            api_url=API_URL,
        )
        try:
            await agent._ensure_registered()

            rpc = await agent._post_message(
                "Reply with exactly: IDLE_OK",
                interrupt=True,
            )
            print(f"\n[idle] submitted with interrupt=True rpc={rpc}")

            # Collect response via arun-style loop
            result_text = ""
            async with agent.events() as stream:
                async for ev in stream:
                    if ev["type"] == "text":
                        result_text += ev["text"]
                    if ev["type"] == "done":
                        break

            print(f"[idle] result: {result_text}")
            assert "IDLE_OK" in result_text
        finally:
            await agent.aclose()

    @pytest.mark.asyncio
    async def test_queue_preserved_on_interrupt(self):
        """When B is queued and C interrupts A, B runs before C.

        1. Submit A (slow)
        2. Submit B (queued, no interrupt)
        3. Submit C (interrupt=True) — cancels A
        4. Order: A(cancelled) -> B -> C (queue preserved, C appended)
        """
        agent = Agent(
            "test-queue",
            agent_type="claude",
            provider="unix_local",
            model=MODEL,
            cwd="/tmp",
            api_url=API_URL,
        )
        try:
            await agent._ensure_registered()

            stop_reasons = []
            texts = []
            done_count = asyncio.Event()

            async def listen():
                current_text = []
                async with agent.events() as stream:
                    async for ev in stream:
                        if ev["type"] == "text":
                            current_text.append(ev["text"])
                        if ev["type"] == "done":
                            texts.append("".join(current_text))
                            current_text = []
                            stop_reasons.append(ev.get("stop_reason"))
                            if len(stop_reasons) >= 3:
                                done_count.set()

            listener = asyncio.create_task(listen())

            # A: slow prompt
            rpc_a = await agent._post_message(
                "Write a 1500-word essay about ocean currents. Start immediately."
            )
            print(f"\n[A] submitted rpc={rpc_a}")
            await asyncio.sleep(3)

            # B: queued (no interrupt)
            rpc_b = await agent._post_message(
                "Reply with exactly: BRAVO"
            )
            print(f"[B] submitted (queued) rpc={rpc_b}")

            # C: interrupt A
            rpc_c = await agent._post_message(
                "Reply with exactly: CHARLIE",
                interrupt=True,
            )
            print(f"[C] submitted (interrupt) rpc={rpc_c}")

            try:
                await asyncio.wait_for(done_count.wait(), timeout=60)
            except asyncio.TimeoutError:
                print(f"[TIMEOUT] stop_reasons={stop_reasons}, texts collected={len(texts)}")

            print(f"[RESULT] stop_reasons={stop_reasons}")
            for i, t in enumerate(texts):
                print(f"[TEXT {i}] {t[:100]}...")

            # A should be cancelled
            assert "cancelled" in stop_reasons, f"A should be cancelled: {stop_reasons}"
            # B and C should both complete
            assert len(stop_reasons) >= 3, f"Expected 3 completions, got {len(stop_reasons)}"

            # Both BRAVO and CHARLIE should appear in collected texts
            all_text = " ".join(texts)
            assert "BRAVO" in all_text, f"B's response missing: {all_text[:200]}"
            assert "CHARLIE" in all_text, f"C's response missing: {all_text[:200]}"

            # B should run before C (queue preserved: A(cancelled) -> B -> C)
            bravo_idx = next(i for i, t in enumerate(texts) if "BRAVO" in t)
            charlie_idx = next(i for i, t in enumerate(texts) if "CHARLIE" in t)
            assert bravo_idx < charlie_idx, (
                f"B should complete before C (queue preserved), "
                f"but BRAVO at index {bravo_idx}, CHARLIE at index {charlie_idx}"
            )

            listener.cancel()
            try:
                await listener
            except (asyncio.CancelledError, Exception):
                pass
        finally:
            await agent.aclose()


# ---------------------------------------------------------------------------
# Gemini CLI integration
# ---------------------------------------------------------------------------

skip_no_gemini_key = pytest.mark.skipif(
    not os.environ.get("GEMINI_API_KEY") and not os.environ.get("GOOGLE_API_KEY"),
    reason="GEMINI_API_KEY or GOOGLE_API_KEY not set",
)


@skip_no_server
@skip_no_gemini_key
class TestGeminiIntegration:

    @pytest.mark.asyncio
    async def test_gemini_basic_prompt(self):
        """Gemini agent: single prompt round-trip."""
        agent = Agent(
            "test-gemini",
            agent_type="gemini",
            provider="unix_local",
            cwd="/tmp",
            api_url=API_URL,
        )
        try:
            result = await agent.arun("Reply with exactly one word: GEMINI_OK")
            print(f"\n[gemini] result: {result}")
            assert "GEMINI_OK" in result
        finally:
            await agent.aclose()

    @pytest.mark.asyncio
    async def test_gemini_streaming(self):
        """Gemini agent: streaming events work."""
        agent = Agent(
            "test-gemini-stream",
            agent_type="gemini",
            provider="unix_local",
            cwd="/tmp",
            api_url=API_URL,
        )
        try:
            events = []
            async for ev in agent.astream("Reply with exactly: STREAM_TEST"):
                events.append(ev)
                print(f"[gemini-stream] {ev['type']}: {str(ev)[:80]}")

            assert any(ev["type"] == "text" for ev in events), "Should have text events"
            assert any(ev["type"] == "done" for ev in events), "Should have done event"
            text = "".join(str(ev) for ev in events if ev["type"] == "text")
            assert "STREAM_TEST" in text
        finally:
            await agent.aclose()

    @pytest.mark.asyncio
    async def test_gemini_interrupt(self):
        """Gemini agent: interrupt cancels running prompt."""
        agent = Agent(
            "test-gemini-int",
            agent_type="gemini",
            provider="unix_local",
            cwd="/tmp",
            api_url=API_URL,
        )
        try:
            await agent._ensure_registered()

            stop_reasons = []
            sse_started = asyncio.Event()
            sse_stop = asyncio.Event()

            async def listen():
                async with agent.events() as stream:
                    sse_started.set()
                    async for ev in stream:
                        if sse_stop.is_set():
                            return
                        if ev["type"] == "done":
                            stop_reasons.append(ev.get("stop_reason"))
                            if len(stop_reasons) >= 2:
                                sse_stop.set()

            listener = asyncio.create_task(listen())
            await sse_started.wait()

            await agent._post_message(
                "Write a 2000-word essay about machine learning history."
            )
            await asyncio.sleep(3)

            await agent._post_message(
                "Reply with exactly: GEMINI_INTERRUPTED",
                interrupt=True,
            )

            try:
                await asyncio.wait_for(sse_stop.wait(), timeout=30)
            except asyncio.TimeoutError:
                pass

            print(f"\n[gemini-int] stop_reasons={stop_reasons}")
            assert "cancelled" in stop_reasons, f"Expected cancelled: {stop_reasons}"

            listener.cancel()
            try:
                await listener
            except (asyncio.CancelledError, Exception):
                pass
        finally:
            await agent.aclose()


# ---------------------------------------------------------------------------
# OpenCode integration
# ---------------------------------------------------------------------------

_OPENROUTER_KEY = os.environ.get("OPENROUTER_API_KEY")
_OPENCODE_MODEL = "openrouter/anthropic/claude-3.5-haiku"


@skip_no_server
@pytest.mark.skipif(
    not _OPENROUTER_KEY,
    reason="OPENROUTER_API_KEY required for OpenCode (uses openrouter/* models)",
)
class TestOpenCodeIntegration:
    """OpenCode delegates fs/terminal access to the client. Tests confirm the
    full ACP loop (handshake → session/new → session/prompt → tool calls →
    stopReason) works with the supervisor's session/request_permission +
    fs/* + terminal/* handlers."""

    @pytest.mark.asyncio
    async def test_opencode_basic_prompt(self):
        """OpenCode agent: single prompt round-trip."""
        agent = Agent(
            "test-opencode",
            agent_type="opencode",
            provider="unix_local",
            model=_OPENCODE_MODEL,
            cwd="/tmp",
            api_url=API_URL,
            secrets={"OPENROUTER_API_KEY": _OPENROUTER_KEY},
        )
        try:
            result = await agent.arun("Reply with exactly one word: OPENCODE_OK")
            print(f"\n[opencode] result: {result}")
            assert "OPENCODE_OK" in result
        finally:
            await agent.aclose()

    @pytest.mark.asyncio
    async def test_opencode_streaming(self):
        """OpenCode agent: streaming events work."""
        agent = Agent(
            "test-opencode-stream",
            agent_type="opencode",
            provider="unix_local",
            model=_OPENCODE_MODEL,
            cwd="/tmp",
            api_url=API_URL,
            secrets={"OPENROUTER_API_KEY": _OPENROUTER_KEY},
        )
        try:
            events = []
            async for ev in agent.astream("Reply with exactly: OC_STREAM"):
                events.append(ev)

            assert any(ev["type"] == "text" for ev in events)
            assert any(ev["type"] == "done" for ev in events)
            text = "".join(str(ev) for ev in events if ev["type"] == "text")
            print(f"\n[opencode-stream] text: {text}")
            assert "OC_STREAM" in text
        finally:
            await agent.aclose()

    @pytest.mark.asyncio
    async def test_opencode_write_in_cwd(self):
        """OpenCode write tool: create a file in cwd via the agent."""
        import uuid as _uuid
        marker = f"oc-write-{_uuid.uuid4().hex[:8]}"
        agent = Agent(
            f"test-{marker}",
            agent_type="opencode",
            provider="unix_local",
            model=_OPENCODE_MODEL,
            cwd="/tmp",
            api_url=API_URL,
            secrets={"OPENROUTER_API_KEY": _OPENROUTER_KEY},
        )
        try:
            await agent.arun(
                f"Use the write tool to create the file /tmp/{marker}.txt "
                f"containing exactly: {marker}-CONTENT"
            )
            assert os.path.exists(f"/tmp/{marker}.txt"), \
                "opencode did not create the file in cwd"
            with open(f"/tmp/{marker}.txt") as f:
                assert marker in f.read()
        finally:
            try: os.remove(f"/tmp/{marker}.txt")
            except Exception: pass
            await agent.aclose()

    @pytest.mark.asyncio
    async def test_opencode_bash(self):
        """OpenCode bash tool: run a shell command and observe output."""
        agent = Agent(
            "test-opencode-bash",
            agent_type="opencode",
            provider="unix_local",
            model=_OPENCODE_MODEL,
            cwd="/tmp",
            api_url=API_URL,
            secrets={"OPENROUTER_API_KEY": _OPENROUTER_KEY},
        )
        try:
            result = await agent.arun(
                "Use the bash tool to run `echo OC_BASH_OK_$(uname -s)` "
                "and tell me what it printed."
            )
            print(f"\n[opencode-bash] result: {result}")
            assert "OC_BASH_OK_Linux" in result or "OC_BASH_OK" in result
        finally:
            await agent.aclose()


# ---------------------------------------------------------------------------
# Cline ACP integration
# ---------------------------------------------------------------------------

@skip_no_server
@pytest.mark.skip(reason="cline-acp needs Cline-specific config; wired but not yet validated")
class TestClineIntegration:
    """Cline ACP agent. Needs Cline-specific configuration."""

    @pytest.mark.asyncio
    async def test_cline_basic_prompt(self):
        """Cline agent: single prompt round-trip."""
        agent = Agent(
            "test-cline",
            agent_type="cline",
            provider="unix_local",
            cwd="/tmp",
            api_url=API_URL,
        )
        try:
            result = await agent.arun("Reply with exactly one word: CLINE_OK")
            print(f"\n[cline] result: {result}")
            assert "CLINE_OK" in result
        finally:
            await agent.aclose()

    @pytest.mark.asyncio
    async def test_cline_streaming(self):
        """Cline agent: streaming events."""
        agent = Agent(
            "test-cline-stream",
            agent_type="cline",
            provider="unix_local",
            cwd="/tmp",
            api_url=API_URL,
        )
        try:
            events = []
            async for ev in agent.astream("Reply with exactly: CLINE_STREAM"):
                events.append(ev)

            assert any(ev["type"] == "done" for ev in events)
            text = "".join(str(ev) for ev in events if ev["type"] == "text")
            print(f"\n[cline-stream] text: {text}")
            assert "CLINE_STREAM" in text
        finally:
            await agent.aclose()


# ---------------------------------------------------------------------------
# SSE ↔ Session Log parity tests
# ---------------------------------------------------------------------------

def _fetch_log_after_n_turn_ends(session_id: str, expected_turn_ends: int,
                                 *, limit: int = 30, deadline_s: float = 5.0):
    """Poll ``/sessions/{id}/log`` until ``expected_turn_ends`` ``turn_end``
    rows are present (or deadline elapses). The broadcast path leads the
    persist path by a few ms — broadcasts are intentionally per-chunk
    while persist coalesces and writes a row per logical block, so when
    an SSE listener sees ``done`` the matching ``turn_end`` row may not
    have landed yet. Polling makes the parity comparison
    durable-record-aware.
    """
    import time as _time
    end = _time.monotonic() + deadline_s
    last = []
    while _time.monotonic() < end:
        r = _httpx.get(f"{API_URL}/sessions/{session_id}/log?limit={limit}")
        last = r.json()
        if sum(1 for e in last if e.get("event_type") == "turn_end") >= expected_turn_ends:
            return last
        _time.sleep(0.1)
    return last

def _sse_to_canonical(events: list[dict]) -> list[dict]:
    """Convert a list of SSE Event dicts to canonical log-comparable form.

    Rules:
      - Consecutive text events are merged into one assistant_message entry.
      - Consecutive reasoning events are merged into one reasoning entry.
      - tool, tool_result, usage, done, error pass through as-is.
      - done becomes turn_end with stop_reason.
    """
    out: list[dict] = []
    text_buf: list[str] = []
    think_buf: list[str] = []

    def flush():
        if text_buf:
            out.append({"type": "assistant_message", "text": "".join(text_buf)})
            text_buf.clear()
        if think_buf:
            out.append({"type": "reasoning", "text": "".join(think_buf)})
            think_buf.clear()

    for ev in events:
        t = ev.get("type")
        if t == "text":
            if think_buf:
                flush()  # reasoning → text transition
            text_buf.append(ev.get("text", ""))
        elif t == "reasoning":
            if text_buf:
                flush()  # text → reasoning transition
            think_buf.append(ev.get("text", ""))
        elif t == "tool":
            flush()
            out.append({"type": "tool_call", "tool": ev.get("tool_name")})
        elif t == "tool_result":
            out.append({"type": "tool_result", "tool": ev.get("tool_name")})
        elif t == "usage":
            out.append({"type": "usage"})
        elif t == "done":
            flush()
            out.append({"type": "turn_end", "stop_reason": ev.get("stop_reason")})
        elif t == "error":
            flush()
            out.append({"type": "error"})
    flush()
    return out


def _log_to_canonical(log_entries: list[dict]) -> list[dict]:
    """Convert session log entries to the same canonical form as SSE.

    The persist path coalesces consecutive ``text`` / ``reasoning``
    chunks into one row at write-time (see
    ``_persist_prompt_events._flush_buffers``), so this is a 1:1
    mapping with no further merging.
    """
    out: list[dict] = []
    for entry in log_entries:
        et = entry["event_type"]
        p = entry.get("payload", {})
        if et == "user_message":
            out.append({"type": "user_message", "text": p.get("text", "")})
        elif et == "assistant_message":
            out.append({"type": "assistant_message", "text": p.get("text", "")})
        elif et == "reasoning":
            out.append({"type": "reasoning", "text": p.get("text", "")})
        elif et == "tool_call":
            out.append({"type": "tool_call", "tool": p.get("tool")})
        elif et == "tool_result":
            out.append({"type": "tool_result", "tool": p.get("tool")})
        elif et == "usage":
            out.append({"type": "usage"})
        elif et == "turn_end":
            out.append({"type": "turn_end", "stop_reason": p.get("stop_reason")})
        elif et == "error":
            out.append({"type": "error"})
    return out


def _assert_parity(sse_canonical: list[dict], log_canonical: list[dict], label: str):
    """Assert SSE and log have the same event types in order.

    Text content may differ slightly (SSE has raw chunks, log has
    accumulated text), so we compare types + structure, and verify
    text content is a substring match (log text contains SSE text).
    """
    sse_types = [e["type"] for e in sse_canonical]
    log_types = [e["type"] for e in log_canonical]
    # Log has user_message at the start (SSE doesn't)
    log_without_user = [t for t in log_types if t != "user_message"]
    assert sse_types == log_without_user, (
        f"[{label}] Event type mismatch:\n"
        f"  SSE: {sse_types}\n"
        f"  Log: {log_without_user}"
    )
    # Verify text content matches
    sse_text = [e for e in sse_canonical if e.get("text")]
    log_text = [e for e in log_canonical if e.get("text") and e["type"] != "user_message"]
    for s, l in zip(sse_text, log_text):
        assert s["type"] == l["type"], f"[{label}] type mismatch: SSE {s['type']} vs log {l['type']}"
        # Log text should contain the SSE text (log may have more from accumulation)
        assert s["text"] in l["text"] or l["text"] in s["text"], (
            f"[{label}] text mismatch ({s['type']}): SSE={s['text'][:60]!r} vs log={l['text'][:60]!r}"
        )


import httpx as _httpx


@skip_no_server
class TestSSELogParity:
    """Verify that SSE events and session log capture the same information."""

    @pytest.mark.asyncio
    async def test_simple_prompt_parity(self):
        """Simple text response: SSE events match session log."""
        agent = Agent(
            "parity-simple", agent_type="claude", provider="unix_local",
            model=MODEL, cwd="/tmp", api_url=API_URL,
        )
        try:
            await agent._ensure_registered()
            sse_events = []

            async def listen():
                async with agent.events() as stream:
                    async for ev in stream:
                        sse_events.append(ev)
                        if ev["type"] == "done":
                            return

            await agent._post_message("Reply with exactly: PARITY_OK")
            await asyncio.wait_for(listen(), timeout=30)

            log_entries = _fetch_log_after_n_turn_ends(
                agent.session_id, 1, limit=20, deadline_s=10.0,
            )

            sse_c = _sse_to_canonical(sse_events)
            log_c = _log_to_canonical(log_entries)
            print(f"\n[simple] SSE: {[e['type'] for e in sse_c]}")
            print(f"[simple] Log: {[e['type'] for e in log_c]}")
            _assert_parity(sse_c, log_c, "simple")
        finally:
            await agent.aclose()

    @pytest.mark.asyncio
    async def test_tool_use_parity(self):
        """Tool call + result: SSE events match session log."""
        agent = Agent(
            "parity-tool", agent_type="claude", provider="unix_local",
            model=MODEL, cwd="/tmp", api_url=API_URL,
        )
        try:
            await agent._ensure_registered()
            sse_events = []

            async def listen():
                async with agent.events() as stream:
                    async for ev in stream:
                        sse_events.append(ev)
                        if ev["type"] == "done":
                            return

            await agent._post_message("Use Bash to run: echo TOOL_PARITY")
            await asyncio.wait_for(listen(), timeout=30)

            log_entries = _fetch_log_after_n_turn_ends(
                agent.session_id, 1, limit=20, deadline_s=10.0,
            )

            sse_c = _sse_to_canonical(sse_events)
            log_c = _log_to_canonical(log_entries)
            print(f"\n[tool] SSE: {[e['type'] for e in sse_c]}")
            print(f"[tool] Log: {[e['type'] for e in log_c]}")
            _assert_parity(sse_c, log_c, "tool")
        finally:
            await agent.aclose()

    @pytest.mark.asyncio
    async def test_interrupt_parity(self):
        """Interrupted prompt: both turns logged with correct stop_reasons."""
        agent = Agent(
            "parity-int", agent_type="claude", provider="unix_local",
            model=MODEL, cwd="/tmp", api_url=API_URL,
        )
        try:
            await agent._ensure_registered()
            sse_events = []
            done_count = 0

            async def listen():
                nonlocal done_count
                async with agent.events() as stream:
                    async for ev in stream:
                        sse_events.append(ev)
                        if ev["type"] == "done":
                            done_count += 1
                            if done_count >= 2:
                                return

            listener = asyncio.create_task(listen())

            # Long prompt
            await agent._post_message(
                "Write a 2000-word essay about the history of computing."
            )
            # Wait until the model has produced SOME output before interrupting,
            # so the cancel actually races a running turn rather than firing
            # before the supervisor has even spawned. Fixed sleeps drift under
            # parallel test load.
            for _ in range(100):
                if any(e["type"] in ("text", "reasoning") for e in sse_events):
                    break
                await asyncio.sleep(0.2)
            else:
                pytest.fail("model produced no output within 20s")

            # Interrupt
            await agent._post_message("Say INTERRUPTED_OK", interrupt=True)

            await asyncio.wait_for(listener, timeout=30)

            log_entries = _fetch_log_after_n_turn_ends(
                agent.session_id, 2, limit=30, deadline_s=10.0,
            )

            # Check stop_reasons match between SSE and log
            sse_stops = [e.get("stop_reason") for e in sse_events if e["type"] == "done"]
            log_stops = [e["payload"].get("stop_reason")
                         for e in log_entries if e["event_type"] == "turn_end"]
            print(f"\n[interrupt] SSE stops: {sse_stops}")
            print(f"[interrupt] Log stops: {log_stops}")
            assert sse_stops == log_stops, (
                f"stop_reason mismatch: SSE={sse_stops} vs log={log_stops}"
            )
            assert "cancelled" in log_stops, "Should have a cancelled turn"
            assert "end_turn" in log_stops, "Should have an end_turn"

            # The cancelled turn may have partial assistant text and/or reasoning.
            # The second turn should have its own assistant_message.
            text_entries = [e for e in log_entries
                           if e["event_type"] in ("assistant_message", "reasoning")]
            assert len(text_entries) >= 1, (
                f"Expected at least 1 text entry, got {len(text_entries)}"
            )
        finally:
            await agent.aclose()

    @pytest.mark.asyncio
    async def test_queued_prompts_parity(self):
        """Multiple queued prompts: all user_messages and turn_ends logged."""
        agent = Agent(
            "parity-queue", agent_type="claude", provider="unix_local",
            model=MODEL, cwd="/tmp", api_url=API_URL,
        )
        try:
            await agent._ensure_registered()
            done_count = 0

            async def listen():
                nonlocal done_count
                async with agent.events() as stream:
                    async for ev in stream:
                        if ev["type"] == "done":
                            done_count += 1
                            if done_count >= 3:
                                return

            listener = asyncio.create_task(listen())

            # Queue 3 prompts
            await agent._post_message("Say ALPHA")
            await agent._post_message("Say BRAVO")
            await agent._post_message("Say CHARLIE")

            await asyncio.wait_for(listener, timeout=120)

            log_entries = _fetch_log_after_n_turn_ends(
                agent.session_id, 1, limit=30, deadline_s=10.0,
            )

            user_msgs = [e for e in log_entries if e["event_type"] == "user_message"]
            turn_ends = [e for e in log_entries if e["event_type"] == "turn_end"]
            assistant_msgs = [e for e in log_entries if e["event_type"] == "assistant_message"]

            print(f"\n[queue] user_messages: {len(user_msgs)}")
            print(f"[queue] turn_ends: {len(turn_ends)}")
            print(f"[queue] assistant_messages: {len(assistant_msgs)}")

            assert len(user_msgs) == 3, f"Expected 3 user_messages, got {len(user_msgs)}"
            assert len(turn_ends) == 3, f"Expected 3 turn_ends, got {len(turn_ends)}"
            assert len(assistant_msgs) >= 3, f"Expected ≥3 assistant_messages, got {len(assistant_msgs)}"

            # All should be end_turn (no interrupts)
            for te in turn_ends:
                assert te["payload"]["stop_reason"] == "end_turn"

            # Verify ordering: each user_message is followed by its turn's events
            types = [e["event_type"] for e in log_entries]
            print(f"[queue] full log types: {types}")
            um_indices = [i for i, t in enumerate(types) if t == "user_message"]
            te_indices = [i for i, t in enumerate(types) if t == "turn_end"]
            for um_i, te_i in zip(um_indices, te_indices):
                assert um_i < te_i, (
                    f"user_message at {um_i} should come before turn_end at {te_i}"
                )
        finally:
            await agent.aclose()

    @pytest.mark.asyncio
    async def test_multi_tool_parity(self):
        """Multiple tool calls in one turn: all logged in order."""
        agent = Agent(
            "parity-multitool", agent_type="claude", provider="unix_local",
            model=MODEL, cwd="/tmp", api_url=API_URL,
        )
        try:
            await agent._ensure_registered()
            sse_events = []

            async def listen():
                async with agent.events() as stream:
                    async for ev in stream:
                        sse_events.append(ev)
                        if ev["type"] == "done":
                            return

            # Force two distinct tool invocations by mixing tools — Claude
            # collapses two echo args into one Bash call when the prompt
            # only mentions Bash, but won't combine across tool kinds.
            await agent._post_message(
                "Do these two things, one at a time, using a tool for each:\n"
                "1. Use Bash to run: echo FIRST_TOOL\n"
                "2. Use the Read tool to read /etc/hostname\n"
                "After both, say DONE."
            )
            await asyncio.wait_for(listen(), timeout=60)

            log_entries = _fetch_log_after_n_turn_ends(
                agent.session_id, 1, limit=30, deadline_s=10.0,
            )

            sse_c = _sse_to_canonical(sse_events)
            log_c = _log_to_canonical(log_entries)
            sse_types = [e["type"] for e in sse_c]
            log_types = [e["type"] for e in log_c if e["type"] != "user_message"]
            print(f"\n[multitool] SSE: {sse_types}")
            print(f"[multitool] Log: {log_types}")

            # Should have at least 2 tool_call events
            sse_tools = [e for e in sse_c if e["type"] == "tool_call"]
            log_tools = [e for e in log_c if e["type"] == "tool_call"]
            assert len(sse_tools) >= 2, f"Expected >=2 tool_calls in SSE, got {len(sse_tools)}"
            assert len(log_tools) >= 2, f"Expected >=2 tool_calls in log, got {len(log_tools)}"

            _assert_parity(sse_c, log_c, "multitool")
        finally:
            await agent.aclose()

    @pytest.mark.asyncio
    async def test_interrupt_mid_tool_parity(self):
        """Interrupt while a tool (Bash sleep) is running."""
        agent = Agent(
            "parity-midtool", agent_type="claude", provider="unix_local",
            model=MODEL, cwd="/tmp", api_url=API_URL,
        )
        try:
            await agent._ensure_registered()
            sse_events = []
            done_count = 0

            async def listen():
                nonlocal done_count
                async with agent.events() as stream:
                    async for ev in stream:
                        sse_events.append(ev)
                        if ev["type"] == "done":
                            done_count += 1
                            if done_count >= 2:
                                return

            listener = asyncio.create_task(listen())

            # Start a slow bash command
            await agent._post_message(
                "Use Bash to run: for i in 1 2 3 4 5; do echo step-$i; sleep 1; done"
            )
            # Wait until the tool call is observed in SSE before interrupting.
            # A fixed sleep races with model latency under CPU pressure (xdist
            # parallelism); waiting for a concrete event makes the test
            # deterministic. Cap at 20s so a stuck model fails the timeout.
            for _ in range(100):
                if any(e["type"] == "tool" for e in sse_events):
                    break
                await asyncio.sleep(0.2)
            else:
                pytest.fail("model never issued a tool call within 20s")

            # Interrupt mid-tool
            await agent._post_message("Say MID_TOOL_OK", interrupt=True)

            await asyncio.wait_for(listener, timeout=30)

            log_entries = _fetch_log_after_n_turn_ends(
                agent.session_id, 2, limit=30, deadline_s=10.0,
            )

            sse_stops = [e.get("stop_reason") for e in sse_events if e["type"] == "done"]
            log_stops = [e["payload"].get("stop_reason")
                         for e in log_entries if e["event_type"] == "turn_end"]
            print(f"\n[midtool] SSE stops: {sse_stops}")
            print(f"[midtool] Log stops: {log_stops}")
            assert sse_stops == log_stops
            assert "end_turn" in log_stops

            # The first turn should have at least one tool_call
            first_turn_end = next(i for i, e in enumerate(log_entries) if e["event_type"] == "turn_end")
            first_turn_events = log_entries[:first_turn_end]
            first_tools = [e for e in first_turn_events if e["event_type"] == "tool_call"]
            print(f"[midtool] first turn had {len(first_tools)} tool_calls")
            assert len(first_tools) >= 1, "First turn should have started a tool"
        finally:
            await agent.aclose()

    @pytest.mark.asyncio
    async def test_queue_plus_interrupt_parity(self):
        """A running, B queued, C interrupts: all 3 logged correctly.

        Expected order: A(cancelled) -> B -> C (queue preserved).
        """
        agent = Agent(
            "parity-qi", agent_type="claude", provider="unix_local",
            model=MODEL, cwd="/tmp", api_url=API_URL,
        )
        try:
            await agent._ensure_registered()
            done_count = 0

            async def listen():
                nonlocal done_count
                async with agent.events() as stream:
                    async for ev in stream:
                        if ev["type"] == "done":
                            done_count += 1
                            if done_count >= 3:
                                return

            listener = asyncio.create_task(listen())

            # A: slow
            await agent._post_message(
                "Write a 1500-word essay about ocean biology."
            )
            await asyncio.sleep(2)

            # B: queued (no interrupt)
            await agent._post_message("Say QUEUED_B")

            # C: interrupt A
            await agent._post_message("Say INTERRUPT_C", interrupt=True)

            await asyncio.wait_for(listener, timeout=120)

            log_entries = _fetch_log_after_n_turn_ends(
                agent.session_id, 3, limit=40, deadline_s=10.0,
            )

            user_msgs = [e for e in log_entries if e["event_type"] == "user_message"]
            turn_ends = [e for e in log_entries if e["event_type"] == "turn_end"]
            stops = [e["payload"]["stop_reason"] for e in turn_ends]
            texts = [e["payload"]["text"] for e in log_entries if e["event_type"] == "assistant_message"]

            print(f"\n[qi] user_messages: {len(user_msgs)}")
            print(f"[qi] turn_ends: {stops}")
            print(f"[qi] assistant texts: {[t[:30] for t in texts]}")

            assert len(user_msgs) == 3, f"Expected 3 user_messages, got {len(user_msgs)}"
            assert len(turn_ends) == 3, f"Expected 3 turn_ends, got {len(turn_ends)}"
            assert "cancelled" in stops, "A should be cancelled"

            # B and C should both complete
            end_turns = [s for s in stops if s == "end_turn"]
            assert len(end_turns) == 2, f"Expected 2 end_turns, got {len(end_turns)}"

            # B runs before C (queue preserved)
            all_texts = " ".join(texts)
            if "QUEUED_B" in all_texts and "INTERRUPT_C" in all_texts:
                b_idx = next(i for i, t in enumerate(texts) if "QUEUED_B" in t)
                c_idx = next(i for i, t in enumerate(texts) if "INTERRUPT_C" in t)
                assert b_idx < c_idx, "B should complete before C (queue preserved)"
        finally:
            await agent.aclose()

    @pytest.mark.asyncio
    async def test_reasoning_only_parity(self):
        """Prompt that triggers reasoning but minimal output: reasoning logged."""
        agent = Agent(
            "parity-reason", agent_type="claude", provider="unix_local",
            model=MODEL, cwd="/tmp", api_url=API_URL,
        )
        try:
            await agent._ensure_registered()
            sse_events = []

            async def listen():
                async with agent.events() as stream:
                    async for ev in stream:
                        sse_events.append(ev)
                        if ev["type"] == "done":
                            return

            await agent._post_message(
                "Think carefully about what 2+2 is, then reply with just the number."
            )
            await asyncio.wait_for(listen(), timeout=30)

            log_entries = _fetch_log_after_n_turn_ends(
                agent.session_id, 3, limit=20, deadline_s=10.0,
            )

            sse_c = _sse_to_canonical(sse_events)
            log_c = _log_to_canonical(log_entries)
            print(f"\n[reason] SSE: {[e['type'] for e in sse_c]}")
            print(f"[reason] Log: {[e['type'] for e in log_c]}")

            # If SSE had reasoning, log should too
            sse_reasoning = [e for e in sse_c if e["type"] == "reasoning"]
            log_reasoning = [e for e in log_c if e["type"] == "reasoning"]
            if sse_reasoning:
                assert len(log_reasoning) >= 1, "Reasoning in SSE but not in log"
                print(f"[reason] reasoning text: {log_reasoning[0]['text'][:80]}...")

            _assert_parity(sse_c, log_c, "reasoning")
        finally:
            await agent.aclose()
