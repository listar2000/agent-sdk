"""The native agent loop — OpenAI-Agents-shaped, minimal.

``run_turn`` drives one user prompt to completion: call the model (LiteLLM,
streaming) → emit text/reasoning/usage → if the model asked for tools,
execute them in the sandbox and feed results back → repeat until the model
stops calling tools or ``max_turns`` is hit.

The loop is PURE with respect to the runtime: it knows nothing about
sessions, frames, broadcast, or checkpoints. It takes an ``emit`` callback
(canonical event dicts) and a ``tools`` map; NativeSession (P0-F) wires emit
to broadcast+yield+persist. This keeps the loop unit-testable with a mocked
``litellm.acompletion`` and a fake transport — no server, no LLM, no docker.

Event vocabulary emitted (canonical taxonomy, == parse_acp_event output):
``text``, ``reasoning``, ``tool``, ``tool_result``, ``usage``, ``done``,
``error`` — see api/native/frames.py for the wire templates.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable

log = logging.getLogger(__name__)

Emit = Callable[[dict], Awaitable[None]]


@dataclass
class NativeAgentSpec:
    instructions: str = ""
    model: str = "openrouter/openai/gpt-4o-mini"
    max_turns: int = 25
    max_tokens: int | None = None
    temperature: float | None = None
    tool_names: list[str] | None = None
    # LiteLLM retries the INITIAL model call on transient failures (rate
    # limits, connection resets, 5xx) before the stream is established — so a
    # blip doesn't fail the whole turn. It does NOT retry mid-stream (no
    # double-emit). Default on (2) for resilience; set 0 via native config to
    # disable, or higher for a flakier provider.
    num_retries: int = 2

    @classmethod
    def from_config(cls, *, model: str | None, native: dict | None) -> "NativeAgentSpec":
        n = native or {}
        return cls(
            instructions=n.get("instructions", ""),
            model=model or n.get("model") or cls.model,
            # Clamp the loop-control knobs to sane floors so a misconfigured
            # agent fails LOUDLY (or just runs) rather than silently: a
            # max_turns <= 0 makes ``range(max_turns)`` empty → a no-op turn
            # with no model call and a bare done(max_turns); a negative
            # num_retries would be handed straight to LiteLLM.
            max_turns=max(1, int(n.get("max_turns", cls.max_turns))),
            max_tokens=n.get("max_tokens"),
            temperature=n.get("temperature"),
            tool_names=n.get("tool_names"),
            num_retries=max(0, int(n.get("num_retries", cls.num_retries))),
        )


@dataclass
class _ToolCallAccum:
    id: str = ""
    name: str = ""
    # Streamed argument fragments, joined once at read time. Accumulating in a
    # list keeps a tool call's arguments O(total) even when a large argument
    # (e.g. a write_file ``content``) arrives across many deltas; the old
    # ``args += fragment`` rebuilt the whole string every delta — O(total²),
    # the same quadratic class as the dangling-heal full scan.
    arg_parts: list[str] = field(default_factory=list)

    @property
    def args(self) -> str:
        return "".join(self.arg_parts)


@dataclass
class TurnResult:
    messages: list[dict]
    stop_reason: str
    usage: dict = field(default_factory=dict)


def initial_messages(spec: NativeAgentSpec, prior: list[dict] | None) -> list[dict]:
    """Seed the message array: system prompt first (once), then prior
    conversation from the checkpoint, if any."""
    if prior:
        return list(prior)
    msgs: list[dict] = []
    if spec.instructions:
        msgs.append({"role": "system", "content": spec.instructions})
    return msgs


def heal_dangling_tool_calls(messages: list[dict]) -> None:
    """Make the transcript valid for the next completion: every assistant
    ``tool_calls`` id must be answered by a following ``tool`` message before
    the next user/assistant turn.

    An interrupt (CancelledError is a BaseException, so it bypasses the loop's
    ``except Exception`` tool guard) or a turn error landing mid-tool-loop —
    after the assistant ``tool_calls`` message is appended (loop.py) but before
    every tool result is — leaves an assistant message with missing tool
    results. Providers (Anthropic/OpenAI via LiteLLM) reject that with a 400,
    so once such a transcript is checkpointed the session is durably wedged:
    every later prompt re-raises across hibernate/resume/restart. Mutates
    ``messages`` in place, inserting an ``interrupted`` stub for each unanswered
    id right after the assistant's existing results. Idempotent — a clean
    transcript is left unchanged.

    Only the LAST assistant message can be dangling, so we scan back to it
    rather than walking the whole transcript: the loop appends a tool result for
    every call before its next model round (and an interrupt heals before the
    next user turn), so any earlier assistant ``tool_calls`` block is already
    fully answered. A checkpoint persisted mid-tool-loop leaves the gap at ITS
    tail too — which, after a new user message is appended, is still the last
    *assistant* message. Bounding the scan to the tail turns this from O(n) per
    turn — i.e. O(n²) over a long session, the dominant per-turn CPU cost — into
    O(tool-calls-in-the-current-turn). Equivalence with the full forward scan on
    every system-producible transcript is pinned by an oracle test in
    tests/test_native_loop.py."""
    n = len(messages)
    a = n - 1
    while a >= 0 and messages[a].get("role") != "assistant":
        a -= 1
    if a < 0:
        return
    m = messages[a]
    if not m.get("tool_calls"):
        return
    call_ids = [tc.get("id") for tc in m["tool_calls"] if tc.get("id")]
    # the contiguous run of tool results immediately after this assistant
    j = a + 1
    answered: set = set()
    while j < n and messages[j].get("role") == "tool":
        answered.add(messages[j].get("tool_call_id"))
        j += 1
    missing = [cid for cid in call_ids if cid not in answered]
    if missing:
        stubs = [{"role": "tool", "tool_call_id": cid,
                  "content": "error: interrupted"} for cid in missing]
        messages[j:j] = stubs   # after existing results, before next turn


async def run_turn(
    spec: NativeAgentSpec,
    messages: list[dict],
    tools: dict,                       # name -> Tool
    transport,                         # SandboxTransport | None (provisioned lazily)
    emit: Emit,
    *,
    completion=None,                   # injectable litellm.acompletion (tests)
    api_key: str | None = None,
    ensure_sandbox=None,               # async () -> transport, called on first tool use
) -> TurnResult:
    """Drive one user turn. ``messages`` already includes the new user
    message. Returns the grown message array + terminal stop reason."""
    if completion is None:
        import litellm
        completion = litellm.acompletion

    # Defensive: a checkpoint persisted mid-tool-loop (interrupt/error) or an
    # in-memory transcript from a prior errored turn can carry an assistant
    # tool_calls with unanswered ids — a provider 400 on the very first call.
    # Self-heal before we touch the model so a poisoned session recovers.
    heal_dangling_tool_calls(messages)

    tool_schemas = [t.schema for t in tools.values()] or None
    total_usage: dict[str, Any] = {}

    # Per-call kwargs are constant across a turn's model rounds — only
    # ``messages`` grows (in place) — so build them once instead of
    # re-evaluating the conditionals and rebuilding the dict every round.
    call_kwargs: dict[str, Any] = {
        "model": spec.model,
        "stream": True,
        "stream_options": {"include_usage": True},
    }
    if spec.num_retries:
        call_kwargs["num_retries"] = spec.num_retries
    if tool_schemas:
        call_kwargs["tools"] = tool_schemas
    if spec.temperature is not None:
        call_kwargs["temperature"] = spec.temperature
    if spec.max_tokens is not None:
        call_kwargs["max_tokens"] = spec.max_tokens
    if api_key:
        call_kwargs["api_key"] = api_key

    for _turn in range(spec.max_turns):
        text_parts: list[str] = []
        tool_calls: dict[int, _ToolCallAccum] = {}

        stream = await completion(messages=messages, **call_kwargs)
        async for chunk in stream:
            usage = getattr(chunk, "usage", None)
            if usage is not None:
                total_usage = _merge_usage(total_usage, usage)
            choices = getattr(chunk, "choices", None)
            if not choices:
                continue
            delta = choices[0].delta
            reasoning = getattr(delta, "reasoning_content", None)
            if reasoning:
                await emit({"type": "reasoning", "text": reasoning})
            content = getattr(delta, "content", None)
            if content:
                text_parts.append(content)
                await emit({"type": "text", "text": content})
            for tc in (getattr(delta, "tool_calls", None) or []):
                acc = tool_calls.setdefault(tc.index, _ToolCallAccum())
                if getattr(tc, "id", None):
                    acc.id = tc.id
                fn = getattr(tc, "function", None)
                if fn is not None:
                    if getattr(fn, "name", None):
                        acc.name = fn.name
                    if getattr(fn, "arguments", None):
                        acc.arg_parts.append(fn.arguments)

        # ── decide: tools or done ───────────────────────────────────────────
        assistant_msg: dict[str, Any] = {
            "role": "assistant",
            "content": "".join(text_parts) or None,
        }
        if not tool_calls:
            messages.append(assistant_msg)
            if total_usage:
                await emit({"type": "usage", "usage": total_usage})
            await emit({"type": "done", "stop_reason": "end_turn"})
            return TurnResult(messages, "end_turn", total_usage)

        # Record the assistant's tool-call request in the message array so
        # the follow-up tool messages are valid.
        ordered = [tool_calls[i] for i in sorted(tool_calls)]
        assistant_msg["tool_calls"] = [{
            "id": c.id, "type": "function",
            "function": {"name": c.name, "arguments": c.args or "{}"},
        } for c in ordered]
        messages.append(assistant_msg)

        if transport is None and ensure_sandbox is not None:
            transport = await ensure_sandbox()

        for c in ordered:
            args = _parse_args(c.args)
            await emit({"type": "tool", "tool_call_id": c.id,
                        "tool_name": c.name, "args": args})
            tool = tools.get(c.name)
            if tool is None:
                result = f"error: unknown tool {c.name!r}"
            else:
                # a recreate (on SandboxGoneError) may swap the transport;
                # reuse the returned one for the rest of this turn.
                result, transport = await _invoke_tool(
                    tool, transport, args, c.name, ensure_sandbox)
            await emit({"type": "tool_result", "tool_call_id": c.id,
                        "tool_name": c.name, "result": result})
            messages.append({"role": "tool", "tool_call_id": c.id,
                             "content": result})
        # loop back to call the model again with tool results in context

    # ran out of turns
    if total_usage:
        await emit({"type": "usage", "usage": total_usage})
    await emit({"type": "done", "stop_reason": "max_turns"})
    return TurnResult(messages, "max_turns", total_usage)


async def _invoke_tool(tool, transport, args, name, ensure_sandbox):
    """Run one tool. A tool failure is data (returned as an ``error:`` string),
    NOT a turn error. The one exception is ``SandboxGoneError`` — the sandbox
    died out from under the live session; recreate it (``replace=`` the dead
    transport, so a concurrent recovery is adopted rather than duplicated) and
    retry the tool ONCE so a routine modal hard-timeout (or docker prune /
    daytona hard-kill) doesn't permanently wedge the session. Returns
    ``(result_str, transport)`` — the transport may be a fresh one."""
    from .transport import SandboxGoneError
    try:
        return await tool.invoke(transport, args), transport
    except SandboxGoneError:
        if ensure_sandbox is None:
            return "error: sandbox gone and no recreate path available", transport
        log.warning("native sandbox gone mid-turn — recreating and retrying %s", name)
        # pass the DEAD transport so a concurrent recovery that already
        # recreated is adopted, not duplicated (no orphaned sandbox).
        transport = await ensure_sandbox(replace=transport)
        try:
            return await tool.invoke(transport, args), transport
        except Exception as e:
            log.exception("native tool %s failed after sandbox recreate", name)
            return f"error: {type(e).__name__}: {e}", transport
    except Exception as e:  # tool failure is data, not a turn error
        log.exception("native tool %s failed", name)
        return f"error: {type(e).__name__}: {e}", transport


def _parse_args(raw: str) -> dict:
    if not raw:
        return {}
    try:
        v = json.loads(raw)
        return v if isinstance(v, dict) else {"_raw": v}
    except json.JSONDecodeError:
        return {"_unparsed": raw}


def _merge_usage(acc: dict, usage) -> dict:
    """Fold a LiteLLM usage object/dict into the canonical cost shape the
    frames templates expect (inputTokens/outputTokens/totalCostUsd)."""
    def g(obj, *names):
        for n in names:
            v = getattr(obj, n, None) if not isinstance(obj, dict) else obj.get(n)
            if v is not None:
                return v
        return None
    inp = g(usage, "prompt_tokens", "input_tokens")
    out = g(usage, "completion_tokens", "output_tokens")
    cost = g(usage, "response_cost", "total_cost") or 0.0
    merged = dict(acc)
    if inp is not None:
        merged["inputTokens"] = merged.get("inputTokens", 0) + int(inp)
    if out is not None:
        merged["outputTokens"] = merged.get("outputTokens", 0) + int(out)
    merged["totalCostUsd"] = merged.get("totalCostUsd", 0.0) + float(cost)
    return merged
