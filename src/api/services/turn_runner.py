"""Turn execution engine: drive one prompt against the session's supervisor
and persist its coalesced ``session_log`` rows.

The ``_PromptGate`` async context manager makes the per-turn bookkeeping a
STRUCTURAL invariant — it holds the prompt lock, marks the turn in-flight for
the reaper (balanced across the mid-prompt recovery swap via ``handoff``), and
runs the shielded final flush under the lock. Extracted from server.py
(refactor slice 7) so the hardest, most concurrency-sensitive code lives in one
cohesive module guarded by tests/test_persist_parser_parity.py.

``get_batcher`` / ``log_event`` are read as module globals so tests can
monkeypatch ``turn_runner.log_event`` (the parity suite does).
"""
from __future__ import annotations

import asyncio
import logging

from api.db import log_event
from api.event_buffer import get_batcher
from api.models import (
    EVT_ASSISTANT_MESSAGE,
    EVT_ERROR,
    EVT_REASONING,
    EVT_TOOL_CALL,
    EVT_TOOL_RESULT,
    EVT_USAGE,
    EVT_USER_MESSAGE,
)
from api.redact import redact_secrets

log = logging.getLogger(__name__)


async def _log_one(
    session, agent_id: str, event_type: str, payload: dict,
) -> None:
    """Write ONE ``session_log`` row — single source for both the
    user_message row and every per-event row.

    Routes through the per-process ``SessionLogBatcher`` when available (the
    production path under lifespan); falls back to a direct ``log_event``
    INSERT in test contexts that bypass ``start_batcher`` so unit tests keep
    seeing rows synchronously. ``get_batcher`` / ``log_event`` are resolved
    as module globals at call time so test monkeypatches bind. Best-effort:
    a DB hiccup is logged and swallowed so it never blocks/aborts the turn.
    """
    try:
        batcher = get_batcher()
        if batcher is not None:
            await batcher.add(
                session_id=session.session_id, agent_id=agent_id,
                event_type=event_type, payload=payload,
            )
        else:
            await log_event(
                session_id=session.session_id, agent_id=agent_id,
                event_type=event_type, payload=payload,
            )
    except Exception:
        log.exception(
            "log_event(%s) failed for session %s rpc=%s",
            event_type, session.session_id, payload.get("prompt_id"),
        )


async def _persist_user_message(session, message: str, rpc_id: str) -> None:
    """Write the EVT_USER_MESSAGE row for a freshly-submitted prompt.

    Best-effort — a DB hiccup must not block the prompt from being sent
    to the supervisor. The matching turn-end / tool / text rows are
    written by ``_persist_prompt_events`` as ``execute_prompt`` yields.
    """
    payload = {"text": redact_secrets(message), "prompt_id": rpc_id}
    await _log_one(session, session._agent_id or "", EVT_USER_MESSAGE, payload)


# execute_prompt yields events whose ``type`` matches what
# ``api.sse.parse_acp_event`` emits — same taxonomy as the SDK
# ``astream`` and the /events SSE consumers. Any type missing from
# this map is logged as-is (forward-compat with new ACP update kinds).
_EVENT_TYPE_TO_LOG = {
    "text": EVT_ASSISTANT_MESSAGE,
    "reasoning": EVT_REASONING,
    "tool": EVT_TOOL_CALL,
    "tool_result": EVT_TOOL_RESULT,
    "usage": EVT_USAGE,
    "error": EVT_ERROR,
    "done": "turn_end",
}


class _PromptGate:
    """Owns one turn's per-session bookkeeping as a STRUCTURAL invariant.

    Replaces the hand-managed observe_prompt_start / observe_prompt_end
    balance that previously had to be threaded by hand across the mid-prompt
    recovery swap (start on entry, end+start at the swap, end in a finally).
    Getting that balance wrong left a session pinned "in flight" forever (the
    reaper would never reclaim it) or underflowed the counter.

    The gate:
      * holds the ORIGINATING session's ``_prompt_lock`` for the whole turn
        (the lock serialises prompts per session_id; the replacement after a
        recovery swap is a different object, but we keep the original lock so
        log-row order stays tied to one mutex);
      * marks the prompt in-flight on enter and releases it on exit, on
        whichever session is current — so a long chunk-silent tool call is
        never reaped mid-turn;
      * runs the shielded final flush while the lock is still held, so the
        next prompt's writes can't interleave with this turn's tail.

    ``handoff(replacement)`` moves the in-flight marker old→new atomically so
    each session's counter balances independently.
    """

    def __init__(self, session, *, final_flush, rpc_id: str) -> None:
        self._origin = session          # whose _prompt_lock we hold
        self.session = session          # current target (changes on handoff)
        self._final_flush = final_flush
        self._rpc_id = rpc_id

    async def __aenter__(self) -> "_PromptGate":
        await self._origin._prompt_lock.acquire()
        try:
            self.session.liveness.observe_prompt_start()
        except BaseException:
            self._origin._prompt_lock.release()
            raise
        return self

    def handoff(self, replacement) -> None:
        """Recovery swap: move the in-flight marker to the session the pool
        now owns. old: +1 on enter, -1 here; new: +1 here, -1 on exit."""
        self.session.liveness.observe_prompt_end()
        self.session = replacement
        self.session.liveness.observe_prompt_start()

    async def __aexit__(self, *_exc) -> bool:
        try:
            self.session.liveness.observe_prompt_end()
            # Final flush MUST run while the lock is still held — otherwise
            # the next prompt's persist task interleaves its writes with this
            # turn's tail and the log row order de-syncs from SSE. shield
            # keeps it running even under task cancellation (CancelledError
            # is a BaseException that bypasses ``except Exception``).
            try:
                await asyncio.shield(self._final_flush())
            except Exception:
                log.exception(
                    "final flush failed for session %s rpc=%s — buffer lost",
                    self._origin.session_id, self._rpc_id,
                )
        finally:
            self._origin._prompt_lock.release()
        return False  # never suppress exceptions


async def _persist_prompt_events(session, message: str, rpc_id: str) -> None:
    """Drive ``execute_prompt`` and write coalesced rows to ``session_log``.

    Consecutive ``text`` and ``reasoning`` chunks are buffered and written
    as ONE row per logical block (flush on type-change, tool call,
    usage, error, done, or end-of-stream). Discrete events (tool, tool
    result, usage, error, done) pass through as-is. This matches what
    SSE consumers see after canonicalization and makes ``/sessions/{id}/log``
    semantically equivalent to the SSE stream — neither per-chunk noise
    nor "one fat blob per turn."

    Each row carries the rpc_id so the log can be sliced by turn. Single
    write failures are non-fatal — log and keep draining so a transient
    DB hiccup doesn't drop the rest of the turn.
    """
    agent_id = session._agent_id or ""

    text_buf: list[str] = []
    think_buf: list[str] = []

    async def _write(event: dict) -> None:
        etype = event.get("type", "event")
        # Flatten ``raw`` (the original ACP update payload) into the row
        # so the dashboard's permissive renderer finds tool/result/usage
        # fields without needing the nested ``raw`` indirection.
        payload = {k: v for k, v in event.items() if k != "type"}
        if isinstance(payload.get("raw"), dict):
            payload.update(payload.pop("raw"))
        if "text" in payload:
            payload["text"] = redact_secrets(payload["text"])
        payload["prompt_id"] = rpc_id
        await _log_one(
            session, agent_id, _EVENT_TYPE_TO_LOG.get(etype, etype), payload,
        )

    async def _flush_buffers() -> None:
        if text_buf:
            await _write({"type": "text", "text": "".join(text_buf)})
            text_buf.clear()
        if think_buf:
            await _write({"type": "reasoning", "text": "".join(think_buf)})
            think_buf.clear()

    # Per-session prompt serialisation: only one execute_prompt drives
    # the supervisor at a time. Without this, two concurrent POST
    # /message calls produce two parallel persist tasks racing on
    # ``session_log`` writes and the row order diverges from SSE
    # arrival order (the
    # ``test_interrupt_mid_tool_parity ['cancelled','end_turn'] vs
    # ['end_turn','cancelled']`` flake under -n auto). FIFO is
    # preserved across queued prompts; ``interrupt=True`` cancels the
    # active turn so the lock releases promptly without reordering.
    #
    # All cleanup paths (final flush on success, error-row write, hard-
    # cancel buffer flush) MUST run while the lock is held — otherwise
    # the next prompt's persist task can interleave its writes with
    # this prompt's tail and the log row order de-syncs from SSE.
    async def _drive_one(active_session) -> tuple[bool, Exception | None]:
        """Drive execute_prompt on a specific session; return
        (terminal_seen, last_exception). ``terminal_seen=True`` means we
        consumed a ``done`` or ``error`` event — the rpc is complete and
        no retry is appropriate. Otherwise ``False`` + exception means
        the supervisor died mid-flight and the caller should retry on
        the pool's current session."""
        terminal = False
        try:
            async for event in active_session.execute_prompt(message, rpc_id=rpc_id):
                if not isinstance(event, dict):
                    continue
                t = event.get("type")
                if t == "text":
                    if think_buf:
                        await _flush_buffers()
                    text_buf.append(event.get("text", ""))
                elif t == "reasoning":
                    if text_buf:
                        await _flush_buffers()
                    think_buf.append(event.get("text", ""))
                elif t == "usage":
                    await _write(event)
                else:
                    await _flush_buffers()
                    await _write(event)
                    if t in ("done", "error"):
                        terminal = True
            await _flush_buffers()
            return True, None
        except Exception as e:
            return terminal, e

    # The gate holds the per-session prompt lock, marks the turn in-flight
    # for the reaper, and runs the shielded final flush on exit — all as a
    # structural invariant so the body below stays focused on driving the
    # turn and handling mid-prompt recovery. See ``_PromptGate``.
    async with _PromptGate(session, final_flush=_flush_buffers, rpc_id=rpc_id) as gate:
        # Log the user_message INSIDE the lock so log row order tracks
        # actual execution order.
        await _persist_user_message(gate.session, message, rpc_id)
        ok, exc = await _drive_one(gate.session)
        # Supervisor died mid-prompt? If the pool already cold-recovered
        # the session (a sibling request observed alive=False and swapped
        # in a new SandboxSession), retry once on the fresh session — this
        # is the race that lost ``rpc=41095a61`` events on modal
        # ``test_message_immediately_after_stop``: the error broadcast would
        # otherwise land on a dict that was cleared during the migration,
        # and the SDK would time out waiting for an event that never arrives.
        if not ok and exc is not None:
            try:
                from api.sandbox import get_pool as _gp
                replacement = await _gp().get_session(gate.session.session_id)
            except Exception:
                replacement = None
            if replacement is not None and replacement is not gate.session:
                log.info(
                    "execute_prompt retry: session %s recovered rpc=%s",
                    gate.session.session_id, rpc_id,
                )
                text_buf.clear(); think_buf.clear()
                gate.handoff(replacement)  # in-flight marker moves old->new
                ok, exc = await _drive_one(gate.session)
        if not ok:
            e = exc if exc is not None else RuntimeError(
                "stream ended without terminal event"
            )
            log.exception(
                "execute_prompt failed for session %s rpc=%s: %s",
                gate.session.session_id, rpc_id, e,
            )
            await _flush_buffers()
            await _write({
                "type": "error",
                "message": str(e)[:500], "kind": type(e).__name__,
            })
            # Broadcast to whichever session the pool currently has — NOT
            # necessarily the one we started with. The old session's
            # ``_subscribers`` dict may have been migrated to the new session
            # by ``pool.get_session``'s subscriber hand-off; broadcasting to
            # the stale ref reaches an empty dict.
            from api.sandbox import get_pool as _gp2
            current = _gp2()._active.get(gate.session.session_id, gate.session)  # noqa: SLF001
            current._broadcast({
                "type": "error", "rpc_id": rpc_id,
                "jsonrpc": "2.0", "id": rpc_id,
                "error": {
                    "code": -32603,
                    "message": str(e),
                    "data": {
                        "kind": type(e).__name__,
                        "exception_type": type(e).__name__,
                    },
                },
            })
