"""Turn engine — drives execute_prompt and persists coalesced events.

Extracted from server.py so the turn logic can be imported and tested
independently without pulling in the full FastAPI app.  The public surface
is :class:`TurnRunner` plus two thin helpers re-exported for server.py:
``_persist_user_message`` and ``_EVENT_TYPE_TO_LOG``.
"""

from __future__ import annotations

import asyncio
import logging

from .event_buffer import get_batcher
from .db import log_event
from .redact import redact_secrets
from .models import (
    EVT_ASSISTANT_MESSAGE,
    EVT_ERROR,
    EVT_REASONING,
    EVT_TOOL_CALL,
    EVT_TOOL_RESULT,
    EVT_USAGE,
    EVT_USER_MESSAGE,
)

log = logging.getLogger(__name__)


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


async def _persist_user_message(
    session,
    message: str,
    rpc_id: str,
    attachments: list[dict] | None = None,
) -> None:
    """Write the EVT_USER_MESSAGE row for a freshly-submitted prompt.

    Best-effort — a DB hiccup must not block the prompt from being sent
    to the supervisor. The matching turn-end / tool / text rows are
    written by ``_persist_prompt_events`` as ``execute_prompt`` yields.

    Routes through the per-process ``SessionLogBatcher`` when available
    (the production path under lifespan); falls back to a direct INSERT
    in test contexts that bypass ``start_batcher`` so unit tests keep
    seeing user_message rows synchronously.

    ``attachments`` is an opaque list of dicts the caller wants to
    persist alongside the prompt text — used by hivespace to round-trip
    file metadata (id, url, sandbox_path, filename, …) so the chat UI
    can re-render images / file chips on cold-load without consulting a
    parallel DB. Treated as opaque here; no schema enforcement.
    """
    payload: dict = {"text": redact_secrets(message), "prompt_id": rpc_id}
    if attachments:
        payload["attachments"] = list(attachments)
    try:
        batcher = get_batcher()
        if batcher is not None:
            await batcher.add(
                session_id=session.session_id,
                agent_id=session._agent_id or "",
                event_type=EVT_USER_MESSAGE,
                payload=payload,
            )
        else:
            await log_event(
                session_id=session.session_id,
                agent_id=session._agent_id or "",
                event_type=EVT_USER_MESSAGE,
                payload=payload,
            )
    except Exception:
        log.exception("user_message log_event failed for session %s rpc=%s",
                      session.session_id, rpc_id)


class TurnRunner:
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

    def __init__(self, session, message: str, rpc_id: str, attachments=None):
        self.session = session
        self.message = message
        self.rpc_id = rpc_id
        self.attachments = attachments
        self.agent_id = session._agent_id or ""
        self.text_buf: list[str] = []
        self.think_buf: list[str] = []
        self.event_counts: dict[str, int] = {}
        self.saw_output_event = False
        self.saw_error_event = False
        self.terminal_stop_reason: str | None = None
        self.last_usage: object | None = None

    def _reset_turn_observability(self) -> None:
        self.event_counts.clear()
        self.saw_output_event = False
        self.saw_error_event = False
        self.terminal_stop_reason = None
        self.last_usage = None

    def _observe_event(self, event: dict) -> None:
        etype = str(event.get("type") or "event")
        self.event_counts[etype] = self.event_counts.get(etype, 0) + 1
        # ``done`` / ``usage`` events from execute_prompt carry their
        # payload under the ACP-style ``raw`` envelope until ``_write``
        # flattens it; read both shapes so observability sees the same
        # values the persisted row will.
        raw = event.get("raw") if isinstance(event.get("raw"), dict) else {}
        if etype in {"text", "reasoning", "tool", "tool_result"}:
            self.saw_output_event = True
        elif etype == "error":
            self.saw_error_event = True
        elif etype == "usage":
            self.last_usage = event.get("usage") or raw.get("usage")
        elif etype == "done":
            self.terminal_stop_reason = (
                event.get("stop_reason")
                or raw.get("stop_reason")
                or raw.get("stopReason")
            )

    def _log_empty_turn_if_needed(self) -> None:
        if self.saw_output_event or self.saw_error_event:
            return
        if self.terminal_stop_reason is None or self.terminal_stop_reason == "cancelled":
            return
        log.warning(
            "empty prompt turn: session=%s agent=%s rpc=%s "
            "stop_reason=%s usage=%r events=%s message_chars=%d",
            self.session.session_id,
            self.agent_id,
            self.rpc_id,
            self.terminal_stop_reason,
            self.last_usage,
            dict(sorted(self.event_counts.items())),
            len(self.message or ""),
        )

    async def _write(self, event: dict) -> None:
        etype = event.get("type", "event")
        # Flatten ``raw`` (the original ACP update payload) into the row
        # so the dashboard's permissive renderer finds tool/result/usage
        # fields without needing the nested ``raw`` indirection.
        payload = {k: v for k, v in event.items() if k != "type"}
        if isinstance(payload.get("raw"), dict):
            payload.update(payload.pop("raw"))
        if "text" in payload:
            payload["text"] = redact_secrets(payload["text"])
        payload["prompt_id"] = self.rpc_id
        try:
            batcher = get_batcher()
            if batcher is not None:
                await batcher.add(
                    session_id=self.session.session_id,
                    agent_id=self.agent_id,
                    event_type=_EVENT_TYPE_TO_LOG.get(etype, etype),
                    payload=payload,
                )
            else:
                await log_event(
                    session_id=self.session.session_id,
                    agent_id=self.agent_id,
                    event_type=_EVENT_TYPE_TO_LOG.get(etype, etype),
                    payload=payload,
                )
        except Exception:
            log.exception("log_event(%s) failed for session %s rpc=%s",
                          etype, self.session.session_id, self.rpc_id)

    async def _flush_buffers(self) -> None:
        if self.text_buf:
            await self._write({"type": "text", "text": "".join(self.text_buf)})
            self.text_buf.clear()
        if self.think_buf:
            await self._write({"type": "reasoning", "text": "".join(self.think_buf)})
            self.think_buf.clear()

    async def _drive_one(self, active_session) -> tuple[bool, Exception | None]:
        """Drive execute_prompt on a specific session; return
        (terminal_seen, last_exception). ``terminal_seen=True`` means we
        consumed a ``done`` or ``error`` event — the rpc is complete and
        no retry is appropriate. Otherwise ``False`` + exception means
        the supervisor died mid-flight and the caller should retry on
        the pool's current session."""
        terminal = False
        try:
            async for event in active_session.execute_prompt(self.message, rpc_id=self.rpc_id):
                if not isinstance(event, dict):
                    continue
                self._observe_event(event)
                t = event.get("type")
                if t == "text":
                    if self.think_buf:
                        await self._flush_buffers()
                    self.text_buf.append(event.get("text", ""))
                elif t == "reasoning":
                    if self.text_buf:
                        await self._flush_buffers()
                    self.think_buf.append(event.get("text", ""))
                elif t == "usage":
                    await self._write(event)
                else:
                    await self._flush_buffers()
                    await self._write(event)
                    if t in ("done", "error"):
                        terminal = True
            await self._flush_buffers()
            # A stream that ENDS WITHOUT a done/error is NOT a successful turn:
            # the supervisor died mid-prompt and the SSE EOF'd cleanly (no
            # exception) — a killed daytona supervisor does this after its proxy
            # holds the dead connection ~180s. Returning ``True`` here claimed
            # success, so run() neither recovered nor wrote a terminal, and the
            # client polling /log (or /events) waited forever for a turn_end
            # that was never persisted: a silently DROPPED prompt. Report
            # ``terminal`` (False when none was seen) so run() recovers or
            # writes an error.
            return terminal, None
        except Exception as e:
            return terminal, e

    async def run(self) -> None:
        """Execute the full turn lifecycle under the session's prompt lock.

        Per-session prompt serialisation: only one execute_prompt drives
        the supervisor at a time. Without this, two concurrent POST
        /message calls produce two parallel persist tasks racing on
        ``session_log`` writes and the row order diverges from SSE
        arrival order (the
        ``test_interrupt_mid_tool_parity ['cancelled','end_turn'] vs
        ['end_turn','cancelled']`` flake under -n auto). FIFO is
        preserved across queued prompts; ``interrupt=True`` cancels the
        active turn so the lock releases promptly without reordering.

        All cleanup paths (final flush on success, error-row write, hard-
        cancel buffer flush) MUST run while the lock is held — otherwise
        the next prompt's persist task can interleave its writes with
        this prompt's tail and the log row order de-syncs from SSE.
        """
        async with self.session._prompt_lock:
            # Log the user_message INSIDE the lock so log row order tracks
            # actual execution order.
            await _persist_user_message(self.session, self.message, self.rpc_id, self.attachments)
            # Mark the prompt in flight so the idle reaper never hibernates this
            # session mid-turn — covers a long, chunk-silent tool call whose
            # compute clock would otherwise go stale. Balanced across the
            # recovery swap below and released in the finally.
            self.session.liveness.observe_prompt_start()
            try:
                ok, exc = await self._drive_one(self.session)
                # Supervisor died mid-prompt? If the pool already cold-recovered
                # the session (a sibling request observed alive=False and swapped
                # in a new SandboxSession), retry once on the fresh session —
                # this is the race that lost ``rpc=41095a61`` events on modal
                # ``test_message_immediately_after_stop``: the error broadcast
                # would otherwise land on a dict that was cleared during the
                # migration, and the SDK would time out waiting for an event
                # that never arrives.
                # Retry on ANY non-terminal outcome — an exception OR a clean
                # stream end that produced no terminal (both mean the supervisor
                # died mid-prompt). The pool may already have cold-recovered a
                # replacement session; re-drive the prompt on it.
                if not ok:
                    try:
                        from api.sandbox import get_pool as _gp
                        replacement = await _gp().get_session(self.session.session_id)
                    except Exception:
                        replacement = None
                    if replacement is not None and replacement is not self.session:
                        log.info(
                            "execute_prompt retry: session %s recovered rpc=%s",
                            self.session.session_id, self.rpc_id,
                        )
                        self.text_buf.clear(); self.think_buf.clear()
                        self._reset_turn_observability()
                        # Move the in-flight marker onto the session the pool now
                        # owns so each session's counter balances independently
                        # (old: +1 at top then -1 here; new: +1 here then -1 in
                        # the finally).
                        self.session.liveness.observe_prompt_end()
                        self.session = replacement  # downstream writes use the new one
                        self.session.liveness.observe_prompt_start()
                        ok, exc = await self._drive_one(replacement)
                if not ok:
                    e = exc if exc is not None else RuntimeError(
                        "stream ended without terminal event"
                    )
                    log.exception(
                        "execute_prompt failed for session %s rpc=%s: %s",
                        self.session.session_id, self.rpc_id, e,
                    )
                    await self._flush_buffers()
                    await self._write({
                        "type": "error",
                        "message": str(e)[:500], "kind": type(e).__name__,
                    })
                    # Broadcast to whichever session the pool currently has —
                    # NOT necessarily the one we started with. The old session's
                    # ``_subscribers`` dict may have been migrated to the new
                    # session by ``pool.get_session``'s subscriber hand-off;
                    # broadcasting to the stale ref reaches an empty dict.
                    from api.sandbox import get_pool as _gp2
                    current = _gp2()._active.get(self.session.session_id, self.session)  # noqa: SLF001
                    current._broadcast({
                        "type": "error", "rpc_id": self.rpc_id,
                        "jsonrpc": "2.0", "id": self.rpc_id,
                        "error": {
                            "code": -32603,
                            "message": str(e),
                            "data": {
                                "kind": type(e).__name__,
                                "exception_type": type(e).__name__,
                            },
                        },
                    })
                else:
                    self._log_empty_turn_if_needed()
            finally:
                # Release the in-flight marker on whichever session is current
                # (the original, or the replacement after a recovery swap) so
                # the reaper can hibernate it once it goes idle. The counter is
                # floored at 0, so this is safe even on the unbalanced error
                # paths.
                self.session.liveness.observe_prompt_end()
                # Hard-cancel path: ``CancelledError`` is a ``BaseException``
                # in Python 3.8+ and bypasses ``except Exception``. Without
                # this finally an asyncio Task cancellation (server shutdown,
                # session DELETE) drops the in-flight buffer. ``asyncio.shield``
                # keeps the flush running even if the surrounding task is in
                # a cancelling state.
                if self.text_buf or self.think_buf:
                    try:
                        await asyncio.shield(self._flush_buffers())
                    except Exception:
                        log.exception(
                            "final flush failed for session %s rpc=%s — buffer lost",
                            self.session.session_id, self.rpc_id,
                        )
