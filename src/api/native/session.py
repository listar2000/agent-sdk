"""NativeSession — the native runtime as a BaseSandboxSession.

Implements the server's ``execute_prompt`` contract from a local LiteLLM
loop instead of a supervisor SSE stream. Everything above the contract
(TurnRunner, session_log, /events, the SDK, goldens) is unchanged; this
class is where the runtime and the sandbox transport meet.

Key behaviors (docs/native_runtime_design.md §3):
- ``execute_prompt`` runs the loop in an INTERNAL task that feeds a queue
  the generator drains. TurnRunner pulls the generator inside its own task;
  cancelling that would kill the persister and skip the turn_end row. So
  ``cancel_active_prompt`` cancels the internal task instead, and its
  CancelledError is converted to a normal ``done(cancelled)`` terminal.
- liveness is always-alive: the session object IS the runtime (the base
  probe reports dead without a supervisor URL, which would make the pool
  tear down and rebuild a native session on every request).
- conversation state is the in-memory ``_messages`` array, checkpointed
  SYNCHRONOUSLY to native_transcripts at each turn end (never the lossy
  batcher) and reloaded on start().
- LLM keys (secrets ∩ AUTH_KEYS) stay server-side for the model call and
  never enter the sandbox; other secrets flow to the transport as env.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from collections.abc import AsyncIterator
from typing import Any

from api.providers._shared import AUTH_KEYS
from api.sandbox.session import BaseSandboxSession
from api.sandbox.state import NativeSandboxState

from . import frames
from .loop import NativeAgentSpec, initial_messages, run_turn
from .tools import build_toolset

log = logging.getLogger(__name__)

_SENTINEL = object()


class NativeSession(BaseSandboxSession):
    # Native's volume can live on any backend (docker/daytona/modal), so the
    # base's volume.provider == volume_provider check must be skipped.
    volume_provider = ""

    def __init__(self, *, session_id: str, state: NativeSandboxState) -> None:
        super().__init__(session_id=session_id, state=state)
        self._messages: list[dict] = []
        self._turn_seq: int = 0
        self._spec: NativeAgentSpec | None = None
        self._tools: dict = {}
        self._transport: Any = None
        self._llm_api_key: str | None = None
        self._sandbox_env: dict[str, str] = {}
        self._active_task: asyncio.Task | None = None
        self._started = False
        # Test seam: when set, passed to run_turn in place of litellm.acompletion.
        self._completion = None

    # ── lifecycle ───────────────────────────────────────────────────────────

    async def start(self) -> None:
        """Load the session/agent rows, build the spec + toolset, and
        rehydrate the conversation from the latest checkpoint. No compute —
        the sandbox is provisioned lazily on the first tool call."""
        if self._started:
            return
        from api import db as _db

        await self._bootstrap_session()  # hydrates _agent_id, _spawn_env, _cwd

        agent_cfg: dict = {}
        if self._agent_id:
            agent = await _db.get_agent(self._agent_id)
            if agent:
                agent_cfg = agent.get("config") or {}
        self._spec = NativeAgentSpec.from_config(
            model=agent_cfg.get("model"), native=agent_cfg.get("native"))
        self._tools = build_toolset(self._spec.tool_names)

        # Secrets split: AUTH_KEYS stay server-side (LLM); the rest are
        # sandbox env. _spawn_env is env ∪ secrets from _bootstrap_session.
        self._llm_api_key = next(
            (v for k, v in self._spawn_env.items() if k in AUTH_KEYS), None)
        self._sandbox_env = {k: v for k, v in self._spawn_env.items()
                             if k not in AUTH_KEYS}

        ckpt = await _db.read_native_checkpoint(self.session_id)
        if ckpt:
            self._messages = ckpt["messages"]
            self._turn_seq = ckpt["turn_seq"]
        else:
            self._messages = initial_messages(self._spec, None)
            self._turn_seq = 0
        self._started = True

    async def stop(self) -> None:
        """Persist-on-release. Checkpoints are already written per turn, so
        this is a no-op for the in-server state; provider pause for a live
        sandbox is a P1 concern (docker/modal volumes are POSIX-real;
        daytona pause lands with the daytona transport)."""
        return None

    async def shutdown(self) -> None:
        await self._cancel_active()
        if self._transport is not None:
            try:
                await self._transport.destroy()
            except Exception:
                log.exception("native: transport destroy failed for %s",
                              self.session_id)
            self._transport = None
        await super().shutdown()

    # ── liveness: the session object is the runtime ─────────────────────────

    async def running(self, *, force_probe: bool = False) -> bool:
        return True

    async def _liveness_probe(self) -> bool:
        return True

    # ── interrupt ───────────────────────────────────────────────────────────

    async def cancel_active_prompt(self) -> None:
        """Cancel the in-flight loop task. Its CancelledError handler emits
        ``done(cancelled)`` so TurnRunner and /events see a normal terminal."""
        await self._cancel_active()

    async def _cancel_active(self) -> None:
        task = self._active_task
        if task is not None and not task.done():
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass

    # ── the contract ────────────────────────────────────────────────────────

    async def execute_prompt(
        self, message: str, *, rpc_id: str | None = None,
    ) -> AsyncIterator[Any]:
        if not self._started:
            await self.start()
        rpc_id = rpc_id or str(uuid.uuid4())
        queue: asyncio.Queue = asyncio.Queue()

        async def emit(event: dict) -> None:
            block = frames.block_for_event(event, rpc_id, self.session_id)
            self._broadcast((rpc_id, block))
            self.liveness.observe_chunk()
            await queue.put(event)

        async def _drive() -> None:
            try:
                self._messages.append({"role": "user", "content": message})
                result = await run_turn(
                    self._spec, self._messages, self._tools, self._transport,
                    emit, completion=self._completion,
                    api_key=self._llm_api_key,
                    ensure_sandbox=self._ensure_sandbox,
                )
                self._messages = result.messages
                self._turn_seq += 1
                await self._checkpoint(result.usage)
            except asyncio.CancelledError:
                # Interrupt: surface a clean cancelled terminal, persist the
                # partial transcript, and DON'T re-raise (the cancellation's
                # job — stop the loop, emit cancelled — is done).
                await emit({"type": "done", "stop_reason": "cancelled"})
                self._turn_seq += 1
                await self._checkpoint({})
            except Exception as e:  # loop/model failure → error terminal
                log.exception("native loop failed for %s rpc=%s",
                              self.session_id, rpc_id)
                await emit({"type": "error", "text": str(e)[:500],
                            "kind": type(e).__name__})
            finally:
                await queue.put(_SENTINEL)

        task = asyncio.create_task(_drive())
        self._active_task = task
        try:
            while True:
                item = await queue.get()
                if item is _SENTINEL:
                    break
                yield item
        finally:
            self._active_task = None
            if not task.done():
                # Caller (TurnRunner) stopped pulling — cancel the loop.
                task.cancel()
                try:
                    await task
                except (asyncio.CancelledError, Exception):
                    pass

    # ── internals ───────────────────────────────────────────────────────────

    async def _checkpoint(self, usage: dict) -> None:
        from api import db as _db
        try:
            await _db.write_native_checkpoint(
                session_id=self.session_id, turn_seq=self._turn_seq,
                messages=self._messages, usage=usage or {})
        except Exception:
            log.exception("native checkpoint write failed for %s",
                          self.session_id)

    async def _ensure_sandbox(self):
        """Lazy provision on first tool call. The transport + state-persist
        wiring lands in P0-G (server); for now a transport injected by the
        server/test is reused, and a missing one is a clear error."""
        if self._transport is not None:
            return self._transport
        raise RuntimeError(
            "native: no sandbox transport bound (lazy provisioning is wired "
            "in P0-G server integration)")
