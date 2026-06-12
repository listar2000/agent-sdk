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


def _origin() -> str:
    import os
    return os.environ.get("AGENT_SDK_ORIGIN", "production")


def _native_image() -> str:
    """Image for native sandboxes — needs only a shell + coreutils/base64.
    The baked runtime image qualifies and is already pulled; override with
    ``AGENT_SDK_NATIVE_IMAGE`` (e.g. a leaner base in P1)."""
    import os
    from api.providers.docker import _read_runtime_image_tag
    return (os.environ.get("AGENT_SDK_NATIVE_IMAGE")
            or _read_runtime_image_tag()
            or "python:3.12-slim")


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
        self._provision_lock = asyncio.Lock()
        # Test seam: when set, passed to run_turn in place of litellm.acompletion.
        self._completion = None
        # Test seam: when set, used in place of provisioning a real container.
        self._transport_factory = None

    # ── lifecycle ───────────────────────────────────────────────────────────

    async def start(self) -> None:
        """Load the session/agent rows, build the spec + toolset, and
        rehydrate the conversation from the latest checkpoint. No compute —
        the sandbox is provisioned lazily on the first tool call."""
        if self._started:
            return
        from api import db as _db

        await self._bootstrap_session()  # hydrates _agent_id, _spawn_env, _cwd

        model = None
        native_cfg = None
        if self._agent_id:
            agent = await _db.get_agent(self._agent_id)
            if agent is not None:
                model = agent.config.model
                native_cfg = agent.config.native
        self._spec = NativeAgentSpec.from_config(model=model, native=native_cfg)
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
        """HIBERNATE (reaper / release): free the sandbox's CPU+RAM but KEEP
        the container so a future prompt resumes it with workspace intact.
        Conversation state is already checkpointed per turn. ``sandbox_ref``
        is preserved on ``state`` (the pool persists it after this returns)
        so the next provision reattaches instead of creating fresh."""
        if self._transport is not None:
            try:
                await self._transport.hibernate()
            except Exception:
                log.exception("native: hibernate failed for %s", self.session_id)

    async def shutdown(self) -> None:
        """In-memory teardown only — must NOT destroy compute (release()
        calls stop()+shutdown(); destroying here would make every reap a
        hard delete). The container is freed by ``destroy()`` on session
        delete, or left hibernated by ``stop()`` on reap."""
        await self._cancel_active()
        self._transport = None
        await super().shutdown()

    async def destroy(self) -> None:
        """Hard-delete the sandbox compute (DELETE /sessions). Idempotent;
        works by ``sandbox_ref`` even when this instance never provisioned
        the container (the post-hibernate cold path)."""
        ref = getattr(self.state, "sandbox_ref", None)
        if not ref:
            return
        provider = getattr(self.state, "provider", "docker")
        try:
            await self._reattach_transport(provider, ref).destroy()
        except Exception:
            log.exception("native: destroy failed for %s ref=%s",
                          self.session_id, ref)
        self._transport = None

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

    # ── sandbox: lazy provisioning + server exec/file routing ───────────────

    async def _ensure_sandbox(self, *, refresh: bool = False):
        """Provision the sandbox on first need (tool call or /sandbox/exec).

        Under a lock so concurrent tool calls in one turn provision once.
        Persists ``state.sandbox_ref`` immediately so a crash can't strand a
        container the boot reconciler would later orphan.

        ``refresh=True`` drops the cached transport first, so a sandbox that
        died out from under a LIVE session (``SandboxGoneError`` — docker
        container pruned, modal hard-timeout, daytona hard-kill) re-runs the
        status→recreate flow instead of being wedged on the dead transport
        forever. The recreate recovers the workspace on modal/daytona volumes;
        docker cold-creates fresh (its writable layer is gone with the container).
        """
        if refresh:
            self._transport = None
        if self._transport is not None:
            return self._transport
        async with self._provision_lock:
            if self._transport is not None:
                return self._transport
            if not self._started:
                await self.start()
            if self._transport_factory is not None:
                t = await self._transport_factory()
                self._transport = t
                self.state.sandbox_ref = t.ref
                await self._persist_state()
                return t

            provider = getattr(self.state, "provider", "docker")
            ref = getattr(self.state, "sandbox_ref", None)

            # RESUME: a prior provision left a hibernated (stopped) or
            # still-running sandbox. ONE status() classifies it — reattach +
            # resume (workspace intact) rather than create fresh. The flow is
            # provider-uniform over the transport interface; only the class
            # and the cold-create args differ.
            if ref:
                t = self._reattach_transport(provider, ref)
                st = await t.status()
                if st == "running":
                    self._transport = t
                    return t
                if st == "stopped":
                    await t.resume()
                    self._transport = t
                    return t
                if st == "error":
                    # Transient control-plane/daemon failure — do NOT
                    # cold-create over a possibly-live sandbox (a spurious
                    # create silently loses the workspace). Fail-closed.
                    raise RuntimeError(
                        f"native: sandbox {ref[:12]} status failed "
                        f"(transient); not creating over a possibly-live "
                        f"sandbox")
                # st == "missing": genuinely gone — best-effort clean the
                # stale ref before creating fresh (no orphan).
                try:
                    await t.destroy()
                except Exception:
                    pass

            t = await self._create_transport(provider)
            self._transport = t
            self.state.sandbox_ref = t.ref
            await self._persist_state()
            return t

    def _reattach_transport(self, provider: str, ref: str):
        """Build a transport bound to an existing sandbox ref (resume path)."""
        from .transport import DaytonaTransport, DockerTransport, ModalTransport
        if provider == "docker":
            return DockerTransport(container_id=ref, workdir=self._cwd)
        if provider == "daytona":
            return DaytonaTransport(sandbox_ref=ref, workdir=self._cwd)
        if provider == "modal":
            return ModalTransport(sandbox_ref=ref, workdir=self._cwd)
        raise RuntimeError(f"native provider {provider!r} not wired")

    async def _create_transport(self, provider: str):
        """Cold-create a fresh sandbox for ``provider`` and return its
        transport. docker: a sleep-infinity container; daytona: a paused-
        capable VM on the session volume."""
        from .transport import DaytonaTransport, DockerTransport, ModalTransport
        if provider == "docker":
            t = DockerTransport(workdir=self._cwd)
            # create() does the workdir mkdir as its readiness step, so no
            # second exec round-trip here (matches daytona/modal below).
            await t.create(image=_native_image(),
                           labels={"agent_sdk_origin": _origin(),
                                   "native_session": self.session_id})
            return t
        if provider == "daytona":
            t = DaytonaTransport(workdir=self._cwd)
            await t.create(root=self._cwd, volume_id=self._volume_ref,
                           subpath=self._subpath)
            return t
        if provider == "modal":
            # Modal is always volume-backed (recreate-on-missing keeps the
            # workspace on the Volume, not the terminated sandbox FS).
            t = ModalTransport(workdir=self._cwd)
            await t.create(volume_ref=self._volume_ref, subpath=self._subpath,
                           root=self._cwd)
            return t
        raise RuntimeError(f"native provider {provider!r} not wired")

    async def _persist_state(self) -> None:
        from api import db as _db
        from api.sandbox.state import serialize
        try:
            await _db.write_sandbox_state(self.session_id, serialize(self.state))
        except Exception:
            log.exception("native: persist sandbox_state failed for %s",
                          self.session_id)

    async def sandbox_exec(self, command: str, timeout: int = 30) -> dict:
        """Back the server's /sandbox/exec route for native sessions. Same
        response shape the supervisor's /v1/exec returns."""
        transport = await self._ensure_sandbox()
        res = await transport.exec(command, cwd=self._cwd,
                                   env=self._sandbox_env or None,
                                   timeout_s=timeout)
        return {
            "stdout": res.stdout,
            "stderr": res.stderr,
            "exit_code": res.exit_code,
            "stdout_truncated": False,
            "stderr_truncated": False,
            "timed_out": res.timed_out,
        }
