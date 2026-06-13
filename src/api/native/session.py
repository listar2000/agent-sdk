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
from .loop import (
    NativeAgentSpec,
    heal_dangling_tool_calls,
    initial_messages,
    run_turn,
)
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
        self._pin_modal_workspace_to_volume()

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

    async def running(self) -> bool:
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
                # job — stop the loop, emit cancelled — is done). Heal any
                # assistant tool_calls left unanswered by the interrupted
                # tool-loop BEFORE checkpointing — persisting that dangling
                # shape would 400 every future prompt (durable session wedge).
                heal_dangling_tool_calls(self._messages)
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

    def _pin_modal_workspace_to_volume(self) -> None:
        """Modal is volume-backed: the workspace persists on the ``/v`` Volume
        across modal's routine terminate→recreate (its only "hibernate"). But
        the base cwd fallback is ``/tmp`` (sandbox/session.py — ``cwd or
        recipe.root or "/tmp"``; the server even special-cases ``cwd != "/tmp"``
        to leave root unset), and the bare-sandbox entrypoint REFUSES to symlink
        a critical dir like ``/tmp`` onto the volume. So a DEFAULT modal native
        session would run in ``/tmp`` on the EPHEMERAL sandbox FS and silently
        lose its whole workspace on recreate — defeating modal's volume-backing.
        Pin the default workspace directly onto the volume. A non-critical
        custom cwd is symlinked onto the volume by the entrypoint (left as-is);
        docker/daytona keep their FS across resume, so this is modal-only."""
        if getattr(self.state, "provider", None) == "modal" \
                and (self._cwd or "").rstrip("/") == "/tmp":
            self._cwd = f"/v/{self._subpath.strip('/')}"

    async def _ensure_sandbox(self, *, refresh: bool = False, replace=None):
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
        # ``replace`` names the EXACT transport a caller found dead
        # (SandboxGoneError). Re-provision ONLY if it is still the cached one:
        # if a CONCURRENT recovery (a second tool call, or /sandbox/exec racing
        # a streaming turn — neither is serialized above _provision_lock) has
        # already swapped in a fresh transport, adopt it instead of creating a
        # SECOND sandbox and orphaning one — a leak the reaper/boot-reconcile
        # would otherwise have to reclaim (and a paid-for idle VM until then on
        # daytona/modal). ``refresh=True`` is shorthand for "replace whatever is
        # cached right now" (forced recreate; the tests use it).
        if refresh and replace is None:
            replace = self._transport
        cached = self._transport
        if cached is not None and cached is not replace:
            return cached
        async with self._provision_lock:
            # Re-check under the lock: a concurrent recovery may have already
            # provisioned a fresh transport (a different object than the dead
            # ``replace``) while we waited — adopt it rather than duplicate.
            if self._transport is not None and self._transport is not replace:
                return self._transport
            self._transport = None
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
                    # A 'dead' sandbox maps to "stopped" but resume() can't
                    # restore it. Two failure shapes, both must fall through to
                    # destroy+recreate below — NOT wedge+leak:
                    #  - docker: resume() (`docker start`) is best-effort and
                    #    SWALLOWS the error on a corpse, so it returns and the
                    #    status() re-check catches the still-not-running case.
                    #  - daytona: resume() (start_daytona) RE-RAISES on a failed
                    #    start (502 retries exhausted / readiness timeout /
                    #    stopped→error). Without this guard that raise propagates
                    #    out of _ensure_sandbox, skips the destroy+recreate, and
                    #    wedges the session (every later prompt re-raises) while
                    #    LEAKING the dead VM. Catch it and fall through.
                    try:
                        await t.resume()
                    except Exception:
                        log.warning("native: resume failed for sandbox %s; "
                                    "destroying + recreating", ref[:12])
                    else:
                        # resume returned — keep the warm sandbox if it's up.
                        # daytona's resume is AUTHORITATIVE (blocks-ready +
                        # re-raises), so a clean return already proves running —
                        # skip the redundant control-plane status() round-trip.
                        # docker/modal resume can return on a still-dead sandbox,
                        # so they need the re-check.
                        if (getattr(t, "resume_is_authoritative", False)
                                or await t.status() == "running"):
                            self._transport = t
                            return t
                elif st == "error":
                    # Transient control-plane/daemon failure — do NOT
                    # cold-create over a possibly-live sandbox (a spurious
                    # create silently loses the workspace). Fail-closed.
                    raise RuntimeError(
                        f"native: sandbox {ref[:12]} status failed "
                        f"(transient); not creating over a possibly-live "
                        f"sandbox")
                # st == "missing", OR "stopped" that resume could not restore
                # (dead) — best-effort destroy the stale/dead sandbox before
                # creating fresh, so we never orphan it (no leak), then recreate
                # (volume-backed providers recover the workspace).
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
        """Build a transport bound to an existing sandbox ref (resume path).
        ``env=`` binds the session's sandbox secrets as the transport's default
        exec env so the LOOP's tools (bash/read/write/edit, which pass no env)
        see the same GITHUB_TOKEN/etc that /sandbox/exec injects explicitly —
        parity with the supervisor runtime, where the agent's shell inherits
        spawn_env."""
        from .transport import (
            DaytonaTransport,
            DockerTransport,
            ModalTransport,
            UnixLocalTransport,
        )
        env = self._sandbox_env
        if provider == "docker":
            return DockerTransport(container_id=ref, workdir=self._cwd, env=env)
        if provider == "daytona":
            return DaytonaTransport(sandbox_ref=ref, workdir=self._cwd, env=env)
        if provider == "modal":
            return ModalTransport(sandbox_ref=ref, workdir=self._cwd, env=env)
        if provider == "unix_local":
            return UnixLocalTransport(sandbox_ref=ref, workdir=self._cwd, env=env)
        raise RuntimeError(f"native provider {provider!r} not wired")

    async def _create_transport(self, provider: str):
        """Cold-create a fresh sandbox for ``provider`` and return its
        transport. docker: a sleep-infinity container; daytona: a paused-
        capable VM on the session volume."""
        from .transport import (
            DaytonaTransport,
            DockerTransport,
            ModalTransport,
            UnixLocalTransport,
        )
        env = self._sandbox_env  # default exec env — see _reattach_transport
        if provider == "unix_local":
            # Record-only sandbox on the host: workspace dir + provider-index
            # record, no resident compute (hibernate/resume are no-ops).
            t = UnixLocalTransport(workdir=self._cwd, env=env)
            await t.create(root=self._cwd)
            return t
        if provider == "docker":
            t = DockerTransport(workdir=self._cwd, env=env)
            # create() does the workdir mkdir as its readiness step, so no
            # second exec round-trip here (matches daytona/modal below).
            await t.create(image=_native_image(),
                           labels={"agent_sdk_origin": _origin(),
                                   "native_session": self.session_id})
            return t
        if provider == "daytona":
            t = DaytonaTransport(workdir=self._cwd, env=env)
            await t.create(root=self._cwd, volume_id=self._volume_ref,
                           subpath=self._subpath)
            return t
        if provider == "modal":
            # Modal is always volume-backed (recreate-on-missing keeps the
            # workspace on the Volume, not the terminated sandbox FS).
            t = ModalTransport(workdir=self._cwd, env=env)
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
        response shape the supervisor's /v1/exec returns. Recovers a sandbox
        that died under the live session (SandboxGoneError) ONCE — mirrors the
        loop's _invoke_tool so /sandbox/exec doesn't 500 on a routine modal
        hard-timeout reap or a docker prune."""
        from .transport import SandboxGoneError

        async def _run(t):
            return await t.exec(command, cwd=self._cwd,
                                env=self._sandbox_env or None, timeout_s=timeout)

        # Pin in_flight for the whole exec — /sandbox/exec runs no turn loop, so
        # without this the reaper's in_flight gate (pool._should_reap) doesn't
        # protect a long exec: a session idle past its provider window could be
        # hibernated (docker stop -t 0 / modal terminate) out from under a
        # multi-second command. Same bracket TurnRunner puts around a turn.
        self.liveness.observe_prompt_start()
        try:
            transport = await self._ensure_sandbox()
            try:
                res = await _run(transport)
            except SandboxGoneError:
                res = await _run(await self._ensure_sandbox(replace=transport))
        finally:
            # Exec IS compute activity (the modal-create design notes the pool
            # reaper "tracks exec activity and hibernates") — advance the
            # compute clock like a turn's emit does, so a completed exec buys
            # one idle window instead of leaving the session reap-eligible the
            # moment it returns (back-to-back execs would otherwise pay a
            # resume/cold-create each).
            self.liveness.observe_chunk()
            self.liveness.observe_prompt_end()
        return {
            "stdout": res.stdout,
            "stderr": res.stderr,
            "exit_code": res.exit_code,
            "stdout_truncated": False,
            "stderr_truncated": False,
            "timed_out": res.timed_out,
        }
