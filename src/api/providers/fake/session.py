"""FakeSandboxSession — in-memory sandbox session for unit-testing the pool.

Implements the two abstract lifecycle methods (``start`` / ``stop``) and
overrides ``running`` / ``_liveness_probe`` / ``execute_prompt`` so the pool's
recovery/reaper logic can be driven without real daytona/docker/modal.

Design notes:
  - ``_bootstrap_session()`` (the DB read of session + volume rows) is
    still called by ``start()``.  Tests must therefore insert the real DB
    rows; the fake replaces the *compute* backend only.
  - ``_alive`` tracks whether the fake "compute" is up.  Tests can flip it
    to False to simulate an external supervisor death and then call
    ``pool.get_session`` to trigger recovery.
  - ``_fail_start`` lets tests verify the pool's Bug-A tear-down path
    (start failure → destroy backstop fires).
  - ``_scripted_events`` lets tests inject a specific sequence of SSE
    events so ``execute_prompt`` callers (TurnRunner, recovery tests)
    see deterministic output.
"""
from __future__ import annotations

import uuid
from collections.abc import AsyncIterator
from typing import Any

from api.sandbox.session import BaseSandboxSession
from api.sandbox.state import SandboxState


# Default scripted event sequence that ``execute_prompt`` emits when no
# custom script is provided.  Mirrors the minimal ACP protocol shape the
# real supervisor emits for a successful single-turn prompt.
_DEFAULT_EVENTS: list[dict] = [
    {"type": "text", "text": "hi"},
    {"type": "usage", "usage": {"input_tokens": 1, "output_tokens": 1}},
    {"type": "done", "stop_reason": "end_turn"},
]


class FakeSandboxSession(BaseSandboxSession):
    """Fully in-memory SandboxSession.  No real provisioning; no HTTP.

    Controllability attributes (set in ``__init__``, freely mutated by tests):
      * ``_alive``           — ``running()`` / ``_liveness_probe()`` return value.
      * ``_fail_start``      — when True, ``start()`` raises RuntimeError.
      * ``_scripted_events`` — list of event dicts ``execute_prompt`` yields.
                               None means use the default three-event script.
    """

    # volume_provider intentionally left blank: fake volumes don't carry a
    # provider-specific volume row so ``_bootstrap_session``'s provider
    # cross-check is bypassed (it only fires when volume_provider != "").
    volume_provider = ""
    _default_root = "/tmp"

    def __init__(self, *, session_id: str, state: SandboxState) -> None:
        super().__init__(session_id=session_id, state=state)
        # Controllability knobs
        self._alive: bool = True
        self._fail_start: bool = False
        self._scripted_events: list[dict] | None = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """In-memory boot.  No real provisioning.

        If ``_fail_start`` is True, raise RuntimeError WITHOUT clearing
        ``state.sandbox_ref`` (so the pool's Bug-A destroy backstop can
        fire, mirroring the docker/daytona shape where the sandbox may have
        been acquired before the failure).

        Otherwise: set state fields the pool expects to see after a
        successful start, allocate an ACP session id, and mark alive.
        """
        if self._fail_start:
            # Simulate a partial start: sandbox_ref is set so the pool's
            # destroy backstop (``if getattr(session.state, 'sandbox_ref', None):``)
            # sees a non-null ref and fires ``_safe_destroy_compute``.
            self.state.sandbox_ref = f"fake-{self.session_id}"
            raise RuntimeError("FakeSandboxSession: _fail_start=True")

        # Normal boot — call bootstrap so the session row is read and
        # _agent_id / _subpath / _spawn_env are populated.  The fake
        # volume_provider="" means the provider cross-check is skipped.
        await self._bootstrap_session()

        # Set the fields the pool and callers read.
        self.state.sandbox_ref = f"fake-{self.session_id}"
        self.state.listen_port = 1
        self._supervisor_url = "http://fake.local"
        if self._acp_session_id is None:
            self._acp_session_id = str(uuid.uuid4())
        self._alive = True
        # Warm the liveness oracle so pool.get_session fast-path works.
        self.liveness.observe_chunk()

    async def running(self, *, force_probe: bool = False) -> bool:
        """Liveness oracle backed by in-memory ``_alive`` flag."""
        return self._alive

    async def _liveness_probe(self) -> bool:
        """Probe hook used by the ``Liveness`` oracle when state is unknown
        or ``force_probe=True``.  Returns ``_alive`` directly — no HTTP."""
        return self._alive

    async def stop(self) -> None:
        """Hibernate: mark dead + clear sandbox_ref/listen_port.

        Mirrors docker/modal ``stop``: after stop the compute is gone;
        a subsequent ``start()`` cold-creates fresh.  No snapshot I/O.
        """
        self._alive = False
        self.state.sandbox_ref = None
        self.state.listen_port = None

    async def destroy(self) -> None:
        """Hard-delete. For the fake there is no real compute, so a destroy is
        indistinguishable from a stop — both just mark the in-memory backend
        dead and clear the refs."""
        await self.stop()

    # ------------------------------------------------------------------
    # execute_prompt — scripted event replay
    # ------------------------------------------------------------------

    async def execute_prompt(
        self, message: str, *, rpc_id: str | None = None,
    ) -> AsyncIterator[Any]:
        """Yield scripted events; mirror the observe/broadcast sequence the
        real ``execute_prompt`` does so TurnRunner and the reaper see correct
        liveness bookkeeping.

        The real ``execute_prompt`` calls:
          * ``self.liveness.observe_chunk()``  for each SSE chunk received
          * ``self._broadcast((rpc_id, raw_block))`` for each parsed event
          * ``self.liveness.observe_close()``  after the stream closes

        We replicate that exact sequence for each scripted event.
        """
        if rpc_id is None:
            rpc_id = str(uuid.uuid4())

        events = self._scripted_events if self._scripted_events is not None else _DEFAULT_EVENTS

        for event in events:
            # Simulate receiving an SSE chunk from the supervisor.
            self.liveness.observe_chunk()
            # Broadcast raw block (same shape the real path uses).
            raw_block = f"data: {event}"
            self._broadcast((rpc_id, raw_block))
            yield event
            # Stop iterating on terminal events (mirrors base class).
            if event.get("type") in ("done", "error"):
                break

        # Mirror the observe_close() the real path calls after the stream.
        self.liveness.observe_close()
