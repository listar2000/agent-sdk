"""Single liveness oracle per session.

Note: ``alive`` state has a 30-second freshness window — after that, the
next ``is_alive()`` call probes (matches the doc's "stale alive should
re-verify" intent). The window matters when a sandbox is killed
externally between prompts; without it the cached ``alive`` would let
the next prompt POST to a dead supervisor URL.


Replaces today's scattered liveness signals: ``state._reader_connected``,
``state._reader_alive``, ``_instance_process_alive``, ad-hoc
``_wait_for_health`` calls. The test-7 race becomes inexpressible
because there's only one variable to read or write.

Single writer (the per-prompt SSE drain inside
``SandboxSession.execute_prompt``). Multiple readers (``running()`` on
the session, ``pool.get_session`` fast-path).
"""
from __future__ import annotations

import asyncio
import time
from typing import Awaitable, Callable, Literal

LivenessState = Literal["unknown", "alive", "dead"]


class Liveness:
    """Tracks whether a session's compute is currently capable of serving
    a prompt. Cheap to check; cheap to update.

    States:
      * ``"unknown"`` — nothing observed yet (just-acquired lease, or
        idle for long enough that we have no recent signal).
      * ``"alive"`` — observed a chunk from the supervisor recently.
      * ``"dead"`` — observed an error or close from the supervisor.

    The ``unknown`` state is the only one that triggers a probe; the
    other two are authoritative.
    """

    def __init__(
        self,
        *,
        probe: Callable[[], Awaitable[bool]] | None = None,
        unknown_after_idle_s: float = 2.0,
    ) -> None:
        self._state: LivenessState = "unknown"
        self._last_chunk_at: float | None = None
        self._probe = probe
        self._unknown_after_idle_s = unknown_after_idle_s

    # --- Writer side (called by the SSE drain) ---

    def observe_chunk(self) -> None:
        self._state = "alive"
        self._last_chunk_at = time.monotonic()

    def observe_error(self) -> None:
        self._state = "dead"

    def observe_close(self) -> None:
        # A clean close at end-of-prompt doesn't mean "dead" — we just have
        # no current observation. Drop back to "unknown" so the next
        # caller probes if needed.
        if self._state == "alive":
            self._state = "unknown"

    # --- Reader side (called by callers wanting to use the session) ---

    @property
    def state(self) -> LivenessState:
        return self._state

    def _stale_after_idle(self) -> bool:
        if self._state != "alive" or self._last_chunk_at is None:
            return False
        return (time.monotonic() - self._last_chunk_at) > self._unknown_after_idle_s

    async def is_alive(
        self, *, probe_timeout_s: float = 2.0, force_probe: bool = False,
    ) -> bool:
        """Returns True iff the compute is known-alive at the moment of
        return. State machine:
          * ``alive`` and not stale → True (no I/O) UNLESS ``force_probe``
          * ``dead`` → False (no I/O)
          * ``unknown`` (or stale ``alive``, or ``force_probe``) → run
            probe (if configured) and update state from result

        ``force_probe=True`` is used by ``pool.get_session()`` so a
        message arriving immediately after an external sandbox stop
        observes the dead supervisor (the test 7 race) — the in-memory
        ``alive`` cache from the previous prompt's last chunk would
        otherwise short-circuit and we'd POST to a dead URL.

        Note on transient probe failures: making the probe itself
        retry-tolerant is the responsibility of each provider's
        ``_liveness_probe`` (e.g. Daytona's signed-URL proxy returns
        502 for ~1-2s after a fresh URL — those probes do internal
        retries before returning False). Doing the retry there rather
        than caching positive signals here avoids the test-7 race
        where a recently-alive but now-dead supervisor would be
        wrongly trusted.
        """
        if not force_probe:
            if self._state == "alive" and not self._stale_after_idle():
                return True
            if self._state == "dead":
                return False
        if self._probe is None:
            return self._state == "alive"
        try:
            result = await asyncio.wait_for(self._probe(), timeout=probe_timeout_s)
        except (asyncio.TimeoutError, Exception):
            self._state = "dead"
            return False
        if result:
            self._state = "alive"
            self._last_chunk_at = time.monotonic()
            return True
        self._state = "dead"
        return False
