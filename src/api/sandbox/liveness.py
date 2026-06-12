"""Per-session liveness: a probe runner plus the two signals the reaper reads.

This used to be a three-state machine (unknown/alive/dead) with a staleness
window, viewer-activity refreshes, and error/close transitions. None of it
decided anything: the only production decision point — ``pool.get_session``'s
warm path — always forced a probe past the cache, exactly because a cached
``alive`` going stale between prompts (the "test-7 race": external sandbox
stop right after a chunk) could not be trusted. With no cached verdict there
is nothing to go stale, which is the invariant the force-probe flag existed
to restore. What remains:

* ``is_alive()`` — run the injected provider probe, bounded by a timeout.
  Every call probes; no caching. A session without a probe reports alive
  (nothing claims otherwise — native sessions override ``running()`` anyway).
* the COMPUTE clock (``_last_compute_at``) — advanced only by
  ``observe_chunk`` (real agent work: prompt chunks, attach round-trips,
  exec activity). The idle reaper keys off this, so viewer traffic (open
  /events, status polls, file browsing) never pins idle compute.
* the in-flight prompt counter — the reaper never hibernates a session
  with a prompt (or /sandbox/exec) in flight, even if the compute clock
  goes stale during a chunk-silent multi-minute tool call.
"""
from __future__ import annotations

import asyncio
import time
from typing import Awaitable, Callable


class Liveness:
    def __init__(
        self,
        *,
        probe: Callable[[], Awaitable[bool]] | None = None,
    ) -> None:
        # Compute-only activity clock — see module docstring. Read by
        # ``pool._should_reap`` and the admin/status views.
        self._last_compute_at: float | None = None
        # Reentrant in-flight counter; >0 while an ``execute_prompt`` drive
        # or a ``/sandbox/exec`` is running (including across a mid-prompt
        # recovery swap). Bumped by ``observe_prompt_start/end``.
        self._in_flight: int = 0
        self._probe = probe

    # --- writers ------------------------------------------------------------

    def observe_chunk(self) -> None:
        """Record real compute work (prompt chunk / attach / exec)."""
        self._last_compute_at = time.monotonic()

    def observe_prompt_start(self) -> None:
        self._in_flight += 1

    def observe_prompt_end(self) -> None:
        # Floored at 0 so an unbalanced end (e.g. after a recovery swap)
        # can't drive the counter negative.
        self._in_flight = max(0, self._in_flight - 1)

    @property
    def in_flight(self) -> bool:
        return self._in_flight > 0

    # --- reader -------------------------------------------------------------

    async def is_alive(self, *, probe_timeout_s: float = 2.0) -> bool:
        """Probe the compute NOW; True iff it answered within the timeout.

        Transient-failure tolerance belongs in each provider's probe (e.g.
        daytona's signed-URL proxy 502s for ~1-2s after a fresh URL — its
        probe retries internally). Caching positive results here instead
        would reintroduce the stale-alive race this class exists to avoid.
        """
        if self._probe is None:
            return True
        try:
            return bool(await asyncio.wait_for(
                self._probe(), timeout=probe_timeout_s))
        except (asyncio.TimeoutError, Exception):
            return False
