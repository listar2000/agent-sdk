"""Unit tests for ``api.sandbox`` state, factory dispatch, and Liveness.

Fast feedback for the bits the live golden suite exercises end-to-end:
Pydantic round-trip of the discriminated SandboxState union, factory
dispatch from state to concrete SandboxSession class, and the Liveness
state machine (including ``force_probe`` for the external-supervisor-kill
race characterized in the recovery tests).
"""
import asyncio
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))


from api.sandbox import (
    BaseSandboxSession,
    DaytonaSandboxState,
    DockerSandboxState,
    Liveness,
    ModalSandboxState,
    Recipe,
    UnixLocalSandboxState,
    UnknownSandboxState,
    deserialize,
    make_session,
    serialize,
)


# ---------------------------------------------------------------------------
# state.py — Pydantic round-trip
# ---------------------------------------------------------------------------

class TestSandboxStateRoundTrip:
    def test_daytona_serialize_then_deserialize(self):
        s = DaytonaSandboxState(
            sandbox_ref="daytona-abc",
            listen_port=9100,
            recipe=Recipe(agent_type="claude", root="/home/daytona"),
        )
        round_tripped = deserialize(serialize(s))
        assert isinstance(round_tripped, DaytonaSandboxState)
        assert round_tripped.sandbox_ref == "daytona-abc"
        assert round_tripped.recipe.agent_type == "claude"

    def test_docker_serialize_then_deserialize(self):
        s = DockerSandboxState(
            sandbox_ref="container-xyz",
            listen_port=2497,
            recipe=Recipe(agent_type="codex", root="/home/agent",
                          shared_mounts=["/srv:/srv:ro"]),
        )
        out = deserialize(serialize(s))
        assert isinstance(out, DockerSandboxState)
        assert out.recipe.shared_mounts == ["/srv:/srv:ro"]

    def test_unknown_state_when_payload_missing_or_garbled(self):
        for payload in (None, {}, {"type": ""}, {"type": "not-a-real-provider"}):
            out = deserialize(payload)
            assert isinstance(out, UnknownSandboxState)


# ---------------------------------------------------------------------------
# factory.py — discriminated dispatch
# ---------------------------------------------------------------------------

class TestFactoryDispatch:
    def test_each_state_type_maps_to_its_concrete_class(self):
        from api.providers.daytona.session import DaytonaSandboxSession
        from api.providers.docker.session import DockerSandboxSession
        from api.providers.modal.session import ModalSandboxSession
        from api.providers.unix_local.session import UnixLocalSandboxSession

        cases = [
            (DaytonaSandboxState(recipe=Recipe()), DaytonaSandboxSession),
            (DockerSandboxState(recipe=Recipe()), DockerSandboxSession),
            (UnixLocalSandboxState(recipe=Recipe()), UnixLocalSandboxSession),
            (ModalSandboxState(recipe=Recipe()), ModalSandboxSession),
        ]
        for state, expected_cls in cases:
            session = make_session("sess-x", state)
            assert isinstance(session, expected_cls)

    def test_unknown_state_falls_back_to_default(self):
        # Unknown coerces through the default-registered provider so a
        # not-yet-provisioned session still gets a usable SandboxSession.
        session = make_session("sess-y", UnknownSandboxState(recipe=Recipe()))
        assert isinstance(session, BaseSandboxSession)


# ---------------------------------------------------------------------------
# liveness.py — state machine
# ---------------------------------------------------------------------------

class TestLiveness:
    def test_initial_state_is_unknown(self):
        live = Liveness()
        assert live.state == "unknown"

    def test_observe_chunk_marks_alive(self):
        live = Liveness()
        live.observe_chunk()
        assert live.state == "alive"

    def test_observe_close_drops_alive_to_unknown(self):
        live = Liveness()
        live.observe_chunk()
        live.observe_close()
        assert live.state == "unknown"

    def test_observe_error_marks_dead(self):
        live = Liveness()
        live.observe_chunk()
        live.observe_error()
        assert live.state == "dead"

    def test_observe_activity_does_not_revive_dead_session(self):
        live = Liveness()
        live.observe_error()
        live.observe_activity()
        assert live.state == "dead"

    @pytest.mark.asyncio
    async def test_is_alive_returns_true_when_recently_observed(self):
        live = Liveness()
        live.observe_chunk()
        assert await live.is_alive() is True

    @pytest.mark.asyncio
    async def test_is_alive_returns_false_when_dead(self):
        live = Liveness()
        live.observe_error()
        assert await live.is_alive() is False

    @pytest.mark.asyncio
    async def test_is_alive_probes_when_unknown(self):
        probe_calls = []

        async def _probe() -> bool:
            probe_calls.append(1)
            return True

        live = Liveness(probe=_probe)
        result = await live.is_alive()
        assert result is True
        assert probe_calls == [1]

    @pytest.mark.asyncio
    async def test_force_probe_runs_even_when_alive(self):
        """``force_probe=True`` must override the cached ``alive`` state.
        The "test 7" race (external supervisor kill between prompts —
        see test_persistent_sse_supervisor_killed_immediate_message) makes
        the cached signal stale-positive: supervisor was alive when we
        last observed a chunk, but is dead now. force_probe MUST hit
        the probe to detect this. Tolerance for transient probe failures
        (e.g. Daytona's signed-URL 502 propagation) is the responsibility
        of the per-provider _liveness_probe (bounded retry there), not
        this oracle's caching policy.
        """
        probe_calls = []

        async def _probe() -> bool:
            probe_calls.append(1)
            return True

        live = Liveness(probe=_probe)
        live.observe_chunk()  # state = alive, no probe needed
        await live.is_alive(force_probe=True)
        assert probe_calls == [1], "force_probe should bypass the alive cache"


# ---------------------------------------------------------------------------
# pool.py — idle reaper provider thresholds
# ---------------------------------------------------------------------------

class _FakePoolSession:
    def __init__(self, state):
        self.state = state
        self.liveness = Liveness()
        self._subscribers = {}


class TestSessionPoolReaper:
    @pytest.mark.asyncio
    async def test_reap_idle_uses_provider_specific_threshold(self, monkeypatch):
        from api.sandbox.pool import SessionPool

        pool = SessionPool(factory=lambda _sid, _state: None)
        daytona = _FakePoolSession(DaytonaSandboxState(recipe=Recipe()))
        modal = _FakePoolSession(ModalSandboxState(recipe=Recipe()))
        for sess in (daytona, modal):
            # Seed the COMPUTE clock (what the reaper reads). observe_chunk
            # sets both clocks; age _last_compute_at to make it stale.
            sess.liveness.observe_chunk()
            sess.liveness._last_compute_at -= 10
        pool._active = {"daytona": daytona, "modal": modal}

        released = []

        async def _release(session_id):
            released.append(session_id)

        monkeypatch.setattr(pool, "release", _release)

        count = await pool.reap_idle(
            5,
            provider_idle_s={"modal": 60},
        )

        assert count == 1
        assert released == ["daytona"]

    @pytest.mark.asyncio
    async def test_reap_idle_reaps_idle_subscriber_but_not_inflight(self, monkeypatch):
        """Subscriber/compute de-conflation contract:

          * an idle session with an open /events subscriber (no prompt in
            flight, stale compute clock) IS reaped — subscriber presence no
            longer pins compute (the Bug B fix); and
          * a session with a prompt in flight is NOT reaped even with a
            stale compute clock (the long chunk-silent command case).
        """
        from api.sandbox.pool import SessionPool

        pool = SessionPool(factory=lambda _sid, _state: None)

        # (a) idle + open subscriber + stale compute -> MUST be reaped.
        watched = _FakePoolSession(ModalSandboxState(recipe=Recipe()))
        watched.liveness.observe_chunk()
        watched.liveness._last_compute_at -= 10
        watched._subscribers["ui"] = asyncio.Queue()

        # (b) prompt in flight + stale compute -> MUST NOT be reaped.
        busy = _FakePoolSession(ModalSandboxState(recipe=Recipe()))
        busy.liveness.observe_chunk()
        busy.liveness._last_compute_at -= 10
        busy.liveness.observe_prompt_start()

        pool._active = {"watched": watched, "busy": busy}

        released = []

        async def _release(session_id):
            released.append(session_id)

        monkeypatch.setattr(pool, "release", _release)

        count = await pool.reap_idle(5)

        assert count == 1
        assert released == ["watched"]


class _MiniSession(BaseSandboxSession):
    """Concrete ``BaseSandboxSession`` with no real compute — exercises the
    in-memory subscriber fan-out + recovery hand-off cleanup without a
    sandbox. ``running()`` reports dead so ``pool.get_session`` always
    takes the hand-off branch."""

    volume_provider = "test"

    async def start(self) -> None:
        pass

    async def running(self, *, force_probe: bool = False) -> bool:
        return False

    async def execute_prompt(self, *args, **kwargs):
        if False:  # pragma: no cover — make this an async generator
            yield

    async def stop(self) -> None:
        pass

    async def shutdown(self) -> None:
        self._close_subscribers()


class TestSubscriberHandoffCleanup:
    """Regression for the zombie-subscriber leak: when a session dies
    mid-prompt and its SSE subscribers are handed off to a replacement,
    ``iterate_subscriber``'s cleanup must pop from the REPLACEMENT (the
    current owner), not the original session it was bound to. A leaked
    entry pins the replacement against ``reap_idle`` forever, leaking the
    backing compute (the 'hibernated' webhook + provider stop never fire)."""

    @pytest.mark.asyncio
    async def test_iterate_subscriber_cleanup_targets_current_owner(self):
        from api.sandbox.session import _END

        a = _MiniSession(session_id="s", state=ModalSandboxState(recipe=Recipe()))
        b = _MiniSession(session_id="s", state=ModalSandboxState(recipe=Recipe()))

        sid, q = a.register_subscriber()
        assert sid in a._subscribers

        # Drain on A; the generator's ``self`` is permanently A.
        agen = a.iterate_subscriber(sid, q)
        step = asyncio.ensure_future(agen.__anext__())
        await asyncio.sleep(0)  # run body to the q.get() await -> captures sub

        # Simulate the pool cold-recovery hand-off A -> B.
        handed = dict(a._subscribers)
        a._subscribers.clear()
        for sub in handed.values():
            sub.owner = b
        b._subscribers.update(handed)
        assert sid in b._subscribers and sid not in a._subscribers

        # End the stream; the generator returns and runs its finally.
        q.put_nowait(_END)
        with pytest.raises(StopAsyncIteration):
            await step

        # Cleanup followed the queue to B — no zombie on either session.
        assert sid not in b._subscribers, "zombie subscriber left on replacement"
        assert sid not in a._subscribers

    @pytest.mark.asyncio
    async def test_pool_handoff_then_drain_lets_reaper_reclaim(self, monkeypatch):
        from api.sandbox.pool import SessionPool
        from api.sandbox.session import _END
        from api import db as db_mod

        pool = SessionPool(factory=lambda sid, state: _MiniSession(
            session_id=sid, state=state,
        ))

        async def _noop(*a, **k):
            return None

        monkeypatch.setattr(pool, "_publish_state", _noop)
        monkeypatch.setattr(db_mod, "write_sandbox_state", _noop)

        # Seed a cached (soon-to-be-dead) session with a live subscriber.
        cached = _MiniSession(
            session_id="sess", state=ModalSandboxState(recipe=Recipe()),
        )
        pool._active["sess"] = cached
        sid, q = cached.register_subscriber()

        # Consumer draining the cached session (generator bound to cached).
        agen = cached.iterate_subscriber(sid, q)
        step = asyncio.ensure_future(agen.__anext__())
        await asyncio.sleep(0)

        # Real hand-off: cached.running() -> False triggers the replacement.
        replacement = await pool.get_session(
            "sess", initial_state=ModalSandboxState(recipe=Recipe()),
        )
        assert replacement is not cached
        assert sid in replacement._subscribers
        assert sid not in cached._subscribers
        # The pool rebound the owner — not just moved the queue.
        assert replacement._subscribers[sid].owner is replacement

        # Consumer ends -> finally cleans the REPLACEMENT (the fix).
        q.put_nowait(_END)
        with pytest.raises(StopAsyncIteration):
            await step
        assert sid not in replacement._subscribers

        # Symptom gone: with an empty _subscribers the idle reaper reclaims it.
        released = []

        async def _release(session_id):
            released.append(session_id)

        monkeypatch.setattr(pool, "release", _release)
        # Seed the COMPUTE clock (what the reaper reads) and age it.
        replacement.liveness.observe_chunk()
        replacement.liveness._last_compute_at -= 10_000
        count = await pool.reap_idle(5)
        assert count == 1
        assert released == ["sess"]

        await asyncio.sleep(0)  # let the background _safe_shutdown(cached) settle
