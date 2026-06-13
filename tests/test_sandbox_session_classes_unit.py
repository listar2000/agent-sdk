"""Unit tests for ``api.sandbox`` state, factory dispatch, and Liveness.

Fast feedback for the bits the live golden suite exercises end-to-end:
Pydantic round-trip of the discriminated SandboxState union, factory
dispatch from state to concrete SandboxSession class, and the Liveness
probe-runner contract (always-probe semantics for the external-supervisor-kill
race characterized in the recovery tests).
"""
import asyncio
import os
import sys

import pytest


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
# liveness.py — probe runner + reaper signals (no cached state machine: every
# is_alive() probes, so a stale-positive verdict — the "test 7" race, external
# supervisor kill between prompts — is inexpressible by construction)
# ---------------------------------------------------------------------------

class TestLiveness:
    @pytest.mark.asyncio
    async def test_is_alive_always_probes(self):
        probe_calls = []

        async def _probe() -> bool:
            probe_calls.append(1)
            return True

        live = Liveness(probe=_probe)
        live.observe_chunk()           # compute activity must NOT cache a verdict
        assert await live.is_alive() is True
        assert await live.is_alive() is True
        assert probe_calls == [1, 1], "every is_alive() must hit the probe"

    @pytest.mark.asyncio
    async def test_is_alive_false_on_probe_failure_or_timeout(self):
        async def _dead() -> bool:
            return False

        async def _hang() -> bool:
            await asyncio.sleep(60)
            return True

        assert await Liveness(probe=_dead).is_alive() is False
        assert await Liveness(probe=_hang).is_alive(probe_timeout_s=0.05) is False

    @pytest.mark.asyncio
    async def test_no_probe_reports_alive(self):
        # No probe configured = nothing claims the compute is dead; sessions
        # that can't be probed (native overrides running() anyway) must not
        # trigger pool recovery.
        assert await Liveness().is_alive() is True

    def test_compute_clock_only_moves_on_chunks(self):
        live = Liveness()
        assert live._last_compute_at is None
        live.observe_chunk()
        assert live._last_compute_at is not None

    def test_in_flight_counter_reentrant_and_floored(self):
        live = Liveness()
        assert live.in_flight is False
        live.observe_prompt_start(); live.observe_prompt_start()
        assert live.in_flight is True
        live.observe_prompt_end()
        assert live.in_flight is True
        live.observe_prompt_end(); live.observe_prompt_end()   # extra end: floored
        assert live.in_flight is False


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

    async def running(self) -> bool:
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
    async def test_iterate_subscriber_hot_path_and_heartbeat(self, monkeypatch):
        """The drain drains queued events with get_nowait (no per-event timer)
        AND still emits a heartbeat after a full idle interval, then keeps
        delivering — the contract the streaming fan-out optimization preserves."""
        from api.sandbox import session as sess_mod
        from api.sandbox.session import _END, _HEARTBEAT

        # tiny interval so the idle-heartbeat path is fast to exercise
        monkeypatch.setattr(sess_mod, "_HEARTBEAT_INTERVAL_S", 0.03)
        a = _MiniSession(session_id="s", state=ModalSandboxState(recipe=Recipe()))
        sid, q = a.register_subscriber()

        # a burst already queued — drained via the get_nowait hot path, in order
        for i in range(5):
            q.put_nowait(("rpc", f"e{i}"))
        agen = a.iterate_subscriber(sid, q)
        got = [await agen.__anext__() for _ in range(5)]
        assert got == [("rpc", f"e{i}") for i in range(5)]

        # queue now empty → the next pull arms the timer and times out into a
        # heartbeat (proves the idle path still fires; a busy-spin would hang)
        assert await agen.__anext__() is _HEARTBEAT

        # real events still flow after a heartbeat
        q.put_nowait(("rpc", "after"))
        assert await agen.__anext__() == ("rpc", "after")

        # _END terminates and cleanup runs
        q.put_nowait(_END)
        with pytest.raises(StopAsyncIteration):
            await agen.__anext__()
        assert sid not in a._subscribers

    @pytest.mark.asyncio
    async def test_subscriber_cleanup_no_leak_under_churn(self):
        """Subscribers must not accumulate in ``_subscribers`` under churn —
        both the normal drain-to-_END path AND the realistic client-disconnect
        (the consumer's ``async for`` cancelled) path must pop their entry.
        Otherwise a busy session slowly leaks memory and every _broadcast
        iterates a growing dead list. The streaming-fan-out path all providers
        share — covers the SSE-drain optimization's cleanup contract."""
        from api.sandbox.session import _END

        a = _MiniSession(session_id="s", state=ModalSandboxState(recipe=Recipe()))

        # normal completion: drain to _END
        for i in range(20):
            sid, q = a.register_subscriber()
            a._broadcast((f"r{i}", "x"))
            q.put_nowait(_END)
            async for _ in a.iterate_subscriber(sid, q):
                pass
        assert a._subscribers == {}, "drain-to-_END leaked a subscriber"

        # realistic disconnect: the consumer task is cancelled mid-iterate
        for i in range(20):
            sid, q = a.register_subscriber()

            async def _consume(sid=sid, q=q):
                async for _ in a.iterate_subscriber(sid, q):
                    pass

            t = asyncio.create_task(_consume())
            await asyncio.sleep(0)           # block on q.get()
            t.cancel()
            try:
                await t
            except asyncio.CancelledError:
                pass
        assert a._subscribers == {}, "cancelled-consumer leaked a subscriber"

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
