"""Phase 2 sub-task 1: pin the new sandbox subpackage's contracts.

Covers:
  * Pydantic state round-trip (JSONB ↔ typed)
  * Discriminator dispatch (each ``type`` value → correct subclass)
  * Liveness state machine (alive/dead/unknown + probe + idle staleness)
  * Subscriber fan-out on BaseSandboxSession (multi-subscriber, slow drop)
  * SessionPool warm-path (cached + alive → no factory call)
  * SessionPool cold-path (no cached → factory + start)
  * SessionPool stale-path (cached + dead → shutdown old + start new)
  * SessionPool release (snapshot + shutdown, idempotent)

All in-memory; no DB, no network.
"""
from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from typing import Any

import pytest

from api.sandbox import (
    BaseSandboxSession,
    DaytonaSandboxState,
    DockerSandboxState,
    Liveness,
    Recipe,
    SessionPool,
    UnknownSandboxState,
    deserialize,
    serialize,
)


# ---------------------------------------------------------------------------
# State models
# ---------------------------------------------------------------------------


def test_deserialize_none_yields_unknown_state():
    s = deserialize(None)
    assert isinstance(s, UnknownSandboxState)
    assert s.sandbox_id is None
    assert s.snapshot_version == 0


def test_deserialize_dispatches_on_type_discriminator():
    payload = {
        "type": "daytona",
        "sandbox_id": "dt-123",
        "listen_port": 9100,
        "snapshot_path": "/vol/snap.tar",
        "snapshot_version": 4,
        "recipe": {
            "dockerfile": "/df",
            "shared_mounts": ["projects"],
            "root": "/home/daytona",
            "agent_type": "claude",
            "pre_start_commands": ["npm install"],
        },
    }
    s = deserialize(payload)
    assert isinstance(s, DaytonaSandboxState)
    assert s.sandbox_id == "dt-123"
    assert s.recipe.shared_mounts == ["projects"]


def test_deserialize_legacy_local_alias_maps_to_unix_local():
    s = deserialize({"type": "local", "sandbox_id": "1234"})
    assert s.type == "unix_local"


def test_deserialize_round_trip_via_serialize():
    s = DaytonaSandboxState(
        sandbox_id="dt-x", listen_port=9100,
        recipe=Recipe(dockerfile="/df", shared_mounts=["a", "b"], root="/r"),
    )
    s2 = deserialize(serialize(s))
    assert isinstance(s2, DaytonaSandboxState)
    assert s2.sandbox_id == "dt-x"
    assert s2.recipe.shared_mounts == ["a", "b"]


def test_deserialize_unknown_type_field_yields_unknown_state():
    s = deserialize({"type": ""})
    assert isinstance(s, UnknownSandboxState)


def test_unknown_state_cannot_carry_a_sandbox_id():
    # The point of UnknownSandboxState is to mean "no compute yet" —
    # sandbox_id MUST be None on this variant.
    s = UnknownSandboxState()
    assert s.sandbox_id is None


# ---------------------------------------------------------------------------
# Liveness oracle
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_liveness_starts_unknown_and_probes():
    probed = {"count": 0}

    async def probe():
        probed["count"] += 1
        return True

    live = Liveness(probe=probe)
    assert live.state == "unknown"
    assert await live.is_alive()
    assert live.state == "alive"
    assert probed["count"] == 1


@pytest.mark.asyncio
async def test_liveness_alive_skips_probe():
    probed = {"count": 0}

    async def probe():
        probed["count"] += 1
        return True

    live = Liveness(probe=probe)
    live.observe_chunk()
    assert await live.is_alive()
    assert probed["count"] == 0  # observed-alive short-circuits


@pytest.mark.asyncio
async def test_liveness_dead_short_circuits_to_false():
    async def probe():
        raise AssertionError("must not probe")

    live = Liveness(probe=probe)
    live.observe_error()
    assert not await live.is_alive()


@pytest.mark.asyncio
async def test_liveness_unknown_with_failing_probe_marks_dead():
    async def probe():
        return False

    live = Liveness(probe=probe)
    assert not await live.is_alive()
    assert live.state == "dead"


@pytest.mark.asyncio
async def test_liveness_probe_timeout_marks_dead():
    async def slow_probe():
        await asyncio.sleep(10)
        return True

    live = Liveness(probe=slow_probe)
    assert not await live.is_alive(probe_timeout_s=0.05)
    assert live.state == "dead"


@pytest.mark.asyncio
async def test_liveness_observe_close_drops_to_unknown_not_dead():
    """A clean stream close at end-of-prompt isn't a death signal."""
    live = Liveness()
    live.observe_chunk()
    assert live.state == "alive"
    live.observe_close()
    assert live.state == "unknown"


# ---------------------------------------------------------------------------
# BaseSandboxSession subscriber fan-out
# ---------------------------------------------------------------------------


class _FakeSession(BaseSandboxSession):
    """Minimal concrete session for testing the base-class fan-out."""

    async def start(self) -> None: ...
    async def running(self) -> bool: return True
    async def execute_prompt(self, message: str) -> AsyncIterator[Any]:
        yield {}
    async def stop(self) -> None: ...
    async def shutdown(self) -> None:
        self._close_subscribers()


@pytest.mark.asyncio
async def test_subscribe_receives_broadcast_events():
    session = _FakeSession(session_id="s1", state=UnknownSandboxState())
    received = []

    async def consume():
        async for ev in session.subscribe():
            received.append(ev)

    task = asyncio.create_task(consume())
    await asyncio.sleep(0)  # let subscribe register itself
    session._broadcast({"id": 1})
    session._broadcast({"id": 2})
    await asyncio.sleep(0)
    await session.shutdown()  # closes subscribers
    await task
    assert received == [{"id": 1}, {"id": 2}]


@pytest.mark.asyncio
async def test_multi_subscriber_each_gets_every_event():
    session = _FakeSession(session_id="s1", state=UnknownSandboxState())
    a, b = [], []

    async def consume(buf):
        async for ev in session.subscribe():
            buf.append(ev)

    ta = asyncio.create_task(consume(a))
    tb = asyncio.create_task(consume(b))
    await asyncio.sleep(0)
    session._broadcast({"id": "x"})
    session._broadcast({"id": "y"})
    await asyncio.sleep(0)
    await session.shutdown()
    await asyncio.gather(ta, tb)
    assert a == [{"id": "x"}, {"id": "y"}]
    assert b == [{"id": "x"}, {"id": "y"}]


@pytest.mark.asyncio
async def test_slow_subscriber_drops_events_does_not_block_others():
    """Per docs §15.5 — slow-subscriber drop must not backpressure other
    subscribers or the source."""
    session = _FakeSession(session_id="s1", state=UnknownSandboxState())
    # Fast subscriber drains immediately.
    fast: list[Any] = []

    async def fast_consume():
        async for ev in session.subscribe():
            fast.append(ev)

    ft = asyncio.create_task(fast_consume())
    await asyncio.sleep(0)

    # Slow subscriber: never reads. Force its queue to fill by setting a
    # tiny maxsize.
    slow_q: asyncio.Queue[Any] = asyncio.Queue(maxsize=2)
    session._subscribers["slow"] = slow_q

    for i in range(50):
        session._broadcast({"i": i})

    await asyncio.sleep(0)
    await session.shutdown()
    await ft

    # Fast subscriber gets every event.
    assert fast == [{"i": i} for i in range(50)]
    # Slow subscriber's queue is bounded at 2; rest were dropped.
    assert slow_q.qsize() <= 2


# ---------------------------------------------------------------------------
# SessionPool
# ---------------------------------------------------------------------------


def _mk_pool(*, started_count: list[int]):
    """Build a pool with a counting factory + in-memory state store."""
    store: dict[str, dict] = {}

    class _PoolSession(BaseSandboxSession):
        def __init__(self, *, session_id, state, alive_ref):
            super().__init__(session_id=session_id, state=state)
            self._alive = alive_ref  # mutable list[bool] so tests can flip

        async def start(self):
            started_count.append(self.session_id)
            # Simulate provisioning: fill in a sandbox_id if missing.
            if isinstance(self.state, DaytonaSandboxState) and self.state.sandbox_id is None:
                self.state.sandbox_id = "dt-new"
            self._alive[0] = True

        async def running(self):
            return self._alive[0]

        async def execute_prompt(self, message):
            yield {}

        async def stop(self):
            self.state.snapshot_path = "/vol/snap.tar"
            self.state.snapshot_version += 1
            self._alive[0] = False

        async def shutdown(self):
            self._alive[0] = False
            self._close_subscribers()

    alive_ref = [False]

    def factory(session_id, state):
        if not isinstance(state, DaytonaSandboxState):
            state = DaytonaSandboxState(recipe=state.recipe)
        return _PoolSession(session_id=session_id, state=state, alive_ref=alive_ref)

    async def load_state(session_id):
        return store.get(session_id)

    async def save_state(session_id, payload):
        store[session_id] = payload

    pool = SessionPool(factory=factory, load_state=load_state, save_state=save_state)
    return pool, store, alive_ref


@pytest.mark.asyncio
async def test_pool_cold_path_starts_session_and_persists_state():
    started: list[str] = []
    pool, store, _ = _mk_pool(started_count=started)
    s = await pool.get_session("sess-1")
    assert started == ["sess-1"]
    assert isinstance(s.state, DaytonaSandboxState)
    assert s.state.sandbox_id == "dt-new"
    # save_state was called with the post-start snapshot.
    assert store["sess-1"]["sandbox_id"] == "dt-new"


@pytest.mark.asyncio
async def test_pool_warm_path_reuses_session_no_second_start():
    started: list[str] = []
    pool, _, _ = _mk_pool(started_count=started)
    s1 = await pool.get_session("sess-1")
    s2 = await pool.get_session("sess-1")
    assert s1 is s2
    assert started == ["sess-1"]  # only one start


@pytest.mark.asyncio
async def test_pool_stale_path_starts_new_session_when_cached_dead():
    started: list[str] = []
    pool, _, alive_ref = _mk_pool(started_count=started)
    s1 = await pool.get_session("sess-1")
    # Simulate compute death
    alive_ref[0] = False
    s2 = await pool.get_session("sess-1")
    # Same session_id; underlying alive flag flipped back via start
    assert started == ["sess-1", "sess-1"]
    assert s2 is not s1


@pytest.mark.asyncio
async def test_pool_concurrent_get_session_serialises_via_lock():
    """Two concurrent get_session for same id → only one factory.start call."""
    started: list[str] = []
    pool, _, _ = _mk_pool(started_count=started)
    results = await asyncio.gather(
        pool.get_session("sess-1"),
        pool.get_session("sess-1"),
        pool.get_session("sess-1"),
    )
    assert all(r is results[0] for r in results)
    assert started == ["sess-1"]  # only one start across 3 concurrent callers


@pytest.mark.asyncio
async def test_pool_release_snapshots_then_shuts_down():
    started: list[str] = []
    pool, store, alive_ref = _mk_pool(started_count=started)
    s = await pool.get_session("sess-1")
    assert pool.has_active("sess-1")

    await pool.release("sess-1")
    assert not pool.has_active("sess-1")
    # Snapshot path got persisted.
    assert store["sess-1"]["snapshot_path"] == "/vol/snap.tar"
    assert store["sess-1"]["snapshot_version"] == 1
    # Underlying compute marked dead.
    assert alive_ref[0] is False


@pytest.mark.asyncio
async def test_pool_release_is_idempotent_when_no_active_session():
    pool, _, _ = _mk_pool(started_count=[])
    await pool.release("never-existed")  # no exception
