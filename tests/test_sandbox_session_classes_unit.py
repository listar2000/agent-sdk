"""Unit tests for the new ``api.sandbox`` classes — verify they're
import-clean and the basic state machine + factory dispatch work.

The classes are NOT yet wired into the server's recovery path; these
tests exercise them in isolation so they're not dead code.
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
            sandbox_id="daytona-abc",
            listen_port=9100,
            recipe=Recipe(agent_type="claude", root="/home/daytona"),
        )
        round_tripped = deserialize(serialize(s))
        assert isinstance(round_tripped, DaytonaSandboxState)
        assert round_tripped.sandbox_id == "daytona-abc"
        assert round_tripped.recipe.agent_type == "claude"

    def test_docker_serialize_then_deserialize(self):
        s = DockerSandboxState(
            sandbox_id="container-xyz",
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
        from api.sandbox.providers.daytona import DaytonaSandboxSession
        from api.sandbox.providers.docker import DockerSandboxSession
        from api.sandbox.providers.modal import ModalSandboxSession
        from api.sandbox.providers.unix_local import UnixLocalSandboxSession

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
    async def test_force_probe_respects_freshness_floor(self):
        """A positive signal observed within ``unknown_after_idle_s`` is
        definitive — re-probing would race the same network we just got
        a successful response on. ``force_probe`` only overrides the
        cached ``alive`` state when the signal is older than the floor
        (test 7 race: external stop between prompts).
        """
        probe_calls = []

        async def _probe() -> bool:
            probe_calls.append(1)
            return True

        live = Liveness(probe=_probe, unknown_after_idle_s=2.0)
        live.observe_chunk()  # fresh positive signal
        await live.is_alive(force_probe=True)
        assert probe_calls == [], (
            "fresh positive signal should short-circuit even under force_probe"
        )

    @pytest.mark.asyncio
    async def test_force_probe_runs_when_signal_is_stale(self):
        """The test 7 race: external stop between prompts makes the
        last-chunk timestamp older than the freshness floor, so
        ``force_probe`` correctly probes and detects the dead supervisor.
        """
        import time as _time
        probe_calls = []

        async def _probe() -> bool:
            probe_calls.append(1)
            return False  # supervisor dead

        live = Liveness(probe=_probe, unknown_after_idle_s=0.001)
        live.observe_chunk()
        await asyncio.sleep(0.01)  # exceed the freshness floor
        result = await live.is_alive(force_probe=True)
        assert probe_calls == [1]
        assert result is False
