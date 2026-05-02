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
