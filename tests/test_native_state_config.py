"""P0-C gate: NativeSandboxState round-trip, factory dispatch contract,
AgentConfig native passthrough, and native_transcripts accessors.

The DB tests need a reachable Postgres at DATABASE_URL (the launch script's
conda instance on :5433 works); they skip cleanly when absent so the rest of
the suite stays hermetic.
"""

from __future__ import annotations

import os
import sys
import uuid

import pytest

# Checkpoint round-trip tests hit the shared test postgres — serialize
# onto the single DB xdist worker (pyproject --dist loadgroup).
pytestmark = pytest.mark.xdist_group("db")


from api.models import AgentConfig  # noqa: E402
from api.sandbox import factory  # noqa: E402
from api.sandbox.state import (  # noqa: E402
    NativeSandboxState,
    UnknownSandboxState,
    _KNOWN_TYPES,
    deserialize,
    serialize,
)


# ── AgentConfig passthrough ─────────────────────────────────────────────────

def test_agent_config_native_passthrough_survives_from_dict():
    c = AgentConfig.from_dict({
        "agent_type": "native",
        "model": "openrouter/openai/gpt-4o-mini",
        "native": {"instructions": "be brief", "max_turns": 7,
                   "tool_names": ["bash"]},
        "unknown_field": "dropped",
    })
    assert c.native == {"instructions": "be brief", "max_turns": 7,
                        "tool_names": ["bash"]}
    assert "native" in c.to_dict()
    assert not hasattr(c, "unknown_field")


def test_agent_config_native_absent_stays_none():
    c = AgentConfig.from_dict({"agent_type": "claude", "model": "haiku"})
    assert c.native is None
    assert "native" not in c.to_dict()


def test_native_config_knobs_flow_end_to_end_through_agentconfig():
    """The native dict is opaque on purpose so loop knobs need no AgentConfig
    migration — but only if AgentConfig preserves arbitrary keys through BOTH
    from_dict AND the to_dict round-trip (the DB serialize path), and from_config
    then reads them. Pins that for the runtime-control knobs (incl. the ones
    added without an AgentConfig change): a regression here silently reverts a
    configured agent to defaults."""
    from api.native.loop import NativeAgentSpec

    native = {"max_concurrent_tools": 16, "num_retries": 5, "max_turns": 7,
              "instructions": "be brief", "tool_names": ["bash"]}
    cfg = AgentConfig.from_dict({
        "agent_type": "native",
        "model": "openrouter/anthropic/claude-3.5-sonnet",
        "native": dict(native),
    })
    # survives the DB serialize round-trip unchanged
    back = AgentConfig.from_dict(cfg.to_dict())
    assert back.native == native

    spec = NativeAgentSpec.from_config(model=cfg.model, native=back.native)
    assert spec.max_concurrent_tools == 16
    assert spec.num_retries == 5
    assert spec.max_turns == 7
    assert spec.model == "openrouter/anthropic/claude-3.5-sonnet"


# ── State variant ───────────────────────────────────────────────────────────

def test_native_state_serialize_roundtrip():
    s = NativeSandboxState(provider="daytona", recipe={"agent_type": "native"})
    back = deserialize(serialize(s))
    assert type(back) is NativeSandboxState
    assert back.provider == "daytona"
    assert back.sandbox_ref is None and back.listen_port is None
    assert back.recipe.agent_type == "native"


def test_native_state_with_ref_roundtrip():
    s = NativeSandboxState(provider="docker", sandbox_ref="cid-123")
    back = deserialize(serialize(s))
    assert back.sandbox_ref == "cid-123"


def test_native_in_known_types_and_unknown_default_unchanged():
    assert "native" in _KNOWN_TYPES
    # Lazy CLI sessions with no persisted state must keep today's behavior.
    assert type(deserialize(None)) is UnknownSandboxState


# ── Factory dispatch ────────────────────────────────────────────────────────

def test_factory_dispatches_native_to_registered_class():
    """The registry contract NativeSession will use in P0-F: a class
    registered for type="native" receives native states; other states keep
    their current routing."""
    built = {}

    class _StubNativeSession:
        def __init__(self, *, session_id, state):
            built["session_id"] = session_id
            built["state"] = state

    # Trigger default registration, then swap in the stub and RESTORE it
    # after — clobbering the global registry would break sibling tests that
    # build real NativeSessions via the same factory.
    # Force default registration (sets _REGISTRY_INITIALIZED so make_session
    # below won't re-run it and clobber our stub), then swap + RESTORE.
    factory._register_default_providers()
    factory._REGISTRY_INITIALIZED = True
    saved = factory._REGISTRY.get("native")
    factory.register("native", factory._adapt(_StubNativeSession))
    try:
        s = NativeSandboxState(provider="docker")
        out = factory.make_session("sess-native-1", s)
        assert isinstance(out, _StubNativeSession)
        assert built["session_id"] == "sess-native-1"
        assert built["state"] is s
    finally:
        if saved is not None:
            factory.register("native", saved)


# ── fast checkpoint JSON serialization (orjson, stdlib fallback) ────────────

def test_fast_dumps_parses_identically_to_stdlib():
    """``_fast_dumps`` (orjson when present) feeds the native checkpoint write.
    The column is JSONB, so byte-equality isn't required — but the PARSED value
    must equal stdlib's exactly across the shapes a transcript carries: unicode,
    escapes, None, bools, floats, nested tool_calls. A divergence here would
    silently corrupt a resumed conversation."""
    import json
    from api import db as dbmod

    samples = [
        [{"role": "system", "content": "sys"}],
        [{"role": "user", "content": "héllo • 日本語 \" \\ \n\ttab"}],
        [{"role": "assistant", "content": None,
          "tool_calls": [{"id": "c1", "type": "function",
                          "function": {"name": "bash",
                                       "arguments": '{"command":"ls -la"}'}}]}],
        [{"role": "tool", "tool_call_id": "c1", "content": "x" * 2000}],
        [{"a": 1, "b": 1.5, "c": True, "d": False, "e": None,
          "f": [1, 2, {"g": "h", "i": [None, "j"]}]}],
    ]
    for s in samples:
        assert json.loads(dbmod._fast_dumps(s)) == s
        assert json.loads(dbmod._fast_dumps(s)) == json.loads(json.dumps(s))


def test_fast_dumps_is_always_callable_returning_str():
    """The import guard must leave ``_fast_dumps`` defined and ``str``-returning
    whether or not orjson is installed (Json re-encodes the str) — a deploy
    without orjson must degrade, not crash the checkpoint write."""
    from api import db as dbmod
    out = dbmod._fast_dumps([{"role": "user", "content": "hi"}])
    assert isinstance(out, str)


def test_fast_dumps_falls_back_for_orjson_rejected_input():
    """``_fast_dumps`` feeds DURABILITY writes (checkpoint, session_log), so it
    must never be LESS robust than the stdlib json it replaced. orjson rejects
    some inputs stdlib coerces — e.g. non-string dict keys — so the wrapper must
    fall back, not let a payload shape crash the write. Pins that contract."""
    import json
    from api import db as dbmod
    out = dbmod._fast_dumps({1: "a", 2: "b"})      # int keys: orjson raises
    assert json.loads(out) == {"1": "a", "2": "b"}  # stdlib coerces → still valid


# ── native_transcripts accessors (Postgres required; skips if absent) ──────

import pytest_asyncio


@pytest_asyncio.fixture()
async def db_pool():
    """Async fixture so the connection pool binds to the SAME event loop the
    tests run on (a sync fixture's asyncio.run() creates the pool on a
    different, closed loop — pool operations then hang forever)."""
    import psycopg

    from api import db as dbmod

    url = os.environ.get("DATABASE_URL",
                         "postgresql://postgres@localhost:5433/agent_sdk_server")
    try:
        psycopg.connect(url, connect_timeout=3).close()
    except Exception:
        pytest.skip(f"postgres not reachable at {url}")
    old_url = dbmod.DATABASE_URL
    old_pool = dbmod._pool
    dbmod.DATABASE_URL = url
    dbmod.init_db()
    await dbmod.init_pool(min_size=1, max_size=2)
    yield dbmod
    if dbmod._pool is not None:
        await dbmod._pool.close()
    dbmod._pool = old_pool
    dbmod.DATABASE_URL = old_url


@pytest.mark.asyncio
async def test_checkpoint_write_read_prune(db_pool):
    dbmod = db_pool
    sid = f"s-native-{uuid.uuid4().hex[:8]}"
    aid = f"a-native-{uuid.uuid4().hex[:8]}"
    vid = f"v-native-{uuid.uuid4().hex[:8]}"
    async with dbmod.get_db() as conn:
        await conn.execute(
            "INSERT INTO agents (id, name, config) VALUES (%s, %s, %s)",
            (aid, aid, dbmod.Json({"agent_type": "native"})))
        await conn.execute(
            "INSERT INTO volumes (id, name, provider, provider_ref)"
            " VALUES (%s, %s, 'unix_local', '/tmp/x')", (vid, vid))
        await conn.execute(
            "INSERT INTO sessions (id, agent_id, volume_id) VALUES (%s, %s, %s)",
            (sid, aid, vid))
    try:
        assert await dbmod.read_native_checkpoint(sid) is None
        for seq in (1, 2, 3):
            await dbmod.write_native_checkpoint(
                session_id=sid, turn_seq=seq,
                messages=[{"role": "user", "content": f"turn {seq}"}],
                usage={"inputTokens": seq})
        row = await dbmod.read_native_checkpoint(sid)
        assert row["turn_seq"] == 3
        assert row["messages"][0]["content"] == "turn 3"
        assert row["usage"]["inputTokens"] == 3
        # keep_last=2: turn_seq 1 pruned, 2 and 3 remain
        async with dbmod.get_db() as conn:
            seqs = [r["turn_seq"] for r in await (await conn.execute(
                "SELECT turn_seq FROM native_transcripts WHERE session_id=%s"
                " ORDER BY turn_seq", (sid,))).fetchall()]
        assert seqs == [2, 3]
        # upsert idempotency (recovery-swap retry path)
        await dbmod.write_native_checkpoint(
            session_id=sid, turn_seq=3,
            messages=[{"role": "user", "content": "turn 3 retry"}])
        row = await dbmod.read_native_checkpoint(sid)
        assert row["messages"][0]["content"] == "turn 3 retry"
    finally:
        async with dbmod.get_db() as conn:
            await conn.execute("DELETE FROM sessions WHERE id = %s", (sid,))
            await conn.execute("DELETE FROM agents WHERE id = %s", (aid,))
            await conn.execute("DELETE FROM volumes WHERE id = %s", (vid,))


@pytest.mark.asyncio
async def test_checkpoint_cascades_with_session_delete(db_pool):
    dbmod = db_pool
    sid = f"s-casc-{uuid.uuid4().hex[:8]}"
    aid = f"a-casc-{uuid.uuid4().hex[:8]}"
    vid = f"v-casc-{uuid.uuid4().hex[:8]}"
    async with dbmod.get_db() as conn:
        await conn.execute(
            "INSERT INTO agents (id, name, config) VALUES (%s, %s, %s)",
            (aid, aid, dbmod.Json({})))
        await conn.execute(
            "INSERT INTO volumes (id, name, provider, provider_ref)"
            " VALUES (%s, %s, 'unix_local', '/tmp/x')", (vid, vid))
        await conn.execute(
            "INSERT INTO sessions (id, agent_id, volume_id) VALUES (%s, %s, %s)",
            (sid, aid, vid))
    await dbmod.write_native_checkpoint(session_id=sid, turn_seq=1,
                                        messages=[{"role": "user", "content": "x"}])
    async with dbmod.get_db() as conn:
        await conn.execute("DELETE FROM sessions WHERE id = %s", (sid,))
        n = (await (await conn.execute(
            "SELECT count(*) AS n FROM native_transcripts WHERE session_id=%s",
            (sid,))).fetchone())["n"]
        await conn.execute("DELETE FROM agents WHERE id = %s", (aid,))
        await conn.execute("DELETE FROM volumes WHERE id = %s", (vid,))
    assert n == 0
