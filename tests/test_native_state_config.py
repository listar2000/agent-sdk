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

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

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
