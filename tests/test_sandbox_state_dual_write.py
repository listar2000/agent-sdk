"""Phase 1 of the ephemeral-sandbox refactor: verify the
``sessions.sandbox_state`` JSONB column is maintained automatically by
the Postgres triggers whenever ``sandboxes`` or ``sessions`` rows change.

Pins the dual-write contract so phase 2 can read from ``sandbox_state``
trusting it's in sync with the legacy ``sandboxes`` table.

See ``docs/ephemeral-sandbox-design.md`` §14.1.
"""
from __future__ import annotations

import os
import sys
import uuid

import pytest
import pytest_asyncio

_SRC = os.path.join(os.path.dirname(__file__), "..", "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

_DB = os.environ.get("TEST_DATABASE_URL")
pytestmark = pytest.mark.skipif(_DB is None, reason="TEST_DATABASE_URL not set")
if _DB:
    os.environ["DATABASE_URL"] = _DB

from api import db as dbmod  # noqa: E402


@pytest_asyncio.fixture
async def setup(clean_db):
    yield


async def _read_state(session_id: str) -> dict | None:
    async with dbmod.get_db() as conn:
        row = await (await conn.execute(
            "SELECT sandbox_state FROM sessions WHERE id = %s", (session_id,)
        )).fetchone()
    return row["sandbox_state"] if row else None


async def _mk_agent_volume(agent_type: str = "claude") -> tuple[str, str]:
    from api.models import AgentConfig, AgentRecord, VolumeRecord
    aid = f"a-{uuid.uuid4().hex[:8]}"
    vid = f"v-{uuid.uuid4().hex[:8]}"
    await dbmod.upsert_agent(AgentRecord(
        id=aid, name="A", config=AgentConfig(agent_type=agent_type)))
    await dbmod.upsert_volume(VolumeRecord(
        id=vid, name=vid, provider="daytona", provider_ref="dt-vol"))
    return aid, vid


@pytest.mark.asyncio
async def test_sandbox_state_populated_on_session_insert_with_no_sandbox(setup):
    """A fresh session row (no sandbox) gets a sandbox_state with type=unknown."""
    aid, vid = await _mk_agent_volume(agent_type="claude")
    sid = f"s-{uuid.uuid4().hex[:8]}"
    async with dbmod.get_db() as conn:
        await conn.execute(
            "INSERT INTO sessions (id, agent_id, volume_id) VALUES (%s,%s,%s)",
            (sid, aid, vid),
        )

    state = await _read_state(sid)
    assert state is not None, "trigger did not populate sandbox_state on INSERT"
    assert state["type"] == "unknown"
    assert state["sandbox_id"] is None
    assert state["recipe"]["agent_type"] == "claude"
    assert state["recipe"]["shared_mounts"] == []


@pytest.mark.asyncio
async def test_sandbox_state_updates_when_current_sandbox_id_set(setup):
    """Binding a session to a sandbox refreshes sandbox_state from that sandbox row."""
    from api.models import SandboxRecord
    aid, vid = await _mk_agent_volume()
    sid = f"s-{uuid.uuid4().hex[:8]}"
    sb_id = f"sb-{uuid.uuid4().hex[:8]}"

    async with dbmod.get_db() as conn:
        await conn.execute(
            "INSERT INTO sessions (id, agent_id, volume_id) VALUES (%s,%s,%s)",
            (sid, aid, vid),
        )

    # Insert a sandbox row, then bind it to the session.
    sb = SandboxRecord(
        id=sb_id, provider="daytona", sandbox_ref="dt-ref-123",
        status="running", root="/home/daytona",
        volume_id=vid, subpath="agents/foo",
        dockerfile="/path/to/Dockerfile",
        shared_mounts=["projects", "datasets"],
        listen_port=9100,
    )
    await dbmod.upsert_sandbox(sb)
    await dbmod.set_session_current_sandbox(sid, sb_id)

    state = await _read_state(sid)
    assert state["type"] == "daytona"
    assert state["sandbox_id"] == "dt-ref-123"
    assert state["listen_port"] == 9100
    assert state["recipe"]["dockerfile"] == "/path/to/Dockerfile"
    assert state["recipe"]["shared_mounts"] == ["projects", "datasets"]
    assert state["recipe"]["root"] == "/home/daytona"
    assert state["recipe"]["agent_type"] == "claude"


@pytest.mark.asyncio
async def test_sandbox_state_updates_when_sandbox_row_mutates(setup):
    """Updating the sandbox row (e.g. shared_mounts changes) propagates to sandbox_state."""
    from api.models import SandboxRecord
    aid, vid = await _mk_agent_volume()
    sid = f"s-{uuid.uuid4().hex[:8]}"
    sb_id = f"sb-{uuid.uuid4().hex[:8]}"

    sb = SandboxRecord(
        id=sb_id, provider="daytona", sandbox_ref="dt-original",
        status="running", root="/home/daytona",
        volume_id=vid, subpath="agents/foo",
        dockerfile=None, shared_mounts=["a"], listen_port=9100,
    )
    await dbmod.upsert_sandbox(sb)
    async with dbmod.get_db() as conn:
        await conn.execute(
            "INSERT INTO sessions (id, agent_id, volume_id, current_sandbox_id) "
            "VALUES (%s,%s,%s,%s)",
            (sid, aid, vid, sb_id),
        )

    state = await _read_state(sid)
    assert state["sandbox_id"] == "dt-original"
    assert state["recipe"]["shared_mounts"] == ["a"]

    # Mutate the sandbox row — sandbox_ref + shared_mounts.
    sb.sandbox_ref = "dt-changed"
    sb.shared_mounts = ["a", "b"]
    await dbmod.upsert_sandbox(sb)

    state = await _read_state(sid)
    assert state["sandbox_id"] == "dt-changed"
    assert state["recipe"]["shared_mounts"] == ["a", "b"]


@pytest.mark.asyncio
async def test_sandbox_state_resets_when_current_sandbox_id_cleared(setup):
    """Setting current_sandbox_id back to NULL collapses to the no-compute shape."""
    from api.models import SandboxRecord
    aid, vid = await _mk_agent_volume()
    sid = f"s-{uuid.uuid4().hex[:8]}"
    sb_id = f"sb-{uuid.uuid4().hex[:8]}"

    sb = SandboxRecord(
        id=sb_id, provider="docker", sandbox_ref="container-1",
        status="running", root="/home/agent",
        volume_id=vid, subpath="agents/foo",
        dockerfile=None, shared_mounts=[], listen_port=2469,
    )
    await dbmod.upsert_sandbox(sb)
    async with dbmod.get_db() as conn:
        await conn.execute(
            "INSERT INTO sessions (id, agent_id, volume_id, current_sandbox_id) "
            "VALUES (%s,%s,%s,%s)",
            (sid, aid, vid, sb_id),
        )

    state_before = await _read_state(sid)
    assert state_before["type"] == "docker"
    assert state_before["sandbox_id"] == "container-1"

    await dbmod.set_session_current_sandbox(sid, None)

    state_after = await _read_state(sid)
    assert state_after["type"] == "unknown"
    assert state_after["sandbox_id"] is None
    # Recipe still has the agent_type from the agent row.
    assert state_after["recipe"]["agent_type"] == "claude"
