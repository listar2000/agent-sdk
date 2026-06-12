"""P0-G gate: server wiring for native sessions.

- POST /sessions with agent_type=native forces the lazy path and persists a
  NativeSandboxState (so recovery routes to NativeSession, not daytona);
- /sandbox/exec routes through the session transport (no supervisor);
- lazy provisioning persists sandbox_ref.

Uses a real Postgres (skips if absent) + an injected transport factory so no
docker is needed. Drives the FastAPI app via httpx ASGITransport.
"""

from __future__ import annotations

import os
import sys
import uuid

import httpx
import pytest
import pytest_asyncio

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

_DB_URL = os.environ.get("DATABASE_URL",
                         "postgresql://postgres@localhost:5433/agent_sdk_server")


def _pg_ok() -> bool:
    import psycopg
    try:
        psycopg.connect(_DB_URL, connect_timeout=3).close()
        return True
    except Exception:
        return False


pytestmark = [pytest.mark.skipif(not _pg_ok(), reason="postgres unavailable"), pytest.mark.xdist_group("db")]


class _FakeTransport:
    """In-memory sandbox standing in for DockerTransport."""

    def __init__(self):
        self.container_id = "fake-cid-" + uuid.uuid4().hex[:8]
        self.fs: dict[str, bytes] = {}

    @property
    def ref(self):
        # Mirror DockerTransport.ref (the provider-agnostic id NativeSession
        # persists as sandbox_ref) — for docker that is the container_id.
        return self.container_id

    async def exec(self, command, *, cwd=None, env=None, timeout_s=300):
        from api.native.transport import TransportExecResult
        # support the golden's write/read shapes minimally
        return TransportExecResult(f"ran:{command}", "", 0, False)

    async def read_file(self, p, *, max_bytes=8 * 1024 * 1024):
        if p not in self.fs:
            raise FileNotFoundError(p)
        return self.fs[p]

    async def write_file(self, p, data):
        self.fs[p] = data

    async def destroy(self):
        pass


@pytest_asyncio.fixture()
async def app_client():
    os.environ["DATABASE_URL"] = _DB_URL
    from api import db as dbmod
    dbmod.DATABASE_URL = _DB_URL
    dbmod.init_db()
    from api import server as srv
    await dbmod.init_pool(min_size=1, max_size=4)
    # the /sandbox/exec route uses the module HTTP client only for the
    # non-native path; native bypasses it, so no lifespan needed.
    transport = httpx.ASGITransport(app=srv.app)
    async with httpx.AsyncClient(transport=transport,
                                 base_url="http://test") as client:
        yield client, srv, dbmod
    if dbmod._pool is not None:
        await dbmod._pool.close()
        dbmod._pool = None


async def _cleanup(dbmod, sid):
    async with dbmod.get_db() as conn:
        row = await (await conn.execute(
            "SELECT agent_id, volume_id FROM sessions WHERE id=%s", (sid,))
        ).fetchone()
        await conn.execute("DELETE FROM sessions WHERE id=%s", (sid,))
        if row:
            await conn.execute("DELETE FROM agents WHERE id=%s", (row["agent_id"],))


@pytest.mark.asyncio
async def test_native_create_is_lazy_and_persists_state(app_client):
    client, srv, dbmod = app_client
    sid = str(uuid.uuid4())
    r = await client.post("/sessions", json={
        "id": sid, "agent_type": "native", "provider": "docker",
        "model": "openrouter/openai/gpt-4o-mini",
        "config": {"native": {"instructions": "hi", "max_turns": 3}},
    })
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["sandbox_ref"] is None and body["connected"] is False
    # state row persisted as native (NOT null/daytona)
    state = await dbmod.read_sandbox_state(sid)
    assert state is not None and state["type"] == "native"
    assert state["provider"] == "docker"
    assert state["recipe"]["agent_type"] == "native"
    # agent config carried the native passthrough
    async with dbmod.get_db() as conn:
        arow = await (await conn.execute(
            "SELECT a.config FROM agents a JOIN sessions s ON s.agent_id=a.id"
            " WHERE s.id=%s", (sid,))).fetchone()
    assert arow["config"]["native"]["max_turns"] == 3
    await _cleanup(dbmod, sid)


@pytest.mark.asyncio
async def test_sandbox_exec_routes_through_transport_and_provisions(app_client):
    client, srv, dbmod = app_client
    sid = str(uuid.uuid4())
    r = await client.post("/sessions", json={
        "id": sid, "agent_type": "native", "provider": "docker",
        "model": "openrouter/x",
    })
    assert r.status_code == 200, r.text

    # inject a fake transport so no docker is needed
    fake = _FakeTransport()
    from api.sandbox import get_pool
    sess = await get_pool().get_session(sid)
    assert sess.state.type == "native"
    sess._transport_factory = lambda: _ret(fake)

    r = await client.post(f"/sessions/{sid}/sandbox/exec",
                          json={"command": "echo hi", "timeout": 10})
    assert r.status_code == 200, r.text
    payload = r.json()
    assert payload["stdout"] == "ran:echo hi"
    assert payload["exit_code"] == 0
    # lazy provision persisted the sandbox_ref
    state = await dbmod.read_sandbox_state(sid)
    assert state["sandbox_ref"] == fake.container_id
    await _cleanup(dbmod, sid)


async def _ret(v):
    return v


@pytest.mark.asyncio
async def test_delete_destroys_recreated_ref_not_just_pre_release(app_client, monkeypatch):
    """#3 (DELETE racing a mid-turn recreate): if a native session recreates its
    sandbox (a NEW ref) concurrently with DELETE, teardown must destroy BOTH the
    original and the recreated ref — not only the ref snapshotted BEFORE release
    — so the recreated sandbox isn't orphaned. _destroy_session_compute re-reads
    the ref AFTER pool.release() (which cancels+awaits the in-flight turn, so the
    post-release state is final) and destroys the union."""
    client, srv, dbmod = app_client
    sid = str(uuid.uuid4())
    r = await client.post("/sessions", json={
        "id": sid, "agent_type": "native", "provider": "docker", "model": "x"})
    assert r.status_code == 200, r.text

    # seed the pre-release ref
    payload = await dbmod.read_sandbox_state(sid)
    payload["sandbox_ref"] = "ref0-original"
    await dbmod.write_sandbox_state(sid, payload)

    # simulate the in-flight turn recreating + persisting a NEW ref during the
    # window release() covers (release cancels+awaits the turn, so the DB ref is
    # final once it returns)
    from api.sandbox import get_pool
    pool = get_pool()

    async def _release_with_recreate(session_id):
        p = await dbmod.read_sandbox_state(session_id)
        p["sandbox_ref"] = "ref1-recreated"
        await dbmod.write_sandbox_state(session_id, p)
    monkeypatch.setattr(pool, "release", _release_with_recreate)

    # native docker routes destroy through the docker provider module
    from api import providers as _prov
    dockermod = _prov._PROVIDER_MODS["docker"]
    destroyed = []

    async def _fake_destroy(inst):
        destroyed.append(inst.sandbox_ref)
    monkeypatch.setattr(dockermod, "destroy_sandbox", _fake_destroy)

    await srv._destroy_session_compute(sid)
    assert set(destroyed) == {"ref0-original", "ref1-recreated"}, (
        f"teardown must destroy BOTH the pre-release and recreated refs "
        f"(no orphan); got {destroyed}")
    await _cleanup(dbmod, sid)
