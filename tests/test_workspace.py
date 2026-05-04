"""Tests for the shared-workspace feature.

Three layers:

1. ``_normalize_workspace`` unit tests — happy + sad paths for the regex
   and trim/lowercase rules. No DB, no fixtures.

2. ``POST /sessions`` integration — the lazy path stores the canonical
   workspace name on the session row, the daytona path 400s with a
   clear error, and an invalid name 400s.

3. ``_bootstrap_session`` subpath wiring — a session row with a
   ``workspace`` value resolves to ``workspaces/<ws>``, falling back to
   ``agents/<agent_id>`` when ``workspace`` is NULL. Verified by
   constructing a real ``UnixLocalSandboxSession`` and inspecting its
   ``_subpath`` after the bootstrap (no compute spawned).
"""
from __future__ import annotations

import os
import sys
import uuid

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

_SRC = os.path.join(os.path.dirname(__file__), "..", "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

from api.providers._shared import _normalize_workspace  # noqa: E402


def _uniq(prefix: str) -> str:
    """Per-call unique id so tests don't collide under xdist parallelism
    (the autouse ``clean_db`` truncates tables on every test, so static
    IDs across parallel workers race). Length kept short for log
    readability.
    """
    return f"{prefix}-{uuid.uuid4().hex[:10]}"


# ---------------------------------------------------------------------------
# (1) _normalize_workspace — pure unit
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("alpha", "alpha"),
        ("Alpha", "alpha"),
        ("Team-Alpha", "team-alpha"),
        ("  team-alpha  ", "team-alpha"),
        ("/team-alpha/", "team-alpha"),
        ("team_alpha.v1", "team_alpha.v1"),
        ("a", "a"),
        ("a" * 64, "a" * 64),
    ],
)
def test_normalize_workspace_accepts_valid(raw, expected):
    assert _normalize_workspace(raw) == expected


@pytest.mark.parametrize(
    "raw",
    [
        "",
        "   ",
        "/",
        "..",
        "../etc",
        "team/alpha",            # slash inside (not stripped)
        "-leading-dash",
        ".leading-dot",
        "_leading-underscore",
        "team alpha",            # internal space
        "team\nalpha",           # newline
        "team\x00alpha",         # NUL
        "TEAM-ÁLPHA",            # non-ASCII
        "a" * 65,                # too long
    ],
)
def test_normalize_workspace_rejects_invalid(raw):
    with pytest.raises(ValueError):
        _normalize_workspace(raw)


def test_normalize_workspace_rejects_non_string():
    with pytest.raises(ValueError):
        _normalize_workspace(None)  # type: ignore[arg-type]
    with pytest.raises(ValueError):
        _normalize_workspace(123)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Shared fixtures for the DB-backed tests
# ---------------------------------------------------------------------------


_DB = os.environ.get("TEST_DATABASE_URL")
_db_required = pytest.mark.skipif(_DB is None, reason="TEST_DATABASE_URL not set")
if _DB:
    os.environ["DATABASE_URL"] = _DB


@pytest_asyncio.fixture
async def client(db_pool):
    """ASGI client. Uses ``db_pool`` (no truncation) — tests below mint
    unique IDs so they don't depend on a clean DB and don't trample
    parallel workers.
    """
    from api import server as srv

    transport = ASGITransport(app=srv.app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


# ---------------------------------------------------------------------------
# (2) POST /sessions — workspace plumbing through the server
# ---------------------------------------------------------------------------


@_db_required
@pytest.mark.asyncio
async def test_lazy_session_normalizes_and_persists_workspace(client):
    """Create a lazy unix_local session with workspace=Team-Alpha and
    confirm:
      * response includes the canonical name
      * DB row stores the canonical name
      * default cwd lands in ``<vol>/workspaces/team-alpha`` (not
        ``agents/<agent_id>``)
    """
    from api import db as dbmod
    from api.models import VolumeRecord

    vol_id = _uniq("v-ws-lazy")
    vol_name = _uniq("vws-lazy")
    vol_ref = f"/tmp/{vol_name}"
    await dbmod.upsert_volume(VolumeRecord(
        id=vol_id, name=vol_name,
        provider="unix_local", provider_ref=vol_ref,
    ))

    r = await client.post("/sessions", json={
        "provider": "unix_local",
        "volume_id": vol_id,
        "provision": False,
        "workspace": "Team-Alpha",
    })
    assert r.status_code == 200, f"got {r.status_code}: {r.text}"
    body = r.json()
    assert body["workspace"] == "team-alpha"
    sid = body["id"]

    sess = await dbmod.get_session(sid)
    assert sess is not None
    assert sess["workspace"] == "team-alpha"
    # cwd defaults to the workspace path on unix_local.
    assert sess["cwd"] == f"{vol_ref}/workspaces/team-alpha"


@_db_required
@pytest.mark.asyncio
async def test_lazy_session_workspace_unset_falls_back_to_agent_subpath(client):
    """Without a workspace, cwd defaults to the per-agent subpath as before."""
    from api import db as dbmod
    from api.models import VolumeRecord

    vol_id = _uniq("v-ws-noset")
    vol_name = _uniq("vws-noset")
    await dbmod.upsert_volume(VolumeRecord(
        id=vol_id, name=vol_name,
        provider="unix_local", provider_ref=f"/tmp/{vol_name}",
    ))

    r = await client.post("/sessions", json={
        "provider": "unix_local",
        "volume_id": vol_id,
        "provision": False,
    })
    assert r.status_code == 200, f"got {r.status_code}: {r.text}"
    body = r.json()
    assert body["workspace"] is None
    sess = await dbmod.get_session(body["id"])
    assert sess["workspace"] is None
    assert "/agents/" in sess["cwd"]
    assert "/workspaces/" not in sess["cwd"]


@_db_required
@pytest.mark.asyncio
async def test_lazy_session_rejects_workspace_on_daytona(client):
    """Daytona's S3-FUSE mounts can't coordinate cross-sandbox writes;
    the server fails fast rather than silently corrupt state.
    """
    from api import db as dbmod
    from api.models import VolumeRecord

    vol_id = _uniq("v-ws-dayt")
    vol_name = _uniq("vws-dayt")
    await dbmod.upsert_volume(VolumeRecord(
        id=vol_id, name=vol_name,
        provider="daytona", provider_ref=f"dt-{vol_name}",
    ))

    r = await client.post("/sessions", json={
        "provider": "daytona",
        "volume_id": vol_id,
        "provision": False,
        "workspace": "team-alpha",
    })
    assert r.status_code == 400, f"got {r.status_code}: {r.text}"
    assert "daytona" in r.text.lower()


@_db_required
@pytest.mark.asyncio
async def test_lazy_session_rejects_invalid_workspace_name(client):
    from api import db as dbmod
    from api.models import VolumeRecord

    vol_id = _uniq("v-ws-bad")
    vol_name = _uniq("vws-bad")
    await dbmod.upsert_volume(VolumeRecord(
        id=vol_id, name=vol_name,
        provider="unix_local", provider_ref=f"/tmp/{vol_name}",
    ))

    r = await client.post("/sessions", json={
        "provider": "unix_local",
        "volume_id": vol_id,
        "provision": False,
        "workspace": "../etc",
    })
    assert r.status_code == 400, f"got {r.status_code}: {r.text}"
    assert "workspace" in r.text.lower()


@_db_required
@pytest.mark.asyncio
async def test_get_session_includes_workspace(client):
    """``GET /sessions/{id}`` surfaces the workspace name."""
    from api import db as dbmod
    from api.models import VolumeRecord

    vol_id = _uniq("v-ws-get")
    vol_name = _uniq("vws-get")
    await dbmod.upsert_volume(VolumeRecord(
        id=vol_id, name=vol_name,
        provider="unix_local", provider_ref=f"/tmp/{vol_name}",
    ))

    r = await client.post("/sessions", json={
        "provider": "unix_local",
        "volume_id": vol_id,
        "provision": False,
        "workspace": "alpha",
    })
    assert r.status_code == 200
    sid = r.json()["id"]

    g = await client.get(f"/sessions/{sid}")
    assert g.status_code == 200
    assert g.json()["workspace"] == "alpha"


# ---------------------------------------------------------------------------
# (3) Subpath resolution in _bootstrap_session
# ---------------------------------------------------------------------------


@_db_required
@pytest.mark.asyncio
async def test_bootstrap_subpath_uses_workspace_when_set(db_pool):
    """When the session row has ``workspace='alpha'``, the SandboxSession
    resolves ``_subpath`` to ``workspaces/alpha`` instead of
    ``agents/<agent_id>`` — this is the single point of truth that
    governs ACP HOME inside the sandbox.
    """
    from api import db as dbmod
    from api.models import AgentConfig, AgentRecord, VolumeRecord
    from api.providers.unix_local.session import UnixLocalSandboxSession
    from api.sandbox.state import UnixLocalSandboxState, Recipe

    vol_id = _uniq("v-ws-sub")
    aid = _uniq("a-ws-sub")
    sid = _uniq("s-ws-sub")
    await dbmod.upsert_volume(VolumeRecord(
        id=vol_id, name=vol_id,
        provider="unix_local", provider_ref=f"/tmp/{vol_id}",
    ))
    await dbmod.upsert_agent(AgentRecord(id=aid, name=aid, config=AgentConfig()))
    await dbmod.upsert_session(
        sid, aid, inner_session_id=None,
        volume_id=vol_id, workspace="alpha",
    )

    state = UnixLocalSandboxState(recipe=Recipe(agent_type="claude"))
    sb = UnixLocalSandboxSession(session_id=sid, state=state)
    await sb._bootstrap_session()
    assert sb._subpath == "workspaces/alpha"


@_db_required
@pytest.mark.asyncio
async def test_bootstrap_subpath_defaults_to_agent_when_workspace_unset(db_pool):
    from api import db as dbmod
    from api.models import AgentConfig, AgentRecord, VolumeRecord
    from api.providers.unix_local.session import UnixLocalSandboxSession
    from api.sandbox.state import UnixLocalSandboxState, Recipe

    vol_id = _uniq("v-ws-sub2")
    aid = _uniq("a-ws-sub2")
    sid = _uniq("s-ws-sub2")
    await dbmod.upsert_volume(VolumeRecord(
        id=vol_id, name=vol_id,
        provider="unix_local", provider_ref=f"/tmp/{vol_id}",
    ))
    await dbmod.upsert_agent(AgentRecord(id=aid, name=aid, config=AgentConfig()))
    await dbmod.upsert_session(
        sid, aid, inner_session_id=None, volume_id=vol_id,
    )

    state = UnixLocalSandboxState(recipe=Recipe(agent_type="claude"))
    sb = UnixLocalSandboxSession(session_id=sid, state=state)
    await sb._bootstrap_session()
    assert sb._subpath == f"agents/{aid}"


# ---------------------------------------------------------------------------
# (4) Two agents on the same workspace converge on the same subpath
# ---------------------------------------------------------------------------


@_db_required
@pytest.mark.asyncio
async def test_per_session_workspace_overrides_agent(client):
    """``Session.workspace`` (set via ``agent.create_session(workspace=X)``)
    must override the Agent's workspace for THAT session only. Same
    agent_id on the wire; different workspace columns on the two
    session rows.
    """
    from api import db as dbmod
    from api.models import VolumeRecord

    vol_id = _uniq("v-ws-override")
    vol_name = _uniq("vws-override")
    await dbmod.upsert_volume(VolumeRecord(
        id=vol_id, name=vol_name,
        provider="unix_local", provider_ref=f"/tmp/{vol_name}",
    ))

    inherit = (await client.post("/sessions", json={
        "provider": "unix_local",
        "volume_id": vol_id,
        "provision": False,
        "workspace": "agent-alpha",
    })).json()

    override = (await client.post("/sessions", json={
        "provider": "unix_local",
        "volume_id": vol_id,
        "provision": False,
        "agent_id": inherit["agent_id"],  # same Agent identity
        "workspace": "session-beta",
    })).json()

    assert inherit["workspace"] == "agent-alpha"
    assert override["workspace"] == "session-beta"
    assert inherit["agent_id"] == override["agent_id"]
    # Confirm storage matches the wire: each session row carries its own
    # workspace, independent of the Agent's value.
    s1 = await dbmod.get_session(inherit["id"])
    s2 = await dbmod.get_session(override["id"])
    assert s1["workspace"] == "agent-alpha"
    assert s2["workspace"] == "session-beta"


@_db_required
@pytest.mark.asyncio
async def test_two_agents_same_workspace_share_subpath(db_pool):
    """The whole point of the feature: two different agents that name the
    same workspace land on the same subpath, and therefore the same HOME
    directory once mounted. Different agent_ids, identical _subpath.
    """
    from api import db as dbmod
    from api.models import AgentConfig, AgentRecord, VolumeRecord
    from api.providers.unix_local.session import UnixLocalSandboxSession
    from api.sandbox.state import UnixLocalSandboxState, Recipe

    vol_id = _uniq("v-ws-share")
    aid_a = _uniq("a-alice")
    aid_b = _uniq("a-bob")
    sid_a = _uniq("s-alice")
    sid_b = _uniq("s-bob")
    await dbmod.upsert_volume(VolumeRecord(
        id=vol_id, name=vol_id,
        provider="unix_local", provider_ref=f"/tmp/{vol_id}",
    ))
    await dbmod.upsert_agent(AgentRecord(id=aid_a, name="Alice", config=AgentConfig()))
    await dbmod.upsert_agent(AgentRecord(id=aid_b, name="Bob", config=AgentConfig()))
    await dbmod.upsert_session(
        sid_a, aid_a, inner_session_id=None,
        volume_id=vol_id, workspace="team-alpha",
    )
    await dbmod.upsert_session(
        sid_b, aid_b, inner_session_id=None,
        volume_id=vol_id, workspace="team-alpha",
    )

    state_a = UnixLocalSandboxState(recipe=Recipe(agent_type="claude"))
    state_b = UnixLocalSandboxState(recipe=Recipe(agent_type="claude"))
    sb_a = UnixLocalSandboxSession(session_id=sid_a, state=state_a)
    sb_b = UnixLocalSandboxSession(session_id=sid_b, state=state_b)

    await sb_a._bootstrap_session()
    await sb_b._bootstrap_session()

    assert sb_a._subpath == "workspaces/team-alpha"
    assert sb_b._subpath == "workspaces/team-alpha"
    assert sb_a._agent_id != sb_b._agent_id  # distinct identities
