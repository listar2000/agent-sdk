"""Contract test: the EAGER ``POST /sessions`` path stores user-only
``pre_start_commands`` on the session row, not the merged (skill+user)
result.

The lazy path's behaviour is already pinned by
``test_pre_start_commands_persist.test_pre_start_commands_stored_on_lazy_session``;
the eager path was a silent contract violation until the 2026-05 flip:
it baked the merged list into the column, causing every ``/reload``
round-trip to re-emit (and double-count) the skill-install lines after
deriving from the column.

This test pins the contract for the eager path so a future refactor
can't regress it without breaking a test. Mocks the SessionPool so we
don't actually provision a sandbox — the assertion is purely on what
hits the ``sessions.pre_start_commands`` column.
"""
from __future__ import annotations

import os
import sys
import uuid
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

_SRC = os.path.join(os.path.dirname(__file__), "..", "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

_DB = os.environ.get("TEST_DATABASE_URL")
pytestmark = pytest.mark.skipif(_DB is None, reason="TEST_DATABASE_URL not set")
if _DB:
    os.environ["DATABASE_URL"] = _DB

from api import db as dbmod, server as srv  # noqa: E402


def _uniq(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:10]}"


@pytest_asyncio.fixture
async def client(db_pool):
    transport = ASGITransport(app=srv.app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


@pytest.mark.asyncio
async def test_eager_post_sessions_stores_user_only_pre_start_commands(client):
    """Eager ``POST /sessions`` with ``skills`` AND ``pre_start_commands``:
    the row's column must contain ONLY the user commands. The merged
    list (skill-install + user) lives on
    ``sandbox_state.recipe.pre_start_commands`` and is the input to
    provisioning + Type-2 recovery, but the column itself is user-only.
    """
    from api.models import VolumeRecord

    v = VolumeRecord(
        id=_uniq("vol-flip"), name=_uniq("flip"),
        provider="daytona", provider_ref=_uniq("dt-flip"),
    )
    await dbmod.upsert_volume(v)

    user_cmds = [
        "echo USER-MARKER-A",
        "echo USER-MARKER-B",
    ]
    skills = ["rllm-org/hive"]

    # Mock the pool so cold_create doesn't try to provision a real
    # sandbox. We only care about what landed in the DB.
    fake_session = MagicMock(
        _supervisor_url="http://127.0.0.1:9999",
        _acp_session_id=None,
        _inner_session_id=None,
    )
    fake_session.state = MagicMock(sandbox_ref="dt-mock-12345")
    fake_pool = MagicMock()
    fake_pool.cold_create = AsyncMock(return_value=fake_session)
    fake_pool.get_session = AsyncMock(return_value=fake_session)

    # ``server._sessions_create_eager`` does ``from api.sandbox import
    # get_pool`` which is re-exported from ``api.sandbox.runtime`` via
    # ``api/sandbox/__init__.py:24``. The lazy import resolves through
    # ``api.sandbox`` — patch THAT binding, not ``runtime.get_pool``.
    with patch("api.sandbox.get_pool", return_value=fake_pool):
        r = await client.post("/sessions", json={
            "name": "eager-column-flip",
            "provider": "daytona",
            "agent_type": "claude",
            "volume_id": v.id,
            # Eager path is the default (no ``provision: false``).
            "skills": skills,
            "pre_start_commands": user_cmds,
        })
    assert r.status_code == 200, r.text
    body = r.json()
    sid = body.get("session_id") or body.get("id")
    assert sid

    # ── The load-bearing assertion: column is user-only ─────────────────
    sess = await dbmod.get_session(sid)
    assert sess is not None
    column_value = sess.get("pre_start_commands") or []
    assert column_value == user_cmds, (
        f"sessions.pre_start_commands column should be user-only after the "
        f"eager-path flip (see server.py around L1774 + "
        f"test_pre_start_commands_persist.py contract). "
        f"Expected {user_cmds!r}; got {column_value!r}"
    )
    # Specifically: NO skill-install line in the column.
    assert not any("npx -y skills add" in c for c in column_value), (
        f"column leaked a skill-install line: {column_value!r}. "
        f"Skills must come from agents.config.skills, not the column."
    )


@pytest.mark.asyncio
async def test_eager_post_sessions_no_user_commands_stores_empty_list(client):
    """Eager session create with skills but no caller ``pre_start_commands``
    leaves the column as ``[]`` (not the skill-install list)."""
    from api.models import VolumeRecord

    v = VolumeRecord(
        id=_uniq("vol-flip2"), name=_uniq("flip2"),
        provider="daytona", provider_ref=_uniq("dt-flip2"),
    )
    await dbmod.upsert_volume(v)

    fake_session = MagicMock(
        _supervisor_url="http://127.0.0.1:9999",
        _acp_session_id=None, _inner_session_id=None,
    )
    fake_session.state = MagicMock(sandbox_ref="dt-mock-22222")
    fake_pool = MagicMock()
    fake_pool.cold_create = AsyncMock(return_value=fake_session)
    fake_pool.get_session = AsyncMock(return_value=fake_session)

    # ``server._sessions_create_eager`` does ``from api.sandbox import
    # get_pool`` which is re-exported from ``api.sandbox.runtime`` via
    # ``api/sandbox/__init__.py:24``. The lazy import resolves through
    # ``api.sandbox`` — patch THAT binding, not ``runtime.get_pool``.
    with patch("api.sandbox.get_pool", return_value=fake_pool):
        r = await client.post("/sessions", json={
            "name": "eager-column-empty",
            "provider": "daytona",
            "agent_type": "claude",
            "volume_id": v.id,
            "skills": ["rllm-org/hive"],
            # No pre_start_commands.
        })
    assert r.status_code == 200, r.text
    sid = (r.json().get("session_id") or r.json().get("id"))
    sess = await dbmod.get_session(sid)
    assert (sess or {}).get("pre_start_commands") == [], (
        f"column should be [] with no user pre_start; got "
        f"{(sess or {}).get('pre_start_commands')!r}"
    )
