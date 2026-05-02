"""Unit tests for volumes DAO and schema."""
from __future__ import annotations

import os, sys
import pytest

_SRC = os.path.join(os.path.dirname(__file__), "..", "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

# Use a per-test Postgres DB URL if set; otherwise skip.
_DB = os.environ.get("TEST_DATABASE_URL")
pytestmark = pytest.mark.skipif(_DB is None, reason="TEST_DATABASE_URL not set")

if _DB:
    os.environ["DATABASE_URL"] = _DB

from api import db as dbmod  # noqa: E402


@pytest.fixture(autouse=True)
def _init_schema():
    dbmod.init_db()
    yield


@pytest.mark.asyncio
async def test_volumes_table_exists():
    await dbmod.init_pool()
    try:
        async with dbmod.get_db() as conn:
            row = await (await conn.execute(
                "SELECT to_regclass('public.volumes') AS t"
            )).fetchone()
        assert row["t"] == "volumes"
    finally:
        await dbmod.close_pool()


def test_volume_record_dataclass():
    from api.models import VolumeRecord
    v = VolumeRecord(id="vol_1", name="proj", provider="daytona",
                     provider_ref="dt-xyz", status="ready")
    assert v.id == "vol_1"
    assert v.name == "proj"
    assert v.provider == "daytona"
    assert v.provider_ref == "dt-xyz"
    assert v.status == "ready"


@pytest.mark.asyncio
async def test_volume_crud_roundtrip():
    from api.models import VolumeRecord
    await dbmod.init_pool()
    try:
        v = VolumeRecord(id="vol_a", name="proj-a", provider="daytona", provider_ref="dt-a")
        await dbmod.upsert_volume(v)

        got = await dbmod.get_volume("vol_a")
        assert got is not None
        assert got.name == "proj-a"

        by_name = await dbmod.get_volume_by_name("proj-a")
        assert by_name is not None and by_name.id == "vol_a"

        listed = await dbmod.list_volumes()
        assert any(x.id == "vol_a" for x in listed)

        await dbmod.delete_volume("vol_a")
        assert await dbmod.get_volume("vol_a") is None
    finally:
        await dbmod.close_pool()


@pytest.mark.asyncio
async def test_sessions_has_volume_id_and_sandbox_state():
    """Sessions row schema: volume_id (NOT NULL) and sandbox_state JSONB.
    The legacy current_sandbox_id FK + standalone sandboxes table were
    dropped; sandbox identity now lives in sessions.sandbox_state JSONB."""
    await dbmod.init_pool()
    try:
        async with dbmod.get_db() as conn:
            rows = await (await conn.execute(
                "SELECT column_name, is_nullable FROM information_schema.columns "
                "WHERE table_name='sessions' "
                "AND column_name IN ('volume_id', 'sandbox_state', 'current_sandbox_id', 'sandbox_id')"
            )).fetchall()
        cols = {r["column_name"]: r["is_nullable"] for r in rows}
        assert "volume_id" in cols
        assert "sandbox_state" in cols
        assert "current_sandbox_id" not in cols, "current_sandbox_id was dropped"
        assert "sandbox_id" not in cols, "sandbox_id was dropped"
        assert cols["volume_id"] == "NO", "sessions.volume_id must be NOT NULL"
    finally:
        await dbmod.close_pool()


@pytest.mark.asyncio
async def test_sandboxes_table_was_dropped():
    """The sandboxes table was removed; sandbox identity moved into
    sessions.sandbox_state JSONB owned by the in-process SessionPool."""
    await dbmod.init_pool()
    try:
        async with dbmod.get_db() as conn:
            row = await (await conn.execute(
                "SELECT to_regclass('public.sandboxes') AS t"
            )).fetchone()
        assert row["t"] is None, "sandboxes table should not exist"
    finally:
        await dbmod.close_pool()
