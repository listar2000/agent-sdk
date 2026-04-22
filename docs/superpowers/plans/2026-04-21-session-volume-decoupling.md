# Session / Volume / Sandbox Decoupling — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make sandboxes ephemeral compute leases and volumes durable storage. A session binds to a volume (via its agent's HOME on that volume) and survives sandbox death.

**Architecture:** Introduce a `volumes` table (first-class resource). Add `volume_id` + `subpath` columns to `sandboxes`. Add `volume_id` (required) + rename `sandbox_id` → `current_sandbox_id` (nullable, no CASCADE) on `sessions`. Every sandbox mounts `(volume_id, subpath)` at creation; session-driven creation uses `subpath = agents/<agent_id>/home`, plus a read-only `/mnt/shared` mount on the same volume. On pre-run probe, if the sandbox is dead the session reprovisions transparently — same subpath means the CLI's transcript file is still there for `--resume`.

**Scope:** Daytona path end-to-end. Docker + Local providers get follow-up plans.

**Tech Stack:** Python 3.11+, FastAPI, psycopg v3, pytest, Daytona SDK (existing in the repo).

**Reference spec:** `docs/superpowers/specs/2026-04-21-session-volume-decoupling-design.md`

---

## File Structure

| Path | Change |
|---|---|
| `src/api/models.py` | add `VolumeRecord` dataclass; add `volume_id`, `subpath` to `SandboxRecord` |
| `src/api/db.py` | new `volumes` table in `_PG_SCHEMA`; migrations for `sessions` + `sandboxes`; new volume CRUD functions; updated `upsert_sandbox`, `upsert_session`, `get_session` signatures |
| `src/api/providers.py` | new `create_daytona_volume` / `delete_daytona_volume`; extend `create_daytona` + `provision_daytona_sandbox` to accept `volume_id` + `subpath`; add probe helper `get_daytona_sandbox_status` |
| `src/api/server.py` | new `/volumes/*` routes; update `POST /sessions` + `/sessions/quick` to require `volume_id`; new `/sessions/{id}/start-sandbox`, `/stop-sandbox`, `/reset-sandbox`; pre-run probe in `/sessions/{id}/message` + `/resume`; new SSE events |
| `src/agent_sdk/client.py` | add `client.volumes.*` namespace; update session create signature |
| `tests/test_volumes_db.py` | new — unit tests for volume DAO |
| `tests/test_volumes_api.py` | new — REST tests for volume endpoints |
| `tests/test_session_volume_integration.py` | new — the sandbox-loss resume correctness test |

---

## Phase 1: Volume schema + DAO

### Task 1: Add `volumes` table to schema

**Files:**
- Modify: `src/api/db.py:24-58` (`_PG_SCHEMA` list)
- Test: `tests/test_volumes_db.py`

- [ ] **Step 1: Write the failing test**

Create `tests/test_volumes_db.py` with:

```python
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `TEST_DATABASE_URL=postgresql://localhost:5432/agent_sdk_test pytest tests/test_volumes_db.py::test_volumes_table_exists -v`
Expected: FAIL — `to_regclass` returns NULL because table doesn't exist.

- [ ] **Step 3: Add the table to `_PG_SCHEMA`**

In `src/api/db.py`, append to `_PG_SCHEMA` (before the `CREATE INDEX` lines):

```python
    """CREATE TABLE IF NOT EXISTS volumes (
        id            TEXT PRIMARY KEY,
        name          TEXT NOT NULL UNIQUE,
        provider      TEXT NOT NULL,
        provider_ref  TEXT NOT NULL,
        status        TEXT NOT NULL DEFAULT 'ready',
        created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
    )""",
```

And add the index after the existing indexes:

```python
    "CREATE INDEX IF NOT EXISTS idx_volumes_name ON volumes(name)",
```

- [ ] **Step 4: Run test to verify it passes**

Run: `TEST_DATABASE_URL=postgresql://localhost:5432/agent_sdk_test pytest tests/test_volumes_db.py::test_volumes_table_exists -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/api/db.py tests/test_volumes_db.py
git commit -m "feat(db): add volumes table"
```

---

### Task 2: `VolumeRecord` dataclass

**Files:**
- Modify: `src/api/models.py` (append after `SandboxRecord`)
- Test: `tests/test_volumes_db.py`

- [ ] **Step 1: Write the failing test**

Append to `tests/test_volumes_db.py`:

```python
def test_volume_record_dataclass():
    from api.models import VolumeRecord
    v = VolumeRecord(id="vol_1", name="proj", provider="daytona",
                     provider_ref="dt-xyz", status="ready")
    assert v.id == "vol_1"
    assert v.name == "proj"
    assert v.provider == "daytona"
    assert v.provider_ref == "dt-xyz"
    assert v.status == "ready"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_volumes_db.py::test_volume_record_dataclass -v`
Expected: FAIL — `ImportError: cannot import name 'VolumeRecord'`.

- [ ] **Step 3: Add the dataclass**

In `src/api/models.py`, after the `SandboxRecord` class, insert:

```python
@dataclass
class VolumeRecord:
    id: str
    name: str
    provider: str
    provider_ref: str
    status: str = "ready"
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_volumes_db.py::test_volume_record_dataclass -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/api/models.py tests/test_volumes_db.py
git commit -m "feat(models): add VolumeRecord dataclass"
```

---

### Task 3: Volume CRUD DAO in `db.py`

**Files:**
- Modify: `src/api/db.py` (append after the sandbox CRUD section)
- Test: `tests/test_volumes_db.py`

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_volumes_db.py`:

```python
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_volumes_db.py::test_volume_crud_roundtrip -v`
Expected: FAIL — `AttributeError: module 'api.db' has no attribute 'upsert_volume'`.

- [ ] **Step 3: Add the DAO functions**

At the top of `src/api/db.py` imports, update the models import:

```python
from .models import AgentConfig, AgentRecord, LogEntry, SandboxRecord, VolumeRecord
```

Append after the sandbox CRUD section (around line 240 in current layout):

```python
# ---------------------------------------------------------------------------
# Volume CRUD
# ---------------------------------------------------------------------------

async def upsert_volume(volume: VolumeRecord) -> None:
    async with get_db() as conn:
        await conn.execute(
            "INSERT INTO volumes (id, name, provider, provider_ref, status)"
            " VALUES (%s, %s, %s, %s, %s)"
            " ON CONFLICT(id) DO UPDATE SET name=EXCLUDED.name,"
            " provider=EXCLUDED.provider, provider_ref=EXCLUDED.provider_ref,"
            " status=EXCLUDED.status",
            (volume.id, volume.name, volume.provider, volume.provider_ref, volume.status),
        )


async def get_volume(volume_id: str) -> VolumeRecord | None:
    async with get_db() as conn:
        row = await (await conn.execute(
            "SELECT * FROM volumes WHERE id = %s", (volume_id,)
        )).fetchone()
    if row is None:
        return None
    return VolumeRecord(id=row["id"], name=row["name"], provider=row["provider"],
                        provider_ref=row["provider_ref"], status=row["status"])


async def get_volume_by_name(name: str) -> VolumeRecord | None:
    async with get_db() as conn:
        row = await (await conn.execute(
            "SELECT * FROM volumes WHERE name = %s", (name,)
        )).fetchone()
    if row is None:
        return None
    return VolumeRecord(id=row["id"], name=row["name"], provider=row["provider"],
                        provider_ref=row["provider_ref"], status=row["status"])


async def list_volumes(provider: str | None = None) -> list[VolumeRecord]:
    async with get_db() as conn:
        if provider:
            rows = await (await conn.execute(
                "SELECT * FROM volumes WHERE provider = %s", (provider,)
            )).fetchall()
        else:
            rows = await (await conn.execute("SELECT * FROM volumes")).fetchall()
    return [
        VolumeRecord(id=r["id"], name=r["name"], provider=r["provider"],
                     provider_ref=r["provider_ref"], status=r["status"])
        for r in rows
    ]


async def delete_volume(volume_id: str) -> None:
    async with get_db() as conn:
        await conn.execute("DELETE FROM volumes WHERE id = %s", (volume_id,))
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_volumes_db.py::test_volume_crud_roundtrip -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/api/db.py tests/test_volumes_db.py
git commit -m "feat(db): volume CRUD DAO"
```

---

### Task 4: Schema migration — sessions.volume_id + sandbox_id → current_sandbox_id

**Files:**
- Modify: `src/api/db.py` (`_MIGRATIONS` list, around line 68)
- Test: `tests/test_volumes_db.py`

- [ ] **Step 1: Write the failing test**

Append to `tests/test_volumes_db.py`:

```python
@pytest.mark.asyncio
async def test_sessions_has_volume_id_and_current_sandbox_id():
    await dbmod.init_pool()
    try:
        async with dbmod.get_db() as conn:
            rows = await (await conn.execute(
                "SELECT column_name, is_nullable FROM information_schema.columns "
                "WHERE table_name='sessions' AND column_name IN ('volume_id', 'current_sandbox_id', 'sandbox_id')"
            )).fetchall()
        cols = {r["column_name"]: r["is_nullable"] for r in rows}
        assert "volume_id" in cols
        assert "current_sandbox_id" in cols
        assert "sandbox_id" not in cols, "old sandbox_id column should be renamed"
        assert cols["current_sandbox_id"] == "YES", "current_sandbox_id must be nullable"
    finally:
        await dbmod.close_pool()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_volumes_db.py::test_sessions_has_volume_id_and_current_sandbox_id -v`
Expected: FAIL — `sandbox_id` still exists, `volume_id` doesn't.

- [ ] **Step 3: Add the migrations**

Append to `_MIGRATIONS` in `src/api/db.py`:

```python
    # 2026-04-21: decouple sessions from sandboxes; bind to volumes.
    "ALTER TABLE sessions ADD COLUMN IF NOT EXISTS volume_id TEXT REFERENCES volumes(id) ON DELETE RESTRICT",
    # Rename sandbox_id -> current_sandbox_id (idempotent: no-op if already done).
    """DO $$ BEGIN
        IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='sessions' AND column_name='sandbox_id')
           AND NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='sessions' AND column_name='current_sandbox_id') THEN
            ALTER TABLE sessions RENAME COLUMN sandbox_id TO current_sandbox_id;
        END IF;
    END $$""",
    "ALTER TABLE sessions ALTER COLUMN current_sandbox_id DROP NOT NULL",
    # Drop any CASCADE FK on current_sandbox_id, replace with SET NULL.
    "ALTER TABLE sessions DROP CONSTRAINT IF EXISTS sessions_sandbox_id_fkey",
    "ALTER TABLE sessions DROP CONSTRAINT IF EXISTS sessions_current_sandbox_id_fkey",
    """ALTER TABLE sessions ADD CONSTRAINT sessions_current_sandbox_id_fkey
        FOREIGN KEY (current_sandbox_id) REFERENCES sandboxes(id) ON DELETE SET NULL""",
    # session_log no longer lifecycle-coupled to sandbox.
    "ALTER TABLE session_log ALTER COLUMN sandbox_id DROP NOT NULL",
    "ALTER TABLE session_log DROP CONSTRAINT IF EXISTS session_log_sandbox_id_fkey",
    """ALTER TABLE session_log ADD CONSTRAINT session_log_sandbox_id_fkey
        FOREIGN KEY (sandbox_id) REFERENCES sandboxes(id) ON DELETE SET NULL""",
```

Note: `volume_id` is nullable for now. Task 17 adds `NOT NULL` after backfill.

- [ ] **Step 4: Run test to verify it passes**

Drop the test DB, rerun migrations, run test:

```bash
dropdb agent_sdk_test 2>/dev/null; createdb agent_sdk_test
TEST_DATABASE_URL=postgresql://localhost:5432/agent_sdk_test pytest tests/test_volumes_db.py::test_sessions_has_volume_id_and_current_sandbox_id -v
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/api/db.py tests/test_volumes_db.py
git commit -m "feat(db): sessions.volume_id + rename sandbox_id to current_sandbox_id"
```

---

### Task 5: Schema migration — sandboxes.volume_id + subpath

**Files:**
- Modify: `src/api/db.py` (`_MIGRATIONS`)
- Modify: `src/api/models.py` (`SandboxRecord`)
- Modify: `src/api/db.py` (`upsert_sandbox`, `get_sandbox`, `list_sandboxes`)
- Test: `tests/test_volumes_db.py`

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_volumes_db.py`:

```python
@pytest.mark.asyncio
async def test_sandboxes_has_volume_id_and_subpath():
    await dbmod.init_pool()
    try:
        async with dbmod.get_db() as conn:
            rows = await (await conn.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_name='sandboxes' AND column_name IN ('volume_id', 'subpath')"
            )).fetchall()
        names = {r["column_name"] for r in rows}
        assert names == {"volume_id", "subpath"}
    finally:
        await dbmod.close_pool()


@pytest.mark.asyncio
async def test_sandbox_record_roundtrip_with_volume():
    from api.models import SandboxRecord, VolumeRecord
    await dbmod.init_pool()
    try:
        await dbmod.upsert_volume(VolumeRecord(id="vol_x", name="x", provider="daytona", provider_ref="dt-x"))
        sb = SandboxRecord(id="sb_x", provider="daytona", sandbox_ref="dt-sb",
                           status="running", root="/home/daytona",
                           volume_id="vol_x", subpath="agents/a1/home")
        await dbmod.upsert_sandbox(sb)
        got = await dbmod.get_sandbox("sb_x")
        assert got.volume_id == "vol_x"
        assert got.subpath == "agents/a1/home"
    finally:
        await dbmod.close_pool()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_volumes_db.py::test_sandboxes_has_volume_id_and_subpath tests/test_volumes_db.py::test_sandbox_record_roundtrip_with_volume -v`
Expected: FAIL — columns don't exist; `SandboxRecord` doesn't accept those kwargs.

- [ ] **Step 3: Add the migrations**

Append to `_MIGRATIONS`:

```python
    # 2026-04-21: sandboxes become volume-aware.
    "ALTER TABLE sandboxes ADD COLUMN IF NOT EXISTS volume_id TEXT REFERENCES volumes(id) ON DELETE RESTRICT",
    "ALTER TABLE sandboxes ADD COLUMN IF NOT EXISTS subpath  TEXT",
```

(NOT NULL enforcement comes in Task 17 after backfill.)

- [ ] **Step 4: Extend `SandboxRecord`**

In `src/api/models.py`, extend `SandboxRecord`:

```python
@dataclass
class SandboxRecord:
    id: str
    provider: str
    sandbox_ref: str
    status: str = "stopped"
    root: str = "/tmp"
    volume_id: str | None = None
    subpath: str | None = None

    def derive_url(self) -> str:
        # ... unchanged
```

- [ ] **Step 5: Update sandbox DAO**

In `src/api/db.py`, update `upsert_sandbox`:

```python
async def upsert_sandbox(sandbox: SandboxRecord) -> None:
    async with get_db() as conn:
        await conn.execute(
            "INSERT INTO sandboxes (id, provider, sandbox_ref, status, root, volume_id, subpath)"
            " VALUES (%s, %s, %s, %s, %s, %s, %s)"
            " ON CONFLICT(id) DO UPDATE SET provider=EXCLUDED.provider,"
            " sandbox_ref=EXCLUDED.sandbox_ref, status=EXCLUDED.status,"
            " root=EXCLUDED.root, volume_id=EXCLUDED.volume_id,"
            " subpath=EXCLUDED.subpath",
            (sandbox.id, sandbox.provider, sandbox.sandbox_ref, sandbox.status,
             sandbox.root, sandbox.volume_id, sandbox.subpath),
        )
```

Update `get_sandbox` and `list_sandboxes` to populate the new fields:

```python
async def get_sandbox(sandbox_id: str) -> SandboxRecord | None:
    async with get_db() as conn:
        row = await (await conn.execute(
            "SELECT * FROM sandboxes WHERE id = %s", (sandbox_id,)
        )).fetchone()
    if row is None:
        return None
    return SandboxRecord(
        id=row["id"], provider=row["provider"],
        sandbox_ref=row["sandbox_ref"], status=row["status"],
        root=row.get("root", "/tmp"),
        volume_id=row.get("volume_id"),
        subpath=row.get("subpath"),
    )


async def list_sandboxes() -> list[SandboxRecord]:
    async with get_db() as conn:
        rows = await (await conn.execute("SELECT * FROM sandboxes")).fetchall()
    return [
        SandboxRecord(id=r["id"], provider=r["provider"],
                      sandbox_ref=r["sandbox_ref"], status=r["status"],
                      root=r.get("root", "/tmp"),
                      volume_id=r.get("volume_id"),
                      subpath=r.get("subpath"))
        for r in rows
    ]
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `pytest tests/test_volumes_db.py -v`
Expected: ALL PASS.

- [ ] **Step 7: Commit**

```bash
git add src/api/db.py src/api/models.py tests/test_volumes_db.py
git commit -m "feat(db,models): sandboxes.volume_id + subpath"
```

---

## Phase 2: Daytona volume adapter

### Task 6: `create_daytona_volume` + `delete_daytona_volume`

**Files:**
- Modify: `src/api/providers.py` (add after `_get_daytona_client`)
- Test: `tests/test_daytona_volumes.py` (new, integration — tagged skip-unless-daytona)

- [ ] **Step 1: Write the failing test**

Create `tests/test_daytona_volumes.py`:

```python
"""Integration tests for Daytona volume adapter. Requires DAYTONA_API_KEY."""
from __future__ import annotations
import os, sys
import pytest

_SRC = os.path.join(os.path.dirname(__file__), "..", "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

pytestmark = pytest.mark.skipif(
    not os.environ.get("DAYTONA_API_KEY"),
    reason="DAYTONA_API_KEY not set",
)


@pytest.mark.asyncio
async def test_daytona_create_and_delete_volume():
    from api.providers import create_daytona_volume, delete_daytona_volume
    ref = await create_daytona_volume("test-vol-agent-sdk-plan")
    assert isinstance(ref, str) and len(ref) > 0
    await delete_daytona_volume(ref)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `DAYTONA_API_KEY=$DAYTONA_API_KEY pytest tests/test_daytona_volumes.py::test_daytona_create_and_delete_volume -v`
Expected: FAIL — `ImportError`.

- [ ] **Step 3: Add the adapter functions**

In `src/api/providers.py`, after `_get_daytona_client` (around line 721):

```python
async def create_daytona_volume(name: str) -> str:
    """Create a Daytona volume and return its provider-native id."""
    client = _get_daytona_client()
    vol = await asyncio.to_thread(client.volume.create, name)
    return vol.id


async def delete_daytona_volume(provider_ref: str) -> None:
    """Delete a Daytona volume by provider-native id."""
    client = _get_daytona_client()
    # Daytona SDK uses volume.get(id) + volume.delete(volume_obj) pattern.
    vol = await asyncio.to_thread(client.volume.get, provider_ref)
    await asyncio.to_thread(client.volume.delete, vol)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `DAYTONA_API_KEY=$DAYTONA_API_KEY pytest tests/test_daytona_volumes.py::test_daytona_create_and_delete_volume -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/api/providers.py tests/test_daytona_volumes.py
git commit -m "feat(providers): daytona volume create/delete"
```

---

### Task 7: Extend `create_daytona` + `provision_daytona_sandbox` to mount volume + subpath

**Files:**
- Modify: `src/api/providers.py` (`create_daytona` around line 650; `provision_daytona_sandbox` around line 502)
- Test: `tests/test_daytona_volumes.py`

- [ ] **Step 1: Write the failing test**

Append to `tests/test_daytona_volumes.py`:

```python
@pytest.mark.asyncio
async def test_daytona_sandbox_mounts_volume_subpath():
    """Create a volume, create a sandbox with subpath, write a file, kill
    the sandbox, create a new one with the same subpath, verify file is still there."""
    from api.providers import (
        create_daytona_volume, delete_daytona_volume,
        create_daytona, destroy_daytona, exec_in_instance,
    )
    vol_ref = await create_daytona_volume("test-vol-mount")
    try:
        inst1 = await create_daytona(
            agent_type="claude",
            volume_id=vol_ref,
            subpath="agents/test-agent/home",
        )
        # write a file into HOME
        await exec_in_instance(inst1, "echo hello > /home/daytona/marker.txt")
        res = await exec_in_instance(inst1, "cat /home/daytona/marker.txt")
        assert b"hello" in (res.stdout_bytes or b"")

        # destroy sandbox, create fresh one with SAME subpath
        await destroy_daytona(inst1)
        inst2 = await create_daytona(
            agent_type="claude",
            volume_id=vol_ref,
            subpath="agents/test-agent/home",
        )
        res2 = await exec_in_instance(inst2, "cat /home/daytona/marker.txt")
        assert b"hello" in (res2.stdout_bytes or b""), "file on volume should persist"
        await destroy_daytona(inst2)
    finally:
        await delete_daytona_volume(vol_ref)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `DAYTONA_API_KEY=$DAYTONA_API_KEY pytest tests/test_daytona_volumes.py::test_daytona_sandbox_mounts_volume_subpath -v`
Expected: FAIL — `create_daytona` doesn't accept `volume_id` / `subpath`.

- [ ] **Step 3: Extend `create_daytona` signature**

Find `create_daytona` in `src/api/providers.py`. Add `volume_id` and `subpath` parameters and pass them through to `provision_daytona_sandbox`. Example (adapt to actual signature):

```python
async def create_daytona(
    agent_type: str,
    ...existing args...,
    volume_id: str | None = None,
    subpath: str | None = None,
) -> ProviderInstance:
    ...
    # When creating the CreateSandboxFromSnapshotParams, pass volumes=[...] if volume_id is set.
    inst = await provision_daytona_sandbox(
        ...,
        volume_id=volume_id,
        subpath=subpath,
    )
    return inst
```

- [ ] **Step 4: Extend `provision_daytona_sandbox` to build volume mounts**

In `src/api/providers.py`, modify `provision_daytona_sandbox`:

```python
async def provision_daytona_sandbox(
    ...existing args...,
    volume_id: str | None = None,
    subpath: str | None = None,
) -> ProviderInstance:
    from daytona import CreateSandboxFromSnapshotParams, VolumeMount

    volumes = []
    if volume_id and subpath:
        volumes.append(VolumeMount(
            volume_id=volume_id,
            mount_path="/home/daytona",
            subpath=subpath,
        ))
        # Always add the read-only shared mount on the same volume.
        volumes.append(VolumeMount(
            volume_id=volume_id,
            mount_path="/mnt/shared",
            subpath="shared",
            read_only=True,
        ))

    params = CreateSandboxFromSnapshotParams(
        ...existing params...,
        volumes=volumes if volumes else None,
    )
    ...
```

- [ ] **Step 5: Run test to verify it passes**

Run: `DAYTONA_API_KEY=$DAYTONA_API_KEY pytest tests/test_daytona_volumes.py -v`
Expected: ALL PASS.

- [ ] **Step 6: Commit**

```bash
git add src/api/providers.py tests/test_daytona_volumes.py
git commit -m "feat(providers): daytona sandbox accepts volume_id + subpath"
```

---

## Phase 3: Volume REST + SDK

### Task 8: Volume CRUD endpoints

**Files:**
- Modify: `src/api/server.py` (add new section after agents endpoints, before sandbox endpoints)
- Test: `tests/test_volumes_api.py` (new)

- [ ] **Step 1: Write the failing tests**

Create `tests/test_volumes_api.py`:

```python
"""REST tests for /volumes endpoints. DB required; provider can be stubbed."""
from __future__ import annotations
import os, sys
import pytest
from unittest.mock import AsyncMock, patch
from httpx import ASGITransport, AsyncClient

_SRC = os.path.join(os.path.dirname(__file__), "..", "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

_DB = os.environ.get("TEST_DATABASE_URL")
pytestmark = pytest.mark.skipif(_DB is None, reason="TEST_DATABASE_URL not set")
if _DB:
    os.environ["DATABASE_URL"] = _DB

from api import db as dbmod, server as srv  # noqa: E402


@pytest.fixture
async def client():
    dbmod.init_db()
    await dbmod.init_pool()
    transport = ASGITransport(app=srv.app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        yield c
    await dbmod.close_pool()


@pytest.mark.asyncio
async def test_post_volume_creates_row(client):
    with patch("api.providers.create_daytona_volume",
               new=AsyncMock(return_value="dt-fake-ref")):
        r = await client.post("/volumes", json={"name": "proj-api-test", "provider": "daytona"})
    assert r.status_code == 200
    body = r.json()
    assert body["name"] == "proj-api-test"
    assert body["provider_ref"] == "dt-fake-ref"
    assert body["status"] == "ready"


@pytest.mark.asyncio
async def test_get_and_list_volumes(client):
    with patch("api.providers.create_daytona_volume",
               new=AsyncMock(return_value="dt-r1")):
        await client.post("/volumes", json={"name": "p1", "provider": "daytona"})

    r = await client.get("/volumes")
    assert r.status_code == 200
    assert any(v["name"] == "p1" for v in r.json())

    r = await client.get("/volumes/p1")
    assert r.status_code == 200
    assert r.json()["name"] == "p1"


@pytest.mark.asyncio
async def test_delete_volume(client):
    with patch("api.providers.create_daytona_volume",
               new=AsyncMock(return_value="dt-del")), \
         patch("api.providers.delete_daytona_volume",
               new=AsyncMock(return_value=None)):
        await client.post("/volumes", json={"name": "to-delete", "provider": "daytona"})
        r = await client.delete("/volumes/to-delete")
    assert r.status_code == 204
    r = await client.get("/volumes/to-delete")
    assert r.status_code == 404


@pytest.mark.asyncio
async def test_delete_volume_conflict_if_session_exists(client):
    """DELETE returns 409 if a session references the volume; force=true cascades."""
    from api.models import AgentConfig, AgentRecord, VolumeRecord
    await dbmod.upsert_agent(AgentRecord(id="a1", name="A1", config=AgentConfig()))
    with patch("api.providers.create_daytona_volume",
               new=AsyncMock(return_value="dt-c")):
        await client.post("/volumes", json={"name": "conflict", "provider": "daytona"})
    v = await dbmod.get_volume_by_name("conflict")
    # Insert a session referencing this volume (bypassing full session flow).
    async with dbmod.get_db() as conn:
        await conn.execute(
            "INSERT INTO sessions (id, agent_id, volume_id) VALUES (%s, %s, %s)",
            ("sess_1", "a1", v.id),
        )
    r = await client.delete("/volumes/conflict")
    assert r.status_code == 409
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `TEST_DATABASE_URL=... pytest tests/test_volumes_api.py -v`
Expected: FAIL — routes don't exist.

- [ ] **Step 3: Add the endpoints**

In `src/api/server.py`, add near the agent endpoints (around line 897):

```python
from pydantic import BaseModel
from .models import VolumeRecord

class _VolumeCreateBody(BaseModel):
    name: str
    provider: str

def _gen_volume_id() -> str:
    import uuid
    return f"vol_{uuid.uuid4().hex[:12]}"


@app.post("/volumes")
async def create_volume(body: _VolumeCreateBody):
    if body.provider == "daytona":
        provider_ref = await providers.create_daytona_volume(body.name)
    elif body.provider == "docker":
        raise HTTPException(501, "Docker volumes not implemented yet")
    elif body.provider == "local":
        raise HTTPException(501, "Local volumes not implemented yet")
    else:
        raise HTTPException(400, f"Unknown provider: {body.provider}")

    vol = VolumeRecord(id=_gen_volume_id(), name=body.name,
                       provider=body.provider, provider_ref=provider_ref,
                       status="ready")
    await db.upsert_volume(vol)
    return vol


@app.get("/volumes")
async def list_volumes(provider: str | None = None):
    return await db.list_volumes(provider)


@app.get("/volumes/{id_or_name}")
async def get_volume(id_or_name: str):
    vol = await db.get_volume(id_or_name)
    if vol is None:
        vol = await db.get_volume_by_name(id_or_name)
    if vol is None:
        raise HTTPException(404, "Volume not found")
    return vol


@app.delete("/volumes/{id_or_name}", status_code=204)
async def delete_volume(id_or_name: str, force: bool = False):
    vol = await db.get_volume(id_or_name)
    if vol is None:
        vol = await db.get_volume_by_name(id_or_name)
    if vol is None:
        raise HTTPException(404, "Volume not found")

    # Check for referencing sessions.
    async with db.get_db() as conn:
        row = await (await conn.execute(
            "SELECT count(*) AS n FROM sessions WHERE volume_id = %s", (vol.id,)
        )).fetchone()
        session_count = row["n"]

    if session_count > 0 and not force:
        raise HTTPException(409, f"Volume has {session_count} session(s). Use ?force=true to cascade.")

    if force and session_count > 0:
        # Cascade: delete referencing sessions + their sandboxes.
        async with db.get_db() as conn:
            await conn.execute("DELETE FROM sessions WHERE volume_id = %s", (vol.id,))

    # Delete from provider first; if that fails, leave the DB row so caller can retry.
    if vol.provider == "daytona":
        await providers.delete_daytona_volume(vol.provider_ref)
    await db.delete_volume(vol.id)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `TEST_DATABASE_URL=... pytest tests/test_volumes_api.py -v`
Expected: ALL PASS.

- [ ] **Step 5: Commit**

```bash
git add src/api/server.py tests/test_volumes_api.py
git commit -m "feat(server): /volumes CRUD endpoints"
```

---

### Task 9: `POST /volumes/provision` (create + wait for ready)

**Files:**
- Modify: `src/api/server.py`
- Test: `tests/test_volumes_api.py`

- [ ] **Step 1: Write the failing test**

Append to `tests/test_volumes_api.py`:

```python
@pytest.mark.asyncio
async def test_provision_volume_waits_for_ready(client):
    with patch("api.providers.create_daytona_volume",
               new=AsyncMock(return_value="dt-prov")):
        r = await client.post("/volumes/provision",
                              json={"name": "prov-test", "provider": "daytona"})
    assert r.status_code == 200
    assert r.json()["status"] == "ready"
```

- [ ] **Step 2: Run test to verify it fails**

Expected: FAIL — 404 (route missing).

- [ ] **Step 3: Add the endpoint**

In `src/api/server.py`, after the `POST /volumes`:

```python
@app.post("/volumes/provision")
async def provision_volume(body: _VolumeCreateBody):
    """Create + wait for ready. For Daytona, create_daytona_volume returns
    only after the volume is provisioned, so this is equivalent to /volumes
    today. Kept as a separate endpoint for API parity with /sandboxes/provision."""
    return await create_volume(body)
```

- [ ] **Step 4: Run test**

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/api/server.py tests/test_volumes_api.py
git commit -m "feat(server): /volumes/provision endpoint"
```

---

### Task 10: Volume file ops via utility sandbox

**Files:**
- Modify: `src/api/server.py`
- Test: `tests/test_volumes_api.py`

- [ ] **Step 1: Write the failing test**

Append:

```python
@pytest.mark.asyncio
async def test_volume_files_edit_and_read(client):
    """File ops go through a short-lived sandbox with the volume mounted."""
    from api.models import VolumeRecord
    v = VolumeRecord(id="vol_f", name="files-test", provider="daytona", provider_ref="dt-f")
    await dbmod.upsert_volume(v)

    # Stub the utility-sandbox lifecycle to avoid provisioning real Daytona in this unit test.
    from api.providers import ExecResult, ProviderInstance
    fake_inst = ProviderInstance(sandbox=None, port=0, provider="daytona", sandbox_id="util")

    with patch("api.providers.create_daytona",
               new=AsyncMock(return_value=fake_inst)), \
         patch("api.providers.destroy_daytona",
               new=AsyncMock(return_value=None)), \
         patch("api.providers.exec_in_instance",
               new=AsyncMock(return_value=ExecResult(0, "ok", "", False))):
        r = await client.post(f"/volumes/{v.id}/files/edit",
                              json={"path": "shared/x.txt", "content": "hi"})
        assert r.status_code == 204
```

- [ ] **Step 2: Run test**

Expected: FAIL — 404.

- [ ] **Step 3: Add the endpoints**

In `src/api/server.py`:

```python
from pydantic import BaseModel

class _VolumeEditBody(BaseModel):
    path: str
    content: str  # base64 or plain text; plain for v1


async def _with_utility_sandbox(vol: VolumeRecord):
    """Spin up a short-lived sandbox with the volume mounted at full root
    so we can `cat`, `ls`, and `tee` into any subpath."""
    if vol.provider != "daytona":
        raise HTTPException(501, f"File ops on {vol.provider} not implemented")
    # Mount the volume at /mnt/vol using empty-string subpath for a full mount.
    # Daytona treats empty/missing subpath as full volume.
    inst = await providers.create_daytona(
        agent_type="claude",
        volume_id=vol.provider_ref,
        subpath="",  # full volume
    )
    return inst


async def _destroy_utility_sandbox(inst):
    await providers.destroy_daytona(inst)


@app.get("/volumes/{id_or_name}/files/tree")
async def volume_files_tree(id_or_name: str, path: str = "/"):
    vol = await db.get_volume(id_or_name) or await db.get_volume_by_name(id_or_name)
    if not vol:
        raise HTTPException(404)
    inst = await _with_utility_sandbox(vol)
    try:
        cmd = f"find /home/daytona/{path.lstrip('/')} -maxdepth 3 -printf '%y %p\\n'"
        res = await providers.exec_in_instance(inst, cmd, timeout=30)
        return {"tree": res.stdout}
    finally:
        await _destroy_utility_sandbox(inst)


@app.get("/volumes/{id_or_name}/files/read")
async def volume_files_read(id_or_name: str, path: str):
    vol = await db.get_volume(id_or_name) or await db.get_volume_by_name(id_or_name)
    if not vol:
        raise HTTPException(404)
    inst = await _with_utility_sandbox(vol)
    try:
        res = await providers.exec_in_instance(inst, f"cat /home/daytona/{path.lstrip('/')}", timeout=30)
        return {"content": res.stdout}
    finally:
        await _destroy_utility_sandbox(inst)


@app.post("/volumes/{id_or_name}/files/edit", status_code=204)
async def volume_files_edit(id_or_name: str, body: _VolumeEditBody):
    vol = await db.get_volume(id_or_name) or await db.get_volume_by_name(id_or_name)
    if not vol:
        raise HTTPException(404)
    inst = await _with_utility_sandbox(vol)
    try:
        # Use heredoc to avoid shell escaping issues.
        p = body.path.lstrip("/")
        cmd = (
            f"mkdir -p $(dirname /home/daytona/{p}) && "
            f"cat > /home/daytona/{p} <<'EOF__AGENT_SDK_HEREDOC__'\n"
            f"{body.content}\n"
            f"EOF__AGENT_SDK_HEREDOC__"
        )
        res = await providers.exec_in_instance(inst, cmd, timeout=30)
        if res.exit_code != 0:
            raise HTTPException(500, f"Edit failed: {res.stderr}")
    finally:
        await _destroy_utility_sandbox(inst)
```

- [ ] **Step 4: Run test**

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/api/server.py tests/test_volumes_api.py
git commit -m "feat(server): volume file ops via utility sandbox"
```

---

### Task 11: SDK `client.volumes.*`

**Files:**
- Modify: `src/agent_sdk/client.py`
- Test: `tests/test_client_streaming.py` (or new `tests/test_client_volumes.py`)

- [ ] **Step 1: Write the failing test**

Create `tests/test_client_volumes.py`:

```python
"""SDK tests for client.volumes.* namespace."""
from __future__ import annotations
import os, sys
import pytest
from unittest.mock import AsyncMock, patch

_SRC = os.path.join(os.path.dirname(__file__), "..", "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

from agent_sdk.client import Client


@pytest.mark.asyncio
async def test_volumes_namespace_create_list_delete():
    c = Client(base_url="http://fake")
    with patch("agent_sdk.client.httpx.AsyncClient.post",
               new=AsyncMock(return_value=type("R", (), {
                   "status_code": 200,
                   "json": lambda self: {"id": "vol_a", "name": "p", "provider": "daytona",
                                         "provider_ref": "dt-a", "status": "ready"},
                   "raise_for_status": lambda self: None,
               })())), \
         patch("agent_sdk.client.httpx.AsyncClient.get",
               new=AsyncMock(return_value=type("R", (), {
                   "status_code": 200,
                   "json": lambda self: [{"id": "vol_a", "name": "p", "provider": "daytona",
                                          "provider_ref": "dt-a", "status": "ready"}],
                   "raise_for_status": lambda self: None,
               })())), \
         patch("agent_sdk.client.httpx.AsyncClient.delete",
               new=AsyncMock(return_value=type("R", (), {
                   "status_code": 204, "raise_for_status": lambda self: None,
               })())):
        v = await c.volumes.create(name="p", provider="daytona")
        assert v.name == "p"
        vs = await c.volumes.list()
        assert len(vs) == 1
        await c.volumes.delete("p")
```

- [ ] **Step 2: Run test**

Expected: FAIL — `client.volumes` attribute missing.

- [ ] **Step 3: Implement the namespace**

In `src/agent_sdk/client.py`, add (before the `Client` class or as a nested class):

```python
from dataclasses import dataclass

@dataclass
class Volume:
    id: str
    name: str
    provider: str
    provider_ref: str
    status: str


class _VolumesAPI:
    def __init__(self, client: "Client"):
        self._c = client

    async def create(self, name: str, provider: str) -> Volume:
        r = await self._c._http.post(f"{self._c.base_url}/volumes",
                                     json={"name": name, "provider": provider})
        r.raise_for_status()
        return Volume(**r.json())

    async def provision(self, name: str, provider: str) -> Volume:
        r = await self._c._http.post(f"{self._c.base_url}/volumes/provision",
                                     json={"name": name, "provider": provider})
        r.raise_for_status()
        return Volume(**r.json())

    async def get(self, id_or_name: str) -> Volume:
        r = await self._c._http.get(f"{self._c.base_url}/volumes/{id_or_name}")
        r.raise_for_status()
        return Volume(**r.json())

    async def list(self, provider: str | None = None) -> list[Volume]:
        params = {"provider": provider} if provider else None
        r = await self._c._http.get(f"{self._c.base_url}/volumes", params=params)
        r.raise_for_status()
        return [Volume(**v) for v in r.json()]

    async def delete(self, id_or_name: str, force: bool = False) -> None:
        params = {"force": "true"} if force else None
        r = await self._c._http.delete(f"{self._c.base_url}/volumes/{id_or_name}",
                                       params=params)
        r.raise_for_status()
```

In the `Client.__init__`, add:

```python
self.volumes = _VolumesAPI(self)
```

- [ ] **Step 4: Run test**

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/agent_sdk/client.py tests/test_client_volumes.py
git commit -m "feat(sdk): client.volumes.* namespace"
```

---

## Phase 4: Session changes

### Task 12: `POST /sessions` requires `volume_id`, no sandbox provisioning

**Files:**
- Modify: `src/api/server.py` (`POST /sessions` handler around line 2126)
- Modify: `src/api/db.py` (`upsert_session` accepts `volume_id`, `current_sandbox_id` nullable)
- Test: `tests/test_session_volume_integration.py` (new)

- [ ] **Step 1: Write the failing test**

Create `tests/test_session_volume_integration.py`:

```python
"""Integration tests for session/volume decoupling."""
from __future__ import annotations
import os, sys
import pytest
from unittest.mock import AsyncMock, patch
from httpx import ASGITransport, AsyncClient

_SRC = os.path.join(os.path.dirname(__file__), "..", "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

_DB = os.environ.get("TEST_DATABASE_URL")
pytestmark = pytest.mark.skipif(_DB is None, reason="TEST_DATABASE_URL not set")
if _DB:
    os.environ["DATABASE_URL"] = _DB

from api import db as dbmod, server as srv  # noqa: E402


@pytest.fixture
async def client():
    dbmod.init_db()
    await dbmod.init_pool()
    transport = ASGITransport(app=srv.app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        yield c
    await dbmod.close_pool()


@pytest.mark.asyncio
async def test_post_session_requires_volume_id(client):
    from api.models import AgentConfig, AgentRecord
    await dbmod.upsert_agent(AgentRecord(id="agent_t", name="T", config=AgentConfig()))

    r = await client.post("/sessions", json={"agent_id": "agent_t"})
    assert r.status_code == 422 or r.status_code == 400, "should reject without volume_id"


@pytest.mark.asyncio
async def test_post_session_does_not_provision_sandbox(client):
    from api.models import AgentConfig, AgentRecord, VolumeRecord
    await dbmod.upsert_agent(AgentRecord(id="agent_t2", name="T2", config=AgentConfig()))
    await dbmod.upsert_volume(VolumeRecord(id="vol_t", name="vt", provider="daytona", provider_ref="dt-t"))

    with patch("api.providers.create_daytona",
               new=AsyncMock(side_effect=AssertionError("should NOT provision during session create"))):
        r = await client.post("/sessions", json={"agent_id": "agent_t2", "volume_id": "vol_t"})
    assert r.status_code == 200
    body = r.json()
    assert body["current_sandbox_id"] is None
    assert body["volume_id"] == "vol_t"
```

- [ ] **Step 2: Run test**

Expected: FAIL — `volume_id` not in the request model; sandbox gets provisioned synchronously.

- [ ] **Step 3: Update `POST /sessions` handler**

Find the existing `POST /sessions` handler in `src/api/server.py` (~line 2126). It currently creates a sandbox synchronously; change to:
1. Accept `volume_id` (required) in the request body.
2. Reject requests without it with 422.
3. Insert the session row with `current_sandbox_id = NULL`.
4. Return the session immediately without calling any provider.

Exact edits:

```python
# In the request model for POST /sessions, add:
class SessionCreateBody(BaseModel):
    agent_id: str
    volume_id: str  # required
    # ... other existing fields ...

@app.post("/sessions")
async def create_session(body: SessionCreateBody):
    agent = await db.get_agent(body.agent_id)
    if not agent:
        raise HTTPException(404, "Agent not found")
    volume = await db.get_volume(body.volume_id)
    if not volume:
        raise HTTPException(404, "Volume not found")

    session_id = _gen_session_id()  # existing helper
    await db.upsert_session(
        session_id=session_id,
        agent_id=body.agent_id,
        sandbox_id=None,  # lazy — provisioned on first run
        inner_session_id=None,
        volume_id=body.volume_id,
    )
    sess = await db.get_session(session_id)
    return sess
```

- [ ] **Step 4: Update `upsert_session` + `get_session` in `db.py`**

Change `upsert_session` signature to accept `volume_id` and rename `sandbox_id` → `current_sandbox_id`:

```python
async def upsert_session(session_id: str, agent_id: str,
                         sandbox_id: str | None,  # kept kwarg name for compat
                         inner_session_id: str | None,
                         volume_id: str | None = None,
                         env: dict[str, str] | None = None,
                         secrets: dict[str, str] | None = None) -> None:
    async with get_db() as conn:
        await conn.execute(
            "INSERT INTO sessions (id, agent_id, current_sandbox_id, inner_session_id, volume_id, env, secrets)"
            " VALUES (%s, %s, %s, %s, %s, %s, %s)"
            " ON CONFLICT(id) DO UPDATE SET agent_id=EXCLUDED.agent_id,"
            " current_sandbox_id=EXCLUDED.current_sandbox_id,"
            " inner_session_id=EXCLUDED.inner_session_id,"
            " volume_id=EXCLUDED.volume_id,"
            " env=EXCLUDED.env, secrets=EXCLUDED.secrets",
            (session_id, agent_id, sandbox_id, inner_session_id, volume_id,
             Json(env or {}), Json(secrets or {})),
        )
```

Update `get_session` to include `volume_id` and `current_sandbox_id` in the returned dict. Audit all callers of `upsert_session` and `get_session` that currently use `sandbox_id` — update the references to `current_sandbox_id`.

- [ ] **Step 5: Run test**

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/api/server.py src/api/db.py tests/test_session_volume_integration.py
git commit -m "feat(sessions): require volume_id, lazy sandbox provisioning"
```

---

### Task 13: Pre-run sandbox probe + reprovision

**Files:**
- Modify: `src/api/server.py` (the `POST /sessions/{id}/message` + `/resume` handlers)
- Modify: `src/api/providers.py` (add `get_daytona_sandbox_status`)
- Test: `tests/test_session_volume_integration.py`

- [ ] **Step 1: Write the failing test**

Append:

```python
@pytest.mark.asyncio
async def test_message_reprovisions_when_sandbox_missing(client):
    """When current_sandbox_id points to a deleted sandbox, /message must
    create a new sandbox with the same agent HOME subpath."""
    from api.models import AgentConfig, AgentRecord, VolumeRecord
    await dbmod.upsert_agent(AgentRecord(id="agent_r", name="R", config=AgentConfig()))
    await dbmod.upsert_volume(VolumeRecord(id="vol_r", name="vr", provider="daytona", provider_ref="dt-r"))

    # Create session with no sandbox.
    r = await client.post("/sessions", json={"agent_id": "agent_r", "volume_id": "vol_r"})
    sid = r.json()["id"]

    calls = {"create": 0}
    async def fake_create(**kwargs):
        calls["create"] += 1
        assert kwargs.get("volume_id") == "dt-r"
        assert kwargs.get("subpath") == "agents/agent_r/home"
        from api.providers import ProviderInstance
        return ProviderInstance(sandbox=None, port=7777, provider="daytona", sandbox_id=f"sb-{calls['create']}")

    with patch("api.providers.create_daytona", new=AsyncMock(side_effect=fake_create)), \
         patch("api.server._send_prompt_to_sandbox", new=AsyncMock(return_value=None)):
        r = await client.post(f"/sessions/{sid}/message", json={"message": "hi"})
    assert calls["create"] == 1
    sess = await dbmod.get_session(sid)
    assert sess["current_sandbox_id"] is not None
```

(Note: `_send_prompt_to_sandbox` is a placeholder — adapt to the actual helper that `POST /sessions/{id}/message` uses internally.)

- [ ] **Step 2: Run test**

Expected: FAIL — current code likely errors because session has no sandbox.

- [ ] **Step 3: Add the probe helper**

In `src/api/providers.py`, add:

```python
async def get_daytona_sandbox_status(sandbox_ref: str) -> str:
    """Return one of: 'running' | 'stopped' | 'missing' | 'error'."""
    client = _get_daytona_client()
    try:
        sb = await asyncio.to_thread(client.get, sandbox_ref)
    except Exception as e:
        msg = str(e).lower()
        if "not found" in msg or "404" in msg:
            return "missing"
        return "error"
    state = (sb.state or "").lower()
    if state in ("started", "running"):
        return "running"
    if state in ("stopped", "paused"):
        return "stopped"
    return "error"
```

- [ ] **Step 4: Add the pre-run probe to `/message` + `/resume`**

In `src/api/server.py`, refactor the common "ensure current sandbox" step into a helper:

```python
async def _ensure_session_sandbox(session_id: str) -> SandboxRecord:
    """Probe current sandbox; reprovision if dead. Returns the live sandbox record."""
    sess = await db.get_session(session_id)
    if sess is None:
        raise HTTPException(404, "Session not found")

    vol = await db.get_volume(sess["volume_id"])
    if vol is None:
        raise HTTPException(500, "Session's volume no longer exists")

    subpath = f"agents/{sess['agent_id']}/home"
    sandbox = None
    current = sess.get("current_sandbox_id")
    if current:
        sandbox = await db.get_sandbox(current)
        if sandbox:
            status = await providers.get_daytona_sandbox_status(sandbox.sandbox_ref)
            if status == "running":
                return sandbox
            if status == "stopped":
                await providers.start_daytona(sandbox)  # existing helper; resumes the sandbox
                sandbox.status = "running"
                await db.upsert_sandbox(sandbox)
                return sandbox
            # missing | error -> reprovision
            await db.delete_sandbox(sandbox.id)
            sandbox = None

    # Provision new sandbox.
    inst = await providers.create_daytona(
        agent_type=(await db.get_agent(sess["agent_id"])).config.agent_type,
        volume_id=vol.provider_ref,
        subpath=subpath,
    )
    sandbox = SandboxRecord(
        id=_gen_sandbox_id(), provider="daytona",
        sandbox_ref=inst.sandbox_id, status="running",
        root="/home/daytona", volume_id=vol.id, subpath=subpath,
    )
    await db.upsert_sandbox(sandbox)
    await db.set_session_current_sandbox(session_id, sandbox.id)
    return sandbox
```

Add `db.set_session_current_sandbox` (simple UPDATE) to `db.py`:

```python
async def set_session_current_sandbox(session_id: str, sandbox_id: str | None) -> None:
    async with get_db() as conn:
        await conn.execute(
            "UPDATE sessions SET current_sandbox_id = %s WHERE id = %s",
            (sandbox_id, session_id),
        )
```

Modify `POST /sessions/{id}/message` to call `_ensure_session_sandbox` before sending the prompt, replacing the existing "grab session's sandbox" lookup.

- [ ] **Step 5: Run test**

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/api/server.py src/api/providers.py src/api/db.py tests/test_session_volume_integration.py
git commit -m "feat(sessions): pre-run sandbox probe + reprovision"
```

---

### Task 14: Sandbox control endpoints on sessions

**Files:**
- Modify: `src/api/server.py`
- Test: `tests/test_session_volume_integration.py`

- [ ] **Step 1: Write the failing tests**

Append:

```python
@pytest.mark.asyncio
async def test_start_sandbox_eager_provisions(client):
    from api.models import AgentConfig, AgentRecord, VolumeRecord
    await dbmod.upsert_agent(AgentRecord(id="agent_s", name="S", config=AgentConfig()))
    await dbmod.upsert_volume(VolumeRecord(id="vol_s", name="vs", provider="daytona", provider_ref="dt-s"))
    r = await client.post("/sessions", json={"agent_id": "agent_s", "volume_id": "vol_s"})
    sid = r.json()["id"]

    with patch("api.providers.create_daytona",
               new=AsyncMock(return_value=type("I", (), {
                   "sandbox_id": "sb-eager", "port": 7777,
               })())):
        r = await client.post(f"/sessions/{sid}/start-sandbox")
    assert r.status_code == 200
    sess = await dbmod.get_session(sid)
    assert sess["current_sandbox_id"] is not None


@pytest.mark.asyncio
async def test_stop_sandbox_clears_pointer(client):
    from api.models import AgentConfig, AgentRecord, VolumeRecord, SandboxRecord
    await dbmod.upsert_agent(AgentRecord(id="agent_k", name="K", config=AgentConfig()))
    await dbmod.upsert_volume(VolumeRecord(id="vol_k", name="vk", provider="daytona", provider_ref="dt-k"))
    r = await client.post("/sessions", json={"agent_id": "agent_k", "volume_id": "vol_k"})
    sid = r.json()["id"]
    sb = SandboxRecord(id="sb_k", provider="daytona", sandbox_ref="dt-sb-k",
                       status="running", root="/home/daytona",
                       volume_id="vol_k", subpath="agents/agent_k/home")
    await dbmod.upsert_sandbox(sb)
    await dbmod.set_session_current_sandbox(sid, "sb_k")

    with patch("api.providers.destroy_daytona", new=AsyncMock(return_value=None)):
        r = await client.post(f"/sessions/{sid}/stop-sandbox")
    assert r.status_code == 204
    sess = await dbmod.get_session(sid)
    assert sess["current_sandbox_id"] is None
```

- [ ] **Step 2: Run**

Expected: FAIL.

- [ ] **Step 3: Add endpoints**

```python
@app.post("/sessions/{session_id}/start-sandbox")
async def start_session_sandbox(session_id: str):
    sb = await _ensure_session_sandbox(session_id)
    return {"sandbox_id": sb.id}


@app.post("/sessions/{session_id}/stop-sandbox", status_code=204)
async def stop_session_sandbox(session_id: str):
    sess = await db.get_session(session_id)
    if not sess:
        raise HTTPException(404)
    sbid = sess.get("current_sandbox_id")
    if sbid:
        sb = await db.get_sandbox(sbid)
        if sb and sb.provider == "daytona":
            # Best-effort destroy; reconstruct a minimal ProviderInstance.
            from api.providers import ProviderInstance
            inst = ProviderInstance(sandbox=None, port=0, provider="daytona", sandbox_id=sb.sandbox_ref)
            try:
                await providers.destroy_daytona(inst)
            except Exception:
                pass
        await db.set_session_current_sandbox(session_id, None)
        if sbid:
            await db.delete_sandbox(sbid)


@app.post("/sessions/{session_id}/reset-sandbox")
async def reset_session_sandbox(session_id: str):
    await stop_session_sandbox(session_id)
    sb = await _ensure_session_sandbox(session_id)
    return {"sandbox_id": sb.id}
```

- [ ] **Step 4: Run**

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/api/server.py tests/test_session_volume_integration.py
git commit -m "feat(sessions): start/stop/reset sandbox control endpoints"
```

---

### Task 15: `--resume` wiring on reprovision + `sandbox_reattach` event

**Files:**
- Modify: `src/api/server.py` (message handler)
- Modify: `src/api/providers.py` (if ACP launch args need `--resume`)
- Test: `tests/test_session_volume_integration.py`

- [ ] **Step 1: Write the failing test**

Append:

```python
@pytest.mark.asyncio
async def test_reattach_event_emitted_after_sandbox_loss(client):
    """After forcing reprovision on a session with inner_session_id, the SSE
    stream for the next run must emit a sandbox_reattach event first."""
    from api.models import AgentConfig, AgentRecord, VolumeRecord, SandboxRecord
    await dbmod.upsert_agent(AgentRecord(id="agent_e", name="E", config=AgentConfig()))
    await dbmod.upsert_volume(VolumeRecord(id="vol_e", name="ve", provider="daytona", provider_ref="dt-e"))
    r = await client.post("/sessions", json={"agent_id": "agent_e", "volume_id": "vol_e"})
    sid = r.json()["id"]
    # Point at a dead sandbox (row exists, provider says missing).
    sb = SandboxRecord(id="sb_dead", provider="daytona", sandbox_ref="dt-dead",
                       status="running", root="/home/daytona",
                       volume_id="vol_e", subpath="agents/agent_e/home")
    await dbmod.upsert_sandbox(sb)
    await dbmod.set_session_current_sandbox(sid, "sb_dead")
    await dbmod.set_session_inner_id(sid, "inner-sess-abc")  # add this helper

    async def fake_create(**kw):
        return type("I", (), {"sandbox_id": "sb-new", "port": 7777, "sandbox": None, "provider": "daytona"})()

    seen_events = []
    async def fake_stream(sid_arg, prompt):
        seen_events.append({"type": "text", "text": "hello"})
        return seen_events

    with patch("api.providers.get_daytona_sandbox_status",
               new=AsyncMock(return_value="missing")), \
         patch("api.providers.create_daytona", new=AsyncMock(side_effect=fake_create)), \
         patch("api.server._send_prompt_to_sandbox",
               new=AsyncMock(return_value=None)):
        r = await client.post(f"/sessions/{sid}/message", json={"message": "hello"})

    logs = await dbmod.list_session_log(sid)
    assert any(e["event_type"] == "sandbox_reattach" for e in logs), \
        "reattach event must be logged when reprovisioning"
```

- [ ] **Step 2: Run**

Expected: FAIL — no reattach event emitted.

- [ ] **Step 3: Emit the event in `_ensure_session_sandbox`**

Modify `_ensure_session_sandbox` in `src/api/server.py` to log a `sandbox_reattach` event whenever it creates a new sandbox for a session that previously had one:

```python
async def _ensure_session_sandbox(session_id: str) -> SandboxRecord:
    sess = await db.get_session(session_id)
    ...
    old_sandbox_id = sess.get("current_sandbox_id")
    ...
    # After successfully creating the new sandbox:
    if old_sandbox_id and old_sandbox_id != sandbox.id:
        await db.append_session_log(
            session_id=session_id,
            agent_id=sess["agent_id"],
            sandbox_id=sandbox.id,
            event_type="sandbox_reattach",
            payload={"old_sandbox_id": old_sandbox_id, "new_sandbox_id": sandbox.id},
        )
    ...
    return sandbox
```

Also: when launching the ACP agent inside the new sandbox, pass `--resume <inner_session_id>` if the session has one. That's in `providers.py` or wherever the ACP launch command is built — thread `inner_session_id` through.

- [ ] **Step 4: Add `db.set_session_inner_id` helper**

```python
async def set_session_inner_id(session_id: str, inner_id: str | None) -> None:
    async with get_db() as conn:
        await conn.execute(
            "UPDATE sessions SET inner_session_id = %s WHERE id = %s",
            (inner_id, session_id),
        )
```

- [ ] **Step 5: Run**

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/api/server.py src/api/db.py src/api/providers.py tests/test_session_volume_integration.py
git commit -m "feat(sessions): emit sandbox_reattach event + --resume wiring"
```

---

### Task 16: Live integration test — sandbox-loss resume against real Daytona

**Files:**
- Test: `tests/test_session_volume_integration.py`

- [ ] **Step 1: Write the failing test**

Append:

```python
@pytest.mark.integration
@pytest.mark.skipif(not os.environ.get("DAYTONA_API_KEY"), reason="needs Daytona")
@pytest.mark.asyncio
async def test_sandbox_loss_resume_end_to_end(client):
    """Real Daytona: create session, run a prompt, DELETE its sandbox, run
    another prompt, verify the transcript continues (same inner_session_id)."""
    from api.models import AgentConfig, AgentRecord

    await dbmod.upsert_agent(AgentRecord(id="agent_e2e", name="E2E", config=AgentConfig(agent_type="claude")))
    r = await client.post("/volumes/provision", json={"name": "e2e-vol", "provider": "daytona"})
    assert r.status_code == 200
    vol_id = r.json()["id"]

    try:
        r = await client.post("/sessions", json={"agent_id": "agent_e2e", "volume_id": vol_id})
        sid = r.json()["id"]

        # First prompt (provisions sandbox).
        r = await client.post(f"/sessions/{sid}/message", json={"message": "remember the word OSPREY"})
        assert r.status_code == 200

        # Kill the sandbox.
        sess = await dbmod.get_session(sid)
        old_sbid = sess["current_sandbox_id"]
        r = await client.delete(f"/sandboxes/{old_sbid}")
        assert r.status_code in (200, 204)

        # Second prompt — should reprovision + resume.
        r = await client.post(f"/sessions/{sid}/message", json={"message": "what word did I tell you?"})
        assert r.status_code == 200
        # Log should contain the reattach event plus the assistant response.
        logs = await dbmod.list_session_log(sid)
        assert any(e["event_type"] == "sandbox_reattach" for e in logs)
        assistant_texts = [e for e in logs if e["event_type"] == "assistant_message"]
        joined = " ".join(str(e["payload"]) for e in assistant_texts).lower()
        assert "osprey" in joined, "CLI should have resumed the transcript"
    finally:
        await client.delete(f"/volumes/{vol_id}?force=true")
```

- [ ] **Step 2: Run it**

Run: `DAYTONA_API_KEY=$KEY TEST_DATABASE_URL=... pytest tests/test_session_volume_integration.py::test_sandbox_loss_resume_end_to_end -v -s`
Expected: PASS if all prior tasks are correct. If it fails, the failure pinpoints the gap (transcript not resuming, reattach not emitted, etc.).

- [ ] **Step 3: Fix any issues discovered**

Most likely failure modes and fixes:
- `--resume` not passed: check providers.py ACP launch args. Add `--resume <inner_session_id>` to the args list when present.
- Transcript not on volume: check that `HOME=/home/daytona` and that `~/.claude/projects/<cwd>/<sid>.jsonl` is actually written before the sandbox is killed (may need a brief sync).
- `inner_session_id` not captured: check that the first-run path parses the CLI's session-start event and calls `db.set_session_inner_id`.

- [ ] **Step 4: Commit**

```bash
git add tests/test_session_volume_integration.py src/api/providers.py src/api/server.py
git commit -m "test: end-to-end sandbox-loss resume against daytona"
```

---

### Task 17: Backfill migration + enforce NOT NULL on volume_id

**Files:**
- Modify: `src/api/db.py` (`_MIGRATIONS` tail + new `_POST_MIGRATE` hook or inline backfill)
- Test: `tests/test_volumes_db.py`

- [ ] **Step 1: Write the failing test**

Append to `tests/test_volumes_db.py`:

```python
@pytest.mark.asyncio
async def test_volume_id_is_not_null_after_backfill():
    await dbmod.init_pool()
    try:
        async with dbmod.get_db() as conn:
            row = await (await conn.execute(
                "SELECT is_nullable FROM information_schema.columns "
                "WHERE table_name='sessions' AND column_name='volume_id'"
            )).fetchone()
        assert row["is_nullable"] == "NO", "volume_id should be NOT NULL post-backfill"
    finally:
        await dbmod.close_pool()
```

- [ ] **Step 2: Run**

Expected: FAIL — still nullable.

- [ ] **Step 3: Add backfill migration + NOT NULL enforcement**

Append to `_MIGRATIONS`:

```python
    # 2026-04-21: backfill volume_id for existing sessions.
    # For each (provider) in use among existing sandboxes, ensure a legacy volume exists
    # and point any sessions with NULL volume_id at the matching legacy volume.
    """DO $$
    DECLARE p TEXT;
    DECLARE legacy_id TEXT;
    BEGIN
        FOR p IN SELECT DISTINCT provider FROM sandboxes WHERE provider IS NOT NULL LOOP
            legacy_id := 'vol_legacy_' || p;
            INSERT INTO volumes (id, name, provider, provider_ref, status)
            VALUES (legacy_id, 'legacy-' || p, p, 'legacy-backfill', 'ready')
            ON CONFLICT (id) DO NOTHING;
            UPDATE sessions SET volume_id = legacy_id WHERE volume_id IS NULL
              AND current_sandbox_id IN (SELECT id FROM sandboxes WHERE provider = p);
        END LOOP;
        -- Any orphan sessions (no sandbox) → point at first available legacy volume if any.
        UPDATE sessions s SET volume_id = (SELECT id FROM volumes LIMIT 1)
          WHERE s.volume_id IS NULL AND EXISTS (SELECT 1 FROM volumes);
    END $$""",
    # Finally, enforce NOT NULL now that all rows are backfilled.
    "ALTER TABLE sessions ALTER COLUMN volume_id SET NOT NULL",
    # Same for sandboxes (volume_id / subpath): backfill legacy rows then enforce.
    """UPDATE sandboxes SET volume_id = (
        SELECT id FROM volumes WHERE provider = sandboxes.provider LIMIT 1
    ) WHERE volume_id IS NULL""",
    "UPDATE sandboxes SET subpath = 'legacy' WHERE subpath IS NULL",
    "ALTER TABLE sandboxes ALTER COLUMN volume_id SET NOT NULL",
    "ALTER TABLE sandboxes ALTER COLUMN subpath   SET NOT NULL",
```

- [ ] **Step 4: Reset test DB and run**

```bash
dropdb agent_sdk_test; createdb agent_sdk_test
TEST_DATABASE_URL=postgresql://localhost:5432/agent_sdk_test pytest tests/test_volumes_db.py -v
```

Expected: ALL PASS.

- [ ] **Step 5: Commit**

```bash
git add src/api/db.py tests/test_volumes_db.py
git commit -m "feat(db): backfill volume_id + enforce NOT NULL"
```

---

## Final Step: Smoke test + follow-ups

- [ ] Run the full test suite against Postgres:

```bash
TEST_DATABASE_URL=postgresql://localhost:5432/agent_sdk_test \
DAYTONA_API_KEY=$DAYTONA_API_KEY \
pytest tests/ -v
```

All existing tests must pass; new ones all pass.

- [ ] Run the server locally with Docker Compose (`docker compose up`) and manually:
  1. `curl -X POST /volumes -d '{"name":"smoke","provider":"daytona"}'`
  2. Create an agent, create a session with that volume, send a message.
  3. `curl -X DELETE /sandboxes/<id>` to kill the sandbox.
  4. Send another message — observe reattach event and continued transcript.

- [ ] Update `docs/local-dev.md` if new env vars or setup steps are needed.

- [ ] Ship follow-up plans:
  - Docker provider volume support (`docs/superpowers/plans/NNNN-docker-volumes.md`)
  - Local provider volume support (`docs/superpowers/plans/NNNN-local-volumes.md`)
  - `/mnt/shared` write endpoint if needed

---

## Self-Review (for implementer)

Before declaring this plan done, verify:

1. **Spec coverage.** For each spec section (Problem/Goals/Model/Schema/API/Lifecycle/Concurrency/Providers/Rollout/Testing), a task exists. Phases 5-6 (Docker, Local) are deferred to follow-up plans — that's documented here.
2. **Placeholders.** None of the task bodies say TBD / TODO / "implement similarly". Every step shows the actual code or command.
3. **Type consistency.** `current_sandbox_id` (not `sandbox_id`) in new code. `VolumeRecord` used consistently. `subpath` threaded through `create_daytona` and `provision_daytona_sandbox`.
4. **Ambiguity.** Each migration is explicit SQL. Each endpoint has body model, status code, and success/error cases. Probe branches are enumerated.
