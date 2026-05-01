"""Server-side database: Postgres schema, connection pool, and query functions.

Uses psycopg v3 + psycopg_pool, matching the pattern in ~/hive/src/hive/server/db.py.
"""

from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Json
from psycopg_pool import AsyncConnectionPool

from .models import AgentConfig, AgentRecord, LogEntry, VolumeRecord

log = logging.getLogger(__name__)

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://localhost:5432/agent_sdk_server")

_PG_SCHEMA = [
    """CREATE TABLE IF NOT EXISTS agents (
        id      TEXT PRIMARY KEY,
        name    TEXT,
        config  JSONB
    )""",
    """CREATE TABLE IF NOT EXISTS volumes (
        id            TEXT PRIMARY KEY,
        name          TEXT NOT NULL UNIQUE,
        provider      TEXT NOT NULL,
        provider_ref  TEXT NOT NULL,
        status        TEXT NOT NULL DEFAULT 'ready',
        created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
    )""",
    # No sandboxes table — provider sandbox identity lives in
    # ``sessions.sandbox_state`` JSONB (single source of truth, owned by
    # SessionPool).
    """CREATE TABLE IF NOT EXISTS sessions (
        id                  TEXT PRIMARY KEY,
        agent_id            TEXT NOT NULL REFERENCES agents(id) ON DELETE CASCADE,
        inner_session_id    TEXT,
        env                 JSONB NOT NULL DEFAULT '{}'::jsonb,
        secrets             JSONB NOT NULL DEFAULT '{}'::jsonb,
        created_at          TIMESTAMPTZ NOT NULL DEFAULT now()
    )""",
    """CREATE TABLE IF NOT EXISTS session_log (
        id          SERIAL PRIMARY KEY,
        session_id  TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
        agent_id    TEXT NOT NULL REFERENCES agents(id) ON DELETE CASCADE,
        event_type  TEXT NOT NULL,
        payload     JSONB NOT NULL,
        created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
    )""",
    "CREATE INDEX IF NOT EXISTS idx_sessions_agent ON sessions(agent_id)",
    "CREATE INDEX IF NOT EXISTS idx_session_log_session ON session_log(session_id, created_at)",
    "CREATE INDEX IF NOT EXISTS idx_session_log_agent ON session_log(agent_id, created_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_volumes_name ON volumes(name)",
]

# ---------------------------------------------------------------------------
# Migrations — idempotent ALTER statements applied after DDL on every startup.
# Use IF EXISTS / IF NOT EXISTS so they're safe to run repeatedly and on
# fresh databases alike. Append new migrations to the bottom.
# ---------------------------------------------------------------------------
_MIGRATIONS = [
    # 2026-04-12: drop unused sandboxes columns from earlier design
    "ALTER TABLE sandboxes DROP COLUMN IF EXISTS name",
    "ALTER TABLE sandboxes DROP COLUMN IF EXISTS image",
    "ALTER TABLE sandboxes DROP COLUMN IF EXISTS auto_stop_min",
    "ALTER TABLE sandboxes DROP COLUMN IF EXISTS labels",
    "ALTER TABLE sandboxes DROP COLUMN IF EXISTS env_vars",
    "ALTER TABLE sandboxes DROP COLUMN IF EXISTS resources",
    "ALTER TABLE sandboxes DROP COLUMN IF EXISTS agent_count",
    "ALTER TABLE sandboxes DROP COLUMN IF EXISTS last_activity",
    "ALTER TABLE sandboxes DROP COLUMN IF EXISTS error_message",
    "ALTER TABLE sandboxes DROP COLUMN IF EXISTS updated_at",
    # 2026-04-16: add root column for sandbox filesystem boundary
    "ALTER TABLE sandboxes ADD COLUMN IF NOT EXISTS root TEXT NOT NULL DEFAULT '/tmp'",
    # 2026-04-21: per-session env (identity, non-secret) and secrets.
    # Secrets are plaintext JSONB for now — see SECRETS_PLAINTEXT tech-debt
    # note in models.py. Phase 2 adds envelope encryption.
    "ALTER TABLE sessions ADD COLUMN IF NOT EXISTS env JSONB NOT NULL DEFAULT '{}'::jsonb",
    "ALTER TABLE sessions ADD COLUMN IF NOT EXISTS secrets JSONB NOT NULL DEFAULT '{}'::jsonb",
    # 2026-04-21: decouple sessions from sandboxes; bind to volumes.
    "ALTER TABLE sessions ADD COLUMN IF NOT EXISTS volume_id TEXT REFERENCES volumes(id) ON DELETE RESTRICT",
    # Rename sandbox_id -> current_sandbox_id (idempotent: no-op if already done).
    """DO $$ BEGIN
        IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='sessions' AND column_name='sandbox_id')
           AND NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='sessions' AND column_name='current_sandbox_id') THEN
            ALTER TABLE sessions RENAME COLUMN sandbox_id TO current_sandbox_id;
        END IF;
    END $$""",
    # Drop the old index (created by _PG_SCHEMA on sandbox_id) and recreate on
    # current_sandbox_id. DROP IF EXISTS is safe if it was never created.
    "DROP INDEX IF EXISTS idx_sessions_sandbox",
    "CREATE INDEX IF NOT EXISTS idx_sessions_current_sandbox ON sessions(current_sandbox_id)",
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
    # 2026-04-21: sandboxes become volume-aware.
    "ALTER TABLE sandboxes ADD COLUMN IF NOT EXISTS volume_id TEXT REFERENCES volumes(id) ON DELETE RESTRICT",
    "ALTER TABLE sandboxes ADD COLUMN IF NOT EXISTS subpath  TEXT",
    # 2026-04-21: backfill volume_id + subpath for pre-existing rows, then enforce NOT NULL.
    # Create a "legacy" volume per distinct provider in the sandboxes table.
    """DO $$
    DECLARE p TEXT;
    DECLARE legacy_id TEXT;
    BEGIN
        -- Providers from sandboxes
        FOR p IN SELECT DISTINCT provider FROM sandboxes WHERE provider IS NOT NULL LOOP
            legacy_id := 'vol_legacy_' || p;
            INSERT INTO volumes (id, name, provider, provider_ref, status)
            VALUES (legacy_id, 'legacy-' || p, p, 'legacy-backfill', 'ready')
            ON CONFLICT (id) DO NOTHING;
        END LOOP;
        -- If there are no sandboxes but there are sessions with NULL volume_id,
        -- ensure at least one legacy volume exists so the backfill has a target.
        IF EXISTS (SELECT 1 FROM sessions WHERE volume_id IS NULL)
           AND NOT EXISTS (SELECT 1 FROM volumes) THEN
            INSERT INTO volumes (id, name, provider, provider_ref, status)
            VALUES ('vol_legacy_daytona', 'legacy-daytona', 'daytona', 'legacy-backfill', 'ready')
            ON CONFLICT (id) DO NOTHING;
        END IF;
    END $$""",
    # Backfill sessions.volume_id from the matching legacy volume (by provider of current sandbox).
    # If the session has no current sandbox, point at the first legacy volume.
    """UPDATE sessions s SET volume_id = (
        SELECT id FROM volumes
        WHERE provider = COALESCE(
            (SELECT provider FROM sandboxes WHERE id = s.current_sandbox_id),
            (SELECT provider FROM volumes LIMIT 1)
        ) LIMIT 1
    ) WHERE s.volume_id IS NULL""",
    # Backfill sandboxes.volume_id from the matching legacy volume.
    """UPDATE sandboxes SET volume_id = (
        SELECT id FROM volumes WHERE provider = sandboxes.provider LIMIT 1
    ) WHERE volume_id IS NULL""",
    # Backfill sandboxes.subpath with a placeholder for pre-existing rows.
    "UPDATE sandboxes SET subpath = 'legacy' WHERE subpath IS NULL",
    # Enforce NOT NULL now that backfill is done.
    """DO $$ BEGIN
        IF EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name='sessions' AND column_name='volume_id' AND is_nullable='YES'
        ) THEN
            ALTER TABLE sessions ALTER COLUMN volume_id SET NOT NULL;
        END IF;
    END $$""",
    """DO $$ BEGIN
        IF EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name='sandboxes' AND column_name='volume_id' AND is_nullable='YES'
        ) THEN
            ALTER TABLE sandboxes ALTER COLUMN volume_id SET NOT NULL;
        END IF;
    END $$""",
    """DO $$ BEGIN
        IF EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name='sandboxes' AND column_name='subpath' AND is_nullable='YES'
        ) THEN
            ALTER TABLE sandboxes ALTER COLUMN subpath SET NOT NULL;
        END IF;
    END $$""",
    # 2026-04-22: cache which agent_types have their supervisor installed on each volume.
    # Avoids a 30s utility-sandbox probe on every Daytona sandbox boot.
    "ALTER TABLE volumes ADD COLUMN IF NOT EXISTS supervisor_agent_types JSONB NOT NULL DEFAULT '[]'::jsonb",
    # 2026-04-22: split sandbox_ref vs. listen_port so docker/local can store
    # both container_id / pid *and* the host port the supervisor is listening on.
    # Daytona rows leave listen_port NULL (URL comes from the signed preview API).
    "ALTER TABLE sandboxes ADD COLUMN IF NOT EXISTS listen_port INTEGER",
    # 2026-04-23: config ownership split — cwd belongs to session (JSONL hash
    # key, per-conversation), dockerfile + shared_mounts belong to sandbox
    # (provisioning-time identity; must survive sandbox replacement). Agent is
    # pure identity (agent_type, model, prompt, tools, mcp_servers, skills).
    "ALTER TABLE sessions  ADD COLUMN IF NOT EXISTS cwd TEXT",
    "ALTER TABLE sandboxes ADD COLUMN IF NOT EXISTS dockerfile TEXT",
    "ALTER TABLE sandboxes ADD COLUMN IF NOT EXISTS shared_mounts JSONB NOT NULL DEFAULT '[]'::jsonb",
    # Backfill from agents.config before dropping those keys. Each session's
    # cwd comes from its agent; each sandbox's dockerfile/shared_mounts come
    # from the agent currently bound via sessions.current_sandbox_id.
    """UPDATE sessions s SET cwd = COALESCE(
        (SELECT a.config->>'cwd' FROM agents a WHERE a.id = s.agent_id),
        '/tmp'
    ) WHERE cwd IS NULL""",
    """UPDATE sandboxes sb SET dockerfile = (
        SELECT a.config->>'dockerfile' FROM agents a
        JOIN sessions s ON s.agent_id = a.id
        WHERE s.current_sandbox_id = sb.id
        LIMIT 1
    ) WHERE dockerfile IS NULL""",
    """UPDATE sandboxes sb SET shared_mounts = COALESCE((
        SELECT a.config->'shared_mounts' FROM agents a
        JOIN sessions s ON s.agent_id = a.id
        WHERE s.current_sandbox_id = sb.id
          AND jsonb_typeof(a.config->'shared_mounts') = 'array'
        LIMIT 1
    ), '[]'::jsonb) WHERE shared_mounts = '[]'::jsonb""",
    # Enforce NOT NULL on sessions.cwd now that backfill is done.
    "ALTER TABLE sessions ALTER COLUMN cwd SET DEFAULT '/tmp'",
    """DO $$ BEGIN
        IF EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name='sessions' AND column_name='cwd' AND is_nullable='YES'
        ) THEN
            ALTER TABLE sessions ALTER COLUMN cwd SET NOT NULL;
        END IF;
    END $$""",
    # Drop the moved keys from agents.config so server reads stop finding them.
    "UPDATE agents SET config = config - 'cwd' - 'dockerfile' - 'shared_mounts' - 'env' WHERE config IS NOT NULL",
    # 2026-04-23: the 2026-04-21 backfill (around line 125 above) inserts
    # synthetic volumes with provider_ref='legacy-backfill' so pre-refactor
    # sessions have SOMETHING to point at. That placeholder isn't a real
    # provider-side volume, so daytona.create rejects every mount of it
    # and the user sees 500 on /events. Redirect any session/sandbox that
    # still points at a legacy stub to the provider's ``default-<provider>``
    # volume (which has a real provider_ref), then delete the stub. If a
    # default isn't present yet for that provider, leave the legacy row
    # alone — the next caller that goes through _resolve_or_default_volume
    # will create one and re-run of this migration on the next deploy will
    # then repoint. Idempotent: the WHERE provider_ref='legacy-backfill'
    # clause means re-running on already-healed rows is a no-op.
    """DO $$
    DECLARE legacy RECORD;
    DECLARE default_id TEXT;
    BEGIN
        FOR legacy IN
            SELECT id, provider FROM volumes WHERE provider_ref = 'legacy-backfill'
        LOOP
            SELECT id INTO default_id
            FROM volumes
            WHERE name = 'default-' || legacy.provider
              AND provider_ref <> 'legacy-backfill'
            LIMIT 1;
            IF default_id IS NOT NULL THEN
                UPDATE sessions  SET volume_id = default_id WHERE volume_id = legacy.id;
                UPDATE sandboxes SET volume_id = default_id WHERE volume_id = legacy.id;
                DELETE FROM volumes WHERE id = legacy.id;
            END IF;
        END LOOP;
    END $$""",
    # 2026-04-26: persist caller-supplied pre_start_commands on the session row
    # so they can be re-run when a new sandbox is provisioned (Type 2 recovery).
    # Only the raw user commands are stored; skill-install commands are re-merged
    # from agent.config.skills at recovery time.
    "ALTER TABLE sessions ADD COLUMN IF NOT EXISTS pre_start_commands"
    " JSONB NOT NULL DEFAULT '[]'::jsonb",
    # 2026-04-30: shadow ``sandbox_state`` JSONB on the session row.
    # Today the canonical sandbox identity lives in the ``sandboxes`` table
    # and the session row holds ``current_sandbox_id`` as an FK. Recovery
    # (``_ensure_sandbox_locked`` / ``_type1_recover`` / ``_type2_recover``)
    # JOINs from sessions through sandboxes on every check.
    #
    # This column dual-writes the relevant fields so callers can read
    # everything they need from the session row alone — foundation for a
    # subsequent PR that drops the sandboxes table entirely. Writes still
    # go to both via the trigger; reads stay through the legacy table for
    # back-compat in this PR.
    #
    # Shape (mirrored from a SandboxRecord row):
    #   { "type": <provider>, "sandbox_id": <provider_ref>,
    #     "listen_port": int|null, "snapshot_path": str|null,
    #     "snapshot_version": int,
    #     "recipe": {dockerfile, shared_mounts, root, agent_type, pre_start_commands} }
    "ALTER TABLE sessions ADD COLUMN IF NOT EXISTS sandbox_state JSONB",
    # Drop any leftover triggers from prior experimental branches so this
    # migration's _sandbox_state_* triggers are the only ones writing to
    # ``sandbox_state``. Idempotent.
    "DROP TRIGGER IF EXISTS _ephemeral_sandboxes_sync ON sandboxes",
    "DROP TRIGGER IF EXISTS _ephemeral_sessions_sync ON sessions",
    "DROP FUNCTION IF EXISTS _ephemeral_sync_sandbox_state_from_sandboxes() CASCADE",
    "DROP FUNCTION IF EXISTS _ephemeral_sync_sandbox_state_from_sessions() CASCADE",
    "DROP FUNCTION IF EXISTS _ephemeral_compute_sandbox_state(TEXT, TEXT, JSONB, TEXT) CASCADE",
    "DROP FUNCTION IF EXISTS _ephemeral_compute_sandbox_state(TEXT, TEXT, JSONB) CASCADE",
    """CREATE OR REPLACE FUNCTION _compute_sandbox_state(
           sb_id TEXT, agent_id_in TEXT, pre_start_in JSONB, volume_id_in TEXT
       ) RETURNS JSONB AS $$
       DECLARE
           sb RECORD;
           agent_type TEXT;
           volume_provider TEXT;
       BEGIN
           SELECT a.config->>'agent_type' INTO agent_type
               FROM agents a WHERE a.id = agent_id_in;
           SELECT v.provider INTO volume_provider
               FROM volumes v WHERE v.id = volume_id_in;
           IF sb_id IS NULL THEN
               RETURN jsonb_build_object(
                   'type',             COALESCE(volume_provider, 'unknown'),
                   'sandbox_id',       NULL,
                   'snapshot_path',    NULL,
                   'snapshot_version', 0,
                   'listen_port',      NULL,
                   'recipe', jsonb_build_object(
                       'dockerfile',          NULL,
                       'shared_mounts',       '[]'::jsonb,
                       'root',                NULL,
                       'agent_type',          COALESCE(agent_type, 'claude'),
                       'pre_start_commands',  COALESCE(pre_start_in, '[]'::jsonb)
                   )
               );
           END IF;
           SELECT * INTO sb FROM sandboxes WHERE id = sb_id;
           IF NOT FOUND THEN
               RETURN jsonb_build_object(
                   'type',             COALESCE(volume_provider, 'unknown'),
                   'sandbox_id',       NULL,
                   'snapshot_path',    NULL,
                   'snapshot_version', 0,
                   'listen_port',      NULL,
                   'recipe', jsonb_build_object(
                       'dockerfile',          NULL,
                       'shared_mounts',       '[]'::jsonb,
                       'root',                NULL,
                       'agent_type',          COALESCE(agent_type, 'claude'),
                       'pre_start_commands',  COALESCE(pre_start_in, '[]'::jsonb)
                   )
               );
           END IF;
           RETURN jsonb_build_object(
               'type',             sb.provider,
               'sandbox_id',       sb.sandbox_ref,
               'snapshot_path',    NULL,
               'snapshot_version', 0,
               'listen_port',      sb.listen_port,
               'recipe', jsonb_build_object(
                   'dockerfile',          sb.dockerfile,
                   'shared_mounts',       COALESCE(sb.shared_mounts, '[]'::jsonb),
                   'root',                sb.root,
                   'agent_type',          COALESCE(agent_type, 'claude'),
                   'pre_start_commands',  COALESCE(pre_start_in, '[]'::jsonb)
               )
           );
       END $$ LANGUAGE plpgsql""",
    """CREATE OR REPLACE FUNCTION _sync_sandbox_state_from_sandboxes()
       RETURNS TRIGGER AS $$
       BEGIN
           UPDATE sessions s
           SET sandbox_state = _compute_sandbox_state(
               NEW.id, s.agent_id, s.pre_start_commands, s.volume_id
           )
           WHERE s.current_sandbox_id = NEW.id;
           RETURN NEW;
       END $$ LANGUAGE plpgsql""",
    """CREATE OR REPLACE FUNCTION _sync_sandbox_state_from_sessions()
       RETURNS TRIGGER AS $$
       BEGIN
           NEW.sandbox_state := _compute_sandbox_state(
               NEW.current_sandbox_id, NEW.agent_id, NEW.pre_start_commands, NEW.volume_id
           );
           RETURN NEW;
       END $$ LANGUAGE plpgsql""",
    "DROP TRIGGER IF EXISTS _sandbox_state_sandboxes_sync ON sandboxes",
    """CREATE TRIGGER _sandbox_state_sandboxes_sync
       AFTER INSERT OR UPDATE ON sandboxes
       FOR EACH ROW EXECUTE FUNCTION _sync_sandbox_state_from_sandboxes()""",
    "DROP TRIGGER IF EXISTS _sandbox_state_sessions_sync ON sessions",
    """CREATE TRIGGER _sandbox_state_sessions_sync
       BEFORE INSERT OR UPDATE OF current_sandbox_id, agent_id, pre_start_commands, volume_id ON sessions
       FOR EACH ROW EXECUTE FUNCTION _sync_sandbox_state_from_sessions()""",
    # One-shot backfill so existing rows aren't NULL until next write.
    """UPDATE sessions s
       SET sandbox_state = _compute_sandbox_state(
           s.current_sandbox_id, s.agent_id, s.pre_start_commands, s.volume_id
       )
       WHERE s.sandbox_state IS NULL""",
    # 2026-04-30: drop the sandboxes table + all its mirrors. The pool's
    # ``sessions.sandbox_state`` JSONB has been the runtime source of
    # truth since the d4 PR; this migration removes the trigger machinery
    # that overwrote it, the parallel ``sandboxes`` table, the
    # ``current_sandbox_id`` FK column, and the ``session_log.sandbox_id``
    # FK column (write-only, zero readers).
    "DROP TRIGGER IF EXISTS _sandbox_state_sandboxes_sync ON sandboxes",
    "DROP TRIGGER IF EXISTS _sandbox_state_sessions_sync ON sessions",
    "DROP FUNCTION IF EXISTS _sync_sandbox_state_from_sandboxes() CASCADE",
    "DROP FUNCTION IF EXISTS _sync_sandbox_state_from_sessions() CASCADE",
    "DROP FUNCTION IF EXISTS _compute_sandbox_state(TEXT, TEXT, JSONB, TEXT) CASCADE",
    # Drop FK columns before the table they reference.
    "ALTER TABLE sessions DROP CONSTRAINT IF EXISTS sessions_current_sandbox_id_fkey",
    "ALTER TABLE sessions DROP COLUMN IF EXISTS current_sandbox_id",
    "ALTER TABLE session_log DROP CONSTRAINT IF EXISTS session_log_sandbox_id_fkey",
    "ALTER TABLE session_log DROP COLUMN IF EXISTS sandbox_id",
    # Drop the now-orphaned table.
    "DROP TABLE IF EXISTS sandboxes",
    # 2026-04-30 (post-d5 rename): rename sandbox_state JSONB key
    # ``sandbox_id`` → ``sandbox_ref``. The field always held the
    # provider's opaque reference (e.g. Daytona sandbox UUID, docker
    # container id, "local-<hex>"), never a DB row PK; the new name
    # reflects that. Idempotent: only rows that still have the old key
    # get rewritten, and the rewrite drops the old key in the same step.
    """UPDATE sessions
       SET sandbox_state = jsonb_set(sandbox_state - 'sandbox_id', '{sandbox_ref}', sandbox_state->'sandbox_id')
       WHERE sandbox_state ? 'sandbox_id'""",
]


# ---------------------------------------------------------------------------
# Init + pool lifecycle
# ---------------------------------------------------------------------------

def init_db() -> None:
    """Run DDL and migrations. Called once on server startup (sync).

    DDL creates tables if missing (fresh setups).
    Migrations ALTER existing tables to the current schema (upgrades).
    Both use IF EXISTS / IF NOT EXISTS so they're idempotent.
    """
    conn = psycopg.connect(DATABASE_URL, row_factory=dict_row)
    try:
        for stmt in _PG_SCHEMA:
            conn.execute(stmt)
        for stmt in _MIGRATIONS:
            try:
                conn.execute(stmt)
            except Exception as e:
                log.warning("migration failed: %s — %s", stmt, e)
        conn.commit()
    finally:
        conn.close()


_pool: AsyncConnectionPool | None = None


async def init_pool(min_size: int = 4, max_size: int = 100) -> None:
    global _pool
    _pool = AsyncConnectionPool(
        DATABASE_URL,
        kwargs={"row_factory": dict_row},
        min_size=min_size,
        max_size=max_size,
        open=False,
    )
    await _pool.open()


async def close_pool() -> None:
    global _pool
    if _pool:
        await _pool.close()
        _pool = None


@asynccontextmanager
async def get_db():
    """Borrow an async connection from the pool. Auto-commits on success, rolls back on error."""
    if _pool is None:
        raise RuntimeError("Database pool not initialized. Call init_pool() first.")
    async with _pool.connection() as conn:
        try:
            yield conn
            await conn.commit()
        except Exception:
            # Let the pool context manager handle broken connections —
            # manual conn.close() here would corrupt pool state (double-close).
            try:
                await conn.rollback()
            except Exception:
                pass
            raise


# ---------------------------------------------------------------------------
# Agent CRUD
# ---------------------------------------------------------------------------

async def upsert_agent(agent: AgentRecord) -> None:
    async with get_db() as conn:
        await conn.execute(
            "INSERT INTO agents (id, name, config) VALUES (%s, %s, %s)"
            " ON CONFLICT(id) DO UPDATE SET name=EXCLUDED.name, config=EXCLUDED.config",
            (agent.id, agent.name, Json(agent.config.to_dict())),
        )


async def get_agent(agent_id: str) -> AgentRecord | None:
    async with get_db() as conn:
        row = await (await conn.execute(
            "SELECT * FROM agents WHERE id = %s", (agent_id,)
        )).fetchone()
    if row is None:
        return None
    config_data = row["config"] if row["config"] else {}
    return AgentRecord(id=row["id"], name=row["name"], config=AgentConfig.from_dict(config_data))


async def list_agents() -> list[AgentRecord]:
    async with get_db() as conn:
        rows = await (await conn.execute("SELECT * FROM agents")).fetchall()
    return [
        AgentRecord(
            id=r["id"], name=r["name"],
            config=AgentConfig.from_dict(r["config"] if r["config"] else {}),
        )
        for r in rows
    ]


async def delete_agent(agent_id: str) -> None:
    async with get_db() as conn:
        await conn.execute("DELETE FROM agents WHERE id = %s", (agent_id,))


# ---------------------------------------------------------------------------
# Sandbox CRUD
# ---------------------------------------------------------------------------

# Sandbox CRUD removed: the sandboxes table is gone (see migration in
# _MIGRATIONS that drops it). Pool's sandbox_state JSONB on the sessions
# row is the single source of truth; provider sandbox refs live there
# directly.


# ---------------------------------------------------------------------------
# Volume CRUD
# ---------------------------------------------------------------------------

async def upsert_volume(volume: VolumeRecord) -> None:
    async with get_db() as conn:
        await conn.execute(
            "INSERT INTO volumes (id, name, provider, provider_ref, status, supervisor_agent_types)"
            " VALUES (%s, %s, %s, %s, %s, %s)"
            " ON CONFLICT(id) DO UPDATE SET name=EXCLUDED.name,"
            " provider=EXCLUDED.provider, provider_ref=EXCLUDED.provider_ref,"
            " status=EXCLUDED.status, supervisor_agent_types=EXCLUDED.supervisor_agent_types",
            (volume.id, volume.name, volume.provider, volume.provider_ref, volume.status,
             Json(volume.supervisor_agent_types)),
        )


def _row_to_volume(row: dict) -> VolumeRecord:
    return VolumeRecord(
        id=row["id"], name=row["name"], provider=row["provider"],
        provider_ref=row["provider_ref"], status=row["status"],
        supervisor_agent_types=list(row.get("supervisor_agent_types") or []),
    )


async def get_volume(volume_id: str) -> VolumeRecord | None:
    async with get_db() as conn:
        row = await (await conn.execute(
            "SELECT * FROM volumes WHERE id = %s", (volume_id,)
        )).fetchone()
    if row is None:
        return None
    return _row_to_volume(row)


async def get_volume_by_name(name: str) -> VolumeRecord | None:
    async with get_db() as conn:
        row = await (await conn.execute(
            "SELECT * FROM volumes WHERE name = %s", (name,)
        )).fetchone()
    if row is None:
        return None
    return _row_to_volume(row)


async def list_volumes(provider: str | None = None) -> list[VolumeRecord]:
    async with get_db() as conn:
        if provider:
            rows = await (await conn.execute(
                "SELECT * FROM volumes WHERE provider = %s", (provider,)
            )).fetchall()
        else:
            rows = await (await conn.execute("SELECT * FROM volumes")).fetchall()
    return [_row_to_volume(r) for r in rows]


async def delete_volume(volume_id: str) -> None:
    async with get_db() as conn:
        await conn.execute("DELETE FROM volumes WHERE id = %s", (volume_id,))


async def add_supervisor_agent_type(volume_id: str, agent_type: str) -> None:
    """Idempotently append agent_type to volumes.supervisor_agent_types."""
    async with get_db() as conn:
        await conn.execute(
            "UPDATE volumes SET supervisor_agent_types = "
            "COALESCE(supervisor_agent_types, '[]'::jsonb) || to_jsonb(%s::text) "
            "WHERE id = %s AND NOT (supervisor_agent_types @> to_jsonb(%s::text))",
            (agent_type, volume_id, agent_type),
        )


# ---------------------------------------------------------------------------
# Session CRUD
# ---------------------------------------------------------------------------

async def upsert_session(session_id: str, agent_id: str,
                         inner_session_id: str | None,
                         volume_id: str | None = None,
                         env: dict[str, str] | None = None,
                         secrets: dict[str, str] | None = None,
                         cwd: str | None = None,
                         pre_start_commands: list[str] | None = None) -> None:
    """Upsert a session row.

    PATCH-like semantics: ``env=None`` / ``secrets=None`` / ``cwd=None``
    means don't touch the stored column on update. Pass ``{}`` / ``""``
    to explicitly wipe.
    """
    cols = ["id", "agent_id", "inner_session_id"]
    vals: list = [session_id, agent_id, inner_session_id]
    update_parts = [
        "inner_session_id=EXCLUDED.inner_session_id",
    ]
    # (column_name, raw_value, value_transform) — included only when raw is not None
    optional = [
        ("volume_id", volume_id, lambda v: v),
        ("env", env, Json),
        ("secrets", secrets, Json),
        ("cwd", cwd, lambda v: v),
        ("pre_start_commands", pre_start_commands, Json),
    ]
    for col, raw, transform in optional:
        if raw is None:
            continue
        cols.append(col)
        vals.append(transform(raw))
        update_parts.append(f"{col}=EXCLUDED.{col}")
    placeholders = ", ".join(["%s"] * len(cols))
    col_list = ", ".join(cols)
    update_sql = ", ".join(update_parts)
    async with get_db() as conn:
        await conn.execute(
            f"INSERT INTO sessions ({col_list}) VALUES ({placeholders})"
            f" ON CONFLICT(id) DO UPDATE SET {update_sql}",
            tuple(vals),
        )


async def update_session_env(session_id: str, env: dict[str, str]) -> None:
    """Replace stored session env. Used on resume when caller sends explicit env."""
    async with get_db() as conn:
        await conn.execute(
            "UPDATE sessions SET env = %s WHERE id = %s",
            (Json(env), session_id),
        )


async def update_session_secrets(session_id: str, secrets: dict[str, str]) -> None:
    """Replace stored session secrets. Used on resume when caller sends explicit secrets."""
    async with get_db() as conn:
        await conn.execute(
            "UPDATE sessions SET secrets = %s WHERE id = %s",
            (Json(secrets), session_id),
        )


async def get_session(session_id: str) -> dict | None:
    async with get_db() as conn:
        row = await (await conn.execute(
            "SELECT * FROM sessions WHERE id = %s", (session_id,)
        )).fetchone()
    if row is None:
        return None
    return dict(row)


async def get_session_env(session_id: str) -> dict[str, str]:
    """Return stored session env, or {} if session not found."""
    async with get_db() as conn:
        row = await (await conn.execute(
            "SELECT env FROM sessions WHERE id = %s", (session_id,)
        )).fetchone()
    if row is None:
        return {}
    return row["env"] or {}


async def get_session_secrets(session_id: str) -> dict[str, str]:
    """Return stored session secrets, or {} if session not found.

    SECURITY: never log the return value. Passed directly into supervisor
    spawn env; never serialized to clients (GET /sessions/{id} returns only
    key names, not values).
    """
    async with get_db() as conn:
        row = await (await conn.execute(
            "SELECT secrets FROM sessions WHERE id = %s", (session_id,)
        )).fetchone()
    if row is None:
        return {}
    return row["secrets"] or {}


# get_any_session_for_sandbox removed: it walked sessions.current_sandbox_id
# (gone with the sandboxes table) and was only used by the legacy back-compat
# spawn_env-reconstruction path that the SessionPool replaced.


# ---------------------------------------------------------------------------
# Session log
# ---------------------------------------------------------------------------

def _row_to_log_entry(r: dict) -> LogEntry:
    return LogEntry(id=r["id"], session_id=r["session_id"], agent_id=r["agent_id"],
                    event_type=r["event_type"],
                    payload=r["payload"], created_at=r["created_at"].timestamp())


async def log_event(*, session_id: str, agent_id: str,
                    event_type: str, payload: dict) -> None:
    async with get_db() as conn:
        await conn.execute(
            "INSERT INTO session_log (session_id, agent_id, event_type, payload)"
            " VALUES (%s, %s, %s, %s)",
            (session_id, agent_id, event_type, Json(payload)),
        )


async def get_session_log(session_id: str, limit: int = 500) -> list[LogEntry]:
    async with get_db() as conn:
        rows = await (await conn.execute(
            "SELECT id, session_id, agent_id, event_type, payload, created_at"
            " FROM session_log WHERE session_id = %s ORDER BY created_at ASC LIMIT %s",
            (session_id, limit),
        )).fetchall()
    return [_row_to_log_entry(r) for r in rows]
