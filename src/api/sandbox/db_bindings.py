"""DB bindings for SessionPool — load/save ``sessions.sandbox_state``.

Per docs/ephemeral-sandbox-design.md §15.2 — uses ``SELECT ... FOR
UPDATE`` so two server processes serving the same session_id serialise
on the row, not just on the in-memory pool lock.
"""
from __future__ import annotations

from typing import Any

from api import db as _db


async def load_sandbox_state(session_id: str) -> dict[str, Any] | None:
    """Read ``sessions.sandbox_state`` JSONB for ``session_id``, taking
    a row-level lock so concurrent ``get_session`` calls from peer
    processes serialise on this row.

    Returns the JSONB dict (suitable for ``deserialize``), or None if
    the session row doesn't exist.

    NOTE: the lock is held only for the duration of the surrounding
    ``async with get_db()`` context, which the caller closes via the
    ``save_sandbox_state`` write that follows. The dual-write triggers
    on the ``sandboxes`` table refresh ``sandbox_state`` whenever a
    back-compat shim row is updated, so this row-level lock plus the
    pool's own per-session lock are belt-and-suspenders for the (rare)
    case of two server processes racing on the same session_id.
    """
    async with _db.get_db() as conn:
        row = await (await conn.execute(
            "SELECT sandbox_state FROM sessions WHERE id = %s FOR UPDATE",
            (session_id,),
        )).fetchone()
    if row is None:
        return None
    return row["sandbox_state"]


async def save_sandbox_state(session_id: str, payload: dict[str, Any]) -> None:
    """Write ``sessions.sandbox_state`` JSONB for ``session_id``.

    The dual-write trigger on ``sessions`` fires only on writes to
    ``current_sandbox_id`` / ``agent_id`` / ``pre_start_commands`` /
    ``volume_id``, NOT on writes to ``sandbox_state`` itself — so this
    update is the sole writer here and won't be clobbered by the sync
    trigger.
    """
    from psycopg.types.json import Json
    async with _db.get_db() as conn:
        await conn.execute(
            "UPDATE sessions SET sandbox_state = %s WHERE id = %s",
            (Json(payload), session_id),
        )
