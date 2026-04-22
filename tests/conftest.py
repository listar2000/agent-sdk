"""Shared test fixtures.

Session-scoped DB pool + function-scoped cleanup. Moves pool init out of
per-test fixtures so the integration suite doesn't pay a TCP handshake +
5-table-DELETE setup cost on every single test.

Tests that need a clean DB per function depend on ``clean_db``. Tests that
want the standard ASGI client use ``http_client`` (replaces the ad-hoc
``client`` fixture each integration file used to define).
"""
from __future__ import annotations
import os
import sys

import pytest
import pytest_asyncio

_SRC = os.path.join(os.path.dirname(__file__), "..", "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

# Mirror the per-file idiom so DATABASE_URL is populated before db import.
_DB = os.environ.get("TEST_DATABASE_URL")
if _DB and not os.environ.get("DATABASE_URL"):
    os.environ["DATABASE_URL"] = _DB


_DB_TABLES = ("session_log", "sessions", "sandboxes", "volumes", "agents")


@pytest.fixture(scope="session")
def event_loop():
    """Session-scoped event loop so session-scoped async fixtures share it."""
    import asyncio
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest_asyncio.fixture(scope="session")
async def db_pool():
    """Open the connection pool once per test session.

    Tests that need DB access depend on ``clean_db`` (which transitively
    depends on this). Kept separate from autouse so tests that don't touch
    the DB don't pay for pool init.
    """
    if not os.environ.get("TEST_DATABASE_URL"):
        yield
        return
    from api import db as dbmod
    dbmod.init_db()
    await dbmod.init_pool()
    try:
        yield
    finally:
        await dbmod.close_pool()


@pytest_asyncio.fixture
async def clean_db(db_pool):
    """Truncate the 5 core tables before the test runs.

    Depends on the session-scoped pool, so the pool opens once and each test
    just pays for the DELETEs.
    """
    if not os.environ.get("TEST_DATABASE_URL"):
        yield
        return
    from api import db as dbmod
    async with dbmod.get_db() as conn:
        for table in _DB_TABLES:
            await conn.execute(f"DELETE FROM {table}")
    yield
