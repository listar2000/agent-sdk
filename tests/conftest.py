"""Shared test fixtures.

Session-scoped DB pool + function-scoped cleanup. Moves pool init out of
per-test fixtures so the integration suite doesn't pay a TCP handshake +
5-table-DELETE setup cost on every single test.

Tests that need a clean DB per function depend on ``clean_db``. Tests that
want the standard ASGI client use ``http_client`` (replaces the ad-hoc
``client`` fixture each integration file used to define).

Sandbox-leak guardrail
----------------------
The ``_auto_cleanup_live_sessions`` autouse fixture wraps every test that
runs against a live server (``localhost:7778`` reachable). It snapshots
``Agent.__init__`` and ``ApiClient.create_session`` so every session
created during the test is tracked, then calls ``DELETE /sessions/{id}``
on each at teardown — even if the test body raises. This destroys the
session row AND releases the pool's compute lease, so daytona / docker /
local sandboxes don't pile up across runs. Tests that already do their
own cleanup (``aclose``, manual ``delete_session``) are unaffected: the
fixture's idempotent 204 path swallows the ``not found`` cases.
"""
from __future__ import annotations
import os
import sys

import pytest
import pytest_asyncio

_SRC = os.path.join(os.path.dirname(__file__), "..", "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

# DB URL resolution. The launch scripts (``scripts/launch_server_*.sh``)
# start a project-local Postgres at port 5433 with database ``agent_sdk_server``;
# tests share that same DB so we don't need a separate ``TEST_DATABASE_URL``.
# Precedence:
#   1. ``TEST_DATABASE_URL`` (explicit override — keeps back-compat with CI
#      runs that point at a separate test DB)
#   2. ``DATABASE_URL`` (whatever the launcher exported)
#   3. Default: the launch script's local Postgres URL — works as long as
#      the launcher has been run once in this checkout.
_DEFAULT_DB_URL = "postgresql://postgres@localhost:5433/agent_sdk_server"
_DB = (
    os.environ.get("TEST_DATABASE_URL")
    or os.environ.get("DATABASE_URL")
    or _DEFAULT_DB_URL
)
if not os.environ.get("DATABASE_URL"):
    os.environ["DATABASE_URL"] = _DB
# Mirror to TEST_DATABASE_URL too so the per-file
# ``pytest.mark.skipif(_DB is None, reason="TEST_DATABASE_URL not set")``
# guards (still in some legacy test files) don't skip.
if not os.environ.get("TEST_DATABASE_URL"):
    os.environ["TEST_DATABASE_URL"] = _DB


# ``sandboxes`` was dropped in commit 27f0cc9 (sandbox_state JSONB took over).
_DB_TABLES = ("session_log", "sessions", "volumes", "agents")


@pytest_asyncio.fixture
async def db_pool():
    """Open the connection pool for this test and tear it down after.

    Function-scoped to match pytest-asyncio 1.x's default ``loop_scope``
    — a session-scoped async fixture on a function-scoped event loop is
    the classic cause of ``psycopg_pool.PoolTimeout`` in CI: the pool
    holds conns bound to a loop that's already closed, so the next
    test's ``get_db()`` waits on a dead loop forever.

    Per-test pool init is ~5-10ms on a warm Postgres, negligible next to
    the DELETE-truncate already in ``clean_db``.
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
    """Truncate the 5 core tables before the test runs."""
    if not os.environ.get("TEST_DATABASE_URL"):
        yield
        return
    from api import db as dbmod
    async with dbmod.get_db() as conn:
        for table in _DB_TABLES:
            await conn.execute(f"DELETE FROM {table}")
    yield


# ---------------------------------------------------------------------------
# Live-server sandbox leak guardrail
# ---------------------------------------------------------------------------

_LIVE_SERVER_URL = os.environ.get("AGENT_SERVER_URL", "http://localhost:7778")


def _live_server_reachable() -> bool:
    """Cheap one-shot probe so the fixture can no-op when no live server
    is running (unit-test mode). Cached on the function to avoid hitting
    the URL on every single test."""
    cached = getattr(_live_server_reachable, "_cached", None)
    if cached is not None:
        return cached
    try:
        import httpx
        ok = httpx.get(f"{_LIVE_SERVER_URL}/health", timeout=2).status_code == 200
    except Exception:
        ok = False
    _live_server_reachable._cached = ok  # type: ignore[attr-defined]
    return ok


@pytest.fixture(autouse=True)
def _auto_cleanup_live_sessions(request):
    """Track every session created during the test and delete it on teardown.

    Wraps two construction sites:

    * ``agent_sdk.client.Agent._ensure_registered`` — captures
      ``self.session_id`` after the SDK's ``POST /sessions`` lands.
    * ``agent_sdk.api_client.ApiClient.create_session`` — captures the
      ``session_id`` field of the returned dict.

    At teardown, fires ``DELETE /sessions/{id}`` against the live server
    for every captured id. The DELETE route is idempotent (returns 204
    even when the session is already gone), so this is safe alongside
    existing per-test cleanup (``Agent.aclose``, manual ``delete_session``).

    No-ops when the live server isn't reachable (unit-test mode), and
    when a test opts out via ``@pytest.mark.no_session_cleanup``.

    Why both sites: tests use both the user-facing ``Agent`` class and the
    operator-facing ``ApiClient``. Wrapping only one would miss leaks
    from the other.
    """
    if "no_session_cleanup" in request.keywords:
        yield
        return
    if not _live_server_reachable():
        yield
        return

    try:
        from agent_sdk.client import Agent
        from agent_sdk.api_client import ApiClient
    except Exception:
        # SDK import failure (e.g. a unit test that monkey-patched the
        # module out from under us) — silently skip cleanup; the test
        # itself isn't going to leak sandboxes.
        yield
        return

    tracked: set[str] = set()

    original_ensure = Agent._ensure_registered
    original_create = ApiClient.create_session

    async def _wrapped_ensure(self):
        await original_ensure(self)
        sid = getattr(self, "session_id", None)
        if sid:
            tracked.add(sid)

    async def _wrapped_create(self, **body):
        result = await original_create(self, **body)
        if isinstance(result, dict):
            sid = result.get("session_id") or result.get("id")
            if isinstance(sid, str) and sid:
                tracked.add(sid)
        return result

    Agent._ensure_registered = _wrapped_ensure
    ApiClient.create_session = _wrapped_create

    try:
        yield
    finally:
        Agent._ensure_registered = original_ensure
        ApiClient.create_session = original_create

        if tracked:
            import httpx
            with httpx.Client(timeout=15) as c:
                for sid in tracked:
                    try:
                        c.delete(f"{_LIVE_SERVER_URL}/sessions/{sid}")
                    except Exception:
                        # Best-effort — daytona's agent_sdk_origin label
                        # + scripts/cleanup_orphans.py is the backstop.
                        pass


def pytest_configure(config):
    """Register custom marks so ``--strict-markers`` doesn't reject them."""
    config.addinivalue_line(
        "markers",
        "no_session_cleanup: skip the autouse session-deletion fixture for "
        "this test (use when the test deliberately leaves a session behind "
        "so a follow-up test can resume it).",
    )


def pytest_sessionfinish(session, exitstatus):
    """Reap test-origin orphan sandboxes on a clean exit, opt-in.

    Backstop for cases where a worker crashes hard (SIGKILL, OOM) and
    the autouse per-test fixture's DELETE never fired, or where a
    paused-not-deleted daytona sandbox piled up across runs. Daytona
    + docker stamp the ``agent_sdk_origin`` label at create time, so
    the cleanup script can find them by filter.

    Off by default — set ``AGENT_SDK_TEST_AUTO_CLEANUP=1`` to opt in
    (CI is the obvious customer). Local dev runs leave it off so a
    quick unit-test run isn't churning the orphan reaper. Manual
    cleanup is always available via::

        python scripts/cleanup_orphans.py --origin test --yes

    xdist workers skip; only the controller runs the script.
    """
    import os
    import subprocess
    if os.environ.get("AGENT_SDK_TEST_AUTO_CLEANUP") != "1":
        return
    if hasattr(session.config, "workerinput"):
        return
    script = os.path.join(
        os.path.dirname(__file__), "..", "scripts", "cleanup_orphans.py",
    )
    if not os.path.exists(script):
        return
    try:
        subprocess.run(
            [sys.executable, script, "--origin", "test", "--yes"],
            timeout=120, check=False,
        )
    except Exception as e:
        print(f"[conftest] post-session cleanup_orphans failed: {e}")
