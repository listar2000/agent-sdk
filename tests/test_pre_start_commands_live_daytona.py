"""Live end-to-end test on real Daytona, exercised through the SDK (ServerClient).

Skipped unless DAYTONA_API_KEY + CLAUDE_CODE_OAUTH_TOKEN + TEST_DATABASE_URL
are all set. Models a realistic agent-setup workflow:

  1. Install a real package (pip install --user six) — durable on snapshot.
  2. Write a CLAUDE.md system-prompt file — durable on snapshot.
  3. Drop a snapshot-excluded counter under .cache/ — proves pre_start
     re-ran on Type 2 recovery (snapshot wouldn't restore an excluded path).

Triggers /reset-sandbox (Type 2 recovery, sandbox replaced) and verifies
all three artifacts on the new sandbox via the SDK.
"""
from __future__ import annotations

import asyncio
import base64
import os
import sys

import httpx
import pytest
import pytest_asyncio

_SRC = os.path.join(os.path.dirname(__file__), "..", "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

from dotenv import load_dotenv  # noqa: E402

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

DAYTONA_API_KEY = os.environ.get("DAYTONA_API_KEY")
OAUTH_TOKEN = os.environ.get("CLAUDE_CODE_OAUTH_TOKEN")
_DB = os.environ.get("TEST_DATABASE_URL")

pytestmark = pytest.mark.skipif(
    not (DAYTONA_API_KEY and OAUTH_TOKEN and _DB),
    reason="DAYTONA_API_KEY + CLAUDE_CODE_OAUTH_TOKEN + TEST_DATABASE_URL required",
)
if _DB:
    os.environ["DATABASE_URL"] = _DB

from agent_sdk.server_client import ServerClient  # noqa: E402
from api import server as srv  # noqa: E402


@pytest_asyncio.fixture
async def sdk(clean_db):
    """ServerClient bound to the in-process app via ASGITransport."""
    transport = httpx.ASGITransport(app=srv.app)
    http = httpx.AsyncClient(
        transport=transport,
        base_url="http://test",
        timeout=httpx.Timeout(300.0, read=None),
    )
    sc = ServerClient(base_url="http://test", http_client=http)
    try:
        yield sc
    finally:
        await sc.close()


def _decode_read(resp: dict) -> str:
    raw = resp.get("content", "")
    if isinstance(raw, str):
        try:
            return base64.b64decode(raw).decode("utf-8", errors="replace")
        except Exception:
            return raw
    return str(raw)


CLAUDE_MD_BODY = "You are a hive agent. Be helpful and concise.\n"


@pytest.mark.asyncio
async def test_live_daytona_realistic_workflow_via_sdk(sdk: ServerClient):
    """Realistic pre_start_commands workflow on a real Daytona sandbox,
    driven entirely through ``ServerClient`` (the SDK that end users call)."""
    cmds = [
        # 1. Real package install. npm is on the base image (the supervisor
        # itself runs on node). cowsay is tiny and has a deterministic --version.
        "npm install -g --silent cowsay 2>&1 | tail -1",
        # 2. Write a CLAUDE.md so the agent picks up its system prompt.
        f"mkdir -p /home/daytona && cat > /home/daytona/CLAUDE.md <<'EOF'\n{CLAUDE_MD_BODY}EOF",
        # 3. Snapshot-excluded marker — proves pre_start re-ran on Type 2.
        "mkdir -p /home/daytona/.cache && date +%s%N > /home/daytona/.cache/psc-counter",
    ]

    sid: str | None = None
    try:
        # ── Phase 1: SDK creates a real Daytona session ────────────────────
        created = await sdk.create_session(
            provider="daytona",
            agent_type="claude",
            secrets={"CLAUDE_CODE_OAUTH_TOKEN": OAUTH_TOKEN},
            pre_start_commands=cmds,
        )
        sid = created.get("id") or created.get("session_id")
        assert sid, f"no session id in SDK response: {created}"

        # CLAUDE.md present and correct (via SDK).
        f1 = await sdk.session_file_read(sid, "CLAUDE.md")
        assert _decode_read(f1) == CLAUDE_MD_BODY

        # Counter present and parseable.
        c1 = await sdk.session_file_read(sid, ".cache/psc-counter")
        first_ts = int(_decode_read(c1).strip())

        # `cowsay` available on PATH (proves npm install -g ran successfully).
        ex1 = await sdk.session_sandbox_exec(
            sid, "command -v cowsay && cowsay --version", timeout=30,
        )
        assert ex1["exit_code"] == 0, (
            f"cowsay not found after first provision: {ex1}"
        )
        first_cowsay_path = ex1["stdout"].splitlines()[0].strip()
        assert first_cowsay_path, f"empty cowsay path: {ex1}"

        # SDK round-trip: get_session must return the persisted commands.
        sess_row = await sdk.get_session(sid)
        assert sess_row.get("pre_start_commands") == cmds, (
            f"pre_start_commands not round-tripped via SDK: "
            f"sent={cmds} got={sess_row.get('pre_start_commands')}"
        )

        await asyncio.sleep(1.5)

        # ── Phase 2: Type 2 recovery via /reset-sandbox ───────────────────
        # Not exposed on ServerClient (operator-persona hides internal
        # routes); reach through the underlying http client.
        r = await sdk._http.post(f"/sessions/{sid}/reset-sandbox", timeout=300)
        assert r.status_code == 200, f"reset failed: {r.status_code} {r.text}"

        # CLAUDE.md still correct on the new sandbox (via SDK).
        f2 = await sdk.session_file_read(sid, "CLAUDE.md")
        assert _decode_read(f2) == CLAUDE_MD_BODY

        # Counter has a NEWER timestamp — proof of re-run.
        c2 = await sdk.session_file_read(sid, ".cache/psc-counter")
        second_ts = int(_decode_read(c2).strip())
        assert second_ts > first_ts, (
            f"pre_start_commands did NOT re-run on Type 2 recovery: "
            f"first={first_ts} second={second_ts}"
        )

        # `cowsay` still on PATH on the replacement sandbox.
        ex2 = await sdk.session_sandbox_exec(
            sid, "command -v cowsay && cowsay --version", timeout=30,
        )
        assert ex2["exit_code"] == 0, (
            f"cowsay missing after Type 2 recovery: {ex2}"
        )
        second_cowsay_path = ex2["stdout"].splitlines()[0].strip()
        assert second_cowsay_path

        delta_s = (second_ts - first_ts) / 1e9
        print(
            f"\n✅ pre_start_commands replayed on real Daytona via SDK:\n"
            f"   counter delta : {delta_s:.2f}s (proves re-run)\n"
            f"   CLAUDE.md     : {len(CLAUDE_MD_BODY)} bytes intact through reset\n"
            f"   npm pkg cowsay: {first_cowsay_path} → {second_cowsay_path}\n"
            f"   SDK round-trip: pre_start_commands persisted on sessions row"
        )

    finally:
        if sid:
            try:
                await sdk.delete_session(sid)
            except Exception as e:
                print(f"cleanup delete_session({sid}) raised: {e}")
