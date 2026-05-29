"""Golden: agent recovers after sandbox replacement when the volume has no
Claude session JSONL to load (the wooden-marmot scenario).

Repro shape — exactly what hive-space hit in production on 2026-05-01:

  1. cold-create session → first attach mints inner_session_id X
     (Claude-side session JSONL only exists on the sandbox's ephemeral
     HOME, not on the volume — no turn has happened yet, so no
     snapshot.tar has been written to /vol).
  2. external-delete the sandbox.
  3. UI sends a prompt → pool.get_session() spins up a fresh sandbox
     and the supervisor restores HOME from /vol/snapshot.tar — but
     there's no snapshot.tar, so the new HOME has no JSONL for X.
  4. _attach_acp calls client.attach(..., inner_session_id=X). With
     the bug, attach forces session/load(X) and the ACP server
     returns ``-32603 Internal error`` because that inner session
     doesn't exist on disk; the SSE handler 500s; the agent is
     wedged forever (every subsequent revival hits the same load
     failure → never replies).

Invariants this test asserts:
  A. The prompt after the external delete returns a non-empty reply.
  B. ``inner_session_id`` on the in-memory SessionState is **different**
     from the pre-delete value — proving the recovery path went through
     ``session/new`` (a fresh ACP session) rather than wedging on
     ``session/load`` of the lost id.

This is the failure mode masked by ``test_session_resume_after_delete``,
which deliberately runs a turn (and therefore writes snapshot.tar)
*before* deleting — exercising the happy session/load path. This test
covers the inverse: snapshot.tar is absent, load **must** fail, attach
**must** fall back.

Requires a live server on localhost:7778. Skipped when daytona/docker
credentials are unavailable; local provider preserves HOME across
delete by design (per ``_external_delete``), so it cannot exercise this
failure mode.
"""
from __future__ import annotations

import asyncio
import os
import shutil
import subprocess
import sys

import httpx
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))
load_dotenv(os.path.expanduser("~/.env"), override=False)

from agent_sdk import ApiClient  # noqa: E402

# Reuse the existing helpers / fixtures from the recovery suite — same
# server contract, same teardown semantics, same provider guards. The
# fixture ``_auto_destroy_test_sandboxes`` autouse lives in that module
# and registers anything appended to ``_CREATED_SESSIONS``.
from tests.test_golden import (  # type: ignore[import-not-found]  # noqa: E402
    SERVER, OAUTH_TOKEN, DAYTONA_API_KEY, PROMPT_TIMEOUT,
    _require_provider,
    _quick_session, _ask, _send_message, _collect_reply,
    _get_sandbox, _external_delete,
    _admin_inner_sid,
    _CREATED_SESSIONS,
)


# Local provider's ``_external_delete`` deliberately preserves HOME (the
# JSONL would survive a "delete"), so it cannot exercise this failure
# mode — exclude it from the parametrize. The fix in
# ``acp_client.attach`` is provider-agnostic; we just can't *exercise*
# it on local.
@pytest.mark.parametrize("provider", ["daytona", "docker", "modal"])
@pytest.mark.asyncio
async def test_attach_recovers_after_sandbox_delete_with_empty_volume(provider):
    """Cold-create → external-delete → prompt: must succeed via session/new
    fallback (snapshot.tar was never written because no turn ran).
    """
    _require_provider(provider)

    async with ApiClient(SERVER) as sdk:
        sess = await _quick_session(sdk, provider)
        session_id = sess["session_id"]
        inner_before = sess["inner_session_id"]
        assert inner_before, (
            "test setup error — _quick_session should have set inner_session_id "
            "during cold-create's _attach_acp"
        )
        print(f"\n[test:{provider}] session={session_id[:8]} inner_sid_before={inner_before}")

        # Step (1) is done — fresh sandbox, fresh ACP session, NO turn yet.
        # No snapshot.tar exists on the volume because the supervisor only
        # writes snapshot.tar after a successful turn.

        # Step (2): out-of-band delete of the sandbox. Mirrors the
        # post-cleanup state in production.
        sandbox = await _get_sandbox(sdk, session_id)
        await _external_delete(sandbox)
        await asyncio.sleep(3)

        # Step (3): UI sends a prompt. With the bug this hangs / 500s on
        # _attach_acp's session/load(stale_inner_sid).
        async with ApiClient(SERVER) as sdk2:
            reply = await _ask(sdk2, session_id, "Reply with a single short word.")
            inner_after = await _admin_inner_sid(sdk2, session_id)

        # Invariant A: prompt returned something — the agent recovered.
        assert reply.strip(), (
            f"empty reply after sandbox delete with no volume snapshot: "
            f"{reply!r}. Likely cause: client.attach() raised "
            f"'ACP error [-32603]: Internal error' on session/load and the "
            f"SSE /events handler 500'd."
        )

        # Invariant B: the inner_session_id changed — proving recovery went
        # through session/new (fresh) instead of session/load (which would
        # have failed silently or wedged).
        assert inner_after and inner_after != inner_before, (
            f"inner_session_id should have been replaced by a fresh session/new "
            f"after the volume's JSONL was lost; got "
            f"before={inner_before!r} after={inner_after!r}"
        )
