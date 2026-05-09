"""Live SDK + ApiClient smoke test.

Exercises every public method on ``agent_sdk.ApiClient`` and the most
common ``agent_sdk.Agent`` flows against the running test server. Skipped
when no server is reachable on localhost:7778.

Goal: a single test run answers "do all the SDK entry points still work
end-to-end after a refactor?" — without needing to scan a dozen
narrowly-scoped test files. Pins haiku via top-level ``model="haiku"`` so
we don't burn the OAuth token's weekly quota when running alongside
other suites.
"""
from __future__ import annotations

import asyncio
import os
import time
import uuid

import httpx
import pytest
import pytest_asyncio

from agent_sdk import ApiClient
from agent_sdk.client import Agent
from tests._acp_runtimes import acp_runtime_param

SERVER = os.environ.get("AGENT_SERVER_URL", "http://localhost:7778")

pytestmark = pytest.mark.asyncio


async def _server_up() -> bool:
    try:
        async with httpx.AsyncClient(timeout=2) as c:
            r = await c.get(f"{SERVER}/health")
            return r.status_code == 200
    except Exception:
        return False


@pytest_asyncio.fixture
async def sc():
    if not await _server_up():
        pytest.skip(f"no server at {SERVER}")
    async with ApiClient(SERVER) as client:
        yield client


# ---------------------------------------------------------------------------
# ApiClient — volume CRUD + file ops
# ---------------------------------------------------------------------------


async def test_serverclient_volume_lifecycle(sc):
    """create_volume → list → get → file_write/read/exists/edit/upload/
    download/mkdir/delete/rename → delete_volume. One round-trip per
    method so a regression in any of them surfaces as a clean failure."""
    name = f"smoke-{uuid.uuid4().hex[:8]}"

    # create + dedup-by-name
    vol = await sc.create_volume(name=name, provider="unix_local")
    assert vol["id"] and vol["name"] == name

    # list / get
    listed = await sc.list_volumes(provider="unix_local")
    assert any(v["name"] == name for v in listed), listed
    fetched = await sc.get_volume(vol["id"])
    assert fetched["id"] == vol["id"]

    # write → read → exists
    await sc.volume_file_write(vol["id"], "smoke.txt", "hello smoke\n")
    rd = await sc.volume_file_read(vol["id"], "smoke.txt")
    # Server returns either ``{content: "..."}`` or raw text in ``content``;
    # both shapes carry the bytes we wrote.
    assert "hello smoke" in (rd.get("content") or rd.get("text") or "")
    assert await sc.volume_file_exists(vol["id"], "smoke.txt") is True
    assert await sc.volume_file_exists(vol["id"], "missing.txt") is False

    # search/replace via volume_file_edit — server now handles both
    # ``{path, content}`` (overwrite, used by volume_file_write) and
    # ``{path, old_string, new_string}`` (read → str.replace → write at
    # the volume layer; no sandbox needed).
    await sc.volume_file_edit(
        vol["id"], "smoke.txt",
        old_string="hello smoke", new_string="hello edited",
    )
    rd2 = await sc.volume_file_read(vol["id"], "smoke.txt")
    assert "hello edited" in (rd2.get("content") or rd2.get("text") or "")

    # upload (raw bytes; SDK base64s internally) → download (raw bytes back)
    raw = b"binary-bytes-\x00\x01\x02"
    await sc.volume_file_upload(vol["id"], "binary.bin", raw)
    dl = await sc.volume_file_download(vol["id"], "binary.bin")
    assert dl == raw

    # tree (recursive listing)
    tree = await sc.volume_file_tree(vol["id"])
    # Tree shape varies — accept either ``{entries: [...]}`` or a raw list.
    entries = tree.get("entries") if isinstance(tree, dict) else tree
    if isinstance(entries, list):
        names = {e.get("name") for e in entries}
        assert "smoke.txt" in names or "binary.bin" in names, entries

    # mkdir + rename + delete
    await sc.volume_file_mkdir(vol["id"], "subdir")
    await sc.volume_file_rename(vol["id"], "smoke.txt", "renamed.txt")
    rd3 = await sc.volume_file_read(vol["id"], "renamed.txt")
    assert "hello edited" in (rd3.get("content") or rd3.get("text") or "")
    await sc.volume_file_delete(vol["id"], "binary.bin")
    assert await sc.volume_file_exists(vol["id"], "binary.bin") is False

    # cleanup
    await sc.delete_volume(vol["id"], force=True)


# ---------------------------------------------------------------------------
# ApiClient — session lifecycle + log + file ops + exec + cancel
# ---------------------------------------------------------------------------


async def _drain_until_done(client: httpx.AsyncClient, sid: str, rpc_id: str,
                            timeout: float = 60.0) -> None:
    deadline = time.time() + timeout
    async with client.stream(
        "GET", f"{SERVER}/sessions/{sid}/events",
        headers={"Accept": "text/event-stream"},
    ) as resp:
        resp.raise_for_status()
        buf = ""
        async for chunk in resp.aiter_text():
            buf += chunk
            while "\n\n" in buf:
                block, buf = buf.split("\n\n", 1)
                if rpc_id in block and ('"stopReason"' in block
                                         or '"type":"done"' in block):
                    return
            if time.time() > deadline:
                raise TimeoutError(f"no done within {timeout}s")


async def test_serverclient_session_lifecycle(sc):
    """create_session → status → send_message → log → file_tree/read →
    sandbox_exec → set_config → cancel → delete. Pinned to haiku so the
    one prompt we send doesn't burn the sonnet quota."""
    sess = await sc.create_session(
        provider="unix_local", agent_type="claude", model="haiku",
    )
    sid = sess.get("session_id") or sess["id"]

    try:
        # list / get / status all return the new session
        listed = await sc.list_sessions()
        assert any(s["session_id"] == sid for s in listed), listed
        rec = await sc.get_session(sid)
        assert rec["session_id"] == sid
        status = await sc.get_session_status(sid)
        assert status["session_id"] == sid
        assert status["agent_id"]

        # send a tiny prompt and wait for it to land in session_log
        rpc = await sc.send_message(sid, "Reply with the single word 'ok'.")
        assert rpc.get("rpc_id")
        async with httpx.AsyncClient(timeout=120) as raw:
            await _drain_until_done(raw, sid, rpc["rpc_id"])
        # The log persister runs as a background task; give it a beat.
        await asyncio.sleep(1.5)
        log = await sc.get_session_log(sid, limit=200)
        types = {row["event_type"] for row in log}
        assert "user_message" in types and "turn_end" in types, types

        # file tree + read of the agent's HOME (just verify the round-trip)
        tree = await sc.session_file_tree(sid)
        assert tree is not None  # shape varies; not blowing up is enough

        # sandbox exec (uses /v1/exec, not ACP)
        exec_result = await sc.session_sandbox_exec(sid, "echo hi", timeout=10)
        assert exec_result.get("stdout", "").strip() == "hi", exec_result
        assert exec_result.get("exit_code") == 0

        # sandbox info via the session-scoped route
        sb = await sc.get_session_sandbox(sid)
        assert sb["session_id"] == sid
        assert sb["provider"] == "unix_local"
        assert sb["sandbox_ref"], sb
        assert sb["status"] == "running"
        assert sb.get("url", "").startswith("http://"), sb

        # set_session_config (mode/model/thought_level — opus IS valid here too)
        await sc.set_session_config(sid, model="haiku")

        # cancel — best-effort no-op if no prompt is in flight; must 200
        cancel = await sc.cancel_session(sid)
        assert cancel.get("status") == "ok", cancel
    finally:
        # Idempotent — release pool + drop the rows. Daytona/docker/local
        # compute is paused (not destroyed); label-based scripts reclaim
        # later. Safe to call even if a prior step blew up.
        try:
            await sc.delete_session(sid)
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Agent SDK — the "user-facing" surface
# ---------------------------------------------------------------------------


@acp_runtime_param
async def test_agent_run_streams_done(acp_runtime):
    """``Agent.arun`` returns the final text after streaming ``done``.
    Validates that the SDK's full streaming pipeline (POST /message+stream
    → SSE drain → parse_acp_event → text accumulation) still works."""
    if not await _server_up():
        pytest.skip(f"no server at {SERVER}")
    agent = Agent(
        f"smoke-{uuid.uuid4().hex[:8]}",
        provider="unix_local", api_url=SERVER, **acp_runtime,
    )
    try:
        text = await asyncio.wait_for(
            agent.arun("Reply with exactly the word 'ack' and nothing else."),
            timeout=120,
        )
        assert "ack" in text.lower(), text
    finally:
        await agent.aclose()


@acp_runtime_param
async def test_agent_astream_yields_typed_dicts(acp_runtime):
    """``Agent.astream`` yields parsed event dicts. Smoke-test that
    text events are produced before done."""
    if not await _server_up():
        pytest.skip(f"no server at {SERVER}")
    agent = Agent(
        f"smoke-ev-{uuid.uuid4().hex[:8]}",
        provider="unix_local", api_url=SERVER, **acp_runtime,
    )
    seen_types: set[str] = set()
    try:
        async for ev in agent.astream("Say 'go' once."):
            seen_types.add(ev.get("type", "?"))
            if ev.get("type") == "done":
                break
        assert "done" in seen_types, seen_types
        # Text or tool_result must have been emitted before done — otherwise
        # the streaming pipeline produced an empty turn.
        assert seen_types & {"text", "tool", "tool_result", "reasoning"}, seen_types
    finally:
        await agent.aclose()


@acp_runtime_param
async def test_agent_sandbox_helpers_round_trip(acp_runtime):
    """``Agent.sandbox`` shells out via ``POST /sessions/{id}/sandbox/exec``
    for read_file / write_file / ls / exec. End-to-end smoke that:

      * a freshly-registered Agent has a live sandbox right after the
        first prompt-less ``_ensure_registered`` call (provided it was
        constructed with ``provider=...`` so /sessions provisions
        eagerly),
      * write_file → read_file round-trips bytes faithfully,
      * exec returns parsed ``stdout``/``exit_code``,
      * ls includes the file we just wrote.

    Doesn't fire any LLM prompts, so doesn't burn quota."""
    if not await _server_up():
        pytest.skip(f"no server at {SERVER}")
    agent = Agent(
        f"smoke-sb-{uuid.uuid4().hex[:8]}",
        provider="unix_local", api_url=SERVER, **acp_runtime,
    )
    try:
        # _ensure_registered is private; trigger it via any public call.
        # ``configure`` is cheap and idempotent and forces registration.
        await agent.configure(model=acp_runtime["model"])

        sb = agent.sandbox

        # write → read
        await sb.write_file("/tmp/smoke-sb.txt", "hello sandbox\n")
        content = await sb.read_file("/tmp/smoke-sb.txt")
        assert "hello sandbox" in content, content

        # exec
        r = await sb.exec("echo from-exec")
        assert r["exit_code"] == 0
        assert "from-exec" in r["stdout"]

        # ls — accept either ls -la output or any line containing the file
        listing = await sb.ls("/tmp")
        assert "smoke-sb.txt" in listing, listing
    finally:
        await agent.aclose()
