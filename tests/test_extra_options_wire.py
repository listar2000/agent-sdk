"""Wire-format snapshot tests for the extra_options pass-through.

Patches ``AcpClient._send_rpc`` to capture the params for ``session/new``
and asserts the ``_meta.<vendor>.options`` translation is correct without
needing a live server / claude binary.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, patch

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from api.acp_client import (  # noqa: E402
    AcpClient,
    _VENDOR_META_NAMESPACE,
    _meta_for_extra_options,
)


def test_translation_claude():
    out = _meta_for_extra_options("claude", {"tools": [{"name": "Read"}]})
    assert out == {"claudeCode": {"options": {"tools": [{"name": "Read"}]}}}


def test_translation_none_passes_through_as_none():
    assert _meta_for_extra_options("claude", None) is None
    assert _meta_for_extra_options("claude", {}) is None


def test_translation_unknown_agent_warns_and_drops(caplog):
    caplog.set_level("WARNING")
    out = _meta_for_extra_options("opencode", {"x": 1})
    assert out is None
    assert "no _meta namespace mapping" in caplog.text


def test_translation_top_level_copy_isolates_dict_replacement():
    # Top-level dict() copy: replacing keys on src doesn't bleed into out.
    # We don't claim deep-copy semantics — the translation is one-level dict()
    # so callers should treat the dict as immutable after handing it in. This
    # test pins the top-level guarantee.
    src: dict = {"tools": "first"}
    out = _meta_for_extra_options("claude", src)
    assert out is not None
    src["tools"] = "REPLACED"
    src["extraArgs"] = "NEW_KEY"
    assert out["claudeCode"]["options"] == {"tools": "first"}


def test_vendor_map_keys_are_canonical_agent_types():
    # Sanity: we only ship a mapping for agents whose vendor namespace is
    # confirmed. Adding an entry should require code review.
    assert _VENDOR_META_NAMESPACE == {"claude": "claudeCode"}


@pytest.mark.asyncio
async def test_session_new_payload_includes_meta_when_extra_options_set():
    """End-to-end of the params dict for session/new — no live server."""
    captured: list[tuple[str, dict[str, Any]]] = []

    async def fake_send_rpc(self, session_id, method, params):
        captured.append((method, params))
        # Mimic ACP success response for session/new.
        if method == "session/new":
            return {"sessionId": "inner-1234"}
        return {}

    async def fake_handshake(self, *args, **kwargs):
        return {}

    async def fake_set_mode(self, *args, **kwargs):
        return None

    with patch.object(AcpClient, "_send_rpc", new=fake_send_rpc), \
         patch.object(AcpClient, "handshake", new=fake_handshake), \
         patch.object(AcpClient, "set_mode", new=fake_set_mode):
        c = AcpClient(base_url="http://example.invalid")
        await c.initialize(
            "sess-1", "claude",
            cwd="/tmp/x",
            extra_options={"tools": [{"name": "Read"}], "maxThinkingTokens": 10000},
        )

    methods = [m for m, _ in captured]
    assert "session/new" in methods
    new_params = next(p for m, p in captured if m == "session/new")
    assert new_params["cwd"] == "/tmp/x"
    assert new_params["mcpServers"] == []
    assert new_params["_meta"] == {
        "claudeCode": {
            "options": {
                "tools": [{"name": "Read"}],
                "maxThinkingTokens": 10000,
            }
        }
    }


@pytest.mark.asyncio
async def test_session_new_omits_meta_when_extra_options_none():
    captured: list[tuple[str, dict[str, Any]]] = []

    async def fake_send_rpc(self, session_id, method, params):
        captured.append((method, params))
        if method == "session/new":
            return {"sessionId": "inner-1234"}
        return {}

    async def fake_handshake(self, *args, **kwargs):
        return {}

    async def fake_set_mode(self, *args, **kwargs):
        return None

    with patch.object(AcpClient, "_send_rpc", new=fake_send_rpc), \
         patch.object(AcpClient, "handshake", new=fake_handshake), \
         patch.object(AcpClient, "set_mode", new=fake_set_mode):
        c = AcpClient(base_url="http://example.invalid")
        await c.initialize("sess-2", "claude", cwd="/tmp/y")

    new_params = next(p for m, p in captured if m == "session/new")
    assert "_meta" not in new_params, (
        "session/new payload must be byte-identical to pre-change when "
        "extra_options is unset"
    )


@pytest.mark.asyncio
async def test_session_new_unknown_agent_drops_meta_with_warning(caplog):
    captured: list[tuple[str, dict[str, Any]]] = []
    caplog.set_level("WARNING")

    async def fake_send_rpc(self, session_id, method, params):
        captured.append((method, params))
        if method == "session/new":
            return {"sessionId": "inner-x"}
        return {}

    async def fake_handshake(self, *args, **kwargs):
        return {}

    async def fake_set_mode(self, *args, **kwargs):
        return None

    with patch.object(AcpClient, "_send_rpc", new=fake_send_rpc), \
         patch.object(AcpClient, "handshake", new=fake_handshake), \
         patch.object(AcpClient, "set_mode", new=fake_set_mode):
        c = AcpClient(base_url="http://example.invalid")
        await c.initialize(
            "sess-3", "opencode",
            cwd="/tmp/z",
            extra_options={"some": "thing"},
        )

    new_params = next(p for m, p in captured if m == "session/new")
    assert "_meta" not in new_params
    assert "no _meta namespace mapping" in caplog.text
