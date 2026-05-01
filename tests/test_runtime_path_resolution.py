"""Unit tests for the runtime-path resolution helper.

Pins the contract documented in ``docs/runtime-image-unification.md`` §2.2:
``$AGENT_SDK_RUNTIME_PATH`` wins, then ``/opt/agent-sdk/runtime`` (image
path), then ``<repo>/src/supervisor`` (source-tree fallback). When none of
those resolves, a ``RuntimeError`` with the remediation command is raised.

Pure unit tests — no fixtures, no servers, no real filesystem outside
the repo and ``tmp_path``.
"""
from __future__ import annotations

import os
import sys

import pytest

_SRC = os.path.join(os.path.dirname(__file__), "..", "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

from api.providers import _shared  # noqa: E402
from api.providers._shared import (  # noqa: E402
    _detect_runtime_path,
    _runtime_acp_bin,
    _runtime_supervisor_js,
)


@pytest.fixture
def clear_runtime_env(monkeypatch):
    monkeypatch.delenv("AGENT_SDK_RUNTIME_PATH", raising=False)


def test_explicit_env_var_wins(monkeypatch):
    """Setting AGENT_SDK_RUNTIME_PATH overrides image and source-tree paths.

    No existence check on the value — callers (each provider's
    create_sandbox) raise with a precise file path if the contents are wrong.
    """
    monkeypatch.setenv("AGENT_SDK_RUNTIME_PATH", "/some/explicit/path")
    assert _detect_runtime_path() == "/some/explicit/path"


def test_image_path_when_present(monkeypatch, clear_runtime_env):
    """If /opt/agent-sdk/runtime exists on disk, return it (we're in the image)."""
    def fake_isdir(path):
        return path == _shared._IMAGE_RUNTIME_PATH

    monkeypatch.setattr(_shared.os.path, "isdir", fake_isdir)
    monkeypatch.setattr(_shared.os.path, "exists", lambda p: False)

    assert _detect_runtime_path() == _shared._IMAGE_RUNTIME_PATH


def test_source_tree_fallback(monkeypatch, clear_runtime_env):
    """If image path is missing but the source-tree sentinel exists, return
    <repo>/src/supervisor."""
    monkeypatch.setattr(_shared.os.path, "isdir", lambda p: False)

    def fake_exists(path):
        return path == _shared._SOURCE_RUNTIME_SENTINEL

    monkeypatch.setattr(_shared.os.path, "exists", fake_exists)

    assert _detect_runtime_path() == _shared._SOURCE_RUNTIME_PATH


def test_neither_present_raises(monkeypatch, clear_runtime_env):
    """Both image and source paths missing → RuntimeError with remediation."""
    monkeypatch.setattr(_shared.os.path, "isdir", lambda p: False)
    monkeypatch.setattr(_shared.os.path, "exists", lambda p: False)

    with pytest.raises(RuntimeError) as excinfo:
        _detect_runtime_path()

    msg = str(excinfo.value)
    # Operators reading this error must see both the env-var knob and the
    # source-tree remediation command, otherwise they'll guess.
    assert "AGENT_SDK_RUNTIME_PATH" in msg
    assert "npm --prefix src/supervisor install" in msg


def test_image_wins_over_source(monkeypatch, clear_runtime_env):
    """When both image and source-tree exist (e.g. a built image with the
    repo bind-mounted for hot-reload), the image path takes precedence."""
    monkeypatch.setattr(
        _shared.os.path, "isdir",
        lambda p: p == _shared._IMAGE_RUNTIME_PATH,
    )
    monkeypatch.setattr(
        _shared.os.path, "exists",
        lambda p: p == _shared._SOURCE_RUNTIME_SENTINEL,
    )

    assert _detect_runtime_path() == _shared._IMAGE_RUNTIME_PATH


def test_runtime_supervisor_js_format(monkeypatch):
    """``_runtime_supervisor_js()`` is just <runtime-path>/supervisor.js."""
    monkeypatch.setenv("AGENT_SDK_RUNTIME_PATH", "/x")
    assert _runtime_supervisor_js() == "/x/supervisor.js"


def test_runtime_acp_bin_format(monkeypatch):
    """``_runtime_acp_bin(agent_type)`` is
    ``<runtime-path>/node_modules/.bin/<bin>``."""
    monkeypatch.setenv("AGENT_SDK_RUNTIME_PATH", "/x")
    assert _runtime_acp_bin("claude") == "/x/node_modules/.bin/claude-agent-acp"
    assert _runtime_acp_bin("codex") == "/x/node_modules/.bin/codex-acp"


def test_runtime_acp_bin_rejects_unknown_agent_type(monkeypatch):
    """Unknown agent_type bubbles through ``_acp_bin_name`` as a ValueError."""
    monkeypatch.setenv("AGENT_SDK_RUNTIME_PATH", "/x")
    with pytest.raises(ValueError, match="unsupported agent_type"):
        _runtime_acp_bin("not-a-real-agent")
