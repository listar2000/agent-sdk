"""Unit tests for Daytona volume FS helper commands.

These tests patch the utility-sandbox executor so no real Daytona API key
or sandbox is required.
"""
from __future__ import annotations

import os
import sys
from types import SimpleNamespace

import pytest

_SRC = os.path.join(os.path.dirname(__file__), "..", "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)


@pytest.mark.asyncio
async def test_volume_mkdir_uses_mkdir_p(monkeypatch):
    from api.providers import daytona

    calls: list[tuple[str, str]] = []

    async def fake_run(ref: str, cmd: str, timeout: int = 30):
        calls.append((ref, cmd))
        return SimpleNamespace(stdout="", stderr="", exit_code=0)

    monkeypatch.setattr(daytona, "_run_in_utility_sandbox", fake_run)
    await daytona.volume_mkdir("vol-ref", "shared/docs/a")

    assert calls
    ref, cmd = calls[0]
    assert ref == "vol-ref"
    assert "mkdir -p" in cmd
    assert "/v/shared/docs/a" in cmd


@pytest.mark.asyncio
async def test_volume_delete_missing_maps_to_filenotfound(monkeypatch):
    from api.providers import daytona

    async def fake_run(_ref: str, _cmd: str, timeout: int = 30):
        return SimpleNamespace(stdout="__MISSING__", stderr="", exit_code=2)

    monkeypatch.setattr(daytona, "_run_in_utility_sandbox", fake_run)
    with pytest.raises(FileNotFoundError):
        await daytona.volume_delete("vol-ref", "shared/missing.txt")


@pytest.mark.asyncio
async def test_volume_rename_creates_parent_and_moves(monkeypatch):
    from api.providers import daytona

    calls: list[str] = []

    async def fake_run(_ref: str, cmd: str, timeout: int = 30):
        calls.append(cmd)
        return SimpleNamespace(stdout="", stderr="", exit_code=0)

    monkeypatch.setattr(daytona, "_run_in_utility_sandbox", fake_run)
    await daytona.volume_rename("vol-ref", "shared/a.txt", "shared/sub/b.txt")

    assert calls
    cmd = calls[0]
    assert "mkdir -p" in cmd
    assert "mv --" in cmd
    assert "/v/shared/a.txt" in cmd
    assert "/v/shared/sub/b.txt" in cmd
