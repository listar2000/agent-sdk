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


@pytest.mark.asyncio
async def test_volume_rename_no_overwrite_uses_conditional_create(monkeypatch):
    from api.providers import daytona

    calls: list[str] = []
    captured: dict[str, object] = {}

    async def fake_run(_ref: str, cmd: str, timeout: int = 30):
        calls.append(cmd)
        return SimpleNamespace(stdout="", stderr="", exit_code=0)

    async def fake_supports(_ref: str) -> bool:
        return True

    async def fake_download(_ref: str, _path: str) -> bytes:
        return b"payload"

    async def fake_conditional(_ref: str, abs_path: str, content: bytes) -> str:
        captured["abs_path"] = abs_path
        captured["content"] = content
        return "created"

    async def fake_delete(_ref: str, _path: str) -> None:
        captured["deleted"] = True

    monkeypatch.setattr(daytona, "_run_in_utility_sandbox", fake_run)
    monkeypatch.setattr(daytona, "_daytona_supports_conditional_create", fake_supports)
    monkeypatch.setattr(daytona, "volume_download", fake_download)
    monkeypatch.setattr(daytona, "_conditional_upload_if_absent", fake_conditional)
    monkeypatch.setattr(daytona, "volume_delete", fake_delete)
    await daytona.volume_rename(
        "vol-ref", "shared/a.txt", "shared/sub/b.txt", overwrite=False,
    )

    assert len(calls) == 2
    assert "mkdir -p" in calls[0]
    assert "ln " not in calls[0]
    assert captured == {
        "abs_path": "/v/shared/sub/b.txt",
        "content": b"payload",
        "deleted": True,
    }


@pytest.mark.asyncio
async def test_volume_rename_no_overwrite_exists_maps_to_error(monkeypatch):
    from api.providers import VolumeFileExistsError, daytona

    async def fake_run(_ref: str, _cmd: str, timeout: int = 30):
        return SimpleNamespace(stdout="", stderr="", exit_code=0)

    async def fake_supports(_ref: str) -> bool:
        return True

    async def fake_download(_ref: str, _path: str) -> bytes:
        return b"payload"

    async def fake_conditional(_ref: str, _abs_path: str, _content: bytes) -> str:
        return "exists"

    monkeypatch.setattr(daytona, "_run_in_utility_sandbox", fake_run)
    monkeypatch.setattr(daytona, "_daytona_supports_conditional_create", fake_supports)
    monkeypatch.setattr(daytona, "volume_download", fake_download)
    monkeypatch.setattr(daytona, "_conditional_upload_if_absent", fake_conditional)
    with pytest.raises(VolumeFileExistsError) as exc:
        await daytona.volume_rename(
            "vol-ref", "shared/a.txt", "shared/sub/b.txt", overwrite=False,
        )
    assert exc.value.path == "shared/sub/b.txt"


@pytest.mark.asyncio
async def test_volume_rename_no_overwrite_unsupported_when_conditional_missing(monkeypatch):
    from api.providers import daytona

    async def fake_supports(_ref: str) -> bool:
        return False

    monkeypatch.setattr(daytona, "_daytona_supports_conditional_create", fake_supports)
    with pytest.raises(NotImplementedError, match="not supported"):
        await daytona.volume_rename(
            "vol-ref", "shared/a.txt", "shared/sub/b.txt", overwrite=False,
        )


@pytest.mark.asyncio
async def test_volume_rename_postcondition_failure_maps_to_runtime_error(monkeypatch):
    from api.providers import daytona

    async def fake_run(_ref: str, _cmd: str, timeout: int = 30):
        return SimpleNamespace(stdout="__RENAME_NOT_VISIBLE__", stderr="", exit_code=98)

    monkeypatch.setattr(daytona, "_run_in_utility_sandbox", fake_run)
    with pytest.raises(RuntimeError, match="postcondition failed"):
        await daytona.volume_rename("vol-ref", "shared/a.txt", "shared/sub/b.txt")
