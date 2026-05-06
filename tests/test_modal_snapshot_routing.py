from __future__ import annotations

import os
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

_SRC = os.path.join(os.path.dirname(__file__), "..", "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

from api.providers import modal as mprov  # noqa: E402


def test_dockerfile_packages_modal_snapshot_tag() -> None:
    """The deployed API image must carry the Modal snapshot pin.

    Without this file in the image, ``modal._get_image()`` cannot see the
    committed snapshot id and silently falls back to ``Image.from_dockerfile``,
    putting production back on the slow cold-start path.
    """
    dockerfile = Path(__file__).resolve().parents[1] / "Dockerfile"
    text = dockerfile.read_text()

    assert ".modal-snapshot-tag*" in text


@pytest.mark.asyncio
async def test_get_image_prefers_committed_modal_snapshot(monkeypatch) -> None:
    snap_id = (Path(__file__).resolve().parents[1] / ".modal-snapshot-tag").read_text().strip()
    calls: list[str] = []

    class FakeImage:
        @staticmethod
        def from_id(image_id: str):
            calls.append(image_id)
            return SimpleNamespace(kind="snapshot", image_id=image_id)

        @staticmethod
        def from_dockerfile(_dockerfile: str):
            raise AssertionError("Image.from_dockerfile should not be used when snapshot tag exists")

    fake_modal = SimpleNamespace(Image=FakeImage)
    monkeypatch.setattr(mprov, "_image", None)
    monkeypatch.setattr(mprov, "_require_modal", lambda: (fake_modal, SimpleNamespace()))

    image = await mprov._get_image()

    assert image.kind == "snapshot"
    assert calls == [snap_id]


@pytest.mark.asyncio
async def test_modal_runs_pre_start_before_supervisor_health(monkeypatch) -> None:
    events: list[tuple[str, str]] = []

    class FakeTunnel:
        url = "https://example.modal.host"

    class FakeSandbox:
        object_id = "sb-test"
        stdout = SimpleNamespace(read=lambda: "")
        stderr = SimpleNamespace(read=lambda: "")

        def tunnels(self, _timeout: int):
            return {mprov._SUPERVISOR_CONTAINER_PORT: FakeTunnel()}

        def terminate(self):
            events.append(("terminate", ""))

    fake_sb = FakeSandbox()

    class FakeSandboxFactory:
        @staticmethod
        def create(*args, **_kwargs):
            events.append(("create", args[2]))
            return fake_sb

    async def fake_get_app():
        return SimpleNamespace(app_id="app-test")

    async def fake_get_image():
        return SimpleNamespace()

    async def fake_get_volume(_ref: str):
        return SimpleNamespace()

    async def fake_exec(_sb, cmd: str, *, timeout: int):
        events.append(("exec", cmd))
        return 0, "", ""

    async def fake_wait(url: str, *, max_retries: int, interval: float):
        events.append(("health", url))
        return True

    from api.providers import _shared as shared

    monkeypatch.setattr(shared, "_runtime_acp_bin_relative", lambda _agent_type: "node_modules/.bin/claude-agent-acp")
    monkeypatch.setattr(mprov, "_require_modal", lambda: (SimpleNamespace(Sandbox=FakeSandboxFactory), SimpleNamespace()))
    monkeypatch.setattr(mprov, "_get_app", fake_get_app)
    monkeypatch.setattr(mprov, "_get_image", fake_get_image)
    monkeypatch.setattr(mprov, "_get_volume", fake_get_volume)
    monkeypatch.setattr(mprov, "_exec_modal_shell", fake_exec)
    monkeypatch.setattr(mprov, "_wait_for_health", fake_wait)

    result = await mprov.create_sandbox(
        volume_ref="vol-test",
        subpath="sessions/test",
        pre_start_commands=["echo setup"],
        shared_mounts=["workspace"],
    )

    assert result.sandbox_ref == "sb-test"
    assert [name for name, _ in events] == ["create", "exec", "exec", "health"]
    entrypoint = events[0][1]
    assert "tail -f /dev/null" in entrypoint
    assert "echo setup" not in entrypoint
    assert "supervisor.js" not in entrypoint
    assert "echo setup" in events[1][1]
    assert "nohup" in events[2][1]
