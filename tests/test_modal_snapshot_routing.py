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
