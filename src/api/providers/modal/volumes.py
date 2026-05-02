"""ModalVolumeAdapter — per-volume ops for the modal provider.

Concrete ``BaseVolumeAdapter`` for modal; delegates to the module-level
volume functions in ``modal/__init__.py``. Modal does NOT expose a
``volume_download`` primitive — this adapter inherits the base default
that delegates to ``read``.
"""

from __future__ import annotations

from .._volume import BaseVolumeAdapter
from . import (
    volume_tree as _tree,
    volume_read as _read,
    volume_exists as _exists,
    volume_write as _write,
    volume_upload as _upload,
    volume_mkdir as _mkdir,
    volume_delete as _delete,
    volume_rename as _rename,
)


class ModalVolumeAdapter(BaseVolumeAdapter):
    provider = "modal"

    async def tree(self, path: str = "") -> str:
        return await _tree(self.provider_ref, path)

    async def read(self, path: str) -> bytes:
        return await _read(self.provider_ref, path)

    # download(): inherited from BaseVolumeAdapter — delegates to read().

    async def exists(self, path: str) -> bool:
        return await _exists(self.provider_ref, path)

    async def write(self, path: str, content: bytes) -> None:
        await _write(self.provider_ref, path, content)

    async def upload(self, path: str, content: bytes) -> None:
        await _upload(self.provider_ref, path, content)

    async def mkdir(self, path: str) -> None:
        await _mkdir(self.provider_ref, path)

    async def delete(self, path: str) -> None:
        await _delete(self.provider_ref, path)

    async def rename(self, path: str, new_path: str, *, overwrite: bool = True) -> None:
        await _rename(self.provider_ref, path, new_path, overwrite=overwrite)
