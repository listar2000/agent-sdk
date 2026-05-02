"""DaytonaVolumeAdapter — per-volume ops for the daytona provider.

Concrete ``BaseVolumeAdapter`` for daytona. Delegates to the module-level
functions in ``daytona/__init__.py`` (``volume_tree``, ``volume_read``,
etc.) — those remain the implementation; this file is the typed seam
that callers go through.
"""

from __future__ import annotations

from .._volume import BaseVolumeAdapter
from . import (
    volume_tree as _tree,
    volume_read as _read,
    volume_download as _download,
    volume_exists as _exists,
    volume_write as _write,
    volume_upload as _upload,
    volume_mkdir as _mkdir,
    volume_delete as _delete,
    volume_rename as _rename,
)


class DaytonaVolumeAdapter(BaseVolumeAdapter):
    provider = "daytona"

    async def tree(self, path: str = "") -> str:
        return await _tree(self.provider_ref, path)

    async def read(self, path: str) -> bytes:
        return await _read(self.provider_ref, path)

    async def download(self, path: str) -> bytes:
        return await _download(self.provider_ref, path)

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
