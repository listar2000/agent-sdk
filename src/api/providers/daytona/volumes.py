"""DaytonaVolumeAdapter — per-volume ops for the daytona provider.

Concrete ``BaseVolumeAdapter`` for daytona. Delegates to the module-level
functions in ``daytona/__init__.py`` via late lookup so
``unittest.mock.patch('api.providers.daytona.volume_X')`` keeps working.
"""

from __future__ import annotations

from .._volume import BaseVolumeAdapter


class DaytonaVolumeAdapter(BaseVolumeAdapter):
    provider = "daytona"

    async def tree(self, path: str = "") -> str:
        from . import volume_tree
        return await volume_tree(self.provider_ref, path)

    async def read(self, path: str) -> bytes:
        from . import volume_read
        return await volume_read(self.provider_ref, path)

    async def download(self, path: str) -> bytes:
        from . import volume_download
        return await volume_download(self.provider_ref, path)

    async def exists(self, path: str) -> bool:
        from . import volume_exists
        return await volume_exists(self.provider_ref, path)

    async def write(self, path: str, content: bytes) -> None:
        from . import volume_write
        await volume_write(self.provider_ref, path, content)

    async def upload(self, path: str, content: bytes) -> None:
        from . import volume_upload
        await volume_upload(self.provider_ref, path, content)

    async def mkdir(self, path: str) -> None:
        from . import volume_mkdir
        await volume_mkdir(self.provider_ref, path)

    async def delete(self, path: str) -> None:
        from . import volume_delete
        await volume_delete(self.provider_ref, path)

    async def rename(self, path: str, new_path: str, *, overwrite: bool = True) -> None:
        from . import volume_rename
        await volume_rename(self.provider_ref, path, new_path, overwrite=overwrite)
