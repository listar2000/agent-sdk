"""``BaseVolumeAdapter`` — interface for per-volume operations.

A ``BaseVolumeAdapter`` is bound to one volume (provider + ``provider_ref``)
at construction. Every method operates inside that volume. Lifecycle
(``create_volume`` / ``delete_volume``) does NOT live here — those are
factory ops that don't fit the per-volume contract; they stay as
module-level functions in each provider's ``__init__.py``.

Concrete adapters live in ``api.providers.<P>.volumes``. The dispatch
factory ``get_volume_adapter(provider, ref)`` lives in
``api.providers.__init__`` and is the supported way to obtain one.

Why an adapter, not module-level dispatch:
- callers stop threading ``provider`` + ``ref`` through every call
- typed, IDE-discoverable surface vs. ``__getattr__`` magic dispatch
- a real seam: tests can subclass with a fake without monkey-patching
  module-level functions
"""

from __future__ import annotations

import abc


class BaseVolumeAdapter(abc.ABC):
    """Per-volume operations for one ``(provider, provider_ref)`` pair.

    Subclasses must implement every method except ``download``, which has
    a default that falls through to ``read`` — providers without a
    streaming download primitive (e.g. modal) inherit it. Override when
    a provider has a cheaper streaming path.
    """

    provider: str = ""

    def __init__(self, provider_ref: str) -> None:
        self.provider_ref = provider_ref

    @abc.abstractmethod
    async def tree(self, path: str = "") -> str:
        """List entries under ``path`` (newline-joined). Empty path = root."""

    @abc.abstractmethod
    async def read(self, path: str) -> bytes:
        """Read the file at ``path`` and return its bytes."""

    @abc.abstractmethod
    async def exists(self, path: str) -> bool:
        """Return True if a file or directory exists at ``path``."""

    @abc.abstractmethod
    async def write(self, path: str, content: bytes) -> None:
        """Write ``content`` to ``path``, creating parent dirs as needed.
        Overwrites if ``path`` exists."""

    @abc.abstractmethod
    async def upload(self, path: str, content: bytes) -> None:
        """Upload ``content`` to ``path``. Same overwrite semantics as
        ``write`` — separate method because some providers have a
        cheaper bulk-upload primitive distinct from edit/write."""

    @abc.abstractmethod
    async def mkdir(self, path: str) -> None:
        """Create the directory at ``path``, including missing parents.
        No error if it already exists."""

    @abc.abstractmethod
    async def delete(self, path: str) -> None:
        """Remove the file or directory at ``path`` (recursive)."""

    @abc.abstractmethod
    async def rename(self, path: str, new_path: str, *, overwrite: bool = True) -> None:
        """Move ``path`` to ``new_path``. When ``overwrite=False`` and
        ``new_path`` exists, raise ``VolumeFileExistsError`` and leave
        both paths untouched. Providers without atomic no-overwrite
        semantics raise a clear unsupported error rather than racing."""

    async def download(self, path: str) -> bytes:
        """Return the bytes at ``path``. Default delegates to ``read``;
        override when a provider has a streaming primitive."""
        return await self.read(path)
