"""``BaseVolumeAdapter`` — interface for per-volume operations.

A ``BaseVolumeAdapter`` is bound to one volume (provider + ``provider_ref``)
at construction. Every method operates inside that volume. Lifecycle
(``create_volume`` / ``delete_volume``) does NOT live here — those are
factory ops that don't fit the per-volume contract; they stay as
module-level functions in each provider's ``__init__.py``.

Concrete adapters live in each provider's ``__init__.py`` (exposed as the
module's ``VolumeAdapter`` attribute). The dispatch factory
``get_volume_adapter(provider, ref)`` in ``api.providers.__init__`` is the
supported way to obtain one.

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


# ---------------------------------------------------------------------------
# ShellVolumeAdapter — the one shell-backed implementation of the contract.
#
# docker, modal, and daytona all execute the SAME sh protocol against a
# volume mounted at ``vol_root``; only the transport that runs the shell
# differs (alpine util container / modal sandbox / daytona utility sandbox).
# Before this class each provider hand-mirrored the protocol — sentinels,
# settle loops, busybox find dialect — and the copies drifted (different
# -maxdepth, different find flags). Subclasses provide ``_run_shell`` and
# may override individual ops where their SDK has a cheaper primitive
# (e.g. daytona download/delete).
# ---------------------------------------------------------------------------

import base64 as _b64
import shlex as _sh

from ._shared import (
    VolumeFileExistsError,
    _safe_path,
    normalize_find_entries,
    normalize_find_output,
)


class ShellVolumeAdapter(BaseVolumeAdapter):
    """All eight ops over one ``_run_shell(shell, *, timeout) -> (rc, out, err)``.

    Class knobs:
      * ``vol_root`` — where the volume is mounted in the shell's namespace.
      * ``tree_max_depth`` — bound the find depth (daytona uses 3); None = full.
      * ``shell_timeout`` — per-op transport timeout in seconds.
      * ``tree_check_rc`` — False preserves daytona's swallow-partial-find
        behaviour; True (default) raises on a failing find.

    Missing-path semantics are uniform: ``tree`` on a nonexistent subpath
    returns ``""`` (guarded, not an error); ``read``/``delete``/``rename``
    raise FileNotFoundError via the ``__MISSING__`` sentinel.
    """

    vol_root: str = "/v"
    tree_max_depth: int | None = None
    shell_timeout: int = 60
    tree_check_rc: bool = True
    #: GNU findutils present in the transport image → single-pass -printf tree
    tree_find_gnu: bool = False

    async def _run_shell(self, shell: str, *, timeout: int) -> tuple[int, bytes, bytes]:
        raise NotImplementedError

    # -- helpers --------------------------------------------------------------

    @staticmethod
    def _rel(path: str) -> str:
        """Normalize + validate a volume-relative path (traversal/control-char
        rejection; the shell only sees the volume mount, so no realpath)."""
        return _safe_path(None, path)

    def _target(self, rel: str) -> str:
        return f"{self.vol_root}/{rel}" if rel else self.vol_root

    def _parent(self, rel: str) -> str:
        return f"{self.vol_root}/" + "/".join(rel.split("/")[:-1])

    def _err(self, op: str, rc: int, err: bytes) -> RuntimeError:
        # Provider tag is load-bearing for ops: during a backend incident the
        # 500s/log greps must distinguish WHICH provider's volumes are failing.
        return RuntimeError(
            f"{self.provider} volume {op} failed (rc={rc}): "
            f"{(err or b'').decode(errors='replace').strip()[:400]}")

    # -- ops -------------------------------------------------------------------

    async def tree(self, path: str = "") -> str:
        rel = self._rel(path)
        qt = _sh.quote(self._target(rel))
        depth = f"-maxdepth {self.tree_max_depth} " if self.tree_max_depth else ""
        if self.tree_find_gnu:
            # Single-pass GNU find: ONE metadata walk. modal (debian_slim) and
            # daytona (debian utility sandbox) both ship GNU findutils, and the
            # 3-pass busybox form is 4-8x slower on large trees — on daytona a
            # >30s tree would blow the exec budget and (rc-swallowed) render a
            # populated volume as EMPTY. Output normalizes identically.
            shell = (
                f"if [ ! -d {qt} ]; then exit 0; fi; "
                f"find {qt} -mindepth 1 {depth}-printf '%y %P\\n' 2>/dev/null"
            )
        else:
            # busybox find lacks -printf: three -type passes with batched
            # -exec (`sh -c '…' sh {} +`: one fork per ARG_MAX batch).
            body = "for p; do printf \"{t} %s\\n\" \"${{p#./}}\"; done"
            shell = (
                f"if [ ! -d {qt} ]; then exit 0; fi; "
                f"cd {qt} 2>/dev/null && ("
                f"find . -mindepth 1 {depth}-type l -exec sh -c '{body.format(t='l')}' sh {{}} + ; "
                f"find . -mindepth 1 {depth}-type d -exec sh -c '{body.format(t='d')}' sh {{}} + ; "
                f"find . -mindepth 1 {depth}-type f -exec sh -c '{body.format(t='f')}' sh {{}} +"
                f") 2>/dev/null"
            )
        rc, out, err = await self._run_shell(shell, timeout=self.shell_timeout)
        if rc != 0 and self.tree_check_rc:
            raise self._err("tree", rc, err)
        normalized = normalize_find_output(out.decode(errors="replace"))
        if not rel or not normalized:
            return normalized
        lines = [f"{rel.rstrip('/')}/{ln}" for ln in normalized.splitlines()]
        return "\n".join(sorted(lines))

    async def tree_entries(self, path: str = "") -> list[dict]:
        """Like :meth:`tree`, but returns structured entries with size + mtime.

        ``[{"path", "is_dir", "size", "mtime"}]`` — paths in the same normalized
        form as ``tree`` (dirs end ``/``), sorted. Only the GNU ``-printf`` path
        carries metadata; on busybox/portable transports (``find`` lacks
        ``-printf``) it degrades to the plain tree with ``size``/``mtime`` of
        ``None`` rather than paying a per-file ``stat`` walk. One ``find`` pass.
        """
        if not self.tree_find_gnu:
            # No -printf here — reuse the plain (already rel-prefixed) tree and
            # report paths only. Callers treat missing metadata as "unknown".
            flat = await self.tree(path)
            return [
                {"path": ln, "is_dir": ln.endswith("/"), "size": None, "mtime": None}
                for ln in flat.splitlines() if ln
            ]
        rel = self._rel(path)
        qt = _sh.quote(self._target(rel))
        depth = f"-maxdepth {self.tree_max_depth} " if self.tree_max_depth else ""
        # Same single GNU walk as tree(), with size (%s) + epoch mtime (%T@)
        # added; tab-delimited so paths containing spaces survive the split.
        shell = (
            f"if [ ! -d {qt} ]; then exit 0; fi; "
            f"find {qt} -mindepth 1 {depth}-printf '%y\\t%s\\t%T@\\t%P\\n' 2>/dev/null"
        )
        rc, out, err = await self._run_shell(shell, timeout=self.shell_timeout)
        if rc != 0 and self.tree_check_rc:
            raise self._err("tree", rc, err)
        entries = normalize_find_entries(out.decode(errors="replace"))
        if rel and entries:
            pre = rel.rstrip("/") + "/"
            for e in entries:
                e["path"] = pre + e["path"]
            entries.sort(key=lambda e: e["path"])
        return entries

    async def read(self, path: str) -> bytes:
        rel = self._rel(path)
        if not rel:
            raise ValueError("volume read: path required")
        q = _sh.quote(self._target(rel))
        shell = (
            f"if [ ! -f {q} ]; then echo __MISSING__; exit 2; fi; "
            f"base64 -w0 {q} 2>/dev/null || base64 {q}"
        )
        rc, out, err = await self._run_shell(shell, timeout=self.shell_timeout)
        if rc != 0:
            if b"__MISSING__" in out:
                raise FileNotFoundError(f"{path} not found on volume {self.provider_ref}")
            raise self._err("read", rc, err)
        try:
            return _b64.b64decode(out.strip())
        except Exception as exc:
            raise RuntimeError(f"volume read: malformed base64 output: {exc}") from exc

    async def exists(self, path: str) -> bool:
        rel = self._rel(path)
        q = _sh.quote(self._target(rel))
        rc, _out, err = await self._run_shell(f"test -e {q}", timeout=self.shell_timeout)
        if rc == 0:
            return True
        if rc == 1:
            return False
        raise self._err("exists", rc, err)

    async def write(self, path: str, content: bytes) -> None:
        rel = self._rel(path)
        if not rel:
            raise ValueError("volume write: path required")
        q = _sh.quote(self._target(rel))
        qp = _sh.quote(self._parent(rel))
        b64 = _b64.b64encode(content).decode()
        shell = (
            f"mkdir -p {qp} && "
            f"printf %s {_sh.quote(b64)} | base64 -d > {q}"
        )
        rc, _out, err = await self._run_shell(shell, timeout=self.shell_timeout)
        if rc != 0:
            raise self._err("write", rc, err)

    async def upload(self, path: str, content: bytes) -> None:
        # Same semantics as write; override where a provider has a cheaper
        # bulk primitive (daytona's SDK upload).
        await self.write(path, content)

    async def mkdir(self, path: str) -> None:
        rel = self._rel(path)
        if not rel:
            raise ValueError("volume mkdir: path required")
        q = _sh.quote(self._target(rel))
        rc, _out, err = await self._run_shell(f"mkdir -p {q}", timeout=self.shell_timeout)
        if rc != 0:
            raise self._err("mkdir", rc, err)

    async def delete(self, path: str) -> None:
        rel = self._rel(path)
        if not rel:
            raise ValueError("volume delete: path required")
        q = _sh.quote(self._target(rel))
        shell = (
            f"if [ ! -e {q} ]; then echo __MISSING__; exit 2; fi; "
            f"rm -rf -- {q}"
        )
        rc, out, err = await self._run_shell(shell, timeout=self.shell_timeout)
        if rc != 0:
            if b"__MISSING__" in out:
                raise FileNotFoundError(f"{path} not found on volume {self.provider_ref}")
            raise self._err("delete", rc, err)

    async def rename(self, path: str, new_path: str, *, overwrite: bool = True) -> None:
        src_rel = self._rel(path)
        dst_rel = self._rel(new_path)
        if not src_rel or not dst_rel:
            raise ValueError("volume rename: path and new_path required")
        qs = _sh.quote(self._target(src_rel))
        qd = _sh.quote(self._target(dst_rel))
        qdp = _sh.quote(self._parent(dst_rel))
        settle = (
            f"for _i in 1 2 3 4 5 6 7 8 9 10; do "
            f"if [ -e {qd} ] && [ ! -e {qs} ]; then exit 0; fi; "
            f"sleep 0.1; "
            f"done; "
            f"echo __RENAME_NOT_VISIBLE__; exit 98"
        )
        if overwrite:
            shell = (
                f"if [ ! -e {qs} ]; then echo __MISSING__; exit 2; fi; "
                f"mkdir -p {qdp} && "
                f"mv -- {qs} {qd} && "
                f"{settle}"
            )
        else:
            shell = (
                f"if [ ! -e {qs} ]; then echo __MISSING__; exit 2; fi; "
                f"mkdir -p {qdp} || exit $?; "
                f"if [ -e {qd} ]; then echo __EXISTS__; exit 17; fi; "
                f"if [ -d {qs} ]; then echo __UNSUPPORTED_DIR__; exit 95; fi; "
                f"ln {qs} {qd} || "
                f"{{ if [ -e {qd} ]; then echo __EXISTS__; exit 17; else exit 1; fi; }}; "
                f"rm -- {qs} || {{ echo __UNLINK_FAILED__; exit 96; }}; "
                f"{settle}"
            )
        rc, out, err = await self._run_shell(shell, timeout=self.shell_timeout)
        if rc != 0:
            if b"__MISSING__" in out:
                raise FileNotFoundError(f"{path} not found on volume {self.provider_ref}")
            if b"__EXISTS__" in out:
                raise VolumeFileExistsError(new_path)
            if b"__UNSUPPORTED_DIR__" in out:
                raise NotImplementedError(
                    "atomic no-overwrite directory rename is not supported")
            if b"__RENAME_NOT_VISIBLE__" in out:
                raise RuntimeError(
                    "volume rename postcondition failed: destination not visible")
            raise self._err("rename", rc, err)
