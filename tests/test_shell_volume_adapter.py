"""ShellVolumeAdapter — the one shell-backed volume-ops protocol.

Pure-unit: a fake ``_run_shell`` captures the generated sh and scripts the
(rc, out, err) reply, pinning the protocol that docker/modal/daytona used
to hand-mirror — sentinels, settle loop, busybox find dialect, base64
transport — plus the uniform missing-path semantics.
"""

from __future__ import annotations

import base64

import pytest

from api.providers._shared import VolumeFileExistsError
from api.providers._volume import ShellVolumeAdapter


class _Scripted(ShellVolumeAdapter):
    provider = "test"

    def __init__(self, ref: str = "vol1", replies=None):
        super().__init__(ref)
        self.calls: list[tuple[str, int]] = []
        self._replies = list(replies or [])

    async def _run_shell(self, shell, *, timeout):
        self.calls.append((shell, timeout))
        if self._replies:
            return self._replies.pop(0)
        return 0, b"", b""


# ── tree ─────────────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_tree_uses_guarded_busybox_three_pass_find():
    a = _Scripted(replies=[(0, b"d sub\nf sub/x.txt\nf top.txt\n", b"")])
    out = await a.tree("")
    shell = a.calls[0][0]
    # -d guard: missing AND non-directory targets are an empty tree (a
    # documented unification — staging docker 500'd on both, modal returned
    # "" for files; "" everywhere now).
    assert shell.startswith("if [ ! -d /v ]; then exit 0; fi; ")
    assert "cd /v 2>/dev/null && (" in shell
    for t in ("-type l", "-type d", "-type f"):
        assert f"find . -mindepth 1 {t} -exec sh -c" in shell
    assert out == "sub/\nsub/x.txt\ntop.txt"


@pytest.mark.asyncio
async def test_tree_subpath_reanchors_and_missing_is_empty():
    a = _Scripted(replies=[(0, b"f y.txt\n", b"")])
    assert await a.tree("nested/dir") == "nested/dir/y.txt"
    # missing subpath OR a file target: the -d guard exits 0 -> ""
    a = _Scripted(replies=[(0, b"", b"")])
    assert await a.tree("absent") == ""
    a = _Scripted(replies=[(0, b"", b"")])
    assert await a.tree("some/file.txt") == ""


@pytest.mark.asyncio
async def test_tree_max_depth_and_rc_policy():
    class Depth(_Scripted):
        tree_max_depth = 3
        tree_check_rc = False
    a = Depth(replies=[(1, b"f partial.txt\n", b"find: boom")])
    # rc swallowed (daytona policy), depth flag present
    assert "-maxdepth 3" in (await a.tree(""), a.calls[0][0])[1]

    strict = _Scripted(replies=[(1, b"", b"find: boom")])
    with pytest.raises(RuntimeError, match="tree failed"):
        await strict.tree("")


# ── read / write / upload ───────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_read_decodes_base64_and_missing_raises():
    payload = b"\x00binary\xff"
    a = _Scripted(replies=[(0, base64.b64encode(payload), b"")])
    assert await a.read("f.bin") == payload
    assert "if [ ! -f /v/f.bin ]; then echo __MISSING__; exit 2; fi;" in a.calls[0][0]

    a = _Scripted(replies=[(2, b"__MISSING__\n", b"")])
    with pytest.raises(FileNotFoundError):
        await a.read("absent.txt")

    with pytest.raises(ValueError):
        await _Scripted().read("")


@pytest.mark.asyncio
async def test_write_base64_pipeline_and_upload_aliases_write():
    a = _Scripted()
    await a.write("sub/f.txt", b"hello")
    shell = a.calls[0][0]
    assert "mkdir -p /v/sub &&" in shell
    assert base64.b64encode(b"hello").decode() in shell
    assert "| base64 -d > /v/sub/f.txt" in shell

    await a.upload("sub/g.txt", b"x")          # same protocol
    assert "| base64 -d > /v/sub/g.txt" in a.calls[1][0]


# ── exists / mkdir / delete ──────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_exists_rc_mapping():
    assert await _Scripted(replies=[(0, b"", b"")]).exists("x") is True
    assert await _Scripted(replies=[(1, b"", b"")]).exists("x") is False
    with pytest.raises(RuntimeError):
        await _Scripted(replies=[(125, b"", b"docker: gone")]).exists("x")


@pytest.mark.asyncio
async def test_delete_sentinel_and_recursive_rm():
    a = _Scripted()
    await a.delete("dir")
    assert "rm -rf -- /v/dir" in a.calls[0][0]
    with pytest.raises(FileNotFoundError):
        await _Scripted(replies=[(2, b"__MISSING__\n", b"")]).delete("absent")


# ── rename ───────────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_rename_overwrite_mv_with_settle_loop():
    a = _Scripted()
    await a.rename("a.txt", "b/c.txt")
    s = a.calls[0][0]
    assert "mv -- /v/a.txt /v/b/c.txt &&" in s
    assert "for _i in 1 2 3 4 5 6 7 8 9 10; do" in s
    assert "__RENAME_NOT_VISIBLE__" in s


@pytest.mark.asyncio
async def test_rename_no_overwrite_link_protocol_and_sentinels():
    a = _Scripted()
    await a.rename("a.txt", "b.txt", overwrite=False)
    s = a.calls[0][0]
    assert ("ln /v/a.txt /v/b.txt || { if [ -e /v/b.txt ]; then echo __EXISTS__;"
            " exit 17; else exit 1; fi; };") in s
    assert "rm -- /v/a.txt || { echo __UNLINK_FAILED__; exit 96; };" in s

    with pytest.raises(VolumeFileExistsError):
        await _Scripted(replies=[(17, b"__EXISTS__\n", b"")]).rename(
            "a", "b", overwrite=False)
    with pytest.raises(NotImplementedError):
        await _Scripted(replies=[(95, b"__UNSUPPORTED_DIR__\n", b"")]).rename(
            "d", "e", overwrite=False)
    with pytest.raises(FileNotFoundError):
        await _Scripted(replies=[(2, b"__MISSING__\n", b"")]).rename("a", "b")
    with pytest.raises(RuntimeError, match="postcondition"):
        await _Scripted(replies=[(98, b"__RENAME_NOT_VISIBLE__\n", b"")]).rename(
            "a", "b")


# ── path safety ──────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_traversal_rejected():
    a = _Scripted()
    for bad in ("../etc/passwd", "a/../../x", "a\x00b"):
        with pytest.raises(Exception):
            await a.read(bad)
    assert a.calls == []   # rejected before any shell ran
