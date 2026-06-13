"""UnixLocalTransport — record-only native sandbox on the host.

No mocks: the transport's compute IS local subprocesses + a json record in
the provider index, so these run the real thing in a tmp workspace. The
lifecycle contract under test: status is 'running' (record exists) or
'missing' (cleared) — never 'stopped'; hibernate/resume are no-ops (nothing
resident to free); destroy clears the record but keeps workspace files;
exec on a cleared record raises SandboxGoneError (the recreate signal).
"""

from __future__ import annotations

import asyncio
import os
import sys

import pytest


from api.native.transport import (  # noqa: E402
    SandboxGoneError,
    UnixLocalTransport,
)


@pytest.fixture(autouse=True)
def _test_origin(monkeypatch):
    monkeypatch.setenv("AGENT_SDK_ORIGIN", "test")


async def _make(tmp_path, **kw) -> UnixLocalTransport:
    t = UnixLocalTransport(workdir=str(tmp_path), **kw)
    await t.create(root=str(tmp_path))
    return t


@pytest.mark.asyncio
async def test_lifecycle_record_only(tmp_path):
    t = await _make(tmp_path)
    try:
        assert t.ref and t.ref.startswith("native-local-")
        assert await t.status() == "running"

        # hibernate/resume are no-ops: nothing resident, status unchanged,
        # ref stable — the reap path costs ~0 and the sandbox stays usable.
        await t.hibernate()
        assert await t.status() == "running"
        await t.resume()
        assert await t.status() == "running"

        # workspace files persist across destroy (volume-data semantics)...
        (tmp_path / "keep.txt").write_text("data")
        await t.destroy()
        assert await t.status() == "missing"
        assert (tmp_path / "keep.txt").read_text() == "data"
        # ...and the record is actually cleared from the provider index.
        from api.providers.unix_local import _load_record
        assert _load_record(t.ref) == (None, None)
    finally:
        await t.destroy()   # idempotent


@pytest.mark.asyncio
async def test_exec_basics_env_and_gone(tmp_path):
    t = await _make(tmp_path, env={"FOO": "bar", "TOK": "s3cr3t"})
    try:
        r = await t.exec("pwd && echo $FOO")
        assert r.exit_code == 0 and not r.timed_out
        assert str(tmp_path) in r.stdout and "bar" in r.stdout

        # per-call env overrides key-by-key; defaults persist
        r = await t.exec("echo $FOO $TOK", env={"FOO": "baz"})
        assert "baz s3cr3t" in r.stdout

        # plumbing opt-out: default env must be skippable
        r = await t.exec("echo [$FOO]", use_default_env=False)
        assert "[]" in r.stdout

        # a PATH-class session env cannot break the spawn (execve resolves
        # /bin/sh absolutely); it only alters the user command's lookup
        t.default_env["PATH"] = "/nonexistent"
        r = await t.exec("echo ok")     # shell builtin — runs regardless
        assert r.exit_code == 0 and "ok" in r.stdout
        del t.default_env["PATH"]

        # destroyed out-of-band → SandboxGoneError (recreate signal)
        await t.destroy()
        with pytest.raises(SandboxGoneError):
            await t.exec("true")
    finally:
        await t.destroy()


@pytest.mark.asyncio
async def test_exec_timeout_kills_process_group(tmp_path):
    t = await _make(tmp_path)
    try:
        r = await t.exec("sleep 30", timeout_s=1)
        assert r.timed_out and r.exit_code == 124
    finally:
        await t.destroy()


@pytest.mark.asyncio
async def test_file_ops_round_trip(tmp_path):
    t = await _make(tmp_path)
    try:
        await t.write_file("sub/dir/a.txt", b"hello")
        assert await t.read_file("sub/dir/a.txt") == b"hello"
        assert (tmp_path / "sub" / "dir" / "a.txt").read_bytes() == b"hello"
        with pytest.raises(FileNotFoundError):
            await t.read_file("absent.txt")
    finally:
        await t.destroy()


@pytest.mark.asyncio
async def test_session_dispatch_creates_and_reattaches(tmp_path, monkeypatch):
    """NativeSession routes provider='unix_local' to UnixLocalTransport with
    the session env threaded in, and a reattach binds the SAME ref."""
    from api.native.session import NativeSession
    from api.sandbox.state import NativeSandboxState, Recipe

    s = NativeSession(
        session_id="s-ulocal",
        state=NativeSandboxState(provider="unix_local",
                                 recipe=Recipe(agent_type="native")))
    s._started = True
    s._cwd = str(tmp_path)
    s._sandbox_env = {"GITHUB_TOKEN": "ghp_x"}

    async def _noop():
        return None
    s._persist_state = _noop  # type: ignore

    t = await s._ensure_sandbox()
    try:
        assert isinstance(t, UnixLocalTransport)
        assert t.default_env == {"GITHUB_TOKEN": "ghp_x"}
        assert s.state.sandbox_ref == t.ref

        # reattach path: a fresh session object binds the same record
        t2 = s._reattach_transport("unix_local", t.ref)
        assert t2.ref == t.ref
        assert await t2.status() == "running"
        assert t2.default_env == {"GITHUB_TOKEN": "ghp_x"}
    finally:
        await t.destroy()
