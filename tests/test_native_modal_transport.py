"""ModalTransport.exec recovery mapping — mocked, no live modal.

Pins the SandboxMissingError → SandboxGoneError mapping that the docker
live-recovery golden's docstring cited as "covered by the modal transport
tests" but which previously had NO test: the modal hard-timeout ceiling can
terminate an active sandbox mid-exec, and that must surface as SandboxGoneError
so NativeSession recreates on the same Volume (the workspace survives there),
not as a generic 500.
"""

from __future__ import annotations

import os
import sys

import pytest


from api.native.transport import ModalTransport, SandboxGoneError  # noqa: E402
from api.providers._shared import ExecResult, SandboxMissingError  # noqa: E402


@pytest.mark.asyncio
async def test_modal_exec_missing_raises_sandbox_gone(monkeypatch):
    import api.providers.modal as md

    async def _exec(inst, cmd, timeout=30):
        raise SandboxMissingError("sandbox terminated (hard-timeout ceiling)")
    monkeypatch.setattr(md, "exec_in_sandbox", _exec)

    t = ModalTransport(sandbox_ref="md-x", workdir="/v/agents/a1")
    with pytest.raises(SandboxGoneError):
        await t.exec("echo hi")


@pytest.mark.asyncio
async def test_modal_exec_success_passes_through(monkeypatch):
    """A healthy modal exec is returned verbatim — the SandboxGoneError mapping
    only fires on SandboxMissingError, not on ordinary results."""
    import api.providers.modal as md

    async def _exec(inst, cmd, timeout=30):
        return ExecResult(stdout="ran", stderr="", exit_code=0)
    monkeypatch.setattr(md, "exec_in_sandbox", _exec)

    t = ModalTransport(sandbox_ref="md-x", workdir="/v/agents/a1")
    r = await t.exec("echo hi")
    assert r.exit_code == 0 and "ran" in r.stdout
