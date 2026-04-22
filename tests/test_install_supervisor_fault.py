"""MT3 — ``install_supervisor`` mid-run crash + staging cleanup.

The local provider's ``install_supervisor`` stages the npm install in a
sibling directory ``system/supervisor.tmp.<uuid>`` and only renames it into
place after a sentinel (``node_modules/.bin/<bin>``) is present.  If the
npm call raises mid-install (e.g., disk-full, network-failure, SIGKILL),
the staging dir must be cleaned up and the previous ``system/supervisor/``
(if any) must be left intact.  A retried call must succeed on a fresh
staging dir rather than inheriting a half-populated prior attempt.

These properties are the whole point of the staging+rename design
(:mod:`api.providers.local` + :mod:`api.providers.docker`).  This test
file exercises them on the local provider, which is simplest to fault-
inject (we stub ``subprocess.run`` on the ``npm install`` call).
"""
from __future__ import annotations

import os
import shutil
import subprocess
import sys
import uuid
from pathlib import Path
from unittest.mock import patch

import pytest


_SRC = os.path.join(os.path.dirname(__file__), "..", "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)


pytestmark = pytest.mark.skipif(
    shutil.which("npm") is None or shutil.which("node") is None,
    reason="npm + node required for local install_supervisor fault tests",
)


def _vol_name() -> str:
    return f"vol-fault-{uuid.uuid4().hex[:8]}"


@pytest.mark.asyncio
async def test_mid_run_crash_cleans_staging_and_leaves_no_supervisor(
    tmp_path, monkeypatch,
):
    """First install: ``npm install`` raises mid-run.  Staging dir is
    cleaned; no ``system/supervisor/`` emerges (this is a first install,
    no previous state).  Retry on the same volume succeeds."""
    monkeypatch.setenv("AGENT_SDK_LOCAL_VOL_ROOT", str(tmp_path))
    from api.providers import local  # noqa: E402

    name = _vol_name()
    ref = await local.create_volume(name)
    vol = Path(ref)
    system = vol / "system"
    final = system / "supervisor"

    # Sanity: create_volume leaves the target dir empty.
    assert final.is_dir()
    assert list(final.iterdir()) == []

    # Inject the fault on the npm-install call (2nd subprocess.run); let
    # npm init through.  We detect via the cwd being the staging dir and
    # the first argv being "install".
    real_run = subprocess.run
    calls = {"init": 0, "install": 0}

    def faulty_run(argv, *a, **kw):
        is_install = (
            isinstance(argv, list)
            and len(argv) >= 2
            and os.path.basename(argv[0]) == "npm"
            and argv[1] == "install"
        )
        is_init = (
            isinstance(argv, list)
            and len(argv) >= 2
            and os.path.basename(argv[0]) == "npm"
            and argv[1] == "init"
        )
        if is_init:
            calls["init"] += 1
            return real_run(argv, *a, **kw)
        if is_install:
            calls["install"] += 1
            raise OSError(28, "simulated disk-full during npm install")
        return real_run(argv, *a, **kw)

    with patch("api.providers.local.subprocess.run", side_effect=faulty_run):
        with pytest.raises(OSError) as excinfo:
            await local.install_supervisor(ref, "claude")

    assert "disk-full" in str(excinfo.value) or excinfo.value.errno == 28
    # After the fault, ``system/supervisor/`` is either still the original
    # empty dir (create_volume created it) OR does not exist.  Either is
    # acceptable: both mean a subsequent install is safe to retry.
    if final.exists():
        assert final.is_dir()
        assert list(final.iterdir()) == [], (
            f"failed install leaked content into {final}: "
            f"{list(final.iterdir())}"
        )

    # No staging leftovers under system/.
    leftovers = [p for p in system.iterdir() if p.name.startswith("supervisor.tmp.")]
    assert leftovers == [], (
        f"staging dirs must be cleaned up after a failed install: {leftovers}"
    )

    # Retry — unpatched subprocess.run now lets npm install succeed.
    await local.install_supervisor(ref, "claude")
    assert (final / "supervisor.js").is_file()
    assert (final / "node_modules" / ".bin" / "claude-agent-acp").exists()
    # Still no staging leftovers.
    leftovers = [p for p in system.iterdir() if p.name.startswith("supervisor.tmp.")]
    assert leftovers == []


@pytest.mark.asyncio
async def test_mid_run_crash_preserves_previous_install(tmp_path, monkeypatch):
    """A first install already succeeded; a follow-up reinstall crashes
    mid-run.  The original ``system/supervisor/`` must be untouched and
    still fully functional (``supervisor.js`` + ACP bin present)."""
    monkeypatch.setenv("AGENT_SDK_LOCAL_VOL_ROOT", str(tmp_path))
    from api.providers import local

    name = _vol_name()
    ref = await local.create_volume(name)
    final = Path(ref) / "system" / "supervisor"

    # First install — lands cleanly.
    await local.install_supervisor(ref, "claude")
    assert (final / "supervisor.js").is_file()
    sup_js_before = (final / "supervisor.js").read_bytes()
    acp_bin = final / "node_modules" / ".bin" / "claude-agent-acp"
    assert acp_bin.exists()

    # Second install — crash mid-run.
    real_run = subprocess.run

    def faulty_run(argv, *a, **kw):
        if (
            isinstance(argv, list)
            and len(argv) >= 2
            and os.path.basename(argv[0]) == "npm"
            and argv[1] == "install"
        ):
            raise OSError(28, "simulated disk-full during npm install")
        return real_run(argv, *a, **kw)

    with patch("api.providers.local.subprocess.run", side_effect=faulty_run):
        with pytest.raises(OSError):
            await local.install_supervisor(ref, "claude")

    # Previous install survives untouched — supervisor.js bytes match.
    assert final.is_dir()
    assert (final / "supervisor.js").is_file()
    assert (final / "supervisor.js").read_bytes() == sup_js_before, (
        "failed reinstall clobbered a live supervisor.js"
    )
    assert acp_bin.exists(), (
        "failed reinstall removed the prior ACP binary"
    )

    # And no staging leftovers.
    system = final.parent
    leftovers = [p for p in system.iterdir() if p.name.startswith("supervisor.tmp.")]
    assert leftovers == []
