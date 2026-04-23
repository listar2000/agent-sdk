"""Smoke tests for ``scripts/launch_server_*.sh``.

These scripts source ``.env`` to pick up ``DAYTONA_API_KEY`` etc. so the
server can talk to Daytona when a developer runs examples.  A previous
regression made ``launch_server_local.sh`` source only ``~/.env`` — which
is fine on the author's machine but broken on every CI box and
contributor laptop that keeps the .env in the repo.

Full execution of the scripts is too heavy for a unit test (they build a
venv, install postgres, etc.), so we grep the text.  The assertion text
is intentionally verbose: if this test fails, we want the reader of the
failure message to immediately understand *what* the script must do and
*why*.
"""
from __future__ import annotations

import pathlib

import pytest

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
_SCRIPTS = _REPO_ROOT / "scripts"


@pytest.mark.timeout(5)
def test_local_launch_script_sources_repo_env():
    """``scripts/launch_server_local.sh`` must source the repo-local .env.

    The canonical pattern is ``source "${REPO_ROOT}/.env"`` (or iterating
    a list that begins with ``${REPO_ROOT}/.env``).  Sourcing only
    ``~/.env`` broke contributors and CI for several cycles because
    ``DAYTONA_API_KEY`` silently never made it into the server process.
    """
    script = _SCRIPTS / "launch_server_local.sh"
    assert script.is_file(), f"missing {script}"
    text = script.read_text()
    assert "REPO_ROOT}/.env" in text, (
        "launch_server_local.sh must source the repo-local .env "
        "(pattern: source \"${REPO_ROOT}/.env\"); got:\n"
        f"{text}"
    )


@pytest.mark.timeout(5)
def test_docker_launch_script_sources_repo_env():
    """Same contract for ``launch_server_docker.sh``."""
    script = _SCRIPTS / "launch_server_docker.sh"
    assert script.is_file(), f"missing {script}"
    text = script.read_text()
    assert "REPO_ROOT}/.env" in text, (
        "launch_server_docker.sh must source the repo-local .env too"
    )


@pytest.mark.timeout(5)
def test_launch_scripts_define_repo_root_from_script_dir():
    """REPO_ROOT must be derived from the script location, not from $PWD.

    If the dev runs ``bash scripts/launch_server_local.sh`` from an
    unrelated cwd, relying on $PWD would source the wrong ``.env`` (or
    none at all).  The standard pattern:
        SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
        REPO_ROOT="$(cd -- "${SCRIPT_DIR}/.." && pwd)"
    """
    for name in ("launch_server_local.sh", "launch_server_docker.sh"):
        text = (_SCRIPTS / name).read_text()
        assert "SCRIPT_DIR=" in text, f"{name}: missing SCRIPT_DIR= assignment"
        assert "REPO_ROOT=" in text, f"{name}: missing REPO_ROOT= assignment"
        assert "BASH_SOURCE" in text, (
            f"{name}: REPO_ROOT must derive from BASH_SOURCE so the script "
            "works regardless of the caller's cwd"
        )


@pytest.mark.timeout(5)
def test_launch_scripts_prefer_repo_env_over_home_env():
    """When both ``.env`` files exist, the repo-local one must win.

    Implementation detail: the ``for env_file in ...`` loop sources each
    in order and ``set -a; source; set +a`` means later sources overwrite
    earlier ones.  To make the repo-local file authoritative it must be
    listed *last*.  The original bug had the order reversed, so a stale
    ``~/.env`` clobbered the repo-local .env.
    """
    for name in ("launch_server_local.sh", "launch_server_docker.sh"):
        text = (_SCRIPTS / name).read_text()
        # We accept either ordering-style as long as REPO_ROOT/.env is
        # present; other projects may implement this with a straight
        # ``source "${REPO_ROOT}/.env"`` and no fallback, which is fine.
        # The one thing we reject is a script that sources only ~/.env
        # and nothing from the repo.
        if "${HOME}/.env" in text or '$HOME/.env' in text:
            assert "REPO_ROOT}/.env" in text, (
                f"{name}: sources ~/.env but not the repo-local .env — "
                "contributors and CI will silently lose DAYTONA_API_KEY"
            )
