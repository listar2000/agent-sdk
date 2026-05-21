"""Unit tests for ``_cli_install_commands`` / ``_normalize_cli_tools``.

Mirrors ``test_skills_install_command_shape.py`` — pins the wire format
of the CLI install lines so future refactors can't silently change what
ends up in ``recipe.pre_start_commands``.

Decisions pinned here:
  * uv (not pip/pipx) is the install tool — baked into the runtime image.
  * ``uv tool install`` (no ``--reinstall``) is the default — idempotent on
    already-installed sources; callers pin versions for forced upgrade.
  * Dict-form ``version`` becomes ``==<version>`` suffix on the source
    when the source doesn't already pin one (PyPI specifier syntax).
  * VCS sources with ``@<ref>`` already pinned are passed through —
    dict-form ``version`` is silently ignored to avoid stomping the ref.
"""
from __future__ import annotations

import os
import sys

_SRC = os.path.join(os.path.dirname(__file__), "..", "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

from api.server import _cli_install_commands, _normalize_cli_tools  # noqa: E402


def test_list_form_passes_each_source_through():
    cmds = _cli_install_commands(["hive-evolve", "ruff"])
    assert cmds == [
        "uv tool install hive-evolve",
        "uv tool install ruff",
    ]


def test_vcs_url_with_ref_passes_through():
    cmds = _cli_install_commands(["git+https://github.com/owner/repo@v1.2"])
    assert cmds == ["uv tool install git+https://github.com/owner/repo@v1.2"]


def test_dict_form_with_version_appends_pep440_pin():
    cmds = _cli_install_commands(
        {"hive": {"source": "hive-evolve", "version": "1.2.3"}}
    )
    assert cmds == ["uv tool install hive-evolve==1.2.3"]


def test_dict_form_without_version_passes_source_through():
    cmds = _cli_install_commands({"gh": {"source": "gh"}})
    assert cmds == ["uv tool install gh"]


def test_dict_string_value_treated_as_source():
    cmds = _cli_install_commands({"ruff": "ruff==0.5"})
    assert cmds == ["uv tool install ruff==0.5"]


def test_vcs_source_with_existing_ref_ignores_version_field():
    """A VCS URL already pinned to ``@v1`` should not get a ``==`` appended.

    Otherwise we'd produce ``git+...@v1==2.0`` which uv can't parse.
    """
    cmds = _cli_install_commands(
        {"h": {"source": "git+https://github.com/o/r@v1", "version": "2.0"}}
    )
    assert cmds == ["uv tool install git+https://github.com/o/r@v1"]


def test_empty_or_none_returns_no_commands():
    assert _cli_install_commands(None) == []
    assert _cli_install_commands([]) == []
    assert _cli_install_commands({}) == []


def test_dict_entries_without_source_are_dropped():
    """Malformed dict entries (no ``source`` key) are silently dropped."""
    cmds = _cli_install_commands({"empty": {"version": "1.0"}})
    assert cmds == []


def test_normalize_strips_falsy_list_entries():
    """An empty string / None inside a list shouldn't produce ``uv tool install ''``."""
    assert _normalize_cli_tools(["hive", "", None, "ruff"]) == ["hive", "ruff"]


def test_install_lines_are_shlex_safe():
    """A source containing spaces / shell metas must be quoted so the install
    runs as one argument, not getting split. Source-controlled injection
    isn't a real risk (callers own their cli_tools list) but the recipe is
    persisted to JSONB and we don't want quoting drift across reads."""
    cmds = _cli_install_commands(["weird name with spaces"])
    # shlex.quote wraps in single quotes
    assert cmds == ["uv tool install 'weird name with spaces'"]
