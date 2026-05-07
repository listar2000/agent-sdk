"""Unit tests for ``_skills_install_commands`` flag selection.

The ``--all`` flag tells ``npx skills add`` to install every skill in a
repo. With a ``@<skill-name>`` suffix on the source, ``--all`` would
override the filter and pull hundreds of unwanted skills (e.g.
``github/awesome-copilot`` ships azure/dotnet/gtm/arize bundles).

So ``--all`` is added only when the source has no ``@<skill>`` filter.
"""
from __future__ import annotations

import os
import sys

_SRC = os.path.join(os.path.dirname(__file__), "..", "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

from api.server import _skills_install_commands  # noqa: E402


def test_single_skill_filter_is_respected():
    cmds = _skills_install_commands(
        ["github/awesome-copilot@excalidraw-diagram-generator"]
    )
    assert len(cmds) == 1
    assert "github/awesome-copilot@excalidraw-diagram-generator" in cmds[0]
    assert "--yes" in cmds[0]
    assert "--all" not in cmds[0]
    assert cmds[0].endswith(" -g")


def test_repo_without_filter_uses_all():
    cmds = _skills_install_commands(["rllm-org/hive"])
    assert len(cmds) == 1
    assert "rllm-org/hive" in cmds[0]
    assert "--yes" in cmds[0]
    assert "--all -g" in cmds[0]


def test_branch_ref_without_skill_filter_still_uses_all():
    cmds = _skills_install_commands(["rllm-org/hive#staging"])
    assert len(cmds) == 1
    assert "rllm-org/hive#staging" in cmds[0]
    assert "--yes" in cmds[0]
    assert "--all -g" in cmds[0]


def test_dict_form_with_skill_filter_is_respected():
    cmds = _skills_install_commands(
        {"frontend": {"source": "anthropics/skills@frontend-design"}}
    )
    assert len(cmds) == 1
    assert "anthropics/skills@frontend-design" in cmds[0]
    assert "--yes" in cmds[0]
    assert "--all" not in cmds[0]


def test_mixed_list_picks_flag_per_entry():
    cmds = _skills_install_commands(
        [
            "anthropics/skills@frontend-design",
            "rllm-org/hive",
        ]
    )
    assert len(cmds) == 2
    assert "--all" not in cmds[0]  # filtered → no --all
    assert "--yes" in cmds[0]
    assert "--all -g" in cmds[1]   # unfiltered → --all
    assert "--yes" in cmds[1]


def test_empty_or_none_returns_no_commands():
    assert _skills_install_commands(None) == []
    assert _skills_install_commands([]) == []
    assert _skills_install_commands({}) == []
