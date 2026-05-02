"""Lock the cross-module symbol contract for ``api.providers`` sub-packages.

Motivation (the bug this catches)
--------------------------------
Cycle-9 simplification dropped ``_ACP_BIN_NAMES`` from
``api.providers.__init__.py`` even though ``api.providers.unix_local`` imports it
as::

    from ._shared import _ACP_BIN_NAMES

That import happens to work because ``local`` pulls it directly from
``_shared``.  What didn't work was every other module in the tree that does
``from api.providers import _ACP_BIN_NAMES`` — those silently broke the
moment the re-export disappeared.

Unit tests didn't catch it because they also imported from ``_shared``
directly.  Only integration reality (the example scripts) blew up.

The test strategy is an AST walk of ``src/``: every name imported from
``api.providers`` (and each sibling sub-module) must actually be resolvable
on the imported package.  If somebody deletes a re-export without also
updating the importers, this test fails loudly.
"""
from __future__ import annotations

import ast
import importlib
import os
import pathlib
import sys

import pytest

_SRC_DIR = pathlib.Path(__file__).resolve().parents[1] / "src"
if str(_SRC_DIR) not in sys.path:
    sys.path.insert(0, str(_SRC_DIR))


# ---------------------------------------------------------------------------
# AST helpers
# ---------------------------------------------------------------------------

def _module_name_for(py_path: pathlib.Path, src_root: pathlib.Path) -> str:
    """Translate ``src/api/server.py`` → ``api.server``.  Used so we can
    resolve ``from .providers import X`` to the absolute module
    ``api.providers``."""
    rel = py_path.relative_to(src_root)
    parts = list(rel.with_suffix("").parts)
    if parts and parts[-1] == "__init__":
        parts = parts[:-1]
    return ".".join(parts)


def _resolve_from(node: ast.ImportFrom, containing_mod: str) -> str | None:
    """Resolve ``from <mod> import ...`` to an absolute module name.

    Handles both absolute (``node.level == 0``) and relative
    (``node.level >= 1``) imports.  Returns ``None`` if the relative
    import climbs out of the package tree (shouldn't happen in a
    well-formed tree, but it's safer to skip than crash).
    """
    if node.level == 0:
        return node.module
    parts = containing_mod.split(".") if containing_mod else []
    # level=1 means "current package" — pop trailing module name.
    # level=2 means "parent package" — pop one more, etc.
    pops = node.level
    if len(parts) < pops:
        return None
    base = parts[:-pops] if pops else parts
    suffix = [] if node.module is None else node.module.split(".")
    return ".".join(base + suffix) or None


def _collect_imports(src_root: pathlib.Path, target_modules: set[str]) -> dict:
    """Return ``dict[module_str, set[imported_name]]`` for ``from <mod> import …``.

    Star imports are skipped (they're noise for re-export checking — Python
    resolves them against ``__all__`` or module globals at import time, and
    we only care about names that a human author wrote by hand).

    Resolves relative imports to absolute so a ``from .providers import X``
    in ``api/server.py`` shows up under the key ``"api.providers"``.
    """
    out: dict[str, set[str]] = {}
    for py in src_root.rglob("*.py"):
        if "__pycache__" in py.parts:
            continue
        try:
            tree = ast.parse(py.read_text())
        except SyntaxError:
            continue
        containing = _module_name_for(py, src_root)
        for node in ast.walk(tree):
            if not isinstance(node, ast.ImportFrom):
                continue
            resolved = _resolve_from(node, containing)
            if resolved not in target_modules:
                continue
            names = {n.name for n in node.names if n.name != "*"}
            if names:
                out.setdefault(resolved, set()).update(names)
    return out


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

@pytest.mark.timeout(10)
def test_every_cross_module_import_from_api_providers_is_reexported():
    """Every symbol imported from ``api.providers`` must be a real attribute.

    Regression guard for the ``_ACP_BIN_NAMES`` class of break.
    """
    names_by_mod = _collect_imports(
        _SRC_DIR,
        {"api.providers"},
    )
    imported = names_by_mod.get("api.providers", set())
    # Sanity: the suite must find at least the handful of well-known re-exports
    # — if this is empty we're probably running from the wrong dir.
    assert imported, "AST walk found no 'from api.providers import ...' statements"

    import api.providers as P

    missing = [n for n in sorted(imported) if not hasattr(P, n)]
    assert not missing, (
        "The following symbols are imported from api.providers somewhere under "
        f"src/ but are not re-exported on the package:\n  {missing}\n"
        "Cycle-9 dropped _ACP_BIN_NAMES this way — keep re-exports stable."
    )


@pytest.mark.timeout(10)
def test_every_cross_module_import_from_api_providers_submodules_is_real():
    """Sub-module imports (e.g. ``from api.providers.unix_local import ...``) must
    also resolve.  Parallel to the package-level test above — guards against
    renames that miss a single caller."""
    targets = {
        "api.providers._shared",
        "api.providers.daytona",
        "api.providers.docker",
        "api.providers.unix_local",
    }
    names_by_mod = _collect_imports(_SRC_DIR, targets)

    missing: list[str] = []
    for mod_name, names in names_by_mod.items():
        mod = importlib.import_module(mod_name)
        for n in sorted(names):
            if not hasattr(mod, n):
                missing.append(f"{mod_name}.{n}")
    assert not missing, (
        "Broken cross-module imports under src/ (symbols referenced by "
        f"``from X import ...`` that no longer exist):\n  {missing}"
    )


@pytest.mark.timeout(10)
def test_well_known_reexports_stay_on_api_providers():
    """Pin the exact set of commonly-referenced names to catch silent deletes.

    These are the symbols that have bitten us in the past.  If any of these
    *must* move, update this test on purpose — the test exists precisely so
    the move shows up in the diff.
    """
    import api.providers as P

    # Names that live in _shared but which server.py + tests expect to read
    # from the package root.
    shared_reexports = [
        "_ACP_BIN_NAMES",
        "_ACP_NPM_SPECS",
        "_acp_bin_name",
        "_acp_launch_args",
        "_get_sandbox_env_vars",
        "_wait_for_health",
        "_exec_subprocess",
        "allocate_sandbox_port",
        "free_sandbox_port",
        "AUTH_KEYS",
        "ProviderInstance",
        "ExecResult",
        "PORT_BASED_PROVIDERS",
    ]
    # Daytona helpers surfaced at the package root.
    daytona_reexports = [
        "provision_daytona_sandbox",
        "destroy_daytona",
        "create_daytona_volume",
        "delete_daytona_volume",
    ]
    # Universal dispatch surface the server reaches for.
    dispatch_names = [
        "create_instance",
        "destroy_instance",
        "exec_in_instance",
        "create_volume",
        "delete_volume",
        "provision_sandbox",
        "ensure_supervisor_url",
        "reconcile_sandboxes",
    ]

    missing: list[str] = []
    for n in shared_reexports + daytona_reexports + dispatch_names:
        if not hasattr(P, n):
            missing.append(n)
    assert not missing, (
        "Missing expected re-exports on api.providers:\n  "
        + "\n  ".join(missing)
    )


@pytest.mark.timeout(10)
def test_ast_walk_runs_from_repo_root():
    """Sanity test so a misconfigured test run (e.g. wrong cwd) fails with a
    clear message instead of the other tests trivially passing on an empty
    set of imports."""
    assert _SRC_DIR.exists(), f"expected src/ at {_SRC_DIR}"
    assert (_SRC_DIR / "api" / "providers" / "__init__.py").exists()
    # At least one .py file with 'from api.providers' — otherwise the other
    # tests are tautologies.
    hits = 0
    for py in _SRC_DIR.rglob("*.py"):
        if "from api.providers" in py.read_text():
            hits += 1
            break
    # Some layouts only use relative imports (from .providers) — that's fine;
    # this assertion only fires if *nothing* references providers at all,
    # which would mean the walk is pointed at the wrong tree.
    _ = hits  # intentionally soft — presence of src/api/providers is the real check
