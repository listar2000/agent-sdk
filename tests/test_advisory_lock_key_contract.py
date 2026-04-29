"""Regression guard for cross-worker supervisor-install locking."""
from __future__ import annotations

import ast
from pathlib import Path


def test_ensure_volume_supervisor_does_not_use_python_hash_for_lock_key():
    """Postgres advisory lock keys must be stable across Python processes.

    Python's built-in ``hash()`` is salted per interpreter by default, so two
    uvicorn workers can compute different lock keys for the same
    (volume_id, agent_type).  The implementation should use a deterministic
    digest-derived int instead.
    """
    server_path = Path(__file__).resolve().parents[1] / "src" / "api" / "server.py"
    tree = ast.parse(server_path.read_text())
    funcs = [
        n for n in ast.walk(tree)
        if isinstance(n, ast.AsyncFunctionDef) and n.name == "ensure_volume_supervisor"
    ]
    assert funcs, "ensure_volume_supervisor not found"
    offenders = []
    for node in ast.walk(funcs[0]):
        if isinstance(node, ast.Assign):
            target_names = [t.id for t in node.targets if isinstance(t, ast.Name)]
            if "lock_key" in target_names and isinstance(node.value, ast.BinOp):
                left = node.value.left
                if (
                    isinstance(left, ast.Call)
                    and isinstance(left.func, ast.Name)
                    and left.func.id == "hash"
                ):
                    offenders.append(node.lineno)
    assert not offenders, (
        "ensure_volume_supervisor derives lock_key with built-in hash() at "
        f"line(s) {offenders}; use a stable hash such as sha256 instead"
    )
