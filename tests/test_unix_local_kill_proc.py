"""Regression: unix_local create-failure cleanup must not NameError.

``create_sandbox``'s health-retry loop kills the freshly-spawned supervisor via
``_kill_proc(proc)`` when it fails to become healthy (lines ~417/423). But
``_kill_proc`` was never defined (only ``_kill_pid`` / ``_kill_and_reap``
existed), so a local supervisor that failed its health check raised
``NameError`` instead of the intended ``RuntimeError`` — a latent crash on the
create-failure path that the happy-path goldens never exercise. This pins the
helper: it must be importable (RED with an ``ImportError`` pre-fix) and must
actually kill + reap the process it's handed.
"""
import os
import subprocess
import sys


def test_kill_proc_is_defined_and_kills_and_reaps():
    # Pre-fix this import raises ImportError (the name the create-failure
    # cleanup path calls did not exist) -> the cleanup path would NameError.
    from api.providers.unix_local import _kill_proc

    proc = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(30)"])
    assert proc.poll() is None, "sanity: child should be running before kill"

    _kill_proc(proc)

    # Killed AND reaped (no lingering zombie): poll() returns the exit status.
    assert proc.poll() is not None, "_kill_proc must terminate (and reap) the process"
