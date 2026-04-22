"""Local provider — filesystem-backed volumes and subprocess-backed sandboxes.

Volumes are plain directories on the host under ``AGENT_SDK_LOCAL_VOL_ROOT``
(default ``~/.agent-sdk/volumes/``). Sandboxes are ``supervisor.js`` child
processes whose HOME/root is a per-sandbox subpath of the volume.

Phase 4 of the volumes-on-docker-local plan.
"""
from __future__ import annotations

import asyncio
import logging
import os
import shutil
import subprocess
import uuid
from pathlib import Path

from ._shared import (
    AUTH_KEYS,
    ProviderInstance,
    _ACP_BIN_NAMES,
    _ACP_NPM_SPECS,
    _acp_bin_name,
    _acp_launch_args,
    _find_free_port,
    _get_sandbox_env_vars,
    _port_lock,
    _freed_ports,
    _safe_path,
    _wait_for_health,
)

log = logging.getLogger(__name__)

# Repo root:  providers/ → api/ → src/ → repo
_REPO_ROOT = Path(__file__).resolve().parents[3]
_SUPERVISOR_JS_SRC = _REPO_ROOT / "src" / "supervisor" / "supervisor.js"


def _vol_root() -> Path:
    raw = os.environ.get("AGENT_SDK_LOCAL_VOL_ROOT")
    if raw:
        return Path(raw).expanduser().resolve()
    return (Path.home() / ".agent-sdk" / "volumes").resolve()


# Module-level registry of sandbox subprocesses keyed by pid (str).
# ``ProviderInstance`` already carries ``process``, but ``sandbox_ref`` stored
# in the DB is a string; we key this registry by the stringified pid so
# get_sandbox_status/destroy_sandbox can look up the live handle even after
# an instance dict has been rehydrated from the DB.
_PROCESSES: dict[int, subprocess.Popen] = {}
_PROCESSES_LOCK = asyncio.Lock()


# ---------------------------------------------------------------------------
# Path safety
# ---------------------------------------------------------------------------

def _safe_join(ref: str, path: str) -> str:
    """Return the realpath of ``<ref>/<path>``, enforcing containment.

    Thin wrapper over :func:`api.providers._shared._safe_path` that also
    returns the resolved absolute path (callers here need it to open the
    file). The shared helper already handles traversal, control-char and
    realpath-escape checks.
    """
    rel = _safe_path(ref, path or "")
    return os.path.realpath(os.path.join(ref, rel)) if rel else os.path.realpath(ref)


# ---------------------------------------------------------------------------
# Volume CRUD
# ---------------------------------------------------------------------------

async def create_volume(name: str) -> str:
    """Create ``<root>/<name>/{shared,system/supervisor}``. Returns the
    absolute path of the volume as its provider_ref."""
    root = _vol_root()
    vol_dir = root / name
    def _mk():
        os.makedirs(vol_dir / "shared", exist_ok=True)
        os.makedirs(vol_dir / "system" / "supervisor", exist_ok=True)
    await asyncio.to_thread(_mk)
    log.info("local volume created: %s", vol_dir)
    return str(vol_dir)


async def delete_volume(ref: str) -> None:
    """rmtree the volume dir. Tolerate a missing dir."""
    def _rm():
        try:
            shutil.rmtree(ref)
        except FileNotFoundError:
            pass
    await asyncio.to_thread(_rm)
    log.info("local volume deleted: %s", ref)


# ---------------------------------------------------------------------------
# Supervisor install
# ---------------------------------------------------------------------------

async def install_supervisor(ref: str, agent_type: str) -> None:
    """Install supervisor.js + npm deps atomically into ``<ref>/system/supervisor/``.

    Populates a sibling staging dir (``system/supervisor.tmp.<uuid>``) first,
    verifies the expected sentinel (``node_modules/.bin/<bin>`` for npm agents
    or just ``supervisor.js`` otherwise), then atomically swaps it into place
    with ``os.rename``. On any failure the staging dir is removed, leaving
    the previous install (if any) untouched. This makes a half-finished
    ``npm install`` — killed by a disk-full or SIGKILL — safe to retry
    because a re-run starts from a fresh staging dir, not a partially
    populated destination.
    """
    final_dir = Path(ref) / "system" / "supervisor"
    system_dir = final_dir.parent
    await asyncio.to_thread(lambda: os.makedirs(system_dir, exist_ok=True))
    staging = system_dir / f"supervisor.tmp.{uuid.uuid4().hex[:8]}"
    await asyncio.to_thread(lambda: os.makedirs(staging, exist_ok=True))

    def _promote() -> None:
        """Atomically replace final_dir with staging. Caller ensures sentinel."""
        if final_dir.exists():
            # shutil.rmtree is not atomic but we've already verified staging
            # is complete; a concurrent ensure would at worst re-install.
            shutil.rmtree(final_dir)
        os.rename(staging, final_dir)

    try:
        if agent_type not in _ACP_NPM_SPECS:
            # Non-npm agents: copy supervisor.js into staging, verify, swap.
            await asyncio.to_thread(shutil.copy, _SUPERVISOR_JS_SRC, staging / "supervisor.js")
            if not (staging / "supervisor.js").exists():
                raise RuntimeError(f"staging sentinel missing: {staging}/supervisor.js")
            await asyncio.to_thread(_promote)
            log.info("local supervisor installed (non-npm agent_type=%s) on %s", agent_type, ref)
            return

        npm = shutil.which("npm")
        if not npm:
            raise RuntimeError("npm not found; install Node.js >=18")

        spec = _ACP_NPM_SPECS[agent_type]

        def _run_npm_init() -> None:
            subprocess.run(
                [npm, "init", "-y"],
                cwd=str(staging),
                check=True,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
            )

        def _run_npm_install() -> None:
            subprocess.run(
                [npm, "install", "--omit=optional", spec],
                cwd=str(staging),
                check=True,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
            )

        await asyncio.to_thread(_run_npm_init)
        await asyncio.to_thread(_run_npm_install)
        await asyncio.to_thread(shutil.copy, _SUPERVISOR_JS_SRC, staging / "supervisor.js")

        # Sentinel check: the per-agent ACP binary must exist in node_modules.
        sentinel = staging / "node_modules" / ".bin" / _acp_bin_name(agent_type)
        if not sentinel.exists():
            raise RuntimeError(f"staging sentinel missing: {sentinel}")
        if not (staging / "supervisor.js").exists():
            raise RuntimeError(f"staging sentinel missing: {staging}/supervisor.js")

        await asyncio.to_thread(_promote)
        log.info("local supervisor installed on volume %s for agent_type=%s", ref, agent_type)
    except BaseException:
        # Best-effort clean up the staging dir; don't mask the original error.
        await asyncio.to_thread(lambda: shutil.rmtree(staging, ignore_errors=True))
        raise


# ---------------------------------------------------------------------------
# Sandbox lifecycle
# ---------------------------------------------------------------------------

async def create_sandbox(
    *,
    volume_ref: str,
    subpath: str,
    agent_type: str = "claude",
    port: int | None = None,
    spawn_env: dict[str, str] | None = None,
    root: str | None = None,
    dockerfile: str | None = None,  # accepted for parity; no effect on local
    pre_start_commands: list[str] | None = None,  # accepted for parity
    sandbox_id: str | None = None,  # accepted for parity; local has no labels
    **_: object,
) -> ProviderInstance:
    """Launch a supervisor subprocess rooted at ``<vol>/<subpath>``.

    Returns a ProviderInstance whose ``sandbox_id`` is the stringified pid
    of the supervisor process; the live ``Popen`` is also kept in
    ``_PROCESSES`` for later status/destroy lookups by pid.
    """
    if agent_type not in _ACP_BIN_NAMES:
        raise ValueError(f"unsupported agent_type: {agent_type!r}")

    effective_env = dict(spawn_env or {})

    node = shutil.which("node")
    if not node:
        raise RuntimeError("node binary not found; install Node.js >=18")

    vol = Path(volume_ref)
    sub = (subpath or "").lstrip("/")
    home_dir = vol / sub
    sup_dir = vol / "system" / "supervisor"
    supervisor_js = sup_dir / "supervisor.js"

    if not supervisor_js.exists():
        raise RuntimeError(
            f"supervisor.js missing at {supervisor_js}; call install_supervisor first"
        )

    # Resolve the ACP binary: volume install for npm agents, PATH for others.
    bin_name = _acp_bin_name(agent_type)
    if agent_type in _ACP_NPM_SPECS:
        acp_bin = sup_dir / "node_modules" / ".bin" / bin_name
        if not acp_bin.exists():
            raise RuntimeError(
                f"ACP binary missing at {acp_bin}; call install_supervisor({agent_type!r}) first"
            )
        acp_bin_str = str(acp_bin)
    else:
        system_bin = shutil.which(bin_name)
        if not system_bin:
            raise RuntimeError(
                f"{bin_name} not found in PATH for agent_type={agent_type!r}"
            )
        acp_bin_str = system_bin

    def _mkhome():
        os.makedirs(home_dir, exist_ok=True)
        os.makedirs(home_dir / ".claude", exist_ok=True)
    await asyncio.to_thread(_mkhome)

    # Allocate a port via the shared allocator.
    if port is None:
        port = await _find_free_port()

    # Build the supervisor env. Local provider is by definition single-tenant
    # on the user's own host — inherit the host's AUTH_KEYS (CLAUDE_CODE_OAUTH_TOKEN,
    # ANTHROPIC_API_KEY, etc.) so the user's locally-configured Claude credentials
    # flow naturally without the SDK having to re-forward them. Caller-supplied
    # env in ``effective_env`` can still override.
    base_env = dict(os.environ)
    base_env.update(_get_sandbox_env_vars(effective_env))
    base_env["HOME"] = str(home_dir)
    base_env["CLAUDE_CONFIG_DIR"] = str(home_dir / ".claude")
    base_env["AGENT_SHARED_DIR"] = str(vol / "shared")

    # Bridge the host user's existing Claude credentials into the per-sandbox
    # CLAUDE_CONFIG_DIR on first start. Makes ``claude setup-token`` done once
    # on the host flow naturally to every sandbox without re-auth per session.
    host_cred = Path.home() / ".claude" / ".credentials.json"
    sandbox_cred = home_dir / ".claude" / ".credentials.json"
    if host_cred.is_file() and not sandbox_cred.exists():
        try:
            shutil.copy(host_cred, sandbox_cred)
        except Exception as e:
            log.warning("could not bridge host .credentials.json: %s", e)

    launch_args = _acp_launch_args(agent_type)
    extra: list[str] = []
    for a in launch_args:
        extra += ["--acp-arg", a]

    effective_root = root or str(home_dir)

    try:
        proc = await asyncio.to_thread(
            subprocess.Popen,
            [
                node, str(supervisor_js),
                "--host", "127.0.0.1",
                "--port", str(port),
                "--acp", acp_bin_str,
                *extra,
                "--root", effective_root,
            ],
            env=base_env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
        )
    except Exception:
        async with _port_lock:
            _freed_ports.append(port)
        raise

    url = f"http://127.0.0.1:{port}"
    try:
        healthy = await _wait_for_health(url)
    except BaseException:
        # ``_kill_proc`` calls blocking ``proc.wait(timeout=5)`` twice; running
        # it on the event-loop thread would stall every other coroutine for up
        # to ten seconds.  Offload to a worker thread.
        await asyncio.to_thread(_kill_proc, proc)
        async with _port_lock:
            _freed_ports.append(port)
        raise
    if not healthy:
        await asyncio.to_thread(_kill_proc, proc)
        async with _port_lock:
            _freed_ports.append(port)
        raise RuntimeError(f"local supervisor failed to become healthy on port {port}")

    async with _PROCESSES_LOCK:
        _PROCESSES[proc.pid] = proc

    log.info("local sandbox started (pid=%d, port=%d, home=%s)", proc.pid, port, home_dir)
    return ProviderInstance(
        provider="local",
        url=url,
        root=str(home_dir),
        sandbox_id=str(proc.pid),
        port=port,
    )


def _kill_proc(proc: subprocess.Popen) -> None:
    """Best-effort terminate → kill."""
    if proc.poll() is not None:
        return
    try:
        proc.terminate()
        try:
            proc.wait(timeout=5)
            return
        except subprocess.TimeoutExpired:
            pass
        proc.kill()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            pass
    except ProcessLookupError:
        pass


def _lookup_proc(ref: str) -> subprocess.Popen | None:
    try:
        pid = int(ref)
    except (TypeError, ValueError):
        return None
    return _PROCESSES.get(pid)


async def get_sandbox_status(ref: str) -> str:
    """Return running / stopped / missing / error based on ``proc.poll()``."""
    proc = _lookup_proc(ref)
    if proc is None:
        return "missing"
    rc = proc.poll()
    if rc is None:
        return "running"
    if rc == 0:
        return "stopped"
    return "error"


async def start_sandbox(ref: str) -> None:
    """Local subprocesses are not restartable — ``ensure_sandbox`` will
    reprovision on status=missing. No-op for parity with the interface."""
    return None


async def stop_sandbox(inst: ProviderInstance) -> None:
    """For local, stop == destroy (nothing valuable lives in the process)."""
    await destroy_sandbox(inst)


async def destroy_sandbox(inst: ProviderInstance) -> None:
    """Terminate the supervisor subprocess and drop it from the registry."""
    proc = None
    pid: int | None = None

    # Resolve both from the instance.process and from the sandbox_id → pid map.
    if hasattr(inst, "process") and inst.process is not None:
        proc = inst.process  # may be an asyncio.subprocess or subprocess.Popen
        pid = getattr(proc, "pid", None)
    sid = getattr(inst, "sandbox_id", None) if hasattr(inst, "sandbox_id") else None
    if sid:
        try:
            pid = int(sid)
        except (TypeError, ValueError):
            pass
    if proc is None and pid is not None:
        proc = _PROCESSES.get(pid)

    if proc is None:
        return

    # Branch on subprocess flavor.
    if isinstance(proc, subprocess.Popen):
        await asyncio.to_thread(_kill_proc, proc)
    else:
        # asyncio.subprocess.Process
        try:
            if proc.returncode is None:
                proc.terminate()
                try:
                    await asyncio.wait_for(proc.wait(), timeout=5)
                except asyncio.TimeoutError:
                    proc.kill()
                    await proc.wait()
        except ProcessLookupError:
            pass

    if pid is not None:
        async with _PROCESSES_LOCK:
            _PROCESSES.pop(pid, None)

    port = getattr(inst, "port", None)
    if port is not None:
        async with _port_lock:
            _freed_ports.append(port)
        try:
            inst.port = None
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Supervisor URL
# ---------------------------------------------------------------------------

async def ensure_supervisor_url(
    inst: ProviderInstance,
    *, agent_type: str = "claude", root: str = "/tmp",
    spawn_env: dict | None = None, port: int | None = None,
) -> str:
    """Local: the supervisor started at create_sandbox time. No-op, return
    the URL already on the instance.

    Signature matches Daytona's ``ensure_supervisor_url`` exactly so
    mis-spelled kwargs surface as TypeError instead of being silently
    swallowed by a ``**_kw`` catch-all."""
    return inst.url


# ---------------------------------------------------------------------------
# Volume file ops (direct FS in-process, with realpath containment)
# ---------------------------------------------------------------------------

async def volume_tree(ref: str, path: str = "") -> str:
    """Return a newline-separated tree listing of ``<ref>/<path>``.

    Symlinks are not followed; any path that resolves outside ``ref`` is
    rejected by ``_safe_join``.

    ``path`` matches the uniform provider API (docker/daytona expose the
    same name).  The previous ``subpath`` name is dropped — callers that
    used the keyword will get a TypeError, which surfaces a clear mismatch
    rather than a silent ``**kw``-swallowed pass-through.
    """
    target = await asyncio.to_thread(_safe_join, ref, path or "")

    def _walk() -> str:
        if not os.path.exists(target):
            return ""
        lines: list[str] = []
        root_real = os.path.realpath(ref)
        for dirpath, dirnames, filenames in os.walk(target, followlinks=False):
            # Keep deterministic ordering for tests.
            dirnames.sort()
            filenames.sort()
            rel = os.path.relpath(dirpath, root_real)
            if rel == ".":
                rel = ""
            for d in dirnames:
                lines.append((os.path.join(rel, d) + "/").lstrip("/"))
            for f in filenames:
                lines.append(os.path.join(rel, f).lstrip("/"))
        lines.sort()
        return "\n".join(lines)

    return await asyncio.to_thread(_walk)


async def volume_read(ref: str, path: str) -> bytes:
    """Read a file from the volume. Symlink-escape is rejected.

    Hardened against TOCTOU: after ``_safe_join`` resolves the path we
    reopen via ``openat(O_NOFOLLOW)`` relative to a directory fd of the
    parent so a concurrent rename-over with a symlink can't escape the
    volume between resolution and open.
    """
    target = await asyncio.to_thread(_safe_join, ref, path)

    def _read() -> bytes:
        parent_dir, basename = os.path.split(target)
        if not basename:
            raise IsADirectoryError(target)
        # Open the parent directory O_NOFOLLOW so a symlink swap on the
        # parent itself fails here rather than silently redirecting.
        parent_fd = os.open(parent_dir, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            fd = os.open(
                basename,
                os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC,
                dir_fd=parent_fd,
            )
            try:
                chunks: list[bytes] = []
                while True:
                    buf = os.read(fd, 1 << 20)  # 1 MiB chunks
                    if not buf:
                        break
                    chunks.append(buf)
                return b"".join(chunks)
            finally:
                os.close(fd)
        finally:
            os.close(parent_fd)

    return await asyncio.to_thread(_read)


async def volume_write(ref: str, path: str, content: bytes) -> None:
    """Write to the volume. Creates parent dirs. Symlink-escape is rejected.

    Hardened against TOCTOU: opens the target via ``openat(O_NOFOLLOW)``
    relative to a directory fd of the parent so a concurrent rename-over
    with a symlink can't redirect the write outside the volume.

    ``content`` is bytes-only (matching docker/daytona).  Callers with a
    ``str`` payload must encode() at the call site; leaving the implicit
    encoding here diverged the local signature from the other providers
    and defeated load-time arg checking.
    """
    target = await asyncio.to_thread(_safe_join, ref, path)
    if not isinstance(content, (bytes, bytearray, memoryview)):
        raise TypeError(
            f"volume_write: content must be bytes, got {type(content).__name__}"
        )
    data = bytes(content)

    def _write() -> None:
        parent_dir, basename = os.path.split(target)
        if not basename:
            raise IsADirectoryError(target)
        # os.makedirs is fine here: even if it races with a symlink
        # plant, the subsequent O_NOFOLLOW open of the parent directory
        # will refuse to follow a symlink that tries to retarget it.
        os.makedirs(parent_dir, exist_ok=True)
        parent_fd = os.open(parent_dir, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            fd = os.open(
                basename,
                os.O_WRONLY | os.O_CREAT | os.O_TRUNC | os.O_NOFOLLOW | os.O_CLOEXEC,
                0o644,
                dir_fd=parent_fd,
            )
            try:
                to_write = memoryview(data)
                while to_write:
                    n = os.write(fd, to_write)
                    to_write = to_write[n:]
            finally:
                os.close(fd)
        finally:
            os.close(parent_fd)

    await asyncio.to_thread(_write)
