"""Local provider — filesystem-backed volumes and subprocess-backed sandboxes.

Volumes are plain directories on the host under ``AGENT_SDK_LOCAL_VOL_ROOT``
(default ``~/.agent-sdk/volumes/``). Sandboxes are ``supervisor.js`` child
processes whose HOME/root is a per-sandbox subpath of the volume.

Phase 4 of the volumes-on-docker-local plan.
"""
from __future__ import annotations

import asyncio
import glob
import json
import logging
import os
import shutil
import signal
import subprocess
import time
import uuid
from dataclasses import asdict, dataclass, field
from pathlib import Path

from .._shared import (
    ProviderInstance,
    VolumeFileExistsError,
    _ACP_BIN_NAMES,
    _ACP_NPM_SPECS,
    _acp_bin_name,
    _acp_launch_args,
    _find_free_port,
    _get_sandbox_env_vars,
    _port_lock,
    _freed_ports,
    _runtime_acp_bin,
    _runtime_supervisor_js,
    _safe_path,
    _wait_for_health,
)

log = logging.getLogger(__name__)


def _vol_root() -> Path:
    raw = os.environ.get("AGENT_SDK_LOCAL_VOL_ROOT")
    if raw:
        return Path(raw).expanduser().resolve()
    return (Path.home() / ".agent-sdk" / "volumes").resolve()


# Global ref → record index. Independent of ``_vol_root()`` and of where the
# volume directory itself sits, so a sandbox stays discoverable when the
# volume row's ``provider_ref`` points outside the default root (custom
# volume_id, AGENT_SDK_LOCAL_VOL_ROOT pinned to a pytest tmp dir on a prior
# server boot, etc.). Without this, ``destroy_sandbox`` silently no-ops
# (record=None → no kill) and ``start_sandbox`` silently provisions a NEW
# sandbox_ref instead of reattaching — both broke goldens under -n auto.
def _index_dir() -> Path:
    return (Path.home() / ".agent-sdk" / "sandbox-markers").resolve()


def _index_path(ref: str) -> Path:
    return _index_dir() / f"{ref}.json"


# Per-volume marker location is still ``<vol>/system/sandboxes/<ref>.json``
# — kept for back-compat reads (legacy sandboxes created before the index
# existed) and for human/debug discovery alongside the volume's data.
# ``_PROCESSES`` is a current-lifetime cache for ``proc.wait()`` zombie
# reaping only; nothing load-bearing reads it.

@dataclass
class _SandboxRecord:
    ref: str
    pid: int
    port: int
    node: str
    supervisor_js: str
    acp_bin: str
    extra: list[str] = field(default_factory=list)
    effective_root: str = "/tmp"
    base_env: dict[str, str] = field(default_factory=dict)
    # Absolute path of the per-volume marker file (``<vol>/system/
    # sandboxes/<ref>.json``). Stored inside the record so the global
    # index lookup can also clean up the per-volume copy on destroy,
    # even when the volume sits outside the current ``_vol_root()``.
    # Optional for back-compat with records written before this field
    # existed; ``_clear_record`` falls back to a glob in that case.
    marker_path: str = ""


def _load_record(ref: str) -> tuple[Path | None, _SandboxRecord | None]:
    """Sync — callers wrap in ``asyncio.to_thread`` to avoid blocking the loop.

    Reads the global index first (works regardless of volume location);
    falls back to the legacy per-volume glob under ``_vol_root()`` for
    sandboxes created before the index existed.
    """
    idx = _index_path(ref)
    if idx.is_file():
        try:
            return idx, _SandboxRecord(**json.loads(idx.read_text()))
        except (json.JSONDecodeError, TypeError):
            pass
    matches = glob.glob(str(_vol_root() / "*" / "system" / "sandboxes" / f"{ref}.json"))
    if not matches:
        return None, None
    marker = Path(matches[0])
    try:
        return marker, _SandboxRecord(**json.loads(marker.read_text()))
    except (FileNotFoundError, json.JSONDecodeError, TypeError):
        return marker, None


def _write_record(marker: Path, record: _SandboxRecord) -> None:
    record.marker_path = str(marker)
    marker.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(asdict(record))
    tmp = marker.with_suffix(marker.suffix + ".tmp")
    tmp.write_text(payload)
    os.replace(tmp, marker)
    # Mirror to the global index so reads work even when ``_vol_root()`` no
    # longer reaches this volume (env var changes between server runs).
    idx_dir = _index_dir()
    idx_dir.mkdir(parents=True, exist_ok=True)
    idx = _index_path(record.ref)
    tmp = idx.with_suffix(".json.tmp")
    tmp.write_text(payload)
    os.replace(tmp, idx)


def _clear_record(
    ref: str,
    marker: Path | None,
    record: _SandboxRecord | None = None,
) -> None:
    """Remove every persisted handle to ``ref``: the global index entry,
    the per-volume marker known via ``marker``/``record.marker_path``,
    and (legacy back-compat) any per-volume marker discoverable under
    the active ``_vol_root()``. Idempotent."""
    paths: set[Path] = {_index_path(ref)}
    if marker is not None:
        paths.add(marker)
    if record is not None and record.marker_path:
        paths.add(Path(record.marker_path))
    paths.update(
        Path(p) for p in glob.glob(
            str(_vol_root() / "*" / "system" / "sandboxes" / f"{ref}.json")
        )
    )
    for path in paths:
        try:
            os.remove(path)
        except FileNotFoundError:
            pass


def _kill_pid(pid: int) -> None:
    """SIGTERM, poll up to 5s, SIGKILL fallback."""
    if pid <= 0:
        return
    try:
        os.kill(pid, signal.SIGTERM)
    except ProcessLookupError:
        return
    deadline = time.time() + 5.0
    while time.time() < deadline:
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return
        time.sleep(0.05)
    try:
        os.kill(pid, signal.SIGKILL)
    except ProcessLookupError:
        pass


_PROCESSES: dict[str, subprocess.Popen] = {}
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
# Sandbox lifecycle
# ---------------------------------------------------------------------------

async def create_sandbox(
    *,
    volume_ref: str,
    subpath: str,
    agent_type: str = "opencode",
    port: int | None = None,
    spawn_env: dict[str, str] | None = None,
    root: str | None = None,
    dockerfile: str | None = None,  # accepted for parity; no effect on local
    pre_start_commands: list[str] | None = None,  # accepted for parity
    sandbox_ref: str | None = None,  # accepted for parity; local has no labels
    **_: object,
) -> ProviderInstance:
    """Launch a supervisor subprocess rooted at ``<vol>/<subpath>``.

    Returns a ProviderInstance whose ``sandbox_ref`` is the stringified pid
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

    # the runtime-image-unification refactor: the supervisor + ACP bins
    # come from the image runtime path (``/opt/agent-sdk/runtime/`` baked
    # into the agent-sdk Docker image; falls back to ``<repo>/src/supervisor``
    # for source-tree dev). No per-volume install.
    bin_name = _acp_bin_name(agent_type)
    supervisor_js = Path(_runtime_supervisor_js())
    if not supervisor_js.exists():
        raise RuntimeError(
            f"runtime supervisor.js missing at {supervisor_js}. "
            f"Set AGENT_SDK_RUNTIME_PATH or run "
            f"`npm --prefix src/supervisor install`."
        )
    # ``AGENT_SDK_MOCK_ACP_PATH`` overrides the per-agent-type ACP bin
    # selection. Used by ``benchmark/scale/mock_acp.js`` to drive server-
    # saturation benches without going through claude. The supervisor's
    # contract is "stdin/stdout JSON-RPC ACP"; whatever path you point
    # at must implement that.
    mock_acp = os.environ.get("AGENT_SDK_MOCK_ACP_PATH")
    if mock_acp:
        if not Path(mock_acp).exists():
            raise RuntimeError(f"AGENT_SDK_MOCK_ACP_PATH does not exist: {mock_acp}")
        acp_bin_str = mock_acp
    elif agent_type in _ACP_NPM_SPECS:
        acp_bin_str = _runtime_acp_bin(agent_type)
        if not Path(acp_bin_str).exists():
            raise RuntimeError(
                f"runtime ACP binary missing at {acp_bin_str}. "
                f"Rebuild image or re-run npm install in src/supervisor."
            )
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

    # Port allocation is the only race-y bit. ``_find_free_port`` does a
    # bind-probe but the OS can hand the same ephemeral out to another
    # process between our probe and supervisor.js's bind. Under 32-way
    # pytest concurrency this happens occasionally and the supervisor
    # process exits with EADDRINUSE; the docker provider already retries
    # the same way (see ``docker/__init__.py:_is_port_collision``). Loop
    # at most ``port_retries`` times, picking a fresh port each round.
    port_retries = 5
    proc = None
    url = None
    final_port = port
    healthy = False
    last_err = ""
    for _attempt in range(port_retries):
        # Allocate fresh on each attempt. port=0 is treated the same as None.
        if final_port is None or final_port == 0:
            final_port = await _find_free_port()
        try:
            proc = await asyncio.to_thread(
                subprocess.Popen,
                [
                    node, str(supervisor_js),
                    "--host", "127.0.0.1",
                    "--port", str(final_port),
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
                _freed_ports.append(final_port)
            raise

        url = f"http://127.0.0.1:{final_port}"
        # Quick liveness check: did the child immediately die from EADDRINUSE?
        # Polling at ~50ms intervals for up to 500ms catches the common case
        # without delaying the happy path more than two polls. The stderr is
        # async-read in a small buffer so we don't drain forever.
        died_eaddrinuse = False
        for _ in range(10):
            await asyncio.sleep(0.05)
            if proc.poll() is None:
                continue
            # Process exited fast. Read whatever stderr made it.
            try:
                err_bytes = await asyncio.to_thread(
                    lambda: proc.stderr.read() if proc.stderr else b""
                )
            except Exception:
                err_bytes = b""
            last_err = err_bytes.decode(errors="replace")[:400]
            if (b"EADDRINUSE" in err_bytes
                    or b"address already in use" in err_bytes.lower()):
                died_eaddrinuse = True
            break
        if died_eaddrinuse:
            # Pick a fresh port and retry.
            log.warning(
                "local supervisor EADDRINUSE on port %d; retrying", final_port,
            )
            final_port = 0  # force re-allocate next round
            continue
        if proc.poll() is not None:
            # Died for another reason — surface immediately.
            raise RuntimeError(
                f"local supervisor exited rc={proc.returncode} during boot: "
                f"{last_err}"
            )

        try:
            healthy = await _wait_for_health(url)
        except BaseException:
            await asyncio.to_thread(_kill_proc, proc)
            async with _port_lock:
                _freed_ports.append(final_port)
            raise
        if healthy:
            break
        # Not healthy — kill and try a fresh port (could be a stuck listener
        # left behind by a peer process that crashed mid-bind).
        await asyncio.to_thread(_kill_proc, proc)
        async with _port_lock:
            _freed_ports.append(final_port)
        final_port = 0
    if not healthy:
        raise RuntimeError(
            f"local supervisor failed to become healthy after {port_retries} "
            f"port-retries; last err={last_err!r}"
        )
    port = final_port

    # Stable ref that outlives the PID — what sandboxes.sandbox_ref stores.
    ref = f"local-{uuid.uuid4().hex[:12]}"

    # Persist the full record at <volume>/system/sandboxes/<ref>.json — the
    # source of truth that destroy/stop/start/status/reconcile all read.
    record = _SandboxRecord(
        ref=ref, pid=proc.pid, port=port, node=node,
        supervisor_js=str(supervisor_js), acp_bin=acp_bin_str,
        extra=list(extra), effective_root=effective_root,
        base_env=dict(base_env),
    )
    marker_path = vol / "system" / "sandboxes" / f"{ref}.json"
    await asyncio.to_thread(_write_record, marker_path, record)

    async with _PROCESSES_LOCK:
        _PROCESSES[ref] = proc

    log.info("local sandbox started (ref=%s, pid=%d, port=%d, home=%s)",
             ref, proc.pid, port, home_dir)
    return ProviderInstance(
        provider="unix_local",
        url=url,
        root=str(home_dir),
        sandbox_ref=ref,
        port=port,
        process=proc,
    )


async def get_sandbox_status(ref: str) -> str:
    """``missing`` (no marker) | ``stopped`` (marker, dead PID) | ``running``.

    Detection strategy (in order):
      1. If we own the Popen handle for this ref, ``proc.poll()`` is
         authoritative — it distinguishes a live process from a zombie
         the way ``os.kill(pid, 0)`` cannot. After an external SIGKILL,
         the supervisor exits but lingers as a zombie until the parent
         (this server) reaps it; ``os.kill(pid, 0)`` returns success on
         zombies, which would falsely report "running" and trick the
         caller into reusing a port with no live listener.
      2. Otherwise (server restart, no Popen retained), fall back to
         ``os.kill(pid, 0)`` to detect a missing PID. This path doesn't
         see zombies, but it also can't — once the API server restarts
         and re-parents to init, the kernel reaps the zombie itself.
    """
    marker, record = await asyncio.to_thread(_load_record, ref)
    if marker is None:
        return "missing"
    if record is None or record.pid <= 0:
        return "stopped"
    async with _PROCESSES_LOCK:
        proc = _PROCESSES.get(ref)
    if proc is not None:
        rc = await asyncio.to_thread(proc.poll)
        if rc is not None:
            # Process has exited; reap to drop the zombie immediately so the
            # caller's restart path doesn't have to fight a stale entry.
            async with _PROCESSES_LOCK:
                _PROCESSES.pop(ref, None)
            return "stopped"
        return "running"
    try:
        os.kill(record.pid, 0)  # signal-only, returns instantly — safe inline
    except ProcessLookupError:
        return "stopped"
    except PermissionError:
        pass  # exists but not ours — alive enough
    return "running"


async def start_sandbox(ref: str) -> None:
    """Respawn the supervisor for ``ref`` from the persisted spawn plan."""
    marker, record = await asyncio.to_thread(_load_record, ref)
    if record is None:
        raise RuntimeError(f"start_sandbox: no marker for ref {ref}")

    async with _PROCESSES_LOCK:
        existing = _PROCESSES.pop(ref, None)
    if existing is not None:
        await asyncio.to_thread(_kill_pid, existing.pid)

    proc = await asyncio.to_thread(
        subprocess.Popen,
        [
            record.node, record.supervisor_js,
            "--host", "127.0.0.1", "--port", str(record.port),
            "--acp", record.acp_bin, *record.extra,
            "--root", record.effective_root,
        ],
        env=record.base_env,
        stdout=subprocess.DEVNULL, stderr=subprocess.PIPE,
    )

    url = f"http://127.0.0.1:{record.port}"
    if not await _wait_for_health(url):
        await asyncio.to_thread(_kill_pid, proc.pid)
        raise RuntimeError(f"local supervisor failed to become healthy on port {record.port} (ref={ref})")

    record.pid = proc.pid
    await asyncio.to_thread(_write_record, marker, record)
    async with _PROCESSES_LOCK:
        _PROCESSES[ref] = proc
    log.info("local sandbox respawned (ref=%s, pid=%d, port=%d)", ref, proc.pid, record.port)


async def _kill_and_reap(ref: str, record: _SandboxRecord | None) -> None:
    """Kill PID via marker; reap Popen if we still own it (current lifetime).

    If both the marker AND the cached Popen are present (typical) the
    record's pid drives the kill and ``proc.wait`` reaps the zombie.
    If only the cached Popen is present (record lookup failed for any
    reason — pre-fix legacy state, marker corruption), fall back to the
    Popen's own pid so the process doesn't survive.
    """
    if record is not None:
        await asyncio.to_thread(_kill_pid, record.pid)
    async with _PROCESSES_LOCK:
        proc = _PROCESSES.pop(ref, None)
    if proc is not None:
        if record is None and proc.poll() is None:
            await asyncio.to_thread(_kill_pid, proc.pid)
        try:
            await asyncio.to_thread(proc.wait, 2)
        except subprocess.TimeoutExpired:
            pass


async def stop_sandbox(inst: ProviderInstance) -> None:
    """Kill the supervisor; keep the marker so ``start_sandbox(ref)`` can revive."""
    ref = getattr(inst, "sandbox_ref", None)
    if not ref:
        return
    _, record = await asyncio.to_thread(_load_record, ref)
    await _kill_and_reap(ref, record)


async def destroy_sandbox(inst: ProviderInstance) -> None:
    """Kill the supervisor and remove its marker — terminal delete."""
    ref = getattr(inst, "sandbox_ref", None)
    if not ref:
        return
    marker, record = await asyncio.to_thread(_load_record, ref)
    await _kill_and_reap(ref, record)
    await asyncio.to_thread(_clear_record, ref, marker, record)
    port = getattr(inst, "port", None) or (record.port if record else None)
    if port:
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
    *, agent_type: str = "opencode", root: str = "/tmp",
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


async def volume_download(ref: str, path: str) -> bytes:
    """Read raw bytes from ``<volume>/<path>`` for the download endpoint."""
    return await volume_read(ref, path)


async def volume_exists(ref: str, path: str) -> bool:
    """Return whether ``<volume>/<path>`` exists."""
    target = await asyncio.to_thread(_safe_join, ref, path or "")
    return await asyncio.to_thread(os.path.exists, target)


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


async def volume_upload(ref: str, path: str, content: bytes) -> None:
    """Upload bytes to ``<volume>/<path>``."""
    await volume_write(ref, path, content)


async def volume_mkdir(ref: str, path: str) -> None:
    """Create a directory at ``<volume>/<path>``."""
    target = await asyncio.to_thread(_safe_join, ref, path or "")
    if target == os.path.realpath(ref):
        raise ValueError("volume_mkdir: path required")
    await asyncio.to_thread(lambda: os.makedirs(target, exist_ok=True))


async def volume_delete(ref: str, path: str) -> None:
    """Delete a file or directory at ``<volume>/<path>``."""
    target = await asyncio.to_thread(_safe_join, ref, path or "")
    if target == os.path.realpath(ref):
        raise ValueError("volume_delete: path required")

    def _delete() -> None:
        if os.path.isdir(target):
            shutil.rmtree(target)
            return
        if os.path.isfile(target):
            os.remove(target)
            return
        raise FileNotFoundError(path)

    await asyncio.to_thread(_delete)


async def volume_rename(ref: str, path: str, new_path: str, *, overwrite: bool = True) -> None:
    """Rename or move ``<volume>/<path>`` to ``<volume>/<new_path>``."""
    src = await asyncio.to_thread(_safe_join, ref, path or "")
    dst = await asyncio.to_thread(_safe_join, ref, new_path or "")
    root = os.path.realpath(ref)
    if src == root or dst == root:
        raise ValueError("volume_rename: path and new_path required")

    def _rename() -> None:
        if not os.path.exists(src):
            raise FileNotFoundError(path)
        parent = os.path.dirname(dst)
        if parent:
            os.makedirs(parent, exist_ok=True)
        if not overwrite:
            if os.path.isdir(src):
                raise NotImplementedError("atomic no-overwrite directory rename is not supported")
            try:
                os.link(src, dst)
            except FileExistsError as exc:
                raise VolumeFileExistsError(new_path) from exc
            try:
                os.unlink(src)
            except Exception:
                log.exception(
                    "volume_rename overwrite=False linked %s to %s but failed to unlink source",
                    src,
                    dst,
                )
                raise
            return
        os.replace(src, dst)

    await asyncio.to_thread(_rename)


# ---------------------------------------------------------------------------
# Reconciliation — kill orphan supervisor PIDs on startup
# ---------------------------------------------------------------------------

async def reconcile_on_startup() -> None:
    """Kill orphan supervisors and unlink their markers on server boot.

    A marker whose ref isn't in any live session row is an orphan — its
    DELETE never landed. Kill the PID, unlink the marker. Markers from
    active sessions are left alone.
    """
    try:
        from ... import db as dbmod
    except Exception as e:
        log.warning("unix_local reconcile: cannot import api.db: %s", e)
        return
    try:
        live_refs = await dbmod.live_sandbox_refs()
    except Exception as e:
        log.warning("unix_local reconcile: live-session query failed: %s", e)
        return

    def _scan() -> list[tuple[str, int | None]]:
        pattern = str(_vol_root() / "*" / "system" / "sandboxes" / "*.json")
        out: list[tuple[str, int | None]] = []
        for m in glob.glob(pattern):
            path = Path(m)
            if path.stem in live_refs:
                continue
            try:
                pid = int(json.loads(path.read_text()).get("pid", 0)) or None
            except (FileNotFoundError, json.JSONDecodeError, TypeError, ValueError):
                pid = None
            out.append((m, pid))
        return out

    for path, pid in await asyncio.to_thread(_scan):
        if pid:
            try:
                await asyncio.to_thread(os.kill, pid, signal.SIGKILL)
                log.info("unix_local reconcile: killed orphan pid=%d ref=%s", pid, Path(path).stem)
            except ProcessLookupError:
                pass
            except Exception as e:
                log.warning("unix_local reconcile: kill pid=%d failed: %s", pid, e)
        try:
            await asyncio.to_thread(os.remove, path)
        except FileNotFoundError:
            pass
