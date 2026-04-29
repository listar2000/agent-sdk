"""Daytona provider — create/destroy/exec in Daytona sandboxes."""

import asyncio
import logging
import os
import shlex
import time
import uuid
from pathlib import Path
from typing import NamedTuple

from .. import load_dotenv

log = logging.getLogger(__name__)

load_dotenv()


class _ExecResult(NamedTuple):
    stdout: str
    stderr: str
    exit_code: int | None

    @property
    def ok(self) -> bool:
        # exit_code None = SDK didn't report it → treat as OK, log warning elsewhere
        return self.exit_code in (None, 0)


def _run_sandbox_exec(sandbox, cmd: str, timeout: int = 120) -> "_ExecResult":
    """Run ``cmd`` in ``sandbox`` and return stdout, stderr, and exit_code.

    Defensive against SDK versions that may not expose all fields.
    Callers that want tolerant behaviour for a command that may fail should
    wrap their command with ``|| true`` so the shell always exits 0.
    """
    r = sandbox.process.exec(cmd, timeout=timeout)
    return _ExecResult(
        stdout=getattr(r, "result", "") or "",
        stderr=getattr(r, "stderr", "") or "",
        exit_code=getattr(r, "exit_code", None),
    )


# Re-import shared helpers from __init__ to avoid circular imports.
# These are defined here inline or imported lazily.
from ._shared import (
    _acp_bin_name,
    _acp_launch_args,
    _ACP_NPM_SPECS,
    _build_env_prefix,
    _get_sandbox_env_vars,
    _safe_path,
    _wait_for_health,
    build_supervisor_argv,
    ProviderInstance,
    VolumeFileExistsError,
    _build_volume_mounts,
    normalize_find_output,
)

_SUPERVISOR_DIR = Path(__file__).resolve().parent.parent.parent / "supervisor"
_SUPERVISOR_REMOTE_DIR = "/tmp/agent-sdk-sup"  # legacy path (pre-volume era)
_SUPERVISOR_VOLUME_DIR = "/opt/supervisor"      # volume-mounted path (Phase 2+)
_SUPERVISOR_REMOTE_PORT = 9100

# The agent's HOME inside a Daytona sandbox — a LOCAL ext4 directory the
# supervisor creates on boot. It is populated either from the snapshot
# tarball at _SNAPSHOT_PATH (see below) or left empty for a brand-new agent.
# Critically this is NOT a volume mount: mountpoint-s3 can't handle
# append-only writes (session JSONLs) or POSIX rename semantics, so we
# keep the hot filesystem local and only round-trip a single tarball to
# the volume.
_DAYTONA_AGENT_HOME = "/home/daytona"

# Per-session volume subpath mount point. Daytona-only. supervisor.js
# writes the rolling workspace snapshot to `{_DAYTONA_VOLUME_MOUNT}/snapshot.tar`
# after every turn and restores from it on boot.
_DAYTONA_VOLUME_MOUNT = "/vol"
_SNAPSHOT_PATH = f"{_DAYTONA_VOLUME_MOUNT}/snapshot.tar"


# Single label so test orphans can be identified and bulk-deleted via
# ``daytona.list(labels={"agent_sdk_origin": "test"})``. Production
# sandboxes default to ``"production"`` so the same query never touches
# them. Set ``AGENT_SDK_ORIGIN=test`` in the test process before launching
# the server.
_LABEL_ORIGIN = "agent_sdk_origin"


def _sandbox_labels() -> dict[str, str]:
    return {_LABEL_ORIGIN: os.environ.get("AGENT_SDK_ORIGIN", "production")}


def _get_daytona_client():
    """Get a Daytona SDK client. Raises ImportError or RuntimeError on failure."""
    from daytona_sdk import Daytona, DaytonaConfig
    api_key = os.environ.get("DAYTONA_API_KEY")
    if not api_key:
        raise RuntimeError("DAYTONA_API_KEY not set")
    return Daytona(DaytonaConfig(api_key=api_key))


async def start_supervisor_in_sandbox(
    sandbox, agent_type: str, port: int, root: str = "/tmp",
    spawn_env: dict[str, str] | None = None,
) -> str:
    """Start a NEW supervisor on a specific port inside an existing sandbox.

    If the volume has a cached deps.tar.gz (Phase 2+), extract it to a local
    ephemeral directory and run supervisor from there.  Extraction from a
    single archive read is fast; writing thousands of node_modules to the
    network volume at install time is avoided entirely.

    Falls back to the legacy /tmp install path when no volume cache exists.
    Returns the signed preview URL for this supervisor.

    Emits ``[BENCH] daytona.start_supervisor phase=<name> s=<seconds>`` log
    lines for each critical-path phase; grep-friendly for recovery-time
    benchmarks (scripts/bench_recovery.py).
    """
    bin_name = _acp_bin_name(agent_type)
    loop = asyncio.get_running_loop()
    sid8 = sandbox.id[:8] if sandbox.id else "?"
    total_t0 = time.monotonic()
    phases: list[tuple[str, float]] = []

    def _bench(phase: str, t0: float) -> None:
        dt = time.monotonic() - t0
        phases.append((phase, dt))
        log.info("[BENCH] daytona.start_supervisor sandbox=%s phase=%s s=%.3f",
                 sid8, phase, dt)

    def _exec(cmd: str, timeout: int = 120) -> str:
        return _run_sandbox_exec(sandbox, cmd, timeout=timeout).stdout

    # (0) Idempotency fast-path: if a supervisor is already healthy on
    # this port (left over from a prior call in the same sandbox), skip
    # the 2-3 s extract+spawn dance and just mint a fresh signed URL
    # pointing at it. Cuts the recovery cascade time dramatically when
    # the SSE reader's _recover_after_disconnect fires concurrently with
    # POST /message's _ensure_state_live → _rebind_state — both end up
    # here under separate sandbox locks, and without this they each
    # respawn node, churning the signed URL twice for no gain.
    t0 = time.monotonic()
    try:
        existing = await loop.run_in_executor(None, lambda: _exec(
            # `-m 2` request-timeout, `-o /dev/null -w '%{http_code}'`
            # prints just the status line so we can string-match cheaply.
            # NOTE: supervisor exposes /v1/health (matches _wait_for_health
            # in providers/_shared.py), not /healthz.
            f"curl -s -m 2 -o /dev/null -w '%{{http_code}}' "
            f"http://127.0.0.1:{port}/v1/health 2>/dev/null || echo 000",
            10,
        ))
    except Exception as e:
        # Not fatal — fall through to the normal spawn path.
        existing = "000"
        log.debug("idempotency probe raised for sandbox=%s port=%d: %s",
                  sid8, port, e)
    _bench("idempotency_probe", t0)
    if existing.strip() == "200":
        t0 = time.monotonic()
        signed = await loop.run_in_executor(
            None, lambda: sandbox.create_signed_preview_url(port, 24 * 3600)
        )
        _bench("mint_url_only", t0)
        url = signed.url.rstrip("/")
        total_dt = time.monotonic() - total_t0
        log.info(
            "[BENCH] daytona.start_supervisor sandbox=%s TOTAL s=%.3f "
            "(reused-existing %s)",
            sid8, total_dt,
            ", ".join(f"{p}={d:.2f}" for p, d in phases),
        )
        log.info(
            "supervisor on port %d already running, reusing: %s "
            "(sandbox %s)", port, url[:60], sandbox.id[:16],
        )
        return url

    vol_tarball = f"{_SUPERVISOR_VOLUME_DIR}/deps.tar.gz"
    vol_supervisor = f"{_SUPERVISOR_VOLUME_DIR}/supervisor.js"
    local_work = f"/tmp/sup-work-{port}"

    # (1) Cache-visibility check. S3-backed FUSE on daytona has seconds-level
    # write-to-read propagation, so a freshly-installed supervisor may not
    # be visible immediately. Exponential backoff instead of the old 10×1s:
    # first check is instant for the hot path (install ran long ago); slow
    # path still has ~6s total before giving up.
    t0 = time.monotonic()
    backoffs = [0.0, 0.2, 0.4, 0.8, 1.6, 3.2]  # sum ≈ 6.2 s
    check_result = "no"
    for delay in backoffs:
        if delay > 0:
            await asyncio.sleep(delay)
        check_result = await loop.run_in_executor(
            None, lambda: _exec(f"test -f {vol_tarball} && echo yes || echo no")
        )
        if check_result.strip() == "yes":
            break
    _bench("cache_check", t0)

    if check_result.strip() != "yes":
        diag = await loop.run_in_executor(
            None, lambda: _exec(
                f"ls -la {_SUPERVISOR_VOLUME_DIR} 2>&1 | head -10; "
                f"echo '---'; mount | grep -i supervisor"
            )
        )
        log.warning(
            "start_supervisor_in_sandbox: volume cache not visible after retries "
            "(sandbox %s); /opt/supervisor contents:\n%s",
            sandbox.id[:16], diag,
        )

    if check_result.strip() == "yes":
        # (2) Volume-cached mode: extract deps tarball to local ephemeral dir
        # AND resolve the ACP bin symlink in the same exec — one fewer
        # sandbox.process.exec round-trip (~100-300ms savings). Emit the
        # resolved path on a parseable ``ACP_BIN=...`` line.
        t0 = time.monotonic()
        extract_out = await loop.run_in_executor(None, lambda: _exec(
            f"set -e && "
            f"mkdir -p {local_work} && "
            # Copy the tarball off the slow volume first; S3-backed FUSE
            # reads block the tar streaming decode if a chunk hasn't been
            # fetched yet and show up as silent tar data corruption.
            f"cp {vol_tarball} /tmp/deps-{port}.tar.gz && "
            f"tar -C {local_work} -xzf /tmp/deps-{port}.tar.gz && "
            f"cp {vol_supervisor} {local_work}/supervisor.js && "
            f"rm -f /tmp/deps-{port}.tar.gz && "
            # npm install should set +x on bin entries — but tar sometimes
            # strips it when packing + extracting across hosts. Re-apply.
            f"target=$(readlink -f {local_work}/node_modules/.bin/{bin_name}) && "
            f"chmod +x \"$target\" && "
            f"echo \"ACP_BIN=$target\"",
            120,
        ))
        _bench("extract", t0)
        sup_dir = local_work
        # Parse ACP_BIN=... from the extract output (last line of set -e chain).
        acp_bin = f"{sup_dir}/node_modules/.bin/{bin_name}"
        for line in (extract_out or "").splitlines():
            if line.startswith("ACP_BIN="):
                acp_bin = line[len("ACP_BIN="):].strip() or acp_bin
                break
        log.info("start_supervisor_in_sandbox: using volume cache → %s "
                 "(port %d, sandbox %s, acp_bin=%s)",
                 local_work, port, sandbox.id[:16], acp_bin)
    else:
        # Legacy path: deps are installed directly in the sandbox.
        sup_dir = _SUPERVISOR_REMOTE_DIR
        acp_bin = f"{sup_dir}/node_modules/.bin/{bin_name}"
        # Separate round-trip only on the legacy fallback.
        t0 = time.monotonic()
        resolved = await loop.run_in_executor(None, lambda: _exec(
            f"readlink -f {sup_dir}/node_modules/.bin/{bin_name}"
        ))
        _bench("symlink_resolve_legacy", t0)
        if resolved.strip():
            acp_bin = resolved.strip()
        log.info("start_supervisor_in_sandbox: using legacy path %s (port %d, sandbox %s)",
                 _SUPERVISOR_REMOTE_DIR, port, sandbox.id[:16])

    env_prefix = _build_env_prefix(spawn_env)
    log_file = f"{sup_dir}/sup-{port}.log"
    supervisor_argv = build_supervisor_argv(
        supervisor_js="supervisor.js", acp_bin=acp_bin,
        acp_launch_args=_acp_launch_args(agent_type),
        port=port, root=root,
        snapshot_path=_SNAPSHOT_PATH, quote_paths=False,
    )
    # Ensure the agent's HOME (``root``) exists inside the sandbox before
    # the supervisor spawns the ACP child with ``cwd=root``. If the volume
    # subpath hasn't been pre-created, node.spawn() fails with a
    # misleading ENOENT pointing at the binary rather than the cwd.
    inner = (
        f"mkdir -p {shlex.quote(root)} && "
        f"cd {sup_dir} && "
        f"setsid env {env_prefix} {supervisor_argv} "
        f"> {log_file} 2>&1 </dev/null & echo started"
    )
    start_cmd = f"sh -c {shlex.quote(inner)}"

    # (3) Parallelize the detached-spawn exec with the signed-URL mint.
    # The URL doesn't depend on whether node has booted yet, and the
    # spawn exec returns as soon as setsid forks — both are in-flight
    # network calls that we don't need to serialize.
    t0 = time.monotonic()
    _, signed = await asyncio.gather(
        loop.run_in_executor(None, lambda: _exec(start_cmd, timeout=10)),
        loop.run_in_executor(
            None, lambda: sandbox.create_signed_preview_url(port, 24 * 3600)
        ),
    )
    _bench("spawn+mint_url", t0)
    url = signed.url.rstrip("/")

    t0 = time.monotonic()
    # Budget covers worst-case Type 2 boot inside supervisor.js: cold
    # snapshot poll (15 s) + agent_memory poll (15 s) + ACP child spawn
    # (~2 s) + slack. Type 1 (warm restart, sentinel present) finishes
    # in <2 s and exits this poll on the first probe.
    healthy = await _wait_for_health(url, max_retries=45, interval=1)
    _bench("health_wait", t0)
    if not healthy:
        log_out = await loop.run_in_executor(None, lambda: _exec(f"tail -40 {log_file} 2>&1"))
        raise RuntimeError(
            f"supervisor on port {port} in sandbox {sandbox.id} failed health check; log:\n{log_out[:800]}"
        )

    total_dt = time.monotonic() - total_t0
    log.info("[BENCH] daytona.start_supervisor sandbox=%s TOTAL s=%.3f (%s)",
             sid8, total_dt, ", ".join(f"{p}={d:.2f}" for p, d in phases))
    log.info("supervisor on port %d ready: %s (sandbox %s, dir %s)", port, url[:60], sandbox.id[:16], sup_dir)
    return url


async def kill_supervisor_in_sandbox(sandbox, port: int) -> None:
    """Kill a supervisor process by port inside a Daytona sandbox."""
    loop = asyncio.get_running_loop()
    try:
        await loop.run_in_executor(
            None,
            lambda: _run_sandbox_exec(sandbox, f"fuser -k {port}/tcp 2>/dev/null || true", timeout=10),
        )
    except Exception as e:
        log.warning("kill_supervisor_in_sandbox port=%d failed: %s", port, e)


async def provision_daytona_sandbox(
    agent_type: str = "claude",
    dockerfile: str | None = None,
    pre_start_commands: list[str] | None = None,
    root: str = "/tmp",
    volume_id: str | None = None,
    subpath: str | None = None,
    shared_mounts: list[str] | None = None,
) -> ProviderInstance:
    """Create a Daytona sandbox with 3 volume mounts, but do NOT install deps
    or start a supervisor (those are handled by ensure_volume_supervisor and
    ensure_supervisor_url respectively).

    Returns a ProviderInstance with sandbox_id but no usable supervisor URL.
    Supervisors are started per-session via start_supervisor_in_sandbox().
    The supervisor binary + ACP package are expected to already be installed on
    the volume at system/supervisor/ (mounted at /opt/supervisor).
    """
    try:
        from daytona_sdk import (
            Daytona, DaytonaConfig, CreateSandboxFromImageParams,
            CreateSandboxFromSnapshotParams,
        )
    except ImportError:
        raise RuntimeError("daytona-sdk not installed. Run: pip install daytona-sdk")

    api_key = os.environ.get("DAYTONA_API_KEY")
    if not api_key:
        raise RuntimeError("DAYTONA_API_KEY not set")

    env_vars = _get_sandbox_env_vars()
    loop = asyncio.get_running_loop()
    daytona = Daytona(DaytonaConfig(api_key=api_key))

    snapshot = os.environ.get("DAYTONA_SNAPSHOT", "hive-large").strip()
    use_snapshot = dockerfile is None and snapshot.lower() not in {"", "0", "false", "image"}

    if dockerfile is not None:
        if not Path(dockerfile).exists():
            raise FileNotFoundError(f"Dockerfile not found: {dockerfile}")
        from daytona_sdk import Image
        image = Image.from_dockerfile(dockerfile)
    elif not use_snapshot:
        image = "node:22-slim"

    create_timeout = 300 if dockerfile else 60

    volumes = _build_volume_mounts(volume_id, subpath, shared_mounts)
    labels = _sandbox_labels()

    if use_snapshot:
        sandbox = await loop.run_in_executor(None, lambda: daytona.create(
            CreateSandboxFromSnapshotParams(
                snapshot=snapshot, auto_stop_interval=0, env_vars=env_vars,
                volumes=volumes, labels=labels,
            ), timeout=create_timeout,
        ))
    else:
        sandbox = await loop.run_in_executor(None, lambda: daytona.create(
            CreateSandboxFromImageParams(
                image=image, auto_stop_interval=0, env_vars=env_vars,
                volumes=volumes, labels=labels,
            ), timeout=create_timeout,
        ))

    try:
        # Run pre-start commands (skills, CLI install, etc.).
        # On non-zero exit the command raises so provisioning fails loudly.
        # Callers that want tolerant behaviour should wrap their command with
        # ``|| true`` so the shell always exits 0.
        if pre_start_commands:
            for cmd in pre_start_commands:
                log.info("provision pre-start: %s", cmd)
                result = await loop.run_in_executor(
                    None, lambda c=cmd: _run_sandbox_exec(sandbox, c, timeout=120),
                )
                if result.exit_code is None:
                    log.warning(
                        "pre-start command ran but Daytona SDK returned no exit_code "
                        "— can't confirm success: %s", cmd,
                    )
                elif result.exit_code != 0:
                    snippet = (result.stderr or result.stdout or "")[-500:]
                    log.error(
                        "pre-start command failed (exit=%s): %s\n---stderr---\n%s",
                        result.exit_code, cmd, snippet,
                    )
                    raise RuntimeError(
                        f"pre_start_commands failed on Daytona sandbox "
                        f"(exit={result.exit_code}): {cmd!r}\n{snippet}"
                    )
                else:
                    log.info("provision pre-start OK (exit=0): %s", cmd)

        log.info("sandbox provisioned: %s (no supervisor yet)", sandbox.id[:16])
        return ProviderInstance(
            provider="daytona",
            url="",  # no supervisor URL yet
            root=root,
            sandbox_id=sandbox.id,
        )
    except BaseException:
        try:
            await loop.run_in_executor(None, lambda: daytona.delete(sandbox))
        except Exception:
            pass
        raise


async def restart_daytona_supervisor(
    daytona_sandbox_id: str, agent_type: str = "claude", root: str = "/tmp",
    spawn_env: dict[str, str] | None = None,
) -> ProviderInstance:
    """Re-attach to an existing daytona sandbox and respawn the supervisor
    inside it. Used by the resume path after the sandbox was stopped (or
    after the supervisor process was killed in place). Preserves the
    sandbox filesystem, so claude-agent-acp's persisted session state is
    available for session/load.

    Routes through ``start_supervisor_in_sandbox`` which reads from the
    per-volume deps.tar.gz cache installed by ``install_supervisor``. Any
    post-volume-refactor sandbox has that cache; sandboxes old enough to
    lack it are no longer supported (pre-2026-04).
    """
    try:
        from daytona_sdk import Daytona, DaytonaConfig
    except ImportError:
        raise RuntimeError("daytona-sdk not installed. Run: pip install daytona-sdk")
    api_key = os.environ.get("DAYTONA_API_KEY")
    if not api_key:
        raise RuntimeError("DAYTONA_API_KEY not set")

    loop = asyncio.get_running_loop()
    daytona = Daytona(DaytonaConfig(api_key=api_key))
    # Wait for a stable (non-transitional) state before attempting start.
    # Without this, an external stop that's still in progress makes
    # `sandbox.start()` reject with "Sandbox state change in progress",
    # which kills the fast recovery path that preserves /events
    # subscribers. Bumped from 15 s → 45 s after observing 2x concurrent
    # load take ~30 s for Daytona's stopping→stopped transition to land.
    sandbox, state_str = await _wait_for_stable_daytona_state(
        daytona, daytona_sandbox_id, max_wait_s=45.0,
    )
    if state_str not in ("started", "running"):
        log.info("starting stopped daytona sandbox %s (state=%s)",
                 daytona_sandbox_id, state_str)
        await loop.run_in_executor(None, sandbox.start)
        await _wait_for_daytona_sandbox_ready(daytona, daytona_sandbox_id)
        sandbox = await loop.run_in_executor(None, lambda: daytona.get(daytona_sandbox_id))

    # Volume-cached path. Uses the fixed supervisor port so the signed URL
    # is stable across restarts for an already-issued session (Daytona maps
    # preview URLs by port). HOME is set to root by supervisor.js when it
    # spawns ACP — no need to force it here.
    url = await start_supervisor_in_sandbox(
        sandbox, agent_type, _SUPERVISOR_REMOTE_PORT,
        root=_DAYTONA_AGENT_HOME, spawn_env=spawn_env,
    )
    return ProviderInstance(
        provider="daytona",
        url=url,
        root=root,
        sandbox_id=sandbox.id,
    )


async def _daytona_sandbox_op(instance: ProviderInstance, op: str) -> None:
    """Shared logic for destroy/stop Daytona sandbox."""
    if not instance.sandbox_id:
        return
    try:
        daytona = _get_daytona_client()
    except (ImportError, RuntimeError) as e:
        log.warning("cannot %s daytona sandbox %s: %s", op, instance.sandbox_id, e)
        return

    loop = asyncio.get_running_loop()
    try:
        sandbox = await loop.run_in_executor(None, lambda: daytona.get(instance.sandbox_id))
        if op == "delete":
            await loop.run_in_executor(None, lambda: daytona.delete(sandbox))
        else:
            await loop.run_in_executor(None, sandbox.stop)
        log.info("daytona sandbox %sd: %s", op, instance.sandbox_id)
    except Exception as e:
        log.warning("failed to %s daytona sandbox %s: %s", op, instance.sandbox_id, e)


async def destroy_daytona(instance: ProviderInstance) -> None:
    """Delete a Daytona sandbox."""
    await _daytona_sandbox_op(instance, "delete")


async def stop_daytona(instance: ProviderInstance) -> None:
    """Stop (not delete) a Daytona sandbox so it can be resumed later."""
    await _daytona_sandbox_op(instance, "stop")


async def _wait_for_stable_daytona_state(
    daytona, sandbox_ref: str, max_wait_s: float = 15.0,
) -> tuple[object, str]:
    """Poll until the sandbox is NOT in a transitional state. Returns
    (sandbox_obj, state_string). Used by the restart/start paths so a
    `sandbox.start()` call doesn't race an external stop still in
    progress (which rejects with "state change in progress").

    Terminal states: started, running, stopped, paused, error, archived.
    Transitional: starting, stopping, pulling_image, resizing, ...
    """
    loop = asyncio.get_running_loop()
    deadline = loop.time() + max_wait_s
    STABLE = {"started", "running", "stopped", "paused", "error",
              "archived", "destroyed"}
    sandbox = None
    state_str = ""
    while True:
        sandbox = await loop.run_in_executor(None, lambda: daytona.get(sandbox_ref))
        raw = sandbox.state
        state_str = (raw.value if hasattr(raw, "value") else str(raw)).lower()
        if state_str in STABLE or loop.time() > deadline:
            return sandbox, state_str
        await asyncio.sleep(0.5)


async def _wait_for_daytona_sandbox_ready(daytona, sandbox_ref: str, sandbox=None) -> None:
    """Poll until the sandbox is "started" AND an exec succeeds (IP allocated).

    sandbox.start() returns as soon as Daytona accepts the request, before the
    container network is configured.  Subsequent exec calls fail with
    "failed to resolve container IP" until the network stack is up.

    Check FIRST, then sleep — the old "sleep 2s up-front" burned ~2s on
    every restart even when the sandbox was already ready. 0.5s cadence
    also surfaces readiness ~4x faster than the old 2s polls.
    """
    loop = asyncio.get_running_loop()
    for attempt in range(30):  # 30 * 0.5s = 15s max
        if attempt > 0:
            await asyncio.sleep(0.5)
        try:
            sandbox = await loop.run_in_executor(None, lambda: daytona.get(sandbox_ref))
            raw_state = sandbox.state
            state_str = raw_state.value if hasattr(raw_state, "value") else str(raw_state)
            if state_str != "started":
                continue
            r = await loop.run_in_executor(
                None, lambda: sandbox.process.exec("echo ready", timeout=5)
            )
            result = (r.result if hasattr(r, "result") else str(r)) or ""
            if "ready" in result:
                log.info("daytona sandbox %s is ready", sandbox_ref[:16])
                return
        except Exception:
            pass
    raise RuntimeError(
        f"Daytona sandbox {sandbox_ref} did not become network-ready after start"
    )


async def start_daytona(sandbox_ref: str) -> None:
    """Start a stopped Daytona sandbox and wait for it to be network-ready.

    Retries `sandbox.start()` while Daytona reports the sandbox is in the
    middle of a state change ("state change in progress"). This race
    happens when /message arrives a few seconds after an external stop:
    the sandbox is still transitioning from started→stopped, and
    `sandbox.start()` rejects until the transition completes. Polling
    for a stable state first (or retrying on the error) is required;
    otherwise the fast-recovery path fails and we fall back to a full
    state rebuild, losing any persistent /events subscribers.
    """
    try:
        daytona = _get_daytona_client()
    except (ImportError, RuntimeError) as e:
        log.warning("cannot start daytona sandbox %s: %s", sandbox_ref, e)
        return

    loop = asyncio.get_running_loop()
    sandbox, state_str = await _wait_for_stable_daytona_state(daytona, sandbox_ref)
    if state_str in ("started", "running"):
        log.info("daytona sandbox %s already started", sandbox_ref)
        await _wait_for_daytona_sandbox_ready(daytona, sandbox_ref, sandbox=sandbox)
        return
    try:
        await loop.run_in_executor(None, sandbox.start)
        log.info("daytona sandbox started: %s", sandbox_ref)
        await _wait_for_daytona_sandbox_ready(daytona, sandbox_ref)
    except Exception as e:
        log.warning("failed to start daytona sandbox %s: %s", sandbox_ref, e)
        raise


async def create_daytona_volume(name: str, wait_ready_timeout: int = 120) -> str:
    """Create a Daytona volume and return its provider-native id.

    Polls until the volume reaches the 'ready' state before returning so that
    callers can immediately attach the volume to a new sandbox. If polling
    fails (timeout or terminal error state), the just-created volume is
    best-effort deleted so callers don't end up with an orphaned resource
    they can't identify later.

    After the volume is ready, a utility sandbox is spun up to pre-create
    the directory structure: shared/ and system/supervisor/.
    """
    from daytona_api_client import VolumesApi
    from daytona_api_client.models import VolumeState

    client = _get_daytona_client()
    # Idempotent: volume.get(name, create=True) returns the existing volume
    # if one already has this name, else creates a new one. Lets
    # `_get_or_create_default_volume` re-enter safely across server restarts
    # and on multi-worker deploys where the DB row was lost but the Daytona
    # volume still exists.
    vol = await asyncio.to_thread(client.volume.get, name, True)
    vol_id = vol.id

    volumes_api = VolumesApi(client._api_client)
    try:
        deadline = asyncio.get_running_loop().time() + wait_ready_timeout
        while True:
            dto = await asyncio.to_thread(volumes_api.get_volume, vol_id)
            state = dto.state
            state_val = state.value if hasattr(state, "value") else str(state)
            if state_val == VolumeState.READY:
                break
            if state_val in {VolumeState.ERROR, VolumeState.DELETED, VolumeState.DELETING}:
                raise RuntimeError(f"Daytona volume {vol_id} entered unexpected state: {state_val}")
            if asyncio.get_running_loop().time() > deadline:
                raise TimeoutError(f"Daytona volume {vol_id} did not become ready within {wait_ready_timeout}s (last state: {state_val})")
            await asyncio.sleep(3)
    except Exception:
        try:
            await asyncio.to_thread(volumes_api.delete_volume, vol_id)
        except Exception as cleanup_err:
            log.warning(
                "orphaned daytona volume %s: cleanup delete failed: %s",
                vol_id, cleanup_err,
            )
        raise

    # Pre-create the standard directory layout on the volume.
    await _init_volume_dirs(vol_id)

    return vol_id


async def _init_volume_dirs(volume_ref: str) -> None:
    """Spin a 1-shot sandbox to mkdir -p shared/ system/supervisor/ on the volume."""
    from daytona_sdk import (
        Daytona, DaytonaConfig, CreateSandboxFromSnapshotParams,
        CreateSandboxFromImageParams, VolumeMount,
    )

    api_key = os.environ.get("DAYTONA_API_KEY")
    if not api_key:
        raise RuntimeError("DAYTONA_API_KEY not set")
    daytona = Daytona(DaytonaConfig(api_key=api_key))
    loop = asyncio.get_running_loop()

    snapshot = os.environ.get("DAYTONA_SNAPSHOT", "hive-large").strip()
    use_snapshot = snapshot.lower() not in {"", "0", "false", "image"}

    # Mount the whole volume at /v (no subpath) so we can create dirs.
    volumes = [VolumeMount(volume_id=volume_ref, mount_path="/v")]
    init_labels = _sandbox_labels()

    if use_snapshot:
        sb = await loop.run_in_executor(None, lambda: daytona.create(
            CreateSandboxFromSnapshotParams(
                snapshot=snapshot, auto_stop_interval=0,
                env_vars=_get_sandbox_env_vars(), volumes=volumes,
                labels=init_labels,
            ), timeout=120,
        ))
    else:
        sb = await loop.run_in_executor(None, lambda: daytona.create(
            CreateSandboxFromImageParams(
                image="node:22-slim", auto_stop_interval=0,
                env_vars=_get_sandbox_env_vars(), volumes=volumes,
                labels=init_labels,
            ), timeout=120,
        ))

    try:
        await loop.run_in_executor(
            None,
            lambda: _run_sandbox_exec(sb, "mkdir -p /v/shared /v/system/supervisor", timeout=30),
        )
        log.info("volume %s: initialized shared/ and system/supervisor/ dirs", volume_ref)
    finally:
        try:
            await loop.run_in_executor(None, lambda: daytona.delete(sb))
        except Exception:
            pass


async def delete_daytona_volume(provider_ref: str) -> None:
    """Delete a Daytona volume by provider-native id (UUID)."""
    from daytona_api_client import VolumesApi
    client = _get_daytona_client()
    volumes_api = VolumesApi(client._api_client)
    await asyncio.to_thread(volumes_api.delete_volume, provider_ref)


async def get_daytona_sandbox_status(sandbox_ref: str) -> str:
    """Return one of: 'running' | 'stopped' | 'missing' | 'error'.

    The caller (``_ensure_sandbox_locked``) treats ``error`` as
    unrecoverable: it destroys the sandbox + nukes the DB row + provisions
    a brand-new replacement (Type 2). So transitional states like
    ``stopping`` / ``starting`` (5–30 s under load) MUST classify by their
    target state, not as ``error`` — otherwise an in-flight stop or boot
    that races a POST /message destroys the live sandbox the caller is
    trying to recover.

    Default for an unrecognized state is ``running`` rather than ``error``
    for the same reason: a future Daytona state name we haven't seen yet
    should fall through to ``_wait_for_health`` / ``start_sandbox``, not
    to destroy + Type 2.
    """
    try:
        client = _get_daytona_client()
        sb = await asyncio.to_thread(client.get, sandbox_ref)
    except Exception as e:
        msg = str(e).lower()
        if "not found" in msg or "404" in msg:
            return "missing"
        return "error"
    state = (getattr(sb, "state", None) or "")
    state_str = (state.value if hasattr(state, "value") else str(state)).lower()
    if state_str in ("started", "running", "starting",
                     "pulling_image", "creating", "resizing"):
        return "running"
    if state_str in ("stopped", "paused", "stopping"):
        return "stopped"
    if state_str in ("destroyed", "destroying", "archived"):
        return "missing"
    if state_str == "error":
        return "error"
    return "running"


# ---------------------------------------------------------------------------
# Uniform API — each provider module exposes these names. Daytona's internals
# already have the right shape, so dispatch via thin wrappers that re-resolve
# the underlying name on each call. Module-level aliases (``foo = bar``) bind
# once at import and break ``unittest.mock.patch("api.providers.daytona.bar")``
# silently — the alias keeps pointing at the original. The wrappers below
# look the name up at call time so patching either the wrapper or the
# underlying name works as expected.
# ---------------------------------------------------------------------------


async def create_volume(*args, **kwargs):
    return await create_daytona_volume(*args, **kwargs)


async def delete_volume(*args, **kwargs):
    return await delete_daytona_volume(*args, **kwargs)


async def get_sandbox_status(*args, **kwargs):
    return await get_daytona_sandbox_status(*args, **kwargs)


async def start_sandbox(*args, **kwargs):
    return await start_daytona(*args, **kwargs)


async def destroy_sandbox(*args, **kwargs):
    return await destroy_daytona(*args, **kwargs)


async def stop_sandbox(*args, **kwargs):
    return await stop_daytona(*args, **kwargs)


async def ensure_supervisor_url(inst: ProviderInstance, *, agent_type: str,
                                root: str = "/tmp",
                                spawn_env: dict | None = None,
                                port: int | None = None) -> str:
    """Daytona: start a supervisor in the sandbox referenced by ``inst`` and
    return its URL.

    Daytona follows a 2-phase "create the sandbox, then start the
    supervisor" model — ``create_sandbox`` returns an instance with
    ``url=""``, and the server calls this helper later to spawn the
    supervisor process and mint a signed preview URL.

    Docker and local providers collapse these two steps: their
    ``create_sandbox`` already has ``url`` set when it returns, so the
    corresponding ``ensure_supervisor_url`` is effectively a no-op that
    just echoes ``inst.url``.  The dispatcher in
    ``providers.__init__.ensure_supervisor_url`` routes transparently to
    whichever provider the instance belongs to.

    Mi3: the ``**_kw`` catch-all was removed so a mis-spelled kwarg
    surfaces as TypeError instead of being silently swallowed — matching
    the docker + local signatures."""
    from daytona_sdk import Daytona, DaytonaConfig
    import os as _os
    api_key = _os.environ.get("DAYTONA_API_KEY")
    if not api_key:
        raise RuntimeError("DAYTONA_API_KEY not set")
    daytona_client = Daytona(DaytonaConfig(api_key=api_key))
    loop = asyncio.get_running_loop()
    try:
        sandbox = await loop.run_in_executor(
            None, lambda: daytona_client.get(inst.sandbox_id)
        )
    except Exception as e:
        # Daytona raises a plain Exception with "not found" in the message when
        # the sandbox has been deleted out-of-band. Surface this as a typed
        # error so the server can re-provision on the same volume.
        if "not found" in str(e).lower():
            from ._shared import SandboxMissingError
            raise SandboxMissingError(
                f"Daytona sandbox {inst.sandbox_id} not found (deleted externally)"
            ) from e
        raise
    # If the sandbox was stopped externally (e.g. daytona.stop()), start it
    # and wait for the container network to be ready before exec-ing.
    raw_state = sandbox.state
    state_str = raw_state.value if hasattr(raw_state, "value") else str(raw_state)
    if state_str != "started":
        log.info("ensure_supervisor_url: sandbox %s is %s; starting", inst.sandbox_id[:16], state_str)
        await loop.run_in_executor(None, sandbox.start)
        await _wait_for_daytona_sandbox_ready(daytona_client, inst.sandbox_id)
        sandbox = await loop.run_in_executor(None, lambda: daytona_client.get(inst.sandbox_id))
    # The agent's HOME is /home/daytona — a local ext4 dir the supervisor
    # creates and populates from the volume snapshot on boot. supervisor.js
    # sets HOME=root when spawning the ACP child so Claude Code's session
    # JSONLs land in the restored workspace. No env-level HOME override
    # needed here anymore.
    return await start_supervisor_in_sandbox(
        sandbox, agent_type, port, root=_DAYTONA_AGENT_HOME, spawn_env=spawn_env,
    )


async def install_supervisor(volume_ref: str, agent_type: str) -> None:
    """Atomically install supervisor.js + ACP deps on this volume.

    Writes the artifacts (``deps.tar.gz`` + ``supervisor.js``) into a sibling
    staging dir (``system/supervisor.tmp.<uuid>/``) first, verifies both
    sentinels are present, then atomically renames the staging dir over
    ``system/supervisor`` via ``mv``. If any step fails the staging dir is
    removed, leaving any previous install untouched — a retried install
    can never end up with a half-written ``deps.tar.gz`` racing against a
    live reader.

    Mounts ``system/`` (not ``system/supervisor``) so staging and final
    share a single mount point and ``mv`` is a rename-within-volume.
    """
    from daytona_sdk import (
        Daytona, DaytonaConfig, CreateSandboxFromSnapshotParams,
        CreateSandboxFromImageParams, VolumeMount,
    )

    api_key = os.environ.get("DAYTONA_API_KEY")
    if not api_key:
        raise RuntimeError("DAYTONA_API_KEY not set")
    daytona = Daytona(DaytonaConfig(api_key=api_key))
    loop = asyncio.get_running_loop()

    snapshot = os.environ.get("DAYTONA_SNAPSHOT", "hive-large").strip()
    use_snapshot = snapshot.lower() not in {"", "0", "false", "image"}

    # Mount whole system/ so staging + final share the mount and mv can rename.
    volumes = [VolumeMount(volume_id=volume_ref, mount_path="/work", subpath="system")]
    install_labels = _sandbox_labels()

    if use_snapshot:
        sb = await loop.run_in_executor(None, lambda: daytona.create(
            CreateSandboxFromSnapshotParams(
                snapshot=snapshot, auto_stop_interval=0,
                env_vars=_get_sandbox_env_vars(), volumes=volumes,
                labels=install_labels,
            ), timeout=120,
        ))
    else:
        sb = await loop.run_in_executor(None, lambda: daytona.create(
            CreateSandboxFromImageParams(
                image="node:22-slim", auto_stop_interval=0,
                env_vars=_get_sandbox_env_vars(), volumes=volumes,
                labels=install_labels,
            ), timeout=120,
        ))

    staging_name = f"supervisor.tmp.{uuid.uuid4().hex[:8]}"
    staging_on_volume = f"/work/{staging_name}"
    final_on_volume = "/work/supervisor"

    try:
        npm_spec = _ACP_NPM_SPECS[agent_type]

        def _exec(cmd: str, timeout: int | None = 60) -> str:
            return _run_sandbox_exec(sb, cmd, timeout=timeout).stdout

        # npm install to local ephemeral FS (fast SSD), pack to a tarball,
        # place it on the volume under the staging dir.  Every shell step
        # uses ``set -e`` so a silent npm failure can't produce an empty
        # node_modules that then tarballs without the acp bin.
        local_dir = "/tmp/sup-install"
        bin_name = _acp_bin_name(agent_type)
        await loop.run_in_executor(None, lambda: _exec(
            f"set -e && rm -rf {local_dir} && mkdir -p {local_dir} && "
            f"cd {local_dir} && npm init -y >/dev/null 2>&1"
        ))
        install_out = await loop.run_in_executor(None, lambda: _exec(
            f"set -e && cd {local_dir} && "
            f"npm install --omit=optional {npm_spec} 2>&1",
            240,
        ))
        # Sentinel: the ACP bin must exist after npm install.  If it
        # doesn't, surface the npm output so the caller knows why.
        verify = await loop.run_in_executor(None, lambda: _exec(
            f"test -f {local_dir}/node_modules/.bin/{bin_name} && echo ok || echo missing"
        ))
        if verify.strip() != "ok":
            raise RuntimeError(
                f"npm install produced no {bin_name} at "
                f"{local_dir}/node_modules/.bin/ — npm output:\n"
                f"{install_out[-2000:]}"
            )

        # Build the staging dir on the volume and drop the tarball there.
        await loop.run_in_executor(None, lambda: _exec(
            f"set -e && mkdir -p {staging_on_volume} && "
            f"tar -C {local_dir} -czf /tmp/deps.tar.gz . && "
            # Sanity-check the tarball contains the acp bin before we ship
            # it onto the slow network volume.  Cheap sanity gate.
            f"tar -tzf /tmp/deps.tar.gz | grep -q 'node_modules/.bin/{bin_name}' && "
            f"cp /tmp/deps.tar.gz {staging_on_volume}/deps.tar.gz && "
            f"rm -f /tmp/deps.tar.gz && rm -rf {local_dir}",
            120,
        ))

        # Upload supervisor.js into the staging dir via the Daytona fs API.
        with open(_SUPERVISOR_DIR / "supervisor.js", "rb") as f:
            sup_js_bytes = f.read()
        await loop.run_in_executor(
            None, lambda: sb.fs.upload_file(
                sup_js_bytes, f"{staging_on_volume}/supervisor.js"
            )
        )

        # Sentinel checks + atomic swap. Using shell ``test`` so a single
        # missing file aborts before we touch the existing supervisor dir.
        # mountpoint-s3 doesn't support rename of non-empty dirs, so we
        # create final/ and copy contents in (idempotent via rm -rf first).
        await loop.run_in_executor(None, lambda: _exec(
            f"set -e && "
            f"test -f {staging_on_volume}/deps.tar.gz && "
            f"test -f {staging_on_volume}/supervisor.js && "
            f"rm -rf {final_on_volume} && "
            f"mkdir -p {final_on_volume} && "
            f"cp -f {staging_on_volume}/deps.tar.gz {final_on_volume}/deps.tar.gz && "
            f"cp -f {staging_on_volume}/supervisor.js {final_on_volume}/supervisor.js && "
            f"rm -rf {staging_on_volume} && "
            f"test -f {final_on_volume}/deps.tar.gz && "
            f"test -f {final_on_volume}/supervisor.js && "
            f"sync",
            120,
        ))

        log.info("supervisor installed on volume %s for %s", volume_ref, agent_type)
    except BaseException:
        # Best-effort staging-dir cleanup; don't mask the original error.
        try:
            await loop.run_in_executor(None, lambda: sb.process.exec(
                f"rm -rf {staging_on_volume}", timeout=30,
            ))
        except Exception as cleanup_err:  # pragma: no cover
            log.warning("daytona install_supervisor staging cleanup failed: %s", cleanup_err)
        raise
    finally:
        try:
            await loop.run_in_executor(None, lambda: daytona.delete(sb))
        except Exception:
            pass


async def create_sandbox(
    *,
    volume_ref: str,
    subpath: str,
    agent_type: str = "claude",
    spawn_env: dict[str, str] | None = None,
    port: int | None = None,
    root: str | None = None,
    dockerfile: str | None = None,
    pre_start_commands: list[str] | None = None,
    sandbox_id: str | None = None,  # accepted for parity; unused here
    shared_mounts: list[str] | None = None,
) -> ProviderInstance:
    """Uniform ``create_sandbox`` for the Daytona provider.

    Delegates to ``provision_daytona_sandbox`` which creates the sandbox with
    the volume mounts (per-agent subpath + supervisor cache + any opt-in
    shared mounts) but does NOT start a supervisor; the caller must run
    ``ensure_supervisor_url`` before talking to the supervisor.

    ``spawn_env`` / ``port`` / ``sandbox_id`` are accepted for parity with
    docker/local but are unused here — the supervisor is started later with
    its own env + port, and Daytona doesn't take a name on create.
    """
    # Per-session sandboxes always root at /home/daytona — the supervisor
    # will mkdir it, restore from the volume snapshot, and use it as HOME.
    # The volume itself is mounted at /vol (see _build_volume_mounts) and
    # the agent never sees it directly.
    effective_root = _DAYTONA_AGENT_HOME if subpath else (root or _DAYTONA_AGENT_HOME)
    return await provision_daytona_sandbox(
        agent_type=agent_type,
        dockerfile=dockerfile,
        pre_start_commands=pre_start_commands,
        root=effective_root,
        volume_id=volume_ref,
        subpath=subpath,
        shared_mounts=shared_mounts,
    )


# ---------------------------------------------------------------------------
# Volume file ops — dispatched from server.py's /volumes/{id}/files/*
# ---------------------------------------------------------------------------
# Every GET /volumes/{id}/files/tree / files/read / files/edit needs a
# sandbox with the volume mounted at /v. Provisioning fresh on each call
# meant ~10s+ per request on daytona and a new sandbox every UI poll. We
# keep one utility sandbox per volume with a short TTL; reuse bypasses
# the whole provision/destroy round-trip on back-to-back calls.
# ---------------------------------------------------------------------------

_UTILITY_TTL_S = 300.0  # 5 minutes idle before the reaper tears it down
_UTILITY_REAPER_TICK_S = 30.0
_utility_cache: dict[str, tuple["ProviderInstance", float]] = {}  # ref -> (inst, last_used)
_utility_cache_lock = asyncio.Lock()
_utility_reaper_started = False


async def _get_or_create_utility(ref: str) -> "ProviderInstance":
    """Return a ready utility sandbox for ``ref`` — cached per volume.

    First call per volume: provisions + caches. Subsequent calls within
    ``_UTILITY_TTL_S`` of the last use: returns the cached instance. The
    reaper tears down idle entries; a torn-down entry is transparently
    re-provisioned on the next call.
    """
    import time as _time
    async with _utility_cache_lock:
        cached = _utility_cache.get(ref)
        if cached is not None:
            inst, _last = cached
            _utility_cache[ref] = (inst, _time.monotonic())
            _ensure_utility_reaper()
            return inst
        # Provision outside the lock? No — provisioning is 5-15s and we
        # want the lock held so a burst of concurrent file-ops on the
        # same volume doesn't create N sandboxes. Readers wait; winner
        # populates cache; other readers then hit the fast path above.
        log.info("daytona utility sandbox: provisioning for volume %s", ref[:16])
        inst = await provision_daytona_sandbox(
            agent_type="claude", volume_id=ref, subpath=None,
        )
        _utility_cache[ref] = (inst, _time.monotonic())
        _ensure_utility_reaper()
        return inst


async def _drop_utility(ref: str) -> None:
    """Remove a cached utility sandbox and destroy it. No-op if absent."""
    async with _utility_cache_lock:
        entry = _utility_cache.pop(ref, None)
    if entry is None:
        return
    inst, _ = entry
    try:
        await destroy_daytona(inst)
    except Exception as e:  # pragma: no cover
        log.warning("utility sandbox destroy failed for %s: %s", ref[:16], e)


def _ensure_utility_reaper() -> None:
    """Lazily start the background reaper on first cache entry."""
    global _utility_reaper_started
    if _utility_reaper_started:
        return
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return  # not in an event loop (e.g. unit tests) — caller manages cleanup
    loop.create_task(_utility_reaper_loop())
    _utility_reaper_started = True


async def _utility_reaper_loop() -> None:
    """Destroy utility sandboxes that have been idle for > _UTILITY_TTL_S."""
    import time as _time
    while True:
        await asyncio.sleep(_UTILITY_REAPER_TICK_S)
        now = _time.monotonic()
        stale: list[tuple[str, "ProviderInstance"]] = []
        async with _utility_cache_lock:
            for ref, (inst, last) in list(_utility_cache.items()):
                if now - last > _UTILITY_TTL_S:
                    stale.append((ref, inst))
                    _utility_cache.pop(ref, None)
        for ref, inst in stale:
            log.info("daytona utility sandbox: reaping idle volume %s", ref[:16])
            try:
                await destroy_daytona(inst)
            except Exception as e:  # pragma: no cover
                log.warning("utility reaper: destroy failed for %s: %s", ref[:16], e)


async def _run_in_utility_sandbox(ref: str, cmd: str, timeout: int = 30):
    """Run ``cmd`` in the utility sandbox for ``ref``. Cached + TTL-reaped.

    Transient errors (sandbox died on the provider side between the cache
    entry's freshness check and the exec call) trigger one retry after
    dropping the cache entry, so callers don't see a single stale-cache
    hit bubble up as a 500.
    """
    from .. import providers as _prov  # local import for cycle
    inst = await _get_or_create_utility(ref)
    try:
        return await _prov.exec_in_instance(inst, cmd, timeout=timeout)
    except Exception as e:
        log.warning("utility exec failed on cached sandbox for %s (%s); retrying with fresh sandbox", ref[:16], e)
        await _drop_utility(ref)
        inst = await _get_or_create_utility(ref)
        return await _prov.exec_in_instance(inst, cmd, timeout=timeout)


async def volume_tree(ref: str, path: str) -> str:
    """Tree listing of ``<volume>/<path>`` in the unified format (max depth 3)."""
    rel = _safe_path(None, path or "")
    target = "/v/" + rel if rel else "/v"
    res = await _run_in_utility_sandbox(
        ref,
        f"find {shlex.quote(target)} -mindepth 1 -maxdepth 3 -printf '%y %P\\n' 2>/dev/null"
    )
    normalized = normalize_find_output(res.stdout)
    if not rel or not normalized:
        return normalized
    lines = [f"{rel.rstrip('/')}/{ln}" for ln in normalized.splitlines()]
    return "\n".join(sorted(lines))


async def volume_read(ref: str, path: str) -> bytes:
    """Read ``<volume>/<path>`` bytes via a short-lived utility sandbox."""
    rel = _safe_path(None, path or "")
    if not rel:
        raise ValueError("volume_read: path required")
    target = "/v/" + rel
    # base64 so binary survives the exec response.
    res = await _run_in_utility_sandbox(
        ref,
        f"if [ ! -f {shlex.quote(target)} ]; then echo __MISSING__; exit 2; fi; "
        f"base64 -w0 {shlex.quote(target)} 2>/dev/null || base64 {shlex.quote(target)}",
    )
    if res.exit_code != 0:
        if "__MISSING__" in (res.stdout or ""):
            raise FileNotFoundError(f"{path} not found on volume {ref}")
        raise RuntimeError(f"volume_read failed: {res.stderr[:400]}")
    import base64 as _b64
    try:
        return _b64.b64decode((res.stdout or "").strip())
    except Exception as exc:
        raise RuntimeError(f"volume_read: malformed base64: {exc}") from exc


async def volume_download(ref: str, path: str) -> bytes:
    """Read raw bytes from ``<volume>/<path>`` via Daytona's filesystem API.

    This bypasses ``volume_read``'s exec/stdout path by using Daytona's
    dedicated file-download endpoint through the SDK.
    """
    rel = _safe_path(None, path or "")
    if not rel:
        raise ValueError("volume_download: path required")
    target = "/v/" + rel
    inst = await _get_or_create_utility(ref)
    if not inst.sandbox_id:
        raise RuntimeError("volume_download: utility sandbox_id missing")

    loop = asyncio.get_running_loop()
    daytona_client = _get_daytona_client()
    try:
        sandbox = await loop.run_in_executor(
            None, lambda: daytona_client.get(inst.sandbox_id)
        )
    except Exception as e:
        raise RuntimeError(f"volume_download: get sandbox failed: {e}") from e

    try:
        return await loop.run_in_executor(
            None, lambda: sandbox.fs.download_file(target)
        )
    except Exception as e:
        msg = str(e)
        if "not found" in msg.lower() or "404" in msg:
            raise FileNotFoundError(f"{path} not found on volume {ref}") from e
        raise RuntimeError(f"volume_download failed: {msg}") from e


async def volume_exists(ref: str, path: str) -> bool:
    """Return whether ``<volume>/<path>`` exists."""
    rel = _safe_path(None, path or "")
    target = "/v/" + rel if rel else "/v"
    res = await _run_in_utility_sandbox(ref, f"test -e {shlex.quote(target)}")
    if res.exit_code == 0:
        return True
    if res.exit_code == 1:
        return False
    raise RuntimeError(f"volume_exists failed: {res.stderr[:400]}")


async def volume_write(ref: str, path: str, content: bytes) -> None:
    """Write ``content`` to ``<volume>/<path>`` via a short-lived utility sandbox."""
    rel = _safe_path(None, path or "")
    if not rel:
        raise ValueError("volume_write: path required")
    target = "/v/" + rel
    parent = "/v/" + "/".join(rel.split("/")[:-1])
    import base64 as _b64
    b64 = _b64.b64encode(content).decode()
    cmd = (
        f"mkdir -p {shlex.quote(parent)} && "
        f"printf %s {shlex.quote(b64)} | base64 -d > {shlex.quote(target)}"
    )
    res = await _run_in_utility_sandbox(ref, cmd)
    if res.exit_code != 0:
        raise RuntimeError(f"volume_write failed: {res.stderr[:400]}")


async def volume_upload(ref: str, path: str, content: bytes) -> None:
    """Upload bytes to ``<volume>/<path>``."""
    await volume_write(ref, path, content)


async def volume_mkdir(ref: str, path: str) -> None:
    """Create a directory at ``<volume>/<path>``."""
    rel = _safe_path(None, path or "")
    if not rel:
        raise ValueError("volume_mkdir: path required")
    target = "/v/" + rel
    res = await _run_in_utility_sandbox(ref, f"mkdir -p {shlex.quote(target)}")
    if res.exit_code != 0:
        raise RuntimeError(f"volume_mkdir failed: {res.stderr[:400]}")


async def volume_delete(ref: str, path: str) -> None:
    """Delete a file or directory at ``<volume>/<path>``."""
    rel = _safe_path(None, path or "")
    if not rel:
        raise ValueError("volume_delete: path required")
    target = "/v/" + rel
    cmd = (
        f"if [ ! -e {shlex.quote(target)} ]; then echo __MISSING__; exit 2; fi; "
        f"rm -rf -- {shlex.quote(target)}"
    )
    res = await _run_in_utility_sandbox(ref, cmd)
    if res.exit_code != 0:
        if "__MISSING__" in (res.stdout or ""):
            raise FileNotFoundError(f"{path} not found on volume {ref}")
        raise RuntimeError(f"volume_delete failed: {res.stderr[:400]}")


async def volume_rename(ref: str, path: str, new_path: str, *, overwrite: bool = True) -> None:
    """Rename or move ``<volume>/<path>`` to ``<volume>/<new_path>``."""
    src_rel = _safe_path(None, path or "")
    dst_rel = _safe_path(None, new_path or "")
    if not src_rel or not dst_rel:
        raise ValueError("volume_rename: path and new_path required")
    src = "/v/" + src_rel
    dst = "/v/" + dst_rel
    dst_parent = "/v/" + "/".join(dst_rel.split("/")[:-1])
    # mountpoint-backed volumes can lag after rename/link+unlink. Do not report
    # success until dst is visible and src is gone in the utility sandbox view.
    settle_check = (
        f"for _i in 1 2 3 4 5 6 7 8 9 10; do "
        f"if [ -e {shlex.quote(dst)} ] && [ ! -e {shlex.quote(src)} ]; then exit 0; fi; "
        f"sleep 0.1; "
        f"done; "
        f"echo __RENAME_NOT_VISIBLE__; exit 98"
    )
    if overwrite:
        cmd = (
            f"if [ ! -e {shlex.quote(src)} ]; then echo __MISSING__; exit 2; fi; "
            f"mkdir -p {shlex.quote(dst_parent)} && "
            f"mv -- {shlex.quote(src)} {shlex.quote(dst)} && "
            f"{settle_check}"
        )
    else:
        cmd = (
            f"if [ ! -e {shlex.quote(src)} ]; then echo __MISSING__; exit 2; fi; "
            f"mkdir -p {shlex.quote(dst_parent)} || exit $?; "
            f"if [ -e {shlex.quote(dst)} ]; then echo __EXISTS__; exit 17; fi; "
            f"if [ -d {shlex.quote(src)} ]; then echo __UNSUPPORTED_DIR__; exit 95; fi; "
            f"ln {shlex.quote(src)} {shlex.quote(dst)} || "
            f"{{ if [ -e {shlex.quote(dst)} ]; then echo __EXISTS__; exit 17; else exit 1; fi; }}; "
            f"rm -- {shlex.quote(src)} || {{ echo __UNLINK_FAILED__; exit 96; }}; "
            f"{settle_check}"
        )
    res = await _run_in_utility_sandbox(ref, cmd)
    if res.exit_code != 0:
        if "__MISSING__" in (res.stdout or ""):
            raise FileNotFoundError(f"{path} not found on volume {ref}")
        if "__EXISTS__" in (res.stdout or ""):
            raise VolumeFileExistsError(new_path)
        if "__UNSUPPORTED_DIR__" in (res.stdout or ""):
            raise NotImplementedError("atomic no-overwrite directory rename is not supported")
        if "__RENAME_NOT_VISIBLE__" in (res.stdout or ""):
            raise RuntimeError("volume_rename postcondition failed: destination not visible")
        raise RuntimeError(f"volume_rename failed: {res.stderr[:400]}")
