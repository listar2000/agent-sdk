"""Daytona provider — create/destroy/exec in Daytona sandboxes.

Package layout:
- ``__init__.py`` (this file) — volume + sandbox primitives (functional,
  stateless). Loaded by ``api.providers.__init__``'s dispatch table.
- ``session.py`` — ``DaytonaSandboxSession``, the per-session lifecycle
  class. Loaded directly via ``from api.providers.daytona.session
  import DaytonaSandboxSession`` (avoids a circular import with
  ``api.sandbox.session`` which session.py depends on).
"""

import asyncio
import logging
import os
import shlex
import time
import uuid
from pathlib import Path
from typing import Any, NamedTuple

from ... import load_dotenv
from ...metrics import timed_provider_op

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


async def _run_sandbox_exec_async(sandbox, cmd: str, timeout: int = 120) -> "_ExecResult":
    """Run ``cmd`` in ``sandbox`` and return stdout, stderr, and exit_code.

    ``sandbox`` is an ``AsyncSandbox`` from ``daytona_sdk._async``.
    Defensive against SDK versions that may not expose all fields.
    Callers that want tolerant behaviour for a command that may fail
    should wrap their command with ``|| true`` so the shell always
    exits 0."""
    r = await sandbox.process.exec(cmd, timeout=timeout)
    return _ExecResult(
        stdout=getattr(r, "result", "") or "",
        stderr=getattr(r, "stderr", "") or "",
        exit_code=getattr(r, "exit_code", None),
    )


# Re-import shared helpers from __init__ to avoid circular imports.
# These are defined here inline or imported lazily.
from .._shared import (
    ExecResult,
    _MAX_OUTPUT_BYTES,
    _acp_launch_args,
    _build_env_prefix,
    _build_volume_mounts,
    _enum_str,
    bounded_gather,
    _get_sandbox_env_vars,
    _read_runtime_image_tag,
    _read_runtime_snapshot_tag,
    _safe_path,
    _truncate,
    _wait_for_health,
    build_supervisor_argv,
    ProviderInstance,
    VolumeFileExistsError,
    normalize_find_output,
)
from .._volume import ShellVolumeAdapter

# The supervisor lives at ``/opt/agent-sdk/runtime/`` inside the daytona
# sandbox image; this is the port it listens on.
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
# ``daytona.list(labels={"agent_sdk_origin": "test"})``. All local-dev
# launchers default ``AGENT_SDK_ORIGIN=test``; production deploys
# (Railway via ``Dockerfile``) leave it unset and the env-default below
# falls back to ``"production"``, so the test-origin query never touches
# real production sandboxes.
_LABEL_ORIGIN = "agent_sdk_origin"


def _sandbox_labels() -> dict[str, str]:
    return {_LABEL_ORIGIN: os.environ.get("AGENT_SDK_ORIGIN", "production")}


def _to_daytona_resources(req: Any) -> Any:
    """Map our ``Resources`` to daytona-sdk ``Resources`` (image path only).

    daytona's ``Resources`` is ``cpu``/``memory`` (GiB) /``disk`` (GiB) /``gpu``
    (count). The ``gpu_type`` half of our request is silently dropped — daytona
    has no API for it. Returns ``None`` for an empty/all-None request so the
    SDK uses defaults.
    """
    if req is None:
        return None
    from api.sandbox.state import parse_gpu
    from daytona_sdk import Resources as _DR
    _, gpu_count = parse_gpu(req.gpu)
    cpu = int(req.cpu) if req.cpu is not None else None
    memory_gib = (req.memory_mib + 1023) // 1024 if req.memory_mib is not None else None
    if cpu is None and memory_gib is None and req.disk_gib is None and gpu_count is None:
        return None
    return _DR(cpu=cpu, memory=memory_gib, disk=req.disk_gib, gpu=gpu_count)


_DAYTONA_CLIENT_ASYNC: "Any | None" = None
_DAYTONA_ASYNC_INIT_LOCK = asyncio.Lock()
_DAYTONA_ASYNC_POOL_MAX = int(os.environ.get("AGENT_SDK_DAYTONA_POOL_MAX", "300"))

# daytona-sdk >=0.184 made ``AsyncDaytona.list`` an auto-paginating async
# generator, so we iterate sandboxes rather than page-walk (the old offset>=1000
# hard limit is handled inside the SDK now). Cap the scan at this many sandboxes
# as a runaway guard; past it the orphan scan reports ``capped=True`` and its
# count is a lower bound.
_DAYTONA_MAX_ITEMS = int(os.environ.get("AGENT_SDK_DAYTONA_MAX_ITEMS", "5000"))


async def _get_async_daytona_client():
    """Process-shared AsyncDaytona client. Lazy-init under a lock."""
    global _DAYTONA_CLIENT_ASYNC
    if _DAYTONA_CLIENT_ASYNC is not None:
        return _DAYTONA_CLIENT_ASYNC
    async with _DAYTONA_ASYNC_INIT_LOCK:
        if _DAYTONA_CLIENT_ASYNC is not None:
            return _DAYTONA_CLIENT_ASYNC
        from daytona_sdk import DaytonaConfig
        from daytona_sdk._async.daytona import AsyncDaytona
        api_key = os.environ.get("DAYTONA_API_KEY")
        if not api_key:
            raise RuntimeError("DAYTONA_API_KEY not set")
        client = AsyncDaytona(DaytonaConfig(api_key=api_key))
        # rest_client.maxsize is snapshotted from configuration at init; set both.
        client._api_client.configuration.connection_pool_maxsize = _DAYTONA_ASYNC_POOL_MAX
        client._api_client.rest_client.maxsize = _DAYTONA_ASYNC_POOL_MAX
        _DAYTONA_CLIENT_ASYNC = client
        return _DAYTONA_CLIENT_ASYNC


@timed_provider_op("daytona", "start_supervisor")
async def start_supervisor_in_sandbox(
    sandbox, agent_type: str, port: int, root: str = "/tmp",
    spawn_env: dict[str, str] | None = None,
    restore_snapshot: bool = True,
) -> str:
    """Start a NEW supervisor on a specific port inside an existing sandbox.

    The supervisor + every ACP binary are baked into the sandbox image at
    ``/opt/agent-sdk/runtime/`` — nothing is installed onto the volume at
    runtime. We just spawn ``node supervisor.js`` from that directory.
    Returns the signed preview URL for this supervisor.

    Emits ``[BENCH] daytona.start_supervisor phase=<name> s=<seconds>`` log
    lines for each critical-path phase; grep-friendly for recovery-time
    benchmarks (scripts/bench_recovery.py).
    """
    sid8 = sandbox.id[:8] if sandbox.id else "?"
    total_t0 = time.monotonic()
    phases: list[tuple[str, float]] = []

    def _bench(phase: str, t0: float) -> None:
        dt = time.monotonic() - t0
        phases.append((phase, dt))
        log.info("[BENCH] daytona.start_supervisor sandbox=%s phase=%s s=%.3f",
                 sid8, phase, dt)

    async def _exec(cmd: str, timeout: int = 120) -> str:
        r = await _run_sandbox_exec_async(sandbox, cmd, timeout=timeout)
        return r.stdout

    # (0) Idempotency fast-path: if a supervisor is already healthy on
    # this port (left over from a prior call in the same sandbox), skip
    # the 2-3 s extract+spawn dance and just mint a fresh signed URL
    # pointing at it. Saves redundant respawns when concurrent recovery
    # paths (Type 1 retries, pool resume + a parallel cancel) both land
    # here under separate sandbox locks.
    t0 = time.monotonic()
    try:
        existing = await _exec(
            # `-m 2` request-timeout, `-o /dev/null -w '%{http_code}'`
            # prints just the status line so we can string-match cheaply.
            # NOTE: supervisor exposes /v1/health (matches _wait_for_health
            # in providers/_shared.py), not /healthz.
            f"curl -s -m 2 -o /dev/null -w '%{{http_code}}' "
            f"http://127.0.0.1:{port}/v1/health 2>/dev/null || echo 000",
            10,
        )
    except Exception as e:
        # Not fatal — fall through to the normal spawn path.
        existing = "000"
        log.debug("idempotency probe raised for sandbox=%s port=%d: %s",
                  sid8, port, e)
    _bench("idempotency_probe", t0)
    if existing.strip() == "200":
        t0 = time.monotonic()
        signed = await sandbox.create_signed_preview_url(port, 24 * 3600)
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

    # The daytona sandbox boots from an image whose
    # ``/opt/agent-sdk/runtime/`` already contains supervisor.js + every
    # ACP bin. The bin path resolves via ``package.json#bin`` (not
    # ``node_modules/.bin/``) — daytona's image-build flattens symlinks.
    from .._shared import _runtime_acp_bin_relative
    sup_dir = "/opt/agent-sdk/runtime"
    acp_bin = f"{sup_dir}/{_runtime_acp_bin_relative(agent_type)}"
    log.info(
        "start_supervisor_in_sandbox: using image runtime %s "
        "(port %d, sandbox %s)",
        sup_dir, port, sandbox.id[:16],
    )

    env_prefix = _build_env_prefix(spawn_env)
    log_file = f"{sup_dir}/sup-{port}.log"
    # ``--snapshot-path`` is ALWAYS passed (per-turn agent_memory.tar writes
    # key off it). ``restore_snapshot=False`` (first cold-create) only adds
    # ``--skip-restore`` so the supervisor skips its 2×15s boot restore poll
    # for tarballs that can't exist yet — writes, and future recovery, are
    # unaffected. See DaytonaSandboxSession.start.
    supervisor_argv = build_supervisor_argv(
        supervisor_js="supervisor.js", acp_bin=acp_bin,
        acp_launch_args=_acp_launch_args(agent_type),
        port=port, root=root,
        snapshot_path=_SNAPSHOT_PATH, skip_restore=not restore_snapshot,
        quote_paths=False,
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
        _exec(start_cmd, timeout=10),
        sandbox.create_signed_preview_url(port, 24 * 3600),
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
        log_out = await _exec(f"tail -40 {log_file} 2>&1")
        raise RuntimeError(
            f"supervisor on port {port} in sandbox {sandbox.id} failed health check; log:\n{log_out[:800]}"
        )

    total_dt = time.monotonic() - total_t0
    log.info("[BENCH] daytona.start_supervisor sandbox=%s TOTAL s=%.3f (%s)",
             sid8, total_dt, ", ".join(f"{p}={d:.2f}" for p, d in phases))
    log.info("supervisor on port %d ready: %s (sandbox %s, dir %s)", port, url[:60], sandbox.id[:16], sup_dir)
    return url


async def provision_daytona_sandbox(
    agent_type: str = "opencode",
    dockerfile: str | None = None,
    pre_start_commands: list[str] | None = None,
    root: str = "/tmp",
    volume_id: str | None = None,
    subpath: str | None = None,
    shared_mounts: list[str] | None = None,
    resources: Any = None,
) -> ProviderInstance:
    """Create a Daytona sandbox with its volume mounts, but do NOT start a
    supervisor (that is handled later by start_supervisor_in_sandbox
    via DaytonaSandboxSession.start).

    Returns a ProviderInstance with sandbox_ref but no usable supervisor URL.
    Supervisors are started per-session via start_supervisor_in_sandbox().
    The supervisor binary + ACP packages are baked into the sandbox image at
    /opt/agent-sdk/runtime/ — the volume carries only user data.
    """
    try:
        from daytona_sdk import (
            CreateSandboxFromImageParams,
            CreateSandboxFromSnapshotParams,
        )
    except ImportError:
        raise RuntimeError("daytona-sdk not installed. Run: pip install daytona-sdk")

    env_vars = _get_sandbox_env_vars()
    daytona = await _get_async_daytona_client()

    # the runtime-image-unification refactor: the runtime is baked
    # into the agent-sdk Docker image. Provisioning needs either a
    # snapshot (faster cold-start, registered by ``scripts/release.sh``
    # via Daytona's ``Image.from_dockerfile``) or an image reference. The
    # legacy "hive-large" default is gone — it predates the runtime
    # baking and would silently skip it.
    #
    # Snapshot precedence: ``DAYTONA_SNAPSHOT`` env > ``.runtime-snapshot-tag``
    # repo file. Image precedence: ``DAYTONA_IMAGE`` > ``AGENT_SDK_IMAGE``
    # > ``.runtime-image-tag``. The repo files are written by release.sh
    # so a fresh checkout's daytona path "just works" without any env-var
    # plumbing in launch scripts.
    snapshot = (
        os.environ.get("DAYTONA_SNAPSHOT", "").strip()
        or (_read_runtime_snapshot_tag() or "")
    )
    use_snapshot = dockerfile is None and snapshot.lower() not in {"", "0", "false", "image"}

    # Daytona snapshots bake resources in at snapshot-creation time;
    # CreateSandboxFromSnapshotParams has no ``resources`` field. When the
    # caller wants per-session resources, fall back to the image path so
    # the request is honoured (slower cold-create, ~30s vs ~2s).
    if use_snapshot and resources is not None:
        log.info(
            "daytona: resources requested (%s); falling back from snapshot %s "
            "to image path", resources, snapshot,
        )
        use_snapshot = False

    if dockerfile is not None:
        if not Path(dockerfile).exists():
            raise FileNotFoundError(f"Dockerfile not found: {dockerfile}")
        from daytona_sdk import Image
        image = Image.from_dockerfile(dockerfile)
    elif not use_snapshot:
        # Same precedence as docker.create_sandbox: per-provider override >
        # cross-provider override > committed pin.
        image = (
            os.environ.get("DAYTONA_IMAGE")
            or os.environ.get("AGENT_SDK_IMAGE")
            or _read_runtime_image_tag()
        )
        if not image:
            raise RuntimeError(
                "Daytona provisioning requires DAYTONA_IMAGE / "
                "AGENT_SDK_IMAGE / .runtime-image-tag (produced by "
                "scripts/release.sh)."
            )

    # Daytona's control plane QUEUES provisioning. On a healthy account a
    # create returns in seconds even under the full `-n auto` burst, but when
    # the account is saturated (orphan/disk pressure from prior runs, or
    # production at >10 prompts/sec) the queue+provision time can run long. We
    # deliberately do NOT throttle create concurrency — that caps throughput
    # and serializes provisioning; instead we give each create a generous
    # CEILING so the legit tail fits without spurious "Function 'create'
    # exceeded timeout" / 502 failures. This is a ceiling, not the typical
    # latency. Not a retry — one create, enough room. Env-overridable per
    # deployment; pair with orphan hygiene (cleanup_orphans.py) to keep the
    # account fast.
    _base = 540 if dockerfile else 480
    create_timeout = int(os.environ.get("DAYTONA_CREATE_TIMEOUT_S", str(_base)))

    volumes = _build_volume_mounts(volume_id, subpath, shared_mounts)
    labels = _sandbox_labels()

    async def _do_create():
        if use_snapshot:
            return await daytona.create(
                CreateSandboxFromSnapshotParams(
                    snapshot=snapshot, auto_stop_interval=0, env_vars=env_vars,
                    volumes=volumes, labels=labels,
                ), timeout=create_timeout,
            )
        return await daytona.create(
            CreateSandboxFromImageParams(
                image=image, auto_stop_interval=0, env_vars=env_vars,
                volumes=volumes, labels=labels,
                resources=_to_daytona_resources(resources),
            ), timeout=create_timeout,
        )

    sandbox = await _daytona_create_with_502_retry(_do_create)

    try:
        # Run pre-start commands (skills, CLI install, etc.).
        # On non-zero exit the command raises so provisioning fails loudly.
        # Callers that want tolerant behaviour should wrap their command with
        # ``|| true`` so the shell always exits 0.
        if pre_start_commands:
            for cmd in pre_start_commands:
                log.info("provision pre-start: %s", cmd)
                # Pin HOME to the daytona agent home so user commands
                # (e.g. ``npx skills add ... -g``) write into the same
                # ``$HOME/.claude/skills/`` directory the supervisor
                # later spawns Claude under (cwd=/home/daytona). Without
                # this the shell's default HOME is /root and skills end
                # up in /root/.claude/skills/, invisible to Claude.
                wrapped = (
                    f"export HOME={_DAYTONA_AGENT_HOME} && "
                    f"mkdir -p {_DAYTONA_AGENT_HOME} && {cmd}"
                )
                result = await _run_sandbox_exec_async(sandbox, wrapped, timeout=120)
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
            sandbox_ref=sandbox.id,
        )
    except BaseException:
        try:
            await daytona.delete(sandbox)
        except Exception:
            pass
        raise


async def restart_daytona_supervisor(
    daytona_sandbox_id: str, agent_type: str = "opencode", root: str = "/tmp",
    spawn_env: dict[str, str] | None = None,
) -> ProviderInstance:
    """Re-attach to an existing daytona sandbox and respawn the supervisor
    inside it. Used by the resume path after the sandbox was stopped (or
    after the supervisor process was killed in place). Preserves the
    sandbox filesystem, so claude-agent-acp's persisted session state is
    available for session/load.

    Routes through ``start_supervisor_in_sandbox`` which spawns the
    supervisor from the image-baked runtime at ``/opt/agent-sdk/runtime/``.
    """
    daytona = await _get_async_daytona_client()
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
        await sandbox.start()
        await _wait_for_daytona_sandbox_ready(daytona, daytona_sandbox_id)
        sandbox = await daytona.get(daytona_sandbox_id)

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
        sandbox_ref=sandbox.id,
    )


async def _daytona_sandbox_op(instance: ProviderInstance, op: str) -> None:
    """Shared logic for destroy/stop Daytona sandbox.

    For ``delete``: poll until Daytona confirms the sandbox is gone
    (``daytona.get`` raises 404) before returning. Daytona's
    ``daytona.delete`` returns as soon as the control plane accepts the
    delete request — but the underlying compute is still being torn down
    for 10-30s. Without this poll, a fast caller (e.g. test asserting
    sandbox-gone after DELETE /sessions) sees state=``destroying``
    instead of gone and fails ``_assert_sandbox_gone`` under load.
    """
    if not instance.sandbox_ref:
        return
    try:
        daytona = await _get_async_daytona_client()
    except (ImportError, RuntimeError) as e:
        log.warning("cannot %s daytona sandbox %s: %s", op, instance.sandbox_ref, e)
        return

    try:
        sandbox = await daytona.get(instance.sandbox_ref)
        if op == "delete":
            await daytona.delete(sandbox)
            # Wait for the destroy to actually land (sandbox gone from
            # daytona's index). Bounded to 60s; if it doesn't go in that
            # window, log a warning and let the caller proceed — daytona
            # will eventually clean up.
            deadline = asyncio.get_running_loop().time() + 60.0
            while asyncio.get_running_loop().time() < deadline:
                try:
                    await daytona.get(instance.sandbox_ref)
                except Exception:
                    break  # get() raised → sandbox is gone
                await asyncio.sleep(0.5)
            else:
                log.warning(
                    "daytona delete confirm timeout for %s (still in"
                    " destroying state after 60s)",
                    instance.sandbox_ref,
                )
        else:
            await sandbox.stop()
        log.info("daytona sandbox %sd: %s", op, instance.sandbox_ref)
    except Exception as e:
        log.warning("failed to %s daytona sandbox %s: %s", op, instance.sandbox_ref, e)


@timed_provider_op("daytona", "destroy")
async def destroy_daytona(instance: ProviderInstance) -> None:
    """Delete a Daytona sandbox."""
    await _daytona_sandbox_op(instance, "delete")


@timed_provider_op("daytona", "stop")
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

    ``daytona`` must be an ``AsyncDaytona``; the returned sandbox is an
    ``AsyncSandbox``. The Pydantic ``state`` field is read directly off
    the freshly-fetched handle (each ``daytona.get()`` returns a new one).
    """
    loop = asyncio.get_running_loop()
    deadline = loop.time() + max_wait_s
    STABLE = {"started", "running", "stopped", "paused", "error",
              "archived", "destroyed"}
    sandbox = None
    state_str = ""
    while True:
        sandbox = await daytona.get(sandbox_ref)
        raw = sandbox.state
        state_str = _enum_str(raw)
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

    ``daytona`` must be an ``AsyncDaytona``.
    """
    for attempt in range(30):  # 30 * 0.5s = 15s max
        if attempt > 0:
            await asyncio.sleep(0.5)
        try:
            sandbox = await daytona.get(sandbox_ref)
            raw_state = sandbox.state
            state_str = _enum_str(raw_state)
            if state_str != "started":
                continue
            r = await sandbox.process.exec("echo ready", timeout=5)
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
        daytona = await _get_async_daytona_client()
    except (ImportError, RuntimeError) as e:
        log.warning("cannot start daytona sandbox %s: %s", sandbox_ref, e)
        return

    sandbox, state_str = await _wait_for_stable_daytona_state(daytona, sandbox_ref)
    if state_str in ("started", "running"):
        log.info("daytona sandbox %s already started", sandbox_ref)
        await _wait_for_daytona_sandbox_ready(daytona, sandbox_ref, sandbox=sandbox)
        return
    try:
        await sandbox.start()
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
    from daytona_api_client_async import VolumesApi as AsyncVolumesApi
    from daytona_api_client.models import VolumeState

    client = await _get_async_daytona_client()
    # Idempotent: volume.get(name, create=True) returns the existing volume
    # if one already has this name, else creates a new one. Lets
    # `_get_or_create_default_volume` re-enter safely across server restarts
    # and on multi-worker deploys where the DB row was lost but the Daytona
    # volume still exists.
    vol = await client.volume.get(name, True)
    vol_id = vol.id

    volumes_api = AsyncVolumesApi(client._api_client)
    try:
        deadline = asyncio.get_running_loop().time() + wait_ready_timeout
        while True:
            dto = await volumes_api.get_volume(vol_id)
            state = dto.state
            state_val = _enum_str(state, lower=False)
            if state_val == VolumeState.READY:
                break
            if state_val in {VolumeState.ERROR, VolumeState.DELETED, VolumeState.DELETING}:
                raise RuntimeError(f"Daytona volume {vol_id} entered unexpected state: {state_val}")
            if asyncio.get_running_loop().time() > deadline:
                raise TimeoutError(f"Daytona volume {vol_id} did not become ready within {wait_ready_timeout}s (last state: {state_val})")
            await asyncio.sleep(3)
    except Exception:
        try:
            await volumes_api.delete_volume(vol_id)
        except Exception as cleanup_err:
            log.warning(
                "orphaned daytona volume %s: cleanup delete failed: %s",
                vol_id, cleanup_err,
            )
        raise

    # Pre-create the standard directory layout on the volume.
    await _init_volume_dirs(vol_id)

    return vol_id


async def _daytona_create_with_502_retry(do_create, retries: int = 2):
    """Run ``do_create()`` and retry up to ``retries`` times on Daytona
    control-plane 502/503/504 (transient infra blips that surface as
    ``<html>...<title>502 Bad Gateway</title>...`` in the SDK message).

    Other errors (DaytonaError, image-not-found, auth, quota) raise on
    the first attempt — the retry is narrowly scoped to the 5xx HTML
    error class. Backoff is 2s, 4s, 8s.
    """
    delay = 2.0
    last_exc: Exception | None = None
    for attempt in range(retries + 1):
        try:
            return await do_create()
        except Exception as e:
            msg = str(e)
            transient = (
                "502 Bad Gateway" in msg
                or "503 Service" in msg
                or "504 Gateway" in msg
                or ("502" in msg and "html" in msg.lower())
            )
            if not transient or attempt == retries:
                raise
            log.warning(
                "daytona.create transient (attempt %d/%d) — retrying after %.1fs: %s",
                attempt + 1, retries + 1, delay, msg.split("\n", 1)[0][:120],
            )
            await asyncio.sleep(delay)
            delay *= 2
            last_exc = e
    raise last_exc  # unreachable but satisfies type-checker


async def _init_volume_dirs(volume_ref: str) -> None:
    """Spin a 1-shot sandbox to mkdir -p shared/ system/supervisor/ on the volume."""
    from daytona_sdk import (
        CreateSandboxFromSnapshotParams,
        CreateSandboxFromImageParams, VolumeMount,
    )

    daytona = await _get_async_daytona_client()

    # _init_volume_dirs only runs `mkdir` on a brand-new volume — any image
    # with a POSIX shell works. Phase E: no implicit hive-large default;
    # operators opt into a snapshot via DAYTONA_SNAPSHOT, otherwise the
    # ``node:22-slim`` fallback is used (volume-init does not need the
    # agent-sdk runtime).
    snapshot = os.environ.get("DAYTONA_SNAPSHOT", "").strip()
    use_snapshot = snapshot.lower() not in {"", "0", "false", "image"}

    # Mount the whole volume at /v (no subpath) so we can create dirs.
    volumes = [VolumeMount(volume_id=volume_ref, mount_path="/v")]
    init_labels = _sandbox_labels()

    async def _do_init_create():
        if use_snapshot:
            return await daytona.create(
                CreateSandboxFromSnapshotParams(
                    snapshot=snapshot, auto_stop_interval=0,
                    env_vars=_get_sandbox_env_vars(), volumes=volumes,
                    labels=init_labels,
                ), timeout=120,
            )
        return await daytona.create(
            CreateSandboxFromImageParams(
                image="node:22-slim", auto_stop_interval=0,
                env_vars=_get_sandbox_env_vars(), volumes=volumes,
                labels=init_labels,
            ), timeout=120,
        )

    sb = await _daytona_create_with_502_retry(_do_init_create)

    try:
        await _run_sandbox_exec_async(
            sb, "mkdir -p /v/shared /v/system/supervisor", timeout=30,
        )
        log.info("volume %s: initialized shared/ and system/supervisor/ dirs", volume_ref)
    finally:
        try:
            await daytona.delete(sb)
        except Exception:
            pass


async def delete_daytona_volume(provider_ref: str) -> None:
    """Delete a Daytona volume by provider-native id (UUID)."""
    from daytona_api_client_async import VolumesApi as AsyncVolumesApi
    client = await _get_async_daytona_client()
    volumes_api = AsyncVolumesApi(client._api_client)
    await volumes_api.delete_volume(provider_ref)


async def get_daytona_sandbox_status(sandbox_ref: str) -> str:
    """Return one of: 'running' | 'stopped' | 'missing' | 'error'.

    Callers (the SessionPool's recovery path) treat ``error`` as
    unrecoverable and provision a brand-new replacement. Transitional
    states like ``stopping`` / ``starting`` (5–30 s under load) MUST
    classify by their target state, not as ``error`` — otherwise an
    in-flight stop or boot that races a POST /message destroys the
    live sandbox the caller is trying to recover.

    Default for an unrecognized state is ``running`` rather than
    ``error`` for the same reason: a future Daytona state name we
    haven't seen yet should fall through to ``_wait_for_health`` /
    ``start_sandbox``, not to destroy + replace.
    """
    try:
        client = await _get_async_daytona_client()
        sb = await client.get(sandbox_ref)
    except Exception as e:
        msg = str(e).lower()
        if "not found" in msg or "404" in msg:
            return "missing"
        return "error"
    state = (getattr(sb, "state", None) or "")
    state_str = _enum_str(state)
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


async def exec_in_sandbox(inst: ProviderInstance, cmd: str, timeout: int = 30) -> ExecResult:
    """Run ``cmd`` inside the daytona sandbox via the SDK process channel."""
    if not inst.sandbox_ref:
        raise RuntimeError("no sandbox_ref for daytona exec")
    daytona = await _get_async_daytona_client()
    sandbox = await daytona.get(inst.sandbox_ref)
    try:
        r = await asyncio.wait_for(
            sandbox.process.exec(cmd, timeout=timeout),
            timeout=timeout + 5,
        )
    except asyncio.TimeoutError:
        return ExecResult(stdout="", stderr="", exit_code=-1, timed_out=True)
    out = (r.result if hasattr(r, "result") else str(r)) or ""
    err = (r.stderr if hasattr(r, "stderr") else "") or ""
    code = r.exit_code if hasattr(r, "exit_code") else None
    out, trunc = _truncate(out.encode(), _MAX_OUTPUT_BYTES)
    return ExecResult(stdout=out, stderr=err, exit_code=code, stdout_truncated=trunc)


@timed_provider_op("daytona", "create_sandbox")
async def create_sandbox(
    *,
    volume_ref: str,
    subpath: str,
    agent_type: str = "opencode",
    spawn_env: dict[str, str] | None = None,
    port: int | None = None,
    root: str | None = None,
    dockerfile: str | None = None,
    pre_start_commands: list[str] | None = None,
    sandbox_ref: str | None = None,  # accepted for parity; unused here
    shared_mounts: list[str] | None = None,
    resources: Any = None,
) -> ProviderInstance:
    """Uniform ``create_sandbox`` for the Daytona provider.

    Delegates to ``provision_daytona_sandbox`` which creates the sandbox with
    the volume mounts (per-agent subpath at /vol + any opt-in shared mounts)
    but does NOT start a supervisor; the caller must run
    ``DaytonaSandboxSession.start`` (start_supervisor_in_sandbox) before
    talking to the supervisor.

    ``spawn_env`` / ``port`` / `sandbox_ref` are accepted for parity with
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
        resources=resources,
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
_conditional_create_support_cache: dict[str, bool] = {}
_conditional_create_probe_lock = asyncio.Lock()


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
            agent_type="opencode", volume_id=ref, subpath=None,
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


async def _reap_stale_utility(stale: list[tuple[str, "ProviderInstance"]]) -> None:
    """Destroy a batch of stale utility sandboxes concurrently.

    Each ``destroy_daytona`` is a control-plane round-trip; reaping them one at
    a time lets a single slow/hung destroy stall the rest of the batch (and the
    reaper tick). Fan out (bounded) so the slow one doesn't block its siblings.
    """
    async def _one(ref, inst) -> None:
        log.info("daytona utility sandbox: reaping idle volume %s", ref[:16])
        try:
            await destroy_daytona(inst)
        except Exception as e:  # pragma: no cover
            log.warning("utility reaper: destroy failed for %s: %s", ref[:16], e)

    await bounded_gather([_one(ref, inst) for ref, inst in stale])


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
        await _reap_stale_utility(stale)


async def _run_in_utility_sandbox(ref: str, cmd: str, timeout: int = 30):
    """Run ``cmd`` in the utility sandbox for ``ref``. Cached + TTL-reaped.

    Transient errors (sandbox died on the provider side between the cache
    entry's freshness check and the exec call) trigger one retry after
    dropping the cache entry, so callers don't see a single stale-cache
    hit bubble up as a 500.
    """
    from ... import providers as _prov  # local import for cycle
    inst = await _get_or_create_utility(ref)
    try:
        return await _prov.exec_in_instance(inst, cmd, timeout=timeout)
    except Exception as e:
        log.warning("utility exec failed on cached sandbox for %s (%s); retrying with fresh sandbox", ref[:16], e)
        await _drop_utility(ref)
        inst = await _get_or_create_utility(ref)
        return await _prov.exec_in_instance(inst, cmd, timeout=timeout)


async def volume_download(ref: str, path: str) -> bytes:
    """Read raw bytes from ``<volume>/<path>`` via Daytona's filesystem API.

    This bypasses the shell exec/stdout path by using Daytona's dedicated
    file-download endpoint through the SDK. Used by DaytonaVolumeAdapter.download
    and _daytona_supports_conditional_create.
    """
    rel = _safe_path(None, path or "")
    if not rel:
        raise ValueError("volume_download: path required")
    target = "/v/" + rel
    inst = await _get_or_create_utility(ref)
    if not inst.sandbox_ref:
        raise RuntimeError("volume_download: utility sandbox_ref missing")

    daytona_client = await _get_async_daytona_client()
    try:
        sandbox = await daytona_client.get(inst.sandbox_ref)
    except Exception as e:
        raise RuntimeError(f"volume_download: get sandbox failed: {e}") from e

    try:
        return await sandbox.fs.download_file(target)
    except Exception as e:
        msg = str(e)
        if "not found" in msg.lower() or "404" in msg:
            raise FileNotFoundError(f"{path} not found on volume {ref}") from e
        raise RuntimeError(f"volume_download failed: {msg}") from e


async def _conditional_upload_if_absent(ref: str, abs_path: str, content: bytes) -> str:
    """Attempt atomic create-if-absent upload through Daytona toolbox.

    Returns:
      - "created" when destination was created
      - "exists" when backend rejected due to precondition
    Raises RuntimeError on transport/protocol failures.
    """
    inst = await _get_or_create_utility(ref)
    if not inst.sandbox_ref:
        raise RuntimeError("conditional upload: utility sandbox_ref missing")
    daytona_client = await _get_async_daytona_client()
    try:
        sandbox = await daytona_client.get(inst.sandbox_ref)
    except Exception as e:
        raise RuntimeError(f"conditional upload: get sandbox failed: {e}") from e
    try:
        await sandbox.fs._api_client.upload_file(  # pyright: ignore[reportPrivateUsage]
            path=abs_path,
            file=content,
            _headers={"If-None-Match": "*"},
        )
        return "created"
    except Exception as e:
        msg = str(e).lower()
        if "412" in msg or "precondition" in msg:
            return "exists"
        raise RuntimeError(f"conditional upload failed: {e}") from e


async def _move_overwrite(ref: str, src_abs: str, dst_abs: str) -> None:
    """Move ``src_abs`` to ``dst_abs`` through Daytona's filesystem API."""
    inst = await _get_or_create_utility(ref)
    if not inst.sandbox_ref:
        raise RuntimeError("move overwrite: utility sandbox_ref missing")
    daytona_client = await _get_async_daytona_client()
    try:
        sandbox = await daytona_client.get(inst.sandbox_ref)
    except Exception as e:
        raise RuntimeError(f"move overwrite: get sandbox failed: {e}") from e
    try:
        await sandbox.fs.move_files(src_abs, dst_abs)
    except Exception as e:
        msg = str(e)
        if "not found" in msg.lower() or "404" in msg:
            raise FileNotFoundError(f"{src_abs} not found on volume {ref}") from e
        raise RuntimeError(f"move overwrite failed: {msg}") from e


async def _daytona_supports_conditional_create(ref: str) -> bool:
    """Detect once per volume whether toolbox upload honors If-None-Match: *."""
    import base64 as _b64
    mode = (os.environ.get("DAYTONA_CONDITIONAL_CREATE_MODE", "auto") or "auto").strip().lower()
    if mode in {"on", "true", "1", "force", "force_on"}:
        return True
    if mode in {"off", "false", "0", "disable", "force_off"}:
        return False
    cached = _conditional_create_support_cache.get(ref)
    if cached is not None:
        return cached
    async with _conditional_create_probe_lock:
        cached = _conditional_create_support_cache.get(ref)
        if cached is not None:
            return cached
        probe_rel = f"system/.conditional-create-probe-{uuid.uuid4().hex}.txt"
        probe_abs = "/v/" + probe_rel
        try:
            # Inline the write: same shell pipeline the adapter's write() uses.
            b64 = _b64.b64encode(b"probe-a").decode()
            parent = shlex.quote("/v/system")
            target = shlex.quote(probe_abs)
            write_cmd = (
                f"mkdir -p {parent} && "
                f"printf %s {shlex.quote(b64)} | base64 -d > {target}"
            )
            res = await _run_in_utility_sandbox(ref, write_cmd)
            if res.exit_code != 0:
                _conditional_create_support_cache[ref] = False
                return False
            result = await _conditional_upload_if_absent(ref, probe_abs, b"probe-b")
            if result != "exists":
                _conditional_create_support_cache[ref] = False
                return False
            current = await volume_download(ref, probe_rel)
            supported = current == b"probe-a"
            _conditional_create_support_cache[ref] = supported
            return supported
        except Exception:
            _conditional_create_support_cache[ref] = False
            return False
        finally:
            try:
                await volume_delete(ref, probe_rel)
            except Exception:
                pass


async def volume_delete(ref: str, path: str) -> None:
    """Delete a file or directory at ``<volume>/<path>`` via Daytona's fs API.

    Kept as a module-level helper (not inlined into the class) because:
    - _daytona_supports_conditional_create calls it in its finally block
    - test_volumes_api.py patches it directly to intercept adapter.delete()
    """
    rel = _safe_path(None, path or "")
    if not rel:
        raise ValueError("volume_delete: path required")
    target = "/v/" + rel
    inst = await _get_or_create_utility(ref)
    if not inst.sandbox_ref:
        raise RuntimeError("volume_delete: utility sandbox_ref missing")

    daytona_client = await _get_async_daytona_client()
    try:
        sandbox = await daytona_client.get(inst.sandbox_ref)
    except Exception as e:
        raise RuntimeError(f"volume_delete: get sandbox failed: {e}") from e

    try:
        await sandbox.fs.delete_file(target, recursive=True)
    except Exception as e:
        msg = str(e)
        if "not found" in msg.lower() or "404" in msg:
            raise FileNotFoundError(f"{path} not found on volume {ref}") from e
        raise RuntimeError(f"volume_delete failed: {msg}") from e


# ---------------------------------------------------------------------------
# DaytonaVolumeAdapter — ShellVolumeAdapter subclass for Daytona volumes.
#
# The transport is the TTL-cached utility sandbox (_run_in_utility_sandbox).
# SDK primitives override shell where cheaper/more reliable:
#   - download: fs.download_file (avoids base64 exec round-trip)
#   - delete:   fs.delete_file   (SDK atomic delete, not shell rm)
#   - rename:   extra Daytona-specific logic (_move_overwrite + conditional
#               create for no-overwrite; same-path early return)
# ---------------------------------------------------------------------------

class DaytonaVolumeAdapter(ShellVolumeAdapter):
    """Volume ops for Daytona via the TTL-cached utility sandbox.

    Inherits tree/read/exists/write/upload/mkdir from ShellVolumeAdapter.
    Overrides download, delete, and rename with Daytona SDK primitives.

    Daytona-specific knobs vs the generic defaults:
      - tree_max_depth = 3   (bounded depth; daytona volumes can be deep)
      - tree_check_rc = False (partial find results tolerated on large trees)
      - shell_timeout = 30   (tight timeout; utility sandbox is always warm)
      - vol_root = "/v"      (volume is mounted at /v in the utility sandbox)
    """

    provider = "daytona"
    tree_find_gnu = True   # debian image — single-pass find -printf
    vol_root = "/v"
    tree_max_depth = 3
    tree_check_rc = False
    shell_timeout = 30

    async def _run_shell(self, shell: str, *, timeout: int) -> tuple[int, bytes, bytes]:
        """Adapt utility-sandbox ExecResult to the (rc, out, err) tuple
        that ShellVolumeAdapter expects.

        exit_code=None means the SDK didn't report it — treated as 0 (OK),
        consistent with _ExecResult.ok in the daytona module.
        """
        res = await _run_in_utility_sandbox(self.provider_ref, shell, timeout=timeout)
        # Fail CLOSED on an unconfirmed exec: the daytona SDK can return
        # exit_code=None (seen in practice on pre-start commands); treating
        # that as success would turn exists() into a false True and let
        # write/mkdir report 204 with no proof the command ran. -1 (not 1:
        # exists() maps rc==1 to False) routes every op to its error path.
        rc = res.exit_code if res.exit_code is not None else -1
        return rc, (res.stdout or "").encode(), (res.stderr or "").encode()

    async def download(self, path: str) -> bytes:
        """Download via Daytona's fs.download_file (cheaper than base64 exec)."""
        return await volume_download(self.provider_ref, path)

    async def delete(self, path: str) -> None:
        """Delete via Daytona's fs.delete_file SDK primitive."""
        await volume_delete(self.provider_ref, path)

    async def rename(self, path: str, new_path: str, *, overwrite: bool = True) -> None:
        """Rename/move with Daytona-specific branches.

        Diverges from generic ShellVolumeAdapter.rename:
          - same-path early return (avoid any sandbox round-trip)
          - overwrite path: shell precheck (missing/dir) + _move_overwrite SDK
            call + settle loop (object-store visibility lag)
          - no-overwrite path: _daytona_supports_conditional_create gating +
            _conditional_upload_if_absent instead of hardlink (hardlinks are
            unreliable on Daytona object-store volumes)
        """
        src_rel = self._rel(path)
        dst_rel = self._rel(new_path)
        if not src_rel or not dst_rel:
            raise ValueError("volume rename: path and new_path required")
        src = self._target(src_rel)
        dst = self._target(dst_rel)
        dst_parent = shlex.quote(self._parent(dst_rel))
        if src_rel == dst_rel:
            return
        # mountpoint-backed volumes can lag after copy/delete.
        settle_check = (
            f"for _i in 1 2 3 4 5 6 7 8 9 10; do "
            f"if [ -e {shlex.quote(dst)} ] && [ ! -e {shlex.quote(src)} ]; then exit 0; fi; "
            f"sleep 0.1; "
            f"done; "
            f"echo __RENAME_NOT_VISIBLE__; exit 98"
        )
        if overwrite:
            # Use Daytona's filesystem move endpoint so creation and source
            # removal happen in the provider layer, not through the
            # eventually-consistent mounted /v view.
            rc, out, err = await self._run_shell(
                (
                    f"if [ ! -e {shlex.quote(src)} ]; then echo __MISSING__; exit 2; fi; "
                    f"mkdir -p {dst_parent} || exit $?; "
                    f"if [ -d {shlex.quote(src)} ]; then echo __UNSUPPORTED_DIR__; exit 95; fi"
                ),
                timeout=self.shell_timeout,
            )
            if rc != 0:
                if b"__MISSING__" in out:
                    raise FileNotFoundError(f"{path} not found on volume {self.provider_ref}")
                if b"__UNSUPPORTED_DIR__" in out:
                    raise NotImplementedError("overwrite rename for directories is not supported")
                raise self._err("rename", rc, err)
            await _move_overwrite(self.provider_ref, src, dst)
            verify_rc, _vout, _verr = await self._run_shell(
                settle_check, timeout=self.shell_timeout,
            )
            if verify_rc != 0:
                raise RuntimeError("volume rename postcondition failed: destination not visible")
            return

        # Daytona volumes are object-store backed; hardlinks are not reliable.
        # Require a real create-if-absent primitive instead of race-prone emulation.
        if not await _daytona_supports_conditional_create(self.provider_ref):
            raise NotImplementedError(
                "atomic no-overwrite rename is not supported on this Daytona volume backend"
            )
        rc, out, err = await self._run_shell(
            (
                f"if [ ! -e {shlex.quote(src)} ]; then echo __MISSING__; exit 2; fi; "
                f"mkdir -p {dst_parent} || exit $?; "
                f"if [ -d {shlex.quote(src)} ]; then echo __UNSUPPORTED_DIR__; exit 95; fi"
            ),
            timeout=self.shell_timeout,
        )
        if rc != 0:
            if b"__MISSING__" in out:
                raise FileNotFoundError(f"{path} not found on volume {self.provider_ref}")
            if b"__UNSUPPORTED_DIR__" in out:
                raise NotImplementedError("atomic no-overwrite directory rename is not supported")
            raise self._err("rename", rc, err)
        src_bytes = await volume_download(self.provider_ref, src_rel)
        outcome = await _conditional_upload_if_absent(self.provider_ref, dst, src_bytes)
        if outcome == "exists":
            raise VolumeFileExistsError(new_path)
        if outcome != "created":
            raise RuntimeError(
                "volume rename failed: conditional destination claim returned unknown result"
            )
        await volume_delete(self.provider_ref, src_rel)
        verify_rc, _vout, _verr = await self._run_shell(
            settle_check, timeout=self.shell_timeout,
        )
        if verify_rc != 0:
            raise RuntimeError("volume rename postcondition failed: destination not visible")


# ---------------------------------------------------------------------------
# Reconciliation — delete orphaned daytona sandboxes on startup
# ---------------------------------------------------------------------------

async def reconcile_on_startup() -> None:
    """Delete daytona sandboxes whose id is not in any live session row.

    Lists by ``agent_sdk_origin=<AGENT_SDK_ORIGIN>`` so dev/test/prod
    deploys sharing one daytona account never cross-reap. Mirrors what
    ``scripts/cleanup_orphans.py`` does, automated at boot.

    Failures are logged and swallowed.
    """
    try:
        from ... import db as dbmod
    except Exception as e:
        log.warning("daytona reconcile: cannot import api.db: %s", e)
        return

    try:
        daytona = await _get_async_daytona_client()
    except Exception as e:
        log.warning("daytona reconcile: client unavailable: %s", e)
        return

    labels = _sandbox_labels()
    try:
        items, _capped = await _list_labeled_sandboxes(daytona, labels)
    except Exception as e:
        log.warning("daytona reconcile: list failed: %s", e)
        return

    try:
        live_refs = await dbmod.live_sandbox_refs()
    except Exception as e:
        log.warning("daytona reconcile: live-session query failed: %s", e)
        return

    orphans = [
        sb for sb in items
        if getattr(sb, "id", None) and getattr(sb, "id") not in live_refs
    ]
    if not orphans:
        return

    # Fan out the deletes: a crash can strand many orphans, and reclaiming them
    # one blocking `daytona.delete` at a time serialises N control-plane RTTs on
    # the startup path. Bounded so we don't stampede the shared client pool.
    async def _reap(sb) -> None:
        sid = sb.id
        log.info("daytona reconcile: deleting orphan %s", sid[:16])
        try:
            await daytona.delete(sb)
        except Exception as e:
            log.warning("daytona reconcile: delete %s: %s", sid[:16], e)

    await bounded_gather([_reap(sb) for sb in orphans])


async def _list_labeled_sandboxes(daytona, labels: dict[str, str]) -> tuple[list, bool]:
    """Collect labelled daytona sandboxes via the SDK's async-iterator ``list``.

    daytona-sdk >=0.184 made ``AsyncDaytona.list`` an async generator that takes
    a ``ListSandboxesQuery`` and auto-paginates, yielding ``AsyncSandbox`` objects
    (older builds returned an awaitable page object via ``list(labels=, page=)``
    — ``await``-ing the new generator raises "async_generator can't be used in
    'await' expression", which is what broke the reconcile + orphan monitor).
    We iterate the generator and stop at ``_DAYTONA_MAX_ITEMS`` as a runaway
    guard. Returns ``(items, capped)``; ``capped`` is True when we hit that cap
    (the caller's result is then a lower bound).
    """
    from daytona_sdk import ListSandboxesQuery

    out: list = []
    capped = False
    async for sb in daytona.list(ListSandboxesQuery(labels=labels)):
        out.append(sb)
        if len(out) >= _DAYTONA_MAX_ITEMS:
            capped = True
            break
    return out, capped


async def detect_orphan_sandboxes(origin: str | None = None, live_refs=None) -> dict:
    """List labelled daytona sandboxes and diff them against the live session
    refs. Pure DETECTION — never deletes.

    ``origin`` defaults to ``AGENT_SDK_ORIGIN`` (via ``_sandbox_labels``);
    ``live_refs`` defaults to ``db.live_sandbox_refs()``. A sandbox whose id is
    not in ``live_refs`` has no session row, i.e. leaked compute.

    Returns ``{"total_seen": int, "orphans": [(id, state)],
    "state_hist": {state: count}, "capped": bool}``.
    """
    from collections import Counter

    labels = {_LABEL_ORIGIN: origin} if origin else _sandbox_labels()
    daytona = await _get_async_daytona_client()
    items, capped = await _list_labeled_sandboxes(daytona, labels)
    if live_refs is None:
        from ... import db as dbmod
        live_refs = await dbmod.live_sandbox_refs()
    orphans: list[tuple[str, str]] = []
    state_hist: Counter = Counter()
    for sb in items:
        sid = getattr(sb, "id", None)
        if not sid or sid in live_refs:
            continue
        raw = getattr(sb, "state", None) or ""
        st = _enum_str(raw)
        orphans.append((sid, st))
        state_hist[st] += 1
    return {
        "total_seen": len(items),
        "orphans": orphans,
        "state_hist": dict(state_hist),
        "capped": capped,
    }


#: uniform per-provider adapter handle — ``get_volume_adapter`` dispatches
#: via ``_dispatch_mod(provider).VolumeAdapter`` (one registry for everything).
VolumeAdapter = DaytonaVolumeAdapter
