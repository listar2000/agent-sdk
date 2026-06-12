"""Sandbox transports for the native runtime — tool effects over provider
primitives, no supervisor anywhere.

A transport owns ONE sandbox for ONE session: create it (native flavor — a
no-op PID-1, nothing listening, nothing inbound), run commands in it, move
file bytes in and out, destroy it. The loop's tools (api/native/tools.py)
call only this surface, so adding daytona/modal in P1 is a new subclass,
not a tool change.

P0 ships DockerTransport. Verified provider-audit constraints baked in:
- the docker CLI's exec timeout kills the CLIENT, not the in-container
  process — commands are wrapped with coreutils/busybox ``timeout`` inside
  the container, with the asyncio wait as a backstop only;
- ``docker exec <args>`` embeds argv in one execve — base64-in-argv caps
  file payloads at ~96KiB (MAX_ARG_STRLEN), so writes stream base64 over
  STDIN instead (no practical size limit);
- cwd rides the ``-w`` flag; env rides an in-command ``export`` preamble
  (NOT ``-e`` — that applies before the runtime resolves ``sh``, so a
  PATH-class env would 127 every exec). The native create passes no
  secrets into the container environment at all — the session's non-LLM
  secrets are injected per-exec, and LLM keys never reach the transport
  (design §6).
"""

from __future__ import annotations

import asyncio
import base64
import logging
import re
import shlex
from dataclasses import dataclass

from api.providers._shared import _MAX_OUTPUT_BYTES, _truncate
from api.providers.docker import _run_docker, _run_docker_checked

log = logging.getLogger(__name__)

#: POSIX env var name — same guard as _shared._build_env_prefix. Keys are
#: interpolated unquoted on the K= side of the export, so a non-name key
#: would escape into shell syntax; ingress (_coerce_env_dict) already
#: enforces this, the re-check here is defence-in-depth.
_ENV_KEY_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_]*$")


def _export_preamble(env: dict[str, str] | None) -> str:
    """``export K='v' … && `` prefix for USER-facing exec commands.

    Injected INSIDE the already-running shell — never as ``docker exec -e``
    flags or a ``K=V cmd`` assignment-prefix — so the shell itself and the
    transport's own plumbing words (sh/setsid) resolve under the sandbox's
    default environment first. A session env named PATH/LD_PRELOAD then
    alters lookup only for the user's command (ordinary Unix semantics)
    instead of 127-ing the exec wrapper itself. ``export`` (not a bare
    assignment-prefix) so the vars reach EVERY statement of a compound
    command and its subprocesses, not just the first simple command.
    """
    if not env:
        return ""
    parts = []
    for k, v in env.items():
        if not _ENV_KEY_RE.match(k):
            raise ValueError(
                f"invalid env var name {k!r}: must match [A-Za-z_][A-Za-z0-9_]*")
        parts.append(f"{k}={shlex.quote(v)}")
    return "export " + " ".join(parts) + " && "

#: in-container wall-clock cap for one tool exec when the caller passes none.
DEFAULT_EXEC_TIMEOUT_S = 300
#: extra slack for the host-side backstop over the in-container timeout.
_BACKSTOP_SLACK_S = 10
#: exit statuses indicating the watchdog (or backstop) killed the command:
#: 137 = 128+KILL from the in-container group kill; 124 = host backstop.
#: Signal statuses are ambiguous (any KILLed command shares them), so
#: timed_out additionally requires the wall clock to have reached the
#: budget.
_TIMEOUT_RCS = frozenset({124, 137})


@dataclass
class TransportExecResult:
    stdout: str
    stderr: str
    exit_code: int
    timed_out: bool


class SandboxGoneError(Exception):
    """Raised by a transport op when the sandbox has been REMOVED/TERMINATED
    out from under a live session (docker container pruned, modal sandbox hit
    its hard timeout ceiling, daytona VM hard-killed) — distinct from
    'stopped', which the transport self-heals via resume. The session catches
    this, drops the dead transport, and re-runs _ensure_sandbox to recreate a
    fresh sandbox (recovering the workspace on modal/daytona volumes)."""


class DockerTransport:
    """One docker container per session, ``sleep infinity`` as PID-1."""

    provider = "docker"

    #: ``docker start`` is best-effort and SWALLOWS a start failure on a
    #: removed/corrupt container, so a post-resume status() re-check is required
    #: to catch the still-dead case. Resume is NOT authoritative.
    resume_is_authoritative = False

    def __init__(self, container_id: str | None = None, workdir: str = "/",
                 env: dict[str, str] | None = None):
        self.container_id = container_id
        # Default working directory for exec and the anchor for relative
        # file paths, so a tool's ``note.txt`` and a later ``cat note.txt``
        # resolve to the same place regardless of which path was used.
        self.workdir = workdir
        # Session env (e.g. GITHUB_TOKEN, custom secrets) injected into every
        # exec — per-call, never baked into the container config, so secrets
        # don't show up in `docker inspect`. The loop's bash/file tools call
        # exec without env; without this default they'd run secret-less while
        # /sandbox/exec (which passes env explicitly) got them — an asymmetry
        # vs the supervisor runtime where the agent's shell sees spawn_env.
        self.default_env = dict(env or {})

    @property
    def ref(self) -> str | None:
        """Provider-agnostic sandbox id (the NativeSession persists this)."""
        return self.container_id

    def _resolve(self, path: str) -> str:
        if path.startswith("/"):
            return path
        base = self.workdir.rstrip("/") or ""
        return f"{base}/{path}"

    # ── lifecycle ──────────────────────────────────────────────────────────

    async def create(self, *, image: str, labels: dict[str, str] | None = None,
                     mounts: list[str] | None = None) -> str:
        """Create + start the sandbox. Returns the container id.

        Native flavor: PID-1 is ``sleep infinity`` — no supervisor, no
        published ports, nothing listening. Readiness is one ``exec true``
        (replaces the supervisor health gate the provider create uses).
        ``mounts`` are raw ``--mount`` values (the P0-G wiring passes the
        same volume-subpath mounts the docker provider builds today).
        """
        args = ["run", "-d"]
        for m in mounts or []:
            args += ["--mount", m]
        for k, v in (labels or {}).items():
            args += ["--label", f"{k}={v}"]
        args += ["--entrypoint", "sleep", image, "infinity"]
        out = await _run_docker_checked(*args, timeout=120)
        cid = out.decode().strip()
        if not cid:
            raise RuntimeError("docker run returned empty container id")
        self.container_id = cid
        # Readiness gate that ALSO creates the working directory in the SAME
        # exec: a successful `mkdir -p <workdir>` is an equally-loud readiness
        # proof, so the caller needs no second `exec mkdir` round-trip. Mirrors
        # how the daytona/modal transports fold the mkdir into create() — keeps
        # native cold-create at one post-run exec across all three providers.
        rc, _, err = await _run_docker(
            "exec", cid, "mkdir", "-p", self.workdir, timeout=30)
        if rc != 0:
            await self.destroy()
            raise RuntimeError(
                f"native sandbox readiness/mkdir failed (rc={rc}): "
                f"{err.decode(errors='replace')[:300]}")
        return cid

    async def is_alive(self) -> bool:
        if not self.container_id:
            return False
        rc, out, _ = await _run_docker(
            "inspect", "-f", "{{.State.Running}}", self.container_id, timeout=15)
        return rc == 0 and out.decode().strip() == "true"

    async def status(self) -> str:
        """One inspect → ``running`` | ``stopped`` | ``missing`` | ``error``.
        Same vocabulary as the docker-CLI provider's ``get_sandbox_status``
        so resume costs ONE round-trip (vs separate exists()+is_alive()).
        ``error`` is a transient daemon failure — caller must NOT treat it as
        ``missing`` and cold-create over a live container."""
        if not self.container_id:
            return "missing"
        rc, out, err = await _run_docker(
            "inspect", "-f", "{{.State.Status}}", self.container_id, timeout=15)
        if rc != 0:
            msg = (err or b"").decode(errors="replace").lower()
            if "no such" in msg:
                return "missing"
            return "error"
        state = out.decode(errors="replace").strip().lower()
        if state == "running":
            return "running"
        if state in {"exited", "created", "paused", "dead"}:
            return "stopped"
        return "error"

    async def hibernate(self) -> None:
        """``docker stop -t 0`` — free CPU/RAM, KEEP the container and its
        writable layer (workspace files survive). Resume = ``start()``.

        ``-t 0`` (immediate SIGKILL, no grace) because the native PID-1 is a
        bare ``sleep infinity`` with NO SIGTERM disposition — as PID-1 the
        kernel IGNORES the default SIGTERM ``docker stop`` sends, so any grace
        period is dead time docker waits out before SIGKILLing anyway (~2s with
        ``-t 2``). Native never snapshots-on-stop (conversation state is
        checkpointed per turn; the workspace lives in the writable layer/volume,
        both of which survive a SIGKILL), and ``sleep`` holds no flushable
        state, so skipping the grace is loss-free — and brings reap latency from
        ~2.1s to ~0.13s, at/under the supervisor hibernate (whose supervisor.js
        PID-1 traps SIGTERM and exits in ~0.2s)."""
        if not self.container_id:
            return
        await _run_docker("stop", "-t", "0", self.container_id, timeout=30)

    async def resume(self) -> None:
        """``docker start`` a hibernated container — files intact, cheap."""
        if not self.container_id:
            return
        await _run_docker("start", self.container_id, timeout=30)

    async def destroy(self) -> None:
        if not self.container_id:
            return
        await _run_docker("rm", "-f", self.container_id, timeout=60)

    # ── exec ───────────────────────────────────────────────────────────────

    async def exec(self, command: str, *, cwd: str | None = None,
                   env: dict[str, str] | None = None,
                   timeout_s: int = DEFAULT_EXEC_TIMEOUT_S,
                   use_default_env: bool = True) -> TransportExecResult:
        """Run one shell command inside the sandbox.

        The in-container ``timeout`` is the real enforcement: killing the
        ``docker exec`` client (what an asyncio timeout alone would do)
        leaves the process running inside the container. Exit code 124 from
        the wrapper maps to ``timed_out=True``.

        ``use_default_env=False`` is for the transport's OWN plumbing
        (read_file's base64, create's readiness mkdir): those commands must
        run env-immune so a session env named PATH/BASH_ENV can't break the
        machinery that moves bytes (see _export_preamble).
        """
        self._require_sandbox()
        merged = {**(self.default_env if use_default_env else {}),
                  **(env or {})}
        command = _export_preamble(merged) + command
        t = max(1, int(timeout_s))
        # In-container timeout that reliably reaps the WHOLE command tree.
        # Neither busybox ``timeout`` nor a TERM trap works here: busybox
        # TERMs only its direct child (orphaning grandchildren), and POSIX
        # shells defer signal traps until the foreground child exits — the
        # trap never fires while ``sleep 30`` runs. Instead: ``setsid``
        # makes the user command lead its OWN process group, a detached
        # watchdog ``kill -9``s that group at the deadline, and ``wait``
        # preserves the natural exit code. The watchdog's fds are detached
        # so docker exec's stream EOFs the moment the wrapper exits instead
        # of waiting out the full deadline.
        inner = (
            f"setsid sh -c {shlex.quote(command)} & c=$!; "
            f"( sleep {t} && kill -9 -$c 2>/dev/null ) >/dev/null 2>&1 </dev/null & w=$!; "
            "wait $c; rc=$?; kill -9 $w 2>/dev/null; exit $rc"
        )
        wrapped = inner
        args = ["exec"]
        args += ["-w", cwd or self.workdir]
        # NO -e flags: env rides the in-command export preamble above. -e
        # applies before the runtime resolves `sh`, so a PATH-class session
        # env would 127 every exec at the OCI layer.
        args += [self.container_id, "sh", "-c", wrapped]
        started = asyncio.get_event_loop().time()
        try:
            rc, out, err = await _run_docker(*args, timeout=t + _BACKSTOP_SLACK_S)
        except RuntimeError:
            # Host-side backstop fired — the in-container wrapper should
            # have ended it first; report as timeout either way.
            return TransportExecResult("", "host-side exec backstop fired",
                                       124, True)
        # Self-heal an externally-stopped sandbox: if the container was
        # hibernated/stopped out from under a live session (the reaper, an
        # ops `docker stop`, a host suspend), docker exec fails with "is not
        # running". Resume the SAME container and retry once — keeps the
        # session warm (no cold-recovery / checkpoint reload) and pays
        # nothing on the happy path. A genuinely-removed container can't
        # resume; that falls to the recreate gate below.
        #
        # `err` is the SHARED docker-exec stderr — it carries BOTH the docker
        # daemon's own error AND the inner command's forwarded stderr. So a
        # marker match alone is NOT trustworthy: a healthy-container command
        # that prints "is not running" / "no such container" and exits
        # non-zero would otherwise spuriously resume (re-running a possibly
        # non-idempotent command) or recreate. Confirm with one inspect —
        # status() is a pure daemon query, immune to that stderr contamination.
        # The short-circuit `and` keeps the inspect OFF the happy path: it runs
        # only when rc!=0 AND the cheap marker pre-filter already matched.
        if rc != 0 and _container_not_running(err) and await self.status() == "stopped":
            await self.resume()
            rc, out, err = await _run_docker(*args, timeout=t + _BACKSTOP_SLACK_S)
            # If resume did NOT restore a runnable container, do not wedge the
            # session re-trying a corpse: a `dead` container (kernel/storage
            # fault) maps to "stopped" but can't `docker start`, so resume()
            # fails silently and every retry keeps hitting "is not running"
            # without ever matching the removed-marker. Re-check the
            # authoritative state — still-not-running ⇒ unrecoverable by resume,
            # so signal the session to recreate. status()=="running" means the
            # command itself just failed (container fine) → return it as data.
            if rc != 0 and await self.status() != "running":
                raise SandboxGoneError(
                    f"docker container {self.container_id[:12]} not runnable after resume")
        # The container was REMOVED (pruned/OOM-reaped) out from under the live
        # session — resume can't bring it back. Signal the session to recreate.
        if rc != 0 and _container_removed(err) and await self.status() == "missing":
            raise SandboxGoneError(f"docker container {self.container_id[:12]} removed")
        elapsed = asyncio.get_event_loop().time() - started
        stdout, _ = _truncate(out, _MAX_OUTPUT_BYTES)
        stderr, _ = _truncate(err, _MAX_OUTPUT_BYTES)
        timed_out = rc in _TIMEOUT_RCS and elapsed >= t * 0.9
        return TransportExecResult(stdout, stderr, rc, timed_out)

    # ── files ──────────────────────────────────────────────────────────────

    async def write_file(self, path: str, data: bytes) -> None:
        """Stream base64 over stdin — safe for payloads beyond the ~96KiB
        argv ceiling that base64-in-argv hits (Linux MAX_ARG_STRLEN).
        Self-heals an externally-stopped container (resume + retry once),
        same as exec()."""
        self._require_sandbox()
        path = self._resolve(path)
        q = shlex.quote(path)
        qdir = shlex.quote(_dirname(path))
        payload = base64.b64encode(data)

        async def _attempt() -> tuple[int, bytes]:
            from api.providers.docker import _require_docker
            proc = await asyncio.create_subprocess_exec(
                _require_docker(), "exec", "-i", self.container_id, "sh", "-c",
                f"mkdir -p {qdir} && base64 -d > {q}",
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            _, e = await asyncio.wait_for(proc.communicate(payload), timeout=120)
            return proc.returncode or 0, e or b""

        rc, err = await _attempt()
        # Same shared-stderr caveat as exec(): a base64 payload or path that
        # contains the sentinel must not spoof a stopped/removed verdict, so
        # confirm with an inspect (status()) before resuming/recreating.
        if rc != 0 and _container_not_running(err) and await self.status() == "stopped":
            await self.resume()
            rc, err = await _attempt()
            # resume couldn't restore a runnable container (e.g. a 'dead'
            # container) — recreate rather than wedge (mirrors exec()).
            if rc != 0 and await self.status() != "running":
                raise SandboxGoneError(
                    f"docker container {self.container_id[:12]} not runnable after resume")
        if rc != 0 and _container_removed(err) and await self.status() == "missing":
            raise SandboxGoneError(f"docker container {self.container_id[:12]} removed")
        if rc != 0:
            raise RuntimeError(
                f"write_file({path}) failed (rc={rc}): "
                f"{err.decode(errors='replace')[:300]}")

    async def read_file(self, path: str, *, max_bytes: int = 8 * 1024 * 1024) -> bytes:
        self._require_sandbox()
        q = shlex.quote(self._resolve(path))
        # plumbing: env-immune (a session PATH must not break `base64`)
        res = await self.exec(f"base64 < {q}", timeout_s=120,
                              use_default_env=False)
        if res.exit_code != 0:
            raise FileNotFoundError(
                f"read_file({path}) failed (rc={res.exit_code}): "
                f"{res.stderr[:300]}")
        data = base64.b64decode(res.stdout)
        if len(data) > max_bytes:
            raise ValueError(f"read_file({path}): {len(data)}B exceeds "
                             f"max_bytes={max_bytes}")
        return data

    # ── internal ───────────────────────────────────────────────────────────

    def _require_sandbox(self) -> None:
        if not self.container_id:
            raise RuntimeError("transport has no sandbox (lazy compute not "
                               "yet provisioned)")


def _container_not_running(err: bytes) -> bool:
    """True if a docker exec error means the target container is stopped
    (recoverable via resume) — not removed (which says 'no such container')."""
    msg = (err or b"").decode(errors="replace").lower()
    return "is not running" in msg or "is not paused" in msg


def _container_removed(err: bytes) -> bool:
    """True if a docker error means the container is GONE (removed/pruned) —
    resume can't recover it; the session must recreate a fresh sandbox."""
    msg = (err or b"").decode(errors="replace").lower()
    return "no such container" in msg


def _dirname(path: str) -> str:
    i = path.rfind("/")
    return path[:i] if i > 0 else "/"


# ===========================================================================
# DaytonaTransport — the native transport over Daytona's SDK (P1).
# Wraps the existing daytona provider primitives so the NativeSession's
# uniform status/resume/hibernate/destroy flow works unchanged. Daytona
# sandboxes are persistent VMs: hibernate = pause (stop_daytona), resume =
# start_daytona — no container writable-layer semantics, the sandbox FS just
# persists across the pause. Exec/files go over the SDK process + fs channels
# (no supervisor, no ACP). Status vocabulary already matches DockerTransport
# (running|stopped|missing|error) via get_daytona_sandbox_status.
# ===========================================================================

class DaytonaTransport:
    provider = "daytona"

    #: ``start_daytona`` BLOCKS until the VM is ready and RE-RAISES on a failed
    #: start (502 retries exhausted / readiness timeout / stopped→error), so a
    #: clean return already proves the sandbox is running — the post-resume
    #: status() re-check is a redundant control-plane round-trip. (One soft
    #: spot: start_daytona returns silently when the daytona CLIENT can't be
    #: built at all — ImportError/missing key. That config-loss path is
    #: backstopped by exec()'s stopped→resume→retry self-heal, so skipping the
    #: re-check can cost one failed exec there but can never wedge.)
    resume_is_authoritative = True

    def __init__(self, sandbox_ref: str | None = None, workdir: str = "/home/daytona",
                 env: dict[str, str] | None = None):
        self.sandbox_ref = sandbox_ref
        self.workdir = workdir
        # Session env injected into every exec (see DockerTransport.default_env).
        self.default_env = dict(env or {})

    @property
    def ref(self) -> str | None:
        return self.sandbox_ref

    def _inst(self):
        from api.providers import ProviderInstance
        return ProviderInstance(provider="daytona", url="", root=self.workdir,
                                sandbox_ref=self.sandbox_ref)

    def _resolve(self, path: str) -> str:
        if path.startswith("/"):
            return path
        base = self.workdir.rstrip("/") or ""
        return f"{base}/{path}"

    # ── lifecycle ──────────────────────────────────────────────────────────

    async def create(self, *, root: str | None = None,
                     volume_id: str | None = None, subpath: str | None = None) -> str:
        from api.providers.daytona import provision_daytona_sandbox
        inst = await provision_daytona_sandbox(
            agent_type="native", root=root or self.workdir,
            volume_id=volume_id, subpath=subpath)
        if not inst.sandbox_ref:
            raise RuntimeError("daytona provision returned no sandbox_ref")
        self.sandbox_ref = inst.sandbox_ref
        # Readiness gate. If it fails the VM is live but unusable — destroy it
        # before propagating so we don't leak a PAID idle daytona sandbox until
        # the next boot reconcile (mirrors Docker/ModalTransport.create's
        # destroy-before-raise; the ref isn't persisted until create returns, so
        # nothing else would tear it down).
        try:
            # plumbing: env-immune (a session PATH must not break `mkdir`)
            await self.exec(f"mkdir -p {shlex.quote(self.workdir)}", cwd="/",
                            use_default_env=False)
        except BaseException:
            try:
                await self.destroy()
            except Exception:
                log.exception("daytona native: cleanup after readiness failure")
            raise
        return self.sandbox_ref

    async def status(self) -> str:
        from api.providers.daytona import get_daytona_sandbox_status
        if not self.sandbox_ref:
            return "missing"
        return await get_daytona_sandbox_status(self.sandbox_ref)

    async def hibernate(self) -> None:
        from api.providers.daytona import stop_daytona
        if self.sandbox_ref:
            await stop_daytona(self._inst())

    async def resume(self) -> None:
        from api.providers.daytona import start_daytona
        if self.sandbox_ref:
            await start_daytona(self.sandbox_ref)

    async def destroy(self) -> None:
        from api.providers.daytona import destroy_daytona
        if self.sandbox_ref:
            await destroy_daytona(self._inst())

    # ── exec / files ─────────────────────────────────────────────────────────

    async def exec(self, command: str, *, cwd: str | None = None,
                   env: dict[str, str] | None = None,
                   timeout_s: int = DEFAULT_EXEC_TIMEOUT_S,
                   use_default_env: bool = True) -> TransportExecResult:
        if not self.sandbox_ref:
            raise RuntimeError("daytona transport has no sandbox")
        from api.providers.daytona import exec_in_sandbox
        wd = cwd or self.workdir
        merged = {**(self.default_env if use_default_env else {}),
                  **(env or {})}
        # Daytona's process.exec has no cwd/env params plumbed in the helper;
        # wrap with cd + an export preamble (reaches every statement of a
        # compound command; an assignment-prefix would bind only the first
        # simple command and break its lookup under a PATH-class env).
        full = f"cd {shlex.quote(wd)} && {_export_preamble(merged)}{command}"

        async def _run():
            return await exec_in_sandbox(self._inst(), full, timeout=timeout_s)

        # Self-heal an externally-paused sandbox (the reaper, an ops pause):
        # exec against a stopped daytona VM raises; resume the SAME VM and
        # retry once — keeps the session warm, mirrors DockerTransport.exec.
        # A MISSING VM (hard-killed) can't resume → signal the session to
        # recreate (workspace survives on the daytona volume).
        try:
            res = await _run()
        except Exception:
            st = await self.status()
            if st == "stopped":
                await self.resume()
                # Mirror DockerTransport.exec's post-resume escalation
                # (`rc!=0 AND status!=running`): a SUCCESSFUL retry is itself
                # proof the VM is up — return it, never second-guess with a
                # status() that may briefly lag to non-running right after
                # resume (that lag would wrongly recreate a working VM). Only
                # when the retry FAILS do we ask why: if the VM isn't running,
                # resume couldn't restore it (an unrecoverable/broken sandbox)
                # → recreate (volume-safe); if it IS running, the command itself
                # failed on a live VM → propagate as an ordinary error.
                try:
                    res = await _run()
                except Exception:
                    if await self.status() != "running":
                        raise SandboxGoneError(
                            f"daytona sandbox {self.sandbox_ref} not runnable "
                            f"after resume")
                    raise
            elif st == "missing":
                raise SandboxGoneError(f"daytona sandbox {self.sandbox_ref} gone")
            else:
                raise
        return TransportExecResult(res.stdout or "", res.stderr or "",
                                   res.exit_code if res.exit_code is not None else -1,
                                   bool(getattr(res, "timed_out", False)))

    async def write_file(self, path: str, data: bytes) -> None:
        import base64 as _b64
        q = shlex.quote(self._resolve(path))
        qdir = shlex.quote(_dirname(self._resolve(path)))
        b64 = _b64.b64encode(data).decode()
        # base64-over-exec keeps it on the same SDK channel; daytona's exec
        # arg limit is generous, but chunk-free is fine for tool-sized writes.
        # plumbing: env-immune (use_default_env=False).
        res = await self.exec(
            f"mkdir -p {qdir} && printf %s {shlex.quote(b64)} | base64 -d > {q}",
            cwd="/", use_default_env=False)
        if res.exit_code != 0:
            raise RuntimeError(f"daytona write_file({path}) failed: {res.stderr[:300]}")

    async def read_file(self, path: str, *, max_bytes: int = 8 * 1024 * 1024) -> bytes:
        import base64 as _b64
        q = shlex.quote(self._resolve(path))
        # plumbing: env-immune (use_default_env=False)
        res = await self.exec(f"base64 < {q}", cwd="/", use_default_env=False)
        if res.exit_code != 0:
            raise FileNotFoundError(f"daytona read_file({path}): {res.stderr[:300]}")
        return _b64.b64decode(res.stdout)


# ===========================================================================
# ModalTransport — native transport over Modal (P1). Modal's lifecycle is
# fundamentally RECREATE-ON-MISSING, not pause/resume: stop terminates the
# sandbox (start raises), so status reports 'missing' after hibernate and the
# NativeSession's _ensure_sandbox missing-branch recreates a fresh sandbox on
# the SAME volume+subpath (workspace persists on the Modal Volume, not the
# sandbox FS). Modal is therefore ALWAYS volume-backed. Unlike docker/daytona
# it cannot warm-self-heal an externally-terminated sandbox at the transport
# layer (recreate needs the session's volume context), so resume() is a no-op
# here — continuity is the session's recreate-on-missing path.
# ===========================================================================

class ModalTransport:
    provider = "modal"

    #: Modal can't resume (terminate is destructive); resume() is a no-op, so
    #: the post-resume status() re-check correctly finds the sandbox missing and
    #: falls through to recreate-on-volume. Resume is NOT authoritative.
    resume_is_authoritative = False

    def __init__(self, sandbox_ref: str | None = None, workdir: str = "/v",
                 env: dict[str, str] | None = None):
        self.sandbox_ref = sandbox_ref
        self.workdir = workdir
        # Session env injected into every exec (see DockerTransport.default_env).
        self.default_env = dict(env or {})

    @property
    def ref(self) -> str | None:
        return self.sandbox_ref

    def _inst(self):
        from api.providers import ProviderInstance
        return ProviderInstance(provider="modal", url="", root=self.workdir,
                                sandbox_ref=self.sandbox_ref)

    def _resolve(self, path: str) -> str:
        if path.startswith("/"):
            return path
        return f"{self.workdir.rstrip('/')}/{path}"

    async def create(self, *, volume_ref: str, subpath: str,
                     root: str | None = None) -> str:
        # NATIVE flavor: a bare `sleep infinity` sandbox (no supervisor/ACP/
        # tunnel), volume mounted at /v so the workspace survives terminate→
        # recreate. Mirrors DockerTransport's bare container; leaner than the
        # supervisor create (no tunnel + health poll).
        from api.providers.modal import create_bare_sandbox
        inst = await create_bare_sandbox(volume_ref=volume_ref, subpath=subpath,
                                         root=root or self.workdir)
        if not inst.sandbox_ref:
            raise RuntimeError("modal create returned no sandbox_ref")
        self.sandbox_ref = inst.sandbox_ref
        # Readiness gate. If it fails the sandbox is live but unusable — destroy
        # it before propagating so we don't leak it until modal's timeout
        # ceiling (mirrors DockerTransport.create's destroy-before-raise).
        try:
            # plumbing: env-immune (a session PATH must not break `mkdir`)
            await self.exec(f"mkdir -p {shlex.quote(self.workdir)}", cwd="/",
                            use_default_env=False)
        except BaseException:
            try:
                await self.destroy()
            except Exception:
                log.exception("modal native: cleanup after readiness failure")
            raise
        return self.sandbox_ref

    async def status(self) -> str:
        from api.providers.modal import get_sandbox_status
        if not self.sandbox_ref:
            return "missing"
        return await get_sandbox_status(self.sandbox_ref)

    async def hibernate(self) -> None:
        from api.providers.modal import stop_sandbox
        if self.sandbox_ref:
            await stop_sandbox(self._inst())

    async def resume(self) -> None:
        # Modal can't resume — the session recreates on missing. No-op so the
        # uniform _ensure_sandbox flow never calls a raising start.
        return None

    async def destroy(self) -> None:
        from api.providers.modal import stop_sandbox  # terminate == destroy
        if self.sandbox_ref:
            await stop_sandbox(self._inst())

    async def exec(self, command: str, *, cwd: str | None = None,
                   env: dict[str, str] | None = None,
                   timeout_s: int = DEFAULT_EXEC_TIMEOUT_S,
                   use_default_env: bool = True) -> TransportExecResult:
        if not self.sandbox_ref:
            raise RuntimeError("modal transport has no sandbox")
        from api.providers._shared import SandboxMissingError
        from api.providers.modal import exec_in_sandbox
        wd = cwd or self.workdir
        merged = {**(self.default_env if use_default_env else {}),
                  **(env or {})}
        # export preamble (not assignment-prefix) — see _export_preamble.
        full = f"cd {shlex.quote(wd)} && {_export_preamble(merged)}{command}"
        try:
            res = await exec_in_sandbox(self._inst(), full, timeout=timeout_s)
        except SandboxMissingError as e:
            # Sandbox terminated out from under us (routine: modal's hard
            # timeout ceiling reaps even active sandboxes). Signal the session
            # to recreate on the same Volume — the workspace survives there.
            raise SandboxGoneError(f"modal sandbox {self.sandbox_ref} gone") from e
        return TransportExecResult(res.stdout or "", res.stderr or "",
                                   res.exit_code if res.exit_code is not None else -1,
                                   bool(getattr(res, "timed_out", False)))

    async def write_file(self, path: str, data: bytes) -> None:
        import base64 as _b64
        q = shlex.quote(self._resolve(path))
        qdir = shlex.quote(_dirname(self._resolve(path)))
        b64 = _b64.b64encode(data).decode()
        # plumbing: env-immune (use_default_env=False)
        res = await self.exec(
            f"mkdir -p {qdir} && printf %s {shlex.quote(b64)} | base64 -d > {q}",
            cwd="/", use_default_env=False)
        if res.exit_code != 0:
            raise RuntimeError(f"modal write_file({path}) failed: {res.stderr[:300]}")

    async def read_file(self, path: str, *, max_bytes: int = 8 * 1024 * 1024) -> bytes:
        import base64 as _b64
        q = shlex.quote(self._resolve(path))
        # plumbing: env-immune (use_default_env=False)
        res = await self.exec(f"base64 < {q}", cwd="/", use_default_env=False)
        if res.exit_code != 0:
            raise FileNotFoundError(f"modal read_file({path}): {res.stderr[:300]}")
        return _b64.b64decode(res.stdout)


# ===========================================================================
# UnixLocalTransport — native transport over host processes (dev provider).
# unix_local has NO compute object at rest: native execs are per-call host
# subprocesses that exit when done, so between turns a session holds nothing
# a hibernate could free. The lifecycle is therefore RECORD-ONLY — a json
# record in the unix_local provider's existing _SandboxRecord index (the
# same registry the supervisor path uses; supervisor_js="native", pid=0 as
# the discriminator) carries identity (stable sandbox_ref), the workspace
# path, and destroyed-out-of-band detection:
#   status     record exists → 'running'; gone → 'missing'. NEVER 'stopped'.
#   hibernate  no-op — the pool evicting the session already frees the only
#              resource (server memory). Reap cost ~0ms: trivially at least
#              as efficient as the supervisor path (which kills a node
#              supervisor + ACP child it keeps resident).
#   resume     no-op (nothing was stopped); unreachable from _ensure_sandbox
#              since status never reads 'stopped'.
#   destroy    clear the record → 'missing'; exec on a cleared record raises
#              SandboxGoneError so the session recreates (workspace files
#              persist on disk — destroy never deletes user data).
# ===========================================================================


class UnixLocalTransport:
    provider = "unix_local"

    #: resume() is an unfailable no-op (and unreachable: status() never
    #: returns 'stopped'), so the post-resume re-check would be meaningless.
    resume_is_authoritative = True

    def __init__(self, sandbox_ref: str | None = None, workdir: str = "/tmp",
                 env: dict[str, str] | None = None):
        self.sandbox_ref = sandbox_ref
        self.workdir = workdir
        # Session env injected into every exec (see DockerTransport.default_env).
        self.default_env = dict(env or {})

    @property
    def ref(self) -> str | None:
        return self.sandbox_ref

    def _resolve(self, path: str) -> str:
        if path.startswith("/"):
            return path
        base = self.workdir.rstrip("/") or ""
        return f"{base}/{path}"

    @staticmethod
    def _load(ref: str):
        from api.providers.unix_local import _load_record
        return _load_record(ref)

    # ── lifecycle ──────────────────────────────────────────────────────────

    async def create(self, *, root: str | None = None, **_kw) -> str:
        import os as _os
        import uuid as _uuid
        from pathlib import Path as _Path

        from api.providers.unix_local import (
            _index_dir,
            _SandboxRecord,
            _write_record,
        )
        workspace = root or self.workdir
        ref = f"native-local-{_uuid.uuid4().hex[:12]}"
        record = _SandboxRecord(
            ref=ref, pid=0, port=0, node="", supervisor_js="native",
            acp_bin="", effective_root=workspace,
            base_env={"AGENT_SDK_ORIGIN":
                      _os.environ.get("AGENT_SDK_ORIGIN", "production")},
        )

        def _provision() -> None:
            _Path(workspace).mkdir(parents=True, exist_ok=True)
            # marker == index path: native has no per-volume marker dir, so
            # both _write_record targets coincide (idempotent double write).
            _write_record(_index_dir() / f"{ref}.json", record)
        await asyncio.to_thread(_provision)
        self.sandbox_ref = ref
        return ref

    async def status(self) -> str:
        if not self.sandbox_ref:
            return "missing"
        _, record = await asyncio.to_thread(self._load, self.sandbox_ref)
        return "running" if record is not None else "missing"

    async def hibernate(self) -> None:
        """No-op: nothing runs between execs, so there is nothing to free.
        The reap's real effect is the POOL evicting the session object."""
        return None

    async def resume(self) -> None:
        """No-op: nothing was stopped. Unreachable in practice — status()
        never returns 'stopped' for a record-only sandbox."""
        return None

    async def destroy(self) -> None:
        from api.providers.unix_local import _clear_record
        if not self.sandbox_ref:
            return
        marker, record = await asyncio.to_thread(self._load, self.sandbox_ref)
        await asyncio.to_thread(_clear_record, self.sandbox_ref, marker, record)

    # ── exec / files ────────────────────────────────────────────────────────

    async def exec(self, command: str, *, cwd: str | None = None,
                   env: dict[str, str] | None = None,
                   timeout_s: int = DEFAULT_EXEC_TIMEOUT_S,
                   use_default_env: bool = True) -> TransportExecResult:
        """Run the command as a host subprocess. A missing RECORD means the
        sandbox was destroyed out-of-band → SandboxGoneError (the session
        recreates on the same workspace)."""
        import os as _os
        import signal as _signal
        if not self.sandbox_ref:
            raise RuntimeError("local transport has no sandbox")
        _, record = await asyncio.to_thread(self._load, self.sandbox_ref)
        if record is None:
            raise SandboxGoneError(
                f"local native sandbox {self.sandbox_ref} record gone")
        merged = {**(self.default_env if use_default_env else {}),
                  **(env or {})}
        # Env rides execve directly (no shell preamble needed): /bin/sh is
        # resolved by absolute path, so even a PATH-class session env cannot
        # break the spawn — it only alters lookup INSIDE the user command,
        # the same semantics the container transports get via the preamble.
        t = max(1, int(timeout_s))
        proc = await asyncio.create_subprocess_exec(
            "/bin/sh", "-c", command,
            cwd=cwd or record.effective_root or self.workdir,
            env={**_os.environ, **merged},
            start_new_session=True,   # own pgroup → timeout kill reaps the tree
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            out, err = await asyncio.wait_for(proc.communicate(), timeout=t)
        except asyncio.TimeoutError:
            try:
                _os.killpg(proc.pid, _signal.SIGKILL)
            except ProcessLookupError:
                pass
            try:
                await proc.wait()
            except Exception:
                pass
            return TransportExecResult("", f"timed out after {t}s", 124, True)
        stdout, _tr = _truncate(out or b"", _MAX_OUTPUT_BYTES)
        stderr, _tr = _truncate(err or b"", _MAX_OUTPUT_BYTES)
        return TransportExecResult(stdout, stderr, proc.returncode or 0, False)

    async def write_file(self, path: str, data: bytes) -> None:
        from pathlib import Path as _Path
        p = _Path(self._resolve(path))

        def _write() -> None:
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_bytes(data)
        await asyncio.to_thread(_write)

    async def read_file(self, path: str, *, max_bytes: int = 8 * 1024 * 1024) -> bytes:
        from pathlib import Path as _Path
        p = _Path(self._resolve(path))
        data = await asyncio.to_thread(p.read_bytes)   # FileNotFoundError as-is
        if len(data) > max_bytes:
            raise ValueError(f"read_file({path}): {len(data)}B exceeds "
                             f"max_bytes={max_bytes}")
        return data
