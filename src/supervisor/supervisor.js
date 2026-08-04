#!/usr/bin/env node
/**
 * Minimal ACP supervisor — spawns claude-agent-acp, bridges stdio to a
 * small HTTP surface for ACP JSON-RPC:
 *
 *   POST /v1/acp/{session_id}   body is a JSON-RPC frame. We write it to
 *                                acp stdin. Requests (with `id`) wait for
 *                                the matching response and return it as
 *                                application/json. Notifications (no `id`)
 *                                fire-and-forget with 202.
 *   GET  /v1/acp/{session_id}   SSE stream of every line emitted by
 *                                claude-agent-acp's stdout, formatted as
 *                                `data: {json}\n\n` blocks.
 *   POST /v1/exec               run a shell command — {command, timeout?}
 *                                returns {stdout, stderr, exit_code, timed_out}
 *   GET  /v1/health             liveness — {status, acp_pid, acp_alive}
 *
 * One claude-agent-acp subprocess per supervisor, shared across all
 * POSTs and SSE subscribers. The session_id in the URL is accepted but
 * ignored — the inner ACP session id goes in JSON-RPC params as usual.
 */
const http = require("node:http");
const { spawn, spawnSync } = require("node:child_process");
const fs = require("node:fs");
const path = require("node:path");
const SSE_HEARTBEAT_MS = 25000;
const LOG_SLICE = 400;
// Drop an SSE subscriber once Node has buffered this many bytes for it
// without the socket draining. Without this, a slow / paused / dead-but-
// not-yet-RST consumer accumulates every ACP stdout line in the
// supervisor's V8 heap until OOM. 8 MB ≈ a full session/prompt of dense
// tool-use chatter; well below any sane sandbox memory ceiling.
const SSE_BACKPRESSURE_LIMIT_BYTES = 8 * 1024 * 1024;
// Defensive cap on pendingPromptIds: each entry is just an id string,
// but if ACP ever drops a session/prompt response (child crash mid-turn,
// malformed frame), the entry would leak forever and grow unbounded.
// FIFO-evict at the cap. We keep cleanup OUT of the request lifecycle
// because we still want a turn-end snapshot even if the client gave up.
const MAX_PENDING_PROMPT_IDS = 1024;

// Caller-owned credential caches arrive through the existing ``secrets``
// channel as environment variables. Materialize them before ACP starts, then
// remove the raw payload from the supervisor environment so neither the ACP
// child nor /v1/exec inherits it. The marker lets a runtime refresh its own
// cache without an unchanged bootstrap payload overwriting the refreshed file
// on every supervisor restart; changing the supplied payload replaces it.
const SECRET_FILE_BOOTSTRAPS = [
  {
    env: "CODEX_AUTH_JSON",
    relativePath: ".codex/auth.json",
    validate(value) {
      const parsed = JSON.parse(value);
      if (parsed === null || Array.isArray(parsed) || typeof parsed !== "object") {
        throw new Error("credential cache must be a JSON object");
      }
    },
  },
];

// Paths under args.root that are rebuildable or purely ephemeral. Excluded
// from snapshots so we don't round-trip hundreds of MB of node_modules
// through S3 on every turn.
const SNAPSHOT_EXCLUDES = [
  "--exclude=./node_modules",
  "--exclude=./.cache",
  "--exclude=./.npm",
  "--exclude=./.claude/shell-snapshots",
  "--exclude=./.claude/statsig",
];

// Cold-tier tarballs are zstd-1 compressed when the binary is on PATH;
// otherwise we fall back to uncompressed (current behaviour) so supervisor
// keeps working on runtime images that haven't yet bundled zstd.
//
// Backward compat: tar -xf autodetects compression by magic bytes, so old
// uncompressed snapshot.tar files in S3 stay readable through the restore
// path with no flag-day coordination. New writes produce zstd content at
// the same filename — no rename, no migration script.
//
// Bench (1 vCPU, 1 GB workspace): zstd-1 cuts artifact size ~10× with the
// same wall-clock as uncompressed (cp time saved ≈ compression CPU spent).
// gzip -1 was ~95% slower at 1 GB on a single core, so we don't fall back
// to gzip — uncompressed-or-zstd, nothing in between.
const ZSTD_AVAILABLE = (() => {
  try { return spawnSync("zstd", ["--version"], { stdio: "ignore" }).status === 0; }
  catch { return false; }
})();
const TAR_COMPRESS_ARGS = ZSTD_AVAILABLE ? ["-I", "zstd -1"] : [];

// Two-tier snapshot layout.
//
//  - filesystem_cache.tar: full HOME tarball. Heavy (workspace files,
//    user scratch, etc.). Written only on lifecycle events
//    (/hibernate, /delete, idle reap, graceful SIGTERM, explicit
//    POST /v1/snapshot). Old name: "snapshot.tar".
//
//  - agent_memory.tar: small tarball of the per-agent session-state
//    dirs listed below. Written after every turn (before the HTTP
//    response returns, so the invariant "once client sees turn done,
//    the JSONL is durable" still holds). These dirs are where
//    agents store session continuity (Claude Code: ~/.claude/projects/
//    contains the JSONLs session/load reads, ~/.claude/todos/ etc.).
//    Missing dirs are tar-skipped via --ignore-failed-read, so we can
//    list all supported agent types' dirs unconditionally — no branch
//    on the active agent_type.
//
//  - On restore, extract filesystem_cache.tar first (base) then
//    agent_memory.tar (overlay), so the latest session state wins.
const AGENT_MEMORY_DIRS = [
  ".claude",
  ".codex",
  ".opencode",
  // OpenCode is XDG-compliant: actual session storage (the SQLite DB,
  // session_diff/ session_message/ JSONs) lives under XDG_DATA_HOME, not
  // ~/.opencode. Without these, session/load on a respawned sandbox
  // finds nothing to load and opencode silently no-ops.
  ".local/share/opencode",
  ".local/state/opencode",
  ".gemini",
  ".cline",
  ".deepagents",
  ".openhands",
  ".config/goose",
];

function _agentMemoryPath(snapshotPath) {
  if (!snapshotPath) return null;
  return path.join(path.dirname(snapshotPath), "agent_memory.tar");
}

function parseArgs(argv) {
  const out = {
    port: 9100,
    acp: null,
    host: "0.0.0.0",
    root: "/tmp",
    snapshotPath: null,
    skipRestore: false,
    acpArgs: [],
  };
  for (let i = 2; i < argv.length; i++) {
    const a = argv[i];
    if (a === "--port") out.port = parseInt(argv[++i], 10);
    else if (a === "--host") out.host = argv[++i];
    else if (a === "--acp") out.acp = argv[++i];
    else if (a === "--root" || a === "--cwd") out.root = argv[++i];
    else if (a === "--snapshot-path") out.snapshotPath = argv[++i];
    // First cold-create: nothing on the volume to restore, so skip the
    // boot-time restore poll. ``--snapshot-path`` is still passed (per-turn
    // agent_memory.tar writes depend on it), only the read tier is skipped.
    else if (a === "--skip-restore") out.skipRestore = true;
    else if (a === "--acp-arg") out.acpArgs.push(argv[++i]);
  }
  if (!out.acp) {
    console.error("--acp required");
    process.exit(1);
  }
  return out;
}

function log(...args) {
  const t = new Date().toISOString();
  process.stderr.write(`${t} [supervisor] ${args.join(" ")}\n`);
}

function materializeSecretFiles(root) {
  const crypto = require("node:crypto");
  for (const bootstrap of SECRET_FILE_BOOTSTRAPS) {
    const value = process.env[bootstrap.env];
    if (!value) continue;

    // Delete first: validation or filesystem failures must never leave the
    // credential available to a subsequently spawned child.
    delete process.env[bootstrap.env];
    bootstrap.validate(value);

    const target = path.join(root, bootstrap.relativePath);
    const dir = path.dirname(target);
    const marker = `${target}.agent-sdk-bootstrap-sha256`;
    const digest = crypto.createHash("sha256").update(value).digest("hex");
    let alreadyBootstrapped = false;
    try {
      alreadyBootstrapped = fs.existsSync(target)
        && fs.readFileSync(marker, "utf8").trim() === digest;
    } catch {
      alreadyBootstrapped = false;
    }
    if (alreadyBootstrapped) continue;

    fs.mkdirSync(dir, { recursive: true, mode: 0o700 });
    fs.chmodSync(dir, 0o700);
    const staged = `${target}.tmp-${process.pid}`;
    try {
      fs.writeFileSync(staged, value, { mode: 0o600 });
      fs.renameSync(staged, target);
      fs.chmodSync(target, 0o600);
      fs.writeFileSync(marker, `${digest}\n`, { mode: 0o600 });
    } finally {
      try { fs.unlinkSync(staged); } catch {}
    }
    log(`materialized credential cache ${bootstrap.relativePath}`);
  }
}

// ── Boot constants (evaluated at module load, before any helpers run) ──────
//
// Two boot modes (Type 1 / Type 2 — see server.py _type2_recover):
//   Type 1 — supervisor restart inside an EXISTING VM. args.root on local
//             ext4 already has the latest workspace bytes; restoring from
//             volume tarballs is pure waste (potentially hundreds of MB) and
//             adds 15 s of FUSE-poll wait.
//   Type 2 — fresh VM, blank args.root. Volume tarballs are the only way to
//             repopulate session + workspace state.
// Sentinel at SUPERVISOR_BOOT_MARKER distinguishes them: /tmp survives
// Type 1 (same VM), is wiped on Type 2 (new VM). Cheaper than a server-side
// --fresh arg; doesn't rely on Daytona's image-level dotfile pre-population.
const SUPERVISOR_BOOT_MARKER = "/tmp/agent-sdk-bootstrapped";
const isWarmRestart = (() => {
  try { return fs.existsSync(SUPERVISOR_BOOT_MARKER); }
  catch { return false; }
})();

// S3-backed FUSE (Daytona) has write→read visibility lag — often 5-15s
// under load. When a sandbox is replaced immediately after an external
// daytona.delete, the new sandbox can start before the previous one's
// snapshot.tar is visible on the new mount. A single existsSync() check
// would miss it and skip the restore, silently losing turn-1 conversation
// state. Poll for up to 15s — cheap when the file truly doesn't exist
// (fresh sandbox: each poll is one FUSE stat), recovers the post-delete
// case within typical S3 propagation windows. Also do an explicit `ls` on
// the parent dir before stating to invalidate any stale FUSE dentry cache.
function _snapshotVisible(path, timeoutMs) {
  const deadline = Date.now() + timeoutMs;
  const parent = path.replace(/\/[^\/]+$/, "") || "/";
  let first = true;
  while (Date.now() < deadline) {
    // Force FUSE to refresh the parent's dir listing — mountpoint-s3 caches
    // readdir results and existsSync alone may return false for a file that
    // just appeared on the backing S3 bucket.
    try { spawnSync("ls", [parent], { stdio: "ignore" }); } catch {}
    try {
      if (fs.existsSync(path)) {
        if (!first) {
          log(`snapshot became visible after poll (${Date.now() - (deadline - timeoutMs)}ms)`);
        }
        return true;
      }
    } catch {}
    first = false;
    spawnSync("sleep", ["0.25"]);
  }
  return false;
}

// ── Boot helpers ────────────────────────────────────────────────────────────

function ensureRootDir(root) {
  try {
    fs.mkdirSync(root, { recursive: true });
  } catch (e) {
    log(`mkdir root failed: ${e.message}`);
  }
}

// Extract a tarball into root. Two modes controlled by skipOnCorrupt:
//
//   skipOnCorrupt=false  (cold full-HOME tier): extract directly into root.
//                        Best-effort — a non-zero rc is logged and we carry on.
//
//   skipOnCorrupt=true   (per-turn agent_memory overlay): extract to a staging
//                        dir first, apply onto root ONLY on a clean (rc==0)
//                        extract, then remove staging. A sandbox killed
//                        mid-snapshot-write leaves a TRUNCATED agent_memory.tar
//                        (mountpoint-s3 can't make the write atomic — rename(2)
//                        is ENOSYS). Extracting a truncated tar straight into
//                        $HOME lands a partial/corrupt opencode SQLite db and
//                        the agent crash-loops on recovery (opencode db-open:
//                        "PRAGMA journal_mode=WAL" → health-fail → reattach →
//                        cold-create loop). Dropping the corrupt overlay boots
//                        from filesystem_cache/fresh instead — worst case is
//                        losing the last turn, not an infinite leaked-VM loop.
//                        (Regression: test_corrupt_agent_memory_recovers_not_loops.)
function _restoreTar(tarPath, root, skipOnCorrupt, label) {
  if (!skipOnCorrupt) {
    log(`restoring ${label} from ${tarPath}`);
    const r = spawnSync("tar", ["-xf", tarPath, "-C", root], {
      stdio: ["ignore", "inherit", "inherit"],
    });
    if (r.status !== 0) log(`${label} restore exited rc=${r.status}; continuing`);
    return;
  }
  // skipOnCorrupt path: stage → validate → apply
  const stage = "/tmp/agent-memory-restore";
  spawnSync("rm", ["-rf", stage]);
  fs.mkdirSync(stage, { recursive: true });
  const r = spawnSync("tar", ["-xf", tarPath, "-C", stage], {
    stdio: ["ignore", "inherit", "inherit"],
  });
  if (r.status === 0) {
    log(`restoring ${label} from ${tarPath}`);
    const cp = spawnSync("cp", ["-a", `${stage}/.`, root], {
      stdio: ["ignore", "inherit", "inherit"],
    });
    if (cp.status !== 0) log(`${label} overlay apply rc=${cp.status}; continuing`);
  } else {
    log(`${label} ${tarPath} is corrupt (extract rc=${r.status}) — SKIPPING overlay; booting from filesystem_cache/fresh`);
  }
  spawnSync("rm", ["-rf", stage]);
}

// Restore the workspace tarballs onto args.root (Type 2 boot only).
//
// Type 1 (warm=true): local ext4 already holds the latest workspace bytes
//   from the previous supervisor in this VM — skip both restore tiers.
// --skip-restore (first cold-create): no tarball on the volume yet — skip
//   the 2×15 s FUSE-visibility poll. Per-turn writes are unaffected
//   (they key off snapshotPath), so the NEXT boot on a fresh VM restores
//   normally.
// Type 2 cold (warm=false, skipRestore=false):
//   Layer 1 — full HOME (filesystem_cache.tar). Best-effort; fresh agents
//     that have never snapshotted don't have this and it's fine.
//   Layer 2 — agent_memory overlay (per-turn snapshot of session dirs).
//     Carries conversation JSONLs (.claude, .codex, etc.). Written EVERY
//     TURN vs. filesystem_cache which is only written on graceful shutdown.
//     For the common Type 2 case (sandbox deleted between turns under
//     concurrent load), cold is often missing while memory IS present.
//     Polling memory is what carries the conversation forward; without it,
//     claude-agent-acp's session/load returns -32603 forever, and the
//     test_session_resume_after_delete/stop[daytona] invariants catch the
//     silent context loss. 15 s budget: ``ls $parent`` invalidates
//     mountpoint-s3's stale dentry cache so existsSync sees the file once
//     S3 propagates (10 s was too tight under 2× concurrent load).
//     Server-side _wait_for_health (45 s) covers worst-case
//     15 + 15 + ACP-spawn on a fresh Type 2 boot.
function restoreWorkspace(args, warm) {
  if (!args.snapshotPath) return;
  if (warm) {
    log(`Type 1 boot detected (sentinel ${SUPERVISOR_BOOT_MARKER} present); skipping snapshot restore`);
    return;
  }
  if (args.skipRestore) {
    log(`first cold-create (--skip-restore); skipping snapshot restore`);
    return;
  }
  // Layer 1: cold restore (full HOME).
  if (_snapshotVisible(args.snapshotPath, 15000)) {
    _restoreTar(args.snapshotPath, args.root, false, "filesystem_cache");
  } else {
    log(`filesystem_cache ${args.snapshotPath} not visible after 15s — assuming fresh sandbox`);
  }
  // Layer 2: agent-memory overlay.
  const memPath = _agentMemoryPath(args.snapshotPath);
  if (memPath && _snapshotVisible(memPath, 15000)) {
    _restoreTar(memPath, args.root, true, "agent_memory");
  } else if (memPath) {
    log(`agent_memory ${memPath} not visible after 15s — skipping overlay restore`);
  }
}

// Write the boot sentinel so the next supervisor start inside this VM is
// detected as Type 1 (warm restart). /tmp is wiped on a new VM (Type 2),
// so the sentinel correctly disappears there.
function markBooted() {
  try {
    fs.writeFileSync(SUPERVISOR_BOOT_MARKER, String(Date.now()));
  } catch (e) {
    log(`failed to write boot sentinel ${SUPERVISOR_BOOT_MARKER}: ${e.message}`);
  }
}

// Spawn the ACP child. HOME is set to args.root so Claude Code's
// ~/.claude/projects/... JSONLs land inside the workspace we just restored
// (and therefore get captured by the next snapshot). PATH is widened to
// include $HOME/.local/bin so ``uv tool install`` binaries (hivespace CLI
// etc.) are invocable without a full path. Mirrors the /v1/exec env.
function spawnAgent(args) {
  const child = spawn(args.acp, args.acpArgs, {
    stdio: ["pipe", "pipe", "pipe"],
    env: { ...process.env, HOME: args.root, PATH: _spawnPath },
    cwd: args.root,
  });
  log("spawned acp pid=" + child.pid);
  log(`snapshot compression: ${ZSTD_AVAILABLE
    ? "zstd-1 (artifact ~10× smaller; restore autodetects)"
    : "none — zstd not on PATH (safe fallback; bundle zstd in runtime image to enable)"}`);
  child.stderr.on("data", (chunk) => {
    process.stderr.write("[acp-stderr] " + chunk.toString());
  });
  child.on("exit", (code, signal) => {
    log(`acp exited code=${code} signal=${signal}`);
    process.exit(code || 1);
  });
  return child;
}

// ── Boot sequence ────────────────────────────────────────────────────────────
const args = parseArgs(process.argv);
// PATH widened with $HOME/.local/bin so uv-tool-installed binaries (hivespace
// CLI etc.) are invocable without a full path. Module-level: shared by the ACP
// child (spawnAgent) AND /v1/exec (handleExec).
const _spawnPath = `${path.join(args.root, ".local/bin")}:${process.env.PATH || ""}`;
ensureRootDir(args.root);
restoreWorkspace(args, isWarmRestart);
materializeSecretFiles(args.root);
markBooted();
const acp = spawnAgent(args);

// Pending POST response resolvers, keyed by rpc id. POST handlers waiting
// for a specific response register here; the stdout reader resolves them.
const pendingResponses = new Map();

// JSON-RPC ids of in-flight session/prompt requests. When we see a matching
// response from the ACP child, the agent's turn is done — kick a snapshot.
const pendingPromptIds = new Set();

// SSE subscribers — every line of acp stdout is fanned out to these.
const sseSubscribers = new Set();

// ── Workspace snapshot machinery ──
//
// We persist args.root to args.snapshotPath on every session/prompt
// turn-end so a freshly provisioned sandbox (e.g., Daytona sandbox
// deleted and replaced) can restore from the last completed turn.
//
// Synchronous w.r.t. the HTTP response: the supervisor delays writing
// the session/prompt response back to the HTTP caller until the
// snapshot has been committed to the volume. Once the client sees
// "turn done" over the wire, the sandbox can be deleted without
// losing the turn — the snapshot is already durable on S3. This is
// the critical correctness boundary; previously the snapshot ran
// async and could lose races against ``daytona.delete()``.
//
// Local staging dir for the tarball. tar's write goes to a local ext4
// filesystem (fast), then a single ``cp`` writes the finished tarball to
// the volume (one S3 PUT). Writing tar directly to the volume path would
// be two sequential S3 PUTs in the worst case — slower AND in the critical
// window between turn-end and a potential ``daytona.delete`` — so we keep
// the staging off-volume.
const LOCAL_SNAPSHOT_STAGING = "/tmp/agent-sdk-snapshot.tar";
const LOCAL_MEMORY_STAGING = "/tmp/agent-sdk-agent-memory.tar";
// ACP lines are handled fire-and-forget (acp.stdout 'data' does not await
// handleAcpLine), so two prompt-responses can drive two runAgentMemorySnapshotOnce
// concurrently. A FIXED staging path would let one run's tar truncate the other's
// cp source -> a partial tarball on the volume (and a TOCTOU hole past the tar -t
// validation). Give every run a UNIQUE stage so concurrent runs never share one;
// the concurrent cp's to the single volume path are each atomic on mountpoint-s3.
let _memStageSeq = 0;

// NOTE on the truncated agent_memory.tar (the prod crash-loop root cause).
// Verified empirically on the real volume: it is NOT the cp-to-volume step.
// The daytona volume is mountpoint-s3 (S3-FUSE), where the S3 object is
// published all-or-nothing at close — a process-killed ``cp`` leaves the OLD
// object intact, and ``rename(2)`` is ENOSYS so a tmp+rename publish can't be
// atomic anyway. The truncation comes from the STAGING ``tar`` failing
// mid-write (e.g. /tmp disk pressure -> SIGXFSZ, or an I/O error on the small
// sandbox): it leaves a PARTIAL stage, and the publish below used to copy that
// partial to the volume. Demonstrated: a ulimit-capped tar (rc=153) -> a 1 MB
// partial stage -> cp -> /vol/agent_memory.tar with ``tar -tf`` rc=2
// ("premature EOF"), exactly the prod symptom. So the WRITE-side fix VALIDATES
// the staged archive with ``tar -t`` and publishes only a complete one (below;
// a benign tar rc=1 "file changed as we read it" still lists fine and IS
// published), and the RESTORE side independently SKIPS a corrupt overlay
// (extract rc!=0) as defense-in-depth (see the Type 2 restore above +
// test_corrupt_agent_memory_recovers_not_loops).

// Per-turn snapshot: tar only AGENT_MEMORY_DIRS. Uses
// --ignore-failed-read so dirs not present for the active agent_type
// are silently skipped (the same tar command works for any agent).
// Result is written to <dir(snapshotPath)>/agent_memory.tar.
// Shared tar→validate→cp pipeline (async). Spawns `tar tarArgs` into
// `stage`, validates the staged archive with `tar -t`, then copies to
// `dest`. On any failure the stage is cleaned up and the previous
// dest is left untouched (keeps the previous overlay safe). `label`
// prefixes all log messages (e.g. "agent_memory", "snapshot").
function _publishTarAsync(tarArgs, stage, dest, label) {
  return new Promise((resolve) => {
    const tar = spawn("tar", tarArgs, { stdio: ["ignore", "ignore", "pipe"] });
    let tarErr = "";
    tar.stderr.on("data", (c) => { tarErr += c.toString("utf8"); });
    tar.on("error", (e) => {
      log(`${label} tar spawn error: ${e.message}`);
      try { fs.unlinkSync(stage); } catch {}
      resolve();
    });
    tar.on("close", (tarCode) => {
      if (tarCode !== 0) {
        log(`${label} tar rc=${tarCode}: ${tarErr.slice(0, LOG_SLICE)}`);
      }
      const stageOk = spawnSync("tar", ["-tf", stage], { stdio: "ignore" }).status === 0;
      if (!stageOk) {
        log(`${label} stage corrupt/truncated (tar rc=${tarCode}) — SKIPPING publish (keeps previous overlay)`);
        try { fs.unlinkSync(stage); } catch {}
        resolve();
        return;
      }
      const cp = spawn("cp", [stage, dest], { stdio: ["ignore", "ignore", "pipe"] });
      let cpErr = "";
      cp.stderr.on("data", (c) => { cpErr += c.toString("utf8"); });
      cp.on("error", (e) => {
        log(`${label} cp spawn error: ${e.message}`);
        try { fs.unlinkSync(stage); } catch {}
        resolve();
      });
      cp.on("close", (cpCode) => {
        try { fs.unlinkSync(stage); } catch {}
        if (cpCode !== 0) {
          log(`${label} cp rc=${cpCode}: ${cpErr.slice(0, LOG_SLICE)}`);
        }
        resolve();
      });
    });
  });
}

// Synchronous variant used by graceful shutdown.
function _publishTarSync(tarArgs, stage, dest, label) {
  try {
    const tr = spawnSync("tar", tarArgs, { stdio: ["ignore", "ignore", "pipe"] });
    if (tr.status !== 0) {
      log(`${label} tar rc=${tr.status}: ${String(tr.stderr || "").slice(0, LOG_SLICE)}`);
    }
    // Gate on archive completeness (tar -t), not the exit code — a benign rc=1
    // ("file changed as we read it") still yields a complete archive we SHOULD
    // publish; only a truncated archive is skipped. Matches _publishTarAsync.
    const stageOk = spawnSync("tar", ["-tf", stage], { stdio: "ignore" }).status === 0;
    if (!stageOk) {
      log(`${label} stage corrupt/truncated (tar rc=${tr.status}) — SKIPPING publish (keeps previous overlay)`);
      try { fs.unlinkSync(stage); } catch {}
      return;
    }
    const cr = spawnSync("cp", [stage, dest], { stdio: ["ignore", "ignore", "pipe"] });
    if (cr.status !== 0) {
      log(`${label} cp rc=${cr.status}: ${String(cr.stderr || "").slice(0, LOG_SLICE)}`);
    }
  } catch (e) {
    log(`${label} snapshot failed: ${e.message}`);
  } finally {
    try { fs.unlinkSync(stage); } catch {}
  }
}

function runAgentMemorySnapshotOnce() {
  const memPath = _agentMemoryPath(args.snapshotPath);
  if (!memPath) return Promise.resolve();
  const stage = `${LOCAL_MEMORY_STAGING}.${_memStageSeq++}`;
  const tarArgs = [
    ...TAR_COMPRESS_ARGS,
    "-cf", stage,
    "--ignore-failed-read",
    "-C", args.root,
    ...AGENT_MEMORY_DIRS,
  ];
  return _publishTarAsync(tarArgs, stage, memPath, "agent_memory");
}

function runSnapshotOnce() {
  if (!args.snapshotPath) return Promise.resolve();
  const stage = LOCAL_SNAPSHOT_STAGING;
  const tarArgs = [...TAR_COMPRESS_ARGS, "-cf", stage, ...SNAPSHOT_EXCLUDES, "-C", args.root, "."];
  return _publishTarAsync(tarArgs, stage, args.snapshotPath, "snapshot");
}

// Synchronous snapshot for graceful shutdown (SIGTERM/SIGINT). Caller waits
// for it so the last turn reliably lands on the volume before the process
// exits.
function runSnapshotSync() {
  if (!args.snapshotPath) return;
  const stage = LOCAL_SNAPSHOT_STAGING;
  const tarArgs = [...TAR_COMPRESS_ARGS, "-cf", stage, ...SNAPSHOT_EXCLUDES, "-C", args.root, "."];
  _publishTarSync(tarArgs, stage, args.snapshotPath, "snapshot");
}

// HTTP handler for POST /v1/snapshot. Wraps runSnapshotOnce so the server
// can trigger a synchronous snapshot before it stops/destroys the sandbox.
// Idempotent; safe to call repeatedly. Returns 200 after the tarball has
// landed on the volume (or immediately if --snapshot-path was not configured,
// in which case runSnapshotOnce is a no-op).
async function handleSnapshot(req, res) {
  try {
    await runSnapshotOnce();
    sendJson(res, 200, { ok: true });
  } catch (e) {
    log(`snapshot endpoint error: ${e.message}`);
    sendJson(res, 500, { error: String(e.message || e) });
  }
}

// Cache the last available_commands_update so late SSE subscribers receive it.
let lastCommandsEvent = null;

// Coalesce consecutive text/reasoning chunks before they hit the SSE wire.
// claude-agent-acp emits ~1 char per ``agent_message_delta`` — at high
// concurrency this is the dominant Python-side parse+fanout load. Buffer
// chunks for FLUSH_MS or until a non-coalescable event arrives (tool call,
// done, error, usage), then emit ONE synthesized session/update message
// with the concatenated text. The downstream parser (api/sse.py
// ``classify_message_content``) is shape-agnostic about chunk size, so the
// coalesced message is observed identically by every existing consumer.
//
// FLUSH_MS = 40 picked from the gap between Claude's typical 25-30 chunks/s
// and a UI cursor refresh rate where "appears responsive" plateaus. Tune
// via AGENT_SDK_SUPERVISOR_FLUSH_MS.
const COALESCE_FLUSH_MS = (() => {
  const v = parseInt(process.env.AGENT_SDK_SUPERVISOR_FLUSH_MS || "40", 10);
  return Number.isFinite(v) && v >= 0 ? v : 40;
})();
let textBuf = null;       // { msg }  — pending coalesced agent_message_* chunk
let thinkBuf = null;      // { msg }  — pending coalesced agent_thought_chunk / thinking
let coalesceTimer = null;

function _scheduleCoalesceFlush() {
  if (coalesceTimer !== null) return;
  // setTimeout 0 still defers across a microtask boundary, so subsequent
  // synchronous chunks from the same acp.stdout 'data' callback keep
  // appending to the same buffer before the flush fires.
  coalesceTimer = setTimeout(_flushCoalesce, COALESCE_FLUSH_MS);
}

function _flushCoalesce() {
  if (coalesceTimer !== null) { clearTimeout(coalesceTimer); coalesceTimer = null; }
  if (textBuf) {
    const line = JSON.stringify(textBuf.msg);
    textBuf = null;
    _broadcastSseRaw(line);
  }
  if (thinkBuf) {
    const line = JSON.stringify(thinkBuf.msg);
    thinkBuf = null;
    _broadcastSseRaw(line);
  }
}

// Mutate ``into.params.update.content``'s text/thinking field by appending
// ``add``. Handles both ``content.text`` (the dominant shape) and
// ``content.thinking`` (older Claude thinking blocks) without flipping
// shape mid-buffer.
function _appendChunkText(into, add) {
  const c = into.params && into.params.update && into.params.update.content;
  if (!c || !add) return;
  if (typeof c.text === "string") c.text += add;
  else if (typeof c.thinking === "string") c.thinking += add;
}

function broadcastSse(line) {
  // Try to interpret as a JSON-RPC frame. Non-JSON lines (shouldn't happen
  // from claude-agent-acp's stdout but defend anyway) just flush + pass
  // through so we never silently drop a line.
  let msg;
  try { msg = JSON.parse(line); } catch { _flushCoalesce(); _broadcastSseRaw(line); return; }
  if (!msg || typeof msg !== "object") { _flushCoalesce(); _broadcastSseRaw(line); return; }
  const update = msg.method === "session/update" && msg.params && msg.params.update;
  if (!update) {
    // JSON-RPC result/error envelopes (turn-end, ACP-initiated requests)
    // must flush any pending text first so the consumer sees the chunk
    // BEFORE the done frame.
    _flushCoalesce();
    _broadcastSseRaw(line);
    return;
  }
  const su = update.sessionUpdate;
  const content = update.content;
  const isText = typeof content === "object" && content !== null
    && typeof content.text === "string" && content.text.length > 0
    && content.type !== "thinking" && typeof content.thinking !== "string";
  const isThinkInline = typeof content === "object" && content !== null
    && (typeof content.thinking === "string" || content.type === "thinking");

  if ((su === "agent_message_delta" || su === "agent_message_chunk") && isText) {
    if (textBuf === null) {
      textBuf = { msg: structuredClone(msg) };
    } else {
      _appendChunkText(textBuf.msg, content.text);
    }
    _scheduleCoalesceFlush();
    return;
  }
  if ((su === "agent_message_delta" || su === "agent_message_chunk") && isThinkInline) {
    const add = content.text || content.thinking || "";
    if (thinkBuf === null) {
      thinkBuf = { msg: structuredClone(msg) };
    } else {
      _appendChunkText(thinkBuf.msg, add);
    }
    _scheduleCoalesceFlush();
    return;
  }
  if (su === "agent_thought_chunk") {
    const add = (content && (content.text || content.thinking)) || "";
    if (thinkBuf === null) {
      thinkBuf = { msg: structuredClone(msg) };
    } else {
      _appendChunkText(thinkBuf.msg, add);
    }
    _scheduleCoalesceFlush();
    return;
  }
  // Non-coalescable session/update: tool_call, tool_call_update, usage,
  // available_commands_update, plan, etc. Flush pending text/think first
  // so consumer-visible order is preserved across the transition.
  _flushCoalesce();
  _broadcastSseRaw(line);
}

function _broadcastSseRaw(line) {
  const block = `data: ${line}\n\n`;
  // Cache available_commands_update for late subscribers
  try {
    const msg = JSON.parse(line);
    if (msg && msg.params && msg.params.update &&
        msg.params.update.sessionUpdate === "available_commands_update") {
      lastCommandsEvent = block;
    }
  } catch { /* not JSON, ignore */ }
  // Slow / paused subscribers are the dominant memory leak at scale: a
  // chatty turn (tool output, large diffs) writes one line per emit, and
  // res.write() returning false just means Node is buffering the bytes
  // for us. Drop the subscriber once the in-Node buffer crosses
  // SSE_BACKPRESSURE_LIMIT_BYTES — that's slower than the ACP stream is
  // producing, and waiting longer just balloons RSS until OOM.
  let drop = null;
  for (const res of sseSubscribers) {
    try {
      // Already over the cap before this write — don't make it worse.
      if (res.writableLength > SSE_BACKPRESSURE_LIMIT_BYTES) {
        (drop || (drop = [])).push(res);
        continue;
      }
      res.write(block);
    } catch {
      // synchronous error path — schedule for removal here too; a 'close'
      // event would cover it eventually but we'd rather not keep iterating
      // a stale entry on the next line.
      (drop || (drop = [])).push(res);
    }
  }
  if (drop) {
    for (const res of drop) {
      sseSubscribers.delete(res);
      try { res.destroy(); } catch {}
    }
    log(`sse drop slow subscribers: ${drop.length} (remaining ${sseSubscribers.size})`);
  }
}

// Pick an "allow" option from a session/request_permission's params.options
// list. ACP options are objects like { optionId: "allow_always", kind:
// "allow_always", name: "..." }. Prefer allow_always so the agent stops
// asking for the same permission later this turn; fall back to allow_once;
// last resort echo the first option's optionId so the agent doesn't deadlock
// if a runtime ships only non-standard option ids.
function pickAllowOptionId(params) {
  const options = params && Array.isArray(params.options) ? params.options : [];
  for (const want of ["allow_always", "allow_once"]) {
    const hit = options.find(
      (o) => o && (o.kind === want || o.optionId === want),
    );
    if (hit && hit.optionId) return hit.optionId;
  }
  return options[0]?.optionId || "allow_always";
}

// ACP client-side method handlers. Each takes the request params and returns
// the JSON-RPC ``result`` value (or throws to surface an error frame).
//
// Only ONE client-side method is actually called by the runtimes we ship
// (verified empirically 2026-06 on the pinned opencode 1.14.30 and
// claude-agent-acp 0.27.0: both execute every tool locally in their own
// process, so fs/shell effects land in-sandbox by co-location — NOT via ACP
// client requests, regardless of advertised client capabilities):
//
//   session/request_permission — load-bearing. opencode routes tool
//   APPROVAL through the client: any tool under permission "ask" config,
//   plus external_directory whenever a tool touches paths outside the
//   session cwd (fires even on default-allow config). An unanswered
//   request hangs the turn forever, so this auto-allow is what keeps
//   turns completing.
//
// fs/* and terminal/* handlers were removed after verification that no
// shipped runtime calls them. dispatchClientRequest answers unknown methods
// with -32601, the correct graceful reply if a future runtime version
// starts delegating (its tool falls back locally or surfaces an error
// instead of hanging the turn).
const CLIENT_HANDLERS = {
  async "session/request_permission"(params) {
    // Sandbox trust model: agent runs in an isolated container/VM, so the
    // OS layer already constrains what tools can do. Auto-allow rather than
    // deadlock waiting for a human approval that won't come. Claude never
    // reaches here — session/setMode("bypassPermissions") at attach turns
    // off its permission flow before the first tool call.
    const optionId = pickAllowOptionId(params);
    return { outcome: { outcome: "selected", optionId } };
  },
};

async function dispatchClientRequest(msg) {
  const handler = CLIENT_HANDLERS[msg.method];
  if (!handler) {
    return {
      jsonrpc: "2.0",
      id: msg.id,
      error: {
        code: -32601,
        message: `Method not found: ${msg.method}`,
      },
    };
  }
  try {
    const result = await handler(msg.params || {});
    return { jsonrpc: "2.0", id: msg.id, result: result === undefined ? null : result };
  } catch (e) {
    return {
      jsonrpc: "2.0",
      id: msg.id,
      error: {
        code: -32000,
        message: `client handler ${msg.method} failed: ${e && e.message ? e.message : e}`,
      },
    };
  }
}

async function handleAcpLine(line) {
  // Always fan out to SSE subscribers — the Python server's reader
  // consumes this stream for event broadcast + terminal attribution.
  broadcastSse(line);

  let msg = null;
  try {
    msg = JSON.parse(line);
  } catch {
    return;
  }
  // ACP-initiated request from the agent (fs/*, terminal/*,
  // session/request_permission). Has method+id, no result/error.
  if (
    msg &&
    typeof msg === "object" &&
    "id" in msg &&
    "method" in msg &&
    !("result" in msg) &&
    !("error" in msg)
  ) {
    const reply = await dispatchClientRequest(msg);
    if (reply.error) {
      log(`client handler ${msg.method} id=${msg.id} returned error: ${reply.error.message}`);
    }
    try {
      acp.stdin.write(JSON.stringify(reply) + "\n");
    } catch (e) {
      log(`failed to send reply for ${msg.method} id=${msg.id}: ${e.message}`);
    }
    return;
  }
  if (
    msg &&
    typeof msg === "object" &&
    "id" in msg &&
    ("result" in msg || "error" in msg)
  ) {
    const rid = String(msg.id);
    const isPromptResponse = pendingPromptIds.has(rid);
    // Per-turn agent-memory snapshot: blocks the HTTP reply until the
    // memory tarball is durable on the volume. Small payload (just the
    // session-state dirs), so turn-end latency is ~50-200ms vs the
    // multi-second full-HOME snapshot this replaced. Preserves the
    // invariant "once the client sees 'turn done', session/load on a
    // replacement sandbox finds the JSONL."
    if (isPromptResponse) {
      pendingPromptIds.delete(rid);
      try {
        await runAgentMemorySnapshotOnce();
      } catch (e) {
        log(`agent_memory error on turn-end: ${e.message}`);
      }
    }
    const resolver = pendingResponses.get(rid);
    if (resolver) {
      pendingResponses.delete(rid);
      resolver(msg);
    }
  }
}

let stdoutBuf = "";
acp.stdout.on("data", (chunk) => {
  stdoutBuf += chunk.toString("utf8");
  let idx;
  while ((idx = stdoutBuf.indexOf("\n")) !== -1) {
    const line = stdoutBuf.slice(0, idx);
    stdoutBuf = stdoutBuf.slice(idx + 1);
    if (!line) continue;
    handleAcpLine(line).catch((e) => log(`acp-line handler error: ${e.message}`));
  }
});

async function handlePost(req, res) {
  let raw = "";
  req.setEncoding("utf8");
  for await (const chunk of req) raw += chunk;

  let body;
  try {
    body = JSON.parse(raw);
  } catch (e) {
    sendJson(res, 400, { error: "invalid JSON body: " + e.message });
    return;
  }

  const line = JSON.stringify(body) + "\n";
  try {
    acp.stdin.write(line);
  } catch (e) {
    sendJson(res, 502, { error: "acp stdin write failed: " + e.message });
    return;
  }

  // Remember session/prompt request ids so the stdout reader can trigger a
  // snapshot when the matching response lands. Covers every client variant —
  // SDK + direct-ACP + external integrations — without parsing update events.
  // Bounded with FIFO eviction so a stuck session that never gets a response
  // (ACP child crash mid-turn, malformed frame ACP silently dropped) can't
  // grow the set unbounded. The eviction is intentionally NOT tied to client
  // disconnect because we still want the turn-end snapshot to fire when ACP
  // eventually responds, even if the upstream caller has given up.
  if (body && body.method === "session/prompt" && "id" in body) {
    if (pendingPromptIds.size >= MAX_PENDING_PROMPT_IDS) {
      // Set iteration is insertion order — the first key is the oldest.
      const oldest = pendingPromptIds.values().next().value;
      if (oldest !== undefined) pendingPromptIds.delete(oldest);
    }
    pendingPromptIds.add(String(body.id));
  }

  // Notification — fire-and-forget.
  if (!("id" in body)) {
    sendJson(res, 202, {});
    return;
  }

  // Request — wait for the matching response.
  //
  // Cleanup-on-disconnect: if the upstream client drops mid-flight, ACP
  // *usually* still responds (the resolver fires, deletes the Map entry,
  // and writes to a closed res — which throws and is caught by the outer
  // handler). The leak only bites when ACP also fails to answer that id —
  // child crash in flight, malformed frame, etc. Cheap defensive cleanup:
  // a CLIENT_GONE sentinel resolves the Promise so the handler unwinds
  // and stops holding the Map entry. The map-delete also guarantees the
  // ACP-stdout path doesn't later try to call a stale resolver.
  const rpcId = String(body.id);
  const CLIENT_GONE = Symbol("client_gone");
  let cleanupOnce = null;
  const envelope = await new Promise((resolve) => {
    pendingResponses.set(rpcId, resolve);
    cleanupOnce = () => {
      if (pendingResponses.get(rpcId) === resolve) {
        pendingResponses.delete(rpcId);
        resolve(CLIENT_GONE);
      }
    };
    // res.on("close") fires when the underlying socket is terminated
    // before res.end() — the actual "client gave up" signal. req.on
    // ("close") was unreliable here under Node's HTTP server semantics
    // when the request body had already been fully consumed.
    res.on("close", cleanupOnce);
  });
  // Detach the listener once the Promise resolves through the normal
  // path. Leaving it attached holds the closure (and the resolved
  // envelope) alive until the response emits 'close', which is later
  // than necessary.
  if (cleanupOnce) res.removeListener("close", cleanupOnce);

  if (envelope === CLIENT_GONE) {
    // Client disconnected before ACP responded. No socket to write to.
    return;
  }

  sendJson(res, 200, envelope);
}

function handleSse(req, res) {
  res.writeHead(200, {
    "content-type": "text/event-stream",
    "cache-control": "no-cache",
    connection: "keep-alive",
    "x-accel-buffering": "no",
  });
  // Important: flush headers immediately so a subscriber can connect
  // before the next ACP stdout line is emitted. Without this, Node may
  // buffer the 200 response until the first res.write(), causing clients
  // that subscribe before session/load replay to hang on connect.
  if (typeof res.flushHeaders === "function") {
    res.flushHeaders();
  }
  // Replay cached commands event for late subscribers
  if (lastCommandsEvent) {
    try { res.write(lastCommandsEvent); } catch { /* noop */ }
  }
  sseSubscribers.add(res);
  log(`sse subscribe (${sseSubscribers.size} total)`);
  const heartbeat = setInterval(() => {
    try {
      res.write(": heartbeat\n\n");
    } catch {
      clearInterval(heartbeat);
    }
  }, SSE_HEARTBEAT_MS);

  req.on("close", () => {
    clearInterval(heartbeat);
    sseSubscribers.delete(res);
    log(`sse unsubscribe (${sseSubscribers.size} total)`);
  });
}

// ── Filesystem helpers ──

const MAX_TREE_DEPTH = 20;

function walk(dir, relPrefix, depth) {
  if (depth >= MAX_TREE_DEPTH) return [];
  const entries = [];
  let dirents;
  try {
    dirents = fs.readdirSync(dir);
  } catch {
    return entries;
  }
  for (const name of dirents) {
    if (name.startsWith(".")) continue;
    const full = path.join(dir, name);
    const rel = relPrefix ? `${relPrefix}/${name}` : name;
    let stat;
    try {
      stat = fs.lstatSync(full);
    } catch {
      continue;
    }
    if (stat.isSymbolicLink()) continue; // skip symlinks to avoid loops
    if (stat.isDirectory()) {
      entries.push({
        name,
        path: rel,
        type: "directory",
        size: 0,
        modifiedAt: stat.mtime.toISOString(),
        children: walk(full, rel, depth + 1),
      });
    } else {
      entries.push({
        name,
        path: rel,
        type: "file",
        size: stat.size,
        modifiedAt: stat.mtime.toISOString(),
      });
    }
  }
  entries.sort((a, b) => {
    if (a.type !== b.type) return a.type === "directory" ? -1 : 1;
    return a.name.localeCompare(b.name);
  });
  return entries;
}

const BINARY_EXTS = new Set([
  ".zip",
  ".tar",
  ".gz",
  ".bz2",
  ".xz",
  ".7z",
  ".bin",
  ".exe",
  ".dll",
  ".so",
  ".dylib",
  ".pkl",
  ".pt",
  ".pth",
  ".onnx",
  ".safetensors",
  ".db",
  ".sqlite",
  ".sqlite3",
  ".woff",
  ".woff2",
  ".ttf",
  ".otf",
  ".eot",
]);
const IMAGE_EXTS = new Set([
  ".png",
  ".jpg",
  ".jpeg",
  ".gif",
  ".bmp",
  ".ico",
  ".svg",
  ".webp",
]);
const AUDIO_EXTS = new Set([".wav", ".mp3", ".ogg", ".flac", ".aac", ".m4a"]);
const VIDEO_EXTS = new Set([".mp4", ".webm", ".mov", ".avi", ".mkv"]);
const MIME_MAP = {
  ".png": "image/png",
  ".jpg": "image/jpeg",
  ".jpeg": "image/jpeg",
  ".gif": "image/gif",
  ".bmp": "image/bmp",
  ".ico": "image/x-icon",
  ".svg": "image/svg+xml",
  ".webp": "image/webp",
  ".wav": "audio/wav",
  ".mp3": "audio/mpeg",
  ".ogg": "audio/ogg",
  ".flac": "audio/flac",
  ".aac": "audio/aac",
  ".m4a": "audio/mp4",
  ".mp4": "video/mp4",
  ".webm": "video/webm",
  ".mov": "video/quicktime",
};
const MAX_FILE_SIZE = 20 * 1024 * 1024; // 20 MB

// Read the request body, aborting once `max` bytes have been received.
// Without this guard, a misbehaving client could stream gigabytes into the
// supervisor's V8 heap before the post-loop size check fired.
async function readBodyCapped(req, res, max) {
  let size = 0;
  const chunks = [];
  for await (const chunk of req) {
    size += chunk.length;
    if (size > max) {
      req.destroy();
      res.writeHead(413, { "content-type": "application/json" });
      res.end(JSON.stringify({ error: "payload too large" }));
      return null;
    }
    chunks.push(chunk);
  }
  return Buffer.concat(chunks).toString("utf8");
}

function sendJson(res, status, obj) {
  res.writeHead(status, { "content-type": "application/json" });
  res.end(JSON.stringify(obj));
}

async function readJsonBody(req, res, max) {
  const raw = await readBodyCapped(req, res, max);
  if (raw === null) return null;
  try {
    return JSON.parse(raw);
  } catch (e) {
    sendJson(res, 400, { error: "invalid JSON body: " + e.message });
    return null;
  }
}

function resolveUnderRoot(res, p) {
  const resolvedRoot = path.resolve(args.root);
  const full = path.resolve(resolvedRoot, p);
  if (!full.startsWith(resolvedRoot + "/") && full !== resolvedRoot) {
    sendJson(res, 403, { error: "path traversal denied" });
    return null;
  }
  return full;
}

async function handleFilesEdit(req, res) {
  const body = await readJsonBody(req, res, MAX_FILE_SIZE);
  if (body === null) return;

  const filePath = body.path;
  const oldString = body.old_string;
  const newString = body.new_string;
  const replaceAll = body.replace_all === true;

  if (typeof filePath !== "string" || !filePath) {
    sendJson(res, 400, { error: "path required" });
    return;
  }
  if (typeof oldString !== "string" || typeof newString !== "string") {
    sendJson(res, 400, { error: "old_string and new_string required" });
    return;
  }

  const fullPath = resolveUnderRoot(res, filePath);
  if (fullPath === null) return;

  // old_string === "" means write/create the entire file
  if (oldString === "") {
    // Create parent directories if needed
    const dir = path.dirname(fullPath);
    fs.mkdirSync(dir, { recursive: true });
    fs.writeFileSync(fullPath, newString, "utf8");
    const stat = fs.statSync(fullPath);
    sendJson(res, 200, { ok: true, path: filePath, size: stat.size, created: true });
    return;
  }

  // Regular edit — file must exist
  let content;
  try {
    content = fs.readFileSync(fullPath, "utf8");
  } catch {
    sendJson(res, 404, { error: "file not found" });
    return;
  }

  if (oldString === newString) {
    sendJson(res, 400, { error: "old_string and new_string are identical" });
    return;
  }

  // Count occurrences
  let count = 0;
  let idx = 0;
  while ((idx = content.indexOf(oldString, idx)) !== -1) {
    count++;
    idx += oldString.length;
  }

  if (count === 0) {
    sendJson(res, 400, { error: "old_string not found in file" });
    return;
  }

  if (count > 1 && !replaceAll) {
    sendJson(res, 400, {
      error: `old_string matches ${count} locations; provide more context to make it unique, or set replace_all: true`,
      matches: count,
    });
    return;
  }

  // Perform replacement
  let updated;
  if (replaceAll) {
    updated = content.split(oldString).join(newString);
  } else {
    const pos = content.indexOf(oldString);
    updated = content.slice(0, pos) + newString + content.slice(pos + oldString.length);
  }

  fs.writeFileSync(fullPath, updated, "utf8");
  const stat = fs.statSync(fullPath);
  sendJson(res, 200, {
    ok: true,
    path: filePath,
    size: stat.size,
    replacements: replaceAll ? count : 1,
  });
}

const MAX_EXEC_OUTPUT = 1 * 1024 * 1024; // 1 MB

async function handleExec(req, res) {
  let raw = "";
  req.setEncoding("utf8");
  for await (const chunk of req) raw += chunk;

  let body;
  try {
    body = JSON.parse(raw);
  } catch (e) {
    sendJson(res, 400, { error: "invalid JSON body: " + e.message });
    return;
  }

  const command = body.command;
  const timeout = Math.min(parseInt(body.timeout, 10) || 30, 300) * 1000;

  if (typeof command !== "string" || !command.trim()) {
    sendJson(res, 400, { error: "command required" });
    return;
  }

  await new Promise((resolve) => {
    // HOME pinned to args.root so /v1/exec sees the same agent home
    // as the ACP child (see ~L287). Without this, the supervisor's own
    // process.env.HOME is whatever it inherited from the provider's
    // launch context (commonly /root on daytona) — and `npx skills
    // add ... -g`, ~/.claude config reads, anything HOME-relative
    // lands in the wrong directory, invisible to Claude / opencode.
    //
    // PATH also extended to include $HOME/.local/bin so uv-installed
    // tools (AgentConfig.cli_tools) are reachable from /sandbox/exec
    // — mirrors the ACP child env. Same _spawnPath as the ACP child.
    const child = spawn("bash", ["-c", command], {
      cwd: args.root,
      env: { ...process.env, HOME: args.root, PATH: _spawnPath },
      stdio: ["ignore", "pipe", "pipe"],
    });

    let stdout = "";
    let stderr = "";
    let stdoutTruncated = false;
    let stderrTruncated = false;
    let timedOut = false;
    // Three exit paths into the resolver: child close, child error, client
    // disconnect. The last one previously didn't exist — if a client gave
    // up mid-exec, the bash subprocess kept running for up to 300 s of
    // stranded CPU + memory + fs writes, plus we'd later try to write a
    // 200 to a destroyed socket. Single-shot guard so the three paths
    // can't double-resolve or write to res twice.
    let finished = false;
    const finish = (fn) => {
      if (finished) return;
      finished = true;
      clearTimeout(timer);
      res.removeListener("close", onClose);
      try { fn(); } catch {}
      resolve();
    };

    const timer = setTimeout(() => {
      timedOut = true;
      try { child.kill("SIGKILL"); } catch {}
    }, timeout);

    const onClose = () => {
      // Client disconnected before bash finished. Kill the child so we
      // don't burn the rest of the timeout window on output nobody will
      // ever see, and skip writing the response (socket is gone). Listen
      // on res — req.on("close") was unreliable post-body-consumption.
      try { child.kill("SIGKILL"); } catch {}
      finish(() => {});
    };
    res.on("close", onClose);

    child.stdout.on("data", (chunk) => {
      if (stdout.length < MAX_EXEC_OUTPUT) {
        stdout += chunk.toString("utf8");
        if (stdout.length >= MAX_EXEC_OUTPUT) {
          stdout = stdout.slice(0, MAX_EXEC_OUTPUT);
          stdoutTruncated = true;
        }
      }
    });
    child.stderr.on("data", (chunk) => {
      if (stderr.length < MAX_EXEC_OUTPUT) {
        stderr += chunk.toString("utf8");
        if (stderr.length >= MAX_EXEC_OUTPUT) {
          stderr = stderr.slice(0, MAX_EXEC_OUTPUT);
          stderrTruncated = true;
        }
      }
    });

    child.on("close", (code) => {
      finish(() => {
        const result = {
          stdout,
          stderr,
          exit_code: timedOut ? -1 : (code ?? -1),
          timed_out: timedOut,
        };
        if (stdoutTruncated) result.stdout_truncated = true;
        if (stderrTruncated) result.stderr_truncated = true;
        sendJson(res, 200, result);
      });
    });

    child.on("error", (e) => {
      finish(() => {
        sendJson(res, 500, { error: e.message, stdout: "", stderr: "", exit_code: -1 });
      });
    });
  });
}

async function handleFilesUpload(req, res) {
  const body = await readJsonBody(req, res, MAX_FILE_SIZE);
  if (body === null) return;

  const filePath = body.path;
  const content = body.content;
  if (typeof filePath !== "string" || !filePath || typeof content !== "string") {
    sendJson(res, 400, { error: "path and content (base64) required" });
    return;
  }

  const fullPath = resolveUnderRoot(res, filePath);
  if (fullPath === null) return;

  fs.mkdirSync(path.dirname(fullPath), { recursive: true });
  fs.writeFileSync(fullPath, Buffer.from(content, "base64"));
  const stat = fs.statSync(fullPath);
  sendJson(res, 200, { ok: true, path: filePath, size: stat.size });
}

async function handleFilesDelete(req, res) {
  const body = await readJsonBody(req, res, MAX_FILE_SIZE);
  if (body === null) return;

  const filePath = body.path;
  if (typeof filePath !== "string" || !filePath) {
    sendJson(res, 400, { error: "path required" });
    return;
  }

  const fullPath = resolveUnderRoot(res, filePath);
  if (fullPath === null) return;

  if (!fs.existsSync(fullPath)) {
    sendJson(res, 404, { error: "not found" });
    return;
  }

  const stat = fs.statSync(fullPath);
  if (stat.isDirectory()) {
    fs.rmSync(fullPath, { recursive: true });
  } else {
    fs.unlinkSync(fullPath);
  }
  sendJson(res, 200, { ok: true });
}

async function handleFilesRename(req, res) {
  const body = await readJsonBody(req, res, MAX_FILE_SIZE);
  if (body === null) return;

  const filePath = body.path;
  const newPath = body.new_path;
  if (typeof filePath !== "string" || !filePath || typeof newPath !== "string" || !newPath) {
    sendJson(res, 400, { error: "path and new_path required" });
    return;
  }

  const fullPath = resolveUnderRoot(res, filePath);
  if (fullPath === null) return;

  const resolvedRoot = path.resolve(args.root);
  const newFullPath = path.resolve(resolvedRoot, newPath);
  if (!newFullPath.startsWith(resolvedRoot + "/") && newFullPath !== resolvedRoot) {
    sendJson(res, 403, { error: "path traversal denied (new_path)" });
    return;
  }

  if (!fs.existsSync(fullPath)) {
    sendJson(res, 404, { error: "not found" });
    return;
  }

  fs.mkdirSync(path.dirname(newFullPath), { recursive: true });
  fs.renameSync(fullPath, newFullPath);
  sendJson(res, 200, { ok: true, path: newPath });
}

function handleFilesDownload(req, res) {
  const u = new URL(req.url, `http://${req.headers.host}`);
  const filePath = u.searchParams.get("path");
  if (!filePath) {
    sendJson(res, 400, { error: "path query param required" });
    return;
  }

  const fullPath = resolveUnderRoot(res, filePath);
  if (fullPath === null) return;

  let stat;
  try { stat = fs.statSync(fullPath); } catch {
    sendJson(res, 404, { error: "file not found" });
    return;
  }
  if (stat.isDirectory()) {
    sendJson(res, 404, { error: "file not found" });
    return;
  }
  if (stat.size > MAX_FILE_SIZE) {
    sendJson(res, 413, { error: `file too large: ${stat.size} bytes (max ${MAX_FILE_SIZE})` });
    return;
  }

  const fileName = path.basename(fullPath);
  const ext = path.extname(fileName).toLowerCase();
  const mimeTypes = { ".html": "text/html", ".js": "text/javascript", ".json": "application/json", ".css": "text/css", ".png": "image/png", ".jpg": "image/jpeg", ".jpeg": "image/jpeg", ".gif": "image/gif", ".svg": "image/svg+xml", ".pdf": "application/pdf", ".zip": "application/zip", ".tar": "application/x-tar", ".gz": "application/gzip" };
  const contentType = mimeTypes[ext] || "application/octet-stream";

  res.writeHead(200, {
    "content-type": contentType,
    "content-disposition": `attachment; filename="${fileName}"`,
    "content-length": stat.size,
  });
  fs.createReadStream(fullPath).pipe(res);
}

function handleHealth(req, res) {
  sendJson(res, 200, {
    status: "ok",
    acp_pid: acp.pid,
    acp_alive: acp.exitCode === null,
    sse_subscribers: sseSubscribers.size,
    pending_responses: pendingResponses.size,
    cwd: args.root,
  });
}

function handleFilesTree(req, res) {
  const u = new URL(req.url, `http://${req.headers.host}`);
  const root = u.searchParams.get("root") || args.root || "/tmp";
  const resolved = path.resolve(root);
  if (!fs.existsSync(resolved)) {
    sendJson(res, 404, { error: "root path not found" });
    return;
  }
  const tree = walk(resolved, "", 0);
  sendJson(res, 200, tree);
}

function handleFilesRead(req, res) {
  const u = new URL(req.url, `http://${req.headers.host}`);
  const filePath = u.searchParams.get("path");
  const root = u.searchParams.get("root") || args.root || "/tmp";
  if (!filePath) {
    sendJson(res, 400, { error: "path query param required" });
    return;
  }
  const resolvedRoot = path.resolve(root);
  const fullPath = path.resolve(resolvedRoot, filePath);
  // Path traversal guard — resolved path must stay under root
  if (!fullPath.startsWith(resolvedRoot + "/") && fullPath !== resolvedRoot) {
    sendJson(res, 403, { error: "path traversal denied" });
    return;
  }
  let stat;
  try {
    stat = fs.statSync(fullPath);
  } catch {
    sendJson(res, 404, { error: "file not found" });
    return;
  }
  if (stat.isDirectory()) {
    sendJson(res, 400, { error: "path is a directory" });
    return;
  }
  if (stat.size > MAX_FILE_SIZE) {
    sendJson(res, 413, { error: `file too large: ${stat.size} bytes (max ${MAX_FILE_SIZE})` });
    return;
  }

  const ext = path.extname(fullPath).toLowerCase();
  const isImage = IMAGE_EXTS.has(ext);
  const isAudio = AUDIO_EXTS.has(ext);
  const isVideo = VIDEO_EXTS.has(ext);
  const isPdf = ext === ".pdf";
  const isBinary =
    BINARY_EXTS.has(ext) || isImage || isAudio || isVideo || isPdf;

  let content;
  if (isImage || isAudio || isVideo) {
    const buf = fs.readFileSync(fullPath);
    const mime = MIME_MAP[ext] || "application/octet-stream";
    content = `data:${mime};base64,${buf.toString("base64")}`;
  } else if (isPdf) {
    const buf = fs.readFileSync(fullPath);
    content = `data:application/pdf;base64,${buf.toString("base64")}`;
  } else if (BINARY_EXTS.has(ext)) {
    content = `[Binary file: ${stat.size} bytes]`;
  } else {
    content = fs.readFileSync(fullPath, "utf8");
  }

  sendJson(res, 200, {
    content,
    name: path.basename(fullPath),
    size: stat.size,
    binary: isBinary,
    image: isImage,
    audio: isAudio,
    video: isVideo,
    pdf: isPdf,
  });
}

function runHandler(label, fn, req, res) {
  try {
    Promise.resolve(fn(req, res)).catch((e) => {
      log(`${label} handler crashed: ` + e.stack);
      try {
        res.writeHead(500, { "content-type": "application/json" });
        res.end(JSON.stringify({ error: e.message }));
      } catch {}
    });
  } catch (e) {
    log(`${label} handler crashed: ` + e.stack);
    try {
      res.writeHead(500, { "content-type": "application/json" });
      res.end(JSON.stringify({ error: e.message }));
    } catch {}
  }
}

// Route table — matched in order (same precedence as the original if-ladder).
// prefix:true uses startsWith; otherwise exact match. method:null matches any method.
const ROUTES = [
  { method: null,   path: "/v1/health",          handler: handleHealth,        label: "health" },
  { method: null,   path: "/health",              handler: handleHealth,        label: "health" },
  { method: "POST", path: "/v1/acp/",  prefix: true, handler: handlePost,      label: "POST" },
  { method: "GET",  path: "/v1/acp/",  prefix: true, handler: handleSse,       label: "SSE" },
  { method: "POST", path: "/v1/snapshot",         handler: handleSnapshot,      label: "snapshot" },
  { method: "GET",  path: "/v1/files/tree", prefix: true, handler: handleFilesTree, label: "files/tree" },
  { method: "GET",  path: "/v1/files/read", prefix: true, handler: handleFilesRead, label: "files/read" },
  { method: "POST", path: "/v1/files/edit", prefix: true, handler: handleFilesEdit, label: "files/edit" },
  { method: "POST", path: "/v1/exec",             handler: handleExec,          label: "exec" },
  { method: "POST", path: "/v1/files/upload", prefix: true, handler: handleFilesUpload, label: "files/upload" },
  { method: "POST", path: "/v1/files/delete", prefix: true, handler: handleFilesDelete, label: "files/delete" },
  { method: "POST", path: "/v1/files/rename", prefix: true, handler: handleFilesRename, label: "files/rename" },
  { method: "GET",  path: "/v1/files/download", prefix: true, handler: handleFilesDownload, label: "files/download" },
];

const server = http.createServer((req, res) => {
  const url = req.url || "";
  for (const route of ROUTES) {
    if (route.method !== null && req.method !== route.method) continue;
    const matched = route.prefix
      ? url.startsWith(route.path)
      : url === route.path;
    if (matched) {
      runHandler(route.label, route.handler, req, res);
      return;
    }
  }
  res.writeHead(404, { "content-type": "text/plain" });
  res.end("not found\n");
});

server.listen(args.port, args.host, () => {
  log(`listening on ${args.host}:${args.port}`);
});

let shuttingDown = false;
function shutdown() {
  if (shuttingDown) return;
  shuttingDown = true;
  log("shutting down");
  try {
    acp.stdin.end();
  } catch {}
  // Final sync snapshot so the most recent turn lands on the volume even
  // when a turn completed but the async path hasn't drained. No-op when
  // --snapshot-path wasn't provided.
  runSnapshotSync();
  try {
    server.close();
  } catch {}
  setTimeout(() => process.exit(0), 500);
}
process.on("SIGTERM", shutdown);
process.on("SIGINT", shutdown);
