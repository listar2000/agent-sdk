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
    acpArgs: [],
  };
  for (let i = 2; i < argv.length; i++) {
    const a = argv[i];
    if (a === "--port") out.port = parseInt(argv[++i], 10);
    else if (a === "--host") out.host = argv[++i];
    else if (a === "--acp") out.acp = argv[++i];
    else if (a === "--root" || a === "--cwd") out.root = argv[++i];
    else if (a === "--snapshot-path") out.snapshotPath = argv[++i];
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

const args = parseArgs(process.argv);

// Ensure args.root exists and, if a snapshot is configured, restore the
// previous workspace before starting ACP.
//
// Two boot modes (called "Type 1" / "Type 2" in server.py — see the block
// above _type2_recover):
//
//   Type 1 — supervisor restart inside an EXISTING VM (daytona
//            restart_daytona_supervisor / port-based start_sandbox).
//            args.root on local ext4 already has the latest workspace
//            bytes from the previous supervisor's writes; restoring from
//            the volume tarballs is pure waste (potentially hundreds of
//            MB of read+write on snapshot.tar) and adds 15s of FUSE-poll
//            wait if the cold tarball isn't already visible.
//
//   Type 2 — fresh VM, blank args.root. The volume tarballs are the
//            only way to repopulate session+workspace state.
//
// We distinguish the two with a sentinel file at SUPERVISOR_BOOT_MARKER:
//   - /tmp survives a Type 1 boot (same VM ⇒ same /tmp)
//   - /tmp is wiped on Type 2 (new VM ⇒ blank /tmp)
// So the sentinel cleanly says "this VM has already been bootstrapped;
// skip the redundant restore." Cheaper than a server-side `--fresh` arg
// and doesn't depend on Daytona's image-level dotfile pre-population
// (which used to make readdir-empty heuristics false-negative on Type 2).
const SUPERVISOR_BOOT_MARKER = "/tmp/agent-sdk-bootstrapped";
const isWarmRestart = (() => {
  try { return fs.existsSync(SUPERVISOR_BOOT_MARKER); }
  catch { return false; }
})();
try {
  fs.mkdirSync(args.root, { recursive: true });
} catch (e) {
  log(`mkdir root failed: ${e.message}`);
}

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

if (args.snapshotPath && isWarmRestart) {
  // Type 1 boot — local ext4 already holds the latest workspace bytes from
  // the previous supervisor in this VM. Skip both restore tiers; they would
  // re-extract the exact same state we already have on disk.
  log(`Type 1 boot detected (sentinel ${SUPERVISOR_BOOT_MARKER} present); skipping snapshot restore`);
} else if (args.snapshotPath) {
  // Type 2 boot — blank /home/daytona on a fresh VM. The volume tarballs
  // are the only way to repopulate state.
  //
  // Layer 1: cold restore (full HOME). Best-effort — fresh agents that have
  // never been snapshotted don't have this and it's fine.
  const coldVisible = _snapshotVisible(args.snapshotPath, 15000);
  if (coldVisible) {
    log(`restoring filesystem_cache from ${args.snapshotPath}`);
    const r = spawnSync("tar", ["-xf", args.snapshotPath, "-C", args.root], {
      stdio: ["ignore", "inherit", "inherit"],
    });
    if (r.status !== 0) {
      log(`filesystem_cache restore exited rc=${r.status}; continuing`);
    }
  } else {
    log(`filesystem_cache ${args.snapshotPath} not visible after 15s — assuming fresh sandbox`);
  }

  // Layer 2: agent-memory overlay (per-turn snapshot of session dirs).
  // This is the tier that carries conversation JSONLs, so even if
  // filesystem_cache is stale (no cold snapshot since last turn), the
  // latest turn's memory still wins.
  //
  // Use _snapshotVisible (poll) instead of a single existsSync. After
  // an external sandbox delete, the prior sandbox's last per-turn write
  // to agent_memory.tar may take 5-15 s to be visible on the new sandbox's
  // S3-backed FUSE mount under concurrent load — and a single existsSync
  // at boot can miss it, silently dropping turn-N conversation state
  // and forcing claude-agent-acp's session/load to return -32603 forever
  // (the JSONL never lands on /home/daytona). Polling the same way
  // filesystem_cache.tar does closes that race.
  //
  // Trade-off: a truly fresh sandbox with no agent_memory.tar yet pays
  // up to the poll-timeout cost (currently ~10s) on first boot. Worth it:
  // the alternative is silent context loss on every Type 2 replacement
  // under load, which the test_session_resume_after_delete[daytona]
  // ``inner_after == inner_before`` invariant deliberately catches.
  const memPath = _agentMemoryPath(args.snapshotPath);
  if (memPath && _snapshotVisible(memPath, 10000)) {
    log(`restoring agent_memory from ${memPath}`);
    const r = spawnSync("tar", ["-xf", memPath, "-C", args.root], {
      stdio: ["ignore", "inherit", "inherit"],
    });
    if (r.status !== 0) {
      log(`agent_memory restore exited rc=${r.status}; continuing`);
    }
  } else if (memPath) {
    log(`agent_memory ${memPath} not visible after 10s — skipping overlay restore`);
  }
}

// Drop the sentinel so the next supervisor boot inside this VM can detect
// it as a Type 1 restart. /tmp is wiped on a fresh VM (Type 2), so the
// sentinel correctly disappears in that case.
try {
  fs.writeFileSync(SUPERVISOR_BOOT_MARKER, String(Date.now()));
} catch (e) {
  log(`failed to write boot sentinel ${SUPERVISOR_BOOT_MARKER}: ${e.message}`);
}

// The ACP child's HOME must match args.root so Claude Code's
// ~/.claude/projects/... JSONLs land inside the workspace we just restored
// (and therefore get captured by the next snapshot). Provider-agnostic —
// local/docker already align HOME with root, Daytona previously needed a
// force-override that this replaces.
const acp = spawn(args.acp, args.acpArgs, {
  stdio: ["pipe", "pipe", "pipe"],
  env: { ...process.env, HOME: args.root },
  cwd: args.root,
});
log("spawned acp pid=" + acp.pid);

acp.stderr.on("data", (chunk) => {
  process.stderr.write("[acp-stderr] " + chunk.toString());
});

acp.on("exit", (code, signal) => {
  log(`acp exited code=${code} signal=${signal}`);
  process.exit(code || 1);
});

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

// Per-turn snapshot: tar only AGENT_MEMORY_DIRS. Uses
// --ignore-failed-read so dirs not present for the active agent_type
// are silently skipped (the same tar command works for any agent).
// Result is written to <dir(snapshotPath)>/agent_memory.tar.
function runAgentMemorySnapshotOnce() {
  return new Promise((resolve) => {
    const memPath = _agentMemoryPath(args.snapshotPath);
    if (!memPath) { resolve(); return; }
    const stage = LOCAL_MEMORY_STAGING;
    const tarArgs = [
      "-cf", stage,
      "--ignore-failed-read",
      "-C", args.root,
      ...AGENT_MEMORY_DIRS,
    ];
    const tar = spawn("tar", tarArgs, { stdio: ["ignore", "ignore", "pipe"] });
    let tarErr = "";
    tar.stderr.on("data", (c) => { tarErr += c.toString("utf8"); });
    tar.on("error", (e) => {
      log(`agent_memory tar spawn error: ${e.message}`);
      try { fs.unlinkSync(stage); } catch {}
      resolve();
    });
    tar.on("close", (tarCode) => {
      // tar with --ignore-failed-read returns 0 even if dirs are missing;
      // a non-zero rc means something real failed. Don't abort — log and
      // try cp anyway; if stage isn't present cp will fail and we move on.
      if (tarCode !== 0) {
        log(`agent_memory tar rc=${tarCode}: ${tarErr.slice(0, 400)}`);
      }
      const cp = spawn("cp", [stage, memPath], { stdio: ["ignore", "ignore", "pipe"] });
      let cpErr = "";
      cp.stderr.on("data", (c) => { cpErr += c.toString("utf8"); });
      cp.on("error", (e) => {
        log(`agent_memory cp spawn error: ${e.message}`);
        try { fs.unlinkSync(stage); } catch {}
        resolve();
      });
      cp.on("close", (cpCode) => {
        try { fs.unlinkSync(stage); } catch {}
        if (cpCode !== 0) {
          log(`agent_memory cp rc=${cpCode}: ${cpErr.slice(0, 400)}`);
        }
        resolve();
      });
    });
  });
}

function runSnapshotOnce() {
  return new Promise((resolve) => {
    if (!args.snapshotPath) {
      resolve();
      return;
    }
    const stage = LOCAL_SNAPSHOT_STAGING;
    const tarArgs = ["-cf", stage, ...SNAPSHOT_EXCLUDES, "-C", args.root, "."];
    const tar = spawn("tar", tarArgs, { stdio: ["ignore", "ignore", "pipe"] });
    let tarErr = "";
    tar.stderr.on("data", (c) => { tarErr += c.toString("utf8"); });
    tar.on("error", (e) => {
      log(`snapshot tar spawn error: ${e.message}`);
      try { fs.unlinkSync(stage); } catch {}
      resolve();
    });
    tar.on("close", (tarCode) => {
      if (tarCode !== 0) {
        log(`snapshot tar rc=${tarCode}: ${tarErr.slice(0, 400)}`);
        try { fs.unlinkSync(stage); } catch {}
        resolve();
        return;
      }
      const cp = spawn("cp", [stage, args.snapshotPath], { stdio: ["ignore", "ignore", "pipe"] });
      let cpErr = "";
      cp.stderr.on("data", (c) => { cpErr += c.toString("utf8"); });
      cp.on("error", (e) => {
        log(`snapshot cp spawn error: ${e.message}`);
        try { fs.unlinkSync(stage); } catch {}
        resolve();
      });
      cp.on("close", (cpCode) => {
        try { fs.unlinkSync(stage); } catch {}
        if (cpCode !== 0) {
          log(`snapshot cp rc=${cpCode}: ${cpErr.slice(0, 400)}`);
        }
        resolve();
      });
    });
  });
}

// Synchronous snapshot for graceful shutdown (SIGTERM/SIGINT). Caller waits
// for it so the last turn reliably lands on the volume before the process
// exits.
function runSnapshotSync() {
  if (!args.snapshotPath) return;
  const stage = LOCAL_SNAPSHOT_STAGING;
  try {
    const tarArgs = ["-cf", stage, ...SNAPSHOT_EXCLUDES, "-C", args.root, "."];
    const tr = spawnSync("tar", tarArgs, { stdio: ["ignore", "ignore", "pipe"] });
    if (tr.status !== 0) {
      log(`shutdown snapshot tar rc=${tr.status}: ${String(tr.stderr || "").slice(0, 400)}`);
      try { fs.unlinkSync(stage); } catch {}
      return;
    }
    const cr = spawnSync("cp", [stage, args.snapshotPath], { stdio: ["ignore", "ignore", "pipe"] });
    if (cr.status !== 0) {
      log(`shutdown snapshot cp rc=${cr.status}: ${String(cr.stderr || "").slice(0, 400)}`);
    }
  } catch (e) {
    log(`shutdown snapshot failed: ${e.message}`);
  } finally {
    try { fs.unlinkSync(stage); } catch {}
  }
}

// HTTP handler for POST /v1/snapshot. Wraps runSnapshotOnce so the server
// can trigger a synchronous snapshot before it stops/destroys the sandbox.
// Idempotent; safe to call repeatedly. Returns 200 after the tarball has
// landed on the volume (or immediately if --snapshot-path was not configured,
// in which case runSnapshotOnce is a no-op).
async function handleSnapshot(req, res) {
  try {
    await runSnapshotOnce();
    res.writeHead(200, { "content-type": "application/json" });
    res.end(JSON.stringify({ ok: true }));
  } catch (e) {
    log(`snapshot endpoint error: ${e.message}`);
    res.writeHead(500, { "content-type": "application/json" });
    res.end(JSON.stringify({ error: String(e.message || e) }));
  }
}

// Cache the last available_commands_update so late SSE subscribers receive it.
let lastCommandsEvent = null;

function broadcastSse(line) {
  const block = `data: ${line}\n\n`;
  // Cache available_commands_update for late subscribers
  try {
    const msg = JSON.parse(line);
    if (msg && msg.params && msg.params.update &&
        msg.params.update.sessionUpdate === "available_commands_update") {
      lastCommandsEvent = block;
    }
  } catch { /* not JSON, ignore */ }
  for (const res of sseSubscribers) {
    try {
      res.write(block);
    } catch {
      // will be removed on 'close'
    }
  }
}

async function handleAcpLine(line) {
  // Always fan out to SSE subscribers — the Python server's reader
  // consumes this stream for event broadcast + terminal attribution.
  broadcastSse(line);

  // If this is a JSON-RPC response, unblock the waiting POST.
  let msg = null;
  try {
    msg = JSON.parse(line);
  } catch {
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
    res.writeHead(400, { "content-type": "application/json" });
    res.end(JSON.stringify({ error: "invalid JSON body: " + e.message }));
    return;
  }

  const line = JSON.stringify(body) + "\n";
  try {
    acp.stdin.write(line);
  } catch (e) {
    res.writeHead(502, { "content-type": "application/json" });
    res.end(JSON.stringify({ error: "acp stdin write failed: " + e.message }));
    return;
  }

  // Remember session/prompt request ids so the stdout reader can trigger a
  // snapshot when the matching response lands. Covers every client variant —
  // SDK + direct-ACP + external integrations — without parsing update events.
  if (body && body.method === "session/prompt" && "id" in body) {
    pendingPromptIds.add(String(body.id));
  }

  // Notification — fire-and-forget.
  if (!("id" in body)) {
    res.writeHead(202, { "content-type": "application/json" });
    res.end("{}");
    return;
  }

  // Request — wait for the matching response.
  const rpcId = String(body.id);
  const envelope = await new Promise((resolve) => {
    pendingResponses.set(rpcId, resolve);
  });

  res.writeHead(200, { "content-type": "application/json" });
  res.end(JSON.stringify(envelope));
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

async function handleFilesEdit(req, res) {
  let raw = "";
  req.setEncoding("utf8");
  for await (const chunk of req) raw += chunk;

  let body;
  try {
    body = JSON.parse(raw);
  } catch (e) {
    res.writeHead(400, { "content-type": "application/json" });
    res.end(JSON.stringify({ error: "invalid JSON body: " + e.message }));
    return;
  }

  const filePath = body.path;
  const oldString = body.old_string;
  const newString = body.new_string;
  const replaceAll = body.replace_all === true;

  if (typeof filePath !== "string" || !filePath) {
    res.writeHead(400, { "content-type": "application/json" });
    res.end(JSON.stringify({ error: "path required" }));
    return;
  }
  if (typeof oldString !== "string" || typeof newString !== "string") {
    res.writeHead(400, { "content-type": "application/json" });
    res.end(JSON.stringify({ error: "old_string and new_string required" }));
    return;
  }

  const resolvedRoot = path.resolve(args.root);
  const fullPath = path.resolve(resolvedRoot, filePath);

  // Path traversal guard
  if (!fullPath.startsWith(resolvedRoot + "/") && fullPath !== resolvedRoot) {
    res.writeHead(403, { "content-type": "application/json" });
    res.end(JSON.stringify({ error: "path traversal denied" }));
    return;
  }

  // old_string === "" means write/create the entire file
  if (oldString === "") {
    // Create parent directories if needed
    const dir = path.dirname(fullPath);
    fs.mkdirSync(dir, { recursive: true });
    fs.writeFileSync(fullPath, newString, "utf8");
    const stat = fs.statSync(fullPath);
    res.writeHead(200, { "content-type": "application/json" });
    res.end(JSON.stringify({ ok: true, path: filePath, size: stat.size, created: true }));
    return;
  }

  // Regular edit — file must exist
  let content;
  try {
    content = fs.readFileSync(fullPath, "utf8");
  } catch {
    res.writeHead(404, { "content-type": "application/json" });
    res.end(JSON.stringify({ error: "file not found" }));
    return;
  }

  if (oldString === newString) {
    res.writeHead(400, { "content-type": "application/json" });
    res.end(JSON.stringify({ error: "old_string and new_string are identical" }));
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
    res.writeHead(400, { "content-type": "application/json" });
    res.end(JSON.stringify({ error: "old_string not found in file" }));
    return;
  }

  if (count > 1 && !replaceAll) {
    res.writeHead(400, { "content-type": "application/json" });
    res.end(
      JSON.stringify({
        error: `old_string matches ${count} locations; provide more context to make it unique, or set replace_all: true`,
        matches: count,
      }),
    );
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
  res.writeHead(200, { "content-type": "application/json" });
  res.end(
    JSON.stringify({
      ok: true,
      path: filePath,
      size: stat.size,
      replacements: replaceAll ? count : 1,
    }),
  );
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
    res.writeHead(400, { "content-type": "application/json" });
    res.end(JSON.stringify({ error: "invalid JSON body: " + e.message }));
    return;
  }

  const command = body.command;
  const timeout = Math.min(parseInt(body.timeout, 10) || 30, 300) * 1000;

  if (typeof command !== "string" || !command.trim()) {
    res.writeHead(400, { "content-type": "application/json" });
    res.end(JSON.stringify({ error: "command required" }));
    return;
  }

  await new Promise((resolve) => {
    const child = spawn("bash", ["-c", command], {
      cwd: args.root,
      env: process.env,
      stdio: ["ignore", "pipe", "pipe"],
    });

    let stdout = "";
    let stderr = "";
    let stdoutTruncated = false;
    let stderrTruncated = false;
    let timedOut = false;

    const timer = setTimeout(() => {
      timedOut = true;
      try { child.kill("SIGKILL"); } catch {}
    }, timeout);

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
      clearTimeout(timer);
      const result = {
        stdout,
        stderr,
        exit_code: timedOut ? -1 : (code ?? -1),
        timed_out: timedOut,
      };
      if (stdoutTruncated) result.stdout_truncated = true;
      if (stderrTruncated) result.stderr_truncated = true;
      res.writeHead(200, { "content-type": "application/json" });
      res.end(JSON.stringify(result));
      resolve();
    });

    child.on("error", (e) => {
      clearTimeout(timer);
      res.writeHead(500, { "content-type": "application/json" });
      res.end(JSON.stringify({ error: e.message, stdout: "", stderr: "", exit_code: -1 }));
      resolve();
    });
  });
}

async function handleFilesUpload(req, res) {
  let raw = "";
  req.setEncoding("utf8");
  for await (const chunk of req) raw += chunk;

  if (Buffer.byteLength(raw) > MAX_FILE_SIZE) {
    res.writeHead(413, { "content-type": "application/json" });
    res.end(JSON.stringify({ error: "payload too large" }));
    return;
  }

  let body;
  try { body = JSON.parse(raw); } catch (e) {
    res.writeHead(400, { "content-type": "application/json" });
    res.end(JSON.stringify({ error: "invalid JSON: " + e.message }));
    return;
  }

  const filePath = body.path;
  const content = body.content;
  if (typeof filePath !== "string" || !filePath || typeof content !== "string") {
    res.writeHead(400, { "content-type": "application/json" });
    res.end(JSON.stringify({ error: "path and content (base64) required" }));
    return;
  }

  const resolvedRoot = path.resolve(args.root);
  const fullPath = path.resolve(resolvedRoot, filePath);
  if (!fullPath.startsWith(resolvedRoot + "/") && fullPath !== resolvedRoot) {
    res.writeHead(403, { "content-type": "application/json" });
    res.end(JSON.stringify({ error: "path traversal denied" }));
    return;
  }

  fs.mkdirSync(path.dirname(fullPath), { recursive: true });
  fs.writeFileSync(fullPath, Buffer.from(content, "base64"));
  const stat = fs.statSync(fullPath);
  res.writeHead(200, { "content-type": "application/json" });
  res.end(JSON.stringify({ ok: true, path: filePath, size: stat.size }));
}

async function handleFilesDelete(req, res) {
  let raw = "";
  req.setEncoding("utf8");
  for await (const chunk of req) raw += chunk;

  let body;
  try { body = JSON.parse(raw); } catch (e) {
    res.writeHead(400, { "content-type": "application/json" });
    res.end(JSON.stringify({ error: "invalid JSON: " + e.message }));
    return;
  }

  const filePath = body.path;
  if (typeof filePath !== "string" || !filePath) {
    res.writeHead(400, { "content-type": "application/json" });
    res.end(JSON.stringify({ error: "path required" }));
    return;
  }

  const resolvedRoot = path.resolve(args.root);
  const fullPath = path.resolve(resolvedRoot, filePath);
  if (!fullPath.startsWith(resolvedRoot + "/") && fullPath !== resolvedRoot) {
    res.writeHead(403, { "content-type": "application/json" });
    res.end(JSON.stringify({ error: "path traversal denied" }));
    return;
  }

  if (!fs.existsSync(fullPath)) {
    res.writeHead(404, { "content-type": "application/json" });
    res.end(JSON.stringify({ error: "not found" }));
    return;
  }

  const stat = fs.statSync(fullPath);
  if (stat.isDirectory()) {
    fs.rmSync(fullPath, { recursive: true });
  } else {
    fs.unlinkSync(fullPath);
  }
  res.writeHead(200, { "content-type": "application/json" });
  res.end(JSON.stringify({ ok: true }));
}

async function handleFilesRename(req, res) {
  let raw = "";
  req.setEncoding("utf8");
  for await (const chunk of req) raw += chunk;

  let body;
  try { body = JSON.parse(raw); } catch (e) {
    res.writeHead(400, { "content-type": "application/json" });
    res.end(JSON.stringify({ error: "invalid JSON: " + e.message }));
    return;
  }

  const filePath = body.path;
  const newPath = body.new_path;
  if (typeof filePath !== "string" || !filePath || typeof newPath !== "string" || !newPath) {
    res.writeHead(400, { "content-type": "application/json" });
    res.end(JSON.stringify({ error: "path and new_path required" }));
    return;
  }

  const resolvedRoot = path.resolve(args.root);
  const fullPath = path.resolve(resolvedRoot, filePath);
  const newFullPath = path.resolve(resolvedRoot, newPath);
  if (!fullPath.startsWith(resolvedRoot + "/") && fullPath !== resolvedRoot) {
    res.writeHead(403, { "content-type": "application/json" });
    res.end(JSON.stringify({ error: "path traversal denied" }));
    return;
  }
  if (!newFullPath.startsWith(resolvedRoot + "/") && newFullPath !== resolvedRoot) {
    res.writeHead(403, { "content-type": "application/json" });
    res.end(JSON.stringify({ error: "path traversal denied (new_path)" }));
    return;
  }

  if (!fs.existsSync(fullPath)) {
    res.writeHead(404, { "content-type": "application/json" });
    res.end(JSON.stringify({ error: "not found" }));
    return;
  }

  fs.mkdirSync(path.dirname(newFullPath), { recursive: true });
  fs.renameSync(fullPath, newFullPath);
  res.writeHead(200, { "content-type": "application/json" });
  res.end(JSON.stringify({ ok: true, path: newPath }));
}

function handleFilesDownload(req, res) {
  const u = new URL(req.url, `http://${req.headers.host}`);
  const filePath = u.searchParams.get("path");
  if (!filePath) {
    res.writeHead(400, { "content-type": "application/json" });
    res.end(JSON.stringify({ error: "path query param required" }));
    return;
  }

  const resolvedRoot = path.resolve(args.root);
  const fullPath = path.resolve(resolvedRoot, filePath);
  if (!fullPath.startsWith(resolvedRoot + "/") && fullPath !== resolvedRoot) {
    res.writeHead(403, { "content-type": "application/json" });
    res.end(JSON.stringify({ error: "path traversal denied" }));
    return;
  }

  if (!fs.existsSync(fullPath) || fs.statSync(fullPath).isDirectory()) {
    res.writeHead(404, { "content-type": "application/json" });
    res.end(JSON.stringify({ error: "file not found" }));
    return;
  }

  const fileName = path.basename(fullPath);
  const ext = path.extname(fileName).toLowerCase();
  const mimeTypes = { ".html": "text/html", ".js": "text/javascript", ".json": "application/json", ".css": "text/css", ".png": "image/png", ".jpg": "image/jpeg", ".jpeg": "image/jpeg", ".gif": "image/gif", ".svg": "image/svg+xml", ".pdf": "application/pdf", ".zip": "application/zip", ".tar": "application/x-tar", ".gz": "application/gzip" };
  const contentType = mimeTypes[ext] || "application/octet-stream";

  const data = fs.readFileSync(fullPath);
  res.writeHead(200, {
    "content-type": contentType,
    "content-disposition": `attachment; filename="${fileName}"`,
    "content-length": data.length,
  });
  res.end(data);
}

const server = http.createServer((req, res) => {
  if (req.url === "/v1/health" || req.url === "/health") {
    const body = JSON.stringify({
      status: "ok",
      acp_pid: acp.pid,
      acp_alive: acp.exitCode === null,
      sse_subscribers: sseSubscribers.size,
      pending_responses: pendingResponses.size,
      cwd: args.root,
    });
    res.writeHead(200, { "content-type": "application/json" });
    res.end(body);
    return;
  }
  if (req.url && req.url.startsWith("/v1/acp/")) {
    if (req.method === "POST") {
      handlePost(req, res).catch((e) => {
        log("POST handler crashed: " + e.stack);
        try {
          res.writeHead(500, { "content-type": "application/json" });
          res.end(JSON.stringify({ error: e.message }));
        } catch {}
      });
      return;
    }
    if (req.method === "GET") {
      handleSse(req, res);
      return;
    }
  }
  if (req.url === "/v1/snapshot" && req.method === "POST") {
    handleSnapshot(req, res).catch((e) => {
      log("snapshot handler crashed: " + e.stack);
      try {
        res.writeHead(500, { "content-type": "application/json" });
        res.end(JSON.stringify({ error: e.message }));
      } catch {}
    });
    return;
  }
  if (req.url && req.url.startsWith("/v1/files/tree") && req.method === "GET") {
    const u = new URL(req.url, `http://${req.headers.host}`);
    const root = u.searchParams.get("root") || args.root || "/tmp";
    const resolved = path.resolve(root);
    if (!fs.existsSync(resolved)) {
      res.writeHead(404, { "content-type": "application/json" });
      res.end(JSON.stringify({ error: "root path not found" }));
      return;
    }
    const tree = walk(resolved, "", 0);
    res.writeHead(200, { "content-type": "application/json" });
    res.end(JSON.stringify(tree));
    return;
  }
  if (req.url && req.url.startsWith("/v1/files/read") && req.method === "GET") {
    const u = new URL(req.url, `http://${req.headers.host}`);
    const filePath = u.searchParams.get("path");
    const root = u.searchParams.get("root") || args.root || "/tmp";
    if (!filePath) {
      res.writeHead(400, { "content-type": "application/json" });
      res.end(JSON.stringify({ error: "path query param required" }));
      return;
    }
    const resolvedRoot = path.resolve(root);
    const fullPath = path.resolve(resolvedRoot, filePath);
    // Path traversal guard — resolved path must stay under root
    if (!fullPath.startsWith(resolvedRoot + "/") && fullPath !== resolvedRoot) {
      res.writeHead(403, { "content-type": "application/json" });
      res.end(JSON.stringify({ error: "path traversal denied" }));
      return;
    }
    let stat;
    try {
      stat = fs.statSync(fullPath);
    } catch {
      res.writeHead(404, { "content-type": "application/json" });
      res.end(JSON.stringify({ error: "file not found" }));
      return;
    }
    if (stat.isDirectory()) {
      res.writeHead(400, { "content-type": "application/json" });
      res.end(JSON.stringify({ error: "path is a directory" }));
      return;
    }
    if (stat.size > MAX_FILE_SIZE) {
      res.writeHead(413, { "content-type": "application/json" });
      res.end(
        JSON.stringify({
          error: `file too large: ${stat.size} bytes (max ${MAX_FILE_SIZE})`,
        }),
      );
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

    const result = {
      content,
      name: path.basename(fullPath),
      size: stat.size,
      binary: isBinary,
      image: isImage,
      audio: isAudio,
      video: isVideo,
      pdf: isPdf,
    };
    res.writeHead(200, { "content-type": "application/json" });
    res.end(JSON.stringify(result));
    return;
  }
  if (req.url && req.url.startsWith("/v1/files/edit") && req.method === "POST") {
    handleFilesEdit(req, res).catch((e) => {
      log("files/edit handler crashed: " + e.stack);
      try {
        res.writeHead(500, { "content-type": "application/json" });
        res.end(JSON.stringify({ error: e.message }));
      } catch {}
    });
    return;
  }
  if (req.url === "/v1/exec" && req.method === "POST") {
    handleExec(req, res).catch((e) => {
      log("exec handler crashed: " + e.stack);
      try {
        res.writeHead(500, { "content-type": "application/json" });
        res.end(JSON.stringify({ error: e.message }));
      } catch {}
    });
    return;
  }
  if (req.url && req.url.startsWith("/v1/files/upload") && req.method === "POST") {
    handleFilesUpload(req, res).catch((e) => {
      log("files/upload handler crashed: " + e.stack);
      try {
        res.writeHead(500, { "content-type": "application/json" });
        res.end(JSON.stringify({ error: e.message }));
      } catch {}
    });
    return;
  }
  if (req.url && req.url.startsWith("/v1/files/delete") && req.method === "POST") {
    handleFilesDelete(req, res).catch((e) => {
      log("files/delete handler crashed: " + e.stack);
      try {
        res.writeHead(500, { "content-type": "application/json" });
        res.end(JSON.stringify({ error: e.message }));
      } catch {}
    });
    return;
  }
  if (req.url && req.url.startsWith("/v1/files/rename") && req.method === "POST") {
    handleFilesRename(req, res).catch((e) => {
      log("files/rename handler crashed: " + e.stack);
      try {
        res.writeHead(500, { "content-type": "application/json" });
        res.end(JSON.stringify({ error: e.message }));
      } catch {}
    });
    return;
  }
  if (req.url && req.url.startsWith("/v1/files/download") && req.method === "GET") {
    handleFilesDownload(req, res);
    return;
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
