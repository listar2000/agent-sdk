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
// Daytona-only path. Idempotent for both fresh sandboxes (empty root →
// snapshot populates it) and stop/restart (local /home/daytona already
// has the same state, tar extract overlays it harmlessly). Using "always
// restore when snapshot exists" is simpler than trying to detect
// fresh-vs-restarted: Daytona VM images pre-populate /home/daytona with
// dotfiles (.bashrc, etc.), so a readdir-empty check false-negatives on
// a freshly provisioned sandbox and we'd fail to restore the workspace.
try {
  fs.mkdirSync(args.root, { recursive: true });
} catch (e) {
  log(`mkdir root failed: ${e.message}`);
}

if (args.snapshotPath && fs.existsSync(args.snapshotPath)) {
  log(`restoring workspace from ${args.snapshotPath}`);
  const r = spawnSync("tar", ["-xf", args.snapshotPath, "-C", args.root], {
    stdio: ["ignore", "inherit", "inherit"],
  });
  if (r.status !== 0) {
    log(`restore exited rc=${r.status}; continuing without restore`);
  }
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
    // Snapshot is no longer triggered per-turn. The server now calls
    // POST /v1/snapshot before stopping the sandbox (snapshot_and_stop in
    // server.py). Turn-end just resolves the pending POST response — no
    // blocking tar/cp on the critical path.
    const rid = String(msg.id);
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
