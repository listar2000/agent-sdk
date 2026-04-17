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
 *   GET  /v1/health             liveness — {status, acp_pid, acp_alive}
 *
 * One claude-agent-acp subprocess per supervisor, shared across all
 * POSTs and SSE subscribers. The session_id in the URL is accepted but
 * ignored — the inner ACP session id goes in JSON-RPC params as usual.
 */
const http = require("node:http");
const { spawn } = require("node:child_process");
const fs = require("node:fs");
const path = require("node:path");
const SSE_HEARTBEAT_MS = 25000;

function parseArgs(argv) {
  const out = {
    port: 9100,
    acp: null,
    host: "0.0.0.0",
    root: "/tmp",
    acpArgs: [],
  };
  for (let i = 2; i < argv.length; i++) {
    const a = argv[i];
    if (a === "--port") out.port = parseInt(argv[++i], 10);
    else if (a === "--host") out.host = argv[++i];
    else if (a === "--acp") out.acp = argv[++i];
    else if (a === "--root" || a === "--cwd") out.root = argv[++i];
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

const acp = spawn(args.acp, args.acpArgs, {
  stdio: ["pipe", "pipe", "pipe"],
  env: process.env,
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

function broadcastSse(line) {
  const block = `data: ${line}\n\n`;
  for (const res of sseSubscribers) {
    try {
      res.write(block);
    } catch {
      // will be removed on 'close'
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

    // Always fan out to SSE subscribers — the Python server's reader
    // consumes this stream for event broadcast + terminal attribution.
    broadcastSse(line);

    // If this is a JSON-RPC response, unblock the waiting POST.
    let msg = null;
    try {
      msg = JSON.parse(line);
    } catch {
      continue;
    }
    if (
      msg &&
      typeof msg === "object" &&
      "id" in msg &&
      ("result" in msg || "error" in msg)
    ) {
      const rid = String(msg.id);
      const resolver = pendingResponses.get(rid);
      if (resolver) {
        pendingResponses.delete(rid);
        resolver(msg);
      }
    }
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
  res.writeHead(404, { "content-type": "text/plain" });
  res.end("not found\n");
});

server.listen(args.port, args.host, () => {
  log(`listening on ${args.host}:${args.port}`);
});

function shutdown() {
  log("shutting down");
  try {
    acp.stdin.end();
  } catch {}
  try {
    server.close();
  } catch {}
  setTimeout(() => process.exit(0), 500);
}
process.on("SIGTERM", shutdown);
process.on("SIGINT", shutdown);
