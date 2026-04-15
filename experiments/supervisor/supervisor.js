#!/usr/bin/env node
/**
 * Minimal ACP supervisor — spawns claude-agent-acp, bridges stdio ↔ WebSocket.
 *
 * Usage: node supervisor.js --port 9100 --acp /path/to/claude-agent-acp
 *
 * HTTP endpoints:
 *   GET  /health      → {status, acp_pid, ws_connected}
 *   WS   /acp         → text frames are JSON-RPC; bidirectional bridge
 *
 * One ws client at a time. For spike use only.
 */
const http = require("node:http");
const { spawn } = require("node:child_process");
const { WebSocketServer } = require("ws");

function parseArgs(argv) {
    const out = { port: 9100, acp: null, host: "0.0.0.0" };
    for (let i = 2; i < argv.length; i++) {
        const a = argv[i];
        if (a === "--port") out.port = parseInt(argv[++i], 10);
        else if (a === "--host") out.host = argv[++i];
        else if (a === "--acp") out.acp = argv[++i];
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

// Spawn claude-agent-acp once at startup; keep it alive for the supervisor lifetime.
const acp = spawn(args.acp, [], {
    stdio: ["pipe", "pipe", "pipe"],
    env: process.env,
    cwd: "/tmp",
});
log("spawned acp pid=" + acp.pid);

acp.stderr.on("data", (chunk) => {
    process.stderr.write("[acp-stderr] " + chunk.toString());
});

acp.on("exit", (code, signal) => {
    log(`acp exited code=${code} signal=${signal}`);
    process.exit(code || 1);
});

// One ws client at a time.
let currentWs = null;

// Forward acp stdout lines to the current ws (line-delimited JSON-RPC).
let stdoutBuf = "";
acp.stdout.on("data", (chunk) => {
    stdoutBuf += chunk.toString("utf8");
    let idx;
    while ((idx = stdoutBuf.indexOf("\n")) !== -1) {
        const line = stdoutBuf.slice(0, idx);
        stdoutBuf = stdoutBuf.slice(idx + 1);
        if (!line) continue;
        if (currentWs && currentWs.readyState === currentWs.OPEN) {
            currentWs.send(line);
        } else {
            // No client — drop. Could buffer but for the spike we don't.
        }
    }
});

const server = http.createServer((req, res) => {
    if (req.url === "/health") {
        const body = JSON.stringify({
            status: "ok",
            acp_pid: acp.pid,
            acp_alive: acp.exitCode === null,
            ws_connected: currentWs !== null,
        });
        res.writeHead(200, { "content-type": "application/json" });
        res.end(body);
        return;
    }
    res.writeHead(404, { "content-type": "text/plain" });
    res.end("not found\n");
});

const wss = new WebSocketServer({ server, path: "/acp" });
wss.on("connection", (ws) => {
    if (currentWs !== null) {
        ws.close(4000, "another client already connected");
        return;
    }
    currentWs = ws;
    log("ws client connected");

    ws.on("message", (data, isBinary) => {
        const text = isBinary ? data.toString("utf8") : data.toString();
        // Ensure single-line frame terminated with \n for the acp stdin reader.
        const line = text.replace(/\n+$/, "") + "\n";
        try {
            acp.stdin.write(line);
        } catch (e) {
            log("acp stdin write failed: " + e.message);
            ws.close(4002, "acp stdin error");
        }
    });

    ws.on("close", () => {
        if (currentWs === ws) currentWs = null;
        log("ws client disconnected");
    });

    ws.on("error", (e) => {
        log("ws error: " + e.message);
    });
});

server.listen(args.port, args.host, () => {
    log(`listening on ${args.host}:${args.port}`);
});

function shutdown() {
    log("shutting down");
    try { acp.stdin.end(); } catch {}
    try { server.close(); } catch {}
    setTimeout(() => process.exit(0), 500);
}
process.on("SIGTERM", shutdown);
process.on("SIGINT", shutdown);
