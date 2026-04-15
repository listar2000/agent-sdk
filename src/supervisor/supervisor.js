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
const SSE_HEARTBEAT_MS = 25000;

function parseArgs(argv) {
    const out = { port: 9100, acp: null, host: "0.0.0.0", cwd: "/tmp", acpArgs: [] };
    for (let i = 2; i < argv.length; i++) {
        const a = argv[i];
        if (a === "--port") out.port = parseInt(argv[++i], 10);
        else if (a === "--host") out.host = argv[++i];
        else if (a === "--acp") out.acp = argv[++i];
        else if (a === "--cwd") out.cwd = argv[++i];
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
    cwd: args.cwd,
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
        if (msg && typeof msg === "object" && "id" in msg && ("result" in msg || "error" in msg)) {
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
        "connection": "keep-alive",
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

const server = http.createServer((req, res) => {
    if (req.url === "/v1/health" || req.url === "/health") {
        const body = JSON.stringify({
            status: "ok",
            acp_pid: acp.pid,
            acp_alive: acp.exitCode === null,
            sse_subscribers: sseSubscribers.size,
            pending_responses: pendingResponses.size,
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
    res.writeHead(404, { "content-type": "text/plain" });
    res.end("not found\n");
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
