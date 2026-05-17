#!/usr/bin/env node
/**
 * Mock ACP — replaces claude-agent-acp / opencode for server-saturation
 * benchmarks. Speaks the same JSON-RPC-over-stdio protocol that
 * supervisor.js expects, but the prompt handler emits a configurable
 * burst of session/update events with NO LLM call, then returns
 * stopReason. Lets us drive a high SSE event rate at near-zero
 * per-prompt latency, isolating server-side CPU scaling.
 *
 * Env:
 *   MOCK_ACP_EVENTS_PER_PROMPT  text chunks to emit (default 50)
 *   MOCK_ACP_CHUNK_SIZE         chars per chunk (default 8)
 *   MOCK_ACP_INTER_EVENT_MS     delay between chunks (default 0)
 */
const readline = require("readline");

const EVENTS = parseInt(process.env.MOCK_ACP_EVENTS_PER_PROMPT || "50", 10);
const CHUNK_SIZE = parseInt(process.env.MOCK_ACP_CHUNK_SIZE || "8", 10);
const INTER_EVENT_MS = parseInt(process.env.MOCK_ACP_INTER_EVENT_MS || "0", 10);

function emit(obj) {
  process.stdout.write(JSON.stringify(obj) + "\n");
}

const sleep = (ms) => (ms <= 0 ? Promise.resolve() : new Promise((r) => setTimeout(r, ms)));

const PHRASE = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

function chunkText(i) {
  // Deterministic ~CHUNK_SIZE-char payload per chunk index.
  const s = PHRASE.repeat(Math.ceil(CHUNK_SIZE / PHRASE.length));
  return s.slice(0, CHUNK_SIZE);
}

async function handlePrompt(msg) {
  const sid = msg.params && msg.params.sessionId;
  for (let i = 0; i < EVENTS; i++) {
    emit({
      jsonrpc: "2.0",
      method: "session/update",
      params: {
        sessionId: sid,
        update: {
          sessionUpdate: "agent_message_delta",
          content: { type: "text", text: chunkText(i) },
        },
      },
    });
    if (INTER_EVENT_MS > 0) await sleep(INTER_EVENT_MS);
  }
  // Stop the turn.
  emit({
    jsonrpc: "2.0",
    id: msg.id,
    result: { stopReason: "end_turn" },
  });
}

function handleInit(msg) {
  // ACP initialize handshake response.
  emit({
    jsonrpc: "2.0",
    id: msg.id,
    result: {
      protocolVersion: 1,
      agentCapabilities: {
        loadSession: true, promptCapabilities: { audio: false, embeddedContext: false, image: false },
      },
      authMethods: [],
    },
  });
}

function handleSessionNew(msg) {
  const newSid = `mock-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
  emit({
    jsonrpc: "2.0",
    id: msg.id,
    result: {
      sessionId: newSid,
      models: [{ modelId: "mock", name: "mock" }],
      modes: { availableModes: [{ modeId: "default", name: "default" }], currentModeId: "default" },
      configOptions: { thoughtLevels: [] },
    },
  });
}

function handleSessionLoad(msg) {
  emit({ jsonrpc: "2.0", id: msg.id, result: null });
}

function handleSetModel(msg)        { emit({ jsonrpc: "2.0", id: msg.id, result: null }); }
function handleSetMode(msg)         { emit({ jsonrpc: "2.0", id: msg.id, result: null }); }
function handleSetThoughtLevel(msg) { emit({ jsonrpc: "2.0", id: msg.id, result: null }); }
function handleCancel(msg) {
  // session/cancel is a notification (no id usually). If id present, ack.
  if ("id" in msg) emit({ jsonrpc: "2.0", id: msg.id, result: null });
}

const rl = readline.createInterface({ input: process.stdin, crlfDelay: Infinity });
rl.on("line", async (line) => {
  if (!line.trim()) return;
  let msg;
  try { msg = JSON.parse(line); } catch { return; }
  if (!msg || typeof msg !== "object") return;
  try {
    switch (msg.method) {
      case "initialize":            return handleInit(msg);
      case "session/new":           return handleSessionNew(msg);
      case "session/load":          return handleSessionLoad(msg);
      case "session/prompt":        return handlePrompt(msg);
      case "session/setModel":      return handleSetModel(msg);
      case "session/setMode":       return handleSetMode(msg);
      case "session/setThoughtLevel": return handleSetThoughtLevel(msg);
      case "session/cancel":        return handleCancel(msg);
      default:
        if ("id" in msg) {
          emit({
            jsonrpc: "2.0", id: msg.id,
            error: { code: -32601, message: `unknown method: ${msg.method}` },
          });
        }
    }
  } catch (e) {
    if (msg && "id" in msg) {
      emit({ jsonrpc: "2.0", id: msg.id,
             error: { code: -32603, message: String(e && e.message || e) }});
    }
  }
});

process.on("SIGTERM", () => process.exit(0));
process.on("SIGINT", () => process.exit(0));
