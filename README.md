# Agent SDK

Python SDK and orchestration server for running AI agents in sandboxes. Supports Claude Code, Codex, OpenCode, and any agent that speaks the ACP protocol.

Agents run inside isolated sandboxes (local subprocess, Docker, or Daytona cloud). The SDK handles provisioning, session management, streaming responses, and multi-agent orchestration.

## Quick start

```bash
pip install agent-sdk
```

```python
from agent_sdk import Agent

agent = Agent("my-agent", provider="local")
response = agent.run("Say hello and create a file called hello.py")
print(response)
```

### Async

```python
import asyncio
from agent_sdk import Agent

async def main():
    agent = Agent("my-agent", provider="local")
    async for chunk in agent.astream("Analyze this codebase"):
        print(chunk, end="", flush=True)

asyncio.run(main())
```

### Daytona (cloud sandboxes)

```bash
export DAYTONA_API_KEY=...
export ANTHROPIC_API_KEY=...
```

```python
agent = Agent("cloud-agent", provider="daytona")
response = agent.run("What OS am I running on?")
```

## Architecture

```
┌─────────────┐     ┌──────────────────┐     ┌─────────────────┐
│  Your code  │────▶│  API server      │────▶│  sandbox-agent  │
│  (SDK)      │     │  (orchestrator)  │     │  (in sandbox)   │
└─────────────┘     └──────────────────┘     └─────────────────┘
                           │                         │
                    Provisions sandbox          Runs Claude/Codex
                    Manages sessions            Streams via SSE
                    Routes messages             Executes tools
```

- **SDK** (`agent_sdk/`) — async Python client. `Agent` class with `run()`, `arun()`, `astream()`, `dispatch()`.
- **Server** (`api/`) — FastAPI orchestration layer. Manages agents, sandboxes, and sessions. Proxies ACP protocol to sandbox-agent instances.
- **Providers** — local (subprocess), Docker, Daytona (cloud). Pluggable via `provider=` param.

## SDK reference

### Agent

```python
agent = Agent(
    "name",
    provider="local",           # "local", "docker", or "daytona"
    tools=["Bash", "Read"],     # limit available tools
    prompt="You are helpful.",   # system prompt
    model="sonnet",             # model override
    skills={...},               # Claude Code skills config
    dockerfile="path/to/Dockerfile",  # custom sandbox image
)

# Sync
response = agent.run("do something")

# Async
response = await agent.arun("do something")

# Streaming
async for chunk in agent.astream("do something"):
    print(chunk, end="")

# Fire-and-forget (background thread)
thread = agent.dispatch("do something")

# Session persists across calls
agent.run("remember the number 42")
agent.run("what number did I say?")  # same session, remembers context
```

### Multi-agent orchestration

```python
from agent_sdk import Agent, chain, parallel, race, map_reduce, Pipeline

# Chain: output of each feeds into next
result = await chain([analyzer, fixer], "Review src/main.py")

# Parallel: all run concurrently
results = await parallel([reviewer1, reviewer2], "Review this PR")

# Race: first to finish wins
result = await race([fast_agent, thorough_agent], "Solve this")

# Map-reduce: distribute work, combine results
result = await map_reduce(workers, items, reducer=combiner)

# Pipeline: named stages
p = Pipeline()
p.add("analyze", analyzer)
p.add("fix", fixer)
result = await p.run("Fix bugs in src/")
```

### Sandbox operations

```python
# Filesystem
files = await agent.list_dir("/app")
content = await agent.read_file("/app/main.py")
await agent.write_file("/app/main.py", "print('hello')")
await agent.upload_files({"main.py": "print('hi')", "data.csv": csv_bytes})

# Execute commands
result = await agent.exec("python", args=["main.py"])
output = await agent.shell("ls -la /app")

# Persistent processes
proc = await agent.start_process("python", args=["server.py"])
logs = await agent.get_process_logs(proc["id"])
await agent.stop_process(proc["id"])
```

## Running the server

```bash
# Local development
uvicorn src.api.server:app --port 7778

# Docker
docker build -t agent-sdk .
docker run -p 7778:7778 agent-sdk

# The SDK connects to http://localhost:7778 by default
# Override with: Agent(..., api_url="https://your-server.com")
```

## Repo layout

```
src/
  agent_sdk/         Python SDK client
    client.py          Agent class
    orchestrate.py     chain, parallel, race, map_reduce, Pipeline
    persist.py         SQLite session persistence
  api/               Orchestration server
    server.py          FastAPI endpoints
    providers.py       Sandbox lifecycle (local, Docker, Daytona)
    sandbox_agent_client.py  Low-level ACP client
    sse.py             SSE stream parser
scripts/
  mention_dispatcher.py  Hive inbox polling dispatcher
examples/            Demo scripts
docs/                API spec, architecture docs, diagrams
assets/              Data model diagram
```

## Docs

- [API Reference](docs/api.md)
- [Sandbox-Agent SDK Reference](docs/rivet_sandbox_agent_sdk_reference.md)
- [Sandbox Integration Design](docs/sandbox-integration-design.md)
- [Multi-Agent Workspace Design](docs/multi-agent-workspace-design.md)
