"""Multi-agent orchestration helpers.

Simple utilities for running agents in sequence or parallel,
passing results between them.

Usage::

    from agent_sdk import Agent
    from agent_sdk.orchestrate import chain, parallel, Pipeline

    # Chain: output of each feeds into next
    result = await chain([
        Agent("analyzer", provider="local", prompt="Analyze code"),
        Agent("fixer", provider="local", prompt="Fix issues found"),
    ], "Review src/main.py")

    # Parallel: all agents run concurrently, results collected
    results = await parallel([
        Agent("reviewer1", provider="local"),
        Agent("reviewer2", provider="local"),
    ], "Review this PR")

    # Pipeline: named stages with custom routing
    pipeline = Pipeline()
    pipeline.add("analyze", Agent("analyzer", provider="local"))
    pipeline.add("fix", Agent("fixer", provider="local"))
    result = await pipeline.run("Fix bugs in src/")
"""

import asyncio
import logging
from typing import Any

from .client import Agent

log = logging.getLogger(__name__)


async def chain(agents: list[Agent], message: str) -> str:
    """Run agents in sequence, passing each output as input to the next.

    Returns the final agent's response.
    """
    current = message
    for agent in agents:
        current = await agent.arun(current)
    return current


async def parallel(agents: list[Agent], message: str,
                   return_exceptions: bool = False) -> list[str | BaseException]:
    """Run all agents concurrently with the same message.

    Returns list of responses in agent order.
    With return_exceptions=True, failed agents return Exception objects instead of raising.
    """
    tasks = [agent.arun(message) for agent in agents]
    return await asyncio.gather(*tasks, return_exceptions=return_exceptions)


async def map_reduce(
    agents: list[Agent],
    items: list[str],
    reducer: Agent | None = None,
) -> list[str] | str:
    """Map items across agents in parallel, optionally reduce results.

    Each agent[i] processes items[i]. If more items than agents, agents are
    reused round-robin. If reducer is provided, all results are combined and
    sent to the reducer agent.

    Usage::

        results = await map_reduce(
            agents=[Agent("worker1", ...), Agent("worker2", ...)],
            items=["Review file1.py", "Review file2.py", "Review file3.py"],
            reducer=Agent("combiner", prompt="Combine reviews"),
        )
    """
    if not agents:
        raise ValueError("At least one agent required")

    # Per-agent locks prevent concurrent arun() calls to the same agent,
    # which would confuse the underlying LLM with interleaved prompts
    agent_locks: dict[int, asyncio.Lock] = {id(a): asyncio.Lock() for a in agents}

    async def _process(idx: int) -> str:
        agent = agents[idx % len(agents)]
        async with agent_locks[id(agent)]:
            return await agent.arun(items[idx])

    tasks = [_process(i) for i in range(len(items))]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    # Convert exceptions to error strings
    str_results = []
    for r in results:
        if isinstance(r, BaseException):
            str_results.append(f"ERROR: {r}")
        else:
            str_results.append(r)

    if reducer is None:
        return str_results

    # Combine and reduce
    combined = "\n\n---\n\n".join(
        f"## Result {i+1}\n{r}" for i, r in enumerate(str_results)
    )
    return await reducer.arun(combined)


async def conversation(agent: Agent, messages: list[str]) -> list[str]:
    """Send multiple messages to the same agent sequentially (multi-turn).

    Returns list of responses in order. The agent maintains context between messages.

    Usage::

        responses = await conversation(agent, [
            "Read src/main.py and summarize it",
            "Now find any bugs in it",
            "Fix the bugs you found",
        ])
    """
    responses = []
    for msg in messages:
        resp = await agent.arun(msg)
        responses.append(resp)
    return responses


async def retry(agent: Agent, message: str, max_attempts: int = 3) -> str:
    """Retry an agent call on failure.

    Returns the first successful response. Raises the last exception if all attempts fail.
    """
    if max_attempts < 1:
        raise ValueError("max_attempts must be >= 1")
    last_error: Exception | None = None
    for attempt in range(max_attempts):
        try:
            return await agent.arun(message)
        except Exception as e:
            last_error = e
            log.warning("Agent '%s' attempt %d/%d failed: %s", agent.name, attempt + 1, max_attempts, e)
    raise last_error  # type: ignore[misc]


async def _cancel_and_await(tasks) -> None:
    """Cancel tasks and await their completion so cleanup code (e.g. httpx aclose) runs."""
    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)


async def race(agents: list[Agent], message: str) -> str:
    """Send message to all agents, return the first *successful* response.

    Cancels remaining agents once one succeeds. If a fast agent fails but
    a slow one succeeds, the slow one's result is returned. Only raises
    if all agents fail.
    """
    if not agents:
        raise ValueError("At least one agent required")

    async def _run(agent: Agent) -> str:
        return await agent.arun(message)

    remaining = {asyncio.create_task(_run(a)) for a in agents}
    last_error: BaseException | None = None
    try:
        while remaining:
            done, remaining = await asyncio.wait(remaining, return_when=asyncio.FIRST_COMPLETED)
            for t in done:
                if t.exception() is None:
                    # Success — cancel the rest and await their cleanup
                    await _cancel_and_await(remaining)
                    return t.result()
                last_error = t.exception()
        # All failed
        raise last_error  # type: ignore[misc]
    except Exception:
        await _cancel_and_await(remaining)
        raise


async def benchmark(agent: Agent, messages: list[str]) -> dict:
    """Benchmark agent response times.

    Sends each message and records timing. Returns stats dict.

    Usage::

        stats = await benchmark(agent, ["Hello", "What is 2+2?", "Summarize X"])
        print(f"Avg: {stats['avg_ms']:.0f}ms, Max: {stats['max_ms']:.0f}ms")
    """
    import time as _time
    times = []
    for msg in messages:
        start = _time.monotonic()
        await agent.arun(msg)
        elapsed = (_time.monotonic() - start) * 1000  # ms
        times.append(elapsed)

    return {
        "count": len(times),
        "times_ms": times,
        "avg_ms": sum(times) / len(times) if times else 0,
        "min_ms": min(times) if times else 0,
        "max_ms": max(times) if times else 0,
        "total_ms": sum(times),
    }


class Pipeline:
    """Named multi-stage agent pipeline with result routing.

    Usage::

        p = Pipeline()
        p.add("analyze", analyzer_agent)
        p.add("fix", fixer_agent)
        p.add("review", reviewer_agent)
        final = await p.run("Fix bugs in src/")
        # Each stage gets: "<stage_name> input:\\n<previous_output>"

        # Access intermediate results
        print(p.results["analyze"])
    """

    def __init__(self):
        self.stages: list[tuple[str, Agent]] = []
        self.results: dict[str, str] = {}

    def add(self, name: str, agent: Agent) -> "Pipeline":
        """Add a named stage. Returns self for chaining."""
        self.stages.append((name, agent))
        return self

    async def run(self, message: str) -> str:
        """Execute all stages in sequence. Returns final stage output."""
        current = message
        self.results.clear()
        for name, agent in self.stages:
            try:
                current = await agent.arun(current)
                self.results[name] = current
                log.info("Pipeline stage '%s' completed (%d chars)", name, len(current))
            except Exception as e:
                log.error("Pipeline stage '%s' failed: %s", name, e)
                self.results[name] = f"ERROR: {e}"
                raise
        return current

    async def aclose(self) -> None:
        """Close all agent clients."""
        for _, agent in self.stages:
            try:
                await agent.aclose()
            except Exception:
                pass
