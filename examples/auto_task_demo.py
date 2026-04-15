"""Demo: full auto-task pipeline — extract from trace, generate task, test in sandbox.

Prerequisites:
  Ensure the API server is reachable.
  curl https://agent-sdk-server-production.up.railway.app/health

Usage:
  # Generate tasks from latest Claude Code session in current dir:
  python examples/auto_task_demo.py generate
  python examples/auto_task_demo.py generate --test

  # Generate from a specific trace:
  python examples/auto_task_demo.py generate --trace ~/.claude/projects/-home-ubuntu/abc123.jsonl

  # Test a generated task by sending an agent into a sandbox:
  python examples/auto_task_demo.py solve ./tasks/some-task-id
  python examples/auto_task_demo.py solve --test ./tasks/some-task-id

  # Validate a task (eval fails on broken state, passes after solution.sh):
  python examples/auto_task_demo.py validate ./tasks/some-task-id
  python examples/auto_task_demo.py validate --test ./tasks/some-task-id

  # Full pipeline: generate + validate + solve
  python examples/auto_task_demo.py full --trace path/to/session.jsonl
  python examples/auto_task_demo.py full --test --trace path/to/session.jsonl
"""

import asyncio
import json
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from afe.auto_task import AutoTaskPipeline, create_task_agents, read_trace, _find_latest_trace
from afe.task_runner import TaskRunner, validate_task

RAILWAY_API_URL = "https://agent-sdk-server-production.up.railway.app"
LOCAL_TEST_API_URL = "http://localhost:7778"


async def demo_generate(trace_path: str | None = None, output: str = "./tasks"):
    """Generate tasks from a Claude Code trace."""
    if trace_path is None:
        cwd = os.getcwd()
        found = _find_latest_trace(cwd)
        if found is None:
            print(f"No trace found for {cwd}. Pass --trace explicitly.")
            return []
        trace_path = str(found)
        print(f"Using latest trace: {trace_path}")

    agents = create_task_agents(provider="local")
    pipeline = AutoTaskPipeline(analyzer=agents["analyzer"], architect=agents["architect"], builder=agents["builder"], validator=agents["validator"])

    try:
        tasks = await pipeline.run(trace_path=trace_path, output_dir=output)
        print(f"\nGenerated {len(tasks)} tasks:")
        for t in tasks:
            print(f"  {output}/{t.task_id}/")
            for f in sorted(t.files.keys()):
                print(f"    {f}")
        return tasks
    finally:
        await pipeline.aclose()


async def demo_solve(task_dir: str, provider: str = "local", model: str = "claude-sonnet-4-6"):
    """Send an agent into a sandbox to solve a task."""
    print(f"Solving task: {task_dir}")
    runner = TaskRunner(provider=provider, model=model)
    result = await runner.run(task_dir)

    print(f"\n{'=' * 60}")
    print(f"Task:      {result.task_id}")
    print(f"Result:    {'PASS' if result.passed else 'FAIL'}")
    print(f"Exit code: {result.eval_exit_code}")
    print(f"Eval output:")
    print(result.eval_output)
    print(f"{'=' * 60}")
    return result


async def demo_validate(task_dir: str, provider: str = "local"):
    """Validate a task is well-formed: broken state fails, solution passes."""
    print(f"Validating task: {task_dir}")
    result = await validate_task(task_dir, provider=provider)

    print(f"\n{'=' * 60}")
    print(f"Valid:            {result['valid']}")
    print(f"Broken fails:     {result['broken_fails']}")
    print(f"Solution passes:  {result['solution_passes']}")
    print(f"Has solution.sh:  {result['has_solution']}")
    print(f"{'=' * 60}")
    return result


async def demo_full(trace_path: str | None = None, output: str = "./tasks"):
    """Full pipeline: generate → validate → solve."""
    # Step 1: Generate
    print("=" * 60)
    print("STEP 1: Generating tasks from trace")
    print("=" * 60)
    tasks = await demo_generate(trace_path=trace_path, output=output)
    if not tasks:
        print("No tasks generated.")
        return

    for task in tasks:
        task_dir = f"{output}/{task.task_id}"

        # Step 2: Validate
        print(f"\n{'=' * 60}")
        print(f"STEP 2: Validating task {task.task_id}")
        print("=" * 60)
        validation = await demo_validate(task_dir)

        if not validation["valid"]:
            print(f"Task {task.task_id} is not valid, skipping solve.")
            continue

        # Step 3: Solve
        print(f"\n{'=' * 60}")
        print(f"STEP 3: Solving task {task.task_id}")
        print("=" * 60)
        result = await demo_solve(task_dir)
        print(f"\nFinal: {task.task_id} → {'PASS' if result.passed else 'FAIL'}")


def main():
    argv = sys.argv[1:]
    test_mode = False
    if "--test" in argv:
        argv.remove("--test")
        test_mode = True
    os.environ["AGENT_API_URL"] = LOCAL_TEST_API_URL if test_mode else RAILWAY_API_URL

    if len(argv) < 1:
        print(__doc__)
        sys.exit(1)

    command = argv[0]

    if command == "generate":
        trace = None
        output = "./tasks"
        args = argv[1:]
        i = 0
        while i < len(args):
            if args[i] == "--trace" and i + 1 < len(args):
                trace = args[i + 1]; i += 2
            elif args[i] == "--output" and i + 1 < len(args):
                output = args[i + 1]; i += 2
            else:
                i += 1
        asyncio.run(demo_generate(trace_path=trace, output=output))

    elif command == "solve":
        if len(argv) < 2:
            print("Usage: auto_task_demo.py solve <task-dir>")
            sys.exit(1)
        asyncio.run(demo_solve(argv[1]))

    elif command == "validate":
        if len(argv) < 2:
            print("Usage: auto_task_demo.py validate <task-dir>")
            sys.exit(1)
        asyncio.run(demo_validate(argv[1]))

    elif command == "full":
        trace = None
        args = argv[1:]
        i = 0
        while i < len(args):
            if args[i] == "--trace" and i + 1 < len(args):
                trace = args[i + 1]; i += 2
            else:
                i += 1
        asyncio.run(demo_full(trace_path=trace))

    else:
        print(f"Unknown command: {command}")
        print(__doc__)
        sys.exit(1)


if __name__ == "__main__":
    main()
