"""Native runtime — a first-party agent loop (LiteLLM) running server-side,
with tools executing in the sandbox. See docs/native_runtime_design.md.

Modules land in P0 order: frames (wire synthesis), transport, loop, tools,
session, checkpoints.
"""
