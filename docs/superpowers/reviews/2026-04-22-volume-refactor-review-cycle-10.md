# Cycle 10 External Review — `volume-refactor`

Final ship-readiness review after 9 cycles of review+fix.

## Verdict
**SHIP**

## Critical / Major
None.

## Minor (addressed inline)

1. Stale `create_daytona` reference in `tests/test_daytona_volumes.py:39-44` docstring. Rewritten.
2. Dead stub attributes (`get_agent_log`, `session_has_log_entries`) still listed in `test_supervisor.py`, `test_adversarial.py`, `test_queue_interrupt.py`. Removed.
3. Unseeded `random.uniform` for latency jitter in `test_install_chaos.py`. Not addressed — reviewer classified as "low flake risk; jitter only affects timing, not assertions".

## What looks clean

- Every simplified symbol has zero live callers across src/ and tests/ (confirmed via grep).
- `iter_sse_blocks` still live — cycle 9 only removed `server.py`'s unused import.
- `_bootstrap_supervisor_in_daytona_sandbox` has one live caller (restart fallback) — correctly kept.
- **Stale-record bug fix is complete** — `start_sandbox_route` was the only affected site. Other `_ensure_sandbox_alive` callers (`_resolve_sandbox_instance`) read fresh `ProviderInstance` from `_INSTANCES` and don't do follow-up upserts.
- `_exec_subprocess` transitive-import footgun is fixed via explicit re-export.
- Autocommit restore block (cycle-6 + cycle-8 hardening) intact after simplification.
- Stress tests assert invariants (orphan rows, `_INSTANCES` consistency), not specific race outcomes — race-tolerant.

## Branch summary

- 50+ commits since main merge-base
- 477 tests passed / 62 skipped / 0 failed
- Net ~+9.4K lines across 59 files after the 260-line cycle-9 cut

Ship it.
