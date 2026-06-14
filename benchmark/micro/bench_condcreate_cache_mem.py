"""Micro-benchmark: the daytona conditional-create cache stays bounded.

Quantifies the RAM win from making ``_conditional_create_support_cache`` an
LRU-bounded OrderedDict instead of an unbounded ``dict``. A long-running
server probes one bool per distinct volume; the old dict kept every entry
forever (volumes also vanish out-of-band without hitting delete_volume), so
memory grew with lifetime volume count. The bound makes it flat.

Pure local (no cloud / creds). Uses tracemalloc to compare the resident
footprint of the old unbounded dict vs the real ``_condcreate_cache_set``
helper after probing N distinct volumes.

    PYTHONPATH=src .venv/bin/python benchmark/micro/bench_condcreate_cache_mem.py

Knob (env): ``BENCH_VOLUMES`` (default 100000).
"""
from __future__ import annotations

import os
import tracemalloc


def main() -> None:
    from api.providers import daytona as dmod

    n = int(os.environ.get("BENCH_VOLUMES", "100000"))
    key = lambda i: f"vol-{i:08d}-{'x' * 24}"  # ~36-char volume ref, like a UUID

    # Baseline: the old unbounded dict.
    tracemalloc.start()
    base: dict[str, bool] = {}
    for i in range(n):
        base[key(i)] = True
    base_cur, _ = tracemalloc.get_traced_memory()
    base_len = len(base)
    tracemalloc.stop()
    del base

    # Bounded LRU — the real helper the provider now uses.
    dmod._conditional_create_support_cache.clear()
    tracemalloc.start()
    for i in range(n):
        dmod._condcreate_cache_set(key(i), True)
    lru_cur, _ = tracemalloc.get_traced_memory()
    lru_len = len(dmod._conditional_create_support_cache)
    tracemalloc.stop()
    dmod._conditional_create_support_cache.clear()

    print(f"distinct volumes probed over server lifetime: {n}")
    print(f"  unbounded dict       : {base_len:>8} entries, ~{base_cur/1e6:6.2f} MB resident")
    print(f"  LRU (cap {dmod._CONDCREATE_CACHE_MAX:<5})      : {lru_len:>8} entries, ~{lru_cur/1e6:6.2f} MB resident")
    print(f"  >>> bounded at {lru_len} entries regardless of churn "
          f"(~{base_cur/max(lru_cur,1):.0f}x less RAM at {n} volumes)")


if __name__ == "__main__":
    main()
