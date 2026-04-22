"""Chaos tests for ``ensure_volume_supervisor``.

Hammers the install path across many (volume, agent_type) pairs with
randomised latencies and injected failures.  Verifies three cross-cutting
invariants:

  1. Uniqueness — for N unique (volume, agent_type) pairs, exactly N
     successful ``install_supervisor`` calls happen.  Duplicates mean
     the advisory-lock double-check is broken.
  2. Cache correctness on failure — when the provider install raises,
     the volume's ``supervisor_agent_types`` cache column must not
     list the failed agent_type.  (Otherwise subsequent callers would
     short-circuit at the fast path and assume an install that never
     happened.)
  3. Retry convergence — a failed install followed by a successful
     retry leaves the cache listing exactly that agent_type once.

All provider calls are mocked; we're testing the cross-worker lock +
cache bookkeeping in ``api.server.ensure_volume_supervisor``.
"""
from __future__ import annotations

import asyncio
import os
import random
import sys
from unittest.mock import AsyncMock, patch

import pytest
import pytest_asyncio

_SRC = os.path.join(os.path.dirname(__file__), "..", "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

_DB = os.environ.get("TEST_DATABASE_URL")
pytestmark = pytest.mark.skipif(_DB is None, reason="TEST_DATABASE_URL not set")
if _DB:
    os.environ["DATABASE_URL"] = _DB

from api import db as dbmod, server as srv  # noqa: E402
from api.models import VolumeRecord  # noqa: E402


@pytest_asyncio.fixture
async def setup(clean_db):
    yield


async def _read_agent_types(volume_id: str) -> list[str]:
    async with dbmod.get_db() as conn:
        row = await (await conn.execute(
            "SELECT supervisor_agent_types FROM volumes WHERE id = %s",
            (volume_id,),
        )).fetchone()
    return list((row or {}).get("supervisor_agent_types") or [])


# ===========================================================================
# Test: N concurrent unique-key calls → exactly N installs.
# ===========================================================================


@pytest.mark.asyncio
@pytest.mark.timeout(10)
async def test_concurrent_unique_keys_install_exactly_once(setup):
    """10 different (volume, agent_type) pairs concurrently.  Each one
    must result in exactly one ``install_supervisor`` call — no duplicates
    (every key is unique, so the cache fast-path never hits)."""
    # Create 10 volumes; pair each with a different agent_type from a
    # rotating list of 3, so duplicate agent_types exist but unique keys.
    agent_types = ["claude", "codex", "gemini"]
    pairs = []
    for i in range(10):
        vol_id = f"vol-chaos-{i}"
        agent_type = agent_types[i % len(agent_types)]
        await dbmod.upsert_volume(VolumeRecord(
            id=vol_id, name=f"chaos-{i}",
            provider="daytona", provider_ref=f"dt-{vol_id}",
        ))
        pairs.append((vol_id, agent_type))

    install_log: list[tuple[str, str]] = []
    install_lock = asyncio.Lock()

    async def fake_install(provider, ref, agent_type):
        # Randomised latency — amplifies races.
        await asyncio.sleep(random.uniform(0.002, 0.015))
        async with install_lock:
            install_log.append((ref, agent_type))

    with patch("api.providers.install_supervisor",
               new=AsyncMock(side_effect=fake_install)):
        tasks = [
            srv.ensure_volume_supervisor(vid, at)
            for (vid, at) in pairs
        ]
        await asyncio.gather(*tasks)

    # Exactly N installs, one per unique (ref, agent_type) pair.
    refs_seen = set(install_log)
    expected = {(f"dt-{vid}", at) for (vid, at) in pairs}
    assert refs_seen == expected, (
        f"mismatched install set:\n"
        f"  expected {expected}\n  got {refs_seen}\n  all={install_log}"
    )
    # Same cardinality — no duplicate installs for the same key.
    assert len(install_log) == len(pairs), (
        f"duplicate install_supervisor calls: {install_log}"
    )

    # Cache reflects each install exactly once.
    for (vid, at) in pairs:
        installed = await _read_agent_types(vid)
        assert at in installed, (
            f"volume {vid}: {at} missing from cache {installed}"
        )


# ===========================================================================
# Test: concurrent same-key calls → exactly one install.
# ===========================================================================


@pytest.mark.asyncio
@pytest.mark.timeout(10)
async def test_concurrent_same_key_installs_once(setup):
    """10 concurrent ensure_volume_supervisor calls on the SAME
    (volume, agent_type) → exactly one install.  This is the whole point
    of the advisory lock — duplicate installs were the pre-lock behavior."""
    await dbmod.upsert_volume(VolumeRecord(
        id="v-same", name="same", provider="daytona", provider_ref="dt-same",
    ))

    install_count = 0
    install_lock = asyncio.Lock()

    async def fake_install(provider, ref, agent_type):
        nonlocal install_count
        await asyncio.sleep(random.uniform(0.01, 0.04))
        async with install_lock:
            install_count += 1

    with patch("api.providers.install_supervisor",
               new=AsyncMock(side_effect=fake_install)):
        tasks = [
            srv.ensure_volume_supervisor("v-same", "claude")
            for _ in range(10)
        ]
        await asyncio.gather(*tasks)

    assert install_count == 1, (
        f"expected 1 install under advisory lock, got {install_count}"
    )
    installed = await _read_agent_types("v-same")
    assert installed == ["claude"]


# ===========================================================================
# Test: 30% injected failures → cache stays clean on failed paths.
# ===========================================================================


@pytest.mark.asyncio
@pytest.mark.timeout(15)
async def test_install_failures_keep_cache_clean(setup):
    """Inject random 30% failures on install_supervisor; confirm the
    supervisor_agent_types cache only lists agent_types whose install
    actually succeeded.  This is the "no footprint on failure" property
    ensure_volume_supervisor's docstring promises."""
    # Seed 20 volumes, each with a single claude install attempt.
    for i in range(20):
        await dbmod.upsert_volume(VolumeRecord(
            id=f"v-fail-{i}", name=f"fail-{i}",
            provider="daytona", provider_ref=f"dt-fail-{i}",
        ))

    failed_volumes: set[str] = set()
    succeeded_volumes: set[str] = set()

    rng = random.Random(0xC0FFEE)  # reproducible

    async def flaky_install(provider, ref, agent_type):
        # Extract volume id from ref prefix (we made them 1:1 above).
        vol_id = ref.replace("dt-fail-", "v-fail-")
        await asyncio.sleep(rng.uniform(0.001, 0.01))
        if rng.random() < 0.30:
            failed_volumes.add(vol_id)
            raise RuntimeError(f"simulated install failure on {ref}")
        succeeded_volumes.add(vol_id)

    results = []

    async def run(vid):
        try:
            await srv.ensure_volume_supervisor(vid, "claude")
            return ("ok", vid)
        except Exception as e:
            return ("err", vid, str(e))

    with patch("api.providers.install_supervisor",
               new=AsyncMock(side_effect=flaky_install)):
        results = await asyncio.gather(*(run(f"v-fail-{i}") for i in range(20)))

    # Every successful return → cache lists claude.
    for r in results:
        if r[0] == "ok":
            vid = r[1]
            installed = await _read_agent_types(vid)
            assert "claude" in installed, (
                f"volume {vid}: successful install didn't update cache: {installed}"
            )
        else:
            vid = r[1]
            installed = await _read_agent_types(vid)
            assert "claude" not in installed, (
                f"volume {vid}: FAILED install polluted cache: {installed}. "
                "ensure_volume_supervisor must not mark the agent_type as "
                "installed if the provider call raised."
            )


# ===========================================================================
# Test: failed install followed by successful retry converges cleanly.
# ===========================================================================


@pytest.mark.asyncio
@pytest.mark.timeout(10)
async def test_retry_after_failure_converges(setup):
    """First install raises; second install succeeds.  After the retry,
    the cache lists the agent_type exactly once."""
    await dbmod.upsert_volume(VolumeRecord(
        id="v-retry", name="retry", provider="daytona", provider_ref="dt-retry",
    ))

    attempts = {"n": 0}

    async def flaky(provider, ref, agent_type):
        attempts["n"] += 1
        if attempts["n"] == 1:
            raise RuntimeError("first attempt fails")
        # second and later succeed

    with patch("api.providers.install_supervisor",
               new=AsyncMock(side_effect=flaky)):
        with pytest.raises(RuntimeError):
            await srv.ensure_volume_supervisor("v-retry", "claude")

        # Cache must NOT list claude yet — the install failed.
        assert await _read_agent_types("v-retry") == []

        # Retry — succeeds, cache updates.
        await srv.ensure_volume_supervisor("v-retry", "claude")
        assert await _read_agent_types("v-retry") == ["claude"]

        # A third call hits the fast-path — no extra install.
        await srv.ensure_volume_supervisor("v-retry", "claude")

    assert attempts["n"] == 2, (
        f"expected 2 install calls (1 fail + 1 success), got {attempts['n']}"
    )


# ===========================================================================
# Test: cache already lists agent_type → no install runs.
# ===========================================================================


@pytest.mark.asyncio
@pytest.mark.timeout(5)
async def test_fast_path_skips_install_when_cached(setup):
    """The cache fast-path must never call install_supervisor when the
    agent_type is already listed."""
    await dbmod.upsert_volume(VolumeRecord(
        id="v-fast", name="fast", provider="daytona", provider_ref="dt-fast",
        supervisor_agent_types=["claude", "codex"],
    ))

    install_count = 0

    async def count_install(*a, **kw):
        nonlocal install_count
        install_count += 1

    with patch("api.providers.install_supervisor",
               new=AsyncMock(side_effect=count_install)):
        # Hit both pre-installed agent_types many times.
        for _ in range(5):
            await srv.ensure_volume_supervisor("v-fast", "claude")
            await srv.ensure_volume_supervisor("v-fast", "codex")

        # Now request a new agent_type — this one must install.
        await srv.ensure_volume_supervisor("v-fast", "gemini")

    # Only the gemini install ran.
    assert install_count == 1, (
        f"expected 1 install (gemini only); got {install_count}"
    )
    installed = await _read_agent_types("v-fast")
    assert set(installed) == {"claude", "codex", "gemini"}


# ===========================================================================
# Test: interleaved unique+same-key calls hold together.
# Mix high-concurrency same-key calls with some distinct keys to make
# sure the lock doesn't starve unrelated work.
# ===========================================================================


@pytest.mark.asyncio
@pytest.mark.timeout(10)
async def test_mixed_concurrent_same_and_unique_keys(setup):
    """20 coroutines: 15 hammer (v1, claude); 5 install (v1, codex)
    concurrently.  Result: 2 installs total (one per unique key); the
    (v1, claude) callers all see the same eventual cache state."""
    await dbmod.upsert_volume(VolumeRecord(
        id="v-mix", name="mix", provider="daytona", provider_ref="dt-mix",
    ))

    log: list[str] = []
    log_lock = asyncio.Lock()

    async def fake_install(provider, ref, agent_type):
        await asyncio.sleep(random.uniform(0.005, 0.02))
        async with log_lock:
            log.append(agent_type)

    with patch("api.providers.install_supervisor",
               new=AsyncMock(side_effect=fake_install)):
        tasks = (
            [srv.ensure_volume_supervisor("v-mix", "claude") for _ in range(15)]
            + [srv.ensure_volume_supervisor("v-mix", "codex") for _ in range(5)]
        )
        random.shuffle(tasks)
        await asyncio.gather(*tasks)

    # 2 unique keys → 2 installs.
    assert sorted(log) == ["claude", "codex"], f"log = {log}"
    installed = await _read_agent_types("v-mix")
    assert set(installed) == {"claude", "codex"}
