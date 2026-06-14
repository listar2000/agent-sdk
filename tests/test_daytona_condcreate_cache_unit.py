"""The per-volume conditional-create support cache must stay bounded.

``_daytona_supports_conditional_create`` caches a bool per volume so the
probe (write + conditional-upload + download round-trip) runs once per
volume. That cache used to be an unbounded ``dict`` with no eviction: a
long-running server churns through many distinct volumes — and volumes
vanish out-of-band (reconcile / external delete) without ever hitting
``delete_volume`` — so it grew one entry per volume forever (a slow leak).

It is now an LRU-bounded ``OrderedDict`` plus evict-on-delete. These tests
pin both: the cap holds under many distinct volumes, recency is honoured,
and an explicit volume delete evicts the entry. All pure unit — no cloud.
"""
from __future__ import annotations

from types import SimpleNamespace

import pytest

pytestmark = pytest.mark.asyncio


async def test_condcreate_cache_is_lru_bounded(monkeypatch):
    from api.providers import daytona as dmod

    dmod._conditional_create_support_cache.clear()
    monkeypatch.setattr(dmod, "_CONDCREATE_CACHE_MAX", 4)

    for i in range(20):
        dmod._condcreate_cache_set(f"vol-{i:02d}", True)

    cache = dmod._conditional_create_support_cache
    assert len(cache) == 4, (
        f"conditional-create cache must stay bounded under many distinct "
        f"volumes; grew to {len(cache)} (unbounded leak)")
    # The four most-recently-written survive.
    assert set(cache) == {f"vol-{i:02d}" for i in range(16, 20)}


async def test_condcreate_cache_lru_keeps_recently_used(monkeypatch):
    from api.providers import daytona as dmod

    dmod._conditional_create_support_cache.clear()
    monkeypatch.setattr(dmod, "_CONDCREATE_CACHE_MAX", 3)

    for r in ("a", "b", "c"):
        dmod._condcreate_cache_set(r, True)
    # Touch 'a' so it becomes most-recently-used; 'b' is now the LRU.
    assert dmod._condcreate_cache_get("a") is True
    dmod._condcreate_cache_set("d", True)  # overflow → evict LRU ('b')

    keys = set(dmod._conditional_create_support_cache)
    assert keys == {"a", "c", "d"}, f"LRU must keep the touched entry; got {keys}"


async def test_condcreate_cache_get_miss_returns_none():
    from api.providers import daytona as dmod
    dmod._conditional_create_support_cache.clear()
    assert dmod._condcreate_cache_get("never-seen") is None


async def test_delete_volume_evicts_condcreate_and_utility(monkeypatch):
    from api.providers import daytona as dmod
    import daytona_api_client_async

    ref = "vol-to-delete"
    dmod._conditional_create_support_cache.clear()
    dmod._condcreate_cache_set(ref, True)
    assert ref in dmod._conditional_create_support_cache

    class _FakeVolumesApi:
        def __init__(self, api_client):
            pass

        async def delete_volume(self, r):
            self.deleted = r

    monkeypatch.setattr(daytona_api_client_async, "VolumesApi", _FakeVolumesApi)

    async def _client():
        return SimpleNamespace(_api_client=object())

    monkeypatch.setattr(dmod, "_get_async_daytona_client", _client)

    dropped: list[str] = []

    async def _drop(r):
        dropped.append(r)

    monkeypatch.setattr(dmod, "_drop_utility", _drop)

    await dmod.delete_daytona_volume(ref)

    assert ref not in dmod._conditional_create_support_cache, (
        "volume delete must evict the conditional-create cache entry")
    assert dropped == [ref], (
        "volume delete must drop the volume's utility sandbox (else it lingers "
        "up to _UTILITY_TTL_S mounting a deleted volume)")
