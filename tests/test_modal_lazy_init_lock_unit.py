"""modal lazy singletons (_get_app/_get_image/_get_volume_image) are race-safe.

Without a lock, a burst of concurrent first-creates all see the singleton as
None and each fire a redundant control-plane call (App.lookup / Image.from_id)
— a thundering herd on a freshly autoscaled replica's first batch of sessions.
Double-checked locking collapses it to one call. Mirrors daytona's
``_DAYTONA_ASYNC_INIT_LOCK``.

Mocked — no cloud.
"""
from __future__ import annotations

import asyncio
import threading
import time
from types import SimpleNamespace

import pytest

pytestmark = pytest.mark.asyncio


@pytest.fixture(autouse=True)
def _reset_singletons():
    import api.providers.modal as mmod
    saved = (mmod._app, mmod._image, mmod._volume_image)
    mmod._app = mmod._image = mmod._volume_image = None
    try:
        yield
    finally:
        mmod._app, mmod._image, mmod._volume_image = saved


async def test_get_app_collapses_concurrent_lookups(monkeypatch):
    import api.providers.modal as mmod

    calls = {"n": 0}
    lk = threading.Lock()

    def _lookup(name, create_if_missing=False):
        with lk:
            calls["n"] += 1
        time.sleep(0.05)  # widen the race window
        return SimpleNamespace(app_id="ap-test")

    fake_modal = SimpleNamespace(App=SimpleNamespace(lookup=_lookup))
    monkeypatch.setattr(mmod, "_require_modal", lambda: (fake_modal, None))

    results = await asyncio.gather(*[mmod._get_app() for _ in range(12)])

    assert calls["n"] == 1, (
        f"App.lookup must fire ONCE under the lock, not once per concurrent "
        f"first-create; got {calls['n']}")
    assert all(r is results[0] for r in results), "all callers share one app"


async def test_get_volume_image_collapses_concurrent(monkeypatch):
    import api.providers.modal as mmod

    calls = {"n": 0}
    lk = threading.Lock()

    def _slim():
        with lk:
            calls["n"] += 1
        return SimpleNamespace(image="debian")

    fake_modal = SimpleNamespace(Image=SimpleNamespace(debian_slim=_slim))
    monkeypatch.setattr(mmod, "_require_modal", lambda: (fake_modal, None))

    results = await asyncio.gather(*[mmod._get_volume_image() for _ in range(12)])
    assert calls["n"] == 1, f"debian_slim must build once, got {calls['n']}"
    assert all(r is results[0] for r in results)


async def test_get_app_memoized_after_first(monkeypatch):
    """Second call returns the cached app without re-looking-up."""
    import api.providers.modal as mmod

    calls = {"n": 0}

    def _lookup(name, create_if_missing=False):
        calls["n"] += 1
        return SimpleNamespace(app_id="ap-test")

    monkeypatch.setattr(mmod, "_require_modal",
                        lambda: (SimpleNamespace(App=SimpleNamespace(lookup=_lookup)), None))
    a = await mmod._get_app()
    b = await mmod._get_app()
    assert a is b and calls["n"] == 1
