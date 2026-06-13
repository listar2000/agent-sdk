"""The native transport's base64 codec is loop-isolated for large payloads.

read_file/write_file move bytes as base64 over the exec channel (docker/daytona/
modal). base64 holds the GIL, so a multi-MB transfer encoded/decoded INLINE
freezes the event loop for every other session on the replica. The transport
offloads the codec to a worker thread above ``_B64_THREAD_THRESHOLD`` — not a
speedup (GIL), purely keeping the loop responsive (the win measured in
benchmark/micro/bench_b64.py). These tests pin both correctness and that
isolation property, backend-free (the helpers are pure).
"""

from __future__ import annotations

import asyncio
import base64

import pytest

from api.native import transport as tr


@pytest.mark.asyncio
async def test_b64_roundtrip_small_and_large():
    for n in (0, 16, tr._B64_THREAD_THRESHOLD - 1, tr._B64_THREAD_THRESHOLD,
              tr._B64_THREAD_THRESHOLD * 2 + 7):
        data = b"\x00\x01\x02hello\xff" * (max(1, n // 8))
        enc = await tr._b64encode(data)
        assert enc == base64.b64encode(data)
        assert await tr._b64decode(enc) == data
        # decode also accepts the str form the daytona/modal path produces
        assert await tr._b64decode(enc.decode()) == data


@pytest.mark.asyncio
async def test_codec_offloads_to_thread_only_when_large(monkeypatch):
    """The size gate is what keeps a multi-MB transfer off the event loop:
    payloads at/above the threshold dispatch to a worker thread, smaller ones
    stay inline (the to_thread hop would cost more than the codec). Asserting
    the OFFLOAD DECISION directly is robust — a wall-clock "did the loop stay
    free" check is inherently flaky for base64, which holds the GIL and so only
    yields the loop partial windows (benchmark/micro/bench_b64.py quantifies the
    real, partial isolation; the GIL means it is never full isolation)."""
    offloaded: list[str] = []
    real_to_thread = asyncio.to_thread

    async def _spy(fn, *a, **k):
        offloaded.append(getattr(fn, "__name__", repr(fn)))
        return await real_to_thread(fn, *a, **k)

    monkeypatch.setattr(asyncio, "to_thread", _spy)

    small = b"x" * 4096
    big = b"x" * (tr._B64_THREAD_THRESHOLD + 1)

    # small: inline, no thread hop, still correct
    assert await tr._b64encode(small) == base64.b64encode(small)
    assert await tr._b64decode(base64.b64encode(small)) == small
    assert offloaded == [], "small payload must not pay the to_thread dispatch"

    # large: offloaded to a worker thread, still correct
    assert await tr._b64encode(big) == base64.b64encode(big)
    assert await tr._b64decode(base64.b64encode(big)) == big
    assert offloaded == ["b64encode", "b64decode"], offloaded


def test_enforce_read_limit_guard():
    """The shared read_file size guard: returns within-limit data unchanged,
    raises ValueError past the limit (the value an oversized read would
    otherwise store in the conversation + checkpoint)."""
    data = b"z" * 100
    assert tr._enforce_read_limit(data, "/p", 100) is data        # exactly at limit
    assert tr._enforce_read_limit(data, "/p", 200) is data        # under
    with pytest.raises(ValueError, match="101B exceeds max_bytes=100"):
        tr._enforce_read_limit(b"z" * 101, "/p", 100)


@pytest.mark.parametrize("make", [
    lambda: tr.DockerTransport(container_id="fake"),
    lambda: tr.DaytonaTransport(),
    lambda: tr.ModalTransport(),
], ids=["docker", "daytona", "modal"])
@pytest.mark.asyncio
async def test_read_file_enforces_max_bytes_on_every_transport(make):
    """Every base64-exec transport must honor read_file's max_bytes. Daytona and
    modal previously accepted the param but never checked it — silently returning
    an oversized file that then bloated ``messages`` + every checkpoint and could
    overflow the model context. Stub exec to deliver an over-limit payload and
    assert the guard fires uniformly; an in-limit read still returns the bytes."""
    t = make()
    payload = b"y" * 5000
    enc = base64.b64encode(payload).decode()

    async def fake_exec(command, **kwargs):
        return tr.TransportExecResult(stdout=enc, stderr="",
                                      exit_code=0, timed_out=False)
    t.exec = fake_exec  # type: ignore[method-assign]

    with pytest.raises(ValueError, match="exceeds max_bytes"):
        await t.read_file("/big", max_bytes=1000)
    assert await t.read_file("/ok", max_bytes=8000) == payload
