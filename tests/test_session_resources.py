"""Unit tests for the per-session ``resources`` contract.

Pins the validator + per-provider translators so a future refactor can't
silently drop an unsupported field, and so the gpu-string parser keeps
its Modal-compatible semantics (``"TYPE"`` / ``"TYPE:COUNT"`` /
count-only).

No live providers are exercised; translators are pure functions.
"""
from __future__ import annotations

import pytest

from api.sandbox.state import (
    Recipe,
    Resources,
    parse_gpu,
    validate_resources_for_provider,
)


# ---------------------------------------------------------------------------
# parse_gpu — Modal-compatible string format
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("s,expected", [
    (None, (None, None)),
    ("", (None, None)),
    ("T4", ("T4", 1)),
    ("a100", ("A100", 1)),  # uppercased
    ("T4:2", ("T4", 2)),
    ("A100:8", ("A100", 8)),
    ("2", (None, 2)),  # bare integer = count-only
    ("1", (None, 1)),
])
def test_parse_gpu(s, expected):
    assert parse_gpu(s) == expected


# ---------------------------------------------------------------------------
# validate_resources_for_provider — the API-boundary fail-loud rules
# ---------------------------------------------------------------------------

def test_validate_none_passes_for_every_provider():
    for p in ("unix_local", "daytona", "docker", "modal"):
        validate_resources_for_provider(p, None)  # no exception


def test_unix_local_rejects_any_resources():
    with pytest.raises(ValueError, match="unix_local"):
        validate_resources_for_provider("unix_local", Resources(cpu=1))


def test_daytona_rejects_typed_gpu():
    with pytest.raises(ValueError, match="gpu type"):
        validate_resources_for_provider("daytona", Resources(gpu="A100"))
    with pytest.raises(ValueError, match="gpu type"):
        validate_resources_for_provider("daytona", Resources(gpu="A100:2"))


def test_daytona_accepts_count_only_gpu():
    validate_resources_for_provider("daytona", Resources(gpu="2"))
    validate_resources_for_provider("daytona", Resources(cpu=4, memory_mib=8192, disk_gib=20))


def test_modal_rejects_disk_gib():
    with pytest.raises(ValueError, match="disk_gib"):
        validate_resources_for_provider("modal", Resources(disk_gib=10))


def test_modal_rejects_count_only_gpu():
    with pytest.raises(ValueError, match="gpu type"):
        validate_resources_for_provider("modal", Resources(gpu="2"))


def test_modal_accepts_typed_gpu():
    validate_resources_for_provider("modal", Resources(gpu="A100"))
    validate_resources_for_provider("modal", Resources(gpu="A100:4"))


def test_docker_rejects_typed_gpu_and_disk():
    with pytest.raises(ValueError, match="gpu type"):
        validate_resources_for_provider("docker", Resources(gpu="T4"))
    with pytest.raises(ValueError, match="disk_gib"):
        validate_resources_for_provider("docker", Resources(disk_gib=5))


def test_docker_accepts_cpu_memory_count_gpu():
    validate_resources_for_provider("docker", Resources(cpu=2, memory_mib=4096, gpu="2"))


# ---------------------------------------------------------------------------
# Per-provider translators — silent-drop AT this layer (validation already
# fail-loud at the API boundary; these helpers are also called from internal
# code paths and stay lenient by design).
# ---------------------------------------------------------------------------

def test_to_daytona_resources_translates():
    from api.providers.daytona import _to_daytona_resources
    r = Resources(cpu=2.5, memory_mib=4096, gpu="2", disk_gib=20)
    out = _to_daytona_resources(r)
    assert out is not None
    assert out.cpu == 2  # int conversion
    assert out.memory == 4  # 4096 MiB → 4 GiB
    assert out.disk == 20
    assert out.gpu == 2


def test_to_daytona_resources_returns_none_for_empty():
    from api.providers.daytona import _to_daytona_resources
    assert _to_daytona_resources(None) is None
    assert _to_daytona_resources(Resources()) is None


def test_to_modal_resources_translates():
    from api.providers.modal import _to_modal_resources
    r = Resources(cpu=1.5, memory_mib=2048, gpu="T4:2")
    out = _to_modal_resources(r)
    assert out == {"cpu": 1.5, "memory": 2048, "gpu": "T4:2"}


def test_to_modal_resources_collapses_single_gpu():
    from api.providers.modal import _to_modal_resources
    out = _to_modal_resources(Resources(gpu="A100"))
    assert out == {"gpu": "A100"}  # not "A100:1"


def test_to_modal_resources_drops_count_only_gpu():
    from api.providers.modal import _to_modal_resources
    # validator rejects this at the API boundary; translator stays lenient
    # for direct callers and silently drops the unsatisfiable count.
    out = _to_modal_resources(Resources(gpu="2"))
    assert out == {}


def test_to_modal_resources_returns_empty_for_none():
    from api.providers.modal import _to_modal_resources
    assert _to_modal_resources(None) == {}


@pytest.mark.asyncio
async def test_modal_create_volume_does_not_spawn_layout_sandbox(monkeypatch):
    from api.providers import modal as modal_provider

    class FakeVolume:
        @staticmethod
        def from_name(name, *, create_if_missing, version):
            assert name == "modal-prod"
            assert create_if_missing is True
            assert version == "v2"
            return object()

    class FakeApiPb2:
        class VolumeFsVersion:
            VOLUME_FS_VERSION_V2 = "v2"

    async def fail_run_volume_shell(*args, **kwargs):
        raise AssertionError("create_volume should not pre-create layout via sandbox")

    monkeypatch.setattr(modal_provider, "_require_modal", lambda: (type("M", (), {"Volume": FakeVolume}), FakeApiPb2))
    monkeypatch.setattr(modal_provider, "_run_volume_shell", fail_run_volume_shell)

    assert await modal_provider.create_volume("modal-prod") == "modal-prod"


@pytest.mark.asyncio
async def test_modal_volume_tree_missing_path_returns_empty(monkeypatch):
    from api.providers import modal as modal_provider

    captured = {}

    async def fake_run_volume_shell(ref, shell, *, timeout=60, vol=None):
        captured["shell"] = shell
        return 0, b"", b""

    monkeypatch.setattr(modal_provider, "_run_volume_shell", fake_run_volume_shell)

    assert await modal_provider.volume_tree("modal-prod", "shared/123") == ""
    assert "if [ ! -e /v/shared/123 ]; then exit 0; fi;" in captured["shell"]


def test_docker_resource_flags_translates():
    from api.providers.docker import _docker_resource_flags
    flags = _docker_resource_flags(Resources(cpu=2.0, memory_mib=4096, gpu="2"))
    assert flags == ["--cpus", "2.0", "--memory", "4096m", "--gpus", "2"]


def test_docker_resource_flags_empty_for_none():
    from api.providers.docker import _docker_resource_flags
    assert _docker_resource_flags(None) == []


# ---------------------------------------------------------------------------
# Recipe round-trip — resources must survive JSONB serialize / deserialize
# so Type-2 recovery rebuilds the same compute spec.
# ---------------------------------------------------------------------------

def test_recipe_resources_jsonb_round_trip():
    from api.sandbox.state import _ADAPTER, DaytonaSandboxState

    recipe = Recipe(resources=Resources(cpu=2, memory_mib=8192, gpu="A10G:4", disk_gib=40))
    state = DaytonaSandboxState(recipe=recipe)
    serialized = _ADAPTER.dump_python(state, mode="json")
    restored = _ADAPTER.validate_python(serialized)
    assert restored.recipe.resources is not None
    assert restored.recipe.resources.cpu == 2
    assert restored.recipe.resources.memory_mib == 8192
    assert restored.recipe.resources.gpu == "A10G:4"
    assert restored.recipe.resources.disk_gib == 40


def test_recipe_without_resources_round_trips():
    from api.sandbox.state import _ADAPTER, DockerSandboxState

    state = DockerSandboxState(recipe=Recipe())
    serialized = _ADAPTER.dump_python(state, mode="json")
    restored = _ADAPTER.validate_python(serialized)
    assert restored.recipe.resources is None
