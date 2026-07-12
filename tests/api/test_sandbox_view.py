"""Public sandbox scope read: DTO contract + ``SandboxFacade.describe_scope``.

Two layers:

1. Contract tests for :class:`lhp.api.SandboxScopeResult` (§8.3 / §9.15 —
   frozen, pickle round-trip, JSON round-trip via :func:`lhp.api.to_dict`,
   field-type discipline).
2. Behavioral coverage of :meth:`lhp.api.SandboxFacade.describe_scope`
   against REAL on-disk temp projects (no mocks, same style as
   ``tests/api/test_sandbox_run.py``): the profile is loaded from
   ``.lhp/profile.yaml``, the team policy from the ``lhp.yaml`` ``sandbox:``
   block, and the scope resolved against real discovery. The read never
   raises for a merely-unusable sandbox config — malformed profile
   (``LHP-CFG-064``), zero-match scope (``LHP-VAL-064``), and env-not-enabled
   (``LHP-CFG-065``) are surfaced on ``error``; the missing-profile case is
   the normal ``profile_exists=False`` state (no ``error``).
"""

from __future__ import annotations

import dataclasses
import json
import pickle
from dataclasses import FrozenInstanceError
from pathlib import Path
from typing import Optional

import pytest
import yaml

from lhp.api import SandboxScopeResult, to_dict


@pytest.fixture
def sample() -> SandboxScopeResult:
    return SandboxScopeResult(
        profile_exists=True,
        namespace="alice",
        patterns=("p_*", "gold"),
        resolved_pipelines=("p_one", "p_two"),
        allowed_envs=("dev", "tst"),
        error=None,
    )


class TestSandboxScopeResultContract:
    """§8.3 — frozen, pickle, JSON round-trip, and field-type discipline."""

    def test_frozen(self, sample: SandboxScopeResult) -> None:
        with pytest.raises(FrozenInstanceError):
            sample.namespace = "bob"  # type: ignore[misc]  # intentional: mutate frozen DTO to trigger FrozenInstanceError

    def test_pickle_round_trip(self, sample: SandboxScopeResult) -> None:
        assert pickle.loads(pickle.dumps(sample)) == sample

    def test_json_round_trip(self, sample: SandboxScopeResult) -> None:
        payload = json.loads(json.dumps(to_dict(sample)))
        allowed = payload["allowed_envs"]
        reconstructed = SandboxScopeResult(
            profile_exists=payload["profile_exists"],
            namespace=payload["namespace"],
            patterns=tuple(payload["patterns"]),
            resolved_pipelines=tuple(payload["resolved_pipelines"]),
            allowed_envs=tuple(allowed) if allowed is not None else None,
            error=payload["error"],
        )
        assert reconstructed == sample

    def test_json_round_trip_empty(self) -> None:
        empty = SandboxScopeResult()
        payload = json.loads(json.dumps(to_dict(empty)))
        assert payload == {
            "profile_exists": False,
            "namespace": None,
            "patterns": [],
            "resolved_pipelines": [],
            "allowed_envs": None,
            "error": None,
        }

    def test_field_types(self) -> None:
        # §4.8: no ``Any`` (outside ``JSONValue``), no bare list/dict, no
        # Pydantic model embedded in the public DTO.
        for f in dataclasses.fields(SandboxScopeResult):
            annotation = str(f.type)
            assert "Any" not in annotation, f.name
            assert "BaseModel" not in annotation, f.name
            assert "List[" not in annotation and "Dict[" not in annotation, f.name


def _write_flowgroup(project_root: Path, pipeline: str, table: str) -> None:
    """One minimal load -> write flowgroup for ``pipeline``."""
    pdir = project_root / "pipelines" / pipeline
    pdir.mkdir(parents=True, exist_ok=True)
    spec = {
        "pipeline": pipeline,
        "flowgroup": f"{pipeline}_fg",
        "actions": [
            {
                "name": f"load_{pipeline}",
                "type": "load",
                "target": f"v_{pipeline}_raw",
                "source": {
                    "type": "cloudfiles",
                    "path": "${landing_path}/" + pipeline,
                    "format": "json",
                },
            },
            {
                "name": f"write_{pipeline}",
                "type": "write",
                "source": f"v_{pipeline}_raw",
                "write_target": {
                    "type": "streaming_table",
                    "catalog": "${catalog}",
                    "schema": "${bronze_schema}",
                    "table": table,
                    "create_table": True,
                },
            },
        ],
    }
    with open(pdir / f"{pipeline}_fg.yaml", "w") as f:
        yaml.dump(spec, f)


def _build_project(
    tmp_path: Path,
    *,
    pipelines: dict[str, str],
    lhp_yaml_extra: str = "",
    profile: str | None = None,
) -> Path:
    """Write a minimal project; ``pipelines`` maps pipeline name -> table name."""
    project_root = tmp_path / "proj"
    for sub in ("presets", "templates", "substitutions"):
        (project_root / sub).mkdir(parents=True, exist_ok=True)
    (project_root / "lhp.yaml").write_text(
        "name: sandbox_view_proj\nversion: '1.0'\n" + lhp_yaml_extra
    )
    with open(project_root / "substitutions" / "dev.yaml", "w") as f:
        yaml.dump(
            {
                "dev": {
                    "catalog": "dev_catalog",
                    "bronze_schema": "bronze",
                    "landing_path": "/mnt/dev/landing",
                }
            },
            f,
        )
    for name, table in pipelines.items():
        _write_flowgroup(project_root, name, table)
    if profile is not None:
        (project_root / ".lhp").mkdir(parents=True, exist_ok=True)
        (project_root / ".lhp" / "profile.yaml").write_text(profile)
    return project_root


def _describe(project_root: Path, env: Optional[str] = "dev") -> SandboxScopeResult:
    """Resolve the sandbox scope via the public facade path."""
    from lhp.api import LakehousePlumberApplicationFacade

    facade = LakehousePlumberApplicationFacade.for_project(
        project_root, enforce_version=False
    )
    return facade.sandbox.describe_scope(env=env)


@pytest.mark.integration
def test_no_profile_reports_not_opted_in(tmp_path: Path) -> None:
    """No ``.lhp/profile.yaml`` -> profile_exists False, no error."""
    project_root = _build_project(tmp_path, pipelines={"p_one": "orders"}, profile=None)

    result = _describe(project_root)

    assert result.profile_exists is False
    assert result.namespace is None
    assert result.patterns == ()
    assert result.resolved_pipelines == ()
    assert result.error is None


@pytest.mark.integration
def test_glob_resolves_to_concrete_pipelines(tmp_path: Path) -> None:
    """A ``p_*`` glob expands (sorted) against discovery; other pipelines excluded."""
    project_root = _build_project(
        tmp_path,
        pipelines={"p_one": "orders", "p_two": "customers", "other": "events"},
        lhp_yaml_extra="sandbox:\n  allowed_envs: [dev]\n",
        profile="sandbox:\n  namespace: alice\n  pipelines:\n    - p_*\n",
    )

    result = _describe(project_root)

    assert result.profile_exists is True
    assert result.namespace == "alice"
    assert result.patterns == ("p_*",)
    assert result.resolved_pipelines == ("p_one", "p_two")
    assert result.allowed_envs == ("dev",)
    assert result.error is None


@pytest.mark.integration
def test_zero_match_glob_surfaces_val_064(tmp_path: Path) -> None:
    """A scope entry matching nothing is surfaced on ``error``, not raised."""
    project_root = _build_project(
        tmp_path,
        pipelines={"p_one": "orders"},
        profile="sandbox:\n  namespace: alice\n  pipelines:\n    - nope_*\n",
    )

    result = _describe(project_root)

    assert result.profile_exists is True
    assert result.namespace == "alice"
    assert result.patterns == ("nope_*",)
    assert result.resolved_pipelines == ()
    assert result.error is not None
    assert result.error.startswith("LHP-VAL-064:")


@pytest.mark.integration
def test_malformed_profile_surfaces_cfg_064(tmp_path: Path) -> None:
    """An invalid profile (missing required ``pipelines``) -> CFG-064 on ``error``."""
    project_root = _build_project(
        tmp_path,
        pipelines={"p_one": "orders"},
        profile="sandbox:\n  namespace: alice\n",
    )

    result = _describe(project_root)

    assert result.profile_exists is True
    assert result.namespace is None
    assert result.patterns == ()
    assert result.resolved_pipelines == ()
    assert result.error is not None
    assert "LHP-CFG-064" in result.error


@pytest.mark.integration
def test_env_not_allowed_surfaces_cfg_065(tmp_path: Path) -> None:
    """``env`` outside ``sandbox.allowed_envs`` -> CFG-065 on ``error``."""
    project_root = _build_project(
        tmp_path,
        pipelines={"p_one": "orders"},
        lhp_yaml_extra="sandbox:\n  allowed_envs: [tst]\n",
        profile="sandbox:\n  namespace: alice\n  pipelines:\n    - p_one\n",
    )

    result = _describe(project_root, env="dev")

    assert result.profile_exists is True
    assert result.allowed_envs == ("tst",)
    assert result.resolved_pipelines == ()
    assert result.error is not None
    assert "LHP-CFG-065" in result.error


@pytest.mark.integration
def test_env_omitted_skips_allowed_envs_gate(tmp_path: Path) -> None:
    """Omitting ``env`` returns the full env-independent scope (no CFG-065).

    Same project as the CFG-065 test (allowed_envs=[tst]); with ``env=None``
    the gate is skipped and the healthy profile reports its full scope.
    """
    project_root = _build_project(
        tmp_path,
        pipelines={"p_one": "orders"},
        lhp_yaml_extra="sandbox:\n  allowed_envs: [tst]\n",
        profile="sandbox:\n  namespace: alice\n  pipelines:\n    - p_one\n",
    )

    result = _describe(project_root, env=None)

    assert result.profile_exists is True
    assert result.resolved_pipelines == ("p_one",)
    assert result.allowed_envs == ("tst",)
    assert result.error is None


@pytest.mark.integration
def test_monitoring_pipeline_excluded_from_scope(tmp_path: Path) -> None:
    """The monitoring pipeline is dropped from the resolved sandbox scope."""
    project_root = _build_project(
        tmp_path,
        pipelines={"p_one": "orders", "p_mon": "events"},
        lhp_yaml_extra=(
            "event_log:\n"
            '  catalog: "${catalog}"\n'
            "  schema: _meta\n"
            "monitoring:\n"
            "  enabled: true\n"
            "  pipeline_name: p_mon\n"
            '  checkpoint_path: "/tmp/ckpt"\n'
            '  job_config_path: "${catalog}/monitoring_job.yaml"\n'
        ),
        profile="sandbox:\n  namespace: alice\n  pipelines:\n    - p_*\n",
    )

    result = _describe(project_root)

    assert result.resolved_pipelines == ("p_one",)
    assert result.error is None
