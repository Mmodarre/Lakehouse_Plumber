"""Sandbox preflight resolution: ``lhp.api._sandbox_run._resolve_sandbox_run``.

Unit-ish coverage of the single seam that turns a ``sandbox=True`` run's
inputs into the frozen :class:`~lhp.models.processing.SandboxRunConfig` the
workers consume — against REAL on-disk temp projects (no mocks: the personal
profile is loaded from ``.lhp/profile.yaml``, the team policy from the
``lhp.yaml`` ``sandbox:`` block off the orchestrator's project config, and the
candidate pipelines from real discovery). Pins:

1. Missing ``.lhp/profile.yaml`` → ``LHP-IO-025`` (raised, never emitted —
   the STREAMS own the §1.4 ``ErrorEmitted`` + raise rendezvous).
2. Valid profile + ``lhp.yaml`` ``sandbox:`` block → a
   :class:`SandboxRunConfig` carrying the profile namespace, the team
   ``table_pattern`` / ``strategy``, and the glob-expanded concrete scope.
3. ``env`` absent from ``sandbox.allowed_envs`` → ``LHP-CFG-065``.
4. A profile scope entry matching zero pipelines → ``LHP-VAL-064``.

The helper is an underscore-prefixed internal seam; importing it from
``lhp.api._sandbox_run`` here is the same in-process reach the
emission-helper tests in ``test_warning_emitted_stream`` perform.
"""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from lhp.errors import LHPError


def _write_flowgroup(project_root: Path, pipeline: str, table: str) -> None:
    """One minimal load → write flowgroup for ``pipeline``."""
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
    """Write a minimal project; ``pipelines`` maps pipeline name → table name.

    ``lhp_yaml_extra`` is appended verbatim to ``lhp.yaml`` (e.g. the
    ``sandbox:`` policy block). ``profile`` is the full ``.lhp/profile.yaml``
    content; ``None`` writes no profile (the missing-profile case).
    """
    project_root = tmp_path / "proj"
    for sub in ("presets", "templates", "substitutions"):
        (project_root / sub).mkdir(parents=True, exist_ok=True)
    (project_root / "lhp.yaml").write_text(
        "name: sandbox_run_proj\nversion: '1.0'\n" + lhp_yaml_extra
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


def _resolve(project_root: Path, env: str = "dev"):
    """Run the resolution seam against real discovery on ``project_root``."""
    from lhp.api._sandbox_run import _resolve_sandbox_run
    from lhp.core.coordination.layers import build_facade_orchestrator

    orchestrator = build_facade_orchestrator(project_root, enforce_version=False)
    flowgroups = orchestrator.bootstrap.discover_all_flowgroups()
    return _resolve_sandbox_run(orchestrator, env, flowgroups)


@pytest.mark.integration
def test_missing_profile_raises_io_025(tmp_path):
    """No ``.lhp/profile.yaml`` → LHP-IO-025 (raised, not emitted)."""
    project_root = _build_project(tmp_path, pipelines={"p_one": "orders"}, profile=None)

    with pytest.raises(LHPError) as exc_info:
        _resolve(project_root)

    assert exc_info.value.code == "LHP-IO-025"


@pytest.mark.integration
def test_valid_profile_and_policy_resolve_run_config(tmp_path):
    """Profile + ``sandbox:`` policy merge into the frozen run config; the
    glob scope entry expands against the DISCOVERED pipeline names."""
    project_root = _build_project(
        tmp_path,
        pipelines={"p_one": "orders", "p_two": "customers", "other": "events"},
        lhp_yaml_extra=(
            "sandbox:\n"
            "  strategy: table\n"
            '  table_pattern: "{namespace}__{table}"\n'
            "  allowed_envs: [dev]\n"
        ),
        profile="sandbox:\n  namespace: alice\n  pipelines:\n    - p_*\n",
    )

    run = _resolve(project_root)

    assert run.namespace == "alice"
    assert run.table_pattern == "{namespace}__{table}"
    assert run.strategy == "table"
    # Scope expansion: the 'p_*' glob matched both p_ pipelines (sorted),
    # and NOT the out-of-pattern 'other' pipeline.
    assert run.pipelines == ("p_one", "p_two")


@pytest.mark.integration
def test_env_not_in_allowed_envs_raises_cfg_065(tmp_path):
    """``allowed_envs`` without the requested env → LHP-CFG-065."""
    project_root = _build_project(
        tmp_path,
        pipelines={"p_one": "orders"},
        lhp_yaml_extra="sandbox:\n  allowed_envs: [tst]\n",
        profile="sandbox:\n  namespace: alice\n  pipelines:\n    - p_one\n",
    )

    with pytest.raises(LHPError) as exc_info:
        _resolve(project_root, env="dev")

    assert exc_info.value.code == "LHP-CFG-065"


@pytest.mark.integration
def test_zero_match_scope_raises_val_064(tmp_path):
    """A profile entry matching no discovered pipeline → LHP-VAL-064."""
    project_root = _build_project(
        tmp_path,
        pipelines={"p_one": "orders"},
        profile="sandbox:\n  namespace: alice\n  pipelines:\n    - nope_*\n",
    )

    with pytest.raises(LHPError) as exc_info:
        _resolve(project_root)

    assert exc_info.value.code == "LHP-VAL-064"
