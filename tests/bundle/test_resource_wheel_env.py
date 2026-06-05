"""Wheel-mode resource rendering: ``environment.dependencies`` injection
and unconditional ``packaging`` strip.

Construction pattern (project_root + multi-doc pipeline_config.yaml +
substitutions/<env>.yaml + ``BundleManager(...)`` + ``generate_resource_file_content``
+ ``yaml.safe_load``) is copied from
``tests/test_bundle_full_substitution.py::TestFullPipelineConfigSubstitution``.

The wheel-mode pieces added on top of that pattern:
  * ``project_config=ProjectConfig(name=..., wheel=WheelConfig(artifact_volume=...))``
    so ``BundleManager._resolve_artifact_volume`` has a ``/Volumes/...`` value;
  * ``packaging: wheel`` on the pipeline doc so
    ``PipelineConfigLoader.resolve_packaging_modes`` returns ``"wheel"``;
  * a planted ``.whl`` at ``generated/<env>/_wheels/<pipeline>/dist/<name>.whl``
    (in wheel mode the on-disk name IS the injected reference's identity, so
    ``BundleManager._find_wheel_filename`` reads it from disk).
"""

import shutil
import tempfile
from pathlib import Path

import pytest
import yaml

from lhp.bundle.manager import BundleManager
from lhp.models import ProjectConfig, WheelConfig

ENV = "dev"
ARTIFACT_VOLUME = "/Volumes/main/artifacts/wheels"


@pytest.fixture
def temp_project():
    temp_dir = tempfile.mkdtemp()
    project_root = Path(temp_dir)

    (project_root / "substitutions").mkdir(parents=True)
    (project_root / "config").mkdir(parents=True)
    (project_root / "pipelines").mkdir(parents=True)
    (project_root / "generated").mkdir(parents=True)

    sub_content = {ENV: {"catalog": "dev_catalog", "bronze_schema": "bronze_dev"}}
    with open(project_root / "substitutions" / f"{ENV}.yaml", "w") as f:
        yaml.dump(sub_content, f)

    yield project_root

    shutil.rmtree(temp_dir)


def _project_config(artifact_volume=ARTIFACT_VOLUME):
    return ProjectConfig(
        name="wheel_test_project",
        wheel=WheelConfig(artifact_volume=artifact_volume),
    )


def _write_pipeline_config(project_root: Path, body: str) -> Path:
    """Write a multi-doc pipeline_config.yaml (project-defaults doc + per-pipeline
    doc) and return its path."""
    config_path = project_root / "config" / "pipeline_config.yaml"
    with open(config_path, "w") as f:
        f.write(body)
    return config_path


def _plant_wheel(project_root: Path, pipeline_name: str, wheel_filename: str) -> Path:
    """Create generated/<env>/_wheels/<pipeline>/dist/<wheel_filename> on disk.

    Content is irrelevant — ``_find_wheel_filename`` only globs for the name.
    """
    dist_dir = project_root / "generated" / ENV / "_wheels" / pipeline_name / "dist"
    dist_dir.mkdir(parents=True, exist_ok=True)
    wheel_path = dist_dir / wheel_filename
    wheel_path.write_bytes(b"PK\x03\x04 not-a-real-wheel")
    return wheel_path


def _render(project_root: Path, config_path: Path, pipeline_name: str) -> dict:
    """Build a real BundleManager (wheel project_config) and return the parsed
    resource YAML for one pipeline."""
    manager = BundleManager(
        project_root=project_root,
        pipeline_config_path=str(config_path),
        project_config=_project_config(),
    )
    output_dir = project_root / "generated"
    rendered = manager.generate_resource_file_content(pipeline_name, output_dir, ENV)
    return yaml.safe_load(rendered)


def _pipeline_block(parsed: dict, pipeline_name: str) -> dict:
    return parsed["resources"]["pipelines"][f"{pipeline_name}_pipeline"]


@pytest.mark.unit
def test_wheel_mode_environment_absent_injects_dependencies(temp_project):
    """environment absent in config -> environment.dependencies created with the
    single wheel reference; no packaging key leaks into the rendered YAML."""
    pipeline_name = "wheel_env_absent"
    wheel_filename = "wheel_env_absent-0.1.0-py3-none-any.whl"
    _plant_wheel(temp_project, pipeline_name, wheel_filename)

    config_path = _write_pipeline_config(
        temp_project,
        f"""
---
serverless: true

---
pipeline: {pipeline_name}
catalog: "{{catalog}}"
schema: "{{bronze_schema}}"
serverless: true
packaging: wheel
""",
    )

    parsed = _render(temp_project, config_path, pipeline_name)
    pipeline = _pipeline_block(parsed, pipeline_name)

    deps = pipeline["environment"]["dependencies"]
    assert deps == [
        f"../../generated/{ENV}/_wheels/{pipeline_name}/dist/{wheel_filename}"
    ]

    # R8: packaging is an LHP-internal toggle and must never reach the resource YAML.
    assert "packaging" not in pipeline


@pytest.mark.unit
def test_wheel_mode_environment_present_no_deps_adds_dependencies(temp_project):
    """environment present but without a dependencies key -> dependencies added
    holding the single wheel reference."""
    pipeline_name = "wheel_env_no_deps"
    wheel_filename = "wheel_env_no_deps-2.3.4-py3-none-any.whl"
    _plant_wheel(temp_project, pipeline_name, wheel_filename)

    config_path = _write_pipeline_config(
        temp_project,
        f"""
---
serverless: true

---
pipeline: {pipeline_name}
catalog: "{{catalog}}"
schema: "{{bronze_schema}}"
serverless: true
packaging: wheel
environment:
  environment_version: "2"
""",
    )

    parsed = _render(temp_project, config_path, pipeline_name)
    pipeline = _pipeline_block(parsed, pipeline_name)

    # Pre-existing environment content is preserved alongside the new deps list.
    assert pipeline["environment"]["environment_version"] == "2"
    assert pipeline["environment"]["dependencies"] == [
        f"../../generated/{ENV}/_wheels/{pipeline_name}/dist/{wheel_filename}"
    ]
    assert "packaging" not in pipeline


@pytest.mark.unit
def test_wheel_mode_user_deps_preserved_wheel_ref_last(temp_project):
    """environment present WITH user-declared dependencies -> user deps preserved
    in order and the wheel reference appended last (R11)."""
    pipeline_name = "wheel_env_user_deps"
    wheel_filename = "wheel_env_user_deps-9.9.9-py3-none-any.whl"
    _plant_wheel(temp_project, pipeline_name, wheel_filename)

    config_path = _write_pipeline_config(
        temp_project,
        f"""
---
serverless: true

---
pipeline: {pipeline_name}
catalog: "{{catalog}}"
schema: "{{bronze_schema}}"
serverless: true
packaging: wheel
environment:
  dependencies:
    - "requests==2.31.0"
    - "pandas>=2.0"
""",
    )

    parsed = _render(temp_project, config_path, pipeline_name)
    pipeline = _pipeline_block(parsed, pipeline_name)

    wheel_ref = f"../../generated/{ENV}/_wheels/{pipeline_name}/dist/{wheel_filename}"
    deps = pipeline["environment"]["dependencies"]
    assert deps == [
        "requests==2.31.0",
        "pandas>=2.0",
        wheel_ref,
    ]
    # Wheel reference is strictly last.
    assert deps[-1] == wheel_ref
    assert "packaging" not in pipeline


@pytest.mark.unit
def test_source_mode_strips_packaging_no_wheel_injection(temp_project):
    """A source-mode pipeline: the unconditional R8 strip still removes the
    packaging key, and NO wheel reference is injected (no environment created)."""
    pipeline_name = "source_pipeline"

    config_path = _write_pipeline_config(
        temp_project,
        f"""
---
serverless: true

---
pipeline: {pipeline_name}
catalog: "{{catalog}}"
schema: "{{bronze_schema}}"
serverless: true
packaging: source
""",
    )

    parsed = _render(temp_project, config_path, pipeline_name)
    pipeline = _pipeline_block(parsed, pipeline_name)

    # Strip always applies, in both modes.
    assert "packaging" not in pipeline
    # No wheel dependency injected for source mode. ``environment`` is either
    # absent or, if present, carries no wheel reference under the artifact volume.
    environment = pipeline.get("environment")
    if environment is not None:
        for dep in environment.get("dependencies", []) or []:
            assert ARTIFACT_VOLUME not in str(dep)
