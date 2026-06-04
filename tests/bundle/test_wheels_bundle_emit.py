"""Reserved ``_wheels`` dir skipping and ``_wheels.bundle.yml`` emission.

Construction pattern (project_root + multi-doc pipeline_config.yaml +
substitutions/<env>.yaml + ``BundleManager(...)``) is copied from
``tests/test_bundle_full_substitution.py::TestFullPipelineConfigSubstitution``,
extended with ``project_config=ProjectConfig(name=..., wheel=WheelConfig(...))``
so the wheel-packaging code path has an artifact volume to resolve.

``emit_wheels_bundle_file`` self-derives its wheel-pipeline list from the
generated pipeline directories under ``output_dir`` (via
``get_pipeline_directories`` + ``resolve_packaging_modes``) and resolves the
artifact volume; it does NOT glob for built ``.whl`` files, so these tests plant
only pipeline directories + ``packaging: wheel`` config, not wheel artifacts.
"""

import shutil
import tempfile
from pathlib import Path

import pytest
import yaml

from lhp.bundle.manager import BundleManager
from lhp.errors import LHPError
from lhp.models import ProjectConfig, WheelConfig

ENV = "dev"
# A ${token}-bearing volume so the emit test can prove resolution actually
# happens (the resolved artifact_path must contain no leftover LHP ${} tokens).
ARTIFACT_VOLUME_RAW = "/Volumes/${wheel_catalog}/${wheel_schema}/artifacts"
RESOLVED_ARTIFACT_VOLUME = "/Volumes/dev_cat/dev_sch/artifacts"


@pytest.fixture
def temp_project():
    temp_dir = tempfile.mkdtemp()
    project_root = Path(temp_dir)

    (project_root / "substitutions").mkdir(parents=True)
    (project_root / "config").mkdir(parents=True)
    (project_root / "pipelines").mkdir(parents=True)
    (project_root / "generated").mkdir(parents=True)

    sub_content = {
        ENV: {
            "catalog": "dev_catalog",
            "bronze_schema": "bronze_dev",
            "wheel_catalog": "dev_cat",
            "wheel_schema": "dev_sch",
        }
    }
    with open(project_root / "substitutions" / f"{ENV}.yaml", "w") as f:
        yaml.dump(sub_content, f)

    yield project_root

    shutil.rmtree(temp_dir)


def _project_config(artifact_volume=ARTIFACT_VOLUME_RAW):
    return ProjectConfig(
        name="wheel_test_project",
        wheel=WheelConfig(artifact_volume=artifact_volume),
    )


def _write_pipeline_config(project_root: Path, body: str) -> Path:
    config_path = project_root / "config" / "pipeline_config.yaml"
    with open(config_path, "w") as f:
        f.write(body)
    return config_path


def _make_generated_pipeline_dir(project_root: Path, pipeline_name: str) -> Path:
    """Create generated/<env>/<pipeline>/ with a stub generated file so it looks
    like a real pipeline output directory."""
    pdir = project_root / "generated" / ENV / pipeline_name
    pdir.mkdir(parents=True, exist_ok=True)
    (pdir / f"{pipeline_name}.py").write_text("# generated\n", encoding="utf-8")
    return pdir


def _manager(project_root: Path, config_path: Path, project_config=None):
    return BundleManager(
        project_root=project_root,
        pipeline_config_path=str(config_path),
        project_config=project_config or _project_config(),
    )


# --------------------------------------------------------------------------- #
# reserved _wheels subdir is not treated as a pipeline directory               #
# --------------------------------------------------------------------------- #


@pytest.mark.unit
def test_wheels_dir_not_a_pipeline(temp_project):
    """A directory literally named ``_wheels`` under ``generated/<env>/`` is NOT
    returned by ``get_pipeline_directories``, while normal pipeline dirs are.
    """
    output_dir = temp_project / "generated" / ENV
    output_dir.mkdir(parents=True)

    # Two real pipeline directories ...
    _make_generated_pipeline_dir(temp_project, "alpha_pipeline")
    _make_generated_pipeline_dir(temp_project, "beta_pipeline")
    # ... and the reserved wheel-staging dir (with nested content, as in real life).
    wheels_dir = output_dir / "_wheels"
    (wheels_dir / "alpha_pipeline" / "dist").mkdir(parents=True)
    (wheels_dir / "alpha_pipeline" / "dist" / "x-0.1-py3-none-any.whl").write_bytes(
        b"PK\x03\x04"
    )

    config_path = _write_pipeline_config(temp_project, "---\nserverless: true\n")
    manager = _manager(temp_project, config_path)

    dirs = manager.get_pipeline_directories(output_dir)
    names = sorted(p.name for p in dirs)

    assert names == ["alpha_pipeline", "beta_pipeline"]
    assert "_wheels" not in names
    # And the reserved dir really does exist on disk — it was skipped, not absent.
    assert wheels_dir.is_dir()


def _setup_wheel_pipelines(temp_project: Path, names) -> Path:
    """Create generated pipeline dirs for ``names`` + a pipeline_config marking
    them all ``packaging: wheel``. Returns the output_dir (generated/<env>)."""
    output_dir = temp_project / "generated" / ENV
    output_dir.mkdir(parents=True, exist_ok=True)

    docs = ["---\nserverless: true\n"]
    for name in names:
        _make_generated_pipeline_dir(temp_project, name)
        docs.append(
            f"---\npipeline: {name}\ncatalog: dev_catalog\n"
            f"schema: bronze_dev\nserverless: true\npackaging: wheel\n"
        )
    _write_pipeline_config(temp_project, "\n".join(docs))
    return output_dir


@pytest.mark.unit
def test_emit_writes_wheels_bundle_file_with_resolved_artifact_path(temp_project):
    """>=1 wheel pipeline -> resources/lhp/_wheels.bundle.yml is written with
    per-pipeline artifacts, a RESOLVED artifact_path (no leftover LHP ${} tokens),
    ${bundle.target} preserved literally in sync.exclude, parseable into the spec
    structure."""
    output_dir = _setup_wheel_pipelines(temp_project, ["alpha_pipe", "beta_pipe"])
    # _setup_wheel_pipelines wrote config/pipeline_config.yaml with both pipelines
    # marked packaging: wheel.
    manager = _manager(temp_project, temp_project / "config" / "pipeline_config.yaml")

    manager.emit_wheels_bundle_file(output_dir, ENV)

    wheels_file = temp_project / "resources" / "lhp" / "_wheels.bundle.yml"
    assert wheels_file.is_file(), "expected _wheels.bundle.yml to be written"

    raw = wheels_file.read_text(encoding="utf-8")
    parsed = yaml.safe_load(raw)

    # artifacts.<pipeline>_whl with type: whl + path: generated/<env>/_wheels/<pipeline>
    artifacts = parsed["artifacts"]
    for name in ("alpha_pipe", "beta_pipe"):
        entry = artifacts[f"{name}_whl"]
        assert entry["type"] == "whl"
        assert entry["path"] == f"generated/{ENV}/_wheels/{name}"

    # targets.<env>.workspace.artifact_path == resolved /Volumes path
    artifact_path = parsed["targets"][ENV]["workspace"]["artifact_path"]
    assert artifact_path == RESOLVED_ARTIFACT_VOLUME
    # No leftover LHP ${...} token survived resolution in the artifact_path.
    assert "${" not in artifact_path

    # sync.exclude preserves the Databricks runtime variable ${bundle.target}
    # LITERALLY (it is not an LHP token and must not be substituted).
    excludes = parsed["sync"]["exclude"]
    assert "generated/${bundle.target}/_wheels/**" in excludes
    assert "${bundle.target}" in raw


@pytest.mark.unit
def test_emit_noop_when_zero_wheel_pipelines(temp_project):
    """Zero wheel-mode pipelines -> no file written (source-only projects gain no
    bundle file)."""
    output_dir = temp_project / "generated" / ENV
    output_dir.mkdir(parents=True)
    _make_generated_pipeline_dir(temp_project, "src_pipe_a")
    _make_generated_pipeline_dir(temp_project, "src_pipe_b")

    config_path = _write_pipeline_config(
        temp_project,
        "---\nserverless: true\n"
        "\n---\npipeline: src_pipe_a\ncatalog: dev_catalog\n"
        "schema: bronze_dev\nserverless: true\npackaging: source\n"
        "\n---\npipeline: src_pipe_b\ncatalog: dev_catalog\n"
        "schema: bronze_dev\nserverless: true\n",  # no packaging key -> source default
    )
    manager = _manager(temp_project, config_path)

    manager.emit_wheels_bundle_file(output_dir, ENV)

    wheels_file = temp_project / "resources" / "lhp" / "_wheels.bundle.yml"
    assert not wheels_file.exists()


@pytest.mark.unit
def test_emit_raises_cfg_061_for_non_volumes_artifact_path(temp_project):
    """A wheel pipeline exists but artifact_volume does not start with /Volumes/
    -> LHP-CFG-061."""
    output_dir = _setup_wheel_pipelines(temp_project, ["alpha_pipe"])
    manager = _manager(
        temp_project,
        temp_project / "config" / "pipeline_config.yaml",
        project_config=_project_config(artifact_volume="/Workspace/not/a/volume"),
    )

    with pytest.raises(LHPError) as exc_info:
        manager.emit_wheels_bundle_file(output_dir, ENV)

    assert exc_info.value.code == "LHP-CFG-061"
    # No file should have been written on the failure path.
    wheels_file = temp_project / "resources" / "lhp" / "_wheels.bundle.yml"
    assert not wheels_file.exists()


@pytest.mark.unit
def test_emit_raises_cfg_061_when_artifact_volume_absent(temp_project):
    """A wheel pipeline exists but the project declares no wheel.artifact_volume
    (absent/empty) -> LHP-CFG-061."""
    output_dir = _setup_wheel_pipelines(temp_project, ["alpha_pipe"])
    manager = _manager(
        temp_project,
        temp_project / "config" / "pipeline_config.yaml",
        project_config=ProjectConfig(name="wheel_test_project", wheel=None),
    )

    with pytest.raises(LHPError) as exc_info:
        manager.emit_wheels_bundle_file(output_dir, ENV)

    assert exc_info.value.code == "LHP-CFG-061"
