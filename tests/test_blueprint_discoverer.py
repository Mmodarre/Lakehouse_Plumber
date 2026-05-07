"""Spec-driven unit tests for BlueprintDiscoverer (Phase 4).

Covers default-pattern + custom-pattern discovery, the empty-project case,
and code 046 for duplicate blueprint names.
"""

from pathlib import Path

import pytest

from lhp.core.services.blueprint_discoverer import BlueprintDiscoverer
from lhp.models.config import ProjectConfig
from lhp.parsers.blueprint_parser import BlueprintParser
from lhp.utils.error_formatter import ErrorCategory, LHPError

pytestmark = pytest.mark.unit


def _write(path: Path, content: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content)
    return path


def _bp_yaml(name: str) -> str:
    return f"""
name: {name}
parameters:
  - name: site_name
    required: true
flowgroups:
  - pipeline: "%{{site_name}}_raw"
    flowgroup: "%{{site_name}}_orders"
    actions: []
"""


def _instance_yaml(blueprint: str, site: str) -> str:
    return f"""
blueprint: {blueprint}
site_name: {site}
"""


def test_discover_blueprints_default_patterns(tmp_path):
    _write(tmp_path / "blueprints" / "erp.yaml", _bp_yaml("erp_ingestion"))
    _write(tmp_path / "blueprints" / "cdc.yaml", _bp_yaml("cdc_pattern"))
    disco = BlueprintDiscoverer(
        tmp_path, project_config=None, blueprint_parser=BlueprintParser()
    )
    registry = disco.discover_blueprints()
    assert set(registry.keys()) == {"erp_ingestion", "cdc_pattern"}
    bp, path = registry["erp_ingestion"]
    assert bp.name == "erp_ingestion"
    assert path.name == "erp.yaml"


def test_discover_blueprints_returns_empty_when_no_files(tmp_path):
    disco = BlueprintDiscoverer(tmp_path)
    assert disco.discover_blueprints() == {}


def test_discover_blueprints_custom_patterns(tmp_path):
    # Place the blueprint in a non-default directory and configure include.
    _write(tmp_path / "shapes" / "erp.yaml", _bp_yaml("erp_ingestion"))
    _write(tmp_path / "blueprints" / "ignored.yaml", _bp_yaml("ignored"))
    config = ProjectConfig(
        name="test_project",
        blueprint_include=["shapes/**/*.yaml"],
    )
    disco = BlueprintDiscoverer(tmp_path, project_config=config)
    registry = disco.discover_blueprints()
    # Only the matching pattern should be picked up.
    assert set(registry.keys()) == {"erp_ingestion"}


def test_duplicate_blueprint_name_raises_046(tmp_path):
    _write(tmp_path / "blueprints" / "a.yaml", _bp_yaml("erp_ingestion"))
    _write(tmp_path / "blueprints" / "b.yaml", _bp_yaml("erp_ingestion"))
    disco = BlueprintDiscoverer(tmp_path)
    with pytest.raises(LHPError) as exc:
        disco.discover_blueprints()
    assert exc.value.code == "LHP-VAL-046"
    assert exc.value.category == ErrorCategory.VALIDATION
    # Must reference both colliding files.
    msg = str(exc.value)
    assert "a.yaml" in msg
    assert "b.yaml" in msg


def test_discover_instances_default_patterns(tmp_path):
    # New default: instance_include = ['pipelines/**/*.yaml']. Instance files
    # live alongside hand-written flowgroups under pipelines/<system>/<layer>/.
    _write(tmp_path / "blueprints" / "erp.yaml", _bp_yaml("erp_ingestion"))
    _write(
        tmp_path / "pipelines" / "erp" / "bronze" / "sg.yaml",
        _instance_yaml("erp_ingestion", "apac_sg"),
    )
    _write(
        tmp_path / "pipelines" / "erp" / "bronze" / "uk.yaml",
        _instance_yaml("erp_ingestion", "emea_uk"),
    )
    disco = BlueprintDiscoverer(tmp_path)
    blueprints = disco.discover_blueprints()
    instances = disco.discover_instances(blueprints)
    assert len(instances) == 2
    # Each result is (BlueprintInstance, Path).
    sites = {inst.parameter_values()["site_name"] for inst, _ in instances}
    assert sites == {"apac_sg", "emea_uk"}


def test_discover_instances_returns_empty_when_no_files(tmp_path):
    _write(tmp_path / "blueprints" / "erp.yaml", _bp_yaml("erp_ingestion"))
    disco = BlueprintDiscoverer(tmp_path)
    blueprints = disco.discover_blueprints()
    assert disco.discover_instances(blueprints) == []


def test_discover_instances_custom_patterns(tmp_path):
    _write(tmp_path / "blueprints" / "erp.yaml", _bp_yaml("erp_ingestion"))
    _write(
        tmp_path / "registry" / "sg.yaml", _instance_yaml("erp_ingestion", "apac_sg")
    )
    _write(
        tmp_path / "instances" / "ignored.yaml",
        _instance_yaml("erp_ingestion", "ignored_site"),
    )
    config = ProjectConfig(
        name="test_project",
        instance_include=["registry/**/*.yaml"],
    )
    disco = BlueprintDiscoverer(tmp_path, project_config=config)
    blueprints = disco.discover_blueprints()
    instances = disco.discover_instances(blueprints)
    sites = {inst.parameter_values()["site_name"] for inst, _ in instances}
    assert sites == {"apac_sg"}
