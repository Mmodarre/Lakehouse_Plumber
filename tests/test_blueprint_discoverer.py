"""Spec-driven unit tests for BlueprintDiscoverer (Phase 4).

Covers default-pattern + custom-pattern discovery, the empty-project case,
and code 046 for duplicate blueprint names.
"""

from pathlib import Path

import pytest

from lhp.core.discovery.blueprint_discoverer import BlueprintDiscoverer
from lhp.errors import ErrorCategory, LHPError
from lhp.models import ProjectConfig
from lhp.parsers.blueprint_parser import BlueprintParser
from lhp.parsers.yaml_parser import CachingYAMLParser, YAMLParser

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


def test_discover_instances_uses_cached_parser_single_load(tmp_path):
    """When wired with a CachingYAMLParser, every unique YAML file is read
    from disk exactly once across the entire blueprint+instance discovery
    flow. A second discover_instances pass adds no misses (all hits)."""
    _write(tmp_path / "blueprints" / "erp.yaml", _bp_yaml("erp_ingestion"))
    _write(
        tmp_path / "pipelines" / "erp" / "sg.yaml",
        _instance_yaml("erp_ingestion", "apac_sg"),
    )
    _write(
        tmp_path / "pipelines" / "erp" / "uk.yaml",
        _instance_yaml("erp_ingestion", "emea_uk"),
    )

    cache = CachingYAMLParser(YAMLParser())
    disco = BlueprintDiscoverer(tmp_path, caching_yaml_parser=cache)

    # discover_blueprints reads 1 file; discover_instances peeks then parses
    # 2 instance files (parse_instance_file re-reads via cache → hit).
    blueprints = disco.discover_blueprints()
    disco.discover_instances(blueprints)

    cold = cache.get_cache_stats()
    # 3 unique files (1 blueprint + 2 instances) → cache has 3 entries,
    # each loaded from disk exactly once.
    assert cold["documents_cache_size"] == 3
    assert cold["misses"] == 3
    # parse_instance_file re-reads each of the 2 instances → 2 hits.
    assert cold["hits"] >= 2

    # Warm pass: no new physical reads, only hits.
    disco.discover_instances(blueprints)
    warm = cache.get_cache_stats()
    assert warm["misses"] == cold["misses"], "warm pass must not re-read disk"
    assert warm["hits"] > cold["hits"], "warm pass must produce additional hits"


def test_blueprint_discoverer_emits_warning_on_load_errors(tmp_path, caplog):
    """A malformed YAML under pipelines/ must surface as a WARNING summary
    (not vanish at default log level), while valid instances still parse."""
    import logging

    _write(tmp_path / "blueprints" / "erp.yaml", _bp_yaml("erp_ingestion"))
    _write(
        tmp_path / "pipelines" / "erp" / "sg.yaml",
        _instance_yaml("erp_ingestion", "apac_sg"),
    )
    # Malformed YAML: unclosed bracket triggers yaml.YAMLError inside the
    # loader, which it wraps into an LHPConfigError.
    _write(
        tmp_path / "pipelines" / "erp" / "bad.yaml",
        "blueprint: erp_ingestion\nparameters: [unclosed\n",
    )

    disco = BlueprintDiscoverer(tmp_path)
    blueprints = disco.discover_blueprints()

    with caplog.at_level(
        logging.WARNING, logger="lhp.core.discovery.blueprint_discoverer"
    ):
        instances = disco.discover_instances(blueprints)

    # Valid instance still returned.
    assert len(instances) == 1
    sites = {inst.parameter_values()["site_name"] for inst, _ in instances}
    assert sites == {"apac_sg"}

    # Filter to *this* logger; unrelated WARNINGs (e.g. deprecation messages
    # from the legacy "lhp.models.config" logger) are noise for this assertion.
    warnings = [
        r
        for r in caplog.records
        if r.levelno == logging.WARNING
        and r.name == "lhp.core.discovery.blueprint_discoverer"
    ]
    assert len(warnings) == 1
    msg = warnings[0].getMessage()
    assert "bad.yaml" in msg
    assert "1 instance candidate" in msg


def test_blueprint_discoverer_silent_on_non_instance_files(tmp_path, caplog):
    """Hand-written flowgroup YAMLs under pipelines/ are NOT instances; the
    discoverer must skip them silently at default (WARNING) log level."""
    import logging

    _write(tmp_path / "blueprints" / "erp.yaml", _bp_yaml("erp_ingestion"))
    _write(
        tmp_path / "pipelines" / "erp" / "sg.yaml",
        _instance_yaml("erp_ingestion", "apac_sg"),
    )
    # Flowgroup-shaped file: no `use_blueprint:` / `blueprint:` key.
    _write(
        tmp_path / "pipelines" / "erp" / "fg.yaml",
        "pipeline: raw_pipeline\nflowgroup: my_fg\nactions: []\n",
    )

    disco = BlueprintDiscoverer(tmp_path)
    blueprints = disco.discover_blueprints()

    with caplog.at_level(
        logging.WARNING, logger="lhp.core.discovery.blueprint_discoverer"
    ):
        instances = disco.discover_instances(blueprints)

    # Valid instance still parsed; flowgroup silently routed away.
    assert len(instances) == 1
    # Scope to the discoverer's logger: deprecation warnings from
    # lhp.models.config (triggered by the legacy `blueprint:` flat syntax
    # in _instance_yaml) are unrelated to this test's invariant.
    warnings = [
        r
        for r in caplog.records
        if r.levelno == logging.WARNING
        and r.name == "lhp.core.discovery.blueprint_discoverer"
    ]
    assert warnings == [], (
        "Non-instance files (regular flowgroups) must not produce "
        "discoverer warnings at default log level; got: "
        f"{[w.getMessage() for w in warnings]}"
    )
