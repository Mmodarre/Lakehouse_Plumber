"""Facade-level integration tests for the persistent parse cache.

On a small real project, runs generate / validate / dependency-analysis in
three modes — cold (empty disk cache), warm (fresh facade over a populated
disk cache), and ``no_cache=True`` — and asserts byte-identical generated
output and identical analysis/validation results across all three. The cache
must be a pure accelerator: never a behavior change.

Also proves the invalidation seam end to end: mutating one YAML file between
facades is reflected in the next run (the mtime_ns/size shard key misses).
"""

import collections
from pathlib import Path
from typing import Dict

import pytest
import yaml

from lhp.api.facade import LakehousePlumberApplicationFacade

_PIPELINE_LAYOUT = {
    "ingest": ["customers", "orders"],
    "reference": ["regions"],
}


def _build_project(project_root: Path) -> Path:
    """Small real LHP project: 3 flowgroups across 2 pipelines + dev env."""
    project_root.mkdir(parents=True, exist_ok=True)
    (project_root / "lhp.yaml").write_text(
        'name: parse_cache_it\nversion: "0.1.0"\nauthor: test\n'
    )
    (project_root / "presets").mkdir()
    (project_root / "templates").mkdir()
    (project_root / "substitutions").mkdir()

    substitutions = {
        "dev": {
            "catalog": "dev_catalog",
            "bronze_schema": "bronze",
            "landing_path": "/mnt/dev/landing",
        }
    }
    with open(project_root / "substitutions" / "dev.yaml", "w") as f:
        yaml.dump(substitutions, f)

    for subdir, table_names in _PIPELINE_LAYOUT.items():
        (project_root / "pipelines" / subdir).mkdir(parents=True)
        for table in table_names:
            flowgroup = {
                "pipeline": subdir,
                "flowgroup": f"{table}_fg",
                "actions": [
                    {
                        "name": f"load_{table}",
                        "type": "load",
                        "target": f"v_{table}_raw",
                        "source": {
                            "type": "cloudfiles",
                            "path": "${landing_path}/" + table,
                            "format": "json",
                        },
                    },
                    {
                        "name": f"write_{table}",
                        "type": "write",
                        "source": f"v_{table}_raw",
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
            with open(
                project_root / "pipelines" / subdir / f"{table}_fg.yaml", "w"
            ) as f:
                yaml.dump(flowgroup, f)

    return project_root


def _fresh_facade(
    project_root: Path, *, no_cache: bool = False
) -> LakehousePlumberApplicationFacade:
    """New facade = new orchestrator = empty in-memory caches.

    ``max_workers=1`` keeps runs sequential so event streams are
    deterministic and comparable across modes.
    """
    return LakehousePlumberApplicationFacade.for_project(
        project_root,
        enforce_version=False,
        max_workers=1,
        no_cache=no_cache,
    )


def _run_all_operations(facade, output_dir: Path):
    """Generate + validate + dependency-analysis; return comparable results."""
    generate_events = list(
        facade.generation.generate_pipelines(
            env="dev",
            output_dir=output_dir,
            bundle_enabled=False,
            apply_formatting=False,
        )
    )
    generated_tree: Dict[str, str] = {
        str(path.relative_to(output_dir)): path.read_text()
        for path in sorted(output_dir.rglob("*"))
        if path.is_file()
    }

    validate_events = list(facade.validation.validate_pipelines(env="dev"))

    analysis = facade.dependency.analyze_dependencies()
    analysis_projection = (
        {k: tuple(v) for k, v in analysis.pipeline_dependencies.items()},
        analysis.execution_stages,
        analysis.external_sources,
        analysis.total_pipelines,
        analysis.total_external_sources,
    )

    return {
        "generate_event_counts": collections.Counter(
            type(e).__name__ for e in generate_events
        ),
        "generated_tree": generated_tree,
        "validate_event_counts": collections.Counter(
            type(e).__name__ for e in validate_events
        ),
        "analysis": analysis_projection,
    }


@pytest.mark.integration
def test_cold_warm_and_no_cache_runs_are_identical(tmp_path):
    project_root = _build_project(tmp_path / "project")

    cold = _run_all_operations(_fresh_facade(project_root), tmp_path / "out_cold")
    cache_dir = project_root / ".lhp" / "cache" / "parse"
    assert len(list(cache_dir.glob("*.pkl"))) == 3, (
        "Cold run must leave one shard per pipeline YAML file"
    )

    warm = _run_all_operations(_fresh_facade(project_root), tmp_path / "out_warm")
    no_cache = _run_all_operations(
        _fresh_facade(project_root, no_cache=True), tmp_path / "out_nocache"
    )

    assert cold["generated_tree"], "Generation produced no files (vacuous test)"
    assert warm == cold
    assert no_cache == cold


@pytest.mark.integration
def test_yaml_mutation_between_facades_is_reflected(tmp_path):
    project_root = _build_project(tmp_path / "project")

    cold = _run_all_operations(_fresh_facade(project_root), tmp_path / "out_cold")
    assert any("customers" in content for content in cold["generated_tree"].values())

    # Mutate one flowgroup on disk: rename the written table. The persistent
    # shard for this file must be invalidated by its new mtime_ns/size.
    mutated_file = project_root / "pipelines" / "ingest" / "customers_fg.yaml"
    with open(mutated_file) as f:
        flowgroup = yaml.safe_load(f)
    flowgroup["actions"][1]["write_target"]["table"] = "customers_renamed"
    with open(mutated_file, "w") as f:
        yaml.dump(flowgroup, f)

    mutated = _run_all_operations(_fresh_facade(project_root), tmp_path / "out_mut")

    assert mutated["generated_tree"] != cold["generated_tree"]
    assert any(
        "customers_renamed" in content for content in mutated["generated_tree"].values()
    ), "The mutated table name must appear in freshly generated output"
