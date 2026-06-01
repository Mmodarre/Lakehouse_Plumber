"""Integration (§-S4 parse-once guard for Phase 2): each pipeline YAML file is
physically read+parsed EXACTLY ONCE per ``discover_all_flowgroups()`` call.

Discovery runs two passes that both glob and load every file matching
``pipelines/**/*.yaml``:

  1. the flowgroup pass —
     :meth:`lhp.core.discovery.flowgroup_discoverer.FlowgroupDiscoveryService.discover_all_flowgroups_with_paths`
     -> ``CachingYAMLParser.parse_flowgroups_from_file`` -> ``load_documents_all``;
  2. the instance pass —
     :meth:`lhp.core.discovery.blueprint_discoverer.BlueprintDiscoverer.discover_instances`,
     which loads EVERY candidate file (including regular flowgroup files) just to
     peek ``documents[0]`` and classify it via
     :meth:`lhp.parsers.blueprint_parser.BlueprintParser.looks_like_instance`,
     then skips the non-instance ones -> ``_load_documents`` ->
     the same shared ``CachingYAMLParser.load_documents_all``.

Before T6/T7 the instance pass re-read every file from disk (physical read
count 2 per file). T6/T7 introduced the shared ``CachingYAMLParser`` and its
mtime-keyed ``_documents_cache``: the flowgroup pass populates it on a miss and
the instance pass hits it, collapsing the read to count 1. T7 proved that 2->1
reduction for a single file; this module makes it a permanent regression guard
over a real multi-file project.

Spy target (§8.8 — spy a real boundary, do not fake what we own): the physical
read is :func:`lhp.parsers.yaml_loader.load_yaml_documents_all`. It is patched
``wraps=`` the real function so genuine parsing still happens and we can count
calls per path. Note the patch target is the binding *as imported into
``lhp.parsers.yaml_parser``* (``lhp.parsers.yaml_parser.load_yaml_documents_all``),
NOT the definition site in ``yaml_loader``. The active read path on this project
is the cache shell ``CachingYAMLParser.load_documents_all``, whose miss-loader
``lambda`` resolves ``load_yaml_documents_all`` against ``yaml_parser``'s module
globals (the line ``from .yaml_loader import load_yaml_documents_all`` binds a
separate name there at import time). Patching only the definition site leaves
that global pointing at the original and the spy records ZERO calls — i.e. the
non-empty assertion below would catch a mis-aimed patch as a vacuous test.

Simplification (sanctioned by the task): this uses a pure-FlowGroup project
rather than a real blueprint+instance pair. Both discovery passes already glob
and load every ``pipelines/**/*.yaml`` file regardless of its shape (the
instance pass must read each one to classify it), so the parse-once assertion is
exercised identically without the fiddle of constructing a valid blueprint.

:stability: provisional
"""

import collections
from pathlib import Path
from unittest.mock import patch

import pytest
import yaml

# Patch the binding as seen by the active read path (see module docstring),
# wrapping the real function object captured here.
import lhp.parsers.yaml_parser as yaml_parser_module
from lhp.core.coordination.layers import build_facade_orchestrator

_PIPELINE_LAYOUT = {
    "ingest": ["customers", "orders"],
    "reference": ["regions"],
}


def _build_multi_flowgroup_project(tmpdir: Path) -> Path:
    """Build a minimal LHP project with several regular FlowGroup files.

    Mirrors the ``tmp_path`` construction idiom in
    ``tests/core/discovery/test_orphan_instance.py`` and
    ``tests/core/coordination/test_bootstrap_discovery_memo.py``
    (presets/, templates/, substitutions/dev.yaml). Lays down >=3 flowgroup
    files spread across two pipeline subdirectories so both discovery passes
    have several files to glob and load. No ``lhp.yaml`` is written, so the
    default ``include = ['pipelines/**/*.yaml']`` glob discovers them all.
    """
    project_root = Path(tmpdir)
    project_root.mkdir(parents=True, exist_ok=True)
    (project_root / "presets").mkdir()
    (project_root / "templates").mkdir()
    (project_root / "substitutions").mkdir()
    for subdir in _PIPELINE_LAYOUT:
        (project_root / "pipelines" / subdir).mkdir(parents=True)

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
                        "name": f"clean_{table}",
                        "type": "transform",
                        "transform_type": "sql",
                        "source": f"v_{table}_raw",
                        "target": f"v_{table}_clean",
                        "sql": f"SELECT * FROM v_{table}_raw",
                    },
                    {
                        "name": f"write_{table}",
                        "type": "write",
                        "source": f"v_{table}_clean",
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
            fg_path = project_root / "pipelines" / subdir / f"{table}_fg.yaml"
            with open(fg_path, "w") as f:
                yaml.dump(flowgroup, f)

    return project_root


def _expected_pipeline_yaml_keys(project_root: Path) -> set:
    """Resolved-path keys for every ``pipelines/**/*.yaml`` file on disk.

    The spy keys its Counter on ``str(Path(file_path).resolve())`` to match the
    cache key, which the production code computes via ``path.resolve()``
    (``CachingYAMLParser._cached_load``).
    """
    return {str(p.resolve()) for p in project_root.glob("pipelines/**/*.yaml")}


@pytest.mark.integration
def test_each_pipeline_file_parsed_exactly_once(tmp_path):
    """One ``discover_all_flowgroups()`` reads each pipeline YAML exactly once.

    The flowgroup pass and the (always-run) instance pass both glob and load
    every ``pipelines/**/*.yaml`` file; the shared ``_documents_cache`` collapses
    the two loads of each file into a single physical
    ``load_yaml_documents_all`` read. Asserts non-vacuously: the spy captured at
    least one read per file, and every expected file appears with count exactly
    one.
    """
    project_root = _build_multi_flowgroup_project(tmp_path / "parse_once_project")
    expected_keys = _expected_pipeline_yaml_keys(project_root)
    # Sanity: the project really has >=3 pipeline YAML files across two dirs.
    assert len(expected_keys) >= 3

    real_load_yaml_documents_all = yaml_parser_module.load_yaml_documents_all
    read_counter: "collections.Counter[str]" = collections.Counter()

    def counting_load(file_path, *args, **kwargs):
        # Count per resolved path, then delegate to the REAL parser so
        # discovery behavior is unchanged (§8.8: wrap, don't fake).
        read_counter[str(Path(file_path).resolve())] += 1
        return real_load_yaml_documents_all(file_path, *args, **kwargs)

    orchestrator = build_facade_orchestrator(project_root, enforce_version=False)

    with patch.object(
        yaml_parser_module,
        "load_yaml_documents_all",
        side_effect=counting_load,
    ):
        flowgroups = orchestrator.bootstrap.discover_all_flowgroups()

    # Real wiring ran end-to-end: every flowgroup file produced a flowgroup.
    assert len(flowgroups) == len(expected_keys), flowgroups

    # Non-vacuous: the spy actually captured the reads.
    assert read_counter, (
        "The load spy captured zero reads. The patch target "
        "(lhp.parsers.yaml_parser.load_yaml_documents_all) is no longer on the "
        "active read path -- a vacuous test. See module docstring."
    )
    assert sum(read_counter.values()) >= len(expected_keys), (
        f"Spy captured {sum(read_counter.values())} read(s) for "
        f"{len(expected_keys)} pipeline YAML file(s); expected at least one "
        "read per file."
    )

    # Parse-once: every pipeline file present, each read exactly once.
    missing = expected_keys - set(read_counter)
    assert not missing, f"Pipeline file(s) never read during discovery: {missing}"

    over_read = {path: c for path, c in read_counter.items() if c != 1}
    assert not over_read, (
        "Each pipelines/**/*.yaml file must be physically read EXACTLY once "
        "across both discovery passes; these were not read exactly once: "
        f"{over_read}. A count > 1 means the instance pass bypassed the shared "
        "_documents_cache and re-read the file from disk (the pre-T6/T7 "
        "regression)."
    )
    assert all(c == 1 for c in read_counter.values())


def _build_n_flowgroup_project(tmpdir: Path, n: int) -> Path:
    """Build an LHP project with exactly ``n`` regular FlowGroup files.

    Same idiom as :func:`_build_multi_flowgroup_project` (presets/, templates/,
    substitutions/dev.yaml, default ``pipelines/**/*.yaml`` glob), but lays down
    a parameterized count of files in a single pipeline subdirectory so the
    discovery working set can be made to exceed a small cache cap cheaply.
    """
    project_root = Path(tmpdir)
    project_root.mkdir(parents=True, exist_ok=True)
    (project_root / "presets").mkdir()
    (project_root / "templates").mkdir()
    (project_root / "substitutions").mkdir()
    (project_root / "pipelines" / "ingest").mkdir(parents=True)

    substitutions = {
        "dev": {
            "catalog": "dev_catalog",
            "bronze_schema": "bronze",
            "landing_path": "/mnt/dev/landing",
        }
    }
    with open(project_root / "substitutions" / "dev.yaml", "w") as f:
        yaml.dump(substitutions, f)

    for i in range(n):
        table = f"t{i:02d}"
        flowgroup = {
            "pipeline": "ingest",
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
        fg_path = project_root / "pipelines" / "ingest" / f"{table}_fg.yaml"
        with open(fg_path, "w") as f:
            yaml.dump(flowgroup, f)

    return project_root


@pytest.mark.integration
def test_capacity_reserved_prevents_eviction_reread(tmp_path):
    """reserve_capacity keeps the cache from evicting its own warmed entries
    when the working set exceeds the cap, so the instance pass never re-reads.

    This is the Design-A capacity fix reproduced at small scale. Without the
    fix, a parser whose cap is below the file count would evict during the
    flowgroup pass and the instance pass would re-read the evicted files from
    disk (physical read count 2 for those files). With reserve_capacity, the
    flowgroup pass raises the shared ceiling to its glob size before loading, so
    every entry stays resident across both passes and each file is read once.

    The full 500-file production scenario is collapsed into 15 files by shrinking
    the parser cap to 10 BEFORE discovery (see comment below): this reproduces
    the >cap working-set condition without a large fixture.
    """
    file_count = 15
    project_root = _build_n_flowgroup_project(tmp_path / "capacity_project", file_count)
    expected_keys = _expected_pipeline_yaml_keys(project_root)
    assert len(expected_keys) == file_count

    orchestrator = build_facade_orchestrator(project_root, enforce_version=False)

    # Simulate the production >cap working set without a 500-file fixture:
    # shrink the shared cache cap below the file count. Eviction removes
    # cap // 10 entries once a sub-cache reaches the cap, so cap 10 with 15
    # files forces real eviction during the flowgroup pass UNLESS
    # reserve_capacity raises the ceiling first.
    orchestrator._cached_yaml_parser._max_cache_size = 10

    real_load_yaml_documents_all = yaml_parser_module.load_yaml_documents_all
    read_counter: "collections.Counter[str]" = collections.Counter()

    def counting_load(file_path, *args, **kwargs):
        read_counter[str(Path(file_path).resolve())] += 1
        return real_load_yaml_documents_all(file_path, *args, **kwargs)

    with patch.object(
        yaml_parser_module,
        "load_yaml_documents_all",
        side_effect=counting_load,
    ):
        flowgroups = orchestrator.bootstrap.discover_all_flowgroups()

    assert len(flowgroups) == file_count, flowgroups

    # (a) The cap GREW past the file count -> reserve_capacity fired during the
    # flowgroup pass (it started at 10, below the 15-file working set).
    assert orchestrator._cached_yaml_parser._max_cache_size >= file_count

    # (b) No eviction-induced re-read: every pipeline file was physically read
    # EXACTLY once across both passes. Without the fix the cap-10 parser would
    # evict during the flowgroup pass and the instance pass would re-read the
    # evicted files (count 2). This is the capacity regression guard.
    assert read_counter, "The load spy captured zero reads (mis-aimed patch)."
    missing = expected_keys - set(read_counter)
    assert not missing, f"Pipeline file(s) never read during discovery: {missing}"
    over_read = {path: c for path, c in read_counter.items() if c != 1}
    assert not over_read, (
        "Each pipelines/**/*.yaml file must be read EXACTLY once across both "
        f"discovery passes; these were re-read after eviction: {over_read}. "
        "A count > 1 means reserve_capacity did not raise the cap and the "
        "instance pass re-read evicted entries (the pre-fix regression)."
    )
    assert all(c == 1 for c in read_counter.values())
