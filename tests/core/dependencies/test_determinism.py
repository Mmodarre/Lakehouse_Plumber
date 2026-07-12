"""Determinism guards for dependency-analysis outputs.

Set iteration order (PYTHONHASHSEED-dependent) and unsorted ``rglob`` order
used to leak into the networkx graphs: external-source node attrs, edge
insertion order, and ``depends_on`` / execution-stage ordering all varied
across byte-identical runs. The fix sorts at INSERTION (builder, discoverer,
analyzer) so the graphs themselves are deterministic — networkx preserves
insertion order in ``nodes`` / ``edges`` / ``predecessors`` /
``topological_generations``, and JSON/DOT/TEXT exports follow.

Two layers of pinning:

- an integration test that runs the full analysis in SUBPROCESSES under
  different ``PYTHONHASHSEED`` values (in-process hash-seed variation is
  impossible: the seed is fixed at interpreter start) and asserts
  byte-identical JSON + DOT output;
- a unit test that hand-builds flowgroups with scrambled insertion order and
  asserts sorted external-source attrs, sorted adjacency, and sorted
  ``depends_on`` lists directly on the built graphs.
"""

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

from lhp.core.dependencies.analyzer import DependencyAnalyzer
from lhp.core.dependencies.builder import DependencyGraphBuilder
from lhp.models import Action, ActionType, FlowGroup

# (pipeline, flowgroup, read sources, (schema, table) written to catalog `cat`).
# Every flowgroup reads >=3 external sources; the `cat.*.*` reads form the
# cross-pipeline edges zeta_ingest -> {alpha_refine, mango_enrich} -> delta_publish.
_PROJECT_FLOWGROUPS = [
    (
        "zeta_ingest",
        "ingest_fg",
        ["raw.zulu_events", "raw.alpha_events", "raw.mike_events"],
        ("zeta", "events"),
    ),
    (
        "alpha_refine",
        "refine_fg",
        ["cat.zeta.events", "ext.zebra_dim", "ext.apple_dim", "ext.mango_dim"],
        ("alpha", "refined"),
    ),
    (
        "mango_enrich",
        "enrich_fg",
        ["cat.zeta.events", "ext.yak_lookup", "ext.bear_lookup", "ext.newt_lookup"],
        ("mango", "enriched"),
    ),
    (
        "delta_publish",
        "publish_fg",
        [
            "cat.alpha.refined",
            "cat.mango.enriched",
            "ext.rho_cfg",
            "ext.phi_cfg",
            "ext.chi_cfg",
        ],
        ("delta", "report"),
    ),
]

# Runs the same entry the `dag` command routes through (analyze_project +
# export) and prints JSON and DOT to stdout. Neither export carries a
# timestamp field, so stdout must be byte-identical across hash seeds.
_DRIVER = """\
import sys
from pathlib import Path

from lhp.core.coordination.validation_service import ValidationService
from lhp.core.dependencies.service import DependencyAnalysisService
from lhp.models import ProjectConfig

project_root = Path(sys.argv[1])
project_config = ProjectConfig(name="determinism", version="1.0")
service = DependencyAnalysisService(
    project_root, project_config, ValidationService(project_root, project_config)
)
result = service.analyze_project()
sys.stdout.write(service.export(result, "json"))
sys.stdout.write("\\n===DOT===\\n")
sys.stdout.write(service.export(result, "dot"))
"""


def _build_project(project_root: Path) -> Path:
    for pipeline, flowgroup, sources, (schema, table) in _PROJECT_FLOWGROUPS:
        doc = {
            "pipeline": pipeline,
            "flowgroup": flowgroup,
            "actions": [
                {
                    "name": f"t_{flowgroup}",
                    "type": "transform",
                    "source": list(sources),
                    "target": f"v_{flowgroup}",
                },
                {
                    "name": f"w_{flowgroup}",
                    "type": "write",
                    "source": f"v_{flowgroup}",
                    "write_target": {
                        "type": "streaming_table",
                        "catalog": "cat",
                        "schema": schema,
                        "table": table,
                        "create_table": True,
                    },
                },
            ],
        }
        fg_path = project_root / "pipelines" / pipeline / f"{flowgroup}.yaml"
        fg_path.parent.mkdir(parents=True, exist_ok=True)
        with open(fg_path, "w") as f:
            yaml.dump(doc, f)
    return project_root


@pytest.mark.integration
def test_outputs_identical_across_hash_seeds(tmp_path):
    """JSON + DOT exports are byte-identical under PYTHONHASHSEED 0, 1, 2."""
    project_root = _build_project(tmp_path / "determinism_project")
    driver = tmp_path / "driver.py"
    driver.write_text(_DRIVER)

    outputs = []
    for seed in ("0", "1", "2"):
        proc = subprocess.run(
            [sys.executable, str(driver), str(project_root)],
            capture_output=True,
            text=True,
            env={**os.environ, "PYTHONHASHSEED": seed},
            cwd=str(tmp_path),
            check=False,
        )
        assert proc.returncode == 0, proc.stderr
        outputs.append(proc.stdout)

    assert outputs[0] == outputs[1] == outputs[2]

    # Non-vacuous: the identical output must describe the full project, with
    # the sorted-insertion guarantees visible in the serialized result.
    json_part, dot_part = outputs[0].split("\n===DOT===\n")
    data = json.loads(json_part)

    assert set(data["pipelines"]) == {
        "zeta_ingest",
        "alpha_refine",
        "mango_enrich",
        "delta_publish",
    }
    for dep in data["pipelines"].values():
        assert dep["depends_on"] == sorted(dep["depends_on"])
        assert dep["external_sources"] == sorted(dep["external_sources"])
        assert len(dep["external_sources"]) >= 3
    assert data["pipelines"]["delta_publish"]["depends_on"] == [
        "alpha_refine",
        "mango_enrich",
    ]

    assert [set(stage) for stage in data["execution_stages"]] == [
        {"zeta_ingest"},
        {"alpha_refine", "mango_enrich"},
        {"delta_publish"},
    ]
    assert '"zeta_ingest" -> "alpha_refine";' in dot_part


def _fg(pipeline: str, name: str, sources: list, schema: str, table: str) -> FlowGroup:
    return FlowGroup(
        pipeline=pipeline,
        flowgroup=name,
        actions=[
            Action(
                name=f"t_{name}",
                type=ActionType.TRANSFORM,
                source=list(sources),
                target=f"v_{name}",
            ),
            Action(
                name=f"w_{name}",
                type=ActionType.WRITE,
                source=f"v_{name}",
                write_target={
                    "type": "streaming_table",
                    "catalog": "cat",
                    "schema": schema,
                    "table": table,
                    "create_table": True,
                },
            ),
        ],
    )


@pytest.mark.unit
def test_edges_and_external_sources_sorted(tmp_path):
    """Builder inserts external sources and edges sorted; depends_on is sorted.

    Fan-out (p_hub -> p_beta/p_mid/p_zeta) and fan-in (-> p_sink) with the
    flowgroup list and every source list deliberately scrambled.
    """
    hub = _fg("p_hub", "fg_hub", ["src.zulu", "src.echo", "src.alpha"], "hub", "core")
    zeta = _fg(
        "p_zeta",
        "fg_zeta",
        ["cat.hub.core", "src.whiskey", "src.delta", "src.kilo"],
        "zeta",
        "out",
    )
    beta = _fg(
        "p_beta",
        "fg_beta",
        ["cat.hub.core", "src.victor", "src.foxtrot", "src.lima"],
        "beta",
        "out",
    )
    mid = _fg(
        "p_mid",
        "fg_mid",
        ["cat.hub.core", "src.uniform", "src.charlie", "src.november"],
        "mid",
        "out",
    )
    sink = _fg(
        "p_sink",
        "fg_sink",
        ["cat.zeta.out", "cat.beta.out", "cat.mid.out", "src.tango", "src.bravo"],
        "sink",
        "report",
    )

    builder = DependencyGraphBuilder(project_root=tmp_path)
    graphs = builder.build_from_flowgroups([mid, sink, hub, zeta, beta], {})

    for graph in (graphs.flowgroup_graph, graphs.pipeline_graph):
        for node in graph.nodes:
            external = graph.nodes[node].get("external_sources", [])
            assert external == sorted(external)
            assert list(graph.successors(node)) == sorted(graph.successors(node))
            assert list(graph.predecessors(node)) == sorted(graph.predecessors(node))

    assert graphs.flowgroup_graph.nodes["p_hub.fg_hub"]["external_sources"] == [
        "src.alpha",
        "src.echo",
        "src.zulu",
    ]
    assert graphs.pipeline_graph.nodes["p_hub"]["external_sources"] == [
        "src.alpha",
        "src.echo",
        "src.zulu",
    ]

    assert list(graphs.pipeline_graph.successors("p_hub")) == [
        "p_beta",
        "p_mid",
        "p_zeta",
    ]
    assert list(graphs.pipeline_graph.predecessors("p_sink")) == [
        "p_beta",
        "p_mid",
        "p_zeta",
    ]
    assert list(graphs.flowgroup_graph.successors("p_hub.fg_hub")) == [
        "p_beta.fg_beta",
        "p_mid.fg_mid",
        "p_zeta.fg_zeta",
    ]
    assert list(graphs.flowgroup_graph.predecessors("p_sink.fg_sink")) == [
        "p_beta.fg_beta",
        "p_mid.fg_mid",
        "p_zeta.fg_zeta",
    ]

    result = DependencyAnalyzer().analyze(graphs)
    for dep in result.pipeline_dependencies.values():
        assert dep.depends_on == sorted(dep.depends_on)
    assert result.pipeline_dependencies["p_sink"].depends_on == [
        "p_beta",
        "p_mid",
        "p_zeta",
    ]
