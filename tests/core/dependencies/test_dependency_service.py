"""Service-level tests: full blueprint expansion + the analysis memo.

The dedup-loss regression pins the correctness fix that removed the
blueprint "dedup view": a blueprint that parameterizes ``pipeline:`` per
instance produces one synthetic flowgroup per instance in DIFFERENT
pipelines — collapsing them to one representative per ``(blueprint_name,
spec_index)`` silently dropped every other instance's pipeline from the
graph, the JSON, and the job orchestration YAML.

The memo tests pin the single-analysis guarantee: ``analyze_project`` runs
one discovery + build + analyze per ``(pipeline_filter, blueprint_filter)``
pair per service instance, shared by the ``dag`` command's analyze and
save paths and by the job-orchestration global pass.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest

from lhp.core.coordination.validation_service import ValidationService
from lhp.core.dependencies.service import DependencyAnalysisService
from lhp.core.processing.blueprint_expander import BlueprintProvenance
from lhp.models import Action, ActionType, FlowGroup, ProjectConfig


def _make_service(project_root: Path) -> DependencyAnalysisService:
    project_config = ProjectConfig(name="test", version="1.0")
    validation_service = ValidationService(project_root, project_config)
    return DependencyAnalysisService(project_root, project_config, validation_service)


def _fg(pipeline: str, name: str) -> FlowGroup:
    return FlowGroup(
        pipeline=pipeline,
        flowgroup=name,
        actions=[
            Action(
                name="prep",
                type=ActionType.TRANSFORM,
                source="raw.src",
                target="v_prep",
            )
        ],
    )


def _provenance(
    fg: FlowGroup, blueprint_name: str, spec_index: int
) -> BlueprintProvenance:
    return BlueprintProvenance(
        blueprint_name=blueprint_name,
        blueprint_path=Path(f"blueprints/{blueprint_name}.yaml"),
        instance_path=Path(f"instances/{fg.pipeline}.yaml"),
        flowgroup=fg,
        spec_index=spec_index,
    )


def _seed_synthetics(service: DependencyAnalysisService) -> list[FlowGroup]:
    """Simulate a blueprint whose ``pipeline:`` varies per instance.

    One spec (index 0) expanded for two instances -> two synthetics in two
    DIFFERENT pipelines, plus one on-disk flowgroup.
    """
    on_disk = _fg("static_pipeline", "static_fg")
    site_a = _fg("edw_site_a", "orders_site_a")
    site_b = _fg("edw_site_b", "orders_site_b")
    service._flowgroups = [on_disk, site_a, site_b]
    service._blueprint_provenance = {
        (site_a.pipeline, site_a.flowgroup): _provenance(site_a, "per_site", 0),
        (site_b.pipeline, site_b.flowgroup): _provenance(site_b, "per_site", 0),
    }
    return [on_disk, site_a, site_b]


@pytest.mark.unit
class TestFullExpansionIsTheOnlyView:
    def test_pipeline_varying_instances_all_survive(self, tmp_path):
        """REGRESSION: two instances of one spec in different pipelines must
        BOTH appear — the old dedup view kept only the first."""
        service = _make_service(tmp_path)
        _seed_synthetics(service)

        flowgroups = service.get_flowgroups()

        assert {(fg.pipeline, fg.flowgroup) for fg in flowgroups} == {
            ("static_pipeline", "static_fg"),
            ("edw_site_a", "orders_site_a"),
            ("edw_site_b", "orders_site_b"),
        }

    def test_blueprint_filter_keeps_only_that_blueprints_synthetics(self, tmp_path):
        service = _make_service(tmp_path)
        _seed_synthetics(service)

        flowgroups = service.get_flowgroups(blueprint_filter="per_site")

        assert {fg.pipeline for fg in flowgroups} == {"edw_site_a", "edw_site_b"}

    def test_pipeline_filter_composes_with_full_expansion(self, tmp_path):
        service = _make_service(tmp_path)
        _seed_synthetics(service)

        flowgroups = service.get_flowgroups(pipeline_filter="edw_site_b")

        assert [fg.flowgroup for fg in flowgroups] == ["orders_site_b"]


@pytest.mark.unit
class TestAnalyzeProjectMemo:
    def test_one_analysis_per_filter_pair(self, tmp_path):
        service = _make_service(tmp_path)
        _seed_synthetics(service)

        with patch.object(
            service._analyzer, "analyze", wraps=service._analyzer.analyze
        ) as spy:
            first = service.analyze_project()
            second = service.analyze_project()
            assert spy.call_count == 1
            assert first is second

            service.analyze_project(blueprint_filter="per_site")
            assert spy.call_count == 2

    def test_job_orchestration_reuses_the_global_memo(self, tmp_path):
        service = _make_service(tmp_path)
        _seed_synthetics(service)

        with patch.object(
            service._analyzer, "analyze", wraps=service._analyzer.analyze
        ) as spy:
            global_result = service.analyze_project()
            job_results, by_job_global = service.analyze_dependencies_by_job()

        assert spy.call_count == 1
        assert by_job_global is global_result
        assert set(job_results) == {"test_orchestration"}
