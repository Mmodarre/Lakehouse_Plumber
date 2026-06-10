"""Behavioural tests for the inspection-facade *orphan* methods.

Per constitution §9.15. The ``show`` / ``info`` / ``stats`` CLI commands were
dropped in the rebuild, leaving ``InspectionFacade.compute_stats``,
``get_project_config``, and ``process_flowgroup`` with no in-tree CLI caller.
They remain PUBLIC API and so must keep behavioural coverage — these tests drive
each method through the public entrypoint
``LakehousePlumberApplicationFacade.for_project(...).inspection`` against a small
real on-disk project (the ``_write_minimal_project`` pattern shared with
``test_build_substitution_view.py``), asserting sensible results rather than
just exercising the DTO in isolation.

``enforce_version=False`` relaxes the ``required_lhp_version`` gate so the
fixture is independent of the installed package version.
"""

from __future__ import annotations

import textwrap
from pathlib import Path

import pytest

from lhp.api import (
    LakehousePlumberApplicationFacade,
    ProcessedFlowgroupView,
    ProjectConfigView,
    StatsResult,
)

pytestmark = pytest.mark.unit

ENV = "dev"
PIPELINE = "bronze"
FLOWGROUP = "fg_customers"


def _write_minimal_project(root: Path) -> None:
    """One pipeline (``bronze``) with one flowgroup of three actions
    (load -> transform -> write). ``operational_metadata`` is configured so the
    ``has_operational_metadata`` projection is exercised as ``True``; event-log /
    monitoring / test-reporting are absent so their flags stay ``False``.
    """
    (root / "lhp.yaml").write_text(
        textwrap.dedent("""\
            name: orphan_behaviour_project
            version: "1.0"
            description: "Orphan behaviour fixture"
            author: "Test Author"
            operational_metadata:
              columns:
                _ingest_ts:
                  expression: "F.current_timestamp()"
                  applies_to: ["streaming_table"]
            """)
    )
    for sub in ("presets", "templates"):
        (root / sub).mkdir(parents=True, exist_ok=True)

    subs = root / "substitutions"
    subs.mkdir(parents=True, exist_ok=True)
    (subs / f"{ENV}.yaml").write_text(
        textwrap.dedent(f"""\
            {ENV}:
              catalog: dev_catalog
              schema: bronze
            """)
    )

    fg_dir = root / "pipelines" / PIPELINE
    fg_dir.mkdir(parents=True, exist_ok=True)
    (fg_dir / "customers.yaml").write_text(
        textwrap.dedent(f"""\
            pipeline: {PIPELINE}
            flowgroup: {FLOWGROUP}
            actions:
              - name: load_customers
                type: load
                source:
                  type: sql
                  sql: "SELECT 1 AS id"
                target: v_customers_raw
              - name: clean_customers
                type: transform
                transform_type: sql
                source: v_customers_raw
                sql: "SELECT * FROM v_customers_raw"
                target: v_customers_clean
              - name: write_customers
                type: write
                source: v_customers_clean
                write_target:
                  type: streaming_table
                  database: "${{catalog}}.${{schema}}"
                  table: customers
            """)
    )


@pytest.fixture
def facade(tmp_path: Path) -> LakehousePlumberApplicationFacade:
    _write_minimal_project(tmp_path)
    return LakehousePlumberApplicationFacade.for_project(
        tmp_path, enforce_version=False
    )


class TestComputeStats:
    def test_returns_stats_result_with_sensible_totals(
        self, facade: LakehousePlumberApplicationFacade
    ) -> None:
        stats = facade.inspection.compute_stats()

        assert isinstance(stats, StatsResult)
        assert stats.pipeline_count == 1
        assert stats.flowgroup_count == 1
        # load + transform + write = 3 actions.
        assert stats.total_actions == 3

    def test_action_counts_break_down_by_type(
        self, facade: LakehousePlumberApplicationFacade
    ) -> None:
        counts = facade.inspection.compute_stats().action_counts_by_type
        # One action of each core type; the converter also tracks finer-grained
        # sub-keys (e.g. ``load_sql``) but the core type keys are the contract.
        assert counts["load"] == 1
        assert counts["transform"] == 1
        assert counts["write"] == 1

    def test_pipeline_breakdown_reports_the_single_pipeline(
        self, facade: LakehousePlumberApplicationFacade
    ) -> None:
        breakdown = facade.inspection.compute_stats().pipeline_breakdown
        assert len(breakdown) == 1
        row = breakdown[0]
        assert row.pipeline_name == PIPELINE
        assert row.flowgroup_count == 1
        assert row.total_actions == 3


class TestGetProjectConfig:
    def test_projects_lhp_yaml_scalar_fields(
        self, facade: LakehousePlumberApplicationFacade
    ) -> None:
        config = facade.inspection.get_project_config()

        assert isinstance(config, ProjectConfigView)
        assert config.name == "orphan_behaviour_project"
        assert config.version == "1.0"
        assert config.description == "Orphan behaviour fixture"
        assert config.author == "Test Author"

    def test_collapses_nested_config_sections_to_boolean_flags(
        self, facade: LakehousePlumberApplicationFacade
    ) -> None:
        config = facade.inspection.get_project_config()
        # operational_metadata is configured -> True; the others are absent.
        assert config.has_operational_metadata is True
        assert config.has_event_log is False
        assert config.has_monitoring is False
        assert config.has_test_reporting is False


class TestProcessFlowgroup:
    def test_resolves_named_flowgroup_to_processed_view(
        self, facade: LakehousePlumberApplicationFacade
    ) -> None:
        view = facade.inspection.process_flowgroup(FLOWGROUP, env=ENV)

        assert isinstance(view, ProcessedFlowgroupView)
        assert view.flowgroup.name == FLOWGROUP
        assert view.flowgroup.pipeline == PIPELINE

    def test_exposes_each_action_with_its_type(
        self, facade: LakehousePlumberApplicationFacade
    ) -> None:
        view = facade.inspection.process_flowgroup(FLOWGROUP, env=ENV)

        assert len(view.actions) == 3
        by_name = {a.name: a for a in view.actions}
        assert by_name["load_customers"].action_type == "load"
        assert by_name["clean_customers"].action_type == "transform"
        assert by_name["write_customers"].action_type == "write"

    def test_unknown_flowgroup_raises_lookup_error(
        self, facade: LakehousePlumberApplicationFacade
    ) -> None:
        with pytest.raises(LookupError):
            facade.inspection.process_flowgroup("not_a_real_flowgroup", env=ENV)
