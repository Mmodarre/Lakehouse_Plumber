"""Unit tests for DependencyOutputWriter dispatch policies."""

from __future__ import annotations

import logging
from unittest.mock import Mock

import networkx as nx
import pytest

from lhp.core.dependencies.output_writer import DependencyOutputWriter
from lhp.models.dependencies import DependencyAnalysisResult, DependencyGraphs


def _empty_result() -> DependencyAnalysisResult:
    return DependencyAnalysisResult(
        graphs=DependencyGraphs(
            action_graph=nx.DiGraph(),
            flowgroup_graph=nx.DiGraph(),
            pipeline_graph=nx.DiGraph(),
            metadata={},
        ),
        pipeline_dependencies={},
        execution_stages=[],
        circular_dependencies=[],
        external_sources=[],
    )


@pytest.mark.unit
class TestJobFormatFilterPolicy:
    """The job format is a whole-project artifact: skipped under filters."""

    def test_job_skipped_when_filters_active(self, tmp_path, caplog):
        writer = DependencyOutputWriter()
        analyzer = Mock()
        with caplog.at_level(logging.WARNING):
            generated = writer.save_outputs(
                analyzer,
                _empty_result(),
                ["job"],
                output_dir=tmp_path,
                filters_active=True,
            )
        assert "job" not in generated
        analyzer.analyze_dependencies_by_job.assert_not_called()
        assert any(
            "Skipping job format" in rec.message and "filter" in rec.message
            for rec in caplog.records
        )

    def test_other_formats_unaffected_by_filters(self, tmp_path):
        writer = DependencyOutputWriter()
        analyzer = Mock()
        generated = writer.save_outputs(
            analyzer,
            _empty_result(),
            ["text"],
            output_dir=tmp_path,
            filters_active=True,
        )
        assert "text" in generated
        assert generated["text"].exists()
