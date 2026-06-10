"""Tests for ActionOrchestrator include_tests parameter."""

import shutil
import tempfile
from pathlib import Path

import pytest

from lhp.core.coordination.layers import build_facade_orchestrator
from lhp.models import Action, ActionType, FlowGroup


class TestOrchestratorIncludeTests:
    """Test ActionOrchestrator include_tests functionality."""

    def setup_method(self):
        self.test_dir = Path(tempfile.mkdtemp())

        (self.test_dir / "substitutions").mkdir()
        (self.test_dir / "presets").mkdir()
        (self.test_dir / "templates").mkdir()

        substitution_content = """
environment: test
catalog: test_catalog
bronze_schema: bronze
"""
        (self.test_dir / "substitutions" / "test.yaml").write_text(substitution_content)

        lhp_config = """
project:
  name: test_project
  version: "1.0"
"""
        (self.test_dir / "lhp.yaml").write_text(lhp_config)

        self.orchestrator = build_facade_orchestrator(self.test_dir)

    def teardown_method(self):
        if self.test_dir.exists():
            shutil.rmtree(self.test_dir)

    def test_generate_pipelines_accepts_include_tests_parameter(self):
        """Test that generate_pipelines method accepts include_tests parameter.

        ``include_tests`` is a keyword-only parameter on the consolidated
        ``ActionOrchestrator.generate_pipelines`` (scoped via ``pipeline_filter``
        / ``pipeline_fields``).
        """
        import inspect

        signature = inspect.signature(self.orchestrator.generate_pipelines)
        params = list(signature.parameters.keys())

        assert "include_tests" in params, (
            f"include_tests parameter not found in method signature. Available parameters: {params}"
        )

        include_tests_param = signature.parameters["include_tests"]
        assert include_tests_param.default is False, (
            "include_tests parameter should default to False"
        )

    def test_generate_flowgroup_code_skips_tests_when_false(self):
        """Test that _generate_flowgroup_code skips TEST actions when include_tests=False."""
        from lhp.core.processing.substitution import EnhancedSubstitutionManager

        flowgroup = FlowGroup(
            pipeline="test_pipeline",
            flowgroup="test_flowgroup",
            actions=[
                Action(
                    name="load_data",
                    type=ActionType.LOAD,
                    source={"type": "sql", "sql": "SELECT 1 as id"},
                    target="v_data",
                ),
                Action(
                    name="test_data",
                    type=ActionType.TEST,
                    test_type="uniqueness",
                    source="v_data",
                    columns=["id"],
                ),
            ],
        )

        substitution_mgr = EnhancedSubstitutionManager(
            self.test_dir / "substitutions" / "test.yaml", "test"
        )

        result = self.orchestrator.codegen.generate(
            flowgroup, substitution_mgr, include_tests=False
        )

        assert "DATA QUALITY TESTS" not in result
        assert "@dp.table(" not in result or "tmp_test_" not in result

    def test_generate_flowgroup_code_includes_tests_when_true(self):
        """Test that _generate_flowgroup_code includes TEST actions when include_tests=True."""
        from lhp.core.processing.substitution import EnhancedSubstitutionManager

        flowgroup = FlowGroup(
            pipeline="test_pipeline",
            flowgroup="test_flowgroup",
            actions=[
                Action(
                    name="load_data",
                    type=ActionType.LOAD,
                    source={"type": "sql", "sql": "SELECT 1 as id"},
                    target="v_data",
                ),
                Action(
                    name="test_data",
                    type=ActionType.TEST,
                    test_type="uniqueness",
                    source="v_data",
                    columns=["id"],
                ),
            ],
        )

        substitution_mgr = EnhancedSubstitutionManager(
            self.test_dir / "substitutions" / "test.yaml", "test"
        )

        result = self.orchestrator.codegen.generate(
            flowgroup, substitution_mgr, include_tests=True
        )

        assert "DATA QUALITY TESTS" in result
        assert (
            "@dp.table(" in result and "tmp_test_" in result
        ) or "@dp.expect" in result

    def test_mixed_flowgroup_filtering(self):
        """Test flowgroup with both TEST and non-TEST actions respects include_tests flag."""
        from lhp.core.processing.substitution import EnhancedSubstitutionManager

        flowgroup = FlowGroup(
            pipeline="mixed_pipeline",
            flowgroup="mixed_flowgroup",
            actions=[
                Action(
                    name="load_data",
                    type=ActionType.LOAD,
                    source={"type": "sql", "sql": "SELECT 1 as id"},
                    target="v_data",
                ),
                Action(
                    name="transform_data",
                    type=ActionType.TRANSFORM,
                    transform_type="sql",
                    source="v_data",
                    target="v_clean_data",
                    sql="SELECT * FROM v_data",
                ),
                Action(
                    name="test_data",
                    type=ActionType.TEST,
                    test_type="uniqueness",
                    source="v_clean_data",
                    columns=["id"],
                ),
                Action(
                    name="write_data",
                    type=ActionType.WRITE,
                    source="v_clean_data",
                    write_target={
                        "type": "streaming_table",
                        "database": "test.bronze",
                        "table": "test",
                    },
                ),
            ],
        )

        substitution_mgr = EnhancedSubstitutionManager(
            self.test_dir / "substitutions" / "test.yaml", "test"
        )

        result_without = self.orchestrator.codegen.generate(
            flowgroup, substitution_mgr, include_tests=False
        )
        assert "SOURCE VIEWS" in result_without
        assert "TRANSFORMATION VIEWS" in result_without
        assert "TARGET TABLES" in result_without
        assert "DATA QUALITY TESTS" not in result_without

        result_with = self.orchestrator.codegen.generate(
            flowgroup, substitution_mgr, include_tests=True
        )
        assert "SOURCE VIEWS" in result_with
        assert "TRANSFORMATION VIEWS" in result_with
        assert "TARGET TABLES" in result_with
        assert "DATA QUALITY TESTS" in result_with

    def test_test_only_flowgroup_behavior(self):
        """Test that test-only flowgroups are skipped entirely when include_tests=False."""
        from lhp.core.processing.substitution import EnhancedSubstitutionManager

        flowgroup = FlowGroup(
            pipeline="test_only_pipeline",
            flowgroup="test_only_flowgroup",
            actions=[
                Action(
                    name="test_uniqueness",
                    type=ActionType.TEST,
                    test_type="uniqueness",
                    source="some_table",
                    columns=["id"],
                ),
                Action(
                    name="test_completeness",
                    type=ActionType.TEST,
                    test_type="completeness",
                    source="some_table",
                    required_columns=["id", "name"],
                ),
            ],
        )

        substitution_mgr = EnhancedSubstitutionManager(
            self.test_dir / "substitutions" / "test.yaml", "test"
        )

        result_without = self.orchestrator.codegen.generate(
            flowgroup, substitution_mgr, include_tests=False
        )
        assert result_without == "", "Test-only flowgroup should be skipped entirely"

        result_with = self.orchestrator.codegen.generate(
            flowgroup, substitution_mgr, include_tests=True
        )
        assert result_with != "", (
            "Test-only flowgroup should generate content when flag is set"
        )
        assert "DATA QUALITY TESTS" in result_with
        assert "@dp.table(" in result_with


class TestEmptyContentCleanup:
    """Test empty content cleanup functionality."""

    def setup_method(self):
        self.test_dir = Path(tempfile.mkdtemp())

        (self.test_dir / "substitutions").mkdir()
        (self.test_dir / "pipelines" / "test_pipeline").mkdir(parents=True)

        substitution_content = """
environment: test
catalog: test_catalog
bronze_schema: bronze
"""
        (self.test_dir / "substitutions" / "test.yaml").write_text(substitution_content)

        lhp_config = """
project:
  name: test_project
  version: "1.0"
"""
        (self.test_dir / "lhp.yaml").write_text(lhp_config)

        test_yaml = """
pipeline: test_pipeline
flowgroup: test_only_flowgroup
actions:
  - name: test_action
    type: test
    test_type: uniqueness
    source: test_table
    columns: [id]
"""
        (self.test_dir / "pipelines" / "test_pipeline" / "test_only.yaml").write_text(
            test_yaml
        )

        self.orchestrator = build_facade_orchestrator(self.test_dir)

    def teardown_method(self):
        if self.test_dir.exists():
            shutil.rmtree(self.test_dir)
