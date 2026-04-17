"""Tests for FlowgroupProcessor include_tests filtering behavior.

Unit tests verify that include_tests=False filters test actions from flowgroups
before expensive processing (presets, substitution, validation). Integration tests
verify the parameter threads correctly through orchestrator.validate_pipeline_by_field.
"""

import pytest
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

from lhp.core.services.flowgroup_processor import FlowgroupProcessor
from lhp.core.template_engine import TemplateEngine
from lhp.presets.preset_manager import PresetManager
from lhp.core.validator import ConfigValidator
from lhp.core.secret_validator import SecretValidator
from lhp.models.config import FlowGroup, Action, ActionType
from lhp.utils.substitution import EnhancedSubstitutionManager
from lhp.utils.error_formatter import LHPValidationError


# ============================================================================
# Fixtures
# ============================================================================


@pytest.fixture
def processor():
    """Create a FlowgroupProcessor with real dependencies."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield FlowgroupProcessor(
            template_engine=TemplateEngine(),
            preset_manager=PresetManager(presets_dir=Path(tmpdir)),
            config_validator=ConfigValidator(),
            secret_validator=SecretValidator(),
        )


@pytest.fixture
def substitution_mgr():
    """Create a substitution manager with no mappings."""
    return EnhancedSubstitutionManager()


@pytest.fixture
def mixed_flowgroup():
    """Flowgroup with LOAD + TEST actions."""
    return FlowGroup(
        pipeline="test_pipeline",
        flowgroup="mixed_fg",
        actions=[
            Action(
                name="load_data",
                type=ActionType.LOAD,
                source={"type": "sql", "sql": "SELECT 1 as id"},
                target="v_data",
            ),
            Action(
                name="test_uniqueness",
                type=ActionType.TEST,
                test_type="uniqueness",
                source="v_data",
                columns=["id"],
                on_violation="fail",
            ),
            Action(
                name="write_data",
                type=ActionType.WRITE,
                source="v_data",
                write_target={
                    "type": "streaming_table",
                    "database": "catalog.schema",
                    "table": "target",
                },
            ),
        ],
    )


@pytest.fixture
def test_only_flowgroup():
    """Flowgroup with only TEST actions."""
    return FlowGroup(
        pipeline="test_pipeline",
        flowgroup="test_only_fg",
        actions=[
            Action(
                name="test_uniqueness",
                type=ActionType.TEST,
                test_type="uniqueness",
                source="some_table",
                columns=["id"],
                on_violation="fail",
            ),
            Action(
                name="test_completeness",
                type=ActionType.TEST,
                test_type="completeness",
                source="some_table",
                required_columns=["id", "name"],
                on_violation="warn",
            ),
        ],
    )


# ============================================================================
# Unit Tests — FlowgroupProcessor.process_flowgroup
# ============================================================================


@pytest.mark.unit
class TestProcessFlowgroupIncludeTests:
    """Test include_tests filtering in FlowgroupProcessor.process_flowgroup."""

    def test_filters_test_actions_when_false(
        self, processor, substitution_mgr, mixed_flowgroup
    ):
        """include_tests=False removes TEST actions, keeps LOAD/WRITE."""
        result = processor.process_flowgroup(
            mixed_flowgroup, substitution_mgr, include_tests=False
        )

        action_types = [a.type for a in result.actions]
        assert ActionType.TEST not in action_types
        assert ActionType.LOAD in action_types
        assert ActionType.WRITE in action_types
        assert len(result.actions) == 2

    def test_keeps_test_actions_when_true(
        self, processor, substitution_mgr, mixed_flowgroup
    ):
        """include_tests=True preserves all actions including TEST."""
        result = processor.process_flowgroup(
            mixed_flowgroup, substitution_mgr, include_tests=True
        )

        action_types = [a.type for a in result.actions]
        assert ActionType.TEST in action_types
        assert len(result.actions) == 3

    def test_default_keeps_test_actions(
        self, processor, substitution_mgr, mixed_flowgroup
    ):
        """Default (no include_tests arg) preserves TEST actions for backward compat."""
        result = processor.process_flowgroup(mixed_flowgroup, substitution_mgr)

        action_types = [a.type for a in result.actions]
        assert ActionType.TEST in action_types
        assert len(result.actions) == 3

    def test_test_only_skips_validation_when_false(
        self, processor, substitution_mgr, test_only_flowgroup
    ):
        """Test-only flowgroup with include_tests=False returns without LHPValidationError.

        ConfigValidator rejects zero-action flowgroups, but when the zero-actions
        state comes from include_tests=False filtering, validation is skipped.
        """
        # This should NOT raise — zero actions is expected when filtering
        result = processor.process_flowgroup(
            test_only_flowgroup, substitution_mgr, include_tests=False
        )

        assert len(result.actions) == 0

    def test_test_only_validates_when_true(
        self, processor, substitution_mgr, test_only_flowgroup
    ):
        """Test-only flowgroup with include_tests=True is validated normally."""
        # Should succeed since the test actions are valid
        result = processor.process_flowgroup(
            test_only_flowgroup, substitution_mgr, include_tests=True
        )

        assert len(result.actions) == 2
        assert all(a.type == ActionType.TEST for a in result.actions)

    def test_filters_template_generated_test_actions(self, substitution_mgr):
        """include_tests=False filters test actions generated by templates.

        Validates the filter placement rationale: filter is after template
        expansion so template-generated test actions are also caught.
        """
        # Create a mock template engine that generates test actions
        mock_template_engine = MagicMock()
        mock_template = MagicMock()
        mock_template.presets = None
        mock_template_engine.get_template.return_value = mock_template
        mock_template_engine.render_template.return_value = [
            Action(
                name="template_test",
                type=ActionType.TEST,
                test_type="row_count",
                source=["table_a", "table_b"],
                tolerance=0,
                on_violation="fail",
            ),
            Action(
                name="template_load",
                type=ActionType.LOAD,
                source={"type": "sql", "sql": "SELECT 1"},
                target="v_template",
            ),
        ]

        with tempfile.TemporaryDirectory() as tmpdir:
            processor = FlowgroupProcessor(
                template_engine=mock_template_engine,
                preset_manager=PresetManager(presets_dir=Path(tmpdir)),
                config_validator=ConfigValidator(),
                secret_validator=SecretValidator(),
            )

            flowgroup = FlowGroup(
                pipeline="test_pipeline",
                flowgroup="template_fg",
                use_template="test_template",
                actions=[
                    Action(
                        name="inline_write",
                        type=ActionType.WRITE,
                        source="v_template",
                        write_target={
                            "type": "streaming_table",
                            "database": "catalog.schema",
                            "table": "target",
                        },
                    ),
                ],
            )

            result = processor.process_flowgroup(
                flowgroup, substitution_mgr, include_tests=False
            )

            # Template-generated test action should be filtered out
            action_types = [a.type for a in result.actions]
            assert ActionType.TEST not in action_types
            # The inline write and template load should remain
            assert len(result.actions) == 2
            action_names = {a.name for a in result.actions}
            assert "template_load" in action_names
            assert "inline_write" in action_names


# ============================================================================
# Integration Tests — Parameter threading through orchestrator
# ============================================================================


@pytest.mark.integration
class TestValidatePipelineIncludeTests:
    """Test include_tests threading through validate_pipeline_by_field."""

    @staticmethod
    def _create_project_with_invalid_test(tmp_path):
        """Create a minimal project with a valid load/write and an invalid test action."""
        (tmp_path / "lhp.yaml").write_text("project_name: test\n")
        (tmp_path / "substitutions").mkdir()
        (tmp_path / "substitutions" / "dev.yaml").write_text("{}")
        pipelines_dir = tmp_path / "pipelines" / "test_pipeline"
        pipelines_dir.mkdir(parents=True)

        # Write a flowgroup with a valid load/write plus a test action missing
        # required fields (uniqueness requires 'columns')
        (pipelines_dir / "test_fg.yaml").write_text(
            """pipeline: test_pipeline
flowgroup: test_fg
actions:
  - name: load_data
    type: load
    source:
      type: sql
      sql: "SELECT 1 as id"
    target: v_data
  - name: write_data
    type: write
    source: v_data
    write_target:
      type: streaming_table
      database: catalog.schema
      table: target
  - name: bad_test
    type: test
    test_type: uniqueness
    source: v_data
"""
        )

    def test_validate_skips_test_actions_when_false(self, tmp_path):
        """validate_pipeline_by_field(include_tests=False) skips test action errors."""
        from lhp.core.orchestrator import ActionOrchestrator

        self._create_project_with_invalid_test(tmp_path)
        orchestrator = ActionOrchestrator(tmp_path)

        errors, _ = orchestrator.validate_pipeline_by_field(
            "test_pipeline", "dev", include_tests=False
        )
        assert len(errors) == 0, (
            f"Expected no errors with include_tests=False, got: {errors}"
        )

    def test_validate_catches_test_actions_when_true(self, tmp_path):
        """validate_pipeline_by_field(include_tests=True) catches test action errors.

        Uses a separate orchestrator to avoid shared-state issues with
        discover_all_flowgroups caching.
        """
        from lhp.core.orchestrator import ActionOrchestrator

        self._create_project_with_invalid_test(tmp_path)
        orchestrator = ActionOrchestrator(tmp_path)

        errors, _ = orchestrator.validate_pipeline_by_field(
            "test_pipeline", "dev", include_tests=True
        )
        assert len(errors) > 0, (
            "Expected validation errors with include_tests=True for missing columns"
        )

    def test_validate_passes_include_tests_through_chain(self):
        """Verify include_tests is forwarded from orchestrator to processor."""
        from lhp.core.orchestrator import ActionOrchestrator

        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            (tmp_path / "lhp.yaml").write_text("project_name: test\n")
            (tmp_path / "substitutions").mkdir()
            (tmp_path / "substitutions" / "dev.yaml").write_text("{}")
            pipelines_dir = tmp_path / "pipelines" / "test_pipeline"
            pipelines_dir.mkdir(parents=True)
            (pipelines_dir / "simple.yaml").write_text(
                """pipeline: test_pipeline
flowgroup: simple_fg
actions:
  - name: load_data
    type: load
    source:
      type: sql
      sql: "SELECT 1 as id"
    target: v_data
"""
            )

            orchestrator = ActionOrchestrator(tmp_path)

            # Patch the processor to verify include_tests is passed
            with patch.object(
                orchestrator.processor, "process_flowgroup", wraps=orchestrator.processor.process_flowgroup
            ) as mock_process:
                orchestrator.validate_pipeline_by_field(
                    "test_pipeline", "dev", include_tests=False
                )

                # Verify process_flowgroup was called with include_tests=False
                assert mock_process.called
                call_kwargs = mock_process.call_args
                assert call_kwargs.kwargs.get("include_tests") is False


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
