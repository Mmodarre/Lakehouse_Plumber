"""Tests that ConfigValidator validates test actions when present in the flowgroup."""

import shutil
import tempfile
from pathlib import Path
from unittest.mock import MagicMock

from lhp.core.validators import ConfigValidator
from lhp.models import Action, ActionType, FlowGroup


class TestValidationIncludeTests:
    """Test validation behavior with include_tests flag scenarios."""

    def setup_method(self):
        self.test_dir = Path(tempfile.mkdtemp())
        (self.test_dir / "substitutions").mkdir()
        self.validator = ConfigValidator(self.test_dir)

    def teardown_method(self):
        if self.test_dir.exists():
            shutil.rmtree(self.test_dir)

    def test_config_validator_processes_test_actions_when_present(self):
        """Test that ConfigValidator validates test actions when present in the flowgroup."""
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
                    name="invalid_test",
                    type=ActionType.TEST,
                    test_type="invalid_type",  # Invalid test type
                    source="v_data",
                ),
                Action(
                    name="write_data",
                    type=ActionType.WRITE,
                    source="v_data",
                    write_target={
                        "type": "streaming_table",
                        "database": "test.bronze",
                        "table": "test",
                    },
                ),
            ],
        )

        errors = self.validator.validate_flowgroup(flowgroup)

        assert len(errors) > 0
        assert any("invalid_type" in error for error in errors)

    def test_config_validator_validates_test_only_flowgroup(self):
        """Test that ConfigValidator validates test-only flowgroups."""
        flowgroup = FlowGroup(
            pipeline="test_only_pipeline",
            flowgroup="test_only_flowgroup",
            actions=[
                Action(
                    name="test_missing_columns",
                    type=ActionType.TEST,
                    test_type="uniqueness",
                    source="some_table",
                    # Missing required 'columns' field
                ),
                Action(
                    name="test_missing_source",
                    type=ActionType.TEST,
                    test_type="completeness",
                    # Missing required 'source' and 'required_columns' fields
                ),
            ],
        )

        errors = self.validator.validate_flowgroup(flowgroup)

        assert len(errors) > 0
        assert any("columns" in error for error in errors)
        assert any("source" in error or "required_columns" in error for error in errors)

    def test_validation_includes_valid_test_actions(self):
        """Test that validation accepts valid test actions."""
        flowgroup = FlowGroup(
            pipeline="valid_test_pipeline",
            flowgroup="valid_test_flowgroup",
            actions=[
                Action(
                    name="valid_uniqueness_test",
                    type=ActionType.TEST,
                    test_type="uniqueness",
                    source="customers",
                    columns=["customer_id"],
                    on_violation="fail",
                ),
                Action(
                    name="valid_row_count_test",
                    type=ActionType.TEST,
                    test_type="row_count",
                    source=["source_table", "target_table"],
                    tolerance=0,
                    on_violation="warn",
                ),
                Action(
                    name="valid_completeness_test",
                    type=ActionType.TEST,
                    test_type="completeness",
                    source="orders",
                    required_columns=["order_id", "customer_id"],
                    on_violation="fail",
                ),
            ],
        )

        errors = self.validator.validate_flowgroup(flowgroup)

        assert len(errors) == 0, f"Unexpected validation errors: {errors}"

    def test_validation_handles_test_with_filter(self):
        """Test that validation handles uniqueness tests with filter field."""
        flowgroup = FlowGroup(
            pipeline="filter_test_pipeline",
            flowgroup="filter_test_flowgroup",
            actions=[
                Action(
                    name="test_active_unique",
                    type=ActionType.TEST,
                    test_type="uniqueness",
                    source="customer_dim",
                    columns=["customer_id"],
                    filter="__END_AT IS NULL",  # Type 2 SCD filter
                    on_violation="fail",
                )
            ],
        )

        errors = self.validator.validate_flowgroup(flowgroup)

        assert len(errors) == 0, f"Filter field should be valid: {errors}"


class TestValidatePassesPreDiscoveredFlowgroups:
    """Test that ``ValidationFacade._consume_validate_stream`` forwards
    ``pre_discovered_all_flowgroups`` verbatim onto the orchestrator.
    """

    @staticmethod
    def _facade_with_mock_orchestrator():
        from lhp.api.facade import ValidationFacade

        mock_orchestrator = MagicMock()
        # ``orchestrator.validate_pipelines`` is now an outcome GENERATOR that
        # the facade drains; an empty iterable suffices for the forwarding
        # assertion (no per-pipeline outcomes needed).
        mock_orchestrator.validate_pipelines.return_value = iter(())
        return ValidationFacade(mock_orchestrator), mock_orchestrator

    def test_validate_pipelines_forwards_pre_discovered(self):
        """``_consume_validate_stream`` forwards ``pre_discovered_all_flowgroups``
        verbatim onto ``orchestrator.validate_pipelines`` (drain the generator
        to force the forward).
        """
        facade, mock_orchestrator = self._facade_with_mock_orchestrator()

        all_flowgroups = [
            FlowGroup(pipeline="p1", flowgroup="fg1"),
            FlowGroup(pipeline="p2", flowgroup="fg2"),
        ]

        list(
            facade._consume_validate_stream(
                pipeline_fields=["p1", "p2"],
                env="dev",
                include_tests=True,
                pre_discovered_all_flowgroups=all_flowgroups,
            )
        )

        assert mock_orchestrator.validate_pipelines.call_count == 1
        call_kwargs = mock_orchestrator.validate_pipelines.call_args.kwargs
        assert call_kwargs["pre_discovered_all_flowgroups"] is all_flowgroups
        assert list(call_kwargs["pipeline_fields"]) == ["p1", "p2"]

    def test_validate_pipelines_default_none_without_pre_discovered(self):
        """Without pre_discovered_all_flowgroups, the forwarded value is None."""
        facade, mock_orchestrator = self._facade_with_mock_orchestrator()

        list(
            facade._consume_validate_stream(
                pipeline_fields=["p1"],
                env="dev",
                include_tests=True,
            )
        )

        call_kwargs = mock_orchestrator.validate_pipelines.call_args.kwargs
        assert call_kwargs["pre_discovered_all_flowgroups"] is None
