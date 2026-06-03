"""Write target validation tests for ConfigValidator."""

import pytest

from lhp.core.validators import ConfigValidator
from lhp.models import Action, ActionType


class TestConfigValidatorWriteTargets:
    def test_streaming_table_source_validation_edge_cases(self):
        validator = ConfigValidator()

        action = Action(
            name="test_streaming_no_source",
            type=ActionType.WRITE,
            # Missing source - should fail for standard mode
            write_target={
                "type": "streaming_table",
                "catalog": "test_cat",
                "schema": "test_sch",
                "table": "test",
                "mode": "standard",  # Standard mode requires source
            },
        )
        errors = validator.validate_action(action, 0)
        assert any(
            "Streaming table must have 'source' to read from" in error
            for error in errors
        )

        action = Action(
            name="test_streaming_invalid_source",
            type=ActionType.WRITE,
            source={"invalid": "dict_source"},  # Should be string or list
            write_target={
                "type": "streaming_table",
                "catalog": "test_cat",
                "schema": "test_sch",
                "table": "test",
                "mode": "standard",
            },
        )
        errors = validator.validate_action(action, 0)
        assert any(
            "source must be a string or list of view names" in error for error in errors
        )

        action = Action(
            name="test_snapshot_cdc_no_source",
            type=ActionType.WRITE,
            # Missing source - OK for snapshot_cdc mode
            write_target={
                "type": "streaming_table",
                "catalog": "test_cat",
                "schema": "test_sch",
                "table": "test",
                "mode": "snapshot_cdc",
                "snapshot_cdc_config": {
                    "source": "raw.customer_snapshots",
                    "keys": ["customer_id"],
                },
            },
        )
        errors = validator.validate_action(action, 0)
        source_errors = [
            e for e in errors if "source" in e and "Streaming table must have" in e
        ]
        assert len(source_errors) == 0

        action = Action(
            name="test_streaming_valid_string_source",
            type=ActionType.WRITE,
            source="v_input_data",  # Valid string source
            write_target={
                "type": "streaming_table",
                "catalog": "test_cat",
                "schema": "test_sch",
                "table": "test",
                "mode": "standard",
            },
        )
        errors = validator.validate_action(action, 0)
        source_errors = [
            e for e in errors if "source" in e and "must be a string or list" in e
        ]
        assert len(source_errors) == 0

        action = Action(
            name="test_streaming_valid_list_source",
            type=ActionType.WRITE,
            source=["v_input1", "v_input2"],  # Valid list source
            write_target={
                "type": "streaming_table",
                "catalog": "test_cat",
                "schema": "test_sch",
                "table": "test",
                "mode": "standard",
            },
        )
        errors = validator.validate_action(action, 0)
        source_errors = [
            e for e in errors if "source" in e and "must be a string or list" in e
        ]
        assert len(source_errors) == 0

    def test_materialized_view_source_validation_edge_cases(self):
        validator = ConfigValidator()

        action = Action(
            name="test_mv_no_source_no_sql",
            type=ActionType.WRITE,
            # Missing both source and SQL
            write_target={
                "type": "materialized_view",
                "catalog": "test_cat",
                "schema": "test_sch",
                "table": "test",
                # Missing both source and sql
            },
        )
        errors = validator.validate_action(action, 0)
        assert any(
            "must have either 'source', 'sql', or 'sql_path' in write_target" in error
            for error in errors
        )

        action = Action(
            name="test_mv_invalid_source",
            type=ActionType.WRITE,
            source={"invalid": "dict_source"},  # Should be string or list, not dict
            write_target={
                "type": "materialized_view",
                "catalog": "test_cat",
                "schema": "test_sch",
                "table": "test",
            },
        )
        errors = validator.validate_action(action, 0)
        assert any(
            "source must be a string or list of view names" in error for error in errors
        )

        action = Action(
            name="test_mv_valid_string_source",
            type=ActionType.WRITE,
            source="v_input_data",  # Valid string source
            write_target={
                "type": "materialized_view",
                "catalog": "test_cat",
                "schema": "test_sch",
                "table": "test",
            },
        )
        errors = validator.validate_action(action, 0)
        source_errors = [e for e in errors if "source must be a string or list" in e]
        assert len(source_errors) == 0

        action = Action(
            name="test_mv_valid_list_source",
            type=ActionType.WRITE,
            source=["v_input1", "v_input2"],  # Valid list source
            write_target={
                "type": "materialized_view",
                "catalog": "test_cat",
                "schema": "test_sch",
                "table": "test",
            },
        )
        errors = validator.validate_action(action, 0)
        source_errors = [e for e in errors if "source must be a string or list" in e]
        assert len(source_errors) == 0

        action = Action(
            name="test_mv_sql_no_source",
            type=ActionType.WRITE,
            # No source field - OK because SQL is provided
            write_target={
                "type": "materialized_view",
                "catalog": "test_cat",
                "schema": "test_sch",
                "table": "test",
                "sql": "SELECT COUNT(*) FROM silver.details",
            },
        )
        errors = validator.validate_action(action, 0)
        source_errors = [
            e for e in errors if "must have either 'source', 'sql', or 'sql_path'" in e
        ]
        assert len(source_errors) == 0

    def test_materialized_view_sql_path_passes(self):
        validator = ConfigValidator()

        action = Action(
            name="test_mv_sql_path",
            type=ActionType.WRITE,
            write_target={
                "type": "materialized_view",
                "catalog": "gold_cat",
                "schema": "gold_sch",
                "table": "ecomm_summary",
                "sql_path": "sql/gold/ecomm_summary.sql",
            },
        )
        errors = validator.validate_action(action, 0)
        source_errors = [
            e for e in errors if "must have either 'source', 'sql', or 'sql_path'" in e
        ]
        assert len(source_errors) == 0

    def test_materialized_view_no_source_sql_or_sql_path_fails(self):
        validator = ConfigValidator()

        action = Action(
            name="test_mv_nothing",
            type=ActionType.WRITE,
            write_target={
                "type": "materialized_view",
                "catalog": "gold_cat",
                "schema": "gold_sch",
                "table": "bad_table",
            },
        )
        errors = validator.validate_action(action, 0)
        assert any(
            "must have either 'source', 'sql', or 'sql_path'" in e for e in errors
        )


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
