"""Tests that validation always processes test actions regardless of generation flag."""

import pytest
from pathlib import Path
import tempfile
import shutil
from lhp.core.validator import ConfigValidator
from lhp.models.config import FlowGroup, Action, ActionType


class TestValidationIncludeTests:
    """Test validation behavior with include_tests flag scenarios."""
    
    def setup_method(self):
        """Set up test environment."""
        # Create temporary directory for test project
        self.test_dir = Path(tempfile.mkdtemp())
        
        # Create basic project structure
        (self.test_dir / "substitutions").mkdir()
        
        # Initialize validator
        self.validator = ConfigValidator(self.test_dir)
    
    def teardown_method(self):
        """Clean up test environment."""
        if self.test_dir.exists():
            shutil.rmtree(self.test_dir)
    
    def test_validation_always_processes_test_actions(self):
        """Test that validation always validates test actions regardless of generation behavior."""
        # Create flowgroup with test action that has a validation error
        flowgroup = FlowGroup(
            pipeline="test_pipeline",
            flowgroup="test_flowgroup",
            actions=[
                Action(
                    name="load_data",
                    type=ActionType.LOAD,
                    source={"type": "sql", "sql": "SELECT 1 as id"},
                    target="v_data"
                ),
                Action(
                    name="invalid_test",
                    type=ActionType.TEST,
                    test_type="invalid_type",  # Invalid test type
                    source="v_data"
                ),
                Action(
                    name="write_data",
                    type=ActionType.WRITE,
                    source="v_data",
                    write_target={"type": "streaming_table", "database": "test.bronze", "table": "test"}
                )
            ]
        )
        
        # Validation should always catch test action errors
        errors = self.validator.validate_flowgroup(flowgroup)
        
        # Should have validation errors for the invalid test type
        assert len(errors) > 0
        assert any("invalid_type" in error for error in errors)
    
    def test_validation_processes_test_only_flowgroup(self):
        """Test that validation processes test-only flowgroups even if generation would skip them."""
        # Create test-only flowgroup with validation error
        flowgroup = FlowGroup(
            pipeline="test_only_pipeline",
            flowgroup="test_only_flowgroup",
            actions=[
                Action(
                    name="test_missing_columns",
                    type=ActionType.TEST,
                    test_type="uniqueness",
                    source="some_table"
                    # Missing required 'columns' field
                ),
                Action(
                    name="test_missing_source",
                    type=ActionType.TEST,
                    test_type="completeness"
                    # Missing required 'source' and 'required_columns' fields
                )
            ]
        )
        
        # Validation should catch errors in test-only flowgroups
        errors = self.validator.validate_flowgroup(flowgroup)
        
        # Should have validation errors for missing required fields
        assert len(errors) > 0
        assert any("columns" in error for error in errors)  # Missing columns for uniqueness
        assert any("source" in error or "required_columns" in error for error in errors)  # Missing fields for completeness
    
    def test_validation_includes_valid_test_actions(self):
        """Test that validation accepts valid test actions."""
        # Create flowgroup with valid test actions
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
                    on_violation="fail"
                ),
                Action(
                    name="valid_row_count_test",
                    type=ActionType.TEST,
                    test_type="row_count",
                    source=["source_table", "target_table"],
                    tolerance=0,
                    on_violation="warn"
                ),
                Action(
                    name="valid_completeness_test",
                    type=ActionType.TEST,
                    test_type="completeness",
                    source="orders",
                    required_columns=["order_id", "customer_id"],
                    on_violation="fail"
                )
            ]
        )
        
        # Validation should pass for valid test actions
        errors = self.validator.validate_flowgroup(flowgroup)
        
        # Should have no validation errors
        assert len(errors) == 0, f"Unexpected validation errors: {errors}"
    
    def test_validation_handles_test_with_filter(self):
        """Test that validation handles uniqueness tests with filter field."""
        # Create flowgroup with uniqueness test using filter
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
                    on_violation="fail"
                )
            ]
        )
        
        # Validation should accept filter field
        errors = self.validator.validate_flowgroup(flowgroup)
        
        # Should have no validation errors
        assert len(errors) == 0, f"Filter field should be valid: {errors}"
