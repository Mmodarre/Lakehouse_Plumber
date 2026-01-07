"""Test ForEachBatch sink action validation."""

import pytest
from lhp.models.config import Action, ActionType
from lhp.core.validators import WriteActionValidator
from lhp.core.action_registry import ActionRegistry
from lhp.core.config_field_validator import ConfigFieldValidator
import logging


class TestForEachBatchSinkValidation:
    """Test ForEachBatch sink write action validation."""

    def setup_method(self):
        """Set up test fixtures."""
        self.logger = logging.getLogger(__name__)
        self.action_registry = ActionRegistry()
        self.field_validator = ConfigFieldValidator()
        self.validator = WriteActionValidator(
            self.action_registry, self.field_validator, self.logger
        )

    def test_valid_foreachbatch_with_module_path(self):
        """Test valid ForEachBatch sink configuration with module_path."""
        action = Action(
            name="test_foreachbatch_sink",
            type=ActionType.WRITE,
            source="v_input_data",
            write_target={
                "type": "sink",
                "sink_type": "foreachbatch",
                "sink_name": "my_batch_sink",
                "module_path": "batch_handlers/my_handler.py"
            }
        )
        
        errors = self.validator.validate(action, "test_foreachbatch_sink")
        
        # Should have no errors
        assert len(errors) == 0

    def test_valid_foreachbatch_with_batch_handler(self):
        """Test valid ForEachBatch sink configuration with inline batch_handler."""
        action = Action(
            name="test_foreachbatch_sink",
            type=ActionType.WRITE,
            source="v_input_data",
            write_target={
                "type": "sink",
                "sink_type": "foreachbatch",
                "sink_name": "my_batch_sink",
                "batch_handler": "df.write.format('delta').mode('append').saveAsTable('target')"
            }
        )
        
        errors = self.validator.validate(action, "test_foreachbatch_sink")
        
        # Should have no errors
        assert len(errors) == 0

    def test_foreachbatch_missing_sink_name(self):
        """Test ForEachBatch sink without sink_name."""
        action = Action(
            name="test_foreachbatch_sink",
            type=ActionType.WRITE,
            source="v_input_data",
            write_target={
                "type": "sink",
                "sink_type": "foreachbatch",
                "module_path": "batch_handlers/my_handler.py"
            }
        )
        
        errors = self.validator.validate(action, "test_foreachbatch_sink")
        
        # Should error about missing sink_name
        assert len(errors) > 0
        assert any("sink_name" in err.lower() for err in errors)

    def test_foreachbatch_missing_source(self):
        """Test ForEachBatch sink without source."""
        action = Action(
            name="test_foreachbatch_sink",
            type=ActionType.WRITE,
            write_target={
                "type": "sink",
                "sink_type": "foreachbatch",
                "sink_name": "my_batch_sink",
                "module_path": "batch_handlers/my_handler.py"
            }
        )
        
        errors = self.validator.validate(action, "test_foreachbatch_sink")
        
        # Should error about missing source
        assert len(errors) > 0
        assert any("source" in err.lower() for err in errors)

    def test_foreachbatch_both_module_and_handler(self):
        """Test ForEachBatch sink with both module_path AND batch_handler."""
        action = Action(
            name="test_foreachbatch_sink",
            type=ActionType.WRITE,
            source="v_input_data",
            write_target={
                "type": "sink",
                "sink_type": "foreachbatch",
                "sink_name": "my_batch_sink",
                "module_path": "batch_handlers/my_handler.py",
                "batch_handler": "df.write.format('delta').saveAsTable('target')"
            }
        )
        
        errors = self.validator.validate(action, "test_foreachbatch_sink")
        
        # Should error about having both
        assert len(errors) > 0
        assert any(("one" in err.lower() or "both" in err.lower()) for err in errors)

    def test_foreachbatch_neither_module_nor_handler(self):
        """Test ForEachBatch sink with neither module_path nor batch_handler."""
        action = Action(
            name="test_foreachbatch_sink",
            type=ActionType.WRITE,
            source="v_input_data",
            write_target={
                "type": "sink",
                "sink_type": "foreachbatch",
                "sink_name": "my_batch_sink"
            }
        )
        
        errors = self.validator.validate(action, "test_foreachbatch_sink")
        
        # Should error about missing both
        assert len(errors) > 0
        assert any(("module_path" in err.lower() or "batch_handler" in err.lower()) for err in errors)

    def test_foreachbatch_list_source_rejected(self):
        """Test ForEachBatch sink with list source (should be rejected)."""
        action = Action(
            name="test_foreachbatch_sink",
            type=ActionType.WRITE,
            source=["v_data1", "v_data2"],
            write_target={
                "type": "sink",
                "sink_type": "foreachbatch",
                "sink_name": "my_batch_sink",
                "module_path": "batch_handlers/my_handler.py"
            }
        )
        
        errors = self.validator.validate(action, "test_foreachbatch_sink")
        
        # Should error about list source
        assert len(errors) > 0
        assert any("single" in err.lower() or "string" in err.lower() for err in errors)

    def test_foreachbatch_empty_batch_handler(self):
        """Test ForEachBatch sink with empty batch_handler string."""
        action = Action(
            name="test_foreachbatch_sink",
            type=ActionType.WRITE,
            source="v_input_data",
            write_target={
                "type": "sink",
                "sink_type": "foreachbatch",
                "sink_name": "my_batch_sink",
                "batch_handler": ""
            }
        )
        
        errors = self.validator.validate(action, "test_foreachbatch_sink")
        
        # Should error about empty batch_handler
        assert len(errors) > 0
        assert any("empty" in err.lower() or "batch_handler" in err.lower() for err in errors)

    def test_foreachbatch_dict_source_rejected(self):
        """Test ForEachBatch sink with dict source (should be rejected)."""
        action = Action(
            name="test_foreachbatch_sink",
            type=ActionType.WRITE,
            source={"view": "v_data"},
            write_target={
                "type": "sink",
                "sink_type": "foreachbatch",
                "sink_name": "my_batch_sink",
                "module_path": "batch_handlers/my_handler.py"
            }
        )
        
        errors = self.validator.validate(action, "test_foreachbatch_sink")
        
        # Should error about dict source
        assert len(errors) > 0
        assert any("single" in err.lower() or "string" in err.lower() for err in errors)

