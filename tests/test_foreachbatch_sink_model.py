"""Test ForEachBatch sink model validation."""

import pytest
from pydantic import ValidationError
from lhp.models.config import WriteTarget, WriteTargetType


class TestForEachBatchSinkModel:
    """Test ForEachBatch sink write target model validation."""

    def test_foreachbatch_sink_minimal_with_module_path(self):
        """Test minimal valid ForEachBatch sink configuration with module_path."""
        write_target = WriteTarget(
            type=WriteTargetType.SINK,
            sink_type="foreachbatch",
            sink_name="test_batch_sink",
            module_path="batch_handlers/my_handler.py"
        )
        
        assert write_target.type == WriteTargetType.SINK
        assert write_target.sink_type == "foreachbatch"
        assert write_target.sink_name == "test_batch_sink"
        assert write_target.module_path == "batch_handlers/my_handler.py"

    def test_foreachbatch_with_batch_handler(self):
        """Test ForEachBatch sink with batch_handler field."""
        write_target = WriteTarget(
            type=WriteTargetType.SINK,
            sink_type="foreachbatch",
            sink_name="test_batch_sink",
            batch_handler="df.write.format('delta').saveAsTable('target')"
        )
        
        assert write_target.type == WriteTargetType.SINK
        assert write_target.sink_type == "foreachbatch"
        assert write_target.sink_name == "test_batch_sink"
        assert write_target.batch_handler == "df.write.format('delta').saveAsTable('target')"

    def test_foreachbatch_with_module_path(self):
        """Test ForEachBatch sink with module_path field."""
        write_target = WriteTarget(
            type=WriteTargetType.SINK,
            sink_type="foreachbatch",
            sink_name="test_batch_sink",
            module_path="handlers/process.py"
        )
        
        assert write_target.module_path == "handlers/process.py"
        assert write_target.batch_handler is None

    def test_foreachbatch_sink_type_value(self):
        """Test that sink_type is 'foreachbatch'."""
        write_target = WriteTarget(
            type=WriteTargetType.SINK,
            sink_type="foreachbatch",
            sink_name="test_sink",
            batch_handler="pass"
        )
        
        assert write_target.sink_type == "foreachbatch"

    def test_foreachbatch_without_database_table(self):
        """Test that ForEachBatch sinks don't require database and table fields."""
        write_target = WriteTarget(
            type=WriteTargetType.SINK,
            sink_type="foreachbatch",
            sink_name="test_sink",
            batch_handler="pass"
        )
        
        # Should not raise - database and table are optional for sinks
        assert write_target.database is None
        assert write_target.table is None

    def test_foreachbatch_with_comment(self):
        """Test ForEachBatch sink with optional comment field."""
        write_target = WriteTarget(
            type=WriteTargetType.SINK,
            sink_type="foreachbatch",
            sink_name="test_sink",
            batch_handler="pass",
            comment="Custom batch processing"
        )
        
        assert write_target.comment == "Custom batch processing"

    def test_foreachbatch_with_options(self):
        """Test ForEachBatch sink with optional options field."""
        write_target = WriteTarget(
            type=WriteTargetType.SINK,
            sink_type="foreachbatch",
            sink_name="test_sink",
            batch_handler="pass",
            options={"custom_option": "value"}
        )
        
        assert write_target.options == {"custom_option": "value"}

