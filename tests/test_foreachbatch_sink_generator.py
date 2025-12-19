"""Tests for ForEachBatch sink write generator of LakehousePlumber."""

import pytest
import tempfile
import shutil
from pathlib import Path
from unittest.mock import Mock, patch

from lhp.models.config import Action, ActionType, FlowGroup, ProjectConfig, ProjectOperationalMetadataConfig, MetadataColumnConfig
from lhp.generators.write.sinks import ForEachBatchSinkWriteGenerator
from lhp.generators.write.sink import SinkWriteGenerator
from lhp.utils.substitution import EnhancedSubstitutionManager


class TestForEachBatchSinkWriteGenerator:
    """Test ForEachBatch sink write generator."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.generator = ForEachBatchSinkWriteGenerator()
        self.temp_dir = Path(tempfile.mkdtemp())
        self.project_root = self.temp_dir / "test_project"
        self.project_root.mkdir()
    
    def teardown_method(self):
        """Clean up test environment."""
        shutil.rmtree(self.temp_dir)
    
    def test_generate_with_module_path(self):
        """Test generation with external file (module_path)."""
        # Create batch handler file
        handler_dir = self.project_root / "batch_handlers"
        handler_dir.mkdir()
        handler_file = handler_dir / "my_handler.py"
        handler_file.write_text("df.write.format('delta').mode('append').saveAsTable('target')")
        
        action = Action(
            name="write_foreachbatch_sink",
            type=ActionType.WRITE,
            source="v_data",
            write_target={
                "type": "sink",
                "sink_type": "foreachbatch",
                "sink_name": "my_batch_sink",
                "module_path": "batch_handlers/my_handler.py"
            }
        )
        
        context = {
            "spec_dir": self.project_root
        }
        
        code = self.generator.generate(action, context)
        
        # Check for decorator
        assert "@dp.foreach_batch_sink" in code
        assert 'name="my_batch_sink"' in code
        
        # Check for function body
        assert "df.write.format('delta')" in code
        assert "saveAsTable('target')" in code
        
        # Check for append_flow
        assert "@dp.append_flow" in code
        assert 'target="my_batch_sink"' in code
        assert "f_my_batch_sink_1" in code
    
    def test_generate_with_batch_handler(self):
        """Test generation with inline batch_handler."""
        action = Action(
            name="write_foreachbatch_sink",
            type=ActionType.WRITE,
            source="v_data",
            write_target={
                "type": "sink",
                "sink_type": "foreachbatch",
                "sink_name": "inline_sink",
                "batch_handler": "df.write.format('delta').mode('append').saveAsTable('target_table')"
            }
        )
        
        context = {}
        
        code = self.generator.generate(action, context)
        
        # Check for decorator
        assert "@dp.foreach_batch_sink" in code
        assert 'name="inline_sink"' in code
        
        # Check for inline code
        assert "df.write.format('delta')" in code
        assert "saveAsTable('target_table')" in code
        
        # Check for append_flow
        assert "@dp.append_flow" in code
        assert 'target="inline_sink"' in code
    
    def test_generate_sink_name_in_output(self):
        """Test that sink_name appears correctly in decorator."""
        action = Action(
            name="test_action",
            type=ActionType.WRITE,
            source="v_source",
            write_target={
                "type": "sink",
                "sink_type": "foreachbatch",
                "sink_name": "custom_sink_name",
                "batch_handler": "pass"
            }
        )
        
        code = self.generator.generate(action, {})
        
        assert '@dp.foreach_batch_sink(name="custom_sink_name")' in code
        assert 'def custom_sink_name(df, batch_id):' in code
    
    def test_generate_source_view_in_append_flow(self):
        """Test that source view appears in readStream."""
        action = Action(
            name="test_action",
            type=ActionType.WRITE,
            source="v_my_source",
            write_target={
                "type": "sink",
                "sink_type": "foreachbatch",
                "sink_name": "test_sink",
                "batch_handler": "pass"
            }
        )
        
        code = self.generator.generate(action, {})
        
        assert 'spark.readStream.table("v_my_source")' in code
    
    def test_generate_with_operational_metadata(self):
        """Test generation with operational metadata."""
        project_config = ProjectConfig(
            name="test_project",
            operational_metadata=ProjectOperationalMetadataConfig(
                columns={
                    "_ingestion_timestamp": MetadataColumnConfig(
                        expression="F.current_timestamp()",
                        applies_to=["view"]
                    )
                }
            )
        )
        
        flowgroup = FlowGroup(
            pipeline="test_pipeline",
            flowgroup="test_flowgroup",
            operational_metadata=["_ingestion_timestamp"]
        )
        
        action = Action(
            name="write_foreachbatch_sink",
            type=ActionType.WRITE,
            source="v_data",
            write_target={
                "type": "sink",
                "sink_type": "foreachbatch",
                "sink_name": "test_sink",
                "batch_handler": "pass"
            }
        )
        
        context = {
            "flowgroup": flowgroup,
            "project_config": project_config,
            "preset_config": {}
        }
        
        code = self.generator.generate(action, context)
        
        # Should have metadata column
        assert "withColumn" in code
        assert "_ingestion_timestamp" in code
    
    def test_generate_without_operational_metadata(self):
        """Test generation without operational metadata."""
        action = Action(
            name="write_foreachbatch_sink",
            type=ActionType.WRITE,
            source="v_data",
            write_target={
                "type": "sink",
                "sink_type": "foreachbatch",
                "sink_name": "test_sink",
                "batch_handler": "pass"
            }
        )
        
        code = self.generator.generate(action, {})
        
        # Should not have metadata columns
        assert "Add operational metadata" not in code
    
    def test_generate_function_signature(self):
        """Test that function has correct signature."""
        action = Action(
            name="test_action",
            type=ActionType.WRITE,
            source="v_source",
            write_target={
                "type": "sink",
                "sink_type": "foreachbatch",
                "sink_name": "my_sink",
                "batch_handler": "pass"
            }
        )
        
        code = self.generator.generate(action, {})
        
        # Check signature
        assert "def my_sink(df, batch_id):" in code
    
    def test_generate_return_statement(self):
        """Test that function ends with return statement."""
        action = Action(
            name="test_action",
            type=ActionType.WRITE,
            source="v_source",
            write_target={
                "type": "sink",
                "sink_type": "foreachbatch",
                "sink_name": "test_sink",
                "batch_handler": "df.write.saveAsTable('target')"
            }
        )
        
        code = self.generator.generate(action, {})
        
        # Should have return statement
        assert "return" in code
        # Return should be after the batch handler code
        assert code.index("saveAsTable") < code.index("return")
    
    def test_generate_docstring(self):
        """Test that function has docstring."""
        action = Action(
            name="my_action",
            type=ActionType.WRITE,
            source="v_source",
            write_target={
                "type": "sink",
                "sink_type": "foreachbatch",
                "sink_name": "test_sink",
                "batch_handler": "pass"
            }
        )
        
        code = self.generator.generate(action, {})
        
        # Should have docstring
        assert '"""ForEachBatch sink: my_action"""' in code
    
    def test_generate_file_not_found(self):
        """Test error when module_path file doesn't exist."""
        action = Action(
            name="test_action",
            type=ActionType.WRITE,
            source="v_source",
            write_target={
                "type": "sink",
                "sink_type": "foreachbatch",
                "sink_name": "test_sink",
                "module_path": "nonexistent/handler.py"
            }
        )
        
        context = {
            "spec_dir": self.project_root
        }
        
        with pytest.raises(FileNotFoundError):
            self.generator.generate(action, context)
    
    def test_generate_with_substitution(self):
        """Test that substitution tokens are replaced."""
        # Create handler file with substitution token
        handler_dir = self.project_root / "batch_handlers"
        handler_dir.mkdir()
        handler_file = handler_dir / "my_handler.py"
        handler_file.write_text("df.write.saveAsTable('${target_table}')")
        
        # Create substitution manager
        substitution_mgr = EnhancedSubstitutionManager(substitution_file=None, env="dev")
        substitution_mgr.mappings["target_table"] = "catalog.schema.my_table"
        
        action = Action(
            name="test_action",
            type=ActionType.WRITE,
            source="v_source",
            write_target={
                "type": "sink",
                "sink_type": "foreachbatch",
                "sink_name": "test_sink",
                "module_path": "batch_handlers/my_handler.py"
            }
        )
        
        context = {
            "spec_dir": self.project_root,
            "substitution_manager": substitution_mgr
        }
        
        code = self.generator.generate(action, context)
        
        # Token should be replaced
        assert "catalog.schema.my_table" in code
        assert "${target_table}" not in code
    
    def test_generate_comment_from_description(self):
        """Test that action description is used in docstring."""
        action = Action(
            name="test_action",
            type=ActionType.WRITE,
            source="v_source",
            description="Custom batch processing logic",
            write_target={
                "type": "sink",
                "sink_type": "foreachbatch",
                "sink_name": "test_sink",
                "batch_handler": "pass"
            }
        )
        
        code = self.generator.generate(action, {})
        
        # Description should appear in docstring
        assert "Custom batch processing logic" in code or "test_action" in code
    
    def test_generate_imports(self):
        """Test that required imports are present."""
        action = Action(
            name="test_action",
            type=ActionType.WRITE,
            source="v_source",
            write_target={
                "type": "sink",
                "sink_type": "foreachbatch",
                "sink_name": "test_sink",
                "batch_handler": "pass"
            }
        )
        
        self.generator.generate(action, {})
        
        # Check imports via import manager
        imports = self.generator.get_import_manager().get_consolidated_imports()
        assert "from pyspark import pipelines as dp" in imports
    
    def test_generate_indentation(self):
        """Test that generated code has proper Python indentation."""
        action = Action(
            name="test_action",
            type=ActionType.WRITE,
            source="v_source",
            write_target={
                "type": "sink",
                "sink_type": "foreachbatch",
                "sink_name": "test_sink",
                "batch_handler": "df.write.saveAsTable('target')"
            }
        )
        
        code = self.generator.generate(action, {})
        
        # Code should be valid Python (basic check)
        assert "def " in code
        assert "    " in code  # Should have indentation
        # Function body should be indented
        lines = code.split("\n")
        func_line_idx = next(i for i, line in enumerate(lines) if "def " in line)
        # Next non-empty line should be indented
        next_line = lines[func_line_idx + 1]
        assert next_line.startswith("    ")


    def test_generate_missing_sink_name_error(self):
        """Test error when sink_name is missing."""
        action = Action(
            name="test_action",
            type=ActionType.WRITE,
            source="v_source",
            write_target={
                "type": "sink",
                "sink_type": "foreachbatch",
                # Missing sink_name
                "batch_handler": "pass"
            }
        )
        
        with pytest.raises(Exception) as exc_info:
            self.generator.generate(action, {})
        
        assert "sink_name" in str(exc_info.value).lower()
    
    def test_generate_neither_module_nor_handler_error(self):
        """Test error when neither module_path nor batch_handler provided."""
        action = Action(
            name="test_action",
            type=ActionType.WRITE,
            source="v_source",
            write_target={
                "type": "sink",
                "sink_type": "foreachbatch",
                "sink_name": "test_sink"
                # Missing both module_path and batch_handler
            }
        )
        
        with pytest.raises(ValueError) as exc_info:
            self.generator.generate(action, {})
        
        assert "module_path" in str(exc_info.value) or "batch_handler" in str(exc_info.value)
    
    def test_generate_missing_source_error(self):
        """Test error when source is missing."""
        action = Action(
            name="test_action",
            type=ActionType.WRITE,
            # Missing source
            write_target={
                "type": "sink",
                "sink_type": "foreachbatch",
                "sink_name": "test_sink",
                "batch_handler": "pass"
            }
        )
        
        with pytest.raises(ValueError) as exc_info:
            self.generator.generate(action, {})
        
        assert "source" in str(exc_info.value).lower()
    
    def test_generate_non_string_source_error(self):
        """Test error when source is not a string."""
        action = Action(
            name="test_action",
            type=ActionType.WRITE,
            source=["v_source1", "v_source2"],  # List instead of string
            write_target={
                "type": "sink",
                "sink_type": "foreachbatch",
                "sink_name": "test_sink",
                "batch_handler": "pass"
            }
        )
        
        with pytest.raises(ValueError) as exc_info:
            self.generator.generate(action, {})
        
        assert "single source" in str(exc_info.value).lower() or "string" in str(exc_info.value).lower()


class TestSinkWriteGeneratorDispatcher:
    """Test SinkWriteGenerator dispatcher with ForEachBatch."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.generator = SinkWriteGenerator()
        self.temp_dir = Path(tempfile.mkdtemp())
        self.project_root = self.temp_dir / "test_project"
        self.project_root.mkdir()
    
    def teardown_method(self):
        """Clean up test environment."""
        shutil.rmtree(self.temp_dir)
    
    def test_dispatch_to_foreachbatch(self):
        """Test dispatching to foreachbatch generator."""
        action = Action(
            name="write_foreachbatch_sink",
            type=ActionType.WRITE,
            source="v_data",
            write_target={
                "type": "sink",
                "sink_type": "foreachbatch",
                "sink_name": "test_sink",
                "batch_handler": "pass"
            }
        )
        
        code = self.generator.generate(action, {})
        
        assert "@dp.foreach_batch_sink" in code
    
    def test_foreachbatch_in_generators_dict(self):
        """Test that foreachbatch is registered in dispatcher."""
        assert "foreachbatch" in self.generator.generators

