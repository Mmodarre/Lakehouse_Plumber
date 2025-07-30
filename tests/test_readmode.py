"""Test readMode functionality across all components."""

import pytest
from pathlib import Path
import tempfile
import yaml

from lhp.models.config import Action, ActionType, TransformType, LoadSourceType, FlowGroup
from lhp.core.orchestrator import ActionOrchestrator
from lhp.generators.load.cloudfiles import CloudFilesLoadGenerator
from lhp.generators.load.delta import DeltaLoadGenerator
from lhp.generators.transform.data_quality import DataQualityTransformGenerator
from lhp.generators.transform.schema import SchemaTransformGenerator
from lhp.generators.transform.python import PythonTransformGenerator


class TestReadMode:
    """Test readMode implementation across the system."""
    
    def test_action_model_readmode(self):
        """Test that Action model accepts readMode field."""
        action = Action(
            name="test_action",
            type=ActionType.LOAD,
            target="v_test",
            readMode="stream"
        )
        assert action.readMode == "stream"
        
        # Test with batch mode
        action2 = Action(
            name="test_action2",
            type=ActionType.LOAD,
            target="v_test2",
            readMode="batch"
        )
        assert action2.readMode == "batch"
    
    def test_cloudfiles_requires_stream(self):
        """Test that CloudFiles enforces stream readMode."""
        generator = CloudFilesLoadGenerator()
        
        # Should work with stream mode
        action = Action(
            name="cf_load",
            type=ActionType.LOAD,
            target="v_data",
            readMode="stream",
            source={
                "type": "cloudfiles",
                "path": "/path/to/files",
                "format": "json"
            }
        )
        
        code = generator.generate(action, {})
        assert "spark.readStream" in code
        
        # Should fail with batch mode
        action_batch = Action(
            name="cf_load_batch",
            type=ActionType.LOAD,
            target="v_data",
            readMode="batch",
            source={
                "type": "cloudfiles",
                "path": "/path/to/files",
                "format": "json"
            }
        )
        
        with pytest.raises(ValueError) as exc:
            generator.generate(action_batch, {})
        assert "requires readMode='stream'" in str(exc.value)
    
    def test_data_quality_requires_stream(self):
        """Test that data quality transforms enforce stream readMode."""
        generator = DataQualityTransformGenerator()
        
        # Create expectations file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            expectations = {
                "col1 IS NOT NULL": {
                    "action": "fail",
                    "name": "not_null_col1"
                }
            }
            yaml.dump(expectations, f)
            expectations_file = f.name
        
        # Should work with stream mode
        action = Action(
            name="dq_check",
            type=ActionType.TRANSFORM,
            transform_type=TransformType.DATA_QUALITY,
            source="v_source",
            target="v_validated",
            readMode="stream",
            expectations_file=expectations_file
        )
        
        code = generator.generate(action, {"spec_dir": Path(expectations_file).parent})
        assert "spark.readStream.table" in code
        
        # Should fail with batch mode
        action_batch = Action(
            name="dq_check_batch",
            type=ActionType.TRANSFORM,
            transform_type=TransformType.DATA_QUALITY,
            source="v_source",
            target="v_validated",
            readMode="batch",
            expectations_file=expectations_file
        )
        
        with pytest.raises(ValueError) as exc:
            generator.generate(action_batch, {"spec_dir": Path(expectations_file).parent})
        assert "requires readMode='stream'" in str(exc.value)
        
        # Clean up
        Path(expectations_file).unlink()
    
    def test_delta_readmode_batch_stream(self):
        """Test Delta generator supports both batch and stream readMode."""
        generator = DeltaLoadGenerator()
        
        # Test batch mode
        action_batch = Action(
            name="delta_batch",
            type=ActionType.LOAD,
            target="v_data",
            readMode="batch",
            source={
                "type": "delta",
                "catalog": "main",
                "database": "bronze",
                "table": "customers"
            }
        )
        
        code = generator.generate(action_batch, {})
        assert "spark.read.table" in code
        assert "spark.readStream" not in code
        
        # Test stream mode
        action_stream = Action(
            name="delta_stream",
            type=ActionType.LOAD,
            target="v_data",
            readMode="stream",
            source={
                "type": "delta",
                "catalog": "main",
                "database": "bronze",
                "table": "customers"
            }
        )
        
        code = generator.generate(action_stream, {})
        assert "spark.readStream" in code
        assert "spark.read.table" not in code
    
    def test_transform_readmode_support(self):
        """Test that transform generators support readMode."""
        # Schema transform
        schema_gen = SchemaTransformGenerator()
        action_schema = Action(
            name="apply_schema",
            type=ActionType.TRANSFORM,
            transform_type=TransformType.SCHEMA,
            source="v_raw",
            target="v_typed",
            readMode="stream"
        )
        
        code = schema_gen.generate(action_schema, {})
        assert "spark.readStream.table" in code
        
        # Python transform
        python_gen = PythonTransformGenerator()
        action_python = Action(
            name="py_transform",
            type=ActionType.TRANSFORM,
            transform_type=TransformType.PYTHON,
            source="v_input",
            module_path="transforms/clean.py",
            function_name="clean_data",
            target="v_cleaned",
            readMode="batch"
        )
        
        # Create temporary Python file for the test
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)
            transforms_dir = tmpdir_path / "transforms"
            transforms_dir.mkdir(parents=True)
            (transforms_dir / "clean.py").write_text("""
def clean_data(df, spark, parameters):
    return df.filter("value IS NOT NULL")
""")
            
            code = python_gen.generate(action_python, {
                "output_dir": tmpdir_path / "generated",
                "spec_dir": tmpdir_path,
                "flowgroup": FlowGroup(
                    pipeline="test_pipeline",
                    flowgroup="test_flowgroup",
                    actions=[]
                )
            })
        assert "spark.read.table" in code
    
    def test_yaml_parsing_with_readmode(self):
        """Test that YAML parser correctly handles readMode."""
        yaml_content = """
pipeline: test_pipeline
flowgroup: test_flow
actions:
  - name: load_data
    type: load
    readMode: stream
    target: v_raw
    source:
      type: cloudfiles
      path: /data/files
      format: json
      
  - name: validate_data
    type: transform
    transform_type: data_quality
    readMode: stream
    source: v_raw
    target: v_validated
    expectations_file: expectations.json
"""
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write(yaml_content)
            yaml_file = f.name
        
        # Create a simple project structure
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            
            # Create required directories
            (project_root / "pipelines").mkdir()
            (project_root / "presets").mkdir()
            (project_root / "templates").mkdir()
            (project_root / "substitutions").mkdir()
            
            # Create expectations file
            exp_file = project_root / "expectations.json"
            exp_file.write_text(yaml.dump({
                "col1 IS NOT NULL": {"action": "fail", "name": "not_null"}
            }))
            
            # Create substitutions file
            (project_root / "substitutions" / "dev.yaml").write_text("environment:\n  catalog: test_catalog")
            
            # Parse the YAML
            orchestrator = ActionOrchestrator(project_root)
            flowgroup = orchestrator.yaml_parser.parse_flowgroup(yaml_file)
            
            # Verify readMode was parsed correctly
            assert flowgroup.actions[0].readMode == "stream"
            assert flowgroup.actions[1].readMode == "stream"
        
        # Clean up
        Path(yaml_file).unlink()
    
    def test_orchestrator_integration(self):
        """Test full integration with orchestrator."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            
            # Create project structure
            pipeline_dir = project_root / "pipelines" / "test_pipeline"
            pipeline_dir.mkdir(parents=True)
            
            # Create required directories
            (project_root / "presets").mkdir()
            (project_root / "templates").mkdir()
            (project_root / "substitutions").mkdir()
            (project_root / "generated").mkdir()
            
            # Create flowgroup with readMode
            flowgroup_content = """
pipeline: test_pipeline
flowgroup: test_flow
actions:
  - name: load_delta
    type: load
    readMode: batch
    target: v_customers
    source:
      type: delta
      catalog: main
      database: bronze
      table: customers
      
  - name: transform_customers
    type: transform
    transform_type: schema
    readMode: batch
    source: v_customers
    target: v_customers_typed
    
  - name: write_customers
    type: write
    source: v_customers_typed
    write_target:
      type: streaming_table
      database: silver
      table: customers_typed
      create_table: true
"""
            
            (pipeline_dir / "test_flow.yaml").write_text(flowgroup_content)
            
            # Create substitutions
            (project_root / "substitutions" / "dev.yaml").write_text("""
environment:
  catalog: dev_catalog
  schema: dev_schema
""")
            
            # Generate code
            orchestrator = ActionOrchestrator(project_root)
            generated_files = orchestrator.generate_pipeline("test_pipeline", "dev")
            
            # Get the generated code for test_flow
            generated_code = generated_files.get("test_flow.py", "")
            
            # Find the sections
            lines = generated_code.split('\n')
            
            # Check that load action uses batch mode
            load_section_found = False
            transform_section_found = False
            
            for i, line in enumerate(lines):
                # Check load section
                if "def v_customers():" in line:
                    load_section_found = True
                    # Look for the read statement in the next few lines
                    for j in range(i, min(i+10, len(lines))):
                        if "spark.read" in lines[j]:
                            assert "spark.read.table" in lines[j], "Load action should use batch mode"
                            assert "spark.readStream" not in lines[j], "Load action should not use stream mode"
                            break
                
                # Check transform section
                if "def v_customers_typed():" in line:
                    transform_section_found = True
                    # Look for the read statement in the next few lines
                    for j in range(i, min(i+10, len(lines))):
                        if "spark.read" in lines[j]:
                            assert "spark.read.table" in lines[j], "Transform action should use batch mode"
                            assert "spark.readStream" not in lines[j], "Transform action should not use stream mode"
                            break
            
            assert load_section_found, "Load action not found in generated code"
            assert transform_section_found, "Transform action not found in generated code"
            
            # Note: Streaming tables always use spark.readStream for their append flows,
            # regardless of the readMode of the actions feeding them


if __name__ == "__main__":
    pytest.main([__file__, "-v"]) 