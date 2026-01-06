"""Tests for notebook-level Spark configuration in flowgroups."""

import pytest
from pathlib import Path
import tempfile

from lhp.models.config import FlowGroup, Action, ActionType
from lhp.core.validator import ConfigValidator
from lhp.core.services.code_generator import CodeGenerator
from lhp.core.action_registry import ActionRegistry
from lhp.core.dependency_resolver import DependencyResolver
from lhp.presets.preset_manager import PresetManager
from lhp.utils.substitution import EnhancedSubstitutionManager


class TestSparkConfigValidation:
    """Test Spark configuration validation at flowgroup level."""

    def test_valid_spark_config_string_values(self):
        """Test validation passes with valid string Spark config values."""
        validator = ConfigValidator()
        
        flowgroup = FlowGroup(
            pipeline="test_pipeline",
            flowgroup="test_flowgroup",
            spark_config={
                "spark.sql.streaming.schemaInference": "false",
                "spark.sql.streaming.stateStore.stateSchemaCheck": "false"
            },
            actions=[
                Action(
                    name="load_data",
                    type=ActionType.LOAD,
                    target="v_data",
                    source={"type": "delta", "table": "test"}
                ),
                Action(
                    name="write_data",
                    type=ActionType.WRITE,
                    source="v_data",
                    write_target={
                        "type": "streaming_table",
                        "database": "test",
                        "table": "test_table"
                    }
                )
            ]
        )
        
        errors = validator.validate_flowgroup(flowgroup)
        assert len(errors) == 0

    def test_valid_spark_config_boolean_values(self):
        """Test validation passes with boolean Spark config values."""
        validator = ConfigValidator()
        
        flowgroup = FlowGroup(
            pipeline="test_pipeline",
            flowgroup="test_flowgroup",
            spark_config={
                "pipelines.incompatibleViewCheck.enabled": False,
                "spark.sql.streaming.stateStore.stateSchemaCheck": True
            },
            actions=[
                Action(
                    name="load_data",
                    type=ActionType.LOAD,
                    target="v_data",
                    source={"type": "delta", "table": "test"}
                )
            ]
        )
        
        errors = validator.validate_flowgroup(flowgroup)
        assert len(errors) == 0

    def test_valid_spark_config_numeric_values(self):
        """Test validation passes with numeric Spark config values."""
        validator = ConfigValidator()
        
        flowgroup = FlowGroup(
            pipeline="test_pipeline",
            flowgroup="test_flowgroup",
            spark_config={
                "spark.sql.shuffle.partitions": 200,
                "spark.sql.streaming.checkpointInterval": 30000
            },
            actions=[
                Action(
                    name="load_data",
                    type=ActionType.LOAD,
                    target="v_data",
                    source={"type": "delta", "table": "test"}
                )
            ]
        )
        
        errors = validator.validate_flowgroup(flowgroup)
        assert len(errors) == 0

    def test_valid_spark_config_mixed_types(self):
        """Test validation passes with mixed type Spark config values."""
        validator = ConfigValidator()
        
        flowgroup = FlowGroup(
            pipeline="test_pipeline",
            flowgroup="test_flowgroup",
            spark_config={
                "pipelines.incompatibleViewCheck.enabled": False,
                "spark.sql.shuffle.partitions": 200,
                "spark.sql.streaming.stateStore.providerClass": "RocksDBStateStoreProvider",
                "spark.sql.streaming.checkpointInterval": 30000.5
            },
            actions=[
                Action(
                    name="load_data",
                    type=ActionType.LOAD,
                    target="v_data",
                    source={"type": "delta", "table": "test"}
                )
            ]
        )
        
        errors = validator.validate_flowgroup(flowgroup)
        assert len(errors) == 0

    def test_invalid_spark_config_not_dict(self):
        """Test validation fails when spark_config is not a dictionary."""
        validator = ConfigValidator()
        
        flowgroup = FlowGroup(
            pipeline="test_pipeline",
            flowgroup="test_flowgroup",
            spark_config="invalid",  # type: ignore
            actions=[
                Action(
                    name="load_data",
                    type=ActionType.LOAD,
                    target="v_data",
                    source={"type": "delta", "table": "test"}
                )
            ]
        )
        
        errors = validator.validate_flowgroup(flowgroup)
        assert len(errors) > 0
        assert any("must be a dictionary" in error for error in errors)

    def test_invalid_spark_config_invalid_key_type(self):
        """Test validation fails when spark_config key is not a string."""
        validator = ConfigValidator()
        
        flowgroup = FlowGroup(
            pipeline="test_pipeline",
            flowgroup="test_flowgroup",
            spark_config={
                123: "value"  # type: ignore
            },
            actions=[
                Action(
                    name="load_data",
                    type=ActionType.LOAD,
                    target="v_data",
                    source={"type": "delta", "table": "test"}
                )
            ]
        )
        
        errors = validator.validate_flowgroup(flowgroup)
        assert len(errors) > 0
        assert any("key must be string" in error for error in errors)

    def test_invalid_spark_config_invalid_value_type(self):
        """Test validation fails when spark_config value has unsupported type."""
        validator = ConfigValidator()
        
        flowgroup = FlowGroup(
            pipeline="test_pipeline",
            flowgroup="test_flowgroup",
            spark_config={
                "spark.sql.streaming.stateStore.stateSchemaCheck": ["invalid"]  # type: ignore
            },
            actions=[
                Action(
                    name="load_data",
                    type=ActionType.LOAD,
                    target="v_data",
                    source={"type": "delta", "table": "test"}
                )
            ]
        )
        
        errors = validator.validate_flowgroup(flowgroup)
        assert len(errors) > 0
        assert any("must be string, bool, int, or float" in error for error in errors)

    def test_empty_spark_config(self):
        """Test validation passes with empty spark_config."""
        validator = ConfigValidator()
        
        flowgroup = FlowGroup(
            pipeline="test_pipeline",
            flowgroup="test_flowgroup",
            spark_config={},
            actions=[
                Action(
                    name="load_data",
                    type=ActionType.LOAD,
                    target="v_data",
                    source={"type": "delta", "table": "test"}
                )
            ]
        )
        
        errors = validator.validate_flowgroup(flowgroup)
        assert len(errors) == 0

    def test_none_spark_config(self):
        """Test validation passes with None spark_config."""
        validator = ConfigValidator()
        
        flowgroup = FlowGroup(
            pipeline="test_pipeline",
            flowgroup="test_flowgroup",
            spark_config=None,
            actions=[
                Action(
                    name="load_data",
                    type=ActionType.LOAD,
                    target="v_data",
                    source={"type": "delta", "table": "test"}
                )
            ]
        )
        
        errors = validator.validate_flowgroup(flowgroup)
        assert len(errors) == 0


class TestSparkConfigCodeGeneration:
    """Test Spark configuration injection into generated code."""

    @pytest.fixture
    def code_generator(self):
        """Create a CodeGenerator instance."""
        registry = ActionRegistry()
        resolver = DependencyResolver()
        preset_dir = Path(tempfile.mkdtemp())
        preset_mgr = PresetManager(presets_dir=preset_dir)
        return CodeGenerator(
            action_registry=registry,
            dependency_resolver=resolver,
            preset_manager=preset_mgr
        )

    def test_spark_config_injection_in_generated_code(self, code_generator):
        """Test that spark_config is injected into generated Python code."""
        flowgroup = FlowGroup(
            pipeline="streaming_pipeline",
            flowgroup="streaming_views",
            spark_config={
                "pipelines.incompatibleViewCheck.enabled": False,
                "spark.sql.streaming.stateStore.stateSchemaCheck": False
            },
            actions=[
                Action(
                    name="load_data",
                    type=ActionType.LOAD,
                    target="v_data",
                    source={"type": "delta", "table": "raw.data"}
                ),
                Action(
                    name="write_data",
                    type=ActionType.WRITE,
                    source="v_data",
                    write_target={
                        "type": "streaming_table",
                        "database": "test",
                        "table": "test_table"
                    }
                )
            ]
        )
        
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            substitution_mgr = EnhancedSubstitutionManager({}, {})
            
            code = code_generator.generate_flowgroup_code(
                flowgroup, 
                substitution_mgr,
                output_dir=project_root
            )
            
            # Verify spark config is in the generated code (False renders as Python bool)
            assert 'spark.conf.set("pipelines.incompatibleViewCheck.enabled", False)' in code
            assert 'spark.conf.set("spark.sql.streaming.stateStore.stateSchemaCheck", False)' in code

    def test_spark_config_string_value_in_generated_code(self, code_generator):
        """Test that string values are properly quoted in generated code."""
        flowgroup = FlowGroup(
            pipeline="test_pipeline",
            flowgroup="test_flowgroup",
            spark_config={
                "spark.sql.streaming.stateStore.providerClass": "RocksDBStateStoreProvider"
            },
            actions=[
                Action(
                    name="load_data",
                    type=ActionType.LOAD,
                    target="v_data",
                    source={"type": "delta", "table": "raw.data"}
                )
            ]
        )
        
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            substitution_mgr = EnhancedSubstitutionManager({}, {})
            
            code = code_generator.generate_flowgroup_code(
                flowgroup,
                substitution_mgr,
                output_dir=project_root
            )
            
            # Verify string value is quoted
            assert 'spark.conf.set("spark.sql.streaming.stateStore.providerClass", "RocksDBStateStoreProvider")' in code

    def test_spark_config_boolean_false_in_generated_code(self, code_generator):
        """Test that boolean False values are properly handled in generated code."""
        flowgroup = FlowGroup(
            pipeline="test_pipeline",
            flowgroup="test_flowgroup",
            spark_config={
                "pipelines.incompatibleViewCheck.enabled": False
            },
            actions=[
                Action(
                    name="load_data",
                    type=ActionType.LOAD,
                    target="v_data",
                    source={"type": "delta", "table": "raw.data"}
                )
            ]
        )
        
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            substitution_mgr = EnhancedSubstitutionManager({}, {})
            
            code = code_generator.generate_flowgroup_code(
                flowgroup,
                substitution_mgr,
                output_dir=project_root
            )
            
            # Verify boolean False is rendered correctly (lowercase in Python)
            assert 'spark.conf.set("pipelines.incompatibleViewCheck.enabled", False)' in code

    def test_spark_config_boolean_true_in_generated_code(self, code_generator):
        """Test that boolean True values are properly handled in generated code."""
        flowgroup = FlowGroup(
            pipeline="test_pipeline",
            flowgroup="test_flowgroup",
            spark_config={
                "spark.streaming.enabled": True
            },
            actions=[
                Action(
                    name="load_data",
                    type=ActionType.LOAD,
                    target="v_data",
                    source={"type": "delta", "table": "raw.data"}
                )
            ]
        )
        
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            substitution_mgr = EnhancedSubstitutionManager({}, {})
            
            code = code_generator.generate_flowgroup_code(
                flowgroup,
                substitution_mgr,
                output_dir=project_root
            )
            
            # Verify boolean True is rendered correctly (capitalized in Python)
            assert 'spark.conf.set("spark.streaming.enabled", True)' in code

    def test_spark_config_numeric_value_in_generated_code(self, code_generator):
        """Test that numeric values are properly handled in generated code."""
        flowgroup = FlowGroup(
            pipeline="test_pipeline",
            flowgroup="test_flowgroup",
            spark_config={
                "spark.sql.shuffle.partitions": 200,
                "spark.sql.streaming.checkpointInterval": 30000.5
            },
            actions=[
                Action(
                    name="load_data",
                    type=ActionType.LOAD,
                    target="v_data",
                    source={"type": "delta", "table": "raw.data"}
                )
            ]
        )
        
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            substitution_mgr = EnhancedSubstitutionManager({}, {})
            
            code = code_generator.generate_flowgroup_code(
                flowgroup,
                substitution_mgr,
                output_dir=project_root
            )
            
            # Verify numeric values are not quoted
            assert 'spark.conf.set("spark.sql.shuffle.partitions", 200)' in code
            assert 'spark.conf.set("spark.sql.streaming.checkpointInterval", 30000.5)' in code

    def test_spark_config_placement_after_flowgroup_id(self, code_generator):
        """Test that spark config is placed after FLOWGROUP_ID in generated code."""
        flowgroup = FlowGroup(
            pipeline="test_pipeline",
            flowgroup="test_flowgroup",
            spark_config={
                "pipelines.incompatibleViewCheck.enabled": False
            },
            actions=[
                Action(
                    name="load_data",
                    type=ActionType.LOAD,
                    target="v_data",
                    source={"type": "delta", "table": "raw.data"}
                )
            ]
        )
        
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            substitution_mgr = EnhancedSubstitutionManager({}, {})
            
            code = code_generator.generate_flowgroup_code(
                flowgroup,
                substitution_mgr,
                output_dir=project_root
            )
            
            # Find positions
            flowgroup_id_pos = code.find('FLOWGROUP_ID = "test_flowgroup"')
            spark_conf_pos = code.find('spark.conf.set("pipelines.incompatibleViewCheck.enabled"')
            
            # Verify spark config appears after FLOWGROUP_ID
            assert flowgroup_id_pos != -1
            assert spark_conf_pos != -1
            assert spark_conf_pos > flowgroup_id_pos

    def test_no_spark_config_in_code_when_not_specified(self, code_generator):
        """Test that no spark config code is generated when not specified."""
        flowgroup = FlowGroup(
            pipeline="test_pipeline",
            flowgroup="test_flowgroup",
            spark_config=None,
            actions=[
                Action(
                    name="load_data",
                    type=ActionType.LOAD,
                    target="v_data",
                    source={"type": "delta", "table": "raw.data"}
                )
            ]
        )
        
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            substitution_mgr = EnhancedSubstitutionManager({}, {})
            
            code = code_generator.generate_flowgroup_code(
                flowgroup,
                substitution_mgr,
                output_dir=project_root
            )
            
            # Verify no spark.conf.set calls are in the code
            assert 'spark.conf.set(' not in code


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
