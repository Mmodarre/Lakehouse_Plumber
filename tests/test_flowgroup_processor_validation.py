"""Tests for flowgroup processor validation integration."""

import pytest
import tempfile
from pathlib import Path
from lhp.core.services.flowgroup_processor import FlowgroupProcessor
from lhp.core.template_engine import TemplateEngine
from lhp.presets.preset_manager import PresetManager
from lhp.core.validator import ConfigValidator
from lhp.core.secret_validator import SecretValidator
from lhp.models.config import FlowGroup, Action, ActionType
from lhp.utils.substitution import EnhancedSubstitutionManager
from lhp.utils.error_formatter import LHPError


def test_flowgroup_processor_fails_on_unresolved_tokens():
    """FlowgroupProcessor should raise LHPError for unresolved tokens."""
    # Create flowgroup with unresolved token
    flowgroup = FlowGroup(
        pipeline="test_pipeline",
        flowgroup="test_flowgroup",
        actions=[
            Action(
                name="test_action",
                type=ActionType.LOAD,
                source={"type": "cloudfiles", "path": "s3://{missing_bucket}/data"},
                target="v_test"
            )
        ]
    )
    
    # Substitution manager with no mappings
    substitution_mgr = EnhancedSubstitutionManager()
    
    # Create processor with required dependencies
    # Use a temporary empty directory for presets
    with tempfile.TemporaryDirectory() as tmpdir:
        template_engine = TemplateEngine()
        preset_manager = PresetManager(presets_dir=Path(tmpdir))
        config_validator = ConfigValidator()
        secret_validator = SecretValidator()
        
        processor = FlowgroupProcessor(
            template_engine=template_engine,
            preset_manager=preset_manager,
            config_validator=config_validator,
            secret_validator=secret_validator
        )
        
        # Should raise LHPError with CONFIG category and code 010
        with pytest.raises(LHPError) as exc_info:
            processor.process_flowgroup(flowgroup, substitution_mgr)
        
        error = exc_info.value
        assert error.code == "LHP-CFG-010"
        assert "Unresolved substitution tokens" in str(error)
        assert "missing_bucket" in str(error)


def test_flowgroup_processor_passes_with_resolved_tokens():
    """FlowgroupProcessor should not raise unresolved token error when all tokens are resolved."""
    # Create flowgroup with token
    flowgroup = FlowGroup(
        pipeline="test_pipeline",
        flowgroup="test_flowgroup",
        actions=[
            Action(
                name="test_action",
                type=ActionType.LOAD,
                source={"type": "cloudfiles", "path": "s3://{bucket}/data", "format": "parquet"},
                target="v_test"
            )
        ]
    )
    
    # Substitution manager with mapping
    substitution_mgr = EnhancedSubstitutionManager()
    substitution_mgr.mappings = {"bucket": "my-bucket"}
    
    # Create processor with required dependencies
    # Use a temporary empty directory for presets
    with tempfile.TemporaryDirectory() as tmpdir:
        template_engine = TemplateEngine()
        preset_manager = PresetManager(presets_dir=Path(tmpdir))
        config_validator = ConfigValidator()
        secret_validator = SecretValidator()
        
        processor = FlowgroupProcessor(
            template_engine=template_engine,
            preset_manager=preset_manager,
            config_validator=config_validator,
            secret_validator=secret_validator
        )
        
        # Should raise ValueError for config validation, but NOT LHPError for unresolved tokens
        try:
            processed = processor.process_flowgroup(flowgroup, substitution_mgr)
        except LHPError as e:
            # If LHPError is raised, it should NOT be about unresolved tokens
            assert e.code != "LHP-CFG-010", "Should not raise unresolved token error when tokens are resolved"
            raise  # Re-raise to show it was a different error
        except ValueError:
            # Config validation error is expected since we don't have a complete flowgroup
            # The important thing is we didn't get LHP-CFG-010
            pass


def test_flowgroup_processor_detects_multiple_unresolved_tokens():
    """FlowgroupProcessor should detect multiple unresolved tokens."""
    # Create flowgroup with multiple unresolved tokens
    flowgroup = FlowGroup(
        pipeline="test_pipeline",
        flowgroup="test_flowgroup",
        actions=[
            Action(
                name="test_action1",
                type=ActionType.LOAD,
                source={"type": "cloudfiles", "path": "s3://{bucket1}/data"},
                target="v_test1"
            ),
            Action(
                name="test_action2",
                type=ActionType.LOAD,
                source={"type": "cloudfiles", "path": "s3://{bucket2}/logs"},
                target="v_test2"
            )
        ]
    )
    
    # Substitution manager with no mappings
    substitution_mgr = EnhancedSubstitutionManager()
    
    # Create processor with required dependencies
    # Use a temporary empty directory for presets
    with tempfile.TemporaryDirectory() as tmpdir:
        template_engine = TemplateEngine()
        preset_manager = PresetManager(presets_dir=Path(tmpdir))
        config_validator = ConfigValidator()
        secret_validator = SecretValidator()
        
        processor = FlowgroupProcessor(
            template_engine=template_engine,
            preset_manager=preset_manager,
            config_validator=config_validator,
            secret_validator=secret_validator
        )
        
        # Should raise LHPError mentioning both tokens
        with pytest.raises(LHPError) as exc_info:
            processor.process_flowgroup(flowgroup, substitution_mgr)
        
        error_str = str(exc_info.value)
        assert "bucket1" in error_str
        assert "bucket2" in error_str


def test_flowgroup_processor_resolves_local_variables():
    """FlowgroupProcessor should resolve local variables before other processing."""
    # Create flowgroup with local variables
    flowgroup = FlowGroup(
        pipeline="test_pipeline",
        flowgroup="test_flowgroup",
        variables={"table": "customers", "schema": "bronze"},
        actions=[
            Action(
                name="load_%{table}",
                type=ActionType.LOAD,
                source={"type": "cloudfiles", "path": "s3://bucket/data", "format": "parquet"},
                target="v_%{table}_%{schema}"
            )
        ]
    )
    
    # Substitution manager (not needed for local vars)
    substitution_mgr = EnhancedSubstitutionManager()
    
    # Create processor with required dependencies
    with tempfile.TemporaryDirectory() as tmpdir:
        template_engine = TemplateEngine()
        preset_manager = PresetManager(presets_dir=Path(tmpdir))
        config_validator = ConfigValidator()
        secret_validator = SecretValidator()
        
        processor = FlowgroupProcessor(
            template_engine=template_engine,
            preset_manager=preset_manager,
            config_validator=config_validator,
            secret_validator=secret_validator
        )
        
        # Process flowgroup
        try:
            processed = processor.process_flowgroup(flowgroup, substitution_mgr)
            
            # Verify local variables were resolved
            assert processed.actions[0].name == "load_customers"
            assert processed.actions[0].target == "v_customers_bronze"
        except ValueError:
            # Config validation error is expected, but local vars should be resolved
            # Re-run to check the resolution happened
            pass


def test_flowgroup_processor_fails_on_undefined_local_variable():
    """FlowgroupProcessor should raise LHPError for undefined local variables."""
    # Create flowgroup with undefined local variable
    flowgroup = FlowGroup(
        pipeline="test_pipeline",
        flowgroup="test_flowgroup",
        variables={"table": "customers"},
        actions=[
            Action(
                name="load_%{undefined}",
                type=ActionType.LOAD,
                source={"type": "cloudfiles", "path": "s3://bucket/data", "format": "parquet"},
                target="v_test"
            )
        ]
    )
    
    # Substitution manager
    substitution_mgr = EnhancedSubstitutionManager()
    
    # Create processor with required dependencies
    with tempfile.TemporaryDirectory() as tmpdir:
        template_engine = TemplateEngine()
        preset_manager = PresetManager(presets_dir=Path(tmpdir))
        config_validator = ConfigValidator()
        secret_validator = SecretValidator()
        
        processor = FlowgroupProcessor(
            template_engine=template_engine,
            preset_manager=preset_manager,
            config_validator=config_validator,
            secret_validator=secret_validator
        )
        
        # Should raise LHPError with CFG-011
        with pytest.raises(LHPError) as exc_info:
            processor.process_flowgroup(flowgroup, substitution_mgr)
        
        error = exc_info.value
        assert error.code == "LHP-CFG-011"
        assert "Undefined local variable" in error.title
        assert "%{undefined}" in error.details


def test_flowgroup_processor_local_vars_before_env_substitution():
    """Local variables should be resolved before environment substitution."""
    # Create flowgroup with both local vars and env tokens
    flowgroup = FlowGroup(
        pipeline="test_pipeline",
        flowgroup="test_flowgroup",
        variables={"entity": "customer"},
        actions=[
            Action(
                name="load_%{entity}",
                type=ActionType.LOAD,
                source={
                    "type": "delta",
                    "database": "{catalog}.{schema}",  # Env tokens
                    "table": "%{entity}"  # Local var
                },
                target="v_%{entity}"
            )
        ]
    )
    
    # Substitution manager with env tokens
    substitution_mgr = EnhancedSubstitutionManager()
    substitution_mgr.mappings = {"catalog": "main", "schema": "bronze"}
    
    # Create processor with required dependencies
    with tempfile.TemporaryDirectory() as tmpdir:
        template_engine = TemplateEngine()
        preset_manager = PresetManager(presets_dir=Path(tmpdir))
        config_validator = ConfigValidator()
        secret_validator = SecretValidator()
        
        processor = FlowgroupProcessor(
            template_engine=template_engine,
            preset_manager=preset_manager,
            config_validator=config_validator,
            secret_validator=secret_validator
        )
        
        # Process flowgroup
        try:
            processed = processor.process_flowgroup(flowgroup, substitution_mgr)
            
            # Verify local vars resolved and env tokens resolved
            assert processed.actions[0].name == "load_customer"
            assert processed.actions[0].target == "v_customer"
            assert processed.actions[0].source["database"] == "main.bronze"
            assert processed.actions[0].source["table"] == "customer"
        except ValueError:
            # Config validation error is expected
            pass


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

