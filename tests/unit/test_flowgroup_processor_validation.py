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


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

