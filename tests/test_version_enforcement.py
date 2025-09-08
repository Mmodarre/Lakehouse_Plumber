"""Tests for version enforcement functionality."""

import os
import pytest
from unittest.mock import patch, MagicMock
from pathlib import Path

from lhp.core.orchestrator import ActionOrchestrator
from lhp.utils.error_formatter import LHPError
from lhp.models.config import ProjectConfig


class TestVersionEnforcement:
    """Test version enforcement in ActionOrchestrator."""

    def test_no_version_requirement_passes(self, tmp_path):
        """Test that orchestrator works normally when no version requirement is set."""
        # Create a basic lhp.yaml without version requirement
        lhp_yaml = tmp_path / "lhp.yaml"
        lhp_yaml.write_text("""
name: test_project
version: "1.0"
""")
        
        # Should initialize without issues
        orchestrator = ActionOrchestrator(tmp_path)
        assert orchestrator.project_config.name == "test_project"
        assert orchestrator.project_config.required_lhp_version is None

    def test_matching_version_requirement_passes(self, tmp_path):
        """Test that matching version requirement allows initialization."""
        # Create lhp.yaml with version requirement
        lhp_yaml = tmp_path / "lhp.yaml"
        lhp_yaml.write_text("""
name: test_project
version: "1.0"
required_lhp_version: ">=0.4.0,<0.5.0"
""")
        
        # Mock get_version to return a compatible version
        with patch('lhp.core.orchestrator.get_version', return_value='0.4.1'):
            orchestrator = ActionOrchestrator(tmp_path)
            assert orchestrator.project_config.required_lhp_version == ">=0.4.0,<0.5.0"

    def test_non_matching_version_requirement_fails(self, tmp_path):
        """Test that non-matching version requirement raises LHPError."""
        # Create lhp.yaml with version requirement
        lhp_yaml = tmp_path / "lhp.yaml"
        lhp_yaml.write_text("""
name: test_project
version: "1.0"
required_lhp_version: ">=0.5.0,<0.6.0"
""")
        
        # Mock get_version to return an incompatible version
        with patch('lhp.core.orchestrator.get_version', return_value='0.4.1'):
            with pytest.raises(LHPError) as exc_info:
                ActionOrchestrator(tmp_path)
            
            error = exc_info.value
            assert error.code == "LHP-CFG-007"
            assert "version requirement not satisfied" in error.title.lower()
            assert ">=0.5.0,<0.6.0" in error.details
            assert "0.4.1" in error.details

    def test_exact_version_match_passes(self, tmp_path):
        """Test exact version matching."""
        lhp_yaml = tmp_path / "lhp.yaml"
        lhp_yaml.write_text("""
name: test_project
version: "1.0"
required_lhp_version: "==0.4.1"
""")
        
        with patch('lhp.core.orchestrator.get_version', return_value='0.4.1'):
            orchestrator = ActionOrchestrator(tmp_path)
            assert orchestrator.project_config.required_lhp_version == "==0.4.1"

    def test_exact_version_mismatch_fails(self, tmp_path):
        """Test exact version mismatch."""
        lhp_yaml = tmp_path / "lhp.yaml"
        lhp_yaml.write_text("""
name: test_project
version: "1.0"
required_lhp_version: "==0.4.1"
""")
        
        with patch('lhp.core.orchestrator.get_version', return_value='0.4.2'):
            with pytest.raises(LHPError) as exc_info:
                ActionOrchestrator(tmp_path)
            
            error = exc_info.value
            assert error.code == "LHP-CFG-007"
            assert "==0.4.1" in error.details
            assert "0.4.2" in error.details

    def test_tilde_version_requirement(self, tmp_path):
        """Test tilde version requirement (~=0.4.1 equivalent to >=0.4.1,<0.5.0)."""
        lhp_yaml = tmp_path / "lhp.yaml"
        lhp_yaml.write_text("""
name: test_project
version: "1.0"
required_lhp_version: "~=0.4.1"
""")
        
        # Should pass for 0.4.x versions
        with patch('lhp.core.orchestrator.get_version', return_value='0.4.5'):
            orchestrator = ActionOrchestrator(tmp_path)
            assert orchestrator.project_config.required_lhp_version == "~=0.4.1"
        
        # Should fail for 0.5.x versions
        with patch('lhp.core.orchestrator.get_version', return_value='0.5.0'):
            with pytest.raises(LHPError):
                ActionOrchestrator(tmp_path)

    def test_bypass_via_environment_variable(self, tmp_path):
        """Test bypassing version check with LHP_IGNORE_VERSION environment variable."""
        lhp_yaml = tmp_path / "lhp.yaml"
        lhp_yaml.write_text("""
name: test_project
version: "1.0"
required_lhp_version: ">=0.5.0,<0.6.0"
""")
        
        # Mock incompatible version but set bypass env var
        with patch('lhp.core.orchestrator.get_version', return_value='0.4.1'):
            # Test various bypass values
            for bypass_value in ["1", "true", "yes", "TRUE", "YES"]:
                with patch.dict(os.environ, {"LHP_IGNORE_VERSION": bypass_value}):
                    # Should not raise error
                    orchestrator = ActionOrchestrator(tmp_path)
                    assert orchestrator.project_config.required_lhp_version == ">=0.5.0,<0.6.0"

    def test_bypass_environment_variable_case_insensitive(self, tmp_path):
        """Test that bypass environment variable is case insensitive."""
        lhp_yaml = tmp_path / "lhp.yaml"
        lhp_yaml.write_text("""
name: test_project
version: "1.0"
required_lhp_version: ">=0.5.0,<0.6.0"
""")
        
        with patch('lhp.core.orchestrator.get_version', return_value='0.4.1'):
            with patch.dict(os.environ, {"LHP_IGNORE_VERSION": "True"}):
                orchestrator = ActionOrchestrator(tmp_path)
                assert orchestrator.project_config is not None

    def test_enforce_version_parameter_false_skips_check(self, tmp_path):
        """Test that enforce_version=False skips version checking."""
        lhp_yaml = tmp_path / "lhp.yaml"
        lhp_yaml.write_text("""
name: test_project
version: "1.0"
required_lhp_version: ">=0.5.0,<0.6.0"
""")
        
        # Mock incompatible version but disable enforcement
        with patch('lhp.core.orchestrator.get_version', return_value='0.4.1'):
            # Should not raise error when enforcement is disabled
            orchestrator = ActionOrchestrator(tmp_path, enforce_version=False)
            assert orchestrator.project_config.required_lhp_version == ">=0.5.0,<0.6.0"
            assert orchestrator.enforce_version is False

    def test_invalid_version_spec_raises_error(self, tmp_path):
        """Test that invalid version specification raises appropriate error."""
        lhp_yaml = tmp_path / "lhp.yaml"
        lhp_yaml.write_text("""
name: test_project
version: "1.0"
required_lhp_version: "invalid_spec"
""")
        
        with patch('lhp.core.orchestrator.get_version', return_value='0.4.1'):
            with pytest.raises(LHPError) as exc_info:
                ActionOrchestrator(tmp_path)
            
            error = exc_info.value
            assert error.code == "LHP-CFG-008"
            assert "invalid version requirement specification" in error.title.lower()
            assert "invalid_spec" in error.details

    def test_missing_packaging_dependency_error(self, tmp_path):
        """Test error when packaging dependency is missing."""
        lhp_yaml = tmp_path / "lhp.yaml"
        lhp_yaml.write_text("""
name: test_project
version: "1.0"
required_lhp_version: ">=0.4.0,<0.5.0"
""")
        
        # Mock ImportError for packaging by patching the specific import path
        with patch('lhp.core.orchestrator.get_version', return_value='0.4.1'):
            # Create a custom import function that raises ImportError only for packaging imports
            original_import = __builtins__['__import__']
            
            def mock_import(name, *args, **kwargs):
                if 'packaging' in name:
                    raise ImportError("No module named 'packaging'")
                return original_import(name, *args, **kwargs)
            
            with patch('builtins.__import__', side_effect=mock_import):
                with pytest.raises(LHPError) as exc_info:
                    ActionOrchestrator(tmp_path)
                
                error = exc_info.value
                assert error.code == "LHP-CFG-006"
                assert "missing packaging dependency" in error.title.lower()

    def test_complex_version_range(self, tmp_path):
        """Test complex version range with exclusions."""
        lhp_yaml = tmp_path / "lhp.yaml"
        lhp_yaml.write_text("""
name: test_project
version: "1.0"
required_lhp_version: ">=0.4.0,<0.5.0,!=0.4.3"
""")
        
        # Should pass for allowed versions
        with patch('lhp.core.orchestrator.get_version', return_value='0.4.1'):
            orchestrator = ActionOrchestrator(tmp_path)
            assert orchestrator.project_config is not None
        
        # Should fail for excluded version
        with patch('lhp.core.orchestrator.get_version', return_value='0.4.3'):
            with pytest.raises(LHPError):
                ActionOrchestrator(tmp_path)

    def test_no_project_config_skips_check(self, tmp_path):
        """Test that missing project config skips version checking."""
        # No lhp.yaml file
        with patch('lhp.core.orchestrator.get_version', return_value='0.4.1'):
            orchestrator = ActionOrchestrator(tmp_path)
            assert orchestrator.project_config is None

    def test_error_context_includes_project_info(self, tmp_path):
        """Test that version error includes project context information."""
        lhp_yaml = tmp_path / "lhp.yaml"
        lhp_yaml.write_text("""
name: my_test_project
version: "2.0"
required_lhp_version: ">=0.5.0,<0.6.0"
""")
        
        with patch('lhp.core.orchestrator.get_version', return_value='0.4.1'):
            with pytest.raises(LHPError) as exc_info:
                ActionOrchestrator(tmp_path)
            
            error = exc_info.value
            assert error.context is not None
            assert error.context.get("Project Name") == "my_test_project"
            assert error.context.get("Required Version") == ">=0.5.0,<0.6.0"
            assert error.context.get("Installed Version") == "0.4.1"
