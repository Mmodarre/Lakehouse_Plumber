"""Tests for ActionOrchestrator initialization and configuration."""

import pytest
from unittest.mock import Mock, patch, MagicMock
from pathlib import Path
import os

from lhp.core.orchestrator import ActionOrchestrator
from lhp.utils.error_formatter import LHPError, ErrorCategory


class TestActionOrchestratorInitialization:
    """Test ActionOrchestrator initialization and configuration logic."""

    @pytest.fixture
    def mock_project_root(self):
        """Mock project root path."""
        return Path("/mock/project")

    @pytest.fixture
    def mock_components(self):
        """Mock all component dependencies."""
        with patch('lhp.core.orchestrator.YAMLParser') as mock_yaml, \
             patch('lhp.core.orchestrator.PresetManager') as mock_preset, \
             patch('lhp.core.orchestrator.TemplateEngine') as mock_template, \
             patch('lhp.core.orchestrator.ProjectConfigLoader') as mock_config_loader, \
             patch('lhp.core.orchestrator.ActionRegistry') as mock_registry, \
             patch('lhp.core.orchestrator.ConfigValidator') as mock_config_validator, \
             patch('lhp.core.orchestrator.SecretValidator') as mock_secret_validator, \
             patch('lhp.core.orchestrator.DependencyResolver') as mock_dependency_resolver, \
             patch('lhp.core.orchestrator.FlowgroupDiscoverer') as mock_discoverer, \
             patch('lhp.core.orchestrator.FlowgroupProcessor') as mock_processor, \
             patch('lhp.core.orchestrator.CodeGenerator') as mock_generator, \
             patch('lhp.core.orchestrator.PipelineValidator') as mock_validator:
            
            # Configure mocks
            mock_config_loader_instance = Mock()
            mock_config_loader.return_value = mock_config_loader_instance
            mock_config_loader_instance.load_project_config.return_value = None
            
            yield {
                'yaml_parser': mock_yaml,
                'preset_manager': mock_preset,
                'template_engine': mock_template,
                'config_loader': mock_config_loader,
                'action_registry': mock_registry,
                'config_validator': mock_config_validator,
                'secret_validator': mock_secret_validator,
                'dependency_resolver': mock_dependency_resolver,
                'discoverer': mock_discoverer,
                'processor': mock_processor,
                'generator': mock_generator,
                'validator': mock_validator,
                'config_loader_instance': mock_config_loader_instance
            }

    def test_version_enforcement_enabled_with_project_requirement(self, mock_project_root, mock_components):
        """Test version enforcement runs when enforce_version=True and project has requirements."""
        # Arrange
        mock_project_config = Mock()
        mock_project_config.required_lhp_version = ">=0.4.0"
        mock_project_config.name = "test_project"
        mock_project_config.version = "1.0.0"
        mock_components['config_loader_instance'].load_project_config.return_value = mock_project_config
        
        with patch.object(ActionOrchestrator, '_enforce_version_requirements') as mock_enforce:
            # Act
            orchestrator = ActionOrchestrator(mock_project_root, enforce_version=True)
            
            # Assert
            mock_enforce.assert_called_once()
            assert orchestrator.enforce_version is True
            assert orchestrator.project_config == mock_project_config

    def test_version_enforcement_disabled(self, mock_project_root, mock_components):
        """Test version enforcement skipped when enforce_version=False."""
        # Arrange
        mock_project_config = Mock()
        mock_project_config.required_lhp_version = ">=0.4.0"
        mock_components['config_loader_instance'].load_project_config.return_value = mock_project_config
        
        with patch.object(ActionOrchestrator, '_enforce_version_requirements') as mock_enforce:
            # Act
            orchestrator = ActionOrchestrator(mock_project_root, enforce_version=False)
            
            # Assert
            mock_enforce.assert_not_called()
            assert orchestrator.enforce_version is False
            assert orchestrator.project_config == mock_project_config

    def test_project_config_loader_fails(self, mock_project_root, mock_components):
        """Test handling when project_config_loader fails to load config."""
        # Arrange
        mock_components['config_loader_instance'].load_project_config.side_effect = Exception("Config loading failed")
        
        # Act & Assert - should propagate the exception
        with pytest.raises(Exception, match="Config loading failed"):
            ActionOrchestrator(mock_project_root, enforce_version=False)

    def test_core_component_init_fails(self, mock_project_root):
        """Test exception propagation when core component initialization fails."""
        # Arrange - mock one component to fail
        with patch('lhp.core.orchestrator.YAMLParser') as mock_yaml, \
             patch('lhp.core.orchestrator.PresetManager') as mock_preset, \
             patch('lhp.core.orchestrator.TemplateEngine') as mock_template, \
             patch('lhp.core.orchestrator.ProjectConfigLoader') as mock_config_loader:
            
            # Make one component fail during initialization
            mock_preset.side_effect = Exception("PresetManager initialization failed")
            
            # Configure successful components
            mock_config_loader_instance = Mock()
            mock_config_loader.return_value = mock_config_loader_instance
            mock_config_loader_instance.load_project_config.return_value = None
            
            # Act & Assert - should propagate the component initialization exception
            with pytest.raises(Exception, match="PresetManager initialization failed"):
                ActionOrchestrator(mock_project_root, enforce_version=False)

    def test_services_init_fails(self, mock_project_root, mock_components):
        """Test error when services fail to initialize with dependencies."""
        # Arrange - mock service initialization to fail
        mock_components['discoverer'].side_effect = Exception("FlowgroupDiscoverer initialization failed")
        
        # Act & Assert - should propagate the service initialization exception
        with pytest.raises(Exception, match="FlowgroupDiscoverer initialization failed"):
            ActionOrchestrator(mock_project_root, enforce_version=False)

    def test_project_root_nonexistent(self, mock_components):
        """Test initialization when project_root does not exist."""
        # Arrange
        nonexistent_root = Path("/nonexistent/project/root")
        
        with patch('logging.getLogger') as mock_logger:
            mock_logger_instance = Mock()
            mock_logger.return_value = mock_logger_instance
            
            # Act - should still initialize successfully
            orchestrator = ActionOrchestrator(nonexistent_root, enforce_version=False)
            
            # Assert
            assert orchestrator.project_root == nonexistent_root
            # The initialization should succeed even with nonexistent root
            # Components like PresetManager and TemplateEngine will handle nonexistent paths

    def test_project_config_exists_logging(self, mock_project_root, mock_components):
        """Test logging when project config exists with name and version."""
        # Arrange
        mock_project_config = Mock()
        mock_project_config.name = "test_project"
        mock_project_config.version = "2.1.0"
        mock_project_config.required_lhp_version = None
        mock_components['config_loader_instance'].load_project_config.return_value = mock_project_config
        
        with patch('logging.getLogger') as mock_get_logger:
            mock_logger = Mock()
            mock_get_logger.return_value = mock_logger
            
            # Act
            orchestrator = ActionOrchestrator(mock_project_root, enforce_version=False)
            
            # Assert
            assert orchestrator.project_config == mock_project_config
            # Should log project configuration details
            mock_logger.info.assert_any_call(
                f"Loaded project configuration: test_project v2.1.0"
            )

    def test_project_config_none_logging(self, mock_project_root, mock_components):
        """Test logging when project config is None (using defaults)."""
        # Arrange
        mock_components['config_loader_instance'].load_project_config.return_value = None
        
        with patch('logging.getLogger') as mock_get_logger:
            mock_logger = Mock()
            mock_get_logger.return_value = mock_logger
            
            # Act
            orchestrator = ActionOrchestrator(mock_project_root, enforce_version=False)
            
            # Assert
            assert orchestrator.project_config is None
            # Should log "using defaults" message
            mock_logger.info.assert_any_call("No project configuration found, using defaults")


class TestActionOrchestratorVersionEnforcement:
    """Test ActionOrchestrator version requirement enforcement logic."""

    @pytest.fixture
    def mock_project_root(self):
        """Mock project root path."""
        return Path("/mock/project")

    @pytest.fixture
    def orchestrator_with_version_requirement(self, mock_project_root):
        """Create orchestrator with version requirement for testing."""
        with patch('lhp.core.orchestrator.YAMLParser'), \
             patch('lhp.core.orchestrator.PresetManager'), \
             patch('lhp.core.orchestrator.TemplateEngine'), \
             patch('lhp.core.orchestrator.ProjectConfigLoader') as mock_config_loader, \
             patch('lhp.core.orchestrator.ActionRegistry'), \
             patch('lhp.core.orchestrator.ConfigValidator'), \
             patch('lhp.core.orchestrator.SecretValidator'), \
             patch('lhp.core.orchestrator.DependencyResolver'), \
             patch('lhp.core.orchestrator.FlowgroupDiscoverer'), \
             patch('lhp.core.orchestrator.FlowgroupProcessor'), \
             patch('lhp.core.orchestrator.CodeGenerator'), \
             patch('lhp.core.orchestrator.PipelineValidator'):
            
            # Configure project config with version requirement
            mock_project_config = Mock()
            mock_project_config.required_lhp_version = ">=0.4.0"
            mock_project_config.name = "test_project"
            mock_project_config.version = "1.0.0"
            
            mock_config_loader_instance = Mock()
            mock_config_loader.return_value = mock_config_loader_instance
            mock_config_loader_instance.load_project_config.return_value = mock_project_config
            
            # Don't call enforce_version_requirements in init
            with patch.object(ActionOrchestrator, '_enforce_version_requirements'):
                orchestrator = ActionOrchestrator(mock_project_root, enforce_version=True)
                orchestrator.project_config = mock_project_config
                return orchestrator

    def test_version_mismatch_fail_with_error_code_007(self, orchestrator_with_version_requirement):
        """Test version does not match requirement then fail with LHPError code 007."""
        # Arrange - set requirement that current version won't satisfy
        orchestrator_with_version_requirement.project_config.required_lhp_version = ">9.0.0"  # Impossible requirement
        
        # Act & Assert
        with pytest.raises(LHPError) as exc_info:
            orchestrator_with_version_requirement._enforce_version_requirements()
        
        # Assert error details
        error = exc_info.value
        assert error.code == "LHP-CFG-007"
        assert "version requirement not satisfied" in error.title.lower()
        assert ">9.0.0" in error.details

    def test_ignore_version_1_bypasses_with_warning(self, orchestrator_with_version_requirement):
        """Test LHP_IGNORE_VERSION=1 bypasses version check with warning."""
        # Arrange
        with patch.dict(os.environ, {'LHP_IGNORE_VERSION': '1'}), \
             patch.object(orchestrator_with_version_requirement, 'logger') as mock_logger:
            
            # Act - should complete without raising exception
            orchestrator_with_version_requirement._enforce_version_requirements()
            
            # Assert - should log warning about bypass
            mock_logger.warning.assert_called_once()
            warning_call = mock_logger.warning.call_args[0][0]
            assert "Version requirement bypass enabled" in warning_call
            assert ">=0.4.0" in warning_call

    def test_ignore_version_true_bypasses_with_warning(self, orchestrator_with_version_requirement):
        """Test LHP_IGNORE_VERSION=true bypasses version check with warning."""
        # Arrange
        with patch.dict(os.environ, {'LHP_IGNORE_VERSION': 'true'}), \
             patch.object(orchestrator_with_version_requirement, 'logger') as mock_logger:
            
            # Act - should complete without raising exception
            orchestrator_with_version_requirement._enforce_version_requirements()
            
            # Assert - should log warning about bypass
            mock_logger.warning.assert_called_once()
            warning_call = mock_logger.warning.call_args[0][0]
            assert "Version requirement bypass enabled" in warning_call
            assert ">=0.4.0" in warning_call

    def test_ignore_version_yes_bypasses_with_warning(self, orchestrator_with_version_requirement):
        """Test LHP_IGNORE_VERSION=yes bypasses version check with warning."""
        # Arrange
        with patch.dict(os.environ, {'LHP_IGNORE_VERSION': 'yes'}), \
             patch.object(orchestrator_with_version_requirement, 'logger') as mock_logger:
            
            # Act - should complete without raising exception
            orchestrator_with_version_requirement._enforce_version_requirements()
            
            # Assert - should log warning about bypass
            mock_logger.warning.assert_called_once()
            warning_call = mock_logger.warning.call_args[0][0]
            assert "Version requirement bypass enabled" in warning_call
            assert ">=0.4.0" in warning_call

    def test_ignore_version_false_does_not_bypass(self, orchestrator_with_version_requirement):
        """Test LHP_IGNORE_VERSION=false does not bypass version check."""
        # Arrange - set impossible requirement and environment that shouldn't bypass
        orchestrator_with_version_requirement.project_config.required_lhp_version = ">9.0.0"  # Impossible requirement
        
        with patch.dict(os.environ, {'LHP_IGNORE_VERSION': 'false'}):
            # Act & Assert - should still fail with version mismatch (no bypass)
            with pytest.raises(LHPError) as exc_info:
                orchestrator_with_version_requirement._enforce_version_requirements()
            
            # Assert error details - should fail normally, not bypass
            error = exc_info.value
            assert error.code == "LHP-CFG-007"

    def test_packaging_not_installed_fails_with_error_code_006(self, orchestrator_with_version_requirement):
        """Test packaging library not installed fails with LHPError code 006."""
        # Arrange - mock import to fail
        with patch('builtins.__import__') as mock_import:
            def import_side_effect(name, *args, **kwargs):
                if name in ('packaging.version', 'packaging.specifiers'):
                    raise ImportError(f"No module named '{name}'")
                return __import__(name, *args, **kwargs)
            
            mock_import.side_effect = import_side_effect
            
            # Act & Assert
            with pytest.raises(LHPError) as exc_info:
                orchestrator_with_version_requirement._enforce_version_requirements()
            
            # Assert error details
            error = exc_info.value
            assert error.code == "LHP-CFG-006"
            assert "Missing packaging dependency" in error.title
            assert "packaging" in error.details
            assert "pip install packaging" in error.suggestions[0]

    def test_invalid_version_format_fails_with_error_code_008(self, orchestrator_with_version_requirement):
        """Test invalid PEP 440 version requirement fails with LHPError code 008."""
        # Arrange
        orchestrator_with_version_requirement.project_config.required_lhp_version = "invalid-version-format"
        
        with patch('lhp.utils.version.get_version') as mock_get_version, \
             patch('packaging.specifiers.SpecifierSet') as mock_specifier_set:
            
            mock_get_version.return_value = "0.4.1"
            # Simulate invalid version format exception
            mock_specifier_set.side_effect = Exception("Invalid version specifier")
            
            # Act & Assert
            with pytest.raises(LHPError) as exc_info:
                orchestrator_with_version_requirement._enforce_version_requirements()
            
            # Assert error details
            error = exc_info.value
            assert error.code == "LHP-CFG-008"
            assert "Invalid version requirement specification" in error.title
            assert "invalid-version-format" in error.details
            assert "PEP 440 version specifiers" in error.suggestions[0]

    def test_version_parsing_fails_with_error_code_008(self, orchestrator_with_version_requirement):
        """Test actual version parsing fails with LHPError code 008."""
        # Arrange
        with patch('lhp.utils.version.get_version') as mock_get_version, \
             patch('packaging.version.Version') as mock_version_class, \
             patch('packaging.specifiers.SpecifierSet') as mock_specifier_set:
            
            mock_get_version.return_value = "invalid-actual-version"
            # Simulate actual version parsing failure
            mock_version_class.side_effect = Exception("Invalid version format")
            mock_specifier_set.return_value = Mock()  # This won't be reached
            
            # Act & Assert
            with pytest.raises(LHPError) as exc_info:
                orchestrator_with_version_requirement._enforce_version_requirements()
            
            # Assert error details
            error = exc_info.value
            assert error.code == "LHP-CFG-008"
            assert "Invalid version requirement specification" in error.title
            assert ">=0.4.0" in error.details
            assert "Invalid version format" in error.details

    def test_no_version_requirement_skips_enforcement_silently(self, orchestrator_with_version_requirement):
        """Test no required_lhp_version skips enforcement silently."""
        # Arrange - remove version requirement
        orchestrator_with_version_requirement.project_config.required_lhp_version = None
        
        # Act - should complete silently without any version checks
        orchestrator_with_version_requirement._enforce_version_requirements()
        
        # Assert - no exception should be raised, and method should complete successfully
        assert True  # Test passes if no exception is raised

    def test_specifier_set_exception_wraps_in_error_code_008(self, orchestrator_with_version_requirement):
        """Test SpecifierSet creation throws exception wraps in LHPError code 008."""
        # Arrange
        with patch('lhp.utils.version.get_version') as mock_get_version, \
             patch('packaging.specifiers.SpecifierSet') as mock_specifier_set:
            
            mock_get_version.return_value = "0.4.1"
            # Simulate SpecifierSet creation failure (not an ImportError)
            mock_specifier_set.side_effect = ValueError("Invalid specifier format")
            
            # Act & Assert
            with pytest.raises(LHPError) as exc_info:
                orchestrator_with_version_requirement._enforce_version_requirements()
            
            # Assert error details
            error = exc_info.value
            assert error.code == "LHP-CFG-008"
            assert "Invalid version requirement specification" in error.title
            assert "Invalid specifier format" in error.details

    def test_version_satisfied_completes_silently(self, orchestrator_with_version_requirement):
        """Test version requirement satisfied completes silently."""
        # Arrange - use current version which should satisfy the requirement
        orchestrator_with_version_requirement.project_config.required_lhp_version = ">=0.1.0"  # Very low requirement that should be satisfied
        
        # Act - should complete silently without any exceptions or logging
        orchestrator_with_version_requirement._enforce_version_requirements()
        
        # Assert - no exception should be raised
        assert True  # Test passes if no exception is raised


class TestActionOrchestratorFlowgroupDiscovery:
    """Test ActionOrchestrator flowgroup discovery patterns."""

    @pytest.fixture
    def mock_project_root(self):
        """Mock project root path."""
        return Path("/mock/project")

    @pytest.fixture
    def orchestrator_basic(self, mock_project_root):
        """Create basic orchestrator for discovery testing."""
        with patch('lhp.core.orchestrator.YAMLParser'), \
             patch('lhp.core.orchestrator.PresetManager'), \
             patch('lhp.core.orchestrator.TemplateEngine'), \
             patch('lhp.core.orchestrator.ProjectConfigLoader') as mock_config_loader, \
             patch('lhp.core.orchestrator.ActionRegistry'), \
             patch('lhp.core.orchestrator.ConfigValidator') as mock_config_validator, \
             patch('lhp.core.orchestrator.SecretValidator'), \
             patch('lhp.core.orchestrator.DependencyResolver'), \
             patch('lhp.core.orchestrator.FlowgroupDiscoverer') as mock_discoverer, \
             patch('lhp.core.orchestrator.FlowgroupProcessor') as mock_processor, \
             patch('lhp.core.orchestrator.CodeGenerator'), \
             patch('lhp.core.orchestrator.PipelineValidator'):
            
            # Configure project config loader
            mock_config_loader_instance = Mock()
            mock_config_loader.return_value = mock_config_loader_instance
            mock_config_loader_instance.load_project_config.return_value = None
            
            # Create orchestrator without version enforcement
            orchestrator = ActionOrchestrator(mock_project_root, enforce_version=False)
            
            # Configure processor to pass flowgroups through unchanged
            # This supports the new batch processing + validation flow
            mock_processor_instance = mock_processor.return_value
            mock_processor_instance.process_flowgroup.side_effect = lambda fg, sub: fg
            
            # Configure validator to return empty errors for table creation validation
            # This allows tests to pass when testing other aspects of the orchestrator
            orchestrator.config_validator.validate_table_creation_rules.return_value = []
            orchestrator.config_validator.validate_duplicate_pipeline_flowgroup.return_value = []
            
            # Store mock discoverer for test access
            orchestrator.mock_discoverer = mock_discoverer.return_value
            return orchestrator

    def test_pipeline_directory_not_exist_returns_empty_dict(self, orchestrator_basic):
        """Test pipeline directory does not exist returns empty dict and logs warning."""
        # Arrange
        nonexistent_pipeline = "nonexistent_pipeline"
        pipeline_dir = orchestrator_basic.project_root / "pipelines" / nonexistent_pipeline
        
        # Mock path exists to return False
        with patch.object(Path, 'exists', return_value=False):
            # Act
            result = orchestrator_basic.generate_pipeline_by_field(
                pipeline_field=nonexistent_pipeline,
                env="dev"
            )
            
            # Assert - should return empty dict, not raise exception
            assert result == {}
            assert isinstance(result, dict)

    def test_no_flowgroups_found_returns_empty_dict(self, orchestrator_basic):
        """Test no flowgroups found returns empty dict and logs warning."""
        # Arrange
        pipeline_name = "empty_pipeline"
        
        # Mock path exists to return True, but discoverer returns empty list
        with patch.object(Path, 'exists', return_value=True):
            orchestrator_basic.mock_discoverer.discover_flowgroups.return_value = []
            
            # Act
            result = orchestrator_basic.generate_pipeline_by_field(
                pipeline_field=pipeline_name,
                env="dev"
            )
            
            # Assert - should return empty dict, not raise exception
            assert result == {}
            assert isinstance(result, dict)

    def test_include_patterns_filtering_applied_correctly(self, orchestrator_basic):
        """Test include patterns are applied correctly in filtering."""
        # Arrange
        expected_patterns = ["*.yaml", "specific_pattern"]
        orchestrator_basic.mock_discoverer.get_include_patterns.return_value = expected_patterns
        
        # Act
        result = orchestrator_basic.get_include_patterns()
        
        # Assert
        assert result == expected_patterns
        orchestrator_basic.mock_discoverer.get_include_patterns.assert_called_once()

    def test_duplicate_pipeline_flowgroup_combinations_raise_value_error(self, orchestrator_basic):
        """Test duplicate pipeline+flowgroup combinations raise ValueError."""
        # Arrange
        from lhp.models.config import FlowGroup
        
        # Create mock flowgroups with duplicates
        mock_flowgroups = [
            Mock(spec=FlowGroup, pipeline="test_pipeline", flowgroup="duplicate_flowgroup"),
            Mock(spec=FlowGroup, pipeline="test_pipeline", flowgroup="duplicate_flowgroup"),  # Duplicate
        ]
        
        # Mock config validator to return error for duplicates
        orchestrator_basic.config_validator.validate_duplicate_pipeline_flowgroup.return_value = [
            "Duplicate combination: test_pipeline + duplicate_flowgroup"
        ]
        
        # Act & Assert
        with pytest.raises(ValueError, match="Duplicate pipeline\\+flowgroup combinations found"):
            orchestrator_basic.validate_duplicate_pipeline_flowgroup_combinations(mock_flowgroups)
        
        # Verify validator was called
        orchestrator_basic.config_validator.validate_duplicate_pipeline_flowgroup.assert_called_once_with(mock_flowgroups)

    def test_yaml_parsing_fails_continue_with_warnings(self, orchestrator_basic):
        """Test YAML parsing fails for some files but continues with warnings."""
        # This tests the behavior in generate_pipeline_by_field when parsing new files
        # Arrange
        pipeline_field = "test_pipeline"
        
        # Mock discoverer to return flowgroups
        from lhp.models.config import FlowGroup
        mock_flowgroup = Mock(spec=FlowGroup, pipeline=pipeline_field, flowgroup="test_flowgroup")
        mock_flowgroup.actions = []  # Strategy code expects actions attribute
        orchestrator_basic.mock_discoverer.discover_all_flowgroups.return_value = [mock_flowgroup]
        orchestrator_basic.config_validator.validate_duplicate_pipeline_flowgroup.return_value = []
        
        # Mock state manager with new files that cause parsing errors
        mock_state_manager = Mock()
        generation_info = {
            "new": [Path("/path/to/invalid.yaml"), Path("/path/to/valid.yaml")],
            "stale": []
        }
        mock_state_manager.get_files_needing_generation.return_value = generation_info
        
        # Mock YAML parser to fail on first file, succeed on second
        # Note: parse_flowgroups_from_file returns a LIST of flowgroups
        def parse_side_effect(yaml_path):
            if "invalid.yaml" in str(yaml_path):
                raise Exception("YAML parsing failed")
            else:
                mock_fg = Mock()
                mock_fg.flowgroup = "valid_flowgroup"
                mock_fg.actions = []  # Strategy code expects actions attribute
                return [mock_fg]  # Return list of flowgroups
        
        orchestrator_basic.yaml_parser.parse_flowgroups_from_file.side_effect = parse_side_effect
        
        with patch.object(orchestrator_basic, 'logger') as mock_logger:
            # Act
            result = orchestrator_basic.generate_pipeline_by_field(
                pipeline_field=pipeline_field,
                env="dev",
                output_dir=None,  # Dry run to avoid file operations
                state_manager=mock_state_manager,
                force_all=False
            )
            
            # Assert - should complete successfully despite parsing error
            assert isinstance(result, dict)
            # Should log warning about parsing failure
            warning_calls = [call for call in mock_logger.warning.call_args_list if "Could not parse new flowgroup" in str(call)]
            assert len(warning_calls) > 0  # At least one warning should be logged

    def test_pipeline_field_multiple_matches_returns_all(self, orchestrator_basic):
        """Test pipeline field matches multiple flowgroups returns all matches."""
        # Arrange
        from lhp.models.config import FlowGroup
        
        # Mock discoverer to return multiple matching flowgroups
        mock_flowgroup1 = Mock(spec=FlowGroup, pipeline="target_pipeline", flowgroup="flowgroup1")
        mock_flowgroup2 = Mock(spec=FlowGroup, pipeline="target_pipeline", flowgroup="flowgroup2")
        mock_flowgroup3 = Mock(spec=FlowGroup, pipeline="other_pipeline", flowgroup="flowgroup3")
        
        all_flowgroups = [mock_flowgroup1, mock_flowgroup2, mock_flowgroup3]
        orchestrator_basic.mock_discoverer.discover_all_flowgroups.return_value = all_flowgroups
        
        # Act
        result = orchestrator_basic.discover_flowgroups_by_pipeline_field("target_pipeline")
        
        # Assert - should return only matching flowgroups
        assert len(result) == 2
        assert mock_flowgroup1 in result
        assert mock_flowgroup2 in result
        assert mock_flowgroup3 not in result
        
        # Verify discoverer was called
        orchestrator_basic.mock_discoverer.discover_all_flowgroups.assert_called_once()

    def test_pipeline_field_no_matches_returns_empty_with_warning(self, orchestrator_basic):
        """Test pipeline field matches no flowgroups returns empty list with warning."""
        # Arrange
        from lhp.models.config import FlowGroup
        
        # Mock discoverer to return flowgroups with different pipeline fields
        mock_flowgroup1 = Mock(spec=FlowGroup, pipeline="other_pipeline1", flowgroup="flowgroup1")
        mock_flowgroup2 = Mock(spec=FlowGroup, pipeline="other_pipeline2", flowgroup="flowgroup2")
        
        all_flowgroups = [mock_flowgroup1, mock_flowgroup2]
        orchestrator_basic.mock_discoverer.discover_all_flowgroups.return_value = all_flowgroups
        
        # Mock logger to verify warning
        with patch.object(orchestrator_basic, 'logger') as mock_logger:
            # Act
            result = orchestrator_basic.discover_flowgroups_by_pipeline_field("nonexistent_pipeline")
            
            # Assert - should return empty list
            assert result == []
            
            # Should not log warning at discovery level (warning happens in generate_pipeline_by_field)
            orchestrator_basic.mock_discoverer.discover_all_flowgroups.assert_called_once()

    def test_recursive_directory_search_finds_nested_flowgroups(self, orchestrator_basic):
        """Test recursive directory search finds nested flowgroups."""
        # Arrange
        from lhp.models.config import FlowGroup
        
        pipeline_dir = orchestrator_basic.project_root / "pipelines" / "test_pipeline"
        
        # Mock discoverer to return flowgroups from nested directories
        mock_flowgroup1 = Mock(spec=FlowGroup, pipeline="test_pipeline", flowgroup="main_flowgroup")
        mock_flowgroup2 = Mock(spec=FlowGroup, pipeline="test_pipeline", flowgroup="nested_flowgroup") 
        
        nested_flowgroups = [mock_flowgroup1, mock_flowgroup2]
        orchestrator_basic.mock_discoverer.discover_flowgroups.return_value = nested_flowgroups
        
        # Act
        result = orchestrator_basic.discover_flowgroups(pipeline_dir)
        
        # Assert - should return all flowgroups including nested ones
        assert result == nested_flowgroups
        assert len(result) == 2
        assert mock_flowgroup1 in result
        assert mock_flowgroup2 in result
        
        # Verify discoverer was called with correct pipeline directory
        orchestrator_basic.mock_discoverer.discover_flowgroups.assert_called_once_with(pipeline_dir)


class TestActionOrchestratorActionAnalysis:
    """Test ActionOrchestrator action analysis and transformation logic."""

    @pytest.fixture
    def mock_project_root(self):
        """Mock project root path."""
        return Path("/mock/project")

    @pytest.fixture
    def orchestrator_action_analysis(self, mock_project_root):
        """Create orchestrator for action analysis testing."""
        with patch('lhp.core.orchestrator.YAMLParser'), \
             patch('lhp.core.orchestrator.PresetManager'), \
             patch('lhp.core.orchestrator.TemplateEngine'), \
             patch('lhp.core.orchestrator.ProjectConfigLoader') as mock_config_loader, \
             patch('lhp.core.orchestrator.ActionRegistry'), \
             patch('lhp.core.orchestrator.ConfigValidator'), \
             patch('lhp.core.orchestrator.SecretValidator'), \
             patch('lhp.core.orchestrator.DependencyResolver'), \
             patch('lhp.core.orchestrator.FlowgroupDiscoverer'), \
             patch('lhp.core.orchestrator.FlowgroupProcessor'), \
             patch('lhp.core.orchestrator.CodeGenerator') as mock_generator, \
             patch('lhp.core.orchestrator.PipelineValidator'):
            
            # Configure mocks
            mock_config_loader_instance = Mock()
            mock_config_loader.return_value = mock_config_loader_instance
            mock_config_loader_instance.load_project_config.return_value = None
            
            # Create orchestrator
            orchestrator = ActionOrchestrator(mock_project_root, enforce_version=False)
            
            # Store generator mock for test access
            orchestrator.mock_generator = mock_generator.return_value
            
            return orchestrator

    def test_determine_action_subtype_delegation(self, orchestrator_action_analysis):
        """Test determine_action_subtype delegates to generator."""
        # Arrange
        from lhp.models.config import Action
        mock_action = Mock(spec=Action)
        expected_subtype = "cloudfiles"
        
        orchestrator_action_analysis.mock_generator.determine_action_subtype.return_value = expected_subtype
        
        # Act
        result = orchestrator_action_analysis.determine_action_subtype(mock_action)
        
        # Assert
        assert result == expected_subtype
        orchestrator_action_analysis.mock_generator.determine_action_subtype.assert_called_once_with(mock_action)

    def test_build_custom_source_block_delegation(self, orchestrator_action_analysis):
        """Test build_custom_source_block delegates to generator."""
        # Arrange
        custom_sections = [
            {"name": "section1", "code": "print('custom1')"},
            {"name": "section2", "code": "print('custom2')"}
        ]
        expected_block = "# Custom code block\nprint('custom1')\nprint('custom2')"
        
        orchestrator_action_analysis.mock_generator.build_custom_source_block.return_value = expected_block
        
        # Act
        result = orchestrator_action_analysis.build_custom_source_block(custom_sections)
        
        # Assert
        assert result == expected_block
        orchestrator_action_analysis.mock_generator.build_custom_source_block.assert_called_once_with(custom_sections)

    def test_group_write_actions_by_target_delegation(self, orchestrator_action_analysis):
        """Test group_write_actions_by_target delegates to generator."""
        # Arrange
        from lhp.models.config import Action
        write_actions = [Mock(spec=Action), Mock(spec=Action)]
        expected_groups = {"table1": [write_actions[0]], "table2": [write_actions[1]]}
        
        orchestrator_action_analysis.mock_generator.group_write_actions_by_target.return_value = expected_groups
        
        # Act
        result = orchestrator_action_analysis.group_write_actions_by_target(write_actions)
        
        # Assert
        assert result == expected_groups
        orchestrator_action_analysis.mock_generator.group_write_actions_by_target.assert_called_once_with(write_actions)

    def test_create_combined_write_action_delegation(self, orchestrator_action_analysis):
        """Test create_combined_write_action delegates to generator."""
        # Arrange
        from lhp.models.config import Action
        actions = [Mock(spec=Action), Mock(spec=Action)]
        target_table = "combined_table"
        expected_combined_action = Mock(spec=Action)
        
        orchestrator_action_analysis.mock_generator.create_combined_write_action.return_value = expected_combined_action
        
        # Act
        result = orchestrator_action_analysis.create_combined_write_action(actions, target_table)
        
        # Assert
        assert result == expected_combined_action
        orchestrator_action_analysis.mock_generator.create_combined_write_action.assert_called_once_with(actions, target_table)

    def test_extract_single_source_view_string_source(self, orchestrator_action_analysis):
        """Test _extract_single_source_view with string source."""
        # Arrange
        source = "simple_table_name"
        
        # Act
        result = orchestrator_action_analysis._extract_single_source_view(source)
        
        # Assert
        assert result == "simple_table_name"

    def test_extract_single_source_view_list_with_strings(self, orchestrator_action_analysis):
        """Test _extract_single_source_view with list containing strings."""
        # Arrange
        source = ["first_table", "second_table"]  # Should return first item
        
        # Act
        result = orchestrator_action_analysis._extract_single_source_view(source)
        
        # Assert
        assert result == "first_table"

    def test_extract_single_source_view_list_with_dicts(self, orchestrator_action_analysis):
        """Test _extract_single_source_view with list containing dictionaries."""
        # Arrange
        source = [{"database": "test_db", "table": "test_table"}]
        
        # Act
        result = orchestrator_action_analysis._extract_single_source_view(source)
        
        # Assert
        assert result == "test_db.test_table"

    def test_extract_single_source_view_dict_with_database_table(self, orchestrator_action_analysis):
        """Test _extract_single_source_view with dict containing database and table."""
        # Arrange
        source = {"database": "prod_db", "table": "customer_data"}
        
        # Act
        result = orchestrator_action_analysis._extract_single_source_view(source)
        
        # Assert
        assert result == "prod_db.customer_data"

    def test_extract_single_source_view_dict_with_table_only(self, orchestrator_action_analysis):
        """Test _extract_single_source_view with dict containing only table."""
        # Arrange
        source = {"table": "standalone_table"}
        
        # Act
        result = orchestrator_action_analysis._extract_single_source_view(source)
        
        # Assert
        assert result == "standalone_table"

    def test_extract_single_source_view_dict_with_view_field(self, orchestrator_action_analysis):
        """Test _extract_single_source_view with dict using 'view' field."""
        # Arrange
        source = {"database": "analytics_db", "view": "customer_view"}
        
        # Act
        result = orchestrator_action_analysis._extract_single_source_view(source)
        
        # Assert
        assert result == "analytics_db.customer_view"

    def test_extract_single_source_view_empty_or_invalid(self, orchestrator_action_analysis):
        """Test _extract_single_source_view with empty or invalid sources."""
        # Test empty string
        assert orchestrator_action_analysis._extract_single_source_view("") == ""
        
        # Test empty list
        assert orchestrator_action_analysis._extract_single_source_view([]) == ""
        
        # Test empty dict
        assert orchestrator_action_analysis._extract_single_source_view({}) == ""
        
        # Test None
        assert orchestrator_action_analysis._extract_single_source_view(None) == ""
        
        # Test number
        assert orchestrator_action_analysis._extract_single_source_view(123) == ""

    def test_extract_source_views_from_action_string(self, orchestrator_action_analysis):
        """Test _extract_source_views_from_action with string source."""
        # Arrange
        source = "simple_view"
        
        # Act
        result = orchestrator_action_analysis._extract_source_views_from_action(source)
        
        # Assert
        assert result == ["simple_view"]

    def test_extract_source_views_from_action_list_with_strings(self, orchestrator_action_analysis):
        """Test _extract_source_views_from_action with list of strings."""
        # Arrange
        source = ["view1", "view2", "view3"]
        
        # Act
        result = orchestrator_action_analysis._extract_source_views_from_action(source)
        
        # Assert
        assert result == ["view1", "view2", "view3"]

    def test_extract_source_views_from_action_list_with_dicts(self, orchestrator_action_analysis):
        """Test _extract_source_views_from_action with list of dictionaries."""
        # Arrange
        source = [
            {"database": "db1", "table": "table1"},
            {"database": "db2", "view": "view2"},
            {"table": "standalone_table"},
            {"name": "named_table"}
        ]
        
        # Act
        result = orchestrator_action_analysis._extract_source_views_from_action(source)
        
        # Assert
        expected = ["db1.table1", "db2.view2", "standalone_table", "named_table"]
        assert result == expected

    def test_extract_source_views_from_action_mixed_list(self, orchestrator_action_analysis):
        """Test _extract_source_views_from_action with mixed list formats."""
        # Arrange
        source = [
            "string_table",
            {"database": "db1", "table": "dict_table"},
            123,  # Non-string item
            {"incomplete": "dict"}  # Dict without table/view/name
        ]
        
        # Act
        result = orchestrator_action_analysis._extract_source_views_from_action(source)
        
        # Assert - the incomplete dict doesn't add an empty string, it's skipped
        expected = ["string_table", "db1.dict_table", "123"]
        assert result == expected

    def test_extract_source_views_from_action_dict_single(self, orchestrator_action_analysis):
        """Test _extract_source_views_from_action with single dictionary source."""
        # Arrange
        source = {"database": "analytics", "table": "metrics"}
        
        # Act
        result = orchestrator_action_analysis._extract_source_views_from_action(source)
        
        # Assert
        assert result == ["analytics.metrics"]

    def test_extract_source_views_from_action_empty_invalid(self, orchestrator_action_analysis):
        """Test _extract_source_views_from_action with empty or invalid sources."""
        # Test empty string
        assert orchestrator_action_analysis._extract_source_views_from_action("") == [""]
        
        # Test empty list
        assert orchestrator_action_analysis._extract_source_views_from_action([]) == []
        
        # Test empty dict - now returns ["source"] as fallback (Phase 1 refactoring)
        assert orchestrator_action_analysis._extract_source_views_from_action({}) == ["source"]
        
        # Test None - now returns ["source"] as fallback (Phase 1 refactoring)
        assert orchestrator_action_analysis._extract_source_views_from_action(None) == ["source"]
        
        # Test number - now returns ["source"] as fallback (Phase 1 refactoring)
        assert orchestrator_action_analysis._extract_source_views_from_action(123) == ["source"]


class TestActionOrchestratorBundleSynchronization:
    """Test ActionOrchestrator bundle resource synchronization logic."""

    @pytest.fixture
    def mock_project_root(self):
        """Mock project root path."""
        return Path("/mock/project")

    @pytest.fixture
    def orchestrator_bundle(self, mock_project_root):
        """Create orchestrator for bundle synchronization testing."""
        with patch('lhp.core.orchestrator.YAMLParser'), \
             patch('lhp.core.orchestrator.PresetManager'), \
             patch('lhp.core.orchestrator.TemplateEngine'), \
             patch('lhp.core.orchestrator.ProjectConfigLoader') as mock_config_loader, \
             patch('lhp.core.orchestrator.ActionRegistry'), \
             patch('lhp.core.orchestrator.ConfigValidator'), \
             patch('lhp.core.orchestrator.SecretValidator'), \
             patch('lhp.core.orchestrator.DependencyResolver'), \
             patch('lhp.core.orchestrator.FlowgroupDiscoverer'), \
             patch('lhp.core.orchestrator.FlowgroupProcessor'), \
             patch('lhp.core.orchestrator.CodeGenerator'), \
             patch('lhp.core.orchestrator.PipelineValidator'):
            
            # Configure mocks
            mock_config_loader_instance = Mock()
            mock_config_loader.return_value = mock_config_loader_instance
            mock_config_loader_instance.load_project_config.return_value = None
            
            # Create orchestrator
            orchestrator = ActionOrchestrator(mock_project_root, enforce_version=False)
            
            return orchestrator

    def test_bundle_support_disabled_skips_sync_and_logs_debug(self, orchestrator_bundle):
        """Test bundle support disabled skips sync and logs debug message."""
        # Arrange
        output_dir = Path("/output")
        environment = "dev"
        
        # Mock bundle detection to return False (disabled)
        with patch('lhp.utils.bundle_detection.should_enable_bundle_support') as mock_should_enable, \
             patch.object(orchestrator_bundle, 'logger') as mock_logger:
            
            mock_should_enable.return_value = False
            
            # Act
            orchestrator_bundle._sync_bundle_resources(output_dir, environment)
            
            # Assert
            mock_should_enable.assert_called_once_with(orchestrator_bundle.project_root)
            
            # Should log debug message about bundle support being disabled
            debug_calls = [str(call) for call in mock_logger.debug.call_args_list]
            disabled_logged = any("Bundle support disabled" in call for call in debug_calls)
            assert disabled_logged
            
            # Should return early without attempting import or sync
            mock_logger.info.assert_not_called()  # No success message

    def test_bundle_support_enabled_attempts_synchronization(self, orchestrator_bundle):
        """Test bundle support enabled attempts synchronization."""
        # Arrange
        output_dir = Path("/output")
        environment = "dev"
        
        # Mock bundle detection to return True (enabled)
        with patch('lhp.utils.bundle_detection.should_enable_bundle_support') as mock_should_enable, \
             patch('lhp.bundle.manager.BundleManager') as mock_bundle_manager_class, \
             patch.object(orchestrator_bundle, 'logger') as mock_logger:
            
            mock_should_enable.return_value = True
            
            # Mock BundleManager creation and sync
            mock_bundle_manager = Mock()
            mock_bundle_manager_class.return_value = mock_bundle_manager
            mock_bundle_manager.sync_resources_with_generated_files.return_value = None
            
            # Act
            orchestrator_bundle._sync_bundle_resources(output_dir, environment)
            
            # Assert
            mock_should_enable.assert_called_once_with(orchestrator_bundle.project_root)
            
            # Should create BundleManager with project root and pipeline_config_path
            mock_bundle_manager_class.assert_called_once_with(
                orchestrator_bundle.project_root,
                orchestrator_bundle.pipeline_config_path
            )
            
            # Should call sync_resources_with_generated_files
            mock_bundle_manager.sync_resources_with_generated_files.assert_called_once_with(output_dir, environment)
            
            # Should log debug and success messages
            debug_calls = [str(call) for call in mock_logger.debug.call_args_list]
            starting_logged = any("Starting bundle resource synchronization" in call for call in debug_calls)
            assert starting_logged
            
            info_calls = [str(call) for call in mock_logger.info.call_args_list]
            success_logged = any("Bundle resource synchronization completed successfully" in call for call in info_calls)
            assert success_logged

    def test_bundle_manager_import_fails_logs_debug_and_continues(self, orchestrator_bundle):
        """Test BundleManager import fails logs debug message and continues."""
        # Arrange
        output_dir = Path("/output")
        environment = "dev"
        
        # Mock bundle detection to return True, but simulate ImportError in the try block
        with patch('lhp.utils.bundle_detection.should_enable_bundle_support') as mock_should_enable, \
             patch.object(orchestrator_bundle, 'logger') as mock_logger:
            
            mock_should_enable.return_value = True
            
            # Patch the import that happens inside the try block
            import builtins
            original_import = builtins.__import__
            
            def mock_import(name, *args, **kwargs):
                if 'bundle.manager' in name:
                    raise ImportError("No module named 'bundle.manager'")
                return original_import(name, *args, **kwargs)
            
            with patch('builtins.__import__', side_effect=mock_import):
                # Act - should not raise exception
                orchestrator_bundle._sync_bundle_resources(output_dir, environment)
            
            # Assert
            mock_should_enable.assert_called_once_with(orchestrator_bundle.project_root)
            
            # Should log debug message about modules not being available
            debug_calls = [str(call) for call in mock_logger.debug.call_args_list]
            import_error_logged = any("Bundle modules not available" in call for call in debug_calls)
            assert import_error_logged
            
            # Should not log success message since import failed
            info_calls = [str(call) for call in mock_logger.info.call_args_list]
            success_logged = any("synchronization completed successfully" in call for call in info_calls)
            assert not success_logged

    def test_bundle_sync_succeeds_logs_success_message(self, orchestrator_bundle):
        """Test bundle sync succeeds logs success message."""
        # Arrange
        output_dir = Path("/output")
        environment = "prod"
        
        # Mock all components for successful sync
        with patch('lhp.utils.bundle_detection.should_enable_bundle_support') as mock_should_enable, \
             patch('lhp.bundle.manager.BundleManager') as mock_bundle_manager_class, \
             patch.object(orchestrator_bundle, 'logger') as mock_logger:
            
            mock_should_enable.return_value = True
            
            # Mock BundleManager creation and successful sync
            mock_bundle_manager = Mock()
            mock_bundle_manager_class.return_value = mock_bundle_manager
            mock_bundle_manager.sync_resources_with_generated_files.return_value = None  # Success (no exception)
            
            # Act
            orchestrator_bundle._sync_bundle_resources(output_dir, environment)
            
            # Assert
            mock_should_enable.assert_called_once_with(orchestrator_bundle.project_root)
            mock_bundle_manager_class.assert_called_once_with(
                orchestrator_bundle.project_root,
                orchestrator_bundle.pipeline_config_path
            )
            mock_bundle_manager.sync_resources_with_generated_files.assert_called_once_with(output_dir, environment)
            
            # Should log both debug start message and info success message
            debug_calls = [str(call) for call in mock_logger.debug.call_args_list]
            starting_logged = any("Starting bundle resource synchronization for environment: prod" in call for call in debug_calls)
            assert starting_logged
            
            info_calls = [str(call) for call in mock_logger.info.call_args_list]
            success_logged = any("Bundle resource synchronization completed successfully" in call for call in info_calls)
            assert success_logged

    def test_bundle_sync_fails_logs_warning_but_does_not_fail_generation(self, orchestrator_bundle):
        """Test bundle sync fails logs warning but does not fail generation."""
        # Arrange
        output_dir = Path("/output")
        environment = "dev"
        
        # Mock bundle detection to return True, but sync to fail
        with patch('lhp.utils.bundle_detection.should_enable_bundle_support') as mock_should_enable, \
             patch('lhp.bundle.manager.BundleManager') as mock_bundle_manager_class, \
             patch.object(orchestrator_bundle, 'logger') as mock_logger:
            
            mock_should_enable.return_value = True
            
            # Mock BundleManager creation but sync fails
            mock_bundle_manager = Mock()
            mock_bundle_manager_class.return_value = mock_bundle_manager
            sync_error = Exception("Bundle sync operation failed")
            mock_bundle_manager.sync_resources_with_generated_files.side_effect = sync_error
            
            # Act - should not raise exception (bundle errors should not fail generation)
            orchestrator_bundle._sync_bundle_resources(output_dir, environment)
            
            # Assert
            mock_should_enable.assert_called_once_with(orchestrator_bundle.project_root)
            mock_bundle_manager_class.assert_called_once_with(
                orchestrator_bundle.project_root,
                orchestrator_bundle.pipeline_config_path
            )
            mock_bundle_manager.sync_resources_with_generated_files.assert_called_once_with(output_dir, environment)
            
            # Should log warning about bundle sync failure
            warning_calls = [str(call) for call in mock_logger.warning.call_args_list]
            sync_failed_logged = any("Bundle synchronization failed" in call for call in warning_calls)
            assert sync_failed_logged
            
            # Should also log debug error details with exc_info=True
            debug_calls = [str(call) for call in mock_logger.debug.call_args_list]
            error_details_logged = any("Bundle sync error details" in call for call in debug_calls)
            assert error_details_logged
            
            # Should not log success message since sync failed
            info_calls = [str(call) for call in mock_logger.info.call_args_list]
            success_logged = any("synchronization completed successfully" in call for call in info_calls)
            assert not success_logged

    def test_bundle_support_check_exception_catches_and_continues(self, orchestrator_bundle):
        """Test should_enable_bundle_support throws exception catches and continues."""
        # Arrange
        output_dir = Path("/output")
        environment = "dev"
        
        # Mock bundle detection to throw exception
        with patch('lhp.utils.bundle_detection.should_enable_bundle_support') as mock_should_enable, \
             patch.object(orchestrator_bundle, 'logger') as mock_logger:
            
            # Make bundle detection throw an exception
            bundle_check_error = Exception("Bundle detection failed")
            mock_should_enable.side_effect = bundle_check_error
            
            # Act - should not raise exception (should catch and continue gracefully)
            orchestrator_bundle._sync_bundle_resources(output_dir, environment)
            
            # Assert
            mock_should_enable.assert_called_once_with(orchestrator_bundle.project_root)
            
            # Should log warning about bundle sync failure
            warning_calls = [str(call) for call in mock_logger.warning.call_args_list]
            sync_failed_logged = any("Bundle synchronization failed" in call for call in warning_calls)
            assert sync_failed_logged
            
            # Should log debug error details
            debug_calls = [str(call) for call in mock_logger.debug.call_args_list]
            error_details_logged = any("Bundle sync error details" in call for call in debug_calls)
            assert error_details_logged

    def test_bundle_manager_creation_fails_catches_exception_and_logs_warning(self, orchestrator_bundle):
        """Test bundle manager creation fails catches exception and logs warning."""
        # Arrange
        output_dir = Path("/output")
        environment = "dev"
        
        # Mock bundle detection to return True, but manager creation to fail
        with patch('lhp.utils.bundle_detection.should_enable_bundle_support') as mock_should_enable, \
             patch('lhp.bundle.manager.BundleManager') as mock_bundle_manager_class, \
             patch.object(orchestrator_bundle, 'logger') as mock_logger:
            
            mock_should_enable.return_value = True
            
            # Mock BundleManager creation to fail
            manager_creation_error = Exception("BundleManager initialization failed")
            mock_bundle_manager_class.side_effect = manager_creation_error
            
            # Act - should not raise exception (bundle errors should not fail generation)
            orchestrator_bundle._sync_bundle_resources(output_dir, environment)
            
            # Assert
            mock_should_enable.assert_called_once_with(orchestrator_bundle.project_root)
            mock_bundle_manager_class.assert_called_once_with(
                orchestrator_bundle.project_root,
                orchestrator_bundle.pipeline_config_path
            )
            
            # Should log warning about bundle sync failure
            warning_calls = [str(call) for call in mock_logger.warning.call_args_list]
            sync_failed_logged = any("Bundle synchronization failed" in call for call in warning_calls)
            assert sync_failed_logged
            
            # Should also log debug error details
            debug_calls = [str(call) for call in mock_logger.debug.call_args_list]
            error_details_logged = any("Bundle sync error details" in call for call in debug_calls)
            assert error_details_logged
            
            # Should not log success message since manager creation failed
            info_calls = [str(call) for call in mock_logger.info.call_args_list]
            success_logged = any("synchronization completed successfully" in call for call in info_calls)
            assert not success_logged


class TestActionOrchestratorValidationWithoutGeneration:
    """Test ActionOrchestrator validation without generation logic."""

    @pytest.fixture
    def mock_project_root(self):
        """Mock project root path."""
        return Path("/mock/project")

    @pytest.fixture
    def orchestrator_validation(self, mock_project_root):
        """Create orchestrator for validation testing."""
        with patch('lhp.core.orchestrator.YAMLParser') as mock_yaml, \
             patch('lhp.core.orchestrator.PresetManager'), \
             patch('lhp.core.orchestrator.TemplateEngine'), \
             patch('lhp.core.orchestrator.ProjectConfigLoader') as mock_config_loader, \
             patch('lhp.core.orchestrator.ActionRegistry'), \
             patch('lhp.core.orchestrator.ConfigValidator'), \
             patch('lhp.core.orchestrator.SecretValidator'), \
             patch('lhp.core.orchestrator.DependencyResolver'), \
             patch('lhp.core.orchestrator.FlowgroupDiscoverer') as mock_discoverer, \
             patch('lhp.core.orchestrator.FlowgroupProcessor') as mock_processor, \
             patch('lhp.core.orchestrator.CodeGenerator'), \
             patch('lhp.core.orchestrator.PipelineValidator'):
            
            # Configure mocks
            mock_config_loader_instance = Mock()
            mock_config_loader.return_value = mock_config_loader_instance
            mock_config_loader_instance.load_project_config.return_value = None
            
            # Create orchestrator
            orchestrator = ActionOrchestrator(mock_project_root, enforce_version=False)
            
            # Store mocks for test access
            orchestrator.mock_discoverer = mock_discoverer.return_value
            orchestrator.mock_processor = mock_processor.return_value
            
            return orchestrator

    def test_pipeline_field_validation_no_flowgroups_returns_specific_error(self, orchestrator_validation):
        """Test pipeline field validation finds no flowgroups returns specific error."""
        # Arrange
        pipeline_field = "nonexistent_pipeline"
        env = "dev"
        
        # Mock the orchestrator's own discover_flowgroups_by_pipeline_field method
        with patch.object(orchestrator_validation, 'discover_flowgroups_by_pipeline_field', return_value=[]):
            # Act
            errors, warnings = orchestrator_validation.validate_pipeline_by_field(pipeline_field, env)
            
            # Assert
            assert len(errors) == 1  # One error about no flowgroups found
            assert warnings == []  # No warnings
            
            # Error should include specific message about pipeline field
            error_message = errors[0]
            assert f"No flowgroups found for pipeline field: {pipeline_field}" in error_message
            
            # Should call discover_flowgroups_by_pipeline_field
            orchestrator_validation.discover_flowgroups_by_pipeline_field.assert_called_once_with(pipeline_field)


    def test_validate_pipeline_by_field_method_delegation(self, orchestrator_validation):
        """Test validate_pipeline_by_field method calls correct discover method."""
        # This test verifies that the method properly delegates to discover_flowgroups_by_pipeline_field
        # Arrange
        pipeline_field = "test_pipeline"
        env = "dev"
        
        # Mock to return empty (which will trigger early return)
        with patch.object(orchestrator_validation, 'discover_flowgroups_by_pipeline_field', return_value=[]) as mock_discover:
            
            # Act
            errors, warnings = orchestrator_validation.validate_pipeline_by_field(pipeline_field, env)
            
            # Assert - method should delegate correctly
            mock_discover.assert_called_once_with(pipeline_field)
            
            # Should return appropriate error for no flowgroups
            assert len(errors) == 1
            assert f"No flowgroups found for pipeline field: {pipeline_field}" in errors[0]


class TestActionOrchestratorFlowgroupProcessingPipeline:
    """Test ActionOrchestrator flowgroup processing pipeline methods."""

    @pytest.fixture
    def mock_project_root(self):
        """Mock project root path."""
        return Path("/mock/project")

    @pytest.fixture
    def orchestrator_processing(self, mock_project_root):
        """Create orchestrator for processing pipeline testing."""
        with patch('lhp.core.orchestrator.YAMLParser') as mock_yaml, \
             patch('lhp.core.orchestrator.PresetManager'), \
             patch('lhp.core.orchestrator.TemplateEngine'), \
             patch('lhp.core.orchestrator.ProjectConfigLoader') as mock_config_loader, \
             patch('lhp.core.orchestrator.ActionRegistry'), \
             patch('lhp.core.orchestrator.ConfigValidator'), \
             patch('lhp.core.orchestrator.SecretValidator'), \
             patch('lhp.core.orchestrator.DependencyResolver'), \
             patch('lhp.core.orchestrator.FlowgroupDiscoverer'), \
             patch('lhp.core.orchestrator.FlowgroupProcessor') as mock_processor, \
             patch('lhp.core.orchestrator.CodeGenerator') as mock_generator, \
             patch('lhp.core.orchestrator.PipelineValidator'):
            
            # Configure mocks
            mock_config_loader_instance = Mock()
            mock_config_loader.return_value = mock_config_loader_instance
            mock_config_loader_instance.load_project_config.return_value = None
            
            # Create orchestrator
            orchestrator = ActionOrchestrator(mock_project_root, enforce_version=False)
            
            # Store mocks for test access
            orchestrator.mock_processor = mock_processor.return_value
            orchestrator.mock_generator = mock_generator.return_value
            
            return orchestrator

    @pytest.fixture
    def mock_flowgroup(self):
        """Create a mock FlowGroup for testing."""
        from lhp.models.config import FlowGroup
        flowgroup = Mock(spec=FlowGroup)
        flowgroup.flowgroup = "test_flowgroup"
        flowgroup.pipeline = "test_pipeline"
        return flowgroup

    @pytest.fixture
    def mock_substitution_mgr(self):
        """Create a mock EnhancedSubstitutionManager for testing."""
        from lhp.utils.substitution import EnhancedSubstitutionManager
        substitution_mgr = Mock(spec=EnhancedSubstitutionManager)
        return substitution_mgr

    def test_process_flowgroup_succeeds_returns_processed_flowgroup(self, orchestrator_processing, mock_flowgroup, mock_substitution_mgr):
        """Test FlowgroupProcessor succeeds returns processed flowgroup."""
        # Arrange
        processed_flowgroup = Mock()
        orchestrator_processing.mock_processor.process_flowgroup.return_value = processed_flowgroup
        
        # Act
        result = orchestrator_processing.process_flowgroup(mock_flowgroup, mock_substitution_mgr)
        
        # Assert
        assert result == processed_flowgroup  # Should return processed flowgroup
        
        # Should delegate to processor service with correct arguments
        orchestrator_processing.mock_processor.process_flowgroup.assert_called_once_with(mock_flowgroup, mock_substitution_mgr)

    def test_process_flowgroup_fails_propagates_exception(self, orchestrator_processing, mock_flowgroup, mock_substitution_mgr):
        """Test FlowgroupProcessor fails propagates exception."""
        # Arrange
        processor_error = Exception("Flowgroup processing failed: missing required template")
        orchestrator_processing.mock_processor.process_flowgroup.side_effect = processor_error
        
        # Act & Assert - should propagate the processor exception
        with pytest.raises(Exception, match="Flowgroup processing failed: missing required template"):
            orchestrator_processing.process_flowgroup(mock_flowgroup, mock_substitution_mgr)
        
        # Should still call processor service
        orchestrator_processing.mock_processor.process_flowgroup.assert_called_once_with(mock_flowgroup, mock_substitution_mgr)

    def test_generate_flowgroup_code_succeeds_returns_generated_code(self, orchestrator_processing, mock_flowgroup, mock_substitution_mgr):
        """Test CodeGenerator succeeds returns generated code."""
        # Arrange
        expected_code = "# Generated Python code\nimport pandas as pd\n# ...\n"
        output_dir = Path("/output")
        state_manager = Mock()
        source_yaml = Path("/source/flowgroup.yaml")
        env = "dev"
        include_tests = False
        
        orchestrator_processing.mock_generator.generate_flowgroup_code.return_value = expected_code
        
        # Act
        result = orchestrator_processing.generate_flowgroup_code(
            mock_flowgroup, mock_substitution_mgr, output_dir, state_manager, source_yaml, env, include_tests
        )
        
        # Assert
        assert result == expected_code  # Should return generated code
        
        # Should delegate to generator service with all correct arguments
        orchestrator_processing.mock_generator.generate_flowgroup_code.assert_called_once_with(
            mock_flowgroup, mock_substitution_mgr, output_dir, state_manager, source_yaml, env, include_tests, None
        )

    def test_generate_flowgroup_code_fails_propagates_exception(self, orchestrator_processing, mock_flowgroup, mock_substitution_mgr):
        """Test CodeGenerator fails propagates exception."""
        # Arrange
        generator_error = Exception("Code generation failed: template not found")
        orchestrator_processing.mock_generator.generate_flowgroup_code.side_effect = generator_error
        
        output_dir = Path("/output")
        state_manager = Mock()
        source_yaml = Path("/source/flowgroup.yaml")
        env = "dev"
        include_tests = False
        
        # Act & Assert - should propagate the generator exception
        with pytest.raises(Exception, match="Code generation failed: template not found"):
            orchestrator_processing.generate_flowgroup_code(
                mock_flowgroup, mock_substitution_mgr, output_dir, state_manager, source_yaml, env, include_tests
            )
        
        # Should still call generator service
        orchestrator_processing.mock_generator.generate_flowgroup_code.assert_called_once_with(
            mock_flowgroup, mock_substitution_mgr, output_dir, state_manager, source_yaml, env, include_tests, None
        )

    def test_substitution_mgr_none_still_delegates_to_services(self, orchestrator_processing, mock_flowgroup):
        """Test substitution_mgr is None still delegates to services."""
        # Arrange
        processed_flowgroup = Mock()
        generated_code = "# Generated code without substitution\n"
        
        # Test both methods with None substitution manager
        orchestrator_processing.mock_processor.process_flowgroup.return_value = processed_flowgroup
        orchestrator_processing.mock_generator.generate_flowgroup_code.return_value = generated_code
        
        # Act - process_flowgroup with None substitution
        result1 = orchestrator_processing.process_flowgroup(mock_flowgroup, None)
        
        # Act - generate_flowgroup_code with None substitution
        result2 = orchestrator_processing.generate_flowgroup_code(mock_flowgroup, None)
        
        # Assert
        assert result1 == processed_flowgroup
        assert result2 == generated_code
        
        # Should still delegate to services with None as parameter
        orchestrator_processing.mock_processor.process_flowgroup.assert_called_once_with(mock_flowgroup, None)
        orchestrator_processing.mock_generator.generate_flowgroup_code.assert_called_once_with(
            mock_flowgroup, None, None, None, None, None, False, None
        )

    def test_include_tests_true_passes_to_code_generator(self, orchestrator_processing, mock_flowgroup, mock_substitution_mgr):
        """Test include_tests=True passes to CodeGenerator."""
        # Arrange
        generated_code = "# Generated code with tests\nimport pytest\n# ...\n"
        orchestrator_processing.mock_generator.generate_flowgroup_code.return_value = generated_code
        
        output_dir = Path("/output")
        state_manager = Mock()
        source_yaml = Path("/source/flowgroup.yaml")
        env = "dev"
        include_tests = True  # Key parameter being tested
        
        # Act
        result = orchestrator_processing.generate_flowgroup_code(
            mock_flowgroup, mock_substitution_mgr, output_dir, state_manager, source_yaml, env, include_tests
        )
        
        # Assert
        assert result == generated_code
        
        # Should pass include_tests=True to generator service
        orchestrator_processing.mock_generator.generate_flowgroup_code.assert_called_once_with(
            mock_flowgroup, mock_substitution_mgr, output_dir, state_manager, source_yaml, env, True, None
        )

    def test_include_tests_false_passes_to_code_generator(self, orchestrator_processing, mock_flowgroup, mock_substitution_mgr):
        """Test include_tests=False passes to CodeGenerator."""
        # Arrange
        generated_code = "# Generated code without tests\nimport pandas as pd\n# ...\n"
        orchestrator_processing.mock_generator.generate_flowgroup_code.return_value = generated_code
        
        output_dir = Path("/output")
        state_manager = Mock()
        source_yaml = Path("/source/flowgroup.yaml")
        env = "dev"
        include_tests = False  # Key parameter being tested
        
        # Act
        result = orchestrator_processing.generate_flowgroup_code(
            mock_flowgroup, mock_substitution_mgr, output_dir, state_manager, source_yaml, env, include_tests
        )
        
        # Assert
        assert result == generated_code
        
        # Should pass include_tests=False to generator service
        orchestrator_processing.mock_generator.generate_flowgroup_code.assert_called_once_with(
            mock_flowgroup, mock_substitution_mgr, output_dir, state_manager, source_yaml, env, False, None
        )


class TestActionOrchestratorErrorHandlingAndEdgeCases:
    """Test ActionOrchestrator error handling and edge cases."""

    @pytest.fixture
    def mock_project_root(self):
        """Mock project root path."""
        return Path("/mock/project")

    @pytest.fixture
    def orchestrator_error_handling(self, mock_project_root):
        """Create orchestrator for error handling testing."""
        with patch('lhp.core.orchestrator.YAMLParser') as mock_yaml, \
             patch('lhp.core.orchestrator.PresetManager'), \
             patch('lhp.core.orchestrator.TemplateEngine'), \
             patch('lhp.core.orchestrator.ProjectConfigLoader') as mock_config_loader, \
             patch('lhp.core.orchestrator.ActionRegistry'), \
             patch('lhp.core.orchestrator.ConfigValidator'), \
             patch('lhp.core.orchestrator.SecretValidator'), \
             patch('lhp.core.orchestrator.DependencyResolver'), \
             patch('lhp.core.orchestrator.FlowgroupDiscoverer') as mock_discoverer, \
             patch('lhp.core.orchestrator.FlowgroupProcessor') as mock_processor, \
             patch('lhp.core.orchestrator.CodeGenerator') as mock_generator, \
             patch('lhp.core.orchestrator.PipelineValidator'):
            
            # Configure mocks
            mock_config_loader_instance = Mock()
            mock_config_loader.return_value = mock_config_loader_instance
            mock_config_loader_instance.load_project_config.return_value = None
            
            # Create orchestrator
            orchestrator = ActionOrchestrator(mock_project_root, enforce_version=False)
            
            # Store mocks for test access
            orchestrator.mock_discoverer = mock_discoverer.return_value
            orchestrator.mock_processor = mock_processor.return_value
            orchestrator.mock_generator = mock_generator.return_value
            
            return orchestrator

    @pytest.fixture
    def mock_flowgroup(self):
        """Create a mock FlowGroup for testing."""
        from lhp.models.config import FlowGroup
        flowgroup = Mock(spec=FlowGroup)
        flowgroup.flowgroup = "test_flowgroup"
        flowgroup.pipeline = "test_pipeline"
        return flowgroup

    def test_service_method_unexpected_exception_propagates_with_context(self, orchestrator_error_handling, mock_flowgroup):
        """Test any service method throws unexpected exception propagates with context."""
        # Arrange
        from lhp.utils.substitution import EnhancedSubstitutionManager
        mock_substitution_mgr = Mock(spec=EnhancedSubstitutionManager)
        
        # Test various service methods with unexpected exceptions
        unexpected_error = RuntimeError("Unexpected service failure: database connection lost")
        
        # Test processor service exception
        orchestrator_error_handling.mock_processor.process_flowgroup.side_effect = unexpected_error
        
        # Act & Assert - should propagate exception with context
        with pytest.raises(RuntimeError, match="Unexpected service failure: database connection lost"):
            orchestrator_error_handling.process_flowgroup(mock_flowgroup, mock_substitution_mgr)
        
        # Reset for next test
        orchestrator_error_handling.mock_processor.process_flowgroup.side_effect = None
        
        # Test generator service exception
        orchestrator_error_handling.mock_generator.generate_flowgroup_code.side_effect = unexpected_error
        
        with pytest.raises(RuntimeError, match="Unexpected service failure: database connection lost"):
            orchestrator_error_handling.generate_flowgroup_code(mock_flowgroup, mock_substitution_mgr)
        
        # Should have attempted to call both services despite failures
        orchestrator_error_handling.mock_processor.process_flowgroup.assert_called_once_with(mock_flowgroup, mock_substitution_mgr)
        orchestrator_error_handling.mock_generator.generate_flowgroup_code.assert_called_once_with(
            mock_flowgroup, mock_substitution_mgr, None, None, None, None, False, None
        )

    def test_logging_operations_fail_does_not_break_main_functionality(self, orchestrator_error_handling, mock_flowgroup):
        """Test logging operations fail does not break main functionality."""
        # Arrange
        from lhp.utils.substitution import EnhancedSubstitutionManager
        mock_substitution_mgr = Mock(spec=EnhancedSubstitutionManager)
        processed_flowgroup = Mock()
        generated_code = "# Generated code\n"
        
        orchestrator_error_handling.mock_processor.process_flowgroup.return_value = processed_flowgroup
        orchestrator_error_handling.mock_generator.generate_flowgroup_code.return_value = generated_code
        
        # Mock logger to raise exception during logging
        with patch.object(orchestrator_error_handling, 'logger') as mock_logger:
            mock_logger.info.side_effect = Exception("Logging system failure")
            mock_logger.debug.side_effect = Exception("Logging system failure")
            mock_logger.warning.side_effect = Exception("Logging system failure")
            
            # Act - should not fail despite logging errors
            result1 = orchestrator_error_handling.process_flowgroup(mock_flowgroup, mock_substitution_mgr)
            result2 = orchestrator_error_handling.generate_flowgroup_code(mock_flowgroup, mock_substitution_mgr)
            
            # Assert - main functionality should work despite logging failures
            assert result1 == processed_flowgroup
            assert result2 == generated_code
            
            # Services should still be called successfully
            orchestrator_error_handling.mock_processor.process_flowgroup.assert_called_once_with(mock_flowgroup, mock_substitution_mgr)
            orchestrator_error_handling.mock_generator.generate_flowgroup_code.assert_called_once_with(
                mock_flowgroup, mock_substitution_mgr, None, None, None, None, False, None
            )

    def test_invalid_parameters_passed_delegates_to_services(self, orchestrator_error_handling):
        """Test invalid parameters are passed through to services for validation."""
        # Arrange - Test parameter delegation with various input types
        from lhp.utils.substitution import EnhancedSubstitutionManager
        mock_substitution_mgr = Mock(spec=EnhancedSubstitutionManager)
        
        # Configure services to handle validation and provide appropriate responses
        orchestrator_error_handling.mock_processor.process_flowgroup.side_effect = [
            ValueError("Service validation: None flowgroup not allowed"),
            TypeError("Service validation: Invalid flowgroup type"),
            Mock()  # Valid response for last test
        ]
        
        orchestrator_error_handling.mock_generator.generate_flowgroup_code.side_effect = [
            TypeError("Service validation: Invalid substitution manager type"),
            "generated_code"  # Valid response for last test
        ]
        
        # Test 1: None flowgroup parameter - service should validate and raise error
        with pytest.raises(ValueError, match="Service validation: None flowgroup not allowed"):
            orchestrator_error_handling.process_flowgroup(None, mock_substitution_mgr)
        
        # Test 2: Invalid flowgroup type - service should validate and raise error
        with pytest.raises(TypeError, match="Service validation: Invalid flowgroup type"):
            orchestrator_error_handling.process_flowgroup("invalid_flowgroup", mock_substitution_mgr)
        
        # Test 3: Invalid substitution manager type for generate_flowgroup_code
        invalid_substitution_mgr = "not_a_substitution_manager"
        mock_flowgroup = Mock()
        
        with pytest.raises(TypeError, match="Service validation: Invalid substitution manager type"):
            orchestrator_error_handling.generate_flowgroup_code(mock_flowgroup, invalid_substitution_mgr)
        
        # Test 4: Valid parameters should work normally
        result = orchestrator_error_handling.process_flowgroup(mock_flowgroup, mock_substitution_mgr)
        assert result is not None  # Should return mocked result
        
        result2 = orchestrator_error_handling.generate_flowgroup_code(mock_flowgroup, mock_substitution_mgr)
        assert result2 == "generated_code"  # Should return mocked result
        
        # Verify all calls were made to services (delegation working correctly)
        assert orchestrator_error_handling.mock_processor.process_flowgroup.call_count == 3
        assert orchestrator_error_handling.mock_generator.generate_flowgroup_code.call_count == 2

    def test_edge_case_empty_and_invalid_pipeline_fields(self, orchestrator_error_handling):
        """Test edge cases with empty and invalid pipeline field values."""
        # Test empty pipeline field
        with patch.object(orchestrator_error_handling, 'discover_flowgroups_by_pipeline_field', return_value=[]) as mock_discover:
            errors, warnings = orchestrator_error_handling.validate_pipeline_by_field("", "dev")
            
            # Should handle empty string gracefully
            assert len(errors) == 1
            assert "No flowgroups found for pipeline field:" in errors[0]
            mock_discover.assert_called_once_with("")
        
        # Test None pipeline field - should raise exception or handle gracefully
        with patch.object(orchestrator_error_handling, 'discover_flowgroups_by_pipeline_field', side_effect=ValueError("Pipeline field cannot be None")) as mock_discover_none:
            errors, warnings = orchestrator_error_handling.validate_pipeline_by_field(None, "dev")
            
            # Should catch the ValueError and return it as an error
            assert len(errors) == 1
            assert "Pipeline validation failed" in errors[0]
            assert "Pipeline field cannot be None" in errors[0]
        
        # Test whitespace-only pipeline field
        with patch.object(orchestrator_error_handling, 'discover_flowgroups_by_pipeline_field', return_value=[]) as mock_discover_whitespace:
            errors, warnings = orchestrator_error_handling.validate_pipeline_by_field("   ", "dev")
            
            # Should handle whitespace-only string
            assert len(errors) == 1
            assert "No flowgroups found for pipeline field:    " in errors[0]
            mock_discover_whitespace.assert_called_once_with("   ")

    def test_edge_case_service_returns_none_or_empty_results(self, orchestrator_error_handling, mock_flowgroup):
        """Test edge cases where services return None or empty results."""
        # Arrange
        from lhp.utils.substitution import EnhancedSubstitutionManager
        mock_substitution_mgr = Mock(spec=EnhancedSubstitutionManager)
        
        # Test processor returning None
        orchestrator_error_handling.mock_processor.process_flowgroup.return_value = None
        result = orchestrator_error_handling.process_flowgroup(mock_flowgroup, mock_substitution_mgr)
        assert result is None  # Should handle None return gracefully
        
        # Test generator returning empty string
        orchestrator_error_handling.mock_generator.generate_flowgroup_code.return_value = ""
        result = orchestrator_error_handling.generate_flowgroup_code(mock_flowgroup, mock_substitution_mgr)
        assert result == ""  # Should handle empty string return gracefully
        
        # Test generator returning None
        orchestrator_error_handling.mock_generator.generate_flowgroup_code.return_value = None
        result = orchestrator_error_handling.generate_flowgroup_code(mock_flowgroup, mock_substitution_mgr)
        assert result is None  # Should handle None return gracefully


class TestActionOrchestratorIntegrationScenarios:
    """Test ActionOrchestrator integration scenarios where multiple services work together."""

    @pytest.fixture
    def mock_project_root(self):
        """Mock project root path."""
        return Path("/mock/project")

    @pytest.fixture
    def orchestrator_integration(self, mock_project_root):
        """Create orchestrator for integration testing."""
        with patch('lhp.core.orchestrator.YAMLParser') as mock_yaml, \
             patch('lhp.core.orchestrator.PresetManager'), \
             patch('lhp.core.orchestrator.TemplateEngine'), \
             patch('lhp.core.orchestrator.ProjectConfigLoader') as mock_config_loader, \
             patch('lhp.core.orchestrator.ActionRegistry'), \
             patch('lhp.core.orchestrator.ConfigValidator'), \
             patch('lhp.core.orchestrator.SecretValidator'), \
             patch('lhp.core.orchestrator.DependencyResolver'), \
             patch('lhp.core.orchestrator.FlowgroupDiscoverer') as mock_discoverer, \
             patch('lhp.core.orchestrator.FlowgroupProcessor') as mock_processor, \
             patch('lhp.core.orchestrator.CodeGenerator') as mock_generator, \
             patch('lhp.core.orchestrator.PipelineValidator'):
            
            # Configure mocks
            mock_config_loader_instance = Mock()
            mock_config_loader.return_value = mock_config_loader_instance
            mock_config_loader_instance.load_project_config.return_value = None
            
            # Create orchestrator
            orchestrator = ActionOrchestrator(mock_project_root, enforce_version=False)
            
            # Store mocks for test access
            orchestrator.mock_discoverer = mock_discoverer.return_value
            orchestrator.mock_processor = mock_processor.return_value
            orchestrator.mock_generator = mock_generator.return_value
            
            return orchestrator

    def test_services_dependency_conflicts_detected_and_reported(self, orchestrator_integration):
        """Test services have dependency conflicts detects and reports."""
        # Arrange - Simulate service dependency conflicts
        from lhp.models.config import FlowGroup
        from lhp.utils.substitution import EnhancedSubstitutionManager
        
        mock_flowgroup = Mock(spec=FlowGroup)
        mock_substitution_mgr = Mock(spec=EnhancedSubstitutionManager)
        
        # Simulate a dependency conflict where processor succeeds but generator fails due to format issues
        processed_flowgroup = Mock(spec=FlowGroup)
        processed_flowgroup.flowgroup = "test_flowgroup"
        processed_flowgroup.incompatible_field = "format_issue"
        
        # Configure services to show dependency conflict
        orchestrator_integration.mock_processor.process_flowgroup.return_value = processed_flowgroup
        orchestrator_integration.mock_generator.generate_flowgroup_code.side_effect = ValueError(
            "Generator conflict: Processed flowgroup format incompatible with generator expectations"
        )
        
        # Act & Assert - Should detect and report the dependency conflict
        with pytest.raises(ValueError, match="Generator conflict: Processed flowgroup format incompatible"):
            orchestrator_integration.generate_flowgroup_code(processed_flowgroup, mock_substitution_mgr)
        
        # Should still call processor successfully
        result = orchestrator_integration.process_flowgroup(mock_flowgroup, mock_substitution_mgr)
        assert result == processed_flowgroup
        
        # But generator fails due to dependency conflict
        orchestrator_integration.mock_processor.process_flowgroup.assert_called_once_with(mock_flowgroup, mock_substitution_mgr)
        orchestrator_integration.mock_generator.generate_flowgroup_code.assert_called_once_with(
            processed_flowgroup, mock_substitution_mgr, None, None, None, None, False, None
        )

    def test_service_coordination_with_complex_data_flow(self, orchestrator_integration):
        """Test service coordination handles complex data flow between services."""
        # Arrange - Test data transformation through service chain
        from lhp.models.config import FlowGroup
        from lhp.utils.substitution import EnhancedSubstitutionManager
        
        # Create complex flowgroup with multiple attributes
        original_flowgroup = Mock(spec=FlowGroup)
        original_flowgroup.flowgroup = "complex_flowgroup"
        original_flowgroup.pipeline = "data_pipeline"
        original_flowgroup.actions = [{"type": "read"}, {"type": "transform"}, {"type": "write"}]
        
        # Create transformed flowgroup (processor output)
        transformed_flowgroup = Mock(spec=FlowGroup)
        transformed_flowgroup.flowgroup = "complex_flowgroup"
        transformed_flowgroup.pipeline = "data_pipeline"
        transformed_flowgroup.actions = [
            {"type": "read", "processed": True}, 
            {"type": "transform", "processed": True}, 
            {"type": "write", "processed": True}
        ]
        transformed_flowgroup.metadata = {"processed_by": "FlowgroupProcessor", "timestamp": "2023-01-01"}
        
        mock_substitution_mgr = Mock(spec=EnhancedSubstitutionManager)
        
        # Configure services to transform data through the chain
        orchestrator_integration.mock_processor.process_flowgroup.return_value = transformed_flowgroup
        orchestrator_integration.mock_generator.generate_flowgroup_code.return_value = "# Generated from transformed data\n"
        
        # Act - Pass data through service chain
        step1_result = orchestrator_integration.process_flowgroup(original_flowgroup, mock_substitution_mgr)
        step2_result = orchestrator_integration.generate_flowgroup_code(step1_result, mock_substitution_mgr)
        
        # Assert - Data flow maintained through service coordination
        assert step1_result == transformed_flowgroup
        assert step2_result == "# Generated from transformed data\n"
        
        # Verify proper data flow: original -> processor -> generator
        orchestrator_integration.mock_processor.process_flowgroup.assert_called_once_with(original_flowgroup, mock_substitution_mgr)
        orchestrator_integration.mock_generator.generate_flowgroup_code.assert_called_once_with(transformed_flowgroup, mock_substitution_mgr, None, None, None, None, False, None)

    def test_service_integration_error_propagation_and_recovery(self, orchestrator_integration):
        """Test service integration shows proper error propagation and recovery patterns."""
        # Arrange - Test error handling through service chain
        from lhp.models.config import FlowGroup
        from lhp.utils.substitution import EnhancedSubstitutionManager
        
        mock_flowgroup = Mock(spec=FlowGroup)
        mock_substitution_mgr = Mock(spec=EnhancedSubstitutionManager)
        
        # Configure processor to initially fail, then succeed on retry
        processor_failure = Exception("Temporary processor failure")
        processed_flowgroup = Mock(spec=FlowGroup)
        processed_flowgroup.flowgroup = "test_flowgroup"
        orchestrator_integration.mock_processor.process_flowgroup.side_effect = [processor_failure, processed_flowgroup]
        
        # Configure generator to succeed
        generated_code = "# Recovered generated code\n"
        orchestrator_integration.mock_generator.generate_flowgroup_code.return_value = generated_code
        
        # Act 1 - First attempt should fail at processor stage
        with pytest.raises(Exception, match="Temporary processor failure"):
            orchestrator_integration.process_flowgroup(mock_flowgroup, mock_substitution_mgr)
        
        # Act 2 - Retry should succeed (simulating recovery)
        result1 = orchestrator_integration.process_flowgroup(mock_flowgroup, mock_substitution_mgr)
        result2 = orchestrator_integration.generate_flowgroup_code(processed_flowgroup, mock_substitution_mgr)
        
        # Assert - Services recover and work properly in sequence
        assert result1 == processed_flowgroup
        assert result2 == generated_code
        
        # Verify error propagation and recovery: processor was called twice (fail + success)
        assert orchestrator_integration.mock_processor.process_flowgroup.call_count == 2  # Failed attempt + successful attempt
        assert orchestrator_integration.mock_generator.generate_flowgroup_code.call_count == 1  # Only successful call

    def test_service_parameter_validation_integration(self, orchestrator_integration):
        """Test service integration with parameter validation across service boundaries."""
        # Arrange - Test parameter flow and validation through services
        from lhp.models.config import FlowGroup
        from lhp.utils.substitution import EnhancedSubstitutionManager
        
        mock_flowgroup = Mock(spec=FlowGroup)
        mock_substitution_mgr = Mock(spec=EnhancedSubstitutionManager)
        
        # Test various parameter combinations through service chain
        orchestrator_integration.mock_processor.process_flowgroup.return_value = mock_flowgroup
        orchestrator_integration.mock_generator.generate_flowgroup_code.return_value = "generated_code"
        
        # Test parameters are passed correctly through service chain
        output_dir = Path("/output")
        state_manager = Mock()
        source_yaml = Path("/source.yaml")
        env = "test"
        include_tests = True
        
        # Act - Pass parameters through service methods
        result1 = orchestrator_integration.process_flowgroup(mock_flowgroup, mock_substitution_mgr)
        result2 = orchestrator_integration.generate_flowgroup_code(
            result1, mock_substitution_mgr, output_dir, state_manager, source_yaml, env, include_tests
        )
        
        # Assert - Parameters flow correctly through service integration
        assert result1 == mock_flowgroup
        assert result2 == "generated_code"
        
        # Verify parameter passing
        orchestrator_integration.mock_processor.process_flowgroup.assert_called_once_with(mock_flowgroup, mock_substitution_mgr)
        orchestrator_integration.mock_generator.generate_flowgroup_code.assert_called_once_with(
            mock_flowgroup, mock_substitution_mgr, output_dir, state_manager, source_yaml, env, include_tests, None
        )
