"""Tests for ActionOrchestrator initialization and configuration."""

import os
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from lhp.core.orchestrator import ActionOrchestrator
from lhp.models.config import FlowGroup, FlowGroupContext
from lhp.utils.error_formatter import LHPError
from tests.helpers import wrap_in_ctx as _wrap_in_ctx


class TestActionOrchestratorInitialization:
    """Test ActionOrchestrator initialization and configuration logic."""

    @pytest.fixture
    def mock_project_root(self):
        """Mock project root path."""
        return Path("/mock/project")

    @pytest.fixture
    def mock_components(self):
        """Mock all component dependencies."""
        with (
            patch("lhp.core.orchestrator.YAMLParser") as mock_yaml,
            patch("lhp.core.orchestrator.PresetManager") as mock_preset,
            patch("lhp.core.orchestrator.TemplateEngine") as mock_template,
            patch("lhp.core.orchestrator.ProjectConfigLoader") as mock_config_loader,
            patch("lhp.core.orchestrator.ActionRegistry") as mock_registry,
            patch("lhp.core.orchestrator.ConfigValidator") as mock_config_validator,
            patch("lhp.core.orchestrator.SecretValidator") as mock_secret_validator,
            patch(
                "lhp.core.orchestrator.DependencyResolver"
            ) as mock_dependency_resolver,
            patch("lhp.core.orchestrator.FlowgroupDiscoverer") as mock_discoverer,
            patch("lhp.core.orchestrator.FlowgroupProcessor") as mock_processor,
            patch("lhp.core.orchestrator.CodeGenerator") as mock_generator,
            patch("lhp.core.orchestrator.PipelineValidator") as mock_validator,
        ):
            # Configure mocks
            mock_config_loader_instance = Mock()
            mock_config_loader.return_value = mock_config_loader_instance
            mock_config_loader_instance.load_project_config.return_value = None

            yield {
                "yaml_parser": mock_yaml,
                "preset_manager": mock_preset,
                "template_engine": mock_template,
                "config_loader": mock_config_loader,
                "action_registry": mock_registry,
                "config_validator": mock_config_validator,
                "secret_validator": mock_secret_validator,
                "dependency_resolver": mock_dependency_resolver,
                "discoverer": mock_discoverer,
                "processor": mock_processor,
                "generator": mock_generator,
                "validator": mock_validator,
                "config_loader_instance": mock_config_loader_instance,
            }

    def test_version_enforcement_enabled_with_project_requirement(
        self, mock_project_root, mock_components
    ):
        """Test version enforcement runs when enforce_version=True and project has requirements."""
        # Arrange
        mock_project_config = Mock()
        mock_project_config.required_lhp_version = ">=0.4.0"
        mock_project_config.name = "test_project"
        mock_project_config.version = "1.0.0"
        mock_components[
            "config_loader_instance"
        ].load_project_config.return_value = mock_project_config

        with patch.object(
            ActionOrchestrator, "_enforce_version_requirements"
        ) as mock_enforce:
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
        mock_components[
            "config_loader_instance"
        ].load_project_config.return_value = mock_project_config

        with patch.object(
            ActionOrchestrator, "_enforce_version_requirements"
        ) as mock_enforce:
            # Act
            orchestrator = ActionOrchestrator(mock_project_root, enforce_version=False)

            # Assert
            mock_enforce.assert_not_called()
            assert orchestrator.enforce_version is False
            assert orchestrator.project_config == mock_project_config

    def test_project_config_loader_fails(self, mock_project_root, mock_components):
        """Test handling when project_config_loader fails to load config."""
        # Arrange
        mock_components[
            "config_loader_instance"
        ].load_project_config.side_effect = Exception("Config loading failed")

        # Act & Assert - should propagate the exception
        with pytest.raises(Exception, match="Config loading failed"):
            ActionOrchestrator(mock_project_root, enforce_version=False)

    def test_core_component_init_fails(self, mock_project_root):
        """Test exception propagation when core component initialization fails."""
        # Arrange - mock one component to fail
        with (
            patch("lhp.core.orchestrator.YAMLParser"),
            patch("lhp.core.orchestrator.PresetManager") as mock_preset,
            patch("lhp.core.orchestrator.TemplateEngine"),
            patch("lhp.core.orchestrator.ProjectConfigLoader") as mock_config_loader,
        ):
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
        mock_components["discoverer"].side_effect = Exception(
            "FlowgroupDiscoverer initialization failed"
        )

        # Act & Assert - should propagate the service initialization exception
        with pytest.raises(
            Exception, match="FlowgroupDiscoverer initialization failed"
        ):
            ActionOrchestrator(mock_project_root, enforce_version=False)

    def test_project_root_nonexistent(self, mock_components):
        """Test initialization when project_root does not exist."""
        # Arrange
        nonexistent_root = Path("/nonexistent/project/root")

        with patch("logging.getLogger") as mock_logger:
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
        mock_components[
            "config_loader_instance"
        ].load_project_config.return_value = mock_project_config

        with patch("logging.getLogger") as mock_get_logger:
            mock_logger = Mock()
            mock_get_logger.return_value = mock_logger

            # Act
            orchestrator = ActionOrchestrator(mock_project_root, enforce_version=False)

            # Assert
            assert orchestrator.project_config == mock_project_config
            # Should log project configuration details
            mock_logger.info.assert_any_call(
                "Loaded project configuration: test_project v2.1.0"
            )

    def test_project_config_none_logging(self, mock_project_root, mock_components):
        """Test logging when project config is None (using defaults)."""
        # Arrange
        mock_components[
            "config_loader_instance"
        ].load_project_config.return_value = None

        with patch("logging.getLogger") as mock_get_logger:
            mock_logger = Mock()
            mock_get_logger.return_value = mock_logger

            # Act
            orchestrator = ActionOrchestrator(mock_project_root, enforce_version=False)

            # Assert
            assert orchestrator.project_config is None
            # Should log "using defaults" message
            mock_logger.info.assert_any_call(
                "No project configuration found, using defaults"
            )


class TestActionOrchestratorVersionEnforcement:
    """Test ActionOrchestrator version requirement enforcement logic."""

    @pytest.fixture
    def mock_project_root(self):
        """Mock project root path."""
        return Path("/mock/project")

    @pytest.fixture
    def orchestrator_with_version_requirement(self, mock_project_root):
        """Create orchestrator with version requirement for testing."""
        with (
            patch("lhp.core.orchestrator.YAMLParser"),
            patch("lhp.core.orchestrator.PresetManager"),
            patch("lhp.core.orchestrator.TemplateEngine"),
            patch("lhp.core.orchestrator.ProjectConfigLoader") as mock_config_loader,
            patch("lhp.core.orchestrator.ActionRegistry"),
            patch("lhp.core.orchestrator.ConfigValidator"),
            patch("lhp.core.orchestrator.SecretValidator"),
            patch("lhp.core.orchestrator.DependencyResolver"),
            patch("lhp.core.orchestrator.FlowgroupDiscoverer"),
            patch("lhp.core.orchestrator.FlowgroupProcessor"),
            patch("lhp.core.orchestrator.CodeGenerator"),
            patch("lhp.core.orchestrator.PipelineValidator"),
        ):
            # Configure project config with version requirement
            mock_project_config = Mock()
            mock_project_config.required_lhp_version = ">=0.4.0"
            mock_project_config.name = "test_project"
            mock_project_config.version = "1.0.0"

            mock_config_loader_instance = Mock()
            mock_config_loader.return_value = mock_config_loader_instance
            mock_config_loader_instance.load_project_config.return_value = (
                mock_project_config
            )

            # Don't call enforce_version_requirements in init
            with patch.object(ActionOrchestrator, "_enforce_version_requirements"):
                orchestrator = ActionOrchestrator(
                    mock_project_root, enforce_version=True
                )
                orchestrator.project_config = mock_project_config
                return orchestrator

    def test_version_mismatch_fail_with_error_code_007(
        self, orchestrator_with_version_requirement
    ):
        """Test version does not match requirement then fail with LHPError code 007."""
        # Arrange - set requirement that current version won't satisfy
        orchestrator_with_version_requirement.project_config.required_lhp_version = (
            ">9.0.0"  # Impossible requirement
        )

        # Act & Assert
        with pytest.raises(LHPError) as exc_info:
            orchestrator_with_version_requirement._enforce_version_requirements()

        # Assert error details
        error = exc_info.value
        assert error.code == "LHP-CFG-007"
        assert "version requirement not satisfied" in error.title.lower()
        assert ">9.0.0" in error.details

    def test_ignore_version_1_bypasses_with_warning(
        self, orchestrator_with_version_requirement
    ):
        """Test LHP_IGNORE_VERSION=1 bypasses version check with warning."""
        # Arrange
        with (
            patch.dict(os.environ, {"LHP_IGNORE_VERSION": "1"}),
            patch.object(
                orchestrator_with_version_requirement, "logger"
            ) as mock_logger,
        ):
            # Act - should complete without raising exception
            orchestrator_with_version_requirement._enforce_version_requirements()

            # Assert - should log warning about bypass
            mock_logger.warning.assert_called_once()
            warning_call = mock_logger.warning.call_args[0][0]
            assert "Version requirement bypass enabled" in warning_call
            assert ">=0.4.0" in warning_call

    def test_ignore_version_true_bypasses_with_warning(
        self, orchestrator_with_version_requirement
    ):
        """Test LHP_IGNORE_VERSION=true bypasses version check with warning."""
        # Arrange
        with (
            patch.dict(os.environ, {"LHP_IGNORE_VERSION": "true"}),
            patch.object(
                orchestrator_with_version_requirement, "logger"
            ) as mock_logger,
        ):
            # Act - should complete without raising exception
            orchestrator_with_version_requirement._enforce_version_requirements()

            # Assert - should log warning about bypass
            mock_logger.warning.assert_called_once()
            warning_call = mock_logger.warning.call_args[0][0]
            assert "Version requirement bypass enabled" in warning_call
            assert ">=0.4.0" in warning_call

    def test_ignore_version_yes_bypasses_with_warning(
        self, orchestrator_with_version_requirement
    ):
        """Test LHP_IGNORE_VERSION=yes bypasses version check with warning."""
        # Arrange
        with (
            patch.dict(os.environ, {"LHP_IGNORE_VERSION": "yes"}),
            patch.object(
                orchestrator_with_version_requirement, "logger"
            ) as mock_logger,
        ):
            # Act - should complete without raising exception
            orchestrator_with_version_requirement._enforce_version_requirements()

            # Assert - should log warning about bypass
            mock_logger.warning.assert_called_once()
            warning_call = mock_logger.warning.call_args[0][0]
            assert "Version requirement bypass enabled" in warning_call
            assert ">=0.4.0" in warning_call

    def test_ignore_version_false_does_not_bypass(
        self, orchestrator_with_version_requirement
    ):
        """Test LHP_IGNORE_VERSION=false does not bypass version check."""
        # Arrange - set impossible requirement and environment that shouldn't bypass
        orchestrator_with_version_requirement.project_config.required_lhp_version = (
            ">9.0.0"  # Impossible requirement
        )

        with patch.dict(os.environ, {"LHP_IGNORE_VERSION": "false"}):
            # Act & Assert - should still fail with version mismatch (no bypass)
            with pytest.raises(LHPError) as exc_info:
                orchestrator_with_version_requirement._enforce_version_requirements()

            # Assert error details - should fail normally, not bypass
            error = exc_info.value
            assert error.code == "LHP-CFG-007"

    def test_packaging_not_installed_fails_with_error_code_006(
        self, orchestrator_with_version_requirement
    ):
        """Test packaging library not installed fails with LHPError code 006."""
        # Arrange - mock import to fail
        with patch("builtins.__import__") as mock_import:

            def import_side_effect(name, *args, **kwargs):
                if name in ("packaging.version", "packaging.specifiers"):
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

    def test_invalid_version_format_fails_with_error_code_008(
        self, orchestrator_with_version_requirement
    ):
        """Test invalid PEP 440 version requirement fails with LHPError code 008."""
        # Arrange
        orchestrator_with_version_requirement.project_config.required_lhp_version = (
            "invalid-version-format"
        )

        with (
            patch("lhp.core.orchestrator.get_version") as mock_get_version,
            patch("packaging.specifiers.SpecifierSet") as mock_specifier_set,
        ):
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

    def test_version_parsing_fails_with_error_code_008(
        self, orchestrator_with_version_requirement
    ):
        """Test actual version parsing fails with LHPError code 008."""
        # Arrange
        with (
            patch("lhp.core.orchestrator.get_version") as mock_get_version,
            patch("packaging.version.Version") as mock_version_class,
            patch("packaging.specifiers.SpecifierSet") as mock_specifier_set,
        ):
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

    def test_no_version_requirement_skips_enforcement_silently(
        self, orchestrator_with_version_requirement
    ):
        """Test no required_lhp_version skips enforcement silently."""
        # Arrange - remove version requirement
        orchestrator_with_version_requirement.project_config.required_lhp_version = None

        # Act - should complete silently without any version checks
        orchestrator_with_version_requirement._enforce_version_requirements()

        # Assert - no exception should be raised, and method should complete successfully

    def test_specifier_set_exception_wraps_in_error_code_008(
        self, orchestrator_with_version_requirement
    ):
        """Test SpecifierSet creation throws exception wraps in LHPError code 008."""
        # Arrange
        with (
            patch("lhp.core.orchestrator.get_version") as mock_get_version,
            patch("packaging.specifiers.SpecifierSet") as mock_specifier_set,
        ):
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

    def test_version_satisfied_completes_silently(
        self, orchestrator_with_version_requirement
    ):
        """Test version requirement satisfied completes silently."""
        # Arrange - use current version which should satisfy the requirement
        orchestrator_with_version_requirement.project_config.required_lhp_version = (
            ">=0.1.0"  # Very low requirement that should be satisfied
        )

        # Act - should complete silently without any exceptions or logging
        orchestrator_with_version_requirement._enforce_version_requirements()

        # Assert - no exception should be raised


class TestActionOrchestratorFlowgroupDiscovery:
    """Test ActionOrchestrator flowgroup discovery patterns."""

    @pytest.fixture
    def mock_project_root(self):
        """Mock project root path."""
        return Path("/mock/project")

    @pytest.fixture
    def orchestrator_basic(self, mock_project_root):
        """Create basic orchestrator for discovery testing."""
        with (
            patch("lhp.core.orchestrator.YAMLParser"),
            patch("lhp.core.orchestrator.PresetManager"),
            patch("lhp.core.orchestrator.TemplateEngine"),
            patch("lhp.core.orchestrator.ProjectConfigLoader") as mock_config_loader,
            patch("lhp.core.orchestrator.ActionRegistry"),
            patch("lhp.core.orchestrator.ConfigValidator"),
            patch("lhp.core.orchestrator.SecretValidator"),
            patch("lhp.core.orchestrator.DependencyResolver"),
            patch("lhp.core.orchestrator.FlowgroupDiscoverer") as mock_discoverer,
            patch("lhp.core.orchestrator.FlowgroupProcessor") as mock_processor,
            patch("lhp.core.orchestrator.CodeGenerator"),
            patch("lhp.core.orchestrator.PipelineValidator"),
        ):
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

        # Mock path exists to return False
        with patch.object(Path, "exists", return_value=False):
            # Act
            result = orchestrator_basic.generate_pipeline_by_field(
                pipeline_field=nonexistent_pipeline, env="dev"
            )

            # Assert - should return empty tuple, not raise exception
            assert result == ()
            assert isinstance(result, tuple)

    def test_no_flowgroups_found_returns_empty_dict(self, orchestrator_basic):
        """Test no flowgroups found returns empty tuple and logs warning."""
        # Arrange
        pipeline_name = "empty_pipeline"

        # Mock path exists to return True, but discoverer returns empty list
        with patch.object(Path, "exists", return_value=True):
            orchestrator_basic.mock_discoverer.discover_flowgroups.return_value = []

            # Act
            result = orchestrator_basic.generate_pipeline_by_field(
                pipeline_field=pipeline_name, env="dev"
            )

            # Assert - should return empty tuple, not raise exception
            assert result == ()
            assert isinstance(result, tuple)

    def test_include_patterns_filtering_applied_correctly(self, orchestrator_basic):
        """Test include patterns are applied correctly in filtering."""
        # Arrange
        expected_patterns = ["*.yaml", "specific_pattern"]
        orchestrator_basic.mock_discoverer.get_include_patterns.return_value = (
            expected_patterns
        )

        # Act
        result = orchestrator_basic.get_include_patterns()

        # Assert
        assert result == expected_patterns
        orchestrator_basic.mock_discoverer.get_include_patterns.assert_called_once()

    def test_duplicate_pipeline_flowgroup_combinations_raise_value_error(
        self, orchestrator_basic
    ):
        """Test duplicate pipeline+flowgroup combinations raise ValueError."""
        # Arrange
        from lhp.models.config import FlowGroup

        # Create mock flowgroups with duplicates
        mock_flowgroups = [
            Mock(
                spec=FlowGroup,
                pipeline="test_pipeline",
                flowgroup="duplicate_flowgroup",
            ),
            Mock(
                spec=FlowGroup,
                pipeline="test_pipeline",
                flowgroup="duplicate_flowgroup",
            ),  # Duplicate
        ]

        # Mock config validator to return error for duplicates
        orchestrator_basic.config_validator.validate_duplicate_pipeline_flowgroup.return_value = [
            "Duplicate combination: test_pipeline + duplicate_flowgroup"
        ]

        # Act & Assert
        with pytest.raises(
            ValueError, match="Duplicate pipeline\\+flowgroup combinations found"
        ):
            orchestrator_basic.validate_duplicate_pipeline_flowgroup_combinations(
                mock_flowgroups
            )

        # Verify validator was called
        orchestrator_basic.config_validator.validate_duplicate_pipeline_flowgroup.assert_called_once_with(
            mock_flowgroups
        )

    # At least one warning should be logged

    def test_pipeline_field_multiple_matches_returns_all(self, orchestrator_basic):
        """Test pipeline field matches multiple flowgroups returns all matches."""
        # Arrange
        from lhp.models.config import FlowGroup

        # Mock discoverer to return multiple matching flowgroups
        mock_flowgroup1 = Mock(
            spec=FlowGroup, pipeline="target_pipeline", flowgroup="flowgroup1"
        )
        mock_flowgroup2 = Mock(
            spec=FlowGroup, pipeline="target_pipeline", flowgroup="flowgroup2"
        )
        mock_flowgroup3 = Mock(
            spec=FlowGroup, pipeline="other_pipeline", flowgroup="flowgroup3"
        )

        all_flowgroups = [mock_flowgroup1, mock_flowgroup2, mock_flowgroup3]
        orchestrator_basic.mock_discoverer.discover_all_flowgroups.return_value = (
            all_flowgroups
        )

        # Act
        result = orchestrator_basic.discover_flowgroups_by_pipeline_field(
            "target_pipeline"
        )

        # Assert - should return only matching flowgroups
        assert len(result) == 2
        assert mock_flowgroup1 in result
        assert mock_flowgroup2 in result
        assert mock_flowgroup3 not in result

        # Verify discoverer was called
        orchestrator_basic.mock_discoverer.discover_all_flowgroups.assert_called_once()

    def test_pipeline_field_no_matches_returns_empty_with_warning(
        self, orchestrator_basic
    ):
        """Test pipeline field matches no flowgroups returns empty list with warning."""
        # Arrange
        from lhp.models.config import FlowGroup

        # Mock discoverer to return flowgroups with different pipeline fields
        mock_flowgroup1 = Mock(
            spec=FlowGroup, pipeline="other_pipeline1", flowgroup="flowgroup1"
        )
        mock_flowgroup2 = Mock(
            spec=FlowGroup, pipeline="other_pipeline2", flowgroup="flowgroup2"
        )

        all_flowgroups = [mock_flowgroup1, mock_flowgroup2]
        orchestrator_basic.mock_discoverer.discover_all_flowgroups.return_value = (
            all_flowgroups
        )

        # Mock logger to verify warning
        with patch.object(orchestrator_basic, "logger"):
            # Act
            result = orchestrator_basic.discover_flowgroups_by_pipeline_field(
                "nonexistent_pipeline"
            )

            # Assert - should return empty list
            assert result == []

            # Should not log warning at discovery level (warning happens in generate_pipeline_by_field)
            orchestrator_basic.mock_discoverer.discover_all_flowgroups.assert_called_once()

    def test_recursive_directory_search_finds_nested_flowgroups(
        self, orchestrator_basic
    ):
        """Test recursive directory search finds nested flowgroups."""
        # Arrange
        from lhp.models.config import FlowGroup

        pipeline_dir = orchestrator_basic.project_root / "pipelines" / "test_pipeline"

        # Mock discoverer to return flowgroups from nested directories
        mock_flowgroup1 = Mock(
            spec=FlowGroup, pipeline="test_pipeline", flowgroup="main_flowgroup"
        )
        mock_flowgroup2 = Mock(
            spec=FlowGroup, pipeline="test_pipeline", flowgroup="nested_flowgroup"
        )

        nested_flowgroups = [mock_flowgroup1, mock_flowgroup2]
        orchestrator_basic.mock_discoverer.discover_flowgroups.return_value = (
            nested_flowgroups
        )

        # Act
        result = orchestrator_basic.discover_flowgroups(pipeline_dir)

        # Assert - should return all flowgroups including nested ones
        assert result == nested_flowgroups
        assert len(result) == 2
        assert mock_flowgroup1 in result
        assert mock_flowgroup2 in result

        # Verify discoverer was called with correct pipeline directory
        orchestrator_basic.mock_discoverer.discover_flowgroups.assert_called_once_with(
            pipeline_dir
        )


class TestActionOrchestratorValidationWithoutGeneration:
    """Test ActionOrchestrator validation without generation logic."""

    @pytest.fixture
    def mock_project_root(self):
        """Mock project root path."""
        return Path("/mock/project")

    @pytest.fixture
    def orchestrator_validation(self, mock_project_root):
        """Create orchestrator for validation testing."""
        with (
            patch("lhp.core.orchestrator.YAMLParser"),
            patch("lhp.core.orchestrator.PresetManager"),
            patch("lhp.core.orchestrator.TemplateEngine"),
            patch("lhp.core.orchestrator.ProjectConfigLoader") as mock_config_loader,
            patch("lhp.core.orchestrator.ActionRegistry"),
            patch("lhp.core.orchestrator.ConfigValidator"),
            patch("lhp.core.orchestrator.SecretValidator"),
            patch("lhp.core.orchestrator.DependencyResolver"),
            patch("lhp.core.orchestrator.FlowgroupDiscoverer") as mock_discoverer,
            patch("lhp.core.orchestrator.FlowgroupProcessor") as mock_processor,
            patch("lhp.core.orchestrator.CodeGenerator"),
            patch("lhp.core.orchestrator.PipelineValidator"),
        ):
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

    def test_pipeline_field_validation_no_flowgroups_returns_specific_error(
        self, orchestrator_validation
    ):
        """Test pipeline field validation finds no flowgroups returns specific error."""
        # Arrange
        pipeline_field = "nonexistent_pipeline"
        env = "dev"

        # Mock the orchestrator's own discover_flowgroups_by_pipeline_field method
        with patch.object(
            orchestrator_validation,
            "discover_flowgroups_by_pipeline_field",
            return_value=[],
        ):
            # Act
            errors, warnings = orchestrator_validation.validate_pipeline_by_field(
                pipeline_field, env
            )

            # Assert
            assert len(errors) == 1  # One error about no flowgroups found
            assert warnings == []  # No warnings

            # Error should include specific message about pipeline field
            error_message = errors[0]
            assert (
                f"No flowgroups found for pipeline field: {pipeline_field}"
                in error_message
            )

            # Should call discover_flowgroups_by_pipeline_field. The shim
            # now delegates through validate_pipelines_by_fields which
            # pre-discovers all_flowgroups once and passes that list as
            # pre_discovered_all_flowgroups; the test asserts the call
            # happened with the right pipeline field, not the exact kwarg
            # value (which is now a non-None list).
            orchestrator_validation.discover_flowgroups_by_pipeline_field.assert_called_once()
            call_args = (
                orchestrator_validation.discover_flowgroups_by_pipeline_field.call_args
            )
            assert call_args.args[0] == pipeline_field

    def test_validate_pipeline_by_field_method_delegation(
        self, orchestrator_validation
    ):
        """Test validate_pipeline_by_field method calls correct discover method."""
        # This test verifies that the method properly delegates to discover_flowgroups_by_pipeline_field
        # Arrange
        pipeline_field = "test_pipeline"
        env = "dev"

        # Mock to return empty (which will trigger early return)
        with patch.object(
            orchestrator_validation,
            "discover_flowgroups_by_pipeline_field",
            return_value=[],
        ) as mock_discover:
            # Act
            errors, warnings = orchestrator_validation.validate_pipeline_by_field(
                pipeline_field, env
            )

            # Assert - method should delegate correctly. The plural method
            # in the new architecture pre-discovers all_flowgroups, so the
            # pre_discovered_all_flowgroups kwarg is no longer None.
            mock_discover.assert_called_once()
            assert mock_discover.call_args.args[0] == pipeline_field

            # Should return appropriate error for no flowgroups
            assert len(errors) == 1
            assert (
                f"No flowgroups found for pipeline field: {pipeline_field}" in errors[0]
            )


class TestActionOrchestratorFlowgroupProcessingPipeline:
    """Test ActionOrchestrator flowgroup processing pipeline methods."""

    @pytest.fixture
    def mock_project_root(self):
        """Mock project root path."""
        return Path("/mock/project")

    @pytest.fixture
    def orchestrator_processing(self, mock_project_root):
        """Create orchestrator for processing pipeline testing."""
        with (
            patch("lhp.core.orchestrator.YAMLParser"),
            patch("lhp.core.orchestrator.PresetManager"),
            patch("lhp.core.orchestrator.TemplateEngine"),
            patch("lhp.core.orchestrator.ProjectConfigLoader") as mock_config_loader,
            patch("lhp.core.orchestrator.ActionRegistry"),
            patch("lhp.core.orchestrator.ConfigValidator"),
            patch("lhp.core.orchestrator.SecretValidator"),
            patch("lhp.core.orchestrator.DependencyResolver"),
            patch("lhp.core.orchestrator.FlowgroupDiscoverer"),
            patch("lhp.core.orchestrator.FlowgroupProcessor") as mock_processor,
            patch("lhp.core.orchestrator.CodeGenerator") as mock_generator,
            patch("lhp.core.orchestrator.PipelineValidator"),
        ):
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

    def test_process_flowgroup_succeeds_returns_processed_flowgroup(
        self, orchestrator_processing, mock_flowgroup, mock_substitution_mgr
    ):
        """Test FlowgroupProcessor succeeds returns processed flowgroup."""
        # Arrange
        from lhp.models.config import FlowGroupContext

        processed_flowgroup = Mock(spec=FlowGroup)
        # The processor now returns a FlowGroupContext envelope; the shim
        # unwraps `.flowgroup` for backward-compatible single-fg callers.
        processed_ctx = FlowGroupContext(
            flowgroup=processed_flowgroup, source_yaml=None
        )
        orchestrator_processing.mock_processor.process_flowgroup.return_value = (
            processed_ctx
        )

        # Act
        result = orchestrator_processing.process_flowgroup(
            mock_flowgroup, mock_substitution_mgr
        )

        # Assert
        assert result == processed_flowgroup  # Should return processed flowgroup

        # Should delegate to processor service. The shim wraps the FlowGroup
        # in a FlowGroupContext envelope at the call site.
        call_args = orchestrator_processing.mock_processor.process_flowgroup.call_args
        passed_ctx = call_args.args[0]
        assert isinstance(passed_ctx, FlowGroupContext)
        assert passed_ctx.flowgroup is mock_flowgroup
        assert call_args.args[1] is mock_substitution_mgr
        assert call_args.kwargs.get("include_tests") is True

    def test_process_flowgroup_fails_propagates_exception(
        self, orchestrator_processing, mock_flowgroup, mock_substitution_mgr
    ):
        """Test FlowgroupProcessor fails propagates exception."""
        from lhp.models.config import FlowGroupContext

        # Arrange
        processor_error = Exception(
            "Flowgroup processing failed: missing required template"
        )
        orchestrator_processing.mock_processor.process_flowgroup.side_effect = (
            processor_error
        )

        # Act & Assert - should propagate the processor exception
        with pytest.raises(
            Exception, match="Flowgroup processing failed: missing required template"
        ):
            orchestrator_processing.process_flowgroup(
                mock_flowgroup, mock_substitution_mgr
            )

        # Should still call processor service with a FlowGroupContext envelope.
        call_args = orchestrator_processing.mock_processor.process_flowgroup.call_args
        passed_ctx = call_args.args[0]
        assert isinstance(passed_ctx, FlowGroupContext)
        assert passed_ctx.flowgroup is mock_flowgroup

    def test_generate_flowgroup_code_succeeds_returns_generated_code(
        self, orchestrator_processing, mock_flowgroup, mock_substitution_mgr
    ):
        """Test CodeGenerator succeeds returns generated code."""
        # Arrange
        expected_code = "# Generated Python code\nimport pandas as pd\n# ...\n"
        output_dir = Path("/output")
        source_yaml = Path("/source/flowgroup.yaml")
        env = "dev"
        include_tests = False

        orchestrator_processing.mock_generator.generate_flowgroup_code.return_value = (
            expected_code
        )

        # Act
        result = orchestrator_processing.generate_flowgroup_code(
            mock_flowgroup,
            mock_substitution_mgr,
            output_dir,
            source_yaml,
            env,
            include_tests,
        )

        # Assert
        assert result == expected_code  # Should return generated code

        # Should delegate to generator service with all correct arguments
        orchestrator_processing.mock_generator.generate_flowgroup_code.assert_called_once_with(
            mock_flowgroup,
            mock_substitution_mgr,
            output_dir,
            source_yaml,
            env,
            include_tests,
            phase_a_records=None,
        )

    def test_generate_flowgroup_code_fails_propagates_exception(
        self, orchestrator_processing, mock_flowgroup, mock_substitution_mgr
    ):
        """Test CodeGenerator fails propagates exception."""
        # Arrange
        generator_error = Exception("Code generation failed: template not found")
        orchestrator_processing.mock_generator.generate_flowgroup_code.side_effect = (
            generator_error
        )

        output_dir = Path("/output")
        source_yaml = Path("/source/flowgroup.yaml")
        env = "dev"
        include_tests = False

        # Act & Assert - should propagate the generator exception
        with pytest.raises(
            Exception, match="Code generation failed: template not found"
        ):
            orchestrator_processing.generate_flowgroup_code(
                mock_flowgroup,
                mock_substitution_mgr,
                output_dir,
                source_yaml,
                env,
                include_tests,
            )

        # Should still call generator service
        orchestrator_processing.mock_generator.generate_flowgroup_code.assert_called_once_with(
            mock_flowgroup,
            mock_substitution_mgr,
            output_dir,
            source_yaml,
            env,
            include_tests,
            phase_a_records=None,
        )

    def test_substitution_mgr_none_still_delegates_to_services(
        self, orchestrator_processing, mock_flowgroup
    ):
        """Test substitution_mgr is None still delegates to services."""
        from lhp.models.config import FlowGroupContext

        # Arrange
        processed_flowgroup = Mock(spec=FlowGroup)
        processed_ctx = FlowGroupContext(
            flowgroup=processed_flowgroup, source_yaml=None
        )
        generated_code = "# Generated code without substitution\n"

        # Test both methods with None substitution manager
        orchestrator_processing.mock_processor.process_flowgroup.return_value = (
            processed_ctx
        )
        orchestrator_processing.mock_generator.generate_flowgroup_code.return_value = (
            generated_code
        )

        # Act - process_flowgroup with None substitution
        result1 = orchestrator_processing.process_flowgroup(mock_flowgroup, None)

        # Act - generate_flowgroup_code with None substitution
        result2 = orchestrator_processing.generate_flowgroup_code(mock_flowgroup, None)

        # Assert
        assert result1 == processed_flowgroup
        assert result2 == generated_code

        # Should still delegate to services. The shim wraps in FlowGroupContext.
        call_args = orchestrator_processing.mock_processor.process_flowgroup.call_args
        passed_ctx = call_args.args[0]
        assert isinstance(passed_ctx, FlowGroupContext)
        assert passed_ctx.flowgroup is mock_flowgroup
        assert call_args.args[1] is None
        assert call_args.kwargs.get("include_tests") is True
        orchestrator_processing.mock_generator.generate_flowgroup_code.assert_called_once_with(
            mock_flowgroup,
            None,
            None,
            None,
            None,
            False,
            phase_a_records=None,
        )

    def test_include_tests_true_passes_to_code_generator(
        self, orchestrator_processing, mock_flowgroup, mock_substitution_mgr
    ):
        """Test include_tests=True passes to CodeGenerator."""
        # Arrange
        generated_code = "# Generated code with tests\nimport pytest\n# ...\n"
        orchestrator_processing.mock_generator.generate_flowgroup_code.return_value = (
            generated_code
        )

        output_dir = Path("/output")
        source_yaml = Path("/source/flowgroup.yaml")
        env = "dev"
        include_tests = True  # Key parameter being tested

        # Act
        result = orchestrator_processing.generate_flowgroup_code(
            mock_flowgroup,
            mock_substitution_mgr,
            output_dir,
            source_yaml,
            env,
            include_tests,
        )

        # Assert
        assert result == generated_code

        # Should pass include_tests=True to generator service
        orchestrator_processing.mock_generator.generate_flowgroup_code.assert_called_once_with(
            mock_flowgroup,
            mock_substitution_mgr,
            output_dir,
            source_yaml,
            env,
            True,
            phase_a_records=None,
        )

    def test_include_tests_false_passes_to_code_generator(
        self, orchestrator_processing, mock_flowgroup, mock_substitution_mgr
    ):
        """Test include_tests=False passes to CodeGenerator."""
        # Arrange
        generated_code = "# Generated code without tests\nimport pandas as pd\n# ...\n"
        orchestrator_processing.mock_generator.generate_flowgroup_code.return_value = (
            generated_code
        )

        output_dir = Path("/output")
        source_yaml = Path("/source/flowgroup.yaml")
        env = "dev"
        include_tests = False  # Key parameter being tested

        # Act
        result = orchestrator_processing.generate_flowgroup_code(
            mock_flowgroup,
            mock_substitution_mgr,
            output_dir,
            source_yaml,
            env,
            include_tests,
        )

        # Assert
        assert result == generated_code

        # Should pass include_tests=False to generator service
        orchestrator_processing.mock_generator.generate_flowgroup_code.assert_called_once_with(
            mock_flowgroup,
            mock_substitution_mgr,
            output_dir,
            source_yaml,
            env,
            False,
            phase_a_records=None,
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
        with (
            patch("lhp.core.orchestrator.YAMLParser"),
            patch("lhp.core.orchestrator.PresetManager"),
            patch("lhp.core.orchestrator.TemplateEngine"),
            patch("lhp.core.orchestrator.ProjectConfigLoader") as mock_config_loader,
            patch("lhp.core.orchestrator.ActionRegistry"),
            patch("lhp.core.orchestrator.ConfigValidator"),
            patch("lhp.core.orchestrator.SecretValidator"),
            patch("lhp.core.orchestrator.DependencyResolver"),
            patch("lhp.core.orchestrator.FlowgroupDiscoverer") as mock_discoverer,
            patch("lhp.core.orchestrator.FlowgroupProcessor") as mock_processor,
            patch("lhp.core.orchestrator.CodeGenerator") as mock_generator,
            patch("lhp.core.orchestrator.PipelineValidator"),
        ):
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

    def test_service_method_unexpected_exception_propagates_with_context(
        self, orchestrator_error_handling, mock_flowgroup
    ):
        """Test any service method throws unexpected exception propagates with context."""
        # Arrange
        from lhp.utils.substitution import EnhancedSubstitutionManager

        mock_substitution_mgr = Mock(spec=EnhancedSubstitutionManager)

        # Test various service methods with unexpected exceptions
        unexpected_error = RuntimeError(
            "Unexpected service failure: database connection lost"
        )

        # Test processor service exception
        orchestrator_error_handling.mock_processor.process_flowgroup.side_effect = (
            unexpected_error
        )

        # Act & Assert - should propagate exception with context
        with pytest.raises(
            RuntimeError, match="Unexpected service failure: database connection lost"
        ):
            orchestrator_error_handling.process_flowgroup(
                mock_flowgroup, mock_substitution_mgr
            )

        # Reset for next test
        orchestrator_error_handling.mock_processor.process_flowgroup.side_effect = None

        # Test generator service exception
        orchestrator_error_handling.mock_generator.generate_flowgroup_code.side_effect = unexpected_error

        with pytest.raises(
            RuntimeError, match="Unexpected service failure: database connection lost"
        ):
            orchestrator_error_handling.generate_flowgroup_code(
                mock_flowgroup, mock_substitution_mgr
            )

        # Should have attempted to call both services despite failures.
        # The shim wraps the FlowGroup in a FlowGroupContext envelope.
        call_args = (
            orchestrator_error_handling.mock_processor.process_flowgroup.call_args
        )
        passed_ctx = call_args.args[0]
        assert isinstance(passed_ctx, FlowGroupContext)
        assert passed_ctx.flowgroup is mock_flowgroup
        assert call_args.args[1] is mock_substitution_mgr
        assert call_args.kwargs.get("include_tests") is True
        orchestrator_error_handling.mock_generator.generate_flowgroup_code.assert_called_once_with(
            mock_flowgroup,
            mock_substitution_mgr,
            None,
            None,
            None,
            False,
            phase_a_records=None,
        )

    def test_logging_operations_fail_does_not_break_main_functionality(
        self, orchestrator_error_handling, mock_flowgroup
    ):
        """Test logging operations fail does not break main functionality."""
        # Arrange
        from lhp.utils.substitution import EnhancedSubstitutionManager

        mock_substitution_mgr = Mock(spec=EnhancedSubstitutionManager)
        processed_flowgroup = Mock(spec=FlowGroup)
        generated_code = "# Generated code\n"

        orchestrator_error_handling.mock_processor.process_flowgroup.return_value = (
            _wrap_in_ctx(processed_flowgroup)
        )
        orchestrator_error_handling.mock_generator.generate_flowgroup_code.return_value = generated_code

        # Mock logger to raise exception during logging
        with patch.object(orchestrator_error_handling, "logger") as mock_logger:
            mock_logger.info.side_effect = Exception("Logging system failure")
            mock_logger.debug.side_effect = Exception("Logging system failure")
            mock_logger.warning.side_effect = Exception("Logging system failure")

            # Act - should not fail despite logging errors
            result1 = orchestrator_error_handling.process_flowgroup(
                mock_flowgroup, mock_substitution_mgr
            )
            result2 = orchestrator_error_handling.generate_flowgroup_code(
                mock_flowgroup, mock_substitution_mgr
            )

            # Assert - main functionality should work despite logging failures
            assert result1 == processed_flowgroup
            assert result2 == generated_code

            # Services should still be called successfully. The shim wraps
            # the FlowGroup in a FlowGroupContext envelope.
            call_args = (
                orchestrator_error_handling.mock_processor.process_flowgroup.call_args
            )
            passed_ctx = call_args.args[0]
            assert isinstance(passed_ctx, FlowGroupContext)
            assert passed_ctx.flowgroup is mock_flowgroup
            assert call_args.args[1] is mock_substitution_mgr
            orchestrator_error_handling.mock_generator.generate_flowgroup_code.assert_called_once_with(
                mock_flowgroup,
                mock_substitution_mgr,
                None,
                None,
                None,
                False,
                phase_a_records=None,
            )

    def test_invalid_parameters_passed_delegates_to_services(
        self, orchestrator_error_handling
    ):
        """Test invalid parameters are passed through to services for validation."""
        # Arrange - Test parameter delegation with various input types
        from lhp.utils.substitution import EnhancedSubstitutionManager

        mock_substitution_mgr = Mock(spec=EnhancedSubstitutionManager)

        # Configure services to handle validation and provide appropriate responses
        orchestrator_error_handling.mock_processor.process_flowgroup.side_effect = [
            ValueError("Service validation: None flowgroup not allowed"),
            TypeError("Service validation: Invalid flowgroup type"),
            Mock(),  # Valid response for last test
        ]

        orchestrator_error_handling.mock_generator.generate_flowgroup_code.side_effect = [
            TypeError("Service validation: Invalid substitution manager type"),
            "generated_code",  # Valid response for last test
        ]

        # Test 1: None flowgroup parameter - service should validate and raise error
        with pytest.raises(
            ValueError, match="Service validation: None flowgroup not allowed"
        ):
            orchestrator_error_handling.process_flowgroup(None, mock_substitution_mgr)

        # Test 2: Invalid flowgroup type - service should validate and raise error
        with pytest.raises(
            TypeError, match="Service validation: Invalid flowgroup type"
        ):
            orchestrator_error_handling.process_flowgroup(
                "invalid_flowgroup", mock_substitution_mgr
            )

        # Test 3: Invalid substitution manager type for generate_flowgroup_code
        invalid_substitution_mgr = "not_a_substitution_manager"
        mock_flowgroup = Mock(spec=FlowGroup)

        with pytest.raises(
            TypeError, match="Service validation: Invalid substitution manager type"
        ):
            orchestrator_error_handling.generate_flowgroup_code(
                mock_flowgroup, invalid_substitution_mgr
            )

        # Test 4: Valid parameters should work normally
        result = orchestrator_error_handling.process_flowgroup(
            mock_flowgroup, mock_substitution_mgr
        )
        assert result is not None  # Should return mocked result

        result2 = orchestrator_error_handling.generate_flowgroup_code(
            mock_flowgroup, mock_substitution_mgr
        )
        assert result2 == "generated_code"  # Should return mocked result

        # Verify all calls were made to services (delegation working correctly)
        assert (
            orchestrator_error_handling.mock_processor.process_flowgroup.call_count == 3
        )
        assert (
            orchestrator_error_handling.mock_generator.generate_flowgroup_code.call_count
            == 2
        )

    def test_edge_case_empty_and_invalid_pipeline_fields(
        self, orchestrator_error_handling
    ):
        """Test edge cases with empty and invalid pipeline field values."""
        # Test empty pipeline field
        with patch.object(
            orchestrator_error_handling,
            "discover_flowgroups_by_pipeline_field",
            return_value=[],
        ) as mock_discover:
            errors, warnings = orchestrator_error_handling.validate_pipeline_by_field(
                "", "dev"
            )

            # Should handle empty string gracefully
            assert len(errors) == 1
            assert "No flowgroups found for pipeline field:" in errors[0]
            mock_discover.assert_called_once()
            assert mock_discover.call_args.args[0] == ""

        # Test None pipeline field - should raise exception or handle gracefully
        with patch.object(
            orchestrator_error_handling,
            "discover_flowgroups_by_pipeline_field",
            side_effect=ValueError("Pipeline field cannot be None"),
        ):
            errors, warnings = orchestrator_error_handling.validate_pipeline_by_field(
                None, "dev"
            )

            # Should catch the ValueError and return it as an error
            assert len(errors) == 1
            assert "Pipeline validation failed" in errors[0]
            assert "Pipeline field cannot be None" in errors[0]

        # Test whitespace-only pipeline field
        with patch.object(
            orchestrator_error_handling,
            "discover_flowgroups_by_pipeline_field",
            return_value=[],
        ) as mock_discover_whitespace:
            errors, warnings = orchestrator_error_handling.validate_pipeline_by_field(
                "   ", "dev"
            )

            # Should handle whitespace-only string
            assert len(errors) == 1
            assert "No flowgroups found for pipeline field:    " in errors[0]
            mock_discover_whitespace.assert_called_once()
            assert mock_discover_whitespace.call_args.args[0] == "   "

    def test_edge_case_service_returns_none_or_empty_results(
        self, orchestrator_error_handling, mock_flowgroup
    ):
        """Test edge cases where services return None or empty results."""
        # Arrange
        from lhp.utils.substitution import EnhancedSubstitutionManager

        mock_substitution_mgr = Mock(spec=EnhancedSubstitutionManager)

        # Test processor returning a context with None flowgroup. The
        # processor's new contract is FlowGroupContext-in / -out; the
        # legacy `process_flowgroup` shim unwraps `.flowgroup` so a
        # None inner flowgroup surfaces as a None result.
        orchestrator_error_handling.mock_processor.process_flowgroup.return_value = (
            _wrap_in_ctx(None)
        )
        result = orchestrator_error_handling.process_flowgroup(
            mock_flowgroup, mock_substitution_mgr
        )
        assert result is None  # Should handle None inner flowgroup gracefully

        # Test generator returning empty string
        orchestrator_error_handling.mock_generator.generate_flowgroup_code.return_value = ""
        result = orchestrator_error_handling.generate_flowgroup_code(
            mock_flowgroup, mock_substitution_mgr
        )
        assert result == ""  # Should handle empty string return gracefully

        # Test generator returning None
        orchestrator_error_handling.mock_generator.generate_flowgroup_code.return_value = None
        result = orchestrator_error_handling.generate_flowgroup_code(
            mock_flowgroup, mock_substitution_mgr
        )
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
        with (
            patch("lhp.core.orchestrator.YAMLParser"),
            patch("lhp.core.orchestrator.PresetManager"),
            patch("lhp.core.orchestrator.TemplateEngine"),
            patch("lhp.core.orchestrator.ProjectConfigLoader") as mock_config_loader,
            patch("lhp.core.orchestrator.ActionRegistry"),
            patch("lhp.core.orchestrator.ConfigValidator"),
            patch("lhp.core.orchestrator.SecretValidator"),
            patch("lhp.core.orchestrator.DependencyResolver"),
            patch("lhp.core.orchestrator.FlowgroupDiscoverer") as mock_discoverer,
            patch("lhp.core.orchestrator.FlowgroupProcessor") as mock_processor,
            patch("lhp.core.orchestrator.CodeGenerator") as mock_generator,
            patch("lhp.core.orchestrator.PipelineValidator"),
        ):
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

    def test_services_dependency_conflicts_detected_and_reported(
        self, orchestrator_integration
    ):
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
        orchestrator_integration.mock_processor.process_flowgroup.return_value = (
            _wrap_in_ctx(processed_flowgroup)
        )
        orchestrator_integration.mock_generator.generate_flowgroup_code.side_effect = ValueError(
            "Generator conflict: Processed flowgroup format incompatible with generator expectations"
        )

        # Act & Assert - Should detect and report the dependency conflict
        with pytest.raises(
            ValueError,
            match="Generator conflict: Processed flowgroup format incompatible",
        ):
            orchestrator_integration.generate_flowgroup_code(
                processed_flowgroup, mock_substitution_mgr
            )

        # Should still call processor successfully
        result = orchestrator_integration.process_flowgroup(
            mock_flowgroup, mock_substitution_mgr
        )
        assert result == processed_flowgroup

        # But generator fails due to dependency conflict.
        # The shim wraps the FlowGroup in a FlowGroupContext envelope.
        call_args = orchestrator_integration.mock_processor.process_flowgroup.call_args
        passed_ctx = call_args.args[0]
        assert isinstance(passed_ctx, FlowGroupContext)
        assert passed_ctx.flowgroup is mock_flowgroup
        assert call_args.args[1] is mock_substitution_mgr
        assert call_args.kwargs.get("include_tests") is True
        orchestrator_integration.mock_generator.generate_flowgroup_code.assert_called_once_with(
            processed_flowgroup,
            mock_substitution_mgr,
            None,
            None,
            None,
            False,
            phase_a_records=None,
        )

    def test_service_coordination_with_complex_data_flow(
        self, orchestrator_integration
    ):
        """Test service coordination handles complex data flow between services."""
        # Arrange - Test data transformation through service chain
        from lhp.models.config import FlowGroup
        from lhp.utils.substitution import EnhancedSubstitutionManager

        # Create complex flowgroup with multiple attributes
        original_flowgroup = Mock(spec=FlowGroup)
        original_flowgroup.flowgroup = "complex_flowgroup"
        original_flowgroup.pipeline = "data_pipeline"
        original_flowgroup.actions = [
            {"type": "read"},
            {"type": "transform"},
            {"type": "write"},
        ]

        # Create transformed flowgroup (processor output)
        transformed_flowgroup = Mock(spec=FlowGroup)
        transformed_flowgroup.flowgroup = "complex_flowgroup"
        transformed_flowgroup.pipeline = "data_pipeline"
        transformed_flowgroup.actions = [
            {"type": "read", "processed": True},
            {"type": "transform", "processed": True},
            {"type": "write", "processed": True},
        ]
        transformed_flowgroup.metadata = {
            "processed_by": "FlowgroupProcessor",
            "timestamp": "2023-01-01",
        }

        mock_substitution_mgr = Mock(spec=EnhancedSubstitutionManager)

        # Configure services to transform data through the chain.
        # Processor returns a FlowGroupContext envelope; the shim unwraps
        # `.flowgroup` for the legacy single-fg caller signature.
        orchestrator_integration.mock_processor.process_flowgroup.return_value = (
            _wrap_in_ctx(transformed_flowgroup)
        )
        orchestrator_integration.mock_generator.generate_flowgroup_code.return_value = (
            "# Generated from transformed data\n"
        )

        # Act - Pass data through service chain
        step1_result = orchestrator_integration.process_flowgroup(
            original_flowgroup, mock_substitution_mgr
        )
        step2_result = orchestrator_integration.generate_flowgroup_code(
            step1_result, mock_substitution_mgr
        )

        # Assert - Data flow maintained through service coordination
        assert step1_result == transformed_flowgroup
        assert step2_result == "# Generated from transformed data\n"

        # Verify proper data flow: original -> processor -> generator.
        # The shim wraps the FlowGroup in a FlowGroupContext envelope.
        call_args = orchestrator_integration.mock_processor.process_flowgroup.call_args
        passed_ctx = call_args.args[0]
        assert isinstance(passed_ctx, FlowGroupContext)
        assert passed_ctx.flowgroup is original_flowgroup
        assert call_args.args[1] is mock_substitution_mgr
        assert call_args.kwargs.get("include_tests") is True
        orchestrator_integration.mock_generator.generate_flowgroup_code.assert_called_once_with(
            transformed_flowgroup,
            mock_substitution_mgr,
            None,
            None,
            None,
            False,
            phase_a_records=None,
        )

    def test_service_integration_error_propagation_and_recovery(
        self, orchestrator_integration
    ):
        """Test service integration shows proper error propagation and recovery patterns."""
        # Arrange - Test error handling through service chain
        from lhp.models.config import FlowGroup
        from lhp.utils.substitution import EnhancedSubstitutionManager

        mock_flowgroup = Mock(spec=FlowGroup)
        mock_substitution_mgr = Mock(spec=EnhancedSubstitutionManager)

        # Configure processor to initially fail, then succeed on retry.
        # The processor now returns a FlowGroupContext envelope; the shim
        # unwraps `.flowgroup` for the legacy single-fg caller signature.
        processor_failure = Exception("Temporary processor failure")
        processed_flowgroup = Mock(spec=FlowGroup)
        processed_flowgroup.flowgroup = "test_flowgroup"
        orchestrator_integration.mock_processor.process_flowgroup.side_effect = [
            processor_failure,
            _wrap_in_ctx(processed_flowgroup),
        ]

        # Configure generator to succeed
        generated_code = "# Recovered generated code\n"
        orchestrator_integration.mock_generator.generate_flowgroup_code.return_value = (
            generated_code
        )

        # Act 1 - First attempt should fail at processor stage
        with pytest.raises(Exception, match="Temporary processor failure"):
            orchestrator_integration.process_flowgroup(
                mock_flowgroup, mock_substitution_mgr
            )

        # Act 2 - Retry should succeed (simulating recovery)
        result1 = orchestrator_integration.process_flowgroup(
            mock_flowgroup, mock_substitution_mgr
        )
        result2 = orchestrator_integration.generate_flowgroup_code(
            processed_flowgroup, mock_substitution_mgr
        )

        # Assert - Services recover and work properly in sequence
        assert result1 == processed_flowgroup
        assert result2 == generated_code

        # Verify error propagation and recovery: processor was called twice (fail + success)
        assert (
            orchestrator_integration.mock_processor.process_flowgroup.call_count == 2
        )  # Failed attempt + successful attempt
        assert (
            orchestrator_integration.mock_generator.generate_flowgroup_code.call_count
            == 1
        )  # Only successful call

    def test_service_parameter_validation_integration(self, orchestrator_integration):
        """Test service integration with parameter validation across service boundaries."""
        # Arrange - Test parameter flow and validation through services
        from lhp.models.config import FlowGroup
        from lhp.utils.substitution import EnhancedSubstitutionManager

        mock_flowgroup = Mock(spec=FlowGroup)
        mock_substitution_mgr = Mock(spec=EnhancedSubstitutionManager)

        # Test various parameter combinations through service chain
        orchestrator_integration.mock_processor.process_flowgroup.return_value = (
            _wrap_in_ctx(mock_flowgroup)
        )
        orchestrator_integration.mock_generator.generate_flowgroup_code.return_value = (
            "generated_code"
        )

        # Test parameters are passed correctly through service chain
        output_dir = Path("/output")
        source_yaml = Path("/source.yaml")
        env = "test"
        include_tests = True

        # Act - Pass parameters through service methods
        result1 = orchestrator_integration.process_flowgroup(
            mock_flowgroup, mock_substitution_mgr
        )
        result2 = orchestrator_integration.generate_flowgroup_code(
            result1,
            mock_substitution_mgr,
            output_dir,
            source_yaml,
            env,
            include_tests,
        )

        # Assert - Parameters flow correctly through service integration
        assert result1 == mock_flowgroup
        assert result2 == "generated_code"

        # Verify parameter passing. The shim wraps the FlowGroup in a
        # FlowGroupContext envelope.
        call_args = orchestrator_integration.mock_processor.process_flowgroup.call_args
        passed_ctx = call_args.args[0]
        assert isinstance(passed_ctx, FlowGroupContext)
        assert passed_ctx.flowgroup is mock_flowgroup
        assert call_args.args[1] is mock_substitution_mgr
        assert call_args.kwargs.get("include_tests") is True
        orchestrator_integration.mock_generator.generate_flowgroup_code.assert_called_once_with(
            mock_flowgroup,
            mock_substitution_mgr,
            output_dir,
            source_yaml,
            env,
            include_tests,
            phase_a_records=None,
        )
