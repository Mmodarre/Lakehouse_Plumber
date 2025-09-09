"""Tests for PipelineValidator service."""

import pytest
from unittest.mock import Mock, patch, MagicMock
from pathlib import Path

from lhp.core.services.pipeline_validator import PipelineValidator
from lhp.models.config import FlowGroup


class TestPipelineValidator:
    """Test cases covering all major business logic branches for PipelineValidator."""

    @pytest.fixture
    def project_root(self):
        """Mock project root path."""
        return Path("/mock/project")

    @pytest.fixture  
    def mock_config_validator(self):
        """Mock config validator."""
        return Mock()

    @pytest.fixture
    def mock_secret_validator(self):
        """Mock secret validator."""
        return Mock()

    @pytest.fixture
    def validator(self, project_root, mock_config_validator, mock_secret_validator):
        """Create PipelineValidator instance with mocked dependencies."""
        return PipelineValidator(
            project_root=project_root,
            config_validator=mock_config_validator,
            secret_validator=mock_secret_validator
        )

    @pytest.fixture
    def minimal_validator(self, project_root):
        """Create PipelineValidator with minimal configuration."""
        return PipelineValidator(project_root=project_root)

    @pytest.fixture
    def mock_flowgroup(self):
        """Create a mock FlowGroup."""
        flowgroup = Mock(spec=FlowGroup)
        flowgroup.flowgroup = "test_flowgroup"
        flowgroup.actions = [Mock()]  # Non-empty actions list
        return flowgroup

    @pytest.fixture
    def empty_flowgroup(self):
        """Create a mock FlowGroup with no actions."""
        flowgroup = Mock(spec=FlowGroup)
        flowgroup.flowgroup = "empty_flowgroup"
        flowgroup.actions = []  # Empty actions list
        return flowgroup

    def test_initialization_with_all_dependencies(self, validator, project_root, mock_config_validator, mock_secret_validator):
        """Test PipelineValidator initialization with all dependencies."""
        assert validator.project_root == project_root
        assert validator.config_validator == mock_config_validator
        assert validator.secret_validator == mock_secret_validator
        assert validator.logger is not None

    def test_initialization_minimal_config(self, minimal_validator, project_root):
        """Test PipelineValidator initialization with minimal configuration."""
        assert minimal_validator.project_root == project_root
        assert minimal_validator.config_validator is None
        assert minimal_validator.secret_validator is None
        assert minimal_validator.logger is not None

    @patch('lhp.core.services.pipeline_validator.EnhancedSubstitutionManager')
    def test_validate_pipeline_by_field_success_with_processor(self, mock_substitution_manager, validator, mock_flowgroup):
        """Test successful validation by field with discoverer and processor."""
        # Arrange
        mock_discoverer = Mock()
        mock_processor = Mock()
        mock_discoverer.discover_flowgroups_by_pipeline_field.return_value = [mock_flowgroup]
        mock_substitution_mgr_instance = Mock()
        mock_substitution_manager.return_value = mock_substitution_mgr_instance
        
        # Act
        errors, warnings = validator.validate_pipeline_by_field(
            pipeline_field="test_pipeline",
            env="dev", 
            discoverer=mock_discoverer,
            processor=mock_processor
        )
        
        # Assert
        assert errors == []
        assert warnings == []
        mock_discoverer.discover_flowgroups_by_pipeline_field.assert_called_once_with("test_pipeline")
        mock_processor.process_flowgroup.assert_called_once_with(mock_flowgroup, mock_substitution_mgr_instance)

    def test_validate_pipeline_by_field_no_discoverer(self, validator):
        """Test validate_pipeline_by_field with no discoverer fallback."""
        # Act
        errors, warnings = validator.validate_pipeline_by_field(
            pipeline_field="test_pipeline",
            env="dev",
            discoverer=None,  # No discoverer provided
            processor=None
        )
        
        # Assert
        assert errors == ["No flowgroup discoverer available"]
        assert warnings == []

    @patch('lhp.core.services.pipeline_validator.EnhancedSubstitutionManager')
    def test_validate_pipeline_by_field_no_flowgroups_found(self, mock_substitution_manager, validator):
        """Test validate_pipeline_by_field when no flowgroups found."""
        # Arrange
        mock_discoverer = Mock()
        mock_processor = Mock()
        mock_discoverer.discover_flowgroups_by_pipeline_field.return_value = []  # No flowgroups found
        
        # Act
        errors, warnings = validator.validate_pipeline_by_field(
            pipeline_field="nonexistent_pipeline",
            env="dev",
            discoverer=mock_discoverer,
            processor=mock_processor
        )
        
        # Assert
        assert errors == ["No flowgroups found for pipeline field: nonexistent_pipeline"]
        assert warnings == []
        mock_discoverer.discover_flowgroups_by_pipeline_field.assert_called_once_with("nonexistent_pipeline")
        # processor should not be called since no flowgroups found
        mock_processor.process_flowgroup.assert_not_called()

    @patch('lhp.core.services.pipeline_validator.EnhancedSubstitutionManager')
    def test_validate_pipeline_by_field_exception_handling(self, mock_substitution_manager, validator, mock_flowgroup):
        """Test validate_pipeline_by_field exception handling during flowgroup processing."""
        # Arrange
        mock_discoverer = Mock()
        mock_processor = Mock()
        mock_discoverer.discover_flowgroups_by_pipeline_field.return_value = [mock_flowgroup]
        mock_substitution_mgr_instance = Mock()
        mock_substitution_manager.return_value = mock_substitution_mgr_instance
        # Simulate processor raising exception
        mock_processor.process_flowgroup.side_effect = Exception("Processing failed")
        
        # Act
        errors, warnings = validator.validate_pipeline_by_field(
            pipeline_field="test_pipeline",
            env="dev",
            discoverer=mock_discoverer,
            processor=mock_processor
        )
        
        # Assert
        assert len(errors) == 1
        assert "Flowgroup 'test_flowgroup': Processing failed" in errors
        assert warnings == []

    @patch('lhp.core.services.pipeline_validator.EnhancedSubstitutionManager')
    def test_validate_pipeline_by_directory_success(self, mock_substitution_manager, validator, mock_flowgroup):
        """Test successful validation by directory with discoverer and processor."""
        # Arrange
        mock_discoverer = Mock()
        mock_processor = Mock()
        mock_discoverer.discover_flowgroups.return_value = [mock_flowgroup]
        mock_substitution_mgr_instance = Mock()
        mock_substitution_manager.return_value = mock_substitution_mgr_instance
        
        # Act
        errors, warnings = validator.validate_pipeline_by_directory(
            pipeline_name="test_pipeline",
            env="dev",
            discoverer=mock_discoverer,
            processor=mock_processor
        )
        
        # Assert
        assert errors == []
        assert warnings == []
        # Verify correct directory path is used
        expected_path = validator.project_root / "pipelines" / "test_pipeline"
        mock_discoverer.discover_flowgroups.assert_called_once_with(expected_path)
        mock_processor.process_flowgroup.assert_called_once_with(mock_flowgroup, mock_substitution_mgr_instance)

    def test_validate_pipeline_by_directory_no_discoverer(self, validator):
        """Test validate_pipeline_by_directory with no discoverer."""
        # Act
        errors, warnings = validator.validate_pipeline_by_directory(
            pipeline_name="test_pipeline",
            env="dev",
            discoverer=None,  # No discoverer provided
            processor=None
        )
        
        # Assert
        assert errors == ["No flowgroup discoverer available"]
        assert warnings == []

    @patch('lhp.core.services.pipeline_validator.EnhancedSubstitutionManager')
    def test_validate_pipeline_by_directory_exception_handling(self, mock_substitution_manager, validator, mock_flowgroup):
        """Test validate_pipeline_by_directory exception handling during flowgroup processing."""
        # Arrange
        mock_discoverer = Mock()
        mock_processor = Mock()
        mock_discoverer.discover_flowgroups.return_value = [mock_flowgroup]
        mock_substitution_mgr_instance = Mock()
        mock_substitution_manager.return_value = mock_substitution_mgr_instance
        # Simulate processor raising exception
        mock_processor.process_flowgroup.side_effect = Exception("Directory processing failed")
        
        # Act
        errors, warnings = validator.validate_pipeline_by_directory(
            pipeline_name="test_pipeline",
            env="dev",
            discoverer=mock_discoverer,
            processor=mock_processor
        )
        
        # Assert
        assert len(errors) == 1
        assert "Flowgroup 'test_flowgroup': Directory processing failed" in errors
        assert warnings == []

    def test_validate_flowgroup_basic_with_config_validator_errors(self, validator, mock_flowgroup):
        """Test _validate_flowgroup_basic when config_validator finds errors."""
        # Arrange
        validator.config_validator.validate_flowgroup.return_value = ["validation error 1", "validation error 2"]
        
        # Act & Assert
        with pytest.raises(ValueError) as exc_info:
            validator._validate_flowgroup_basic(mock_flowgroup)
        
        # Assert error message contains validation details
        assert "Flowgroup validation failed: ['validation error 1', 'validation error 2']" in str(exc_info.value)
        validator.config_validator.validate_flowgroup.assert_called_once_with(mock_flowgroup)

    def test_validate_flowgroup_basic_no_actions_fallback(self, minimal_validator, empty_flowgroup):
        """Test _validate_flowgroup_basic fallback when no config_validator and no actions."""
        # Act & Assert
        with pytest.raises(ValueError) as exc_info:
            minimal_validator._validate_flowgroup_basic(empty_flowgroup)
        
        # Assert error message about missing actions
        assert "Flowgroup must have at least one action" in str(exc_info.value)

    def test_validate_action_dependencies_with_resolver(self, validator):
        """Test validate_action_dependencies with dependency resolver."""
        # Arrange
        mock_actions = [Mock(), Mock()]
        mock_dependency_resolver = Mock()
        validator.config_validator.dependency_resolver = mock_dependency_resolver
        expected_errors = ["dependency error 1", "dependency error 2"]
        mock_dependency_resolver.validate_relationships.return_value = expected_errors
        
        # Act
        result = validator.validate_action_dependencies(mock_actions)
        
        # Assert
        assert result == expected_errors
        mock_dependency_resolver.validate_relationships.assert_called_once_with(mock_actions)

    def test_validate_action_dependencies_without_resolver(self, validator):
        """Test validate_action_dependencies without dependency resolver."""
        # Arrange
        mock_actions = [Mock(), Mock()]
        # Remove dependency_resolver attribute to simulate missing resolver
        del validator.config_validator.dependency_resolver
        
        # Act
        result = validator.validate_action_dependencies(mock_actions)
        
        # Assert
        assert result == []

    def test_validate_table_creation_rules_with_validator(self, validator):
        """Test validate_table_creation_rules with config validator."""
        # Arrange
        mock_actions = [Mock(), Mock()]
        expected_errors = ["table creation error 1", "table creation error 2"]
        validator.config_validator._validate_table_creation_rules.return_value = expected_errors
        
        # Act
        result = validator.validate_table_creation_rules(mock_actions)
        
        # Assert
        assert result == expected_errors
        validator.config_validator._validate_table_creation_rules.assert_called_once_with(mock_actions)

    def test_validate_table_creation_rules_missing_method(self, validator):
        """Test validate_table_creation_rules when method doesn't exist."""
        # Arrange
        mock_actions = [Mock(), Mock()]
        # Remove the method to simulate AttributeError
        del validator.config_validator._validate_table_creation_rules
        
        # Act
        result = validator.validate_table_creation_rules(mock_actions)
        
        # Assert - should return empty list when method is missing
        assert result == []

    def test_validate_table_creation_rules_no_validator(self, minimal_validator):
        """Test validate_table_creation_rules without config validator."""
        # Arrange
        mock_actions = [Mock(), Mock()]
        
        # Act
        result = minimal_validator.validate_table_creation_rules(mock_actions)
        
        # Assert - should return empty list when no config validator
        assert result == []

    @patch('lhp.core.services.pipeline_validator.EnhancedSubstitutionManager')
    def test_validate_pipeline_by_field_without_processor(self, mock_substitution_manager, validator, mock_flowgroup):
        """Test validate_pipeline_by_field without processor using basic validation."""
        # Arrange
        mock_discoverer = Mock()
        mock_discoverer.discover_flowgroups_by_pipeline_field.return_value = [mock_flowgroup]
        validator.config_validator.validate_flowgroup.return_value = []  # No validation errors
        
        # Act
        errors, warnings = validator.validate_pipeline_by_field(
            pipeline_field="test_pipeline",
            env="dev",
            discoverer=mock_discoverer,
            processor=None  # No processor - should use basic validation
        )
        
        # Assert
        assert errors == []
        assert warnings == []
        validator.config_validator.validate_flowgroup.assert_called_once_with(mock_flowgroup)

    @patch('lhp.core.services.pipeline_validator.EnhancedSubstitutionManager')
    def test_validate_pipeline_by_field_general_exception(self, mock_substitution_manager, validator):
        """Test validate_pipeline_by_field general exception handling."""
        # Arrange
        mock_discoverer = Mock()
        mock_discoverer.discover_flowgroups_by_pipeline_field.side_effect = Exception("Discovery failed")
        
        # Act
        errors, warnings = validator.validate_pipeline_by_field(
            pipeline_field="test_pipeline",
            env="dev",
            discoverer=mock_discoverer,
            processor=None
        )
        
        # Assert
        assert len(errors) == 1
        assert "Pipeline validation failed: Discovery failed" in errors
        assert warnings == []

    @patch('lhp.core.services.pipeline_validator.EnhancedSubstitutionManager')
    def test_validate_pipeline_by_directory_without_processor(self, mock_substitution_manager, validator, mock_flowgroup):
        """Test validate_pipeline_by_directory without processor using basic validation."""
        # Arrange
        mock_discoverer = Mock()
        mock_discoverer.discover_flowgroups.return_value = [mock_flowgroup]
        validator.config_validator.validate_flowgroup.return_value = []  # No validation errors
        
        # Act
        errors, warnings = validator.validate_pipeline_by_directory(
            pipeline_name="test_pipeline",
            env="dev",
            discoverer=mock_discoverer,
            processor=None  # No processor - should use basic validation
        )
        
        # Assert
        assert errors == []
        assert warnings == []
        validator.config_validator.validate_flowgroup.assert_called_once_with(mock_flowgroup)

    @patch('lhp.core.services.pipeline_validator.EnhancedSubstitutionManager')
    def test_validate_pipeline_by_directory_general_exception(self, mock_substitution_manager, validator):
        """Test validate_pipeline_by_directory general exception handling."""
        # Arrange
        mock_discoverer = Mock()
        # Simulate exception at the directory discovery level
        mock_substitution_manager.side_effect = Exception("Substitution failed")
        
        # Act
        errors, warnings = validator.validate_pipeline_by_directory(
            pipeline_name="test_pipeline",
            env="dev",
            discoverer=mock_discoverer,
            processor=None
        )
        
        # Assert
        assert len(errors) == 1
        assert "Pipeline validation failed: Substitution failed" in errors
        assert warnings == []
