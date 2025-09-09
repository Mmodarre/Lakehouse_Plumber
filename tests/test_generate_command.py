"""Tests for GenerateCommand CLI implementation."""

import pytest
from unittest.mock import Mock, patch, MagicMock, call
from pathlib import Path
import click
import sys
import yaml

from lhp.cli.commands.generate_command import GenerateCommand
from lhp.core.orchestrator import ActionOrchestrator
from lhp.core.state_manager import StateManager
from lhp.bundle.exceptions import BundleResourceError


class TestGenerateCommandExecuteCoordination:
    """Test GenerateCommand execute method coordination and flow."""

    @pytest.fixture
    def mock_generate_command(self):
        """Create a GenerateCommand instance with mocked dependencies."""
        command = GenerateCommand()
        
        # Mock base command methods
        command.setup_from_context = Mock()
        command.ensure_project_root = Mock(return_value=Path("/mock/project"))
        command.echo_verbose_info = Mock()
        command.verbose = False
        command.log_file = None
        command.handle_error = Mock()
        
        return command

    @pytest.fixture
    def mock_project_root(self):
        """Mock project root path."""
        return Path("/mock/project")

    def test_execute_method_coordinates_all_sub_methods_correctly(self, mock_generate_command, mock_project_root):
        """Test execute method coordinates all sub-methods correctly."""
        # Arrange
        env = "dev"
        pipeline = "test_pipeline"
        output = "custom_output"
        
        # Mock all the sub-methods that should be called
        mock_generate_command._validate_environment_setup = Mock()
        mock_generate_command._determine_pipelines_to_generate = Mock(return_value=["test_pipeline"])
        mock_generate_command._handle_cleanup_operations = Mock()
        mock_generate_command._analyze_generation_needs = Mock(return_value={"test_pipeline": {"reason": "new"}})
        mock_generate_command._generate_pipelines = Mock(return_value=(5, {"test_pipeline": {"file1.py": "code"}}))
        mock_generate_command._handle_bundle_operations = Mock()
        mock_generate_command._display_completion_message = Mock()
        
        # Mock orchestrator and state manager creation
        mock_orchestrator = Mock(spec=ActionOrchestrator)
        mock_state_manager = Mock(spec=StateManager)
        
        with patch('lhp.cli.commands.generate_command.ActionOrchestrator', return_value=mock_orchestrator) as mock_orch_class, \
             patch('lhp.cli.commands.generate_command.StateManager', return_value=mock_state_manager) as mock_state_class, \
             patch('lhp.cli.commands.generate_command.click.echo') as mock_echo:
            
            # Act
            mock_generate_command.execute(
                env=env, 
                pipeline=pipeline, 
                output=output,
                dry_run=False,
                no_cleanup=False, 
                force=False,
                no_bundle=False,
                include_tests=False
            )
            
            # Assert - Verify execution flow coordination
            mock_generate_command.setup_from_context.assert_called_once()
            mock_generate_command.ensure_project_root.assert_called_once()
            
            # Should validate environment setup
            mock_generate_command._validate_environment_setup.assert_called_once_with(env, mock_project_root)
            
            # Should create orchestrator and state manager
            mock_orch_class.assert_called_once_with(mock_project_root)
            mock_state_class.assert_called_once_with(mock_project_root)
            
            # Should determine pipelines to generate
            mock_generate_command._determine_pipelines_to_generate.assert_called_once_with(pipeline, mock_orchestrator)
            
            # Should handle cleanup operations (no_cleanup=False)
            mock_generate_command._handle_cleanup_operations.assert_called_once_with(
                env, mock_state_manager, mock_project_root / output, False
            )
            
            # Should analyze generation needs
            mock_generate_command._analyze_generation_needs.assert_called_once_with(
                ["test_pipeline"], env, mock_state_manager, False, False
            )
            
            # Should generate pipelines
            mock_generate_command._generate_pipelines.assert_called_once_with(
                {"test_pipeline": {"reason": "new"}}, env, mock_project_root / output, False,
                mock_orchestrator, mock_state_manager, False, False, False
            )
            
            # Should save state
            mock_state_manager.save.assert_called_once()
            
            # Should handle bundle operations
            mock_generate_command._handle_bundle_operations.assert_called_once_with(
                mock_project_root, mock_project_root / output, env, False, {"test_pipeline": {"file1.py": "code"}}, False
            )
            
            # Should display completion message
            mock_generate_command._display_completion_message.assert_called_once_with(
                5, mock_project_root / output, mock_project_root, False, False, mock_state_manager
            )
            
            # Should echo generation start message
            mock_echo.assert_any_call(f"🚀 Generating pipeline code for environment: {env}")


class TestGenerateCommandEnvironmentValidation:
    """Test GenerateCommand environment validation logic."""

    @pytest.fixture
    def mock_generate_command(self):
        """Create a GenerateCommand instance with mocked dependencies."""
        command = GenerateCommand()
        command.check_substitution_file = Mock()
        command.verbose = False
        return command

    def test_validate_environment_setup_success_with_valid_environment(self, mock_generate_command):
        """Test _validate_environment_setup with valid environment."""
        # Arrange
        env = "dev"
        project_root = Path("/mock/project")
        
        # Mock databricks.yml exists with valid targets
        databricks_yml_content = {
            "targets": {
                "dev": {"workspace_host": "https://dev.databricks.com"},
                "test": {"workspace_host": "https://test.databricks.com"}
            }
        }
        
        with patch('builtins.open', mock_open_yaml(databricks_yml_content)) as mock_file, \
             patch.object(Path, 'exists', return_value=True), \
             patch('lhp.cli.commands.generate_command.click.echo') as mock_echo:
            
            # Act - should complete without warnings
            mock_generate_command._validate_environment_setup(env, project_root)
            
            # Assert
            mock_generate_command.check_substitution_file.assert_called_once_with(env)
            
            # Should not echo any warnings since environment exists in targets
            warning_calls = [call for call in mock_echo.call_args_list if "Warning" in str(call)]
            assert len(warning_calls) == 0

    def test_validate_environment_setup_missing_substitution_file(self, mock_generate_command):
        """Test _validate_environment_setup with missing substitution file."""
        # Arrange
        env = "dev"
        project_root = Path("/mock/project")
        
        # Mock substitution file check to raise exception
        substitution_error = FileNotFoundError("Substitution file not found: substitutions/dev.yaml")
        mock_generate_command.check_substitution_file.side_effect = substitution_error
        
        with patch.object(Path, 'exists', return_value=False):  # No databricks.yml
            # Act & Assert - should propagate substitution file error
            with pytest.raises(FileNotFoundError, match="Substitution file not found"):
                mock_generate_command._validate_environment_setup(env, project_root)
            
            mock_generate_command.check_substitution_file.assert_called_once_with(env)

    def test_validate_environment_databricks_yml_validation_and_warnings(self, mock_generate_command):
        """Test _validate_environment_setup databricks.yml validation and warnings."""
        # Arrange
        env = "staging"  # Environment not in databricks.yml targets
        project_root = Path("/mock/project")
        
        # Mock databricks.yml exists but missing staging environment
        databricks_yml_content = {
            "targets": {
                "dev": {"workspace_host": "https://dev.databricks.com"},
                "prod": {"workspace_host": "https://prod.databricks.com"}
            }
        }
        
        with patch('builtins.open', mock_open_yaml(databricks_yml_content)) as mock_file, \
             patch.object(Path, 'exists', return_value=True), \
             patch('lhp.cli.commands.generate_command.click.echo') as mock_echo:
            
            # Act
            mock_generate_command._validate_environment_setup(env, project_root)
            
            # Assert
            mock_generate_command.check_substitution_file.assert_called_once_with(env)
            
            # Should echo warning about missing environment in databricks.yml targets
            warning_calls = [str(call) for call in mock_echo.call_args_list]
            warning_found = any("Warning" in call and "staging" in call and "not found in databricks.yml targets" in call for call in warning_calls)
            assert warning_found
            
            # Should suggest adding target
            suggestion_found = any("Add a target named 'staging'" in call for call in warning_calls)
            assert suggestion_found

    def test_validate_environment_databricks_yml_missing_file(self, mock_generate_command):
        """Test _validate_environment_setup when databricks.yml does not exist."""
        # Arrange
        env = "dev"
        project_root = Path("/mock/project")
        
        with patch.object(Path, 'exists', return_value=False), \
             patch('lhp.cli.commands.generate_command.click.echo') as mock_echo:
            
            # Act - should complete successfully without warnings
            mock_generate_command._validate_environment_setup(env, project_root)
            
            # Assert
            mock_generate_command.check_substitution_file.assert_called_once_with(env)
            
            # Should not attempt to read databricks.yml or echo warnings
            mock_echo.assert_not_called()

    def test_validate_environment_databricks_yml_parsing_error(self, mock_generate_command):
        """Test _validate_environment_setup when databricks.yml parsing fails."""
        # Arrange
        env = "dev"
        project_root = Path("/mock/project")
        mock_generate_command.verbose = True  # Enable verbose mode to see parsing error
        
        # Mock databricks.yml exists but contains invalid YAML
        with patch('builtins.open', side_effect=yaml.YAMLError("Invalid YAML syntax")) as mock_file, \
             patch.object(Path, 'exists', return_value=True), \
             patch('lhp.cli.commands.generate_command.click.echo') as mock_echo:
            
            # Act - should handle YAML parsing error gracefully
            mock_generate_command._validate_environment_setup(env, project_root)
            
            # Assert
            mock_generate_command.check_substitution_file.assert_called_once_with(env)
            
            # Should echo warning about databricks.yml parsing failure in verbose mode
            warning_calls = [str(call) for call in mock_echo.call_args_list]
            parsing_error_found = any("Could not validate databricks.yml" in call for call in warning_calls)
            assert parsing_error_found


class TestGenerateCommandPipelineDetermination:
    """Test GenerateCommand pipeline determination logic."""

    @pytest.fixture
    def mock_generate_command(self):
        """Create a GenerateCommand instance."""
        return GenerateCommand()

    @pytest.fixture
    def mock_orchestrator(self):
        """Create a mock ActionOrchestrator."""
        from lhp.models.config import FlowGroup
        
        # Create mock flowgroups for testing
        flowgroup1 = Mock(spec=FlowGroup)
        flowgroup1.pipeline = "data_pipeline"
        flowgroup1.flowgroup = "ingestion"
        
        flowgroup2 = Mock(spec=FlowGroup)
        flowgroup2.pipeline = "analytics_pipeline"
        flowgroup2.flowgroup = "transform"
        
        flowgroup3 = Mock(spec=FlowGroup)
        flowgroup3.pipeline = "data_pipeline"  # Same pipeline as flowgroup1
        flowgroup3.flowgroup = "processing"
        
        orchestrator = Mock(spec=ActionOrchestrator)
        orchestrator.discover_all_flowgroups.return_value = [flowgroup1, flowgroup2, flowgroup3]
        
        return orchestrator

    def test_determine_pipelines_to_generate_with_specific_pipeline_parameter(self, mock_generate_command, mock_orchestrator):
        """Test _determine_pipelines_to_generate with specific pipeline parameter."""
        # Arrange
        specific_pipeline = "data_pipeline"
        
        # Act
        result = mock_generate_command._determine_pipelines_to_generate(specific_pipeline, mock_orchestrator)
        
        # Assert
        assert result == [specific_pipeline]  # Should return the specific pipeline
        
        # Should discover all flowgroups to validate the pipeline exists
        mock_orchestrator.discover_all_flowgroups.assert_called_once()

    def test_determine_pipelines_to_generate_with_invalid_pipeline_name(self, mock_generate_command, mock_orchestrator):
        """Test _determine_pipelines_to_generate with invalid pipeline name."""
        # Arrange
        invalid_pipeline = "nonexistent_pipeline"
        
        with patch('lhp.cli.commands.generate_command.click.echo') as mock_echo, \
             patch('lhp.cli.commands.generate_command.sys.exit') as mock_exit:
            
            # Act
            mock_generate_command._determine_pipelines_to_generate(invalid_pipeline, mock_orchestrator)
            
            # Assert
            # Should discover all flowgroups
            mock_orchestrator.discover_all_flowgroups.assert_called_once()
            
            # Should echo error about pipeline not found
            error_calls = [str(call) for call in mock_echo.call_args_list]
            error_found = any("Pipeline field 'nonexistent_pipeline' not found" in call for call in error_calls)
            assert error_found
            
            # Should echo available pipelines
            available_found = any("Available pipeline fields" in call for call in error_calls)
            assert available_found
            
            # Should exit with code 1
            mock_exit.assert_called_once_with(1)

    def test_determine_pipelines_to_generate_discovery_mode_no_specific_pipeline(self, mock_generate_command, mock_orchestrator):
        """Test _determine_pipelines_to_generate with discovery mode (no specific pipeline)."""
        # Arrange
        pipeline = None  # Discovery mode
        
        # Act
        result = mock_generate_command._determine_pipelines_to_generate(pipeline, mock_orchestrator)
        
        # Assert
        # Should return sorted unique pipeline fields
        expected_pipelines = ["analytics_pipeline", "data_pipeline"]  # Sorted unique values
        assert result == expected_pipelines
        
        # Should discover all flowgroups
        mock_orchestrator.discover_all_flowgroups.assert_called_once()

    def test_determine_pipelines_to_generate_when_no_flowgroups_found(self, mock_generate_command):
        """Test _determine_pipelines_to_generate when no flowgroups found."""
        # Arrange
        pipeline = None  # Discovery mode
        mock_orchestrator = Mock(spec=ActionOrchestrator)
        mock_orchestrator.discover_all_flowgroups.return_value = []  # No flowgroups
        
        with patch('lhp.cli.commands.generate_command.click.echo') as mock_echo, \
             patch('lhp.cli.commands.generate_command.sys.exit') as mock_exit:
            
            # Act
            mock_generate_command._determine_pipelines_to_generate(pipeline, mock_orchestrator)
            
            # Assert
            # Should discover all flowgroups
            mock_orchestrator.discover_all_flowgroups.assert_called_once()
            
            # Should echo error about no flowgroups found
            error_calls = [str(call) for call in mock_echo.call_args_list]
            error_found = any("No flowgroups found in project" in call for call in error_calls)
            assert error_found
            
            # Should exit with code 1
            mock_exit.assert_called_once_with(1)


class TestGenerateCommandGenerationNeedsAnalysis:
    """Test GenerateCommand generation needs analysis logic."""

    @pytest.fixture
    def mock_generate_command(self):
        """Create a GenerateCommand instance."""
        return GenerateCommand()

    @pytest.fixture
    def mock_state_manager(self):
        """Create a mock StateManager with generation info."""
        # Mock file states for testing
        from lhp.core.state_models import FileState
        
        new_file = Mock(spec=FileState)
        new_file.generated_path = Path("generated/dev/new_flowgroup.py")
        
        stale_file = Mock(spec=FileState)
        stale_file.generated_path = Path("generated/dev/stale_flowgroup.py")
        
        up_to_date_file = Mock(spec=FileState)
        up_to_date_file.generated_path = Path("generated/dev/current_flowgroup.py")
        
        # Create mock state manager
        state_manager = Mock(spec=StateManager)
        state_manager.get_files_needing_generation.return_value = {
            "new": [new_file],
            "stale": [stale_file], 
            "up_to_date": [up_to_date_file]
        }
        
        state_manager.get_detailed_staleness_info.return_value = {
            "global_changes": [],
            "files": {
                stale_file.generated_path: {
                    "details": ["Source YAML modified", "Dependency changed"]
                }
            }
        }
        
        return state_manager

    def test_analyze_generation_needs_smart_mode_with_state_tracking(self, mock_generate_command, mock_state_manager):
        """Test _analyze_generation_needs with smart generation (state tracking)."""
        # Arrange
        pipelines_to_generate = ["test_pipeline", "analytics_pipeline"]
        env = "dev"
        no_cleanup = False
        force = False
        
        with patch('lhp.cli.commands.generate_command.click.echo') as mock_echo:
            # Act
            result = mock_generate_command._analyze_generation_needs(
                pipelines_to_generate, env, mock_state_manager, no_cleanup, force
            )
            
            # Assert
            # Should return pipelines that need generation (those with new or stale files)
            assert "test_pipeline" in result  # Has generation info
            assert result["test_pipeline"] == {
                "new": [mock_state_manager.get_files_needing_generation.return_value["new"][0]],
                "stale": [mock_state_manager.get_files_needing_generation.return_value["stale"][0]],
                "up_to_date": [mock_state_manager.get_files_needing_generation.return_value["up_to_date"][0]]
            }
            
            # Should call state manager for each pipeline
            assert mock_state_manager.get_files_needing_generation.call_count == 2
            mock_state_manager.get_detailed_staleness_info.assert_called_once_with(env)
            
            # Should echo analysis message
            echo_calls = [str(call) for call in mock_echo.call_args_list]
            analysis_found = any("Analyzing changes in environment" in call for call in echo_calls)
            assert analysis_found

    def test_analyze_generation_needs_force_mode(self, mock_generate_command):
        """Test _analyze_generation_needs with force=True."""
        # Arrange
        pipelines_to_generate = ["pipeline1", "pipeline2"]
        env = "dev"
        state_manager = Mock(spec=StateManager)  # State manager provided but force=True
        no_cleanup = False
        force = True  # Force mode
        
        with patch('lhp.cli.commands.generate_command.click.echo') as mock_echo:
            # Act
            result = mock_generate_command._analyze_generation_needs(
                pipelines_to_generate, env, state_manager, no_cleanup, force
            )
            
            # Assert
            # Should include all pipelines with force reason
            assert result == {
                "pipeline1": {"reason": "force"},
                "pipeline2": {"reason": "force"}
            }
            
            # Should echo force mode message
            echo_calls = [str(call) for call in mock_echo.call_args_list]
            force_found = any("Force mode: regenerating all files regardless of changes" in call for call in echo_calls)
            assert force_found
            
            # Should NOT call state manager methods in force mode
            state_manager.get_files_needing_generation.assert_not_called()
            state_manager.get_detailed_staleness_info.assert_not_called()

    def test_analyze_generation_needs_no_cleanup_mode(self, mock_generate_command):
        """Test _analyze_generation_needs with no_cleanup=True."""
        # Arrange
        pipelines_to_generate = ["pipeline1", "pipeline2"]
        env = "dev"
        state_manager = None  # No state manager when no_cleanup=True
        no_cleanup = True  # No cleanup mode
        force = False
        
        with patch('lhp.cli.commands.generate_command.click.echo') as mock_echo:
            # Act
            result = mock_generate_command._analyze_generation_needs(
                pipelines_to_generate, env, state_manager, no_cleanup, force
            )
            
            # Assert
            # Should include all pipelines with no_state_tracking reason
            assert result == {
                "pipeline1": {"reason": "no_state_tracking"},
                "pipeline2": {"reason": "no_state_tracking"}
            }
            
            # Should echo state tracking disabled message
            echo_calls = [str(call) for call in mock_echo.call_args_list]
            no_state_found = any("State tracking disabled: generating all files" in call for call in echo_calls)
            assert no_state_found

    def test_analyze_generation_needs_when_all_files_up_to_date(self, mock_generate_command):
        """Test _analyze_generation_needs when all files up-to-date."""
        # Arrange
        pipelines_to_generate = ["up_to_date_pipeline"]
        env = "dev"
        no_cleanup = False
        force = False
        
        # Mock state manager with no files needing generation
        state_manager = Mock(spec=StateManager)
        state_manager.get_files_needing_generation.return_value = {
            "new": [],
            "stale": [],
            "up_to_date": [Mock()]  # Some up-to-date files
        }
        state_manager.get_detailed_staleness_info.return_value = {"global_changes": [], "files": {}}
        
        with patch('lhp.cli.commands.generate_command.click.echo') as mock_echo:
            # Act
            result = mock_generate_command._analyze_generation_needs(
                pipelines_to_generate, env, state_manager, no_cleanup, force
            )
            
            # Assert
            # Should return empty dict (no pipelines need generation)
            assert result == {}
            
            # Should echo up-to-date messages
            echo_calls = [str(call) for call in mock_echo.call_args_list]
            up_to_date_found = any("All files are up-to-date! Nothing to generate." in call for call in echo_calls)
            assert up_to_date_found
            
            force_hint_found = any("Use --force flag to regenerate" in call for call in echo_calls)
            assert force_hint_found


class TestGenerateCommandBundleOperations:
    """Test GenerateCommand bundle operations logic."""

    @pytest.fixture
    def mock_generate_command(self):
        """Create a GenerateCommand instance."""
        command = GenerateCommand()
        command.verbose = False
        command.log_file = None
        return command

    def test_handle_bundle_operations_when_bundle_support_enabled(self, mock_generate_command):
        """Test _handle_bundle_operations when bundle support enabled."""
        # Arrange
        project_root = Path("/mock/project")
        output_dir = Path("/mock/project/generated/dev")
        env = "dev"
        no_bundle = False
        all_generated_files = {"pipeline1": {"file1.py": "code"}}
        dry_run = False
        
        # Mock bundle support enabled
        with patch('lhp.cli.commands.generate_command.should_enable_bundle_support', return_value=True) as mock_should_enable, \
             patch('lhp.cli.commands.generate_command.BundleManager') as mock_bundle_manager_class, \
             patch('lhp.cli.commands.generate_command.click.echo') as mock_echo:
            
            # Mock BundleManager instance
            mock_bundle_manager = Mock()
            mock_bundle_manager_class.return_value = mock_bundle_manager
            
            # Act
            mock_generate_command._handle_bundle_operations(
                project_root, output_dir, env, no_bundle, all_generated_files, dry_run
            )
            
            # Assert
            # Should check if bundle support is enabled
            mock_should_enable.assert_called_once_with(project_root, no_bundle)
            
            # Should create BundleManager and sync resources
            mock_bundle_manager_class.assert_called_once_with(project_root)
            mock_bundle_manager.sync_resources_with_generated_files.assert_called_once_with(output_dir, env)
            
            # Should not echo bundle sync messages in non-verbose mode
            bundle_calls = [str(call) for call in mock_echo.call_args_list if "Bundle" in str(call)]
            assert len(bundle_calls) == 0  # No bundle messages in non-verbose mode

    def test_handle_bundle_operations_when_bundle_support_disabled(self, mock_generate_command):
        """Test _handle_bundle_operations when bundle support disabled."""
        # Arrange
        project_root = Path("/mock/project")
        output_dir = Path("/mock/project/generated/dev")
        env = "dev"
        no_bundle = True  # Bundle disabled
        all_generated_files = {"pipeline1": {"file1.py": "code"}}
        dry_run = False
        
        # Mock bundle support disabled
        with patch('lhp.cli.commands.generate_command.should_enable_bundle_support', return_value=False) as mock_should_enable, \
             patch('lhp.cli.commands.generate_command.BundleManager') as mock_bundle_manager_class, \
             patch('lhp.cli.commands.generate_command.click.echo') as mock_echo:
            
            # Act
            mock_generate_command._handle_bundle_operations(
                project_root, output_dir, env, no_bundle, all_generated_files, dry_run
            )
            
            # Assert
            # Should check if bundle support is enabled
            mock_should_enable.assert_called_once_with(project_root, no_bundle)
            
            # Should NOT create BundleManager or sync resources
            mock_bundle_manager_class.assert_not_called()
            
            # Should not echo any bundle messages
            mock_echo.assert_not_called()

    def test_handle_bundle_operations_with_bundle_resource_errors(self, mock_generate_command):
        """Test _handle_bundle_operations with bundle errors."""
        # Arrange
        project_root = Path("/mock/project")
        output_dir = Path("/mock/project/generated/dev")
        env = "dev"
        no_bundle = False
        all_generated_files = {"pipeline1": {"file1.py": "code"}}
        dry_run = False
        
        # Mock bundle support enabled but operations fail
        bundle_error = BundleResourceError("Bundle sync failed: workspace connection error")
        
        with patch('lhp.cli.commands.generate_command.should_enable_bundle_support', return_value=True), \
             patch('lhp.cli.commands.generate_command.BundleManager') as mock_bundle_manager_class, \
             patch('lhp.cli.commands.generate_command.click.echo') as mock_echo:
            
            # Mock BundleManager to raise error
            mock_bundle_manager = Mock()
            mock_bundle_manager.sync_resources_with_generated_files.side_effect = bundle_error
            mock_bundle_manager_class.return_value = mock_bundle_manager
            
            # Act - should handle error gracefully
            mock_generate_command._handle_bundle_operations(
                project_root, output_dir, env, no_bundle, all_generated_files, dry_run
            )
            
            # Assert
            # Should create BundleManager and attempt sync
            mock_bundle_manager_class.assert_called_once_with(project_root)
            mock_bundle_manager.sync_resources_with_generated_files.assert_called_once_with(output_dir, env)
            
            # Should echo warning about bundle sync failure
            warning_calls = [str(call) for call in mock_echo.call_args_list]
            bundle_warning_found = any("Bundle sync warning" in call and "workspace connection error" in call for call in warning_calls)
            assert bundle_warning_found

    def test_handle_bundle_operations_dry_run_mode(self, mock_generate_command):
        """Test _handle_bundle_operations in dry-run mode."""
        # Arrange
        project_root = Path("/mock/project")
        output_dir = Path("/mock/project/generated/dev")
        env = "dev"
        no_bundle = False
        all_generated_files = {"pipeline1": {"file1.py": "code"}}
        dry_run = True  # Dry-run mode
        mock_generate_command.verbose = True  # Enable verbose to see dry-run message
        
        # Mock bundle support enabled
        with patch('lhp.cli.commands.generate_command.should_enable_bundle_support', return_value=True), \
             patch('lhp.cli.commands.generate_command.BundleManager') as mock_bundle_manager_class, \
             patch('lhp.cli.commands.generate_command.click.echo') as mock_echo:
            
            # Act
            mock_generate_command._handle_bundle_operations(
                project_root, output_dir, env, no_bundle, all_generated_files, dry_run
            )
            
            # Assert
            # Should NOT create BundleManager in dry-run mode
            mock_bundle_manager_class.assert_not_called()
            
            # Should echo dry-run bundle message in verbose mode
            echo_calls = [str(call) for call in mock_echo.call_args_list]
            dry_run_found = any("Dry run: Bundle sync would be performed" in call for call in echo_calls)
            assert dry_run_found


class TestGenerateCommandCompletionDisplay:
    """Test GenerateCommand completion message display logic."""

    @pytest.fixture
    def mock_generate_command(self):
        """Create a GenerateCommand instance."""
        return GenerateCommand()

    def test_display_completion_message_successful_generation_with_files(self, mock_generate_command):
        """Test _display_completion_message for successful generation with files."""
        # Arrange
        total_files = 5
        output_dir = Path("/mock/project/generated/dev")
        project_root = Path("/mock/project")
        dry_run = False
        no_cleanup = False
        mock_state_manager = Mock(spec=StateManager)
        
        with patch('lhp.cli.commands.generate_command.click.echo') as mock_echo:
            # Act
            mock_generate_command._display_completion_message(
                total_files, output_dir, project_root, dry_run, no_cleanup, mock_state_manager
            )
            
            # Assert
            echo_calls = [str(call) for call in mock_echo.call_args_list]
            
            # Should echo total files generated
            total_files_found = any("Total files generated: 5" in call for call in echo_calls)
            assert total_files_found
            
            # Should echo output location
            output_location_found = any("Output location: generated/dev" in call for call in echo_calls)
            assert output_location_found
            
            # Should echo state tracking info
            state_tracking_found = any("State tracking: Enabled" in call for call in echo_calls)
            assert state_tracking_found
            
            # Should echo completion success
            completion_found = any("Code generation completed successfully" in call for call in echo_calls)
            assert completion_found
            
            # Should echo next steps
            next_steps_found = any("Next steps:" in call for call in echo_calls)
            assert next_steps_found

    def test_display_completion_message_dry_run_mode(self, mock_generate_command):
        """Test _display_completion_message in dry-run mode."""
        # Arrange
        total_files = 3
        output_dir = Path("/mock/project/generated/dev")
        project_root = Path("/mock/project")
        dry_run = True  # Dry-run mode
        no_cleanup = False
        mock_state_manager = Mock(spec=StateManager)
        
        with patch('lhp.cli.commands.generate_command.click.echo') as mock_echo:
            # Act
            mock_generate_command._display_completion_message(
                total_files, output_dir, project_root, dry_run, no_cleanup, mock_state_manager
            )
            
            # Assert
            echo_calls = [str(call) for call in mock_echo.call_args_list]
            
            # Should echo total files generated
            total_files_found = any("Total files generated: 3" in call for call in echo_calls)
            assert total_files_found
            
            # Should echo dry run completion message
            dry_run_found = any("Dry run completed - no files were written" in call for call in echo_calls)
            assert dry_run_found
            
            # Should echo dry run hint
            dry_run_hint_found = any("Remove --dry-run flag to generate files" in call for call in echo_calls)
            assert dry_run_hint_found

    def test_display_completion_message_no_files_generated(self, mock_generate_command):
        """Test _display_completion_message when no files generated."""
        # Arrange
        total_files = 0  # No files generated
        output_dir = Path("/mock/project/generated/dev")
        project_root = Path("/mock/project")
        dry_run = False
        no_cleanup = False
        mock_state_manager = Mock(spec=StateManager)
        
        with patch('lhp.cli.commands.generate_command.click.echo') as mock_echo:
            # Act
            mock_generate_command._display_completion_message(
                total_files, output_dir, project_root, dry_run, no_cleanup, mock_state_manager
            )
            
            # Assert
            echo_calls = [str(call) for call in mock_echo.call_args_list]
            
            # Should echo total files (0)
            total_files_found = any("Total files generated: 0" in call for call in echo_calls)
            assert total_files_found
            
            # Should echo pipeline processing completed
            processing_found = any("Pipeline processing completed" in call for call in echo_calls)
            assert processing_found
            
            # Should echo cleanup performed
            cleanup_found = any("Cleanup operations were performed" in call for call in echo_calls)
            assert cleanup_found
            
            # Should echo no files generated explanation
            no_files_found = any("No files were generated (no flowgroups found)" in call for call in echo_calls)
            assert no_files_found

    def test_display_completion_message_no_cleanup_mode(self, mock_generate_command):
        """Test _display_completion_message in no-cleanup mode."""
        # Arrange
        total_files = 2
        output_dir = Path("/mock/project/generated/dev")
        project_root = Path("/mock/project")
        dry_run = False
        no_cleanup = True  # No cleanup mode
        state_manager = None  # No state manager
        
        with patch('lhp.cli.commands.generate_command.click.echo') as mock_echo:
            # Act
            mock_generate_command._display_completion_message(
                total_files, output_dir, project_root, dry_run, no_cleanup, state_manager
            )
            
            # Assert
            echo_calls = [str(call) for call in mock_echo.call_args_list]
            
            # Should echo total files generated
            total_files_found = any("Total files generated: 2" in call for call in echo_calls)
            assert total_files_found
            
            # Should echo output location
            output_location_found = any("Output location: generated/dev" in call for call in echo_calls)
            assert output_location_found
            
            # Should NOT echo state tracking info (no cleanup mode)
            state_tracking_calls = [call for call in echo_calls if "State tracking" in str(call)]
            assert len(state_tracking_calls) == 0
            
            # Should echo completion success
            completion_found = any("Code generation completed successfully" in call for call in echo_calls)
            assert completion_found


class TestGenerateCommandCleanupOperations:
    """Test GenerateCommand cleanup operations logic."""

    @pytest.fixture
    def mock_generate_command(self):
        """Create a GenerateCommand instance."""
        return GenerateCommand()

    def test_handle_cleanup_operations_fresh_start_scenario(self, mock_generate_command):
        """Test _handle_cleanup_operations for fresh start scenario."""
        # Arrange
        env = "dev"
        output_dir = Path("/mock/project/generated/dev")
        dry_run = False
        
        # Mock state manager for fresh start (no state file)
        mock_state_manager = Mock(spec=StateManager)
        mock_state_manager.state_file_exists.return_value = False  # Fresh start
        mock_state_manager.find_orphaned_files.return_value = []  # No orphaned files
        mock_state_manager.cleanup_untracked_files.return_value = [
            Path("/mock/project/generated/dev/old_file.py"),
            Path("/mock/project/generated/dev/unused_file.py")
        ]
        
        with patch('lhp.cli.commands.generate_command.click.echo') as mock_echo:
            # Act
            mock_generate_command._handle_cleanup_operations(env, mock_state_manager, output_dir, dry_run)
            
            # Assert
            # Should check for state file existence
            mock_state_manager.state_file_exists.assert_called_once()
            
            # Should run fresh start cleanup
            mock_state_manager.cleanup_untracked_files.assert_called_once_with(output_dir, env)
            
            # Should check for orphaned files
            mock_state_manager.find_orphaned_files.assert_called_once_with(env)
            
            # Should echo fresh start messages
            echo_calls = [str(call) for call in mock_echo.call_args_list]
            fresh_start_found = any("Fresh start detected" in call for call in echo_calls)
            assert fresh_start_found
            
            cleanup_found = any("Fresh start cleanup: removed 2 orphaned file(s)" in call for call in echo_calls)
            assert cleanup_found

    def test_handle_cleanup_operations_with_orphaned_files(self, mock_generate_command):
        """Test _handle_cleanup_operations with orphaned files."""
        # Arrange
        env = "dev"
        output_dir = Path("/mock/project/generated/dev")
        dry_run = False
        
        # Mock state manager with existing state and orphaned files
        from lhp.core.state_models import FileState
        orphaned_file1 = Mock(spec=FileState)
        orphaned_file1.generated_path = Path("generated/dev/orphaned1.py")
        orphaned_file1.source_yaml = Path("pipelines/old/flowgroup.yaml")
        
        orphaned_file2 = Mock(spec=FileState)
        orphaned_file2.generated_path = Path("generated/dev/orphaned2.py")
        orphaned_file2.source_yaml = Path("pipelines/removed/flowgroup.yaml")
        
        mock_state_manager = Mock(spec=StateManager)
        mock_state_manager.state_file_exists.return_value = True  # Existing state
        mock_state_manager.find_orphaned_files.return_value = [orphaned_file1, orphaned_file2]
        mock_state_manager.cleanup_orphaned_files.return_value = [
            orphaned_file1.generated_path,
            orphaned_file2.generated_path
        ]
        
        with patch('lhp.cli.commands.generate_command.click.echo') as mock_echo:
            # Act
            mock_generate_command._handle_cleanup_operations(env, mock_state_manager, output_dir, dry_run)
            
            # Assert
            # Should check for state file existence
            mock_state_manager.state_file_exists.assert_called_once()
            
            # Should NOT run fresh start cleanup (state exists)
            mock_state_manager.cleanup_untracked_files.assert_not_called()
            
            # Should check for and clean up orphaned files
            mock_state_manager.find_orphaned_files.assert_called_once_with(env)
            mock_state_manager.cleanup_orphaned_files.assert_called_once_with(env, dry_run=False)
            
            # Should echo orphaned file cleanup messages
            echo_calls = [str(call) for call in mock_echo.call_args_list]
            cleanup_count_found = any("Cleaning up 2 orphaned file(s)" in call for call in echo_calls)
            assert cleanup_count_found
            
            deleted_found = any("Deleted: generated/dev/orphaned1.py" in call for call in echo_calls)
            assert deleted_found

    def test_handle_cleanup_operations_dry_run_mode(self, mock_generate_command):
        """Test _handle_cleanup_operations in dry-run mode."""
        # Arrange
        env = "dev"
        output_dir = Path("/mock/project/generated/dev")
        dry_run = True  # Dry-run mode
        
        # Mock state manager with orphaned files
        from lhp.core.state_models import FileState
        orphaned_file = Mock(spec=FileState)
        orphaned_file.generated_path = Path("generated/dev/would_be_deleted.py")
        orphaned_file.source_yaml = Path("pipelines/removed/flowgroup.yaml")
        
        mock_state_manager = Mock(spec=StateManager)
        mock_state_manager.state_file_exists.return_value = True  # Existing state
        mock_state_manager.find_orphaned_files.return_value = [orphaned_file]
        
        # Mock fresh start preview method
        mock_generate_command._show_fresh_start_preview = Mock()
        
        with patch('lhp.cli.commands.generate_command.click.echo') as mock_echo:
            # Act
            mock_generate_command._handle_cleanup_operations(env, mock_state_manager, output_dir, dry_run)
            
            # Assert
            # Should check for state file existence
            mock_state_manager.state_file_exists.assert_called_once()
            
            # Should check for orphaned files but NOT clean them up in dry-run
            mock_state_manager.find_orphaned_files.assert_called_once_with(env)
            mock_state_manager.cleanup_orphaned_files.assert_not_called()
            
            # Should echo dry-run preview messages
            echo_calls = [str(call) for call in mock_echo.call_args_list]
            would_cleanup_found = any("Would clean up 1 orphaned file(s)" in call for call in echo_calls)
            assert would_cleanup_found
            
            preview_found = any("would_be_deleted.py" in call and "pipelines/removed/flowgroup.yaml" in call for call in echo_calls)
            assert preview_found


class TestGenerateCommandPipelineGeneration:
    """Test GenerateCommand pipeline generation logic."""

    @pytest.fixture
    def mock_generate_command(self):
        """Create a GenerateCommand instance."""
        command = GenerateCommand()
        command.handle_error = Mock()
        return command

    def test_generate_pipelines_successful_generation_flow(self, mock_generate_command):
        """Test _generate_pipelines successful generation flow."""
        # Arrange
        pipelines_needing_generation = {
            "pipeline1": {"reason": "new"},
            "pipeline2": {"reason": "stale"}
        }
        env = "dev"
        output_dir = Path("/mock/project/generated/dev")
        dry_run = False
        force = False
        no_cleanup = False
        include_tests = False
        
        # Mock orchestrator and state manager
        mock_orchestrator = Mock(spec=ActionOrchestrator)
        mock_state_manager = Mock(spec=StateManager)
        
        # Mock successful generation for both pipelines
        generated_files_p1 = {"flowgroup1.py": "# Pipeline 1 code", "flowgroup2.py": "# More code"}
        generated_files_p2 = {"analytics.py": "# Analytics code"}
        
        mock_orchestrator.generate_pipeline_by_field.side_effect = [generated_files_p1, generated_files_p2]
        
        # Mock display methods
        mock_generate_command._show_generation_results = Mock()
        
        with patch('lhp.cli.commands.generate_command.click.echo') as mock_echo:
            # Act
            total_files, all_generated_files = mock_generate_command._generate_pipelines(
                pipelines_needing_generation, env, output_dir, dry_run, mock_orchestrator,
                mock_state_manager, force, no_cleanup, include_tests
            )
            
            # Assert
            # Should return correct totals
            assert total_files == 3  # 2 + 1 files
            assert all_generated_files == {
                "pipeline1": generated_files_p1,
                "pipeline2": generated_files_p2
            }
            
            # Should call orchestrator for each pipeline
            assert mock_orchestrator.generate_pipeline_by_field.call_count == 2
            
            # Verify orchestrator calls with correct parameters
            calls = mock_orchestrator.generate_pipeline_by_field.call_args_list
            assert calls[0] == call("pipeline1", env, output_dir, state_manager=mock_state_manager, force_all=False, include_tests=False)
            assert calls[1] == call("pipeline2", env, output_dir, state_manager=mock_state_manager, force_all=False, include_tests=False)
            
            # Should show generation results for each pipeline
            assert mock_generate_command._show_generation_results.call_count == 2
            
            # Should echo processing messages
            echo_calls = [str(call) for call in mock_echo.call_args_list]
            pipeline1_found = any("Processing pipeline: pipeline1" in call for call in echo_calls)
            pipeline2_found = any("Processing pipeline: pipeline2" in call for call in echo_calls)
            assert pipeline1_found
            assert pipeline2_found

    def test_generate_pipelines_with_missing_pipeline_error(self, mock_generate_command):
        """Test _generate_pipelines with missing pipeline error."""
        # Arrange
        pipelines_needing_generation = {"missing_pipeline": {"reason": "force"}}
        env = "dev"
        output_dir = Path("/mock/project/generated/dev")
        dry_run = False
        force = False
        no_cleanup = False
        include_tests = False
        
        # Mock orchestrator and state manager
        mock_orchestrator = Mock(spec=ActionOrchestrator)
        mock_state_manager = Mock(spec=StateManager)
        
        # Mock orchestrator to raise ValueError for missing pipeline
        missing_pipeline_error = ValueError("No flowgroups found in pipeline directory: missing_pipeline")
        mock_orchestrator.generate_pipeline_by_field.side_effect = missing_pipeline_error
        
        # Mock missing pipeline handler
        mock_generate_command._handle_missing_pipeline = Mock()
        
        with patch('lhp.cli.commands.generate_command.click.echo') as mock_echo:
            # Act
            total_files, all_generated_files = mock_generate_command._generate_pipelines(
                pipelines_needing_generation, env, output_dir, dry_run, mock_orchestrator,
                mock_state_manager, force, no_cleanup, include_tests
            )
            
            # Assert
            # Should return zero files (error handled)
            assert total_files == 0
            assert all_generated_files == {}
            
            # Should call orchestrator
            mock_orchestrator.generate_pipeline_by_field.assert_called_once_with(
                "missing_pipeline", env, output_dir, state_manager=mock_state_manager, force_all=False, include_tests=False
            )
            
            # Should handle missing pipeline error
            mock_generate_command._handle_missing_pipeline.assert_called_once_with(
                "missing_pipeline", env, mock_state_manager, dry_run, output_dir.parent
            )
            
            # Should NOT call general error handler
            mock_generate_command.handle_error.assert_not_called()

    def test_generate_pipelines_with_unexpected_error(self, mock_generate_command):
        """Test _generate_pipelines with unexpected error."""
        # Arrange
        pipelines_needing_generation = {"error_pipeline": {"reason": "force"}}
        env = "dev"
        output_dir = Path("/mock/project/generated/dev")
        dry_run = False
        force = False
        no_cleanup = False
        include_tests = False
        
        # Mock orchestrator and state manager
        mock_orchestrator = Mock(spec=ActionOrchestrator)
        mock_state_manager = Mock(spec=StateManager)
        
        # Mock orchestrator to raise unexpected error
        unexpected_error = RuntimeError("Database connection failed during generation")
        mock_orchestrator.generate_pipeline_by_field.side_effect = unexpected_error
        
        with patch('lhp.cli.commands.generate_command.click.echo') as mock_echo:
            # Act
            total_files, all_generated_files = mock_generate_command._generate_pipelines(
                pipelines_needing_generation, env, output_dir, dry_run, mock_orchestrator,
                mock_state_manager, force, no_cleanup, include_tests
            )
            
            # Assert
            # Should return zero files (error handled)
            assert total_files == 0
            assert all_generated_files == {}
            
            # Should call orchestrator
            mock_orchestrator.generate_pipeline_by_field.assert_called_once_with(
                "error_pipeline", env, output_dir, state_manager=mock_state_manager, force_all=False, include_tests=False
            )
            
            # Should call general error handler for unexpected errors
            mock_generate_command.handle_error.assert_called_once_with(
                unexpected_error, "Unexpected error generating pipeline error_pipeline"
            )

    def test_show_dry_run_preview_with_generated_files(self, mock_generate_command):
        """Test _show_dry_run_preview method displays files correctly."""
        # Arrange
        generated_files = {
            "flowgroup1.py": "# Generated code 1\nimport pandas as pd\n",
            "analytics.py": "# Analytics code\nimport numpy as np\n"
        }
        
        # Mock logging level for code preview
        with patch('lhp.cli.commands.generate_command.logger.isEnabledFor', return_value=True), \
             patch('lhp.cli.commands.generate_command.click.echo') as mock_echo:
            
            # Act
            mock_generate_command._show_dry_run_preview(generated_files)
            
            # Assert
            echo_calls = [str(call) for call in mock_echo.call_args_list]
            
            # Should echo file count
            file_count_found = any("Would generate 2 file(s)" in call for call in echo_calls)
            assert file_count_found
            
            # Should echo file names
            flowgroup1_found = any("flowgroup1.py" in call for call in echo_calls)
            analytics_found = any("analytics.py" in call for call in echo_calls)
            assert flowgroup1_found
            assert analytics_found
            
            # Should echo code preview (debug mode)
            preview_found = any("Preview of generated code" in call for call in echo_calls)
            assert preview_found

    def test_show_generation_results_displays_files_correctly(self, mock_generate_command):
        """Test _show_generation_results method displays results correctly."""
        # Arrange
        generated_files = {"flowgroup1.py": "code", "flowgroup2.py": "more code"}
        output_dir = Path("/mock/project/generated/dev")
        pipeline_name = "test_pipeline"
        project_root = Path("/mock/project")
        
        with patch('lhp.cli.commands.generate_command.click.echo') as mock_echo:
            # Act
            mock_generate_command._show_generation_results(generated_files, output_dir, pipeline_name, project_root)
            
            # Assert
            echo_calls = [str(call) for call in mock_echo.call_args_list]
            
            # Should echo generation success with file count
            success_found = any("Generated 2 file(s)" in call and "test_pipeline" in call for call in echo_calls)
            assert success_found
            
            # Should echo relative paths for generated files
            flowgroup1_path_found = any("generated/dev/test_pipeline/flowgroup1.py" in call for call in echo_calls)
            flowgroup2_path_found = any("generated/dev/test_pipeline/flowgroup2.py" in call for call in echo_calls)
            assert flowgroup1_path_found
            assert flowgroup2_path_found

    def test_handle_missing_pipeline_with_cleanup_operations(self, mock_generate_command):
        """Test _handle_missing_pipeline with cleanup operations."""
        # Arrange
        pipeline_name = "missing_pipeline"
        env = "dev"
        dry_run = False
        project_root = Path("/mock/project")
        
        # Mock state manager with orphaned files from this pipeline
        from lhp.core.state_models import FileState
        orphaned_file = Mock(spec=FileState)
        orphaned_file.pipeline = pipeline_name
        orphaned_file.generated_path = Path("generated/dev/old_missing_pipeline_file.py")
        
        mock_state_manager = Mock(spec=StateManager)
        mock_state_manager.find_orphaned_files.return_value = [orphaned_file]
        mock_state_manager.state = Mock()
        mock_state_manager.state.environments = {"dev": {orphaned_file.generated_path: "some_state"}}
        
        with patch('lhp.cli.commands.generate_command.click.echo') as mock_echo, \
             patch.object(Path, 'exists', return_value=True), \
             patch.object(Path, 'unlink') as mock_unlink:  # Mock file deletion
            
            # Act
            mock_generate_command._handle_missing_pipeline(pipeline_name, env, mock_state_manager, dry_run, project_root)
            
            # Assert
            # Should find orphaned files for the specific pipeline
            mock_state_manager.find_orphaned_files.assert_called_once_with(env)
            
            # Should call cleanup methods
            mock_state_manager.cleanup_empty_directories.assert_called_once_with(env)
            
            # Should echo missing pipeline message
            echo_calls = [str(call) for call in mock_echo.call_args_list]
            missing_found = any("No flowgroups found in pipeline: missing_pipeline" in call for call in echo_calls)
            assert missing_found
            
            # Should echo orphaned files cleanup
            orphaned_found = any("Found 1 orphaned file(s) from missing_pipeline" in call for call in echo_calls)
            assert orphaned_found


# Helper function for YAML mocking
def mock_open_yaml(yaml_content):
    """Create mock for opening and reading YAML files."""
    import yaml
    from unittest.mock import mock_open
    
    yaml_string = yaml.dump(yaml_content)
    mock_file = mock_open(read_data=yaml_string)
    
    return mock_file

