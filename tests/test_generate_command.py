"""Tests for generate command CLI implementation."""

import pytest
import tempfile
import shutil
import sys
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
from click.testing import CliRunner

from lhp.cli.commands.generate_command import GenerateCommand
from lhp.core.layers import (
    GenerationResponse, ValidationResponse, AnalysisResponse,
    LakehousePlumberApplicationFacade
)
from lhp.bundle.exceptions import BundleResourceError


@pytest.fixture
def temp_project():
    """Create a temporary project structure."""
    temp_dir = Path(tempfile.mkdtemp())
    project_root = temp_dir / "test_project"
    project_root.mkdir()
    
    # Create lhp.yaml
    (project_root / "lhp.yaml").write_text("""
name: TestProject
version: 1.0.0
""")
    
    # Create substitutions directory and file
    (project_root / "substitutions").mkdir()
    (project_root / "substitutions" / "dev.yaml").write_text("""
dev:
  catalog: dev_catalog
  schema: dev_schema
""")
    
    # Create pipelines directory
    (project_root / "pipelines").mkdir()
    
    yield project_root
    
    shutil.rmtree(temp_dir)


@pytest.fixture
def generate_command():
    """Create GenerateCommand instance."""
    return GenerateCommand()


class TestGenerateCommandDisplayMethods:
    """Test display methods of GenerateCommand."""
    
    def test_display_generation_results_success_with_files(self, generate_command):
        """Test displaying successful generation with files written."""
        response = GenerationResponse(
            success=True,
            generated_files={"test.py": "# Generated"},
            files_written=1,
            total_flowgroups=1,
            output_location=Path("/output"),
            performance_info={}
        )
        
        with patch('click.echo') as mock_echo:
            generate_command.display_generation_results(response)
            
            assert mock_echo.called
            calls = [str(call) for call in mock_echo.call_args_list]
            assert any("Generated 1 file" in str(call) for call in calls)
            assert any("Output location" in str(call) for call in calls)
    
    def test_display_generation_results_success_dry_run(self, generate_command):
        """Test displaying successful generation in dry-run mode."""
        response = GenerationResponse(
            success=True,
            generated_files={},
            files_written=0,
            total_flowgroups=1,
            output_location=None,
            performance_info={"dry_run": True}
        )
        
        with patch('click.echo') as mock_echo:
            generate_command.display_generation_results(response)
            
            assert mock_echo.called
            calls = [str(call) for call in mock_echo.call_args_list]
            assert any("Dry run completed" in str(call) for call in calls)
    
    def test_display_generation_results_success_no_files(self, generate_command):
        """Test displaying successful generation with no files written."""
        response = GenerationResponse(
            success=True,
            generated_files={},
            files_written=0,
            total_flowgroups=1,
            output_location=None,
            performance_info={}
        )
        
        with patch('click.echo') as mock_echo:
            generate_command.display_generation_results(response)
            
            assert mock_echo.called
            calls = [str(call) for call in mock_echo.call_args_list]
            assert any("up-to-date" in str(call).lower() for call in calls)
    
    def test_display_generation_results_failure(self, generate_command):
        """Test displaying failed generation."""
        response = GenerationResponse(
            success=False,
            generated_files={},
            files_written=0,
            total_flowgroups=0,
            output_location=None,
            performance_info={},
            error_message="Test error"
        )
        
        with patch('click.echo') as mock_echo:
            generate_command.display_generation_results(response)
            
            assert mock_echo.called
            calls = [str(call) for call in mock_echo.call_args_list]
            assert any("Generation failed" in str(call) for call in calls)
            assert any("Test error" in str(call) for call in calls)
    
    def test_display_validation_results_success(self, generate_command):
        """Test displaying successful validation."""
        response = ValidationResponse(
            success=True,
            errors=[],
            warnings=[],
            validated_pipelines=["test_pipeline"]
        )
        
        with patch('click.echo') as mock_echo:
            generate_command.display_validation_results(response)
            
            assert mock_echo.called
            calls = [str(call) for call in mock_echo.call_args_list]
            assert any("Validation successful" in str(call) for call in calls)
    
    def test_display_validation_results_with_warnings(self, generate_command):
        """Test displaying validation with warnings."""
        response = ValidationResponse(
            success=True,
            errors=[],
            warnings=["Warning 1", "Warning 2"],
            validated_pipelines=["test_pipeline"]
        )
        
        with patch('click.echo') as mock_echo:
            generate_command.display_validation_results(response)
            
            assert mock_echo.called
            calls = [str(call) for call in mock_echo.call_args_list]
            assert any("Warnings found" in str(call) for call in calls)
            assert any("Warning 1" in str(call) for call in calls)
    
    def test_display_validation_results_failure(self, generate_command):
        """Test displaying failed validation."""
        response = ValidationResponse(
            success=False,
            errors=["Error 1", "Error 2"],
            warnings=[],
            validated_pipelines=[]
        )
        
        with patch('click.echo') as mock_echo:
            generate_command.display_validation_results(response)
            
            assert mock_echo.called
            calls = [str(call) for call in mock_echo.call_args_list]
            assert any("Validation failed" in str(call) for call in calls)
            assert any("Error 1" in str(call) for call in calls)
    
    def test_display_analysis_results_success_with_work(self, generate_command):
        """Test displaying analysis with work to do."""
        response = AnalysisResponse(
            success=True,
            pipelines_needing_generation={"pipeline1": {}, "pipeline2": {}},
            pipelines_up_to_date={},
            has_global_changes=False,
            global_changes=[],
            include_tests_context_applied=False,
            total_new_files=2,
            total_stale_files=0,
            total_up_to_date_files=0
        )
        
        with patch('click.echo') as mock_echo:
            generate_command.display_analysis_results(response)
            
            assert mock_echo.called
            calls = [str(call) for call in mock_echo.call_args_list]
            assert any("need generation" in str(call).lower() for call in calls)
    
    def test_display_analysis_results_success_no_work(self, generate_command):
        """Test displaying analysis with no work to do."""
        response = AnalysisResponse(
            success=True,
            pipelines_needing_generation={},
            pipelines_up_to_date={"pipeline1": 1},
            has_global_changes=False,
            global_changes=[],
            include_tests_context_applied=False,
            total_new_files=0,
            total_stale_files=0,
            total_up_to_date_files=1
        )
        
        with patch('click.echo') as mock_echo:
            generate_command.display_analysis_results(response)
            
            assert mock_echo.called
            calls = [str(call) for call in mock_echo.call_args_list]
            assert any("up-to-date" in str(call).lower() for call in calls)
    
    def test_display_analysis_results_with_context_changes(self, generate_command):
        """Test displaying analysis with context changes."""
        response = AnalysisResponse(
            success=True,
            pipelines_needing_generation={},
            pipelines_up_to_date={},
            has_global_changes=False,
            global_changes=[],
            include_tests_context_applied=True,
            total_new_files=0,
            total_stale_files=0,
            total_up_to_date_files=0
        )
        
        with patch('click.echo') as mock_echo:
            generate_command.display_analysis_results(response)
            
            assert mock_echo.called
            calls = [str(call) for call in mock_echo.call_args_list]
            assert any("include_tests" in str(call).lower() for call in calls)
    
    def test_display_analysis_results_failure(self, generate_command):
        """Test displaying failed analysis."""
        response = AnalysisResponse(
            success=False,
            pipelines_needing_generation={},
            pipelines_up_to_date={},
            has_global_changes=False,
            global_changes=[],
            include_tests_context_applied=False,
            total_new_files=0,
            total_stale_files=0,
            total_up_to_date_files=0,
            error_message="Analysis error"
        )
        
        with patch('click.echo') as mock_echo:
            generate_command.display_analysis_results(response)
            
            assert mock_echo.called
            calls = [str(call) for call in mock_echo.call_args_list]
            assert any("Analysis failed" in str(call) for call in calls)
    
    def test_display_startup_message(self, generate_command):
        """Test displaying startup message."""
        with patch('click.echo') as mock_echo, \
             patch.object(generate_command, 'echo_verbose_info'):
            generate_command._display_startup_message("dev")
            
            assert mock_echo.called
            calls = [str(call) for call in mock_echo.call_args_list]
            assert any("Generating pipeline code" in str(call) for call in calls)
            assert any("dev" in str(call) for call in calls)
    
    def test_display_generation_response_success_with_files(self, generate_command):
        """Test displaying generation response with files written."""
        response = GenerationResponse(
            success=True,
            generated_files={"test.py": "# Generated"},
            files_written=1,
            total_flowgroups=1,
            output_location=Path("/output"),
            performance_info={}
        )
        
        with patch('click.echo') as mock_echo:
            generate_command._display_generation_response(response, "test_pipeline")
            
            assert mock_echo.called
            calls = [str(call) for call in mock_echo.call_args_list]
            assert any("test_pipeline" in str(call) for call in calls)
            assert any("Generated 1 file" in str(call) for call in calls)
    
    def test_display_generation_response_dry_run(self, generate_command):
        """Test displaying generation response in dry-run mode."""
        response = GenerationResponse(
            success=True,
            generated_files={"test.py": "# Generated"},
            files_written=0,
            total_flowgroups=1,
            output_location=None,
            performance_info={"dry_run": True}
        )
        
        with patch('click.echo') as mock_echo:
            generate_command._display_generation_response(response, "test_pipeline")
            
            assert mock_echo.called
            calls = [str(call) for call in mock_echo.call_args_list]
            assert any("Would generate" in str(call) for call in calls)
            assert any("test.py" in str(call) for call in calls)
    
    def test_display_generation_response_up_to_date(self, generate_command):
        """Test displaying generation response when up-to-date."""
        response = GenerationResponse(
            success=True,
            generated_files={},
            files_written=0,
            total_flowgroups=1,
            output_location=None,
            performance_info={}
        )
        
        with patch('click.echo') as mock_echo:
            generate_command._display_generation_response(response, "test_pipeline")
            
            assert mock_echo.called
            calls = [str(call) for call in mock_echo.call_args_list]
            assert any("Up-to-date" in str(call) for call in calls)
    
    def test_display_generation_response_failure(self, generate_command):
        """Test displaying failed generation response."""
        response = GenerationResponse(
            success=False,
            generated_files={},
            files_written=0,
            total_flowgroups=0,
            output_location=None,
            performance_info={},
            error_message="Generation error"
        )
        
        with patch('click.echo') as mock_echo:
            generate_command._display_generation_response(response, "test_pipeline")
            
            assert mock_echo.called
            calls = [str(call) for call in mock_echo.call_args_list]
            assert any("Generation failed" in str(call) for call in calls)
            assert any("Generation error" in str(call) for call in calls)
    
    def test_display_completion_message_dry_run(self, generate_command):
        """Test displaying completion message for dry-run."""
        output_dir = Path("/output")
        
        with patch('click.echo') as mock_echo:
            generate_command._display_completion_message(0, output_dir, dry_run=True)
            
            assert mock_echo.called
            calls = [str(call) for call in mock_echo.call_args_list]
            assert any("Dry run completed" in str(call) for call in calls)
    
    def test_display_completion_message_with_files(self, generate_command):
        """Test displaying completion message with files generated."""
        output_dir = Path("/output")
        
        with patch('click.echo') as mock_echo:
            generate_command._display_completion_message(5, output_dir, dry_run=False)
            
            assert mock_echo.called
            calls = [str(call) for call in mock_echo.call_args_list]
            assert any("completed successfully" in str(call).lower() for call in calls)
            assert any("5" in str(call) for call in calls)
    
    def test_display_completion_message_no_files(self, generate_command):
        """Test displaying completion message with no files."""
        output_dir = Path("/output")
        
        with patch('click.echo') as mock_echo:
            generate_command._display_completion_message(0, output_dir, dry_run=False)
            
            assert mock_echo.called
            calls = [str(call) for call in mock_echo.call_args_list]
            assert any("up-to-date" in str(call).lower() for call in calls)


class TestGenerateCommandHelperMethods:
    """Test helper methods of GenerateCommand."""
    
    def test_create_application_facade(self, generate_command, temp_project):
        """Test creating application facade."""
        facade = generate_command._create_application_facade(temp_project, no_cleanup=False, pipeline_config_path=None)
        
        assert isinstance(facade, LakehousePlumberApplicationFacade)
        assert facade.orchestrator is not None
        assert facade.state_manager is not None
    
    def test_create_application_facade_no_cleanup(self, generate_command, temp_project):
        """Test creating application facade with no cleanup."""
        facade = generate_command._create_application_facade(temp_project, no_cleanup=True, pipeline_config_path=None)
        
        assert isinstance(facade, LakehousePlumberApplicationFacade)
        assert facade.state_manager is None
    
    def test_create_application_facade_with_pipeline_config(self, generate_command, temp_project):
        """Test creating application facade with pipeline config."""
        config_file = temp_project / "pipeline_config.yaml"
        config_file.write_text("project_defaults:\n  serverless: false")
        
        facade = generate_command._create_application_facade(
            temp_project, 
            no_cleanup=False, 
            pipeline_config_path=str(config_file)
        )
        
        assert isinstance(facade, LakehousePlumberApplicationFacade)
    
    def test_execute_pipeline_generation(self, generate_command, temp_project):
        """Test executing pipeline generation."""
        mock_facade = Mock(spec=LakehousePlumberApplicationFacade)
        mock_response = GenerationResponse(
            success=True,
            generated_files={"test.py": "# Generated"},
            files_written=1,
            total_flowgroups=1,
            output_location=Path("/output"),
            performance_info={}
        )
        mock_facade.generate_pipeline.return_value = mock_response
        
        output_dir = temp_project / "generated" / "dev"
        response = generate_command._execute_pipeline_generation(
            mock_facade, "test_pipeline", "dev", output_dir,
            dry_run=False, force=False, include_tests=False,
            no_cleanup=False, pipeline_config_path=None
        )
        
        assert response == mock_response
        mock_facade.generate_pipeline.assert_called_once()
    
    def test_discover_pipelines_for_generation_specific(self, generate_command):
        """Test discovering pipelines with specific pipeline."""
        mock_facade = Mock()
        
        result = generate_command._discover_pipelines_for_generation("test_pipeline", mock_facade)
        
        assert result == ["test_pipeline"]
        mock_facade.orchestrator.discover_all_flowgroups.assert_not_called()
    
    def test_discover_pipelines_for_generation_all(self, generate_command):
        """Test discovering all pipelines."""
        from lhp.models.config import FlowGroup
        
        mock_facade = Mock()
        mock_facade.orchestrator.discover_all_flowgroups.return_value = [
            FlowGroup(pipeline="pipeline1", flowgroup="fg1", actions=[]),
            FlowGroup(pipeline="pipeline2", flowgroup="fg2", actions=[]),
            FlowGroup(pipeline="pipeline1", flowgroup="fg3", actions=[])
        ]
        
        result = generate_command._discover_pipelines_for_generation(None, mock_facade)
        
        assert len(result) == 2
        assert "pipeline1" in result
        assert "pipeline2" in result
    
    def test_discover_pipelines_for_generation_empty(self, generate_command):
        """Test discovering pipelines when none exist."""
        mock_facade = Mock()
        mock_facade.orchestrator.discover_all_flowgroups.return_value = []
        
        with patch('click.echo'):
            result = generate_command._discover_pipelines_for_generation(None, mock_facade)
        
        assert result == []
    
    def test_discover_pipelines_for_generation_error(self, generate_command):
        """Test discovering pipelines when error occurs."""
        mock_facade = Mock()
        mock_facade.orchestrator.discover_all_flowgroups.side_effect = Exception("Discovery error")
        
        with patch('click.echo'):
            result = generate_command._discover_pipelines_for_generation(None, mock_facade)
        
        assert result == []
    
    def test_display_generation_analysis_force_mode(self, generate_command):
        """Test displaying generation analysis with force mode."""
        mock_facade = Mock()
        mock_response = AnalysisResponse(
            success=True,
            pipelines_needing_generation={},
            pipelines_up_to_date={},
            has_global_changes=False,
            global_changes=[],
            include_tests_context_applied=False,
            total_new_files=0,
            total_stale_files=0,
            total_up_to_date_files=0
        )
        mock_facade.analyze_staleness.return_value = mock_response
        
        with patch('click.echo') as mock_echo:
            generate_command._display_generation_analysis(
                mock_facade, ["test_pipeline"], "dev", 
                include_tests=False, force=True, no_cleanup=False
            )
            
            assert mock_echo.called
            calls = [str(call) for call in mock_echo.call_args_list]
            assert any("Force mode" in str(call) for call in calls)
    
    def test_handle_cleanup_operations_with_orphaned_files(self, generate_command):
        """Test handling cleanup operations with orphaned files."""
        mock_facade = Mock()
        mock_facade.state_manager.find_orphaned_files.return_value = ["file1.py", "file2.py"]
        mock_facade.state_manager.cleanup_orphaned_files.return_value = ["file1.py", "file2.py"]
        
        output_dir = Path("/output")
        
        with patch('click.echo') as mock_echo:
            generate_command._handle_cleanup_operations(mock_facade, "dev", output_dir, dry_run=False)
            
            assert mock_echo.called
            calls = [str(call) for call in mock_echo.call_args_list]
            assert any("Cleaning up" in str(call) for call in calls)
            assert any("file1.py" in str(call) for call in calls)
    
    def test_handle_cleanup_operations_dry_run(self, generate_command):
        """Test handling cleanup operations in dry-run mode."""
        mock_facade = Mock()
        mock_facade.state_manager.find_orphaned_files.return_value = ["file1.py"]
        
        output_dir = Path("/output")
        
        with patch('click.echo') as mock_echo:
            generate_command._handle_cleanup_operations(mock_facade, "dev", output_dir, dry_run=True)
            
            assert mock_echo.called
            calls = [str(call) for call in mock_echo.call_args_list]
            assert any("Would clean up" in str(call) for call in calls)
            mock_facade.state_manager.cleanup_orphaned_files.assert_not_called()
    
    def test_handle_cleanup_operations_no_orphaned_files(self, generate_command):
        """Test handling cleanup operations with no orphaned files."""
        mock_facade = Mock()
        mock_facade.state_manager.find_orphaned_files.return_value = []
        
        output_dir = Path("/output")
        
        with patch('click.echo') as mock_echo:
            generate_command._handle_cleanup_operations(mock_facade, "dev", output_dir, dry_run=False)
            
            assert mock_echo.called
            calls = [str(call) for call in mock_echo.call_args_list]
            assert any("No orphaned files" in str(call) for call in calls)
    
    def test_handle_cleanup_operations_no_state_manager(self, generate_command):
        """Test handling cleanup operations without state manager."""
        mock_facade = Mock()
        mock_facade.state_manager = None
        
        output_dir = Path("/output")
        
        # Should not raise error
        generate_command._handle_cleanup_operations(mock_facade, "dev", output_dir, dry_run=False)
    
    def test_handle_bundle_operations_enabled(self, generate_command, temp_project):
        """Test handling bundle operations when enabled."""
        output_dir = temp_project / "generated" / "dev"
        output_dir.mkdir(parents=True)
        
        # Create databricks.yml
        (temp_project / "databricks.yml").write_text("targets:\n  dev: {}")
        
        with patch('click.echo') as mock_echo, \
             patch('lhp.cli.commands.generate_command.BundleManager') as mock_bundle_manager:
            mock_manager_instance = Mock()
            mock_bundle_manager.return_value = mock_manager_instance
            
            generate_command._handle_bundle_operations(
                temp_project, output_dir, "dev", 
                no_bundle=False, dry_run=False, force=False, pipeline_config_path=None
            )
            
            assert mock_echo.called
            calls = [str(call) for call in mock_echo.call_args_list]
            assert any("Bundle support detected" in str(call) for call in calls)
            mock_manager_instance.sync_resources_with_generated_files.assert_called_once()
    
    def test_handle_bundle_operations_dry_run(self, generate_command, temp_project):
        """Test handling bundle operations in dry-run mode."""
        output_dir = temp_project / "generated" / "dev"
        output_dir.mkdir(parents=True)
        
        (temp_project / "databricks.yml").write_text("targets:\n  dev: {}")
        
        with patch('click.echo') as mock_echo, \
             patch('lhp.cli.commands.generate_command.BundleManager') as mock_bundle_manager:
            generate_command._handle_bundle_operations(
                temp_project, output_dir, "dev",
                no_bundle=False, dry_run=True, force=False, pipeline_config_path=None
            )
            
            assert mock_echo.called
            calls = [str(call) for call in mock_echo.call_args_list]
            assert any("would be performed" in str(call).lower() for call in calls)
            mock_bundle_manager.assert_not_called()
    
    def test_handle_bundle_operations_with_force_and_config(self, generate_command, temp_project):
        """Test handling bundle operations with force and pipeline config."""
        output_dir = temp_project / "generated" / "dev"
        output_dir.mkdir(parents=True)
        
        (temp_project / "databricks.yml").write_text("targets:\n  dev: {}")
        
        with patch('click.echo') as mock_echo, \
             patch('lhp.cli.commands.generate_command.BundleManager') as mock_bundle_manager:
            mock_manager_instance = Mock()
            mock_bundle_manager.return_value = mock_manager_instance
            
            generate_command._handle_bundle_operations(
                temp_project, output_dir, "dev",
                no_bundle=False, dry_run=False, force=True, 
                pipeline_config_path="pipeline_config.yaml"
            )
            
            assert mock_echo.called
            calls = [str(call) for call in mock_echo.call_args_list]
            assert any("Force regenerating" in str(call) for call in calls)
    
    def test_handle_bundle_operations_bundle_error(self, generate_command, temp_project):
        """Test handling bundle operations when bundle error occurs."""
        output_dir = temp_project / "generated" / "dev"
        output_dir.mkdir(parents=True)
        
        (temp_project / "databricks.yml").write_text("targets:\n  dev: {}")
        
        with patch('click.echo') as mock_echo, \
             patch('lhp.cli.commands.generate_command.BundleManager') as mock_bundle_manager, \
             patch('sys.exit') as mock_exit:
            mock_manager_instance = Mock()
            mock_bundle_manager.return_value = mock_manager_instance
            mock_manager_instance.sync_resources_with_generated_files.side_effect = BundleResourceError("Bundle error")
            
            generate_command._handle_bundle_operations(
                temp_project, output_dir, "dev",
                no_bundle=False, dry_run=False, force=False, pipeline_config_path=None
            )
            
            assert mock_echo.called
            calls = [str(call) for call in mock_echo.call_args_list]
            assert any("Bundle sync failed" in str(call) for call in calls)
            mock_exit.assert_called_once_with(1)
    
    def test_handle_bundle_operations_unexpected_error(self, generate_command, temp_project):
        """Test handling bundle operations when unexpected error occurs."""
        output_dir = temp_project / "generated" / "dev"
        output_dir.mkdir(parents=True)
        
        (temp_project / "databricks.yml").write_text("targets:\n  dev: {}")
        
        with patch('click.echo') as mock_echo, \
             patch('lhp.cli.commands.generate_command.BundleManager') as mock_bundle_manager, \
             patch('sys.exit') as mock_exit:
            mock_bundle_manager.side_effect = Exception("Unexpected error")
            
            generate_command._handle_bundle_operations(
                temp_project, output_dir, "dev",
                no_bundle=False, dry_run=False, force=False, pipeline_config_path=None
            )
            
            assert mock_echo.called
            calls = [str(call) for call in mock_echo.call_args_list]
            assert any("Unexpected bundle error" in str(call) for call in calls)
            mock_exit.assert_called_once_with(1)
    
    def test_get_user_input(self, generate_command):
        """Test getting user input."""
        with patch('builtins.input', return_value="user response"):
            result = generate_command.get_user_input("Prompt: ")
            assert result == "user response"


class TestGenerateCommandExecute:
    """Test execute method of GenerateCommand."""
    
    def test_execute_no_pipelines_found(self, generate_command, temp_project):
        """Test execute when no pipelines are found."""
        with patch.object(generate_command, 'setup_from_context'), \
             patch.object(generate_command, 'ensure_project_root', return_value=temp_project), \
             patch.object(generate_command, '_display_startup_message'), \
             patch.object(generate_command, 'check_substitution_file', return_value=temp_project / "substitutions" / "dev.yaml"), \
             patch.object(generate_command, '_create_application_facade') as mock_facade, \
             patch.object(generate_command, '_discover_pipelines_for_generation', return_value=[]), \
             patch('click.echo'), \
             patch('sys.exit') as mock_exit:
            
            # Should exit before reaching other methods, so no need to mock them fully
            generate_command.execute("dev")
            
            mock_exit.assert_called_once_with(1)
    
    def test_execute_basic_flow(self, generate_command, temp_project):
        """Test basic execute flow."""
        from lhp.models.config import FlowGroup
        
        output_dir = temp_project / "generated" / "dev"
        output_dir.mkdir(parents=True)
        
        mock_response = GenerationResponse(
            success=True,
            generated_files={"test.py": "# Generated"},
            files_written=1,
            total_flowgroups=1,
            output_location=output_dir,
            performance_info={}
        )
        
        mock_analysis_response = AnalysisResponse(
            success=True,
            pipelines_needing_generation={},
            pipelines_up_to_date={},
            has_global_changes=False,
            global_changes=[],
            include_tests_context_applied=False,
            total_new_files=0,
            total_stale_files=0,
            total_up_to_date_files=0
        )
        
        with patch.object(generate_command, 'setup_from_context'), \
             patch.object(generate_command, 'ensure_project_root', return_value=temp_project), \
             patch.object(generate_command, '_display_startup_message'), \
             patch.object(generate_command, 'check_substitution_file', return_value=temp_project / "substitutions" / "dev.yaml"), \
             patch.object(generate_command, '_create_application_facade') as mock_facade, \
             patch.object(generate_command, '_discover_pipelines_for_generation', return_value=["test_pipeline"]), \
             patch.object(generate_command, '_handle_cleanup_operations'), \
             patch.object(generate_command, '_display_generation_analysis'), \
             patch.object(generate_command, '_execute_pipeline_generation', return_value=mock_response), \
             patch.object(generate_command, '_display_generation_response'), \
             patch.object(generate_command, '_handle_bundle_operations'), \
             patch.object(generate_command, '_display_completion_message'), \
             patch('click.echo'):
            
            mock_facade_instance = Mock()
            mock_facade.return_value = mock_facade_instance
            
            generate_command.execute("dev")
            
            # Verify key methods were called
            generate_command._discover_pipelines_for_generation.assert_called_once()
            generate_command._execute_pipeline_generation.assert_called_once()
    
    def test_execute_with_dry_run(self, generate_command, temp_project):
        """Test execute with dry-run flag."""
        mock_response = GenerationResponse(
            success=True,
            generated_files={"test.py": "# Generated"},
            files_written=0,
            total_flowgroups=1,
            output_location=None,
            performance_info={"dry_run": True}
        )
        
        with patch.object(generate_command, 'setup_from_context'), \
             patch.object(generate_command, 'ensure_project_root', return_value=temp_project), \
             patch.object(generate_command, '_display_startup_message'), \
             patch.object(generate_command, 'check_substitution_file', return_value=temp_project / "substitutions" / "dev.yaml"), \
             patch.object(generate_command, '_create_application_facade') as mock_facade, \
             patch.object(generate_command, '_discover_pipelines_for_generation', return_value=["test_pipeline"]), \
             patch.object(generate_command, '_handle_cleanup_operations'), \
             patch.object(generate_command, '_display_generation_analysis'), \
             patch.object(generate_command, '_execute_pipeline_generation', return_value=mock_response), \
             patch.object(generate_command, '_display_generation_response'), \
             patch.object(generate_command, '_handle_bundle_operations'), \
             patch.object(generate_command, '_display_completion_message'), \
             patch('click.echo'):
            
            mock_facade_instance = Mock()
            mock_facade.return_value = mock_facade_instance
            
            generate_command.execute("dev", dry_run=True)
            
            # Verify dry_run was passed through
            call_args = generate_command._execute_pipeline_generation.call_args
            # Check both positional and keyword arguments
            assert call_args.kwargs.get('dry_run', False) is True or (len(call_args.args) > 4 and call_args.args[4] is True)
    
    def test_execute_with_force(self, generate_command, temp_project):
        """Test execute with force flag."""
        with patch.object(generate_command, 'setup_from_context'), \
             patch.object(generate_command, 'ensure_project_root', return_value=temp_project), \
             patch.object(generate_command, '_display_startup_message'), \
             patch.object(generate_command, 'check_substitution_file', return_value=temp_project / "substitutions" / "dev.yaml"), \
             patch.object(generate_command, '_create_application_facade') as mock_facade, \
             patch.object(generate_command, '_discover_pipelines_for_generation', return_value=["test_pipeline"]), \
             patch.object(generate_command, '_handle_cleanup_operations'), \
             patch.object(generate_command, '_display_generation_analysis') as mock_analysis, \
             patch.object(generate_command, '_execute_pipeline_generation'), \
             patch.object(generate_command, '_display_generation_response'), \
             patch.object(generate_command, '_handle_bundle_operations'), \
             patch.object(generate_command, '_display_completion_message'), \
             patch('click.echo'):
            
            mock_facade_instance = Mock()
            mock_facade.return_value = mock_facade_instance
            
            generate_command.execute("dev", force=True)
            
            # Verify force was passed to analysis
            call_args = mock_analysis.call_args
            # force is the 5th positional argument (index 4)
            assert len(call_args.args) > 4 and call_args.args[4] is True
    
    def test_execute_with_no_cleanup(self, generate_command, temp_project):
        """Test execute with no_cleanup flag."""
        with patch.object(generate_command, 'setup_from_context'), \
             patch.object(generate_command, 'ensure_project_root', return_value=temp_project), \
             patch.object(generate_command, '_display_startup_message'), \
             patch.object(generate_command, 'check_substitution_file', return_value=temp_project / "substitutions" / "dev.yaml"), \
             patch.object(generate_command, '_create_application_facade') as mock_facade, \
             patch.object(generate_command, '_discover_pipelines_for_generation', return_value=["test_pipeline"]), \
             patch.object(generate_command, '_handle_cleanup_operations') as mock_cleanup, \
             patch.object(generate_command, '_display_generation_analysis'), \
             patch.object(generate_command, '_execute_pipeline_generation'), \
             patch.object(generate_command, '_display_generation_response'), \
             patch.object(generate_command, '_handle_bundle_operations'), \
             patch.object(generate_command, '_display_completion_message'), \
             patch('click.echo'):
            
            mock_facade_instance = Mock()
            mock_facade.return_value = mock_facade_instance
            
            generate_command.execute("dev", no_cleanup=True)
            
            # Verify cleanup was not called
            mock_cleanup.assert_not_called()
    
    def test_execute_with_no_bundle(self, generate_command, temp_project):
        """Test execute with no_bundle flag."""
        with patch.object(generate_command, 'setup_from_context'), \
             patch.object(generate_command, 'ensure_project_root', return_value=temp_project), \
             patch.object(generate_command, '_display_startup_message'), \
             patch.object(generate_command, 'check_substitution_file', return_value=temp_project / "substitutions" / "dev.yaml"), \
             patch.object(generate_command, '_create_application_facade') as mock_facade, \
             patch.object(generate_command, '_discover_pipelines_for_generation', return_value=["test_pipeline"]), \
             patch.object(generate_command, '_handle_cleanup_operations'), \
             patch.object(generate_command, '_display_generation_analysis'), \
             patch.object(generate_command, '_execute_pipeline_generation'), \
             patch.object(generate_command, '_display_generation_response'), \
             patch.object(generate_command, '_handle_bundle_operations') as mock_bundle, \
             patch.object(generate_command, '_display_completion_message'), \
             patch('click.echo'):
            
            mock_facade_instance = Mock()
            mock_facade.return_value = mock_facade_instance
            
            generate_command.execute("dev", no_bundle=True)
            
            # Verify bundle operations were not called
            mock_bundle.assert_not_called()
    
    def test_execute_with_custom_output(self, generate_command, temp_project):
        """Test execute with custom output directory."""
        custom_output = temp_project / "custom_output"
        
        with patch.object(generate_command, 'setup_from_context'), \
             patch.object(generate_command, 'ensure_project_root', return_value=temp_project), \
             patch.object(generate_command, '_display_startup_message'), \
             patch.object(generate_command, 'check_substitution_file', return_value=temp_project / "substitutions" / "dev.yaml"), \
             patch.object(generate_command, '_create_application_facade') as mock_facade, \
             patch.object(generate_command, '_discover_pipelines_for_generation', return_value=["test_pipeline"]), \
             patch.object(generate_command, '_handle_cleanup_operations'), \
             patch.object(generate_command, '_display_generation_analysis'), \
             patch.object(generate_command, '_execute_pipeline_generation') as mock_execute, \
             patch.object(generate_command, '_display_generation_response'), \
             patch.object(generate_command, '_handle_bundle_operations'), \
             patch.object(generate_command, '_display_completion_message'), \
             patch('click.echo'):
            
            mock_facade_instance = Mock()
            mock_facade.return_value = mock_facade_instance
            
            generate_command.execute("dev", output=str(custom_output))
            
            # Verify custom output was used - check the actual call
            call_args = mock_execute.call_args
            # output_dir is the 4th positional argument (index 3)
            if len(call_args.args) > 3:
                output_dir_arg = call_args.args[3]
            else:
                output_dir_arg = call_args.kwargs.get('output_dir')
            assert output_dir_arg == custom_output

