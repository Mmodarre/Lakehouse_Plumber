"""Tests for command pattern implementations."""

import pytest
import tempfile
from pathlib import Path
from unittest.mock import Mock, MagicMock

from lhp.core.commands import (
    GeneratePipelineCommand, ValidatePipelineCommand, AnalyzeStalenessCommand,
    CommandFactory, CommandRegistry, CommandContext, CommandResult,
    GenerationCommandResult, ValidationCommandResult, AnalysisCommandResult
)
from lhp.core.state_manager import StateManager


class TestCommandResults:
    """Test command result objects."""
    
    def test_command_result_basic(self):
        """Test basic CommandResult functionality."""
        # Test successful result
        result = CommandResult(success=True, data="test_data", metadata={"test": "meta"})
        assert result.is_successful() == True
        assert result.get_data() == "test_data"
        assert result.get_error() is None
        assert result.metadata["test"] == "meta"
        
        # Test failed result
        failed_result = CommandResult(success=False, error="Test error")
        assert failed_result.is_successful() == False
        assert failed_result.get_error() == "Test error"
        assert failed_result.get_data() is None
    
    def test_generation_command_result(self):
        """Test GenerationCommandResult specialization."""
        result = GenerationCommandResult(
            success=True,
            generated_files={"test.py": "# Generated code"},
            files_written=1,
            total_flowgroups=1,
            performance_stats={"time": 1.5}
        )
        
        assert result.is_successful() == True
        assert len(result.generated_files) == 1
        assert result.files_written == 1
        assert result.total_flowgroups == 1
        assert result.performance_stats["time"] == 1.5
    
    def test_validation_command_result(self):
        """Test ValidationCommandResult specialization."""
        result = ValidationCommandResult(
            success=False,
            errors=["Error 1", "Error 2"],
            warnings=["Warning 1"],
            validated_pipelines=["pipeline1"]
        )
        
        assert result.is_successful() == False
        assert len(result.errors) == 2
        assert len(result.warnings) == 1
        assert result.validated_pipelines == ["pipeline1"]


class TestCommandContext:
    """Test CommandContext functionality."""
    
    def test_command_context_creation(self):
        """Test CommandContext object creation."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            mock_orchestrator = Mock()
            mock_state_manager = Mock(spec=StateManager)
            
            context = CommandContext(
                project_root=project_root,
                env="dev",
                orchestrator=mock_orchestrator,
                state_manager=mock_state_manager,
                custom_param="test_value",
                pipeline_identifier="test_pipeline"
            )
            
            assert context.project_root == project_root
            assert context.env == "dev"
            assert context.orchestrator == mock_orchestrator
            assert context.state_manager == mock_state_manager
            assert context.has_state_management() == True
            assert context.get_parameter('custom_param') == "test_value"
            assert context.get_parameter('nonexistent', 'default') == 'default'
    
    def test_command_context_no_state_manager(self):
        """Test CommandContext without state manager."""
        context = CommandContext(Path("/test"), "dev")
        assert context.has_state_management() == False


class TestIndividualCommands:
    """Test individual command implementations."""
    
    def test_generate_pipeline_command(self):
        """Test GeneratePipelineCommand."""
        command = GeneratePipelineCommand()
        assert command.name == "generate_pipeline"
        
        # Mock orchestrator
        mock_orchestrator = Mock()
        mock_orchestrator.generate_pipeline_by_field.return_value = {"test.py": "# Generated"}
        
        context = CommandContext(
            project_root=Path("/test"),
            env="dev", 
            orchestrator=mock_orchestrator,
            pipeline_identifier="test_pipeline",
            include_tests=True,
            output_dir=Path("/output")
        )
        
        # Test successful execution
        assert command.validate(context) == True
        
        result = command.execute(context)
        assert isinstance(result, GenerationCommandResult)
        assert result.is_successful() == True
        assert result.files_written == 1  # Output dir provided and dry_run is False
        assert len(result.generated_files) == 1
    
    def test_generate_pipeline_command_validation_failure(self):
        """Test GeneratePipelineCommand validation failure."""
        command = GeneratePipelineCommand()
        
        # Context missing pipeline_identifier
        context = CommandContext(
            project_root=Path("/test"),
            env="dev",
            orchestrator=Mock()
        )
        
        assert command.validate(context) == False
        
        result = command.execute(context)
        assert result.is_successful() == False
        assert "pipeline_identifier is required" in result.error
    
    def test_validate_pipeline_command(self):
        """Test ValidatePipelineCommand."""
        command = ValidatePipelineCommand()
        assert command.name == "validate_pipeline"
        
        # Mock orchestrator
        mock_orchestrator = Mock()
        mock_orchestrator.validate_pipeline_by_field.return_value = ([], ["warning1"])
        
        context = CommandContext(
            project_root=Path("/test"),
            env="dev",
            orchestrator=mock_orchestrator,
            pipeline_identifier="test_pipeline"
        )
        
        # Test successful execution
        assert command.validate(context) == True
        
        result = command.execute(context)
        assert isinstance(result, ValidationCommandResult)
        assert result.is_successful() == True  # No errors
        assert len(result.warnings) == 1
        assert result.validated_pipelines == ["test_pipeline"]
    
    def test_analyze_staleness_command(self):
        """Test AnalyzeStalenessCommand."""
        command = AnalyzeStalenessCommand()
        assert command.name == "analyze_staleness"
        
        # Mock orchestrator and analysis result
        mock_analysis = Mock()
        mock_analysis.has_work_to_do.return_value = True
        mock_analysis.total_new_files = 2
        mock_analysis.total_stale_files = 1
        mock_analysis.include_tests_context_applied = True
        
        mock_orchestrator = Mock()
        mock_orchestrator.analyze_generation_requirements.return_value = mock_analysis
        
        context = CommandContext(
            project_root=Path("/test"),
            env="dev",
            orchestrator=mock_orchestrator,
            pipeline_names=["pipeline1", "pipeline2"],
            include_tests=False
        )
        
        # Test successful execution
        assert command.validate(context) == True
        
        result = command.execute(context)
        assert isinstance(result, AnalysisCommandResult)
        assert result.is_successful() == True
        assert result.generation_analysis == mock_analysis
        assert result.staleness_report["has_work"] == True
        assert result.staleness_report["total_new"] == 2


class TestCommandFactory:
    """Test CommandFactory functionality."""
    
    def test_create_command(self):
        """Test creating commands by type."""
        # Test all supported command types
        generate_cmd = CommandFactory.create_command("generate")
        assert isinstance(generate_cmd, GeneratePipelineCommand)
        
        validate_cmd = CommandFactory.create_command("validate")
        assert isinstance(validate_cmd, ValidatePipelineCommand)
        
        analyze_cmd = CommandFactory.create_command("analyze")
        assert isinstance(analyze_cmd, AnalyzeStalenessCommand)
    
    def test_create_unknown_command(self):
        """Test creating unknown command type raises error."""
        with pytest.raises(ValueError, match="Unknown command type: unknown"):
            CommandFactory.create_command("unknown")
    
    def test_get_available_commands(self):
        """Test getting all available commands."""
        commands = CommandFactory.get_available_commands()
        assert len(commands) == 3
        assert "generate" in commands
        assert "validate" in commands
        assert "analyze" in commands


class TestCommandRegistry:
    """Test CommandRegistry functionality."""
    
    def test_command_registry_execution(self):
        """Test command registry execution."""
        registry = CommandRegistry()
        
        # Mock context with successful command
        mock_orchestrator = Mock()
        mock_orchestrator.analyze_generation_requirements.return_value = Mock()
        mock_orchestrator.analyze_generation_requirements.return_value.has_work_to_do.return_value = False
        mock_orchestrator.analyze_generation_requirements.return_value.total_new_files = 0
        mock_orchestrator.analyze_generation_requirements.return_value.total_stale_files = 0
        mock_orchestrator.analyze_generation_requirements.return_value.include_tests_context_applied = False
        
        context = CommandContext(
            project_root=Path("/test"),
            env="dev",
            orchestrator=mock_orchestrator,
            pipeline_names=["test_pipeline"]
        )
        
        # Test successful command execution
        result = registry.execute_command("analyze", context)
        assert result.is_successful() == True
        assert isinstance(result, AnalysisCommandResult)
    
    def test_command_registry_unknown_command(self):
        """Test executing unknown command."""
        registry = CommandRegistry()
        context = CommandContext(Path("/test"), "dev")
        
        result = registry.execute_command("unknown", context)
        assert result.is_successful() == False
        assert "Unknown command: unknown" in result.error
    
    def test_command_registry_validation_failure(self):
        """Test command execution with validation failure."""
        registry = CommandRegistry()
        
        # Context missing required parameters
        context = CommandContext(Path("/test"), "dev", orchestrator=Mock())
        
        result = registry.execute_command("generate", context)
        assert result.is_successful() == False
        assert "validation failed" in result.error
    
    def test_list_commands(self):
        """Test listing available commands."""
        registry = CommandRegistry()
        commands = registry.list_commands()
        
        assert len(commands) == 3
        assert "generate" in commands
        assert "validate" in commands
        assert "analyze" in commands
    
    def test_get_command(self):
        """Test getting specific command."""
        registry = CommandRegistry()
        
        command = registry.get_command("generate")
        assert command is not None
        assert isinstance(command, GeneratePipelineCommand)
        
        unknown = registry.get_command("unknown")
        assert unknown is None
