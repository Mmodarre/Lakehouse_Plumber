"""Command pattern implementations for LakehousePlumber orchestration."""

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Dict, List, Any, Optional, Union
from dataclasses import dataclass
import logging

from .state_manager import StateManager
from .services.generation_planning_service import GenerationPlan
from ..models.config import FlowGroup


class CommandResult:
    """Base result object for command execution."""
    
    def __init__(self, success: bool, data: Any = None, error: str = None, 
                 metadata: Dict[str, Any] = None):
        self.success = success
        self.data = data
        self.error = error
        self.metadata = metadata or {}
    
    def is_successful(self) -> bool:
        """Check if command executed successfully."""
        return self.success
    
    def get_data(self):
        """Get command execution data."""
        return self.data
    
    def get_error(self) -> Optional[str]:
        """Get error message if command failed."""
        return self.error


class GenerationCommandResult(CommandResult):
    """Specialized result for generation commands."""
    
    def __init__(self, success: bool, data: Any = None, error: str = None, 
                 metadata: Dict[str, Any] = None,
                 generated_files: Dict[str, str] = None,
                 files_written: int = 0, total_flowgroups: int = 0,
                 performance_stats: Dict[str, Any] = None):
        super().__init__(success, data, error, metadata)
        self.generated_files = generated_files or {}
        self.files_written = files_written
        self.total_flowgroups = total_flowgroups
        self.performance_stats = performance_stats or {}


class ValidationCommandResult(CommandResult):
    """Specialized result for validation commands."""
    
    def __init__(self, success: bool, data: Any = None, error: str = None,
                 metadata: Dict[str, Any] = None,
                 errors: List[str] = None, warnings: List[str] = None,
                 validated_pipelines: List[str] = None):
        super().__init__(success, data, error, metadata)
        self.errors = errors or []
        self.warnings = warnings or []
        self.validated_pipelines = validated_pipelines or []


class AnalysisCommandResult(CommandResult):
    """Specialized result for analysis commands."""
    
    def __init__(self, success: bool, data: Any = None, error: str = None,
                 metadata: Dict[str, Any] = None,
                 generation_analysis: Any = None,
                 staleness_report: Dict[str, Any] = None,
                 performance_metrics: Dict[str, Any] = None):
        super().__init__(success, data, error, metadata)
        self.generation_analysis = generation_analysis
        self.staleness_report = staleness_report or {}
        self.performance_metrics = performance_metrics or {}


class Command(ABC):
    """Abstract base command for orchestration operations."""
    
    def __init__(self, name: str):
        self.name = name
        self.logger = logging.getLogger(self.__class__.__name__)
    
    @abstractmethod
    def execute(self, context: 'CommandContext') -> CommandResult:
        """
        Execute the command with given context.
        
        Args:
            context: CommandContext with all necessary execution information
            
        Returns:
            CommandResult with execution results and metadata
        """
        pass
    
    @abstractmethod
    def validate(self, context: 'CommandContext') -> bool:
        """
        Validate that command can be executed with given context.
        
        Args:
            context: CommandContext to validate
            
        Returns:
            True if command can be executed, False otherwise
        """
        pass


class CommandContext:
    """Context object containing all information needed for command execution."""
    
    def __init__(self, project_root: Path, env: str, orchestrator=None,
                 state_manager: StateManager = None, **kwargs):
        self.project_root = project_root
        self.env = env
        self.orchestrator = orchestrator
        self.state_manager = state_manager
        
        # Store additional parameters
        for key, value in kwargs.items():
            setattr(self, key, value)
    
    def has_state_management(self) -> bool:
        """Check if state management is available."""
        return self.state_manager is not None
    
    def get_parameter(self, name: str, default=None):
        """Get a parameter value with optional default."""
        return getattr(self, name, default)


class GeneratePipelineCommand(Command):
    """Command for pipeline generation operations."""
    
    def __init__(self):
        super().__init__("generate_pipeline")
    
    def execute(self, context: CommandContext) -> GenerationCommandResult:
        """Execute pipeline generation."""
        try:
            # Extract parameters from context
            pipeline_identifier = context.get_parameter('pipeline_identifier')
            include_tests = context.get_parameter('include_tests', False)
            force_all = context.get_parameter('force_all', False)
            specific_flowgroups = context.get_parameter('specific_flowgroups')
            output_dir = context.get_parameter('output_dir')
            dry_run = context.get_parameter('dry_run', False)
            
            if not pipeline_identifier:
                return GenerationCommandResult(
                    success=False,
                    error="pipeline_identifier is required",
                    generated_files={},
                    files_written=0,
                    total_flowgroups=0
                )
            
            # Delegate to orchestrator for actual generation
            # Always use generate_pipeline_by_field for consistent Python file handling
            generated_files = context.orchestrator.generate_pipeline_by_field(
                pipeline_field=pipeline_identifier,
                env=context.env,
                output_dir=output_dir if not dry_run else None,
                state_manager=context.state_manager,
                force_all=force_all,
                specific_flowgroups=specific_flowgroups,
                include_tests=include_tests
            )
            
            return GenerationCommandResult(
                success=True,
                data=generated_files,
                generated_files=generated_files,
                files_written=len(generated_files) if not dry_run else 0,
                total_flowgroups=len(generated_files),
                performance_stats={"dry_run": dry_run}
            )
            
        except Exception as e:
            self.logger.error(f"Generation command failed: {e}")
            return GenerationCommandResult(
                success=False,
                error=str(e),
                generated_files={},
                files_written=0,
                total_flowgroups=0
            )
    
    def validate(self, context: CommandContext) -> bool:
        """Validate generation command can be executed."""
        return (
            context.orchestrator is not None and
            context.project_root is not None and
            context.env is not None and
            context.get_parameter('pipeline_identifier') is not None
        )


class ValidatePipelineCommand(Command):
    """Command for pipeline validation operations."""
    
    def __init__(self):
        super().__init__("validate_pipeline")
    
    def execute(self, context: CommandContext) -> ValidationCommandResult:
        """Execute pipeline validation."""
        try:
            pipeline_identifier = context.get_parameter('pipeline_identifier')
            
            if not pipeline_identifier:
                return ValidationCommandResult(
                    success=False,
                    error="pipeline_identifier is required",
                    errors=["pipeline_identifier is required"],
                    warnings=[],
                    validated_pipelines=[]
                )
            
            # Delegate to orchestrator for validation
            errors, warnings = context.orchestrator.validate_pipeline_by_field(
                pipeline_field=pipeline_identifier,
                env=context.env
            )
            
            return ValidationCommandResult(
                success=len(errors) == 0,
                data={"errors": errors, "warnings": warnings},
                errors=errors,
                warnings=warnings,
                validated_pipelines=[pipeline_identifier]
            )
            
        except Exception as e:
            self.logger.error(f"Validation command failed: {e}")
            return ValidationCommandResult(
                success=False,
                error=str(e),
                errors=[str(e)],
                warnings=[],
                validated_pipelines=[]
            )
    
    def validate(self, context: CommandContext) -> bool:
        """Validate validation command can be executed."""
        return (
            context.orchestrator is not None and
            context.project_root is not None and
            context.env is not None and
            context.get_parameter('pipeline_identifier') is not None
        )


class AnalyzeStalenessCommand(Command):
    """Command for staleness analysis operations."""
    
    def __init__(self):
        super().__init__("analyze_staleness")
    
    def execute(self, context: CommandContext) -> AnalysisCommandResult:
        """Execute staleness analysis."""
        try:
            pipeline_names = context.get_parameter('pipeline_names', [])
            include_tests = context.get_parameter('include_tests', False)
            force = context.get_parameter('force', False)
            
            if not pipeline_names:
                return AnalysisCommandResult(
                    success=False,
                    error="pipeline_names is required",
                    generation_analysis=None
                )
            
            # Delegate to orchestrator for analysis
            analysis = context.orchestrator.analyze_generation_requirements(
                env=context.env,
                pipeline_names=pipeline_names,
                include_tests=include_tests,
                force=force,
                state_manager=context.state_manager
            )
            
            return AnalysisCommandResult(
                success=True,
                data=analysis,
                generation_analysis=analysis,
                staleness_report={
                    "has_work": analysis.has_work_to_do(),
                    "total_new": analysis.total_new_files,
                    "total_stale": analysis.total_stale_files,
                    "context_applied": analysis.include_tests_context_applied
                }
            )
            
        except Exception as e:
            self.logger.error(f"Analysis command failed: {e}")
            return AnalysisCommandResult(
                success=False,
                error=str(e),
                generation_analysis=None
            )
    
    def validate(self, context: CommandContext) -> bool:
        """Validate analysis command can be executed."""
        return (
            context.orchestrator is not None and
            context.project_root is not None and
            context.env is not None and
            bool(context.get_parameter('pipeline_names'))
        )


class CommandFactory:
    """Factory for creating command instances."""
    
    @staticmethod
    def create_command(command_type: str) -> Command:
        """
        Create command instance by type.
        
        Args:
            command_type: Type of command to create
            
        Returns:
            Command instance
            
        Raises:
            ValueError: If command type is not supported
        """
        commands = {
            "generate": GeneratePipelineCommand,
            "validate": ValidatePipelineCommand,
            "analyze": AnalyzeStalenessCommand
        }
        
        if command_type not in commands:
            raise ValueError(f"Unknown command type: {command_type}. Available: {list(commands.keys())}")
        
        return commands[command_type]()
    
    @staticmethod
    def get_available_commands() -> Dict[str, Command]:
        """Get all available command instances."""
        return {
            "generate": GeneratePipelineCommand(),
            "validate": ValidatePipelineCommand(),
            "analyze": AnalyzeStalenessCommand()
        }


class CommandRegistry:
    """Registry for managing and executing commands."""
    
    def __init__(self):
        self.commands = CommandFactory.get_available_commands()
        self.logger = logging.getLogger(__name__)
    
    def execute_command(self, command_type: str, context: CommandContext) -> CommandResult:
        """
        Execute a command with validation.
        
        Args:
            command_type: Type of command to execute
            context: CommandContext for execution
            
        Returns:
            CommandResult with execution results
        """
        if command_type not in self.commands:
            return CommandResult(
                success=False,
                error=f"Unknown command: {command_type}"
            )
        
        command = self.commands[command_type]
        
        # Validate command can be executed
        if not command.validate(context):
            return CommandResult(
                success=False,
                error=f"Command {command_type} validation failed"
            )
        
        # Execute command
        self.logger.debug(f"Executing command: {command_type}")
        result = command.execute(context)
        
        if result.success:
            self.logger.debug(f"Command {command_type} completed successfully")
        else:
            self.logger.error(f"Command {command_type} failed: {result.error}")
        
        return result
    
    def list_commands(self) -> List[str]:
        """List all available command types."""
        return list(self.commands.keys())
    
    def get_command(self, command_type: str) -> Optional[Command]:
        """Get command instance by type."""
        return self.commands.get(command_type)
