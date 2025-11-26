"""Clean Architecture layer definitions for LakehousePlumber."""

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Dict, List, Any, Optional, TYPE_CHECKING
from dataclasses import dataclass
import logging

if TYPE_CHECKING:
    from .services.generation_planning_service import GenerationPlan
    from .state_manager import StateManager


# ============================================================================
# DATA TRANSFER OBJECTS (DTOs) - Cross-layer communication
# ============================================================================

@dataclass
class PipelineGenerationRequest:
    """DTO for pipeline generation requests from presentation to application layer."""
    
    pipeline_identifier: str
    environment: str
    include_tests: bool = False
    force_all: bool = False
    specific_flowgroups: Optional[List[str]] = None
    output_directory: Optional[Path] = None
    dry_run: bool = False
    no_cleanup: bool = False
    pipeline_config_path: Optional[str] = None


@dataclass
class PipelineValidationRequest:
    """DTO for pipeline validation requests."""
    
    pipeline_identifier: str
    environment: str
    verbose: bool = False


@dataclass
class StalenessAnalysisRequest:
    """DTO for staleness analysis requests."""
    
    pipeline_names: List[str]
    environment: str
    include_tests: bool = False
    force: bool = False


@dataclass
class GenerationResponse:
    """DTO for generation responses from application to presentation layer."""
    
    success: bool
    generated_files: Dict[str, str]  # filename -> content
    files_written: int
    total_flowgroups: int
    output_location: Optional[Path]
    performance_info: Dict[str, Any]
    error_message: Optional[str] = None
    
    def is_successful(self) -> bool:
        """Check if generation was successful."""
        return self.success


@dataclass
class ValidationResponse:
    """DTO for validation responses."""
    
    success: bool
    errors: List[str]
    warnings: List[str]
    validated_pipelines: List[str]
    error_message: Optional[str] = None
    
    def has_errors(self) -> bool:
        """Check if validation found errors."""
        return len(self.errors) > 0
    
    def has_warnings(self) -> bool:
        """Check if validation found warnings."""
        return len(self.warnings) > 0


@dataclass
class AnalysisResponse:
    """DTO for staleness analysis responses."""
    
    success: bool
    pipelines_needing_generation: Dict[str, Dict]
    pipelines_up_to_date: Dict[str, int] 
    has_global_changes: bool
    global_changes: List[str]
    include_tests_context_applied: bool
    total_new_files: int
    total_stale_files: int
    total_up_to_date_files: int
    error_message: Optional[str] = None
    
    def has_work_to_do(self) -> bool:
        """Check if any generation work needs to be done."""
        return len(self.pipelines_needing_generation) > 0


# ============================================================================
# LAYER INTERFACES - Define contracts between layers
# ============================================================================

class ApplicationLayer(ABC):
    """Interface for the application layer - coordinates use cases."""
    
    @abstractmethod
    def generate_pipeline(self, request: PipelineGenerationRequest) -> GenerationResponse:
        """Coordinate pipeline generation use case."""
        pass
    
    @abstractmethod
    def validate_pipeline(self, request: PipelineValidationRequest) -> ValidationResponse:
        """Coordinate pipeline validation use case."""
        pass
    
    @abstractmethod
    def analyze_staleness(self, request: StalenessAnalysisRequest) -> AnalysisResponse:
        """Coordinate staleness analysis use case."""
        pass


class BusinessLayer(ABC):
    """Interface for the business layer - contains business rules."""
    
    @abstractmethod
    def create_generation_plan(self, env: str, pipeline_identifier: str, 
                               include_tests: bool, **kwargs) -> 'GenerationPlan':
        """Create generation plan based on business rules."""
        pass
    
    @abstractmethod
    def execute_generation_strategy(self, strategy_type: str, context: Any) -> Any:
        """Execute generation strategy based on business logic."""
        pass
    
    @abstractmethod
    def validate_configuration(self, pipeline_identifier: str, env: str) -> tuple:
        """Validate configuration based on business rules."""
        pass


class DataLayer(ABC):
    """Interface for the data layer - handles data access and persistence."""
    
    @abstractmethod
    def get_generation_state(self, env: str, pipeline: str = None) -> Dict[str, List]:
        """Get current generation state from persistence."""
        pass
    
    @abstractmethod
    def track_generated_file(self, file_path: Path, metadata: Dict[str, Any]) -> None:
        """Track generated file in persistent state."""
        pass
    
    @abstractmethod
    def cleanup_orphaned_files(self, env: str, dry_run: bool = False) -> List[str]:
        """Clean up orphaned files from persistence."""
        pass


class PresentationLayer(ABC):
    """Interface for the presentation layer - handles user interaction."""
    
    @abstractmethod
    def display_generation_results(self, response: GenerationResponse) -> None:
        """Display generation results to user."""
        pass
    
    @abstractmethod
    def display_validation_results(self, response: ValidationResponse) -> None:
        """Display validation results to user."""
        pass
    
    @abstractmethod
    def display_analysis_results(self, response: AnalysisResponse) -> None:
        """Display analysis results to user."""
        pass
    
    @abstractmethod
    def get_user_input(self, prompt: str) -> str:
        """Get input from user."""
        pass


# ============================================================================
# APPLICATION FACADE - Implements application layer interface
# ============================================================================

class LakehousePlumberApplicationFacade(ApplicationLayer):
    """
    Application layer facade providing clean interface to business layer.
    
    This facade abstracts the complexity of the orchestrator and provides
    a clean, testable interface for the CLI layer.
    """
    
    def __init__(self, orchestrator, state_manager: Optional['StateManager'] = None):
        """
        Initialize application facade.
        
        Args:
            orchestrator: Business layer orchestrator
            state_manager: Optional state manager for data layer
        """
        self.orchestrator = orchestrator
        self.state_manager = state_manager
        self.logger = logging.getLogger(__name__)
    
    def generate_pipeline(self, request: PipelineGenerationRequest) -> GenerationResponse:
        """
        Coordinate pipeline generation use case.
        
        Translates presentation layer request into business layer operations
        and returns structured response for presentation layer.
        """
        try:
            # Execute generation through orchestrator
            generated_files = self.orchestrator.generate_pipeline_by_field(
                pipeline_field=request.pipeline_identifier,
                env=request.environment,
                output_dir=request.output_directory if not request.dry_run else None,
                state_manager=self.state_manager if not request.no_cleanup else None,
                force_all=request.force_all,
                specific_flowgroups=request.specific_flowgroups,
                include_tests=request.include_tests
            )
            
            return GenerationResponse(
                success=True,
                generated_files=generated_files,
                files_written=len(generated_files) if not request.dry_run else 0,
                total_flowgroups=len(generated_files),
                output_location=request.output_directory,
                performance_info={
                    "dry_run": request.dry_run,
                    "force_all": request.force_all,
                    "include_tests": request.include_tests
                }
            )
            
        except Exception as e:
            # Log brief context without full error details (avoids duplication)
            from ..utils.error_formatter import LHPError
            if isinstance(e, LHPError):
                # LHPError already has formatted details, just log context
                self.logger.debug(f"Pipeline generation failed for {request.pipeline_identifier}")
            else:
                # Regular exception - log full details
                self.logger.error(f"Pipeline generation failed: {e}")
            
            return GenerationResponse(
                success=False,
                generated_files={},
                files_written=0,
                total_flowgroups=0,
                output_location=None,
                performance_info={},
                error_message=str(e)
            )
    
    def validate_pipeline(self, request: PipelineValidationRequest) -> ValidationResponse:
        """Coordinate pipeline validation use case."""
        try:
            errors, warnings = self.orchestrator.validate_pipeline_by_field(
                pipeline_field=request.pipeline_identifier,
                env=request.environment
            )
            
            return ValidationResponse(
                success=len(errors) == 0,
                errors=errors,
                warnings=warnings,
                validated_pipelines=[request.pipeline_identifier]
            )
            
        except Exception as e:
            self.logger.error(f"Pipeline validation failed: {e}")
            return ValidationResponse(
                success=False,
                errors=[str(e)],
                warnings=[],
                validated_pipelines=[],
                error_message=str(e)
            )
    
    def analyze_staleness(self, request: StalenessAnalysisRequest) -> AnalysisResponse:
        """Coordinate staleness analysis use case."""
        try:
            analysis = self.orchestrator.analyze_generation_requirements(
                env=request.environment,
                pipeline_names=request.pipeline_names,
                include_tests=request.include_tests,
                force=request.force,
                state_manager=self.state_manager
            )
            
            return AnalysisResponse(
                success=True,
                pipelines_needing_generation=analysis.pipelines_needing_generation,
                pipelines_up_to_date=analysis.pipelines_up_to_date,
                has_global_changes=analysis.has_global_changes,
                global_changes=analysis.global_changes,
                include_tests_context_applied=analysis.include_tests_context_applied,
                total_new_files=analysis.total_new_files,
                total_stale_files=analysis.total_stale_files,
                total_up_to_date_files=analysis.total_up_to_date_files
            )
            
        except Exception as e:
            self.logger.error(f"Staleness analysis failed: {e}")
            return AnalysisResponse(
                success=False,
                pipelines_needing_generation={},
                pipelines_up_to_date={},
                has_global_changes=False,
                global_changes=[],
                include_tests_context_applied=False,
                total_new_files=0,
                total_stale_files=0,
                total_up_to_date_files=0,
                error_message=str(e)
            )
