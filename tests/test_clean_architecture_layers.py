"""Tests for Clean Architecture layer separation."""

import pytest
import tempfile
from pathlib import Path
from unittest.mock import Mock, MagicMock

from lhp.core.layers import (
    PipelineGenerationRequest, PipelineValidationRequest, StalenessAnalysisRequest,
    GenerationResponse, ValidationResponse, AnalysisResponse,
    LakehousePlumberApplicationFacade, ApplicationLayer, BusinessLayer, DataLayer, PresentationLayer
)
from lhp.core.orchestrator import ActionOrchestrator
from lhp.core.state_manager import StateManager
from lhp.cli.commands.generate_command import GenerateCommand


class TestDataTransferObjects:
    """Test Data Transfer Objects for clean layer communication."""
    
    def test_pipeline_generation_request(self):
        """Test PipelineGenerationRequest DTO."""
        request = PipelineGenerationRequest(
            pipeline_identifier="test_pipeline",
            environment="dev",
            include_tests=True,
            force_all=False,
            output_directory=Path("/output"),
            dry_run=True
        )
        
        assert request.pipeline_identifier == "test_pipeline"
        assert request.environment == "dev"
        assert request.include_tests == True
        assert request.force_all == False
        assert request.output_directory == Path("/output")
        assert request.dry_run == True
        assert request.no_cleanup == False  # default value
    
    def test_generation_response(self):
        """Test GenerationResponse DTO."""
        response = GenerationResponse(
            success=True,
            generated_files={"test.py": "# Generated"},
            files_written=1,
            total_flowgroups=1,
            output_location=Path("/output"),
            performance_info={"time": 1.5}
        )
        
        assert response.is_successful() == True
        assert len(response.generated_files) == 1
        assert response.files_written == 1
        assert response.total_flowgroups == 1
        assert response.output_location == Path("/output")
        assert response.performance_info["time"] == 1.5
        assert response.error_message is None
    
    def test_validation_response(self):
        """Test ValidationResponse DTO."""
        response = ValidationResponse(
            success=False,
            errors=["Error 1", "Error 2"],
            warnings=["Warning 1"],
            validated_pipelines=["pipeline1"]
        )
        
        assert response.success == False
        assert response.has_errors() == True
        assert response.has_warnings() == True
        assert len(response.errors) == 2
        assert len(response.warnings) == 1
        assert response.validated_pipelines == ["pipeline1"]
    
    def test_analysis_response(self):
        """Test AnalysisResponse DTO."""
        response = AnalysisResponse(
            success=True,
            pipelines_needing_generation={"pipeline1": {"new": []}},
            pipelines_up_to_date={"pipeline2": 5},
            has_global_changes=False,
            global_changes=[],
            include_tests_context_applied=True,
            total_new_files=1,
            total_stale_files=0,
            total_up_to_date_files=5
        )
        
        assert response.success == True
        assert response.has_work_to_do() == True
        assert response.include_tests_context_applied == True
        assert response.total_new_files == 1
        assert response.total_up_to_date_files == 5


class TestApplicationFacade:
    """Test LakehousePlumberApplicationFacade."""
    
    def test_application_facade_initialization(self):
        """Test application facade initialization."""
        mock_orchestrator = Mock()
        mock_state_manager = Mock(spec=StateManager)
        
        facade = LakehousePlumberApplicationFacade(mock_orchestrator, mock_state_manager)
        
        assert facade.orchestrator == mock_orchestrator
        assert facade.state_manager == mock_state_manager
    
    def test_generate_pipeline_success(self):
        """Test successful pipeline generation through facade."""
        # Mock orchestrator
        mock_orchestrator = Mock()
        mock_orchestrator.generate_pipeline_by_field.return_value = {"test.py": "# Generated"}
        
        facade = LakehousePlumberApplicationFacade(mock_orchestrator, Mock())
        
        # Create request
        request = PipelineGenerationRequest(
            pipeline_identifier="test_pipeline",
            environment="dev",
            include_tests=True,
            output_directory=Path("/output")
        )
        
        # Execute through facade
        response = facade.generate_pipeline(request)
        
        # Verify response
        assert response.is_successful() == True
        assert len(response.generated_files) == 1
        assert response.files_written == 1
        assert response.error_message is None
        
        # Verify orchestrator was called correctly
        mock_orchestrator.generate_pipeline_by_field.assert_called_once_with(
            pipeline_field="test_pipeline",
            env="dev", 
            output_dir=Path("/output"),
            state_manager=facade.state_manager,
            force_all=False,
            specific_flowgroups=None,
            include_tests=True
        )
    
    def test_generate_pipeline_failure(self):
        """Test pipeline generation failure through facade."""
        # Mock orchestrator to raise exception
        mock_orchestrator = Mock()
        mock_orchestrator.generate_pipeline_by_field.side_effect = Exception("Generation failed")
        
        facade = LakehousePlumberApplicationFacade(mock_orchestrator, Mock())
        
        request = PipelineGenerationRequest(
            pipeline_identifier="test_pipeline", 
            environment="dev"
        )
        
        response = facade.generate_pipeline(request)
        
        assert response.is_successful() == False
        assert response.error_message == "Generation failed"
        assert response.files_written == 0
    
    def test_validate_pipeline_success(self):
        """Test successful pipeline validation through facade."""
        mock_orchestrator = Mock()
        mock_orchestrator.validate_pipeline_by_field.return_value = ([], ["warning"])
        
        facade = LakehousePlumberApplicationFacade(mock_orchestrator, Mock())
        
        request = PipelineValidationRequest(
            pipeline_identifier="test_pipeline",
            environment="dev"
        )
        
        response = facade.validate_pipeline(request)
        
        assert response.success == True
        assert response.has_errors() == False
        assert response.has_warnings() == True
        assert len(response.warnings) == 1


class TestLayerInterfaces:
    """Test that layer interfaces are properly implemented."""
    
    def test_orchestrator_implements_business_layer(self):
        """Test that ActionOrchestrator implements BusinessLayer interface."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            (project_root / "substitutions").mkdir()
            
            orchestrator = ActionOrchestrator(project_root)
            
        # Verify it implements BusinessLayer interface (via method existence)
        # Note: Interface inheritance removed to avoid circular imports, but methods preserved
        assert hasattr(orchestrator, 'create_generation_plan')
        assert hasattr(orchestrator, 'execute_generation_strategy')
        assert hasattr(orchestrator, 'validate_configuration')
        
        # Verify methods are callable
        assert callable(getattr(orchestrator, 'create_generation_plan'))
        assert callable(getattr(orchestrator, 'execute_generation_strategy'))
        assert callable(getattr(orchestrator, 'validate_configuration'))
    
    def test_state_manager_implements_data_layer(self):
        """Test that StateManager implements DataLayer interface.""" 
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            
            state_manager = StateManager(project_root)
            
        # Verify it implements DataLayer interface (via method existence)
        # Note: Interface inheritance removed to avoid circular imports, but methods preserved
        assert hasattr(state_manager, 'get_generation_state')
        assert hasattr(state_manager, 'track_generated_file_metadata')
        assert hasattr(state_manager, 'cleanup_orphaned_files')
        
        # Verify methods are callable
        assert callable(getattr(state_manager, 'get_generation_state'))
        assert callable(getattr(state_manager, 'track_generated_file_metadata'))
        assert callable(getattr(state_manager, 'cleanup_orphaned_files'))
    
    def test_application_facade_implements_application_layer(self):
        """Test that ApplicationFacade implements ApplicationLayer interface."""
        facade = LakehousePlumberApplicationFacade(Mock(), Mock())
        
        # Verify it implements ApplicationLayer interface
        assert isinstance(facade, ApplicationLayer)
        
        # Verify it has required interface methods
        assert hasattr(facade, 'generate_pipeline')
        assert hasattr(facade, 'validate_pipeline')
        assert hasattr(facade, 'analyze_staleness')
    
    def test_clean_generate_command_implements_presentation_layer(self):
        """Test that GenerateCommand implements PresentationLayer interface."""
        command = GenerateCommand()
        
        # Verify it implements PresentationLayer interface (via method existence)
        # Note: Interface inheritance removed to avoid circular imports, but methods preserved
        assert hasattr(command, 'display_generation_results')
        assert hasattr(command, 'display_validation_results')
        assert hasattr(command, 'display_analysis_results')
        assert hasattr(command, 'get_user_input')
        
        # Verify methods are callable
        assert callable(getattr(command, 'display_generation_results'))
        assert callable(getattr(command, 'display_validation_results'))
        assert callable(getattr(command, 'display_analysis_results'))
        assert callable(getattr(command, 'get_user_input'))


class TestLayerSeparation:
    """Test that layers are properly separated and don't violate boundaries."""
    
    def test_presentation_layer_has_no_business_logic(self):
        """Test that presentation layer doesn't contain business logic."""
        # Read the clean generate command source
        import inspect
        
        command = GenerateCommand()
        
        # Get all methods
        methods = inspect.getmembers(command, predicate=inspect.ismethod)
        
        # Check that methods are presentation-focused
        method_names = [name for name, _ in methods]
        
        # Should have display methods (presentation)
        assert any("display" in name for name in method_names)
        
        # Should not have business logic methods
        business_terms = ["calculate", "analyze", "process", "validate", "generate"]
        for method_name in method_names:
            if method_name.startswith('_'):  # Private methods might coordinate
                continue
            for business_term in business_terms:
                if business_term in method_name.lower() and "display" not in method_name.lower():
                    pytest.fail(f"Method {method_name} appears to contain business logic in presentation layer")
    
    def test_dto_isolation(self):
        """Test that DTOs don't contain business logic."""
        # DTOs should be pure data structures
        request = PipelineGenerationRequest("test", "dev")
        response = GenerationResponse(
            success=True, generated_files={}, files_written=0,
            total_flowgroups=0, output_location=None, performance_info={}
        )
        
        # DTOs should not have business logic methods
        request_methods = [m for m in dir(request) if not m.startswith('_')]
        response_methods = [m for m in dir(response) if not m.startswith('_')]
        
        # Only simple getters/properties should be present
        for method_name in request_methods:
            assert not any(term in method_name for term in ["calculate", "process", "analyze", "validate"])
        
        for method_name in response_methods:
            if method_name not in ["is_successful"]:  # Allowed simple check methods
                assert not any(term in method_name for term in ["calculate", "process", "analyze", "validate"])
