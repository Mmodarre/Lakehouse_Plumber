"""Tests for dependencies command implementation."""

import pytest
import tempfile
import logging
from pathlib import Path
from unittest.mock import Mock, MagicMock, patch, call
import click

from lhp.cli.commands.dependencies_command import DependenciesCommand, create_dependencies_command
from lhp.models.dependencies import DependencyAnalysisResult, DependencyGraphs, PipelineDependency
from lhp.utils.error_formatter import LHPError, ErrorCategory
import networkx as nx


class TestDependenciesCommand:
    """Test DependenciesCommand functionality."""

    def setup_method(self):
        """Set up test fixtures."""
        self.temp_dir = Path(tempfile.mkdtemp())
        self.command = DependenciesCommand()

    def teardown_method(self):
        """Clean up test fixtures."""
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def create_mock_analysis_result(self):
        """Create mock dependency analysis result."""
        graphs = DependencyGraphs(
            action_graph=nx.DiGraph(),
            flowgroup_graph=nx.DiGraph(),
            pipeline_graph=nx.DiGraph(),
            metadata={'total_pipelines': 2}
        )

        pipeline_deps = {
            'pipeline1': PipelineDependency(
                pipeline='pipeline1',
                depends_on=[],
                flowgroup_count=1,
                action_count=2,
                external_sources=['external.table1'],
                stage=0
            ),
            'pipeline2': PipelineDependency(
                pipeline='pipeline2',
                depends_on=['pipeline1'],
                flowgroup_count=1,
                action_count=3,
                external_sources=[],
                stage=1
            )
        }

        return DependencyAnalysisResult(
            graphs=graphs,
            pipeline_dependencies=pipeline_deps,
            execution_stages=[['pipeline1'], ['pipeline2']],
            circular_dependencies=[],
            external_sources=['external.table1']
        )

    @patch('lhp.cli.commands.dependencies_command.DependencyOutputManager')
    @patch('lhp.cli.commands.dependencies_command.DependencyAnalyzer')
    @patch('lhp.cli.commands.dependencies_command.ProjectConfigLoader')
    @patch.object(DependenciesCommand, 'setup_from_context')
    @patch.object(DependenciesCommand, 'ensure_project_root')
    @patch('click.echo')
    def test_execute_basic_functionality(self, mock_echo, mock_ensure_root, mock_setup,
                                        mock_config_loader, mock_analyzer_class, mock_output_manager_class):
        """Test basic command execution."""
        # Setup mocks
        mock_ensure_root.return_value = self.temp_dir
        mock_analyzer = Mock()
        mock_analyzer_class.return_value = mock_analyzer

        mock_output_manager = Mock()
        mock_output_manager_class.return_value = mock_output_manager

        # Create mock analysis result
        result = self.create_mock_analysis_result()
        mock_analyzer.analyze_dependencies.return_value = result

        # Mock file generation
        generated_files = {
            'dot': self.temp_dir / 'deps.dot',
            'json': self.temp_dir / 'deps.json'
        }
        for file_path in generated_files.values():
            file_path.touch()  # Create empty files
        mock_output_manager.save_outputs.return_value = generated_files

        # Execute command
        self.command.execute(output_format="dot,json", output_dir=str(self.temp_dir))

        # Verify setup
        mock_setup.assert_called_once()
        mock_ensure_root.assert_called_once()

        # Verify analyzer was created and called
        mock_analyzer_class.assert_called_once()
        mock_analyzer.analyze_dependencies.assert_called_once_with(pipeline_filter=None)

        # Verify output manager was called
        mock_output_manager.save_outputs.assert_called_once()

        # Verify click.echo was called with expected messages
        echo_calls = [call[0][0] for call in mock_echo.call_args_list]
        assert any("Analyzing Pipeline Dependencies" in call for call in echo_calls)
        assert any("Building dependency graphs" in call for call in echo_calls)
        assert any("Generating output files" in call for call in echo_calls)
        assert any("Dependency analysis complete" in call for call in echo_calls)

    @patch('lhp.cli.commands.dependencies_command.DependencyAnalyzer')
    @patch.object(DependenciesCommand, 'setup_from_context')
    @patch.object(DependenciesCommand, 'ensure_project_root')
    def test_execute_with_pipeline_filter(self, mock_ensure_root, mock_setup, mock_analyzer_class):
        """Test command execution with pipeline filter."""
        mock_ensure_root.return_value = self.temp_dir
        mock_analyzer = Mock()
        mock_analyzer_class.return_value = mock_analyzer

        # Create mock result with filtered pipeline
        result = self.create_mock_analysis_result()
        result.pipeline_dependencies = {'target_pipeline': result.pipeline_dependencies['pipeline1']}
        mock_analyzer.analyze_dependencies.return_value = result

        # Mock pipeline validation - explicitly set job_name=None for flowgroups
        mock_fg1 = Mock(pipeline='target_pipeline', job_name=None)
        mock_fg2 = Mock(pipeline='other_pipeline', job_name=None)
        mock_analyzer._get_flowgroups.return_value = [mock_fg1, mock_fg2]

        with patch('lhp.cli.commands.dependencies_command.DependencyOutputManager'), \
             patch('click.echo'):
            self.command.execute(pipeline="target_pipeline")

        # Verify analyzer was called with pipeline filter
        mock_analyzer.analyze_dependencies.assert_called_once_with(pipeline_filter="target_pipeline")

    @patch('lhp.cli.commands.dependencies_command.DependencyAnalyzer')
    @patch.object(DependenciesCommand, 'setup_from_context')
    @patch.object(DependenciesCommand, 'ensure_project_root')
    def test_pipeline_validation_failure(self, mock_ensure_root, mock_setup, mock_analyzer_class):
        """Test pipeline validation when specified pipeline doesn't exist."""
        mock_ensure_root.return_value = self.temp_dir
        mock_analyzer = Mock()
        mock_analyzer_class.return_value = mock_analyzer

        # Mock available pipelines (excluding the requested one)
        mock_flowgroups = [Mock(pipeline='existing_pipeline1'), Mock(pipeline='existing_pipeline2')]
        mock_analyzer._get_flowgroups.return_value = mock_flowgroups

        # Should raise LHPError for non-existent pipeline
        with pytest.raises(LHPError) as exc_info:
            with patch('lhp.cli.commands.dependencies_command.DependencyOutputManager'), \
                 patch('click.echo'):
                self.command.execute(pipeline="nonexistent_pipeline")

        assert "LHP-CFG-" in exc_info.value.code
        assert "Pipeline 'nonexistent_pipeline' not found" in exc_info.value.title

    @patch.object(DependenciesCommand, 'setup_from_context')
    @patch.object(DependenciesCommand, 'ensure_project_root')
    def test_verbose_logging_setup(self, mock_ensure_root, mock_setup):
        """Test verbose logging configuration."""
        mock_ensure_root.return_value = self.temp_dir

        with patch('lhp.cli.commands.dependencies_command.DependencyAnalyzer') as mock_analyzer_class, \
             patch('lhp.cli.commands.dependencies_command.DependencyOutputManager'), \
             patch('click.echo'), \
             patch('logging.getLogger') as mock_get_logger:

            # Mock analyzer with proper result structure
            mock_analyzer = Mock()
            mock_analyzer_class.return_value = mock_analyzer
            mock_result = Mock()
            mock_result.total_external_sources = 0
            mock_result.execution_stages = []
            mock_result.circular_dependencies = []
            mock_analyzer.analyze_dependencies.return_value = mock_result

            # Mock loggers
            dep_logger = Mock()
            out_logger = Mock()
            mock_get_logger.side_effect = lambda name: {
                "lhp.core.services.dependency_analyzer": dep_logger,
                "lhp.core.services.dependency_output_manager": out_logger
            }.get(name, Mock())

            self.command.execute(verbose=True)

            # Verify verbose logging was set up
            dep_logger.setLevel.assert_called_with(logging.DEBUG)
            out_logger.setLevel.assert_called_with(logging.DEBUG)

    def test_parse_output_formats_valid(self):
        """Test output format parsing with valid formats."""
        valid_formats = "dot,json,text"
        result = self.command._parse_output_formats(valid_formats)
        assert sorted(result) == ["dot", "json", "text"]

    def test_parse_output_formats_single(self):
        """Test output format parsing with single format."""
        result = self.command._parse_output_formats("json")
        assert result == ["json"]

    def test_parse_output_formats_all(self):
        """Test output format parsing with 'all' format."""
        result = self.command._parse_output_formats("all")
        assert result == ["all"]

    def test_parse_output_formats_invalid(self):
        """Test output format parsing with invalid formats."""
        with pytest.raises(click.BadParameter) as exc_info:
            self.command._parse_output_formats("invalid,another_invalid")

        # Check that both invalid formats are mentioned (order may vary due to set operations)
        error_msg = str(exc_info.value)
        assert "invalid" in error_msg and "another_invalid" in error_msg
        assert "Invalid output format(s):" in error_msg

    def test_resolve_output_path_custom(self):
        """Test output path resolution with custom directory."""
        custom_dir = "/custom/output/path"
        project_root = Path("/project")

        result = self.command._resolve_output_path(custom_dir, project_root)
        assert result == Path(custom_dir).resolve()

    def test_resolve_output_path_default(self):
        """Test output path resolution with default directory."""
        project_root = Path("/project")

        result = self.command._resolve_output_path(None, project_root)
        assert result == project_root / ".lhp" / "dependencies"

    @patch('click.echo')
    def test_display_analysis_summary_with_pipeline_filter(self, mock_echo):
        """Test analysis summary display with pipeline filter."""
        result = self.create_mock_analysis_result()

        self.command._display_analysis_summary(result, "specific_pipeline")

        # Check that pipeline-specific summary was displayed
        echo_calls = [call[0][0] for call in mock_echo.call_args_list]
        assert any("Pipeline: specific_pipeline" in call for call in echo_calls)

    @patch('click.echo')
    def test_display_analysis_summary_all_pipelines(self, mock_echo):
        """Test analysis summary display for all pipelines."""
        result = self.create_mock_analysis_result()

        self.command._display_analysis_summary(result, None)

        # Check that total pipeline count was displayed
        echo_calls = [call[0][0] for call in mock_echo.call_args_list]
        assert any("Total pipelines analyzed: 2" in call for call in echo_calls)

    @patch('click.echo')
    def test_display_generated_files(self, mock_echo):
        """Test generated files display."""
        generated_files = {
            'dot': self.temp_dir / 'deps.dot',
            'json': self.temp_dir / 'deps.json'
        }

        # Create files with some content
        for file_path in generated_files.values():
            file_path.write_text("test content")

        self.command._display_generated_files(generated_files)

        echo_calls = [call[0][0] for call in mock_echo.call_args_list]
        assert any("DOT:" in call and "deps.dot" in call for call in echo_calls)
        assert any("JSON:" in call and "deps.json" in call for call in echo_calls)
        assert any("12 bytes" in call for call in echo_calls)  # "test content" length

    @patch('click.echo')
    def test_display_execution_order(self, mock_echo):
        """Test execution order display."""
        result = self.create_mock_analysis_result()
        result.execution_stages = [['pipeline1'], ['pipeline2', 'pipeline3'], ['pipeline4']]

        self.command._display_execution_order(result)

        echo_calls = [call[0][0] for call in mock_echo.call_args_list]
        assert any("Execution Order:" in call for call in echo_calls)
        assert any("Stage 1: pipeline1" in call for call in echo_calls)
        assert any("Stage 2: pipeline2, pipeline3 (can run in parallel)" in call for call in echo_calls)
        assert any("Stage 3: pipeline4" in call for call in echo_calls)

    @patch('click.echo')
    def test_display_execution_order_empty(self, mock_echo):
        """Test execution order display when empty."""
        result = self.create_mock_analysis_result()
        result.execution_stages = []

        self.command._display_execution_order(result)

        echo_calls = [call[0][0] for call in mock_echo.call_args_list]
        assert any("No pipelines found or circular dependencies" in call for call in echo_calls)

    @patch('click.echo')
    def test_display_warnings_circular_dependencies(self, mock_echo):
        """Test warnings display with circular dependencies."""
        result = self.create_mock_analysis_result()
        result.circular_dependencies = [["pipeline cycle: pipeline1 -> pipeline2 -> pipeline1"]]

        self.command._display_warnings(result)

        echo_calls = [call[0][0] for call in mock_echo.call_args_list]
        assert any("Warnings:" in call for call in echo_calls)
        assert any("Circular dependencies detected" in call for call in echo_calls)
        assert any("pipeline cycle: pipeline1 -> pipeline2 -> pipeline1" in call for call in echo_calls)

    @patch('click.echo')
    def test_display_warnings_external_sources_few(self, mock_echo):
        """Test warnings display with few external sources."""
        result = self.create_mock_analysis_result()
        result.external_sources = ['external.table1', 'external.table2']

        self.command._display_warnings(result)

        echo_calls = [call[0][0] for call in mock_echo.call_args_list]
        assert any("external sources detected" in call for call in echo_calls)
        assert any("external.table1" in call for call in echo_calls)

    @patch('click.echo')
    def test_display_warnings_external_sources_many(self, mock_echo):
        """Test warnings display with many external sources."""
        result = self.create_mock_analysis_result()
        result.external_sources = [f'external.table{i}' for i in range(10)]

        self.command._display_warnings(result)

        echo_calls = [call[0][0] for call in mock_echo.call_args_list]
        assert any("10 external sources detected" in call for call in echo_calls)
        assert any("Use generated files to see complete list" in call for call in echo_calls)

    @patch.object(DependenciesCommand, 'setup_from_context', side_effect=Exception("Setup failed"))
    def test_error_handling_generic_exception(self, mock_setup):
        """Test error handling for generic exceptions."""
        with pytest.raises(LHPError) as exc_info:
            self.command.execute()

        assert "LHP-DEP-" in exc_info.value.code
        assert "Dependency analysis failed" in exc_info.value.title
        assert "Setup failed" in str(exc_info.value.details)

    @patch.object(DependenciesCommand, 'setup_from_context', side_effect=LHPError(
        category=ErrorCategory.CONFIG,
        code_number="001",
        title="Configuration error",
        details="Invalid config"
    ))
    def test_error_handling_lhp_error_reraise(self, mock_setup):
        """Test that LHPError is re-raised without wrapping."""
        with pytest.raises(LHPError) as exc_info:
            self.command.execute()

        # Should be the original LHPError, not wrapped
        assert "LHP-CFG-" in exc_info.value.code
        assert "Configuration error" in exc_info.value.title

    def test_job_name_parameter_handling(self):
        """Test job name parameter is passed correctly."""
        with patch('lhp.cli.commands.dependencies_command.DependencyAnalyzer') as mock_analyzer_class, \
             patch('lhp.cli.commands.dependencies_command.DependencyOutputManager') as mock_output_manager_class, \
             patch.object(DependenciesCommand, 'setup_from_context'), \
             patch.object(DependenciesCommand, 'ensure_project_root', return_value=self.temp_dir), \
             patch('click.echo'):

            # Mock analyzer with proper result structure
            mock_analyzer = Mock()
            mock_analyzer_class.return_value = mock_analyzer
            mock_result = Mock()
            mock_result.total_external_sources = 0
            mock_result.execution_stages = []
            mock_result.circular_dependencies = []
            mock_analyzer.analyze_dependencies.return_value = mock_result

            mock_output_manager = Mock()
            mock_output_manager_class.return_value = mock_output_manager
            mock_output_manager.save_outputs.return_value = {}

            custom_job_name = "my_custom_job"
            self.command.execute(job_name=custom_job_name, output_format="job")

            # Verify job name was passed to output manager as positional argument (index 4)
            call_args = mock_output_manager.save_outputs.call_args
            assert call_args[0][4] == custom_job_name  # 5th positional argument (job_name)


class TestCreateDependenciesCommand:
    """Test the create_dependencies_command factory function."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.temp_dir = Path(tempfile.mkdtemp())
        self.command = DependenciesCommand()
    
    def teardown_method(self):
        """Clean up test fixtures."""
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    def create_mock_analysis_result(self):
        """Create mock dependency analysis result."""
        graphs = DependencyGraphs(
            action_graph=nx.DiGraph(),
            flowgroup_graph=nx.DiGraph(),
            pipeline_graph=nx.DiGraph(),
            metadata={'total_pipelines': 2}
        )

        pipeline_deps = {
            'pipeline1': PipelineDependency(
                pipeline='pipeline1',
                depends_on=[],
                flowgroup_count=1,
                action_count=2,
                external_sources=['external.table1'],
                stage=0
            ),
            'pipeline2': PipelineDependency(
                pipeline='pipeline2',
                depends_on=['pipeline1'],
                flowgroup_count=1,
                action_count=3,
                external_sources=[],
                stage=1
            )
        }

        return DependencyAnalysisResult(
            graphs=graphs,
            pipeline_dependencies=pipeline_deps,
            execution_stages=[['pipeline1'], ['pipeline2']],
            circular_dependencies=[],
            external_sources=['external.table1']
        )

    def test_command_creation(self):
        """Test that command is created properly."""
        command_func = create_dependencies_command()

        assert callable(command_func)
        assert hasattr(command_func, 'params')  # Click Command object has params attribute
        assert len(command_func.params) > 0  # Should have parameters
        assert command_func.name == 'deps'  # Click Command has name attribute

    def test_command_options(self):
        """Test that command has all expected options."""
        command_func = create_dependencies_command()
        params = command_func.params

        param_names = [param.name for param in params]
        assert 'output_format' in param_names
        assert 'output_dir' in param_names
        assert 'pipeline' in param_names
        assert 'verbose' in param_names

    @patch.object(DependenciesCommand, 'execute')
    def test_command_execution_integration(self, mock_execute):
        """Test command execution through Click interface."""
        from click.testing import CliRunner

        command_func = create_dependencies_command()
        runner = CliRunner()

        # Test command execution with various options
        result = runner.invoke(command_func, [
            '--format', 'json',
            '--output', '/tmp/test',
            '--pipeline', 'test_pipeline',
            '--verbose'
        ])

        # Should not have errors in Click processing
        assert result.exit_code == 0

        # Verify execute was called with correct parameters (updated for new signature)
        mock_execute.assert_called_once_with(
            'json',  # output_format
            '/tmp/test',  # output_dir
            'test_pipeline',  # pipeline
            None,  # job_name (not provided in test)
            None,  # job_config_path (not provided in test)
            False,  # bundle_output (default)
            True  # verbose
        )

    def test_command_help_text(self):
        """Test that command help text is appropriate."""
        command_func = create_dependencies_command()

        assert "Analyze and visualize pipeline dependencies" in command_func.__doc__

        # Check option help texts
        format_option = next(p for p in command_func.params if p.name == 'output_format')
        assert "Output format" in format_option.help

    def test_format_option_choices(self):
        """Test that format option has correct choices."""
        command_func = create_dependencies_command()
        format_option = next(p for p in command_func.params if p.name == 'output_format')

        # Should be a Choice type with specific options
        assert hasattr(format_option.type, 'choices')
        expected_choices = ("dot", "json", "text", "all")  # Note: tuples not lists, and no "job"
        assert format_option.type.choices == expected_choices

    def test_default_values(self):
        """Test command default values."""
        command_func = create_dependencies_command()

        format_option = next(p for p in command_func.params if p.name == 'output_format')
        assert format_option.default == "all"

        verbose_option = next(p for p in command_func.params if p.name == 'verbose')
        assert verbose_option.is_flag is True
    
    @patch('lhp.cli.commands.dependencies_command.DependencyOutputManager')
    @patch('lhp.cli.commands.dependencies_command.DependencyAnalyzer')
    @patch('lhp.cli.commands.dependencies_command.ProjectConfigLoader')
    @patch.object(DependenciesCommand, 'setup_from_context')
    @patch.object(DependenciesCommand, 'ensure_project_root')
    def test_pipeline_filter_with_job_name_raises_error_003(self, mock_ensure_root, mock_setup,
                                                            mock_config_loader, mock_analyzer_class,
                                                            mock_output_manager_class):
        """Test that --pipeline filter with job_name raises error 003."""
        # Setup mocks
        mock_ensure_root.return_value = self.temp_dir
        mock_analyzer = Mock()
        mock_analyzer_class.return_value = mock_analyzer
        
        # Mock _get_flowgroups to return flowgroups WITH job_name
        from lhp.models.config import FlowGroup, Action, ActionType
        flowgroups = [
            FlowGroup(pipeline="bronze_pipeline", flowgroup="fg1", job_name="bronze_job",
                     actions=[Action(name="load", type=ActionType.LOAD, source="raw.table", target="v_table")]),
            FlowGroup(pipeline="silver_pipeline", flowgroup="fg2", job_name="silver_job",
                     actions=[Action(name="load", type=ActionType.LOAD, source="bronze.table", target="v_table")]),
        ]
        mock_analyzer._get_flowgroups.return_value = flowgroups
        
        # Execute with --pipeline filter
        with pytest.raises(LHPError) as exc_info:
            self.command.execute(pipeline="bronze_pipeline")
        
        # Verify error code and message
        error = exc_info.value
        assert error.code == "LHP-VAL-003"
        assert "Pipeline filter not supported with job_name" in error.title or "pipeline" in error.title.lower()
    
    @patch('lhp.cli.commands.dependencies_command.DependencyOutputManager')
    @patch('lhp.cli.commands.dependencies_command.DependencyAnalyzer')
    @patch('lhp.cli.commands.dependencies_command.ProjectConfigLoader')
    @patch.object(DependenciesCommand, 'setup_from_context')
    @patch.object(DependenciesCommand, 'ensure_project_root')
    @patch('click.echo')
    def test_pipeline_filter_without_job_name_works(self, mock_echo, mock_ensure_root, mock_setup,
                                                    mock_config_loader, mock_analyzer_class,
                                                    mock_output_manager_class):
        """Test that --pipeline filter works normally when no job_name is defined."""
        # Setup mocks
        mock_ensure_root.return_value = self.temp_dir
        mock_analyzer = Mock()
        mock_analyzer_class.return_value = mock_analyzer
        
        mock_output_manager = Mock()
        mock_output_manager_class.return_value = mock_output_manager
        
        # Mock _get_flowgroups to return flowgroups WITHOUT job_name
        from lhp.models.config import FlowGroup, Action, ActionType
        flowgroups = [
            FlowGroup(pipeline="bronze_pipeline", flowgroup="fg1", job_name=None,
                     actions=[Action(name="load", type=ActionType.LOAD, source="raw.table", target="v_table")]),
            FlowGroup(pipeline="silver_pipeline", flowgroup="fg2", job_name=None,
                     actions=[Action(name="load", type=ActionType.LOAD, source="bronze.table", target="v_table")]),
        ]
        mock_analyzer._get_flowgroups.return_value = flowgroups
        
        # Mock analysis result
        result = self.create_mock_analysis_result()
        mock_analyzer.analyze_dependencies.return_value = result
        
        # Mock file generation
        generated_files = {'dot': self.temp_dir / 'deps.dot'}
        (self.temp_dir / 'deps.dot').touch()
        mock_output_manager.save_outputs.return_value = generated_files
        
        # Execute with --pipeline filter
        self.command.execute(output_format="dot", pipeline="bronze_pipeline")
        
        # Should succeed without raising
        mock_analyzer.analyze_dependencies.assert_called_once()
    
    @patch('lhp.cli.commands.dependencies_command.DependencyOutputManager')
    @patch('lhp.cli.commands.dependencies_command.DependencyAnalyzer')
    @patch('lhp.cli.commands.dependencies_command.ProjectConfigLoader')
    @patch.object(DependenciesCommand, 'setup_from_context')
    @patch.object(DependenciesCommand, 'ensure_project_root')
    @patch('click.echo')
    def test_cli_displays_multiple_job_files_correctly(self, mock_echo, mock_ensure_root, mock_setup,
                                                       mock_config_loader, mock_analyzer_class,
                                                       mock_output_manager_class):
        """Test that CLI displays multiple job files in organized way."""
        # Setup mocks
        mock_ensure_root.return_value = self.temp_dir
        mock_analyzer = Mock()
        mock_analyzer_class.return_value = mock_analyzer
        
        mock_output_manager = Mock()
        mock_output_manager_class.return_value = mock_output_manager
        
        # Mock analysis result
        result = self.create_mock_analysis_result()
        mock_analyzer.analyze_dependencies.return_value = result
        
        # Mock file generation with multiple job files
        bronze_job_file = self.temp_dir / 'bronze_job.job.yml'
        silver_job_file = self.temp_dir / 'silver_job.job.yml'
        master_job_file = self.temp_dir / 'test_master.job.yml'
        
        for f in [bronze_job_file, silver_job_file, master_job_file]:
            f.write_text("# test content")
        
        generated_files = {
            'dot': self.temp_dir / 'deps.dot',
            'job': {
                'bronze_job': bronze_job_file,
                'silver_job': silver_job_file,
                '_master': master_job_file
            }
        }
        (self.temp_dir / 'deps.dot').touch()
        mock_output_manager.save_outputs.return_value = generated_files
        
        # Execute command
        self.command.execute(output_format="dot,job")
        
        # Verify display called for each job file
        # Check that echo was called with job file information
        echo_calls = [str(call) for call in mock_echo.call_args_list]
        combined_output = " ".join(echo_calls)
        
        # Should mention job files
        assert any("bronze_job" in call or "job" in call.lower() for call in echo_calls)