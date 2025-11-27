"""Integration tests for the deps CLI command."""

import pytest
import tempfile
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
from click.testing import CliRunner
import sys
import os

# Import the CLI module and deps command
from lhp.cli.main import cli, deps
from lhp.cli.commands.dependencies_command import DependenciesCommand
from lhp.models.dependencies import DependencyAnalysisResult, DependencyGraphs, PipelineDependency
from lhp.utils.error_formatter import LHPError, ErrorCategory
import networkx as nx


class TestCliDepsIntegration:
    """Test CLI integration for the deps command."""

    def setup_method(self):
        """Set up test fixtures."""
        self.runner = CliRunner()
        self.temp_dir = Path(tempfile.mkdtemp())

    def teardown_method(self):
        """Clean up test fixtures."""
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def create_mock_analysis_result(self):
        """Create a mock dependency analysis result."""
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
                external_sources=['external.source1'],
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
            external_sources=['external.source1']
        )

    def test_deps_command_exists_in_cli(self):
        """Test that the deps command is registered in the CLI."""
        result = self.runner.invoke(cli, ['--help'])
        assert result.exit_code == 0
        assert 'deps' in result.output
        assert 'Analyze and visualize pipeline dependencies' in result.output

    def test_deps_command_help(self):
        """Test deps command help output."""
        result = self.runner.invoke(cli, ['deps', '--help'])
        assert result.exit_code == 0

        # Check for expected options
        assert '--format' in result.output
        assert '--output' in result.output
        assert '--pipeline' in result.output
        assert '--job-name' in result.output
        assert '--verbose' in result.output

        # Check help text
        assert 'Analyze and visualize pipeline dependencies' in result.output

    @patch('lhp.cli.commands.dependencies_command.DependenciesCommand.execute')
    def test_deps_command_basic_execution(self, mock_execute):
        """Test basic deps command execution."""
        result = self.runner.invoke(cli, ['deps'])

        assert result.exit_code == 0
        mock_execute.assert_called_once_with(
            'all',  # default format
            None,   # no output dir specified
            None,   # no pipeline filter
            None,   # no job name
            None,   # no job config
            False,  # bundle_output=False
            False   # verbose=False
        )

    @patch('lhp.cli.commands.dependencies_command.DependenciesCommand.execute')
    def test_deps_command_with_all_options(self, mock_execute):
        """Test deps command with all options specified."""
        result = self.runner.invoke(cli, [
            'deps',
            '--format', 'json',
            '--output', str(self.temp_dir),
            '--pipeline', 'test_pipeline',
            '--job-name', 'custom_job',
            '--verbose'
        ])

        assert result.exit_code == 0
        mock_execute.assert_called_once_with(
            'json',  # format
            str(self.temp_dir),  # output dir
            'test_pipeline',  # pipeline filter
            'custom_job',  # job name
            None,   # no job config
            False,  # bundle_output=False
            True  # verbose=True
        )

    @patch('lhp.cli.commands.dependencies_command.DependenciesCommand.execute')
    def test_deps_command_short_options(self, mock_execute):
        """Test deps command with short option flags."""
        result = self.runner.invoke(cli, [
            'deps',
            '-f', 'dot',
            '-o', str(self.temp_dir),
            '-p', 'specific_pipeline',
            '-j', 'my_job',
            '-v'
        ])

        assert result.exit_code == 0
        mock_execute.assert_called_once_with(
            'dot',  # format
            str(self.temp_dir),  # output dir
            'specific_pipeline',  # pipeline filter
            'my_job',  # job name
            None,   # no job config
            False,  # bundle_output=False
            True  # verbose=True
        )

    def test_deps_command_invalid_format(self):
        """Test deps command with invalid format option."""
        result = self.runner.invoke(cli, ['deps', '--format', 'invalid_format'])

        # Should exit with error due to invalid choice
        assert result.exit_code != 0
        assert 'Invalid value' in result.output or 'invalid choice' in result.output.lower()

    def test_deps_command_format_choices(self):
        """Test that deps command accepts all valid format choices."""
        valid_formats = ['dot', 'json', 'text', 'job', 'all']

        for format_choice in valid_formats:
            with patch('lhp.cli.commands.dependencies_command.DependenciesCommand.execute') as mock_execute:
                result = self.runner.invoke(cli, ['deps', '--format', format_choice])

                assert result.exit_code == 0, f"Format {format_choice} should be valid"
                mock_execute.assert_called_once()
                assert mock_execute.call_args[0][0] == format_choice

    @patch('lhp.cli.commands.dependencies_command.DependenciesCommand.execute',
           side_effect=LHPError(
               category=ErrorCategory.DEPENDENCY,
               code_number="001",
               title="Test error",
               details="Test error details"
           ))
    def test_deps_command_lhp_error_handling(self, mock_execute):
        """Test deps command error handling for LHPError."""
        result = self.runner.invoke(cli, ['deps'])

        # Command should fail with non-zero exit code
        assert result.exit_code != 0
        # Click's test runner doesn't always capture exception output in result.output
        # The important thing is that the command fails appropriately

    @patch('lhp.cli.commands.dependencies_command.DependenciesCommand.execute',
           side_effect=Exception("Generic error"))
    def test_deps_command_generic_error_handling(self, mock_execute):
        """Test deps command error handling for generic exceptions."""
        result = self.runner.invoke(cli, ['deps'])

        # Command should fail with non-zero exit code
        assert result.exit_code != 0

    def test_deps_command_case_insensitive_format(self):
        """Test that format option is case insensitive."""
        with patch('lhp.cli.commands.dependencies_command.DependenciesCommand.execute') as mock_execute:
            result = self.runner.invoke(cli, ['deps', '--format', 'JSON'])

            # Click Choice should handle case insensitivity
            assert result.exit_code == 0
            mock_execute.assert_called_once()

    @patch('lhp.cli.commands.dependencies_command.DependenciesCommand')
    def test_deps_command_object_creation(self, mock_command_class):
        """Test that DependenciesCommand object is created correctly."""
        mock_command = Mock()
        mock_command_class.return_value = mock_command

        result = self.runner.invoke(cli, ['deps'])

        # Should create DependenciesCommand instance
        mock_command_class.assert_called_once()
        mock_command.execute.assert_called_once()

    def test_deps_command_output_path_validation(self):
        """Test output path option validation."""
        # Test with valid path
        with patch('lhp.cli.commands.dependencies_command.DependenciesCommand.execute') as mock_execute:
            result = self.runner.invoke(cli, ['deps', '--output', str(self.temp_dir)])
            assert result.exit_code == 0
            mock_execute.assert_called_once()

        # Test with relative path
        with patch('lhp.cli.commands.dependencies_command.DependenciesCommand.execute') as mock_execute:
            result = self.runner.invoke(cli, ['deps', '--output', './relative/path'])
            assert result.exit_code == 0
            mock_execute.assert_called_once()

    def test_deps_command_pipeline_option_string(self):
        """Test pipeline option accepts any string value."""
        with patch('lhp.cli.commands.dependencies_command.DependenciesCommand.execute') as mock_execute:
            pipeline_names = ['simple', 'pipeline-with-dashes', 'pipeline_with_underscores', 'pipeline123']

            for pipeline_name in pipeline_names:
                result = self.runner.invoke(cli, ['deps', '--pipeline', pipeline_name])
                assert result.exit_code == 0
                mock_execute.assert_called_with('all', None, pipeline_name, None, None, False, False)

    def test_deps_command_job_name_option(self):
        """Test job name option functionality."""
        with patch('lhp.cli.commands.dependencies_command.DependenciesCommand.execute') as mock_execute:
            job_names = ['simple_job', 'job-with-dashes', 'job_123', 'My Custom Job']

            for job_name in job_names:
                result = self.runner.invoke(cli, ['deps', '--job-name', job_name])
                assert result.exit_code == 0
                # job_name should be passed as 4th argument
                assert mock_execute.call_args[0][3] == job_name

    def test_deps_command_verbose_flag(self):
        """Test verbose flag functionality."""
        with patch('lhp.cli.commands.dependencies_command.DependenciesCommand.execute') as mock_execute:
            # Test without verbose
            result = self.runner.invoke(cli, ['deps'])
            assert result.exit_code == 0
            assert mock_execute.call_args[0][6] is False  # verbose=False (now 7th argument, index 6)

            # Test with verbose
            result = self.runner.invoke(cli, ['deps', '--verbose'])
            assert result.exit_code == 0
            assert mock_execute.call_args[0][6] is True  # verbose=True (now 7th argument, index 6)

    def test_deps_command_multiple_format_handling(self):
        """Test that format option handles single values correctly."""
        # The CLI option is single choice, so test individual formats
        formats_to_test = ['dot', 'json', 'text', 'job', 'all']

        for format_choice in formats_to_test:
            with patch('lhp.cli.commands.dependencies_command.DependenciesCommand.execute') as mock_execute:
                result = self.runner.invoke(cli, ['deps', '--format', format_choice])
                assert result.exit_code == 0
                assert mock_execute.call_args[0][0] == format_choice

    @patch('lhp.cli.commands.dependencies_command.DependenciesCommand.execute')
    def test_deps_command_integration_with_real_paths(self, mock_execute):
        """Test deps command with real filesystem paths."""
        # Create a real directory structure
        test_output_dir = self.temp_dir / "output"
        test_output_dir.mkdir()

        result = self.runner.invoke(cli, [
            'deps',
            '--output', str(test_output_dir),
            '--format', 'json'
        ])

        assert result.exit_code == 0
        mock_execute.assert_called_once_with(
            'json',
            str(test_output_dir),
            None,
            None,
            None,
            False,
            False
        )

    def test_deps_command_default_values(self):
        """Test that deps command uses correct default values."""
        with patch('lhp.cli.commands.dependencies_command.DependenciesCommand.execute') as mock_execute:
            result = self.runner.invoke(cli, ['deps'])

            assert result.exit_code == 0
            # Check default values (updated for new signature)
            call_args = mock_execute.call_args[0]
            assert call_args[0] == 'all'  # default format
            assert call_args[1] is None   # default output (None)
            assert call_args[2] is None   # default pipeline (None)
            assert call_args[3] is None   # default job_name (None)
            assert call_args[4] is None   # default job_config_path (None)
            assert call_args[5] is False  # default bundle_output (False)
            assert call_args[6] is False  # default verbose (False)

    @patch('lhp.cli.commands.dependencies_command.DependenciesCommand.execute')
    def test_deps_command_option_order_independence(self, mock_execute):
        """Test that option order doesn't matter."""
        # Test different orders of the same options
        option_sets = [
            ['--format', 'json', '--verbose', '--pipeline', 'test'],
            ['--verbose', '--pipeline', 'test', '--format', 'json'],
            ['--pipeline', 'test', '--format', 'json', '--verbose']
        ]

        for options in option_sets:
            result = self.runner.invoke(cli, ['deps'] + options)
            assert result.exit_code == 0

            # All should result in the same call (updated for new signature)
            call_args = mock_execute.call_args[0]
            assert call_args[0] == 'json'  # format
            assert call_args[2] == 'test'  # pipeline
            assert call_args[6] is True    # verbose (now 7th argument, index 6)

    def test_deps_command_in_main_cli_registration(self):
        """Test that deps command is properly registered in main CLI."""
        # Check that the command exists in the CLI group
        assert hasattr(cli, 'commands') or hasattr(cli, 'list_commands')

        # Run CLI help and verify deps is listed
        result = self.runner.invoke(cli, ['--help'])
        commands_output = result.output
        assert 'deps' in commands_output

    def test_deps_command_import_path(self):
        """Test that the deps command can be imported correctly."""
        # This test ensures the import path is correct
        from lhp.cli.main import deps as imported_deps
        assert imported_deps is not None
        assert callable(imported_deps)

    @patch('lhp.cli.commands.dependencies_command.DependenciesCommand.execute')
    def test_deps_command_stdin_handling(self, mock_execute):
        """Test that deps command handles stdin gracefully."""
        # Test with empty stdin
        result = self.runner.invoke(cli, ['deps'], input='')
        assert result.exit_code == 0
        mock_execute.assert_called_once()

    def test_deps_command_environment_isolation(self):
        """Test that deps command works in isolated environment."""
        with patch('lhp.cli.commands.dependencies_command.DependenciesCommand.execute') as mock_execute:
            # Test in isolated environment
            with self.runner.isolated_filesystem():
                result = self.runner.invoke(cli, ['deps'])
                assert result.exit_code == 0
                mock_execute.assert_called_once()


class TestDepsCommandEdgeCases:
    """Test edge cases for the deps command."""

    def setup_method(self):
        """Set up test fixtures."""
        self.runner = CliRunner()

    def test_deps_command_with_empty_string_options(self):
        """Test deps command with empty string options."""
        with patch('lhp.cli.commands.dependencies_command.DependenciesCommand.execute') as mock_execute:
            # Empty strings should be passed as empty strings, not None
            result = self.runner.invoke(cli, [
                'deps',
                '--output', '',
                '--pipeline', '',
                '--job-name', ''
            ])

            assert result.exit_code == 0
            call_args = mock_execute.call_args[0]
            assert call_args[1] == ''  # output
            assert call_args[2] == ''  # pipeline
            assert call_args[3] == ''  # job_name

    def test_deps_command_with_special_characters(self):
        """Test deps command with special characters in options."""
        with patch('lhp.cli.commands.dependencies_command.DependenciesCommand.execute') as mock_execute:
            special_names = [
                'pipeline-with-dashes',
                'pipeline_with_underscores',
                'pipeline.with.dots',
                'pipeline with spaces',
                'pipeline123'
            ]

            for name in special_names:
                result = self.runner.invoke(cli, ['deps', '--pipeline', name])
                assert result.exit_code == 0
                assert mock_execute.call_args[0][2] == name

    def test_deps_command_unicode_handling(self):
        """Test deps command with Unicode characters."""
        with patch('lhp.cli.commands.dependencies_command.DependenciesCommand.execute') as mock_execute:
            unicode_names = ['测试管道', 'пайплайн', '🚰pipeline']

            for name in unicode_names:
                result = self.runner.invoke(cli, ['deps', '--pipeline', name])
                assert result.exit_code == 0
                assert mock_execute.call_args[0][2] == name

    def test_deps_command_very_long_options(self):
        """Test deps command with very long option values."""
        with patch('lhp.cli.commands.dependencies_command.DependenciesCommand.execute') as mock_execute:
            long_string = 'a' * 1000  # 1000 character string

            result = self.runner.invoke(cli, ['deps', '--pipeline', long_string])
            assert result.exit_code == 0
            assert mock_execute.call_args[0][2] == long_string