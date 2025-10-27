"""Tests for dependency output manager service."""

import pytest
import json
import tempfile
from pathlib import Path
from unittest.mock import Mock, MagicMock, patch, mock_open
import networkx as nx

from lhp.core.services.dependency_output_manager import DependencyOutputManager
from lhp.models.dependencies import DependencyGraphs, DependencyAnalysisResult, PipelineDependency


class TestDependencyOutputManager:
    """Test DependencyOutputManager functionality."""

    def setup_method(self):
        """Set up test fixtures."""
        self.temp_dir = Path(tempfile.mkdtemp())
        self.output_manager = DependencyOutputManager()

    def teardown_method(self):
        """Clean up test fixtures."""
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def create_mock_graphs(self):
        """Create mock dependency graphs for testing."""
        action_graph = nx.DiGraph()
        action_graph.add_node('fg1.action1', type='load', flowgroup='fg1', pipeline='pipeline1')
        action_graph.add_node('fg2.action2', type='transform', flowgroup='fg2', pipeline='pipeline2')
        action_graph.add_edge('fg1.action1', 'fg2.action2')

        flowgroup_graph = nx.DiGraph()
        flowgroup_graph.add_node('fg1', pipeline='pipeline1', action_count=1)
        flowgroup_graph.add_node('fg2', pipeline='pipeline2', action_count=1)
        flowgroup_graph.add_edge('fg1', 'fg2')

        pipeline_graph = nx.DiGraph()
        pipeline_graph.add_node('pipeline1', flowgroup_count=1, action_count=1)
        pipeline_graph.add_node('pipeline2', flowgroup_count=1, action_count=1)
        pipeline_graph.add_edge('pipeline1', 'pipeline2')

        return DependencyGraphs(
            action_graph=action_graph,
            flowgroup_graph=flowgroup_graph,
            pipeline_graph=pipeline_graph,
            metadata={'total_pipelines': 2, 'total_actions': 2}
        )

    def create_mock_analysis_result(self, graphs=None):
        """Create mock dependency analysis result for testing."""
        if graphs is None:
            graphs = self.create_mock_graphs()

        pipeline_dependencies = {
            'pipeline1': PipelineDependency(
                pipeline='pipeline1',
                depends_on=[],
                flowgroup_count=1,
                action_count=1,
                external_sources=['external.source1'],
                can_run_parallel=False,
                stage=0
            ),
            'pipeline2': PipelineDependency(
                pipeline='pipeline2',
                depends_on=['pipeline1'],
                flowgroup_count=1,
                action_count=1,
                external_sources=[],
                can_run_parallel=False,
                stage=1
            )
        }

        return DependencyAnalysisResult(
            graphs=graphs,
            pipeline_dependencies=pipeline_dependencies,
            execution_stages=[['pipeline1'], ['pipeline2']],
            circular_dependencies=[],
            external_sources=['external.source1']
        )

    def test_save_outputs_all_formats(self):
        """Test saving outputs in all formats."""
        mock_analyzer = Mock()
        mock_analyzer.export_to_dot.return_value = "digraph test { a -> b; }"
        mock_analyzer.export_to_json.return_value = {"test": "data"}
        mock_analyzer.project_root = self.temp_dir  # Add project_root for JobGenerator

        result = self.create_mock_analysis_result()
        output_formats = ["all"]

        generated_files = self.output_manager.save_outputs(
            mock_analyzer, result, output_formats, self.temp_dir
        )

        # Should generate dot, json, text, and job files (HTML format was removed)
        assert "dot" in generated_files
        assert "json" in generated_files
        assert "text" in generated_files
        assert "job" in generated_files

        # Verify files were created
        for file_path in generated_files.values():
            assert file_path.exists()

    def test_save_outputs_specific_formats(self):
        """Test saving outputs with specific formats."""
        mock_analyzer = Mock()
        mock_analyzer.export_to_dot.return_value = "digraph test { a -> b; }"
        mock_analyzer.export_to_json.return_value = {"test": "data"}

        result = self.create_mock_analysis_result()
        output_formats = ["dot", "json"]

        generated_files = self.output_manager.save_outputs(
            mock_analyzer, result, output_formats, self.temp_dir
        )

        # Should only generate dot and json files
        assert "dot" in generated_files
        assert "json" in generated_files
        assert "text" not in generated_files
        assert "job" not in generated_files

        # Verify files were created
        assert generated_files["dot"].exists()
        assert generated_files["json"].exists()

    def test_save_outputs_invalid_format(self):
        """Test error handling for invalid output formats."""
        mock_analyzer = Mock()
        result = self.create_mock_analysis_result()
        output_formats = ["invalid_format"]

        with pytest.raises(ValueError) as exc_info:
            self.output_manager.save_outputs(
                mock_analyzer, result, output_formats, self.temp_dir
            )

        assert "Invalid output formats: {'invalid_format'}" in str(exc_info.value)

    def test_save_dot_format(self):
        """Test DOT format saving."""
        mock_analyzer = Mock()
        mock_analyzer.export_to_dot.return_value = "digraph pipeline_dependencies { pipeline1 -> pipeline2; }"

        graphs = self.create_mock_graphs()
        output_path = self.temp_dir / "dependencies.dot"

        result_path = self.output_manager.save_dot_format(
            mock_analyzer, graphs, output_path
        )

        assert result_path.exists()
        assert result_path == output_path
        content = result_path.read_text()
        assert "digraph pipeline_dependencies" in content
        assert "pipeline1 -> pipeline2" in content

        # Verify analyzer was called with correct parameters
        mock_analyzer.export_to_dot.assert_called_once_with(graphs, "pipeline")

    def test_save_json_format(self):
        """Test JSON format saving."""
        mock_analyzer = Mock()
        test_data = {
            "metadata": {"total_pipelines": 2},
            "pipelines": {"pipeline1": {"depends_on": []}}
        }
        mock_analyzer.export_to_json.return_value = test_data

        result = self.create_mock_analysis_result()
        output_path = self.temp_dir / "dependencies.json"

        result_path = self.output_manager.save_json_format(
            mock_analyzer, result, output_path
        )

        assert result_path.exists()
        assert result_path == output_path

        # Verify JSON content
        with open(result_path, 'r') as f:
            saved_data = json.load(f)

        assert saved_data == test_data

        # Verify analyzer was called
        mock_analyzer.export_to_json.assert_called_once_with(result)

    def test_save_text_format(self):
        """Test text format saving."""
        result = self.create_mock_analysis_result()
        output_path = self.temp_dir / "dependencies.txt"

        result_path = self.output_manager.save_text_format(result, output_path)

        assert result_path.exists()
        assert result_path == output_path

        content = result_path.read_text()
        assert "LAKEHOUSE PLUMBER - PIPELINE DEPENDENCY ANALYSIS" in content
        assert "pipeline1" in content
        assert "pipeline2" in content
        assert "EXECUTION ORDER" in content
        assert "EXTERNAL SOURCES" in content

    def test_save_text_format_with_circular_dependencies(self):
        """Test text format with circular dependencies."""
        result = self.create_mock_analysis_result()
        result.circular_dependencies = [["pipeline cycle: pipeline1 -> pipeline2 -> pipeline1"]]

        output_path = self.temp_dir / "dependencies.txt"
        result_path = self.output_manager.save_text_format(result, output_path)

        content = result_path.read_text()
        assert "CIRCULAR DEPENDENCIES" in content
        assert "pipeline cycle: pipeline1 -> pipeline2 -> pipeline1" in content

    def test_save_job_format_default_name(self):
        """Test job format saving with default name."""
        mock_analyzer = Mock()
        mock_job_generator = Mock()
        mock_job_generator.save_job_to_file.return_value = self.temp_dir / "test_orchestration.job.yml"

        result = self.create_mock_analysis_result()

        with patch('lhp.core.services.dependency_output_manager.JobGenerator', return_value=mock_job_generator):
            result_path = self.output_manager._save_job_format(
                mock_analyzer, result, self.temp_dir
            )

        # Verify job generator was called
        mock_job_generator.save_job_to_file.assert_called_once()

    def test_save_job_format_custom_name(self):
        """Test job format saving with custom job name."""
        mock_analyzer = Mock()
        mock_job_generator = Mock()
        mock_job_generator.save_job_to_file.return_value = self.temp_dir / "custom_job.job.yml"

        result = self.create_mock_analysis_result()
        custom_name = "custom_orchestration_job"

        with patch('lhp.core.services.dependency_output_manager.JobGenerator', return_value=mock_job_generator):
            result_path = self.output_manager._save_job_format(
                mock_analyzer, result, self.temp_dir, custom_name
            )

        # Verify job generator was called with custom name
        args = mock_job_generator.save_job_to_file.call_args[0]
        assert args[2] == custom_name  # job_name parameter

    def test_resolve_output_directory_default(self):
        """Test output directory resolution with default path."""
        expected_path = Path.cwd() / ".lhp" / "dependencies"

        result_path = self.output_manager._resolve_output_directory(None)
        assert result_path == expected_path

    def test_resolve_output_directory_custom(self):
        """Test output directory resolution with custom path."""
        custom_path = Path("/custom/output/dir")

        result_path = self.output_manager._resolve_output_directory(custom_path)
        assert result_path == custom_path

    def test_ensure_directory_exists_new(self):
        """Test directory creation when it doesn't exist."""
        new_dir = self.temp_dir / "new_directory"
        assert not new_dir.exists()

        self.output_manager._ensure_directory_exists(new_dir)
        assert new_dir.exists()
        assert new_dir.is_dir()

    def test_ensure_directory_exists_existing(self):
        """Test directory handling when it already exists."""
        existing_dir = self.temp_dir / "existing"
        existing_dir.mkdir()
        assert existing_dir.exists()

        # Should not raise an error
        self.output_manager._ensure_directory_exists(existing_dir)
        assert existing_dir.exists()

    def test_text_format_integration(self):
        """Test that text format includes expected execution stage and pipeline information."""
        result = self.create_mock_analysis_result()
        output_path = self.temp_dir / "integration_test.txt"

        result_path = self.output_manager.save_text_format(result, output_path)
        content = result_path.read_text()

        # Test that execution stages are formatted correctly in the text
        assert "Stage 1: pipeline1" in content
        assert "Stage 2: pipeline2" in content

        # Test that pipeline details are formatted correctly
        assert "Pipeline: pipeline1" in content
        assert "Pipeline: pipeline2" in content
        assert "Flowgroups: 1" in content
        assert "Actions: 1" in content
        assert "Depends on: None" in content
        assert "Depends on: pipeline1" in content

    def test_io_error_handling(self):
        """Test I/O error handling during file operations."""
        mock_analyzer = Mock()
        mock_analyzer.export_to_dot.side_effect = IOError("Disk full")

        result = self.create_mock_analysis_result()
        output_formats = ["dot"]

        with pytest.raises(IOError) as exc_info:
            self.output_manager.save_outputs(
                mock_analyzer, result, output_formats, self.temp_dir
            )

        assert "Failed to save dependency outputs" in str(exc_info.value)

    def test_file_generation_summary(self):
        """Test file generation summary in save_outputs."""
        mock_analyzer = Mock()
        mock_analyzer.export_to_dot.return_value = "digraph { }"
        mock_analyzer.export_to_json.return_value = {"test": "data"}

        result = self.create_mock_analysis_result()
        output_formats = ["dot", "json"]

        generated_files = self.output_manager.save_outputs(
            mock_analyzer, result, output_formats, self.temp_dir
        )

        # Check that file sizes are reported correctly
        for file_path in generated_files.values():
            assert file_path.exists()
            assert file_path.stat().st_size > 0

    def test_empty_execution_stages_handling(self):
        """Test handling of empty execution stages in text format."""
        result = self.create_mock_analysis_result()
        result.execution_stages = []

        output_path = self.temp_dir / "dependencies.txt"
        result_path = self.output_manager.save_text_format(result, output_path)

        content = result_path.read_text()
        assert "No pipelines found or circular dependencies prevent execution order" in content

    def test_large_external_sources_handling(self):
        """Test handling of large external source lists in text format."""
        result = self.create_mock_analysis_result()
        # Add many external sources
        many_sources = [f"external.table_{i}" for i in range(20)]
        result.external_sources = many_sources
        # Also update the pipeline dependency to include some of these external sources
        result.pipeline_dependencies['pipeline1'].external_sources = many_sources[:7]

        output_path = self.temp_dir / "dependencies.txt"
        result_path = self.output_manager.save_text_format(result, output_path)

        content = result_path.read_text()
        # Should show external sources in both pipeline details and external sources section
        assert "external.table_0" in content
        # For pipeline details, should truncate after 5 and show "... and X more"
        assert "... and" in content

    def test_base_output_dir_initialization(self):
        """Test base output directory initialization."""
        custom_base_dir = Path("/custom/base")
        manager = DependencyOutputManager(custom_base_dir)
        assert manager.base_output_dir == custom_base_dir

        # Test with None (default)
        default_manager = DependencyOutputManager()
        assert default_manager.base_output_dir is None

    def test_concurrent_file_operations(self):
        """Test that file operations are atomic and don't interfere."""
        mock_analyzer = Mock()
        mock_analyzer.export_to_dot.return_value = "digraph test { }"
        mock_analyzer.export_to_json.return_value = {"test": "concurrent"}

        result = self.create_mock_analysis_result()

        # Simulate concurrent saves (though this test is still sequential)
        files1 = self.output_manager.save_outputs(
            mock_analyzer, result, ["dot"], self.temp_dir / "output1"
        )
        files2 = self.output_manager.save_outputs(
            mock_analyzer, result, ["json"], self.temp_dir / "output2"
        )

        # Both should succeed independently
        assert files1["dot"].exists()
        assert files2["json"].exists()

    @patch('builtins.open', mock_open())
    def test_unicode_handling_in_text_output(self):
        """Test Unicode character handling in text output."""
        result = self.create_mock_analysis_result()

        # Add Unicode characters to pipeline names (simulate international usage)
        result.pipeline_dependencies['pipeline_测试'] = PipelineDependency(
            pipeline='pipeline_测试',
            depends_on=[],
            flowgroup_count=1,
            action_count=1,
            external_sources=[],
            can_run_parallel=False,
            stage=0
        )

        output_path = self.temp_dir / "unicode_test.txt"

        # Should not raise UnicodeEncodeError
        result_path = self.output_manager.save_text_format(result, output_path)
        assert result_path == output_path


# ============================================================================
# Tests for Custom Job Config and Bundle Output
# ============================================================================


def test_save_job_to_default_location(tmp_path):
    """Job saves to .lhp/dependencies/ by default."""
    output_manager = DependencyOutputManager()
    
    # Create mock analyzer and result
    analyzer = Mock()
    analyzer.project_root = tmp_path / "project"
    analyzer.export_to_dot = Mock(return_value="digraph {}")
    analyzer.export_to_json = Mock(return_value={})
    
    result = create_test_dependency_result()
    
    # Save with default location
    output_dir = tmp_path / ".lhp" / "dependencies"
    generated_files = output_manager.save_outputs(
        analyzer, result, ["job"], output_dir
    )
    
    # Check that job file was created in default location
    assert "job" in generated_files
    assert str(generated_files["job"]).endswith(".job.yml")
    assert generated_files["job"].parent == output_dir


def test_save_job_to_resources_with_bundle_flag(tmp_path):
    """Job saves to resources/ when bundle_output=True."""
    output_manager = DependencyOutputManager()
    
    # Create mock analyzer and result
    analyzer = Mock()
    analyzer.project_root = tmp_path / "project"
    analyzer.project_root.mkdir(parents=True)
    analyzer.export_to_dot = Mock(return_value="digraph {}")
    analyzer.export_to_json = Mock(return_value={})
    
    result = create_test_dependency_result()
    
    # Save with bundle_output flag
    output_dir = tmp_path / ".lhp" / "dependencies"
    generated_files = output_manager.save_outputs(
        analyzer, result, ["job"], output_dir, 
        bundle_output=True, job_name="test_job"
    )
    
    # Check that job file was created in resources/ directory
    assert "job" in generated_files
    expected_path = analyzer.project_root / "resources" / "test_job.job.yml"
    assert generated_files["job"] == expected_path


def test_save_job_passes_config_path_to_generator(tmp_path):
    """Config file path is passed to JobGenerator."""
    output_manager = DependencyOutputManager()
    
    # Create project with custom config
    project_root = tmp_path / "project"
    project_root.mkdir()
    custom_config = project_root / "custom_config.yaml"
    custom_config.write_text("max_concurrent_runs: 10\n")
    
    # Create mock analyzer
    analyzer = Mock()
    analyzer.project_root = project_root
    analyzer.export_to_dot = Mock(return_value="digraph {}")
    analyzer.export_to_json = Mock(return_value={})
    
    result = create_test_dependency_result()
    
    # Save with custom config path
    output_dir = tmp_path / ".lhp" / "dependencies"
    output_manager.save_outputs(
        analyzer, result, ["job"], output_dir,
        job_config_path="custom_config.yaml"
    )
    
    # Verify the job file was created
    job_files = list(output_dir.glob("*.job.yml"))
    assert len(job_files) == 1
    
    # Verify custom config was used (check for max_concurrent_runs: 10)
    with open(job_files[0]) as f:
        content = f.read()
        assert "max_concurrent_runs: 10" in content


def test_save_job_creates_resources_directory_if_not_exists(tmp_path):
    """Resources directory is created if it doesn't exist."""
    output_manager = DependencyOutputManager()
    
    # Create project without resources directory
    project_root = tmp_path / "project"
    project_root.mkdir()
    
    # Create mock analyzer
    analyzer = Mock()
    analyzer.project_root = project_root
    analyzer.export_to_dot = Mock(return_value="digraph {}")
    analyzer.export_to_json = Mock(return_value={})
    
    result = create_test_dependency_result()
    
    # Resources directory shouldn't exist yet
    resources_dir = project_root / "resources"
    assert not resources_dir.exists()
    
    # Save with bundle_output flag
    output_dir = tmp_path / ".lhp" / "dependencies"
    generated_files = output_manager.save_outputs(
        analyzer, result, ["job"], output_dir,
        bundle_output=True, job_name="test_job"
    )
    
    # Resources directory should now exist
    assert resources_dir.exists()
    assert generated_files["job"].exists()


def create_test_dependency_result():
    """Helper to create a minimal DependencyAnalysisResult for testing."""
    # Create minimal graphs
    action_graph = nx.DiGraph()
    flowgroup_graph = nx.DiGraph()
    pipeline_graph = nx.DiGraph()
    pipeline_graph.add_node("test_pipeline")
    
    graphs = DependencyGraphs(
        action_graph=action_graph,
        flowgroup_graph=flowgroup_graph,
        pipeline_graph=pipeline_graph,
        metadata={}
    )
    
    # Create pipeline dependency
    pipeline_dep = PipelineDependency(
        pipeline="test_pipeline",
        depends_on=[],
        flowgroup_count=1,
        action_count=1,
        external_sources=[],
        stage=1
    )
    
    return DependencyAnalysisResult(
        graphs=graphs,
        pipeline_dependencies={"test_pipeline": pipeline_dep},
        execution_stages=[["test_pipeline"]],
        circular_dependencies=[],
        external_sources=[]
    )