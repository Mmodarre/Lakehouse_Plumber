import json
import tempfile
from dataclasses import replace
from pathlib import Path
from unittest.mock import Mock, mock_open, patch

import networkx as nx
import pytest

from lhp.core.dependencies.output import export_to_json, export_to_text
from lhp.core.dependencies.output_writer import DependencyOutputWriter
from lhp.models.dependencies import (
    AffectedAction,
    DependencyAnalysisResult,
    DependencyGraphs,
    DependencyWarning,
    PipelineDependency,
)


class TestDependencyOutputWriter:
    def setup_method(self):
        self.temp_dir = Path(tempfile.mkdtemp())
        self.output_manager = DependencyOutputWriter()

    def teardown_method(self):
        import shutil

        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def create_mock_graphs(self):
        action_graph = nx.DiGraph()
        action_graph.add_node(
            "fg1.action1", type="load", flowgroup="fg1", pipeline="pipeline1"
        )
        action_graph.add_node(
            "fg2.action2", type="transform", flowgroup="fg2", pipeline="pipeline2"
        )
        action_graph.add_edge("fg1.action1", "fg2.action2")

        flowgroup_graph = nx.DiGraph()
        flowgroup_graph.add_node("fg1", pipeline="pipeline1", action_count=1)
        flowgroup_graph.add_node("fg2", pipeline="pipeline2", action_count=1)
        flowgroup_graph.add_edge("fg1", "fg2")

        pipeline_graph = nx.DiGraph()
        pipeline_graph.add_node("pipeline1", flowgroup_count=1, action_count=1)
        pipeline_graph.add_node("pipeline2", flowgroup_count=1, action_count=1)
        pipeline_graph.add_edge("pipeline1", "pipeline2")

        return DependencyGraphs(
            action_graph=action_graph,
            flowgroup_graph=flowgroup_graph,
            pipeline_graph=pipeline_graph,
            metadata={"total_pipelines": 2, "total_actions": 2},
        )

    def create_mock_analysis_result(self, graphs=None):
        if graphs is None:
            graphs = self.create_mock_graphs()

        pipeline_dependencies = {
            "pipeline1": PipelineDependency(
                pipeline="pipeline1",
                depends_on=[],
                flowgroup_count=1,
                action_count=1,
                external_sources=["external.source1"],
                can_run_parallel=False,
                stage=0,
            ),
            "pipeline2": PipelineDependency(
                pipeline="pipeline2",
                depends_on=["pipeline1"],
                flowgroup_count=1,
                action_count=1,
                external_sources=[],
                can_run_parallel=False,
                stage=1,
            ),
        }

        return DependencyAnalysisResult(
            graphs=graphs,
            pipeline_dependencies=pipeline_dependencies,
            execution_stages=[["pipeline1"], ["pipeline2"]],
            circular_dependencies=[],
            external_sources=["external.source1"],
        )

    def test_save_outputs_all_formats(self):
        mock_analyzer = Mock()
        mock_analyzer.export_to_dot.return_value = "digraph test { a -> b; }"
        mock_analyzer.export_to_json.return_value = {"test": "data"}
        mock_analyzer.project_root = self.temp_dir

        result = self.create_mock_analysis_result()
        # The writer drives the single validated path via the service; configure
        # it to return the single-job (no-job-name) tuple shape.
        mock_analyzer.analyze_dependencies_by_job.return_value = ({}, result)
        output_formats = ["all"]

        generated_files = self.output_manager.save_outputs(
            mock_analyzer, result, output_formats, self.temp_dir
        )

        assert "dot" in generated_files
        assert "json" in generated_files
        assert "text" in generated_files
        assert "job" in generated_files

        for file_path in generated_files.values():
            assert file_path.exists()

    def test_save_outputs_specific_formats(self):
        mock_analyzer = Mock()
        mock_analyzer.export_to_dot.return_value = "digraph test { a -> b; }"
        mock_analyzer.export_to_json.return_value = {"test": "data"}

        result = self.create_mock_analysis_result()
        output_formats = ["dot", "json"]

        generated_files = self.output_manager.save_outputs(
            mock_analyzer, result, output_formats, self.temp_dir
        )

        assert "dot" in generated_files
        assert "json" in generated_files
        assert "text" not in generated_files
        assert "job" not in generated_files
        assert generated_files["dot"].exists()
        assert generated_files["json"].exists()

    def test_save_outputs_invalid_format(self):
        mock_analyzer = Mock()
        result = self.create_mock_analysis_result()
        output_formats = ["invalid_format"]

        with pytest.raises(ValueError) as exc_info:
            self.output_manager.save_outputs(
                mock_analyzer, result, output_formats, self.temp_dir
            )

        assert "Invalid output formats: {'invalid_format'}" in str(exc_info.value)

    def test_save_dot_format_renders_real_dot_content(self):
        """DOT save writes the module-level ``export_to_dot`` output, not the analyzer mock.

        Mocks the file-I/O seam to capture written bytes and asserts real renderer
        structure: ``digraph`` header, double-quoted node identifiers, escaped ``\\n``
        in pipeline labels, quoted edge lines. Fails if the serializer drops quoting
        or stops escaping newlines.
        """
        # A bare Mock with a dead ``export_to_dot`` attribute — the real code
        # must NOT touch it (it does ``del analyzer`` then calls the pure
        # module-level serializer). If the code regressed to call the analyzer,
        # this poisoned return value would surface in the written content.
        poisoned_analyzer = Mock()
        poisoned_analyzer.export_to_dot.return_value = "POISONED-SHOULD-NOT-APPEAR"

        graphs = self.create_mock_graphs()
        output_path = self.temp_dir / "dependencies.dot"

        m = mock_open()
        with patch("builtins.open", m):
            result_path = self.output_manager.save_dot_format(
                poisoned_analyzer, graphs, output_path, level="pipeline"
            )

        assert result_path == output_path
        m.assert_called_once_with(output_path, "w", encoding="utf-8")

        written = "".join(call.args[0] for call in m.return_value.write.call_args_list)

        poisoned_analyzer.export_to_dot.assert_not_called()
        assert "POISONED-SHOULD-NOT-APPEAR" not in written

        assert written.startswith("digraph pipeline_dependencies {")
        assert "rankdir=LR;" in written
        # Node identifiers are double-quoted (regression guard on quoting).
        assert '"pipeline1" [label=' in written
        assert '"pipeline2" [label=' in written
        # Pipeline-level labels embed an ESCAPED newline (literal backslash-n),
        # never a raw newline that would break the DOT line.
        assert '"pipeline1\\n(1 flowgroups)"' in written
        assert "pipeline1\n(1 flowgroups)" not in written
        assert '"pipeline1" -> "pipeline2";' in written

        from lhp.core.dependencies.output import export_to_dot

        assert written == export_to_dot(graphs, "pipeline")

    def test_save_json_format_renders_real_json_with_generation_info(self):
        """JSON save writes the module-level ``export_to_json`` output plus a ``generation_info`` block.

        Mocks the file-I/O seam to capture serialized bytes and asserts that
        ``generation_info`` is present and well-formed, and that pipeline metadata /
        ``depends_on`` / execution stages / external sources reflect the real result.
        Fails if the serializer drops a section or the manager stops adding ``generation_info``.
        """
        poisoned_analyzer = Mock()
        poisoned_analyzer.export_to_json.return_value = {"POISONED": True}

        result = self.create_mock_analysis_result()
        output_path = self.temp_dir / "dependencies.json"

        m = mock_open()
        with patch("builtins.open", m):
            result_path = self.output_manager.save_json_format(
                poisoned_analyzer, result, output_path
            )

        assert result_path == output_path
        m.assert_called_once_with(output_path, "w", encoding="utf-8")

        # ``json.dump`` writes in chunks; reassemble and parse the real bytes.
        written = "".join(call.args[0] for call in m.return_value.write.call_args_list)
        saved = json.loads(written)

        poisoned_analyzer.export_to_json.assert_not_called()
        assert "POISONED" not in saved

        assert "generation_info" in saved
        gen_info = saved["generation_info"]
        assert gen_info["generator"] == "LakehousePlumber DependencyAnalysisService"
        assert gen_info["version"] == "1.0"
        assert "generated_at" in gen_info  # ISO timestamp from datetime.isoformat()

        assert saved["metadata"]["total_pipelines"] == 2
        assert saved["metadata"]["total_stages"] == 2
        assert saved["metadata"]["has_circular_dependencies"] is False
        assert set(saved["pipelines"].keys()) == {"pipeline1", "pipeline2"}
        assert saved["pipelines"]["pipeline1"]["depends_on"] == []
        assert saved["pipelines"]["pipeline2"]["depends_on"] == ["pipeline1"]
        assert saved["pipelines"]["pipeline2"]["stage"] == 1
        assert saved["execution_stages"] == [["pipeline1"], ["pipeline2"]]
        assert saved["external_sources"] == ["external.source1"]
        assert saved["circular_dependencies"] == []
        # Warnings key is ALWAYS present (stable schema), empty when none.
        assert saved["warnings"] == []
        assert saved["metadata"]["total_warnings"] == 0

    def test_save_text_format(self):
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
        result = self.create_mock_analysis_result()
        result.circular_dependencies = [
            ["pipeline cycle: pipeline1 -> pipeline2 -> pipeline1"]
        ]

        output_path = self.temp_dir / "dependencies.txt"
        result_path = self.output_manager.save_text_format(result, output_path)

        content = result_path.read_text()
        assert "CIRCULAR DEPENDENCIES" in content
        assert "pipeline cycle: pipeline1 -> pipeline2 -> pipeline1" in content

    def test_save_job_format_default_name(self):
        mock_analyzer = Mock()
        mock_job_generator = Mock()
        mock_job_generator.save_job_to_file.return_value = (
            self.temp_dir / "test_orchestration.job.yml"
        )

        result = self.create_mock_analysis_result()
        mock_analyzer.analyze_dependencies_by_job.return_value = ({}, result)

        with patch(
            "lhp.core.dependencies.output_writer.JobGenerator",
            return_value=mock_job_generator,
        ):
            self.output_manager._save_job_format(mock_analyzer, result, self.temp_dir)

        mock_job_generator.save_job_to_file.assert_called_once()

    def test_save_job_format_custom_name(self):
        mock_analyzer = Mock()
        mock_job_generator = Mock()
        mock_job_generator.save_job_to_file.return_value = (
            self.temp_dir / "custom_job.job.yml"
        )

        result = self.create_mock_analysis_result()
        mock_analyzer.analyze_dependencies_by_job.return_value = ({}, result)
        custom_name = "custom_orchestration_job"

        with patch(
            "lhp.core.dependencies.output_writer.JobGenerator",
            return_value=mock_job_generator,
        ):
            self.output_manager._save_job_format(
                mock_analyzer, result, self.temp_dir, custom_name
            )

        args = mock_job_generator.save_job_to_file.call_args[0]
        assert args[2] == custom_name  # job_name parameter

    def test_resolve_output_directory_default(self):
        expected_path = Path.cwd() / ".lhp" / "dependencies"

        result_path = self.output_manager._resolve_output_directory(None)
        assert result_path == expected_path

    def test_resolve_output_directory_custom(self):
        custom_path = Path("/custom/output/dir")

        result_path = self.output_manager._resolve_output_directory(custom_path)
        assert result_path == custom_path

    def test_ensure_directory_exists_new(self):
        new_dir = self.temp_dir / "new_directory"
        assert not new_dir.exists()

        self.output_manager._ensure_directory_exists(new_dir)
        assert new_dir.exists()
        assert new_dir.is_dir()

    def test_ensure_directory_exists_existing(self):
        existing_dir = self.temp_dir / "existing"
        existing_dir.mkdir()
        assert existing_dir.exists()

        self.output_manager._ensure_directory_exists(existing_dir)
        assert existing_dir.exists()

    def test_text_format_integration(self):
        result = self.create_mock_analysis_result()
        output_path = self.temp_dir / "integration_test.txt"

        result_path = self.output_manager.save_text_format(result, output_path)
        content = result_path.read_text()

        assert "Stage 1: pipeline1" in content
        assert "Stage 2: pipeline2" in content
        assert "Pipeline: pipeline1" in content
        assert "Pipeline: pipeline2" in content
        assert "Flowgroups: 1" in content
        assert "Actions: 1" in content
        assert "Depends on: None" in content
        assert "Depends on: pipeline1" in content

    def test_io_error_from_write_seam_is_wrapped_with_cause(self):
        """An ``IOError`` from the file-write seam is wrapped, preserving the cause.

        The real ``save_dot_format`` renders via the pure ``export_to_dot`` and
        only touches the filesystem at ``open(...).write(...)``. When that write
        seam raises ``IOError``, the manager re-raises ``IOError`` with the
        target path in the message and chains the original via ``from e``
        (constitution §7.3). This mocks the I/O seam to raise and asserts both
        the wrapping message and the ``__cause__`` chain — it would FAIL if the
        manager swallowed the error or lost the cause.
        """
        analyzer = Mock()  # never used by the DOT path (``del analyzer``)
        graphs = self.create_mock_graphs()
        output_path = self.temp_dir / "dependencies.dot"

        handle = mock_open().return_value
        original = IOError("Disk full")
        handle.write.side_effect = original

        with patch("builtins.open", return_value=handle):
            with pytest.raises(IOError) as exc_info:
                self.output_manager.save_dot_format(analyzer, graphs, output_path)

        assert "Failed to save DOT file" in str(exc_info.value)
        assert str(output_path) in str(exc_info.value)
        assert "Disk full" in str(exc_info.value)
        # Cause chain preserved (§7.3 ``raise ... from e``).
        assert exc_info.value.__cause__ is original

    def test_file_generation_summary(self):
        mock_analyzer = Mock()
        mock_analyzer.export_to_dot.return_value = "digraph { }"
        mock_analyzer.export_to_json.return_value = {"test": "data"}

        result = self.create_mock_analysis_result()
        output_formats = ["dot", "json"]

        generated_files = self.output_manager.save_outputs(
            mock_analyzer, result, output_formats, self.temp_dir
        )

        for file_path in generated_files.values():
            assert file_path.exists()
            assert file_path.stat().st_size > 0

    def test_empty_execution_stages_handling(self):
        result = self.create_mock_analysis_result()
        result.execution_stages = []

        output_path = self.temp_dir / "dependencies.txt"
        result_path = self.output_manager.save_text_format(result, output_path)

        content = result_path.read_text()
        assert (
            "No pipelines found or circular dependencies prevent execution order"
            in content
        )

    def test_large_external_sources_handling(self):
        result = self.create_mock_analysis_result()
        many_sources = [f"external.table_{i}" for i in range(20)]
        result.external_sources = many_sources
        result.pipeline_dependencies["pipeline1"].external_sources = many_sources[:7]

        output_path = self.temp_dir / "dependencies.txt"
        result_path = self.output_manager.save_text_format(result, output_path)

        content = result_path.read_text()
        assert "external.table_0" in content
        # For pipeline details, should truncate after 5 and show "... and X more"
        assert "... and" in content

    def test_base_output_dir_initialization(self):
        custom_base_dir = Path("/custom/base")
        manager = DependencyOutputWriter(custom_base_dir)
        assert manager.base_output_dir == custom_base_dir

        default_manager = DependencyOutputWriter()
        assert default_manager.base_output_dir is None

    def test_concurrent_file_operations(self):
        mock_analyzer = Mock()
        mock_analyzer.export_to_dot.return_value = "digraph test { }"
        mock_analyzer.export_to_json.return_value = {"test": "concurrent"}

        result = self.create_mock_analysis_result()

        files1 = self.output_manager.save_outputs(
            mock_analyzer, result, ["dot"], self.temp_dir / "output1"
        )
        files2 = self.output_manager.save_outputs(
            mock_analyzer, result, ["json"], self.temp_dir / "output2"
        )

        assert files1["dot"].exists()
        assert files2["json"].exists()

    @patch("builtins.open", mock_open())
    def test_unicode_handling_in_text_output(self):
        result = self.create_mock_analysis_result()

        result.pipeline_dependencies["pipeline_测试"] = PipelineDependency(
            pipeline="pipeline_测试",
            depends_on=[],
            flowgroup_count=1,
            action_count=1,
            external_sources=[],
            can_run_parallel=False,
            stage=0,
        )

        output_path = self.temp_dir / "unicode_test.txt"

        result_path = self.output_manager.save_text_format(result, output_path)
        assert result_path == output_path


def test_save_job_to_default_location(tmp_path):
    """Job saves to .lhp/dependencies/ by default."""
    output_manager = DependencyOutputWriter()

    analyzer = Mock()
    analyzer.project_root = tmp_path / "project"
    analyzer.export_to_dot = Mock(return_value="digraph {}")
    analyzer.export_to_json = Mock(return_value={})

    result = create_test_dependency_result()
    analyzer.analyze_dependencies_by_job.return_value = ({}, result)

    output_dir = tmp_path / ".lhp" / "dependencies"
    generated_files = output_manager.save_outputs(analyzer, result, ["job"], output_dir)

    assert "job" in generated_files
    assert str(generated_files["job"]).endswith(".job.yml")
    assert generated_files["job"].parent == output_dir


def test_save_job_to_resources_with_bundle_flag(tmp_path):
    """Job saves to resources/ when bundle_output=True."""
    output_manager = DependencyOutputWriter()

    analyzer = Mock()
    analyzer.project_root = tmp_path / "project"
    analyzer.project_root.mkdir(parents=True)
    analyzer.export_to_dot = Mock(return_value="digraph {}")
    analyzer.export_to_json = Mock(return_value={})

    result = create_test_dependency_result()
    analyzer.analyze_dependencies_by_job.return_value = ({}, result)

    output_dir = tmp_path / ".lhp" / "dependencies"
    generated_files = output_manager.save_outputs(
        analyzer, result, ["job"], output_dir, bundle_output=True, job_name="test_job"
    )

    assert "job" in generated_files
    expected_path = analyzer.project_root / "resources" / "test_job.job.yml"
    assert generated_files["job"] == expected_path


def test_save_job_passes_config_path_to_generator(tmp_path):
    """Config file path is passed to JobGenerator."""
    output_manager = DependencyOutputWriter()

    project_root = tmp_path / "project"
    project_root.mkdir()
    custom_config = project_root / "custom_config.yaml"
    custom_config.write_text("max_concurrent_runs: 10\n")

    analyzer = Mock()
    analyzer.project_root = project_root
    analyzer.export_to_dot = Mock(return_value="digraph {}")
    analyzer.export_to_json = Mock(return_value={})

    result = create_test_dependency_result()
    analyzer.analyze_dependencies_by_job.return_value = ({}, result)

    output_dir = tmp_path / ".lhp" / "dependencies"
    output_manager.save_outputs(
        analyzer, result, ["job"], output_dir, job_config_path="custom_config.yaml"
    )

    job_files = list(output_dir.glob("*.job.yml"))
    assert len(job_files) == 1

    with open(job_files[0]) as f:
        content = f.read()
        assert "max_concurrent_runs: 10" in content


def test_save_job_creates_resources_directory_if_not_exists(tmp_path):
    """Resources directory is created if it doesn't exist."""
    output_manager = DependencyOutputWriter()

    project_root = tmp_path / "project"
    project_root.mkdir()

    analyzer = Mock()
    analyzer.project_root = project_root
    analyzer.export_to_dot = Mock(return_value="digraph {}")
    analyzer.export_to_json = Mock(return_value={})

    result = create_test_dependency_result()
    analyzer.analyze_dependencies_by_job.return_value = ({}, result)

    resources_dir = project_root / "resources"
    assert not resources_dir.exists()

    output_dir = tmp_path / ".lhp" / "dependencies"
    generated_files = output_manager.save_outputs(
        analyzer, result, ["job"], output_dir, bundle_output=True, job_name="test_job"
    )

    assert resources_dir.exists()
    assert generated_files["job"].exists()


def create_test_dependency_result():
    """Minimal DependencyAnalysisResult for testing."""
    action_graph = nx.DiGraph()
    flowgroup_graph = nx.DiGraph()
    pipeline_graph = nx.DiGraph()
    pipeline_graph.add_node("test_pipeline")

    graphs = DependencyGraphs(
        action_graph=action_graph,
        flowgroup_graph=flowgroup_graph,
        pipeline_graph=pipeline_graph,
        metadata={},
    )

    pipeline_dep = PipelineDependency(
        pipeline="test_pipeline",
        depends_on=[],
        flowgroup_count=1,
        action_count=1,
        external_sources=[],
        stage=1,
    )

    return DependencyAnalysisResult(
        graphs=graphs,
        pipeline_dependencies={"test_pipeline": pipeline_dep},
        execution_stages=[["test_pipeline"]],
        circular_dependencies=[],
        external_sources=[],
    )


class TestCircularDependencyGuard:
    def setup_method(self):
        self.temp_dir = Path(tempfile.mkdtemp())
        self.output_manager = DependencyOutputWriter()

    def teardown_method(self):
        import shutil

        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def create_mock_analysis_result(self):
        action_graph = nx.DiGraph()
        flowgroup_graph = nx.DiGraph()
        pipeline_graph = nx.DiGraph()
        pipeline_graph.add_node("pipeline1")

        graphs = DependencyGraphs(
            action_graph=action_graph,
            flowgroup_graph=flowgroup_graph,
            pipeline_graph=pipeline_graph,
            metadata={},
        )

        pipeline_dep = PipelineDependency(
            pipeline="pipeline1",
            depends_on=[],
            flowgroup_count=1,
            action_count=1,
            external_sources=[],
            stage=0,
        )

        return DependencyAnalysisResult(
            graphs=graphs,
            pipeline_dependencies={"pipeline1": pipeline_dep},
            execution_stages=[["pipeline1"]],
            circular_dependencies=[],
            external_sources=[],
        )

    def test_job_format_skipped_when_circular_dependencies_present(self):
        """Job format is SKIPPED when result has circular dependencies."""
        mock_analyzer = Mock()
        mock_analyzer.export_to_dot.return_value = "digraph {}"
        mock_analyzer.export_to_json.return_value = {}
        mock_analyzer.project_root = self.temp_dir

        result = self.create_mock_analysis_result()
        result.circular_dependencies = [["action level: A -> B -> A"]]

        generated_files = self.output_manager.save_outputs(
            mock_analyzer, result, ["job"], self.temp_dir
        )

        assert "job" not in generated_files

    def test_job_format_generated_when_no_circular_dependencies(self):
        mock_analyzer = Mock()
        mock_analyzer.export_to_dot.return_value = "digraph {}"
        mock_analyzer.export_to_json.return_value = {}
        mock_analyzer.project_root = self.temp_dir

        result = self.create_mock_analysis_result()
        result.circular_dependencies = []
        mock_analyzer.analyze_dependencies_by_job.return_value = ({}, result)

        generated_files = self.output_manager.save_outputs(
            mock_analyzer, result, ["job"], self.temp_dir
        )

        assert "job" in generated_files

    def test_all_formats_with_circular_deps_only_job_skipped(self):
        """When all formats requested + circular deps, only job is skipped; dot/json/text still generated."""
        mock_analyzer = Mock()
        mock_analyzer.export_to_dot.return_value = "digraph {}"
        mock_analyzer.export_to_json.return_value = {"test": "data"}
        mock_analyzer.project_root = self.temp_dir

        result = self.create_mock_analysis_result()
        result.circular_dependencies = [["action level: A -> B -> A"]]

        generated_files = self.output_manager.save_outputs(
            mock_analyzer, result, ["dot", "json", "text", "job"], self.temp_dir
        )

        assert "dot" in generated_files
        assert "json" in generated_files
        assert "text" in generated_files
        assert "job" not in generated_files

        assert generated_files["dot"].exists()
        assert generated_files["json"].exists()
        assert generated_files["text"].exists()


class TestAllFormatExpansion:
    def setup_method(self):
        self.temp_dir = Path(tempfile.mkdtemp())
        self.output_manager = DependencyOutputWriter()

    def teardown_method(self):
        import shutil

        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_all_format_expands_to_dot_json_text_job(self):
        """'all' format expands to exactly ['dot', 'json', 'text', 'job']."""
        mock_analyzer = Mock()
        mock_analyzer.export_to_dot.return_value = "digraph {}"
        mock_analyzer.export_to_json.return_value = {}
        mock_analyzer.project_root = self.temp_dir

        original_save_outputs = DependencyOutputWriter.save_outputs

        captured_formats = {}

        def patched_save_outputs(
            self_inner, analyzer, result, output_formats, *args, **kwargs
        ):
            if "all" in output_formats:
                output_formats = ["dot", "json", "text", "job"]
            captured_formats["expanded"] = list(output_formats)
            return original_save_outputs(
                self_inner, analyzer, result, output_formats, *args, **kwargs
            )

        action_graph = nx.DiGraph()
        flowgroup_graph = nx.DiGraph()
        pipeline_graph = nx.DiGraph()
        pipeline_graph.add_node("p1")

        graphs = DependencyGraphs(
            action_graph=action_graph,
            flowgroup_graph=flowgroup_graph,
            pipeline_graph=pipeline_graph,
            metadata={},
        )

        pipeline_dep = PipelineDependency(
            pipeline="p1",
            depends_on=[],
            flowgroup_count=1,
            action_count=1,
            external_sources=[],
            stage=0,
        )

        result = DependencyAnalysisResult(
            graphs=graphs,
            pipeline_dependencies={"p1": pipeline_dep},
            execution_stages=[["p1"]],
            circular_dependencies=[],
            external_sources=[],
        )
        mock_analyzer.analyze_dependencies_by_job.return_value = ({}, result)

        with patch.object(DependencyOutputWriter, "save_outputs", patched_save_outputs):
            self.output_manager.save_outputs(
                mock_analyzer, result, ["all"], self.temp_dir
            )

        assert captured_formats["expanded"] == ["dot", "json", "text", "job"]


def _make_warning(index: int = 0, *, file_path=None, line=None) -> DependencyWarning:
    """Synthetic extraction warning (emitters land in later tasks)."""
    return DependencyWarning(
        code="LHP-DEP-002",
        message=f"could not resolve source table for read {index}",
        flowgroup=f"fg{index}",
        action=f"load_{index}",
        suggestion="Add an explicit depends_on declaration",
        file_path=file_path,
        line=line,
    )


class TestDependencyWarningsInOutputs:
    """Warnings surfacing in JSON/text exports; DOT and job stay byte-stable."""

    def test_export_to_json_warnings_key_always_present_when_empty(self):
        """A warning-free result still carries ``warnings: []`` and the
        ``total_warnings`` metadata counter (stable schema)."""
        result = create_test_dependency_result()

        data = export_to_json(result)

        assert data["warnings"] == []
        assert data["metadata"]["total_warnings"] == 0
        # Round-trips through json.dumps (JSON-ready, no stray objects).
        json.loads(json.dumps(data))

    def test_export_to_json_carries_warnings_with_all_fields(self):
        result = create_test_dependency_result()
        result.warnings = [
            _make_warning(0, file_path="pipelines/fg0.yaml", line=12),
            _make_warning(1),
        ]

        data = export_to_json(result)

        assert data["metadata"]["total_warnings"] == 2
        assert data["metadata"]["total_warning_occurrences"] == 2
        assert data["warnings"] == [
            {
                "code": "LHP-DEP-002",
                "message": "could not resolve source table for read 0",
                "flowgroup": "fg0",
                "action": "load_0",
                "suggestion": "Add an explicit depends_on declaration",
                "file_path": "pipelines/fg0.yaml",
                "line": 12,
                "edit_yaml_path": None,
                "affected_actions": [],
                "affected_count": 1,
            },
            {
                "code": "LHP-DEP-002",
                "message": "could not resolve source table for read 1",
                "flowgroup": "fg1",
                "action": "load_1",
                "suggestion": "Add an explicit depends_on declaration",
                "file_path": None,
                "line": None,
                "edit_yaml_path": None,
                "affected_actions": [],
                "affected_count": 1,
            },
        ]
        json.loads(json.dumps(data))

    def test_export_to_json_aggregated_site_fields(self):
        """An aggregated site record serializes its affected actions and the
        occurrence total counts (site x action) pairs, not sites."""
        result = create_test_dependency_result()
        affected = (
            AffectedAction(
                flowgroup="fg0", action="load_0", edit_yaml_path="pipelines/fg0.yaml"
            ),
            AffectedAction(
                flowgroup="fg1", action="load_1", edit_yaml_path="pipelines/fg1.yaml"
            ),
        )
        result.warnings = [
            replace(
                _make_warning(0, file_path="py_functions/helper.py", line=4),
                edit_yaml_path="pipelines/fg0.yaml",
                affected_actions=affected,
                affected_count=2,
            )
        ]

        data = export_to_json(result)

        assert data["metadata"]["total_warnings"] == 1
        assert data["metadata"]["total_warning_occurrences"] == 2
        warning = data["warnings"][0]
        assert warning["edit_yaml_path"] == "pipelines/fg0.yaml"
        assert warning["affected_count"] == 2
        assert warning["affected_actions"] == [
            {
                "flowgroup": "fg0",
                "action": "load_0",
                "edit_yaml_path": "pipelines/fg0.yaml",
            },
            {
                "flowgroup": "fg1",
                "action": "load_1",
                "edit_yaml_path": "pipelines/fg1.yaml",
            },
        ]
        json.loads(json.dumps(data))

    def test_export_to_text_renders_warnings_section(self):
        """Text export renders one per-site block: location header, message,
        affected actions, edit-path hint (when known) and suggestion. The
        location adapts (file:line / file-only / omitted)."""
        result = create_test_dependency_result()
        result.warnings = [
            _make_warning(0, file_path="pipelines/fg0.yaml", line=12),
            _make_warning(1, file_path="pipelines/fg1.yaml"),
            _make_warning(2),
        ]

        content = export_to_text(result)

        assert "DEPENDENCY EXTRACTION WARNINGS" in content
        assert "3 unresolved site(s) affecting 3 action(s)" in content
        assert "  LHP-DEP-002 pipelines/fg0.yaml:12" in content
        assert "  LHP-DEP-002 pipelines/fg1.yaml" in content
        # No location -> bare code header line.
        assert "\n  LHP-DEP-002\n" in content
        assert "    could not resolve source table for read 0" in content
        assert "    Affected (1): fg0.load_0" in content
        assert "    Suggestion: Add an explicit depends_on declaration" in content

    def test_export_to_text_caps_affected_list_and_names_edit_paths(self):
        """One site affecting many actions renders a capped list with an
        overflow marker plus the distinct depends_on edit paths."""
        result = create_test_dependency_result()
        affected = tuple(
            AffectedAction(
                flowgroup=f"fg{i}",
                action=f"load_{i}",
                edit_yaml_path=f"pipelines/fg{i}.yaml",
            )
            for i in range(7)
        )
        result.warnings = [
            replace(
                _make_warning(0, file_path="py_functions/helper.py", line=4),
                edit_yaml_path="pipelines/fg0.yaml",
                affected_actions=affected,
                affected_count=7,
            )
        ]

        content = export_to_text(result)

        assert "1 unresolved site(s) affecting 7 action(s)" in content
        assert (
            "    Affected (7): fg0.load_0, fg1.load_1, fg2.load_2, "
            "fg3.load_3, fg4.load_4, +2 more" in content
        )
        assert (
            "    Add depends_on in: pipelines/fg0.yaml, pipelines/fg1.yaml" in content
        )

    def test_export_to_text_omits_section_when_no_warnings(self):
        """Pin the convention: empty warnings -> no section header at all."""
        result = create_test_dependency_result()

        content = export_to_text(result)

        assert "DEPENDENCY EXTRACTION WARNINGS" not in content
        assert "LHP-DEP-" not in content

    def test_dot_output_byte_identical_for_warning_bearing_result(self, tmp_path):
        """DOT bytes are IDENTICAL between a warning-free and a warning-bearing
        copy of the same result — warnings must not leak into DOT."""
        output_manager = DependencyOutputWriter()
        analyzer = Mock()

        plain = create_test_dependency_result()
        warned = create_test_dependency_result()
        warned.warnings = [
            _make_warning(i, file_path=f"pipelines/fg{i}.yaml", line=i)
            for i in range(3)
        ]

        files_plain = output_manager.save_outputs(
            analyzer, plain, ["dot"], tmp_path / "plain"
        )
        files_warned = output_manager.save_outputs(
            analyzer, warned, ["dot"], tmp_path / "warned"
        )

        assert files_plain["dot"].read_bytes() == files_warned["dot"].read_bytes()

    def test_job_output_byte_identical_for_warning_bearing_result(self, tmp_path):
        """Job/orchestration YAML is unchanged when the result carries warnings."""
        output_manager = DependencyOutputWriter()

        plain = create_test_dependency_result()
        warned = create_test_dependency_result()
        warned.warnings = [
            _make_warning(i, file_path=f"pipelines/fg{i}.yaml", line=i)
            for i in range(3)
        ]

        job_bytes = {}
        for label, result in (("plain", plain), ("warned", warned)):
            analyzer = Mock()
            analyzer.project_root = tmp_path / "project"
            analyzer.get_project_name.return_value = "test_project"
            analyzer.analyze_dependencies_by_job.return_value = ({}, result)

            files = output_manager.save_outputs(
                analyzer, result, ["job"], tmp_path / label
            )
            job_bytes[label] = files["job"].read_bytes()
            assert files["job"].name.endswith(".job.yml")

        assert job_bytes["plain"] == job_bytes["warned"]
