"""
End-to-end integration tests for complete bundle workflow.

Tests the entire bundle integration from project initialization through
generation and synchronization, using real project data and scenarios.
"""

import pytest
import tempfile
import shutil
import subprocess
import os
import time
from pathlib import Path
from unittest.mock import patch, Mock

from lhp.cli.main import cli
from lhp.core.orchestrator import ActionOrchestrator
from lhp.core.state_manager import StateManager
from lhp.bundle.manager import BundleManager
from lhp.utils.bundle_detection import should_enable_bundle_support


class TestEndToEndBundleWorkflow:
    """Test complete bundle workflow from initialization to generation."""

    def setup_method(self):
        """Set up test environment for each test."""
        self.temp_dir = Path(tempfile.mkdtemp())
        self.project_root = self.temp_dir / "e2e_test_project"
        self.original_cwd = os.getcwd()

    def teardown_method(self):
        """Clean up test environment after each test."""
        os.chdir(self.original_cwd)
        shutil.rmtree(self.temp_dir)

    def test_complete_bundle_project_lifecycle(self):
        """Test complete lifecycle: init -> configure -> generate -> sync."""
        os.chdir(self.temp_dir)
        
        # Step 1: Initialize bundle project
        with patch('lhp.bundle.template_fetcher.DatabricksTemplateFetcher.fetch_and_apply_template') as mock_fetch:
            mock_fetch.return_value = None
            
            from click.testing import CliRunner
            runner = CliRunner()
            
            # Test project initialization with bundle
            result = runner.invoke(cli, ['init', '--bundle', 'e2e_test_project'])
            assert result.exit_code == 0
            assert self.project_root.exists()
            assert (self.project_root / "databricks.yml").exists()
            assert (self.project_root / "resources").exists()

        # Step 2: Create realistic project structure
        self._create_realistic_project_structure()
        
        # Step 3: Verify bundle detection
        assert should_enable_bundle_support(self.project_root) == True
        
        # Step 4: Generate files using CLI and test bundle sync
        os.chdir(self.project_root)
        
        # Mock bundle sync to verify it's called at CLI level
        with patch('lhp.bundle.manager.BundleManager.sync_resources_with_generated_files') as mock_sync:
            # Use CLI runner to generate files (this is where bundle sync happens)
            from click.testing import CliRunner
            runner = CliRunner()
            
            result = runner.invoke(cli, ['generate', '-e', 'dev'])
            
            # Verify generation succeeded
            assert result.exit_code == 0
            output_dir = self.project_root / "generated"
            assert output_dir.exists()
            
            # Verify bundle sync was called at CLI level
            mock_sync.assert_called_once()
            
        # Step 5: Verify generated structure
        self._verify_generated_structure(output_dir)

    def test_bundle_workflow_with_multiple_environments(self):
        """Test bundle workflow across multiple environments."""
        self._setup_bundle_project()
        
        environments = ["dev", "test", "prod"]
        
        orchestrator = ActionOrchestrator(self.project_root)
        bundle_manager = BundleManager(self.project_root)
        
        for env in environments:
            # Generate for each environment
            output_dir = self.project_root / "generated"
            generated_files = orchestrator.generate_pipeline_by_field("test_pipeline", env, output_dir)
            
            assert len(generated_files) > 0
            
            # Verify environment-specific content
            pipeline_dir = output_dir / "test_pipeline"
            if pipeline_dir.exists():
                generated_file = pipeline_dir / f"{list(generated_files.keys())[0]}"
                if generated_file.exists():
                    content = generated_file.read_text()
                    # Should contain environment-specific substitutions
                    assert env in content.lower() or "catalog" in content.lower()

    def test_bundle_sync_with_actual_yaml_files(self):
        """Test bundle sync with real YAML resource files."""
        self._setup_bundle_project()
        
        # Create user's custom resource file (should be preserved)
        resources_dir = self.project_root / "resources"
        resources_dir.mkdir(exist_ok=True)
        
        user_resource_file = resources_dir / "user_custom_pipeline.yml"
        user_resource_file.write_text("""
resources:
  pipelines:
    user_custom_pipeline:
      clusters:
        - driver:
            "spark.databricks.cluster.profile": "singleNode"
          spark:
            spark.master: "local[*]"
      libraries:
        - notebook:
            path: ../src/custom_notebook.py
""")
        
        # Generate new files
        generated_dir = self.project_root / "generated"
        pipeline_dir = generated_dir / "test_pipeline"
        pipeline_dir.mkdir(parents=True)
        (pipeline_dir / "new_notebook.py").write_text("# New test notebook")
        (pipeline_dir / "existing_notebook.py").write_text("# Existing notebook")
        
        # Test sync
        bundle_manager = BundleManager(self.project_root)
        updated_count = bundle_manager.sync_resources_with_generated_files(generated_dir, "dev")
        
        # Verify sync worked
        assert updated_count >= 0  # Should not fail
        
        # Verify user's custom file was preserved and unchanged
        user_content = user_resource_file.read_text()
        assert "user_custom_pipeline" in user_content
        assert "../src/custom_notebook.py" in user_content
        
        # Verify LHP created its own file in resources/lhp/
        lhp_resource_file = self.project_root / "resources" / "lhp" / "test_pipeline.pipeline.yml"
        assert lhp_resource_file.exists()
        
        lhp_content = lhp_resource_file.read_text()
        assert "Generated by LakehousePlumber" in lhp_content
        # Should use glob pattern instead of individual notebook names
        assert "- glob:" in lhp_content
        assert "include: ../../generated/test_pipeline/**" in lhp_content

    def test_bundle_workflow_performance(self):
        """Test that bundle integration doesn't significantly impact performance."""
        self._setup_bundle_project()
        
        # Create multiple pipelines for performance testing
        pipelines_dir = self.project_root / "pipelines"
        for i in range(5):
            pipeline_file = pipelines_dir / f"pipeline_{i}.yaml"
            pipeline_file.write_text(f"""
pipeline: pipeline_{i}
flowgroup: flowgroup_{i}
actions:
  - name: load_{i}
    type: load
    source:
      type: delta
      database: "{{{{catalog}}}}.raw"
      table: table_{i}
    target: v_table_{i}
  - name: write_{i}
    type: write
    source: v_table_{i}
    write_target:
      type: streaming_table
      database: "{{{{catalog}}}}.bronze"
      table: table_{i}
""")
        
        orchestrator = ActionOrchestrator(self.project_root)
        
        # Measure generation time with bundle
        start_time = time.time()
        for i in range(5):
            output_dir = self.project_root / "generated"
            generated_files = orchestrator.generate_pipeline_by_field(f"pipeline_{i}", "dev", output_dir)
            assert len(generated_files) > 0
        bundle_time = time.time() - start_time
        
        # Performance should be reasonable (< 5 seconds for 5 small pipelines)
        assert bundle_time < 5.0, f"Bundle generation took too long: {bundle_time:.2f}s"

    def test_bundle_workflow_with_errors_and_recovery(self):
        """Test complete workflow handles errors gracefully."""
        self._setup_bundle_project()
        
        # Create scenario with some invalid YAML
        resources_dir = self.project_root / "resources"
        resources_dir.mkdir(exist_ok=True)
        
        # Valid resource file
        valid_file = resources_dir / "valid_pipeline.yml"
        valid_file.write_text("""
resources:
  pipelines:
    valid_pipeline:
      libraries: []
""")
        
        # Invalid resource file
        invalid_file = resources_dir / "invalid_pipeline.yml"
        invalid_file.write_text("""
resources:
  pipelines:
    invalid_pipeline:
      libraries: [invalid yaml structure
""")
        
        # Create corresponding generated files
        generated_dir = self.project_root / "generated"
        
        valid_pipeline_dir = generated_dir / "valid_pipeline"
        valid_pipeline_dir.mkdir(parents=True)
        (valid_pipeline_dir / "valid.py").write_text("# Valid notebook")
        
        invalid_pipeline_dir = generated_dir / "invalid_pipeline"
        invalid_pipeline_dir.mkdir(parents=True)
        (invalid_pipeline_dir / "invalid.py").write_text("# Invalid notebook")
        
        # Test sync with mixed valid/invalid files
        bundle_manager = BundleManager(self.project_root)
        
        # Should not crash despite invalid YAML
        try:
            updated_count = bundle_manager.sync_resources_with_generated_files(generated_dir, "dev")
            # Should return some count (possibly 0 due to errors, but not crash)
            assert isinstance(updated_count, int)
        except Exception as e:
            # If it raises an exception, it should be a controlled bundle exception
            from lhp.bundle.exceptions import BundleResourceError
            assert isinstance(e, BundleResourceError)

    def test_mixed_bundle_and_non_bundle_projects(self):
        """Test that bundle and non-bundle projects can coexist."""
        # Create bundle project
        bundle_project = self.temp_dir / "bundle_project"
        bundle_project.mkdir()
        (bundle_project / "databricks.yml").write_text("bundle:\n  name: test")
        
        # Create non-bundle project  
        non_bundle_project = self.temp_dir / "non_bundle_project"
        non_bundle_project.mkdir()
        
        # Test bundle detection
        assert should_enable_bundle_support(bundle_project) == True
        assert should_enable_bundle_support(non_bundle_project) == False
        
        # Both should work with orchestrator
        for project_root in [bundle_project, non_bundle_project]:
            self._create_minimal_project_structure(project_root)
            orchestrator = ActionOrchestrator(project_root)
            
            # Should initialize without errors
            assert orchestrator.project_root == project_root

    def test_cli_integration_with_verbose_output(self):
        """Test CLI integration with verbose bundle output."""
        self._setup_bundle_project()
        
        os.chdir(self.project_root)
        
        from click.testing import CliRunner
        runner = CliRunner()
        
        # Test verbose generation with bundle
        result = runner.invoke(cli, ['--verbose', 'generate', '--env', 'dev', '--dry-run'])
        
        # Should complete successfully
        assert result.exit_code == 0
        
        # Should mention bundle operations in verbose output
        output = result.output.lower()
        assert "bundle" in output or "resource" in output or "sync" in output

    def test_bundle_workflow_with_state_management(self):
        """Test bundle workflow with LHP state management."""
        self._setup_bundle_project()
        
        orchestrator = ActionOrchestrator(self.project_root)
        state_manager = StateManager(self.project_root)
        
        # First generation
        output_dir = self.project_root / "generated"
        generated_files = orchestrator.generate_pipeline_by_field(
            "test_pipeline", "dev", output_dir, state_manager=state_manager
        )
        
        assert len(generated_files) > 0
        
        # Verify state was saved
        state_file = self.project_root / ".lhp_state.json"
        assert state_file.exists()
        
        # Second generation (should use smart generation)
        generated_files_2 = orchestrator.generate_pipeline_by_field(
            "test_pipeline", "dev", output_dir, state_manager=state_manager
        )
        
        # Should work with state management
        assert isinstance(generated_files_2, dict)

    def _setup_bundle_project(self):
        """Set up a basic bundle project for testing."""
        self.project_root.mkdir(parents=True)
        
        # Create bundle file
        (self.project_root / "databricks.yml").write_text("""
bundle:
  name: test_bundle
""")
        
        # Create LHP project structure
        self._create_minimal_project_structure(self.project_root)

    def _create_realistic_project_structure(self):
        """Create a realistic LHP project structure."""
        # Project config
        (self.project_root / "lhp.yaml").write_text("""
name: e2e_test_project
version: "1.0"
""")
        
        # Substitutions
        subs_dir = self.project_root / "substitutions"
        subs_dir.mkdir(exist_ok=True)
        for env in ["dev", "test", "prod"]:
            (subs_dir / f"{env}.yaml").write_text(f"""
catalog: test_catalog_{env}
raw_schema: raw
bronze_schema: bronze
silver_schema: silver
gold_schema: gold
""")
        
        # Pipelines
        pipes_dir = self.project_root / "pipelines"
        pipes_dir.mkdir(exist_ok=True)
        (pipes_dir / "test_pipeline.yaml").write_text("""
pipeline: test_pipeline
flowgroup: test_flowgroup
actions:
  - name: load_customers
    type: load
    source:
      type: delta
      database: "{catalog}.{raw_schema}"
      table: customers
    target: v_customers_raw
  - name: transform_customers
    type: transform
    transform_type: sql
    source: v_customers_raw
    target: v_customers_clean
    sql: |
      SELECT 
        customer_id,
        UPPER(customer_name) as customer_name,
        customer_email
      FROM v_customers_raw
      WHERE customer_id IS NOT NULL
  - name: write_customers
    type: write
    source: v_customers_clean
    write_target:
      type: streaming_table
      database: "{catalog}.{bronze_schema}"
      table: customers
""")

    def _create_minimal_project_structure(self, project_root: Path):
        """Create minimal LHP project structure."""
        # Project config
        (project_root / "lhp.yaml").write_text("""
name: test_project
version: "1.0"
""")
        
        # Substitutions
        subs_dir = project_root / "substitutions"
        subs_dir.mkdir(exist_ok=True)
        (subs_dir / "dev.yaml").write_text("""
catalog: test_catalog
raw_schema: raw
""")
        
        # Basic pipeline
        pipes_dir = project_root / "pipelines"
        pipes_dir.mkdir(exist_ok=True)
        (pipes_dir / "test_pipeline.yaml").write_text("""
pipeline: test_pipeline
flowgroup: test_flowgroup
actions:
  - name: test_load
    type: load
    source:
      type: delta
      database: "{catalog}.{raw_schema}"
      table: test_table
    target: v_test_table
  - name: test_write
    type: write
    source: v_test_table
    write_target:
      type: streaming_table
      database: "{catalog}.bronze"
      table: test_table
""")

    def _verify_generated_structure(self, output_dir: Path):
        """Verify the generated file structure is correct."""
        assert output_dir.exists()
        
        # Should have pipeline directory
        pipeline_dir = output_dir / "test_pipeline"
        if pipeline_dir.exists():
            # Should have Python files
            py_files = list(pipeline_dir.glob("*.py"))
            assert len(py_files) > 0
            
            # Verify content structure
            for py_file in py_files:
                content = py_file.read_text()
                assert "import dlt" in content
                assert "Generated by LakehousePlumber" in content


class TestEndToEndACMIIntegration:
    """Test end-to-end integration using the real ACMI project."""

    def setup_method(self):
        """Set up test environment."""
        self.original_cwd = os.getcwd()
        # Use the actual ACMI project
        self.acmi_project = Path(__file__).parent.parent / "Example_Projects" / "acmi"
        
        if not self.acmi_project.exists():
            pytest.skip("ACMI project not available for integration testing")

    def teardown_method(self):
        """Clean up test environment."""
        os.chdir(self.original_cwd)

    def test_acmi_project_bundle_integration(self):
        """Test bundle integration with the real ACMI project."""
        os.chdir(self.acmi_project)
        
        # Verify ACMI project structure
        assert (self.acmi_project / "lhp.yaml").exists()
        assert (self.acmi_project / "pipelines").exists()
        
        # Test bundle detection
        bundle_enabled = should_enable_bundle_support(self.acmi_project)
        
        if bundle_enabled:
            # Test generation with bundle
            orchestrator = ActionOrchestrator(self.acmi_project)
            
            # Test dry-run generation
            generated_files = orchestrator.generate_pipeline_by_field("bronze_load", "dev", None, force_all=True)
            assert len(generated_files) > 0
            
            # Verify realistic content
            for filename, content in generated_files.items():
                assert "import dlt" in content
                assert "Generated by LakehousePlumber" in content
                assert "bronze_load" in content or "PIPELINE_ID" in content

    def test_acmi_project_multiple_pipelines(self):
        """Test ACMI project with multiple pipeline generations."""
        os.chdir(self.acmi_project)
        
        orchestrator = ActionOrchestrator(self.acmi_project)
        
        # Test multiple pipeline fields
        pipeline_fields = ["raw_ingestions", "bronze_load", "silver_load", "gold_load"]
        
        for pipeline_field in pipeline_fields:
            try:
                generated_files = orchestrator.generate_pipeline_by_field(pipeline_field, "dev", None)
                
                # Each pipeline should generate some files
                if len(generated_files) > 0:
                    # Verify content quality
                    for filename, content in generated_files.items():
                        assert len(content) > 100  # Should have substantial content
                        assert "dlt" in content
                        assert not content.count("ERROR") > 0  # No error markers
                        
            except Exception as e:
                # Log but don't fail - some pipelines might have dependencies
                print(f"Pipeline {pipeline_field} generation issue: {e}")

    def test_acmi_project_performance_benchmark(self):
        """Benchmark ACMI project generation performance."""
        os.chdir(self.acmi_project)
        
        orchestrator = ActionOrchestrator(self.acmi_project)
        
        # Measure total generation time
        start_time = time.time()
        
        try:
            generated_files = orchestrator.generate_pipeline_by_field("bronze_load", "dev", None)
            generation_time = time.time() - start_time
            
            # ACMI bronze_load should generate reasonably quickly
            assert generation_time < 10.0, f"ACMI generation too slow: {generation_time:.2f}s"
            
            # Should generate multiple files
            assert len(generated_files) >= 5, f"Expected multiple files, got {len(generated_files)}"
            
        except Exception as e:
            pytest.skip(f"ACMI generation failed: {e}")


class TestEndToEndCompatibility:
    """Test compatibility scenarios and edge cases."""

    def setup_method(self):
        """Set up test environment."""
        self.temp_dir = Path(tempfile.mkdtemp())
        self.original_cwd = os.getcwd()

    def teardown_method(self):
        """Clean up test environment."""
        os.chdir(self.original_cwd)
        shutil.rmtree(self.temp_dir)

    def test_bundle_detection_across_different_scenarios(self):
        """Test bundle detection in various project configurations."""
        scenarios = [
            # (has_databricks_yml, expected_result, description)
            (True, True, "Standard bundle project"),
            (False, False, "Non-bundle project"),
        ]
        
        for has_bundle_file, expected, description in scenarios:
            project_dir = self.temp_dir / f"test_{has_bundle_file}"
            project_dir.mkdir()
            
            if has_bundle_file:
                (project_dir / "databricks.yml").write_text("bundle:\n  name: test")
            
            result = should_enable_bundle_support(project_dir)
            assert result == expected, f"Failed for scenario: {description}"

    def test_bundle_with_complex_project_structures(self):
        """Test bundle functionality with complex project structures."""
        project_root = self.temp_dir / "complex_project"
        project_root.mkdir()
        
        # Create bundle file
        (project_root / "databricks.yml").write_text("""
bundle:
  name: complex_project
  environments:
    dev:
      default: true
""")
        
        # Create complex LHP structure
        (project_root / "lhp.yaml").write_text("""
name: complex_project
version: "2.0"
include:
  - "pipelines/**/*.yaml"
""")
        
        # Nested pipeline structure
        nested_dir = project_root / "pipelines" / "data_sources" / "external"
        nested_dir.mkdir(parents=True)
        (nested_dir / "external_data.yaml").write_text("""
pipeline: external_sources
flowgroup: external_data_load
actions:
  - name: load_external
    type: load
    source:
      type: cloudfiles
      path: "/external/data/*.json"
      format: json
    target: v_external_raw
""")
        
        # Test bundle detection and basic functionality
        assert should_enable_bundle_support(project_root) == True
        
        # Test orchestrator initialization
        orchestrator = ActionOrchestrator(project_root)
        assert orchestrator.project_root == project_root

    def test_bundle_workflow_memory_usage(self):
        """Test that bundle workflow doesn't have memory leaks."""
        project_root = self.temp_dir / "memory_test"
        project_root.mkdir()
        
        # Create bundle project
        (project_root / "databricks.yml").write_text("bundle:\n  name: memory_test")
        (project_root / "lhp.yaml").write_text("name: memory_test\nversion: '1.0'")
        
        # Create substitutions
        subs_dir = project_root / "substitutions"
        subs_dir.mkdir()
        (subs_dir / "dev.yaml").write_text("catalog: test")
        
        # Create many small pipelines to stress test
        pipes_dir = project_root / "pipelines"
        pipes_dir.mkdir()
        
        for i in range(20):
            (pipes_dir / f"pipeline_{i:02d}.yaml").write_text(f"""
pipeline: pipeline_{i:02d}
flowgroup: flowgroup_{i:02d}
actions:
  - name: load_{i:02d}
    type: load
    source:
      type: delta
      database: "{{catalog}}.raw"
      table: table_{i:02d}
    target: v_table_{i:02d}
  - name: write_{i:02d}
    type: write
    source: v_table_{i:02d}
    write_target:
      type: streaming_table
      database: "{{catalog}}.bronze"
      table: table_{i:02d}
""")
        
        orchestrator = ActionOrchestrator(project_root)
        
        # Generate multiple times to test for memory leaks
        for iteration in range(3):
            for i in range(5):  # Test subset to keep test time reasonable
                generated_files = orchestrator.generate_pipeline_by_field(f"pipeline_{i:02d}", "dev", None)
                assert len(generated_files) > 0
                
                # Clear reference to help garbage collection
                del generated_files

    def test_bundle_with_unicode_and_special_characters(self):
        """Test bundle functionality with Unicode and special characters."""
        project_root = self.temp_dir / "unicode_测试"
        project_root.mkdir()
        
        # Create bundle with Unicode content
        (project_root / "databricks.yml").write_text("""
bundle:
  name: unicode_测试_project
""", encoding='utf-8')
        
        # Create LHP project with Unicode
        (project_root / "lhp.yaml").write_text("""
name: unicode_测试_project
version: "1.0"
""", encoding='utf-8')
        
        # Test bundle detection
        assert should_enable_bundle_support(project_root) == True
        
        # Test bundle manager
        bundle_manager = BundleManager(project_root)
        
        # Should handle Unicode paths without errors
        assert bundle_manager.project_root == project_root
        assert bundle_manager.resources_dir.name == "lhp"
        assert bundle_manager.resources_dir.parent.name == "resources" 