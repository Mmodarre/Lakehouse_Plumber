"""
Tests for orchestrator behavior in bundle-enabled projects.

Tests that the orchestrator correctly handles bundle-related scenarios
while ensuring bundle synchronization is handled at the CLI level only.
Bundle sync is no longer called from within orchestrator methods.
"""

import pytest
import tempfile
import shutil
from pathlib import Path
from unittest.mock import Mock, patch

from lhp.core.orchestrator import ActionOrchestrator


class TestOrchestratorBundleBehavior:
    """Test suite for orchestrator behavior in bundle-enabled projects."""

    def setup_method(self):
        """Set up test environment for each test."""
        self.temp_dir = Path(tempfile.mkdtemp())
        self.project_root = self.temp_dir / "test_project"
        self.project_root.mkdir()
        
        # Create basic project structure
        self._create_test_project()
        
        self.orchestrator = ActionOrchestrator(self.project_root)

    def teardown_method(self):
        """Clean up test environment after each test."""
        shutil.rmtree(self.temp_dir)

    def _create_test_project(self):
        """Create a test project with proper structure."""
        # Create project config
        (self.project_root / "lhp.yaml").write_text("""name: test_project
version: "1.0"
""")
        
        # Create substitutions
        sub_dir = self.project_root / "substitutions"
        sub_dir.mkdir()
        (sub_dir / "dev.yaml").write_text("""catalog: test_catalog
raw_schema: raw
bronze_schema: bronze
""")
        
        # Create pipelines directory with test flowgroup
        pipelines_dir = self.project_root / "pipelines" / "test_pipeline"
        pipelines_dir.mkdir(parents=True)
        
        flowgroup_yaml = pipelines_dir / "test_flowgroup.yaml"
        flowgroup_yaml.write_text("""
flowgroup: test_flowgroup
pipeline: test_pipeline
actions:
  - name: test_action
    type: load
    source:
      type: sql
      sql: "SELECT 1 as test_col"
    target: test_table
  - name: write_test
    type: write
    source: test_table
    write_target:
      type: streaming_table
      database: "{catalog}.{raw_schema}"
      table: "test_output"
""")

        # Create templates directory
        templates_dir = self.project_root / "templates"
        templates_dir.mkdir()
        
        # Create presets directory
        presets_dir = self.project_root / "presets"
        presets_dir.mkdir()

    @patch('lhp.bundle.manager.BundleManager')
    def test_orchestrator_does_not_call_bundle_sync(self, mock_bundle_manager_class):
        """Should NOT call bundle sync - this is now handled at CLI level only."""
        # Mock bundle manager
        mock_bundle_manager = Mock()
        mock_bundle_manager_class.return_value = mock_bundle_manager
        
        # Generate files
        output_dir = self.project_root / "generated"
        generated_files = self.orchestrator.generate_pipeline_by_field(
            "test_pipeline", "dev", output_dir
        )
        
        # Verify files were generated
        assert len(generated_files) == 1
        assert "test_flowgroup.py" in generated_files
        
        # Verify bundle manager was NOT created or called from orchestrator
        mock_bundle_manager_class.assert_not_called()
        mock_bundle_manager.sync_resources_with_generated_files.assert_not_called()

    def test_orchestrator_generates_files_successfully_in_bundle_project(self):
        """Should generate files successfully without any bundle sync involvement."""
        # Create databricks.yml to simulate bundle project
        (self.project_root / "databricks.yml").write_text("""
bundle:
  name: test_project
""")
        
        # Generate files
        output_dir = self.project_root / "generated"
        generated_files = self.orchestrator.generate_pipeline_by_field(
            "test_pipeline", "dev", output_dir
        )
        
        # Verify files were generated correctly
        assert len(generated_files) == 1
        assert "test_flowgroup.py" in generated_files
        
        # Verify output file exists
        output_file = output_dir / "test_pipeline" / "test_flowgroup.py"
        assert output_file.exists()
        
        # Verify generated code content
        generated_code = output_file.read_text()
        assert "test_pipeline" in generated_code
        assert "test_flowgroup" in generated_code

    def test_orchestrator_preserves_generation_behavior(self):
        """Should preserve normal generation behavior regardless of bundle setup."""
        # Generate files
        output_dir = self.project_root / "generated"
        generated_files = self.orchestrator.generate_pipeline_by_field(
            "test_pipeline", "dev", output_dir
        )
        
        # Verify normal generation behavior
        assert isinstance(generated_files, dict)
        assert len(generated_files) == 1
        
        filename, code = next(iter(generated_files.items()))
        assert filename == "test_flowgroup.py"
        assert isinstance(code, str)
        assert len(code) > 0
        
        # Verify file structure
        assert "import dlt" in code
        assert "test_pipeline" in code
        assert "test_flowgroup" in code 