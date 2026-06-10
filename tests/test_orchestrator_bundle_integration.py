import shutil
import tempfile
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from lhp.api import LakehousePlumberApplicationFacade, collect_response


class TestOrchestratorBundleBehavior:
    def setup_method(self):
        self.temp_dir = Path(tempfile.mkdtemp())
        self.project_root = self.temp_dir / "test_project"
        self.project_root.mkdir()

        # Create basic project structure
        self._create_test_project()

        self.facade = LakehousePlumberApplicationFacade.for_project(
            self.project_root, enforce_version=False
        )

    def teardown_method(self):
        shutil.rmtree(self.temp_dir)

    def _create_test_project(self):
        (self.project_root / "lhp.yaml").write_text("""name: test_project
version: "1.0"
""")

        sub_dir = self.project_root / "substitutions"
        sub_dir.mkdir()
        (sub_dir / "dev.yaml").write_text("""dev:
  catalog: test_catalog
  raw_schema: raw
  bronze_schema: bronze
""")

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

        templates_dir = self.project_root / "templates"
        templates_dir.mkdir()

        presets_dir = self.project_root / "presets"
        presets_dir.mkdir()

    @patch("lhp.bundle.manager.BundleManager")
    def test_orchestrator_does_not_call_bundle_sync(self, mock_bundle_manager_class):
        """Bundle sync must not be called from orchestrator — CLI level only."""
        mock_bundle_manager = Mock()
        mock_bundle_manager_class.return_value = mock_bundle_manager

        output_dir = self.project_root / "generated"
        batch = collect_response(
            self.facade.generation.generate_pipelines(
                pipeline_filter="test_pipeline",
                env="dev",
                output_dir=output_dir,
            )
        )
        generated_files = batch.pipeline_responses["test_pipeline"].generated_filenames

        assert len(generated_files) == 1
        assert "test_flowgroup.py" in generated_files

        # NOT called from orchestrator — bundle sync belongs to CLI
        mock_bundle_manager_class.assert_not_called()
        mock_bundle_manager.sync_resources_with_generated_files.assert_not_called()

    def test_orchestrator_generates_files_successfully_in_bundle_project(self):
        # Simulate a bundle project
        (self.project_root / "databricks.yml").write_text("""
bundle:
  name: test_project
""")

        output_dir = self.project_root / "generated"
        batch = collect_response(
            self.facade.generation.generate_pipelines(
                pipeline_filter="test_pipeline",
                env="dev",
                output_dir=output_dir,
            )
        )
        generated_files = batch.pipeline_responses["test_pipeline"].generated_filenames

        assert len(generated_files) == 1
        assert "test_flowgroup.py" in generated_files

        output_file = output_dir / "test_pipeline" / "test_flowgroup.py"
        assert output_file.exists()

        generated_code = output_file.read_text()
        assert "test_pipeline" in generated_code
        assert "test_flowgroup" in generated_code

    def test_orchestrator_preserves_generation_behavior(self):
        output_dir = self.project_root / "generated"
        batch = collect_response(
            self.facade.generation.generate_pipelines(
                pipeline_filter="test_pipeline",
                env="dev",
                output_dir=output_dir,
            )
        )
        generated_files = batch.pipeline_responses["test_pipeline"].generated_filenames

        assert isinstance(generated_files, tuple)
        assert len(generated_files) == 1

        filename = generated_files[0]
        code = (output_dir / "test_pipeline" / filename).read_text()
        assert filename == "test_flowgroup.py"
        assert isinstance(code, str)
        assert len(code) > 0

        assert "from pyspark import pipelines as dp" in code
        assert "test_pipeline" in code
        assert "test_flowgroup" in code
