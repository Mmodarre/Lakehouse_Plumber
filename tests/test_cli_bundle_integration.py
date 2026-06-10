"""Tests for CLI bundle integration functionality."""

import os
from pathlib import Path
from unittest.mock import Mock, patch

import pytest
import yaml
from click.testing import CliRunner

from lhp.cli.main import cli


class TestCLIBundleFlags:
    """Test CLI flag handling for bundle support."""

    @pytest.fixture(autouse=True)
    def setup_test_env(self, windows_safe_tempdir):
        self.temp_dir = windows_safe_tempdir
        self.project_root = self.temp_dir / "test_project"
        self.project_root.mkdir()
        self.runner = CliRunner()

    def _create_basic_project(self, with_bundle=False):
        """Create a basic LHP project structure."""
        (self.project_root / "lhp.yaml").write_text("""name: test_project
version: "1.0"
""")

        sub_dir = self.project_root / "substitutions"
        sub_dir.mkdir()
        (sub_dir / "dev.yaml").write_text(
            "dev:\n  catalog: dev_catalog\n  raw_schema: raw\n  bronze_schema: bronze\n"
        )

        pipe_dir = self.project_root / "pipelines"
        pipe_dir.mkdir()
        (pipe_dir / "test_pipeline.yaml").write_text("""pipeline: test
flowgroup: test_pipeline
actions:
  - name: test_load
    type: load
    source:
      type: delta
      database: "${catalog}.raw"
      table: test_table
    target: v_test_table
  - name: test_write
    type: write
    source: v_test_table
    write_target:
      type: streaming_table
      database: "${catalog}.bronze"
      table: test_table
""")

        if with_bundle:
            (self.project_root / "databricks.yml").write_text("""
bundle:
  name: test_bundle
""")
            # v0.8.7: bundle-enabled projects must ship a pipeline_config that
            # supplies catalog/schema, or preflight (LHP-CFG-023/026) blocks
            # generation. Project defaults apply to every discovered pipeline.
            config_dir = self.project_root / "config"
            config_dir.mkdir(parents=True, exist_ok=True)
            (config_dir / "pipeline_config.yaml").write_text(
                "project_defaults:\n"
                "  catalog: dev_catalog\n"
                "  schema: bronze\n"
                "  serverless: true\n"
            )

    def test_generate_with_no_bundle_flag_overrides_detection(self):
        """Should respect --no-bundle flag even when bundle files exist."""
        self._create_basic_project(with_bundle=True)

        original_cwd = os.getcwd()
        try:
            os.chdir(str(self.project_root))

            # Run generate with --no-bundle flag
            result = self.runner.invoke(
                cli,
                ["--verbose", "generate", "--env", "dev", "--no-bundle"],
            )

            assert result.exit_code == 0
        finally:
            os.chdir(original_cwd)

    def test_generate_without_no_bundle_flag_enables_bundle_when_detected(self):
        """Should enable bundle support when files exist and no --no-bundle flag."""
        self._create_basic_project(with_bundle=True)

        original_cwd = os.getcwd()
        try:
            os.chdir(str(self.project_root))

            result = self.runner.invoke(
                cli,
                [
                    "--verbose",
                    "generate",
                    "--env",
                    "dev",
                    "--pipeline-config",
                    "config/pipeline_config.yaml",
                ],
            )

            assert result.exit_code == 0
        finally:
            os.chdir(original_cwd)

    def test_no_bundle_flag_is_not_required(self):
        """Should work normally when --no-bundle flag is not provided."""
        self._create_basic_project()

        original_cwd = os.getcwd()
        try:
            os.chdir(str(self.project_root))

            result = self.runner.invoke(cli, ["generate", "--env", "dev"])

            assert result.exit_code == 0
        finally:
            os.chdir(original_cwd)

    def test_no_bundle_flag_help_text(self):
        """Should display help text for --no-bundle flag."""
        result = self.runner.invoke(cli, ["generate", "--help"])

        assert result.exit_code == 0
        assert "--no-bundle" in result.output


class TestCLIInitBundleCommand:
    """Test init command with bundle support."""

    @pytest.fixture(autouse=True)
    def setup_test_env(self, windows_safe_tempdir):
        self.temp_dir = windows_safe_tempdir
        self.runner = CliRunner()

    def test_init_creates_bundle_structure_by_default(self):
        """Should create bundle project structure by default (no flag needed)."""
        with self.runner.isolated_filesystem(temp_dir=self.temp_dir):
            result = self.runner.invoke(cli, ["init", "test_bundle_project"])

            assert result.exit_code == 0

            assert Path("lhp.yaml").exists()
            assert Path("substitutions").exists()
            assert Path("pipelines").exists()
            assert Path("databricks.yml").exists()
            assert Path("resources").exists()

            bundle_content = yaml.safe_load(Path("databricks.yml").read_text())
            assert "bundle" in bundle_content
            assert bundle_content["bundle"]["name"] == "test_bundle_project"
            assert "uuid" in bundle_content["bundle"]

    def test_init_no_bundle_creates_standard_project(self):
        """Should create standard project without bundle files with --no-bundle."""
        with self.runner.isolated_filesystem(temp_dir=self.temp_dir):
            result = self.runner.invoke(
                cli, ["init", "--no-bundle", "test_standard_project"]
            )

            assert result.exit_code == 0

            assert Path("lhp.yaml").exists()
            assert Path("substitutions").exists()
            assert not Path("databricks.yml").exists()
            assert not Path("resources").exists()

    def test_init_no_bundle_help_text(self):
        """Should display help text for --no-bundle flag in init command."""
        result = self.runner.invoke(cli, ["init", "--help"])

        assert result.exit_code == 0
        assert "--no-bundle" in result.output

    def test_init_bundle_integrates_with_template_fetcher(self):
        """Should use template fetcher to create bundle files."""
        with self.runner.isolated_filesystem(temp_dir=self.temp_dir):
            result = self.runner.invoke(cli, ["init", "test_template_project"])

            assert result.exit_code == 0

            assert Path("databricks.yml").exists()
            assert Path("resources").exists()

            content = Path("databricks.yml").read_text()
            assert "name: test_template_project" in content

    def test_init_handles_existing_lhp_yaml_error(self):
        """Should handle error when lhp.yaml already exists."""
        with self.runner.isolated_filesystem(temp_dir=self.temp_dir):
            Path("lhp.yaml").write_text("name: existing\n")

            result = self.runner.invoke(cli, ["init", "existing_project"])

            assert result.exit_code != 0
            # "already exists" lives verbatim in the LHPError title
            # (init_command.py raises LHPFileError code IO-007).
            assert "already exists" in result.output

    def test_init_bundle_creates_resources_directory(self):
        """Should create resources/lhp directory for bundle resource files."""
        with self.runner.isolated_filesystem(temp_dir=self.temp_dir):
            result = self.runner.invoke(cli, ["init", "test_resources_project"])

            assert result.exit_code == 0

            assert Path("resources").exists()
            assert Path("resources").is_dir()
            assert Path("resources/lhp").exists()
            assert Path("resources/lhp").is_dir()

    def test_init_bundle_uses_local_template_no_network(self):
        """Should create bundle files using local template without network calls."""
        with self.runner.isolated_filesystem(temp_dir=self.temp_dir):
            with patch("requests.get") as mock_get:
                result = self.runner.invoke(cli, ["init", "local_template_project"])

                assert result.exit_code == 0
                mock_get.assert_not_called()

                assert Path("databricks.yml").exists()
                assert Path("resources").exists()

                content = Path("databricks.yml").read_text()
                assert "name: local_template_project" in content

    def test_init_bundle_template_content_accuracy(self):
        """Should generate databricks.yml with accurate template content including UUID."""
        with self.runner.isolated_filesystem(temp_dir=self.temp_dir):
            result = self.runner.invoke(cli, ["init", "template_accuracy_test"])

            assert result.exit_code == 0

            content = Path("databricks.yml").read_text()

            assert "bundle:" in content
            assert "name: template_accuracy_test" in content
            assert "uuid:" in content
            assert "include:" in content
            assert "resources/*.yml" in content
            assert "resources/lhp/*.yml" in content
            assert "targets:" in content
            assert "dev:" in content
            assert "prod:" in content
            assert "mode: development" in content
            assert "mode: production" in content

            import re

            uuid_match = re.search(r"uuid:\s+([0-9a-f-]+)", content)
            assert uuid_match, "UUID not found in databricks.yml"
            assert len(uuid_match.group(1)) == 36

    def test_init_bundle_with_special_project_names(self):
        """Should handle special characters in project names correctly."""
        special_names = [
            "my-project-123",
            "project_with_underscores",
            "MixedCaseProject",
        ]

        for project_name in special_names:
            with self.runner.isolated_filesystem(temp_dir=self.temp_dir):
                result = self.runner.invoke(cli, ["init", project_name])

                assert result.exit_code == 0, f"Failed for project name: {project_name}"

                content = Path("databricks.yml").read_text()
                assert f"name: {project_name}" in content

    def test_init_bundle_complete_project_structure(self):
        """Should create complete LHP + Bundle project structure in CWD."""
        with self.runner.isolated_filesystem(temp_dir=self.temp_dir):
            result = self.runner.invoke(cli, ["init", "complete_structure_test"])

            assert result.exit_code == 0

            expected_structure = [
                "lhp.yaml",
                "databricks.yml",
                "substitutions",
                "substitutions/dev.yaml.tmpl",
                "pipelines",
                "resources",
                "presets",
                "templates",
            ]

            for path in expected_structure:
                assert Path(path).exists(), f"Missing: {path}"

            directory_paths = [
                "substitutions",
                "pipelines",
                "resources",
                "presets",
                "templates",
            ]
            for dir_path in directory_paths:
                assert Path(dir_path).is_dir(), f"Not a directory: {dir_path}"

    def test_init_bundle_preserves_lhp_content(self):
        """Should preserve LHP-specific file contents when adding bundle support."""
        with self.runner.isolated_filesystem(temp_dir=self.temp_dir):
            result = self.runner.invoke(cli, ["init", "lhp_content_test"])

            assert result.exit_code == 0

            lhp_config = yaml.safe_load(Path("lhp.yaml").read_text())
            assert lhp_config["name"] == "lhp_content_test"
            assert "version" in lhp_config

            import shutil

            shutil.copy("substitutions/dev.yaml.tmpl", "substitutions/dev.yaml")

            dev_subs = yaml.safe_load(Path("substitutions/dev.yaml").read_text())
            assert "dev" in dev_subs
            assert "catalog" in dev_subs["dev"]

    def test_init_bundle_resources_directory_empty(self):
        """Should create empty resources/lhp directory for bundle resource files."""
        with self.runner.isolated_filesystem(temp_dir=self.temp_dir):
            result = self.runner.invoke(cli, ["init", "empty_resources_test"])

            assert result.exit_code == 0

            resources_lhp_dir = Path("resources/lhp")

            assert Path("resources").exists()
            assert Path("resources").is_dir()
            assert resources_lhp_dir.exists()
            assert resources_lhp_dir.is_dir()

            lhp_contents = list(resources_lhp_dir.iterdir())
            assert len(lhp_contents) == 0, (
                f"LHP resources directory should be empty, found: {lhp_contents}"
            )


class TestCLIGenerateBundleIntegration:
    """Test generate command integration with bundle sync."""

    @pytest.fixture(autouse=True)
    def setup_test_env(self, windows_safe_tempdir):
        self.temp_dir = windows_safe_tempdir
        self.project_root = self.temp_dir / "test_project"
        self.project_root.mkdir()
        self.runner = CliRunner()

        self._create_project_with_bundle()

    def _create_project_with_bundle(self):
        """Create a complete project with bundle support."""
        (self.project_root / "lhp.yaml").write_text("""name: test_project
version: "1.0"
""")

        (self.project_root / "databricks.yml").write_text("""
bundle:
  name: test_project
target:
  dev:
    default: true
    mode: development
""")

        sub_dir = self.project_root / "substitutions"
        sub_dir.mkdir()
        (sub_dir / "dev.yaml").write_text(
            "dev:\n  catalog: dev_catalog\n  raw_schema: raw\n  bronze_schema: bronze\n"
        )

        pipe_dir = self.project_root / "pipelines"
        pipe_dir.mkdir()
        (pipe_dir / "raw_ingestion.yaml").write_text("""pipeline: raw_ingestion
flowgroup: raw_ingestion
actions:
  - name: customer_load
    type: load
    source:
      type: delta
      database: "${catalog}.${raw_schema}"
      table: customer
    target: v_customer
  - name: customer_write
    type: write
    source: v_customer
    write_target:
      type: streaming_table
      database: "${catalog}.${bronze_schema}"
      table: customer
""")

        resources_lhp_dir = self.project_root / "resources" / "lhp"
        resources_lhp_dir.mkdir(parents=True)

        # v0.8.7: bundle-enabled projects must supply catalog/schema via
        # pipeline_config.yaml or preflight blocks generation.
        config_dir = self.project_root / "config"
        config_dir.mkdir(parents=True, exist_ok=True)
        (config_dir / "pipeline_config.yaml").write_text(
            "project_defaults:\n"
            "  catalog: dev_catalog\n"
            "  schema: bronze\n"
            "  serverless: true\n"
        )

    @patch("lhp.bundle.manager.BundleManager")
    @patch("lhp.bundle.detection.should_enable_bundle_support")
    def test_generate_calls_bundle_sync_when_enabled(
        self, mock_bundle_detection, mock_bundle_manager_class
    ):
        """Should call bundle sync when bundle support is enabled."""
        mock_bundle_detection.return_value = True
        mock_bundle_manager = Mock()
        mock_bundle_manager_class.return_value = mock_bundle_manager

        original_cwd = os.getcwd()
        try:
            os.chdir(str(self.project_root))

            result = self.runner.invoke(
                cli,
                [
                    "--verbose",
                    "generate",
                    "--env",
                    "dev",
                    "--pipeline-config",
                    "config/pipeline_config.yaml",
                ],
            )

            assert result.exit_code == 0
        finally:
            os.chdir(original_cwd)

    def test_generate_skips_bundle_sync_when_disabled(self):
        """Should skip bundle sync when bundle support is disabled."""
        # Remove databricks.yml to disable bundle support
        databricks_file = self.project_root / "databricks.yml"
        if databricks_file.exists():
            databricks_file.unlink()

        original_cwd = os.getcwd()
        try:
            os.chdir(str(self.project_root))

            result = self.runner.invoke(cli, ["--verbose", "generate", "--env", "dev"])

            assert result.exit_code == 0
        finally:
            os.chdir(original_cwd)

    def test_generate_with_no_bundle_flag_disables_sync(self):
        """Should disable bundle sync when --no-bundle flag is used."""
        original_cwd = os.getcwd()
        try:
            os.chdir(str(self.project_root))

            result = self.runner.invoke(
                cli,
                ["--verbose", "generate", "--env", "dev", "--no-bundle"],
            )

            assert result.exit_code == 0
        finally:
            os.chdir(original_cwd)

    def test_generate_with_custom_output_directory(self):
        """Should work with custom output directory."""
        original_cwd = os.getcwd()
        try:
            os.chdir(str(self.project_root))

            result = self.runner.invoke(
                cli,
                [
                    "--verbose",
                    "generate",
                    "--env",
                    "dev",
                    "--output",
                    "custom_output",
                    "--pipeline-config",
                    "config/pipeline_config.yaml",
                ],
            )

            assert result.exit_code == 0
        finally:
            os.chdir(original_cwd)

    def test_generate_bundle_sync_runs(self):
        """Should perform bundle sync during generation."""
        original_cwd = os.getcwd()
        try:
            os.chdir(str(self.project_root))

            result = self.runner.invoke(
                cli,
                [
                    "--verbose",
                    "generate",
                    "--env",
                    "dev",
                    "--pipeline-config",
                    "config/pipeline_config.yaml",
                ],
            )

            assert result.exit_code == 0
        finally:
            os.chdir(original_cwd)


class TestCLIBundleErrorHandling:
    """Test CLI error handling for bundle operations."""

    @pytest.fixture(autouse=True)
    def setup_test_env(self, windows_safe_tempdir):
        self.temp_dir = windows_safe_tempdir
        self.project_root = self.temp_dir / "test_project"
        self.project_root.mkdir()
        self.runner = CliRunner()

    def test_generate_fails_without_substitution_file(self):
        """Should fail when the environment's substitution file is missing."""
        # Create minimal project without bundle setup
        (self.project_root / "lhp.yaml").write_text("name: test")

        # A flowgroup that references a ${catalog} token, so generation must
        # consult the (absent) dev substitution file. Without any flowgroup an
        # empty project generates 0 files and exits 0 (ratified, spec §6.6) —
        # the substitution file is never read. The flowgroup forces the
        # missing-substitution error path this test asserts on.
        pipe_dir = self.project_root / "pipelines"
        pipe_dir.mkdir()
        (pipe_dir / "test_pipeline.yaml").write_text("""pipeline: test
flowgroup: test_pipeline
actions:
  - name: test_load
    type: load
    source:
      type: delta
      database: "${catalog}.raw"
      table: test_table
    target: v_test_table
  - name: test_write
    type: write
    source: v_test_table
    write_target:
      type: streaming_table
      database: "${catalog}.bronze"
      table: test_table
""")

        original_cwd = os.getcwd()
        try:
            os.chdir(str(self.project_root))

            result = self.runner.invoke(cli, ["generate", "--env", "dev"])

            assert result.exit_code != 0
        finally:
            os.chdir(original_cwd)


class TestCLIBundleIntegrationEndToEnd:
    """End-to-end tests for CLI bundle integration."""

    @pytest.fixture(autouse=True)
    def setup_test_env(self, windows_safe_tempdir):
        self.temp_dir = windows_safe_tempdir
        self.runner = CliRunner()

    def test_complete_bundle_workflow(self):
        """Test complete workflow: init bundle project, add pipeline, generate with bundle sync."""
        with self.runner.isolated_filesystem(temp_dir=self.temp_dir):
            result = self.runner.invoke(cli, ["init", "test_workflow"])
            assert result.exit_code == 0
            assert Path("databricks.yml").exists()

            import shutil

            shutil.copy("substitutions/dev.yaml.tmpl", "substitutions/dev.yaml")

            Path("pipelines/test_pipeline.yaml").write_text("""pipeline: test_pipeline
flowgroup: test_pipeline
actions:
  - name: test_load
    type: load
    source:
      type: delta
      database: "${catalog}.${raw_schema}"
      table: test_table
    target: v_test_table
  - name: test_write
    type: write
    source: v_test_table
    write_target:
      type: streaming_table
      database: "${catalog}.${bronze_schema}"
      table: test_table
""")

            # v0.8.7: bundle-enabled projects require an explicit
            # pipeline_config.yaml. ``lhp init`` ships a .tmpl, not the
            # rendered file, so create a minimal valid config here.
            Path("config/pipeline_config.yaml").write_text(
                "project_defaults:\n"
                "  catalog: dev_catalog\n"
                "  schema: bronze\n"
                "  serverless: true\n"
            )

            result = self.runner.invoke(
                cli,
                [
                    "generate",
                    "--env",
                    "dev",
                    "--pipeline-config",
                    "config/pipeline_config.yaml",
                ],
            )

            assert result.exit_code == 0

    def test_bundle_sync_integration_with_multiple_pipelines(self):
        """Test bundle sync with multiple pipelines."""
        with self.runner.isolated_filesystem(temp_dir=self.temp_dir):
            result = self.runner.invoke(cli, ["init", "multi_pipeline_project"])

            import shutil

            shutil.copy("substitutions/dev.yaml.tmpl", "substitutions/dev.yaml")

            Path("pipelines/raw.yaml").write_text("""pipeline: raw
flowgroup: raw
actions:
  - name: customer_load
    type: load
    source:
      type: delta
      database: "${catalog}.${raw_schema}"
      table: customer
    target: v_customer
  - name: customer_write
    type: write
    source: v_customer
    write_target:
      type: streaming_table
      database: "${catalog}.${bronze_schema}"
      table: customer
""")
            Path("pipelines/bronze.yaml").write_text("""pipeline: bronze
flowgroup: bronze
actions:
  - name: customer_bronze_load
    type: load
    source:
      type: delta
      database: "${catalog}.${bronze_schema}"
      table: customer
    target: v_customer_bronze
  - name: customer_bronze_write
    type: write
    source: v_customer_bronze
    write_target:
      type: streaming_table
      database: "${catalog}.${silver_schema}"
      table: customer
""")

            # v0.8.7: pipeline_config.yaml is required for bundle-enabled
            # projects. project_defaults apply to every pipeline.
            Path("config/pipeline_config.yaml").write_text(
                "project_defaults:\n"
                "  catalog: dev_catalog\n"
                "  schema: bronze\n"
                "  serverless: true\n"
            )

            result = self.runner.invoke(
                cli,
                [
                    "--verbose",
                    "generate",
                    "--env",
                    "dev",
                    "--pipeline-config",
                    "config/pipeline_config.yaml",
                ],
            )

            assert result.exit_code == 0

    def test_no_bundle_flag_overrides_bundle_project(self):
        """Test that --no-bundle flag works even in bundle projects."""
        with self.runner.isolated_filesystem(temp_dir=self.temp_dir):
            result = self.runner.invoke(cli, ["init", "bundle_override_test"])

            import shutil

            shutil.copy("substitutions/dev.yaml.tmpl", "substitutions/dev.yaml")

            Path("pipelines/test.yaml").write_text("""pipeline: test
flowgroup: test
actions:
  - name: test_load
    type: load
    source:
      type: delta
      database: "${catalog}.${raw_schema}"
      table: test_table
    target: v_test_table
  - name: test_write
    type: write
    source: v_test_table
    write_target:
      type: streaming_table
      database: "${catalog}.${bronze_schema}"
      table: test_table
""")

            result = self.runner.invoke(
                cli, ["generate", "--env", "dev", "--no-bundle"]
            )

            assert result.exit_code == 0
