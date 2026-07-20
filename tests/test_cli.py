"""Tests for LakehousePlumber CLI commands."""

import shutil
import tempfile
from pathlib import Path

import pytest
import yaml
from click.testing import CliRunner

from lhp.cli.main import cli, get_version


class TestCLI:
    """Test CLI commands."""

    @pytest.fixture
    def runner(self):
        return CliRunner()

    @pytest.fixture
    def temp_project(self, windows_safe_tempdir):
        """Create a temporary project directory with Windows-safe cleanup."""
        return windows_safe_tempdir

    def test_cli_version(self, runner):
        """Test version command."""
        result = runner.invoke(cli, ["--version"])
        assert result.exit_code == 0
        # Click's --version contract embeds the version string; checking the
        # behavior (exit_code 0 + version present) is meaningful.
        expected_version = get_version()
        assert expected_version in result.output

    def test_init_command(self, runner, temp_project):
        """Test project initialization in CWD with bundle as default."""
        project_name = "test_project"

        with runner.isolated_filesystem(temp_dir=temp_project):
            result = runner.invoke(cli, ["init", project_name])

            assert result.exit_code == 0

            assert Path("lhp.yaml").exists()
            assert Path("pipelines").exists()
            assert Path("presets").exists()
            assert Path("templates").exists()
            assert Path("substitutions").exists()
            assert Path("schemas").exists()
            assert Path("substitutions/dev.yaml.tmpl").exists()
            assert Path("presets/bronze_layer.yaml.tmpl").exists()
            assert Path("README.md").exists()
            assert Path(".gitignore").exists()
            assert Path("databricks.yml").exists()
            assert Path("resources").exists()

    def test_init_existing_lhp_yaml(self, runner, temp_project):
        """Test init when lhp.yaml already exists in CWD."""
        from lhp.cli.exit_codes import ExitCode

        with runner.isolated_filesystem(temp_dir=temp_project):
            Path("lhp.yaml").write_text("name: existing\n")

            result = runner.invoke(cli, ["init", "test_project"])

            # LHP-IO-007 raised when lhp.yaml already exists; the error boundary
            # maps every LHPError to the collapsed ERROR exit code.
            assert result.exit_code == ExitCode.ERROR
            assert "LHP-IO-007" in result.output

    def test_init_no_bundle(self, runner, temp_project):
        """Test init with --no-bundle flag skips bundle files."""
        with runner.isolated_filesystem(temp_dir=temp_project):
            result = runner.invoke(cli, ["init", "--no-bundle", "test_project"])

            assert result.exit_code == 0

            assert Path("lhp.yaml").exists()
            assert Path("pipelines").exists()

            assert not Path("databricks.yml").exists()
            assert not Path("resources").exists()

    def test_validate_not_in_project(self, runner):
        """Test validate when not in a project directory."""
        from lhp.cli.exit_codes import ExitCode

        result = runner.invoke(cli, ["validate"])

        # LHP-CFG-011 raised by resolve_project_root; the error boundary maps
        # every LHPError to the collapsed ERROR exit code.
        assert result.exit_code == ExitCode.ERROR
        assert "LHP-CFG-011" in result.output

    def test_validate_empty_project(self, runner, temp_project):
        """Test validate with empty project."""
        with runner.isolated_filesystem(temp_dir=temp_project):
            # Initialize project in CWD
            runner.invoke(cli, ["init", "test_project"])

            # Create dev.yaml for testing by copying the template
            shutil.copy("substitutions/dev.yaml.tmpl", "substitutions/dev.yaml")

            # Run validate. This test exercises non-bundle behavior; ``lhp init``
            # scaffolds ``databricks.yml``, which auto-enables bundle support and
            # would otherwise require ``--pipeline-config`` (LHP-CFG-023), so pass
            # ``--no-bundle`` to opt out.
            result = runner.invoke(cli, ["validate", "--no-bundle"])

            # Ratified behaviour (spec §6.6): an empty project — no flowgroups
            # discovered — yields a clean ``ValidationCompleted`` and exits 0.
            # (This is intentionally asymmetric with generate's LHP-CFG-014.)
            from lhp.cli.exit_codes import ExitCode

            assert result.exit_code == ExitCode.SUCCESS

    def test_generate_bundle_sync_dry_run(self, runner, temp_project):
        """Test that ``lhp diff`` (the dry-run successor) materializes nothing.

        ``generate --dry-run`` was removed; the plan-only ``diff`` command is its
        successor. The original intent here was that a dry run never writes bundle
        resource files — ``diff`` plans every flowgroup but writes nothing, so the
        ``resources/lhp`` tree must remain unmaterialized.
        """
        with runner.isolated_filesystem(temp_dir=temp_project):
            runner.invoke(cli, ["init", "test_project"])

            # Create dev.yaml for testing by copying the template
            shutil.copy("substitutions/dev.yaml.tmpl", "substitutions/dev.yaml")

            # Create a pipeline
            pipeline_dir = Path("pipelines/test_pipeline")
            pipeline_dir.mkdir(parents=True)

            flowgroup_content = {
                "pipeline": "test_pipeline",
                "flowgroup": "test_flowgroup",
                "actions": [
                    {
                        "name": "load_data",
                        "type": "load",
                        "target": "v_raw_data",
                        "source": {"type": "sql", "sql": "SELECT * FROM raw_table"},
                    },
                    {
                        "name": "write_bronze",
                        "type": "write",
                        "source": "v_raw_data",
                        "write_target": {
                            "type": "streaming_table",
                            "catalog": "test_cat",
                            "schema": "bronze",
                            "table": "test_table",
                            "create_table": True,
                        },
                    },
                ],
            }

            with open(pipeline_dir / "test_flowgroup.yaml", "w") as f:
                yaml.dump(flowgroup_content, f)

            # ``diff`` is plan-only and bundle-independent, so it needs neither
            # ``--pipeline-config`` nor ``--no-bundle``. ``--verbose`` is now a
            # global group option and must precede the subcommand.
            result = runner.invoke(cli, ["--verbose", "diff", "--env", "dev"])

            assert result.exit_code == 0, (
                f"Unexpected failure; output:\n{result.output}"
            )
            # The plan-only diff must not materialize bundle resource files.
            assert not (Path("resources") / "lhp").exists() or not any(
                (Path("resources") / "lhp").iterdir()
            )

    def test_cli_help(self, runner):
        """Test CLI help command."""
        result = runner.invoke(cli, ["--help"])
        assert result.exit_code == 0
        assert "LakehousePlumber" in result.output
        assert "generate Lakeflow pipelines from YAML configs" in result.output

    def test_validate_with_pipeline(self, runner, temp_project):
        """Test validate with a valid pipeline."""
        with runner.isolated_filesystem(temp_dir=temp_project):
            runner.invoke(cli, ["init", "test_project"])
            shutil.copy("substitutions/dev.yaml.tmpl", "substitutions/dev.yaml")

            pipeline_dir = Path("pipelines/test_pipeline")
            pipeline_dir.mkdir(parents=True)

            flowgroup_content = {
                "pipeline": "test_pipeline",
                "flowgroup": "test_flowgroup",
                "actions": [
                    {
                        "name": "load_data",
                        "type": "load",
                        "target": "v_raw_data",
                        "source": {
                            "type": "cloudfiles",
                            "path": "/mnt/data/raw",
                            "format": "json",
                        },
                    },
                    {
                        "name": "write_data",
                        "type": "write",
                        "source": "v_raw_data",
                        "write_target": {
                            "type": "streaming_table",
                            "catalog": "test_cat",
                            "schema": "bronze",
                            "table": "test_table",
                            "create_table": True,
                        },
                    },
                ],
            }

            with open(pipeline_dir / "test_flowgroup.yaml", "w") as f:
                yaml.dump(flowgroup_content, f)

            # Run validate
            result = runner.invoke(cli, ["validate", "--env", "dev", "--no-bundle"])

            assert result.exit_code == 0

    def test_list_presets(self, runner, temp_project):
        """Test ``list presets`` command."""
        with runner.isolated_filesystem(temp_dir=temp_project):
            runner.invoke(cli, ["init", "test_project"])

            result = runner.invoke(cli, ["list", "presets"])

            assert result.exit_code == 0
            # Behavioral: bronze_layer.yaml was discovered and surfaced.
            assert "bronze_layer" in result.output

    def test_list_templates(self, runner, temp_project):
        """Test ``list templates`` command."""
        with runner.isolated_filesystem(temp_dir=temp_project):
            runner.invoke(cli, ["init", "test_project"])

            result = runner.invoke(cli, ["list", "templates"])

            assert result.exit_code == 0
            # Behavioral: standard_ingestion.yaml was discovered and surfaced.
            assert "standard_ingestion" in result.output

    def test_generate_dry_run(self, runner, temp_project):
        """Test the plan-only ``lhp diff`` command (the dry-run successor)."""
        with runner.isolated_filesystem(temp_dir=temp_project):
            runner.invoke(cli, ["init", "test_project"])
            shutil.copy("substitutions/dev.yaml.tmpl", "substitutions/dev.yaml")

            pipeline_dir = Path("pipelines/test_pipeline")
            pipeline_dir.mkdir(parents=True)

            flowgroup_content = {
                "pipeline": "test_pipeline",
                "flowgroup": "test_flowgroup",
                "actions": [
                    {
                        "name": "load_data",
                        "type": "load",
                        "target": "v_raw_data",
                        "source": {"type": "sql", "sql": "SELECT * FROM raw_table"},
                    },
                    {
                        "name": "write_bronze",
                        "type": "write",
                        "source": "v_raw_data",
                        "write_target": {
                            "type": "streaming_table",
                            "catalog": "test_cat",
                            "schema": "bronze",
                            "table": "test_table",
                            "create_table": True,
                        },
                    },
                ],
            }

            with open(pipeline_dir / "test_flowgroup.yaml", "w") as f:
                yaml.dump(flowgroup_content, f)

            # ``generate --dry-run`` was removed; ``diff`` is the plan-only
            # successor. It is bundle-independent, so no ``--no-bundle`` /
            # ``--pipeline-config`` is needed.
            result = runner.invoke(cli, ["diff", "--env", "dev"])

            assert result.exit_code == 0
            # The plan-only diff must not materialize any .py output.
            generated_root = Path("generated")
            assert not generated_root.exists() or not list(
                generated_root.glob("**/*.py")
            )

    def test_validate_with_secrets(self, runner, temp_project):
        """Test validate with secret references."""
        with runner.isolated_filesystem(temp_dir=temp_project):
            runner.invoke(cli, ["init", "test_project"])

            # Create dev.yaml for testing by copying the template
            shutil.copy("substitutions/dev.yaml.tmpl", "substitutions/dev.yaml")

            pipeline_dir = Path("pipelines/test_pipeline")
            pipeline_dir.mkdir(parents=True)

            flowgroup_content = {
                "pipeline": "test_pipeline",
                "flowgroup": "test_flowgroup",
                "actions": [
                    {
                        "name": "load_jdbc",
                        "type": "load",
                        "target": "v_jdbc_data",
                        "source": {
                            "type": "jdbc",
                            "url": "jdbc:postgresql://${secret:database/host}:5432/db",
                            "user": "${secret:database/username}",
                            "password": "${secret:database/password}",
                            "driver": "org.postgresql.Driver",
                            "table": "customers",
                        },
                    },
                    {
                        "name": "write_customers",
                        "type": "write",
                        "source": "v_jdbc_data",
                        "write_target": {
                            "type": "streaming_table",
                            "catalog": "test_cat",
                            "schema": "bronze",
                            "table": "customers_raw",
                            "create_table": True,
                        },
                    },
                ],
            }

            with open(pipeline_dir / "test_flowgroup.yaml", "w") as f:
                yaml.dump(flowgroup_content, f)

            # ``--verbose`` is now a global group option and must precede the
            # subcommand.
            result = runner.invoke(
                cli, ["--verbose", "validate", "--env", "dev", "--no-bundle"]
            )

            assert result.exit_code == 0

    def test_get_version_fallbacks(self, runner, temp_project):
        """Test get_version() fallback logic when package metadata is not available.

        ``get_version`` and the ``version`` metadata helper now live in
        ``lhp.cli._version`` (re-exported into ``lhp.cli.main``); the patch
        target and ``__file__`` override move with them.
        """
        from unittest.mock import patch

        import lhp.cli._version

        with patch("lhp.cli._version.version") as mock_version:
            mock_version.side_effect = Exception("Package not found")

            with tempfile.TemporaryDirectory() as tmpdir:
                pyproject_path = Path(tmpdir) / "pyproject.toml"
                pyproject_path.write_text("""
[tool.poetry]
name = "test-package"
version = "1.2.3"
description = "Test package"
""")

                original_file = lhp.cli._version.__file__
                try:
                    lhp.cli._version.__file__ = str(
                        Path(tmpdir) / "src" / "lhp" / "cli" / "_version.py"
                    )
                    version_result = lhp.cli._version.get_version()
                    assert version_result == "1.2.3"
                finally:
                    lhp.cli._version.__file__ = original_file

        with patch("lhp.cli._version.version") as mock_version:
            mock_version.side_effect = Exception("Package not found")

            with tempfile.TemporaryDirectory() as tmpdir:
                original_file = lhp.cli._version.__file__
                try:
                    lhp.cli._version.__file__ = str(
                        Path(tmpdir) / "deep" / "nested" / "path" / "_version.py"
                    )
                    version_result = lhp.cli._version.get_version()
                    assert version_result == "0.2.11"
                finally:
                    lhp.cli._version.__file__ = original_file

    def test_list_templates_empty_dir(self, runner, temp_project):
        """Test ``list templates`` command with no template files."""
        with runner.isolated_filesystem(temp_dir=temp_project):
            runner.invoke(cli, ["init", "test_project"])

            templates_dir = Path("templates")
            if templates_dir.exists():
                for template_file in templates_dir.glob("*.yaml"):
                    template_file.unlink()
                for template_file in templates_dir.glob("*.yml"):
                    template_file.unlink()

            result = runner.invoke(cli, ["list", "templates"])

            assert result.exit_code == 0

    def test_list_presets_empty_dir(self, runner, temp_project):
        """Test ``list presets`` command with no preset files."""
        with runner.isolated_filesystem(temp_dir=temp_project):
            runner.invoke(cli, ["init", "test_project"])

            presets_dir = Path("presets")
            if presets_dir.exists():
                for preset_file in presets_dir.glob("*.yaml"):
                    preset_file.unlink()
                for preset_file in presets_dir.glob("*.yml"):
                    preset_file.unlink()

            result = runner.invoke(cli, ["list", "presets"])

            assert result.exit_code == 0

    def test_generate_no_flowgroups_error(self, runner, temp_project):
        """Test generate command when no flowgroups found in project."""
        with runner.isolated_filesystem(temp_dir=temp_project):
            runner.invoke(cli, ["init", "test_project"])

            shutil.copy("substitutions/dev.yaml.tmpl", "substitutions/dev.yaml")

            pipeline_dir = Path("pipelines/empty_pipeline")
            pipeline_dir.mkdir(parents=True)

            # Run generate with ``--no-bundle`` so the v0.8.7 preflight
            # ``LHP-CFG-023`` check doesn't fire before flowgroup discovery —
            # this test specifically verifies the "no flowgroups" error.
            result = runner.invoke(cli, ["generate", "--env", "dev", "--no-bundle"])

            # Spec §6.6 mandates generate-on-empty raise LHP-CFG-014 (exit 1) —
            # intentionally asymmetric with validate's clean exit 0. The
            # exit-code constant rename (CONFIG_ERROR -> ERROR) is the only
            # mechanical change applied here; the asserted INTENT is unchanged.
            from lhp.cli.exit_codes import ExitCode

            assert result.exit_code == ExitCode.ERROR
            assert "LHP-CFG-014" in result.output
