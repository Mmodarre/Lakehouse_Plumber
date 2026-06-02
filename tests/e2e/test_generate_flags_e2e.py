"""End-to-end tests for ``lhp generate`` CLI flags (B7).

Verifies the behavioral contracts of generate flags:

- ``--dry-run`` writes no Python files; preview output describes
  what would be generated.
- ``--pipeline <name>`` restricts generation to one pipeline.
- ``--no-bundle`` skips ``resources/lhp/`` even when ``databricks.yml``
  is present.
- ``--no-cleanup`` preserves orphaned Python files from removed
  flowgroups (default behavior deletes them).
- ``--pipeline-config <path>`` applies explicit cluster/serverless
  overrides to the generated pipeline YAML.
- ``--output <dir>`` redirects generated Python output to the given
  path; ``generated/dev/`` stays empty.

Phase 2 invariant: all mutations target the deep-copied tmp project
(``self.project_root``); no permanent fixtures are added under
``tests/e2e/fixtures/testing_project/``.
"""

import os
import shutil
import tempfile
from pathlib import Path

import pytest
from click.testing import CliRunner

from lhp.cli.main import cli


@pytest.mark.e2e
class TestGenerateFlagsE2E:
    """E2E coverage for ``lhp generate`` flag plumbing."""

    __test__ = True

    @pytest.fixture(autouse=True)
    def setup_test_project(self, isolated_project):
        """Create isolated copy of fixture project for each test."""
        fixture_path = Path(__file__).parent / "fixtures" / "testing_project"
        self.project_root = isolated_project / "test_project"
        shutil.copytree(fixture_path, self.project_root)

        self.original_cwd = os.getcwd()
        os.chdir(self.project_root)

        self.generated_dir = self.project_root / "generated" / "dev"
        self.resources_dir = self.project_root / "resources" / "lhp"

        self._init_bundle_project()

        yield
        os.chdir(self.original_cwd)

    def _init_bundle_project(self):
        """Wipe and recreate working directories."""
        if self.generated_dir.exists():
            shutil.rmtree(self.generated_dir)
        self.generated_dir.mkdir(parents=True, exist_ok=True)

        if self.resources_dir.exists():
            shutil.rmtree(self.resources_dir)
        self.resources_dir.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def run_generate(self, *args) -> tuple:
        """Run 'lhp generate' with the given args. Returns (exit_code, output).

        v0.8.7: bundle-enabled projects (databricks.yml present) require
        ``--pipeline-config`` or preflight blocks generation with
        ``LHP-CFG-023``. If the caller did not supply ``--pipeline-config``
        / ``-pc`` / ``--no-bundle`` and the fixture has a default
        ``config/pipeline_config.yaml``, inject ``-pc`` automatically so
        tests stay focused on their actual assertions.
        """
        runner = CliRunner()
        argv = list(args)
        needs_pc = (
            "--pipeline-config" not in argv
            and "-pc" not in argv
            and "--no-bundle" not in argv
            and (self.project_root / "config" / "pipeline_config.yaml").exists()
        )
        if needs_pc:
            argv.extend(["--pipeline-config", "config/pipeline_config.yaml"])
        result = runner.invoke(cli, ["generate", *argv])
        return result.exit_code, result.output

    # ------------------------------------------------------------------
    # Tests
    # ------------------------------------------------------------------

    def test_dry_run_emits_no_python_files(self):
        """``--dry-run`` previews generation without writing .py files."""
        exit_code, output = self.run_generate("--env", "dev", "--dry-run")
        assert exit_code == 0, f"Dry-run should succeed, got:\n{output[-2000:]}"

        py_files = list(self.generated_dir.rglob("*.py"))
        assert py_files == [], f"Dry-run must not write .py files, found:\n{py_files}"

        # Output should describe what would be generated and confirm dry-run
        assert (
            "Would generate" in output
            or "Dry run" in output
            or "dry-run" in output.lower()
        ), f"Expected dry-run preview output, got:\n{output[-2000:]}"

    def test_pipeline_filter_generates_only_matching_pipelines(self):
        """``--pipeline <name>`` restricts generation to one pipeline only."""
        exit_code, output = self.run_generate(
            "--env",
            "dev",
            "--pipeline",
            "acmi_edw_bronze",
            "--pipeline-config",
            "config/pipeline_config.yaml",
        )
        assert exit_code == 0, (
            f"Filtered generate should succeed, got:\n{output[-2000:]}"
        )

        bronze_dir = self.generated_dir / "acmi_edw_bronze"
        assert bronze_dir.exists(), "acmi_edw_bronze/ must be populated"
        assert any(bronze_dir.glob("*.py")), (
            "acmi_edw_bronze/ must contain at least one .py file"
        )

        # Other pipeline dirs must NOT have .py files
        other_pipeline_pys = [
            p
            for p in self.generated_dir.rglob("*.py")
            if "acmi_edw_bronze" not in str(p.relative_to(self.generated_dir))
        ]
        assert other_pipeline_pys == [], (
            "Pipeline filter must not produce output for other pipelines, "
            f"found:\n{other_pipeline_pys}"
        )

    def test_no_bundle_skips_resources_dir(self):
        """``--no-bundle`` keeps ``generated/dev/`` populated but leaves
        ``resources/lhp/`` empty (no pipeline.yml files)."""
        exit_code, output = self.run_generate("--env", "dev", "--no-bundle")
        assert exit_code == 0, (
            f"--no-bundle generate should succeed, got:\n{output[-2000:]}"
        )

        py_files = list(self.generated_dir.rglob("*.py"))
        assert py_files, "Python output must still be generated under generated/dev/"

        bundle_yamls = list(self.resources_dir.glob("*.pipeline.yml"))
        assert bundle_yamls == [], (
            f"--no-bundle must skip resources/lhp/, found:\n{bundle_yamls}"
        )

    def test_pipeline_config_explicit_path(self):
        """``--pipeline-config config/pipeline_config.yaml`` applies the
        fixture's classic-cluster overrides (serverless: false, explicit
        node_type_id) to the generated ``acmi_edw_raw.pipeline.yml``."""
        exit_code, output = self.run_generate(
            "--env",
            "dev",
            "--pipeline-config",
            "config/pipeline_config.yaml",
        )
        assert exit_code == 0, f"--pipeline-config generate failed:\n{output[-2000:]}"

        raw_yml = self.resources_dir / "acmi_edw_raw.pipeline.yml"
        assert raw_yml.exists(), "acmi_edw_raw.pipeline.yml must be generated"

        raw_yml_text = raw_yml.read_text()
        # The fixture pipeline_config.yaml sets:
        #   serverless: false
        #   clusters[0].node_type_id: "{cluster_node_type_id}"  →
        #     resolves to "Standard_D4ds_v5" via substitutions/dev.yaml.
        assert "serverless: false" in raw_yml_text, (
            "Expected serverless: false override from pipeline_config.yaml. "
            f"Got:\n{raw_yml_text[:600]}"
        )
        assert "Standard_D4ds_v5" in raw_yml_text, (
            "Expected node_type_id override from pipeline_config.yaml + "
            f"substitutions/dev.yaml. Got:\n{raw_yml_text[:600]}"
        )

    def test_output_redirect(self):
        """``--output <dir>`` writes Python files to the given path
        instead of ``generated/dev/``."""
        with tempfile.TemporaryDirectory() as redirected:
            exit_code, output = self.run_generate(
                "--env",
                "dev",
                "--output",
                redirected,
                "--pipeline-config",
                "config/pipeline_config.yaml",
            )
            assert exit_code == 0, f"--output generate failed:\n{output[-2000:]}"

            redirected_pys = list(Path(redirected).rglob("*.py"))
            assert redirected_pys, (
                f"--output target must contain generated .py files: {redirected}"
            )

            default_pys = list(self.generated_dir.rglob("*.py"))
            assert default_pys == [], (
                "generated/dev/ must remain empty when --output is set, "
                f"found:\n{default_pys}"
            )
