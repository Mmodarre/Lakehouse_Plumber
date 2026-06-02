"""E2E tests for LHP JDBC load action (B5.1).

Covers both modes of the JDBC load source documented in
``docs/actions/load_actions.rst:836-988``:

  - **table-mode** — ``spark.read.format("jdbc").option("dbtable", ...).load()``
    via a ``table:`` key in the YAML source.
  - **query-mode** — ``spark.read.format("jdbc").option("query", ...).load()``
    via a ``query:`` key in the YAML source.

Both fixtures resolve ``${secret:database/...}`` references through the
``database`` scope alias (``dev_db_secrets`` per ``substitutions/dev.yaml``),
producing ``dbutils.secrets.get(scope="dev_db_secrets", key=...)`` calls in
the generated code.

Each test uses ``lhp generate --env dev --force`` and hash-compares the
generated file under ``generated/dev/14_jdbc_load/`` against the baseline at
``generated_baseline/dev/14_jdbc_load/``. The pipeline resource YAML at
``resources/lhp/14_jdbc_load.pipeline.yml`` is also compared.

Fixture flowgroups live in ``tests/e2e/fixtures/testing_project/pipelines/14_jdbc_load/``.
"""

import hashlib
import os
import shutil
from pathlib import Path

import pytest
from click.testing import CliRunner

from lhp.cli.main import cli


@pytest.mark.e2e
class TestLoadJDBCE2E:
    """E2E tests for LHP JDBC load generation (table-mode + query-mode)."""

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
        self.jdbc_dir = self.generated_dir / "14_jdbc_load"
        self.jdbc_baseline_dir = (
            self.project_root / "generated_baseline" / "dev" / "14_jdbc_load"
        )
        self.resource_baseline = (
            self.project_root
            / "resources_baseline"
            / "lhp"
            / "14_jdbc_load.pipeline.yml"
        )

        self._init_bundle_project()

        yield
        os.chdir(self.original_cwd)

    def _init_bundle_project(self):
        if self.generated_dir.exists():
            shutil.rmtree(self.generated_dir)
        if self.resources_dir.exists():
            shutil.rmtree(self.resources_dir)
        self.generated_dir.mkdir(parents=True, exist_ok=True)
        self.resources_dir.mkdir(parents=True, exist_ok=True)

    def run_generate(self) -> tuple:
        """Run 'lhp generate --env dev --force' (no --include-tests)."""
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "generate",
                "--env",
                "dev",
                "--pipeline-config",
                "config/pipeline_config.yaml",
            ],
        )
        return result.exit_code, result.output

    def _compare_file_hashes(self, file1: Path, file2: Path) -> str:
        """Compare two files by SHA-256. Returns '' if identical, error string otherwise."""

        def get_hash(f: Path) -> str:
            return hashlib.sha256(f.read_bytes()).hexdigest()

        h1, h2 = get_hash(file1), get_hash(file2)
        if h1 != h2:
            return (
                f"Hash mismatch: {file1.name} ({h1[:12]}) != {file2.name} ({h2[:12]})"
            )
        return ""

    def _assert_baseline_match(self, filename: str):
        """Generate, then assert the named JDBC flowgroup file matches its baseline.

        Also verifies the generated pipeline resource YAML matches its baseline.
        """
        exit_code, output = self.run_generate()
        assert exit_code == 0, f"Generation failed: {output}"

        generated = self.jdbc_dir / filename
        baseline = self.jdbc_baseline_dir / filename
        assert generated.exists(), f"{filename} should be generated under 14_jdbc_load/"
        assert baseline.exists(), f"Baseline {filename} should exist"

        diff = self._compare_file_hashes(generated, baseline)
        assert diff == "", f"Baseline mismatch for {filename}: {diff}"

        # Verify the pipeline resource YAML also matches its baseline.
        generated_resource = self.resources_dir / "14_jdbc_load.pipeline.yml"
        assert generated_resource.exists(), (
            "14_jdbc_load.pipeline.yml should be generated under resources/lhp/"
        )
        assert self.resource_baseline.exists(), (
            "Resource baseline 14_jdbc_load.pipeline.yml should exist"
        )
        resource_diff = self._compare_file_hashes(
            generated_resource, self.resource_baseline
        )
        assert resource_diff == "", (
            f"Resource baseline mismatch for 14_jdbc_load.pipeline.yml: {resource_diff}"
        )

    # ------------------------------------------------------------------
    # 2 JDBC reader modes — one method per (table-mode, query-mode).
    # ------------------------------------------------------------------

    def test_jdbc_table_mode_matches_baseline(self):
        """JDBC table-mode: spark.read.format('jdbc').option('dbtable', ...).load()."""
        self._assert_baseline_match("jdbc_table_mode.py")

    def test_jdbc_query_mode_matches_baseline(self):
        """JDBC query-mode: spark.read.format('jdbc').option('query', ...).load() with operational metadata."""
        self._assert_baseline_match("jdbc_query_mode.py")
