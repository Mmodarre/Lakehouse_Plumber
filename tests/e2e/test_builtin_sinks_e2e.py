"""E2E tests for LHP built-in sinks (B2).

Covers the 3 streaming sink types documented in
``docs/actions/write_actions.rst``:

  - **delta sink** — ``dp.create_sink(format="delta", ...)`` + ``@dp.append_flow``
    targeting an external Unity Catalog table or Delta path.
  - **kafka sink** — ``dp.create_sink(format="kafka", ...)`` + ``@dp.append_flow``;
    auto-injects a runtime ``"value" not in df.columns`` guard.
  - **foreach_batch sink** — ``@dp.foreach_batch_sink`` decorator wrapping a
    user-supplied batch handler body, paired with ``@dp.append_flow`` on a
    function reading the source view.

Each test uses ``lhp generate --env dev --force`` (no ``--include-tests``) and
hash-compares the generated file under ``generated/dev/13_sinks/`` against the
baseline at ``generated_baseline/dev/13_sinks/``.

Fixture flowgroups live in ``tests/e2e/fixtures/testing_project/pipelines/13_sinks/``.
"""

import hashlib
import os
import shutil
from pathlib import Path

import pytest
from click.testing import CliRunner

from lhp.cli.main import cli


@pytest.mark.e2e
class TestBuiltinSinksE2E:
    """E2E tests for LHP built-in sink generation (Delta, Kafka, ForEachBatch)."""

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
        self.sinks_dir = self.generated_dir / "13_sinks"
        self.sinks_baseline_dir = (
            self.project_root / "generated_baseline" / "dev" / "13_sinks"
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
        result = runner.invoke(cli, ["generate", "--env", "dev", "--force"])
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
        """Generate, then assert the named sink file matches its baseline."""
        exit_code, output = self.run_generate()
        assert exit_code == 0, f"Generation failed: {output}"

        generated = self.sinks_dir / filename
        baseline = self.sinks_baseline_dir / filename
        assert generated.exists(), f"{filename} should be generated under 13_sinks/"
        assert baseline.exists(), f"Baseline {filename} should exist"

        diff = self._compare_file_hashes(generated, baseline)
        assert diff == "", f"Baseline mismatch for {filename}: {diff}"

    # ------------------------------------------------------------------
    # 3 sink types — one method per sink_type.
    # ------------------------------------------------------------------

    def test_delta_sink_matches_baseline(self):
        """Delta sink: dp.create_sink(format='delta') + @dp.append_flow streaming write."""
        self._assert_baseline_match("sk_delta.py")

    def test_kafka_sink_matches_baseline(self):
        """Kafka sink: dp.create_sink(format='kafka') + @dp.append_flow + runtime 'value' column guard."""
        self._assert_baseline_match("sk_kafka.py")

    def test_foreach_batch_sink_matches_baseline(self):
        """ForEachBatch sink: @dp.foreach_batch_sink wrapping inline batch_handler + @dp.append_flow."""
        self._assert_baseline_match("sk_foreach_batch.py")
