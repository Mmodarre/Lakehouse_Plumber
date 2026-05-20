"""E2E tests for LHP Python load action (B5.2).

Covers the Python load source documented in
``docs/actions/load_actions.rst:990-1090`` and unified in LHP v0.8.6 with the
custom_datasource/custom_sink copy-and-import pattern (commit ``c1058c07``):

  - The user's ``.py`` module is **copied** into
    ``<output>/custom_python_functions/<leaf>.py`` next to the generated
    pipeline file (with an ``LHP-SOURCE:`` provenance header).
  - The generated pipeline imports the loader function via
    ``from custom_python_functions.<leaf> import <function>``.
  - The source view is emitted with ``@dp.temporary_view()`` (NOT
    ``@dp.table``) — Python load creates an in-memory view, not a persisted
    table.

The fixture flowgroup and extractor module live under
``tests/e2e/fixtures/testing_project/pipelines/15_python_load/``. The test
verifies the generated pipeline ``.py`` matches the baseline AND the copied
module ``custom_python_functions/api_extractor.py`` and its ``__init__.py``
match their baselines.
"""

import hashlib
import os
import shutil
from pathlib import Path

import pytest
from click.testing import CliRunner

from lhp.cli.main import cli


@pytest.mark.e2e
class TestLoadPythonE2E:
    """E2E test for LHP Python load generation (v0.8.6 unified copy-and-import)."""

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
        self.pyload_dir = self.generated_dir / "15_python_load"
        self.pyload_baseline_dir = (
            self.project_root / "generated_baseline" / "dev" / "15_python_load"
        )
        self.resource_baseline = (
            self.project_root
            / "resources_baseline"
            / "lhp"
            / "15_python_load.pipeline.yml"
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
        result = runner.invoke(cli, ["generate", "--env", "dev"])
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

    def test_python_load_matches_baseline(self):
        """Python load: copy-and-import pattern, @dp.temporary_view(), parameters dict.

        Verifies all four artifacts of a Python-load generation:
          1. The pipeline .py file (with the import + decorator + parameters dict).
          2. The copied extractor module (with LHP-SOURCE provenance header).
          3. The custom_python_functions/ package __init__.py.
          4. The bundle resource YAML.
        """
        exit_code, output = self.run_generate()
        assert exit_code == 0, f"Generation failed: {output}"

        artifacts = [
            "python_load_basic.py",
            "custom_python_functions/api_extractor.py",
            "custom_python_functions/__init__.py",
        ]
        for rel_path in artifacts:
            generated = self.pyload_dir / rel_path
            baseline = self.pyload_baseline_dir / rel_path
            assert (
                generated.exists()
            ), f"{rel_path} should be generated under 15_python_load/"
            assert baseline.exists(), f"Baseline {rel_path} should exist"
            diff = self._compare_file_hashes(generated, baseline)
            assert diff == "", f"Baseline mismatch for {rel_path}: {diff}"

        # Verify the pipeline resource YAML also matches its baseline.
        generated_resource = self.resources_dir / "15_python_load.pipeline.yml"
        assert (
            generated_resource.exists()
        ), "15_python_load.pipeline.yml should be generated under resources/lhp/"
        assert (
            self.resource_baseline.exists()
        ), "Resource baseline 15_python_load.pipeline.yml should exist"
        resource_diff = self._compare_file_hashes(
            generated_resource, self.resource_baseline
        )
        assert (
            resource_diff == ""
        ), f"Resource baseline mismatch for 15_python_load.pipeline.yml: {resource_diff}"
