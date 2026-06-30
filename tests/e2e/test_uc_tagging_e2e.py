"""E2E tests for Unity Catalog tagging hook generation.

Covers the real CLI generate path (YAML -> generated/dev/<pipeline>/_uc_tagging_hook.py)
against byte-exact baselines, mirroring test_test_reporting_e2e.py. The dedicated
uc_tagging_* fixture pipelines exercise table-only / column-only / combined tags,
value coercions, materialized_view parity, token substitution in tag keys/values,
and the two negative emission cases (no tags, disabled).
"""

import hashlib
import os
import shutil
from pathlib import Path

import pytest
from click.testing import CliRunner

from lhp.cli.main import cli


@pytest.mark.e2e
class TestUCTaggingE2E:
    __test__ = True

    @pytest.fixture(autouse=True)
    def setup_test_project(self, isolated_project):
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
        if self.generated_dir.exists():
            shutil.rmtree(self.generated_dir)
        self.generated_dir.mkdir(parents=True, exist_ok=True)

        if self.resources_dir.exists():
            shutil.rmtree(self.resources_dir)
        self.resources_dir.mkdir(parents=True, exist_ok=True)

    def run_generate(self) -> tuple:
        """Run 'lhp generate --env dev'. Returns (exit_code, output)."""
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
        """Compare two files by SHA-256. Returns '' if identical."""

        def get_hash(f):
            return hashlib.sha256(f.read_bytes()).hexdigest()

        h1, h2 = get_hash(file1), get_hash(file2)
        if h1 != h2:
            return (
                f"Hash mismatch: {file1.name} ({h1[:12]}) != {file2.name} ({h2[:12]})"
            )
        return ""

    def test_core_hook_matches_baseline(self):
        """uc_tagging_core hook matches baseline (table/column tags + value coercions
        + multi-flowgroup aggregation + combined database: form)."""
        exit_code, output = self.run_generate()
        assert exit_code == 0, f"Generation failed: {output}"

        generated_file = self.generated_dir / "uc_tagging_core" / "_uc_tagging_hook.py"
        assert generated_file.exists(), "_uc_tagging_hook.py should be generated"

        baseline_file = (
            self.project_root
            / "generated_baseline"
            / "dev"
            / "uc_tagging_core"
            / "_uc_tagging_hook.py"
        )
        assert baseline_file.exists(), "Baseline file should exist"

        hash_diff = self._compare_file_hashes(generated_file, baseline_file)
        assert hash_diff == "", f"Baseline mismatch: {hash_diff}"

    def test_mv_hook_matches_baseline(self):
        """uc_tagging_mv hook matches baseline (materialized_view parity, table+column
        tags on one FQN, ${token}/{token} substitution in a tag key and value)."""
        exit_code, output = self.run_generate()
        assert exit_code == 0, f"Generation failed: {output}"

        generated_file = self.generated_dir / "uc_tagging_mv" / "_uc_tagging_hook.py"
        assert generated_file.exists(), "_uc_tagging_hook.py should be generated"

        baseline_file = (
            self.project_root
            / "generated_baseline"
            / "dev"
            / "uc_tagging_mv"
            / "_uc_tagging_hook.py"
        )
        assert baseline_file.exists(), "Baseline file should exist"

        hash_diff = self._compare_file_hashes(generated_file, baseline_file)
        assert hash_diff == "", f"Baseline mismatch: {hash_diff}"

    def test_no_hook_when_no_tags(self):
        """A taggable table with no tags anywhere produces no hook (emission gated off)."""
        exit_code, output = self.run_generate()
        assert exit_code == 0, f"Generation failed: {output}"

        hook_file = self.generated_dir / "uc_tagging_none" / "_uc_tagging_hook.py"
        assert not hook_file.exists(), (
            "Hook file should NOT exist when no tags are declared"
        )

    def test_no_hook_when_disabled(self):
        """With uc_tagging.enabled=false, no hook is emitted even for tagged tables."""
        lhp_yaml = self.project_root / "lhp.yaml"
        lhp_yaml.write_text(lhp_yaml.read_text() + "\nuc_tagging:\n  enabled: false\n")

        exit_code, output = self.run_generate()
        assert exit_code == 0, f"Generation failed: {output}"

        hook_file = self.generated_dir / "uc_tagging_core" / "_uc_tagging_hook.py"
        assert not hook_file.exists(), (
            "Hook file should NOT exist when uc_tagging is disabled"
        )
